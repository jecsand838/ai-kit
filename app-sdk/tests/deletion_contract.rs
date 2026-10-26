//! Explicit retained-history deletion through real LanceDB transactions.
//!
//! These faults obscure native receipts or delay read observation. They never
//! fabricate a successful commit, cancel an admitted native finalizer, or use a
//! second workflow implementation. Reopen here is a fresh App generation in
//! the same process; process-death recovery remains a separate proof.

mod app_v6_fixture;
mod lance_state_support;

use app_v6_fixture::AppV6Fixture;

use lance_state_support::LanceStateDirectory;

use std::collections::BTreeSet;
use std::future::Future;
use std::io::Write;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use agent_sdk::Agent;
use app_sdk::workflow::{
    CheckpointSelector, ExecutionOptions, Interaction, RestoreOptions, ResumePosition, RunSelector,
    RunSnapshot, RunStatus, Workflow, WorkflowDefinition, WorkflowFailure, WorkflowRun,
    WorkflowState,
};
use app_sdk::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::FutureExt;
use serde_json::{Map, Value, json};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateKeyPrefix, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::Notify;
use tracing::instrument::WithSubscriber;

#[derive(Clone, Copy)]
enum DeletionFault {
    None,
    LostPageReceipt,
    HeldSealReceipt,
    HeldPageReceipt,
    FailedDeletionPage,
    FailedAfterFirstPage,
    MigrationPageInterrupted,
    HeldRead,
    CapturedRead,
    CapturedCapacityRead,
    HeldRestoreAdmission,
    UnavailableCapacityRead,
    ReadCommitUnavailable,
    ReadCommitConflict,
    ReadCommitConflictPersistent,
    ReadLoadUnavailable,
    ReadLoadAndRollbackUnavailable,
    ReadRollbackUnavailable,
}

struct DeletionControl {
    fault: DeletionFault,
    armed: AtomicBool,
    begin_armed: AtomicBool,
    entered: Notify,
    release: Notify,
    read_key: Mutex<Option<StateKey>>,
    capacity_read: Mutex<Option<(tokio::task::Id, StateKey)>>,
    receipt_read_fault: AtomicBool,
    lost_commits: AtomicUsize,
    lost_receipt_reads: AtomicUsize,
    retained_snapshot_reads: AtomicUsize,
    read_commits: AtomicUsize,
    read_rollbacks: AtomicUsize,
    read_attempts: AtomicUsize,
    abandoned_receipts: AtomicUsize,
    committed_pages: Mutex<Vec<Vec<String>>>,
    page_limit: AtomicUsize,
    listed_pages: AtomicUsize,
    failed_pages: AtomicUsize,
}

impl DeletionControl {
    fn new(fault: DeletionFault) -> Arc<Self> {
        Arc::new(Self {
            fault,
            armed: AtomicBool::new(false),
            begin_armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            read_key: Mutex::new(None),
            capacity_read: Mutex::new(None),
            receipt_read_fault: AtomicBool::new(false),
            lost_commits: AtomicUsize::new(0),
            lost_receipt_reads: AtomicUsize::new(0),
            retained_snapshot_reads: AtomicUsize::new(0),
            read_commits: AtomicUsize::new(0),
            read_rollbacks: AtomicUsize::new(0),
            read_attempts: AtomicUsize::new(0),
            abandoned_receipts: AtomicUsize::new(0),
            committed_pages: Mutex::new(Vec::new()),
            page_limit: AtomicUsize::new(StateListRequest::MAX_LIMIT),
            listed_pages: AtomicUsize::new(0),
            failed_pages: AtomicUsize::new(0),
        })
    }

    fn assert_pages(&self, expected: &[StateRecordSnapshot]) {
        let pages = self.committed_pages.lock().unwrap().clone();
        assert!(
            pages.len() >= 2,
            "the real cleanup must cross a physical page"
        );
        assert!(
            pages
                .iter()
                .all(|page| !page.is_empty() && page.len() <= 256)
        );
        let actual: Vec<_> = pages.iter().flatten().cloned().collect();
        let unique: BTreeSet<_> = actual.iter().cloned().collect();
        let wanted: BTreeSet<_> = expected
            .iter()
            .map(|row| row.record.key.key().to_owned())
            .collect();
        assert_eq!(
            unique, wanted,
            "all and only this run's history/index rows are deleted"
        );
        assert_eq!(
            actual.len(),
            unique.len(),
            "a lost receipt must not replay an applied page"
        );
        assert_eq!(self.abandoned_receipts.load(Ordering::SeqCst), 0);
    }
}

struct DeletionStore {
    inner: StateStore,
    control: Arc<DeletionControl>,
}

struct DeletionTransaction {
    inner: Option<StateTransaction>,
    control: Arc<DeletionControl>,
    deleted: Vec<String>,
    deleting: bool,
    held_seal: bool,
    held_receipt: bool,
    selected_read: bool,
}

impl StateStoreBackend for DeletionStore {
    type Transaction = DeletionTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if matches!(self.control.fault, DeletionFault::UnavailableCapacityRead)
            && key.key().starts_with("active/")
            && self.control.armed.swap(false, Ordering::SeqCst)
        {
            return Err(StateStoreError::unavailable(
                "load",
                "one pre-seal capacity observation is unavailable",
            ));
        }
        if key.key().starts_with("receipt/")
            && self
                .control
                .receipt_read_fault
                .swap(false, Ordering::SeqCst)
        {
            self.control
                .lost_receipt_reads
                .fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "load",
                "one applied receipt observation is unavailable",
            ));
        }
        let capacity_owner = matches!(self.control.fault, DeletionFault::CapturedCapacityRead)
            && self
                .control
                .capacity_read
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|(owner, head)| tokio::task::try_id() == Some(*owner) && head == &key);
        let selected = (capacity_owner
            || matches!(
                self.control.fault,
                DeletionFault::HeldRead | DeletionFault::CapturedRead
            ) && self.control.read_key.lock().unwrap().as_ref() == Some(&key))
            && self.control.armed.swap(false, Ordering::SeqCst);
        if selected && matches!(self.control.fault, DeletionFault::HeldRead) {
            // Hold this exact selected head or immutable-row read before
            // native admission. Deletion then changes the real LanceDB result.
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        let snapshot = self.inner.load(key.clone()).await?;
        if matches!(self.control.fault, DeletionFault::CapturedCapacityRead) {
            if key.key().starts_with("active/")
                && snapshot.is_some()
                && self.control.armed.load(Ordering::SeqCst)
                && self.control.capacity_read.lock().unwrap().is_none()
            {
                let scope = key.key().strip_prefix("active/").unwrap();
                let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
                let Some(head) = self.inner.load(head_key.clone()).await? else {
                    return Ok(snapshot);
                };
                let sequence = head.record.value["sequence"].as_u64().unwrap();
                let Some(row) = self
                    .inner
                    .load(
                        StateKey::new(
                            "app-sdk",
                            format!("history/{scope}/checkpoint/{sequence:020}"),
                        )
                        .unwrap(),
                    )
                    .await?
                else {
                    return Ok(snapshot);
                };
                if head.record.value.pointer("/retention/state") == Some(&json!("active"))
                    && row.record.value.pointer("/checkpoint/phase/state")
                        == Some(&json!("succeeded"))
                {
                    self.control.capacity_read.lock().unwrap().get_or_insert((
                        tokio::task::try_id()
                            .expect("the real retention task owns the marker read"),
                        head_key,
                    ));
                }
            }
            if selected {
                assert_eq!(
                    snapshot
                        .as_ref()
                        .unwrap()
                        .record
                        .value
                        .pointer("/retention/state"),
                    Some(&json!("active"))
                );
                self.control.entered.notify_one();
                self.control.release.notified().await;
            } else if key.key() == "app"
                && !self.control.armed.load(Ordering::SeqCst)
                && self
                    .control
                    .capacity_read
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|(owner, _)| tokio::task::try_id() == Some(*owner))
            {
                assert_eq!(
                    snapshot.as_ref().unwrap().record.value["run_count"],
                    0,
                    "the same captured-read owner observes the actual competing retirement"
                );
                self.control.entered.notify_one();
            }
        }
        if selected && matches!(self.control.fault, DeletionFault::CapturedRead) {
            // Retain this real successful read while public deletion removes
            // the same head. The caller then rechecks its captured authority.
            assert!(snapshot.is_some());
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        Ok(snapshot)
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        if matches!(self.control.fault, DeletionFault::FailedAfterFirstPage)
            && request
                .prefix()
                .is_some_and(|prefix| prefix.starts_with("history/"))
            && self.control.armed.load(Ordering::SeqCst)
            && !self.control.committed_pages.lock().unwrap().is_empty()
        {
            self.control.failed_pages.fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::serialization(
                "selected page after the first committed deletion is unreadable",
            ));
        }
        if matches!(self.control.fault, DeletionFault::FailedDeletionPage)
            && request
                .prefix()
                .is_some_and(|prefix| prefix.starts_with("history/"))
            && self.control.armed.swap(false, Ordering::SeqCst)
        {
            self.control.failed_pages.fetch_add(1, Ordering::SeqCst);
            self.control.entered.notify_one();
            return Err(StateStoreError::serialization(
                "selected deletion page is unreadable",
            ));
        }
        self.control.listed_pages.fetch_add(1, Ordering::SeqCst);
        let limit = request
            .limit()
            .min(self.control.page_limit.load(Ordering::SeqCst));
        self.inner.list(request.with_limit(limit)?).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        if self.control.begin_armed.swap(false, Ordering::SeqCst) {
            if matches!(self.control.fault, DeletionFault::MigrationPageInterrupted) {
                self.control.failed_pages.fetch_add(1, Ordering::SeqCst);
                return Err(StateStoreError::serialization(
                    "one migration admission fails after an acknowledged deletion page",
                ));
            }
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        Ok(DeletionTransaction {
            inner: Some(self.inner.begin_transaction().await?),
            control: Arc::clone(&self.control),
            deleted: Vec::new(),
            deleting: false,
            held_seal: false,
            held_receipt: false,
            selected_read: false,
        })
    }
}

impl Drop for DeletionTransaction {
    fn drop(&mut self) {
        if self.held_receipt {
            self.control
                .abandoned_receipts
                .fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl StateTransactionBackend for DeletionTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if matches!(
            self.control.fault,
            DeletionFault::ReadLoadUnavailable
                | DeletionFault::ReadLoadAndRollbackUnavailable
                | DeletionFault::ReadCommitConflict
                | DeletionFault::ReadCommitConflictPersistent
        ) && self.control.read_key.lock().unwrap().as_ref() == Some(&key)
        {
            self.control.read_attempts.fetch_add(1, Ordering::SeqCst);
        }
        let selected = matches!(
            self.control.fault,
            DeletionFault::HeldRead
                | DeletionFault::CapturedRead
                | DeletionFault::ReadCommitUnavailable
                | DeletionFault::ReadCommitConflict
                | DeletionFault::ReadCommitConflictPersistent
                | DeletionFault::ReadLoadUnavailable
                | DeletionFault::ReadLoadAndRollbackUnavailable
                | DeletionFault::ReadRollbackUnavailable
                | DeletionFault::HeldRestoreAdmission
        ) && self.control.read_key.lock().unwrap().as_ref() == Some(&key)
            && if matches!(
                self.control.fault,
                DeletionFault::ReadCommitConflictPersistent
            ) {
                self.control.armed.load(Ordering::SeqCst)
            } else {
                self.control.armed.swap(false, Ordering::SeqCst)
            };
        self.selected_read |= selected;
        if selected
            && matches!(
                self.control.fault,
                DeletionFault::ReadLoadUnavailable | DeletionFault::ReadLoadAndRollbackUnavailable
            )
        {
            return Err(StateStoreError::unavailable(
                "load",
                "one real read transaction observation is unavailable",
            ));
        }
        if selected && matches!(self.control.fault, DeletionFault::HeldRead) {
            // The real read transaction already owns its immutable snapshot.
            // Hold this lookup while another connection deletes the live rows.
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        let snapshot = self.inner.as_mut().unwrap().load(key.clone()).await?;
        if selected && matches!(self.control.fault, DeletionFault::CapturedRead) {
            assert!(snapshot.is_some());
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        if selected
            && matches!(
                self.control.fault,
                DeletionFault::HeldRead | DeletionFault::CapturedRead
            )
        {
            let original = snapshot
                .as_ref()
                .expect("the admitted snapshot retains the row");
            let repeated = self
                .inner
                .as_mut()
                .unwrap()
                .load(key)
                .await?
                .expect("the same native snapshot retains the deleted row");
            assert_eq!(repeated.revision, original.revision);
            assert_eq!(repeated.record.key, original.record.key);
            assert_eq!(repeated.record.value, original.record.value);
            self.control
                .retained_snapshot_reads
                .fetch_add(1, Ordering::SeqCst);
        }
        self.deleting |= snapshot.as_ref().is_some_and(|snapshot| {
            snapshot.record.key.key().starts_with("run/")
                && snapshot
                    .record
                    .value
                    .pointer("/retention/state")
                    .and_then(Value::as_str)
                    == Some("deleting")
        });
        Ok(snapshot)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        if matches!(self.control.fault, DeletionFault::HeldSealReceipt)
            && record.key.key().starts_with("run/")
            && record.value.pointer("/retention/state") == Some(&json!("deleting"))
            && self.control.armed.swap(false, Ordering::SeqCst)
        {
            self.held_seal = true;
        }
        self.inner.as_mut().unwrap().put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        if key.key().starts_with("history/") {
            self.deleted.push(key.key().to_owned());
        }
        self.inner.as_mut().unwrap().delete(key).await
    }

    async fn commit(mut self) -> Result<(), StateStoreError> {
        self.inner.take().unwrap().commit().await?;
        if self.selected_read {
            self.control.read_commits.fetch_add(1, Ordering::SeqCst);
            // These no-write receipt faults prove App's caller-Store error
            // handling; they do not claim a native Lance serialization failure.
            match self.control.fault {
                DeletionFault::HeldRestoreAdmission => {
                    // Capture and settle the genuine coherent source read first.
                    // The competing edit then precedes the next native snapshot,
                    // so rejection cannot be a stale read transaction's conflict.
                    self.control.begin_armed.store(true, Ordering::SeqCst);
                }
                DeletionFault::ReadCommitUnavailable => {
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "one completed read transaction receipt is unavailable",
                    ));
                }
                DeletionFault::ReadCommitConflict | DeletionFault::ReadCommitConflictPersistent => {
                    return Err(StateStoreError::transaction_conflict(
                        "commit",
                        "selected read transaction reports a serialization rejection",
                    ));
                }
                _ => {}
            }
        }
        // Normal commits also unlink projection rows. Record every history
        // deletion only when this transaction observed an admitted deletion.
        if self.deleting && !self.deleted.is_empty() {
            self.control
                .committed_pages
                .lock()
                .unwrap()
                .push(std::mem::take(&mut self.deleted));
            if matches!(self.control.fault, DeletionFault::MigrationPageInterrupted)
                && self.control.armed.swap(false, Ordering::SeqCst)
            {
                self.control.begin_armed.store(true, Ordering::SeqCst);
            }
            if matches!(self.control.fault, DeletionFault::HeldPageReceipt)
                && self.control.armed.swap(false, Ordering::SeqCst)
            {
                self.held_seal = true;
            }
            if matches!(self.control.fault, DeletionFault::LostPageReceipt)
                && self.control.armed.swap(false, Ordering::SeqCst)
            {
                self.control.lost_commits.fetch_add(1, Ordering::SeqCst);
                self.control
                    .receipt_read_fault
                    .store(true, Ordering::SeqCst);
                return Err(StateStoreError::unavailable(
                    "commit",
                    "one real applied page receipt is unavailable",
                ));
            }
        }
        if self.held_seal {
            // The real transaction already committed and cleaned up. The
            // accepted App operation must retain this receipt observation.
            self.held_receipt = true;
            self.control.entered.notify_one();
            self.control.release.notified().await;
            self.held_receipt = false;
        }
        Ok(())
    }

    async fn rollback(mut self) -> Result<(), StateStoreError> {
        self.inner.take().unwrap().rollback().await?;
        if self.selected_read {
            self.control.read_rollbacks.fetch_add(1, Ordering::SeqCst);
            if matches!(
                self.control.fault,
                DeletionFault::ReadRollbackUnavailable
                    | DeletionFault::ReadLoadAndRollbackUnavailable
            ) {
                return Err(StateStoreError::unavailable(
                    "rollback",
                    "one completed read rollback receipt is unavailable",
                ));
            }
        }
        Ok(())
    }
}

struct DeletionCase {
    app: App<DeletionStore, Agent>,
    raw: StateStore,
    control: Arc<DeletionControl>,
    terminal_timeout: Duration,
}

impl DeletionCase {
    fn definition(length: usize) -> WorkflowDefinition {
        let nodes: Vec<_> = (0..length).map(|index| json!({
            "id":format!("n{index:03}"),
            "step":{"kind":"state","mutations":[{
                "kind":"assign","target":"count","value":{"kind":"literal","value":index + 1}
            }]}
        })).collect();
        let routes: Map<String, Value> = (0..length - 1).map(|index| (
            format!("n{index:03}"),
            json!({"mode":"first","branches":[{"target":{"kind":"node","id":format!("n{:03}",index + 1)}}]})
        )).collect();
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2","id":"deletion","revision":"r1",
            "state":{"count":0,"left":10,"right":20},"required":["count"],
            "reducers":{"count":"reject_concurrent","left":"reject_concurrent","right":"reject_concurrent"},
                "entry":"n000","nodes":nodes,"routes":routes
            })
            .to_string(),
        )
        .unwrap()
    }

    async fn new(length: usize, fault: DeletionFault) -> (Self, Workflow<DeletionStore, Agent>) {
        Self::with_state(
            length,
            fault,
            StateStoreConfig::Lance(LanceStateStoreConfig::memory()),
        )
        .await
    }

    async fn with_state(
        length: usize,
        fault: DeletionFault,
        config: StateStoreConfig,
    ) -> (Self, Workflow<DeletionStore, Agent>) {
        // Preserve the existing Lance supervision budget for 130 native steps
        // and multiple physical deletion pages. App deadlines are unchanged.
        let terminal_timeout = Duration::from_secs(120);
        let raw = StateStore::connect(config).await.unwrap();
        let control = DeletionControl::new(fault);
        let app = App::open(AppConfig::new(
            DeletionStore {
                inner: raw.clone(),
                control: Arc::clone(&control),
            },
            Vec::new(),
            vec![Self::definition(length)],
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
            .workflow("deletion".parse().unwrap())
            .await
            .unwrap();
        (
            Self {
                app,
                raw,
                control,
                terminal_timeout,
            },
            workflow,
        )
    }

    async fn terminal(
        workflow: &Workflow<DeletionStore, Agent>,
        raw: &StateStore,
        terminal_timeout: Duration,
    ) -> (WorkflowRun<DeletionStore, Agent>, RunSnapshot) {
        let started = Instant::now();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        let observed = tokio::time::timeout(terminal_timeout, run.wait()).await;
        eprintln!(
            "deletion workflow observation: elapsed_ms={} timeout_secs={} completed={}",
            started.elapsed().as_millis(),
            terminal_timeout.as_secs(),
            matches!(&observed, Ok(Ok(_))),
        );
        let snapshot = observed.expect("bounded workflow completion").unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        let scope = Self::scope(raw, &run).await;
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let head = raw
                    .load(StateKey::new("app-sdk", format!("run/{scope}")).unwrap())
                    .await
                    .unwrap()
                    .unwrap();
                if head.record.value.pointer("/retention/state") == Some(&json!("retained"))
                    && raw
                        .load(StateKey::new("app-sdk", format!("active/{scope}")).unwrap())
                        .await
                        .unwrap()
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("terminal cleanup releases active capacity before a read fault is armed");
        (run, snapshot)
    }

    async fn records(raw: &StateStore, prefix: &str) -> Vec<StateRecordSnapshot> {
        let mut after = None;
        let mut rows = Vec::new();
        for _ in 0..64 {
            let mut request = StateListRequest::new("app-sdk")
                .unwrap()
                .with_limit(256)
                .unwrap();
            if !prefix.is_empty() {
                request = request.with_key_prefix(StateKeyPrefix::new(prefix).unwrap());
            }
            if let Some(after) = after.take() {
                request = request.start_after(after).unwrap();
            }
            let page = raw.list(request).await.unwrap();
            rows.extend(page.records);
            match page.next_start_after {
                Some(next) => after = Some(next),
                None => return rows,
            }
        }
        panic!("fixture inventory exceeded its bounded pages");
    }

    async fn scope(raw: &StateStore, run: &WorkflowRun<DeletionStore, Agent>) -> String {
        let heads = Self::records(raw, "run/").await;
        heads
            .iter()
            .find(|row| row.record.value["id"] == run.id().to_string())
            .unwrap()
            .record
            .key
            .key()
            .strip_prefix("run/")
            .unwrap()
            .to_owned()
    }

    async fn assert_absent(raw: &StateStore, scope: &str) {
        assert!(
            raw.load(StateKey::new("app-sdk", format!("run/{scope}")).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            raw.load(StateKey::new("app-sdk", format!("active/{scope}")).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            Self::records(raw, &format!("history/{scope}/"))
                .await
                .is_empty()
        );
    }

    async fn close_after(mut self, superseded: bool, observation: impl Future<Output = ()>) {
        let observed = AssertUnwindSafe(tokio::time::timeout(
            self.terminal_timeout + Duration::from_secs(15),
            observation,
        ))
        .catch_unwind()
        .await;
        self.control.release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(6),
            self.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        )
        .await;
        let abandoned = self.control.abandoned_receipts.load(Ordering::SeqCst);
        drop(self.app);
        match observed {
            Ok(result) => result.expect("bounded deletion observation"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match closed.expect("all admitted deletion operations join") {
            Ok(()) => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(error) => panic!("owned deletion cleanup failed: {error}"),
        }
        assert_eq!(
            abandoned, 0,
            "dropping an observer never abandons an accepted receipt"
        );
    }

    async fn page_case(fault: DeletionFault) {
        Self::page_case_with_state(
            fault,
            StateStoreConfig::Lance(LanceStateStoreConfig::memory()),
        )
        .await;
    }

    async fn page_case_with_state(fault: DeletionFault, config: StateStoreConfig) {
        let (case, workflow) = Self::with_state(130, fault, config).await;
        let raw = case.raw.clone();
        let control = Arc::clone(&case.control);
        let terminal_timeout = case.terminal_timeout;
        case.close_after(false, async {
            let (mut source, terminal) = Self::terminal(&workflow, &raw, terminal_timeout).await;
            let scope = Self::scope(&raw, &source).await;
            let rows = Self::records(&raw, &format!("history/{scope}/")).await;
            assert!(
                rows.len() > 256,
                "the case must require multiple real deletion pages"
            );
            let sibling = workflow.prepare(WorkflowState::default()).await.unwrap();
            let fork = workflow
                .restore(RestoreOptions {
                    source: RunSelector::Latest(source.id().clone()),
                    checkpoint: CheckpointSelector::Latest,
                    overrides: WorkflowState::default(),
                    position: ResumePosition::AfterCompleted,
                    fallback: None,
                })
                .await
                .unwrap();
            let fork_before = fork.snapshot().await.unwrap();
            control.armed.store(true, Ordering::SeqCst);
            source.delete(terminal.checkpoint).await.unwrap();
            Self::assert_absent(&raw, &scope).await;
            assert!(matches!(
                source.snapshot().await,
                Err(AppError::NotFound(_))
            ));
            assert!(matches!(
                source.history(None, NonZeroUsize::new(1).unwrap()).await,
                Err(AppError::NotFound(_))
            ));
            assert_eq!(sibling.snapshot().await.unwrap().state.values()["count"], 0);
            let after = fork.snapshot().await.unwrap();
            assert_eq!(after.state, fork_before.state);
            assert!(
                after.outcomes == fork_before.outcomes,
                "the copied fork does not depend on source rows"
            );
            assert!(
                workflow
                    .list_runs(None, NonZeroUsize::new(10).unwrap())
                    .await
                    .unwrap()
                    .iter()
                    .all(|row| &row.id != source.id())
            );
            control.assert_pages(&rows);
            let expected_loss = usize::from(matches!(fault, DeletionFault::LostPageReceipt));
            assert_eq!(control.lost_commits.load(Ordering::SeqCst), expected_loss);
            assert_eq!(
                control.lost_receipt_reads.load(Ordering::SeqCst),
                expected_loss
            );
        })
        .await;
    }
}

#[tokio::test]
async fn terminal_deletion_uses_bounded_pages_and_preserves_other_runs_and_copied_forks() {
    DeletionCase::page_case(DeletionFault::None).await;
}

#[tokio::test]
async fn an_applied_page_with_lost_commit_and_receipt_observations_is_not_replayed() {
    DeletionCase::page_case(DeletionFault::LostPageReceipt).await;
}

#[tokio::test]
async fn a_fresh_app_resumes_a_sealed_deletion_after_its_observer_is_dropped() {
    let (case, workflow) = DeletionCase::new(3, DeletionFault::HeldSealReceipt).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(true, async {
        let (mut source, terminal) =
            DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        let observer = workflow
            .open_run(RunSelector::Latest(source.id().clone()))
            .await
            .unwrap();
        let scope = DeletionCase::scope(&raw, &source).await;
        control.armed.store(true, Ordering::SeqCst);
        let mut deletion = Box::pin(source.delete(terminal.checkpoint));
        tokio::select! {
            _ = control.entered.notified() => {}
            result = &mut deletion => panic!("seal receipt was not retained: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("real seal was not committed"),
        }
        drop(deletion);
        let head = raw
            .load(StateKey::new("app-sdk", format!("run/{scope}")).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            head.record.value.pointer("/retention/state"),
            Some(&json!("deleting"))
        );
        assert!(
            !DeletionCase::records(&raw, &format!("history/{scope}/"))
                .await
                .is_empty()
        );
        assert!(matches!(
            observer.snapshot().await,
            Err(AppError::Closed(_))
        ));
        assert!(matches!(
            observer.history(None, NonZeroUsize::new(1).unwrap()).await,
            Err(AppError::Closed(_))
        ));
        assert!(matches!(
            workflow
                .restore(RestoreOptions {
                    source: RunSelector::Latest(observer.id().clone()),
                    checkpoint: CheckpointSelector::Latest,
                    overrides: WorkflowState::default(),
                    position: ResumePosition::AfterCompleted,
                    fallback: None,
                })
                .await,
            Err(AppError::Closed(_))
        ));

        let mut fresh: App<StateStore, Agent> = App::open(AppConfig::new(
            raw.clone(),
            Vec::new(),
            vec![DeletionCase::definition(3)],
        ))
        .await
        .unwrap();
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if raw.load(head.record.key.clone()).await.unwrap().is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            DeletionCase::assert_absent(&raw, &scope).await;
            assert_eq!(control.abandoned_receipts.load(Ordering::SeqCst), 0);
        }))
        .catch_unwind()
        .await;
        control.release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(6),
            fresh.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        )
        .await;
        drop(fresh);
        match observed {
            Ok(result) => result.expect("fresh tracked deletion completes"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed
            .expect("fresh App cleanup joins")
            .expect("fresh App closes");
    })
    .await;
}

impl DeletionCase {
    async fn migrate_sealed_deletion_golden(interrupt: bool) {
        let fixture = AppV6Fixture::copy("sealed-deletion");
        let expected = fixture.expected();
        let raw = StateStore::connect(fixture.config()).await.unwrap();
        let before = fixture.records(&raw).await;
        let scope = expected["scope"].as_str().unwrap();
        let root_before = before
            .iter()
            .find(|row| row.record.key.key() == "app")
            .unwrap();
        assert_eq!(root_before.record.value["epoch"], "app/v6");
        let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let head_before = raw.load(head_key.clone()).await.unwrap().unwrap();
        assert_eq!(*head_before.record.value, expected["head_after_shutdown"]);
        assert_eq!(
            head_before.revision.get(),
            expected["head_revision"].as_u64().unwrap()
        );
        let old_history = Self::records(&raw, &format!("history/{scope}/")).await;
        assert_eq!(
            old_history.len(),
            expected["remaining_keys"].as_array().unwrap().len()
        );
        assert_eq!(expected["deleted_keys"].as_array().unwrap().len(), 256);
        for key in expected["deleted_keys"].as_array().unwrap() {
            assert!(
                raw.load(StateKey::new("app-sdk", key.as_str().unwrap()).unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        if interrupt {
            let control = DeletionControl::new(DeletionFault::MigrationPageInterrupted);
            control.armed.store(true, Ordering::SeqCst);
            let result = App::<DeletionStore, Agent>::open(AppConfig::new(
                DeletionStore {
                    inner: raw.clone(),
                    control: Arc::clone(&control),
                },
                Vec::new(),
                vec![Self::definition(
                    expected["definition_length"].as_u64().unwrap() as usize,
                )],
            ))
            .await;
            if let Ok(mut app) = result {
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(20),
                })
                .await
                .unwrap();
                panic!("the admission after the acknowledged migration deletion page must fail");
            }
            assert!(matches!(result, Err(AppError::Integration(_))));
            assert_eq!(control.failed_pages.load(Ordering::SeqCst), 1);
            let published = control.committed_pages.lock().unwrap().clone();
            assert_eq!(published.len(), 1);
            assert_eq!(published[0].len(), 256);
            for key in &published[0] {
                assert!(
                    raw.load(StateKey::new("app-sdk", key).unwrap())
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            assert_eq!(
                Self::records(&raw, &format!("history/{scope}/"))
                    .await
                    .len(),
                old_history.len() - 256
            );
            let stopped = raw
                .load(root_before.record.key.clone())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                stopped.revision.get(),
                root_before.revision.get() + 2,
                "one claim and one actual deletion page"
            );
            assert_eq!(
                stopped.record.value["generation"],
                root_before.record.value["generation"]
            );
            assert_eq!(stopped.record.value["migration"]["state"], "migrating");
            assert_eq!(
                stopped.record.value["migration"]["phase"]["phase"],
                "deletions"
            );
            assert_eq!(control.abandoned_receipts.load(Ordering::SeqCst), 0);
        }
        let mut app = App::<StateStore, Agent>::open(AppConfig::new(
            raw.clone(),
            Vec::new(),
            vec![Self::definition(
                expected["definition_length"].as_u64().unwrap() as usize,
            )],
        ))
        .await
        .expect("preflight accepts the sealed deletion's already removed prefix");
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let root = raw
                .load(StateKey::new("app-sdk", "app").unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(root.record.value["epoch"], "app/v7");
            assert_eq!(
                root.record.value["generation"].as_u64().unwrap(),
                root_before.record.value["generation"].as_u64().unwrap() + 1
            );
            assert!(
                raw.load(head_key).await.unwrap().is_none(),
                "the original seal is finished before open returns"
            );
            assert!(
                Self::records(&raw, &format!("history/{scope}/"))
                    .await
                    .is_empty()
            );
            for row in fixture.records(&raw).await {
                assert_eq!(row.record.value["epoch"], "app/v7");
            }
            let parts = scope.split('/').collect::<Vec<_>>();
            let workspace = app.workspace(parts[0].parse().unwrap()).await.unwrap();
            let workflow = workspace.workflow(parts[1].parse().unwrap()).await.unwrap();
            assert!(matches!(
                workflow
                    .open_run(RunSelector::Latest(
                        expected["run"].as_str().unwrap().parse().unwrap()
                    ))
                    .await,
                Err(AppError::NotFound(_))
            ));
            let mut next = workflow.prepare(WorkflowState::default()).await.unwrap();
            assert_ne!(next.id().to_string(), expected["run"].as_str().unwrap());
            let snapshot = next.snapshot().await.unwrap();
            assert_eq!(snapshot.checkpoint, 0);
            assert!(matches!(snapshot.status, RunStatus::Paused));
            next.cancel(snapshot.checkpoint).await.unwrap();
            let terminal = next.wait().await.unwrap();
            assert!(matches!(terminal.status, RunStatus::Cancelled));
            next.delete(terminal.checkpoint).await.unwrap();
            assert!(matches!(
                workflow
                    .open_run(RunSelector::Latest(next.id().clone()))
                    .await,
                Err(AppError::NotFound(_))
            ));
        }))
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "v6-sealed-deletion phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(raw);
        match checked {
            Ok(result) => result.expect("bounded real migration/deletion"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            .expect("joined migration cleanup");
    }

    async fn export_sealed_deletion_golden() {
        let retained = std::env::var_os("AI_KIT_APP_GOLDEN_OUTPUT").map(std::path::PathBuf::from);
        let directory = retained.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!(
                "app-sealed-deletion-golden-{}",
                uuid::Uuid::new_v4(),
            ))
        });
        assert!(directory.is_absolute(), "golden output must be absolute");
        std::fs::create_dir(&directory).expect("new invocation-owned golden directory");
        let checked = AssertUnwindSafe(async {
            let (mut case, workflow) = Self::with_state(130, DeletionFault::FailedAfterFirstPage,
                StateStoreConfig::Lance(LanceStateStoreConfig::directory(directory.join("state"))),
            ).await;
            let observed = AssertUnwindSafe(tokio::time::timeout(case.terminal_timeout + Duration::from_secs(15), async {
                let (mut source, terminal) = Self::terminal(&workflow, &case.raw, case.terminal_timeout).await;
                let scope = Self::scope(&case.raw, &source).await;
                let before = Self::records(&case.raw, &format!("history/{scope}/")).await;
                assert!(before.len() > 256, "actual history spans the deletion page boundary");
                case.control.armed.store(true, Ordering::SeqCst);
                assert!(matches!(source.delete(terminal.checkpoint).await, Err(AppError::Integration(_))),
                    "the selected second page fails after one actual atomic deletion");
                let pages = case.control.committed_pages.lock().unwrap().clone();
                assert_eq!(pages.len(), 1);
                assert_eq!(pages[0].len(), 256, "a complete original page was really deleted");
                assert_eq!(pages[0], before[..256].iter().map(|row| row.record.key.key().to_owned()).collect::<Vec<_>>());
                let remaining = Self::records(&case.raw, &format!("history/{scope}/")).await;
                assert_eq!(remaining.len(), before.len() - 256);
                for (actual, expected) in remaining.iter().zip(&before[256..]) {
                    assert_eq!(actual.record.key, expected.record.key);
                    assert_eq!(actual.revision, expected.revision);
                    assert_eq!(actual.record.value, expected.record.value);
                }
                assert_eq!(case.control.failed_pages.load(Ordering::SeqCst), 1);
                assert_eq!(case.control.abandoned_receipts.load(Ordering::SeqCst), 0);
                let head = case.raw.load(StateKey::new("app-sdk", format!("run/{scope}")).unwrap()).await.unwrap().unwrap();
                assert_eq!(head.record.value.pointer("/retention/state"), Some(&json!("deleting")));
                json!({
                    "epoch": head.record.value["epoch"], "definition_length": 130,
                    "scope": scope, "run": terminal.id.to_string(), "occurrence": terminal.occurrence,
                    "checkpoint": terminal.checkpoint, "state": terminal.state.values(),
                    "head_before_shutdown": head.record.value,
                    "deleted_keys": pages[0],
                    "remaining_keys": remaining.iter().map(|row| row.record.key.key()).collect::<Vec<_>>(),
                    "original_history_rows": before.len(),
                    "evidence": "real App and Lance; controlled failure after first full deletion page; no Agent instances",
                })
            })).catch_unwind().await;
            let cleanup = AssertUnwindSafe(case.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            })).catch_unwind().await;
            if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                loop {
                    let joined = AssertUnwindSafe(case.app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(4),
                    })).catch_unwind().await;
                    eprintln!("deletion-golden phase=settlement original={cleanup:?} subsequent={joined:?}");
                    // This fixture requests only valid CancelAndDrain. A retained
                    // Integration is joined failure, not successful publication.
                    if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            drop(workflow);
            drop(case.app);
            let mut manifest = match observed {
                Ok(result) => result.expect("original bounded deletion/generation budget"),
                Err(panic) => std::panic::resume_unwind(panic),
            };
            assert!(matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))),
                "the original cleanup result must establish joined ownership");
            let head = case.raw.load(StateKey::new("app-sdk", format!("run/{}", manifest["scope"].as_str().unwrap())).unwrap()).await.unwrap().unwrap();
            assert_eq!(head.record.value.pointer("/retention/state"), Some(&json!("deleting")));
            manifest["head_after_shutdown"] = (*head.record.value).clone();
            manifest["head_revision"] = Value::from(head.revision.get());
            assert_eq!(case.control.committed_pages.lock().unwrap().len(), 1);
            assert_eq!(case.control.failed_pages.load(Ordering::SeqCst), 1);
            assert_eq!(case.control.abandoned_receipts.load(Ordering::SeqCst), 0);
            drop(case.raw);
            eprintln!("deletion-golden phase=settled real_deleted_rows=256 retained_error=integration ownership=joined");
            manifest
        }).catch_unwind().await;
        match checked {
            Ok(manifest) if retained.is_some() => {
                std::fs::write(
                    directory.join("expected.json"),
                    serde_json::to_vec_pretty(&manifest).unwrap(),
                )
                .unwrap();
            }
            outcome => {
                let removed = std::fs::remove_dir_all(&directory);
                if let Err(error) = &removed {
                    eprintln!("deletion-golden secondary cleanup: {error}");
                }
                if let Err(panic) = outcome {
                    std::panic::resume_unwind(panic);
                }
                removed.expect("remove settled fixture directory");
            }
        }
    }

    async fn reopen_sealed_pages(fault: DeletionFault) {
        let (case, workflow) = Self::new(3, DeletionFault::HeldSealReceipt).await;
        let raw = case.raw.clone();
        let original = Arc::clone(&case.control);
        let workspace_id = case
            .app
            .list_workspaces(None, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap()[0]
            .id
            .clone();
        let recovery = DeletionControl::new(fault);
        recovery.page_limit.store(1, Ordering::SeqCst);
        recovery.armed.store(true, Ordering::SeqCst);
        let terminal_timeout = case.terminal_timeout;
        case.close_after(true, async {
            let mut fresh = None;
            let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
                let (unrelated, unrelated_terminal) = Self::terminal(&workflow, &raw, terminal_timeout).await;
                let unrelated_scope = Self::scope(&raw, &unrelated).await;
                let mut deleted = Vec::new();
                for _ in 0..3 {
                    let (mut run, terminal) = Self::terminal(&workflow, &raw, terminal_timeout).await;
                    let scope = Self::scope(&raw, &run).await;
                    original.armed.store(true, Ordering::SeqCst);
                    let mut deletion = Box::pin(run.delete(terminal.checkpoint));
                    tokio::select! {
                        _ = original.entered.notified() => {},
                        result = &mut deletion => panic!("each original seal receipt remains owned: {}", result.is_ok()),
                    }
                    drop(deletion);
                    drop(run);
                    deleted.push(scope);
                }
                let mut opening = Box::pin(App::<DeletionStore, Agent>::open(AppConfig::new(
                    DeletionStore { inner: raw.clone(), control: Arc::clone(&recovery) },
                    Vec::new(), vec![Self::definition(3)],
                )));
                if matches!(fault, DeletionFault::HeldPageReceipt) {
                    tokio::select! {
                        _ = recovery.entered.notified() => {},
                        result = &mut opening => {
                            fresh = Some(result.expect("startup with valid sealed pages"));
                            tokio::time::timeout(Duration::from_secs(3), recovery.entered.notified()).await
                                .expect("early-returned startup still admits the selected native deletion receipt");
                            panic!("startup returned before its accepted deletion receipt settled");
                        }
                    }
                    assert!(tokio::time::timeout(Duration::from_millis(25), &mut opening).await.is_err(),
                        "open must retain observation through the current deletion owner");
                    assert_eq!(recovery.committed_pages.lock().unwrap().len(), 1,
                        "holding one receipt must not admit cleanup for every later head");
                    recovery.release.notify_one();
                    fresh = Some(opening.await.unwrap());
                } else {
                    match opening.await {
                        Err(AppError::Integration(_)) => {},
                        Err(error) => panic!("permanent page failure keeps its integration classification: {error}"),
                        Ok(app) => {
                            fresh = Some(app);
                            tokio::time::timeout(Duration::from_secs(3), recovery.entered.notified()).await
                                .expect("early-returned startup reaches the permanent deletion-page failure");
                            panic!("startup must report its sealed-deletion failure");
                        }
                    }
                    for scope in &deleted {
                        assert!(!Self::records(&raw, &format!("history/{scope}/")).await.is_empty(),
                            "failed cleanup must preserve histories that have not been deleted");
                    }
                    fresh = Some(App::open(AppConfig::new(
                        DeletionStore { inner: raw.clone(), control: Arc::clone(&recovery) },
                        Vec::new(), vec![Self::definition(3)],
                    )).await.unwrap());
                }
                for scope in &deleted {
                    Self::assert_absent(&raw, scope).await;
                }
                assert!(!Self::records(&raw, &format!("history/{unrelated_scope}/")).await.is_empty());
                let workspace = fresh.as_ref().unwrap().workspace(workspace_id).await.unwrap();
                let retained = workspace.workflow("deletion".parse().unwrap()).await.unwrap()
                    .open_run(RunSelector::Exact { id: unrelated_terminal.id, occurrence: unrelated_terminal.occurrence }).await.unwrap();
                assert_eq!(retained.snapshot().await.unwrap().state.values()["count"], 3);
                assert!(recovery.listed_pages.load(Ordering::SeqCst) > 3);
                assert!(recovery.committed_pages.lock().unwrap().iter().all(|page| page.len() == 1));
                assert_eq!(original.abandoned_receipts.load(Ordering::SeqCst), 0);
                assert_eq!(recovery.abandoned_receipts.load(Ordering::SeqCst), 0);
            })).catch_unwind().await;
            original.release.notify_waiters();
            recovery.release.notify_one();
            let closed = match fresh.as_mut() {
                Some(app) => Some(tokio::time::timeout(Duration::from_secs(6), app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(4),
                })).await),
                None => None,
            };
            drop(fresh);
            if let Some(closed) = closed {
                let outcome = closed.expect("recovery owners join");
                if matches!(fault, DeletionFault::FailedDeletionPage) {
                    assert!(outcome.is_ok() || matches!(outcome, Err(AppError::Integration(_))),
                        "cleanup preserves the injected permanent failure classification");
                } else {
                    outcome.unwrap();
                }
            }
            if matches!(fault, DeletionFault::FailedDeletionPage) {
                assert_eq!(recovery.failed_pages.load(Ordering::SeqCst), 1,
                    "the permanent failure was reached exactly once before cleanup completed");
            } else {
                assert!(!recovery.committed_pages.lock().unwrap().is_empty(),
                    "the selected held receipt follows an actual committed deletion page");
            }
            assert_eq!(recovery.abandoned_receipts.load(Ordering::SeqCst), 0);
            match checked {
                Ok(result) => result.expect("short-page deletion recovery remains bounded"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }).await;
    }
}

#[tokio::test]
async fn cold_sealed_deletion_after_one_full_page_is_retained_for_migration() {
    DeletionCase::export_sealed_deletion_golden().await;
}

#[tokio::test]
async fn reopening_waits_for_each_sealed_deletion_before_advancing_short_pages() {
    DeletionCase::reopen_sealed_pages(DeletionFault::HeldPageReceipt).await;
}

#[tokio::test]
async fn reopening_reports_sealed_deletion_failure_without_losing_retained_history() {
    DeletionCase::reopen_sealed_pages(DeletionFault::FailedDeletionPage).await;
}

#[tokio::test]
async fn a_read_crossing_sealed_deletion_reports_closure_instead_of_corrupt_history() {
    let (case, workflow) = DeletionCase::new(3, DeletionFault::HeldRead).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(false, async {
        let (source, terminal) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        let mut deleter = workflow.open_run(RunSelector::Latest(source.id().clone())).await.unwrap();
        let scope = DeletionCase::scope(&raw, &source).await;
        *control.read_key.lock().unwrap() = Some(StateKey::new(
            "app-sdk", format!("history/{scope}/checkpoint/{:020}", terminal.checkpoint),
        ).unwrap());
        control.armed.store(true, Ordering::SeqCst);
        let mut reading = Box::pin(source.snapshot());
        tokio::select! {
            _ = control.entered.notified() => {}
            result = &mut reading => panic!("read did not retain its captured head: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("checkpoint read not observed"),
        }
        deleter.delete(terminal.checkpoint).await.unwrap();
        DeletionCase::assert_absent(&raw, &scope).await;
        control.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(3), &mut reading).await.unwrap();
        assert!(matches!(result, Err(AppError::Closed(_) | AppError::NotFound(_))),
            "a legitimately removed row is a closed/missing scope, not storage corruption");
        assert_eq!(control.retained_snapshot_reads.load(Ordering::SeqCst), 1,
            "the original native snapshot retains the deleted row before the fresh scope fence rejects it");
        drop(reading);
    }).await;
}

#[tokio::test]
async fn terminal_execution_read_crossing_explicit_deletion_keeps_app_usable() {
    for (fault, checkpoint_offset) in [
        (DeletionFault::HeldRead, None),
        (DeletionFault::HeldRead, Some(0)),
        (DeletionFault::HeldRead, Some(1)),
        (DeletionFault::CapturedRead, Some(1)),
    ] {
        let (case, workflow) = DeletionCase::new(3, fault).await;
        let raw = case.raw.clone();
        let control = Arc::clone(&case.control);
        let terminal_timeout = case.terminal_timeout;
        case.close_after(false, async {
        let (mut source, terminal) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        let mut deleter = workflow.open_run(RunSelector::Latest(source.id().clone())).await.unwrap();
        let scope = DeletionCase::scope(&raw, &source).await;
        // Initial public acquisition reads real authority without a fault.
        // Hold the next head/current/predecessor read, or the successful final
        // predecessor read immediately before the captured-head recheck.
        let mut execution = source.execution(ExecutionOptions::default()).await.unwrap();
        let selected = match checkpoint_offset {
            None => format!("run/{scope}"),
            Some(offset) => format!("history/{scope}/checkpoint/{:020}",
                terminal.checkpoint.checked_sub(offset).unwrap()),
        };
        *control.read_key.lock().unwrap() = Some(StateKey::new("app-sdk", selected).unwrap());
        control.armed.store(true, Ordering::SeqCst);
        let mut reading = Box::pin(execution.next());
        tokio::select! {
            _ = control.entered.notified() => {}
            result = &mut reading => panic!("execution did not reach its held authority read: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("terminal execution authority read not observed"),
        }
        assert_eq!(control.retained_snapshot_reads.load(Ordering::SeqCst), 0);
        deleter.delete(terminal.checkpoint).await.unwrap();
        DeletionCase::assert_absent(&raw, &scope).await;
        control.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(3), &mut reading).await.unwrap();
        assert!(matches!(result, Err(AppError::Closed(_) | AppError::NotFound(_))),
            "explicit deletion revokes this observation without corrupting App authority");
        assert_eq!(control.retained_snapshot_reads.load(Ordering::SeqCst), 1,
            "an admitted read snapshot retains its exact row while the fresh final fence observes deletion");
        drop(reading);
        drop(execution);
        assert!(matches!(source.snapshot().await, Err(AppError::NotFound(_))));
        // A revoked read must not take unrelated workflows or App admission down.
        let (next, completed) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        assert_ne!(next.id(), source.id());
        assert_eq!(completed.state.values()["count"], 3);
    }).await;
    }
}

#[tokio::test]
async fn missing_current_or_predecessor_with_retained_head_remains_corruption() {
    for offset in [0, 1] {
        let (case, workflow) = DeletionCase::new(3, DeletionFault::None).await;
        let raw = case.raw.clone();
        let terminal_timeout = case.terminal_timeout;
        case.close_after(false, async {
            let (source, terminal) =
                DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
            let scope = DeletionCase::scope(&raw, &source).await;
            let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
            let head = raw.load(head_key.clone()).await.unwrap().unwrap();
            let key = StateKey::new(
                "app-sdk",
                format!(
                    "history/{scope}/checkpoint/{:020}",
                    terminal.checkpoint.checked_sub(offset).unwrap()
                ),
            )
            .unwrap();
            let original = raw.load(key.clone()).await.unwrap().unwrap();
            let mut transaction = raw.begin_transaction().await.unwrap();
            transaction.delete(key.clone()).await.unwrap();
            transaction.commit().await.unwrap();
            let observed = AssertUnwindSafe(async {
                assert!(raw.load(key).await.unwrap().is_none());
                let current_head = raw.load(head_key.clone()).await.unwrap().unwrap();
                assert_eq!(current_head.record.value, head.record.value);
                assert_eq!(current_head.revision, head.revision);
                assert!(
                    matches!(source.snapshot().await, Err(AppError::Integration(_))),
                    "a missing row under its unchanged retained head is corruption"
                );
            })
            .catch_unwind()
            .await;
            // Restore the exact native row before either panic or joined cleanup.
            let mut transaction = raw.begin_transaction().await.unwrap();
            transaction.put(original.record).await.unwrap();
            transaction.commit().await.unwrap();
            if let Err(panic) = observed {
                std::panic::resume_unwind(panic);
            }
            let restored = source.snapshot().await.unwrap();
            assert_eq!(restored.checkpoint, terminal.checkpoint);
            assert_eq!(restored.state, terminal.state);
            assert_eq!(restored.last_action_ordinal, terminal.last_action_ordinal);
            assert!(restored.outcomes == terminal.outcomes);
            let current_head = raw.load(head_key).await.unwrap().unwrap();
            assert_eq!(current_head.record.value, head.record.value);
            assert_eq!(current_head.revision, head.revision);
        })
        .await;
    }
}

#[tokio::test]
async fn read_only_run_observation_preserves_finalization_failures() {
    for fault in [
        DeletionFault::ReadCommitUnavailable,
        DeletionFault::ReadLoadUnavailable,
    ] {
        let (case, workflow) = DeletionCase::new(3, fault).await;
        let raw = case.raw.clone();
        let control = Arc::clone(&case.control);
        let terminal_timeout = case.terminal_timeout;
        case.close_after(false, async {
            let (source, terminal) =
                DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
            let scope = DeletionCase::scope(&raw, &source).await;
            let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
            let before = DeletionCase::records(&raw, "").await;
            *control.read_key.lock().unwrap() = Some(head_key);
            control.armed.store(true, Ordering::SeqCst);
            let result = source.snapshot().await;
            assert!(
                matches!(&result, Err(AppError::Integration(_))),
                "non-conflict read finalization failures remain visible: {:?}",
                result.as_ref().err()
            );
            assert!(
                !control.armed.load(Ordering::SeqCst),
                "the selected real transaction was observed"
            );
            assert_eq!(
                control.read_commits.load(Ordering::SeqCst),
                usize::from(!matches!(fault, DeletionFault::ReadLoadUnavailable))
            );
            assert_eq!(
                control.read_rollbacks.load(Ordering::SeqCst),
                usize::from(matches!(fault, DeletionFault::ReadLoadUnavailable))
            );
            let after = DeletionCase::records(&raw, "").await;
            assert_eq!(
                after.len(),
                before.len(),
                "read finalization creates no receipt or checkpoint"
            );
            for (before, after) in before.iter().zip(&after) {
                assert_eq!(after.record.key, before.record.key);
                assert_eq!(after.record.value, before.record.value);
                assert_eq!(after.revision, before.revision);
            }
            let recovered = source.snapshot().await.unwrap();
            assert_eq!(recovered.checkpoint, terminal.checkpoint);
            assert_eq!(recovered.state, terminal.state);
        })
        .await;
    }
}

#[tokio::test]
async fn read_only_run_observation_retries_conflicts_with_a_bounded_budget() {
    for fault in [
        DeletionFault::ReadCommitConflict,
        DeletionFault::ReadCommitConflictPersistent,
    ] {
        let (case, workflow) = DeletionCase::new(3, fault).await;
        let raw = case.raw.clone();
        let control = Arc::clone(&case.control);
        let terminal_timeout = case.terminal_timeout;
        case.close_after(false, async {
            let (source, terminal) =
                DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
            let scope = DeletionCase::scope(&raw, &source).await;
            let before = DeletionCase::records(&raw, "").await;
            *control.read_key.lock().unwrap() =
                Some(StateKey::new("app-sdk", format!("run/{scope}")).unwrap());
            control.armed.store(true, Ordering::SeqCst);
            let result = tokio::time::timeout(Duration::from_secs(10), source.snapshot()).await;
            // Always end repeated injection before assertions or joined shutdown.
            control.armed.store(false, Ordering::SeqCst);
            let result =
                result.expect("conflicting read observations settle within a finite bound");
            assert!(
                control.read_attempts.load(Ordering::SeqCst) >= 2,
                "a rejected observation must be followed by a fresh transaction"
            );
            if matches!(fault, DeletionFault::ReadCommitConflictPersistent) {
                assert!(
                    matches!(&result, Err(AppError::Busy(_))),
                    "persistent read conflicts exhaust the bounded observation budget: {:?}",
                    result.as_ref().err()
                );
                assert!(control.read_commits.load(Ordering::SeqCst) >= 2);
            } else {
                let observed =
                    result.expect("one read conflict permits a fresh successful observation");
                assert_eq!(observed.checkpoint, terminal.checkpoint);
                assert_eq!(observed.state, terminal.state);
                assert_eq!(control.read_commits.load(Ordering::SeqCst), 1);
            }
            assert_eq!(control.read_rollbacks.load(Ordering::SeqCst), 0);
            let after = DeletionCase::records(&raw, "").await;
            assert_eq!(
                after.len(),
                before.len(),
                "read conflict recovery creates no receipt or checkpoint"
            );
            for (before, after) in before.iter().zip(&after) {
                assert_eq!(after.record.key, before.record.key);
                assert_eq!(after.record.value, before.record.value);
                assert_eq!(after.revision, before.revision);
            }
            let recovered = source.snapshot().await.unwrap();
            assert_eq!(recovered.checkpoint, terminal.checkpoint);
            assert_eq!(recovered.state, terminal.state);
        })
        .await;
    }
}

#[tokio::test]
async fn failed_read_rollback_vetoes_execution_recovery() {
    for fault in [
        DeletionFault::ReadLoadUnavailable,
        DeletionFault::ReadLoadAndRollbackUnavailable,
    ] {
        let (case, workflow) = DeletionCase::new(3, fault).await;
        let DeletionCase {
            mut app,
            raw,
            control,
            terminal_timeout,
        } = case;
        let rollback_fails = matches!(fault, DeletionFault::ReadLoadAndRollbackUnavailable);
        let observed = AssertUnwindSafe(tokio::time::timeout(
            terminal_timeout + Duration::from_secs(15),
            async {
            let (mut source, terminal) =
                DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
            let scope = DeletionCase::scope(&raw, &source).await;
            let history = DeletionCase::records(&raw, &format!("history/{scope}/")).await;
            let mut execution = source.execution(ExecutionOptions::default()).await.unwrap();
            *control.read_key.lock().unwrap() =
                Some(StateKey::new("app-sdk", format!("run/{scope}")).unwrap());
            control.armed.store(true, Ordering::SeqCst);
            let result = tokio::time::timeout(Duration::from_secs(3), execution.next())
                .await
                .expect("one recoverable read failure has bounded recovery or a cleanup veto");
            if rollback_fails {
                assert!(matches!(result,
                    Err(AppError::Integration(message)) if message == "app state-store operation failed"),
                    "failed cleanup preserves the original authority-read error");
                assert_eq!(control.read_attempts.load(Ordering::SeqCst), 1,
                    "failed rollback vetoes a fresh execution authority attempt");
            } else {
                assert!(matches!(result, Ok(None)),
                    "a settled recoverable read retries and observes original terminal EOF");
                assert!(control.read_attempts.load(Ordering::SeqCst) >= 2,
                    "the successful case proves this public execution owner retries recoverable reads");
            }
            assert_eq!(control.read_rollbacks.load(Ordering::SeqCst), 1);
            assert_eq!(control.read_commits.load(Ordering::SeqCst), 0);
            let after = DeletionCase::records(&raw, &format!("history/{scope}/")).await;
            assert_eq!(after.len(), history.len());
            for (before, after) in history.iter().zip(&after) {
                assert_eq!(after.record.key, before.record.key);
                assert_eq!(after.record.value, before.record.value);
                assert_eq!(after.revision, before.revision);
            }
            drop(execution);
            if !rollback_fails {
                assert_eq!(source.snapshot().await.unwrap().checkpoint, terminal.checkpoint);
            }
            },
        ))
        .catch_unwind()
        .await;
        control.armed.store(false, Ordering::SeqCst);
        control.release.notify_one();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        ))
        .catch_unwind()
        .await;
        if !matches!(
            &cleanup,
            Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
        ) {
            let mut observations = 1;
            loop {
                let settled = AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(6),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(4),
                    }),
                ))
                .catch_unwind()
                .await;
                eprintln!("read rollback cleanup observer={observations} result={settled:?}");
                // This exact owner uses valid CancelAndDrain exclusively. Only
                // these terminal outcomes prove owned joining; retain the runtime
                // for other results under the external process supervisor.
                if matches!(
                    &settled,
                    Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
                ) {
                    break;
                }
                observations += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        drop(app);
        match observed {
            Ok(result) => result.expect("bounded deletion observation"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let cleanup = cleanup
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            .expect("the original cleanup observer completes within its deadline");
        if rollback_fails {
            // Failed execution authority makes App fatal. This retained outcome is
            // delivered only after the cancellation-only owner joins its finalizer.
            assert!(matches!(cleanup, Err(AppError::Integration(_))));
        } else {
            cleanup.unwrap();
        }
        assert_eq!(control.abandoned_receipts.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn absent_run_observation_requires_successful_read_finalization() {
    let (case, workflow) = DeletionCase::new(3, DeletionFault::ReadCommitUnavailable).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(false, async {
        let (mut source, terminal) =
            DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        let observer = workflow
            .open_run(RunSelector::Latest(source.id().clone()))
            .await
            .unwrap();
        let scope = DeletionCase::scope(&raw, &source).await;
        source.delete(terminal.checkpoint).await.unwrap();
        DeletionCase::assert_absent(&raw, &scope).await;
        *control.read_key.lock().unwrap() =
            Some(StateKey::new("app-sdk", format!("run/{scope}")).unwrap());
        control.armed.store(true, Ordering::SeqCst);
        assert!(
            matches!(observer.snapshot().await, Err(AppError::Integration(_))),
            "failed read finalization cannot become authoritative absence"
        );
        assert_eq!(control.read_commits.load(Ordering::SeqCst), 1);
        assert_eq!(control.read_rollbacks.load(Ordering::SeqCst), 0);
        assert!(matches!(
            observer.snapshot().await,
            Err(AppError::NotFound(_))
        ));
        let (next, completed) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        assert_ne!(next.id(), source.id());
        assert_eq!(completed.state.values()["count"], 3);
    })
    .await;
}

#[tokio::test]
async fn failed_read_rollback_preserves_the_original_corruption_error() {
    let (case, workflow) = DeletionCase::new(3, DeletionFault::ReadRollbackUnavailable).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(false, async {
        let (source, terminal) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        let scope = DeletionCase::scope(&raw, &source).await;
        let key = StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", terminal.checkpoint)).unwrap();
        let original = raw.load(key.clone()).await.unwrap().unwrap().record;
        let mut corrupt = original.clone();
        Arc::make_mut(&mut corrupt.value)["epoch"] = json!("unsupported-read-epoch");
        let mut transaction = raw.begin_transaction().await.unwrap();
        transaction.put(corrupt).await.unwrap();
        transaction.commit().await.unwrap();
        *control.read_key.lock().unwrap() = Some(key);
        control.armed.store(true, Ordering::SeqCst);
        let observed = AssertUnwindSafe(async {
            assert!(matches!(source.snapshot().await,
                Err(AppError::Integration(message)) if message == "unsupported app record schema version"),
                "a secondary rollback receipt failure cannot erase the primary corruption");
            assert_eq!(control.read_rollbacks.load(Ordering::SeqCst), 1);
            assert_eq!(control.read_commits.load(Ordering::SeqCst), 0);
        }).catch_unwind().await;
        control.armed.store(false, Ordering::SeqCst);
        let mut transaction = raw.begin_transaction().await.unwrap();
        transaction.put(original).await.unwrap();
        transaction.commit().await.unwrap();
        if let Err(panic) = observed { std::panic::resume_unwind(panic); }
        let restored = source.snapshot().await.unwrap();
        assert_eq!(restored.checkpoint, terminal.checkpoint);
        assert_eq!(restored.state, terminal.state);
    }).await;
}

#[tokio::test]
async fn cancelled_read_observer_releases_its_snapshot_before_later_progress() {
    let (case, workflow) = DeletionCase::new(3, DeletionFault::CapturedRead).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(false, async {
        let (source, terminal) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        let scope = DeletionCase::scope(&raw, &source).await;
        *control.read_key.lock().unwrap() = Some(StateKey::new("app-sdk", format!("run/{scope}")).unwrap());
        control.armed.store(true, Ordering::SeqCst);
        let mut reading = Box::pin(source.snapshot());
        tokio::select! {
            _ = control.entered.notified() => {}
            result = &mut reading => panic!("read did not reach its retained snapshot: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("read snapshot was not observed"),
        }
        drop(reading);
        control.release.notify_one();
        let recovered = source.snapshot().await.unwrap();
        assert_eq!(recovered.checkpoint, terminal.checkpoint);
        assert_eq!(recovered.state, terminal.state);
        let (next, completed) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        assert_ne!(next.id(), source.id());
        assert_eq!(completed.state.values()["count"], 3);
    }).await;
}

#[derive(Clone, Copy)]
enum SourceOrderChange {
    State,
    RecordFields,
}

impl DeletionCase {
    async fn captured_source_order(change: SourceOrderChange) {
        let (case, workflow) = DeletionCase::new(3, DeletionFault::HeldRestoreAdmission).await;
        let raw = case.raw.clone();
        let control = Arc::clone(&case.control);
        let terminal_timeout = case.terminal_timeout;
        case.close_after(false, async {
            let (source, terminal) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
            let scope = DeletionCase::scope(&raw, &source).await;
            let row_key = StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", terminal.checkpoint)).unwrap();
            let original = raw.load(row_key.clone()).await.unwrap().unwrap().record;
            let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
            let head = raw.load(head_key.clone()).await.unwrap().unwrap();
            let initial_heads = DeletionCase::records(&raw, "run/").await.len();
            *control.read_key.lock().unwrap() = Some(row_key);
            control.armed.store(true, Ordering::SeqCst);
            let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6), async {
                let mut restoring = Box::pin(workflow.restore(RestoreOptions {
                    source: RunSelector::Latest(source.id().clone()), checkpoint: CheckpointSelector::Sequence(terminal.checkpoint),
                    overrides: WorkflowState::default(), position: ResumePosition::AfterCompleted, fallback: None,
                }));
                tokio::select! {
                    _ = control.entered.notified() => {}
                    result = &mut restoring => panic!("restore did not reach native admission: {}", result.is_ok()),
                    _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("captured restore was not admitted"),
                }
                assert_eq!(control.read_commits.load(Ordering::SeqCst), 1,
                    "captured source observation committed before the competing edit");
                let mut changed = original.clone();
                match change {
                    SourceOrderChange::State => {
                        let state = Arc::make_mut(&mut changed.value)["checkpoint"]["state"].as_object_mut().unwrap();
                        let left = state.shift_remove("left").unwrap();
                        let right = state.shift_remove("right").unwrap();
                        state.insert("right".into(), right);
                        state.insert("left".into(), left);
                        assert_ne!(serde_json::to_vec(&changed.value["checkpoint"]["state"]).unwrap(),
                            serde_json::to_vec(&original.value["checkpoint"]["state"]).unwrap(),
                            "user-state insertion order really changed");
                    }
                    SourceOrderChange::RecordFields => {
                        let fields = Arc::make_mut(&mut changed.value).as_object_mut().unwrap();
                        let epoch = fields.shift_remove("epoch").unwrap();
                        fields.insert("epoch".into(), epoch);
                        assert_eq!(serde_json::to_vec(&changed.value["checkpoint"]["state"]).unwrap(),
                            serde_json::to_vec(&original.value["checkpoint"]["state"]).unwrap(),
                            "changing DTO field order preserves exact user-state order");
                    }
                }
                assert_eq!(changed.value, original.value, "the fault changes no key or value");
                assert_ne!(serde_json::to_vec(changed.value.as_ref()).unwrap(),
                    serde_json::to_vec(original.value.as_ref()).unwrap(),
                    "the native row really has a different encoded order");
                let mut transaction = raw.begin_transaction().await.unwrap();
                transaction.put(changed).await.unwrap();
                transaction.commit().await.unwrap();
                let unchanged = raw.load(head_key.clone()).await.unwrap().unwrap();
                assert_eq!(unchanged.revision, head.revision);
                assert_eq!(unchanged.record.value, head.record.value);
                control.release.notify_one();
                let result = (&mut restoring).await;
                match change {
                    SourceOrderChange::State => {
                        assert!(matches!(result, Err(AppError::Conflict(_))),
                            "restore must reject changed ordered source evidence");
                        assert_eq!(DeletionCase::records(&raw, "run/").await.len(), initial_heads,
                            "rejected restore publishes no copied run");
                    }
                    SourceOrderChange::RecordFields => {
                        let copied = result.expect("typed DTO field ordering is not changed workflow evidence");
                        let snapshot = copied.snapshot().await.unwrap();
                        assert!(matches!(snapshot.status, RunStatus::Paused));
                        assert_eq!(snapshot.checkpoint, 0);
                        assert_eq!(snapshot.state, terminal.state);
                        assert_eq!(snapshot.last_action_ordinal, terminal.last_action_ordinal);
                        assert!(snapshot.outcomes.is_empty(), "restore cannot execute or replay source work");
                        assert_eq!(DeletionCase::records(&raw, "run/").await.len(), initial_heads + 1,
                            "the one captured restore admits exactly one new run");
                    }
                }
            })).catch_unwind().await;
            control.release.notify_one();
            let mut transaction = raw.begin_transaction().await.unwrap();
            transaction.put(original).await.unwrap();
            transaction.commit().await.unwrap();
            match observed {
                Ok(result) => result.expect("bounded source-order observation"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }).await;
    }
}

#[tokio::test]
async fn restoring_captured_state_rejects_an_order_only_source_change_before_admission() {
    DeletionCase::captured_source_order(SourceOrderChange::State).await;
}

#[tokio::test]
async fn restoring_captured_state_accepts_dto_field_reordering_before_native_admission() {
    DeletionCase::captured_source_order(SourceOrderChange::RecordFields).await;
}

impl DeletionCase {
    fn child_definitions() -> Vec<WorkflowDefinition> {
        // This successful-copy workload executes two real 130-step children.
        // Its authored budget allows Lance persistence before deletion is tested.
        vec![Self::definition(130), WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2", "id":"parent", "revision":"r1",
            "state":{"collected":[]}, "required":[], "reducers":{"collected":"collect"},
            "entry":"compose", "nodes":[
                {"id":"compose","deadline_ms":180000,"step":{"kind":"map","workflow":"deletion",
                    "items":{"kind":"literal","value":[10,20]},"input_key":"count","input":{},"max_parallel":2,
                    "history":"delete_after_reduction",
                    "reductions":[{"source":"count","target":"collected","reducer":"collect"}]}},
                {"id":"after","deadline_ms":20000,"step":{"kind":"input",
                    "message":[{"kind":"text","text":"copied child result is committed"}]}}
            ],
            "routes":{"compose":{"mode":"first","branches":[{"target":{"kind":"node","id":"after"}}]}}
        }).to_string()).unwrap()]
    }

    async fn child_policy_receipt(fault: DeletionFault) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let control = DeletionControl::new(fault);
        let app: App<DeletionStore, Agent> = App::open(AppConfig::new(
            DeletionStore {
                inner: raw.clone(),
                control: Arc::clone(&control),
            },
            Vec::new(),
            Self::child_definitions(),
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
        let workflow = workspace.workflow("parent".parse().unwrap()).await.unwrap();
        let case = Self {
            app,
            raw: raw.clone(),
            control: Arc::clone(&control),
            terminal_timeout: Duration::from_secs(180),
        };
        case.close_after(matches!(fault, DeletionFault::HeldSealReceipt), async {
            control.armed.store(true, Ordering::SeqCst);
            let started = Instant::now();
            let parent = workflow.start(ExecutionOptions::default()).await.unwrap();
            if matches!(fault, DeletionFault::HeldSealReceipt) {
                let mut observing = Box::pin(parent.wait());
                tokio::select! {
                    _ = control.entered.notified() => {}
                    result = &mut observing => panic!("original parent barrier receipt was not retained: {}", result.is_ok()),
                    _ = tokio::time::sleep(Duration::from_secs(180)) => panic!("native copied-result barrier did not seal its children"),
                }
                drop(observing);
                let copied = parent.snapshot().await.unwrap();
                assert_eq!(copied.state.values()["collected"], json!([130,130]));
                assert_eq!(copied.last_action_ordinal, Some(0));
                assert_eq!(copied.outcomes.len(), 1);
                assert_eq!(copied.outcomes[0].node, "compose");
                let heads = Self::records(&raw, &format!("run/{}/deletion/", workspace.id())).await;
                assert_eq!(heads.len(), 2, "both original child occurrences are sealed with the same copied barrier");
                let mut scopes = Vec::new();
                for head in heads {
                    assert_eq!(head.record.value.pointer("/retention/state"), Some(&json!("deleting")));
                    let scope = head.record.key.key().strip_prefix("run/").unwrap().to_owned();
                    assert!(Self::records(&raw, &format!("history/{scope}/")).await.len() > 256,
                        "restart must resume real bounded history pages, not an empty sealed head");
                    assert!(raw.load(StateKey::new("app-sdk", format!("active/{scope}")).unwrap()).await.unwrap().is_none());
                    scopes.push(scope);
                }
                assert_eq!(raw.load(StateKey::new("app-sdk", "app").unwrap()).await.unwrap().unwrap().record.value["run_count"], 1);
                assert_eq!(control.abandoned_receipts.load(Ordering::SeqCst), 0);
                let mut fresh: App<StateStore, Agent> = App::open(AppConfig::new(raw.clone(), Vec::new(), Self::child_definitions())).await.unwrap();
                let resumed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
                    let saved = fresh.workspace(workspace.id().clone()).await.unwrap()
                        .workflow("parent".parse().unwrap()).await.unwrap()
                        .open_run(RunSelector::Latest(parent.id().clone())).await.unwrap();
                    let settled = saved.wait().await.unwrap();
                    assert!(matches!(settled.interactions.as_slice(), [Interaction::Input { .. }]));
                    assert_eq!(settled.state, copied.state);
                    assert!(settled.outcomes == copied.outcomes);
                    assert_eq!(settled.last_action_ordinal, copied.last_action_ordinal);
                    for scope in &scopes {
                        loop {
                            if raw.load(StateKey::new("app-sdk", format!("run/{scope}")).unwrap()).await.unwrap().is_none() { break; }
                            tokio::task::yield_now().await;
                        }
                        Self::assert_absent(&raw, scope).await;
                    }
                    assert!(Self::records(&raw, &format!("run/{}/deletion/", workspace.id())).await.is_empty(),
                        "reopen cannot execute the copied child group again");
                    assert_eq!(control.abandoned_receipts.load(Ordering::SeqCst), 0);
                })).catch_unwind().await;
                // The real lower commit already completed. Releasing this held
                // receipt permits the original App to join its fenced owner.
                control.release.notify_one();
                let closed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6),
                    fresh.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(4) }),
                )).catch_unwind().await;
                drop(fresh);
                match resumed {
                    Ok(result) => result.expect("reopen resumes the original sealed child history"),
                    Err(panic) => std::panic::resume_unwind(panic),
                }
                match closed {
                    Ok(result) => result.expect("fresh cleanup bounded").expect("fresh cleanup joins"),
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            } else {
                let copied = parent.wait().await.unwrap();
                let failure = match &copied.status {
                    RunStatus::Failed(WorkflowFailure::Timeout(message)) => Some(("timeout", message.as_str())),
                    RunStatus::Failed(WorkflowFailure::Child(message)) => Some(("child", message.as_str())),
                    RunStatus::Failed(WorkflowFailure::Execution(message)) => Some(("execution", message.as_str())),
                    _ => None,
                };
                eprintln!(
                    "copied child observation: elapsed_ms={} checkpoint={} interactions={} outcomes={} failed={} failure={failure:?} collected={:?} last_ordinal={:?}",
                    started.elapsed().as_millis(),
                    copied.checkpoint,
                    copied.interactions.len(),
                    copied.outcomes.len(),
                    matches!(&copied.status, RunStatus::Failed(_)),
                    copied.state.values().get("collected"),
                    copied.last_action_ordinal,
                );
                assert!(matches!(copied.interactions.as_slice(), [Interaction::Input { .. }]));
                assert_eq!(copied.state.values()["collected"], json!([130,130]));
                assert_eq!(copied.last_action_ordinal, Some(0));
                assert_eq!(copied.outcomes.len(), 1);
                loop {
                    if Self::records(&raw, &format!("run/{}/deletion/", workspace.id())).await.is_empty() { break; }
                    tokio::task::yield_now().await;
                }
                assert!(Self::records(&raw, &format!("history/{}/deletion/", workspace.id())).await.is_empty());
                assert!(Self::records(&raw, &format!("active/{}/deletion/", workspace.id())).await.is_empty());
                assert_eq!(control.lost_commits.load(Ordering::SeqCst), 1);
                assert_eq!(control.lost_receipt_reads.load(Ordering::SeqCst), 1);
                {
                    let pages = control.committed_pages.lock().unwrap().clone();
                    assert!(pages.len() >= 4 && pages.iter().all(|page| !page.is_empty() && page.len() <= 256));
                    let rows = pages.iter().flatten().collect::<Vec<_>>();
                    assert_eq!(rows.len(), rows.iter().copied().collect::<BTreeSet<_>>().len(),
                        "an applied child page with a lost receipt is never executed twice");
                }
                let after = parent.snapshot().await.unwrap();
                assert_eq!(after.state, copied.state);
                assert!(after.outcomes == copied.outcomes);
                assert_eq!(after.last_action_ordinal, copied.last_action_ordinal);
                assert_eq!(control.abandoned_receipts.load(Ordering::SeqCst), 0);
            }
        }).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copied_child_history_survives_lost_page_and_receipt_observations_without_replay() {
    DeletionCase::child_policy_receipt(DeletionFault::LostPageReceipt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopen_resumes_children_sealed_by_an_original_parent_barrier_with_a_held_receipt() {
    DeletionCase::child_policy_receipt(DeletionFault::HeldSealReceipt).await;
}

#[tokio::test]
async fn deletion_retries_a_recoverable_capacity_read_before_sealing() {
    let (case, workflow) = DeletionCase::new(2, DeletionFault::UnavailableCapacityRead).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(false, async {
        let (mut run, terminal) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        let scope = DeletionCase::scope(&raw, &run).await;
        let other = workflow.prepare(WorkflowState::default()).await.unwrap();
        let before = other.snapshot().await.unwrap();
        control.armed.store(true, Ordering::SeqCst);
        let result =
            tokio::time::timeout(Duration::from_secs(3), run.delete(terminal.checkpoint)).await;
        assert!(
            !control.armed.load(Ordering::SeqCst),
            "the selected pre-seal capacity read must reach the fault"
        );
        result
            .expect("one unavailable read cannot strand deletion")
            .unwrap();
        DeletionCase::assert_absent(&raw, &scope).await;
        let after = other.snapshot().await.unwrap();
        assert_eq!(after.checkpoint, before.checkpoint);
        assert_eq!(after.state, before.state);
        assert!(after.outcomes == before.outcomes);
        assert_eq!(control.abandoned_receipts.load(Ordering::SeqCst), 0);
    })
    .await;
}

#[tokio::test]
async fn competing_terminal_retention_does_not_make_stale_capacity_fatal() {
    let (case, workflow) = DeletionCase::new(3, DeletionFault::CapturedCapacityRead).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(false, async {
        control.armed.store(true, Ordering::SeqCst);
        let mut run = workflow.start(ExecutionOptions::default()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), control.entered.notified())
            .await
            .expect("terminal retention captures its successful active-head recheck");
        let head_key = control
            .capacity_read
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .1
            .clone();
        let head = raw.load(head_key.clone()).await.unwrap().unwrap();
        assert_eq!(
            head.record.value.pointer("/retention/state"),
            Some(&json!("active"))
        );
        let scope = head_key.key().strip_prefix("run/").unwrap();
        let marker = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        assert!(raw.load(marker).await.unwrap().is_some());
        let app_key = StateKey::new("app-sdk", "app").unwrap();
        let before = raw.load(app_key.clone()).await.unwrap().unwrap();
        assert_eq!(before.record.value["run_count"], 1);
        let terminal = run.snapshot().await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(terminal.state.values()["count"], 3);
        tokio::time::timeout(Duration::from_secs(3), run.delete(terminal.checkpoint))
            .await
            .expect("a competing public delete performs real terminal retention")
            .unwrap();
        DeletionCase::assert_absent(&raw, scope).await;
        let retired = raw.load(app_key.clone()).await.unwrap().unwrap();
        assert_eq!(retired.record.value["run_count"], 0);
        assert!(retired.revision.get() > before.revision.get());
        control.release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), control.entered.notified())
            .await
            .expect("the original captured-read task observes native zero capacity");
        assert!(
            matches!(run.snapshot().await, Err(AppError::NotFound(_))),
            "another retainer finishing must not close the App"
        );
        let (next, completed) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        assert_ne!(next.id(), run.id());
        assert_eq!(completed.state.values()["count"], 3);
        assert_eq!(
            raw.load(app_key).await.unwrap().unwrap().record.value["run_count"],
            0
        );
    })
    .await;
}

#[tokio::test]
async fn stable_zero_capacity_with_an_active_run_remains_fatal() {
    let (mut case, workflow) = DeletionCase::new(3, DeletionFault::None).await;
    let raw = case.raw.clone();
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(35), async {
        let run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let initial = run.snapshot().await.unwrap();
        let scope = DeletionCase::scope(&raw, &run).await;
        let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let marker = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        assert!(raw.load(marker.clone()).await.unwrap().is_some());
        let app_key = StateKey::new("app-sdk", "app").unwrap();
        let mut corrupted = raw.load(app_key.clone()).await.unwrap().unwrap().record;
        assert_eq!(corrupted.value["run_count"], 1);
        Arc::make_mut(&mut corrupted.value)["run_count"] = json!(0);
        let mut transaction = raw.begin_transaction().await.unwrap();
        transaction.put(corrupted).await.unwrap();
        transaction.commit().await.unwrap();
        let cancellation = run.cancel(initial.checkpoint).await;
        assert!(
            matches!(cancellation, Ok(_) | Err(AppError::Closed(_))),
            "cancellation may observe the retained fatal cleanup"
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match run.snapshot().await {
                    Err(AppError::Closed(_)) => break,
                    Ok(snapshot) => {
                        assert_eq!(snapshot.state, initial.state);
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected corruption observation: {error}"),
                }
            }
        })
        .await
        .expect("the actual inconsistent capacity must fail closed");
        assert_eq!(
            raw.load(app_key).await.unwrap().unwrap().record.value["run_count"],
            0
        );
        assert!(raw.load(marker).await.unwrap().is_some());
        let head = raw.load(head_key).await.unwrap().unwrap();
        assert_eq!(
            head.record.value.pointer("/retention/state"),
            Some(&json!("active"))
        );
        let sequence = head.record.value["sequence"].as_u64().unwrap();
        let row = raw
            .load(
                StateKey::new(
                    "app-sdk",
                    format!("history/{scope}/checkpoint/{sequence:020}"),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.record.value.pointer("/checkpoint/phase/state"),
            Some(&json!("cancelled"))
        );
        assert_eq!(row.record.value["checkpoint"]["state"]["count"], 0);
    }))
    .catch_unwind()
    .await;
    let closed = tokio::time::timeout(
        Duration::from_secs(6),
        case.app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(case.app);
    match observed {
        Ok(result) => result.expect("bounded real capacity corruption"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    assert!(matches!(closed.expect("corrupt App cleanup joins"),
        Err(AppError::Integration(message)) if message == "workflow execution authority observation failed"));
}

// These public workloads also serve as subprocesses for native live-allocation
// profiling. Ordinary execution checks behavior; only the external profile can
// prove that otherwise unobservable runtime allocations have been reclaimed.
#[derive(Clone, Copy)]
enum RuntimeReleaseWorkload {
    Deleted,
    Rejected,
    Retained,
    Children,
}

struct RuntimeReleaseCase {
    app: App<StateStore, Agent>,
    store: StateStore,
    workspace: WorkspaceId,
    workflow: Workflow<StateStore, Agent>,
    stale_workflow: Workflow<StateStore, Agent>,
}

struct RuntimeReleaseDispatch {
    released: Arc<Notify>,
}

impl tracing::Subscriber for RuntimeReleaseDispatch {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        unreachable!("the release observer disables spans")
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

impl Drop for RuntimeReleaseDispatch {
    fn drop(&mut self) {
        self.released.notify_one();
    }
}

impl RuntimeReleaseDispatch {
    async fn observe<T>(operation: impl Future<Output = T>) -> T {
        let released = Arc::new(Notify::new());
        let dispatch = tracing::Dispatch::new(Self {
            released: Arc::clone(&released),
        });
        let output =
            tokio::time::timeout(Duration::from_secs(30), operation.with_subscriber(dispatch))
                .await
                .expect("the public ownership workload completes within its deadline");
        // The operation's future has dropped its Dispatch. Accepted tasks keep
        // theirs through supervision, so this signal also fences that handoff.
        tokio::time::timeout(Duration::from_secs(5), released.notified())
            .await
            .expect("all operation dispatch owners finish before heap observation");
        output
    }
}

impl RuntimeReleaseCase {
    async fn open(workload: RuntimeReleaseWorkload) -> Self {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut definitions = vec![DeletionCase::definition(1)];
        if matches!(workload, RuntimeReleaseWorkload::Children) {
            definitions.push(WorkflowDefinition::parse(&json!({
                "schema":"app-sdk.workflow/v2", "id":"parent", "revision":"r1",
                "state":{"collected":[]}, "required":[], "reducers":{"collected":"collect"},
                "entry":"compose", "nodes":[
                    {"id":"compose","deadline_ms":20000,"step":{"kind":"map","workflow":"deletion",
                        "items":{"kind":"literal","value":[10,20]},"input_key":"count","input":{},"max_parallel":2,
                        "history":"retain",
                        "reductions":[{"source":"count","target":"collected","reducer":"collect"}]}}
                ], "routes":{}
            }).to_string()).unwrap());
        }
        let app: App<StateStore, Agent> =
            App::open(AppConfig::new(store.clone(), Vec::new(), definitions))
                .await
                .unwrap();
        let root = std::env::current_dir().unwrap();
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: root.clone(),
            })
            .await
            .unwrap();
        let stale_workflow = workspace
            .workflow("deletion".parse().unwrap())
            .await
            .unwrap();
        let revision = workspace.snapshot().await.unwrap().revision;
        workspace
            .replace(
                revision,
                WorkspaceConfig {
                    name: Some("replacement".into()),
                    root,
                },
            )
            .await
            .unwrap();
        let workflow = workspace
            .workflow("deletion".parse().unwrap())
            .await
            .unwrap();
        Self {
            app,
            store,
            workspace: workspace.id().clone(),
            workflow,
            stale_workflow,
        }
    }

    async fn delete(&self, mut run: WorkflowRun<StateStore, Agent>, checkpoint: u64) {
        let selector = RunSelector::Exact {
            id: run.id().clone(),
            occurrence: run.snapshot().await.unwrap().occurrence,
        };
        run.delete(checkpoint).await.unwrap();
        assert!(matches!(
            self.workflow.open_run(selector).await,
            Err(AppError::NotFound(_))
        ));
        drop(run);
        assert!(
            self.workflow
                .list_runs(None, NonZeroUsize::new(1).unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    async fn cycle(&self, workload: RuntimeReleaseWorkload) {
        match workload {
            RuntimeReleaseWorkload::Deleted => {
                let run = self
                    .workflow
                    .start(ExecutionOptions::default())
                    .await
                    .unwrap();
                let terminal = run.wait().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                self.delete(run, terminal.checkpoint).await;
            }
            RuntimeReleaseWorkload::Rejected => {
                // A stale workspace revision rejects inside the admitted
                // operation, after its runtime ownership was acquired.
                assert!(matches!(
                    self.stale_workflow.start(ExecutionOptions::default()).await,
                    Err(AppError::Conflict(_))
                ));
                assert!(
                    self.workflow
                        .list_runs(None, NonZeroUsize::new(1).unwrap())
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            RuntimeReleaseWorkload::Retained => {
                let run = self
                    .workflow
                    .start(ExecutionOptions::default())
                    .await
                    .unwrap();
                let terminal = run.wait().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert_eq!(terminal.state.values()["count"], 1);
                drop(run);
            }
            RuntimeReleaseWorkload::Children => {
                let workflow = self
                    .app
                    .workspace(self.workspace.clone())
                    .await
                    .unwrap()
                    .workflow("parent".parse().unwrap())
                    .await
                    .unwrap();
                let run = workflow.start(ExecutionOptions::default()).await.unwrap();
                let terminal = run.wait().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert_eq!(terminal.state.values()["collected"], json!([1, 1]));
                drop(run);
            }
        }
    }

    async fn observe_retained(&self, expected: usize, concurrent: bool) {
        let retained = self
            .workflow
            .list_runs(None, NonZeroUsize::new(256).unwrap())
            .await
            .unwrap();
        assert_eq!(
            retained.len(),
            expected,
            "profiling must not delete retained histories"
        );
        for summary in retained {
            let selector = RunSelector::Exact {
                id: summary.id.clone(),
                occurrence: summary.occurrence.clone(),
            };
            let first = self.workflow.open_run(selector.clone()).await.unwrap();
            let terminal = first.wait().await.unwrap();
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert_eq!(terminal.state.values()["count"], 1);
            let history = first
                .history(None, NonZeroUsize::new(32).unwrap())
                .await
                .unwrap();
            assert!(!history.checkpoints.is_empty());
            assert!(history.next.is_none());
            assert_eq!(
                history.checkpoints.last().unwrap().sequence,
                terminal.checkpoint
            );
            if concurrent {
                let second = self.workflow.open_run(selector).await.unwrap();
                let release = Barrier::new(2);
                std::thread::scope(|scope| {
                    let left = scope.spawn(|| {
                        release.wait();
                        drop(first);
                    });
                    let right = scope.spawn(|| {
                        release.wait();
                        drop(second);
                    });
                    left.join().unwrap();
                    right.join().unwrap();
                });
            } else {
                drop(first);
            }
        }
    }

    async fn reopen(&mut self) {
        self.app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            })
            .await
            .unwrap();
        let app = App::open(AppConfig::new(
            self.store.clone(),
            Vec::new(),
            vec![DeletionCase::definition(1)],
        ))
        .await
        .unwrap();
        let workspace = app.workspace(self.workspace.clone()).await.unwrap();
        self.workflow = workspace
            .workflow("deletion".parse().unwrap())
            .await
            .unwrap();
        self.stale_workflow = workspace
            .workflow("deletion".parse().unwrap())
            .await
            .unwrap();
        self.app = app;
    }

    async fn close(mut self) {
        self.app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            })
            .await
            .unwrap();
    }
}

impl RuntimeReleaseWorkload {
    fn checkpoint(self, phase: &str) {
        match std::env::var("AI_KIT_APP_RUNTIME_PROFILE") {
            Err(std::env::VarError::NotPresent) => return,
            Ok(value) if value == "1" => {}
            _ => panic!("AI_KIT_APP_RUNTIME_PROFILE must be absent or equal to 1"),
        }
        let workload = match self {
            Self::Deleted => "deleted",
            Self::Rejected => "rejected",
            Self::Retained => "retained",
            Self::Children => "children",
        };
        // The profiling parent bounds the subprocess lifetime. Park its main
        // thread outside block_on, keeping the App and runtime alive throughout
        // each current-allocation capture. No private runtime names are asserted.
        println!(
            "APP_RUNTIME_PROFILE_READY {workload} {phase} {}",
            std::process::id()
        );
        std::io::stdout().flush().unwrap();
        let mut command = String::new();
        assert_ne!(std::io::stdin().read_line(&mut command).unwrap(), 0);
        assert_eq!(command.trim(), format!("continue {phase}"));
    }

    fn verify(self) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut case = runtime.block_on(RuntimeReleaseCase::open(self));
        let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            // The first driver creates the App's long-lived worker and its
            // dispatcher. Warm that owner before using a scoped dispatcher as
            // the completion fence for the measured cycle and all later work.
            runtime.block_on(case.cycle(Self::Deleted));
            runtime.block_on(RuntimeReleaseDispatch::observe(case.cycle(Self::Deleted)));
            self.checkpoint("baseline");
            let positive = runtime.block_on(RuntimeReleaseDispatch::observe(async {
                case.workflow
                    .prepare(WorkflowState::default())
                    .await
                    .unwrap()
            }));
            self.checkpoint("prepared");
            runtime.block_on(RuntimeReleaseDispatch::observe(async {
                let initial = positive.snapshot().await.unwrap();
                positive.cancel(initial.checkpoint).await.unwrap();
                let terminal = positive.wait().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Cancelled));
                case.delete(positive, terminal.checkpoint).await;
            }));
            self.checkpoint("released");
            let cycles = if matches!(self, Self::Children) {
                4
            } else {
                64
            };
            for _ in 0..cycles {
                runtime.block_on(RuntimeReleaseDispatch::observe(case.cycle(self)));
            }
            self.checkpoint(if matches!(self, Self::Children) {
                "after_children"
            } else {
                "after_64"
            });
            if matches!(self, Self::Children) {
                runtime.block_on(RuntimeReleaseDispatch::observe(async {
                    let parents = case
                        .app
                        .workspace(case.workspace.clone())
                        .await
                        .unwrap()
                        .workflow("parent".parse().unwrap())
                        .await
                        .unwrap()
                        .list_runs(None, NonZeroUsize::new(256).unwrap())
                        .await
                        .unwrap();
                    assert_eq!(parents.len(), 4);
                    assert!(
                        parents
                            .iter()
                            .all(|run| matches!(run.status, RunStatus::Succeeded)
                                && run.state.values()["collected"] == json!([1, 1]))
                    );
                    let children = case
                        .workflow
                        .list_runs(None, NonZeroUsize::new(256).unwrap())
                        .await
                        .unwrap();
                    assert_eq!(
                        children.len(),
                        8,
                        "both exact child histories survive each reduction"
                    );
                    assert!(
                        children
                            .iter()
                            .all(|run| matches!(run.status, RunStatus::Succeeded)
                                && run.state.values()["count"] == 1)
                    );
                    let expected: BTreeSet<_> = parents
                        .iter()
                        .flat_map(|parent| {
                            (0..2).map(move |index| {
                                (parent.id.to_string(), parent.occurrence.clone(), index)
                            })
                        })
                        .collect();
                    let mut actual = BTreeSet::new();
                    for child in &children {
                        let row = case
                            .store
                            .load(
                                StateKey::new(
                                    "app-sdk",
                                    format!(
                                        "history/{}/deletion/{}/{}/checkpoint/{:020}",
                                        case.workspace,
                                        child.id,
                                        child.occurrence,
                                        child.checkpoint,
                                    ),
                                )
                                .unwrap(),
                            )
                            .await
                            .unwrap()
                            .unwrap();
                        let parent = &row.record.value["parent"];
                        assert_eq!(parent["workflow_id"], "parent");
                        assert!(actual.insert((
                            parent["run_id"].as_str().unwrap().to_owned(),
                            parent["occurrence"].as_str().unwrap().to_owned(),
                            parent["index"].as_u64().unwrap()
                        )));
                    }
                    assert_eq!(
                        actual, expected,
                        "retained histories preserve each exact parent occurrence and ordinal"
                    );
                    assert!(
                        DeletionCase::records(&case.store, "active/")
                            .await
                            .is_empty(),
                        "the applied parent reductions release all original child capacity"
                    );
                }));
                self.checkpoint("after_child_history_observation");
            }
            if matches!(self, Self::Retained) {
                runtime.block_on(RuntimeReleaseDispatch::observe(
                    case.observe_retained(64, false),
                ));
                self.checkpoint("after_observing_64");
                for _ in 0..64 {
                    runtime.block_on(RuntimeReleaseDispatch::observe(case.cycle(self)));
                }
                self.checkpoint("after_128");
                runtime.block_on(RuntimeReleaseDispatch::observe(
                    case.observe_retained(128, true),
                ));
                self.checkpoint("after_concurrent_drops");
                // Opening owns its subscriber for the App's lifetime. The old
                // App shutdown already joins its owners; fence only the later
                // observer operations with an operation-scoped dispatcher.
                runtime.block_on(case.reopen());
                self.checkpoint("reopened");
                for _ in 0..2 {
                    runtime.block_on(RuntimeReleaseDispatch::observe(
                        case.observe_retained(128, true),
                    ));
                }
                self.checkpoint("after_reopened_observers");
            }
        }));
        let cleanup = runtime.block_on(async {
            AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6), case.close()))
                .catch_unwind()
                .await
        });
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
        cleanup
            .expect("App cleanup does not panic")
            .expect("App cleanup joins within its deadline");
        self.checkpoint("closed");
    }
}

#[test]
fn deleted_runs_release_runtime_ownership() {
    RuntimeReleaseWorkload::Deleted.verify();
}

#[test]
fn rejected_admissions_release_runtime_ownership() {
    RuntimeReleaseWorkload::Rejected.verify();
}

#[test]
fn retained_terminal_runs_release_runtime_ownership_without_deleting_history() {
    RuntimeReleaseWorkload::Retained.verify();
}

#[test]
fn retained_child_runs_release_runtime_ownership_after_parent_reduction() {
    RuntimeReleaseWorkload::Children.verify();
}

#[tokio::test]
async fn rejected_terminal_open_preserves_history_and_subsequent_observation() {
    let (case, workflow) = DeletionCase::new(1, DeletionFault::UnavailableCapacityRead).await;
    let raw = case.raw.clone();
    let control = Arc::clone(&case.control);
    let terminal_timeout = case.terminal_timeout;
    case.close_after(false, async {
        let (warm, _) = DeletionCase::terminal(&workflow, &raw, terminal_timeout).await;
        drop(warm);
        let (run, terminal) = RuntimeReleaseDispatch::observe(DeletionCase::terminal(
            &workflow,
            &raw,
            terminal_timeout,
        ))
        .await;
        let selector = RunSelector::Exact {
            id: terminal.id.clone(),
            occurrence: terminal.occurrence.clone(),
        };
        let scope = DeletionCase::scope(&raw, &run).await;
        let history = DeletionCase::records(&raw, &format!("history/{scope}/")).await;
        drop(run);
        control.armed.store(true, Ordering::SeqCst);
        assert!(
            matches!(
                workflow.open_run(selector.clone()).await,
                Err(AppError::Integration(_))
            ),
            "a failed terminal-capacity read remains an observation error"
        );
        assert!(
            !control.armed.load(Ordering::SeqCst),
            "the selected native read was reached"
        );
        let reopened = workflow.open_run(selector).await.unwrap();
        let observed = reopened.snapshot().await.unwrap();
        assert!(matches!(observed.status, RunStatus::Succeeded));
        assert_eq!(observed.checkpoint, terminal.checkpoint);
        assert_eq!(observed.state, terminal.state);
        assert_eq!(
            DeletionCase::records(&raw, &format!("history/{scope}/")).await,
            history
        );
    })
    .await;
}

#[tokio::test]
async fn lance_state_directory_deletion_preserves_bounded_pages_and_forks() {
    let directory = LanceStateDirectory::new();
    DeletionCase::page_case_with_state(DeletionFault::None, directory.config()).await;
}

#[tokio::test]
async fn lance_state_directory_lost_deletion_receipt_is_not_replayed() {
    let directory = LanceStateDirectory::new();
    DeletionCase::page_case_with_state(DeletionFault::LostPageReceipt, directory.config()).await;
}

#[tokio::test]
async fn cold_v6_sealed_deletion_finishes_before_the_new_generation_is_claimed() {
    DeletionCase::migrate_sealed_deletion_golden(false).await;
}

#[tokio::test]
async fn cold_v6_sealed_deletion_resumes_after_an_acknowledged_migration_page() {
    DeletionCase::migrate_sealed_deletion_golden(true).await;
}
