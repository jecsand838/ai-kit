//! Process-local transactional state. Retaining Control is reconstruction, not durability.
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot, StateRevision,
};
use storage_sdk::{StateStoreBackend, StateStoreError, StateTransactionBackend};

#[derive(Clone)]
struct Entry {
    revision: StateRevision,
    record: Option<StateRecord>,
}

impl Entry {
    fn address(key: &StateKey) -> (String, String) {
        (key.namespace().as_str().to_owned(), key.key().to_owned())
    }

    fn snapshot(&self) -> Option<StateRecordSnapshot> {
        self.record.clone().map(|record| StateRecordSnapshot {
            record,
            revision: self.revision,
        })
    }
}

#[derive(Default)]
struct Ledger {
    records: BTreeMap<(String, String), Entry>,
    outcomes: VecDeque<CommitOutcome>,
    begins: VecDeque<Arc<StateGate>>,
    loads: VecDeque<LoadStep>,
    commits: usize,
    pending: Vec<Publication>,
}

pub(super) struct StateGate {
    entered: tokio::sync::Semaphore,
    released: tokio::sync::Semaphore,
}

impl StateGate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Semaphore::new(0),
            released: tokio::sync::Semaphore::new(0),
        })
    }

    pub(super) async fn entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(3), self.entered.acquire())
            .await
            .expect("bounded state operation admission")
            .expect("state gate open")
            .forget();
    }

    pub(super) fn release(&self) {
        self.released.add_permits(1);
    }

    async fn wait(&self) {
        self.entered.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(3), self.released.acquire())
            .await
            .expect("bounded state operation release")
            .expect("state gate open")
            .forget();
    }
}

pub(super) enum LoadStep {
    Current(Arc<StateGate>),
    Failure {
        gate: Arc<StateGate>,
        error: StateStoreError,
    },
}

pub(super) enum CommitOutcome {
    Conflict,
    BeforePublication,
    AfterPublication,
    Admitted(Arc<CommitGate>),
}

struct Publication {
    writes: WriteSet,
    gate: Arc<CommitGate>,
}

pub(super) struct CommitGate {
    entered: tokio::sync::Semaphore,
    ready: tokio::sync::Notify,
    result: Mutex<Option<Result<(), StateStoreError>>>,
}

impl CommitGate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Semaphore::new(0),
            ready: tokio::sync::Notify::new(),
            result: Mutex::new(None),
        })
    }

    pub(super) async fn entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(3), self.entered.acquire())
            .await
            .expect("bounded commit admission")
            .expect("gate open")
            .forget();
    }

    async fn wait(&self) -> Result<(), StateStoreError> {
        loop {
            let ready = self.ready.notified();
            if let Some(result) = self.result.lock().expect("commit result").clone() {
                return result;
            }
            ready.await;
        }
    }
}

impl Ledger {
    fn publish(&mut self, writes: WriteSet) -> Result<(), StateStoreError> {
        for address in writes.reads {
            if self.records.get(&address).map(|entry| entry.revision)
                != writes.observed.get(&address).map(|entry| entry.revision)
            {
                return Err(StateStoreError::transaction_conflict(
                    "commit",
                    "observed revision changed",
                ));
            }
        }
        self.records.extend(writes.staged);
        self.commits += 1;
        Ok(())
    }

    fn settle(&mut self) -> Vec<Arc<CommitGate>> {
        let mut ready = Vec::new();
        for publication in std::mem::take(&mut self.pending) {
            *publication.gate.result.lock().expect("commit result") =
                Some(self.publish(publication.writes));
            ready.push(publication.gate);
        }
        ready
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        for gate in self.settle() {
            gate.ready.notify_one();
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct Control {
    ledger: Arc<Mutex<Ledger>>,
}

impl Control {
    pub(super) fn store(&self) -> MemoryStore {
        MemoryStore {
            ledger: Arc::clone(&self.ledger),
        }
    }

    pub(super) fn commit_as(&self, outcome: CommitOutcome) {
        self.ledger
            .lock()
            .expect("ledger")
            .outcomes
            .push_back(outcome);
    }

    pub(super) fn gate_next_begin(&self, gate: Arc<StateGate>) {
        self.ledger.lock().expect("ledger").begins.push_back(gate);
    }

    pub(super) fn load_as(&self, step: LoadStep) {
        self.ledger.lock().expect("ledger").loads.push_back(step);
    }

    pub(super) fn assert_consumed(&self) {
        let ledger = self.ledger.lock().expect("ledger");
        assert!(
            ledger.outcomes.is_empty(),
            "required commit outcome was not consumed"
        );
        assert!(ledger.pending.is_empty(), "admitted commit was not settled");
        assert!(
            ledger.begins.is_empty(),
            "required transaction gate was not consumed"
        );
        assert!(
            ledger.loads.is_empty(),
            "required load step was not consumed"
        );
    }

    pub(super) fn settle_admitted(&self) {
        let ready = self.ledger.lock().expect("ledger").settle();
        // Wake only after releasing the ledger, since a caller waker may inspect it.
        for gate in ready {
            gate.ready.notify_one();
        }
    }

    pub(super) fn commits(&self) -> usize {
        self.ledger.lock().expect("ledger").commits
    }
}

pub(super) struct MemoryStore {
    ledger: Arc<Mutex<Ledger>>,
}

struct WriteSet {
    observed: BTreeMap<(String, String), Entry>,
    staged: BTreeMap<(String, String), Entry>,
    reads: BTreeSet<(String, String)>,
}

enum TransactionState {
    Active(WriteSet),
    Invalid,
}

pub(super) struct MemoryTransaction {
    ledger: Arc<Mutex<Ledger>>,
    state: TransactionState,
    // A transaction is Send, and intentionally need not be Sync.
    operations: Cell<usize>,
}

impl MemoryTransaction {
    fn admit(&mut self) -> Result<WriteSet, StateStoreError> {
        self.operations.set(self.operations.get() + 1);
        match std::mem::replace(&mut self.state, TransactionState::Invalid) {
            TransactionState::Active(writes) => Ok(writes),
            TransactionState::Invalid => {
                Err(StateStoreError::invalid_request("transaction invalidated"))
            }
        }
    }

    fn write(&mut self, key: StateKey, record: Option<StateRecord>) -> Result<(), StateStoreError> {
        let mut writes = self.admit()?;
        let address = Entry::address(&key);
        writes.reads.insert(address.clone());
        let prior = writes
            .staged
            .get(&address)
            .or_else(|| writes.observed.get(&address));
        let revision = StateRevision::new(
            prior
                .map_or(Some(1), |entry| entry.revision.get().checked_add(1))
                .ok_or_else(|| StateStoreError::invalid_request("revision overflow"))?,
        )?;
        writes.staged.insert(address, Entry { revision, record });
        self.state = TransactionState::Active(writes);
        Ok(())
    }
}

impl StateStoreBackend for MemoryStore {
    type Transaction = MemoryTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let step = self.ledger.lock().expect("ledger").loads.pop_front();
        match step {
            Some(LoadStep::Current(gate)) => gate.wait().await,
            Some(LoadStep::Failure { gate, error }) => {
                gate.wait().await;
                return Err(error);
            }
            None => {}
        }
        Ok(self
            .ledger
            .lock()
            .expect("ledger")
            .records
            .get(&Entry::address(&key))
            .and_then(Entry::snapshot))
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let ledger = self.ledger.lock().expect("ledger");
        let mut matches = ledger
            .records
            .iter()
            .filter(|((namespace, key), _)| {
                namespace == request.namespace().as_str()
                    && request
                        .prefix()
                        .is_none_or(|prefix| key.starts_with(prefix))
                    && request
                        .start_after_key()
                        .is_none_or(|cursor| key.as_str() > cursor)
            })
            .filter_map(|(_, entry)| entry.snapshot());
        let records = matches.by_ref().take(request.limit()).collect::<Vec<_>>();
        let next_start_after = matches.next().and_then(|_| {
            records
                .last()
                .map(|record| record.record.key.key().to_owned())
        });
        Ok(StateListResponse {
            records,
            next_start_after,
        })
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        let gate = self.ledger.lock().expect("ledger").begins.pop_front();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        let observed = self.ledger.lock().expect("ledger").records.clone();
        Ok(MemoryTransaction {
            ledger: Arc::clone(&self.ledger),
            state: TransactionState::Active(WriteSet {
                observed,
                staged: BTreeMap::new(),
                reads: BTreeSet::new(),
            }),
            operations: Cell::new(0),
        })
    }
}

impl StateTransactionBackend for MemoryTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let mut writes = self.admit()?;
        let address = Entry::address(&key);
        writes.reads.insert(address.clone());
        let snapshot = writes
            .staged
            .get(&address)
            .or_else(|| writes.observed.get(&address))
            .and_then(Entry::snapshot);
        self.state = TransactionState::Active(writes);
        Ok(snapshot)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.write(record.key.clone(), Some(record))
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.write(key, None)
    }

    async fn commit(mut self) -> Result<(), StateStoreError> {
        let writes = self.admit()?;
        let gate = {
            let mut ledger = self.ledger.lock().expect("ledger");
            match ledger.outcomes.pop_front() {
                Some(CommitOutcome::Conflict) => {
                    return Err(StateStoreError::transaction_conflict(
                        "commit",
                        "scripted conflict",
                    ));
                }
                Some(CommitOutcome::BeforePublication) => {
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "failure before publication",
                    ));
                }
                Some(CommitOutcome::AfterPublication) => {
                    ledger.publish(writes)?;
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "publication completed but reply lost",
                    ));
                }
                Some(CommitOutcome::Admitted(gate)) => {
                    ledger.pending.push(Publication {
                        writes,
                        gate: Arc::clone(&gate),
                    });
                    gate
                }
                None => return ledger.publish(writes),
            }
        };
        gate.entered.add_permits(1);
        gate.wait().await
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn memory_fixture_retains_tombstones_and_detects_absent_read_conflicts() {
    let control = Control::default();
    let store = control.store();
    let key = StateKey::new("fixture", "key").expect("key");
    let mut absent = store.begin_transaction().await.expect("begin");
    assert!(absent.load(key.clone()).await.expect("load").is_none());
    let mut write = store.begin_transaction().await.expect("begin");
    write
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .expect("put");
    write.delete(key.clone()).await.expect("delete");
    write.commit().await.expect("commit");
    assert!(matches!(
        absent.commit().await,
        Err(StateStoreError::TransactionConflict { .. })
    ));
    let mut recreate = store.begin_transaction().await.expect("begin");
    recreate
        .put(StateRecord::new(key.clone(), serde_json::json!(2)))
        .await
        .expect("put");
    let retained = recreate
        .load(key.clone())
        .await
        .expect("load")
        .expect("record");
    assert_eq!(retained.revision.get(), 3);
    assert!(store.load(key.clone()).await.expect("load").is_none());
    recreate.commit().await.expect("commit");
    assert_eq!(store.load(key).await.expect("load"), Some(retained));
}

#[tokio::test]
async fn transaction_gate_defers_only_one_snapshot_without_blocking_other_transactions() {
    let state = Control::default();
    let store = state.store();
    let key = StateKey::new("fixture", "gated-read").expect("key");
    let gate = StateGate::new();
    state.gate_next_begin(Arc::clone(&gate));
    let mut delayed = Box::pin(store.begin_transaction());
    tokio::select! {
        () = gate.entered() => {},
        result = &mut delayed => panic!("snapshot crossed its gate: {}", result.is_ok()),
    }
    let mut writer = store.begin_transaction().await.expect("independent begin");
    writer
        .put(StateRecord::new(key.clone(), serde_json::json!(42)))
        .await
        .expect("write");
    writer.commit().await.expect("independent publication");
    gate.release();
    let mut observed = tokio::time::timeout(std::time::Duration::from_secs(3), delayed)
        .await
        .expect("bounded snapshot")
        .expect("begin");
    assert_eq!(
        observed
            .load(key)
            .await
            .expect("read")
            .expect("published")
            .record
            .value
            .as_ref(),
        &serde_json::json!(42)
    );
    observed.rollback().await.expect("rollback");
    state.assert_consumed();
}

#[tokio::test]
async fn load_gate_is_one_shot_reads_current_state_and_preserves_failures() {
    let control = Control::default();
    let store = control.store();
    let key = StateKey::new("fixture", "gated-load").expect("key");
    let gate = StateGate::new();
    control.load_as(LoadStep::Current(Arc::clone(&gate)));
    let mut delayed = Box::pin(store.load(key.clone()));
    tokio::select! {
        () = gate.entered() => {},
        result = &mut delayed => panic!("load crossed its gate: {result:?}"),
    }
    assert!(
        store
            .load(key.clone())
            .await
            .expect("independent load")
            .is_none()
    );
    let mut writer = store.begin_transaction().await.expect("independent begin");
    writer
        .put(StateRecord::new(key.clone(), serde_json::json!(42)))
        .await
        .expect("write");
    writer.commit().await.expect("independent publication");
    gate.release();
    let observed = tokio::time::timeout(std::time::Duration::from_secs(3), delayed)
        .await
        .expect("bounded current read")
        .expect("read")
        .expect("published");
    assert_eq!(observed.record.value.as_ref(), &serde_json::json!(42));

    let gate = StateGate::new();
    control.load_as(LoadStep::Failure {
        gate: Arc::clone(&gate),
        error: StateStoreError::unavailable("load", "scripted read failure"),
    });
    let mut delayed = Box::pin(store.load(key.clone()));
    tokio::select! {
        () = gate.entered() => {},
        result = &mut delayed => panic!("failure crossed its gate: {result:?}"),
    }
    assert_eq!(
        store.load(key.clone()).await.expect("independent read"),
        Some(observed.clone())
    );
    gate.release();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(3), delayed)
            .await.expect("bounded error"),
        Err(StateStoreError::Unavailable { operation, message })
            if operation == "load" && message == "scripted read failure"
    ));
    assert_eq!(
        store.load(key).await.expect("failure is one shot"),
        Some(observed)
    );
    control.assert_consumed();
}

#[tokio::test]
async fn memory_fixture_drop_and_rollback_never_publish_or_spend_revisions() {
    let control = Control::default();
    let store = control.store();
    let key = StateKey::new("fixture", "key").expect("key");
    for rollback in [false, true] {
        let mut tx = store.begin_transaction().await.expect("begin");
        tx.put(StateRecord::new(key.clone(), serde_json::json!(1)))
            .await
            .expect("put");
        if rollback {
            tx.rollback().await.expect("rollback");
        } else {
            drop(tx);
        }
        assert!(store.load(key.clone()).await.expect("load").is_none());
    }
    assert_eq!(control.commits(), 0);
    let mut tx = store.begin_transaction().await.expect("begin");
    tx.put(StateRecord::new(key.clone(), serde_json::json!(2)))
        .await
        .expect("put");
    tx.commit().await.expect("commit");
    assert_eq!(
        store
            .load(key)
            .await
            .expect("load")
            .expect("record")
            .revision
            .get(),
        1
    );
}

pub(super) enum CallerStore {
    First(MemoryStore),
    Second(MemoryStore),
}
pub(super) enum CallerTransaction {
    First(MemoryTransaction),
    Second(MemoryTransaction),
}

impl StateStoreBackend for CallerStore {
    type Transaction = CallerTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::First(store) => store.load(key).await,
            Self::Second(store) => store.load(key).await,
        }
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        match self {
            Self::First(store) => store.list(request).await,
            Self::Second(store) => store.list(request).await,
        }
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        match self {
            Self::First(store) => store
                .begin_transaction()
                .await
                .map(CallerTransaction::First),
            Self::Second(store) => store
                .begin_transaction()
                .await
                .map(CallerTransaction::Second),
        }
    }
}

impl StateTransactionBackend for CallerTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::First(tx) => tx.load(key).await,
            Self::Second(tx) => tx.load(key).await,
        }
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        match self {
            Self::First(tx) => tx.put(record).await,
            Self::Second(tx) => tx.put(record).await,
        }
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        match self {
            Self::First(tx) => tx.delete(key).await,
            Self::Second(tx) => tx.delete(key).await,
        }
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        match self {
            Self::First(tx) => tx.commit().await,
            Self::Second(tx) => tx.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        match self {
            Self::First(tx) => tx.rollback().await,
            Self::Second(tx) => tx.rollback().await,
        }
    }
}

#[tokio::test]
async fn memory_fixture_revision_failure_invalidates_transaction_without_publication() {
    let control = Control::default();
    let key = StateKey::new("fixture", "overflow").expect("key");
    control.ledger.lock().expect("ledger").records.insert(
        Entry::address(&key),
        Entry {
            revision: StateRevision::new(u64::MAX).expect("revision"),
            record: Some(StateRecord::new(key.clone(), serde_json::json!("retained"))),
        },
    );
    let store = control.store();
    let mut transaction = store.begin_transaction().await.expect("begin");
    assert!(matches!(
        transaction
            .put(StateRecord::new(
                key.clone(),
                serde_json::json!("not published")
            ))
            .await,
        Err(StateStoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        transaction.load(key.clone()).await,
        Err(StateStoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        transaction.commit().await,
        Err(StateStoreError::InvalidRequest(_))
    ));
    let observed = store.load(key).await.expect("read").expect("record");
    assert_eq!(observed.revision.get(), u64::MAX);
    assert_eq!(
        observed.record.value.as_ref(),
        &serde_json::json!("retained")
    );
}

#[tokio::test]
async fn memory_fixture_pages_are_namespace_scoped_exclusive_and_retained_observations_are_immutable()
 {
    use storage_sdk::state::record::StateKeyPrefix;
    let control = Control::default();
    let store = control.store();
    let mut transaction = store.begin_transaction().await.expect("begin");
    for (namespace, key) in [
        ("a", "prefix/3"),
        ("b", "prefix/1"),
        ("a", "prefix/1"),
        ("a", "prefix/2"),
        ("a", "other"),
    ] {
        transaction
            .put(StateRecord::new(
                StateKey::new(namespace, key).expect("key"),
                serde_json::json!(key),
            ))
            .await
            .expect("put");
    }
    transaction.commit().await.expect("commit");
    let request = StateListRequest::new("a")
        .expect("namespace")
        .with_key_prefix(StateKeyPrefix::new("prefix/").expect("prefix"))
        .with_limit(2)
        .expect("limit");
    let first = store.list(request.clone()).await.expect("first page");
    assert_eq!(
        first
            .records
            .iter()
            .map(|entry| entry.record.key.key())
            .collect::<Vec<_>>(),
        ["prefix/1", "prefix/2"]
    );
    assert_eq!(first.next_start_after.as_deref(), Some("prefix/2"));
    let second = store
        .list(
            request
                .start_after(first.next_start_after.clone().expect("cursor"))
                .expect("cursor"),
        )
        .await
        .expect("second page");
    assert_eq!(
        second
            .records
            .iter()
            .map(|entry| entry.record.key.key())
            .collect::<Vec<_>>(),
        ["prefix/3"]
    );
    assert!(second.next_start_after.is_none());
    let mut transaction = store.begin_transaction().await.expect("begin");
    transaction
        .delete(first.records[0].record.key.clone())
        .await
        .expect("delete");
    transaction.commit().await.expect("commit");
    assert_eq!(
        first.records[0].record.value.as_ref(),
        &serde_json::json!("prefix/1")
    );
    assert!(
        store
            .load(first.records[0].record.key.clone())
            .await
            .expect("read")
            .is_none()
    );
}
