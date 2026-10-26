//! Process-local committed state retained only by the test's observation owner.
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot, StateRevision,
};
use storage_sdk::{StateStoreBackend, StateStoreError, StateTransactionBackend};

#[derive(Default)]
pub struct Memory {
    records: Mutex<BTreeMap<(String, String), Entry>>,
    faults: Mutex<VecDeque<CommitFault>>,
    read_commits: Mutex<VecDeque<ReadCommit>>,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
#[derive(Clone)]
struct Entry {
    revision: StateRevision,
    record: Option<StateRecord>,
}
pub enum CommitFault {
    BeforePublication,
    AfterPublication,
    AdmittedAfterWaiterLoss(Arc<CommitGate>),
}
pub enum ReadCommit {
    Gated(Arc<CommitGate>),
    SerializationFailure,
}
#[derive(Default)]
pub struct CommitGate {
    admitted: tokio::sync::Notify,
    released: tokio::sync::Notify,
    conflicts: AtomicUsize,
}
impl CommitGate {
    pub async fn admitted(&self) {
        self.admitted.notified().await;
    }
    pub fn release(&self) {
        self.released.notify_one();
    }
    pub fn conflicts(&self) -> usize {
        self.conflicts.load(Ordering::SeqCst)
    }
}
pub struct Store {
    memory: Arc<Memory>,
}
pub struct Transaction {
    memory: Arc<Memory>,
    base: BTreeMap<(String, String), Entry>,
    writes: BTreeMap<(String, String), Entry>,
    reads: BTreeSet<(String, String)>,
    state: TransactionState,
    // The public transaction contract requires Send, but does not require Sync.
    _not_sync: PhantomData<Cell<()>>,
}
enum TransactionState {
    Active,
    Invalidated,
}
pub enum StoreChoice {
    First(Store),
    Second(Store),
}
pub enum TransactionChoice {
    First(Transaction),
    Second(Transaction),
}
impl Memory {
    pub fn store(self: &Arc<Self>) -> Store {
        Store {
            memory: Arc::clone(self),
        }
    }
    pub fn fail_next_commit(&self, fault: CommitFault) {
        self.faults.lock().unwrap().push_back(fault);
    }
    pub fn read_commit_as(&self, outcome: ReadCommit) {
        self.read_commits.lock().unwrap().push_back(outcome);
    }
    pub fn assert_faults_consumed(&self) {
        assert!(
            self.faults.lock().unwrap().is_empty(),
            "unconsumed commit fault"
        );
        assert!(
            self.read_commits.lock().unwrap().is_empty(),
            "unconsumed read-only commit expectation"
        );
    }
    pub async fn join_admitted(&self) {
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        for mut worker in workers {
            match tokio::time::timeout(std::time::Duration::from_secs(5), &mut worker).await {
                Ok(joined) => joined.expect("commit worker joins"),
                Err(_) => {
                    worker.abort();
                    let _ = worker.await;
                    panic!("commit worker exceeded bounded join");
                }
            }
        }
    }
    pub fn retain_at_revision(&self, record: StateRecord, revision: StateRevision) {
        self.records.lock().unwrap().insert(
            Entry::address(&record.key),
            Entry {
                revision,
                record: Some(record),
            },
        );
    }
    pub fn records(&self) -> Vec<StateRecordSnapshot> {
        self.records
            .lock()
            .unwrap()
            .values()
            .filter_map(Entry::snapshot)
            .collect()
    }
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
impl StateStoreBackend for Store {
    type Transaction = Transaction;
    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        Ok(self
            .memory
            .records
            .lock()
            .unwrap()
            .get(&Entry::address(&key))
            .and_then(Entry::snapshot))
    }
    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let records = self.memory.records.lock().unwrap();
        let mut matches = records
            .iter()
            .filter(|((ns, key), _)| {
                ns == request.namespace().as_str()
                    && request.prefix().is_none_or(|p| key.starts_with(p))
                    && request.start_after_key().is_none_or(|c| key.as_str() > c)
            })
            .filter_map(|(_, r)| r.snapshot());
        let page = matches.by_ref().take(request.limit()).collect::<Vec<_>>();
        let next_start_after = matches
            .next()
            .and_then(|_| page.last().map(|r| r.record.key.key().to_owned()));
        Ok(StateListResponse {
            records: page,
            next_start_after,
        })
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(Transaction {
            memory: Arc::clone(&self.memory),
            base: self.memory.records.lock().unwrap().clone(),
            writes: BTreeMap::new(),
            reads: BTreeSet::new(),
            state: TransactionState::Active,
            _not_sync: PhantomData,
        })
    }
}
impl Transaction {
    fn publish(self) -> Result<(), StateStoreError> {
        let mut records = self.memory.records.lock().unwrap();
        for address in &self.reads {
            if records.get(address).map(|e| e.revision)
                != self.base.get(address).map(|e| e.revision)
            {
                return Err(StateStoreError::transaction_conflict(
                    "commit",
                    "read revision changed",
                ));
            }
        }
        records.extend(self.writes);
        Ok(())
    }
    fn check(&self) -> Result<(), StateStoreError> {
        if matches!(self.state, TransactionState::Active) {
            Ok(())
        } else {
            Err(StateStoreError::invalid_request("invalidated transaction"))
        }
    }
    fn write(&mut self, key: StateKey, record: Option<StateRecord>) -> Result<(), StateStoreError> {
        self.check()?;
        let address = Entry::address(&key);
        self.reads.insert(address.clone());
        let previous = self
            .writes
            .get(&address)
            .or_else(|| self.base.get(&address));
        let revision = previous.map_or(Some(1), |r| r.revision.get().checked_add(1));
        let Some(revision) = revision else {
            self.state = TransactionState::Invalidated;
            return Err(StateStoreError::invalid_request("revision overflow"));
        };
        self.writes.insert(
            address,
            Entry {
                revision: StateRevision::new(revision)?,
                record,
            },
        );
        Ok(())
    }
}
impl StateTransactionBackend for Transaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.check()?;
        let address = Entry::address(&key);
        self.reads.insert(address.clone());
        Ok(self
            .writes
            .get(&address)
            .or_else(|| self.base.get(&address))
            .and_then(Entry::snapshot))
    }
    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.write(record.key.clone(), Some(record))
    }
    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.write(key, None)
    }
    async fn commit(self) -> Result<(), StateStoreError> {
        self.check()?;
        let read_commit = if self.writes.is_empty() && !self.reads.is_empty() {
            self.memory.read_commits.lock().unwrap().pop_front()
        } else {
            None
        };
        match read_commit {
            Some(ReadCommit::Gated(gate)) => {
                gate.admitted.notify_one();
                tokio::time::timeout(std::time::Duration::from_secs(5), gate.released.notified())
                    .await
                    .map_err(|_| StateStoreError::timed_out("read commit gate", 5000))?;
                let outcome = self.publish();
                if matches!(&outcome, Err(StateStoreError::TransactionConflict { .. })) {
                    gate.conflicts.fetch_add(1, Ordering::SeqCst);
                }
                return outcome;
            }
            Some(ReadCommit::SerializationFailure) => {
                return Err(StateStoreError::serialization(
                    "injected permanent read commit failure",
                ));
            }
            None => {}
        }
        let fault = self.memory.faults.lock().unwrap().pop_front();
        match fault {
            Some(CommitFault::BeforePublication) => Err(StateStoreError::backend_operation(
                "commit",
                "injected before publication",
            )),
            Some(CommitFault::AfterPublication) => {
                self.publish()?;
                Err(StateStoreError::backend_operation(
                    "commit",
                    "injected reply loss after publication",
                ))
            }
            Some(CommitFault::AdmittedAfterWaiterLoss(gate)) => {
                let memory = Arc::clone(&self.memory);
                let (sender, receiver) = tokio::sync::oneshot::channel();
                let worker = tokio::spawn(async move {
                    gate.admitted.notify_one();
                    let outcome = match tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        gate.released.notified(),
                    )
                    .await
                    {
                        Ok(()) => self.publish(),
                        Err(_) => Err(StateStoreError::timed_out("commit gate", 3000)),
                    };
                    let _ = sender.send(outcome);
                });
                memory.workers.lock().unwrap().push(worker);
                receiver
                    .await
                    .map_err(|_| StateStoreError::backend_operation("commit", "worker lost"))?
            }
            None => self.publish(),
        }
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        Ok(())
    }
}
impl StateStoreBackend for StoreChoice {
    type Transaction = TransactionChoice;
    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::First(s) => s.load(key).await,
            Self::Second(s) => s.load(key).await,
        }
    }
    async fn list(&self, r: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        match self {
            Self::First(s) => s.list(r).await,
            Self::Second(s) => s.list(r).await,
        }
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        match self {
            Self::First(s) => s.begin_transaction().await.map(TransactionChoice::First),
            Self::Second(s) => s.begin_transaction().await.map(TransactionChoice::Second),
        }
    }
}
impl StateTransactionBackend for TransactionChoice {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::First(s) => s.load(key).await,
            Self::Second(s) => s.load(key).await,
        }
    }
    async fn put(&mut self, r: StateRecord) -> Result<(), StateStoreError> {
        match self {
            Self::First(s) => s.put(r).await,
            Self::Second(s) => s.put(r).await,
        }
    }
    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        match self {
            Self::First(s) => s.delete(key).await,
            Self::Second(s) => s.delete(key).await,
        }
    }
    async fn commit(self) -> Result<(), StateStoreError> {
        match self {
            Self::First(s) => s.commit().await,
            Self::Second(s) => s.commit().await,
        }
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        match self {
            Self::First(s) => s.rollback().await,
            Self::Second(s) => s.rollback().await,
        }
    }
}
