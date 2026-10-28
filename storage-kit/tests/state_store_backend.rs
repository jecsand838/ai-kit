use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use storage_kit::{
    StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::record::{
        StateKey, StateKeyPrefix, StateListRequest, StateListResponse, StateRecord,
        StateRecordSnapshot, StateRevision,
    },
};

// These caller-owned implementations deliberately do not implement Clone. Only
// the observation state is shared; no Kit handle or normalization is involved.
#[derive(Default)]
struct CallerStore {
    records: Arc<Mutex<BTreeMap<(String, String), CallerRecord>>>,
}

#[derive(Clone)]
struct CallerRecord {
    revision: StateRevision,
    present: Option<StateRecord>,
}

struct CallerTransaction {
    records: Arc<Mutex<BTreeMap<(String, String), CallerRecord>>>,
    observed: BTreeMap<(String, String), CallerRecord>,
    staged: BTreeMap<(String, String), CallerRecord>,
    reads: BTreeSet<(String, String)>,
}

impl CallerRecord {
    fn address(key: &StateKey) -> (String, String) {
        (key.namespace().as_str().to_owned(), key.key().to_owned())
    }

    fn snapshot(&self) -> Option<StateRecordSnapshot> {
        self.present.clone().map(|record| StateRecordSnapshot {
            record,
            revision: self.revision,
        })
    }
}

impl StateStoreBackend for CallerStore {
    type Transaction = CallerTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        Ok(self
            .records
            .lock()
            .expect("records")
            .get(&CallerRecord::address(&key))
            .and_then(CallerRecord::snapshot))
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let records = self.records.lock().expect("records");
        let mut matching = records
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
            .filter_map(|(_, record)| record.snapshot());
        let page = matching.by_ref().take(request.limit()).collect::<Vec<_>>();
        let next_start_after = matching
            .next()
            .and_then(|_| page.last().map(|record| record.record.key.key().to_owned()));
        Ok(StateListResponse {
            records: page,
            next_start_after,
        })
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(CallerTransaction {
            records: Arc::clone(&self.records),
            observed: self.records.lock().expect("records").clone(),
            staged: BTreeMap::new(),
            reads: BTreeSet::new(),
        })
    }
}

impl CallerTransaction {
    fn write(
        &mut self,
        key: StateKey,
        present: Option<StateRecord>,
    ) -> Result<(), StateStoreError> {
        let address = CallerRecord::address(&key);
        self.reads.insert(address.clone());
        let previous = self
            .staged
            .get(&address)
            .or_else(|| self.observed.get(&address));
        let revision = StateRevision::new(match previous {
            Some(record) => record
                .revision
                .get()
                .checked_add(1)
                .ok_or_else(|| StateStoreError::invalid_request("revision overflow"))?,
            None => 1,
        })?;
        self.staged
            .insert(address, CallerRecord { revision, present });
        Ok(())
    }
}

impl StateTransactionBackend for CallerTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let address = CallerRecord::address(&key);
        self.reads.insert(address.clone());
        Ok(self
            .staged
            .get(&address)
            .or_else(|| self.observed.get(&address))
            .and_then(CallerRecord::snapshot))
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.write(record.key.clone(), Some(record))
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.write(key, None)
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let mut current = self.records.lock().expect("records");
        for address in &self.reads {
            if current.get(address).map(|record| record.revision)
                != self.observed.get(address).map(|record| record.revision)
            {
                return Err(StateStoreError::transaction_conflict(
                    "commit",
                    "read revision changed",
                ));
            }
        }
        current.extend(self.staged);
        Ok(())
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn non_clone_store_returns_populated_associated_transaction_and_pages() {
    let store = CallerStore::default();
    let key = StateKey::new("agent", "workspace/1").expect("key");
    let mut transaction = store.begin_transaction().await.expect("transaction");
    assert!(
        transaction
            .load(key.clone())
            .await
            .expect("absent")
            .is_none()
    );
    transaction
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!({"turn": 1}),
        ))
        .await
        .expect("put");
    let staged = transaction
        .load(key.clone())
        .await
        .expect("load")
        .expect("staged");
    assert_eq!(
        staged.revision,
        StateRevision::new(1).expect("positive revision")
    );
    assert_eq!(
        staged.record.value.as_ref(),
        &serde_json::json!({"turn": 1})
    );
    assert!(
        store
            .load(key.clone())
            .await
            .expect("uncommitted")
            .is_none()
    );
    for fragment in ["workspace/3", "workspace/2", "other/1"] {
        transaction
            .put(StateRecord::new(
                StateKey::new("agent", fragment).expect("key"),
                serde_json::json!(fragment),
            ))
            .await
            .expect("put");
    }
    transaction.commit().await.expect("commit");
    assert_eq!(store.load(key).await.expect("load"), Some(staged));
    let request = StateListRequest::new("agent")
        .expect("namespace")
        .with_key_prefix(StateKeyPrefix::new("workspace/").expect("prefix"))
        .with_limit(2)
        .expect("limit");
    let first = store.list(request.clone()).await.expect("first page");
    assert_eq!(
        first
            .records
            .iter()
            .map(|record| record.record.key.key())
            .collect::<Vec<_>>(),
        ["workspace/1", "workspace/2"]
    );
    assert_eq!(first.next_start_after.as_deref(), Some("workspace/2"));
    let second = store
        .list(
            request
                .start_after(first.next_start_after.expect("cursor"))
                .expect("cursor"),
        )
        .await
        .expect("second page");
    assert_eq!(second.records[0].record.key.key(), "workspace/3");
    assert!(second.next_start_after.is_none());
}

#[tokio::test]
async fn caller_revision_sequence_retains_tombstones_and_rollback_discards_writes() {
    let store = CallerStore::default();
    let key = StateKey::new("agent", "revision").expect("key");
    let mut transaction = store.begin_transaction().await.expect("transaction");
    transaction
        .delete(key.clone())
        .await
        .expect("missing delete");
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .expect("put");
    assert_eq!(
        transaction
            .load(key.clone())
            .await
            .expect("load")
            .expect("value")
            .revision
            .get(),
        2
    );
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!(2)))
        .await
        .expect("put");
    transaction.delete(key.clone()).await.expect("delete");
    transaction.commit().await.expect("commit tombstone");
    assert!(store.load(key.clone()).await.expect("load").is_none());
    let mut recreate = store.begin_transaction().await.expect("transaction");
    recreate
        .put(StateRecord::new(key.clone(), serde_json::json!(3)))
        .await
        .expect("recreate");
    assert_eq!(
        recreate
            .load(key.clone())
            .await
            .expect("load")
            .expect("value")
            .revision
            .get(),
        5
    );
    recreate.commit().await.expect("commit");
    let committed = store.load(key.clone()).await.expect("load");
    let mut rollback = store.begin_transaction().await.expect("transaction");
    rollback.delete(key.clone()).await.expect("delete");
    rollback.rollback().await.expect("rollback");
    let mut dropped = store.begin_transaction().await.expect("transaction");
    dropped.delete(key.clone()).await.expect("delete");
    drop(dropped);
    assert_eq!(store.load(key).await.expect("load"), committed);
}

#[tokio::test]
async fn caller_conflicts_fence_present_and_absent_reads_without_partial_commit() {
    for initially_present in [false, true] {
        let store = CallerStore::default();
        let observed = StateKey::new("app", "head").expect("key");
        let receipt = StateKey::new("app", "receipt").expect("key");
        if initially_present {
            let mut seed = store.begin_transaction().await.expect("seed");
            seed.put(StateRecord::new(observed.clone(), serde_json::json!(0)))
                .await
                .expect("seed");
            seed.commit().await.expect("seed commit");
        }
        let mut stale = store.begin_transaction().await.expect("stale");
        assert_eq!(
            stale.load(observed.clone()).await.expect("read").is_some(),
            initially_present
        );
        stale
            .put(StateRecord::new(
                receipt.clone(),
                serde_json::json!("stale"),
            ))
            .await
            .expect("receipt");
        let mut winner = store.begin_transaction().await.expect("winner");
        winner
            .put(StateRecord::new(observed.clone(), serde_json::json!(1)))
            .await
            .expect("write");
        winner.commit().await.expect("winning commit");
        let error = stale.commit().await.expect_err("read fence must conflict");
        assert_eq!(
            error,
            StateStoreError::TransactionConflict {
                operation: "commit".to_owned(),
                message: "read revision changed".to_owned(),
            }
        );
        assert!(store.load(receipt).await.expect("receipt").is_none());
        assert_eq!(
            store
                .load(observed)
                .await
                .expect("head")
                .expect("head")
                .record
                .value
                .as_ref(),
            &serde_json::json!(1)
        );
    }
}
