//! Real Lance publication conflicts for App's lower-operation failure cases.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};

#[derive(Clone, Copy)]
enum ConflictMode {
    Idle,
    Once,
    Armed,
}

struct ConflictControl {
    mode: Mutex<ConflictMode>,
    observed: AtomicU64,
}

#[derive(Clone)]
pub struct ConflictStore {
    inner: StateStore,
    peer: StateStore,
    control: Arc<ConflictControl>,
}

pub struct ConflictTransaction {
    inner: StateTransaction,
    peer: StateStore,
    control: Arc<ConflictControl>,
    authored: bool,
}

impl ConflictStore {
    pub fn new(inner: StateStore, peer: StateStore) -> Self {
        Self {
            inner,
            peer,
            control: Arc::new(ConflictControl {
                mode: Mutex::new(ConflictMode::Idle),
                observed: AtomicU64::new(0),
            }),
        }
    }

    pub fn once(&self) {
        *self.control.mode.lock().expect("conflict mode") = ConflictMode::Once;
    }

    pub fn arm(&self) {
        *self.control.mode.lock().expect("conflict mode") = ConflictMode::Armed;
    }

    pub fn disarm(&self) {
        *self.control.mode.lock().expect("conflict mode") = ConflictMode::Idle;
    }

    pub fn observed(&self) -> u64 {
        self.control.observed.load(Ordering::SeqCst)
    }
}

impl ConflictControl {
    fn claim(&self) -> bool {
        let mut mode = self.mode.lock().expect("conflict mode");
        match *mode {
            ConflictMode::Idle => false,
            ConflictMode::Once => {
                *mode = ConflictMode::Idle;
                true
            }
            ConflictMode::Armed => true,
        }
    }
}

impl StateStoreBackend for ConflictStore {
    type Transaction = ConflictTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(ConflictTransaction {
            inner: self.inner.begin_transaction().await?,
            peer: self.peer.clone(),
            control: Arc::clone(&self.control),
            authored: false,
        })
    }
}

impl StateTransactionBackend for ConflictTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.inner.put(record).await?;
        self.authored = true;
        Ok(())
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await?;
        self.authored = true;
        Ok(())
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let conflict = self.authored && self.control.claim();
        if conflict {
            // Publish through a separately opened connection only after the
            // original transaction has staged its domain writes. Lance's real
            // original-snapshot guard must reject that stale publication.
            let mut competing = self.peer.begin_transaction().await?;
            competing
                .put(StateRecord::new(
                    StateKey::new(
                        "app-live-conflict-receipts",
                        uuid::Uuid::new_v4().to_string(),
                    )?,
                    serde_json::json!({"published": true}),
                ))
                .await?;
            competing.commit().await?;
        }
        let result = self.inner.commit().await;
        if conflict {
            assert!(
                matches!(&result, Err(StateStoreError::TransactionConflict { .. })),
                "the actual stale Lance publication must conflict: {result:?}"
            );
            self.control.observed.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}
