//! Test-only failure coordination delegates every commit outcome to Lance.
mod lance_conflict;
mod lance_state_support;

use lance_conflict::ConflictStore;
use lance_state_support::LanceStateDirectory;
use storage_sdk::state::record::{StateKey, StateListRequest, StateRecord};
use storage_sdk::{StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend};

#[tokio::test]
async fn publication_control_preserves_real_conflicts_atomicity_and_later_progress() {
    let directory = LanceStateDirectory::new();
    let native = StateStore::connect(directory.config()).await.unwrap();
    let peer = StateStore::connect(directory.config()).await.unwrap();
    let store = ConflictStore::new(native.clone(), peer);
    let key = StateKey::new("application", "value").unwrap();
    let receipt = StateKey::new("application", "receipt").unwrap();

    store.once();
    let mut read_only = store.begin_transaction().await.unwrap();
    assert!(read_only.load(key.clone()).await.unwrap().is_none());
    read_only.commit().await.unwrap();
    assert_eq!(
        store.observed(),
        0,
        "read-only work cannot consume the conflict"
    );

    for attempt in 0..4 {
        if attempt == 1 {
            store.arm();
        }
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!(attempt)))
            .await
            .unwrap();
        transaction
            .put(StateRecord::new(
                receipt.clone(),
                serde_json::json!(attempt),
            ))
            .await
            .unwrap();
        assert!(matches!(
            transaction.commit().await,
            Err(StateStoreError::TransactionConflict { .. })
        ));
        let mut observed = native.begin_transaction().await.unwrap();
        assert!(observed.load(key.clone()).await.unwrap().is_none());
        assert!(observed.load(receipt.clone()).await.unwrap().is_none());
        observed.rollback().await.unwrap();
    }
    assert_eq!(store.observed(), 4);
    store.disarm();
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!("progress")))
        .await
        .unwrap();
    transaction
        .put(StateRecord::new(
            receipt.clone(),
            serde_json::json!("progress"),
        ))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(store.load(key).await.unwrap().unwrap().revision.get(), 1);
    assert_eq!(
        store.load(receipt).await.unwrap().unwrap().revision.get(),
        1
    );
    let receipts = store
        .list(
            StateListRequest::new("app-live-conflict-receipts")
                .unwrap()
                .with_limit(10)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        receipts.records.len(),
        4,
        "every forced conflict has an actual competing publication"
    );
    drop(store);
    drop(native);
}
