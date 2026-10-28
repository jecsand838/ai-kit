mod lancedb_state_failures;
mod lancedb_state_lifecycle;
mod lancedb_state_maintenance;
mod lancedb_state_maintenance_recovery;
mod lancedb_state_optimization;
mod lancedb_state_reference;
mod lancedb_state_transactions;
mod state_cases;
mod state_file_cases;

use std::time::Duration;

use storage_kit::{
    Database, DatabaseBackend, StateStore, StateStoreBackend, StateStoreError,
    StateTransactionBackend, StorageError,
    database::{
        StorageLocator,
        catalog::{
            CatalogRequest, CatalogResponse, CreateCollectionMode, CreateCollectionRequest,
            DropColumnsRequest, ListCollectionsRequest, SchemaEvolutionRequest,
        },
        config::{DatabaseConfig, LanceConfig},
        data::{
            CountRequest, DataRecord, DataRequest, DataResponse, DeleteRequest, FilterExpr,
            InsertRequest, ScanRequest,
        },
        index::{CreateIndexRequest, IndexKind, IndexRequest, WaitForIndexRequest},
        query::{Projection, QueryRequest, ScrollRequest},
    },
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateListRequest, StateRecord},
    },
};
use tempfile::TempDir;

struct LanceDirectory {
    directory: TempDir,
}

enum InitialRecords {
    Absent,
    Tombstones,
    Present,
}

impl InitialRecords {
    fn revision(&self) -> u64 {
        match self {
            Self::Absent => 0,
            Self::Tombstones | Self::Present => 1,
        }
    }

    fn is_present(&self) -> bool {
        match self {
            Self::Present => true,
            Self::Absent | Self::Tombstones => false,
        }
    }
}

impl LanceDirectory {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("Lance StateStore directory"),
        }
    }

    fn config(&self) -> StateStoreConfig {
        StateStoreConfig::Lance(
            LanceStateStoreConfig::directory(self.directory.path().join("database"))
                .with_operation_timeout(Duration::from_secs(10))
                .expect("bounded operation timeout"),
        )
    }

    async fn connect(&self) -> StateStore {
        tokio::time::timeout(Duration::from_secs(30), StateStore::connect(self.config()))
            .await
            .expect("bounded Lance StateStore connection")
            .expect("connect Lance StateStore")
    }

    async fn database(&self) -> Database {
        tokio::time::timeout(
            Duration::from_secs(30),
            Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
                self.directory.path().join("database"),
            ))),
        )
        .await
        .expect("bounded shared-directory Database connection")
        .expect("connect shared-directory Database")
    }

    fn committed(result: Result<(), StateStoreError>) -> bool {
        match result {
            Ok(()) => true,
            Err(StateStoreError::TransactionConflict { .. }) => false,
            Err(error) => panic!("unexpected finalization error: {error:?}"),
        }
    }

    async fn write_skew(&self, initial: InitialRecords) {
        let first = self.connect().await;
        let second = self.connect().await;
        // A state transaction spans namespaces. Partitioning their physical
        // storage must not weaken the serial ordering of this decision.
        let keys = [
            StateKey::new("left-namespace", "decision").expect("left key"),
            StateKey::new("right-namespace", "decision").expect("right key"),
        ];
        let receipts = [
            StateKey::new("receipts", "left").expect("left receipt"),
            StateKey::new("receipts", "right").expect("right receipt"),
        ];
        let mut seed = first.begin_transaction().await.expect("seed");
        for key in &keys {
            match initial {
                InitialRecords::Absent => {}
                InitialRecords::Tombstones => {
                    seed.delete(key.clone()).await.expect("seed tombstone");
                }
                InitialRecords::Present => {
                    seed.put(StateRecord::new(key.clone(), serde_json::json!(false)))
                        .await
                        .expect("seed present decision");
                }
            }
        }
        seed.commit().await.expect("seed commit");

        let mut left = first.begin_transaction().await.expect("left transaction");
        let mut right = second.begin_transaction().await.expect("right transaction");
        for key in &keys {
            for transaction in [&mut left, &mut right] {
                let observed = transaction.load(key.clone()).await.expect("decision read");
                assert_eq!(observed.is_some(), initial.is_present());
                if let Some(observed) = observed {
                    assert_eq!(observed.revision.get(), initial.revision());
                    assert_eq!(observed.record.value.as_ref(), &serde_json::json!(false));
                }
            }
        }
        left.put(StateRecord::new(keys[0].clone(), serde_json::json!(true)))
            .await
            .expect("left decision");
        left.put(StateRecord::new(
            receipts[0].clone(),
            serde_json::json!(true),
        ))
        .await
        .expect("left receipt");
        right
            .put(StateRecord::new(keys[1].clone(), serde_json::json!(true)))
            .await
            .expect("right decision");
        right
            .put(StateRecord::new(
                receipts[1].clone(),
                serde_json::json!(true),
            ))
            .await
            .expect("right receipt");

        let (left, right) = tokio::join!(left.commit(), right.commit());
        let committed = [Self::committed(left), Self::committed(right)];
        assert!(
            !(committed[0] && committed[1]),
            "both decisions cannot commit after both transactions observed the old decisions"
        );
        let verifier = self.connect().await;
        for index in 0..2 {
            let decision = verifier.load(keys[index].clone()).await.expect("decision");
            assert_eq!(
                verifier
                    .load(receipts[index].clone())
                    .await
                    .expect("receipt")
                    .is_some(),
                committed[index],
                "a conflict proves that the whole transaction did not publish"
            );
            if committed[index] {
                let decision = decision.expect("committed decision");
                assert_eq!(decision.record.value.as_ref(), &serde_json::json!(true));
                assert_eq!(decision.revision.get(), initial.revision() + 1);
            } else if initial.is_present() {
                let decision = decision.expect("original decision");
                assert_eq!(decision.record.value.as_ref(), &serde_json::json!(false));
                assert_eq!(decision.revision.get(), initial.revision());
            } else {
                assert!(decision.is_none(), "losing decision remains absent");
            }
        }
        // No particular contender must win. A fresh transaction must still
        // progress, and failed attempts must not consume committed revisions.
        let mut next = verifier
            .begin_transaction()
            .await
            .expect("next transaction");
        for key in &keys {
            next.put(StateRecord::new(key.clone(), serde_json::json!("settled")))
                .await
                .expect("next decision");
        }
        next.commit().await.expect("next commit");
        for index in 0..2 {
            assert_eq!(
                verifier
                    .load(keys[index].clone())
                    .await
                    .expect("settled decision")
                    .expect("present decision")
                    .revision
                    .get(),
                initial.revision() + u64::from(committed[index]) + 1
            );
        }
    }
}

#[test]
fn lancedb_state_configuration_rejects_zero_operation_timeout() {
    assert!(matches!(
        LanceStateStoreConfig::memory().with_operation_timeout(Duration::ZERO),
        Err(StateStoreError::InvalidConfig(_))
    ));
}

#[tokio::test]
async fn lancedb_state_memory_preserves_the_portable_contract() {
    let config = LanceStateStoreConfig::memory()
        .with_operation_timeout(Duration::from_secs(10))
        .expect("operation timeout");
    let store = StateStore::connect(StateStoreConfig::Lance(config))
        .await
        .expect("memory Lance StateStore");
    state_cases::StateCases::new(store).verify().await;
}

#[tokio::test]
async fn lancedb_state_directory_preserves_the_portable_contract() {
    let directory = LanceDirectory::new();
    state_cases::StateCases::verify_file(directory.config()).await;
}

#[tokio::test]
async fn lancedb_state_delta_commits_pass_the_shared_reference_concurrency_cases() {
    let directory = LanceDirectory::new();
    lancedb_state_reference::SharedStateCases::new(
        directory.connect().await,
        directory.connect().await,
        directory.connect().await,
    )
    .verify()
    .await;
}

#[tokio::test]
async fn lancedb_state_reopen_retains_commits_and_tombstone_revisions() {
    let directory = LanceDirectory::new();
    let key = StateKey::new("reopen", "record").expect("key");
    let abandoned = StateKey::new("reopen", "abandoned").expect("abandoned key");
    let store = directory.connect().await;
    let mut transaction = store.begin_transaction().await.expect("seed");
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!("original")))
        .await
        .expect("seed put");
    transaction.delete(key.clone()).await.expect("tombstone");
    transaction.commit().await.expect("commit tombstone");
    let mut dropped = store
        .begin_transaction()
        .await
        .expect("dropped transaction");
    dropped
        .put(StateRecord::new(abandoned.clone(), serde_json::json!(true)))
        .await
        .expect("uncommitted put");
    drop(dropped);
    drop(store);

    let reopened = directory.connect().await;
    assert!(
        reopened
            .load(key.clone())
            .await
            .expect("tombstone")
            .is_none()
    );
    assert!(reopened.load(abandoned).await.expect("abandoned").is_none());
    let mut recreate = reopened.begin_transaction().await.expect("recreate");
    recreate
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!("recreated"),
        ))
        .await
        .expect("recreate put");
    recreate.commit().await.expect("recreate commit");
    drop(reopened);

    let verifier = directory.connect().await;
    let record = verifier.load(key).await.expect("load").expect("record");
    assert_eq!(record.revision.get(), 3);
    assert_eq!(
        record.record.value.as_ref(),
        &serde_json::json!("recreated")
    );
}

#[tokio::test]
async fn lancedb_state_independent_reads_are_fresh_and_transactions_are_consistent() {
    let directory = LanceDirectory::new();
    let writer = directory.connect().await;
    let reader = directory.connect().await;
    let keys = [
        StateKey::new("snapshot", "left").expect("left"),
        StateKey::new("snapshot", "right").expect("right"),
    ];
    let mut seed = writer.begin_transaction().await.expect("seed");
    for key in &keys {
        seed.put(StateRecord::new(key.clone(), serde_json::json!(0)))
            .await
            .expect("seed record");
    }
    seed.commit().await.expect("seed commit");
    let mut observing = reader
        .begin_transaction()
        .await
        .expect("observing transaction");
    assert_eq!(
        observing
            .load(keys[0].clone())
            .await
            .expect("first snapshot read")
            .expect("left")
            .record
            .value
            .as_ref(),
        &serde_json::json!(0)
    );
    let mut update = writer.begin_transaction().await.expect("update");
    for key in &keys {
        update
            .put(StateRecord::new(key.clone(), serde_json::json!(1)))
            .await
            .expect("replace both records");
    }
    update.commit().await.expect("atomic update");
    match observing.load(keys[1].clone()).await {
        Ok(Some(record)) => {
            assert_eq!(record.record.value.as_ref(), &serde_json::json!(0));
            // A read-only transaction may serialize before the writer. A
            // conservative conflict is also permitted; success is not forced.
            LanceDirectory::committed(observing.commit().await);
        }
        Err(StateStoreError::TransactionConflict { .. }) => {
            observing.rollback().await.expect("join conflict cleanup");
        }
        other => panic!("transaction observed an incompatible view: {other:?}"),
    }
    for key in &keys {
        let record = reader
            .load(key.clone())
            .await
            .expect("fresh independent read")
            .expect("updated record");
        assert_eq!(record.record.value.as_ref(), &serde_json::json!(1));
        assert_eq!(record.revision.get(), 2);
    }
    let page = reader
        .list(StateListRequest::new("snapshot").expect("list request"))
        .await
        .expect("fresh independent page");
    assert_eq!(page.records.len(), 2);
    assert!(page.next_start_after.is_none());
    for record in page.records {
        assert_eq!(record.record.value.as_ref(), &serde_json::json!(1));
        assert_eq!(record.revision.get(), 2);
    }
}

#[tokio::test]
async fn lancedb_state_same_owner_snapshots_preserve_mixed_revisions_and_conflict_nonpublication() {
    let directory = LanceDirectory::new();
    let store = directory.connect().await;
    let peer = store.clone();
    let keys = [
        StateKey::new("image", "updated").expect("updated key"),
        StateKey::new("image", "replaced").expect("replaced key"),
        StateKey::new("image", "deleted").expect("deleted key"),
        StateKey::new("image", "created").expect("created key"),
    ];
    let winner = StateKey::new("receipts", "winner").expect("winner receipt");
    let loser = StateKey::new("receipts", "loser").expect("loser receipt");
    let mut seed = store.begin_transaction().await.expect("seed snapshot");
    for key in &keys[..3] {
        seed.put(StateRecord::new(key.clone(), serde_json::json!(1)))
            .await
            .expect("seed record");
    }
    seed.commit().await.expect("seed publication");
    let mut original = peer
        .begin_transaction()
        .await
        .expect("retained old snapshot");
    let mut stale = peer.begin_transaction().await.expect("future stale writer");
    let mut update = store.begin_transaction().await.expect("mixed update");
    update
        .put(StateRecord::new(keys[0].clone(), serde_json::json!(2)))
        .await
        .expect("update record");
    update
        .delete(keys[1].clone())
        .await
        .expect("delete before replacement");
    update
        .put(StateRecord::new(keys[1].clone(), serde_json::json!(2)))
        .await
        .expect("recreate within one transaction");
    update
        .delete(keys[2].clone())
        .await
        .expect("retain tombstone");
    for value in [1, 2] {
        update
            .put(StateRecord::new(keys[3].clone(), serde_json::json!(value)))
            .await
            .expect("count every authored put");
    }
    update
        .put(StateRecord::new(
            winner.clone(),
            serde_json::json!("winner"),
        ))
        .await
        .expect("atomic winner receipt");
    update.commit().await.expect("mixed publication");

    for key in &keys[..3] {
        let before = original
            .load(key.clone())
            .await
            .expect("retained snapshot read")
            .expect("original record");
        assert_eq!(before.revision.get(), 1);
        assert_eq!(before.record.value.as_u64(), Some(1));
    }
    assert!(
        original
            .load(keys[3].clone())
            .await
            .expect("old absence")
            .is_none()
    );
    assert!(
        original
            .load(winner.clone())
            .await
            .expect("old receipt absence")
            .is_none()
    );
    let before = stale
        .load(keys[0].clone())
        .await
        .expect("stale read")
        .expect("original record");
    assert_eq!(before.revision.get(), 1);
    assert_eq!(before.record.value.as_u64(), Some(1));
    stale
        .put(StateRecord::new(keys[0].clone(), serde_json::json!(999)))
        .await
        .expect("stage losing mutation");
    stale
        .put(StateRecord::new(loser.clone(), serde_json::json!("loser")))
        .await
        .expect("stage losing receipt");
    assert!(matches!(
        stale.commit().await,
        Err(StateStoreError::TransactionConflict { .. })
    ));
    original
        .commit()
        .await
        .expect("old read-only snapshot can complete");

    let mut current = store
        .begin_transaction()
        .await
        .expect("atomic current view");
    for (index, revision) in [(0, 2), (1, 3), (3, 2)] {
        let row = current
            .load(keys[index].clone())
            .await
            .expect("current row")
            .expect("present row");
        assert_eq!(row.revision.get(), revision);
        assert_eq!(row.record.value.as_u64(), Some(2));
    }
    assert!(
        current
            .load(keys[2].clone())
            .await
            .expect("current tombstone")
            .is_none()
    );
    assert!(
        current
            .load(loser.clone())
            .await
            .expect("losing receipt absent")
            .is_none()
    );
    let receipt = current
        .load(winner.clone())
        .await
        .expect("winner receipt")
        .expect("published receipt");
    assert_eq!(receipt.revision.get(), 1);
    assert_eq!(receipt.record.value.as_str(), Some("winner"));
    current.rollback().await.expect("verification settled");

    // No peer snapshot remains: subsequent advancement must still preserve
    // authored increments and the deleted record's durable revision history.
    let mut progress = store
        .begin_transaction()
        .await
        .expect("subsequent progress");
    progress
        .delete(keys[0].clone())
        .await
        .expect("delete updated record");
    for index in [0, 2, 3] {
        progress
            .put(StateRecord::new(keys[index].clone(), serde_json::json!(3)))
            .await
            .expect("subsequent recreation or update");
    }
    progress.commit().await.expect("progress publication");
    drop(peer);
    drop(store);
    let reopened = directory.connect().await;
    let mut verified = reopened
        .begin_transaction()
        .await
        .expect("reopened atomic view");
    for (index, revision, value) in [(0, 4, 3), (1, 3, 2), (2, 3, 3), (3, 3, 3)] {
        let row = verified
            .load(keys[index].clone())
            .await
            .expect("reopened record")
            .expect("all records present");
        assert_eq!(row.revision.get(), revision);
        assert_eq!(row.record.value.as_u64(), Some(value));
    }
    assert!(
        verified
            .load(loser)
            .await
            .expect("loser remains absent")
            .is_none()
    );
    assert_eq!(
        verified
            .load(winner)
            .await
            .expect("winner remains present")
            .expect("receipt")
            .revision
            .get(),
        1
    );
    verified
        .rollback()
        .await
        .expect("reopen verification settled");
}

#[tokio::test]
async fn lancedb_state_concurrent_read_modify_write_cannot_lose_an_update() {
    let directory = LanceDirectory::new();
    let first = directory.connect().await;
    let second = directory.connect().await;
    let key = StateKey::new("counter", "value").expect("key");
    let mut seed = first.begin_transaction().await.expect("seed");
    seed.put(StateRecord::new(key.clone(), serde_json::json!(0)))
        .await
        .expect("counter seed");
    seed.commit().await.expect("seed commit");
    let mut left = first.begin_transaction().await.expect("left");
    let mut right = second.begin_transaction().await.expect("right");
    for transaction in [&mut left, &mut right] {
        let observed = transaction
            .load(key.clone())
            .await
            .expect("read counter")
            .expect("counter");
        assert_eq!(observed.revision.get(), 1);
        let value = observed.record.value.as_u64().expect("integer counter");
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!(value + 1)))
            .await
            .expect("stage increment");
    }
    let (left, right) = tokio::join!(left.commit(), right.commit());
    let committed =
        u64::from(LanceDirectory::committed(left)) + u64::from(LanceDirectory::committed(right));
    assert!(
        committed <= 1,
        "both increments cannot publish the same value"
    );
    let record = second
        .load(key.clone())
        .await
        .expect("counter")
        .expect("present");
    assert_eq!(record.record.value.as_u64(), Some(committed));
    assert_eq!(record.revision.get(), committed + 1);
    let mut retry = second.begin_transaction().await.expect("fresh increment");
    let before = retry
        .load(key.clone())
        .await
        .expect("read")
        .expect("counter");
    retry
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!(before.record.value.as_u64().expect("integer") + 1),
        ))
        .await
        .expect("retry increment");
    retry.commit().await.expect("fresh commit");
    let record = first.load(key).await.expect("final read").expect("counter");
    assert_eq!(record.record.value.as_u64(), Some(committed + 1));
    assert_eq!(record.revision.get(), committed + 2);
}

#[tokio::test]
async fn lancedb_state_present_reads_prevent_cross_namespace_write_skew() {
    LanceDirectory::new()
        .write_skew(InitialRecords::Present)
        .await;
}

#[tokio::test]
async fn lancedb_state_absent_reads_prevent_cross_namespace_write_skew() {
    LanceDirectory::new()
        .write_skew(InitialRecords::Absent)
        .await;
}

#[tokio::test]
async fn lancedb_state_tombstone_reads_prevent_cross_namespace_write_skew() {
    LanceDirectory::new()
        .write_skew(InitialRecords::Tombstones)
        .await;
}

#[tokio::test]
async fn lancedb_state_delete_and_recreate_cannot_hide_a_revision_change() {
    let directory = LanceDirectory::new();
    let first = directory.connect().await;
    let second = directory.connect().await;
    let key = StateKey::new("aba", "head").expect("head");
    let receipt = StateKey::new("aba", "stale-receipt").expect("receipt");
    let mut seed = first.begin_transaction().await.expect("seed");
    seed.put(StateRecord::new(key.clone(), serde_json::json!("same")))
        .await
        .expect("seed put");
    seed.commit().await.expect("seed commit");
    let mut stale = first.begin_transaction().await.expect("stale transaction");
    let original = stale.load(key.clone()).await.expect("read").expect("head");
    assert_eq!(original.revision.get(), 1);
    stale
        .put(StateRecord::new(key.clone(), serde_json::json!("stale")))
        .await
        .expect("stage stale head");
    stale
        .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
        .await
        .expect("stage stale receipt");
    let mut replacement = second.begin_transaction().await.expect("replacement");
    replacement.delete(key.clone()).await.expect("delete");
    replacement
        .put(StateRecord::new(key.clone(), serde_json::json!("same")))
        .await
        .expect("recreate identical value");
    replacement.commit().await.expect("replacement commit");
    assert!(!LanceDirectory::committed(stale.commit().await));
    assert!(second.load(receipt).await.expect("stale receipt").is_none());
    let current = first
        .load(key)
        .await
        .expect("read")
        .expect("recreated head");
    assert_eq!(current.record.value, original.record.value);
    assert_eq!(current.revision.get(), 3);
}

#[tokio::test]
async fn lancedb_state_concurrent_initialization_preserves_independent_connections() {
    let directory = LanceDirectory::new();
    let stores = futures::future::join_all((0..6).map(|_| directory.connect())).await;
    for (index, store) in stores.iter().enumerate() {
        let key = StateKey::new("initialization", format!("record-{index}")).expect("key");
        let mut transaction = store.begin_transaction().await.expect("transaction");
        transaction
            .put(StateRecord::new(key, serde_json::json!(index)))
            .await
            .expect("write from independent connection");
        transaction
            .commit()
            .await
            .expect("commit independent write");
    }
    for store in &stores {
        let page = store
            .list(StateListRequest::new("initialization").expect("namespace"))
            .await
            .expect("all independently committed records");
        assert_eq!(page.records.len(), stores.len());
        assert!(page.next_start_after.is_none());
        for (index, record) in page.records.iter().enumerate() {
            assert_eq!(record.record.key.key(), format!("record-{index}"));
            assert_eq!(record.record.value.as_u64(), Some(index as u64));
            assert_eq!(record.revision.get(), 1);
        }
    }
    drop(stores);
    let reopened = directory.connect().await;
    assert_eq!(
        reopened
            .list(StateListRequest::new("initialization").expect("namespace"))
            .await
            .expect("reopened page")
            .records
            .len(),
        6
    );
}

#[tokio::test]
async fn lancedb_state_json_encoding_survives_reopen() {
    let directory = LanceDirectory::new();
    let key = StateKey::new("json", "ordered-value").expect("key");
    let value: serde_json::Value = serde_json::from_str(
        r#"{"z-last":null,"a-first":{"second":-0.0,"first":[true,false,"雪\n\"",18446744073709551615,-9223372036854775808,1.25]},"middle":[]}"#,
    )
    .expect("JSON fixture");
    // Compare serialization as well as Value equality: object equality alone
    // cannot detect a codec that changes an enabled preserve_order map.
    let encoded = serde_json::to_string(&value).expect("original encoding");
    let store = directory.connect().await;
    let mut transaction = store.begin_transaction().await.expect("transaction");
    transaction
        .put(StateRecord::new(key.clone(), value))
        .await
        .expect("stage JSON");
    let staged = transaction
        .load(key.clone())
        .await
        .expect("staged JSON")
        .expect("staged record");
    assert_eq!(
        serde_json::to_string(staged.record.value.as_ref()).expect("staged encoding"),
        encoded
    );
    transaction.commit().await.expect("commit JSON");
    let committed = store
        .load(key.clone())
        .await
        .expect("load")
        .expect("record");
    assert_eq!(
        serde_json::to_string(committed.record.value.as_ref()).expect("committed encoding"),
        encoded
    );
    drop(store);
    let reopened = directory.connect().await;
    let observed = reopened
        .load(key)
        .await
        .expect("reopen read")
        .expect("record");
    assert_eq!(observed.revision.get(), 1);
    assert_eq!(
        serde_json::to_string(observed.record.value.as_ref()).expect("reopened encoding"),
        encoded
    );
}

#[tokio::test]
async fn lancedb_state_large_json_commit_reopens_without_truncation() {
    let directory = LanceDirectory::new();
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    let mut payload = String::with_capacity(12 * 1024 * 1024);
    for _ in 0..12 * 1024 * 1024 {
        // Fixed high-entropy printable data keeps the native encoded object
        // large without requiring a random dependency or storing a fixture.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(char::from(alphabet[(state & 63) as usize]));
    }
    let value = serde_json::json!({"payload": payload});
    let key = StateKey::new("large-json", "payload").expect("key");
    let store = directory.connect().await;
    let mut transaction = store.begin_transaction().await.expect("large transaction");
    transaction
        .put(StateRecord::new(key.clone(), value.clone()))
        .await
        .expect("large put");
    transaction.commit().await.expect("large commit");
    drop(store);
    let reopened = directory.connect().await;
    let observed = reopened
        .load(key)
        .await
        .expect("large read")
        .expect("large record");
    assert_eq!(observed.revision.get(), 1);
    assert_eq!(observed.record.value.as_ref(), &value);
}

#[tokio::test]
async fn lancedb_state_memory_isolated_connections_and_shared_handle_clones() {
    let config = StateStoreConfig::Lance(LanceStateStoreConfig::memory());
    let first = StateStore::connect(config.clone())
        .await
        .expect("first memory store");
    let shared = first.clone();
    let independent = StateStore::connect(config)
        .await
        .expect("independent memory store");
    let key = StateKey::new("memory", "same-key").expect("key");
    let mut transaction = first.begin_transaction().await.expect("first transaction");
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!("shared")))
        .await
        .expect("first put");
    transaction.commit().await.expect("first commit");
    drop(first);
    let observed = shared
        .load(key.clone())
        .await
        .expect("clone read")
        .expect("shared record");
    assert_eq!(observed.record.value.as_ref(), &serde_json::json!("shared"));
    assert!(
        independent
            .load(key.clone())
            .await
            .expect("independent read")
            .is_none()
    );
    let mut transaction = independent
        .begin_transaction()
        .await
        .expect("independent transaction");
    transaction
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!("independent"),
        ))
        .await
        .expect("independent put");
    transaction.commit().await.expect("independent commit");
    for (store, value) in [(&shared, "shared"), (&independent, "independent")] {
        let observed = store
            .load(key.clone())
            .await
            .expect("read")
            .expect("record");
        assert_eq!(observed.revision.get(), 1);
        assert_eq!(observed.record.value.as_ref(), &serde_json::json!(value));
    }
}

#[tokio::test]
async fn lancedb_state_reserved_collection_is_rejected_before_initialization() {
    let directory = LanceDirectory::new();
    let database = directory.database().await;
    for mode in [
        CreateCollectionMode::Create,
        CreateCollectionMode::Overwrite,
        CreateCollectionMode::CreateIfMissing,
    ] {
        let result = database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: StorageLocator::new("__ai_kit_state"),
                schema: None,
                records: vec![
                    DataRecord::without_id().json_field("caller", serde_json::json!(true)),
                ],
                mode,
            }))
            .await;
        assert!(
            matches!(result, Err(StorageError::InvalidRequest(_))),
            "reserved name must reject {mode:?} before creating a native table: {result:?}"
        );
    }
    let store = directory.connect().await;
    let mut transaction = store
        .begin_transaction()
        .await
        .expect("state initialization");
    transaction
        .put(StateRecord::new(
            StateKey::new("state", "proof").expect("key"),
            serde_json::json!(true),
        ))
        .await
        .expect("put after rejected creation");
    transaction
        .commit()
        .await
        .expect("commit after rejected creation");
}

#[tokio::test]
async fn lancedb_state_reserved_collection_rejects_case_insensitive_aliases() {
    let directory = LanceDirectory::new();
    let database = directory.database().await;
    let key = StateKey::new("case-alias", "checkpoint").expect("key");
    for initialized in [false, true] {
        if initialized {
            let store = directory.connect().await;
            let mut transaction = store.begin_transaction().await.expect("state transaction");
            transaction
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::json!("preserved"),
                ))
                .await
                .expect("checkpoint");
            transaction.commit().await.expect("state commit");
        }
        for alias in ["__AI_KIT_STATE", "__Ai_Kit_State"] {
            let locator = StorageLocator::new(alias);
            for request in [
                CatalogRequest::CreateCollection(CreateCollectionRequest {
                    locator: locator.clone(),
                    schema: None,
                    records: vec![
                        DataRecord::without_id().json_field("caller", serde_json::json!(true)),
                    ],
                    mode: CreateCollectionMode::Overwrite,
                }),
                CatalogRequest::OpenCollection {
                    locator: locator.clone(),
                },
                CatalogRequest::DropCollection {
                    locator,
                    ignore_missing: true,
                },
            ] {
                let result = database.catalog(request.clone()).await;
                assert!(
                    matches!(result, Err(StorageError::InvalidRequest(_))),
                    "reserved alias must reject before native I/O (initialized={initialized}): {request:?}: {result:?}"
                );
            }
        }
    }
    let reopened = directory.connect().await;
    let observed = reopened
        .load(key)
        .await
        .expect("checkpoint read")
        .expect("checkpoint");
    assert_eq!(
        observed.record.value.as_ref(),
        &serde_json::json!("preserved")
    );
    assert_eq!(observed.revision.get(), 1);
}

#[tokio::test]
async fn lancedb_state_and_database_share_a_directory_with_reserved_state_boundaries() {
    let directory = LanceDirectory::new();
    let store = directory.connect().await;
    let key = StateKey::new("control-plane", "checkpoint").expect("state key");
    let mut transaction = store.begin_transaction().await.expect("state transaction");
    transaction
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!({"step": 7}),
        ))
        .await
        .expect("state put");
    transaction.commit().await.expect("state commit");
    let database = directory.database().await;
    let knowledge = StorageLocator::new("knowledge");
    database
        .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
            locator: knowledge.clone(),
            schema: None,
            records: vec![
                DataRecord::without_id().json_field("body", serde_json::json!("knowledge")),
            ],
            mode: CreateCollectionMode::Create,
        }))
        .await
        .expect("ordinary knowledge collection beside state");
    for limit in [None, Some(1)] {
        let CatalogResponse::Collections(collections) = database
            .catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
                namespace: Vec::new(),
                limit,
                start_after: None,
            }))
            .await
            .expect("public catalog")
        else {
            panic!("collection list response")
        };
        assert_eq!(
            collections
                .iter()
                .map(|info| info.locator.collection.as_str())
                .collect::<Vec<_>>(),
            vec!["knowledge"],
            "reserved collections must be hidden before pagination"
        );
    }
    let reserved = StorageLocator::new("__ai_kit_state");
    let mut catalogs = vec![
        CatalogRequest::OpenCollection {
            locator: reserved.clone(),
        },
        CatalogRequest::DropCollection {
            locator: reserved.clone(),
            ignore_missing: true,
        },
        CatalogRequest::AlterCollection(SchemaEvolutionRequest::DropColumns(DropColumnsRequest {
            locator: reserved.clone(),
            columns: vec!["namespace".to_owned()],
        })),
    ];
    for mode in [
        CreateCollectionMode::Create,
        CreateCollectionMode::Overwrite,
        CreateCollectionMode::CreateIfMissing,
    ] {
        catalogs.push(CatalogRequest::CreateCollection(CreateCollectionRequest {
            locator: reserved.clone(),
            schema: None,
            records: vec![DataRecord::without_id().json_field("caller", serde_json::json!(true))],
            mode,
        }));
    }
    for request in catalogs {
        let result = database.catalog(request.clone()).await;
        assert!(
            matches!(result, Err(StorageError::InvalidRequest(_))),
            "reserved catalog target must be rejected: {request:?}: {result:?}"
        );
    }
    for request in [
        DataRequest::Count(CountRequest {
            locator: reserved.clone(),
            filter: None,
        }),
        DataRequest::Scan(ScanRequest::new(reserved.clone())),
        DataRequest::Insert(InsertRequest::append(
            reserved.clone(),
            vec![DataRecord::without_id().json_field("caller", serde_json::json!(true))],
        )),
        DataRequest::Delete(DeleteRequest {
            locator: reserved.clone(),
            filter: FilterExpr::True,
        }),
    ] {
        let result = database.data(request.clone()).await;
        assert!(
            matches!(result, Err(StorageError::InvalidRequest(_))),
            "reserved data target must be rejected: {request:?}: {result:?}"
        );
    }
    let result = database
        .query(QueryRequest::Scroll(ScrollRequest {
            locator: reserved.clone(),
            filter: None,
            projection: Projection::All,
            limit: Some(10),
            offset: None,
            page_token: None,
        }))
        .await;
    assert!(
        matches!(result, Err(StorageError::InvalidRequest(_))),
        "reserved query target must be rejected: {result:?}"
    );
    for request in [
        IndexRequest::List {
            locator: reserved.clone(),
        },
        IndexRequest::Create(CreateIndexRequest {
            locator: reserved.clone(),
            columns: vec!["namespace".to_owned()],
            kind: IndexKind::Scalar,
            wait_timeout_ms: None,
        }),
        IndexRequest::Drop {
            locator: reserved.clone(),
            name: "caller-index".to_owned(),
        },
        IndexRequest::WaitFor(WaitForIndexRequest {
            locator: reserved,
            index_names: vec!["caller-index".to_owned()],
            timeout_ms: 100,
        }),
    ] {
        let result = database.index(request.clone()).await;
        assert!(
            matches!(result, Err(StorageError::InvalidRequest(_))),
            "reserved index target must be rejected: {request:?}: {result:?}"
        );
    }
    assert_eq!(
        database
            .data(DataRequest::Count(CountRequest {
                locator: knowledge.clone(),
                filter: None,
            }))
            .await
            .expect("knowledge count"),
        DataResponse::Count(1)
    );
    let page = database
        .query(QueryRequest::Scroll(ScrollRequest {
            locator: knowledge.clone(),
            filter: None,
            projection: Projection::All,
            limit: Some(10),
            offset: None,
            page_token: None,
        }))
        .await
        .expect("ordinary knowledge query");
    assert_eq!(page.hits.len(), 1);
    database
        .catalog(CatalogRequest::DropCollection {
            locator: knowledge,
            ignore_missing: false,
        })
        .await
        .expect("drop ordinary knowledge collection");
    drop(database);
    drop(store);
    let reopened = directory.connect().await;
    let observed = reopened
        .load(key)
        .await
        .expect("state read")
        .expect("preserved checkpoint");
    assert_eq!(
        observed.record.value.as_ref(),
        &serde_json::json!({"step": 7})
    );
    assert_eq!(observed.revision.get(), 1);
}
