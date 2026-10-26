use std::{path::Path, sync::Arc};

use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateKeyPrefix, StateListRequest, StateRecord},
    },
};

use crate::lancedb_state_failures::{NativeFault, NativeFixture};

struct OptimizationFixture {
    directory: tempfile::TempDir,
}

impl OptimizationFixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("owned optimization fixture"),
        }
    }

    async fn connect(&self) -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            self.directory.path(),
        )))
        .await
        .expect("embedded state store")
    }

    fn key(namespace: &str, key: &str) -> StateKey {
        StateKey::new(namespace, key).expect("valid optimization key")
    }

    fn bytes(root: &Path) -> u64 {
        let mut pending = vec![root.to_path_buf()];
        let mut total = 0;
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).expect("owned directory") {
                let entry = entry.expect("owned entry");
                let metadata = entry.metadata().expect("owned metadata");
                if metadata.is_dir() {
                    pending.push(entry.path());
                } else {
                    assert!(metadata.is_file());
                    total += metadata.len();
                }
            }
        }
        total
    }
}

impl NativeFixture {
    async fn cached_maintenance_boundary(&self, store: &StateStore) -> StateKey {
        let key = OptimizationFixture::key("fault", "record");
        let mut seed = store.begin_transaction().await.expect("seed snapshot");
        seed.put(StateRecord::new(
            key.clone(),
            serde_json::json!({"value": 1}),
        ))
        .await
        .expect("seed cached value");
        seed.commit().await.expect("publish cached value");
        let cached = store
            .load(key.clone())
            .await
            .expect("warm read")
            .expect("record");
        assert_eq!(cached.revision.get(), 1);
        assert_eq!(*cached.record.value, serde_json::json!({"value": 1}));
        drop(cached);
        for revision in 1..=62 {
            let mut transaction = store.begin_transaction().await.expect("advance snapshot");
            transaction
                .put(StateRecord::new(
                    OptimizationFixture::key("optimization", "maintenance-head"),
                    serde_json::json!(revision),
                ))
                .await
                .expect("advance head without changing cached record");
            transaction.commit().await.expect("publish head advance");
        }
        let native = self.open_native().await;
        assert_eq!(native.version_id(), 64);
        assert_eq!(native.versions().await.expect("retained history").len(), 64);
        key
    }
}

#[tokio::test]
async fn maintenance_decodes_changed_json_bytes_at_the_same_record_revision() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let key = fixture.cached_maintenance_boundary(&store).await;
    let bytes = br#" { "changed": 1, "changed": 2, "array": [true, null, "\u6771"] } "#;
    let expected: serde_json::Value = serde_json::from_slice(bytes).expect("valid fixture JSON");
    assert_eq!(expected["changed"], 2);

    // Native replacement is confined to this disposable validation fixture.
    // The public cache still contains the earlier bytes at this key/revision.
    assert_eq!(
        fixture
            .publish_content(NativeFault::Json, Some(bytes.to_vec()))
            .await,
        65
    );
    let current = store
        .load(key.clone())
        .await
        .expect("maintenance validates changed bytes")
        .expect("maintained record");
    assert_eq!(current.revision.get(), 1);
    assert_eq!(*current.record.value, expected);
    let native = fixture.open_native().await;
    assert_eq!(native.version_id(), 66, "full rewrite was published");
    assert_eq!(native.versions().await.expect("reclaimed history").len(), 1);
    drop(native);
    drop(current);
    drop(store);
    let reopened = fixture.connect().await;
    let persisted = reopened
        .load(key)
        .await
        .expect("reopen read")
        .expect("record");
    assert_eq!(persisted.revision.get(), 1);
    assert_eq!(*persisted.record.value, expected);
}

#[tokio::test]
async fn maintenance_rejects_changed_malformed_json_before_reclamation() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let key = fixture.cached_maintenance_boundary(&store).await;
    assert_eq!(
        fixture
            .publish_content(
                NativeFault::Json,
                Some(br#"{"value": 2} trailing"#.to_vec())
            )
            .await,
        65
    );
    let original_versions = fixture
        .open_native()
        .await
        .versions()
        .await
        .expect("history before rejected maintenance")
        .into_iter()
        .map(|version| version.version)
        .collect::<Vec<_>>();
    let error = store
        .load(key)
        .await
        .expect_err("changed bytes must be decoded");
    let StateStoreError::BackendOperation { operation, .. } = error else {
        panic!("malformed JSON must fail maintenance validation: {error:?}");
    };
    assert_eq!(operation, "maintenance");
    let native = fixture.open_native().await;
    assert_eq!(native.version_id(), 65, "no rewrite of corrupt records");
    assert_eq!(
        native
            .versions()
            .await
            .expect("history after rejected maintenance")
            .into_iter()
            .map(|version| version.version)
            .collect::<Vec<_>>(),
        original_versions,
        "validation failure cannot reclaim history"
    );
}

#[tokio::test]
async fn bounded_pages_match_literal_byte_order_across_many_records() {
    let fixture = OptimizationFixture::new();
    let store = fixture.connect().await;
    let mut expected = Vec::new();
    let mut transaction = store.begin_transaction().await.expect("seed snapshot");
    for index in 0..2049 {
        let key = format!(
            "run/%_{index:04}/{}",
            if index % 2 == 0 { "é" } else { "東" }
        );
        transaction
            .put(StateRecord::new(
                OptimizationFixture::key("pages", &key),
                serde_json::json!(index),
            ))
            .await
            .expect("seed page row");
        if index % 7 == 0 {
            transaction
                .delete(OptimizationFixture::key("pages", &key))
                .await
                .expect("page tombstone");
        } else {
            expected.push(key);
        }
        transaction
            .put(StateRecord::new(
                OptimizationFixture::key("other", &format!("run/%_{index:04}")),
                serde_json::json!(true),
            ))
            .await
            .expect("other namespace");
    }
    transaction
        .put(StateRecord::new(
            OptimizationFixture::key("pages", "run/ordinary"),
            serde_json::json!(true),
        ))
        .await
        .expect("literal prefix exclusion");
    transaction.commit().await.expect("publish page fixture");
    expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

    for limit in [1, 1000] {
        // A bounded suffix gives the limit=1 case meaningful continuation
        // coverage without requiring thousands of full-snapshot baseline reads.
        let cursor = if limit == 1 {
            "run/%_2029/absent"
        } else {
            "run/%_0000/absent"
        };
        let oracle = expected
            .iter()
            .filter(|key| key.as_bytes() > cursor.as_bytes())
            .cloned()
            .collect::<Vec<_>>();
        let mut after = cursor.to_owned();
        let mut observed = Vec::new();
        loop {
            let page = store
                .list(
                    StateListRequest::new("pages")
                        .expect("namespace")
                        .with_key_prefix(StateKeyPrefix::new("run/%_").expect("literal prefix"))
                        .with_limit(limit)
                        .expect("limit")
                        .start_after(after.clone())
                        .expect("cursor"),
                )
                .await
                .expect("page");
            assert!(page.records.len() <= limit);
            let keys = page
                .records
                .iter()
                .map(|record| record.record.key.key().to_owned())
                .collect::<Vec<_>>();
            observed.extend(keys);
            let more = observed.len() < oracle.len();
            assert_eq!(page.next_start_after.is_some(), more);
            match page.next_start_after {
                Some(next) => {
                    assert!(next.as_bytes() > after.as_bytes());
                    after = next;
                }
                None => break,
            }
        }
        assert_eq!(observed, oracle);
    }
}

#[tokio::test]
async fn repeated_views_remain_fresh_and_caller_edits_stay_private() {
    let fixture = OptimizationFixture::new();
    let store = fixture.connect().await;
    let writer = fixture.connect().await;
    let key = OptimizationFixture::key("image", "record");
    let mut seed = store.begin_transaction().await.expect("seed");
    seed.put(StateRecord::new(
        key.clone(),
        serde_json::json!({"value": 1}),
    ))
    .await
    .expect("seed put");
    seed.commit().await.expect("seed commit");
    let mut original = store
        .begin_transaction()
        .await
        .expect("retained original image");
    for _ in 0..3 {
        let mut observed = store
            .load(key.clone())
            .await
            .expect("warm load")
            .expect("present");
        Arc::make_mut(&mut observed.record.value)["value"] = serde_json::json!(999);
        assert_eq!(
            store
                .list(StateListRequest::new("image").expect("namespace"))
                .await
                .expect("warm list")
                .records[0]
                .record
                .value["value"],
            1
        );
    }
    let mut changed = writer
        .begin_transaction()
        .await
        .expect("independent writer");
    changed.delete(key.clone()).await.expect("delete");
    changed
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!({"value": 2}),
        ))
        .await
        .expect("recreate");
    changed.commit().await.expect("publish changed image");
    assert_eq!(
        original
            .load(key.clone())
            .await
            .expect("old image")
            .expect("original")
            .revision
            .get(),
        1
    );
    original.commit().await.expect("read-only old image");
    let current = store
        .load(key.clone())
        .await
        .expect("fresh load")
        .expect("current");
    assert_eq!(current.revision.get(), 3);
    assert_eq!(current.record.value["value"], 2);
    let mut fresh = store.begin_transaction().await.expect("fresh transaction");
    assert_eq!(
        fresh
            .load(key)
            .await
            .expect("fresh transaction read")
            .expect("record")
            .revision
            .get(),
        3
    );
    fresh.rollback().await.expect("rollback");
}

#[tokio::test]
async fn multi_batch_mutations_preserve_every_authored_revision() {
    let fixture = OptimizationFixture::new();
    let store = fixture.connect().await;
    for count in [127, 128, 129, 385] {
        let namespace = format!("batch-{count}");
        let mut transaction = store.begin_transaction().await.expect("batch transaction");
        for index in 0..count {
            let key = OptimizationFixture::key(&namespace, &format!("{index:04}"));
            transaction
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::json!({"index": index, "payload": "x".repeat(1024)}),
                ))
                .await
                .expect("first put");
            transaction
                .delete(key.clone())
                .await
                .expect("authored delete");
            transaction
                .put(StateRecord::new(key, serde_json::json!({"index": index})))
                .await
                .expect("authored recreation");
        }
        transaction.commit().await.expect("batch commit");
        let mut verifier = store
            .begin_transaction()
            .await
            .expect("atomic verification");
        for index in 0..count {
            let record = verifier
                .load(OptimizationFixture::key(&namespace, &format!("{index:04}")))
                .await
                .expect("batch read")
                .expect("present row");
            assert_eq!(record.revision.get(), 3);
            assert_eq!(record.record.value["index"], index);
        }
        verifier.rollback().await.expect("verification rollback");

        // Keep an old image while publishing a second mixed delta. This also
        // proves that a locally advanced decoded image preserves authored
        // revisions and cannot mutate a transaction's earlier view.
        let mut old = store.begin_transaction().await.expect("retained image");
        let mut changed = store.begin_transaction().await.expect("mixed delta");
        for index in 0..count {
            let key = OptimizationFixture::key(&namespace, &format!("{index:04}"));
            changed.delete(key.clone()).await.expect("mixed tombstone");
            if index % 2 == 0 {
                changed
                    .put(StateRecord::new(key, serde_json::json!({"updated": index})))
                    .await
                    .expect("mixed recreation");
            }
        }
        changed.commit().await.expect("mixed publication");
        let mut current = store.begin_transaction().await.expect("current image");
        for index in 0..count {
            let key = OptimizationFixture::key(&namespace, &format!("{index:04}"));
            let previous = old
                .load(key.clone())
                .await
                .expect("old read")
                .expect("old row");
            assert_eq!(previous.revision.get(), 3);
            assert_eq!(previous.record.value["index"], index);
            let record = current.load(key.clone()).await.expect("current read");
            if index % 2 == 0 {
                let record = record.expect("recreated row");
                assert_eq!(record.revision.get(), 5);
                assert_eq!(record.record.value["updated"], index);
            } else {
                assert!(record.is_none());
                current
                    .put(StateRecord::new(
                        key,
                        serde_json::json!({"restored": index}),
                    ))
                    .await
                    .expect("recreate cached tombstone");
            }
        }
        current
            .commit()
            .await
            .expect("tombstone recreation publication");
        old.rollback().await.expect("retained image settled");
        let restored = store
            .load(OptimizationFixture::key(&namespace, "0001"))
            .await
            .expect("fresh restored read")
            .expect("restored row");
        assert_eq!(restored.revision.get(), 5);
        assert_eq!(restored.record.value["restored"], 1);
    }
}

#[tokio::test]
async fn already_stale_preparation_does_not_grow_an_owned_quiescent_directory() {
    let fixture = OptimizationFixture::new();
    let store = fixture.connect().await;
    let mut stale = store.begin_transaction().await.expect("stale snapshot");
    for index in 0..129 {
        stale
            .put(StateRecord::new(
                OptimizationFixture::key("loser", &format!("{index:04}")),
                serde_json::json!("x".repeat(8192)),
            ))
            .await
            .expect("large stale overlay");
    }
    let receipt = OptimizationFixture::key("loser", "receipt");
    stale
        .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
        .await
        .expect("stale receipt");
    let mut winner = store.begin_transaction().await.expect("winner");
    winner
        .put(StateRecord::new(
            OptimizationFixture::key("winner", "receipt"),
            serde_json::json!(true),
        ))
        .await
        .expect("winner receipt");
    winner.commit().await.expect("intervening publication");
    let before = OptimizationFixture::bytes(fixture.directory.path());
    assert!(matches!(
        stale.commit().await,
        Err(StateStoreError::TransactionConflict { .. })
    ));
    let after = OptimizationFixture::bytes(fixture.directory.path());
    assert!(
        after <= before,
        "a positively stale input must be rejected before native data preparation: {before} -> {after}"
    );
    let mut verifier = store.begin_transaction().await.expect("fresh verifier");
    assert!(
        verifier
            .load(receipt)
            .await
            .expect("receipt absent")
            .is_none()
    );
    assert!(
        verifier
            .load(OptimizationFixture::key("loser", "0000"))
            .await
            .expect("data absent")
            .is_none()
    );
    verifier
        .put(StateRecord::new(
            OptimizationFixture::key("progress", "receipt"),
            serde_json::json!(true),
        ))
        .await
        .expect("subsequent progress");
    verifier.commit().await.expect("subsequent publication");
}
