#![cfg(any(feature = "redb", feature = "sqlite"))]

#[cfg(feature = "redb")]
use storage_kit::state::config::RedbStateStoreConfig;
#[cfg(feature = "redb")]
use storage_kit::state::record::{StateKeyPrefix, StateRecordSnapshot, StateRevision};
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::{
        config::StateStoreConfig,
        record::{StateKey, StateListRequest, StateRecord},
    },
};
use tempfile::tempdir;

use std::time::Duration;

#[cfg(feature = "sqlite")]
use storage_kit::state::config::SqliteStateStoreConfig;

#[cfg(feature = "sqlite")]
enum SqliteLifecycleCase {
    Memory,
    File(tempfile::TempDir),
}

#[cfg(feature = "sqlite")]
impl SqliteLifecycleCase {
    fn config(&self) -> SqliteStateStoreConfig {
        let config = match self {
            Self::Memory => SqliteStateStoreConfig::memory(),
            Self::File(directory) => {
                SqliteStateStoreConfig::file(directory.path().join("state.sqlite"))
            }
        };
        config.with_operation_timeout(Duration::from_secs(2))
    }

    async fn connect(&self) -> StateStore {
        StateStore::connect(StateStoreConfig::sqlite(self.config()))
            .await
            .expect("connect lifecycle store")
    }

    async fn observe(&self, store: StateStore) -> StateStore {
        match self {
            Self::Memory => {
                let observer = store.clone();
                drop(store);
                // A shared owner observes this database; a fresh memory store does not reopen it.
                let fresh = self.connect().await;
                assert!(
                    fresh
                        .list(StateListRequest::new("sqlite-lifecycle").expect("namespace"))
                        .await
                        .expect("fresh memory list")
                        .records
                        .is_empty()
                );
                observer
            }
            Self::File(_) => {
                drop(store);
                self.connect().await
            }
        }
    }

    fn key(fragment: &str) -> StateKey {
        StateKey::new("sqlite-lifecycle", fragment).expect("key")
    }

    async fn assert_record(
        store: &StateStore,
        fragment: &str,
        revision: u64,
        value: serde_json::Value,
    ) {
        let snapshot = store
            .load(Self::key(fragment))
            .await
            .expect("load record")
            .expect("present");
        assert_eq!(snapshot.record.key, Self::key(fragment));
        assert_eq!(snapshot.revision.get(), revision);
        assert_eq!(snapshot.record.value.as_ref(), &value);
    }

    async fn assert_absent(store: &StateStore, fragments: &[&str]) {
        for fragment in fragments {
            assert!(
                store
                    .load(Self::key(fragment))
                    .await
                    .expect("load absent record")
                    .is_none(),
                "{fragment}"
            );
        }
    }

    async fn repeated_operations(&self) {
        let store = self.connect().await;
        let mut transaction = store.begin_transaction().await.expect("transaction");
        for fragment in ["a", "b"] {
            let key = Self::key(fragment);
            assert!(
                transaction
                    .load(key.clone())
                    .await
                    .expect("missing load")
                    .is_none()
            );
            for cycle in 0..3 {
                transaction
                    .put(StateRecord::new(key.clone(), serde_json::json!(cycle)))
                    .await
                    .expect("put");
                let snapshot = transaction
                    .load(key.clone())
                    .await
                    .expect("load put")
                    .expect("present");
                assert_eq!(snapshot.revision.get(), 3 * cycle + 1);
                assert_eq!(snapshot.record.value.as_ref(), &serde_json::json!(cycle));
                transaction.delete(key.clone()).await.expect("delete");
                assert!(
                    transaction
                        .load(key.clone())
                        .await
                        .expect("tombstone load")
                        .is_none()
                );
                transaction
                    .put(StateRecord::new(key.clone(), serde_json::json!(cycle)))
                    .await
                    .expect("recreate");
                let snapshot = transaction
                    .load(key.clone())
                    .await
                    .expect("load recreated")
                    .expect("present");
                assert_eq!(snapshot.revision.get(), 3 * cycle + 3);
                assert_eq!(snapshot.record.value.as_ref(), &serde_json::json!(cycle));
            }
        }
        transaction
            .delete(Self::key("tombstone"))
            .await
            .expect("absent delete");
        transaction
            .commit()
            .await
            .expect("commit repeated operations");

        for _ in 0..3 {
            Self::assert_record(&store, "a", 9, serde_json::json!(2)).await;
            let page = store
                .list(StateListRequest::new("sqlite-lifecycle").expect("namespace"))
                .await
                .expect("exhausted multi-row list");
            assert!(page.next_start_after.is_none());
            assert_eq!(page.records.len(), 2);
            for (snapshot, fragment) in page.records.iter().zip(["a", "b"]) {
                assert_eq!(snapshot.record.key, Self::key(fragment));
                assert_eq!(snapshot.revision.get(), 9);
                assert_eq!(snapshot.record.value.as_ref(), &serde_json::json!(2));
            }
            let mut rollback = store
                .begin_transaction()
                .await
                .expect("rollback transaction");
            for (fragment, revision) in [("a", 10), ("tombstone", 2), ("rolled-back", 1)] {
                let key = Self::key(fragment);
                rollback
                    .put(StateRecord::new(key.clone(), serde_json::json!("discard")))
                    .await
                    .expect("stage rollback");
                let snapshot = rollback
                    .load(key.clone())
                    .await
                    .expect("rollback read")
                    .expect("staged");
                assert_eq!(snapshot.revision.get(), revision);
                assert_eq!(
                    snapshot.record.value.as_ref(),
                    &serde_json::json!("discard")
                );
                rollback.delete(key.clone()).await.expect("rollback delete");
                assert!(
                    rollback
                        .load(key)
                        .await
                        .expect("rollback tombstone")
                        .is_none()
                );
            }
            rollback.rollback().await.expect("rollback after reads");
            Self::assert_record(&store, "b", 9, serde_json::json!(2)).await;
            Self::assert_absent(&store, &["tombstone", "rolled-back"]).await;
        }

        let store = self.observe(store).await;
        Self::assert_record(&store, "a", 9, serde_json::json!(2)).await;
        Self::assert_record(&store, "b", 9, serde_json::json!(2)).await;
        Self::assert_absent(&store, &["tombstone", "rolled-back"]).await;
        let mut progress = store
            .begin_transaction()
            .await
            .expect("progress transaction");
        for (fragment, revision) in [("a", 10), ("tombstone", 2)] {
            let key = Self::key(fragment);
            progress
                .put(StateRecord::new(key.clone(), serde_json::json!("progress")))
                .await
                .expect("progress put");
            let snapshot = progress
                .load(key)
                .await
                .expect("progress read")
                .expect("present");
            assert_eq!(
                snapshot.revision.get(),
                revision,
                "rolled-back revisions must not survive"
            );
        }
        progress
            .delete(Self::key("tombstone"))
            .await
            .expect("restore tombstone");
        progress.commit().await.expect("progress commit");
        let store = self.observe(store).await;
        Self::assert_record(&store, "a", 10, serde_json::json!("progress")).await;
        Self::assert_record(&store, "b", 9, serde_json::json!(2)).await;
        Self::assert_absent(&store, &["tombstone", "rolled-back"]).await;
    }

    async fn cancelled_nonterminal_operation(&self) {
        use std::{
            future::{Future, poll_fn},
            task::Poll,
        };

        let store = self.connect().await;
        for reject_commit in [false, true] {
            let mut transaction = store.begin_transaction().await.expect("transaction");
            transaction
                .put(StateRecord::new(
                    Self::key("staged"),
                    serde_json::json!("discard"),
                ))
                .await
                .expect("stage before cancellation");
            assert_eq!(
                transaction
                    .load(Self::key("staged"))
                    .await
                    .expect("staged read")
                    .expect("present")
                    .revision
                    .get(),
                1
            );
            let mut put = Box::pin(transaction.put(StateRecord::new(
                Self::key("cancelled"),
                serde_json::json!("discard"),
            )));
            // No other task can run before this poll returns on the current-thread runtime.
            poll_fn(|context| match put.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => {
                    panic!("operation unexpectedly completed before yielding: {result:?}")
                }
            })
            .await;
            drop(put);
            assert!(matches!(
                transaction.delete(Self::key("staged")).await,
                Err(StateStoreError::InvalidRequest(_))
            ));
            if reject_commit {
                assert!(matches!(
                    transaction.commit().await,
                    Err(StateStoreError::InvalidRequest(_))
                ));
            } else {
                transaction
                    .rollback()
                    .await
                    .expect("join cancelled operation rollback");
            }
            Self::assert_absent(&store, &["staged", "cancelled"]).await;
        }
        let store = self.observe(store).await;
        Self::assert_absent(&store, &["staged", "cancelled"]).await;
        let mut progress = store
            .begin_transaction()
            .await
            .expect("progress transaction");
        progress
            .put(StateRecord::new(
                Self::key("progress"),
                serde_json::json!("committed"),
            ))
            .await
            .expect("progress put");
        progress.commit().await.expect("progress commit");
        let store = self.observe(store).await;
        Self::assert_record(&store, "progress", 1, serde_json::json!("committed")).await;
        Self::assert_absent(&store, &["staged", "cancelled"]).await;
    }

    async fn admitted_commit(&self) {
        use std::{
            future::{Future, poll_fn},
            task::Poll,
        };

        let store = self.connect().await;
        let mut transaction = store.begin_transaction().await.expect("transaction");
        for fragment in ["receipt-a", "receipt-b"] {
            let key = Self::key(fragment);
            assert!(
                transaction
                    .load(key.clone())
                    .await
                    .expect("missing receipt")
                    .is_none()
            );
            for revision in 1..=2 {
                transaction
                    .put(StateRecord::new(key.clone(), serde_json::json!(revision)))
                    .await
                    .expect("stage receipt");
                let snapshot = transaction
                    .load(key.clone())
                    .await
                    .expect("read receipt")
                    .expect("present");
                assert_eq!(snapshot.revision.get(), revision);
                assert_eq!(snapshot.record.value.as_ref(), &serde_json::json!(revision));
            }
        }
        let mut commit = Box::pin(transaction.commit());
        // The terminal command is admitted before the actor can run; losing this waiter must not revoke it.
        poll_fn(|context| match commit.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => {
                panic!("commit unexpectedly completed before yielding: {result:?}")
            }
        })
        .await;
        drop(commit);
        // File observation drops every store owner before reopening. Memory keeps a shared observer.
        let store = self.observe(store).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let page = store
                .list(StateListRequest::new("sqlite-lifecycle").expect("namespace"))
                .await
                .expect("reconcile atomic receipts");
            assert!(page.next_start_after.is_none());
            if !page.records.is_empty() {
                assert_eq!(
                    page.records.len(),
                    2,
                    "commit publishes both receipts atomically"
                );
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "admitted commit must finish after waiter loss"
            );
            tokio::task::yield_now().await;
        }
        Self::assert_record(&store, "receipt-a", 2, serde_json::json!(2)).await;
        Self::assert_record(&store, "receipt-b", 2, serde_json::json!(2)).await;
        let mut progress = store
            .begin_transaction()
            .await
            .expect("progress transaction");
        progress
            .put(StateRecord::new(
                Self::key("receipt-a"),
                serde_json::json!("next"),
            ))
            .await
            .expect("progress put");
        progress.commit().await.expect("progress commit");
        let store = self.observe(store).await;
        Self::assert_record(&store, "receipt-a", 3, serde_json::json!("next")).await;
        Self::assert_record(&store, "receipt-b", 2, serde_json::json!(2)).await;
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_repeated_operations_release_rows_memory() {
    tokio::time::timeout(
        Duration::from_secs(5),
        SqliteLifecycleCase::Memory.repeated_operations(),
    )
    .await
    .expect("bounded repeated memory operations");
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_repeated_operations_release_rows_file() {
    let case = SqliteLifecycleCase::File(tempdir().expect("temporary directory"));
    tokio::time::timeout(Duration::from_secs(5), case.repeated_operations())
        .await
        .expect("bounded repeated file operations");
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_cancelled_nonterminal_operation_rolls_back_memory() {
    tokio::time::timeout(
        Duration::from_secs(5),
        SqliteLifecycleCase::Memory.cancelled_nonterminal_operation(),
    )
    .await
    .expect("bounded memory cancellation");
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_cancelled_nonterminal_operation_rolls_back_file() {
    let case = SqliteLifecycleCase::File(tempdir().expect("temporary directory"));
    tokio::time::timeout(
        Duration::from_secs(5),
        case.cancelled_nonterminal_operation(),
    )
    .await
    .expect("bounded file cancellation");
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_admitted_commit_survives_lost_waiter_memory() {
    tokio::time::timeout(
        Duration::from_secs(5),
        SqliteLifecycleCase::Memory.admitted_commit(),
    )
    .await
    .expect("bounded memory commit");
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_admitted_commit_survives_lost_waiter_file() {
    let case = SqliteLifecycleCase::File(tempdir().expect("temporary directory"));
    tokio::time::timeout(Duration::from_secs(5), case.admitted_commit())
        .await
        .expect("bounded file commit");
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_commit_and_rollback_preserve_transaction_authority() {
    let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::memory()))
        .await
        .expect("connect Redb");
    let committed_key = StateKey::new("agent", "committed").expect("valid key");
    let rolled_back_key = StateKey::new("agent", "rolled-back").expect("valid key");

    let mut committed = store.begin_transaction().await.expect("transaction");
    committed
        .put(StateRecord::new(
            committed_key.clone(),
            serde_json::json!({"value": "committed"}),
        ))
        .await
        .expect("stage committed record");
    committed.commit().await.expect("commit");

    let snapshot = store
        .load(committed_key.clone())
        .await
        .expect("load")
        .expect("committed record");
    assert_eq!(
        snapshot.record.value.as_ref(),
        &serde_json::json!({"value": "committed"})
    );

    let mut rolled_back = store.begin_transaction().await.expect("transaction");
    rolled_back
        .put(StateRecord::new(
            rolled_back_key.clone(),
            serde_json::json!({"value": "rolled back"}),
        ))
        .await
        .expect("stage rolled-back record");
    rolled_back.rollback().await.expect("rollback");
    assert!(
        store
            .load(rolled_back_key)
            .await
            .expect("load after rollback")
            .is_none()
    );

    let mut deleted = store.begin_transaction().await.expect("transaction");
    deleted.delete(committed_key.clone()).await.expect("delete");
    assert!(
        deleted
            .load(committed_key.clone())
            .await
            .expect("transactional tombstone")
            .is_none()
    );
    deleted.commit().await.expect("commit delete");
    assert!(
        store
            .load(committed_key)
            .await
            .expect("load after delete")
            .is_none()
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_revisions_pagination_and_tombstones_are_observable() {
    let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::memory()))
        .await
        .expect("connect Redb");
    let key = StateKey::new("redb-pagination", "records/a-1").expect("valid key");

    let mut create = store.begin_transaction().await.expect("create transaction");
    create
        .put(StateRecord::new(key.clone(), serde_json::json!({"n": 1})))
        .await
        .expect("create record");
    create.commit().await.expect("commit create");
    let first_revision = store
        .load(key.clone())
        .await
        .expect("load first revision")
        .expect("first record")
        .revision;

    let mut update = store.begin_transaction().await.expect("update transaction");
    update
        .put(StateRecord::new(key.clone(), serde_json::json!({"n": 2})))
        .await
        .expect("update record");
    update
        .put(StateRecord::new(
            StateKey::new("redb-pagination", "records/a-2").expect("valid key"),
            serde_json::json!({"n": 2}),
        ))
        .await
        .expect("second record");
    update
        .put(StateRecord::new(
            StateKey::new("redb-pagination", "records/a-3").expect("valid key"),
            serde_json::json!({"n": 3}),
        ))
        .await
        .expect("third record");
    update.commit().await.expect("commit update");
    let second_revision = store
        .load(key)
        .await
        .expect("load second revision")
        .expect("updated record")
        .revision;
    assert_eq!(first_revision.get(), 1);
    assert_eq!(second_revision.get(), 2);

    let first_page = store
        .list(
            StateListRequest::new("redb-pagination")
                .expect("list request")
                .with_key_prefix(StateKeyPrefix::new("records/a-").expect("prefix"))
                .with_limit(1)
                .expect("limit"),
        )
        .await
        .expect("first page");
    assert_eq!(first_page.records.len(), 1);
    let cursor = first_page.next_start_after.expect("next cursor");

    let second_page = store
        .list(
            StateListRequest::new("redb-pagination")
                .expect("list request")
                .with_key_prefix(StateKeyPrefix::new("records/a-").expect("prefix"))
                .start_after(cursor)
                .expect("cursor")
                .with_limit(10)
                .expect("limit"),
        )
        .await
        .expect("second page");
    assert_eq!(second_page.records.len(), 2);
    assert!(second_page.next_start_after.is_none());

    let deleted_key = second_page.records[0].record.key.clone();
    let mut delete = store.begin_transaction().await.expect("delete transaction");
    delete.delete(deleted_key.clone()).await.expect("delete");
    delete.commit().await.expect("commit delete");
    assert!(
        store
            .load(deleted_key)
            .await
            .expect("load tombstone")
            .is_none()
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_file_shared_prefix_pagination_survives_reopen() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shared-prefix.redb");
        let prefix = format!("records/%_*/{}", "共同/".repeat(96));
        let mut keys = Vec::new();
        for suffix in ["é", "ê", "中", "😀"] {
            for index in 0..128 {
                keys.push(
                    StateKey::new("redb-prefix", format!("{prefix}{suffix}/{index:04}"))
                        .expect("valid long UTF-8 key"),
                );
            }
        }
        keys.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
        assert_eq!(keys.len(), 512);
        let mut saved_cursor = None;

        // Each iteration opens a new file owner; only plain cursor/fixture data survives.
        for opening in 0..3 {
            let config = RedbStateStoreConfig::file(&path)
                .with_operation_timeout(Duration::from_secs(2))
                .expect("valid timeout");
            let store = StateStore::connect(StateStoreConfig::redb(config))
                .await
                .expect("open Redb pagination file");
            if opening == 0 {
                let mut seed = store.begin_transaction().await.expect("seed transaction");
                // Long keys and reverse insertion exercise native page splits without
                // depending on private page layout or exposing native statistics.
                for (ordinal, key) in keys.iter().enumerate().rev() {
                    seed.put(StateRecord::new(
                        key.clone(),
                        serde_json::json!({"ordinal": ordinal, "generation": "initial"}),
                    ))
                    .await
                    .expect("seed matching key");
                    seed.put(StateRecord::new(
                        StateKey::new("redb-prefix-neighbor", key.key()).expect("neighbor key"),
                        serde_json::json!({"ordinal": ordinal, "generation": "neighbor"}),
                    ))
                    .await
                    .expect("seed neighboring namespace");
                }
                for fragment in [
                    "before/records",
                    "zz-after",
                    &prefix.replace('%', "wildcard"),
                    &prefix.replace('_', "x"),
                    &prefix.replace('*', "glob"),
                ] {
                    seed.put(StateRecord::new(
                        StateKey::new("redb-prefix", fragment).expect("outside-prefix key"),
                        serde_json::json!("outside literal prefix"),
                    ))
                    .await
                    .expect("seed outside-prefix key");
                }
                seed.commit().await.expect("commit seed");
                let mut mutate = store
                    .begin_transaction()
                    .await
                    .expect("mutation transaction");
                for (ordinal, key) in keys.iter().enumerate() {
                    match ordinal % 4 {
                        1 => mutate
                            .put(StateRecord::new(
                                key.clone(),
                                serde_json::json!({"ordinal": ordinal, "generation": "updated"}),
                            ))
                            .await
                            .expect("update matching key"),
                        3 => mutate
                            .delete(key.clone())
                            .await
                            .expect("delete matching key"),
                        _ => {}
                    }
                }
                mutate.commit().await.expect("commit mutations");
            }

            // Expectations come from the authored operations, never from returned pages.
            let mut expected = Vec::new();
            for (ordinal, key) in keys.iter().enumerate() {
                let (revision, generation) = if opening == 2 && ordinal == 3 {
                    (3, "recreated")
                } else {
                    match ordinal % 4 {
                        1 => (2, "updated"),
                        3 => {
                            assert!(
                                store
                                    .load(key.clone())
                                    .await
                                    .expect("load tombstone")
                                    .is_none()
                            );
                            continue;
                        }
                        _ => (1, "initial"),
                    }
                };
                expected.push(StateRecordSnapshot::new(
                    StateRecord::new(
                        key.clone(),
                        serde_json::json!({"ordinal": ordinal, "generation": generation}),
                    ),
                    StateRevision::new(revision).expect("expected revision"),
                ));
            }
            assert_eq!(expected.len(), if opening == 2 { 385 } else { 384 });
            for limit in [32, 31] {
                let request = StateListRequest::new("redb-prefix")
                    .expect("namespace")
                    .with_key_prefix(StateKeyPrefix::new(&prefix).expect("literal prefix"))
                    .with_limit(limit)
                    .expect("limit");
                let mut cursor = None;
                for (page_index, records) in expected.chunks(limit).enumerate() {
                    let mut next = request.clone();
                    if let Some(previous) = cursor {
                        next = next.start_after(previous).expect("cursor");
                    }
                    let page = store.list(next).await.expect("exact page");
                    assert_eq!(
                        page.records, records,
                        "opening {opening}, limit {limit}, page {page_index}"
                    );
                    let expected_cursor = if (page_index + 1) * limit < expected.len() {
                        Some(
                            records
                                .last()
                                .expect("nonempty page")
                                .record
                                .key
                                .key()
                                .to_owned(),
                        )
                    } else {
                        None
                    };
                    assert_eq!(page.next_start_after, expected_cursor, "exact continuation");
                    if opening == 0 && limit == 32 && page_index == 0 {
                        saved_cursor = page.next_start_after.clone();
                    }
                    cursor = page.next_start_after;
                }
                let exhausted = store
                    .list(
                        request
                            .start_after(keys.last().expect("last key").key())
                            .expect("end cursor"),
                    )
                    .await
                    .expect("exhausted range");
                assert!(exhausted.records.is_empty());
                assert!(exhausted.next_start_after.is_none());
            }
            if opening > 0 {
                let cursor = saved_cursor.as_deref().expect("pre-close cursor");
                let start = expected.partition_point(|record| {
                    record.record.key.key().as_bytes() <= cursor.as_bytes()
                });
                let page = store
                    .list(
                        StateListRequest::new("redb-prefix")
                            .expect("namespace")
                            .with_key_prefix(StateKeyPrefix::new(&prefix).expect("prefix"))
                            .with_limit(32)
                            .expect("limit")
                            .start_after(cursor)
                            .expect("persisted cursor"),
                    )
                    .await
                    .expect("continue pre-close cursor");
                assert_eq!(page.records, expected[start..start + 32]);
                assert_eq!(
                    page.next_start_after.as_deref(),
                    Some(expected[start + 31].record.key.key())
                );
            }
            if opening == 1 {
                let mut recreate = store
                    .begin_transaction()
                    .await
                    .expect("recreate transaction");
                recreate
                    .put(StateRecord::new(
                        keys[3].clone(),
                        serde_json::json!({"ordinal": 3, "generation": "recreated"}),
                    ))
                    .await
                    .expect("recreate tombstone");
                recreate.commit().await.expect("commit recreation");
            }
            drop(store);
        }
    })
    .await
    .expect("bounded file pagination and reopen");
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_fifo_cancels_dropped_queued_reservations() {
    let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::memory()))
        .await
        .expect("connect Redb");
    let active_key = StateKey::new("redb-fifo", "active").expect("valid key");
    let progress_key = StateKey::new("redb-fifo", "progress").expect("valid key");

    let mut active = tokio::time::timeout(Duration::from_secs(2), store.begin_transaction())
        .await
        .expect("begin active timeout")
        .expect("begin active");
    active
        .put(StateRecord::new(
            active_key.clone(),
            serde_json::json!({"committed": true}),
        ))
        .await
        .expect("active put");

    let queued = tokio::time::timeout(Duration::from_secs(2), store.begin_transaction())
        .await
        .expect("begin queued timeout")
        .expect("begin queued");
    drop(queued);
    active.commit().await.expect("commit active");

    let mut progress = tokio::time::timeout(Duration::from_secs(2), store.begin_transaction())
        .await
        .expect("begin progress timeout")
        .expect("begin progress");
    progress
        .put(StateRecord::new(
            progress_key.clone(),
            serde_json::json!({"committed": true}),
        ))
        .await
        .expect("progress put");
    progress.commit().await.expect("commit progress");

    assert!(store.load(active_key).await.expect("load active").is_some());
    assert!(
        store
            .load(progress_key)
            .await
            .expect("load progress")
            .is_some()
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_drop_rolls_back_and_settles_before_file_reopen() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("owned-drop.redb");
    let config = RedbStateStoreConfig::file(path.clone())
        .with_operation_timeout(Duration::from_secs(2))
        .expect("valid timeout");
    let store = StateStore::connect(StateStoreConfig::redb(config))
        .await
        .expect("connect Redb file");
    let dropped_key = StateKey::new("redb-drop", "dropped").expect("valid key");
    let committed_key = StateKey::new("redb-drop", "committed").expect("valid key");

    let mut dropped = store.begin_transaction().await.expect("transaction");
    dropped
        .put(StateRecord::new(
            dropped_key.clone(),
            serde_json::json!({"committed": false}),
        ))
        .await
        .expect("put dropped record");
    drop(dropped);

    let mut committed = store.begin_transaction().await.expect("transaction");
    committed
        .put(StateRecord::new(
            committed_key.clone(),
            serde_json::json!({"committed": true}),
        ))
        .await
        .expect("put committed record");
    committed.commit().await.expect("commit");
    drop(store);

    let reopened = tokio::time::timeout(
        Duration::from_secs(2),
        StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::file(path))),
    )
    .await
    .expect("reopen timeout")
    .expect("reopen Redb");
    assert!(
        reopened
            .load(dropped_key)
            .await
            .expect("load dropped")
            .is_none()
    );
    assert!(
        reopened
            .load(committed_key)
            .await
            .expect("load committed")
            .is_some()
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn admitted_redb_transaction_retains_terminal_authority_after_store_drop() {
    let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::memory()))
        .await
        .expect("connect Redb");
    let key = StateKey::new("redb-terminal", "commit").expect("valid key");
    let mut transaction = store.begin_transaction().await.expect("transaction");
    transaction
        .put(StateRecord::new(
            key,
            serde_json::json!({"committed": true}),
        ))
        .await
        .expect("admitted put");
    drop(store);

    tokio::time::timeout(Duration::from_secs(2), transaction.commit())
        .await
        .expect("commit timeout")
        .expect("authoritative commit");
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_file_admitted_transaction_commits_after_store_drop_and_reopens() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("terminal-authority.redb");
        let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::file(&path)))
            .await
            .expect("connect Redb file");
        let other_owner = store.clone();
        let mut transaction = store.begin_transaction().await.expect("transaction");
        for (fragment, value) in [
            ("a", "first"),
            ("b", "second"),
            ("a", "updated"),
            ("c", "third"),
        ] {
            transaction
                .put(StateRecord::new(
                    StateKey::new("redb-terminal-file", fragment).expect("key"),
                    serde_json::json!(value),
                ))
                .await
                .expect("admitted put");
        }
        // A successful mutation proves native admission before every store owner closes.
        drop(other_owner);
        drop(store);
        tokio::time::timeout(Duration::from_secs(2), transaction.commit())
            .await
            .expect("commit timeout")
            .expect("authoritative file commit");

        for opening in 0..2 {
            let store =
                StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::file(&path)))
                    .await
                    .expect("reopen after terminal commit");
            let mut expected = vec![
                (
                    "a",
                    if opening == 0 { 2 } else { 3 },
                    if opening == 0 { "updated" } else { "reopened" },
                ),
                ("b", 1, "second"),
                ("c", 1, "third"),
            ];
            if opening == 1 {
                expected.push(("d", 1, "subsequent"));
            }
            let expected: Vec<_> = expected
                .into_iter()
                .map(|(fragment, revision, value)| {
                    StateRecordSnapshot::new(
                        StateRecord::new(
                            StateKey::new("redb-terminal-file", fragment).expect("key"),
                            serde_json::json!(value),
                        ),
                        StateRevision::new(revision).expect("revision"),
                    )
                })
                .collect();
            for snapshot in &expected {
                assert_eq!(
                    store
                        .load(snapshot.record.key.clone())
                        .await
                        .expect("load committed record")
                        .as_ref(),
                    Some(snapshot),
                );
            }
            let page = store
                .list(StateListRequest::new("redb-terminal-file").expect("namespace"))
                .await
                .expect("committed inventory");
            assert_eq!(page.records, expected);
            assert!(page.next_start_after.is_none());
            if opening == 0 {
                let mut next = store
                    .begin_transaction()
                    .await
                    .expect("subsequent transaction");
                for (fragment, value) in [("a", "reopened"), ("d", "subsequent")] {
                    next.put(StateRecord::new(
                        StateKey::new("redb-terminal-file", fragment).expect("key"),
                        serde_json::json!(value),
                    ))
                    .await
                    .expect("subsequent put");
                }
                next.commit().await.expect("subsequent commit");
            }
            drop(store);
        }
    })
    .await
    .expect("bounded file terminal authority and reopen");
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_repeated_reads_preserve_native_json_depth_errors() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::memory()))
            .await
            .expect("connect Redb");
        let key = StateKey::new("redb-depth", "nested").expect("key");
        let mut value = serde_json::Value::Null;
        for _ in 0..160 {
            value = serde_json::Value::Array(vec![value]);
        }
        let mut write = store.begin_transaction().await.expect("transaction");
        write
            .put(StateRecord::new(key.clone(), value))
            .await
            .expect("encode and stage deep Value");
        write.commit().await.expect("commit encoded Value");
        for _ in 0..2 {
            assert!(matches!(
                store.load(key.clone()).await,
                Err(StateStoreError::Serialization(_)),
            ));
            assert!(matches!(
                store
                    .list(StateListRequest::new("redb-depth").expect("namespace"))
                    .await,
                Err(StateStoreError::Serialization(_)),
            ));
        }
        let mut read = store.begin_transaction().await.expect("read transaction");
        assert!(matches!(
            read.load(key.clone()).await,
            Err(StateStoreError::Serialization(_))
        ));
        read.rollback().await.expect("join failed read transaction");
        for delete in [false, true] {
            let sentinel =
                StateKey::new("redb-depth-sentinel", if delete { "delete" } else { "put" })
                    .expect("sentinel key");
            let mut failed = store
                .begin_transaction()
                .await
                .expect("failing transaction");
            failed
                .put(StateRecord::new(
                    sentinel.clone(),
                    serde_json::json!("uncommitted"),
                ))
                .await
                .expect("stage valid sentinel before failure");
            let error = if delete {
                failed.delete(key.clone()).await
            } else {
                failed
                    .put(StateRecord::new(
                        key.clone(),
                        serde_json::json!("replacement"),
                    ))
                    .await
            }
            .expect_err("mutation must decode the existing deep record");
            assert!(matches!(error, StateStoreError::Serialization(_)));
            assert!(matches!(
                failed
                    .put(StateRecord::new(
                        sentinel.clone(),
                        serde_json::json!("after failure")
                    ))
                    .await,
                Err(StateStoreError::InvalidRequest(_)),
            ));
            assert!(matches!(
                failed.delete(sentinel.clone()).await,
                Err(StateStoreError::InvalidRequest(_)),
            ));
            assert!(matches!(
                failed.commit().await,
                Err(StateStoreError::InvalidRequest(_))
            ));
            assert!(
                store
                    .load(sentinel.clone())
                    .await
                    .expect("failed sentinel load")
                    .is_none()
            );
            let mut fresh = store.begin_transaction().await.expect("fresh transaction");
            fresh
                .put(StateRecord::new(
                    sentinel.clone(),
                    serde_json::json!("committed"),
                ))
                .await
                .expect("fresh sentinel put");
            fresh.commit().await.expect("fresh sentinel commit");
            let snapshot = store
                .load(sentinel.clone())
                .await
                .expect("sentinel load")
                .expect("sentinel");
            assert_eq!(
                snapshot.record,
                StateRecord::new(sentinel, serde_json::json!("committed"))
            );
            assert_eq!(
                snapshot.revision.get(),
                1,
                "failed work reserves no revision"
            );
        }
        let progress_key = StateKey::new("redb-depth", "progress").expect("key");
        let mut progress = store
            .begin_transaction()
            .await
            .expect("progress transaction");
        progress
            .put(StateRecord::new(
                progress_key.clone(),
                serde_json::json!(true),
            ))
            .await
            .expect("progress put");
        progress.commit().await.expect("progress commit");
        assert_eq!(
            store
                .load(progress_key)
                .await
                .expect("progress read")
                .expect("present")
                .record
                .value
                .as_bool(),
            Some(true)
        );
    })
    .await
    .expect("bounded depth-error preservation case");
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_large_valid_values_remain_readable_across_load_and_list() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for payload_bytes in [1024 * 1024, 17 * 1024 * 1024] {
            let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::memory()))
                .await
                .expect("connect Redb");
            let key = StateKey::new("redb-large-value", "payload").expect("key");
            // Storage has no App checkpoint byte limit; this is an ordinary valid JSON string.
            let expected = "x".repeat(payload_bytes);
            let mut write = store.begin_transaction().await.expect("transaction");
            write
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::Value::String(expected.clone()),
                ))
                .await
                .expect("stage large Value");
            write.commit().await.expect("commit large Value");
            let (first, second, transactional) =
                tokio::join!(store.load(key.clone()), store.load(key.clone()), async {
                    let mut read = store.begin_transaction().await.expect("read transaction");
                    let snapshot = read.load(key.clone()).await;
                    read.rollback().await.expect("join read transaction");
                    snapshot
                },);
            for snapshot in [first, second] {
                let snapshot = snapshot.expect("large load").expect("present");
                assert_eq!(snapshot.record.key, key);
                assert_eq!(snapshot.revision.get(), 1);
                assert!(
                    snapshot.record.value.as_str() == Some(expected.as_str()),
                    "full large payload preserved"
                );
            }
            let page = store
                .list(StateListRequest::new("redb-large-value").expect("namespace"))
                .await
                .expect("large list");
            assert_eq!(page.records.len(), 1);
            assert!(page.next_start_after.is_none());
            assert_eq!(page.records[0].record.key, key);
            assert_eq!(page.records[0].revision.get(), 1);
            assert!(
                page.records[0].record.value.as_str() == Some(expected.as_str()),
                "listed large payload preserved"
            );
            let snapshot = transactional
                .expect("transactional large load")
                .expect("present");
            assert_eq!(snapshot.revision.get(), 1);
            assert!(
                snapshot.record.value.as_str() == Some(expected.as_str()),
                "transactional large payload preserved"
            );
        }
    })
    .await
    .expect("bounded large-value preservation case");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_commit_and_rollback_match_the_portable_contract() {
    use storage_kit::state::config::SqliteStateStoreConfig;

    let store = StateStore::connect(StateStoreConfig::sqlite(SqliteStateStoreConfig::memory()))
        .await
        .expect("connect SQLite");
    let key = StateKey::new("agent", "sqlite").expect("valid key");

    let mut transaction = store.begin_transaction().await.expect("transaction");
    transaction
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!({"backend": "sqlite"}),
        ))
        .await
        .expect("put");
    transaction.commit().await.expect("commit");
    assert!(store.load(key.clone()).await.expect("load").is_some());

    let mut transaction = store.begin_transaction().await.expect("transaction");
    transaction.delete(key.clone()).await.expect("delete");
    transaction.rollback().await.expect("rollback");
    assert!(
        store
            .load(key)
            .await
            .expect("load after rollback")
            .is_some()
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_dropped_transaction_cleanup_is_owned_across_reopen() {
    use storage_kit::state::config::SqliteStateStoreConfig;

    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("owned-drop.sqlite");
    let store = StateStore::connect(StateStoreConfig::sqlite(
        SqliteStateStoreConfig::file(path.clone()).with_operation_timeout(Duration::from_secs(2)),
    ))
    .await
    .expect("connect SQLite file");
    let dropped_key = StateKey::new("sqlite-drop", "dropped").expect("valid key");
    let progress_key = StateKey::new("sqlite-drop", "progress").expect("valid key");

    let mut dropped = store.begin_transaction().await.expect("transaction");
    dropped
        .put(StateRecord::new(
            dropped_key.clone(),
            serde_json::json!({"committed": false}),
        ))
        .await
        .expect("put dropped record");
    drop(store);
    drop(dropped);

    let verifier = tokio::time::timeout(
        Duration::from_secs(2),
        StateStore::connect(StateStoreConfig::sqlite(
            SqliteStateStoreConfig::file(path.clone())
                .with_operation_timeout(Duration::from_secs(2)),
        )),
    )
    .await
    .expect("reopen timeout")
    .expect("reopen SQLite");
    let mut progress = verifier.begin_transaction().await.expect("transaction");
    progress
        .put(StateRecord::new(
            progress_key.clone(),
            serde_json::json!({"committed": true}),
        ))
        .await
        .expect("put progress record");
    progress.commit().await.expect("commit progress");
    drop(verifier);

    let reopened =
        StateStore::connect(StateStoreConfig::sqlite(SqliteStateStoreConfig::file(path)))
            .await
            .expect("final reopen");
    assert!(
        reopened
            .load(dropped_key)
            .await
            .expect("load dropped")
            .is_none()
    );
    assert!(
        reopened
            .load(progress_key)
            .await
            .expect("load progress")
            .is_some()
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_transaction_reply_respects_operation_timeout() {
    use storage_kit::state::config::SqliteStateStoreConfig;

    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("reply-timeout.sqlite");
    let bootstrap = StateStore::connect(StateStoreConfig::sqlite(SqliteStateStoreConfig::file(
        path.clone(),
    )))
    .await
    .expect("initialize SQLite schema");
    drop(bootstrap);

    let database = turso::Builder::new_local(path.to_str().expect("UTF-8 test path"))
        .build()
        .await
        .expect("open test-owned Turso database");
    let connection = database.connect().expect("test-owned Turso connection");
    connection
        .execute_batch(
            "CREATE TABLE slow_trigger_digits (value INTEGER NOT NULL);
             INSERT INTO slow_trigger_digits VALUES
                 (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
             CREATE TABLE slow_trigger_rows (value INTEGER NOT NULL);
             INSERT INTO slow_trigger_rows
             SELECT a.value + 10 * b.value + 100 * c.value + 1000 * d.value
             FROM slow_trigger_digits AS a
             CROSS JOIN slow_trigger_digits AS b
             CROSS JOIN slow_trigger_digits AS c
             CROSS JOIN slow_trigger_digits AS d;
             CREATE TRIGGER slow_state_put
             BEFORE INSERT ON state_records
             BEGIN
                 SELECT sum(value) FROM slow_trigger_rows;
             END;",
        )
        .await
        .expect("install slow insert trigger");
    drop(connection);
    drop(database);

    let store = StateStore::connect(StateStoreConfig::sqlite(
        SqliteStateStoreConfig::file(path.clone()).with_operation_timeout(Duration::from_millis(1)),
    ))
    .await
    .expect("connect timeout-configured SQLite store");
    let key = StateKey::new("sqlite-timeout", "timed-out-put").expect("valid key");
    let mut transaction = store.begin_transaction().await.expect("transaction");
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        transaction.put(StateRecord::new(
            key.clone(),
            serde_json::json!({"persisted": false}),
        )),
    )
    .await
    .expect("Kit operation timeout must fire before the outer guard")
    .expect_err("slow put must time out");
    assert_eq!(
        error,
        StateStoreError::TimedOut {
            operation: "put".to_owned(),
            timeout_ms: 1,
        }
    );
    tokio::time::timeout(Duration::from_secs(2), transaction.rollback())
        .await
        .expect("rollback cleanup must settle within the outer guard")
        .expect("rollback joins actor-owned cleanup");
    drop(store);
    let verifier = StateStore::connect(StateStoreConfig::sqlite(
        SqliteStateStoreConfig::file(path).with_operation_timeout(Duration::from_secs(2)),
    ))
    .await
    .expect("reopen SQLite after cleanup");
    assert!(
        verifier
            .load(key)
            .await
            .expect("load after timed-out transaction")
            .is_none()
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_previous_release_file_preserves_records_revisions_and_tombstones() {
    let directory = tempdir().expect("upgrade copy directory");
    let path = directory.path().join("upgrade.redb");
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/redb-4.1.0-state.redb"),
        &path,
    )
    .expect("copy frozen Redb 4.1 fixture without mutating its source");
    let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::file(&path)))
        .await
        .expect("open previous stable file");
    let key = StateKey::new("vendor-upgrade", "records/a").expect("key");
    let snapshot = store.load(key).await.expect("load").expect("record");
    assert_eq!(snapshot.revision.get(), 4);
    assert_eq!(
        snapshot.record.value.as_ref(),
        &serde_json::json!({"text": "preserved", "n": 42})
    );
    for fragment in [
        "records/deleted",
        "records/absent-delete",
        "rolled-back",
        "dropped",
    ] {
        assert!(
            store
                .load(StateKey::new("vendor-upgrade", fragment).expect("key"))
                .await
                .expect("load")
                .is_none()
        );
    }
    let page = store
        .list(
            StateListRequest::new("vendor-upgrade")
                .expect("namespace")
                .with_key_prefix(StateKeyPrefix::new("records/").expect("prefix"))
                .with_limit(1)
                .expect("limit"),
        )
        .await
        .expect("first page");
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].record.key.key(), "records/a");
    let cursor = page.next_start_after.expect("second page exists");
    let page = store
        .list(
            StateListRequest::new("vendor-upgrade")
                .expect("namespace")
                .with_key_prefix(StateKeyPrefix::new("records/").expect("prefix"))
                .start_after(cursor)
                .expect("cursor")
                .with_limit(1)
                .expect("limit"),
        )
        .await
        .expect("second page");
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].record.key.key(), "records/b");
    assert!(page.next_start_after.is_none());
    let mut transaction = store.begin_transaction().await.expect("transaction");
    for (fragment, expected) in [("records/deleted", 3), ("records/absent-delete", 2)] {
        let key = StateKey::new("vendor-upgrade", fragment).expect("key");
        transaction
            .put(StateRecord::new(
                key.clone(),
                serde_json::json!("recreated"),
            ))
            .await
            .expect("recreate tombstone");
        assert_eq!(
            transaction
                .load(key)
                .await
                .expect("load")
                .expect("record")
                .revision
                .get(),
            expected
        );
    }
    transaction.commit().await.expect("commit upgraded file");
    drop(store);
    let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::file(path)))
        .await
        .expect("reopen upgraded file after writing");
    for (fragment, revision, value) in [
        (
            "records/a",
            4,
            serde_json::json!({"text": "preserved", "n": 42}),
        ),
        ("records/deleted", 3, serde_json::json!("recreated")),
        ("records/absent-delete", 2, serde_json::json!("recreated")),
    ] {
        let snapshot = store
            .load(StateKey::new("vendor-upgrade", fragment).expect("key"))
            .await
            .expect("load upgraded record")
            .expect("persisted record");
        assert_eq!(snapshot.revision.get(), revision);
        assert_eq!(snapshot.record.value.as_ref(), &value);
    }
}
