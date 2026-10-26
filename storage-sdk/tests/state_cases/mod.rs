use storage_sdk::{
    StateStoreBackend, StateTransactionBackend,
    state::record::{StateKey, StateKeyPrefix, StateListRequest, StateRecord},
};

// Private cases shared by real embedded and PostgreSQL tests in this crate.
// This owner does not emulate storage or expose a production conformance API.
pub(super) struct StateCases<S: StateStoreBackend> {
    store: S,
    namespace: String,
}

impl<S: StateStoreBackend> StateCases<S> {
    pub(super) fn new(store: S) -> Self {
        Self {
            store,
            namespace: format!("portable-{}", uuid::Uuid::new_v4()),
        }
    }

    pub(super) fn key(&self, fragment: &str) -> StateKey {
        StateKey::new(&self.namespace, fragment).expect("key")
    }

    pub(super) async fn verify(&self) {
        self.revisions("revision", [1, 2, 3, 4, 5].map(serde_json::Value::from))
            .await;
        let padding = "x".repeat(2048);
        self.revisions(
            "revision-large",
            [1, 2, 3, 4, 5].map(|n| serde_json::json!({"n": n, "padding": &padding})),
        )
        .await;
        self.atomic_publication().await;
        self.pagination().await;
    }

    async fn revisions(&self, fragment: &str, values: [serde_json::Value; 5]) {
        let key = self.key(fragment);
        let mut transaction = self.store.begin_transaction().await.expect("transaction");
        assert!(
            transaction
                .load(key.clone())
                .await
                .expect("missing")
                .is_none()
        );
        transaction
            .delete(key.clone())
            .await
            .expect("delete absent at revision one");
        transaction
            .put(StateRecord::new(key.clone(), values[0].clone()))
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
            .put(StateRecord::new(key.clone(), values[1].clone()))
            .await
            .expect("second put");
        let before_delete = transaction
            .load(key.clone())
            .await
            .expect("load")
            .expect("value");
        assert_eq!(before_delete.revision.get(), 3);
        assert_eq!(before_delete.record.value.as_ref(), &values[1]);
        transaction
            .delete(key.clone())
            .await
            .expect("delete at revision four");
        assert!(
            transaction
                .load(key.clone())
                .await
                .expect("tombstone")
                .is_none()
        );
        transaction.commit().await.expect("commit tombstone");
        assert!(
            self.store
                .load(key.clone())
                .await
                .expect("deleted")
                .is_none()
        );
        let mut recreate = self.store.begin_transaction().await.expect("recreate");
        recreate
            .put(StateRecord::new(key.clone(), values[2].clone()))
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
        recreate.commit().await.expect("commit recreate");
        let committed = self.store.load(key.clone()).await.expect("load");
        let retained_committed = committed.clone().expect("committed recreation");
        assert_eq!(retained_committed.revision.get(), 5);
        assert_eq!(retained_committed.record.value.as_ref(), &values[2]);
        assert_eq!(before_delete.revision.get(), 3);
        assert_eq!(before_delete.record.value.as_ref(), &values[1]);
        let mut rollback = self.store.begin_transaction().await.expect("rollback");
        rollback
            .put(StateRecord::new(key.clone(), values[3].clone()))
            .await
            .expect("staged put");
        let retained_staged = rollback
            .load(key.clone())
            .await
            .expect("read staged replacement")
            .expect("staged replacement");
        assert_eq!(retained_staged.revision.get(), 6);
        assert_eq!(retained_staged.record.value.as_ref(), &values[3]);
        rollback.rollback().await.expect("rollback");
        assert_eq!(self.store.load(key.clone()).await.expect("load"), committed);
        assert_eq!(retained_committed.record.value.as_ref(), &values[2]);
        assert_eq!(retained_staged.record.value.as_ref(), &values[3]);
        let mut dropped = self.store.begin_transaction().await.expect("dropped");
        dropped.delete(key.clone()).await.expect("dropped delete");
        drop(dropped);
        // A subsequent write also proves actor/native-writer progress after drop.
        let mut next = self.store.begin_transaction().await.expect("next");
        next.put(StateRecord::new(key.clone(), values[4].clone()))
            .await
            .expect("next put");
        next.commit().await.expect("next commit");
        let next_committed = self.store.load(key).await.expect("load").expect("value");
        assert_eq!(next_committed.revision.get(), 6);
        assert_eq!(next_committed.record.value.as_ref(), &values[4]);
        // A rolled-back staged revision may recur with different committed content.
        assert_eq!(retained_staged.revision, next_committed.revision);
        assert_eq!(retained_staged.record.value.as_ref(), &values[3]);
        assert_eq!(retained_committed.revision.get(), 5);
        assert_eq!(retained_committed.record.value.as_ref(), &values[2]);
        assert_eq!(before_delete.revision.get(), 3);
        assert_eq!(before_delete.record.value.as_ref(), &values[1]);
    }

    async fn atomic_publication(&self) {
        let keys = [
            self.key("history/0001"),
            self.key("head"),
            self.key("receipt"),
        ];
        let mut transaction = self.store.begin_transaction().await.expect("transaction");
        for key in &keys {
            assert!(
                transaction
                    .load(key.clone())
                    .await
                    .expect("absence fence")
                    .is_none()
            );
            transaction
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::json!({"sequence": 1}),
                ))
                .await
                .expect("staged publication");
            assert!(
                self.store
                    .load(key.clone())
                    .await
                    .expect("uncommitted read")
                    .is_none()
            );
        }
        transaction.commit().await.expect("atomic publication");
        for key in &keys {
            let record = self
                .store
                .load(key.clone())
                .await
                .expect("load")
                .expect("committed");
            assert_eq!(record.revision.get(), 1);
            assert_eq!(
                record.record.value.as_ref(),
                &serde_json::json!({"sequence": 1})
            );
        }
        let mut rollback = self.store.begin_transaction().await.expect("rollback");
        for key in &keys {
            rollback.delete(key.clone()).await.expect("delete");
        }
        rollback.rollback().await.expect("rollback batch");
        for key in &keys {
            assert!(self.store.load(key.clone()).await.expect("load").is_some());
        }
    }

    pub(super) async fn pagination(&self) {
        let expected = [
            "item/A",
            "item/Z",
            "item/a",
            "item/a%_\\1",
            "item/a%_\\2",
            "item/é",
            "item/中",
        ];
        let tombstones = ["item/0", "item/a%_\\1x", "item/tombstone", "item/中tail"];
        let mut transaction = self.store.begin_transaction().await.expect("seed");
        for fragment in expected
            .iter()
            .rev()
            .chain(["ITEM/false", "item/aXX1"].iter())
            .chain(tombstones.iter())
        {
            transaction
                .put(StateRecord::new(
                    self.key(fragment),
                    serde_json::json!(fragment),
                ))
                .await
                .expect("seed page record");
        }
        for fragment in tombstones {
            transaction
                .delete(self.key(fragment))
                .await
                .expect("delete tombstone");
        }
        transaction
            .put(StateRecord::new(
                StateKey::new(format!("{}-neighbor", self.namespace), "item/A").expect("neighbor"),
                serde_json::json!("other namespace"),
            ))
            .await
            .expect("neighbor namespace");
        transaction.commit().await.expect("seed commit");
        let request = StateListRequest::new(&self.namespace)
            .expect("namespace")
            .with_key_prefix(StateKeyPrefix::new("item/").expect("prefix"))
            .with_limit(2)
            .expect("page limit");
        let mut sorted = expected.to_vec();
        sorted.push("item/aXX1");
        sorted.sort_unstable();
        let mut next = request.clone();
        for (index, expected_page) in sorted.chunks(2).enumerate() {
            let page = self.store.list(next).await.expect("ordered page");
            assert_eq!(
                page.records
                    .iter()
                    .map(|snapshot| snapshot.record.key.key())
                    .collect::<Vec<_>>(),
                expected_page,
                "each nonempty page advances in canonical UTF-8 order"
            );
            for snapshot in &page.records {
                assert_eq!(snapshot.record.key.namespace().as_str(), self.namespace);
                assert_eq!(snapshot.revision.get(), 1);
            }
            let expected_cursor = if (index + 1) * 2 < sorted.len() {
                expected_page.last().copied()
            } else {
                None
            };
            assert_eq!(
                page.next_start_after.as_deref(),
                expected_cursor,
                "only pages with more records carry their final key as a cursor"
            );
            next = match page.next_start_after {
                Some(cursor) => request
                    .clone()
                    .start_after(cursor)
                    .expect("exclusive cursor"),
                None => request.clone(),
            };
        }
        let literal = self
            .store
            .list(
                StateListRequest::new(&self.namespace)
                    .expect("namespace")
                    .with_key_prefix(StateKeyPrefix::new("item/a%_\\").expect("literal prefix")),
            )
            .await
            .expect("literal wildcard page");
        assert_eq!(
            literal
                .records
                .iter()
                .map(|record| record.record.key.key())
                .collect::<Vec<_>>(),
            ["item/a%_\\1", "item/a%_\\2"]
        );
        assert!(literal.next_start_after.is_none());
        for (prefix, cursor, limit, expected_keys, expected_cursor) in [
            (
                Some("item/"),
                "ITEM/false",
                2,
                ["item/A", "item/Z"].as_slice(),
                Some("item/Z"),
            ),
            (
                Some("item/"),
                "item/",
                2,
                ["item/A", "item/Z"].as_slice(),
                Some("item/Z"),
            ),
            (Some("item/A"), "item/A", 1, [].as_slice(), None),
            (
                Some("item/"),
                "item/B",
                2,
                ["item/Z", "item/a"].as_slice(),
                Some("item/a"),
            ),
            (
                Some("item/a%_\\"),
                "item/a",
                1,
                ["item/a%_\\1"].as_slice(),
                Some("item/a%_\\1"),
            ),
            (
                Some("item/a%_\\"),
                "item/a%_\\1",
                1,
                ["item/a%_\\2"].as_slice(),
                None,
            ),
            (
                Some("item/a%_\\"),
                "item/a%_\\1x",
                StateListRequest::MAX_LIMIT,
                ["item/a%_\\2"].as_slice(),
                None,
            ),
            (
                Some("item/"),
                "item/tombstone",
                2,
                ["item/é", "item/中"].as_slice(),
                None,
            ),
            (Some("item/"), "item0", 2, [].as_slice(), None),
            (Some("item/"), "zzz", 2, [].as_slice(), None),
            (Some("item/é"), "item/ê", 2, [].as_slice(), None),
            (None, "item/Z", 1, ["item/a"].as_slice(), Some("item/a")),
        ] {
            let mut request = StateListRequest::new(&self.namespace)
                .expect("namespace")
                .with_limit(limit)
                .expect("limit")
                .start_after(cursor)
                .expect("exclusive cursor");
            if let Some(prefix) = prefix {
                request = request.with_key_prefix(StateKeyPrefix::new(prefix).expect("prefix"));
            }
            let page = self
                .store
                .list(request)
                .await
                .expect("cursor and prefix intersection");
            assert_eq!(
                page.records
                    .iter()
                    .map(|row| row.record.key.key())
                    .collect::<Vec<_>>(),
                expected_keys,
                "prefix {prefix:?}, cursor {cursor:?}, limit {limit}"
            );
            assert_eq!(page.next_start_after.as_deref(), expected_cursor);
            for row in &page.records {
                assert_eq!(row.record.key.namespace().as_str(), self.namespace);
                assert_eq!(row.revision.get(), 1);
                assert_eq!(
                    row.record.value.as_ref(),
                    &serde_json::json!(row.record.key.key())
                );
            }
        }
        let empty = self
            .store
            .list(request.start_after("item/中").expect("end cursor"))
            .await
            .expect("empty tail");
        assert!(empty.records.is_empty());
        assert!(empty.next_start_after.is_none());
    }
}
