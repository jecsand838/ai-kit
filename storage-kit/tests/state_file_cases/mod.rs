use storage_kit::{
    StateStore, StateStoreBackend, StateTransactionBackend, state::config::StateStoreConfig,
    state::record::StateRecord,
};

use crate::state_cases::StateCases;

// The file lifecycle belongs to the existing StateCases owner, but only targets
// that exercise file reopen load this implementation.
impl StateCases<StateStore> {
    async fn connect_file(config: StateStoreConfig) -> StateStore {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            StateStore::connect(config),
        )
        .await
        .expect("bounded file StateStore connection")
        .expect("file StateStore connection")
    }

    pub(super) async fn verify_file(config: StateStoreConfig) {
        let store = Self::connect_file(config.clone()).await;
        let cases = Self::new(store.clone());
        cases.verify().await;
        let rolled_back = cases.key("reopen/rolled-back");
        let dropped = cases.key("reopen/dropped");
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("rollback transaction");
        transaction
            .put(StateRecord::new(
                rolled_back.clone(),
                serde_json::json!(true),
            ))
            .await
            .expect("stage rollback");
        transaction.rollback().await.expect("rollback");
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("dropped transaction");
        transaction
            .put(StateRecord::new(dropped.clone(), serde_json::json!(true)))
            .await
            .expect("stage dropped write");
        drop(transaction);
        let revisions =
            ["revision", "revision-large"].map(|fragment| (fragment, cases.key(fragment)));
        let published = ["history/0001", "head", "receipt"].map(|fragment| cases.key(fragment));
        let tombstone = cases.key("item/tombstone");
        // No retained store or transaction may keep the native file open.
        drop(cases);
        drop(store);

        let store = Self::connect_file(config.clone()).await;
        for (fragment, key) in revisions {
            let snapshot = store
                .load(key)
                .await
                .expect("load persisted revision")
                .expect("record");
            assert_eq!(snapshot.revision.get(), 6);
            if fragment == "revision" {
                assert_eq!(snapshot.record.value.as_ref(), &serde_json::json!(5));
            } else {
                assert_eq!(
                    snapshot.record.value.as_ref(),
                    &serde_json::json!({"n": 5, "padding": "x".repeat(2048)})
                );
            }
        }
        for key in [rolled_back, dropped, tombstone.clone()] {
            assert!(
                store
                    .load(key)
                    .await
                    .expect("absent after reopen")
                    .is_none()
            );
        }
        for key in published {
            let snapshot = store
                .load(key)
                .await
                .expect("published record")
                .expect("record");
            assert_eq!(snapshot.revision.get(), 1);
            assert_eq!(
                snapshot.record.value.as_ref(),
                &serde_json::json!({"sequence": 1})
            );
        }
        let mut transaction = store.begin_transaction().await.expect("recreate tombstone");
        transaction
            .put(StateRecord::new(
                tombstone.clone(),
                serde_json::json!("recreated"),
            ))
            .await
            .expect("recreate");
        assert_eq!(
            transaction
                .load(tombstone.clone())
                .await
                .expect("load")
                .expect("record")
                .revision
                .get(),
            3
        );
        transaction
            .commit()
            .await
            .expect("commit recreated tombstone");
        drop(store);
        let store = Self::connect_file(config).await;
        let snapshot = store.load(tombstone).await.expect("load").expect("record");
        assert_eq!(snapshot.revision.get(), 3);
        assert_eq!(
            snapshot.record.value.as_ref(),
            &serde_json::json!("recreated")
        );
    }
}
