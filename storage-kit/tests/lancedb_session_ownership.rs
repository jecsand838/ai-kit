//! Native ownership evidence for the supplied Database's separate sessions.
//!
//! The shared-session case records the pinned engine's ownership cycle as a
//! counterexample. Public Database behavior is tested in `database_backend`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::session::Session;
use lancedb::connection::Connection;
use lancedb::database::namespace::LanceNamespaceDatabase;
use lancedb::embeddings::MemoryRegistry;
use lancedb::index::{Index, scalar::BTreeIndexBuilder};
use lancedb::query::{ExecutableQuery, QueryBase};

enum TablePlacement {
    NoTable,
    Root,
    Namespace,
    IndexedNamespace,
}

struct SessionRetentionProbe;

impl SessionRetentionProbe {
    fn single_session(placement: TablePlacement) -> usize {
        let directory = tempfile::tempdir().expect("owned native directory");
        let weak = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("owned native runtime");
            runtime.block_on(async {
                let session = Arc::new(Session::default());
                let weak = Arc::downgrade(&session);
                let connection =
                    lancedb::connect(directory.path().to_str().expect("UTF-8 fixture path"))
                        .session(session.clone())
                        .execute()
                        .await
                        .expect("native directory connection");

                Self::exercise(&connection, placement).await;
                drop(connection);
                drop(session);
                weak
            })
            // Runtime destruction precedes the sole Weak-only observation.
        };
        Self::observe_and_release(vec![("shared session", weak)])[0].1
    }

    fn separate_sessions() {
        let directory = tempfile::tempdir().expect("owned native directory");
        let weak_sessions = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("owned native runtime");
            runtime.block_on(async {
                // Each default Session owns a distinct native registry. Only
                // filesystem bytes are common; no registry is shared here.
                let namespace_session = Arc::new(Session::default());
                let table_session = Arc::new(Session::default());
                let weak_namespace = Arc::downgrade(&namespace_session);
                let weak_table = Arc::downgrade(&table_session);
                let bootstrap =
                    lancedb::connect(directory.path().to_str().expect("UTF-8 fixture path"))
                        .session(namespace_session.clone())
                        .execute()
                        .await
                        .expect("native namespace connection");
                let namespace = bootstrap
                    .namespace_client()
                    .await
                    .expect("native directory namespace client");
                let (implementation, properties) = bootstrap
                    .namespace_client_config()
                    .await
                    .expect("native namespace configuration");
                let database = LanceNamespaceDatabase::from_namespace_client(
                    namespace,
                    implementation,
                    properties,
                    HashMap::new(),
                    None,
                    Some(table_session.clone()),
                    HashSet::new(),
                );
                let connection =
                    Connection::new(Arc::new(database), Arc::new(MemoryRegistry::new()));
                drop(bootstrap);

                Self::exercise(&connection, TablePlacement::IndexedNamespace).await;
                drop(connection);
                drop(table_session);
                drop(namespace_session);
                vec![
                    ("namespace session", weak_namespace),
                    ("table session", weak_table),
                ]
            })
        };
        let retained = Self::observe_and_release(weak_sessions);
        assert!(
            retained.iter().all(|(_, count)| *count == 0),
            "separate native sessions survived all explicit owners: {retained:?}",
        );
    }

    async fn exercise(connection: &Connection, placement: TablePlacement) {
        let namespace = match placement {
            TablePlacement::NoTable => {
                // Exercise an actual supported operation even in the control.
                let names = connection
                    .table_names()
                    .execute()
                    .await
                    .expect("list empty native catalog");
                assert!(names.is_empty());
                return;
            }
            TablePlacement::Root => Vec::new(),
            TablePlacement::Namespace | TablePlacement::IndexedNamespace => {
                vec!["probe".to_owned()]
            }
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let table = connection
            .create_empty_table("ownership_probe", schema.clone())
            .namespace(namespace.clone())
            .execute()
            .await
            .expect("create native table");
        assert_eq!(
            table.count_rows(None).await.expect("count created table"),
            0
        );
        if matches!(placement, TablePlacement::IndexedNamespace) {
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from_iter_values(0..1024))],
            )
            .expect("populated ownership fixture");
            table
                .add(batch)
                .execute()
                .await
                .expect("populate native table");
            table
                .create_index(&["value"], Index::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
                .expect("create native scalar index");
        }
        drop(table);

        let reopened = connection
            .open_table("ownership_probe")
            .namespace(namespace)
            .execute()
            .await
            .expect("reopen native table");
        assert_eq!(
            reopened
                .count_rows(None)
                .await
                .expect("count reopened table"),
            if matches!(placement, TablePlacement::IndexedNamespace) {
                1024
            } else {
                0
            },
        );
        if matches!(placement, TablePlacement::IndexedNamespace) {
            let query = reopened.query().only_if("value = 513");
            let plan = query
                .explain_plan(false)
                .await
                .expect("explain indexed query");
            assert!(plan.contains("ScalarIndexQuery"), "{plan}");
            let batches = query
                .execute()
                .await
                .expect("start indexed query")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("drain indexed results");
            let values = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("Int32 result")
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, vec![513]);
        }
        drop(reopened);
    }

    fn observe_and_release(
        sessions: Vec<(&'static str, Weak<Session>)>,
    ) -> Vec<(&'static str, usize)> {
        // Do not retain a registry or upgrade a Weak before capturing the
        // terminal observation: either would contaminate ownership evidence.
        let retained = sessions
            .iter()
            .map(|(label, weak)| (*label, weak.strong_count()))
            .collect::<Vec<_>>();

        // Diagnostic cleanup only, after preserving the assertion evidence.
        // A sweep may release expired keys; cached stores may prevent that.
        // Neither outcome is substituted for the unmodified drop result.
        for (_, weak) in &sessions {
            if let Some(session) = weak.upgrade() {
                drop(session.store_registry().active_stores());
            }
        }
        let after_expired_key_sweep = sessions
            .iter()
            .map(|(label, weak)| (*label, weak.strong_count()))
            .collect::<Vec<_>>();
        eprintln!(
            "native session references after owner/runtime drop: {retained:?}; \
             after diagnostic expired-key sweep: {after_expired_key_sweep:?}",
        );
        assert!(
            after_expired_key_sweep.iter().all(|(_, count)| *count == 0),
            "native diagnostic cleanup retained sessions: {after_expired_key_sweep:?}",
        );
        retained
    }
}

#[test]
fn directory_catalog_only_releases_native_session() {
    assert_eq!(
        SessionRetentionProbe::single_session(TablePlacement::NoTable),
        0
    );
}

#[test]
fn directory_root_table_releases_native_session() {
    assert_eq!(
        SessionRetentionProbe::single_session(TablePlacement::Root),
        0
    );
}

#[test]
fn native_shared_session_retains_namespaced_metadata_until_explicit_cleanup() {
    assert!(SessionRetentionProbe::single_session(TablePlacement::Namespace) > 0);
}

#[test]
fn directory_namespaced_table_releases_separate_native_sessions() {
    SessionRetentionProbe::separate_sessions();
}
