mod database_cases;
#[cfg(feature = "postgres")]
mod postgres_cleanup;
#[cfg(any(feature = "postgres", feature = "redb", feature = "sqlite"))]
mod state_cases;
#[cfg(any(feature = "redb", feature = "sqlite"))]
mod state_file_cases;

use std::{collections::BTreeMap, path::PathBuf};

use futures::FutureExt;
use storage_kit::{
    Database, DatabaseBackend,
    database::{
        StorageLocator,
        catalog::{
            CatalogRequest, CatalogResponse, CollectionSchema, CreateCollectionMode,
            CreateCollectionRequest, FieldSchema, FieldType,
        },
        config::{DatabaseConfig, LanceConfig},
        data::{CountRequest, DataRecord, DataRequest, DataResponse, DenseVector, ScanRequest},
    },
};

#[cfg(any(feature = "postgres", feature = "surrealdb"))]
use storage_kit::database::config::Password;
#[cfg(feature = "surrealdb")]
use storage_kit::database::{
    catalog::{AlterColumnsRequest, ColumnAlterationSpec, SchemaEvolutionRequest},
    config::{SurrealConfig, SurrealModeConfig},
};
#[cfg(any(feature = "postgres", feature = "redb", feature = "sqlite"))]
use storage_kit::{StateStore, state::config::StateStoreConfig};
#[cfg(feature = "postgres")]
use storage_kit::{
    StateStoreBackend, StateTransactionBackend,
    state::{
        config::PostgresStateStoreConfig,
        record::{StateKey, StateRecord},
    },
};

async fn assert_database_round_trip(config: DatabaseConfig, requires_native_id: bool) {
    let database = Database::connect(config)
        .await
        .expect("strict-live database connection");
    let run_id = uuid::Uuid::new_v4();
    let locator = StorageLocator::new(format!("strict_live_{}", run_id.simple()));
    let record = if requires_native_id {
        DataRecord::new(run_id)
    } else {
        DataRecord::without_id()
    }
    .vector("embedding", DenseVector::F32(vec![0.25, 0.5, 0.75]))
    .json_field("strict_live", serde_json::json!(true));

    let created = database
        .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
            locator: locator.clone(),
            schema: None,
            records: vec![record],
            mode: CreateCollectionMode::Create,
        }))
        .await
        .expect("strict-live create collection");
    assert!(matches!(created, CatalogResponse::Collection(_)));

    let count = database
        .data(DataRequest::Count(CountRequest {
            locator: locator.clone(),
            filter: None,
        }))
        .await
        .expect("strict-live count seeded record");
    assert_eq!(count, DataResponse::Count(1));

    let dropped = database
        .catalog(CatalogRequest::DropCollection {
            locator,
            ignore_missing: false,
        })
        .await
        .expect("strict-live drop collection");
    assert_eq!(dropped, CatalogResponse::Completed);
}

#[cfg(feature = "surrealdb")]
fn authenticated_surreal(mode: SurrealModeConfig) -> SurrealConfig {
    SurrealConfig {
        mode,
        namespace: Some(std::env::var("TEST_SURREALDB_NS").expect("TEST_SURREALDB_NS")),
        database: Some(std::env::var("TEST_SURREALDB_DB").expect("TEST_SURREALDB_DB")),
        username: Some(
            std::env::var("TEST_SURREALDB_ROOT_USER").expect("TEST_SURREALDB_ROOT_USER"),
        ),
        password: Some(
            Password::from_string(
                std::env::var("TEST_SURREALDB_ROOT_PASS").expect("TEST_SURREALDB_ROOT_PASS"),
            )
            .expect("valid SurrealDB password"),
        ),
        ..SurrealConfig::default()
    }
}

#[tokio::test]
async fn lancedb_embedded_is_live() {
    let path = PathBuf::from(
        std::env::var("TEST_LANCEDB_EMBEDDED_PATH").expect("TEST_LANCEDB_EMBEDDED_PATH"),
    );
    assert_database_round_trip(
        DatabaseConfig::lance(LanceConfig::embedded_path(path)),
        false,
    )
    .await;
}

#[tokio::test]
async fn lancedb_create_if_missing_reuses_concurrent_creations() {
    use std::{sync::Arc, time::Duration};

    let directory = tempfile::tempdir().expect("real isolated Lance directory");
    let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
        directory.path(),
    )))
    .await
    .expect("real Lance connection");
    let mut failures = Vec::new();
    for namespaced in [true, false] {
        for seeded in [false, true] {
            let mut locator = StorageLocator::new(format!("concurrent_{}_{}", namespaced, seeded));
            if namespaced {
                locator = locator
                    .with_namespace(["shared"])
                    .with_database("knowledge");
            }
            let schema = CollectionSchema {
                fields: vec![FieldSchema {
                    name: "winner".to_owned(),
                    field_type: FieldType::String,
                    nullable: false,
                    metadata: BTreeMap::new(),
                }],
                metadata: BTreeMap::new(),
            };
            let barrier = Arc::new(tokio::sync::Barrier::new(8));
            let mut tasks = tokio::task::JoinSet::new();
            for index in 0..8 {
                let database = database.clone();
                let barrier = Arc::clone(&barrier);
                let request = CreateCollectionRequest {
                    locator: locator.clone(),
                    schema: Some(schema.clone()),
                    records: if seeded {
                        vec![
                            DataRecord::without_id()
                                .json_field("winner", serde_json::json!(index.to_string())),
                        ]
                    } else {
                        Vec::new()
                    },
                    mode: CreateCollectionMode::CreateIfMissing,
                };
                tasks.spawn(async move {
                    barrier.wait().await;
                    database
                        .catalog(CatalogRequest::CreateCollection(request))
                        .await
                });
            }
            let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(20),
                async {
                    let mut results = Vec::new();
                    while let Some(result) = tasks.join_next().await {
                        results.push(result.expect("every creator task joins"));
                    }
                    assert_eq!(results.len(), 8);
                    assert!(
                        results.iter().all(Result::is_ok),
                        "all concurrent CreateIfMissing callers must reuse the winner: {results:?}"
                    );
                    let opened = database
                        .catalog(CatalogRequest::OpenCollection {
                            locator: locator.clone(),
                        })
                        .await
                        .expect("winning collection remains openable");
                    assert!(
                        results
                            .iter()
                            .all(|result| result.as_ref().is_ok_and(|value| value == &opened)),
                        "every creator returns the actual winning collection schema and count"
                    );
                    let before = database
                        .data(DataRequest::Scan(ScanRequest::new(locator.clone())))
                        .await
                        .expect("winning records are readable");
                    let DataResponse::Records(page) = &before else {
                        panic!("scan returns actual records");
                    };
                    assert_eq!(page.records.len(), usize::from(seeded));
                    if seeded {
                        assert!((0..8).any(|index| page.records[0].fields["winner"]
                            == serde_json::json!(index.to_string())));
                    }
                    // Reuse ignores new seed data/schema; it must never append,
                    // overwrite, or reject the already existing collection.
                    let ignored = CreateCollectionRequest {
                        locator: locator.clone(),
                        schema: None,
                        records: vec![
                            DataRecord::without_id()
                                .json_field("different", serde_json::json!(true)),
                        ],
                        mode: CreateCollectionMode::CreateIfMissing,
                    };
                    assert_eq!(
                        database
                            .catalog(CatalogRequest::CreateCollection(ignored.clone()))
                            .await
                            .expect("reuse preserves the original collection"),
                        opened
                    );
                    assert_eq!(
                        database
                            .data(DataRequest::Scan(ScanRequest::new(locator.clone())))
                            .await
                            .expect("reused collection remains readable"),
                        before
                    );
                    assert!(
                        database
                            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                                mode: CreateCollectionMode::Create,
                                ..ignored
                            }))
                            .await
                            .is_err(),
                        "strict Create still rejects the existing collection"
                    );
                },
            ))
            .catch_unwind()
            .await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            let cleanup = database
                .catalog(CatalogRequest::DropCollection {
                    locator,
                    ignore_missing: true,
                })
                .await;
            if !matches!(observed, Ok(Ok(()))) {
                failures.push((namespaced, seeded));
            }
            cleanup.expect("release the real test-owned collection after every outcome");
        }
    }
    drop(database);
    directory
        .close()
        .expect("release the isolated Lance directory");
    assert!(
        failures.is_empty(),
        "failed create/reuse cases: {failures:?}"
    );
}

#[tokio::test]
async fn lancedb_create_if_missing_preserves_native_failure_without_a_collection() {
    use std::time::Duration;

    let directory = tempfile::tempdir().expect("real isolated Lance directory");
    let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
        directory.path(),
    )))
    .await
    .expect("real Lance connection");
    let mut outcomes = Vec::new();
    for seeded in [false, true] {
        let locator = StorageLocator::new(format!("native_failure_{seeded}"));
        // Lance 13 rejects its reserved _rowid field during native write
        // validation. Portable schema conversion accepts this request, so the
        // creation error reaches the same owner as a native creation conflict.
        let created = tokio::time::timeout(
            Duration::from_secs(10),
            database.catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: locator.clone(),
                schema: Some(CollectionSchema {
                    fields: vec![FieldSchema {
                        name: "_rowid".to_owned(),
                        field_type: FieldType::String,
                        nullable: false,
                        metadata: BTreeMap::new(),
                    }],
                    metadata: BTreeMap::new(),
                }),
                records: if seeded {
                    vec![
                        DataRecord::without_id().json_field("_rowid", serde_json::json!("invalid")),
                    ]
                } else {
                    Vec::new()
                },
                mode: CreateCollectionMode::CreateIfMissing,
            })),
        )
        .await;
        let opened = tokio::time::timeout(
            Duration::from_secs(10),
            database.catalog(CatalogRequest::OpenCollection { locator }),
        )
        .await;
        outcomes.push((seeded, created, opened));
    }
    drop(database);
    let cleanup = directory.close();
    for (seeded, created, opened) in outcomes {
        let error = created
            .expect("native invalid-schema creation is bounded")
            .expect_err("native reserved field must fail creation");
        let storage_kit::StorageError::BackendOperation { operation, message } = error else {
            panic!("the error must originate from native creation, not portable validation");
        };
        assert_eq!(
            operation,
            if seeded {
                "create_table"
            } else {
                "create_empty_table"
            }
        );
        assert!(
            message.contains("_rowid") && message.contains("reserved"),
            "the original native creation error survives: {message}"
        );
        assert!(
            opened
                .expect("missing collection observation is bounded")
                .is_err(),
            "failed creation cannot manufacture a collection"
        );
    }
    cleanup.expect("release the isolated failed-creation directory");
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
async fn surrealdb_memory_is_live() {
    assert_database_round_trip(DatabaseConfig::surreal(SurrealConfig::default()), false).await;
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
async fn surrealdb_combined_rename_and_cast_is_live() {
    let database = Database::connect(DatabaseConfig::surreal(SurrealConfig::default()))
        .await
        .expect("SurrealDB memory connection");
    let locator = StorageLocator::new(format!("surreal_alter_{}", uuid::Uuid::new_v4().simple()));
    let fields = ["title", "label", "atomic_title"]
        .into_iter()
        .map(|name| FieldSchema {
            name: name.to_owned(),
            field_type: FieldType::String,
            nullable: false,
            metadata: BTreeMap::new(),
        })
        .collect();
    database
        .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
            locator: locator.clone(),
            schema: Some(CollectionSchema {
                fields,
                metadata: BTreeMap::new(),
            }),
            records: vec![
                DataRecord::without_id()
                    .json_field("title", serde_json::json!("41"))
                    .json_field("label", serde_json::json!("rename-only"))
                    .json_field("atomic_title", serde_json::json!("preserved")),
            ],
            mode: CreateCollectionMode::Create,
        }))
        .await
        .expect("create alter collection");

    database
        .catalog(CatalogRequest::AlterCollection(
            SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                locator: locator.clone(),
                alterations: vec![
                    ColumnAlterationSpec {
                        path: "title".to_owned(),
                        rename: Some("heading".to_owned()),
                        nullable: Some(false),
                        cast_to: Some(FieldType::I64),
                    },
                    ColumnAlterationSpec {
                        path: "label".to_owned(),
                        rename: Some("name".to_owned()),
                        nullable: None,
                        cast_to: None,
                    },
                ],
            }),
        ))
        .await
        .expect("alter combined and rename-only columns");
    let DataResponse::Records(page) = database
        .data(DataRequest::Scan(ScanRequest::new(locator.clone())))
        .await
        .expect("scan altered record")
    else {
        panic!("scan must return records");
    };
    let record = page.records.first().expect("altered record");
    assert_eq!(record.fields.get("heading"), Some(&serde_json::json!(41)));
    assert!(!record.fields.contains_key("title"));
    assert_eq!(
        record.fields.get("name"),
        Some(&serde_json::json!("rename-only"))
    );
    assert!(!record.fields.contains_key("label"));

    database
        .catalog(CatalogRequest::AlterCollection(
            SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                locator: locator.clone(),
                alterations: vec![
                    ColumnAlterationSpec {
                        path: "atomic_title".to_owned(),
                        rename: Some("atomic_heading".to_owned()),
                        nullable: None,
                        cast_to: None,
                    },
                    ColumnAlterationSpec {
                        path: "heading".to_owned(),
                        rename: None,
                        nullable: Some(true),
                        cast_to: None,
                    },
                ],
            }),
        ))
        .await
        .expect_err("invalid later alteration must reject the request");
    let DataResponse::Records(page) = database
        .data(DataRequest::Scan(ScanRequest::new(locator.clone())))
        .await
        .expect("scan after rejected alteration")
    else {
        panic!("scan must return records");
    };
    let record = page.records.first().expect("record after rejection");
    assert_eq!(
        record.fields.get("atomic_title"),
        Some(&serde_json::json!("preserved"))
    );
    assert!(!record.fields.contains_key("atomic_heading"));

    database
        .catalog(CatalogRequest::DropCollection {
            locator,
            ignore_missing: false,
        })
        .await
        .expect("drop alter collection");
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
async fn surrealdb_rocksdb_is_live() {
    let path = PathBuf::from(
        std::env::var("TEST_SURREALDB_ROCKSDB_PATH").expect("TEST_SURREALDB_ROCKSDB_PATH"),
    );
    assert_database_round_trip(
        DatabaseConfig::surreal(SurrealConfig {
            mode: SurrealModeConfig::RocksDb { path },
            ..SurrealConfig::default()
        }),
        false,
    )
    .await;
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
async fn surrealdb_surrealkv_is_live() {
    let path = PathBuf::from(
        std::env::var("TEST_SURREALDB_SURREALKV_PATH").expect("TEST_SURREALDB_SURREALKV_PATH"),
    );
    assert_database_round_trip(
        DatabaseConfig::surreal(SurrealConfig {
            mode: SurrealModeConfig::SurrealKv { path },
            ..SurrealConfig::default()
        }),
        false,
    )
    .await;
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
async fn surrealdb_remote_websocket_is_live() {
    let endpoint = std::env::var("TEST_SURREALDB_REMOTE_WS").expect("TEST_SURREALDB_REMOTE_WS");
    assert_database_round_trip(
        DatabaseConfig::surreal(authenticated_surreal(SurrealModeConfig::RemoteWs {
            endpoint,
        })),
        false,
    )
    .await;
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
async fn surrealdb_remote_http_is_live() {
    let endpoint = std::env::var("TEST_SURREALDB_REMOTE_HTTP").expect("TEST_SURREALDB_REMOTE_HTTP");
    assert_database_round_trip(
        DatabaseConfig::surreal(authenticated_surreal(SurrealModeConfig::RemoteHttp {
            endpoint,
        })),
        false,
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_state_store_is_live() {
    let store = PostgresCases::from_env().connect().await;
    state_cases::StateCases::new(store).verify().await;
}

#[cfg(feature = "postgres")]
struct PostgresCases {
    config: PostgresStateStoreConfig,
    native_config: tokio_postgres::Config,
    namespace: String,
}

#[cfg(feature = "postgres")]
impl PostgresCases {
    fn from_env() -> Self {
        let host = std::env::var("TEST_POSTGRES_STATE_STORE_HOST")
            .expect("TEST_POSTGRES_STATE_STORE_HOST");
        let port = std::env::var("TEST_POSTGRES_STATE_STORE_PORT")
            .expect("TEST_POSTGRES_STATE_STORE_PORT")
            .parse::<u16>()
            .expect("PostgreSQL port");
        let database =
            std::env::var("TEST_POSTGRES_STATE_STORE_DB").expect("TEST_POSTGRES_STATE_STORE_DB");
        let username = std::env::var("TEST_POSTGRES_STATE_STORE_USER")
            .expect("TEST_POSTGRES_STATE_STORE_USER");
        let password = std::env::var("TEST_POSTGRES_STATE_STORE_PASS")
            .expect("TEST_POSTGRES_STATE_STORE_PASS");
        let mut native_config = tokio_postgres::Config::new();
        native_config
            .host(&host)
            .port(port)
            .dbname(&database)
            .user(&username)
            .password(&password)
            .connect_timeout(std::time::Duration::from_secs(5));
        Self {
            config: PostgresStateStoreConfig::new(host, database, username)
                .with_port(port)
                .with_password(Password::from_string(password).expect("PostgreSQL password")),
            native_config,
            namespace: format!("conflict-{}", uuid::Uuid::new_v4()),
        }
    }

    fn key(&self, fragment: &str) -> StateKey {
        StateKey::new(&self.namespace, fragment).expect("key")
    }

    async fn connect(&self) -> StateStore {
        StateStore::connect(StateStoreConfig::postgres(self.config.clone()))
            .await
            .expect("PostgreSQL")
    }
}

#[cfg(feature = "postgres")]
struct PostgresCollationCase {
    case: PostgresCases,
    database: String,
    client: tokio_postgres::Client,
    driver: Option<tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>>,
}

#[cfg(feature = "postgres")]
impl PostgresCollationCase {
    async fn connect(case: PostgresCases) -> Self {
        let (client, connection) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            case.native_config.connect(tokio_postgres::NoTls),
        )
        .await
        .expect("PostgreSQL fixture connection must be bounded")
        .expect("PostgreSQL fixture must connect");
        Self {
            case,
            database: format!("storage_kit_collation_{}", uuid::Uuid::new_v4().simple()),
            client,
            driver: Some(tokio::spawn(connection)),
        }
    }

    async fn run(mut self) {
        let result = std::panic::AssertUnwindSafe(tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.verify(),
        ))
        .catch_unwind()
        .await;
        // Cleanup is outside the caught assertion so a meaningful red still
        // removes only this case's UUID-named database and settles its driver.
        let cleanup = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.client.batch_execute(&format!(
                "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
                self.database
            )),
        )
        .await;
        if !matches!(&cleanup, Ok(Ok(()))) {
            eprintln!("PostgreSQL collation fixture cleanup failed: {cleanup:?}");
        }
        if let Some(driver) = self.driver.take() {
            driver.abort();
            let _driver_result = driver.await;
        }
        match result {
            Ok(Ok(())) => cleanup
                .expect("collation fixture cleanup must be bounded")
                .expect("collation fixture database must be removed"),
            Ok(Err(_)) => panic!("PostgreSQL collation case exceeded its deadline"),
            Err(primary) => std::panic::resume_unwind(primary),
        }
    }

    async fn verify(&self) {
        self.client
            .batch_execute("SET statement_timeout = 10000")
            .await
            .expect("fixture statements must have a server deadline");
        // PostgreSQL 18 CREATE DATABASE permits an independent ICU locale with
        // template0; no host operating-system locale installation is required.
        self.client
            .batch_execute(&format!(
                "CREATE DATABASE \"{}\" TEMPLATE template0 ENCODING 'UTF8' \
                 LOCALE_PROVIDER icu ICU_LOCALE 'en-US'",
                self.database
            ))
            .await
            .expect("strict PostgreSQL must provide CREATEDB privilege and ICU support");
        let locale = self
            .client
            .query_one(
                "SELECT datlocprovider::text, datlocale FROM pg_database WHERE datname = $1",
                &[&self.database],
            )
            .await
            .expect("fixture locale must be observable");
        assert_eq!(locale.get::<_, String>(0), "i");
        assert_eq!(locale.get::<_, String>(1), "en-US");
        let config = PostgresStateStoreConfig::new(
            self.case.config.host(),
            &self.database,
            self.case.config.username(),
        )
        .with_port(self.case.config.port())
        .with_password(
            self.case
                .config
                .password()
                .cloned()
                .expect("strict password"),
        );
        let store = StateStore::connect(StateStoreConfig::postgres(config))
            .await
            .expect("StateStore must connect to the ICU database");
        state_cases::StateCases::new(store).pagination().await;
    }
}

#[cfg(feature = "postgres")]
impl Drop for PostgresCollationCase {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_state_pagination_ignores_database_collation() {
    PostgresCollationCase::connect(PostgresCases::from_env())
        .await
        .run()
        .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_serializable_absence_fences_prevent_write_skew() {
    let case = PostgresCases::from_env();
    let first = case.connect().await;
    let second = case.connect().await;
    let keys = [case.key("first"), case.key("second")];
    let receipt = case.key("losing-receipt");
    let mut left = first.begin_transaction().await.expect("left");
    let mut right = second.begin_transaction().await.expect("right");
    for key in &keys {
        assert!(
            left.load(key.clone())
                .await
                .expect("left absence")
                .is_none()
        );
        assert!(
            right
                .load(key.clone())
                .await
                .expect("right absence")
                .is_none()
        );
    }
    // Both have observed the absent keys before either writer publishes.
    right
        .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
        .await
        .expect("staged receipt");
    left.put(StateRecord::new(keys[0].clone(), serde_json::json!(1)))
        .await
        .expect("left put");
    left.commit().await.expect("first commit");
    let error = match right
        .put(StateRecord::new(keys[1].clone(), serde_json::json!(2)))
        .await
    {
        Ok(()) => right
            .commit()
            .await
            .expect_err("serialization must fail by commit"),
        Err(error) => {
            right.rollback().await.expect("join conflict cleanup");
            error
        }
    };
    assert!(
        matches!(
            error,
            storage_kit::StateStoreError::TransactionConflict { .. }
        ),
        "{error:?}"
    );
    assert!(first.load(keys[0].clone()).await.expect("winner").is_some());
    assert!(first.load(keys[1].clone()).await.expect("loser").is_none());
    assert!(first.load(receipt).await.expect("losing receipt").is_none());
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_stale_snapshot_cannot_lock_a_changed_present_key() {
    let case = PostgresCases::from_env();
    let first = case.connect().await;
    let second = case.connect().await;
    let key = case.key("head");
    let anchor = case.key("snapshot-anchor");
    let receipt = case.key("losing-receipt");
    let mut seed = first.begin_transaction().await.expect("seed");
    seed.put(StateRecord::new(key.clone(), serde_json::json!(0)))
        .await
        .expect("seed head");
    seed.put(StateRecord::new(anchor.clone(), serde_json::json!(0)))
        .await
        .expect("seed anchor");
    seed.commit().await.expect("seed commit");
    let mut stale = second.begin_transaction().await.expect("stale");
    stale
        .load(anchor)
        .await
        .expect("establish independent snapshot")
        .expect("anchor");
    stale
        .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
        .await
        .expect("receipt");
    let mut winner = first.begin_transaction().await.expect("winner");
    winner
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .expect("winner put");
    winner.commit().await.expect("winner commit");
    let error = stale
        .load(key.clone())
        .await
        .expect_err("stale lock must conflict");
    assert!(
        matches!(
            error,
            storage_kit::StateStoreError::TransactionConflict { .. }
        ),
        "{error:?}"
    );
    stale.rollback().await.expect("join conflict cleanup");
    assert!(first.load(receipt).await.expect("losing receipt").is_none());
    assert_eq!(
        first
            .load(key)
            .await
            .expect("head")
            .expect("head")
            .record
            .value
            .as_ref(),
        &serde_json::json!(1)
    );
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_native_lock_timeout_rolls_back_all_staged_writes() {
    let case = PostgresCases::from_env();
    let holder = case.connect().await;
    let contender = StateStore::connect(StateStoreConfig::postgres(
        case.config
            .clone()
            .with_operation_timeout(std::time::Duration::from_millis(150))
            .expect("timeout"),
    ))
    .await
    .expect("contender");
    let key = case.key("locked");
    let receipt = case.key("losing-receipt");
    let mut seed = holder.begin_transaction().await.expect("seed");
    seed.put(StateRecord::new(key.clone(), serde_json::json!(0)))
        .await
        .expect("seed");
    seed.commit().await.expect("commit");
    let mut holding = holder.begin_transaction().await.expect("holding");
    holding
        .load(key.clone())
        .await
        .expect("row lock")
        .expect("record");
    let mut blocked = contender.begin_transaction().await.expect("blocked");
    blocked
        .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
        .await
        .expect("receipt");
    let error = blocked
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .expect_err("native timeout");
    assert_eq!(
        error,
        storage_kit::StateStoreError::TimedOut {
            operation: "put".to_owned(),
            timeout_ms: 150
        }
    );
    blocked.rollback().await.expect("join timeout cleanup");
    holding.rollback().await.expect("release row lock");
    assert!(holder.load(receipt).await.expect("receipt").is_none());
    assert_eq!(
        holder
            .load(key)
            .await
            .expect("head")
            .expect("head")
            .record
            .value
            .as_ref(),
        &serde_json::json!(0)
    );
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_admitted_commit_survives_a_lost_waiter() {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
        time::Duration,
    };
    let case = PostgresCases::from_env();
    let store = case.connect().await;
    let key = case.key("receipt");
    let mut transaction = store.begin_transaction().await.expect("transaction");
    transaction
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!("committed"),
        ))
        .await
        .expect("put");
    let mut commit = Box::pin(transaction.commit());
    // On this current-thread runtime, the first poll admits the terminal
    // command and waits on the actor reply before another task can run.
    poll_fn(|context| match commit.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => panic!("actor unexpectedly ran before yielding: {result:?}"),
    })
    .await;
    drop(commit);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(record) = store.load(key.clone()).await.expect("reconcile receipt") {
            assert_eq!(
                record.record.value.as_ref(),
                &serde_json::json!("committed")
            );
            assert_eq!(record.revision.get(), 1);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "admitted commit must finish after waiter loss"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut next = store.begin_transaction().await.expect("next transaction");
    next.put(StateRecord::new(key.clone(), serde_json::json!("next")))
        .await
        .expect("next put");
    next.commit().await.expect("next commit");
    assert_eq!(
        store
            .load(key)
            .await
            .expect("next receipt")
            .expect("value")
            .revision
            .get(),
        2
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_memory_state_store_is_live() {
    use storage_kit::state::config::RedbStateStoreConfig;
    let store = StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::memory()))
        .await
        .expect("real Redb memory store");
    state_cases::StateCases::new(store).verify().await;
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn redb_file_state_store_is_live() {
    use storage_kit::state::config::RedbStateStoreConfig;
    let directory = tempfile::tempdir().expect("real Redb directory");
    state_cases::StateCases::verify_file(StateStoreConfig::redb(RedbStateStoreConfig::file(
        directory.path().join("state.redb"),
    )))
    .await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_memory_state_store_is_live() {
    use storage_kit::state::config::SqliteStateStoreConfig;
    let store = StateStore::connect(StateStoreConfig::sqlite(SqliteStateStoreConfig::memory()))
        .await
        .expect("real SQLite memory store");
    state_cases::StateCases::new(store).verify().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_file_state_store_is_live() {
    use storage_kit::state::config::SqliteStateStoreConfig;
    let directory = tempfile::tempdir().expect("real SQLite directory");
    state_cases::StateCases::verify_file(StateStoreConfig::sqlite(SqliteStateStoreConfig::file(
        directory.path().join("state.sqlite"),
    )))
    .await;
}
