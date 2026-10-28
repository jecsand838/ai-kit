use storage_kit::database::config::{DatabaseConfig, SurrealConfig};
use storage_kit::state::config::{
    PostgresStateStoreConfig, RedbStateStoreConfig, StateStoreConfig,
};
use storage_kit::{Database, StateStore, StateStoreError, StorageError};

#[tokio::test]
async fn disabled_redb_is_rejected_without_creating_a_database() {
    if cfg!(feature = "redb") {
        return;
    }
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("must-not-exist.redb");
    let result =
        StateStore::connect(StateStoreConfig::redb(RedbStateStoreConfig::file(&path))).await;
    assert!(
        matches!(result, Err(StateStoreError::InvalidConfig(ref message)) if message.contains("redb")),
        "disabled Redb must identify its required feature"
    );
    assert!(
        !path.exists(),
        "disabled construction must not create storage"
    );
}

#[tokio::test]
async fn disabled_postgres_is_rejected_before_connection() {
    if cfg!(feature = "postgres") {
        return;
    }
    let result = StateStore::connect(StateStoreConfig::postgres(
        PostgresStateStoreConfig::localhost("feature_contract", "feature_contract"),
    ))
    .await;
    assert!(
        matches!(result, Err(StateStoreError::InvalidConfig(ref message)) if message.contains("postgres")),
        "disabled PostgreSQL must identify its required feature"
    );
}

#[tokio::test]
async fn disabled_surreal_is_rejected_before_construction() {
    if cfg!(feature = "surrealdb") {
        return;
    }
    let result = Database::connect(DatabaseConfig::surreal(SurrealConfig::default())).await;
    assert!(
        matches!(result, Err(StorageError::InvalidConfig(ref message)) if message.contains("surrealdb")),
        "disabled SurrealDB must identify its required feature"
    );
}

#[test]
fn optional_implementation_configs_keep_their_serialization_contract() {
    for config in [
        StateStoreConfig::redb(RedbStateStoreConfig::memory()),
        StateStoreConfig::postgres(PostgresStateStoreConfig::localhost("app", "user")),
    ] {
        let encoded = serde_json::to_value(&config).expect("serialize configuration");
        let decoded: StateStoreConfig =
            serde_json::from_value(encoded.clone()).expect("deserialize configuration");
        decoded
            .validate()
            .expect("valid configuration without constructing backend");
        assert_eq!(
            serde_json::to_value(decoded).expect("serialize roundtrip"),
            encoded
        );
    }
    let config = DatabaseConfig::surreal(SurrealConfig::default());
    let encoded = serde_json::to_value(&config).expect("serialize database configuration");
    let decoded: DatabaseConfig =
        serde_json::from_value(encoded.clone()).expect("deserialize database configuration");
    decoded.validate().expect("valid database configuration");
    assert_eq!(
        serde_json::to_value(decoded).expect("serialize roundtrip"),
        encoded
    );
}
