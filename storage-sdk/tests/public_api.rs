use std::sync::Arc;

use storage_sdk::{
    Database, DatabaseBackend, StateStore, StateStoreBackend, StateTransaction,
    StateTransactionBackend,
    database::{
        CapabilitySet, StorageLocator,
        catalog::{FieldSchema, FieldType, VectorElementType},
        config::{ApiKey, DatabaseConfig, LanceConfig, Password},
        data::{DataRecord, DenseVector, RecordId},
        index::{DistanceMetric, IndexKind},
        query::{Projection, QueryScope, VectorCoercionPolicy},
    },
    state::{
        config::{RedbStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateListRequest, StateNamespace, StateRecord, StateRevision},
        transaction,
    },
};

fn assert_database_backend<T: DatabaseBackend>() {}

fn assert_state_store_backend<T: StateStoreBackend>() {}

fn assert_state_transaction_backend<T: StateTransactionBackend>() {}

#[test]
fn root_exports_only_the_behavioral_owners_and_traits() {
    assert_database_backend::<Database>();
    assert_state_store_backend::<StateStore>();
    assert_state_transaction_backend::<StateTransaction>();
    assert_state_transaction_backend::<transaction::StateTransaction>();
}

#[test]
fn supporting_types_live_in_functional_modules() {
    let locator = StorageLocator::new("documents")
        .with_namespace(["tenant"])
        .with_database("application");
    let scope =
        QueryScope::new(locator.clone()).projection(Projection::Columns(vec!["title".to_owned()]));
    assert_eq!(scope.locator, locator);

    let vector = DenseVector::I16(vec![1, 2, 3]);
    assert_eq!(
        vector
            .to_f32(VectorCoercionPolicy::Lossless)
            .expect("i16 values convert losslessly"),
        vec![1.0, 2.0, 3.0]
    );

    let record = DataRecord::new(RecordId::String("doc-1".to_owned()))
        .vector("embedding", vector)
        .json_field("title", serde_json::json!("portable storage"));
    assert_eq!(record.vectors["embedding"].len(), 3);

    let field = FieldSchema {
        name: "embedding".to_owned(),
        field_type: FieldType::DenseVector {
            dim: 3,
            element: VectorElementType::F32,
        },
        nullable: false,
        metadata: std::collections::BTreeMap::new(),
    };
    assert_eq!(field.name, "embedding");

    let kind = IndexKind::Vector {
        dimension: 3,
        distance: Some(DistanceMetric::Cosine),
    };
    assert!(matches!(kind, IndexKind::Vector { dimension: 3, .. }));
}

#[test]
fn construction_configs_are_direct_and_validated() {
    let database = DatabaseConfig::lance(LanceConfig::embedded_path("target/public-api-lancedb"));
    database.validate().expect("valid LanceDB configuration");

    let state_store = StateStoreConfig::redb(RedbStateStoreConfig::memory());
    state_store
        .validate()
        .expect("valid in-memory Redb configuration");

    let namespace = StateNamespace::new("agent").expect("valid namespace");
    assert_eq!(namespace.as_str(), "agent");
    let key = StateKey::new("agent", "workspace/1").expect("valid key");
    assert_eq!(key.key(), "workspace/1");
    let request = StateListRequest::new("agent")
        .expect("valid list")
        .with_limit(25)
        .expect("valid limit");
    assert_eq!(request.limit(), 25);
    assert_eq!(StateRevision::first().get(), 1);
}

#[test]
fn state_record_shared_value_keeps_copy_on_write_content_independent() {
    let original = StateRecord::new(
        StateKey::new("shared-record", "input").expect("key"),
        serde_json::json!({"nested": {"value": 1}, "payload": [null, "original"]}),
    );
    let retained: Arc<serde_json::Value> = Arc::clone(&original.value);
    let mut changed = original.clone();
    Arc::make_mut(&mut changed.value)["nested"]["value"] = serde_json::json!(2);
    Arc::make_mut(&mut changed.value)["payload"][1] = serde_json::json!("changed");
    assert_eq!(original.key, changed.key);
    assert_eq!(original.value.as_ref(), retained.as_ref());
    assert_eq!(
        retained.as_ref(),
        &serde_json::json!({"nested": {"value": 1}, "payload": [null, "original"]}),
    );
    assert_eq!(
        changed.value.as_ref(),
        &serde_json::json!({"nested": {"value": 2}, "payload": [null, "changed"]}),
    );
    assert_ne!(original, changed);
}

#[test]
fn secrets_validate_and_redact() {
    assert!(ApiKey::from_string(" ").is_err());
    assert!(ApiKey::from_string(" padded").is_err());
    let api_key = ApiKey::from_string("qdrant-secret").expect("valid API key");
    assert!(!format!("{api_key:?}").contains("qdrant-secret"));

    assert!(Password::from_string(" ").is_err());
    let password = Password::from_string("postgres secret").expect("valid password");
    assert!(!format!("{password:?}").contains("postgres secret"));
}

#[test]
fn capabilities_describe_only_retained_optional_behavior() {
    let capabilities = CapabilitySet::DENSE_VECTOR
        | CapabilitySet::FULL_TEXT
        | CapabilitySet::FILTERING
        | CapabilitySet::RANDOM_SAMPLE
        | CapabilitySet::SCHEMA_EVOLUTION
        | CapabilitySet::WAIT_FOR_INDEX
        | CapabilitySet::ATOMIC_PARTITION_REPLACE;
    assert_eq!(capabilities.bits().count_ones(), 7);
}

#[test]
fn lance_configuration_rejects_native_uri_routes_without_connecting() {
    for uri in [
        "s3://bucket/database",
        "s3:bucket/database",
        "s3:\t//bucket/database",
        "gs://bucket/database",
        "gs:\n//bucket/database",
        "az://container/database",
        "oss://bucket/database",
        "db://remote-database",
        "https://database.invalid",
        "file:///database",
    ] {
        assert!(
            DatabaseConfig::lance(LanceConfig::embedded_path(uri))
                .validate()
                .is_err(),
            "embedded filesystem configuration must reject native URI routing before connection: {uri}"
        );
    }
}

#[test]
fn lance_configuration_preserves_filesystem_paths_and_memory_selection() {
    for path in [
        "target/database",
        "/var/data/database",
        "C:\\data\\database",
        "C:/data/database",
        "./name:part",
        "memory://",
    ] {
        DatabaseConfig::lance(LanceConfig::embedded_path(path))
            .validate()
            .expect("filesystem paths and the supported memory selector remain valid");
    }
}
