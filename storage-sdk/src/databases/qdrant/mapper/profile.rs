use std::collections::{BTreeMap, HashMap};

use serde_json::{Map, Value, json};

use crate::{CollectionSchema, DataRecord, FieldType, StorageError};

#[derive(Debug, Clone, Default)]
pub(crate) struct VectorProfile {
    dense: BTreeMap<String, usize>,
}

impl VectorProfile {
    fn insert_dimension(&mut self, name: &str, dimension: usize) -> Result<(), StorageError> {
        ensure_named_vector(name)?;
        if dimension == 0
            || self
                .dense
                .get(name)
                .is_some_and(|previous| *previous != dimension)
        {
            return Err(StorageError::InvalidConfig(format!(
                "Qdrant vector `{name}` must have one nonzero dimension"
            )));
        }
        self.dense.insert(name.to_owned(), dimension);
        Ok(())
    }

    pub(crate) fn for_edge(shard: &qdrant_edge::EdgeShard) -> Result<Self, StorageError> {
        ensure_edge_named_dense_vectors(shard, "write records")?;
        let mut profile = Self::default();
        for (name, params) in &shard.config().vectors {
            if params.multivector_config.is_some() {
                return Err(Self::unsupported_native("write records"));
            }
            profile.insert_dimension(name, params.size)?;
        }
        Ok(profile)
    }

    pub(crate) fn for_remote(raw: &Value) -> Result<Self, StorageError> {
        let vectors = raw
            .pointer("/result/config/params/vectors")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                StorageError::Serialization(
                    "Qdrant collection is missing vector configuration".into(),
                )
            })?;
        if vectors.get("size").is_some_and(Value::is_number) {
            return Err(remote_unnamed_default_vector_unsupported("write records"));
        }
        let mut profile = Self::default();
        for (name, params) in vectors {
            if params
                .get("multivector_config")
                .is_some_and(|value| !value.is_null())
            {
                return Err(Self::unsupported_native("write records"));
            }
            let dimension = params
                .get("size")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    StorageError::Serialization(format!("invalid Qdrant vector size for `{name}`"))
                })?;
            profile.insert_dimension(name, dimension)?;
        }
        Ok(profile)
    }

    pub(crate) fn validate_records(&self, records: &[DataRecord]) -> Result<(), StorageError> {
        for record in records {
            for (name, vector) in &record.vectors {
                ensure_named_vector(name)?;
                if self.dense.get(name) != Some(&vector.len()) {
                    return Err(StorageError::InvalidRequest(format!(
                        "Qdrant vector `{name}` does not match the collection's dense vector schema"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn unsupported_native(action: &str) -> StorageError {
        StorageError::unsupported_capability(
            "NON_DENSE_VECTOR",
            format!("Qdrant {action} encountered a vector outside the portable dense model"),
        )
    }

    pub(crate) fn edge_config(&self) -> qdrant_edge::EdgeConfig {
        let mut vectors = HashMap::new();
        for (name, size) in &self.dense {
            vectors.insert(
                name.clone(),
                qdrant_edge::EdgeVectorParams::builder(*size, qdrant_edge::Distance::Cosine)
                    .build(),
            );
        }
        qdrant_edge::EdgeConfig::builder()
            .vectors(vectors)
            .payload_memory(qdrant_edge::Memory::Cached)
            .build()
    }
}

pub(crate) fn infer_collection_profile(
    schema: Option<&CollectionSchema>,
    records: &[DataRecord],
) -> Result<VectorProfile, StorageError> {
    let mut profile = VectorProfile::default();
    if let Some(schema) = schema {
        for field in &schema.fields {
            if let FieldType::DenseVector { dim, .. } = &field.field_type {
                profile.insert_dimension(&field.name, *dim)?;
            }
        }
    }
    for record in records {
        for (name, value) in &record.vectors {
            profile.insert_dimension(name, value.len())?;
        }
    }
    if profile.dense.is_empty() {
        return Err(StorageError::InvalidConfig(
            "Qdrant collection creation requires a dense vector in the schema or seed records"
                .into(),
        ));
    }
    Ok(profile)
}

fn ensure_named_vector(name: &str) -> Result<(), StorageError> {
    if name.trim().is_empty() {
        return Err(StorageError::unsupported_capability(
            "UNNAMED_DEFAULT_VECTOR",
            "Qdrant portable operations require named dense vectors",
        ));
    }
    Ok(())
}

pub(crate) fn edge_uses_unnamed_default_dense(shard: &qdrant_edge::EdgeShard) -> bool {
    shard
        .config()
        .vectors
        .contains_key(qdrant_edge::DEFAULT_VECTOR_NAME)
}

pub(crate) fn edge_unnamed_default_vector_unsupported(action: &str) -> StorageError {
    StorageError::unsupported_capability(
        "UNNAMED_DEFAULT_VECTOR",
        format!("qdrant-edge cannot perform {action} on an unnamed dense vector"),
    )
}

pub(crate) fn remote_unnamed_default_vector_unsupported(action: &str) -> StorageError {
    StorageError::unsupported_capability(
        "UNNAMED_DEFAULT_VECTOR",
        format!("Qdrant REST cannot perform {action} on an unnamed dense vector"),
    )
}

pub(crate) fn ensure_edge_named_dense_vectors(
    shard: &qdrant_edge::EdgeShard,
    action: &str,
) -> Result<(), StorageError> {
    if edge_uses_unnamed_default_dense(shard) {
        Err(edge_unnamed_default_vector_unsupported(action))
    } else {
        Ok(())
    }
}

pub(crate) fn build_remote_collection_body(profile: &VectorProfile) -> Result<Value, StorageError> {
    // Pinned Qdrant REST VectorParams validates 1..=65536. Edge's native
    // configuration has a different contract, so this limit stays REST-only.
    if profile
        .dense
        .values()
        .any(|dimension| !(1..=65_536).contains(dimension))
    {
        return Err(StorageError::InvalidConfig(
            "Qdrant REST vector dimensions must be between 1 and 65536".to_owned(),
        ));
    }
    let mut vectors = Map::new();
    for (name, size) in &profile.dense {
        vectors.insert(
            name.clone(),
            json!({
                "size": size,
                "distance": "Cosine",
            }),
        );
    }
    Ok(json!({ "vectors": Value::Object(vectors) }))
}
