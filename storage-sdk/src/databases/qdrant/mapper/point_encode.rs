use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::{DataRecord, DenseVector, RecordId, StorageError, VectorCoercionPolicy};

use super::super::{manager::QdrantManager, point_id};

pub(crate) fn encode_point(
    _db: &QdrantManager,
    record: &DataRecord,
) -> Result<Value, StorageError> {
    let id = record.id.as_ref().ok_or_else(|| {
        StorageError::InvalidConfig("Qdrant records require an id for upsert/insert".into())
    })?;
    let vector = encode_vectors_map(&record.vectors)?;

    Ok(json!({
        "id": point_id::encode_native_point_id(id)?.to_rest_json(),
        "vector": vector,
        "payload": record.fields,
    }))
}

fn encode_vectors_map(vectors: &BTreeMap<String, DenseVector>) -> Result<Value, StorageError> {
    if vectors.keys().any(|name| name.is_empty()) {
        return Err(StorageError::unsupported_capability(
            "UNNAMED_DEFAULT_VECTOR",
            "Qdrant vector writes require a non-empty portable vector name",
        ));
    }

    let mut root = Map::new();

    for (name, vector) in vectors {
        root.insert(
            name.clone(),
            json!(vector.to_f32(VectorCoercionPolicy::Lossless)?),
        );
    }

    Ok(Value::Object(root))
}

pub(crate) fn encode_point_id(id: &RecordId, _db: &QdrantManager) -> Result<Value, StorageError> {
    Ok(point_id::encode_native_point_id(id)?.to_rest_json())
}
