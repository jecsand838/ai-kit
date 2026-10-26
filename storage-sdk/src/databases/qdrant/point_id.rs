use serde_json::Value;
use uuid::Uuid;

use crate::{RecordId, StorageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QdrantPointId {
    U64(u64),
    Uuid(Uuid),
}

impl QdrantPointId {
    pub(crate) fn to_rest_json(self) -> Value {
        match self {
            Self::U64(value) => serde_json::json!(value),
            Self::Uuid(value) => Value::String(value.to_string()),
        }
    }

    pub(crate) fn to_edge_id(self) -> qdrant_edge::PointId {
        match self {
            Self::U64(value) => qdrant_edge::PointId::NumId(value),
            Self::Uuid(value) => qdrant_edge::PointId::Uuid(value),
        }
    }
}

pub(crate) fn encode_native_point_id(id: &RecordId) -> Result<QdrantPointId, StorageError> {
    match id {
        RecordId::U64(value) => Ok(QdrantPointId::U64(*value)),
        RecordId::Uuid(value) => Ok(QdrantPointId::Uuid(*value)),
        other => Err(StorageError::unsupported_capability(
            "POINT_IDS",
            format!(
                "Qdrant only supports native point ids as u64 or UUID; received {} `{}`",
                record_id_kind(other),
                other.as_string_lossy()
            ),
        )),
    }
}

pub(crate) fn decode_point_id(value: &Value) -> Result<Option<RecordId>, StorageError> {
    match value {
        Value::Number(number) => number.as_u64().map(RecordId::U64).map(Some).ok_or_else(|| {
            StorageError::Serialization(format!(
                "Qdrant point id must be an unsigned integer when encoded as a number: {value}"
            ))
        }),
        Value::String(text) => Uuid::parse_str(text)
            .map(RecordId::Uuid)
            .map(Some)
            .map_err(|err| {
                StorageError::Serialization(format!(
                    "Qdrant point id must be a UUID when encoded as string, got `{text}`: {err}"
                ))
            }),
        Value::Null => Ok(None),
        other => Err(StorageError::Serialization(format!(
            "Qdrant point id must be an unsigned integer, UUID string, or null: {other}"
        ))),
    }
}

fn record_id_kind(id: &RecordId) -> &'static str {
    match id {
        RecordId::String(_) => "string",
        RecordId::U64(_) => "u64",
        RecordId::I64(_) => "i64",
        RecordId::Uuid(_) => "uuid",
        RecordId::Opaque(_) => "opaque",
    }
}
