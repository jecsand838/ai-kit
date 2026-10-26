use std::path::Path;

use serde_json::{Map, Value, json};

use crate::{CollectionInfo, CreateIndexRequest, IndexInfo, StorageError};

pub(crate) fn extract_result_object(value: &Value) -> Result<&Map<String, Value>, StorageError> {
    value
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            StorageError::Serialization(format!(
                "Qdrant response is missing object `result`: {value}"
            ))
        })
}

pub(crate) fn parse_page_token_value(token: &str) -> Value {
    let trimmed = token.trim();
    if let Ok(value) = trimmed.parse::<u64>() {
        json!(value)
    } else if let Ok(value) = trimmed.parse::<i64>() {
        json!(value)
    } else {
        Value::String(trimmed.to_owned())
    }
}

pub(crate) fn parse_page_token_for_edge(token: &str) -> Result<qdrant_edge::PointId, StorageError> {
    match parse_page_token_value(token) {
        Value::Number(number) => {
            number
                .as_u64()
                .map(qdrant_edge::PointId::NumId)
                .ok_or_else(|| {
                    StorageError::Serialization(format!(
                        "unsupported numeric page token `{number}`"
                    ))
                })
        }
        Value::String(text) => uuid::Uuid::parse_str(&text)
            .map(qdrant_edge::PointId::Uuid)
            .map_err(|error| {
                StorageError::Serialization(format!("invalid Qdrant page token `{text}`: {error}"))
            }),
        other => Err(StorageError::Serialization(format!(
            "unsupported Qdrant page token `{other}`"
        ))),
    }
}

pub(crate) fn qdrant_collection_info(
    _db: &super::super::manager::QdrantManager,
    locator: crate::StorageLocator,
    _physical_name: String,
    raw: &Value,
) -> CollectionInfo {
    CollectionInfo {
        locator,
        schema: None,
        record_count: raw.pointer("/result/points_count").and_then(Value::as_u64),
    }
}

pub(crate) fn edge_directory_collection_info(_path: &Path, name: String) -> CollectionInfo {
    CollectionInfo {
        locator: crate::StorageLocator::new(name),
        schema: None,
        record_count: None,
    }
}

pub(crate) fn edge_shard_collection_info(
    locator: crate::StorageLocator,
    _physical_name: String,
    _path: &Path,
    info: &qdrant_edge::ShardInfo,
) -> CollectionInfo {
    CollectionInfo {
        locator,
        schema: None,
        record_count: Some(info.points_count as u64),
    }
}

pub(crate) fn remote_collection_infos(
    _db: &super::super::manager::QdrantManager,
    raw: &Value,
) -> Result<Vec<CollectionInfo>, StorageError> {
    extract_result_object(raw)?
        .get("collections")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            StorageError::Serialization(format!(
                "Qdrant collections response is missing `result.collections`: {raw}"
            ))
        })?
        .iter()
        .map(|value| {
            let name = value.get("name").and_then(Value::as_str).ok_or_else(|| {
                StorageError::Serialization(format!(
                    "Qdrant collection entry is missing string `name`: {value}"
                ))
            })?;
            Ok(CollectionInfo {
                locator: crate::StorageLocator::new(name),
                schema: None,
                record_count: None,
            })
        })
        .collect()
}

pub(crate) fn remote_index_infos(
    _db: &super::super::manager::QdrantManager,
    raw: &Value,
) -> Result<Vec<IndexInfo>, StorageError> {
    let payload_schema = raw
        .pointer("/result/payload_schema")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            StorageError::Serialization(format!(
                "Qdrant collection info is missing `result.payload_schema`: {raw}"
            ))
        })?;
    Ok(payload_schema
        .keys()
        .map(|field| IndexInfo {
            name: Some(field.clone()),
            columns: vec![field.clone()],
            ready: Some(true),
        })
        .collect())
}

pub(crate) fn field_schema_from_index(
    kind: &crate::IndexKind,
) -> Result<&'static str, StorageError> {
    match kind {
        crate::IndexKind::FullText => Ok("text"),
        crate::IndexKind::Scalar => Err(StorageError::unsupported_capability(
            "SCALAR_INDEX",
            "Qdrant payload indexes require a native scalar field type that the portable index request does not guess",
        )),
        crate::IndexKind::Vector { .. } => Err(StorageError::unsupported_capability(
            "PAYLOAD_INDEX_KIND",
            "Qdrant vector indexes are configured with the collection",
        )),
    }
}

pub(crate) fn field_schema_for_edge(
    kind: &crate::IndexKind,
) -> Result<qdrant_edge::PayloadFieldSchema, StorageError> {
    serde_json::from_value(json!(field_schema_from_index(kind)?)).map_err(Into::into)
}

pub(crate) fn build_remote_field_schema(
    request: &CreateIndexRequest,
) -> Result<Value, StorageError> {
    Ok(match &request.kind {
        crate::IndexKind::FullText => json!({ "type": "text", "tokenizer": "word" }),
        _ => Value::String(field_schema_from_index(&request.kind)?.into()),
    })
}
