use serde_json::Value;

use crate::{DataRecord, RecordId, RecordsPage, StorageError};

pub(crate) fn value_to_record_id(value: &Value) -> Result<Option<RecordId>, StorageError> {
    match value {
        Value::String(raw) => Ok(Some(RecordId::String(raw.clone()))),
        Value::Number(num) => {
            if let Some(value) = num.as_u64() {
                Ok(Some(RecordId::U64(value)))
            } else {
                Ok(num.as_i64().map(RecordId::I64))
            }
        }
        Value::Object(object) => {
            let table = object.get("tb").and_then(Value::as_str).ok_or_else(|| {
                StorageError::Serialization(format!(
                    "Surreal record id object is missing string `tb`: {value}"
                ))
            })?;
            let id = object.get("id").ok_or_else(|| {
                StorageError::Serialization(format!(
                    "Surreal record id object is missing `id`: {value}"
                ))
            })?;
            match id {
                Value::String(raw) => Ok(Some(RecordId::Opaque(format!("{table}:{raw}")))),
                Value::Number(num) => Ok(Some(RecordId::Opaque(format!("{table}:{num}")))),
                _ => Err(StorageError::Serialization(format!(
                    "Surreal record id object has unsupported `id` shape: {value}"
                ))),
            }
        }
        Value::Null => Ok(None),
        _ => Err(StorageError::Serialization(format!(
            "Surreal record id must be a string, number, object, or null: {value}"
        ))),
    }
}

pub(crate) fn value_to_record(value: Value) -> Result<DataRecord, StorageError> {
    match value {
        Value::Object(mut object) => {
            let id = object
                .remove("id")
                .map(|value| value_to_record_id(&value))
                .transpose()?
                .flatten();
            Ok(DataRecord {
                id,
                vectors: std::collections::BTreeMap::new(),
                fields: object,
            })
        }
        other => Err(StorageError::Serialization(format!(
            "Surreal row payload must be an object, got {other}"
        ))),
    }
}

pub(crate) fn rows_page_from_value(value: Value) -> Result<RecordsPage, StorageError> {
    match value {
        Value::Array(rows) => Ok(RecordsPage {
            records: rows
                .into_iter()
                .map(value_to_record)
                .collect::<Result<Vec<_>, StorageError>>()?,
            total: None,
            next_page: None,
        }),
        other => Err(StorageError::Serialization(format!(
            "Surreal rows payload must be an array, got {other}"
        ))),
    }
}
