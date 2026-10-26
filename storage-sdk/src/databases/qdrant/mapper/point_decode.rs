use std::collections::BTreeMap;

use serde_json::Value;

use crate::{DataRecord, DenseVector, PageToken, RecordsPage, StorageError};

use super::{
    super::{manager::QdrantManager, point_id},
    VectorSelectionPlan,
};

pub(crate) fn decode_point_to_record(
    _db: &QdrantManager,
    point: &Value,
    vector_selection: &VectorSelectionPlan,
) -> Result<DataRecord, StorageError> {
    let payload_map = point
        .get("payload")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let id = point
        .get("id")
        .map(point_id::decode_point_id)
        .transpose()?
        .flatten();

    let mut vectors = decode_vectors_from_point(point, vector_selection)?;
    vector_selection.retain_decoded_vectors(&mut vectors);

    Ok(DataRecord {
        id,
        vectors,
        fields: payload_map,
    })
}

pub(crate) fn decode_records_page(
    db: &QdrantManager,
    points: &[Value],
    vector_selection: &VectorSelectionPlan,
    next_page: Option<PageToken>,
) -> Result<RecordsPage, StorageError> {
    let records = points
        .iter()
        .map(|point| decode_point_to_record(db, point, vector_selection))
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(RecordsPage {
        records,
        total: None,
        next_page,
    })
}

fn decode_vectors_from_point(
    point: &Value,
    vector_selection: &VectorSelectionPlan,
) -> Result<BTreeMap<String, DenseVector>, StorageError> {
    let mut vectors = BTreeMap::new();
    if let Some(vector) = point.get("vector") {
        decode_named_vector_container(vector, vector_selection, &mut vectors)?;
    }
    Ok(vectors)
}

fn decode_named_vector_container(
    value: &Value,
    vector_selection: &VectorSelectionPlan,
    out: &mut BTreeMap<String, DenseVector>,
) -> Result<(), StorageError> {
    match value {
        Value::Object(map) if !(map.contains_key("indices") || map.contains_key("values")) => {
            decode_vector_value_into("", value, out)
        }
        Value::Array(_) | Value::Object(_) if vector_selection.requests_vectors() => Err(
            super::remote_unnamed_default_vector_unsupported("vector decode"),
        ),
        Value::Array(_) | Value::Object(_) => Ok(()),
        other => Err(crate::StorageError::Serialization(format!(
            "unsupported Qdrant vector payload shape: {other}"
        ))),
    }
}

fn decode_vector_value_into(
    default_name: &str,
    value: &Value,
    out: &mut BTreeMap<String, DenseVector>,
) -> Result<(), StorageError> {
    match value {
        Value::Array(items) => {
            let dense = strict_f32_values(items, "Qdrant dense vector")?;
            out.insert(default_name.to_string(), DenseVector::F32(dense));
        }
        Value::Object(map) => {
            if map.contains_key("indices") || map.contains_key("values") {
                return Err(super::profile::VectorProfile::unsupported_native(
                    "remote vector decode",
                ));
            } else {
                for (name, child) in map {
                    if name == "sparse" {
                        return Err(super::profile::VectorProfile::unsupported_native(
                            "remote vector decode",
                        ));
                    } else {
                        match child {
                            Value::Array(first)
                                if first.first().map(Value::is_array).unwrap_or(false) =>
                            {
                                return Err(super::profile::VectorProfile::unsupported_native(
                                    "remote vector decode",
                                ));
                            }
                            _ => decode_vector_value_into(name, child, out)?,
                        }
                    }
                }
            }
        }
        other => {
            return Err(crate::StorageError::Serialization(format!(
                "unsupported Qdrant vector payload shape: {other}"
            )));
        }
    }
    Ok(())
}

fn strict_f32_values(values: &[Value], label: &str) -> Result<Vec<f32>, StorageError> {
    values
        .iter()
        .map(|value| {
            value.as_f64().map(|value| value as f32).ok_or_else(|| {
                crate::StorageError::Serialization(format!(
                    "{label} must contain only numeric values, got {value}"
                ))
            })
        })
        .collect()
}
