use crate::{DenseVector, StorageError, VectorCoercionPolicy};

pub(super) fn parse_f32_json_array(
    value: &serde_json::Value,
    dim: usize,
) -> Result<Vec<f32>, StorageError> {
    parse_json_array(value, dim, "f32")
        .map(|values| values.into_iter().map(|value| value as f32).collect())
}

pub(super) fn parse_f64_json_array(
    value: &serde_json::Value,
    dim: usize,
) -> Result<Vec<f64>, StorageError> {
    parse_json_array(value, dim, "f64")
}

pub(super) fn vector_as_f64(vector: &DenseVector) -> Result<Vec<f64>, StorageError> {
    match vector {
        DenseVector::F64(values) => Ok(values.clone()),
        DenseVector::F32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        DenseVector::I16(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        DenseVector::I32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        DenseVector::I64(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let converted = *value as f64;
                if converted as i128 == i128::from(*value) {
                    Ok(converted)
                } else {
                    Err(StorageError::InvalidRequest(format!(
                        "cannot convert i64 dense vector value at index {index} (`{value}`) to f64 without precision loss under {:?} policy",
                        VectorCoercionPolicy::Lossless
                    )))
                }
            })
            .collect(),
    }
}

pub(super) fn json_to_string(value: &serde_json::Value) -> Result<String, StorageError> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        other => serde_json::to_string(other).map_err(StorageError::from),
    }
}

fn parse_json_array(
    value: &serde_json::Value,
    dim: usize,
    scalar: &str,
) -> Result<Vec<f64>, StorageError> {
    let values = value.as_array().ok_or_else(|| {
        StorageError::Serialization(format!(
            "expected JSON array for Lance {scalar} vector field, got {value}"
        ))
    })?;
    if values.len() != dim {
        return Err(StorageError::Serialization(format!(
            "expected Lance {scalar} vector length {dim}, got {}",
            values.len()
        )));
    }
    values
        .iter()
        .map(|value| {
            value.as_f64().ok_or_else(|| {
                StorageError::Serialization(format!(
                    "expected numeric Lance {scalar} vector entry, got {value}"
                ))
            })
        })
        .collect()
}
