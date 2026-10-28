use arrow_schema::DataType;

use crate::{DenseVector, FieldType, VectorElementType};

pub(crate) const LANCE_UTF8_FIELD_TYPE_METADATA_KEY: &str = "storage_kit_utf8_field_type";

pub(super) fn dense_arrow_type(dense: &DenseVector) -> DataType {
    match dense {
        DenseVector::F32(_) | DenseVector::I16(_) | DenseVector::I32(_) | DenseVector::I64(_) => {
            DataType::Float32
        }
        DenseVector::F64(_) => DataType::Float64,
    }
}

pub(crate) fn vector_element_type(element: &VectorElementType) -> DataType {
    match element {
        VectorElementType::F16
        | VectorElementType::F32
        | VectorElementType::I16
        | VectorElementType::I32
        | VectorElementType::I64 => DataType::Float32,
        VectorElementType::F64 => DataType::Float64,
    }
}

pub(super) fn json_value_type(value: &serde_json::Value) -> DataType {
    match value {
        serde_json::Value::Null => DataType::Utf8,
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(number) => {
            if number.is_u64() {
                DataType::UInt64
            } else if number.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => DataType::Utf8,
    }
}

pub(crate) fn metadata_field_type_tag(field_type: &FieldType) -> Option<&'static str> {
    match field_type {
        FieldType::String => Some("string"),
        FieldType::Json => Some("json"),
        _ => None,
    }
}

pub(crate) fn field_type_from_arrow(field: &arrow_schema::Field) -> FieldType {
    use arrow_schema::DataType;

    if let Some(tag) = field.metadata().get(LANCE_UTF8_FIELD_TYPE_METADATA_KEY) {
        match tag.as_str() {
            "string" => return FieldType::String,
            "json" => return FieldType::Json,
            _ => {}
        }
    }

    match field.data_type() {
        DataType::Boolean => FieldType::Bool,
        DataType::Int64 => FieldType::I64,
        DataType::UInt64 => FieldType::U64,
        DataType::Float64 => FieldType::F64,
        DataType::Utf8 | DataType::LargeUtf8 => FieldType::String,
        DataType::Binary | DataType::LargeBinary => FieldType::Binary,
        DataType::FixedSizeList(inner, dim) => FieldType::DenseVector {
            dim: *dim as usize,
            element: match inner.data_type() {
                DataType::Float64 => VectorElementType::F64,
                _ => VectorElementType::F32,
            },
        },
        DataType::List(_) => FieldType::Json,
        _ => FieldType::Json,
    }
}
