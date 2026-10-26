use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray, UInt64Array,
    types::{Float32Type, Float64Type},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{DataRecord, DenseVector, StorageError, VectorCoercionPolicy};

use super::{
    json::{json_to_string, parse_f32_json_array, parse_f64_json_array, vector_as_f64},
    schema::{field_type_from_arrow, infer_arrow_schema},
};

pub(crate) fn encode_records(
    records: &[DataRecord],
    declared: Option<&Schema>,
) -> Result<
    RecordBatchIterator<std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>>,
    StorageError,
> {
    let inferred = infer_arrow_schema(records)?;
    // Keep the incoming field set and native validation of unknown/type-mismatched
    // fields. Only declared JSON fields need a different physical encoding than
    // inference from a scalar-only batch would choose.
    let schema = match declared {
        Some(declared) => Arc::new(Schema::new_with_metadata(
            inferred
                .fields()
                .iter()
                .map(|field| match declared.field_with_name(field.name()) {
                    Ok(target) if field_type_from_arrow(target) == crate::FieldType::Json => {
                        Arc::new(target.clone())
                    }
                    _ => Arc::clone(field),
                })
                .collect::<Vec<_>>(),
            inferred.metadata().clone(),
        )),
        None => inferred,
    };
    let batch = build_record_batch(records, schema.clone())?;
    let iter = vec![Ok(batch)].into_iter();
    Ok(RecordBatchIterator::new(iter, schema))
}

fn build_record_batch(
    records: &[DataRecord],
    schema: SchemaRef,
) -> Result<RecordBatch, StorageError> {
    RecordBatchEncoder { records, schema }.build()
}

struct RecordBatchEncoder<'a> {
    records: &'a [DataRecord],
    schema: SchemaRef,
}

impl RecordBatchEncoder<'_> {
    fn build(&self) -> Result<RecordBatch, StorageError> {
        let mut columns = Vec::new();

        for field in self.schema.fields() {
            let name = field.name();
            let array = match field.data_type() {
                DataType::Utf8 => self.build_utf8_column(field)?,
                DataType::Boolean => self.build_bool_column(name),
                DataType::Int64 => self.build_i64_column(name),
                DataType::UInt64 => self.build_u64_column(name),
                DataType::Float64 => self.build_f64_column(name),
                DataType::Binary => self.build_binary_column(name),
                DataType::FixedSizeList(inner, dim) => {
                    self.build_fixed_size_list_column(name, inner.data_type(), *dim as usize)?
                }
                other => {
                    return Err(StorageError::Serialization(format!(
                        "unsupported Arrow field in Lance codec: {other:?}"
                    )));
                }
            };
            columns.push(array);
        }

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|err| StorageError::Serialization(err.to_string()))
    }

    fn build_utf8_column(&self, field: &Field) -> Result<ArrayRef, StorageError> {
        let name = field.name();
        let json = field_type_from_arrow(field) == crate::FieldType::Json;
        let values = self
            .records
            .iter()
            .map(|record| {
                if record.vectors.contains_key(name) {
                    Err(StorageError::InvalidConfig(format!(
                        "Lance dense vector column `{name}` cannot be encoded as UTF-8"
                    )))
                } else {
                    record
                        .fields
                        .get(name)
                        .map(|value| {
                            if json {
                                serde_json::to_string(value)
                                    .map_err(|error| StorageError::Serialization(error.to_string()))
                            } else {
                                json_to_string(value)
                            }
                        })
                        .transpose()
                }
            })
            .collect::<Result<Vec<Option<String>>, StorageError>>()?;

        Ok(Arc::new(StringArray::from(values)))
    }

    fn build_bool_column(&self, name: &str) -> ArrayRef {
        let values = self
            .records
            .iter()
            .map(|record| record.fields.get(name).and_then(|v| v.as_bool()))
            .collect::<Vec<_>>();
        Arc::new(BooleanArray::from(values))
    }

    fn build_i64_column(&self, name: &str) -> ArrayRef {
        let values = self
            .records
            .iter()
            .map(|record| record.fields.get(name).and_then(|v| v.as_i64()))
            .collect::<Vec<_>>();
        Arc::new(Int64Array::from(values))
    }

    fn build_u64_column(&self, name: &str) -> ArrayRef {
        let values = self
            .records
            .iter()
            .map(|record| record.fields.get(name).and_then(|v| v.as_u64()))
            .collect::<Vec<_>>();
        Arc::new(UInt64Array::from(values))
    }

    fn build_f64_column(&self, name: &str) -> ArrayRef {
        let values = self
            .records
            .iter()
            .map(|record| record.fields.get(name).and_then(|v| v.as_f64()))
            .collect::<Vec<_>>();
        Arc::new(Float64Array::from(values))
    }

    fn build_binary_column(&self, name: &str) -> ArrayRef {
        use arrow_array::BinaryArray;

        let values = self
            .records
            .iter()
            .map(|record| {
                record
                    .fields
                    .get(name)
                    .and_then(|v| v.as_str())
                    .map(|s| s.as_bytes().to_vec())
            })
            .collect::<Vec<_>>();
        let refs = values.iter().map(|v| v.as_deref()).collect::<Vec<_>>();
        Arc::new(BinaryArray::from(refs))
    }

    fn build_fixed_size_list_column(
        &self,
        name: &str,
        inner: &DataType,
        dim: usize,
    ) -> Result<ArrayRef, StorageError> {
        match inner {
            DataType::Float32 => self.build_fixed_size_list_column_typed::<Float32Type, _, _>(
                name,
                dim,
                |vector: &DenseVector| vector.to_f32(VectorCoercionPolicy::Lossless),
                parse_f32_json_array,
            ),
            DataType::Float64 => self.build_fixed_size_list_column_typed::<Float64Type, _, _>(
                name,
                dim,
                vector_as_f64,
                parse_f64_json_array,
            ),
            other => Err(StorageError::Serialization(format!(
                "unsupported fixed-size-list inner type in Lance codec: {other:?}"
            ))),
        }
    }

    fn build_fixed_size_list_column_typed<T, FV, FP>(
        &self,
        name: &str,
        dim: usize,
        dense_values: FV,
        parse_field: FP,
    ) -> Result<ArrayRef, StorageError>
    where
        T: arrow_array::types::ArrowPrimitiveType,
        T::Native: Copy,
        FV: Fn(&DenseVector) -> Result<Vec<T::Native>, StorageError>,
        FP: Fn(&serde_json::Value, usize) -> Result<Vec<T::Native>, StorageError>,
    {
        let values = self
            .records
            .iter()
            .map(|record| -> Result<_, StorageError> {
                Ok(if let Some(vector) = record.vectors.get(name) {
                    Some(optional_values(exact_values(
                        name,
                        dense_values(vector)?,
                        dim,
                    )?))
                } else {
                    record
                        .fields
                        .get(name)
                        .map(|field| parse_field(field, dim).map(optional_values))
                        .transpose()?
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Arc::new(
            FixedSizeListArray::from_iter_primitive::<T, _, _>(values, dim as i32),
        ))
    }
}

fn exact_values<T>(name: &str, values: Vec<T>, dim: usize) -> Result<Vec<T>, StorageError> {
    if values.len() != dim {
        return Err(StorageError::InvalidConfig(format!(
            "Lance vector column `{name}` expected dimension {dim}, got {}",
            values.len()
        )));
    }
    Ok(values)
}

fn optional_values<T>(values: Vec<T>) -> Vec<Option<T>> {
    values.into_iter().map(Some).collect()
}
