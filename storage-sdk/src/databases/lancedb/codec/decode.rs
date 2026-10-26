use arrow_array::types::{ArrowPrimitiveType, Float32Type, Float64Type};
use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array,
    PrimitiveArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field};

use crate::{CollectionSchema, DataRecord, DenseVector, FieldSchema, RecordsPage, StorageError};

use super::schema::{LANCE_UTF8_FIELD_TYPE_METADATA_KEY, field_type_from_arrow};

pub(crate) fn decode_batches(batches: &[RecordBatch]) -> Result<RecordsPage, StorageError> {
    let mut out = Vec::new();
    for batch in batches {
        out.extend(decode_batch(batch)?);
    }
    Ok(RecordsPage {
        records: out,
        total: None,
        next_page: None,
    })
}

pub(crate) fn collection_schema_from_arrow(schema: &arrow_schema::Schema) -> CollectionSchema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| FieldSchema {
            name: field.name().to_string(),
            field_type: field_type_from_arrow(field.as_ref()),
            nullable: field.is_nullable(),
            metadata: field
                .metadata()
                .iter()
                .filter(|(key, _)| key.as_str() != LANCE_UTF8_FIELD_TYPE_METADATA_KEY)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
        .collect();

    CollectionSchema {
        fields,
        metadata: schema
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

fn decode_batch(batch: &RecordBatch) -> Result<Vec<DataRecord>, StorageError> {
    BatchDecoder::new(batch).decode()
}

struct BatchDecoder<'a> {
    batch: &'a RecordBatch,
    records: Vec<DataRecord>,
}

impl<'a> BatchDecoder<'a> {
    fn new(batch: &'a RecordBatch) -> Self {
        Self {
            batch,
            records: vec![DataRecord::default(); batch.num_rows()],
        }
    }

    fn decode(mut self) -> Result<Vec<DataRecord>, StorageError> {
        let schema = self.batch.schema();
        for (index, field) in schema.fields().iter().enumerate() {
            let column = self.batch.column(index).clone();

            match field.data_type() {
                DataType::Utf8 => self.decode_utf8_field(field.as_ref(), column.as_ref())?,
                DataType::Boolean => self.decode_scalar_field::<BooleanArray, _>(
                    field.name(),
                    column.as_ref(),
                    "Boolean",
                    |array, row| serde_json::Value::Bool(array.value(row)),
                )?,
                DataType::Int32 => self.decode_scalar_field::<Int32Array, _>(
                    field.name(),
                    column.as_ref(),
                    "Int32",
                    |array, row| serde_json::Value::Number((array.value(row) as i64).into()),
                )?,
                DataType::Int64 => self.decode_scalar_field::<Int64Array, _>(
                    field.name(),
                    column.as_ref(),
                    "Int64",
                    |array, row| serde_json::Value::Number(array.value(row).into()),
                )?,
                DataType::UInt32 => self.decode_scalar_field::<UInt32Array, _>(
                    field.name(),
                    column.as_ref(),
                    "UInt32",
                    |array, row| serde_json::Value::Number((array.value(row) as u64).into()),
                )?,
                DataType::UInt64 => self.decode_scalar_field::<UInt64Array, _>(
                    field.name(),
                    column.as_ref(),
                    "UInt64",
                    |array, row| serde_json::Value::Number(array.value(row).into()),
                )?,
                DataType::Float32 => self.decode_scalar_field::<Float32Array, _>(
                    field.name(),
                    column.as_ref(),
                    "Float32",
                    |array, row| serde_json::Value::from(array.value(row) as f64),
                )?,
                DataType::Float64 => self.decode_scalar_field::<Float64Array, _>(
                    field.name(),
                    column.as_ref(),
                    "Float64",
                    |array, row| serde_json::Value::from(array.value(row)),
                )?,
                DataType::FixedSizeList(inner, _dim)
                    if matches!(inner.data_type(), DataType::Float32) =>
                {
                    self.decode_dense_vector::<Float32Type, _>(
                        field.name(),
                        column.as_ref(),
                        "Float32",
                        DenseVector::F32,
                    )?;
                }
                DataType::FixedSizeList(inner, _dim)
                    if matches!(inner.data_type(), DataType::Float64) =>
                {
                    self.decode_dense_vector::<Float64Type, _>(
                        field.name(),
                        column.as_ref(),
                        "Float64",
                        DenseVector::F64,
                    )?;
                }
                _ => {
                    return Err(StorageError::Serialization(format!(
                        "unsupported Lance column `{}` with type `{}` during decode",
                        field.name(),
                        field.data_type()
                    )));
                }
            }
        }
        Ok(self.records)
    }

    fn decode_utf8_field(&mut self, field: &Field, column: &dyn Array) -> Result<(), StorageError> {
        let array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| StorageError::Serialization("expected Utf8 column".into()))?;
        for (row, record) in self
            .records
            .iter_mut()
            .enumerate()
            .take(self.batch.num_rows())
        {
            if array.is_null(row) {
                continue;
            }
            let value = array.value(row);
            match field_type_from_arrow(field) {
                crate::FieldType::String => {
                    record.fields.insert(
                        field.name().to_string(),
                        serde_json::Value::String(value.to_string()),
                    );
                }
                crate::FieldType::Json => {
                    let json = serde_json::from_str::<serde_json::Value>(value).map_err(|err| {
                        StorageError::Serialization(format!(
                            "invalid JSON payload for Lance field `{}`: {err}",
                            field.name()
                        ))
                    })?;
                    record.fields.insert(field.name().to_string(), json);
                }
                _ => {
                    record.fields.insert(
                        field.name().to_string(),
                        serde_json::Value::String(value.to_string()),
                    );
                }
            }
        }
        Ok(())
    }

    fn decode_scalar_field<A, F>(
        &mut self,
        field_name: &str,
        column: &dyn Array,
        expected: &'static str,
        to_json: F,
    ) -> Result<(), StorageError>
    where
        A: Array + 'static,
        F: Fn(&A, usize) -> serde_json::Value,
    {
        let array = column
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| StorageError::Serialization(format!("expected {expected} column")))?;
        for (row, record) in self
            .records
            .iter_mut()
            .enumerate()
            .take(self.batch.num_rows())
        {
            if !array.is_null(row) {
                record
                    .fields
                    .insert(field_name.to_owned(), to_json(array, row));
            }
        }
        Ok(())
    }

    fn decode_dense_vector<T, F>(
        &mut self,
        field_name: &str,
        column: &dyn Array,
        expected: &'static str,
        build: F,
    ) -> Result<(), StorageError>
    where
        T: ArrowPrimitiveType,
        T::Native: Copy,
        F: Fn(Vec<T::Native>) -> DenseVector,
    {
        let array = column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| StorageError::Serialization("expected FixedSizeList column".into()))?;
        for (row, record) in self.records.iter_mut().enumerate() {
            if array.is_null(row) {
                continue;
            }
            let row_values = array.value(row);
            let values = row_values
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or_else(|| {
                    StorageError::Serialization(format!("expected {expected} child array"))
                })?;
            let dense = (0..values.len()).map(|idx| values.value(idx)).collect();
            record.vectors.insert(field_name.to_owned(), build(dense));
        }
        Ok(())
    }
}
