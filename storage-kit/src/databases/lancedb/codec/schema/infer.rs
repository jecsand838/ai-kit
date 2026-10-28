use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{DataRecord, FieldType, StorageError};

use super::vector::{
    LANCE_UTF8_FIELD_TYPE_METADATA_KEY, dense_arrow_type, json_value_type, metadata_field_type_tag,
};

pub(crate) fn infer_arrow_schema(records: &[DataRecord]) -> Result<SchemaRef, StorageError> {
    let mut scalar_fields: BTreeMap<String, (DataType, Option<FieldType>)> = BTreeMap::new();
    let mut vector_dims: BTreeMap<String, usize> = BTreeMap::new();
    let mut vector_types: BTreeMap<String, DataType> = BTreeMap::new();

    for record in records {
        for (name, dense) in &record.vectors {
            let dim = dense.len();
            match vector_dims.get(name) {
                Some(existing) if *existing != dim => {
                    return Err(StorageError::InvalidConfig(format!(
                        "Lance dense vector column `{name}` has inconsistent dimensions"
                    )));
                }
                Some(_) => {}
                None => {
                    vector_dims.insert(name.clone(), dim);
                }
            }
            vector_types
                .entry(name.clone())
                .or_insert_with(|| dense_arrow_type(dense));
        }

        for (name, value) in &record.fields {
            let field_type = inferred_utf8_field_type(value);
            let data_type = json_value_type(value);
            scalar_fields
                .entry(name.clone())
                .and_modify(|existing| {
                    if existing.0 == DataType::Utf8 && matches!(field_type, Some(FieldType::Json)) {
                        existing.1 = Some(FieldType::Json);
                    } else if existing.1.is_none() {
                        existing.1 = field_type.clone();
                    }
                })
                .or_insert((data_type, field_type));
        }
    }

    let mut fields = Vec::new();

    for (name, element_type) in &vector_types {
        let dim = *vector_dims.get(name).unwrap_or(&0);
        fields.push(Field::new(
            name,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", element_type.clone(), true)),
                dim as i32,
            ),
            true,
        ));
    }

    for (name, (data_type, field_type)) in scalar_fields {
        if vector_types.contains_key(&name) {
            continue;
        }
        let mut metadata = HashMap::new();
        if let Some(field_type) = field_type.as_ref()
            && let Some(tag) = metadata_field_type_tag(field_type)
        {
            metadata.insert(LANCE_UTF8_FIELD_TYPE_METADATA_KEY.into(), tag.into());
        }
        fields.push(Field::new(&name, data_type, true).with_metadata(metadata));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn inferred_utf8_field_type(value: &serde_json::Value) -> Option<FieldType> {
    match value {
        serde_json::Value::String(_) => Some(FieldType::String),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) | serde_json::Value::Null => {
            Some(FieldType::Json)
        }
        _ => None,
    }
}
