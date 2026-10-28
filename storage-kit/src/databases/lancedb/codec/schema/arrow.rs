use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{CollectionSchema, FieldSchema, FieldType};

use super::vector::{
    LANCE_UTF8_FIELD_TYPE_METADATA_KEY, metadata_field_type_tag, vector_element_type,
};

pub(crate) fn schema_to_arrow(schema: &CollectionSchema) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields
            .iter()
            .map(field_schema_to_arrow)
            .collect::<Vec<_>>(),
    ))
}

fn field_schema_to_arrow(field: &FieldSchema) -> Field {
    let data_type = field_type_to_arrow_data_type(&field.field_type);

    let mut metadata: HashMap<String, String> = field.metadata.clone().into_iter().collect();
    if let Some(tag) = metadata_field_type_tag(&field.field_type) {
        metadata.insert(LANCE_UTF8_FIELD_TYPE_METADATA_KEY.into(), tag.into());
    }

    Field::new(&field.name, data_type, field.nullable).with_metadata(metadata)
}

pub(crate) fn field_type_to_arrow_data_type(field_type: &FieldType) -> DataType {
    match field_type {
        FieldType::Bool => DataType::Boolean,
        FieldType::I64 => DataType::Int64,
        FieldType::U64 => DataType::UInt64,
        FieldType::F64 => DataType::Float64,
        FieldType::String => DataType::Utf8,
        FieldType::Json => DataType::Utf8,
        FieldType::Binary => DataType::Binary,
        FieldType::DenseVector { dim, element } => DataType::FixedSizeList(
            Arc::new(Field::new("item", vector_element_type(element), true)),
            *dim as i32,
        ),
    }
}
