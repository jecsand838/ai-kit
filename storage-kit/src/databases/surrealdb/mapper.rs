use serde_json::{Value, json};

use crate::{
    ComparisonOp, DataRecord, DenseVector, DistanceMetric, FieldType, FilterExpr, Projection,
    RecordId, StorageError, StorageLocator, SurrealModeConfig, VectorElementType,
};

pub(crate) fn endpoint_from_mode(mode: &SurrealModeConfig) -> String {
    match mode {
        SurrealModeConfig::Mem => "mem://".to_string(),
        SurrealModeConfig::RocksDb { path } => format!("rocksdb://{}", path.to_string_lossy()),
        SurrealModeConfig::SurrealKv { path } => {
            format!("surrealkv://{}", path.to_string_lossy())
        }
        SurrealModeConfig::RemoteWs { endpoint } | SurrealModeConfig::RemoteHttp { endpoint } => {
            normalize_remote_endpoint(endpoint)
        }
    }
}

fn normalize_remote_endpoint(endpoint: &str) -> String {
    endpoint.trim().to_string()
}

pub(crate) fn table_name(locator: &StorageLocator) -> String {
    locator.collection.clone()
}

pub(crate) fn quote_ident(ident: &str) -> String {
    if ident
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | ':' | '.'))
    {
        ident.to_string()
    } else {
        format!("⟨{}⟩", ident.replace(['⟨', '⟩'], ""))
    }
}

pub(crate) fn sanitize_identifier(raw: &str) -> Result<String, StorageError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(StorageError::InvalidConfig(
            "identifier cannot be empty".into(),
        ));
    }
    Ok(quote_ident(trimmed))
}

pub(crate) fn projection_clause(projection: &Projection) -> Result<String, StorageError> {
    match projection {
        Projection::All => Ok("*".into()),
        Projection::Columns(columns) if columns.is_empty() => {
            Err(StorageError::unsupported_capability(
                "EMPTY_PROJECTION",
                "SurrealDB empty column projections are unsupported",
            ))
        }
        Projection::Columns(columns) => Ok(columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")),
    }
}

pub(crate) fn compile_filter(filter: &FilterExpr) -> Result<String, StorageError> {
    match filter {
        FilterExpr::True => Ok("true".into()),
        FilterExpr::False => Ok("false".into()),
        FilterExpr::Compare { field, op, value } => Ok(format!(
            "{} {} {}",
            quote_ident(field),
            comparison_sql(*op),
            literal_sql(value)?
        )),
        FilterExpr::In { field, values } => {
            let joined = values
                .iter()
                .map(literal_sql)
                .collect::<Result<Vec<_>, StorageError>>()?
                .join(", ");
            Ok(format!("{} INSIDE [{}]", quote_ident(field), joined))
        }
        FilterExpr::Between {
            field,
            lower,
            upper,
        } => Ok(format!(
            "{} >= {} AND {} <= {}",
            quote_ident(field),
            literal_sql(lower)?,
            quote_ident(field),
            literal_sql(upper)?
        )),
        FilterExpr::IsNull { field, negated } => {
            if *negated {
                Ok(format!("{} != NONE", quote_ident(field)))
            } else {
                Ok(format!("{} = NONE", quote_ident(field)))
            }
        }
        FilterExpr::Like {
            field,
            pattern,
            case_insensitive,
            negated,
        } => {
            let predicate = format!(
                "string::matches({}, /{}/)",
                quote_ident(field),
                like_pattern_regex(pattern, *case_insensitive)
            );
            if *negated {
                Ok(format!("NOT ({predicate})"))
            } else {
                Ok(predicate)
            }
        }
        FilterExpr::And(items) => parenthesize_join("AND", items),
        FilterExpr::Or(items) => parenthesize_join("OR", items),
        FilterExpr::Not(item) => Ok(format!("NOT ({})", compile_filter(item)?)),
    }
}

pub(crate) fn where_clause(filter: Option<&FilterExpr>) -> Result<Option<String>, StorageError> {
    filter.map(compile_filter).transpose()
}

pub(crate) fn limit_offset_clause(limit: Option<usize>, offset: Option<usize>) -> String {
    let mut parts = Vec::new();
    if let Some(limit) = limit {
        parts.push(format!("LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        parts.push(format!("START {offset}"));
    }
    parts.join(" ")
}

pub(crate) fn field_type_sql(
    field_type: &FieldType,
    nullable: bool,
) -> Result<String, StorageError> {
    let base = match field_type {
        FieldType::Bool => "bool".into(),
        FieldType::I64 => "int".into(),
        FieldType::U64 => "int".into(),
        FieldType::F64 => "float".into(),
        FieldType::String => "string".into(),
        FieldType::Json => "object".into(),
        FieldType::DenseVector { dim, element } => {
            format!("array<{}, {}>", element_type_sql(element), dim)
        }
        FieldType::Binary => "bytes".into(),
    };
    if nullable {
        Ok(format!("option<{base}>"))
    } else {
        Ok(base)
    }
}

fn element_type_sql(element: &VectorElementType) -> &'static str {
    match element {
        VectorElementType::F16 | VectorElementType::F32 => "float",
        VectorElementType::F64 => "float",
        VectorElementType::I16 | VectorElementType::I32 | VectorElementType::I64 => "int",
    }
}

pub(crate) fn vector_metric_token(metric: Option<DistanceMetric>) -> &'static str {
    match metric.unwrap_or(DistanceMetric::Cosine) {
        DistanceMetric::L2 => "EUCLIDEAN",
        DistanceMetric::Cosine => "COSINE",
        DistanceMetric::Hamming => "HAMMING",
    }
}

pub(crate) fn dense_vector_to_json(vector: &DenseVector) -> Value {
    match vector {
        DenseVector::F32(values) => json!(values),
        DenseVector::F64(values) => json!(values),
        DenseVector::I16(values) => json!(values),
        DenseVector::I32(values) => json!(values),
        DenseVector::I64(values) => json!(values),
    }
}

pub(crate) fn record_id_to_value(id: &RecordId) -> Value {
    match id {
        RecordId::String(v) | RecordId::Opaque(v) => Value::String(v.clone()),
        RecordId::Uuid(v) => Value::String(v.to_string()),
        RecordId::U64(v) => json!(v),
        RecordId::I64(v) => json!(v),
    }
}

fn comparison_sql(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Ne => "!=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
    }
}

fn parenthesize_join(op: &str, items: &[FilterExpr]) -> Result<String, StorageError> {
    let inner = items
        .iter()
        .map(compile_filter)
        .collect::<Result<Vec<_>, StorageError>>()?
        .join(&format!(" {op} "));
    Ok(format!("({inner})"))
}

fn like_pattern_regex(pattern: &str, case_insensitive: bool) -> String {
    let mut regex = String::from("(?s");
    if case_insensitive {
        regex.push('i');
    }
    regex.push(')');
    regex.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' | '/' | '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$'
            | '|' => {
                regex.push('\\');
                regex.push(ch);
            }
            other => regex.push(other),
        }
    }
    regex.push('$');
    regex
}

fn literal_sql(value: &Value) -> Result<String, StorageError> {
    match value {
        Value::Null => Ok("NONE".into()),
        Value::Bool(v) => Ok(v.to_string()),
        Value::Number(v) => Ok(v.to_string()),
        Value::String(v) => Ok(format!("'{}'", v.replace('\'', "\\'"))),
        Value::Array(_) | Value::Object(_) => Err(StorageError::unsupported_capability(
            "JSON_FILTER_VALUE",
            format!("SurrealDB filter literals do not support JSON array/object values: {value}"),
        )),
    }
}

pub(crate) fn data_record_to_value(record: &DataRecord) -> Result<Value, StorageError> {
    let mut object = record.fields.clone();
    for (name, vector) in &record.vectors {
        object.insert(name.clone(), dense_vector_to_json(vector));
    }
    Ok(Value::Object(object))
}
