use crate::{
    ComparisonOp, DistanceMetric, FilterExpr, IndexKind, Projection, StorageError,
    UpdateAssignment, VectorSearchParams,
};

use lancedb::{
    DistanceType,
    index::{
        Index,
        scalar::{BTreeIndexBuilder, FtsIndexBuilder},
        vector::IvfFlatIndexBuilder,
    },
    query::{QueryBase, Select},
};

pub(crate) struct CommonQueryOptions<'a> {
    pub(crate) projection: &'a Projection,
    pub(crate) filter_sql: Option<&'a str>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

pub(crate) fn projection_to_select(projection: &Projection) -> Option<Select> {
    match projection {
        Projection::All => None,
        Projection::Columns(columns) => Some(Select::columns(
            &columns.iter().map(String::as_str).collect::<Vec<_>>(),
        )),
    }
}

pub(crate) fn apply_common_query_options<Q>(mut query: Q, options: CommonQueryOptions<'_>) -> Q
where
    Q: QueryBase,
{
    if let Some(filter) = options.filter_sql {
        query = query.only_if(filter);
    }

    if let Some(select) = projection_to_select(options.projection) {
        query = query.select(select);
    }

    if let Some(limit) = options.limit {
        query = query.limit(limit);
    }

    if let Some(offset) = options.offset {
        query = query.offset(offset);
    }

    query
}

pub(crate) fn apply_vector_options(
    query: lancedb::query::VectorQuery,
    params: &VectorSearchParams,
    filter_sql: Option<&str>,
    projection: &Projection,
) -> Result<lancedb::query::VectorQuery, StorageError> {
    LanceVectorParams { params }.apply(query, filter_sql, projection)
}

pub(crate) struct LanceVectorParams<'a> {
    params: &'a VectorSearchParams,
}

impl LanceVectorParams<'_> {
    fn apply(
        self,
        mut query: lancedb::query::VectorQuery,
        filter_sql: Option<&str>,
        projection: &Projection,
    ) -> Result<lancedb::query::VectorQuery, StorageError> {
        let params = self.params;
        query = apply_common_query_options(
            query,
            CommonQueryOptions {
                projection,
                filter_sql,
                limit: Some(params.limit),
                offset: params.offset,
            },
        );

        if let Some(metric) = params.distance {
            query = query.distance_type(distance_type(metric));
        }
        if let Some(ef) = params.ef {
            query = query.ef(ef);
        }
        Ok(query)
    }
}

pub(crate) struct LanceExpressions {
    schema: arrow_schema::SchemaRef,
}

impl LanceExpressions {
    pub(crate) fn new(schema: arrow_schema::SchemaRef) -> Self {
        Self { schema }
    }

    pub(crate) fn compile(&self, filter: &FilterExpr) -> Result<String, StorageError> {
        match filter {
            FilterExpr::True => Ok("TRUE".into()),
            FilterExpr::False => Ok("FALSE".into()),
            FilterExpr::Compare { field, op, value } => {
                let literal = if matches!(op, ComparisonOp::Eq | ComparisonOp::Ne) {
                    self.literal(field, value)?
                } else {
                    self.require_native_scalar(field)?;
                    literal_sql(value)?
                };
                Ok(format!(
                    "{} {} {literal}",
                    quote_ident(field),
                    comparison_sql(*op),
                ))
            }
            FilterExpr::In { field, values } => {
                let joined = values
                    .iter()
                    .map(|value| self.literal(field, value))
                    .collect::<Result<Vec<_>, StorageError>>()?
                    .join(", ");
                Ok(format!("{} IN ({joined})", quote_ident(field)))
            }
            FilterExpr::Between {
                field,
                lower,
                upper,
            } => {
                self.require_native_scalar(field)?;
                Ok(format!(
                    "{} BETWEEN {} AND {}",
                    quote_ident(field),
                    literal_sql(lower)?,
                    literal_sql(upper)?
                ))
            }
            FilterExpr::IsNull { field, negated } => Ok(format!(
                "{} IS {}NULL",
                quote_ident(field),
                if *negated { "NOT " } else { "" }
            )),
            FilterExpr::Like {
                field,
                pattern,
                case_insensitive,
                negated,
            } => {
                self.require_native_scalar(field)?;
                let op = if *case_insensitive { "ILIKE" } else { "LIKE" };
                let not = if *negated { "NOT " } else { "" };
                Ok(format!(
                    "{} {}{} {}",
                    quote_ident(field),
                    not,
                    op,
                    literal_sql(&serde_json::Value::String(pattern.clone()))?
                ))
            }
            FilterExpr::And(items) => self.join("AND", items),
            FilterExpr::Or(items) => self.join("OR", items),
            FilterExpr::Not(item) => Ok(format!("NOT ({})", self.compile(item)?)),
        }
    }

    pub(crate) fn optional(
        &self,
        filter: Option<&FilterExpr>,
    ) -> Result<Option<String>, StorageError> {
        filter.map(|filter| self.compile(filter)).transpose()
    }

    pub(crate) fn assignment(&self, assignment: &UpdateAssignment) -> Result<String, StorageError> {
        self.literal(&assignment.column, &assignment.value)
    }

    fn literal(&self, name: &str, value: &serde_json::Value) -> Result<String, StorageError> {
        let literal = literal_sql(value)?;
        if self.is_json(name) {
            let encoded = serde_json::to_string(value)
                .map_err(|error| StorageError::Serialization(error.to_string()))?;
            literal_sql(&serde_json::Value::String(encoded))
        } else {
            Ok(literal)
        }
    }

    fn is_json(&self, name: &str) -> bool {
        self.schema
            .field_with_name(name)
            .is_ok_and(|field| super::codec::field_type_from_arrow(field) == crate::FieldType::Json)
    }

    fn require_native_scalar(&self, name: &str) -> Result<(), StorageError> {
        if self.is_json(name) {
            return Err(StorageError::unsupported_capability(
                "JSON_RANGE_PATTERN_FILTER",
                "LanceDB does not support portable range or pattern predicates on Json fields",
            ));
        }
        Ok(())
    }

    fn join(&self, op: &str, items: &[FilterExpr]) -> Result<String, StorageError> {
        let inner = items
            .iter()
            .map(|filter| self.compile(filter))
            .collect::<Result<Vec<_>, StorageError>>()?
            .join(&format!(" {op} "));
        Ok(format!("({inner})"))
    }
}

pub(crate) fn distance_type(metric: DistanceMetric) -> DistanceType {
    match metric {
        DistanceMetric::L2 => DistanceType::L2,
        DistanceMetric::Cosine => DistanceType::Cosine,
        DistanceMetric::Hamming => DistanceType::Hamming,
    }
}

pub(crate) fn index_kind(kind: &IndexKind) -> Result<Index, StorageError> {
    match kind {
        IndexKind::Scalar => Ok(Index::BTree(BTreeIndexBuilder::default())),
        IndexKind::FullText => Ok(Index::FTS(
            FtsIndexBuilder::default().base_tokenizer("simple".to_owned()),
        )),
        IndexKind::Vector {
            dimension,
            distance,
        } => {
            if *dimension == 0 {
                return Err(StorageError::InvalidConfig(
                    "vector index dimension must be positive".into(),
                ));
            }
            let builder = match distance {
                Some(metric) => {
                    IvfFlatIndexBuilder::default().distance_type(distance_type(*metric))
                }
                None => IvfFlatIndexBuilder::default(),
            };
            Ok(Index::IvfFlat(builder))
        }
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

fn quote_ident(field: &str) -> String {
    if field
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        field.to_string()
    } else {
        format!("\"{}\"", field.replace('"', "\"\""))
    }
}

fn literal_sql(value: &serde_json::Value) -> Result<String, StorageError> {
    match value {
        serde_json::Value::Null => Ok("NULL".into()),
        serde_json::Value::Bool(v) => {
            if *v {
                Ok("TRUE".into())
            } else {
                Ok("FALSE".into())
            }
        }
        serde_json::Value::Number(v) => Ok(v.to_string()),
        serde_json::Value::String(v) => Ok(format!("'{}'", v.replace('\'', "''"))),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(StorageError::unsupported_capability(
                "JSON_FILTER_VALUE",
                format!("Lance filter literals do not support JSON array/object values: {value}"),
            ))
        }
    }
}
