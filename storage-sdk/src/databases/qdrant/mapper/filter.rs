use serde_json::{Map, Value, json};

use crate::{ComparisonOp, FilterExpr, StorageError, UpdateAssignment};

pub(crate) fn compile_filter(filter: Option<&FilterExpr>) -> Result<Option<Value>, StorageError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    Ok(QdrantFilterPlan::from_expr(filter)?.map(QdrantFilterPlan::into_rest_json))
}

pub(crate) fn compile_filter_for_edge(
    filter: Option<&FilterExpr>,
) -> Result<Option<qdrant_edge::Filter>, StorageError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    QdrantFilterPlan::from_expr(filter)?
        .map(QdrantFilterPlan::into_edge_filter)
        .transpose()
}

pub(crate) fn literal_assignment(
    assignment: &UpdateAssignment,
) -> Result<(String, Value), StorageError> {
    Ok((assignment.column.clone(), assignment.value.clone()))
}

#[derive(Clone, Debug, PartialEq)]
struct QdrantFilterPlan(QdrantFilterClauses);

impl QdrantFilterPlan {
    fn from_expr(expr: &FilterExpr) -> Result<Option<Self>, StorageError> {
        Ok(match expr {
            FilterExpr::True => None,
            FilterExpr::False => return Err(false_filter_error()),
            FilterExpr::Compare { field, op, value } => match op {
                ComparisonOp::Eq => {
                    Some(Self::must(QdrantConditionPlan::match_value(field, value)?))
                }
                ComparisonOp::Ne => Some(Self::must_not(QdrantConditionPlan::match_value(
                    field, value,
                )?)),
                ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => {
                    Some(Self::must(QdrantConditionPlan::range_value(
                        field,
                        RangeBounds::single(field, RangeBound::from_comparison(*op), value)?,
                    )))
                }
            },
            FilterExpr::In { field, values } => {
                Some(Self::must(QdrantConditionPlan::any_value(field, values)?))
            }
            FilterExpr::Between {
                field,
                lower,
                upper,
            } => Some(Self::must(QdrantConditionPlan::range_value(
                field,
                RangeBounds::between(field, lower, upper)?,
            ))),
            FilterExpr::IsNull { field, negated } => {
                let condition = QdrantConditionPlan::IsNull {
                    field: field.clone(),
                };
                if *negated {
                    Some(Self::must_not(condition))
                } else {
                    Some(Self::must(condition))
                }
            }
            FilterExpr::Like {
                field,
                pattern,
                case_insensitive,
                negated,
            } => {
                return Err(StorageError::unsupported_capability(
                    "LIKE_FILTER",
                    format!(
                        "Qdrant LIKE filters are unsupported without a proven lowering for field `{field}`, pattern `{pattern}`, case_insensitive={case_insensitive}, negated={negated}"
                    ),
                ));
            }
            FilterExpr::And(items) => Self::all_of(items)?,
            FilterExpr::Or(items) => Self::any_of(items)?,
            FilterExpr::Not(item) => match Self::from_expr(item)?.map(Self::into_condition) {
                Some(condition) => Some(Self::must_not(condition)),
                None => return Err(false_filter_error()),
            },
        })
    }

    fn all_of(items: &[FilterExpr]) -> Result<Option<Self>, StorageError> {
        let mut clauses = QdrantFilterClauses::default();
        for item in items {
            if let Some(condition) = Self::from_expr(item)?.map(Self::into_condition) {
                clauses.must.push(condition);
            }
        }
        Ok(Self::from_clauses(clauses))
    }

    fn any_of(items: &[FilterExpr]) -> Result<Option<Self>, StorageError> {
        if items.is_empty() {
            return Err(false_filter_error());
        }

        let mut clauses = QdrantFilterClauses::default();
        for item in items {
            match Self::from_expr(item)?.map(Self::into_condition) {
                Some(condition) => clauses.should.push(condition),
                None => return Ok(None),
            }
        }
        Ok(Self::from_clauses(clauses))
    }

    fn must(condition: QdrantConditionPlan) -> Self {
        Self(QdrantFilterClauses {
            must: vec![condition],
            ..QdrantFilterClauses::default()
        })
    }

    fn must_not(condition: QdrantConditionPlan) -> Self {
        Self(QdrantFilterClauses {
            must_not: vec![condition],
            ..QdrantFilterClauses::default()
        })
    }

    fn from_clauses(clauses: QdrantFilterClauses) -> Option<Self> {
        if clauses.is_empty() {
            None
        } else {
            Some(Self(clauses))
        }
    }

    fn into_condition(self) -> QdrantConditionPlan {
        self.0.into_condition()
    }

    fn into_rest_json(self) -> Value {
        self.0.into_rest_json()
    }

    fn into_edge_filter(self) -> Result<qdrant_edge::Filter, StorageError> {
        self.0.into_edge_filter()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct QdrantFilterClauses {
    must: Vec<QdrantConditionPlan>,
    should: Vec<QdrantConditionPlan>,
    must_not: Vec<QdrantConditionPlan>,
}

impl QdrantFilterClauses {
    fn is_empty(&self) -> bool {
        self.must.is_empty() && self.should.is_empty() && self.must_not.is_empty()
    }

    fn into_condition(self) -> QdrantConditionPlan {
        let Self {
            mut must,
            should,
            must_not,
        } = self;
        if should.is_empty()
            && must_not.is_empty()
            && let Some(condition) = must.pop()
        {
            if must.is_empty() {
                return condition;
            }
            must.push(condition);
        }
        QdrantConditionPlan::Clauses(Box::new(Self {
            must,
            should,
            must_not,
        }))
    }

    fn into_rest_json(self) -> Value {
        let Self {
            must,
            should,
            must_not,
        } = self;
        let mut object = Map::new();
        for (key, conditions) in [("must", must), ("should", should), ("must_not", must_not)] {
            if !conditions.is_empty() {
                object.insert(
                    key.to_string(),
                    Value::Array(
                        conditions
                            .into_iter()
                            .map(QdrantConditionPlan::into_rest_json)
                            .collect(),
                    ),
                );
            }
        }
        Value::Object(object)
    }

    fn into_edge_filter(self) -> Result<qdrant_edge::Filter, StorageError> {
        Ok(qdrant_edge::Filter {
            must: Self::edge_conditions(self.must)?,
            should: Self::edge_conditions(self.should)?,
            must_not: Self::edge_conditions(self.must_not)?,
            ..qdrant_edge::Filter::default()
        })
    }

    fn edge_conditions(
        conditions: Vec<QdrantConditionPlan>,
    ) -> Result<Option<Vec<qdrant_edge::Condition>>, StorageError> {
        if conditions.is_empty() {
            Ok(None)
        } else {
            conditions
                .into_iter()
                .map(QdrantConditionPlan::into_edge_condition)
                .collect::<Result<Vec<_>, StorageError>>()
                .map(Some)
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum QdrantConditionPlan {
    Match {
        field: String,
        value: MatchValue,
    },
    Any {
        field: String,
        values: Vec<MatchValue>,
    },
    Range {
        field: String,
        bounds: RangeBounds,
    },
    IsNull {
        field: String,
    },
    Clauses(Box<QdrantFilterClauses>),
}

impl QdrantConditionPlan {
    fn match_value(field: &str, value: &Value) -> Result<Self, StorageError> {
        Ok(Self::Match {
            field: field.to_string(),
            value: MatchValue::from_json(field, value)?,
        })
    }

    fn any_value(field: &str, values: &[Value]) -> Result<Self, StorageError> {
        if values.is_empty() {
            return Err(false_filter_error());
        }
        Ok(Self::Any {
            field: field.to_string(),
            values: values
                .iter()
                .map(|value| MatchValue::from_json(field, value))
                .collect::<Result<Vec<_>, StorageError>>()?,
        })
    }

    fn range_value(field: &str, bounds: RangeBounds) -> Self {
        Self::Range {
            field: field.to_string(),
            bounds,
        }
    }

    fn into_rest_json(self) -> Value {
        match self {
            Self::Match { field, value } => {
                json!({ "key": field, "match": { "value": value.into_rest_json() } })
            }
            Self::Any { field, values } => json!({
                "key": field,
                "match": {
                    "any": values.into_iter().map(MatchValue::into_rest_json).collect::<Vec<_>>()
                }
            }),
            Self::Range { field, bounds } => {
                json!({ "key": field, "range": bounds.into_rest_json() })
            }
            Self::IsNull { field } => json!({ "is_null": { "key": field } }),
            Self::Clauses(clauses) => clauses.into_rest_json(),
        }
    }

    fn into_edge_condition(self) -> Result<qdrant_edge::Condition, StorageError> {
        Ok(match self {
            Self::Match { field, value } => {
                qdrant_edge::Condition::Field(qdrant_edge::FieldCondition::new_match(
                    edge_payload_key(&field)?,
                    qdrant_edge::Match::new_value(value.into_edge_value(&field)?),
                ))
            }
            Self::Any { field, values } => {
                qdrant_edge::Condition::Field(qdrant_edge::FieldCondition::new_match(
                    edge_payload_key(&field)?,
                    edge_any_match(&field, values)?,
                ))
            }
            Self::Range { field, bounds } => {
                qdrant_edge::Condition::Field(qdrant_edge::FieldCondition {
                    key: edge_payload_key(&field)?,
                    r#match: None,
                    range: Some(bounds.into_edge_range(&field)?),
                    geo_bounding_box: None,
                    geo_radius: None,
                    geo_polygon: None,
                    values_count: None,
                    is_empty: None,
                    is_null: None,
                })
            }
            Self::IsNull { field } => {
                qdrant_edge::Condition::IsNull(edge_payload_key(&field)?.into())
            }
            Self::Clauses(clauses) => qdrant_edge::Condition::Filter(clauses.into_edge_filter()?),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum MatchValue {
    String(String),
    Number(serde_json::Number),
    Bool(bool),
}

impl MatchValue {
    fn from_json(field: &str, value: &Value) -> Result<Self, StorageError> {
        match value {
            Value::String(value) => Ok(Self::String(value.clone())),
            Value::Number(value) => Ok(Self::Number(value.clone())),
            Value::Bool(value) => Ok(Self::Bool(*value)),
            Value::Null => Err(StorageError::unsupported_capability(
                "NULL_FILTER_VALUE",
                format!(
                    "Qdrant match filters cannot test null on field `{field}`; use FilterExpr::IsNull"
                ),
            )),
            Value::Array(_) | Value::Object(_) => Err(StorageError::unsupported_capability(
                "JSON_FILTER_VALUE",
                format!(
                    "Qdrant payload filters only support scalar match values for field `{field}`"
                ),
            )),
        }
    }

    fn from_range_json(field: &str, value: &Value) -> Result<Self, StorageError> {
        match value {
            Value::String(_) | Value::Number(_) => Self::from_json(field, value),
            Value::Null => Err(StorageError::unsupported_capability(
                "NULL_FILTER_VALUE",
                format!(
                    "Qdrant range filters cannot test null on field `{field}`; use FilterExpr::IsNull"
                ),
            )),
            Value::Array(_) | Value::Object(_) => Err(StorageError::unsupported_capability(
                "JSON_FILTER_VALUE",
                format!("Qdrant range filters only support scalar values for field `{field}`"),
            )),
            Value::Bool(_) => Err(StorageError::unsupported_capability(
                "FILTER_VALUE",
                format!("Qdrant range filters do not support boolean values for field `{field}`"),
            )),
        }
    }

    fn into_rest_json(self) -> Value {
        match self {
            Self::String(value) => Value::String(value),
            Self::Number(value) => Value::Number(value),
            Self::Bool(value) => Value::Bool(value),
        }
    }

    fn into_edge_value(self, field: &str) -> Result<qdrant_edge::ValueVariants, StorageError> {
        match self {
            Self::String(value) => Ok(qdrant_edge::ValueVariants::String(value)),
            Self::Number(value) => Ok(qdrant_edge::ValueVariants::Integer(edge_integer(
                field, &value,
            )?)),
            Self::Bool(value) => Ok(qdrant_edge::ValueVariants::Bool(value)),
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => value.as_f64(),
            Self::String(_) | Self::Bool(_) => None,
        }
    }

    fn as_datetime(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            Self::Number(_) | Self::Bool(_) => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct RangeBounds {
    lt: Option<MatchValue>,
    gt: Option<MatchValue>,
    gte: Option<MatchValue>,
    lte: Option<MatchValue>,
}

impl RangeBounds {
    fn single(field: &str, bound: RangeBound, value: &Value) -> Result<Self, StorageError> {
        let mut bounds = Self::default();
        bounds.set(field, bound, value)?;
        Ok(bounds)
    }

    fn between(field: &str, lower: &Value, upper: &Value) -> Result<Self, StorageError> {
        let mut bounds = Self::default();
        bounds.set(field, RangeBound::Gte, lower)?;
        bounds.set(field, RangeBound::Lte, upper)?;
        Ok(bounds)
    }

    fn set(&mut self, field: &str, bound: RangeBound, value: &Value) -> Result<(), StorageError> {
        let value = MatchValue::from_range_json(field, value)?;
        match bound {
            RangeBound::Lt => self.lt = Some(value),
            RangeBound::Gt => self.gt = Some(value),
            RangeBound::Gte => self.gte = Some(value),
            RangeBound::Lte => self.lte = Some(value),
        }
        Ok(())
    }

    fn into_rest_json(self) -> Value {
        let Self { lt, gt, gte, lte } = self;
        let mut object = Map::new();
        for (key, value) in [("lt", lt), ("gt", gt), ("gte", gte), ("lte", lte)] {
            if let Some(value) = value {
                object.insert(key.to_string(), value.into_rest_json());
            }
        }
        Value::Object(object)
    }

    fn into_edge_range(self, field: &str) -> Result<qdrant_edge::RangeInterface, StorageError> {
        if self.edge_uses_datetime(field)? {
            Ok(qdrant_edge::RangeInterface::DateTime(qdrant_edge::Range {
                lt: edge_datetime(field, self.lt.as_ref())?,
                gt: edge_datetime(field, self.gt.as_ref())?,
                gte: edge_datetime(field, self.gte.as_ref())?,
                lte: edge_datetime(field, self.lte.as_ref())?,
            }))
        } else {
            Ok(qdrant_edge::RangeInterface::Float(qdrant_edge::Range {
                lt: edge_float(self.lt.as_ref()),
                gt: edge_float(self.gt.as_ref()),
                gte: edge_float(self.gte.as_ref()),
                lte: edge_float(self.lte.as_ref()),
            }))
        }
    }

    fn edge_uses_datetime(&self, field: &str) -> Result<bool, StorageError> {
        let has_number = self
            .values()
            .any(|value| matches!(value, MatchValue::Number(_)));
        let has_datetime = self
            .values()
            .any(|value| matches!(value, MatchValue::String(_)));
        match (has_number, has_datetime) {
            (true, false) | (false, false) => Ok(false),
            (false, true) => Ok(true),
            (true, true) => Err(StorageError::unsupported_capability(
                "FILTER_VALUE",
                format!(
                    "qdrant-edge range filters require all bounds for field `{field}` to use the same numeric or datetime type"
                ),
            )),
        }
    }

    fn values(&self) -> impl Iterator<Item = &MatchValue> {
        [&self.lt, &self.gt, &self.gte, &self.lte]
            .into_iter()
            .flatten()
    }
}

#[derive(Clone, Copy, Debug)]
enum RangeBound {
    Lt,
    Gt,
    Gte,
    Lte,
}

impl RangeBound {
    fn from_comparison(op: ComparisonOp) -> Self {
        match op {
            ComparisonOp::Gt => Self::Gt,
            ComparisonOp::Gte => Self::Gte,
            ComparisonOp::Lt => Self::Lt,
            ComparisonOp::Lte => Self::Lte,
            ComparisonOp::Eq | ComparisonOp::Ne => {
                unreachable!("equality comparisons are lowered before range-bound conversion")
            }
        }
    }
}

fn false_filter_error() -> StorageError {
    StorageError::unsupported_capability(
        "FALSE_FILTER",
        "Qdrant does not expose a proven portable false-filter lowering",
    )
}

fn edge_any_match(
    field: &str,
    values: Vec<MatchValue>,
) -> Result<qdrant_edge::Match, StorageError> {
    let mut strings = Vec::new();
    let mut integers = Vec::new();
    for value in values {
        match value {
            MatchValue::String(value) if integers.is_empty() => strings.push(value),
            MatchValue::Number(value) if strings.is_empty() => {
                integers.push(edge_integer(field, &value)?)
            }
            MatchValue::Bool(_) => {
                return Err(StorageError::unsupported_capability(
                    "FILTER_VALUE",
                    format!(
                        "qdrant-edge `IN` filters do not support boolean values for field `{field}`"
                    ),
                ));
            }
            MatchValue::String(_) | MatchValue::Number(_) => {
                return Err(StorageError::unsupported_capability(
                    "FILTER_VALUE",
                    format!(
                        "qdrant-edge `IN` filters require all values for field `{field}` to have the same string or integer type"
                    ),
                ));
            }
        }
    }

    if strings.is_empty() {
        Ok(qdrant_edge::Match::from(integers))
    } else {
        Ok(qdrant_edge::Match::from(strings))
    }
}

fn edge_integer(field: &str, value: &serde_json::Number) -> Result<i64, StorageError> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .ok_or_else(|| {
            StorageError::unsupported_capability(
                "FILTER_VALUE",
                format!(
                    "qdrant-edge match filters only support integer numeric values for field `{field}`"
                ),
            )
        })
}

fn edge_float(
    value: Option<&MatchValue>,
) -> Option<qdrant_edge::external::ordered_float::OrderedFloat<f64>> {
    value
        .and_then(MatchValue::as_f64)
        .map(qdrant_edge::external::ordered_float::OrderedFloat)
}

fn edge_datetime(
    field: &str,
    value: Option<&MatchValue>,
) -> Result<Option<qdrant_edge::DateTimeWrapper>, StorageError> {
    value
        .and_then(MatchValue::as_datetime)
        .map(|value| {
            value.parse().map_err(|err| {
                StorageError::Serialization(format!(
                    "invalid qdrant-edge datetime range value for field `{field}`: {err}"
                ))
            })
        })
        .transpose()
}

fn edge_payload_key(field: &str) -> Result<qdrant_edge::JsonPath, StorageError> {
    field.parse().map_err(|err| {
        StorageError::Serialization(format!(
            "invalid Qdrant edge payload field path `{field}`: {err:?}"
        ))
    })
}
