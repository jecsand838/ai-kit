use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Portable filter expression tree used across data and query requests.
///
/// Operator acceptance depends on the backend and field type. In particular,
/// supplied LanceDB Json fields support scalar equality, non-equality, and
/// membership; range and pattern predicates on Json fields are unsupported.
/// A filtering capability bit does not override these request checks.
///
/// # Examples
///
/// ```
/// let filter = storage_sdk::database::data::FilterExpr::eq("status", "published")
///     .and(storage_sdk::database::data::FilterExpr::Compare {
///         field: "views".into(),
///         op: storage_sdk::database::data::ComparisonOp::Gte,
///         value: serde_json::json!(10),
///     });
/// assert!(matches!(filter, storage_sdk::database::data::FilterExpr::And(_)));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterExpr {
    /// Matches every record when the backend exposes a native true predicate.
    True,
    /// Matches no records when the backend exposes a native false predicate.
    False,
    /// Binary comparison against a single field.
    Compare {
        /// Field path to compare.
        field: String,
        /// Comparison operator.
        op: ComparisonOp,
        /// Comparison value. Backend support is limited to documented payload
        /// scalar kinds for the selected comparison operator.
        value: Value,
    },
    /// Matches records whose field value appears in the supplied candidate list.
    In {
        /// Field path to compare.
        field: String,
        /// Candidate values. Backends may reject values outside their documented
        /// scalar match surface.
        values: Vec<Value>,
    },
    /// Matches records whose field value falls inside the inclusive range.
    Between {
        /// Field path to compare.
        field: String,
        /// Lower inclusive bound.
        lower: Value,
        /// Upper inclusive bound.
        upper: Value,
    },
    /// Matches records whose field value is `NULL`, or not `NULL` when negated.
    IsNull {
        /// Field path to compare.
        field: String,
        /// When `true`, invert the predicate into `IS NOT NULL`.
        negated: bool,
    },
    /// String pattern match.
    Like {
        /// Field path to compare.
        field: String,
        /// Pattern string interpreted by the backend SQL dialect.
        pattern: String,
        /// Whether matching should ignore case.
        case_insensitive: bool,
        /// When `true`, invert the predicate.
        negated: bool,
    },
    /// Conjunction of child expressions.
    And(Vec<FilterExpr>),
    /// Disjunction of child expressions.
    Or(Vec<FilterExpr>),
    /// Negation of a child expression.
    Not(Box<FilterExpr>),
}

/// Comparison operator used in [`FilterExpr::Compare`].
///
/// # Examples
///
/// ```
/// let op = storage_sdk::database::data::ComparisonOp::Gte;
/// assert!(matches!(op, storage_sdk::database::data::ComparisonOp::Gte));
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ComparisonOp {
    /// Requires the field value to equal the supplied comparison value.
    Eq,
    /// Requires the field value to differ from the supplied comparison value.
    Ne,
    /// Requires the field value to be greater than the supplied comparison value.
    Gt,
    /// Requires the field value to be greater than or equal to the supplied comparison value.
    Gte,
    /// Requires the field value to be less than the supplied comparison value.
    Lt,
    /// Requires the field value to be less than or equal to the supplied comparison value.
    Lte,
}

impl FilterExpr {
    /// Creates an equality comparison.
    ///
    /// # Examples
    ///
    /// ```
    /// let filter = storage_sdk::database::data::FilterExpr::eq("status", "published");
    /// assert!(matches!(filter, storage_sdk::database::data::FilterExpr::Compare { .. }));
    /// ```
    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Compare {
            field: field.into(),
            op: ComparisonOp::Eq,
            value: value.into(),
        }
    }

    /// Appends `rhs` to an `AND` expression, flattening nested conjunctions.
    ///
    /// # Examples
    ///
    /// ```
    /// let filter = storage_sdk::database::data::FilterExpr::eq("tenant", "acme")
    ///     .and(storage_sdk::database::data::FilterExpr::eq("status", "published"));
    /// assert!(matches!(filter, storage_sdk::database::data::FilterExpr::And(_)));
    /// ```
    pub fn and(self, rhs: FilterExpr) -> Self {
        match self {
            FilterExpr::And(mut terms) => {
                terms.push(rhs);
                FilterExpr::And(terms)
            }
            lhs => FilterExpr::And(vec![lhs, rhs]),
        }
    }
}
