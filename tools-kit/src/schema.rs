//! JSON Schema declarations, rendering preferences, and validation diagnostics.
//!
//! [`JsonSchema`](crate::schema::JsonSchema) is a passive wrapper. Owning execution boundaries validate
//! arguments and successful structured output; strictness describes rendering
//! preference and does not turn an invalid schema into a valid one.

use serde::{Deserialize, Serialize};

use crate::ToolError;

/// Rendering strictness for a JSON Schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaStrictness {
    /// Require exact schema adherence where the source supports it.
    Strict,
    /// Request schema adherence without requiring source enforcement.
    BestEffort,
    /// Apply no portable strictness preference.
    Unspecified,
}

/// JSON Schema plus portable strictness metadata.
/// Construction wraps the document without compiling it. The owning Local/MCP
/// operation validates schemas and values at its operation boundary.
///
/// # Examples
///
/// ```
/// use tools_kit::schema::{JsonSchema, SchemaStrictness};
/// let schema = JsonSchema::strict(serde_json::json!({"type":"object"}));
/// assert_eq!(schema.schema["type"], "object");
/// assert_eq!(schema.strictness, SchemaStrictness::Strict);
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonSchema {
    /// JSON Schema document.
    pub schema: serde_json::Value,
    /// Portable strictness requested for the schema.
    pub strictness: SchemaStrictness,
}

impl JsonSchema {
    /// Wraps a schema with strict rendering semantics.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::schema::{JsonSchema, SchemaStrictness};
    /// let schema = JsonSchema::strict(serde_json::json!({"type":"object"}));
    /// assert_eq!(schema.schema["type"], "object");
    /// assert_eq!(schema.strictness, SchemaStrictness::Strict);
    /// ```
    ///
    #[must_use]
    pub fn strict(schema: serde_json::Value) -> Self {
        Self {
            schema,
            strictness: SchemaStrictness::Strict,
        }
    }

    /// Wraps a schema with best-effort rendering semantics.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::schema::{JsonSchema, SchemaStrictness};
    /// let schema = JsonSchema::best_effort(serde_json::json!({"type":"object"}));
    /// assert_eq!(schema.schema["type"], "object");
    /// assert_eq!(schema.strictness, SchemaStrictness::BestEffort);
    /// ```
    ///
    #[must_use]
    pub fn best_effort(schema: serde_json::Value) -> Self {
        Self {
            schema,
            strictness: SchemaStrictness::BestEffort,
        }
    }

    /// Wraps a schema without a strictness preference.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::schema::{JsonSchema, SchemaStrictness};
    /// let schema = JsonSchema::unspecified(serde_json::json!({"type":"object"}));
    /// assert_eq!(schema.schema["type"], "object");
    /// assert_eq!(schema.strictness, SchemaStrictness::Unspecified);
    /// ```
    ///
    #[must_use]
    pub fn unspecified(schema: serde_json::Value) -> Self {
        Self {
            schema,
            strictness: SchemaStrictness::Unspecified,
        }
    }

    pub(crate) fn validate(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<ValidationIssue>, ToolError> {
        Self::validate_raw(&self.schema, value)
    }

    pub(crate) fn validate_raw(
        schema: &serde_json::Value,
        value: &serde_json::Value,
    ) -> Result<Vec<ValidationIssue>, ToolError> {
        let validator = jsonschema::validator_for(schema).map_err(|error| ToolError::Protocol {
            message: format!("invalid JSON Schema: {error}"),
        })?;
        Ok(validator
            .iter_errors(value)
            .map(|error| ValidationIssue {
                path: error.instance_path().to_string(),
                message: error.to_string(),
            })
            .collect())
    }
}

/// Structured validation diagnostic.
///
/// # Examples
///
/// ```
/// let issue = tools_kit::schema::ValidationIssue { path: "/city".into(), message: "expected a string".into() };
/// assert_eq!(issue.path, "/city");
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationIssue {
    /// JSON Pointer-like path to the invalid value.
    pub path: String,
    /// Caller-safe description of the validation failure.
    pub message: String,
}
