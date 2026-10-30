//! Tool identities and complete executable declarations.
//!
//! [`ToolInfo`](crate::declaration::ToolInfo) carries schemas, interaction capabilities and model-visible metadata.
//! [`ToolName`](crate::declaration::ToolName) is the rendered identity; native MCP routing stays private.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use crate::name::ToolName;

use crate::{
    ToolError,
    execution::ToolResult,
    interaction::ToolInteractionCapabilities,
    schema::{JsonSchema, ValidationIssue},
};

/// Complete source-neutral declaration for one executable tool.
/// A declared output schema requires structured content on every successful result.
/// Missing content fails; explicit JSON null is validated against the schema.
/// Model-visible error results (`is_error = true`) are exempt from output-schema validation.
///
/// # Examples
///
/// ```
/// use tools_kit::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
/// let info = ToolInfo::new(
///     ToolName::new("ping")?,
///     JsonSchema::strict(serde_json::json!({"type":"object"})),
/// );
/// assert!(info.output_schema.is_none());
///
/// # Ok::<(), tools_kit::ToolError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInfo {
    /// Stable caller-facing name.
    pub name: ToolName,
    /// Optional display label.
    pub title: Option<String>,
    /// Model-visible behavior description.
    pub description: Option<String>,
    /// JSON Schema for invocation arguments.
    pub input_schema: JsonSchema,
    /// Optional JSON Schema for structured output.
    pub output_schema: Option<JsonSchema>,
    /// Portable behavior hints.
    pub annotations: ToolAnnotations,
    /// Optional visual metadata.
    pub icons: Vec<ToolIcon>,
    /// Supported source-neutral caller interactions.
    pub interactions: ToolInteractionCapabilities,
    /// Uninterpreted source-neutral metadata.
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl ToolInfo {
    /// Creates a complete declaration with no optional metadata.
    ///
    /// See [`crate::declaration::ToolInfo`] for a complete example.
    ///
    #[must_use]
    pub fn new(name: ToolName, input_schema: JsonSchema) -> Self {
        Self {
            name,
            title: None,
            description: None,
            input_schema,
            output_schema: None,
            annotations: ToolAnnotations::default(),
            icons: Vec::new(),
            interactions: ToolInteractionCapabilities::default(),
            metadata: BTreeMap::new(),
        }
    }

    pub(crate) fn validate_output(&self, result: &ToolResult) -> Result<(), ToolError> {
        if result.is_error {
            return Ok(());
        }
        let Some(schema) = &self.output_schema else {
            return Ok(());
        };
        let value = result
            .structured_content
            .as_ref()
            .ok_or_else(|| ToolError::InvalidOutput {
                message: format!(
                    "successful output from `{}` omitted structured content",
                    self.name
                ),
                issues: vec![ValidationIssue {
                    path: String::new(),
                    message: "structured content is required by the declared output schema"
                        .to_owned(),
                }],
            })?;
        let issues = schema.validate(value)?;
        if issues.is_empty() {
            Ok(())
        } else {
            Err(ToolError::InvalidOutput {
                message: format!(
                    "structured output from `{}` failed schema validation",
                    self.name
                ),
                issues,
            })
        }
    }
}

/// Source-neutral tool behavior hints.
/// These hints describe expected behavior; they do not enforce permissions or retry safety.
///
/// # Examples
///
/// ```
/// use tools_kit::declaration::ToolAnnotations;
/// let hints = ToolAnnotations { read_only_hint: Some(true), ..Default::default() };
/// assert_eq!(hints.read_only_hint, Some(true));
/// ```
///
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolAnnotations {
    /// Optional model-visible title.
    pub title: Option<String>,
    /// Whether execution is expected to avoid environmental mutation.
    pub read_only_hint: Option<bool>,
    /// Whether execution may perform destructive changes.
    pub destructive_hint: Option<bool>,
    /// Whether repeating the same invocation is expected to be safe.
    pub idempotent_hint: Option<bool>,
    /// Whether execution may interact with entities outside a closed domain.
    pub open_world_hint: Option<bool>,
}

/// Source-neutral icon metadata.
///
/// # Examples
///
/// ```
/// use tools_kit::declaration::ToolIcon;
/// let icon = ToolIcon {
///     uri: "https://example.com/icon.png".into(),
///     mime_type: Some("image/png".into()),
/// };
/// assert_eq!(icon.mime_type.as_deref(), Some("image/png"));
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolIcon {
    /// URI identifying or containing the icon resource.
    pub uri: String,
    /// Optional media type for the icon resource.
    pub mime_type: Option<String>,
}
