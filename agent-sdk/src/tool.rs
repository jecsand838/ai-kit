//! Managed tool inventory and per-prompt selection.
//!
//! [`ToolInfo`] describes tools without owning their executable handlers.
//! [`ToolSelection`] restricts the effective catalog for a prompt; named
//! selections are validated when placed in a request or backend configuration.
//! Executable tools themselves belong to `tool-sdk`.

use std::collections::BTreeSet;

use crate::SdkError;

/// Portable per-prompt selection over the complete effective tool catalog.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "mode", content = "names")]
pub enum ToolSelection {
    /// Exposes the complete catalog and lets the backend decide whether to call a tool.
    Auto,
    /// Exposes no portable tool for this prompt.
    None,
    /// Exposes the complete nonempty catalog and requires at least one call.
    Required,
    /// Exposes only the named tools without requiring a call.
    Allowed(Vec<String>),
    /// Exposes only the named tools and requires at least one of them.
    RequiredOneOf(Vec<String>),
}

impl ToolSelection {
    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        match self {
            Self::Auto | Self::None | Self::Required => Ok(()),
            Self::Allowed(names) | Self::RequiredOneOf(names) => Self::validate_names(names),
        }
    }

    fn validate_names(names: &[String]) -> Result<(), SdkError> {
        if names.is_empty() {
            return Err(SdkError::invalid_input(
                "named tool selection must not be empty",
            ));
        }
        let mut unique = BTreeSet::new();
        for name in names {
            if name.is_empty()
                || name.len() > 128
                || name.trim() != name
                || name.split('.').any(str::is_empty)
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
            {
                return Err(SdkError::invalid_input(
                    "tool selection names must be canonical portable names",
                ));
            }
            if !unique.insert(name) {
                return Err(SdkError::invalid_input(
                    "tool selection names must not contain duplicates",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Describes one agent-sdk-managed tool installed for a workspace.
///
/// ```
/// use agent_sdk::tool::ToolInfo;
///
/// let tool = ToolInfo::new("lookup", Some("Looks up a value".into()), None)?;
/// assert_eq!(tool.name(), "lookup");
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct ToolInfo {
    name: String,
    description: Option<String>,
    input_schema: Option<serde_json::Value>,
}

impl ToolInfo {
    /// Validates and constructs managed tool inventory information.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for an empty or padded
    /// name, or a present empty or padded description. The optional schema is
    /// inventory metadata; this constructor does not compile it.
    ///
    /// ```
    /// use agent_sdk::tool::ToolInfo;
    /// let tool = ToolInfo::new("lookup", None, None)?;
    /// assert_eq!(tool.name(), "lookup");
    /// assert!(ToolInfo::new("", None, None).is_err());
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(
        name: impl Into<String>,
        description: Option<String>,
        input_schema: Option<serde_json::Value>,
    ) -> Result<Self, SdkError> {
        let name = name.into();
        if name.is_empty() || name.trim() != name {
            return Err(SdkError::invalid_input(
                "tool name must be nonempty and unpadded",
            ));
        }
        if description
            .as_deref()
            .is_some_and(|value| value.is_empty() || value.trim() != value)
        {
            return Err(SdkError::invalid_input(
                "tool description must be nonempty and unpadded when present",
            ));
        }
        Ok(Self {
            name,
            description,
            input_schema,
        })
    }

    /// Returns the unique managed tool name.
    ///
    /// ```
    /// use agent_sdk::tool::ToolInfo;
    /// assert_eq!(ToolInfo::new("lookup", None, None)?.name(), "lookup");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional caller-facing description.
    ///
    /// ```
    /// use agent_sdk::tool::ToolInfo;
    /// let tool = ToolInfo::new("lookup", Some("Find a record".into()), None)?;
    /// assert_eq!(tool.description(), Some("Find a record"));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the optional source-neutral input schema.
    ///
    /// ```
    /// use agent_sdk::tool::ToolInfo;
    /// let schema = serde_json::json!({"type": "object"});
    /// let tool = ToolInfo::new("lookup", None, Some(schema.clone()))?;
    /// assert_eq!(tool.input_schema(), Some(&schema));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn input_schema(&self) -> Option<&serde_json::Value> {
        self.input_schema.as_ref()
    }
}
