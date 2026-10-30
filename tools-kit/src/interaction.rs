//! Caller input requested by a suspended tool invocation.
//!
//! Declare capabilities before execution. Preserve the pending request id in
//! [`ToolInteractionResponse`](crate::interaction::ToolInteractionResponse); text replies are strings and form replies are objects.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ToolError, name::ToolName, schema::JsonSchema};

/// Interaction behavior declared by one tool.
///
/// # Examples
///
/// ```
/// use tools_kit::interaction::ToolInteractionCapabilities;
/// let capabilities = ToolInteractionCapabilities { text: true, ..Default::default() };
/// assert!(capabilities.text);
/// assert!(!capabilities.form);
/// ```
///
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInteractionCapabilities {
    /// Whether the tool may request structured form input.
    pub form: bool,
    /// Whether form responses may be validated against the supplied schema.
    pub form_schema_validation: bool,
    /// Whether the tool may request single-line text input.
    pub text: bool,
    /// Whether the tool may request multiline text input.
    pub multiline_text: bool,
}

/// Opaque interaction id.
/// Keep the returned identity unchanged when constructing the response. Its spelling
/// is opaque; do not derive invocation or tool identity from it.
///
/// # Examples
///
/// ```
/// use tools_kit::interaction::ToolInteractionId;
/// let id = ToolInteractionId::new();
/// let encoded = serde_json::to_value(&id).unwrap();
/// assert_eq!(encoded.as_str(), Some(id.as_str()));
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolInteractionId(String);

impl ToolInteractionId {
    /// Creates a fresh opaque interaction id.
    ///
    /// See [`crate::interaction::ToolInteractionId`] for a complete example.
    ///
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Borrows the serialized interaction id.
    ///
    /// See [`crate::interaction::ToolInteractionId`] for a complete example.
    ///
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ToolInteractionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Source-neutral interaction request produced by a tool handler.
///
/// # Examples
///
/// ```
/// use tools_kit::{
///     declaration::ToolName,
///     interaction::{
///         ToolInteractionId, ToolInteractionKind, ToolInteractionRequest, ToolInteractionResponse,
///     },
/// };
/// let request = ToolInteractionRequest {
///     id: ToolInteractionId::new(), tool_name: ToolName::new("ask")?,
///     call_id: Some("call-1".into()), prompt: "Which city?".into(),
///     kind: ToolInteractionKind::Text { multiline: false },
/// };
/// let response = ToolInteractionResponse::submit(request.id.clone(), serde_json::json!("Chicago"));
/// assert_eq!(response.interaction_id(), &request.id);
///
/// # Ok::<(), tools_kit::ToolError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInteractionRequest {
    /// Opaque id used to correlate the caller response.
    pub id: ToolInteractionId,
    /// Tool that requested the interaction.
    pub tool_name: ToolName,
    /// Optional caller-owned invocation correlation id.
    pub call_id: Option<String>,
    /// Human-readable prompt shown to the caller.
    pub prompt: String,
    /// Input shape requested from the caller.
    pub kind: ToolInteractionKind,
}

impl ToolInteractionRequest {
    pub(crate) fn validate_response(
        &self,
        response: &ToolInteractionResponse,
    ) -> Result<(), ToolError> {
        if response.interaction_id() != &self.id {
            return Err(ToolError::InvalidInteraction {
                id: response.interaction_id().as_str().to_owned(),
                message: format!(
                    "response id does not match pending interaction `{}`",
                    self.id.as_str()
                ),
            });
        }

        match (response, &self.kind) {
            (
                ToolInteractionResponse::Submit { content, .. },
                ToolInteractionKind::Form {
                    schema,
                    schema_validation: true,
                },
            ) => {
                if !content.is_object() {
                    return Err(ToolError::InvalidInteraction {
                        id: self.id.as_str().to_owned(),
                        message: "form responses must be JSON objects".to_owned(),
                    });
                }
                let issues = JsonSchema::validate_raw(schema, content)?;
                if issues.is_empty() {
                    Ok(())
                } else {
                    Err(ToolError::InvalidArguments {
                        tool: self.tool_name.clone(),
                        message: "form response failed schema validation".to_owned(),
                        issues,
                    })
                }
            }
            (
                ToolInteractionResponse::Submit { content, .. },
                ToolInteractionKind::Form {
                    schema_validation: false,
                    ..
                },
            ) if content.is_object() => Ok(()),
            (ToolInteractionResponse::Submit { content, .. }, ToolInteractionKind::Text { .. })
                if content.is_string() =>
            {
                Ok(())
            }
            (ToolInteractionResponse::Submit { .. }, _) => Err(ToolError::InvalidInteraction {
                id: self.id.as_str().to_owned(),
                message: "interaction response content does not match request kind".to_owned(),
            }),
            (
                ToolInteractionResponse::Decline { .. } | ToolInteractionResponse::Cancel { .. },
                _,
            ) => Ok(()),
        }
    }
}

/// Source-neutral interaction request kind.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolInteractionKind {
    /// Requests a JSON object described by a JSON Schema.
    Form {
        /// JSON Schema describing the expected object.
        schema: serde_json::Value,
        /// Whether the response must be validated against `schema`.
        schema_validation: bool,
    },
    /// Requests textual input.
    Text {
        /// Whether the response may contain multiple lines.
        multiline: bool,
    },
}

/// Caller response to a pending tool interaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolInteractionResponse {
    /// Supplies caller input and resumes execution.
    Submit {
        /// Id of the interaction being resolved.
        id: ToolInteractionId,
        /// Submitted text or form value.
        content: serde_json::Value,
    },
    /// Declines to provide the requested input.
    Decline {
        /// Id of the interaction being resolved.
        id: ToolInteractionId,
        /// Optional caller-safe explanation.
        reason: Option<String>,
    },
    /// Cancels the pending interaction and invocation.
    Cancel {
        /// Id of the interaction being resolved.
        id: ToolInteractionId,
        /// Optional caller-safe cancellation reason.
        reason: Option<String>,
    },
}

pub(crate) enum InteractionResolution {
    Submit(serde_json::Value),
    Decline(Option<String>),
    Cancel(Option<String>),
}

impl ToolInteractionResponse {
    /// Creates a submitted interaction response.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::interaction::{ToolInteractionId, ToolInteractionResponse};
    /// let id = ToolInteractionId::new();
    /// let response = ToolInteractionResponse::submit(id.clone(), serde_json::json!("Chicago"));
    /// assert_eq!(response.interaction_id(), &id);
    /// ```
    ///
    #[must_use]
    pub fn submit(id: ToolInteractionId, content: serde_json::Value) -> Self {
        Self::Submit { id, content }
    }

    /// Creates a declined interaction response.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::interaction::{ToolInteractionId, ToolInteractionResponse};
    /// let id = ToolInteractionId::new();
    /// let response = ToolInteractionResponse::decline(id.clone(), Some("not available".into()));
    /// assert_eq!(response.interaction_id(), &id);
    /// ```
    ///
    #[must_use]
    pub fn decline(id: ToolInteractionId, reason: Option<String>) -> Self {
        Self::Decline { id, reason }
    }

    /// Creates a cancelled interaction response.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::interaction::{ToolInteractionId, ToolInteractionResponse};
    /// let id = ToolInteractionId::new();
    /// let response = ToolInteractionResponse::cancel(id.clone(), None);
    /// assert_eq!(response.interaction_id(), &id);
    /// ```
    ///
    #[must_use]
    pub fn cancel(id: ToolInteractionId, reason: Option<String>) -> Self {
        Self::Cancel { id, reason }
    }

    /// Borrows the id of the interaction being resolved.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::interaction::{ToolInteractionId, ToolInteractionResponse};
    /// let id = ToolInteractionId::new();
    /// let response = ToolInteractionResponse::submit(id.clone(), serde_json::json!({"approved":true}));
    /// assert_eq!(response.interaction_id(), &id);
    /// ```
    ///
    #[must_use]
    pub fn interaction_id(&self) -> &ToolInteractionId {
        match self {
            Self::Submit { id, .. } | Self::Decline { id, .. } | Self::Cancel { id, .. } => id,
        }
    }

    pub(crate) fn into_resolution(self) -> InteractionResolution {
        match self {
            Self::Submit { content, .. } => InteractionResolution::Submit(content),
            Self::Decline { reason, .. } => InteractionResolution::Decline(reason),
            Self::Cancel { reason, .. } => InteractionResolution::Cancel(reason),
        }
    }
}
