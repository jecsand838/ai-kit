//! Invocation inputs, resumable transitions, and completed tool outcomes.
//!
//! Invoke one unique handler, resolve its exact pending interaction, or cancel it.
//! An accepted model-visible failure is [`ToolResult::is_error`](crate::execution::ToolResult::is_error), while [`ToolError`]
//! reports an operation failure. The caller retains call/name correlation.
//!
//! # Accepted tool failure versus an operation error
//!
//! An unavailable application record can be a completed model-visible error. Invalid
//! arguments never reach the callback. Neither case supplies an invented successful
//! answer, and the caller retains invocation correlation outside `ToolResult`.
//!
//! ```
//! use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, ToolError};
//! use tool_sdk::declaration::{ToolInfo, ToolName};
//! use tool_sdk::execution::{ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult};
//! use tool_sdk::local::ToolRegistration;
//! use tool_sdk::schema::JsonSchema;
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), ToolError> {
//! let name = ToolName::new("record")?;
//! let info = ToolInfo::new(name.clone(), JsonSchema::strict(serde_json::json!({
//!     "type":"object", "required":["key"], "properties":{"key":{"type":"string"}}, "additionalProperties":false
//! })));
//! let tools = Toolset::local(vec![ToolRegistration::function(info, |input| async move {
//!     // The application accepts the key but cannot find its record.
//!     Ok(ToolResult::text(format!("No record for {}", input.arguments["key"])).with_error(true))
//! })])?;
//! let work = async {
//!     let mut handler = tools.tool_handler(name.clone()).await?;
//!     let output = handler.step(ToolHandlerInput::Invoke(ToolInvocation {
//!         call_id: Some("owned-call".into()), arguments: serde_json::json!({"key":"missing"}),
//!     })).await?;
//!     let ToolHandlerOutput::Completed(result) = output else { panic!("terminal result expected") };
//!     assert!(result.is_error);
//!     assert!(matches!(result.content.as_slice(), [ToolContent::Text { text, .. }] if text.contains("missing")));
//!     let mut invalid = tools.tool_handler(name).await?;
//!     let rejected = invalid.step(ToolHandlerInput::Invoke(ToolInvocation {
//!         call_id: None, arguments: serde_json::json!({}),
//!     })).await;
//!     assert!(matches!(rejected, Err(ToolError::InvalidArguments { .. })));
//!     Ok::<(), ToolError>(())
//! }.await;
//! let cleanup = tools.shutdown().await;
//! work?;
//! cleanup?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    ToolError,
    interaction::{ToolInteractionRequest, ToolInteractionResponse},
};

/// Invocation input for one tool handler.
///
/// # Examples
///
/// ```
/// use tool_sdk::execution::ToolInvocation;
/// let input = ToolInvocation { call_id: Some("call-1".into()), arguments: serde_json::json!({"city":"Chicago"}) };
/// assert_eq!(input.arguments["city"], "Chicago");
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocation {
    /// Opaque caller-owned correlation id.
    pub call_id: Option<String>,
    /// JSON arguments supplied by the caller.
    pub arguments: serde_json::Value,
}

/// Source-neutral content returned by a tool.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolContent {
    /// Human-readable text content.
    Text {
        /// Returned text.
        text: String,
        /// Optional source-neutral metadata for this content item.
        metadata: Option<serde_json::Value>,
    },
    /// Structured JSON content.
    Json {
        /// Returned JSON value.
        value: serde_json::Value,
        /// Optional source-neutral metadata for this content item.
        metadata: Option<serde_json::Value>,
    },
    /// Base64-encoded image content.
    Image {
        /// Media type of the encoded image.
        mime_type: String,
        /// Base64-encoded image bytes.
        data_base64: String,
        /// Optional source-neutral metadata for this content item.
        metadata: Option<serde_json::Value>,
    },
    /// Base64-encoded audio content.
    Audio {
        /// Media type of the encoded audio.
        mime_type: String,
        /// Base64-encoded audio bytes.
        data_base64: String,
        /// Optional source-neutral metadata for this content item.
        metadata: Option<serde_json::Value>,
    },
    /// Link to a resource that the caller may retrieve.
    ResourceLink {
        /// Resource URI.
        uri: String,
        /// Optional display title.
        title: Option<String>,
        /// Source-neutral resource metadata.
        metadata: serde_json::Value,
    },
    /// Resource contents embedded directly in the result.
    EmbeddedResource {
        /// Resource URI.
        uri: String,
        /// Source-defined resource contents.
        contents: serde_json::Value,
        /// Source-neutral resource metadata.
        metadata: serde_json::Value,
    },
}

/// Source-neutral result for a completed tool call.
/// Successful output must include structured content when the declaration has an
/// output schema. `Some(Value::Null)` is present content and is validated normally;
/// `None` is missing. An error result remains a completed model-visible outcome.
///
/// # Examples
///
/// ```
/// use tool_sdk::execution::ToolResult;
/// let result = ToolResult::json(serde_json::json!({"count":2}));
/// assert_eq!(result.structured_content, Some(serde_json::json!({"count":2})));
/// assert!(!result.is_error);
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Ordered model-visible content items.
    pub content: Vec<ToolContent>,
    /// Optional structured value validated against the tool output schema.
    pub structured_content: Option<serde_json::Value>,
    /// Whether this completed result represents a model-visible tool failure.
    pub is_error: bool,
    /// Uninterpreted source-neutral result metadata.
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl ToolResult {
    /// Creates a completed result.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidOutput`] if `content` is empty. Output-schema validation occurs when the owning handler completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::execution::{ToolContent, ToolResult};
    /// let result = ToolResult::new(vec![ToolContent::Text { text: "done".into(), metadata: None }])?;
    /// assert_eq!(result.content.len(), 1);
    /// assert!(ToolResult::new(Vec::new()).is_err());
    ///
    /// # Ok::<(), tool_sdk::ToolError>(())
    /// ```
    ///
    pub fn new(content: Vec<ToolContent>) -> Result<Self, ToolError> {
        if content.is_empty() {
            return Err(ToolError::InvalidOutput {
                message: "tool result content must not be empty".to_owned(),
                issues: Vec::new(),
            });
        }
        Ok(Self {
            content,
            structured_content: None,
            is_error: false,
            metadata: BTreeMap::new(),
        })
    }

    /// Creates a text result.
    ///
    /// # Examples
    ///
    /// ```
    /// let result = tool_sdk::execution::ToolResult::text("done");
    /// assert!(!result.is_error);
    /// assert!(result.structured_content.is_none());
    /// ```
    ///
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent::Text {
                text: text.into(),
                metadata: None,
            }],
            structured_content: None,
            is_error: false,
            metadata: BTreeMap::new(),
        }
    }

    /// Creates a structured JSON result.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::execution::ToolResult;
    /// let result = ToolResult::json(serde_json::json!({"count":2}));
    /// assert_eq!(result.structured_content, Some(serde_json::json!({"count":2})));
    /// assert!(!result.is_error);
    /// ```
    ///
    #[must_use]
    pub fn json(value: serde_json::Value) -> Self {
        Self {
            content: vec![ToolContent::Json {
                value: value.clone(),
                metadata: None,
            }],
            structured_content: Some(value),
            is_error: false,
            metadata: BTreeMap::new(),
        }
    }

    /// Marks this completed result as model-visible failure.
    /// Use a completed error result for an accepted tool-authored failure. Outer
    /// [`ToolError`] values describe input, state, protocol, transport, or lifecycle failures.
    ///
    /// # Examples
    ///
    /// ```
    /// let result = tool_sdk::execution::ToolResult::text("record unavailable").with_error(true);
    /// assert!(result.is_error);
    /// ```
    ///
    #[must_use]
    pub fn with_error(mut self, is_error: bool) -> Self {
        self.is_error = is_error;
        self
    }

    /// Sets structured output.
    ///
    /// # Examples
    ///
    /// ```
    /// let result = tool_sdk::execution::ToolResult::text("no value")
    ///     .with_structured_content(serde_json::Value::Null);
    /// assert_eq!(result.structured_content, Some(serde_json::Value::Null));
    /// ```
    ///
    #[must_use]
    pub fn with_structured_content(mut self, value: serde_json::Value) -> Self {
        self.structured_content = Some(value);
        self
    }
}

/// Input to one tool-handler transition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolHandlerInput {
    /// Starts the invocation with caller-supplied arguments.
    Invoke(ToolInvocation),
    /// Resolves the handler's pending caller interaction.
    Interaction(ToolInteractionResponse),
    /// Cancels the invocation.
    Cancel {
        /// Optional caller-safe cancellation reason.
        reason: Option<String>,
    },
}

/// Output from one tool-handler transition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolHandlerOutput {
    /// The invocation reached a terminal result.
    Completed(ToolResult),
    /// The invocation is suspended pending a caller response.
    Interaction(ToolInteractionRequest),
}
