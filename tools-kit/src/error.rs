use serde::{Deserialize, Serialize};

use crate::{name::ToolName, schema::ValidationIssue};

/// Portable executable-tool errors.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolError {
    /// Toolset, registration, client, or server configuration is invalid.
    #[error("invalid configuration: {message}")]
    InvalidConfiguration {
        /// Caller-safe validation failure.
        message: String,
    },
    /// The requested tool name is not present in the inventory.
    #[error("tool `{name}` was not found")]
    UnknownTool {
        /// Requested caller-facing name.
        name: ToolName,
    },
    /// More than one registration resolves to the same caller-facing name.
    #[error("duplicate tool name `{name}`")]
    DuplicateName {
        /// Duplicated caller-facing name.
        name: ToolName,
    },
    /// Invocation arguments failed source-neutral validation.
    #[error("invalid tool arguments for `{tool}`: {message}")]
    InvalidArguments {
        /// Tool whose invocation was rejected.
        tool: ToolName,
        /// Caller-safe validation summary.
        message: String,
        /// Structured schema diagnostics, when available.
        issues: Vec<ValidationIssue>,
    },
    /// A completed result failed source-neutral output validation.
    #[error("invalid tool output: {message}")]
    InvalidOutput {
        /// Caller-safe validation summary.
        message: String,
        /// Structured schema diagnostics, when available.
        issues: Vec<ValidationIssue>,
    },
    /// A caller response does not match the pending interaction.
    #[error("invalid interaction `{id}`: {message}")]
    InvalidInteraction {
        /// Interaction id supplied by the caller.
        id: String,
        /// Caller-safe validation summary.
        message: String,
    },
    /// A local tool requested an interaction capability it did not declare.
    #[error("tool `{tool}` requested undeclared {kind} interaction")]
    UndeclaredInteraction {
        /// Tool that requested the interaction.
        tool: ToolName,
        /// Source-neutral interaction kind.
        kind: String,
    },
    /// The caller declined a pending interaction.
    #[error("interaction for tool `{tool}` was declined")]
    InteractionDeclined {
        /// Tool whose interaction was declined.
        tool: ToolName,
        /// Optional caller-safe reason.
        reason: Option<String>,
    },
    /// Tool execution or interaction resolution was cancelled.
    #[error("tool `{tool}` was cancelled")]
    Cancelled {
        /// Cancelled tool.
        tool: ToolName,
        /// Optional caller-safe cancellation reason.
        reason: Option<String>,
    },
    /// The executable source or interaction transport is unavailable.
    #[error("tool transport is unavailable: {message}")]
    Transport {
        /// Affected tool, when known.
        tool: Option<ToolName>,
        /// Sanitized transport failure.
        message: String,
    },
    /// The source returned or received invalid protocol data.
    #[error("tool protocol error: {message}")]
    Protocol {
        /// Sanitized protocol failure.
        message: String,
    },
    /// A bounded tool operation exceeded its deadline.
    #[error("tool `{tool}` timed out after {timeout_secs}s")]
    Timeout {
        /// Tool whose operation timed out.
        tool: ToolName,
        /// Applied timeout in whole seconds.
        timeout_secs: u64,
    },
    /// The selected source does not support an operation.
    #[error("unsupported operation: {operation}")]
    Unsupported {
        /// Portable operation name.
        operation: String,
    },
    /// Admission was rejected because Toolset shutdown has begun.
    #[error("toolset is closed for operation `{operation}`")]
    Closed {
        /// Rejected operation name.
        operation: String,
    },
    /// Toolset construction failed and its compensating cleanup also failed.
    #[error("tool construction failed and cleanup also failed")]
    BuildCleanup {
        /// Original construction failure.
        primary: Box<ToolError>,
        /// Failure produced while cleaning up partial construction.
        cleanup: Box<ToolError>,
    },
}

impl ToolError {
    pub(crate) fn closed(operation: impl Into<String>) -> Self {
        Self::Closed {
            operation: operation.into(),
        }
    }
}
