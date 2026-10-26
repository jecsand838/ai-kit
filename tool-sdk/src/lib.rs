//! Executable Local and MCP tools.
//!
//! The public hierarchy is [`Toolset`] → [`ToolHandler`]. Toolset is cloneable
//! and owns shared source lifecycle; each ToolHandler is a unique resumable
//! invocation state machine. Callers implement [`ToolsetBackend`] directly and
//! retain its exact associated handler. [`Toolset::from_backend`] optionally
//! normalizes that hierarchy into supplied handles at the sole public root seam.
//!
//! Supporting API is grouped under [`declaration`], [`execution`],
//! [`interaction`], [`local`], [`mcp`], and [`schema`].
//!
//! ```
//! use tool_sdk::{
//!     ToolHandlerBackend, Toolset, ToolsetBackend,
//!     declaration::{ToolInfo, ToolName},
//!     execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
//!     local::ToolRegistration,
//!     schema::JsonSchema,
//! };
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), tool_sdk::ToolError> {
//! let name = ToolName::new("ping")?;
//! let info = ToolInfo::new(
//!     name.clone(),
//!     JsonSchema::strict(serde_json::json!({"type": "object"})),
//! );
//! let toolset = Toolset::local(vec![ToolRegistration::function(
//!     info,
//!     |_invocation| async { Ok(ToolResult::text("pong")) },
//! )])?;
//! let mut handler = toolset.tool_handler(name).await?;
//! let output = handler
//!     .step(ToolHandlerInput::Invoke(ToolInvocation {
//!         call_id: Some("call-1".to_owned()),
//!         arguments: serde_json::json!({}),
//!     }))
//!     .await;
//! let cleanup = toolset.shutdown().await;
//! let ToolHandlerOutput::Completed(result) = output? else { panic!("completion expected") };
//! assert!(!result.is_error);
//! assert!(matches!(result.content.as_slice(), [tool_sdk::execution::ToolContent::Text { text, .. }] if text == "pong"));
//! cleanup?;
//! # Ok(())
//! # }
//! ```
//!
//! # Choose a workflow
//!
//! | Audience | Start here |
//! | --- | --- |
//! | Application builders | [Local registration and caller interactions](local), [terminal results and failures](execution), [configured MCP clients and servers](mcp) |
//! | Custom-backend implementers | Complete executable [`ToolsetBackend`] example, then the optional [`Toolset::from_backend`] normalization seam |
//!
//! Local construction performs no network request. Configuring an MCP connection is
//! passive, but connecting or serving requires its external transport. The optional
//! `mcp-server` feature adds a separately owned server; stopping it never shuts down
//! the shared executable Toolset. No Handler normalization constructor exists.

#![doc(html_logo_url = "../assets/ai-kit-logo.png")]
#![forbid(unsafe_code)]

/// Tool names and source-neutral executable declarations.
pub mod declaration;
/// Invocation, continuation, and completed-result values.
pub mod execution;
/// Portable caller interactions requested during execution.
pub mod interaction;
/// In-process tool registration and interaction context.
pub mod local;
/// MCP client configuration and optional independent server support.
pub mod mcp;
/// JSON Schema values and validation diagnostics.
pub mod schema;

mod error;
mod handler;
mod name;
mod sources;
mod toolset;

pub use crate::{
    error::ToolError,
    handler::{ToolHandler, ToolHandlerBackend},
    toolset::{Toolset, ToolsetBackend},
};

/// Builds a typed local registration.
///
/// The macro is intentionally limited to typed-handler ergonomics. It does
/// not generate dispatch, routing, or lifecycle code.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tool_sdk::ToolError> {
/// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend};
/// use tool_sdk::declaration::ToolName;
/// use tool_sdk::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation};
/// let registration = tool_sdk::tool!("double", |value: i64| async move { Ok(value * 2) })?;
/// let tools = Toolset::local(vec![registration])?;
/// let mut handler = tools.tool_handler(ToolName::new("double")?).await?;
/// let result = handler.step(ToolHandlerInput::Invoke(ToolInvocation {
///     call_id: None, arguments: serde_json::json!(3),
/// })).await;
/// let cleanup = tools.shutdown().await;
/// let ToolHandlerOutput::Completed(result) = result? else { panic!("completion expected") };
/// assert_eq!(result.structured_content, Some(serde_json::json!(6)));
/// cleanup?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! tool {
    ($name:expr, $handler:expr $(,)?) => {
        $crate::local::ToolRegistration::typed($name, $handler)
    };
    ($name:expr, description = $description:expr, $handler:expr $(,)?) => {
        $crate::local::ToolRegistration::typed($name, $handler)
            .map(|registration| registration.description($description))
    };
}
