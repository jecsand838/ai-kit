use std::{future::Future, pin::Pin};

use crate::{
    ToolError,
    execution::{ToolHandlerInput, ToolHandlerOutput},
    sources::{local, mcp},
};

/// Public implementation contract for one unique resumable tool handler.
/// One handler owns one invocation. Start with `Invoke`, resume only the current
/// interaction with its exact id, and use `Cancel` for cancellation. A completed
/// result is terminal; a model-visible failure is a completed result with `is_error`.
/// Import [`ToolHandlerBackend`] to call behavior on the supplied handle.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tools_kit::ToolError> {
/// use tools_kit::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
/// let info = ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"})));
/// use tools_kit::{local::ToolRegistration, execution::ToolResult};
/// let registration = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("pong")) });
/// use tools_kit::{Toolset, ToolsetBackend, ToolHandlerBackend, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation}};
/// let tools = Toolset::local(vec![registration])?;
/// let mut handler = tools.tool_handler(ToolName::new("ping")?).await?;
/// let result = handler.step(ToolHandlerInput::Invoke(ToolInvocation { call_id: None, arguments: serde_json::json!({}) })).await;
/// let cleanup = tools.shutdown().await;
/// assert!(matches!(result?, ToolHandlerOutput::Completed(_)));
/// cleanup?;
///
/// # Ok(())
/// # }
/// ```
///
pub trait ToolHandlerBackend: Send + 'static {
    /// Advances the unique handler with an invocation, interaction response,
    /// or cancellation request.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError`] for invalid transitions, arguments or interaction replies, unsupported behavior, cancellation, closure, transport or protocol failure. An accepted tool-authored failure is returned as a completed error result.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tools_kit::ToolError> {
    /// use tools_kit::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
    /// let info = ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"})));
    /// use tools_kit::{local::ToolRegistration, execution::ToolResult};
    /// let registration = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("pong")) });
    /// use tools_kit::{Toolset, ToolsetBackend, ToolHandlerBackend, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation}};
    /// let tools = Toolset::local(vec![registration])?;
    /// let mut handler = tools.tool_handler(ToolName::new("ping")?).await?;
    /// let result = handler.step(ToolHandlerInput::Invoke(ToolInvocation { call_id: None, arguments: serde_json::json!({}) })).await;
    /// let cleanup = tools.shutdown().await;
    /// assert!(matches!(result?, ToolHandlerOutput::Completed(_)));
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn step(
        &mut self,
        input: ToolHandlerInput,
    ) -> impl Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_;
}

/// Opaque resumable handler for one selected executable tool.
/// One handler owns one invocation. Start with `Invoke`, resume only the current
/// interaction with its exact id, and use `Cancel` for cancellation. A completed
/// result is terminal; a model-visible failure is a completed result with `is_error`.
/// Import [`ToolHandlerBackend`] to call behavior on the supplied handle.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tools_kit::ToolError> {
/// use tools_kit::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
/// let info = ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"})));
/// use tools_kit::{local::ToolRegistration, execution::ToolResult};
/// let registration = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("pong")) });
/// use tools_kit::{Toolset, ToolsetBackend, ToolHandlerBackend, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation}};
/// let tools = Toolset::local(vec![registration])?;
/// let mut handler = tools.tool_handler(ToolName::new("ping")?).await?;
/// let result = handler.step(ToolHandlerInput::Invoke(ToolInvocation { call_id: None, arguments: serde_json::json!({}) })).await;
/// let cleanup = tools.shutdown().await;
/// assert!(matches!(result?, ToolHandlerOutput::Completed(_)));
/// cleanup?;
///
/// # Ok(())
/// # }
/// ```
///
pub struct ToolHandler {
    inner: InnerToolHandler,
}

enum InnerToolHandler {
    Local(local::handler::LocalToolHandler),
    Mcp(mcp::handler::McpToolHandler),
    Extension(Box<dyn ErasedToolHandlerBackend>),
}

impl ToolHandler {
    pub(crate) fn from_local(handler: local::handler::LocalToolHandler) -> Self {
        Self {
            inner: InnerToolHandler::Local(handler),
        }
    }

    pub(crate) fn from_mcp(handler: mcp::handler::McpToolHandler) -> Self {
        Self {
            inner: InnerToolHandler::Mcp(handler),
        }
    }

    pub(crate) fn from_extension<B: ToolHandlerBackend>(backend: B) -> Self {
        Self {
            inner: InnerToolHandler::Extension(Box::new(backend)),
        }
    }
}

impl std::fmt::Debug for ToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolHandler")
            .finish_non_exhaustive()
    }
}

trait ErasedToolHandlerBackend: Send {
    fn step_erased(
        &mut self,
        input: ToolHandlerInput,
    ) -> Pin<Box<dyn Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_>>;
}

impl<B: ToolHandlerBackend> ErasedToolHandlerBackend for B {
    fn step_erased(
        &mut self,
        input: ToolHandlerInput,
    ) -> Pin<Box<dyn Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_>> {
        Box::pin(self.step(input))
    }
}

impl ToolHandlerBackend for InnerToolHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match self {
            Self::Local(handler) => handler.step(input).await,
            Self::Mcp(handler) => handler.step(input).await,
            Self::Extension(handler) => handler.step_erased(input).await,
        }
    }
}

impl ToolHandlerBackend for ToolHandler {
    fn step(
        &mut self,
        input: ToolHandlerInput,
    ) -> impl Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_ {
        self.inner.step(input)
    }
}
