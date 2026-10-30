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
/// See [`crate::local::ToolRegistration`] for a complete example.
///
pub trait ToolHandlerBackend: Send + 'static {
    /// Advances the unique handler with an invocation, interaction response,
    /// or cancellation request.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError`] for invalid transitions, arguments or interaction replies, unsupported behavior, cancellation, closure, transport or protocol failure. An accepted tool-authored failure is returned as a completed error result.
    ///
    /// See [`crate::local::ToolRegistration`] for a complete example.
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
/// See [`crate::local::ToolRegistration`] for a complete example.
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
