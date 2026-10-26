//! Local executable registrations and declared caller interactions.
//!
//! [`ToolRegistration::function`](crate::local::ToolRegistration::function) accepts normalized invocation values;
//! [`ToolRegistration::typed`](crate::local::ToolRegistration::typed) derives schemas for typed arguments and results.
//! Interactive handlers use their [`ToolInvocationContext`](crate::local::ToolInvocationContext) and resume through
//! the same unique handler. Construct a source with [`crate::Toolset::local`].
//!
//! # Caller interaction and cancellation
//!
//! The [`crate::local::ToolInvocationContext::request_text`] and
//! [`crate::local::ToolInvocationContext::request_form`] examples below execute the complete
//! submit/resume path and inspect the result. Cancellation uses the same unique
//! handler and exact pending interaction id. It is an outer cancellation outcome,
//! not a completed model-visible result.
//!
//! ```
//! use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, ToolError};
//! use tool_sdk::declaration::{ToolInfo, ToolName};
//! use tool_sdk::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult};
//! use tool_sdk::interaction::{ToolInteractionCapabilities, ToolInteractionResponse};
//! use tool_sdk::local::ToolRegistration;
//! use tool_sdk::schema::JsonSchema;
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), ToolError> {
//! let name = ToolName::new("ask")?;
//! let mut info = ToolInfo::new(name.clone(), JsonSchema::strict(serde_json::json!({"type":"object"})));
//! info.interactions = ToolInteractionCapabilities { text: true, ..Default::default() };
//! let tools = Toolset::local(vec![ToolRegistration::interactive(info, |_input, context| async move {
//!     let answer = context.request_text("Which city?", false).await?;
//!     Ok(ToolResult::text(answer))
//! })])?;
//! let work = async {
//!     let mut handler = tools.tool_handler(name.clone()).await?;
//!     let output = handler.step(ToolHandlerInput::Invoke(ToolInvocation {
//!         call_id: Some("city-call".into()), arguments: serde_json::json!({}),
//!     })).await?;
//!     let ToolHandlerOutput::Interaction(request) = output else { panic!("interaction expected") };
//!     assert_eq!(request.tool_name, name);
//!     assert_eq!(request.call_id.as_deref(), Some("city-call"));
//!     let reply = ToolInteractionResponse::cancel(request.id, Some("caller stopped".into()));
//!     let cancelled = handler.step(ToolHandlerInput::Interaction(reply)).await;
//!     assert!(matches!(cancelled, Err(ToolError::Cancelled { .. })));
//!     assert!(handler.step(ToolHandlerInput::Cancel { reason: None }).await.is_err());
//!     Ok::<(), ToolError>(())
//! }.await;
//! let cleanup = tools.shutdown().await;
//! work?;
//! cleanup?;
//! # Ok(())
//! # }
//! ```

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use schemars::JsonSchema as SchemarsJsonSchema;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{mpsc, oneshot};

use crate::{
    ToolError,
    declaration::ToolInfo,
    execution::{ToolInvocation, ToolResult},
    interaction::{
        InteractionResolution, ToolInteractionCapabilities, ToolInteractionId, ToolInteractionKind,
        ToolInteractionRequest, ToolInteractionResponse,
    },
    name::ToolName,
    schema::JsonSchema,
};

/// One complete local tool declaration plus its private executable handler.
/// Registration constructs passive declarations and closures; execution begins through
/// the selected unique handler. Cloning a registration shares its immutable handler.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tool_sdk::ToolError> {
/// use tool_sdk::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
/// let info = ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"})));
/// use tool_sdk::{local::ToolRegistration, execution::ToolResult};
/// let registration = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("pong")) });
/// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation}};
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
#[derive(Clone)]
pub struct ToolRegistration {
    inner: Arc<RegisteredTool>,
}

pub(crate) struct RegisteredTool {
    info: ToolInfo,
    callback: Arc<dyn RegisteredToolCallback>,
}

pub(crate) struct RegisteredToolCall {
    future: Pin<Box<dyn Future<Output = Result<ToolResult, ToolError>> + Send>>,
}

impl RegisteredToolCall {
    pub(crate) async fn run(self) -> Result<ToolResult, ToolError> {
        self.future.await
    }
}

trait RegisteredToolCallback: Send + Sync {
    fn start(
        &self,
        invocation: ToolInvocation,
        context: ToolInvocationContext,
    ) -> RegisteredToolCall;
}

impl<F, Fut> RegisteredToolCallback for F
where
    F: Fn(ToolInvocation, ToolInvocationContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ToolResult, ToolError>> + Send + 'static,
{
    fn start(
        &self,
        invocation: ToolInvocation,
        context: ToolInvocationContext,
    ) -> RegisteredToolCall {
        RegisteredToolCall {
            future: Box::pin(self(invocation, context)),
        }
    }
}

impl RegisteredTool {
    pub(crate) fn info(&self) -> &ToolInfo {
        &self.info
    }

    pub(crate) fn call(
        &self,
        invocation: ToolInvocation,
        context: ToolInvocationContext,
    ) -> Result<RegisteredToolCall, ToolError> {
        let issues = self.info.input_schema.validate(&invocation.arguments)?;
        if !issues.is_empty() {
            return Err(ToolError::InvalidArguments {
                tool: self.info.name.clone(),
                message: "tool arguments failed schema validation".to_owned(),
                issues,
            });
        }
        Ok(self.callback.start(invocation, context))
    }
}

/// Context available to local tools that declare caller interaction support.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tool_sdk::ToolError> {
/// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, declaration::{ToolInfo, ToolName}, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult}, interaction::{ToolInteractionCapabilities, ToolInteractionResponse}, local::ToolRegistration, schema::JsonSchema};
/// let name = ToolName::new("ask")?;
/// let mut info = ToolInfo::new(name.clone(), JsonSchema::strict(serde_json::json!({"type":"object"})));
/// info.interactions = ToolInteractionCapabilities { text: true, ..Default::default() };
/// let registration = ToolRegistration::interactive(info, |_input, context| async move {
///     let city = context.request_text("Which city?", false).await?;
///     Ok(ToolResult::text(city))
/// });
/// let tools = Toolset::local(vec![registration])?;
/// let observed = async {
///     let mut handler = tools.tool_handler(name).await?;
///     let pending = handler.step(ToolHandlerInput::Invoke(ToolInvocation { call_id: None, arguments: serde_json::json!({}) })).await?;
///     let ToolHandlerOutput::Interaction(request) = pending else { panic!("interaction expected") };
///     let reply = ToolInteractionResponse::submit(request.id, serde_json::json!("Chicago"));
///     let done = handler.step(ToolHandlerInput::Interaction(reply)).await?;
///     assert!(matches!(done, ToolHandlerOutput::Completed(_)));
///     Ok::<(), tool_sdk::ToolError>(())
/// }.await;
/// let cleanup = tools.shutdown().await;
/// observed?;
/// cleanup?;
///
/// # Ok(())
/// # }
/// ```
///
#[derive(Clone)]
pub struct ToolInvocationContext {
    tool_name: ToolName,
    call_id: Option<String>,
    interactions: mpsc::Sender<LocalInteractionEnvelope>,
    capabilities: ToolInteractionCapabilities,
}

pub(crate) struct LocalInteractionEnvelope {
    request: ToolInteractionRequest,
    response: oneshot::Sender<ToolInteractionResponse>,
}

impl LocalInteractionEnvelope {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ToolInteractionRequest,
        oneshot::Sender<ToolInteractionResponse>,
    ) {
        (self.request, self.response)
    }
}

impl ToolRegistration {
    pub(crate) fn into_registered(self) -> RegisteredTool {
        Arc::try_unwrap(self.inner).unwrap_or_else(|inner| (*inner).clone())
    }

    /// Registers a non-interactive local handler.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use tool_sdk::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
    /// let info = ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"})));
    /// use tool_sdk::{local::ToolRegistration, execution::ToolResult};
    /// let registration = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("pong")) });
    /// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation}};
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
    #[must_use]
    pub fn function<F, Fut>(info: ToolInfo, handler: F) -> Self
    where
        F: Fn(ToolInvocation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, ToolError>> + Send + 'static,
    {
        let callback = move |invocation, _context| handler(invocation);
        Self::new(info, callback)
    }

    /// Registers a handler that may request declared caller interactions.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, declaration::{ToolInfo, ToolName}, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult}, interaction::{ToolInteractionCapabilities, ToolInteractionResponse}, local::ToolRegistration, schema::JsonSchema};
    /// let name = ToolName::new("ask")?;
    /// let mut info = ToolInfo::new(name.clone(), JsonSchema::strict(serde_json::json!({"type":"object"})));
    /// info.interactions = ToolInteractionCapabilities { text: true, ..Default::default() };
    /// let registration = ToolRegistration::interactive(info, |_input, context| async move {
    ///     let city = context.request_text("Which city?", false).await?;
    ///     Ok(ToolResult::text(city))
    /// });
    /// let tools = Toolset::local(vec![registration])?;
    /// let observed = async {
    ///     let mut handler = tools.tool_handler(name).await?;
    ///     let pending = handler.step(ToolHandlerInput::Invoke(ToolInvocation { call_id: None, arguments: serde_json::json!({}) })).await?;
    ///     let ToolHandlerOutput::Interaction(request) = pending else { panic!("interaction expected") };
    ///     let reply = ToolInteractionResponse::submit(request.id, serde_json::json!("Chicago"));
    ///     let done = handler.step(ToolHandlerInput::Interaction(reply)).await?;
    ///     let ToolHandlerOutput::Completed(result) = done else { panic!("completion expected") };
    ///     assert!(!result.is_error);
    ///     assert!(matches!(result.content.as_slice(), [tool_sdk::execution::ToolContent::Text { text, .. }] if text == "Chicago"));
    ///     Ok::<(), tool_sdk::ToolError>(())
    /// }.await;
    /// let cleanup = tools.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    #[must_use]
    pub fn interactive<F, Fut>(info: ToolInfo, handler: F) -> Self
    where
        F: Fn(ToolInvocation, ToolInvocationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, ToolError>> + Send + 'static,
    {
        let callback = move |invocation, context| handler(invocation, context);
        Self::new(info, callback)
    }

    /// Registers a typed local function and derives strict input/output schemas.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidConfiguration`] for an invalid name or [`ToolError::Protocol`] when schema serialization fails. Later invocation validates and deserializes arguments and serializes the returned output.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, local::ToolRegistration};
    /// use tool_sdk::declaration::ToolName;
    /// use tool_sdk::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation};
    /// let registration = ToolRegistration::typed("double", |value: i64| async move { Ok(value * 2) })?;
    /// let tools = Toolset::local(vec![registration])?;
    /// let observed = async {
    ///     assert!(tools.list_tools().await?[0].output_schema.is_some());
    ///     let mut handler = tools.tool_handler(ToolName::new("double")?).await?;
    ///     let output = handler.step(ToolHandlerInput::Invoke(ToolInvocation {
    ///         call_id: Some("double-1".into()), arguments: serde_json::json!(21),
    ///     })).await?;
    ///     let ToolHandlerOutput::Completed(result) = output else { panic!("completion expected") };
    ///     assert!(!result.is_error);
    ///     assert_eq!(result.structured_content, Some(serde_json::json!(42)));
    ///     Ok::<(), tool_sdk::ToolError>(())
    /// }.await;
    /// let cleanup = tools.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn typed<N, TArgs, TOutput, F, Fut>(name: N, handler: F) -> Result<Self, ToolError>
    where
        N: TryInto<ToolName>,
        N::Error: fmt::Display,
        TArgs: DeserializeOwned + SchemarsJsonSchema + Send + 'static,
        TOutput: Serialize + SchemarsJsonSchema + Send + 'static,
        F: Fn(TArgs) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TOutput, ToolError>> + Send + 'static,
    {
        let name = name
            .try_into()
            .map_err(|error| ToolError::InvalidConfiguration {
                message: error.to_string(),
            })?;
        let handler_name = name.clone();
        let handler = Arc::new(handler);
        let input_schema = Self::schema_document::<TArgs>()?;
        let output_schema = Self::schema_document::<TOutput>()?;
        let mut info = ToolInfo::new(name, input_schema);
        info.output_schema = Some(output_schema);
        Ok(Self::function(info, move |invocation| {
            let handler_name = handler_name.clone();
            let handler = Arc::clone(&handler);
            async move {
                let arguments =
                    Self::deserialize_arguments::<TArgs>(&handler_name, invocation.arguments)?;
                let output = handler(arguments).await?;
                let value = serde_json::to_value(output).map_err(|error| ToolError::Protocol {
                    message: format!("failed to serialize typed tool output: {error}"),
                })?;
                Ok(ToolResult::json(value))
            }
        }))
    }

    fn new<F, Fut>(info: ToolInfo, callback: F) -> Self
    where
        F: Fn(ToolInvocation, ToolInvocationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, ToolError>> + Send + 'static,
    {
        Self {
            inner: Arc::new(RegisteredTool {
                info,
                callback: Arc::new(callback),
            }),
        }
    }

    /// Replaces the description on this registration.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use tool_sdk::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
    /// let info = ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"})));
    /// use tool_sdk::{local::ToolRegistration, execution::ToolResult};
    /// let registration = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("pong")) });
    /// use tool_sdk::{Toolset, ToolsetBackend};
    /// let tools = Toolset::local(vec![registration.description("Responds with pong")])?;
    /// let inventory = tools.list_tools().await;
    /// let cleanup = tools.shutdown().await;
    /// assert_eq!(inventory?[0].description.as_deref(), Some("Responds with pong"));
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    #[must_use]
    pub fn description(self, description: impl Into<String>) -> Self {
        self.map_info(|info| info.description = Some(description.into()))
    }

    /// Replaces the interaction declaration on this registration.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use tool_sdk::{declaration::{ToolInfo, ToolName}, schema::JsonSchema};
    /// let info = ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"})));
    /// use tool_sdk::{local::ToolRegistration, execution::ToolResult};
    /// let registration = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("pong")) });
    /// use tool_sdk::{Toolset, ToolsetBackend, interaction::ToolInteractionCapabilities};
    /// let registration = registration.interactions(ToolInteractionCapabilities::default());
    /// let tools = Toolset::local(vec![registration])?;
    /// let inventory = tools.list_tools().await;
    /// let cleanup = tools.shutdown().await;
    /// assert!(!inventory?[0].interactions.text);
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    #[must_use]
    pub fn interactions(self, interactions: ToolInteractionCapabilities) -> Self {
        self.map_info(|info| info.interactions = interactions)
    }

    fn map_info(self, update: impl FnOnce(&mut ToolInfo)) -> Self {
        let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|inner| (*inner).clone());
        update(&mut inner.info);
        Self {
            inner: Arc::new(inner),
        }
    }

    fn schema_document<T: SchemarsJsonSchema>() -> Result<JsonSchema, ToolError> {
        let schema = serde_json::to_value(schemars::schema_for!(T)).map_err(|error| {
            ToolError::Protocol {
                message: format!("failed to serialize generated JSON Schema: {error}"),
            }
        })?;
        Ok(JsonSchema::strict(schema))
    }

    fn deserialize_arguments<TArgs>(
        tool: &ToolName,
        arguments: serde_json::Value,
    ) -> Result<TArgs, ToolError>
    where
        TArgs: DeserializeOwned,
    {
        let serialized = arguments.to_string();
        let mut deserializer = serde_json::Deserializer::from_str(&serialized);
        serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
            ToolError::InvalidArguments {
                tool: tool.clone(),
                message: error.to_string(),
                issues: Vec::new(),
            }
        })
    }
}

impl Clone for RegisteredTool {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            callback: Arc::clone(&self.callback),
        }
    }
}

impl fmt::Debug for RegisteredTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredTool")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ToolRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistration")
            .field("name", &self.inner.info.name)
            .finish_non_exhaustive()
    }
}

impl ToolInvocationContext {
    pub(crate) fn new(
        tool_name: ToolName,
        call_id: Option<String>,
        interactions: mpsc::Sender<LocalInteractionEnvelope>,
        capabilities: ToolInteractionCapabilities,
    ) -> Self {
        Self {
            tool_name,
            call_id,
            interactions,
            capabilities,
        }
    }

    /// Requests text from the caller and waits for its resolution.
    ///
    /// Returns [`ToolError::UndeclaredInteraction`] when the registration did
    /// not declare the requested text capability.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::UndeclaredInteraction`] for absent text or multiline capability, [`ToolError::InvalidInteraction`] for an invalid reply, or the corresponding decline, cancellation, or closure error.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, declaration::{ToolInfo, ToolName}, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult}, interaction::{ToolInteractionCapabilities, ToolInteractionResponse}, local::ToolRegistration, schema::JsonSchema};
    /// let name = ToolName::new("ask")?;
    /// let mut info = ToolInfo::new(name.clone(), JsonSchema::strict(serde_json::json!({"type":"object"})));
    /// info.interactions = ToolInteractionCapabilities { text: true, ..Default::default() };
    /// let registration = ToolRegistration::interactive(info, |_input, context| async move {
    ///     let city = context.request_text("Which city?", false).await?;
    ///     Ok(ToolResult::text(city))
    /// });
    /// let tools = Toolset::local(vec![registration])?;
    /// let observed = async {
    ///     let mut handler = tools.tool_handler(name).await?;
    ///     let pending = handler.step(ToolHandlerInput::Invoke(ToolInvocation { call_id: None, arguments: serde_json::json!({}) })).await?;
    ///     let ToolHandlerOutput::Interaction(request) = pending else { panic!("interaction expected") };
    ///     let reply = ToolInteractionResponse::submit(request.id, serde_json::json!("Chicago"));
    ///     let done = handler.step(ToolHandlerInput::Interaction(reply)).await?;
    ///     let ToolHandlerOutput::Completed(result) = done else { panic!("completion expected") };
    ///     assert!(!result.is_error);
    ///     assert!(matches!(result.content.as_slice(), [tool_sdk::execution::ToolContent::Text { text, .. }] if text == "Chicago"));
    ///     Ok::<(), tool_sdk::ToolError>(())
    /// }.await;
    /// let cleanup = tools.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub async fn request_text(
        &self,
        prompt: impl Into<String>,
        multiline: bool,
    ) -> Result<String, ToolError> {
        if !self.capabilities.text || (multiline && !self.capabilities.multiline_text) {
            return Err(ToolError::UndeclaredInteraction {
                tool: self.tool_name.clone(),
                kind: "text".to_owned(),
            });
        }
        let response = self
            .request(ToolInteractionRequest {
                id: ToolInteractionId::new(),
                tool_name: self.tool_name.clone(),
                call_id: self.call_id.clone(),
                prompt: prompt.into(),
                kind: ToolInteractionKind::Text { multiline },
            })
            .await?;
        match response.into_resolution() {
            InteractionResolution::Submit(content) => content
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| ToolError::InvalidInteraction {
                    id: "submitted-text".to_owned(),
                    message: "text interaction content must be a string".to_owned(),
                }),
            InteractionResolution::Decline(reason) => Err(ToolError::InteractionDeclined {
                tool: self.tool_name.clone(),
                reason,
            }),
            InteractionResolution::Cancel(reason) => Err(ToolError::Cancelled {
                tool: self.tool_name.clone(),
                reason,
            }),
        }
    }

    /// Requests a JSON form value from the caller and waits for its resolution.
    ///
    /// When `schema_validation` is true, the submitted object is validated
    /// before it is returned.
    /// Form replies must be objects even when schema validation is disabled.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::UndeclaredInteraction`] for an absent form capability, validation errors for malformed replies or schemas, or the corresponding decline, cancellation, or closure error.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, declaration::{ToolInfo, ToolName}, execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult}, interaction::{ToolInteractionCapabilities, ToolInteractionResponse}, local::ToolRegistration, schema::JsonSchema};
    /// let name = ToolName::new("ask")?;
    /// let mut info = ToolInfo::new(name.clone(), JsonSchema::strict(serde_json::json!({"type":"object"})));
    /// info.interactions = ToolInteractionCapabilities { form: true, form_schema_validation: true, ..Default::default() };
    /// let registration = ToolRegistration::interactive(info, |_input, context| async move {
    ///     let city = context.request_form("Which city?", serde_json::json!({"type":"object","required":["city"],"properties":{"city":{"type":"string"}}}), true).await?;
    ///     Ok(ToolResult::json(city))
    /// });
    /// let tools = Toolset::local(vec![registration])?;
    /// let observed = async {
    ///     let mut handler = tools.tool_handler(name).await?;
    ///     let pending = handler.step(ToolHandlerInput::Invoke(ToolInvocation { call_id: None, arguments: serde_json::json!({}) })).await?;
    ///     let ToolHandlerOutput::Interaction(request) = pending else { panic!("interaction expected") };
    ///     let reply = ToolInteractionResponse::submit(request.id, serde_json::json!({"city":"Chicago"}));
    ///     let done = handler.step(ToolHandlerInput::Interaction(reply)).await?;
    ///     let ToolHandlerOutput::Completed(result) = done else { panic!("completion expected") };
    ///     assert!(!result.is_error);
    ///     assert_eq!(result.structured_content, Some(serde_json::json!({"city":"Chicago"})));
    ///     Ok::<(), tool_sdk::ToolError>(())
    /// }.await;
    /// let cleanup = tools.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub async fn request_form(
        &self,
        prompt: impl Into<String>,
        schema: serde_json::Value,
        schema_validation: bool,
    ) -> Result<serde_json::Value, ToolError> {
        if !self.capabilities.form
            || (schema_validation && !self.capabilities.form_schema_validation)
        {
            return Err(ToolError::UndeclaredInteraction {
                tool: self.tool_name.clone(),
                kind: "form".to_owned(),
            });
        }
        let response = self
            .request(ToolInteractionRequest {
                id: ToolInteractionId::new(),
                tool_name: self.tool_name.clone(),
                call_id: self.call_id.clone(),
                prompt: prompt.into(),
                kind: ToolInteractionKind::Form {
                    schema,
                    schema_validation,
                },
            })
            .await?;
        match response.into_resolution() {
            InteractionResolution::Submit(content) => Ok(content),
            InteractionResolution::Decline(reason) => Err(ToolError::InteractionDeclined {
                tool: self.tool_name.clone(),
                reason,
            }),
            InteractionResolution::Cancel(reason) => Err(ToolError::Cancelled {
                tool: self.tool_name.clone(),
                reason,
            }),
        }
    }

    async fn request(
        &self,
        request: ToolInteractionRequest,
    ) -> Result<ToolInteractionResponse, ToolError> {
        let (response, receiver) = oneshot::channel();
        self.interactions
            .send(LocalInteractionEnvelope {
                request: request.clone(),
                response,
            })
            .await
            .map_err(|_| ToolError::Transport {
                tool: Some(self.tool_name.clone()),
                message: "local interaction channel closed".to_owned(),
            })?;
        let response = receiver.await.map_err(|_| ToolError::Cancelled {
            tool: self.tool_name.clone(),
            reason: Some("local interaction response channel closed".to_owned()),
        })?;
        request.validate_response(&response)?;
        Ok(response)
    }
}

impl fmt::Debug for ToolInvocationContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolInvocationContext")
            .field("tool_name", &self.tool_name)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}
