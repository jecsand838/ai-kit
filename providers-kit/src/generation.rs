//! Portable generation, messages, function continuation, and streaming values.
//!
//! Construct a [`GenerateRequest`] for an already selected [`crate::ModelBackend`].
//! Returned assistant [`Message`] values retain the exact associated continuation;
//! append them unchanged, then append correlated [`FunctionResult`] values in a
//! tool message. providers-kit declares functions but never executes them.
//!
//! [`GenerateResponse`] is authoritative at completion. [`GenerateEventStream`]
//! provides bounded incremental observation from supplied Models; partial text or
//! hosted projections cannot reconstruct opaque replay state. Serialization rejects
//! messages carrying continuation instead of silently dropping it.
//!
//! # Consume a stream and retain the authoritative response
//!
//! This requires OpenAI credentials and access to the exact selected text model.
//! A complete stream contains one `Completed`; earlier text is only incremental
//! observation. Keep the returned assistant message, not a reconstruction from deltas.
//! A terminal reason such as `Length` or `ContentFilter` is not an ordinary successful
//! answer. This example accepts only `Stop` and does not compare wording to its prompt.
//!
//! ```no_run
//! use futures_util::StreamExt;
//! use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId};
//! use providers_kit::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
//! use providers_kit::generation::{ContentPart, FinishReason, GenerateEvent, GenerateRequest, Message};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
//!     TransportConfig::default(),
//! ))?;
//! let work = async {
//!     let model = provider.model(ModelId::new("gpt-6-luna")?)?;
//!     let request = GenerateRequest::new(vec![Message::user_text("Explain a rainbow briefly.")?])?;
//!     let mut stream = model.generate_stream(&request).await?;
//!     let mut text = String::new();
//!     let mut completed = None;
//!     while let Some(event) = stream.next().await {
//!         match event? {
//!             GenerateEvent::Started { model: returned } => assert_eq!(returned, *model.model_id()),
//!             GenerateEvent::ContentDelta { delta } => text.push_str(&delta),
//!             GenerateEvent::Completed(response) => {
//!                 assert!(completed.is_none(), "duplicate completion");
//!                 completed = Some(response);
//!             }
//!             GenerateEvent::ThinkingDelta { .. } | GenerateEvent::HostedTool(_) | GenerateEvent::Usage(_) => {}
//!         }
//!     }
//!     let response = completed.ok_or("stream ended before Completed")?;
//!     assert_eq!(response.model, *model.model_id());
//!     assert_eq!(response.finish_reason, Some(FinishReason::Stop));
//!     let final_text: String = response.message.content().iter().filter_map(|part| match part {
//!         ContentPart::Text { text } => Some(text.as_str()), _ => None,
//!     }).collect();
//!     assert!(!final_text.trim().is_empty());
//!     assert_eq!(text, final_text); // Fidelity within Kit output, not native parity.
//!     Ok::<_, Box<dyn std::error::Error>>(response.message)
//! }.await;
//! let cleanup = provider.shutdown().await;
//! if let Err(error) = &cleanup {
//!     eprintln!("Cleanup failed: {error}");
//! }
//! let _unchanged_assistant = work?;
//! cleanup?;
//! # Ok(())
//! # }
//! ```
//!
//! # Correlated function continuation
//!
//! providers-kit declares functions; the application executes them. This bounded
//! two-generation example requires the selected OpenAI model, credentials, and
//! function support. Validate the call before dispatching application behavior. The
//! result copies its exact id/name, and the assistant is moved unchanged into history.
//! It includes opaque reasoning/replay state that visible text cannot reconstruct.
//!
//! ```no_run
//! use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId};
//! use providers_kit::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
//! use providers_kit::generation::{ContentPart, FinishReason, FunctionResult, GenerateRequest,
//!     GenerationConfig, JsonSchema, Message, ProviderFunctionTool, ProviderToolSpec, ToolChoice};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
//!     TransportConfig::default(),
//! ))?;
//! let work = async {
//!     let model = provider.model(ModelId::new("gpt-6-luna")?)?;
//!     let lookup = ProviderFunctionTool::new("lookup", Some("Read the local item".into()),
//!         JsonSchema::new(serde_json::json!({"type":"object","properties":{},"additionalProperties":false}))?)?;
//!     let initial = Message::user_text("Look up the local item and summarize it.")?;
//!     let request = GenerateRequest::new(vec![initial.clone()])?
//!         .with_tools(vec![ProviderToolSpec::Function(lookup)])?
//!         .with_generation(GenerationConfig::default().with_tool_choice(ToolChoice::Required))?;
//!     let first = model.generate(&request).await?;
//!     assert_eq!(first.model, *model.model_id());
//!     assert_eq!(first.finish_reason, Some(FinishReason::ToolCall));
//!     let [call] = first.message.function_calls() else { return Err("expected one lookup".into()); };
//!     assert_eq!(call.name(), "lookup");
//!     assert_eq!(call.arguments(), &serde_json::json!({}));
//!     let local_item = serde_json::json!({"name":"paper","quantity":3});
//!     let result = FunctionResult::new(call.id().map(str::to_owned), call.name(),
//!         vec![ContentPart::Json { value: local_item }], false)?;
//!     assert_eq!(result.call_id(), call.id());
//!     let followup = GenerateRequest::new(vec![initial, first.message, Message::tool(vec![result])?])?;
//!     let second = model.generate(&followup).await?;
//!     assert_eq!(second.model, *model.model_id());
//!     assert_eq!(second.finish_reason, Some(FinishReason::Stop));
//!     assert!(second.message.function_calls().is_empty());
//!     assert!(second.message.content().iter().any(|part| matches!(part,
//!         ContentPart::Text { text } if !text.trim().is_empty())));
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! }.await;
//! let cleanup = provider.shutdown().await;
//! if let Err(error) = &cleanup {
//!     eprintln!("Cleanup failed: {error}");
//! }
//! work?;
//! cleanup?;
//! # Ok(())
//! # }
//! ```
//!
//! # Structured output and reasoning
//!
//! Request shape is local and testable. Model compatibility is checked when the
//! request executes: an omitted reasoning override remains omitted; unsupported
//! sampling/reasoning/schema combinations fail rather than silently changing settings.
//! A schema constrains shape; ordinary prompt wording is not an equality guarantee.
//!
//! ```
//! use providers_kit::generation::{GenerateRequest, GenerationConfig, JsonSchema, Message,
//!     ReasoningConfig, ReasoningEffort, ResponseFormat};
//! let schema = JsonSchema::new(serde_json::json!({
//!     "type":"object", "properties":{"summary":{"type":"string"}},
//!     "required":["summary"], "additionalProperties":false
//! }))?;
//! let request = GenerateRequest::<()>::new(vec![Message::user_text("Summarize the supplied note.")?])?
//!     .with_response_format(ResponseFormat::JsonSchema(schema))
//!     .with_reasoning(ReasoningConfig { effort: Some(ReasoningEffort::Low), ..Default::default() })
//!     .with_generation(GenerationConfig::default()
//!         .with_max_output_tokens(std::num::NonZeroU32::new(4096).unwrap()))?;
//! assert!(matches!(request.response_format(), Some(ResponseFormat::JsonSchema(_))));
//! assert_eq!(request.reasoning().unwrap().effort, Some(ReasoningEffort::Low));
//! assert!(request.generation().temperature().is_none());
//! # Ok::<(), providers_kit::KitError>(())
//! ```

//! The same request pattern can be executed on an exact supporting model. This
//! requires credentials and current access to `gpt-6-luna`; no sampling override
//! is inferred from the reasoning setting. Inspect both terminal outcome and the
//! returned JSON rather than assuming the prompt's prose guarantees a value.
//!
//! ```no_run
//! use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId};
//! use providers_kit::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
//! use providers_kit::generation::{ContentPart, FinishReason, GenerateRequest, JsonSchema,
//!     Message, ReasoningConfig, ReasoningEffort, ResponseFormat};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
//!     TransportConfig::default(),
//! ))?;
//! let work = async {
//!     let model = provider.model(ModelId::new("gpt-6-luna")?)?;
//!     let schema = JsonSchema::new(serde_json::json!({"type":"object",
//!         "properties":{"summary":{"type":"string"}}, "required":["summary"], "additionalProperties":false}))?;
//!     let request = GenerateRequest::new(vec![Message::user_text("Summarize: rain supports plant growth.")?])?
//!         .with_response_format(ResponseFormat::JsonSchema(schema))
//!         .with_reasoning(ReasoningConfig { effort: Some(ReasoningEffort::Low), ..Default::default() });
//!     let response = model.generate(&request).await?;
//!     assert_eq!(response.model, *model.model_id());
//!     assert_eq!(response.finish_reason, Some(FinishReason::Stop));
//!     let text: String = response.message.content().iter().filter_map(|part| match part {
//!         ContentPart::Text { text } => Some(text.as_str()), _ => None,
//!     }).collect();
//!     let value: serde_json::Value = serde_json::from_str(&text)?;
//!     assert!(value["summary"].is_string());
//!     assert_eq!(value.as_object().ok_or("expected an object")?.len(), 1);
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! }.await;
//! let cleanup = provider.shutdown().await;
//! if let Err(error) = &cleanup {
//!     eprintln!("Cleanup failed: {error}");
//! }
//! work?;
//! cleanup?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::Stream;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::Error as _};
use serde_json::Value;
use url::Url;

use crate::KitError;
use crate::core::content::base64_bytes;
use crate::core::{GenerateRequestView, selected_tools_for_choice};
use crate::error::ValidationViolation;
use crate::hosted::{HostedTool, HostedToolOutput};
use crate::models::ModelId;

/// Reference that can be submitted as provider-managed or remote model input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileReference {
    /// Provider-managed resource identifier.
    Provider {
        /// Exact provider-assigned identifier or input URI.
        id: String,
        /// Optional media type for selecting the reusable input block kind.
        media_type: Option<String>,
    },
    /// Externally hosted resource.
    Url {
        /// Exact resource URL.
        url: Url,
        /// Optional media-type hint.
        media_type: Option<String>,
    },
}

/// Artifact returned by generation or hosted execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Artifact {
    /// Inline artifact bytes.
    Inline {
        /// Media type describing the bytes.
        media_type: String,
        /// Optional human-readable file name.
        file_name: Option<String>,
        /// Optional model-authored description.
        description: Option<String>,
        /// Raw artifact bytes, serialized as base64.
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
    },
    /// Provider-managed or remote artifact reference.
    File {
        /// Reference usable by a later request.
        reference: FileReference,
        /// Optional human-readable file name.
        file_name: Option<String>,
        /// Optional model-authored description.
        description: Option<String>,
    },
}

/// Ordered portable message content.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentPart {
    /// Plain UTF-8 text.
    Text {
        /// Text content.
        text: String,
    },
    /// Provider-authored thinking replayed with continuation state.
    Thinking {
        /// Provider-visible thinking text, when returned.
        text: Option<String>,
    },
    /// Remote image input.
    ImageUrl {
        /// Absolute image URL.
        url: Url,
    },
    /// Inline image bytes.
    Image {
        /// Image media type.
        media_type: String,
        /// Raw image bytes.
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
    },
    /// Inline audio bytes.
    Audio {
        /// Audio media type.
        media_type: String,
        /// Raw audio bytes.
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
    },
    /// Inline document bytes.
    Document {
        /// Document media type.
        media_type: String,
        /// Raw document bytes.
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
        /// Optional file name presented to the provider.
        file_name: Option<String>,
    },
    /// Provider-managed or remote file input.
    File {
        /// Exact provider or remote file reference.
        reference: FileReference,
    },
    /// Structured function-result content.
    Json {
        /// Structured JSON value.
        value: Value,
    },
}

/// Portable message role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// System instruction.
    System,
    /// Developer instruction.
    Developer,
    /// User input.
    User,
    /// Model-authored assistant output.
    Assistant,
    /// Caller-supplied function output.
    Tool,
}

/// Opaque provider-authored state required to continue one model turn.
///
/// Returned assistant messages carry the selected Model's exact continuation type.
/// Append that message unchanged before the corresponding tool-result message.
/// Native continuation contents have no public constructor or serialization contract.
/// Serializing any message that carries continuation fails rather than discarding it.
/// Caller models may choose their own Clone + Send + Sync continuation type.
///
/// # Examples
///
/// ```
/// use providers_kit::generation::{GenerationContinuation, Message};
/// let message = Message::<GenerationContinuation>::assistant_text("Hello")?;
/// assert!(message.continuation().is_none());
/// assert!(serde_json::to_value(&message).is_ok());
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, PartialEq)]
pub struct GenerationContinuation {
    pub(crate) model_id: ModelId,
    pub(crate) inner: NativeContinuation,
}

#[derive(Clone, PartialEq)]
pub(crate) enum NativeContinuation {
    OpenAi { output_items: Vec<Value> },
    Anthropic { content_blocks: Vec<Value> },
    Gemini { signed_parts: Vec<Value> },
    Ollama { thinking: String },
}

impl fmt::Debug for GenerationContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenerationContinuation")
            .field("model_id", &self.model_id)
            .field("state", &"<redacted>")
            .finish()
    }
}

/// Portable message with private function and continuation state.
///
/// A caller Model can return its own continuation type. Preserve that exact
/// assistant value before appending the matching function-result message:
///
/// ```
/// use providers_kit::generation::{
///     ContentPart, FunctionCall, FunctionResult, GenerateRequest, Message,
/// };
/// let user = Message::<u32>::user_text("Look up record a")?;
/// // Representative output constructed by a caller Model with Continuation = u32.
/// let call = FunctionCall::new(Some("call-1".into()), "lookup", serde_json::json!({"key":"a"}))?;
/// let assistant = Message::assistant(Vec::new(), vec![call])?.with_continuation(7)?;
/// let result = FunctionResult::new(Some("call-1".into()), "lookup",
///     vec![ContentPart::Json { value: serde_json::json!({"value":2}) }], false)?;
/// let next = GenerateRequest::new(vec![user, assistant, Message::tool(vec![result])?])?;
/// assert_eq!(next.messages()[1].continuation(), Some(&7));
/// assert_eq!(next.messages()[2].function_results()[0].call_id(), Some("call-1"));
/// assert!(serde_json::to_value(&next).is_err());
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
/// Role, function calls/results, and continuation are validated together. Supplied
/// continuations stay opaque; caller Models use their own exact associated type.
/// Assistant messages returned by a Model must be replayed unchanged.
///
/// See [`crate::generation::GenerationContinuation`] for a complete example.
///
#[derive(Clone, PartialEq)]
pub struct Message<C = GenerationContinuation> {
    role: Role,
    content: Vec<ContentPart>,
    function_calls: Vec<FunctionCall>,
    function_results: Vec<FunctionResult>,
    continuation: Option<C>,
}

impl<C> fmt::Debug for Message<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Message")
            .field("role", &self.role)
            .field("content", &self.content)
            .field("function_calls", &self.function_calls)
            .field("function_results", &self.function_results)
            .field(
                "continuation",
                &self.continuation.is_some().then_some("<redacted>"),
            )
            .finish()
    }
}

impl<C> Message<C> {
    fn from_role(role: Role, content: Vec<ContentPart>) -> Self {
        Self {
            role,
            content,
            function_calls: Vec::new(),
            function_results: Vec::new(),
            continuation: None,
        }
    }

    /// Creates a text-only system instruction.
    ///
    /// # Errors
    ///
    /// Returns an input error for empty or invalid content, or content incompatible with the role. Instruction messages permit only text; user messages reject thinking and raw JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{Message, ContentPart, Role};
    /// let message = Message::<()>::system(vec![ContentPart::Text { text: "Hello".into() }])?;
    /// assert_eq!(message.role(), Role::System);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn system(content: Vec<ContentPart>) -> Result<Self, KitError> {
        Self::instruction(Role::System, content)
    }

    /// Creates a text-only developer instruction.
    ///
    /// # Errors
    ///
    /// Returns an input error for empty or invalid content, or content incompatible with the role. Instruction messages permit only text; user messages reject thinking and raw JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{Message, ContentPart, Role};
    /// let message = Message::<()>::developer(vec![ContentPart::Text { text: "Hello".into() }])?;
    /// assert_eq!(message.role(), Role::Developer);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn developer(content: Vec<ContentPart>) -> Result<Self, KitError> {
        Self::instruction(Role::Developer, content)
    }

    fn instruction(role: Role, content: Vec<ContentPart>) -> Result<Self, KitError> {
        let message = Self::from_role(role, content);
        message.validate_shape()?;
        Ok(message)
    }

    /// Creates a user message.
    ///
    /// # Errors
    ///
    /// Returns an input error for empty or invalid content, or content incompatible with the role. Instruction messages permit only text; user messages reject thinking and raw JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{Message, ContentPart, Role};
    /// let message = Message::<()>::user(vec![ContentPart::Text { text: "Hello".into() }])?;
    /// assert_eq!(message.role(), Role::User);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn user(content: Vec<ContentPart>) -> Result<Self, KitError> {
        let message = Self::from_role(Role::User, content);
        message.validate_shape()?;
        Ok(message)
    }

    /// Creates an assistant message with visible content and/or function calls.
    ///
    /// # Errors
    ///
    /// Returns an input error if both content and calls are empty, content is malformed, or the role cannot represent the supplied values.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::FunctionCall;
    /// let call = FunctionCall::new(Some("call-1".into()), "lookup", serde_json::json!({"key":"a"}))?;
    /// use providers_kit::generation::Message;
    /// let message = Message::<()>::assistant(Vec::new(), vec![call])?;
    /// assert_eq!(message.function_calls().len(), 1);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn assistant(
        content: Vec<ContentPart>,
        function_calls: Vec<FunctionCall>,
    ) -> Result<Self, KitError> {
        let message = Self {
            function_calls,
            ..Self::from_role(Role::Assistant, content)
        };
        message.validate_shape()?;
        Ok(message)
    }

    /// Creates a tool message containing validated function results.
    ///
    /// # Errors
    ///
    /// Returns an input error when no function results are supplied or the message shape is invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{FunctionResult, ContentPart};
    /// let result = FunctionResult::new(
    ///     Some("call-1".into()),
    ///     "lookup",
    ///     vec![ContentPart::Json {
    ///         value: serde_json::json!({"value":2}),
    ///     }],
    ///     false,
    /// )?;
    /// use providers_kit::generation::Message;
    /// let message = Message::<()>::tool(vec![result])?;
    /// assert_eq!(message.function_results()[0].call_id(), Some("call-1"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn tool(results: Vec<FunctionResult>) -> Result<Self, KitError> {
        let message = Self {
            function_results: results,
            ..Self::from_role(Role::Tool, Vec::new())
        };
        message.validate_shape()?;
        Ok(message)
    }

    /// Creates one user text message.
    ///
    /// # Errors
    ///
    /// Returns an input error when the text is empty or whitespace-only.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{Message, Role};
    /// let message = Message::<()>::user_text("Hello")?;
    /// assert_eq!(message.role(), Role::User);
    /// assert_eq!(message.content().len(), 1);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn user_text(text: impl Into<String>) -> Result<Self, KitError> {
        Self::user(vec![ContentPart::Text { text: text.into() }])
    }

    /// Creates one assistant text message.
    ///
    /// # Errors
    ///
    /// Returns an input error when the text is empty or whitespace-only.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{Message, Role};
    /// let message = Message::<()>::assistant_text("Hello")?;
    /// assert_eq!(message.role(), Role::Assistant);
    /// assert_eq!(message.content().len(), 1);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn assistant_text(text: impl Into<String>) -> Result<Self, KitError> {
        Self::assistant(vec![ContentPart::Text { text: text.into() }], Vec::new())
    }

    /// Attaches continuation state to an assistant message.
    ///
    /// Continuation is valid only on assistant messages and is never silently serialized.
    /// Use this constructor for a caller Model's continuation; preserve supplied returned
    /// messages unchanged rather than reconstructing their private replay state.
    ///
    /// # Errors
    ///
    /// Returns an input error when the message role is not Assistant.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::Message;
    /// let message = Message::assistant_text("continue")?.with_continuation(7_u32)?;
    /// assert_eq!(message.continuation(), Some(&7));
    /// assert!(serde_json::to_value(&message).is_err());
    /// assert!(Message::user_text("hello")?.with_continuation(7_u32).is_err());
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_continuation(mut self, continuation: C) -> Result<Self, KitError> {
        if self.role != Role::Assistant {
            return Err(KitError::invalid_input(
                ValidationViolation::Incompatible {
                    field: "message.continuation",
                    other: "message.role",
                },
                "continuation can be attached only to assistant messages",
            ));
        }
        self.continuation = Some(continuation);
        Ok(self)
    }

    /// Returns the speaker role.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{Message, ContentPart, Role};
    /// let message = Message::<()>::user_text("Hello")?;
    /// assert_eq!(message.role(), Role::User);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Returns ordered visible content.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{Message, ContentPart, Role};
    /// let message = Message::<()>::user_text("Hello")?;
    /// assert!(matches!(&message.content()[0], ContentPart::Text { text } if text == "Hello"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn content(&self) -> &[ContentPart] {
        &self.content
    }

    /// Returns model-requested function calls.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::FunctionCall;
    /// let call = FunctionCall::new(Some("call-1".into()), "lookup", serde_json::json!({"key":"a"}))?;
    /// let message = providers_kit::generation::Message::<()>::assistant(Vec::new(), vec![call])?;
    /// assert_eq!(message.function_calls()[0].name(), "lookup");
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn function_calls(&self) -> &[FunctionCall] {
        &self.function_calls
    }

    /// Returns caller-supplied function results.
    ///
    /// See [`crate::generation::Message::tool`] for a complete example.
    ///
    #[must_use]
    pub fn function_results(&self) -> &[FunctionResult] {
        &self.function_results
    }

    /// Returns opaque continuation state when this is a returned assistant message.
    ///
    /// See [`crate::generation::Message::with_continuation`] for a complete example.
    ///
    #[must_use]
    pub const fn continuation(&self) -> Option<&C> {
        self.continuation.as_ref()
    }

    pub(crate) fn try_map_continuation<D>(
        self,
        map: &mut impl FnMut(C) -> Result<D, KitError>,
    ) -> Result<Message<D>, KitError> {
        Ok(Message {
            role: self.role,
            content: self.content,
            function_calls: self.function_calls,
            function_results: self.function_results,
            continuation: self.continuation.map(map).transpose()?,
        })
    }

    pub(crate) fn returned_assistant(
        content: Vec<ContentPart>,
        function_calls: Vec<FunctionCall>,
        continuation: Option<C>,
    ) -> Self {
        Self {
            role: Role::Assistant,
            content,
            function_calls,
            function_results: Vec::new(),
            continuation,
        }
    }

    pub(crate) fn validate_for_request(&self) -> Result<(), KitError> {
        self.validate_shape()?;
        if self
            .content
            .iter()
            .any(|part| matches!(part, ContentPart::Thinking { .. }))
            && self.continuation.is_none()
        {
            return Err(KitError::invalid_input(
                ValidationViolation::MissingField {
                    field: "message.continuation",
                },
                "thinking input requires assistant continuation state",
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), KitError> {
        for part in &self.content {
            part.validate()?;
        }
        match self.role {
            Role::System | Role::Developer => {
                if self.content.is_empty()
                    || self
                        .content
                        .iter()
                        .any(|part| !matches!(part, ContentPart::Text { .. }))
                {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "message.content",
                            rule: "instructions require non-empty text-only content",
                        },
                        "instruction messages require non-empty text-only content",
                    ));
                }
                if !self.function_calls.is_empty()
                    || !self.function_results.is_empty()
                    || self.continuation.is_some()
                {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "message.role",
                            rule: "instructions cannot carry functions or continuation",
                        },
                        "instruction messages cannot carry functions or continuation state",
                    ));
                }
            }
            Role::User => {
                if self.content.is_empty() {
                    return Err(KitError::invalid_input(
                        ValidationViolation::Empty {
                            field: "message.content",
                        },
                        "user message content must not be empty",
                    ));
                }
                if self.content.iter().any(|part| {
                    matches!(
                        part,
                        ContentPart::Thinking { .. } | ContentPart::Json { .. }
                    )
                }) {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "message.content",
                            rule: "user content cannot include thinking or raw JSON",
                        },
                        "user messages cannot contain thinking or raw JSON content",
                    ));
                }
                if !self.function_calls.is_empty()
                    || !self.function_results.is_empty()
                    || self.continuation.is_some()
                {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "message.role",
                            rule: "user messages cannot carry functions or continuation",
                        },
                        "user messages cannot carry functions or continuation state",
                    ));
                }
            }
            Role::Assistant => {
                if self.content.is_empty()
                    && self.function_calls.is_empty()
                    && self.continuation.is_none()
                {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "message",
                            rule: "assistant requires content, calls, or continuation",
                        },
                        "assistant message requires content, a function call, or retained continuation",
                    ));
                }
                if self.content.iter().any(|part| {
                    !matches!(
                        part,
                        ContentPart::Text { .. } | ContentPart::Thinking { .. }
                    )
                }) {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "message.content",
                            rule: "assistant input requires text or continued thinking",
                        },
                        "assistant input supports only text and continued thinking content",
                    ));
                }
                if !self.function_results.is_empty() {
                    return Err(KitError::invalid_input(
                        ValidationViolation::Incompatible {
                            field: "message.function_results",
                            other: "message.role",
                        },
                        "assistant messages cannot carry function results",
                    ));
                }
            }
            Role::Tool => {
                if !self.content.is_empty()
                    || !self.function_calls.is_empty()
                    || self.function_results.is_empty()
                    || self.continuation.is_some()
                {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "message",
                            rule: "tool messages require only function results",
                        },
                        "tool messages require function results and no direct content, calls, or continuation",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl GenerationContinuation {
    pub(crate) fn open_ai(model_id: ModelId, output_items: Vec<Value>) -> Self {
        Self {
            model_id,
            inner: NativeContinuation::OpenAi { output_items },
        }
    }

    pub(crate) fn anthropic(model_id: ModelId, content_blocks: Vec<Value>) -> Self {
        Self {
            model_id,
            inner: NativeContinuation::Anthropic { content_blocks },
        }
    }

    pub(crate) fn gemini(model_id: ModelId, signed_parts: Vec<Value>) -> Self {
        Self {
            model_id,
            inner: NativeContinuation::Gemini { signed_parts },
        }
    }

    pub(crate) fn ollama(model_id: ModelId, thinking: String) -> Self {
        Self {
            model_id,
            inner: NativeContinuation::Ollama { thinking },
        }
    }
}

impl<C> Serialize for Message<C> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.continuation.is_some() {
            return Err(S::Error::custom(
                "provider continuation cannot be serialized; retain the returned message in memory",
            ));
        }
        #[derive(Serialize)]
        struct Wire<'a> {
            role: Role,
            content: &'a [ContentPart],
            function_calls: &'a [FunctionCall],
            function_results: &'a [FunctionResult],
        }
        Wire {
            role: self.role,
            content: &self.content,
            function_calls: &self.function_calls,
            function_results: &self.function_results,
        }
        .serialize(serializer)
    }
}

impl<'de, C> Deserialize<'de> for Message<C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            role: Role,
            #[serde(default)]
            content: Vec<ContentPart>,
            #[serde(default)]
            function_calls: Vec<FunctionCall>,
            #[serde(default)]
            function_results: Vec<FunctionResult>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let message = Self {
            role: wire.role,
            content: wire.content,
            function_calls: wire.function_calls,
            function_results: wire.function_results,
            continuation: None,
        };
        message
            .validate_for_request()
            .map_err(serde::de::Error::custom)?;
        Ok(message)
    }
}

/// Validated strict object JSON schema.
///
/// # Examples
///
/// ```
/// use providers_kit::generation::JsonSchema;
/// let schema = JsonSchema::new(
///     serde_json::json!({"type":"object", "properties":{"answer":{"type":"string"}}}),
/// )?;
/// assert_eq!(schema.schema()["type"], "object");
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq)]
pub struct JsonSchema(Value);

impl JsonSchema {
    /// Validates and owns an object-shaped JSON schema.
    ///
    /// # Errors
    ///
    /// Returns an input error unless the schema is a JSON object whose root declares `type: object`. The selected operation performs any stricter schema or provider validation.
    ///
    /// See [`crate::generation::JsonSchema`] for a complete example.
    ///
    pub fn new(schema: Value) -> Result<Self, KitError> {
        let object = schema.as_object().ok_or_else(|| {
            KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "json_schema",
                    rule: "JSON object",
                },
                "JSON schema must be encoded as an object",
            )
        })?;
        if !matches!(object.get("type"), Some(Value::String(kind)) if kind == "object") {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidValue {
                    field: "json_schema.type",
                    rule: "must declare object",
                },
                "JSON schema root must declare `type: object`",
            ));
        }
        Ok(Self(schema))
    }

    /// Borrows the validated schema document.
    ///
    /// See [`crate::generation::JsonSchema`] for a complete example.
    ///
    #[must_use]
    pub const fn schema(&self) -> &Value {
        &self.0
    }

    /// Consumes the wrapper and returns the schema document.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::JsonSchema;
    /// let schema = JsonSchema::new(
    ///     serde_json::json!({"type":"object", "properties":{"answer":{"type":"string"}}}),
    /// )?;
    /// assert_eq!(schema.into_value()["type"], "object");
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn into_value(self) -> Value {
        self.0
    }
}

impl Serialize for JsonSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JsonSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Requested output format for a generation request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseFormat {
    /// Plain text output.
    Text,
    /// A provider-validated JSON object without a caller schema.
    JsonObject,
    /// JSON validated against a strict caller schema.
    JsonSchema(JsonSchema),
}

/// Portable reasoning effort hint for providers that support explicit thinking controls.
///
/// # Examples
/// ```rust
/// let effort = providers_kit::generation::ReasoningEffort::High;
/// assert!(matches!(effort, providers_kit::generation::ReasoningEffort::High));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    /// Requests the lightest available reasoning budget.
    Minimal,
    /// Requests a low reasoning budget when the provider exposes several tiers.
    Low,
    /// Requests a balanced reasoning budget between latency and depth.
    Medium,
    /// Requests a high reasoning budget for harder prompts.
    High,
    /// Requests the deepest reasoning tier the provider allows.
    Maximum,
}

/// Controls how much reasoning summary a provider should return.
///
/// # Examples
/// ```rust
/// let mode = providers_kit::generation::ReasoningSummaryMode::Summary;
/// assert!(matches!(mode, providers_kit::generation::ReasoningSummaryMode::Summary));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningSummaryMode {
    /// Lets the provider decide whether and how to summarize its reasoning.
    Auto,
    /// Suppresses any reasoning summary in the provider response when supported.
    Hidden,
    /// Requests a concise reasoning summary.
    Summary,
    /// Requests the most detailed reasoning summary the provider exposes.
    Verbose,
}

/// Optional reasoning controls attached to a generation request.
///
/// # Examples
/// ```rust
/// let config = providers_kit::generation::ReasoningConfig {
///     effort: Some(providers_kit::generation::ReasoningEffort::High),
///     ..Default::default()
/// };
/// assert_eq!(config.effort, Some(providers_kit::generation::ReasoningEffort::High));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningConfig {
    /// Requested reasoning effort tier when the backend supports explicit controls.
    pub effort: Option<ReasoningEffort>,
    /// Upper bound for provider-native reasoning tokens when that concept exists.
    pub max_reasoning_tokens: Option<u32>,
    /// Requested reasoning-summary presentation mode.
    pub summary_mode: ReasoningSummaryMode,
}

impl Default for ReasoningConfig {
    fn default() -> Self {
        Self {
            effort: None,
            max_reasoning_tokens: None,
            summary_mode: ReasoningSummaryMode::Auto,
        }
    }
}

/// Policy for how strongly the allowed-tool list should be enforced.
///
/// # Examples
/// ```rust
/// let mode = providers_kit::generation::AllowedToolMode::Required;
/// assert!(matches!(mode, providers_kit::generation::AllowedToolMode::Required));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllowedToolMode {
    /// Lets the provider decide how strictly to enforce the allowed-tool list.
    Auto,
    /// Requires the provider to keep tool calls inside the allowed-tool list.
    Required,
}

impl Serialize for AllowedToolMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AllowedToolMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_wire(&value)
    }
}

impl AllowedToolMode {
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Required => "required",
        }
    }

    fn from_wire<E>(value: &str) -> Result<Self, E>
    where
        E: serde::de::Error,
    {
        match value {
            "required" => Ok(Self::Required),
            "auto" => Ok(Self::Auto),
            other => Err(E::custom(format!("unknown allowed tool mode `{other}`"))),
        }
    }
}

/// Portable tool-selection policy for a request.
///
/// Provider renderers map this policy into their native tool-choice shape after
/// the selected model has validated whether tools, strict schemas, and forced
/// choices are supported together.
///
/// # Examples
/// ```rust
/// let choice = providers_kit::generation::ToolChoice::Tool {
///     name: "lookup_weather".into(),
/// };
/// assert!(matches!(choice, providers_kit::generation::ToolChoice::Tool { .. }));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolChoice {
    /// Lets the provider choose whether tools should be used.
    Auto,
    /// Disables tool calling for the request.
    None,
    /// Requires at least one tool call before completion.
    Required,
    /// Allows any declared tool to be called.
    Any,
    /// Forces the provider to call one specific tool.
    Tool {
        /// Declared tool name that must be used.
        name: String,
    },
    /// Restricts tool calls to an explicit subset of tool names.
    Allowed {
        /// Enforcement mode for the allowed-tool subset.
        mode: AllowedToolMode,
        /// Tool names that remain eligible for selection.
        names: Vec<String>,
    },
}

impl Serialize for ToolChoice {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ToolChoice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Value::deserialize(deserializer)? {
            Value::String(value) => match value.as_str() {
                "auto" => Ok(Self::Auto),
                "none" => Ok(Self::None),
                "required" => Ok(Self::Required),
                "any" => Ok(Self::Any),
                other => Err(serde::de::Error::custom(format!(
                    "unknown tool choice variant `{other}`"
                ))),
            },
            Value::Object(object) => {
                serde_json::from_value::<ToolChoiceObjectWire>(Value::Object(object))
                    .map_err(serde::de::Error::custom)?
                    .into_tool_choice()
            }
            _ => Err(serde::de::Error::custom(
                "tool choice must be encoded as a string or object",
            )),
        }
    }
}

impl ToolChoice {
    fn as_wire(&self) -> ToolChoiceWireRef<'_> {
        match self {
            Self::Auto => ToolChoiceWireRef::String("auto"),
            Self::None => ToolChoiceWireRef::String("none"),
            Self::Required => ToolChoiceWireRef::String("required"),
            Self::Any => ToolChoiceWireRef::String("any"),
            Self::Tool { name } => ToolChoiceWireRef::Object(ToolChoiceObjectWireRef {
                kind: "tool",
                mode: None,
                name: Some(name),
                names: None,
            }),
            Self::Allowed { mode, names } => ToolChoiceWireRef::Object(ToolChoiceObjectWireRef {
                kind: "allowed",
                mode: Some(mode.as_wire()),
                name: None,
                names: Some(names.as_slice()),
            }),
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum ToolChoiceWireRef<'a> {
    String(&'static str),
    Object(ToolChoiceObjectWireRef<'a>),
}

#[derive(Serialize)]
struct ToolChoiceObjectWireRef<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    names: Option<&'a [String]>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolChoiceObjectWire {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    names: Option<Value>,
}

impl ToolChoiceObjectWire {
    fn into_tool_choice<E>(self) -> Result<ToolChoice, E>
    where
        E: serde::de::Error,
    {
        let kind = self
            .kind
            .ok_or_else(|| E::custom("tool choice objects require a string `type` field"))?;
        match kind.as_str() {
            "tool" => {
                let name = self
                    .name
                    .filter(|name| !name.trim().is_empty())
                    .ok_or_else(|| E::custom("tool choice objects require a non-empty `name`"))?;
                Ok(ToolChoice::Tool { name })
            }
            "allowed" => Ok(ToolChoice::Allowed {
                mode: self
                    .mode
                    .as_deref()
                    .map(AllowedToolMode::from_wire)
                    .transpose()?
                    .unwrap_or(AllowedToolMode::Auto),
                names: parse_allowed_tool_names(self.names)?,
            }),
            other => Err(E::custom(format!(
                "unknown tool choice object type `{other}`"
            ))),
        }
    }
}

fn parse_allowed_tool_names<E>(value: Option<Value>) -> Result<Vec<String>, E>
where
    E: serde::de::Error,
{
    match value {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .into_iter()
            .map(|item| match item {
                Value::String(name) if !name.trim().is_empty() => Ok(name),
                Value::String(_) => {
                    Err(E::custom("allowed tool entries must be non-empty strings"))
                }
                _ => Err(E::custom("allowed tool entries must be strings")),
            })
            .collect(),
        Some(_) => Err(E::custom("allowed tool names must be encoded as an array")),
    }
}

/// Validated client-executed function declaration shown to a model.
///
/// This declares a caller-executed function. providers-kit never executes the callback;
/// the caller supplies correlated [`FunctionResult`] values after returned calls.
///
/// # Examples
///
/// ```
/// use providers_kit::generation::{ProviderFunctionTool, JsonSchema};
/// let tool = ProviderFunctionTool::new(
///     "lookup",
///     Some("Find a record".into()),
///     JsonSchema::new(serde_json::json!({"type":"object"}))?,
/// )?;
/// assert_eq!(tool.name(), "lookup");
/// assert_eq!(tool.description(), Some("Find a record"));
/// assert_eq!(tool.input_schema().schema()["type"], "object");
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProviderFunctionTool {
    name: String,
    description: Option<String>,
    input_schema: JsonSchema,
}

impl ProviderFunctionTool {
    /// Creates a function declaration with a stable non-empty name.
    ///
    /// # Errors
    ///
    /// Returns an input error for an invalid function name or blank description. The input schema has already been validated by [`JsonSchema::new`].
    ///
    /// See [`crate::generation::ProviderFunctionTool`] for a complete example.
    ///
    pub fn new(
        name: impl Into<String>,
        description: Option<String>,
        input_schema: JsonSchema,
    ) -> Result<Self, KitError> {
        let name = name.into();
        FunctionCall::validate_name("tools.function.name", &name)?;
        if description
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "tools.function.description",
                },
                "function tool description must be omitted or non-empty",
            ));
        }
        Ok(Self {
            name,
            description,
            input_schema,
        })
    }

    /// Returns the provider-visible function name.
    ///
    /// See [`crate::generation::ProviderFunctionTool`] for a complete example.
    ///
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional model-visible description.
    ///
    /// See [`crate::generation::ProviderFunctionTool`] for a complete example.
    ///
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the strict argument schema.
    ///
    /// See [`crate::generation::ProviderFunctionTool`] for a complete example.
    ///
    #[must_use]
    pub const fn input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    /// Consumes the declaration into its validated parts.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{ProviderFunctionTool, JsonSchema};
    /// let tool = ProviderFunctionTool::new(
    ///     "lookup",
    ///     Some("Find a record".into()),
    ///     JsonSchema::new(serde_json::json!({"type":"object"}))?,
    /// )?;
    /// let (name, description, schema) = tool.into_parts();
    /// assert_eq!(name, "lookup");
    /// assert_eq!(description.as_deref(), Some("Find a record"));
    /// assert_eq!(schema.schema()["type"], "object");
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn into_parts(self) -> (String, Option<String>, JsonSchema) {
        (self.name, self.description, self.input_schema)
    }
}

impl<'de> Deserialize<'de> for ProviderFunctionTool {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            name: String,
            description: Option<String>,
            input_schema: JsonSchema,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.name, wire.description, wire.input_schema).map_err(serde::de::Error::custom)
    }
}

/// Tool declaration supplied to a generation request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "declaration", rename_all = "snake_case")]
pub enum ProviderToolSpec {
    /// Client-executed function.
    Function(ProviderFunctionTool),
    /// Provider-hosted execution.
    Hosted(HostedTool),
}

impl ProviderToolSpec {
    pub(crate) const fn hosted_tool(&self) -> Option<&HostedTool> {
        match self {
            Self::Function(_) => None,
            Self::Hosted(tool) => Some(tool),
        }
    }
}

/// Validated client function call requested by a model.
///
/// # Examples
///
/// ```
/// use providers_kit::generation::FunctionCall;
/// let call = FunctionCall::new(Some("call-1".into()), "lookup", serde_json::json!({"key":"a"}))?;
/// assert_eq!(call.id(), Some("call-1"));
/// assert_eq!(call.name(), "lookup");
/// assert_eq!(call.arguments()["key"], "a");
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FunctionCall {
    id: Option<String>,
    name: String,
    arguments: Value,
}

impl<'de> Deserialize<'de> for FunctionCall {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            id: Option<String>,
            name: String,
            arguments: Value,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.id, wire.name, wire.arguments).map_err(serde::de::Error::custom)
    }
}

impl FunctionCall {
    /// Creates a validated function call.
    ///
    /// # Errors
    ///
    /// Returns an input error for an invalid portable function name, invalid optional call id, or arguments that are not a JSON object. Keep returned call correlation unchanged in its result.
    ///
    /// See [`crate::generation::FunctionCall`] for a complete example.
    ///
    pub fn new(
        id: Option<String>,
        name: impl Into<String>,
        arguments: Value,
    ) -> Result<Self, KitError> {
        let name = name.into();
        Self::validate_name("function_call.name", &name)?;
        if let Some(id) = id.as_deref() {
            Self::validate_id("function_call.id", id)?;
        }
        if !arguments.is_object() {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "function_call.arguments",
                    rule: "JSON object",
                },
                "function arguments must be a JSON object",
            ));
        }
        Ok(Self {
            id,
            name,
            arguments,
        })
    }

    /// Returns the provider correlation id, when one was supplied.
    ///
    /// See [`crate::generation::FunctionCall`] for a complete example.
    ///
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Returns the function name.
    ///
    /// See [`crate::generation::FunctionCall`] for a complete example.
    ///
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the validated object-shaped arguments.
    ///
    /// See [`crate::generation::FunctionCall`] for a complete example.
    ///
    #[must_use]
    pub fn arguments(&self) -> &Value {
        &self.arguments
    }

    pub(crate) fn validate_name(label: &'static str, value: &str) -> Result<(), KitError> {
        let mut bytes = value.bytes();
        let first = bytes.next();
        if value.len() > 64
            || !first.is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: label,
                    rule: "1..=64 ASCII letters, digits, underscores, or hyphens; first character letter or underscore",
                },
                format!(
                    "{label} must use 1..=64 portable ASCII letters, digits, underscores, or hyphens and start with a letter or underscore"
                ),
            ))
        } else {
            Ok(())
        }
    }

    fn validate_id(label: &'static str, value: &str) -> Result<(), KitError> {
        if value.is_empty()
            || value.len() > 256
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: label,
                    rule: "1..=256 unpadded UTF-8 bytes without control characters",
                },
                format!(
                    "{label} must contain 1..=256 unpadded UTF-8 bytes without control characters"
                ),
            ))
        } else {
            Ok(())
        }
    }
}

/// Validated caller-supplied result for a client function call.
///
/// # Examples
///
/// ```
/// use providers_kit::generation::{FunctionResult, ContentPart};
/// let result = FunctionResult::new(
///     Some("call-1".into()),
///     "lookup",
///     vec![ContentPart::Json {
///         value: serde_json::json!({"value":2}),
///     }],
///     false,
/// )?;
/// assert_eq!(result.call_id(), Some("call-1"));
/// assert_eq!(result.name(), "lookup");
/// assert_eq!(result.content().len(), 1);
/// assert!(!result.is_error());
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FunctionResult {
    call_id: Option<String>,
    name: String,
    content: Vec<ContentPart>,
    is_error: bool,
}

impl<'de> Deserialize<'de> for FunctionResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            call_id: Option<String>,
            name: String,
            content: Vec<ContentPart>,
            is_error: bool,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.call_id, wire.name, wire.content, wire.is_error)
            .map_err(serde::de::Error::custom)
    }
}

impl FunctionResult {
    /// Creates a validated function result.
    ///
    /// # Errors
    ///
    /// Returns an input error for an invalid name/id, empty or invalid content, or provider thinking content. Accepted function failures use `is_error = true` with nonempty model-visible content.
    ///
    /// See [`crate::generation::FunctionResult`] for a complete example.
    ///
    pub fn new(
        call_id: Option<String>,
        name: impl Into<String>,
        content: Vec<ContentPart>,
        is_error: bool,
    ) -> Result<Self, KitError> {
        let name = name.into();
        FunctionCall::validate_name("function_result.name", &name)?;
        if let Some(call_id) = call_id.as_deref() {
            FunctionCall::validate_id("function_result.call_id", call_id)?;
        }
        if content.is_empty() {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "function_result.content",
                },
                "function result content must not be empty",
            ));
        }
        for part in &content {
            part.validate()?;
        }
        if content
            .iter()
            .any(|part| matches!(part, ContentPart::Thinking { .. }))
        {
            return Err(KitError::invalid_input(
                ValidationViolation::Unsupported {
                    field: "function_result.content",
                    rule: "thinking content is not permitted",
                },
                "function results cannot contain provider thinking state",
            ));
        }
        Ok(Self {
            call_id,
            name,
            content,
            is_error,
        })
    }

    /// Returns the originating call id, when available.
    ///
    /// See [`crate::generation::FunctionResult`] for a complete example.
    ///
    #[must_use]
    pub fn call_id(&self) -> Option<&str> {
        self.call_id.as_deref()
    }

    /// Returns the function name.
    ///
    /// See [`crate::generation::FunctionResult`] for a complete example.
    ///
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns ordered model-visible result content.
    ///
    /// See [`crate::generation::FunctionResult`] for a complete example.
    ///
    #[must_use]
    pub fn content(&self) -> &[ContentPart] {
        &self.content
    }

    /// Reports whether the completed function execution failed.
    ///
    /// See [`crate::generation::FunctionResult`] for a complete example.
    ///
    #[must_use]
    pub const fn is_error(&self) -> bool {
        self.is_error
    }
}

/// Optional sampling and tool-choice overrides.
///
/// These are portable requested controls. Local validation does not establish support
/// for every selected model; unsupported combinations fail at the model operation.
///
/// # Examples
///
/// ```
/// use providers_kit::generation::GenerationConfig;
/// let config = GenerationConfig::default().with_temperature(0.5)?.with_seed(42);
/// assert_eq!(config.temperature(), Some(0.5));
/// assert_eq!(config.seed(), Some(42));
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GenerationConfig {
    temperature: Option<f32>,
    top_p: Option<f32>,
    top_k: Option<NonZeroU32>,
    max_output_tokens: Option<NonZeroU32>,
    stop_sequences: Vec<String>,
    presence_penalty: Option<f32>,
    frequency_penalty: Option<f32>,
    seed: Option<u64>,
    tool_choice: Option<ToolChoice>,
}

impl GenerationConfig {
    /// Sets a finite non-negative sampling temperature.
    ///
    /// # Errors
    ///
    /// Returns an input error unless temperature is finite and between 0 and 2 inclusive.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config = GenerationConfig::default().with_temperature(0.5)?;
    /// assert_eq!(config.temperature(), Some(0.5));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_temperature(mut self, value: f32) -> Result<Self, KitError> {
        if !value.is_finite() || value < 0.0 {
            return Err(KitError::invalid_input(
                ValidationViolation::OutOfRange {
                    field: "generation.temperature",
                    rule: "finite and non-negative",
                },
                "temperature must be finite and non-negative",
            ));
        }
        self.temperature = Some(value);
        Ok(self)
    }

    /// Sets a finite nucleus-sampling probability in `(0, 1]`.
    ///
    /// # Errors
    ///
    /// Returns an input error unless top_p is finite and between 0 and 1 inclusive.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config = GenerationConfig::default().with_top_p(0.8)?;
    /// assert_eq!(config.top_p(), Some(0.8));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_top_p(mut self, value: f32) -> Result<Self, KitError> {
        if !(value.is_finite() && 0.0 < value && value <= 1.0) {
            return Err(KitError::invalid_input(
                ValidationViolation::OutOfRange {
                    field: "generation.top_p",
                    rule: "finite and in (0, 1]",
                },
                "top_p must be in (0, 1]",
            ));
        }
        self.top_p = Some(value);
        Ok(self)
    }

    /// Sets a non-zero top-k cutoff.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config = GenerationConfig::default().with_top_k(std::num::NonZeroU32::new(20).unwrap());
    /// assert_eq!(config.top_k(), std::num::NonZeroU32::new(20));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_top_k(mut self, value: NonZeroU32) -> Self {
        self.top_k = Some(value);
        self
    }

    /// Sets a non-zero maximum output-token count.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config =
    ///     GenerationConfig::default().with_max_output_tokens(std::num::NonZeroU32::new(256).unwrap());
    /// assert_eq!(config.max_output_tokens(), std::num::NonZeroU32::new(256));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_max_output_tokens(mut self, value: NonZeroU32) -> Self {
        self.max_output_tokens = Some(value);
        self
    }

    /// Replaces validated unique stop sequences.
    ///
    /// # Errors
    ///
    /// Returns an input error for a blank or repeated sequence. An empty list clears existing stop sequences.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config = GenerationConfig::default().with_stop_sequences(vec!["END".into()])?;
    /// assert_eq!(config.stop_sequences(), &["END"]);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_stop_sequences(mut self, values: Vec<String>) -> Result<Self, KitError> {
        let mut seen = BTreeSet::new();
        for value in &values {
            if value.trim().is_empty() {
                return Err(KitError::invalid_input(
                    ValidationViolation::Empty {
                        field: "generation.stop_sequences",
                    },
                    "stop sequences must not be empty",
                ));
            }
            if !seen.insert(value) {
                return Err(KitError::invalid_input(
                    ValidationViolation::Duplicate {
                        field: "generation.stop_sequences",
                    },
                    "stop sequences must be unique",
                ));
            }
        }
        self.stop_sequences = values;
        Ok(self)
    }

    /// Sets a finite presence penalty in `[-2, 2]`.
    ///
    /// # Errors
    ///
    /// Returns an input error unless the penalty is finite and between -2 and 2 inclusive.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config = GenerationConfig::default().with_presence_penalty(0.25)?;
    /// assert_eq!(config.presence_penalty(), Some(0.25));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_presence_penalty(mut self, value: f32) -> Result<Self, KitError> {
        self.presence_penalty = Some(Self::penalty("generation.presence_penalty", value)?);
        Ok(self)
    }

    /// Sets a finite frequency penalty in `[-2, 2]`.
    ///
    /// # Errors
    ///
    /// Returns an input error unless the penalty is finite and between -2 and 2 inclusive.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config = GenerationConfig::default().with_frequency_penalty(0.25)?;
    /// assert_eq!(config.frequency_penalty(), Some(0.25));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_frequency_penalty(mut self, value: f32) -> Result<Self, KitError> {
        self.frequency_penalty = Some(Self::penalty("generation.frequency_penalty", value)?);
        Ok(self)
    }

    fn penalty(name: &'static str, value: f32) -> Result<f32, KitError> {
        if value.is_finite() && (-2.0..=2.0).contains(&value) {
            Ok(value)
        } else {
            Err(KitError::invalid_input(
                ValidationViolation::OutOfRange {
                    field: name,
                    rule: "finite and in [-2, 2]",
                },
                format!("{name} must be finite and in [-2, 2]"),
            ))
        }
    }

    /// Sets a deterministic sampling seed.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config = GenerationConfig::default().with_seed(42);
    /// assert_eq!(config.seed(), Some(42));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_seed(mut self, value: u64) -> Self {
        self.seed = Some(value);
        self
    }

    /// Sets a validated tool-selection policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::GenerationConfig;
    /// let config =
    ///     GenerationConfig::default().with_tool_choice(providers_kit::generation::ToolChoice::Auto);
    /// assert_eq!(config.tool_choice(), Some(&providers_kit::generation::ToolChoice::Auto));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_tool_choice(mut self, value: ToolChoice) -> Self {
        self.tool_choice = Some(value);
        self
    }

    #[must_use]
    /// Returns the requested sampling temperature.
    ///
    /// See [`crate::generation::GenerationConfig::with_temperature`] for a complete example.
    ///
    pub const fn temperature(&self) -> Option<f32> {
        self.temperature
    }

    #[must_use]
    /// Returns the requested nucleus-sampling probability.
    ///
    /// See [`crate::generation::GenerationConfig::with_top_p`] for a complete example.
    ///
    pub const fn top_p(&self) -> Option<f32> {
        self.top_p
    }

    #[must_use]
    /// Returns the requested top-k cutoff.
    ///
    /// See [`crate::generation::GenerationConfig::with_top_k`] for a complete example.
    ///
    pub const fn top_k(&self) -> Option<NonZeroU32> {
        self.top_k
    }

    #[must_use]
    /// Returns the requested maximum output-token count.
    ///
    /// See [`crate::generation::GenerationConfig::with_max_output_tokens`] for a complete example.
    ///
    pub const fn max_output_tokens(&self) -> Option<NonZeroU32> {
        self.max_output_tokens
    }

    #[must_use]
    /// Returns validated stop sequences in caller order.
    ///
    /// See [`crate::generation::GenerationConfig::with_stop_sequences`] for a complete example.
    ///
    pub fn stop_sequences(&self) -> &[String] {
        &self.stop_sequences
    }

    #[must_use]
    /// Returns the requested presence penalty.
    ///
    /// See [`crate::generation::GenerationConfig::with_presence_penalty`] for a complete example.
    ///
    pub const fn presence_penalty(&self) -> Option<f32> {
        self.presence_penalty
    }

    #[must_use]
    /// Returns the requested frequency penalty.
    ///
    /// See [`crate::generation::GenerationConfig::with_frequency_penalty`] for a complete example.
    ///
    pub const fn frequency_penalty(&self) -> Option<f32> {
        self.frequency_penalty
    }

    #[must_use]
    /// Returns the requested sampling seed.
    ///
    /// See [`crate::generation::GenerationConfig::with_seed`] for a complete example.
    ///
    pub const fn seed(&self) -> Option<u64> {
        self.seed
    }

    #[must_use]
    /// Returns the requested tool-selection policy.
    ///
    /// See [`crate::generation::GenerationConfig::with_tool_choice`] for a complete example.
    ///
    pub const fn tool_choice(&self) -> Option<&ToolChoice> {
        self.tool_choice.as_ref()
    }
}

impl<'de> Deserialize<'de> for GenerationConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            temperature: Option<f32>,
            top_p: Option<f32>,
            top_k: Option<NonZeroU32>,
            max_output_tokens: Option<NonZeroU32>,
            #[serde(default)]
            stop_sequences: Vec<String>,
            presence_penalty: Option<f32>,
            frequency_penalty: Option<f32>,
            seed: Option<u64>,
            tool_choice: Option<ToolChoice>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut config = Self::default();
        if let Some(value) = wire.temperature {
            config = config
                .with_temperature(value)
                .map_err(serde::de::Error::custom)?;
        }
        if let Some(value) = wire.top_p {
            config = config.with_top_p(value).map_err(serde::de::Error::custom)?;
        }
        config.top_k = wire.top_k;
        config.max_output_tokens = wire.max_output_tokens;
        config = config
            .with_stop_sequences(wire.stop_sequences)
            .map_err(serde::de::Error::custom)?;
        if let Some(value) = wire.presence_penalty {
            config = config
                .with_presence_penalty(value)
                .map_err(serde::de::Error::custom)?;
        }
        if let Some(value) = wire.frequency_penalty {
            config = config
                .with_frequency_penalty(value)
                .map_err(serde::de::Error::custom)?;
        }
        config.seed = wire.seed;
        config.tool_choice = wire.tool_choice;
        Ok(config)
    }
}

/// Validated model-bound generation request.
///
/// Requests borrow no provider resources and carry no model selector. The selected
/// Model owns exact identity and continuation type; use the same type across all
/// messages in this request.
///
/// # Examples
///
/// ```
/// use providers_kit::generation::{GenerateRequest, Message};
/// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
/// assert_eq!(request.messages().len(), 1);
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(bound(serialize = ""), deny_unknown_fields)]
pub struct GenerateRequest<C = GenerationContinuation> {
    messages: Vec<Message<C>>,
    tools: Vec<ProviderToolSpec>,
    response_format: Option<ResponseFormat>,
    reasoning: Option<ReasoningConfig>,
    generation: GenerationConfig,
}

impl<C> GenerateRequest<C> {
    /// Creates and validates a request containing at least one ordinary message.
    ///
    /// # Errors
    ///
    /// Returns an input error when messages are empty, contain only instructions, place instructions after ordinary messages, or violate a message role/content invariant.
    ///
    /// See [`crate::generation::GenerateRequest`] for a complete example.
    ///
    pub fn new(messages: Vec<Message<C>>) -> Result<Self, KitError> {
        let request = Self {
            messages,
            tools: Vec::new(),
            response_format: None,
            reasoning: None,
            generation: GenerationConfig::default(),
        };
        request.validate()?;
        Ok(request)
    }

    /// Replaces model-visible tool declarations.
    ///
    /// # Errors
    ///
    /// Returns an input error for duplicate function declarations or a tool selection inconsistent with the inventory. Hosted-tool and model compatibility are checked by the selected model.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{ProviderFunctionTool, JsonSchema};
    /// let tool = ProviderFunctionTool::new(
    ///     "lookup",
    ///     Some("Find a record".into()),
    ///     JsonSchema::new(serde_json::json!({"type":"object"}))?,
    /// )?;
    /// use providers_kit::generation::{GenerateRequest, Message, ProviderToolSpec};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Look up a record")?])?
    ///     .with_tools(vec![ProviderToolSpec::Function(tool)])?;
    /// assert_eq!(request.tools().len(), 1);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_tools(mut self, tools: Vec<ProviderToolSpec>) -> Result<Self, KitError> {
        self.tools = tools;
        self.validate_tools()?;
        Ok(self)
    }

    /// Replaces structured-output configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
    /// use providers_kit::generation::ResponseFormat;
    /// let request = request.with_response_format(ResponseFormat::JsonObject);
    /// assert!(matches!(request.response_format(), Some(ResponseFormat::JsonObject)));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_response_format(mut self, value: ResponseFormat) -> Self {
        self.response_format = Some(value);
        self
    }

    /// Replaces reasoning configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
    /// use providers_kit::generation::{ReasoningConfig, ReasoningEffort};
    /// let request = request.with_reasoning(ReasoningConfig {
    ///     effort: Some(ReasoningEffort::Low),
    ///     ..Default::default()
    /// });
    /// assert_eq!(request.reasoning().unwrap().effort, Some(ReasoningEffort::Low));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_reasoning(mut self, value: ReasoningConfig) -> Self {
        self.reasoning = Some(value);
        self
    }

    /// Replaces generation controls after validating tool references.
    ///
    /// # Errors
    ///
    /// Returns an input error when the generation/tool-choice settings do not match the declared tools, including an undeclared selected function or required choice without tools.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
    /// use providers_kit::generation::GenerationConfig;
    /// let request = request.with_generation(GenerationConfig::default().with_seed(42))?;
    /// assert_eq!(request.generation().seed(), Some(42));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_generation(mut self, value: GenerationConfig) -> Result<Self, KitError> {
        self.generation = value;
        self.validate_tools()?;
        Ok(self)
    }

    #[must_use]
    /// Returns validated messages in request order.
    ///
    /// See [`crate::generation::GenerateRequest`] for a complete example.
    ///
    pub fn messages(&self) -> &[Message<C>] {
        &self.messages
    }

    #[must_use]
    /// Returns declared client and hosted tools.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
    /// assert!(request.tools().is_empty());
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn tools(&self) -> &[ProviderToolSpec] {
        &self.tools
    }

    #[must_use]
    /// Returns the requested structured-output format.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
    /// assert!(request.response_format().is_none());
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub const fn response_format(&self) -> Option<&ResponseFormat> {
        self.response_format.as_ref()
    }

    #[must_use]
    /// Returns the requested reasoning controls.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
    /// assert!(request.reasoning().is_none());
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub const fn reasoning(&self) -> Option<&ReasoningConfig> {
        self.reasoning.as_ref()
    }

    #[must_use]
    /// Returns sampling and tool-choice controls.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let request = GenerateRequest::<()>::new(vec![Message::user_text("Hello")?])?;
    /// assert_eq!(request.generation().seed(), None);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub const fn generation(&self) -> &GenerationConfig {
        &self.generation
    }

    /// Maps only opaque continuation values while preserving validated request data.
    ///
    /// This is value conversion for caller-owned concrete composition. It does not
    /// inspect a supplied continuation or authorize dropping its replay information.
    ///
    /// # Errors
    ///
    /// Returns the first callback error unchanged. Messages without continuation do not invoke the callback.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::generation::{GenerateRequest, Message};
    /// let assistant = Message::assistant_text("continue")?.with_continuation(7_u32)?;
    /// let request = GenerateRequest::new(vec![assistant])?;
    /// let mapped = request.try_map_continuation(|value| Ok(value.to_string()))?;
    /// assert_eq!(mapped.messages()[0].continuation().map(String::as_str), Some("7"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn try_map_continuation<D>(
        self,
        mut map: impl FnMut(C) -> Result<D, KitError>,
    ) -> Result<GenerateRequest<D>, KitError> {
        Ok(GenerateRequest {
            messages: self
                .messages
                .into_iter()
                .map(|message| message.try_map_continuation(&mut map))
                .collect::<Result<_, _>>()?,
            tools: self.tools,
            response_format: self.response_format,
            reasoning: self.reasoning,
            generation: self.generation,
        })
    }

    fn validate(&self) -> Result<(), KitError> {
        if self.messages.is_empty() {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "generation.messages",
                },
                "generation request messages must not be empty",
            ));
        }
        let mut saw_ordinary = false;
        for message in &self.messages {
            match message.role() {
                crate::generation::Role::System | crate::generation::Role::Developer
                    if saw_ordinary =>
                {
                    return Err(KitError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "generation.messages",
                            rule: "instructions must form a leading prefix",
                        },
                        "system and developer instructions must form one leading prefix",
                    ));
                }
                crate::generation::Role::System | crate::generation::Role::Developer => {}
                _ => saw_ordinary = true,
            }
            message.validate_for_request()?;
        }
        if !saw_ordinary {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidValue {
                    field: "generation.messages",
                    rule: "requires a non-instruction message",
                },
                "generation request requires at least one non-instruction message",
            ));
        }
        self.validate_tools()
    }

    fn validate_tools(&self) -> Result<(), KitError> {
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            if let ProviderToolSpec::Function(function) = tool
                && !names.insert(function.name())
            {
                return Err(KitError::invalid_input(
                    ValidationViolation::Duplicate {
                        field: "tools.function.name",
                    },
                    format!("duplicate function tool name `{}`", function.name()),
                ));
            }
        }
        selected_tools_for_choice(&self.tools, self.generation.tool_choice())?;
        Ok(())
    }

    #[must_use]
    pub(crate) fn as_view(&self) -> GenerateRequestView<'_, C> {
        GenerateRequestView {
            messages: &self.messages,
            tools: &self.tools,
            response_format: self.response_format.as_ref(),
            reasoning: self.reasoning.as_ref(),
            generation: &self.generation,
        }
    }
}

impl<'de, C> Deserialize<'de> for GenerateRequest<C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(bound(deserialize = ""), deny_unknown_fields)]
        struct Wire<C> {
            messages: Vec<Message<C>>,
            #[serde(default)]
            tools: Vec<ProviderToolSpec>,
            response_format: Option<ResponseFormat>,
            reasoning: Option<ReasoningConfig>,
            #[serde(default)]
            generation: GenerationConfig,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut request = Self::new(wire.messages).map_err(serde::de::Error::custom)?;
        request.tools = wire.tools;
        request.response_format = wire.response_format;
        request.reasoning = wire.reasoning;
        request.generation = wire.generation;
        request.validate().map_err(serde::de::Error::custom)?;
        Ok(request)
    }
}

/// Portable completion reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// The model reached a natural stop.
    Stop,
    /// The configured or provider output limit was reached.
    Length,
    /// The model stopped to request a function call.
    ToolCall,
    /// Provider safety filtering stopped output.
    ContentFilter,
    /// A provider-authored terminal error stopped output.
    Error,
    /// Generation paused for provider-managed continuation.
    Paused,
}

/// Citation attached to generated output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Citation {
    /// Citation of an external URL.
    Url {
        /// Cited URL.
        url: String,
        /// Provider-reported title.
        title: Option<String>,
        /// Provider-reported excerpt.
        snippet: Option<String>,
        /// Inclusive output start index, when reported.
        start_index: Option<usize>,
        /// Exclusive output end index, when reported.
        end_index: Option<usize>,
    },
    /// Citation of a provider-managed or remote file.
    File {
        /// Exact cited file reference.
        reference: FileReference,
        /// Provider-reported title.
        title: Option<String>,
        /// Provider-reported excerpt.
        snippet: Option<String>,
        /// Inclusive output start index, when reported.
        start_index: Option<usize>,
        /// Exclusive output end index, when reported.
        end_index: Option<usize>,
    },
}

/// Final normalized generation result.
///
/// A returned assistant message is the replay owner. Preserve it unchanged through
/// function continuation; projections of text, hosted results, or artifacts cannot
/// reconstruct native state. Serialization fails while continuation is present.
///
/// # Examples
///
/// ```
/// use providers_kit::{ModelId, generation::{GenerateResponse, Message, FinishReason, Usage}};
/// let response = GenerateResponse {
///     model: ModelId::new("exact-model")?,
///     message: Message::assistant_text("done")?.with_continuation(7_u32)?,
///     finish_reason: Some(FinishReason::Stop),
///     citations: Vec::new(),
///     artifacts: Vec::new(),
///     hosted_tool_outputs: Vec::new(),
///     usage: Usage::default(),
/// };
/// let mapped = response.try_map_continuation(|value| Ok(value.to_string()))?;
/// assert_eq!(mapped.message.continuation().map(String::as_str), Some("7"));
/// assert_eq!(mapped.model.as_str(), "exact-model");
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = ""))]
pub struct GenerateResponse<C = GenerationContinuation> {
    /// Exact model that produced the response.
    pub model: ModelId,
    /// Returned assistant message and opaque continuation state.
    pub message: Message<C>,
    /// Provider-reported completion reason.
    pub finish_reason: Option<FinishReason>,
    /// Citations attached to generated output.
    pub citations: Vec<Citation>,
    /// Generated artifacts.
    pub artifacts: Vec<Artifact>,
    /// Normalized provider-hosted tool outputs.
    pub hosted_tool_outputs: Vec<HostedToolOutput>,
    /// Provider-reported token accounting.
    pub usage: Usage,
}

impl<C> GenerateResponse<C> {
    /// Maps only opaque continuation values.
    ///
    /// # Errors
    ///
    /// Returns the callback error unchanged if converting the response continuation fails; other response fields retain their exact values.
    ///
    /// See [`crate::generation::GenerateResponse`] for a complete example.
    ///
    pub fn try_map_continuation<D>(
        self,
        mut map: impl FnMut(C) -> Result<D, KitError>,
    ) -> Result<GenerateResponse<D>, KitError> {
        Ok(GenerateResponse {
            model: self.model,
            message: self.message.try_map_continuation(&mut map)?,
            finish_reason: self.finish_reason,
            citations: self.citations,
            artifacts: self.artifacts,
            hosted_tool_outputs: self.hosted_tool_outputs,
            usage: self.usage,
        })
    }
}

/// Incremental generation event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    bound(serialize = "", deserialize = ""),
    tag = "event",
    rename_all = "snake_case"
)]
pub enum GenerateEvent<C = GenerationContinuation> {
    /// Stream metadata is ready.
    Started {
        /// Exact model producing the stream.
        model: ModelId,
    },
    /// Incremental visible text.
    ContentDelta {
        /// Text appended by this event.
        delta: String,
    },
    /// Incremental provider-authored thinking summary.
    ThinkingDelta {
        /// Thinking text appended by this event.
        delta: String,
    },
    /// Incremental provider-hosted tool activity.
    HostedTool(Box<HostedToolOutput>),
    /// Updated provider usage accounting.
    Usage(Usage),
    /// Terminal normalized response.
    Completed(Box<GenerateResponse<C>>),
}

impl<C> GenerateEvent<C> {
    /// Maps continuation state carried by a completion event.
    ///
    /// # Errors
    ///
    /// Returns the callback error for a completion continuation. Other events retain their data without invoking the callback.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::{ModelId, generation::{GenerateResponse, Message, FinishReason, Usage}};
    /// let response = GenerateResponse {
    ///     model: ModelId::new("exact-model")?,
    ///     message: Message::assistant_text("done")?.with_continuation(7_u32)?,
    ///     finish_reason: Some(FinishReason::Stop),
    ///     citations: Vec::new(),
    ///     artifacts: Vec::new(),
    ///     hosted_tool_outputs: Vec::new(),
    ///     usage: Usage::default(),
    /// };
    /// use providers_kit::generation::GenerateEvent;
    /// let event = GenerateEvent::Completed(Box::new(response));
    /// let mapped = event.try_map_continuation(|value| Ok(value.to_string()))?;
    /// assert!(
    ///     matches!(mapped, GenerateEvent::Completed(response) if response.message.continuation().map(String::as_str) == Some("7"))
    /// );
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn try_map_continuation<D>(
        self,
        map: impl FnMut(C) -> Result<D, KitError>,
    ) -> Result<GenerateEvent<D>, KitError> {
        Ok(match self {
            Self::Started { model } => GenerateEvent::Started { model },
            Self::ContentDelta { delta } => GenerateEvent::ContentDelta { delta },
            Self::ThinkingDelta { delta } => GenerateEvent::ThinkingDelta { delta },
            Self::HostedTool(output) => GenerateEvent::HostedTool(output),
            Self::Usage(usage) => GenerateEvent::Usage(usage),
            Self::Completed(response) => {
                GenerateEvent::Completed(Box::new(response.try_map_continuation(map)?))
            }
        })
    }
}

/// Value-erased stream of normalized generation events.
///
/// This value adapter does not enforce a backend protocol. Caller Models own
/// bounded production, terminal ordering, admission release and cancellation.
/// Supplied model streams produce no events after Completed and own no forwarding task.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), providers_kit::KitError> {
/// use providers_kit::generation::{GenerateEvent, GenerateEventStream};
/// use futures_util::{stream, StreamExt};
/// let source = stream::iter([Ok(GenerateEvent::<()>::ContentDelta { delta: "hello".into() })]);
/// let mut stream = GenerateEventStream::new(source);
/// assert!(
///     matches!(stream.next().await.transpose()?, Some(GenerateEvent::ContentDelta { delta }) if delta == "hello")
/// );
/// assert!(stream.next().await.is_none());
///
/// # Ok(())
/// # }
/// ```
///
pub struct GenerateEventStream<C = GenerationContinuation> {
    inner: Pin<Box<dyn Stream<Item = Result<GenerateEvent<C>, KitError>> + Send + 'static>>,
}

impl<C: 'static> GenerateEventStream<C> {
    /// Wraps a concrete event stream without erasing model behavior.
    ///
    /// This value adapter does not enforce a backend protocol. Caller Models own
    /// bounded production, terminal ordering, admission release and cancellation.
    /// Supplied model streams produce no events after Completed and own no forwarding task.
    ///
    /// See [`crate::generation::GenerateEventStream`] for a complete example.
    ///
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<GenerateEvent<C>, KitError>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<C> Stream for GenerateEventStream<C> {
    type Item = Result<GenerateEvent<C>, KitError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Token accounting reported by a provider response.
///
/// Fields stay `None` when the provider did not report that exact value.
/// The Kit does not derive aggregate totals or request counts from other
/// fields.
///
/// # Examples
/// ```rust
/// let usage = providers_kit::generation::Usage {
///     input_tokens: Some(12),
///     output_tokens: Some(34),
///     reasoning_tokens: None,
///     cached_input_tokens: Some(5),
///     total_tokens: Some(46),
/// };
/// assert_eq!(usage.cached_input_tokens, Some(5));
/// assert_eq!(usage.total_tokens, Some(46));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// Input tokens billed or counted for the request.
    pub input_tokens: Option<u64>,
    /// Output tokens generated by the provider.
    pub output_tokens: Option<u64>,
    /// Reasoning or thinking tokens reported separately by the provider.
    pub reasoning_tokens: Option<u64>,
    /// Input tokens served from prompt cache rather than recomputed.
    pub cached_input_tokens: Option<u64>,
    /// Total token count when the provider reports an aggregate value.
    pub total_tokens: Option<u64>,
}

impl Usage {
    pub(crate) fn reported_tokens(input_tokens: Option<u64>, output_tokens: Option<u64>) -> Self {
        Self {
            input_tokens,
            output_tokens,
            ..Self::default()
        }
    }

    pub(crate) fn with_reported_total(mut self, total_tokens: Option<u64>) -> Self {
        self.total_tokens = total_tokens;
        self
    }

    pub(crate) fn with_cached_input(mut self, cached_input_tokens: Option<u64>) -> Self {
        self.cached_input_tokens = cached_input_tokens;
        self
    }

    pub(crate) fn with_reasoning(mut self, reasoning_tokens: Option<u64>) -> Self {
        self.reasoning_tokens = reasoning_tokens;
        self
    }
}
