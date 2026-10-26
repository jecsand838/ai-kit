//! Provider-native interactive values and the unique session handle.
//!
//! [`SessionConfig`] carries no model selector: open it through one exact
//! [`crate::ModelBackend`]. Drive [`SessionBackend::send`] and [`SessionBackend::recv`]
//! through a unique mutable Session, then consume it with [`SessionBackend::close`].
//! Provider shutdown remains shared. Ordinary multi-turn generation uses messages
//! and opaque continuation; it does not require a native Session.
//!
//! # Supplied support and a complete text turn
//!
//! The supplied Session implementation is **OpenAI-only**. Supplied Gemini,
//! Anthropic and Ollama Models return Unsupported; their stateless operations remain
//! separate. The portable trait also supports independently implemented caller
//! Sessions, as illustrated on [`crate::ProviderBackend`]. Stateless OpenAI audio
//! input is unsupported; OpenAI Realtime audio is a Session operation.
//!
//! This example requires OpenAI credentials and access to `gpt-realtime-2.1`.
//! A text turn may produce several audio and transcription events before completion.
//! Advisory usage/output is not a terminal signal; cancellation is not completion.
//! The caller applies a single turn deadline and consumes close even after a turn
//! error. Function requests would need a configured application handler and exact
//! `FunctionResult` correlation; this text-only example configures no functions.
//!
//! ```no_run
//! use std::time::Duration;
//! use provider_sdk::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId, SessionBackend};
//! use provider_sdk::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
//! use provider_sdk::session::{SessionConfig, SessionEvent, SessionInput};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
//!     TransportConfig::default(),
//! ))?;
//! let work = async {
//!     let model = provider.model(ModelId::new("gpt-realtime-2.1")?)?;
//!     let config = SessionConfig::default().with_output_audio("audio/pcm;rate=24000", true)?;
//!     let mut session = model.open_session(config).await?;
//!     let turn = tokio::time::timeout(Duration::from_secs(90), async {
//!         session.send(SessionInput::Text { text: "Describe one use of a bicycle.".into() }).await?;
//!         let mut audio_bytes = 0usize;
//!         let mut transcript_bytes = 0usize;
//!         let mut substantive_transcript = false;
//!         loop {
//!             match session.recv().await? {
//!                 SessionEvent::Audio { data, .. } => audio_bytes += data.len(),
//!                 SessionEvent::OutputTranscriptionDelta { delta } => {
//!                     transcript_bytes += delta.len();
//!                     substantive_transcript |= !delta.trim().is_empty();
//!                 }
//!                 SessionEvent::Completed => break,
//!                 SessionEvent::TurnCancelled | SessionEvent::FunctionCall(_)
//!                 | SessionEvent::FunctionCallsCancelled { .. } => return Err("unexpected control event".into()),
//!                 SessionEvent::TextDelta { .. } | SessionEvent::InputTranscriptionDelta { .. }
//!                 | SessionEvent::Usage(_) => {}
//!             }
//!             if audio_bytes > 8 * 1024 * 1024 || transcript_bytes > 1024 * 1024 {
//!                 return Err("example output bound exceeded".into());
//!             }
//!         }
//!         if audio_bytes == 0 || !substantive_transcript { return Err("missing output".into()); }
//!         Ok::<(), Box<dyn std::error::Error>>(())
//!     }).await;
//!     let closed = session.close().await;
//!     turn??;
//!     closed?;
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! }.await;
//! let shutdown = provider.shutdown().await;
//! work?;
//! shutdown?;
//! # Ok(())
//! # }
//! ```
//!
//! For manual audio, send `TurnStart`, one or more raw `Audio` chunks in the exact
//! configured format, then `TurnEnd`; receive until a native terminal event. A raw
//! chunk is not a whole-turn or automatic flush operation. Output wording is
//! model-authored, not an SDK exact-copy guarantee.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use reqwest::multipart::Part;
use serde::{Deserialize, Serialize};

use crate::SdkError;
use crate::core::filter_tools_for_choice;
use crate::error::Operation;
use crate::error::ValidationViolation;
use crate::generation::{
    FunctionCall, FunctionResult, ProviderFunctionTool, ProviderToolSpec, ToolChoice, Usage,
};
use crate::models::ModelId;
use crate::provider::ProviderRuntime;
use crate::providers::openai::OpenAiSession;

/// Defaults applied to one provider-native interactive session.
///
/// Configuration is passive. Select the exact model first and call
/// [`crate::ModelBackend::open_session`] only when native sessions are supported.
///
/// # Examples
///
/// ```
/// use provider_sdk::session::SessionConfig;
/// let config = SessionConfig::default().with_instructions("Answer briefly")?;
/// assert_eq!(config.instructions(), Some("Answer briefly"));
///
/// # Ok::<(), provider_sdk::SdkError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq)]
pub struct SessionConfig {
    instructions: Option<String>,
    input_audio: Option<(String, bool)>,
    output_audio: Option<(String, bool)>,
    turn_detection: SessionTurnDetection,
    functions: Vec<ProviderFunctionTool>,
    tool_choice: Option<ToolChoice>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            instructions: None,
            input_audio: None,
            output_audio: None,
            turn_detection: SessionTurnDetection::Automatic,
            functions: Vec::new(),
            tool_choice: None,
        }
    }
}

impl SessionConfig {
    /// Sets nonblank text instructions for the session.
    ///
    /// # Errors
    ///
    /// Returns an input error for empty or whitespace-only instructions.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_instructions("Answer briefly")?;
    /// assert_eq!(config.instructions(), Some("Answer briefly"));
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Result<Self, SdkError> {
        let instructions = instructions.into();
        if instructions.trim().is_empty() {
            return Err(SdkError::invalid_input(
                ValidationViolation::Empty {
                    field: "session.instructions",
                },
                "interactive session instructions must not be blank",
            ));
        }
        self.instructions = Some(instructions);
        Ok(self)
    }

    /// Enables audio input with an exact IANA media type.
    ///
    /// # Errors
    ///
    /// Returns an input error unless the value is an unpadded valid IANA audio media type without control characters. Exact format/model support is checked on session opening.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_input_audio("audio/pcm", true)?;
    /// assert_eq!(config.input_audio_format(), Some("audio/pcm"));
    /// assert!(config.input_transcription());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn with_input_audio(
        mut self,
        media_type: impl Into<String>,
        transcription: bool,
    ) -> Result<Self, SdkError> {
        self.input_audio = Some((
            Self::validate_audio_format(media_type.into())?,
            transcription,
        ));
        Ok(self)
    }

    /// Enables audio output with an exact IANA media type.
    ///
    /// # Errors
    ///
    /// Returns an input error unless the value is an unpadded valid IANA audio media type without control characters. Exact format/model support is checked on session opening.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_output_audio("audio/pcm", true)?;
    /// assert_eq!(config.output_audio_format(), Some("audio/pcm"));
    /// assert!(config.output_transcription());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn with_output_audio(
        mut self,
        media_type: impl Into<String>,
        transcription: bool,
    ) -> Result<Self, SdkError> {
        self.output_audio = Some((
            Self::validate_audio_format(media_type.into())?,
            transcription,
        ));
        Ok(self)
    }

    /// Selects provider-managed or caller-managed turn boundaries.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::{SessionConfig, SessionTurnDetection};
    /// let config = SessionConfig::default().with_turn_detection(SessionTurnDetection::Manual);
    /// assert_eq!(config.turn_detection(), SessionTurnDetection::Manual);
    /// ```
    ///
    #[must_use]
    pub const fn with_turn_detection(mut self, turn_detection: SessionTurnDetection) -> Self {
        self.turn_detection = turn_detection;
        self
    }

    /// Sets the client functions available throughout the session.
    ///
    /// # Errors
    ///
    /// Returns an input error for duplicate functions, required tool choice with an empty inventory, or a selection that cannot be resolved against the supplied functions.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::generation::{ProviderFunctionTool, JsonSchema};
    /// let tool = ProviderFunctionTool::new("lookup", Some("Find a record".into()), JsonSchema::new(serde_json::json!({"type":"object"}))?)?;
    /// use provider_sdk::{session::SessionConfig, generation::ToolChoice};
    /// let config = SessionConfig::default().with_functions(vec![tool], Some(ToolChoice::Auto))?;
    /// assert_eq!(config.functions()[0].name(), "lookup");
    /// assert_eq!(config.tool_choice(), Some(&ToolChoice::Auto));
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn with_functions(
        mut self,
        functions: Vec<ProviderFunctionTool>,
        tool_choice: Option<ToolChoice>,
    ) -> Result<Self, SdkError> {
        let mut names = BTreeSet::new();
        for function in &functions {
            if !names.insert(function.name()) {
                return Err(SdkError::invalid_input(
                    ValidationViolation::Duplicate {
                        field: "session.functions.name",
                    },
                    format!(
                        "interactive function name `{}` is duplicated",
                        function.name()
                    ),
                ));
            }
        }
        if functions.is_empty()
            && matches!(
                tool_choice.as_ref(),
                Some(ToolChoice::Required | ToolChoice::Any)
            )
        {
            return Err(SdkError::invalid_input(
                ValidationViolation::MissingField {
                    field: "session.functions",
                },
                "interactive required tool choice needs at least one function",
            ));
        }
        let specs = functions
            .iter()
            .cloned()
            .map(ProviderToolSpec::Function)
            .collect::<Vec<_>>();
        filter_tools_for_choice(&specs, tool_choice.as_ref())?;
        self.functions = functions;
        self.tool_choice = tool_choice;
        Ok(self)
    }

    /// Returns the session instructions, when configured.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_instructions("Answer briefly")?;
    /// assert_eq!(config.instructions(), Some("Answer briefly"));
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }

    /// Returns the required input-audio media type, when enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_input_audio("audio/pcm", true)?;
    /// assert_eq!(config.input_audio_format(), Some("audio/pcm"));
    /// assert!(config.input_transcription());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn input_audio_format(&self) -> Option<&str> {
        self.input_audio
            .as_ref()
            .map(|(media_type, _)| media_type.as_str())
    }

    /// Returns the requested output-audio media type, when enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_output_audio("audio/pcm", true)?;
    /// assert_eq!(config.output_audio_format(), Some("audio/pcm"));
    /// assert!(config.output_transcription());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn output_audio_format(&self) -> Option<&str> {
        self.output_audio
            .as_ref()
            .map(|(media_type, _)| media_type.as_str())
    }

    /// Reports whether input transcription was requested.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_input_audio("audio/pcm", true)?;
    /// assert_eq!(config.input_audio_format(), Some("audio/pcm"));
    /// assert!(config.input_transcription());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn input_transcription(&self) -> bool {
        self.input_audio
            .as_ref()
            .is_some_and(|(_, transcription)| *transcription)
    }

    /// Reports whether output transcription was requested.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::SessionConfig;
    /// let config = SessionConfig::default().with_output_audio("audio/pcm", true)?;
    /// assert_eq!(config.output_audio_format(), Some("audio/pcm"));
    /// assert!(config.output_transcription());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn output_transcription(&self) -> bool {
        self.output_audio
            .as_ref()
            .is_some_and(|(_, transcription)| *transcription)
    }

    /// Returns the configured turn-boundary policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::session::{SessionConfig, SessionTurnDetection};
    /// let config = SessionConfig::default().with_turn_detection(SessionTurnDetection::Manual);
    /// assert_eq!(config.turn_detection(), SessionTurnDetection::Manual);
    /// ```
    ///
    #[must_use]
    pub const fn turn_detection(&self) -> SessionTurnDetection {
        self.turn_detection
    }

    /// Returns the client functions available to the session.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::generation::{ProviderFunctionTool, JsonSchema};
    /// let tool = ProviderFunctionTool::new("lookup", Some("Find a record".into()), JsonSchema::new(serde_json::json!({"type":"object"}))?)?;
    /// use provider_sdk::{session::SessionConfig, generation::ToolChoice};
    /// let config = SessionConfig::default().with_functions(vec![tool], Some(ToolChoice::Auto))?;
    /// assert_eq!(config.functions()[0].name(), "lookup");
    /// assert_eq!(config.tool_choice(), Some(&ToolChoice::Auto));
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn functions(&self) -> &[ProviderFunctionTool] {
        &self.functions
    }

    /// Returns the configured function-selection policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::generation::{ProviderFunctionTool, JsonSchema};
    /// let tool = ProviderFunctionTool::new("lookup", Some("Find a record".into()), JsonSchema::new(serde_json::json!({"type":"object"}))?)?;
    /// use provider_sdk::{session::SessionConfig, generation::ToolChoice};
    /// let config = SessionConfig::default().with_functions(vec![tool], Some(ToolChoice::Auto))?;
    /// assert_eq!(config.functions()[0].name(), "lookup");
    /// assert_eq!(config.tool_choice(), Some(&ToolChoice::Auto));
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub const fn tool_choice(&self) -> Option<&ToolChoice> {
        self.tool_choice.as_ref()
    }

    fn validate_audio_format(media_type: String) -> Result<String, SdkError> {
        if !media_type.starts_with("audio/")
            || media_type.trim() != media_type
            || media_type.chars().any(char::is_control)
            || Part::bytes(Vec::new()).mime_str(&media_type).is_err()
        {
            return Err(SdkError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "session.audio_format",
                    rule: "unpadded IANA audio media type",
                },
                "interactive audio format must be an unpadded IANA audio media type",
            ));
        }
        Ok(media_type)
    }
}

/// Portable interactive audio turn detection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnDetection {
    /// Provider-managed boundaries.
    #[default]
    Automatic,
    /// Caller-managed boundaries.
    Manual,
}

/// Input sent to an interactive session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionInput {
    /// One complete text turn.
    Text {
        /// Nonblank caller text.
        text: String,
    },
    /// One raw audio chunk in the configured input format.
    Audio {
        /// Exact media type configured for session input.
        media_type: String,
        /// Raw, unencoded audio bytes.
        #[serde(with = "crate::core::content::base64_bytes")]
        data: Vec<u8>,
    },
    /// A completed client-function result.
    FunctionResult(FunctionResult),
    /// Starts a caller-managed audio turn.
    TurnStart,
    /// Ends caller-managed audio input or, where supported, explicitly ends
    /// the currently submitted automatic-audio segment.
    ///
    /// The supplied OpenAI backend requires manual turn detection.
    /// This input does not complete or cancel the provider response.
    TurnEnd,
    /// Cancels the active provider response when supported.
    CancelTurn,
}

/// Event received from an interactive session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionEvent {
    /// Incremental model text.
    TextDelta {
        /// Newly received text.
        delta: String,
    },
    /// Raw model audio in its declared media type.
    Audio {
        /// Provider-declared audio media type.
        media_type: String,
        /// Raw, decoded audio bytes.
        #[serde(with = "crate::core::content::base64_bytes")]
        data: Vec<u8>,
    },
    /// Incremental transcription of caller audio.
    InputTranscriptionDelta {
        /// Newly transcribed caller text.
        delta: String,
    },
    /// Incremental transcription of model audio.
    OutputTranscriptionDelta {
        /// Newly transcribed model text.
        delta: String,
    },
    /// Client function requested by the model.
    FunctionCall(FunctionCall),
    /// Provider-cancelled client-function calls.
    FunctionCallsCancelled {
        /// Provider correlation identifiers for the cancelled calls.
        call_ids: Vec<String>,
    },
    /// Usage update.
    Usage(Usage),
    /// The active model turn was interrupted or cancelled.
    TurnCancelled,
    /// Current interactive turn completed.
    Completed,
}

/// Public implementation contract for one unique provider-native session.
///
/// Caller models return their concrete session implementation directly through
/// [`crate::ModelBackend::Session`].
///
/// This example requires valid credentials and a live model with native session
/// support. Sessions are unique, mutable, and non-Clone. Consuming close settles
/// the session transport; the Provider retains separate shared shutdown authority.
/// Ordinary multi-turn generation instead uses messages and continuation.
///
/// # Examples
///
/// ```no_run
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), provider_sdk::SdkError> {
/// use provider_sdk::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
/// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
/// use provider_sdk::{ModelBackend, ModelId, SessionBackend, session::{SessionConfig, SessionInput}};
/// let model = provider.model(ModelId::new("your-exact-session-model-id")?)?;
/// let observed = async {
///     let mut session = model.open_session(SessionConfig::default()).await?;
///     assert!(!session.id().is_empty());
///     assert_eq!(session.model_id(), model.model_id());
///     let closed = session.close().await;
///     closed
/// }.await;
/// let cleanup = provider.shutdown().await;
/// observed?;
/// cleanup?;
///
/// # Ok(())
/// # }
/// ```
///
pub trait SessionBackend: Send + 'static {
    /// Stable session identifier.
    ///
    /// This example requires a live native session on a supporting exact model.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), provider_sdk::SdkError> {
    /// use provider_sdk::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use provider_sdk::{ModelBackend, ModelId, SessionBackend, session::{SessionConfig, SessionInput}};
    /// let model = provider.model(ModelId::new("your-exact-session-model-id")?)?;
    /// let observed = async {
    ///     let mut session = model.open_session(SessionConfig::default()).await?;
    ///     assert!(!session.id().is_empty());
    ///     assert_eq!(session.model_id(), model.model_id());
    ///     let closed = session.close().await;
    ///     closed
    /// }.await;
    /// let cleanup = provider.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn id(&self) -> &str;
    /// Exact selected model.
    ///
    /// This example requires a live native session on a supporting exact model.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), provider_sdk::SdkError> {
    /// use provider_sdk::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use provider_sdk::{ModelBackend, ModelId, SessionBackend, session::{SessionConfig, SessionInput}};
    /// let model = provider.model(ModelId::new("your-exact-session-model-id")?)?;
    /// let observed = async {
    ///     let mut session = model.open_session(SessionConfig::default()).await?;
    ///     assert!(!session.id().is_empty());
    ///     assert_eq!(session.model_id(), model.model_id());
    ///     let closed = session.close().await;
    ///     closed
    /// }.await;
    /// let cleanup = provider.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn model_id(&self) -> &ModelId;
    /// Sends interactive input.
    ///
    /// This example requires a live native session on a supporting exact model.
    ///
    /// # Errors
    ///
    /// Returns the owning input, unsupported, closed, transport, deadline, service-rejection, or protocol error. Consuming close does not make the session reusable.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), provider_sdk::SdkError> {
    /// use provider_sdk::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use provider_sdk::{ModelBackend, ModelId, SessionBackend, session::{SessionConfig, SessionInput}};
    /// let model = provider.model(ModelId::new("your-exact-session-model-id")?)?;
    /// let observed = async {
    ///     let mut session = model.open_session(SessionConfig::default()).await?;
    ///     let sent = session.send(SessionInput::Text { text: "Hello".into() }).await;
    ///     let closed = session.close().await;
    ///     sent?;
    ///     closed
    /// }.await;
    /// let cleanup = provider.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn send(
        &mut self,
        input: SessionInput,
    ) -> impl Future<Output = Result<(), SdkError>> + Send + '_;
    /// Receives the next interactive event.
    ///
    /// This example requires a live native session on a supporting exact model.
    ///
    /// # Errors
    ///
    /// Returns the owning input, unsupported, closed, transport, deadline, service-rejection, or protocol error. Consuming close does not make the session reusable.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), provider_sdk::SdkError> {
    /// use provider_sdk::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use provider_sdk::{ModelBackend, ModelId, SessionBackend, session::{SessionConfig, SessionInput}};
    /// let model = provider.model(ModelId::new("your-exact-session-model-id")?)?;
    /// let observed = async {
    ///     let mut session = model.open_session(SessionConfig::default()).await?;
    ///     let sent = session.send(SessionInput::Text { text: "Hello".into() }).await;
    ///     let event = if sent.is_ok() { Some(session.recv().await) } else { None };
    ///     let closed = session.close().await;
    ///     sent?;
    ///     if let Some(event) = event { let _event = event?; }
    ///     closed
    /// }.await;
    /// let cleanup = provider.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn recv(&mut self) -> impl Future<Output = Result<SessionEvent, SdkError>> + Send + '_;
    /// Closes the unique session and its transport.
    ///
    /// This example requires a live native session on a supporting exact model.
    ///
    /// # Errors
    ///
    /// Returns the owning input, unsupported, closed, transport, deadline, service-rejection, or protocol error. Consuming close does not make the session reusable.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), provider_sdk::SdkError> {
    /// use provider_sdk::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use provider_sdk::{ModelBackend, ModelId, SessionBackend, session::{SessionConfig, SessionInput}};
    /// let model = provider.model(ModelId::new("your-exact-session-model-id")?)?;
    /// let observed = async {
    ///     let mut session = model.open_session(SessionConfig::default()).await?;
    ///     let closed = session.close().await;
    ///     closed
    /// }.await;
    /// let cleanup = provider.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn close(self) -> impl Future<Output = Result<(), SdkError>> + Send;
}

impl SessionBackend for Infallible {
    fn id(&self) -> &str {
        match *self {}
    }

    fn model_id(&self) -> &ModelId {
        match *self {}
    }

    async fn send(&mut self, _input: SessionInput) -> Result<(), SdkError> {
        match *self {}
    }

    async fn recv(&mut self) -> Result<SessionEvent, SdkError> {
        match *self {}
    }

    async fn close(self) -> Result<(), SdkError> {
        match self {}
    }
}

pub(crate) enum SessionInner {
    OpenAi(OpenAiSession),
}

/// Unique opaque provider-native interactive session supplied for OpenAI models.
///
/// This example requires valid credentials and a live model with native session
/// support. Sessions are unique, mutable, and non-Clone. Consuming close settles
/// the session transport; the Provider retains separate shared shutdown authority.
/// Ordinary multi-turn generation instead uses messages and continuation.
///
/// # Examples
///
/// ```no_run
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), provider_sdk::SdkError> {
/// use provider_sdk::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
/// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
/// use provider_sdk::{ModelBackend, ModelId, SessionBackend, session::{SessionConfig, SessionInput}};
/// let model = provider.model(ModelId::new("your-exact-session-model-id")?)?;
/// let observed = async {
///     let mut session = model.open_session(SessionConfig::default()).await?;
///     assert!(!session.id().is_empty());
///     assert_eq!(session.model_id(), model.model_id());
///     let closed = session.close().await;
///     closed
/// }.await;
/// let cleanup = provider.shutdown().await;
/// observed?;
/// cleanup?;
///
/// # Ok(())
/// # }
/// ```
///
pub struct Session {
    inner: SessionInner,
    provider: Arc<ProviderRuntime>,
}

impl Session {
    pub(crate) fn new(inner: SessionInner, provider: Arc<ProviderRuntime>) -> Self {
        Self { inner, provider }
    }
}

impl SessionBackend for SessionInner {
    fn id(&self) -> &str {
        match self {
            Self::OpenAi(session) => session.id(),
        }
    }

    fn model_id(&self) -> &ModelId {
        match self {
            Self::OpenAi(session) => session.model_id(),
        }
    }

    async fn send(&mut self, input: SessionInput) -> Result<(), SdkError> {
        match self {
            Self::OpenAi(session) => session.send(input).await,
        }
    }

    async fn recv(&mut self) -> Result<SessionEvent, SdkError> {
        match self {
            Self::OpenAi(session) => session.recv().await,
        }
    }

    async fn close(self) -> Result<(), SdkError> {
        match self {
            Self::OpenAi(session) => session.close().await,
        }
    }
}

impl SessionBackend for Session {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn model_id(&self) -> &ModelId {
        self.inner.model_id()
    }

    async fn send(&mut self, input: SessionInput) -> Result<(), SdkError> {
        let _admission = self.provider.admit(Operation::SessionSend)?;
        self.inner
            .send(input)
            .await
            .map_err(|error| error.at_operation(Operation::SessionSend))
    }

    async fn recv(&mut self) -> Result<SessionEvent, SdkError> {
        let _admission = self.provider.admit(Operation::SessionReceive)?;
        self.inner
            .recv()
            .await
            .map_err(|error| error.at_operation(Operation::SessionReceive))
    }

    async fn close(self) -> Result<(), SdkError> {
        self.inner
            .close()
            .await
            .map_err(|error| error.at_operation(Operation::SessionClose))
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("id", &self.id())
            .field("model_id", &self.model_id())
            .finish()
    }
}
