//! Conversational continuity and explicitly selected native realtime interaction.
//!
//! Select an exact [`crate::ModelBackend`], open an explicit [`SessionProfile`],
//! and borrow command/event halves with [`SessionBackend::split`]. Submission
//! acknowledges local acceptance; provider outcomes arrive through the event half.
//! Only the unique owner can close. Tools and agent policy remain caller-owned.
//!
//! [`SessionProfile::Conversation`] retains exact generation history for OpenAI,
//! Anthropic, Gemini and Ollama. [`SessionProfile::Realtime`] selects native OpenAI
//! Realtime or Gemini Live on a compatible exact model. Unsupported combinations
//! fail explicitly, without changing the selected model or profile.
//!
//! # Submit, observe and close
//!
//! This example requires a running Ollama service with the exact model installed.
//! It observes the response caused by its accepted input, then closes the Session
//! and shuts down the Provider even if submission or observation fails. Its deadline
//! bounds local observation; neither timeout nor local cleanup proves remote stop.
//! Response outcomes are distinct from command acceptance and must be inspected.
//!
//! ```no_run
//! use std::time::Duration;
//! use providers_kit::{KitError, ModelBackend, ModelId, Provider, ProviderBackend,
//!     ProviderConfig, SessionBackend};
//! use providers_kit::config::{OllamaConfig, ProviderKind, TransportConfig};
//! use providers_kit::error::{ErrorKind, ErrorPhase, Operation};
//! use providers_kit::generation::Message;
//! use providers_kit::session::{SessionConfig, SessionProfile, SessionCommand,
//!     SessionCommands, SessionEvents, SessionEvent, ResponseOutcome};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), KitError> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default(),
//! ))?;
//! let work = async {
//!     let model = provider.model(ModelId::new("exact-local-model-id")?)?;
//!     let mut session = model.open_session(SessionConfig::new(SessionProfile::Conversation)).await?;
//!     let received = tokio::time::timeout(Duration::from_secs(60), async {
//!         let (mut commands, mut events) = session.split();
//!         let accepted = commands.submit(SessionCommand::Input(Message::user_text("Hello")?)).await?;
//!         let mut expected_response = None;
//!         while let Some(event) = events.recv().await {
//!             match event {
//!                 SessionEvent::ResponseStarted { command: Some(command), response }
//!                     if command == accepted => expected_response = Some(response),
//!                 SessionEvent::ResponseFinished { response, outcome, .. }
//!                     if expected_response.as_ref() == Some(&response) => return Ok(outcome),
//!                 SessionEvent::CommandFailed { command, error, .. }
//!                     if command == accepted => return Err(error),
//!                 SessionEvent::Terminated { reason, .. } => return Err(reason),
//!                 _ => {}
//!             }
//!         }
//!         Err(KitError::new(ErrorKind::Closed, Operation::SessionReceive, ErrorPhase::SessionReceive))
//!     }).await;
//!     let result = received.unwrap_or_else(|_| Err(KitError::new(
//!         ErrorKind::Timeout, Operation::SessionReceive, ErrorPhase::SessionReceive,
//!     ))).and_then(|outcome| match outcome {
//!         ResponseOutcome::Failed(error) | ResponseOutcome::Unresolved(error) => Err(error),
//!         other => Ok(other),
//!     });
//!     match (result, session.close().await) {
//!         (Ok(outcome), Ok(())) => Ok(outcome),
//!         (Err(primary), Ok(())) | (Ok(_), Err(primary)) => Err(primary),
//!         (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!     }
//! }.await;
//! let outcome = match (work, provider.shutdown().await) {
//!     (Ok(outcome), Ok(())) => Ok(outcome),
//!     (Err(primary), Ok(())) | (Ok(_), Err(primary)) => Err(primary),
//!     (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//! }?;
//! println!("Response outcome: {outcome:?}");
//! # Ok(())
//! # }
//! ```
//!
//! # Continue caller-executed tools
//!
//! Execute only calls delivered by [`SessionEvent::ToolRequested`]. Preserve each
//! [`ToolRequestId`] and the call's genuine optional native id and name when
//! constructing [`SessionToolResult`]. [`SessionCommand::ToolResults`] validates
//! the whole submitted batch before accepting any results. Managed Conversation
//! retains accepted results once and starts one continuation generation after the
//! required set is complete; it never executes a tool or runs an autonomous loop.
//! Native Live calls may need results before the response completes. Delivered
//! calls can be invalidated by [`SessionEvent::ToolsInvalidated`]; invalidation
//! cannot undo side effects the application already performed.
//!
//! # Observe independently of control
//!
//! A pending [`SessionEvents::recv`] can coexist with command submission. Dropping
//! that receive loses neither retained events nor accepted work. `None` means
//! permanent drain; an idle session remains pending. [`SessionEvent::Terminated`]
//! is the single fatal observation and retains unfinished commands, responses and
//! tool obligations. A response ending does not end capture, playback or late
//! transcription; retain each event's original attribution, including `None`.
//!
//! Cancellation, capture discard and playback reconciliation are separate
//! commands. [`CancellationResolution`] reports stop evidence independently from
//! [`SessionContinuity`]. Uncertain continuity blocks further generation mutations;
//! observation and cleanup remain available. Provider sealing rejects new work,
//! settles accepted work that has not started, and leaves observation, cancellation
//! and consuming close available. Close does not require ordinary output draining
//! or application tool execution. Keep the runtime driven until cleanup settles.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use crate::KitError;
use crate::core::filter_tools_for_choice;
use crate::error::{Operation, ValidationViolation};
use crate::generation::{
    ContentPart, FinishReason, FunctionCall, FunctionResult, GenerateResponse, GenerationConfig,
    Message, ProviderToolSpec, ReasoningConfig, ResponseFormat, ToolChoice, Usage,
};
use crate::models::ModelId;
use crate::provider::ProviderRuntime;
use crate::providers::anthropic::AnthropicSession;
use crate::providers::gemini::GeminiSession;
use crate::providers::ollama::OllamaSession;
use crate::providers::openai::OpenAiSession;

/// The explicitly requested interaction protocol; there is no default or fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionProfile {
    /// Library-retained conversation through the exact selected generation model.
    /// All four supplied providers support this profile; model/settings support
    /// remains authoritative at the operation boundary.
    Conversation,
    /// Provider-native realtime interaction on a compatible exact model.
    /// Supplied OpenAI and Gemini implementations support this profile.
    Realtime,
}

/// Automatic or caller-managed native input activity boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionTurnDetection {
    /// Native voice activity detection.
    Automatic,
    /// Explicit input activity start/end commands.
    Manual,
}

/// Fixed, validated session configuration. Select the profile explicitly.
///
/// Configuration applies for the entire Session lifetime. Set tool declarations
/// before a generation policy that requires them. Constructors validate local
/// invariants. [`ModelBackend::open_session`](crate::ModelBackend::open_session)
/// checks profile support and performs any native setup handshake. Managed
/// generation can reject model-specific settings later through Session events.
///
/// ```
/// use providers_kit::session::{SessionConfig, SessionProfile};
/// let config = SessionConfig::new(SessionProfile::Conversation)
///     .with_instructions("Answer briefly")?;
/// assert_eq!(config.profile(), SessionProfile::Conversation);
/// assert_eq!(config.instructions(), Some("Answer briefly"));
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct SessionConfig {
    profile: SessionProfile,
    instructions: Option<String>,
    input_audio: Option<(String, bool)>,
    output_audio: Option<(String, bool)>,
    turn_detection: Option<SessionTurnDetection>,
    tools: Vec<ProviderToolSpec>,
    generation: GenerationConfig,
    response_format: Option<ResponseFormat>,
    reasoning: Option<ReasoningConfig>,
}

impl SessionConfig {
    /// Selects a profile without inferring it from a model name. See [`Self`].
    #[must_use]
    pub fn new(profile: SessionProfile) -> Self {
        Self {
            profile,
            instructions: None,
            input_audio: None,
            output_audio: None,
            turn_detection: None,
            tools: Vec::new(),
            generation: GenerationConfig::default(),
            response_format: None,
            reasoning: None,
        }
    }

    /// Returns the explicitly selected profile. See [`Self`].
    #[must_use]
    pub const fn profile(&self) -> SessionProfile {
        self.profile
    }

    /// Sets nonblank session instructions. See [`Self`].
    ///
    /// # Errors
    /// Rejects blank instructions.
    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Result<Self, KitError> {
        let instructions = instructions.into();
        if instructions.trim().is_empty() {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "session.instructions",
                },
                "session instructions must not be blank",
            ));
        }
        self.instructions = Some(instructions);
        Ok(self)
    }

    /// Returns configured instructions. See [`Self`].
    #[must_use]
    pub fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }

    /// Sets the existing generation options without duplicating their schema.
    ///
    /// # Errors
    /// Rejects a tool selection inconsistent with configured tools. See [`Self`].
    pub fn with_generation(mut self, generation: GenerationConfig) -> Result<Self, KitError> {
        Self::validate_tools(&self.tools, generation.tool_choice())?;
        self.generation = generation;
        Ok(self)
    }

    /// Returns exact configured generation options. See [`Self`].
    #[must_use]
    pub const fn generation(&self) -> &GenerationConfig {
        &self.generation
    }

    /// Sets function and hosted declarations; native support is checked on open.
    ///
    /// # Errors
    /// Rejects duplicate names or inconsistent selection. See [`Self`].
    pub fn with_tools(mut self, tools: Vec<ProviderToolSpec>) -> Result<Self, KitError> {
        Self::validate_tools(&tools, self.generation.tool_choice())?;
        self.tools = tools;
        Ok(self)
    }

    /// Returns configured declarations. See [`Self`].
    #[must_use]
    pub fn tools(&self) -> &[ProviderToolSpec] {
        &self.tools
    }

    /// Sets the existing response format; unsupported profiles reject it on open.
    /// See [`Self`] for configuration and [`ResponseFormat`] for request values.
    #[must_use]
    pub fn with_response_format(mut self, format: ResponseFormat) -> Self {
        self.response_format = Some(format);
        self
    }

    /// Returns the configured response format. See [`Self`].
    #[must_use]
    pub const fn response_format(&self) -> Option<&ResponseFormat> {
        self.response_format.as_ref()
    }

    /// Sets the existing reasoning configuration; native support is checked on open.
    /// See [`Self`] for configuration and [`ReasoningConfig`] for request values.
    #[must_use]
    pub fn with_reasoning(mut self, reasoning: ReasoningConfig) -> Self {
        self.reasoning = Some(reasoning);
        self
    }

    /// Returns the configured reasoning options. See [`Self`].
    #[must_use]
    pub const fn reasoning(&self) -> Option<&ReasoningConfig> {
        self.reasoning.as_ref()
    }

    /// Enables native audio capture in an exact media type. See [`Self`].
    ///
    /// # Errors
    /// Rejects malformed audio media types. Profile/model support is checked on open.
    pub fn with_input_audio(
        mut self,
        media_type: impl Into<String>,
        transcription: bool,
    ) -> Result<Self, KitError> {
        self.input_audio = Some((
            Self::validate_audio_format(media_type.into())?,
            transcription,
        ));
        Ok(self)
    }

    /// Enables native audio output in an exact media type. See [`Self`].
    ///
    /// # Errors
    /// Rejects malformed audio media types. Profile/model support is checked on open.
    pub fn with_output_audio(
        mut self,
        media_type: impl Into<String>,
        transcription: bool,
    ) -> Result<Self, KitError> {
        self.output_audio = Some((
            Self::validate_audio_format(media_type.into())?,
            transcription,
        ));
        Ok(self)
    }

    /// Returns the configured input media type. See [`Self`].
    #[must_use]
    pub fn input_audio_format(&self) -> Option<&str> {
        self.input_audio.as_ref().map(|(value, _)| value.as_str())
    }
    /// Returns the configured output media type. See [`Self`].
    #[must_use]
    pub fn output_audio_format(&self) -> Option<&str> {
        self.output_audio.as_ref().map(|(value, _)| value.as_str())
    }
    /// Reports requested input transcription. See [`Self`].
    #[must_use]
    pub fn input_transcription(&self) -> bool {
        self.input_audio.as_ref().is_some_and(|(_, value)| *value)
    }
    /// Reports requested output transcription. See [`Self`].
    #[must_use]
    pub fn output_transcription(&self) -> bool {
        self.output_audio.as_ref().is_some_and(|(_, value)| *value)
    }
    /// Sets the fixed native activity-detection policy. See [`Self`].
    #[must_use]
    pub const fn with_turn_detection(mut self, detection: SessionTurnDetection) -> Self {
        self.turn_detection = Some(detection);
        self
    }
    /// Returns the native activity-detection policy, defaulting to
    /// [`SessionTurnDetection::Automatic`] when omitted. See [`Self`].
    #[must_use]
    pub const fn turn_detection(&self) -> SessionTurnDetection {
        match self.turn_detection {
            Some(detection) => detection,
            None => SessionTurnDetection::Automatic,
        }
    }

    pub(crate) const fn has_turn_detection(&self) -> bool {
        self.turn_detection.is_some()
    }

    fn validate_tools(
        tools: &[ProviderToolSpec],
        choice: Option<&ToolChoice>,
    ) -> Result<(), KitError> {
        let mut names = BTreeSet::new();
        for tool in tools {
            if let ProviderToolSpec::Function(function) = tool
                && !names.insert(function.name())
            {
                return Err(KitError::invalid_input(
                    ValidationViolation::Duplicate {
                        field: "session.tools.name",
                    },
                    "session function name is duplicated",
                ));
            }
        }
        if tools.is_empty() && matches!(choice, Some(ToolChoice::Required | ToolChoice::Any)) {
            return Err(KitError::invalid_input(
                ValidationViolation::MissingField {
                    field: "session.tools",
                },
                "required tool choice needs declarations",
            ));
        }
        filter_tools_for_choice(tools, choice)?;
        Ok(())
    }

    fn validate_audio_format(value: String) -> Result<String, KitError> {
        if !Self::valid_audio_format(&value) {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "session.audio_format",
                    rule: "unpadded audio media type with valid parameters",
                },
                "invalid audio media type",
            ));
        }
        Ok(value)
    }

    fn audio_token(byte: u8) -> bool {
        byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    }

    fn valid_audio_format(value: &str) -> bool {
        if value.trim() != value
            || !value.starts_with("audio/")
            || !value.is_ascii()
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return false;
        }
        let bytes = value.as_bytes();
        let mut position = 6;
        let start = position;
        while position < bytes.len() && Self::audio_token(bytes[position]) {
            position += 1;
        }
        if position == start {
            return false;
        }
        while position < bytes.len() {
            if bytes[position] != b';' {
                return false;
            }
            position += 1;
            while position < bytes.len() && bytes[position] == b' ' {
                position += 1;
            }
            let start = position;
            while position < bytes.len() && Self::audio_token(bytes[position]) {
                position += 1;
            }
            if position == start || bytes.get(position) != Some(&b'=') {
                return false;
            }
            position += 1;
            if bytes.get(position) == Some(&b'"') {
                position += 1;
                let mut closed = false;
                while position < bytes.len() {
                    match bytes[position] {
                        b'"' => {
                            position += 1;
                            closed = true;
                            break;
                        }
                        b'\\' => {
                            position += 1;
                            if position == bytes.len() {
                                return false;
                            }
                        }
                        _ => {}
                    }
                    position += 1;
                }
                if !closed {
                    return false;
                }
            } else {
                let start = position;
                while position < bytes.len() && Self::audio_token(bytes[position]) {
                    position += 1;
                }
                if position == start {
                    return false;
                }
            }
        }
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ScopedSequence {
    session: String,
    sequence: NonZeroU64,
}

impl ScopedSequence {
    fn new(session: String, sequence: NonZeroU64) -> Result<Self, KitError> {
        if session.is_empty()
            || session.len() > 256
            || session.trim() != session
            || session.chars().any(char::is_control)
        {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "session.token.scope",
                    rule: "1..=256 bytes, unpadded, without control characters",
                },
                "invalid local session token scope",
            ));
        }
        Ok(Self { session, sequence })
    }
}

/// Session-local command acceptance identity; never a native event identifier.
///
/// Token construction is available to caller implementations. It validates a
/// local scope and nonzero sequence, not live ownership; submission checks tokens
/// against the Session that issued them. Native ids must not be inferred from them.
///
/// ```
/// use std::num::NonZeroU64;
/// use providers_kit::session::CommandId;
/// let id = CommandId::new("local-session", NonZeroU64::new(1).unwrap())?;
/// assert_eq!(id.session_id(), "local-session");
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(ScopedSequence);
impl CommandId {
    /// Constructs a scoped nonzero identity. See [`Self`].
    /// # Errors
    /// Rejects a local session identity outside 1 through 256 UTF-8 bytes, or one
    /// that is padded or contains control characters.
    pub fn new(session: impl Into<String>, sequence: NonZeroU64) -> Result<Self, KitError> {
        Ok(Self(ScopedSequence::new(session.into(), sequence)?))
    }
    /// Returns the local owner scope. See [`Self`].
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.0.session
    }
    /// Returns the owner-issued sequence. See [`Self`].
    #[must_use]
    pub const fn sequence(&self) -> NonZeroU64 {
        self.0.sequence
    }
}

/// Session-local response identity, separate from the command that caused it.
/// See [`CommandId`] for scoped token construction.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResponseId(ScopedSequence);
impl ResponseId {
    /// Constructs a scoped nonzero identity, as demonstrated by [`CommandId`].
    /// # Errors
    /// Rejects an invalid local session identity.
    pub fn new(session: impl Into<String>, sequence: NonZeroU64) -> Result<Self, KitError> {
        Ok(Self(ScopedSequence::new(session.into(), sequence)?))
    }
    /// Returns the local owner scope. See [`CommandId`].
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.0.session
    }
    /// Returns the owner-issued sequence. See [`CommandId`].
    #[must_use]
    pub const fn sequence(&self) -> NonZeroU64 {
        self.0.sequence
    }
}

/// Session-local executable tool obligation; absent native IDs remain absent.
/// See [`SessionToolResult`] for result construction and [`CommandId`] for scopes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ToolRequestId(ScopedSequence);
impl ToolRequestId {
    /// Constructs a scoped nonzero identity, as demonstrated by [`CommandId`].
    /// # Errors
    /// Rejects an invalid local session identity.
    pub fn new(session: impl Into<String>, sequence: NonZeroU64) -> Result<Self, KitError> {
        Ok(Self(ScopedSequence::new(session.into(), sequence)?))
    }
    /// Returns the local owner scope. See [`CommandId`].
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.0.session
    }
    /// Returns the owner-issued sequence. See [`CommandId`].
    #[must_use]
    pub const fn sequence(&self) -> NonZeroU64 {
        self.0.sequence
    }
}

/// Session-local output segment used for playback/context reconciliation.
/// See [`CommandId`] for scoped token construction and
/// [`SessionCommand::ReconcilePlayback`] for its use.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutputSegmentId(ScopedSequence);
impl OutputSegmentId {
    /// Constructs a scoped nonzero identity, as demonstrated by [`CommandId`].
    /// # Errors
    /// Rejects an invalid local session identity.
    pub fn new(session: impl Into<String>, sequence: NonZeroU64) -> Result<Self, KitError> {
        Ok(Self(ScopedSequence::new(session.into(), sequence)?))
    }
    /// Returns the local owner scope. See [`CommandId`].
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.0.session
    }
    /// Returns the owner-issued sequence. See [`CommandId`].
    #[must_use]
    pub const fn sequence(&self) -> NonZeroU64 {
        self.0.sequence
    }
}

/// A validated function result associated with one session-local obligation.
///
/// Use the token delivered with [`SessionEvent::ToolRequested`] and preserve the
/// call's native id/name in the result. Construction does not establish that the
/// obligation is still outstanding; [`SessionCommands::submit`] checks the entire
/// result batch against live ownership and correlation before accepting it.
///
/// ```
/// use std::num::NonZeroU64;
/// use providers_kit::generation::{ContentPart, FunctionResult};
/// use providers_kit::session::{SessionToolResult, ToolRequestId};
/// let request = ToolRequestId::new("local-session", NonZeroU64::new(1).unwrap())?;
/// let result = FunctionResult::new(None, "lookup", vec![ContentPart::Text { text: "found".into() }], false)?;
/// let correlated = SessionToolResult::new(request.clone(), result);
/// assert_eq!(correlated.request(), &request);
/// assert_eq!(correlated.result().name(), "lookup");
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct SessionToolResult {
    request: ToolRequestId,
    result: FunctionResult,
}
impl SessionToolResult {
    /// Associates validated values; submission validates ownership and correlation. See [`Self`].
    #[must_use]
    pub fn new(request: ToolRequestId, result: FunctionResult) -> Self {
        Self { request, result }
    }
    /// Returns the obligation identity. See [`Self`].
    #[must_use]
    pub const fn request(&self) -> &ToolRequestId {
        &self.request
    }
    /// Returns the supplied result, with genuine native ID unchanged. See [`Self`].
    #[must_use]
    pub const fn result(&self) -> &FunctionResult {
        &self.result
    }
    pub(crate) fn into_result(self) -> FunctionResult {
        self.result
    }
}

/// Exact cancellation scope; a command can be targeted before a response exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancelTarget {
    /// Work caused by this accepted command.
    Command(CommandId),
    /// This model response only.
    Response(ResponseId),
}

/// Canonical commands. Submission validates support, ownership, bounds and state.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionCommand {
    /// Validated user input. Assistant/history injection is rejected.
    Input(Message<()>),
    /// An atomically validated batch of caller-executed tool results.
    /// Accepted managed results are retained once; a complete required set may
    /// start one continuation generation. Submission never executes the tools.
    ToolResults(Vec<SessionToolResult>),
    /// Stop exactly the identified work where the native protocol permits it.
    /// This does not discard input, rewind accepted history or reconcile playback.
    Cancel(CancelTarget),
    /// Begin caller-managed native input activity.
    StartInput,
    /// Raw capture bytes, with no implicit activity end or response creation.
    Audio {
        /// Exact capture encoding, matching the fixed input audio configuration.
        media_type: String,
        /// One raw audio chunk; framing and activity boundaries are separate commands.
        data: Vec<u8>,
    },
    /// End caller-managed input activity; not output completion.
    EndInput,
    /// End/flush a native input audio stream where supported.
    EndAudioStream,
    /// Discard uncommitted capture, independently of response cancellation.
    DiscardInput,
    /// Reconcile the consumed portion of an output segment with native context.
    ReconcilePlayback {
        /// The originally observed audio output segment.
        segment: OutputSegmentId,
        /// Duration actually consumed from the start of this segment.
        played: Duration,
    },
}

/// Whether retained conversation state can support further mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionContinuity {
    /// Retained state is valid; this does not imply idle or remotely stopped.
    Reusable,
    /// Mutation is blocked; observation and cleanup remain available.
    Uncertain,
    /// The session can no longer continue.
    Closed,
}

/// Authoritative status of one response, not the entire conversation.
#[derive(Clone, Debug)]
pub enum ResponseOutcome {
    /// Affirmative native completion evidence.
    Completed,
    /// Affirmative partial completion, with a portable reason when supplied.
    Partial {
        /// The portable partial-completion reason when native evidence supplies one.
        reason: Option<FinishReason>,
    },
    /// A correlated cancellation settled this response.
    Cancelled,
    /// Native activity interrupted this response.
    Interrupted,
    /// Authoritatively failed response.
    Failed(KitError),
    /// Work ended without sufficient evidence to determine its remote outcome.
    Unresolved(KitError),
}

/// Cancellation resolution independently of conversation continuity.
#[derive(Clone, Debug)]
pub enum CancellationResolution {
    /// Natural completion was already authoritative.
    AlreadySettled,
    /// Matching native evidence acknowledged a stop.
    Stopped,
    /// Local streaming stopped; no remote-stop guarantee is made.
    LocallyStopped,
    /// Stop could not be confirmed; the diagnostic retains its actual cause.
    Unconfirmed(KitError),
}

/// Actual attribution of a transcription observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TranscriptionSource {
    /// Native input attribution can be absent; it is never guessed from activity.
    Input {
        /// Original accepted command, or `None` when native attribution is unavailable.
        /// After a finalized `None` input transcript, the next observation begins
        /// another unattributed transcript, including a final-only observation.
        /// It does not identify an accepted command or deduplicate prior text.
        command: Option<CommandId>,
    },
    /// Output is attributed to its original response and audio segment.
    Output {
        /// The session-local response identity; late observations keep the original identity.
        response: ResponseId,
        /// The originally observed audio output segment.
        segment: OutputSegmentId,
    },
}

/// Native accounting scope; snapshots must not be added together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UsageScope {
    /// Usage attributable to one response.
    Response(ResponseId),
    /// Native usage snapshot without authoritative response attribution.
    SessionSnapshot,
    /// Explicitly cumulative native session counters.
    SessionCumulative,
}

/// Correlated observations. A fatal error appears only as `Terminated`.
///
/// Managed `output` is an observational projection with its continuation omitted;
/// authoritative replay remains private and unchanged. The projection must not be
/// used to reconstruct history. Native realtime never fabricates that projection.
/// Response completion, command failure and permanent Session termination are
/// distinct. See the [module workflow](self) for correlation and cleanup.
#[derive(Clone, Debug)]
pub enum SessionEvent {
    /// A model generation began, possibly from autonomous native activity.
    ResponseStarted {
        /// Original accepted command, or `None` when native attribution is unavailable.
        command: Option<CommandId>,
        /// The session-local response identity; late observations keep the original identity.
        response: ResponseId,
    },
    /// Provisional ordered content; it is not a replay source.
    Output {
        /// The session-local response identity; late observations keep the original identity.
        response: ResponseId,
        /// The native output segment when available; managed deltas can lack a segment.
        segment: Option<OutputSegmentId>,
        /// One validated provisional content part.
        content: ContentPart,
    },
    /// Transcription delta or native final transcription.
    Transcription {
        /// Actual input/output attribution without guessing an active response.
        source: TranscriptionSource,
        /// A text delta, or the authoritative full transcript when `finalized` is true.
        /// A native marker-only ending preserves the accumulated text in this snapshot.
        text: String,
        /// Whether this observation is an authoritative final value rather than an interim value.
        finalized: bool,
    },
    /// A complete validated executable call whose result can be correlated.
    /// Managed calls follow committed assistant replay; native Live calls can
    /// require results before response completion.
    ToolRequested {
        /// The session-local response identity; late observations keep the original identity.
        response: ResponseId,
        /// Unique obligation token required when submitting its result.
        request: ToolRequestId,
        /// Validated function name, arguments and genuine optional native call identity.
        call: FunctionCall,
    },
    /// Previously delivered calls are no longer valid for submission.
    /// This cannot undo application tool effects that have already occurred.
    ToolsInvalidated {
        /// Delivered obligations that can no longer accept results.
        requests: Vec<ToolRequestId>,
    },
    /// The sole authoritative outcome for one response.
    ResponseFinished {
        /// The session-local response identity; late observations keep the original identity.
        response: ResponseId,
        /// Authoritative completion, partial, interruption, failure or unresolved outcome.
        outcome: ResponseOutcome,
        /// Whether retained state permits further mutation independently of this outcome.
        continuity: SessionContinuity,
        /// Managed observational response with replay omitted; absent for native Realtime.
        output: Option<Box<GenerateResponse<()>>>,
    },
    /// Native interaction/playback interruption, independent of generation outcome.
    /// It can follow completion without revising that response's final outcome.
    Interruption {
        /// Original response when attributable; absent attribution is never guessed.
        response: Option<ResponseId>,
    },
    /// Native interaction/activity boundary; not evidence of successful generation.
    TurnEnded {
        /// Original response when attributable; absent attribution is never guessed.
        response: Option<ResponseId>,
    },
    /// Scoped usage; unknown fields remain unknown, final totals replace snapshots.
    Usage {
        /// Response-local or session-level meaning of the counters.
        scope: UsageScope,
        /// Portable counters preserving unknown values; final totals replace snapshots.
        usage: Usage,
        /// Whether this observation is an authoritative final value rather than an interim value.
        finalized: bool,
    },
    /// Resolution of an accepted cancellation command.
    Cancellation {
        /// The accepted command whose operation is being settled.
        command: CommandId,
        /// Original target of the accepted cancellation request.
        target: CancelTarget,
        /// Actual stop guarantee, independently of subsequent continuity.
        resolution: CancellationResolution,
        /// Whether retained state permits further mutation independently of this outcome.
        continuity: SessionContinuity,
    },
    /// An accepted command failed, including work that never reached a provider.
    CommandFailed {
        /// The accepted command whose operation is being settled.
        command: CommandId,
        /// The post-acceptance failure, preserving its original diagnostic context.
        error: KitError,
        /// True when accepted work never reached its provider operation.
        not_started: bool,
    },
    /// The control operation completed its supported local/native send path.
    /// This does not promise remote application or response-stop acknowledgement.
    ControlApplied {
        /// The accepted command whose operation is being settled.
        command: CommandId,
    },
    /// Permanent terminal observation, including all unfinished local obligations.
    Terminated {
        /// The reason the session permanently terminated.
        reason: KitError,
        /// Whether retained state permits further mutation independently of this outcome.
        continuity: SessionContinuity,
        /// Accepted commands lacking an authoritative operation outcome at termination.
        unfinished_commands: Vec<CommandId>,
        /// Started responses lacking an authoritative final outcome at termination.
        unfinished_responses: Vec<ResponseId>,
        /// Tool obligations unresolved when the session terminated.
        outstanding_tools: Vec<ToolRequestId>,
    },
}

/// Static command-half contract. Acceptance and receipt occur in one poll.
///
/// The half borrows its unique [`SessionBackend`] owner and has no independent
/// close authority. See the [module workflow](self).
pub trait SessionCommands: Send {
    /// Admits a command locally and returns its correlation token.
    ///
    /// The receipt proves bounded local ownership, not provider acceptance or
    /// successful completion. Dropping submission before acceptance leaves no
    /// accepted work; after acceptance, the owner settles it independently of
    /// the waiter. A pending receive cannot block this path. See the
    /// [module workflow](self) for observation and consuming cleanup.
    ///
    /// # Errors
    /// Pre-acceptance validation, capacity, lifecycle and unsupported errors have
    /// no accepted or remote side effect. Later failures arrive through events.
    fn submit(
        &mut self,
        command: SessionCommand,
    ) -> impl Future<Output = Result<CommandId, KitError>> + Send + '_;
}

/// Static event-half contract. Pending receive never excludes command progress.
/// See the [module workflow](self) for borrowing this half alongside commands.
pub trait SessionEvents: Send {
    /// Receives an observation; `None` means permanently drained, never idle.
    /// Dropping a pending receive loses no accepted work or retained event.
    /// Fatal failure arrives once as [`SessionEvent::Terminated`], not through
    /// a separate receive error. See the [module workflow](self).
    fn recv(&mut self) -> impl Future<Output = Option<SessionEvent>> + Send + '_;
}

/// Unique owning session with concrete borrowed halves and one close authority.
///
/// Caller implementations retain their own associated halves directly. The
/// supplied [`Session`] and halves are non-Clone; neither half can outlive or
/// independently close its owner. See the [module workflow](self).
pub trait SessionBackend: Send + 'static {
    /// Concrete command half borrowing this owner.
    type Commands<'a>: SessionCommands + 'a
    where
        Self: 'a;
    /// Concrete event half borrowing this owner.
    type Events<'a>: SessionEvents + 'a
    where
        Self: 'a;
    /// Local opaque session identity; native identifiers remain private.
    /// See [`CommandId`] for the scope of locally issued tokens.
    fn id(&self) -> &str;
    /// The exact model selected before opening.
    /// See the [module workflow](self); opening never selects a fallback model.
    fn model_id(&self) -> &ModelId;
    /// Borrows independent command and event halves. See the [module workflow](self).
    /// Release both borrows before consuming the owner with [`Self::close`].
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>);
    /// Consumes the owner, settles local work and performs bounded cleanup.
    ///
    /// Cleanup does not wait for ordinary event draining or application tool
    /// execution. Supplied owners retain join authority when the owner or close
    /// future is dropped; keep the runtime driven for cleanup to finish. Idle
    /// Sessions hold no normal generation admission. See the [module workflow](self).
    ///
    /// # Errors
    /// Returns actual teardown failures; an already sealed parent alone is not
    /// a failure. Local closure does not assert unobserved remote acknowledgement.
    fn close(self) -> impl Future<Output = Result<(), KitError>> + Send;
}

impl SessionCommands for Infallible {
    async fn submit(&mut self, _command: SessionCommand) -> Result<CommandId, KitError> {
        match *self {}
    }
}
impl SessionEvents for Infallible {
    async fn recv(&mut self) -> Option<SessionEvent> {
        match *self {}
    }
}
impl SessionBackend for Infallible {
    type Commands<'a> = Infallible;
    type Events<'a> = Infallible;
    fn id(&self) -> &str {
        match *self {}
    }
    fn model_id(&self) -> &ModelId {
        match *self {}
    }
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        match *self {}
    }
    async fn close(self) -> Result<(), KitError> {
        match self {}
    }
}

pub(crate) enum SessionInner {
    OpenAi(OpenAiSession),
    Anthropic(AnthropicSession),
    Gemini(GeminiSession),
    Ollama(OllamaSession),
}

pub(crate) enum SessionCommandInner<'a> {
    OpenAi(<OpenAiSession as SessionBackend>::Commands<'a>),
    Anthropic(<AnthropicSession as SessionBackend>::Commands<'a>),
    Gemini(<GeminiSession as SessionBackend>::Commands<'a>),
    Ollama(<OllamaSession as SessionBackend>::Commands<'a>),
}

pub(crate) enum SessionEventInner<'a> {
    OpenAi(<OpenAiSession as SessionBackend>::Events<'a>),
    Anthropic(<AnthropicSession as SessionBackend>::Events<'a>),
    Gemini(<GeminiSession as SessionBackend>::Events<'a>),
    Ollama(<OllamaSession as SessionBackend>::Events<'a>),
}

/// Opaque, non-Clone command half borrowing the supplied unique Session.
/// See the [module example](self) for submission and consuming owner cleanup.
pub struct SessionCommandHalf<'a> {
    inner: SessionCommandInner<'a>,
}
/// Opaque, non-Clone event half borrowing the supplied unique Session.
/// See the [module example](self) for observation and consuming owner cleanup.
pub struct SessionEventHalf<'a> {
    inner: SessionEventInner<'a>,
}

/// Unique supplied session, backed by a closed concrete provider implementation.
/// See the [module example](self) for opening, splitting and closing this owner.
pub struct Session {
    inner: SessionInner,
    _provider: Arc<ProviderRuntime>,
}

impl Session {
    pub(crate) fn new(
        mut inner: SessionInner,
        provider: Arc<ProviderRuntime>,
    ) -> Result<Self, KitError> {
        inner.bind_provider(Arc::clone(&provider))?;
        Ok(Self {
            inner,
            _provider: provider,
        })
    }
}

impl SessionInner {
    fn bind_provider(&mut self, provider: Arc<ProviderRuntime>) -> Result<(), KitError> {
        match self {
            Self::OpenAi(session) => session.bind_provider(provider),
            Self::Anthropic(session) => session.bind_provider(provider),
            Self::Gemini(session) => session.bind_provider(provider),
            Self::Ollama(session) => session.bind_provider(provider),
        }
    }
}

impl SessionBackend for SessionInner {
    type Commands<'a> = SessionCommandInner<'a>;
    type Events<'a> = SessionEventInner<'a>;
    fn id(&self) -> &str {
        match self {
            Self::OpenAi(session) => session.id(),
            Self::Anthropic(session) => session.id(),
            Self::Gemini(session) => session.id(),
            Self::Ollama(session) => session.id(),
        }
    }
    fn model_id(&self) -> &ModelId {
        match self {
            Self::OpenAi(session) => session.model_id(),
            Self::Anthropic(session) => session.model_id(),
            Self::Gemini(session) => session.model_id(),
            Self::Ollama(session) => session.model_id(),
        }
    }
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        match self {
            Self::OpenAi(session) => {
                let (commands, events) = session.split();
                (
                    SessionCommandInner::OpenAi(commands),
                    SessionEventInner::OpenAi(events),
                )
            }
            Self::Anthropic(session) => {
                let (commands, events) = session.split();
                (
                    SessionCommandInner::Anthropic(commands),
                    SessionEventInner::Anthropic(events),
                )
            }
            Self::Gemini(session) => {
                let (commands, events) = session.split();
                (
                    SessionCommandInner::Gemini(commands),
                    SessionEventInner::Gemini(events),
                )
            }
            Self::Ollama(session) => {
                let (commands, events) = session.split();
                (
                    SessionCommandInner::Ollama(commands),
                    SessionEventInner::Ollama(events),
                )
            }
        }
    }
    async fn close(self) -> Result<(), KitError> {
        match self {
            Self::OpenAi(session) => session.close().await,
            Self::Anthropic(session) => session.close().await,
            Self::Gemini(session) => session.close().await,
            Self::Ollama(session) => session.close().await,
        }
    }
}

impl SessionCommands for SessionCommandInner<'_> {
    async fn submit(&mut self, command: SessionCommand) -> Result<CommandId, KitError> {
        match self {
            Self::OpenAi(commands) => commands.submit(command).await,
            Self::Anthropic(commands) => commands.submit(command).await,
            Self::Gemini(commands) => commands.submit(command).await,
            Self::Ollama(commands) => commands.submit(command).await,
        }
    }
}
impl SessionEvents for SessionEventInner<'_> {
    async fn recv(&mut self) -> Option<SessionEvent> {
        match self {
            Self::OpenAi(events) => events.recv().await,
            Self::Anthropic(events) => events.recv().await,
            Self::Gemini(events) => events.recv().await,
            Self::Ollama(events) => events.recv().await,
        }
    }
}
impl SessionCommands for SessionCommandHalf<'_> {
    async fn submit(&mut self, command: SessionCommand) -> Result<CommandId, KitError> {
        self.inner
            .submit(command)
            .await
            .map_err(|error| error.at_operation(Operation::SessionSend))
    }
}
impl SessionEvents for SessionEventHalf<'_> {
    async fn recv(&mut self) -> Option<SessionEvent> {
        self.inner.recv().await
    }
}
impl SessionBackend for Session {
    type Commands<'a> = SessionCommandHalf<'a>;
    type Events<'a> = SessionEventHalf<'a>;
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn model_id(&self) -> &ModelId {
        self.inner.model_id()
    }
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        let (commands, events) = self.inner.split();
        (
            SessionCommandHalf { inner: commands },
            SessionEventHalf { inner: events },
        )
    }
    async fn close(self) -> Result<(), KitError> {
        self.inner.close().await
    }
}
impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("id", &self.id())
            .field("model_id", self.model_id())
            .finish()
    }
}
