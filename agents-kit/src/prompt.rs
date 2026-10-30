//! Prompt inputs, exact terminal responses, and transient output observation.
//!
//! [`crate::prompt::PromptRequest`] applies per-prompt overrides to inherited workspace and
//! Agent settings. [`crate::prompt::PromptStream`] observes the same accepted owner as unary
//! prompting: deltas are transient, and only [`crate::prompt::PromptEvent::Completed`] conveys
//! its authoritative response. Dropping observation does not abort accepted work.

use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::{
    KitError, KnowledgePolicy, ModelSelector, PersonalitySelector, ReasoningEffort, ToolSelection,
    WorkspaceContext,
};

/// Strict self-contained JSON Schema requested for structured prompt output.
///
/// ```
/// use agents_kit::prompt::StructuredOutputRequest;
/// let output = StructuredOutputRequest::new(serde_json::json!({"type": "integer"}))?;
/// assert_eq!(output.schema()["type"], "integer");
/// # Ok::<(), agents_kit::KitError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructuredOutputRequest {
    schema: serde_json::Value,
}

#[derive(Clone, Copy, Debug)]
struct StructuredOutputLocalRetriever;

impl fmt::Display for StructuredOutputLocalRetriever {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("structured output schema references must remain local")
    }
}

impl std::error::Error for StructuredOutputLocalRetriever {}

impl jsonschema::Retrieve for StructuredOutputLocalRetriever {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(*self))
    }
}

impl StructuredOutputRequest {
    /// Validates and compiles a self-contained Draft 2020-12 schema.
    ///
    /// Instance data that happens to contain a `$ref` key is not interpreted
    /// as a schema reference, while actual external schema references remain
    /// unavailable.
    ///
    /// ```
    /// use agents_kit::prompt::StructuredOutputRequest;
    /// use serde_json::json;
    ///
    /// let request = StructuredOutputRequest::new(json!({
    ///     "$defs": {"value": {"type": "string"}},
    ///     "allOf": [{"$ref": "#/$defs/value"}],
    ///     "const": {"$ref": "literal instance data"}
    /// }))?;
    /// assert!(request.schema().is_object());
    ///
    /// assert!(StructuredOutputRequest::new(json!({
    ///     "$ref": "https://example.invalid/schema.json"
    /// })).is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    ///
    /// # Errors
    /// Returns InvalidInput unless the schema is a nonempty object that validates
    /// against Draft 2020-12 and compiles using only local schema references.
    pub fn new(schema: serde_json::Value) -> Result<Self, KitError> {
        if schema.as_object().is_none_or(|object| object.is_empty()) {
            return Err(KitError::invalid_input(
                "structured output schema must be a nonempty JSON object",
            ));
        }
        jsonschema::draft202012::meta::validate(&schema).map_err(|_| {
            KitError::invalid_input("structured output schema is not valid Draft 2020-12")
        })?;
        jsonschema::draft202012::options()
            .with_retriever(StructuredOutputLocalRetriever)
            .build(&schema)
            .map_err(|_| {
                KitError::invalid_input(
                    "structured output schema must compile as a self-contained document",
                )
            })?;
        Ok(Self { schema })
    }

    /// Returns the validated schema document.
    #[must_use]
    ///
    /// ```
    /// use agents_kit::prompt::StructuredOutputRequest;
    /// let schema = serde_json::json!({"type":"boolean"});
    /// assert_eq!(StructuredOutputRequest::new(schema.clone())?.schema(), &schema);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn schema(&self) -> &serde_json::Value {
        &self.schema
    }
}

#[derive(Clone, Debug)]
/// Supplies one foreground prompt and its optional portable overrides.
///
/// ```
/// use agents_kit::{
///     model::ReasoningEffort,
///     prompt::PromptRequest,
/// };
///
/// let request = PromptRequest::new("Summarize the current task")?
///     .with_reasoning_effort(ReasoningEffort::medium());
/// assert_eq!(request.text(), "Summarize the current task");
/// # Ok::<(), agents_kit::KitError>(())
/// ```
pub struct PromptRequest {
    text: String,
    cwd: Option<PathBuf>,
    model: Option<ModelSelector>,
    reasoning_effort: Option<ReasoningEffort>,
    personality: Option<PersonalitySelector>,
    instructions: Option<String>,
    timeout: Option<Duration>,
    knowledge_policy: Option<KnowledgePolicy>,
    structured_output: Option<StructuredOutputRequest>,
    tool_selection: Option<ToolSelection>,
}

impl PromptRequest {
    /// Validates and constructs a request with no overrides.
    ///
    /// Prompt text is retained exactly, including whitespace.
    ///
    /// # Errors
    /// An empty prompt returns InvalidInput.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.text(), "Hello");
    /// assert!(PromptRequest::new("").is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(text: impl Into<String>) -> Result<Self, KitError> {
        let text = text.into();
        if text.is_empty() {
            return Err(KitError::invalid_input("prompt text must not be empty"));
        }
        Ok(Self {
            text,
            cwd: None,
            model: None,
            reasoning_effort: None,
            personality: None,
            instructions: None,
            timeout: None,
            knowledge_policy: None,
            structured_output: None,
            tool_selection: None,
        })
    }

    /// Sets a validated path relative to the active workspace context.
    ///
    /// This path is relative to the active workspace, not the caller process.
    ///
    /// # Errors
    /// Absolute paths, parent traversal, and invalid portable components return InvalidInput.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_cwd("src".into())?;
    /// assert_eq!(request.cwd(), Some(std::path::Path::new("src")));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_cwd(mut self, cwd: PathBuf) -> Result<Self, KitError> {
        self.cwd = Some(WorkspaceContext::normalized_relative(&cwd)?);
        Ok(self)
    }

    /// Sets the model selector for this prompt.
    ///
    /// Availability is validated by the selected backend when submitting the prompt.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_model(agents_kit::model::ModelSelector::new("model-id")?);
    /// assert_eq!(request.model().unwrap().as_str(), "model-id");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_model(mut self, model: ModelSelector) -> Self {
        self.model = Some(model);
        self
    }

    /// Sets the reasoning effort for this prompt.
    ///
    /// The value must be supported by the selected backend and model; setting it
    /// does not validate availability. See the [backend restrictions](crate::model#supplied-selector-restrictions).
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_reasoning_effort(agents_kit::model::ReasoningEffort::low());
    /// assert_eq!(request.reasoning_effort(), Some(&agents_kit::model::ReasoningEffort::low()));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_reasoning_effort(mut self, reasoning_effort: ReasoningEffort) -> Self {
        self.reasoning_effort = Some(reasoning_effort);
        self
    }

    /// Sets the personality selector for this prompt.
    #[must_use]
    ///
    /// This retains the caller value without validating backend support. The supplied
    /// Codex, OpenCode and Custom implementations currently reject explicit personality
    /// selection; caller backends may advertise and support it. See [`crate::model`].
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_personality(agents_kit::model::PersonalitySelector::new("concise")?);
    /// assert_eq!(request.personality().unwrap().as_str(), "concise");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_personality(mut self, personality: PersonalitySelector) -> Self {
        self.personality = Some(personality);
        self
    }

    /// Sets nonempty prompt-level instructions.
    ///
    /// Text is preserved without trimming.
    ///
    /// # Errors
    /// Empty instructions return InvalidInput.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_instructions("Be concise.")?;
    /// assert_eq!(request.instructions(), Some("Be concise."));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Result<Self, KitError> {
        let instructions = instructions.into();
        if instructions.is_empty() {
            return Err(KitError::invalid_input(
                "prompt instructions must not be empty",
            ));
        }
        self.instructions = Some(instructions);
        Ok(self)
    }

    /// Sets the effective managed-knowledge policy for this prompt.
    ///
    /// This configures private retrieval; it does not install a model-visible tool.
    ///
    /// # Errors
    /// An Only policy with no scopes or duplicate scopes returns InvalidInput.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_knowledge_policy(agents_kit::knowledge::KnowledgePolicy::Disabled)?;
    /// assert_eq!(request.knowledge_policy(), Some(&agents_kit::knowledge::KnowledgePolicy::Disabled));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_knowledge_policy(mut self, policy: KnowledgePolicy) -> Result<Self, KitError> {
        policy.validate()?;
        self.knowledge_policy = Some(policy);
        Ok(self)
    }

    /// Sets a nonzero foreground prompt timeout that starts after admission.
    ///
    /// The timeout bounds execution after admission; it does not renew lifecycle deadlines.
    ///
    /// # Errors
    /// A zero timeout returns InvalidInput.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_timeout(std::time::Duration::from_secs(20))?;
    /// assert_eq!(request.timeout(), Some(std::time::Duration::from_secs(20)));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, KitError> {
        if timeout.is_zero() {
            return Err(KitError::invalid_input(
                "prompt timeout must be greater than zero",
            ));
        }
        self.timeout = Some(timeout);
        Ok(self)
    }

    /// Requests strict output matching the supplied schema.
    #[must_use]
    ///
    /// Selected backends may reject unsupported structured-output execution.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let schema = agents_kit::prompt::StructuredOutputRequest::new(serde_json::json!({"type":"string"}))?;
    /// let request = request.with_structured_output(schema);
    /// assert!(request.structured_output().is_some());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_structured_output(mut self, structured_output: StructuredOutputRequest) -> Self {
        self.structured_output = Some(structured_output);
        self
    }

    /// Sets the portable tool-selection override.
    ///
    /// A restricted catalog is distinct from requiring a call.
    ///
    /// # Errors
    /// Empty named selections, duplicate names, or invalid portable names return InvalidInput.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// let request = request.with_tool_selection(agents_kit::tool::ToolSelection::None)?;
    /// assert_eq!(request.tool_selection(), Some(&agents_kit::tool::ToolSelection::None));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_tool_selection(mut self, selection: ToolSelection) -> Result<Self, KitError> {
        selection.validate()?;
        self.tool_selection = Some(selection);
        Ok(self)
    }

    /// Returns the required prompt text.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.text(), "Hello");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the optional workspace-relative cwd override.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.cwd(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// Returns the optional model override.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.model(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn model(&self) -> Option<&ModelSelector> {
        self.model.as_ref()
    }

    /// Returns the optional reasoning-effort override.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.reasoning_effort(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn reasoning_effort(&self) -> Option<&ReasoningEffort> {
        self.reasoning_effort.as_ref()
    }

    /// Returns the optional personality override.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.personality(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn personality(&self) -> Option<&PersonalitySelector> {
        self.personality.as_ref()
    }

    /// Returns the optional prompt-level instructions.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.instructions(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }

    /// Returns the optional managed-knowledge policy override.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.knowledge_policy(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn knowledge_policy(&self) -> Option<&KnowledgePolicy> {
        self.knowledge_policy.as_ref()
    }

    /// Returns the optional foreground prompt timeout.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.timeout(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Returns the optional strict structured-output request.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.structured_output(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn structured_output(&self) -> Option<&StructuredOutputRequest> {
        self.structured_output.as_ref()
    }

    /// Returns the optional portable tool-selection override.
    ///
    /// ```
    /// use agents_kit::prompt::PromptRequest;
    /// let request = PromptRequest::new("Hello")?;
    /// assert_eq!(request.tool_selection(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn tool_selection(&self) -> Option<&ToolSelection> {
        self.tool_selection.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Contains the exact terminal output of one foreground prompt.
///
/// ```
/// use agents_kit::prompt::{PromptResponse, UsageInfo};
/// let response = PromptResponse::new("Done".into(), Some(UsageInfo::new(10, 2)));
/// assert_eq!(response.output(), "Done");
/// assert_eq!(response.usage().unwrap().input_tokens(), 10);
/// ```
pub struct PromptResponse {
    output: String,
    structured_output: Option<serde_json::Value>,
    usage: Option<UsageInfo>,
}

impl PromptResponse {
    /// Constructs a terminal response from output and whole-call usage.
    ///
    /// ```
    /// use agents_kit::prompt::PromptResponse;
    /// let response = PromptResponse::new("Done".into(), None);
    /// assert_eq!(response.output(), "Done");
    /// ```
    pub fn new(output: String, usage: Option<UsageInfo>) -> Self {
        Self {
            output,
            structured_output: None,
            usage,
        }
    }

    /// Constructs a terminal response from exact text, structured value, and usage.
    #[must_use]
    ///
    /// ```
    /// use agents_kit::prompt::PromptResponse;
    /// let response = PromptResponse::from_parts("7".into(), Some(serde_json::json!(7)), None);
    /// assert_eq!(response.structured_output(), Some(&serde_json::json!(7)));
    /// ```
    pub fn from_parts(
        output: String,
        structured_output: Option<serde_json::Value>,
        usage: Option<UsageInfo>,
    ) -> Self {
        Self {
            output,
            structured_output,
            usage,
        }
    }

    /// Returns the exact terminal output.
    ///
    /// ```
    /// use agents_kit::prompt::PromptResponse;
    /// let response = PromptResponse::new(" Done\n".into(), None);
    /// assert_eq!(response.output(), " Done\n");
    /// ```
    pub fn output(&self) -> &str {
        &self.output
    }

    /// Returns whole-call usage when the backend can report it honestly.
    ///
    /// ```
    /// use agents_kit::prompt::PromptResponse;
    /// let response = PromptResponse::new("Done".into(), None);
    /// assert_eq!(response.usage(), None);
    /// ```
    pub fn usage(&self) -> Option<&UsageInfo> {
        self.usage.as_ref()
    }

    /// Returns the separately parsed strict structured value, when requested.
    ///
    /// ```
    /// use agents_kit::prompt::PromptResponse;
    /// let response = PromptResponse::new("7".into(), None);
    /// assert_eq!(response.structured_output(), None);
    /// ```
    pub fn structured_output(&self) -> Option<&serde_json::Value> {
        self.structured_output.as_ref()
    }

    /// Consumes the response into exact text, structured value, and usage.
    #[must_use]
    ///
    /// ```
    /// use agents_kit::prompt::PromptResponse;
    /// let response = PromptResponse::from_parts("7".into(), Some(serde_json::json!(7)), None);
    /// let (text, value, usage) = response.into_parts();
    /// assert_eq!(text, "7");
    /// assert_eq!(value, Some(serde_json::json!(7)));
    /// assert_eq!(usage, None);
    /// ```
    pub fn into_parts(self) -> (String, Option<serde_json::Value>, Option<UsageInfo>) {
        (self.output, self.structured_output, self.usage)
    }
}

/// Transient text observation or the authoritative terminal prompt response.
///
/// Intermediate assistant text can differ from the completed response, for
/// example when a prompt contains tool calls before its final answer. Only
/// `Completed` confirms terminal durability; deltas are not durable history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptEvent {
    /// Real source text in its observed order, excluding reasoning and tools.
    OutputDelta {
        /// Exact assistant text fragment.
        text: String,
    },
    /// Exact terminal output, structured value and whole-prompt usage.
    Completed(PromptResponse),
}

/// Non-Clone observation of one accepted prompt's transient output.
///
/// Dropping this value releases observation only. An implementation's accepted
/// prompt remains owned by its conversation and is cancelled through
/// [`ConversationBackend::abort`](crate::ConversationBackend::abort).
/// Completion or error fuses the stream; premature EOF becomes one protocol
/// error. A caller source owns bounded production and accepted-work cleanup.
///
/// ```
/// use agents_kit::prompt::{PromptEvent, PromptResponse, PromptStream};
/// let runtime = tokio::runtime::Builder::new_current_thread().build()?;
/// runtime.block_on(async {
///     let mut output = PromptStream::from_stream(futures_util::stream::iter([
///         Ok(PromptEvent::OutputDelta { text: "ready".into() }),
///         Ok(PromptEvent::Completed(PromptResponse::new("done".into(), None))),
///     ]));
///     assert!(matches!(output.recv().await.transpose()?, Some(PromptEvent::OutputDelta { .. })));
///     let Some(PromptEvent::Completed(response)) = output.recv().await.transpose()? else { panic!("terminal response"); };
///     assert_eq!(response.output(), "done");
///     assert!(output.recv().await.is_none());
///     Ok::<(), agents_kit::KitError>(())
/// })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct PromptStream {
    source: Option<PromptSource>,
}

enum PromptSource {
    Caller(Pin<Box<dyn Stream<Item = Result<PromptEvent, KitError>> + Send + 'static>>),
    Buffered {
        admission: PromptAdmission,
        fragments: mpsc::Receiver<String>,
        terminal: oneshot::Receiver<Result<PromptResponse, KitError>>,
    },
}

enum PromptAdmission {
    Pending(oneshot::Receiver<()>),
    Accepted,
    Rejected,
}

pub(crate) struct PromptOutput {
    state: Mutex<PromptOutputState>,
}

enum PromptOutputState {
    Open {
        admission: Option<oneshot::Sender<()>>,
        fragments: mpsc::Sender<String>,
        terminal: oneshot::Sender<Result<PromptResponse, KitError>>,
    },
    Closed,
}

impl PromptOutput {
    const FRAGMENTS: usize = 64;
    const FRAGMENT_BYTES: usize = 16 * 1024;

    pub(crate) fn channel() -> (Arc<Self>, PromptStream) {
        let (output, stream) = Self::gated_channel();
        output.admit();
        (output, stream)
    }

    pub(crate) fn gated_channel() -> (Arc<Self>, PromptStream) {
        let (admission, admitted) = oneshot::channel();
        let (fragments, receiver) = mpsc::channel(Self::FRAGMENTS);
        let (terminal, completion) = oneshot::channel();
        (
            Arc::new(Self {
                state: Mutex::new(PromptOutputState::Open {
                    admission: Some(admission),
                    fragments,
                    terminal,
                }),
            }),
            PromptStream {
                source: Some(PromptSource::Buffered {
                    admission: PromptAdmission::Pending(admitted),
                    fragments: receiver,
                    terminal: completion,
                }),
            },
        )
    }

    pub(crate) fn admit(&self) {
        if let PromptOutputState::Open { admission, .. } = &mut *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            && let Some(admission) = admission.take()
        {
            let _ = admission.send(());
        }
    }

    pub(crate) fn fragment(&self, mut text: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let PromptOutputState::Open { fragments, .. } = &*state else {
            return;
        };
        loop {
            let mut end = text.len().min(Self::FRAGMENT_BYTES);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            if let Err(error) = fragments.try_send(text[..end].to_owned()) {
                let previous = std::mem::replace(&mut *state, PromptOutputState::Closed);
                if let PromptOutputState::Open { terminal, .. } = previous {
                    let failure = match error {
                        mpsc::error::TrySendError::Full(_) => {
                            KitError::integration("prompt output observation overflow")
                                .with_detail(crate::error::ErrorDetail::ResponseLimit {
                                    limit: Self::FRAGMENTS,
                                })
                                .with_context(
                                    crate::error::Operation::Prompt,
                                    crate::error::ErrorPhase::Publication,
                                )
                        }
                        mpsc::error::TrySendError::Closed(_) => {
                            KitError::cancelled("prompt output observer was dropped")
                        }
                    };
                    let _ = terminal.send(Err(failure));
                }
                return;
            }
            text = &text[end..];
            if text.is_empty() {
                return;
            }
        }
    }

    pub(crate) fn finish(&self, result: Result<PromptResponse, KitError>) {
        let previous = std::mem::replace(
            &mut *self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            PromptOutputState::Closed,
        );
        if let PromptOutputState::Open { terminal, .. } = previous {
            let _ = terminal.send(result);
        }
    }
}

impl PromptStream {
    /// Adapts a caller's concrete value stream without normalizing a backend.
    ///
    /// The source may be non-Unpin. It must produce ordered real text and one
    /// authoritative completed response or error, retain accepted work after
    /// observer drop, and keep pending output bounded.
    ///
    /// See [`PromptStream`] for a complete caller-stream example.
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<PromptEvent, KitError>> + Send + 'static,
    {
        Self {
            source: Some(PromptSource::Caller(Box::pin(stream))),
        }
    }

    /// Receives the next observation, or None after its single terminal item.
    ///
    /// Cancelling this read does not abort the accepted prompt. A premature
    /// source EOF returns Protocol once rather than claiming successful output.
    ///
    /// # Errors
    /// A source failure is yielded once with prompt/publication context. EOF
    /// before completion yields Protocol with IncompleteStream; terminal results
    /// fuse subsequent reads to None.
    ///
    /// ```
    /// use agents_kit::{error::ErrorKind, prompt::PromptStream};
    /// # tokio::runtime::Builder::new_current_thread().build()?.block_on(async {
    /// let mut output = PromptStream::from_stream(futures_util::stream::empty());
    /// let error = output.recv().await.expect("one terminal error").unwrap_err();
    /// assert_eq!(error.kind(), ErrorKind::Protocol);
    /// assert!(output.recv().await.is_none());
    /// # });
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn recv(&mut self) -> Option<Result<PromptEvent, KitError>> {
        let item = match self.source.as_mut()? {
            PromptSource::Caller(source) => source.next().await,
            PromptSource::Buffered {
                admission,
                fragments,
                terminal,
            } => {
                if let PromptAdmission::Pending(receiver) = admission {
                    *admission = if receiver.await.is_ok() {
                        PromptAdmission::Accepted
                    } else {
                        PromptAdmission::Rejected
                    };
                }
                if matches!(admission, PromptAdmission::Accepted)
                    && let Some(text) = fragments.recv().await
                {
                    Some(Ok(PromptEvent::OutputDelta { text }))
                } else {
                    terminal
                        .await
                        .ok()
                        .map(|result| result.map(PromptEvent::Completed))
                }
            }
        };
        match item {
            Some(Ok(PromptEvent::OutputDelta { text })) => {
                Some(Ok(PromptEvent::OutputDelta { text }))
            }
            Some(terminal) => {
                self.source.take();
                Some(terminal.map_err(|error| {
                    error
                        .at_operation(crate::error::Operation::Prompt)
                        .in_phase(crate::error::ErrorPhase::Publication)
                }))
            }
            None => {
                self.source.take();
                Some(Err(KitError::protocol(
                    "prompt output ended before its terminal response",
                )
                .with_context(
                    crate::error::Operation::Prompt,
                    crate::error::ErrorPhase::Publication,
                )
                .with_detail(crate::error::ErrorDetail::IncompleteStream)))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Reports token usage covering every native model call in one prompt.
///
/// ```
/// use agents_kit::prompt::UsageInfo;
/// let usage = UsageInfo::new(10, 2);
/// assert_eq!(usage.input_tokens(), 10);
/// assert_eq!(usage.output_tokens(), 2);
/// ```
pub struct UsageInfo {
    input_tokens: u64,
    output_tokens: u64,
}

impl UsageInfo {
    /// Constructs whole-call input and output token counts.
    ///
    /// ```
    /// use agents_kit::prompt::UsageInfo;
    /// let usage = UsageInfo::new(10, 2);
    /// assert_eq!(usage.output_tokens(), 2);
    /// ```
    pub fn new(input_tokens: u64, output_tokens: u64) -> Self {
        Self {
            input_tokens,
            output_tokens,
        }
    }

    /// Returns the total input-token count.
    ///
    /// ```
    /// use agents_kit::prompt::UsageInfo;
    /// assert_eq!(UsageInfo::new(10, 2).input_tokens(), 10);
    /// ```
    pub fn input_tokens(&self) -> u64 {
        self.input_tokens
    }

    /// Returns the total output-token count.
    ///
    /// ```
    /// use agents_kit::prompt::UsageInfo;
    /// assert_eq!(UsageInfo::new(10, 2).output_tokens(), 2);
    /// ```
    pub fn output_tokens(&self) -> u64 {
        self.output_tokens
    }
}
