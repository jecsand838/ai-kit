//! Conversation metadata, accepted prompts, exact file reads and durable interactions.
//!
//! Creation inputs are passive. Live operations use [`ConversationBackend`]; its
//! consuming close permanently terminates the conversation. Prompt streams observe
//! runtime-owned accepted work, so dropping an observer is not cancellation.
//!
//! # Streaming, durable approval and explicit cancellation
//!
//! This complete Custom journey requires a running Ollama server and the exact
//! configured model with function-calling support. It is therefore `no_run`.
//! Replace only the caller configuration to use your intended model; selection
//! does not substitute a fallback. The caller supplies the executable tool.
//! Caller-built actions run in process without Kit sandbox enforcement, so Custom
//! requires `FullAccess`, `Enabled` and read-write roots for this configuration; roots do
//! not confine the action. This example's `inspect_status` action performs no
//! filesystem or network I/O.
//!
//! Notifications are an exclusive, durable, at-least-once delivery path. Inspect
//! an Interaction before resolving its exact ID; advancing passive delivery is
//! acknowledgment, and dropping the receiver is not acknowledgment. The example
//! deliberately denies every requested action: a production UI can instead
//! collect a caller decision and send `Allow` or schema-valid `Submit` content.
//! Denial is a real interaction resolution, not a fabricated tool result.
//!
//! The output observer requires the authoritative `Completed` value. Deltas can
//! include earlier assistant turns around tool execution, so concatenating them
//! is not an alternative final answer. Observer loss does not abort accepted work;
//! the separate cancellation portion uses `abort`, then observes settlement.
//!
//! ```no_run
//! use std::{num::NonZeroUsize, time::Duration};
//! use agents_kit::{AgentBackend, AgentBuilder, AgentStorage, ConversationBackend, WorkspaceBackend};
//! use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
//! use agents_kit::conversation::NewConversation;
//! use agents_kit::notification::{ConversationNotificationKind, NotificationResolution};
//! use agents_kit::prompt::{PromptEvent, PromptRequest};
//! use agents_kit::{model::ModelSelector, tool::ToolSelection};
//! use agents_kit::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
//!     WorkspaceExecutionPolicy, WorkspaceNetworkAccess, WorkspaceRoot,
//!     WorkspaceRootAccess, WorkspaceSandboxMode};
//! use providers_kit::{Provider, ProviderConfig,
//!     config::{OllamaConfig, ProviderKind, TransportConfig}};
//! use storage_kit::{StateStore,
//!     state::config::{LanceStateStoreConfig, StateStoreConfig}};
//! use tools_kit::{Toolset, declaration::{ToolInfo, ToolName}, execution::ToolResult,
//!     local::ToolRegistration, schema::JsonSchema};
//! # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
//! # runtime.block_on(async {
//! let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
//! let tools = Toolset::local(vec![ToolRegistration::function(
//!     ToolInfo::new(ToolName::new("inspect_status")?,
//!         JsonSchema::strict(serde_json::json!({"type": "object", "additionalProperties": false}))),
//!     |_| async { Ok(ToolResult::json(serde_json::json!({"status": "ready"}))) },
//! )])?;
//! let custom = CustomAgentConfig::new(ModelSelector::new("exact-tool-capable-model")?,
//!     ToolSelection::Auto, CustomLoopLimits::new(4, 4, 1)?, CustomToolApproval::Interaction)?;
//! let config = AgentConfig::new(custom.into(), Duration::from_secs(60), NonZeroUsize::MIN)?;
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
//! let agent = AgentBuilder::create(config, AgentStorage::new(store))
//!     .with_provider(provider).with_toolset(tools).build().await?;
//! let outcome = async {
//!     let cwd = std::env::current_dir()?;
//!     let context = WorkspaceContext::new(cwd.clone(),
//!         vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadWrite)?],
//!         WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::FullAccess, WorkspaceNetworkAccess::Enabled))?;
//!     let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
//!     let work = async {
//!         let conversation = workspace.new_conversation(NewConversation::new(None, None, vec![])?).await?;
//!         let work = async {
//!             let mut notifications = conversation.notification_stream()?;
//!             let request = PromptRequest::new("Call inspect_status, then describe its outcome.")?
//!                 .with_tool_selection(ToolSelection::Required)?
//!                 .with_timeout(Duration::from_secs(90))?;
//!             let mut output = conversation.prompt_stream(request).await?;
//!             let completion = tokio::time::timeout(Duration::from_secs(90), async {
//!                 loop {
//!                     tokio::select! {
//!                         event = output.recv() => match event {
//!                             Some(Ok(PromptEvent::OutputDelta { text })) => print!("{text}"),
//!                             Some(Ok(PromptEvent::Completed(response))) => break Ok(response),
//!                             Some(Err(error)) => break Err(error),
//!                             None => break Err(agents_kit::KitError::protocol("completion is missing")),
//!                         },
//!                         event = notifications.recv() => {
//!                             let event = event.ok_or_else(|| agents_kit::KitError::closed("notification stream ended"))??;
//!                             match event.kind() {
//!                                 ConversationNotificationKind::Interaction { title, body, .. } => {
//!                                     println!("Decision requested: {title}: {body}");
//!                                     conversation.resolve_notification(event.id().clone(),
//!                                         NotificationResolution::Deny { reason: "No action approved by this example".into() }).await?;
//!                                 }
//!                                 _ => println!("Observed passive notification {}", event.id()),
//!                             }
//!                         }
//!                     }
//!                 }
//!             }).await.map_err(|_| agents_kit::KitError::timeout("caller observation deadline"))??;
//!             assert!(output.recv().await.is_none());
//!             assert!(!completion.output().trim().is_empty());
//!             println!("Authoritative answer: {}", completion.output());
//!
//!             // Admission succeeds before cancellation. Dropping `output` would not cancel.
//!             let mut cancelled = conversation.prompt_stream(PromptRequest::new("Explain the task in detail.")?
//!                 .with_tool_selection(ToolSelection::None)?
//!                 .with_timeout(Duration::from_secs(90))?).await?;
//!             conversation.abort().await?;
//!             let terminal = tokio::time::timeout(Duration::from_secs(90), async {
//!                 loop {
//!                     match cancelled.recv().await {
//!                         Some(Ok(PromptEvent::OutputDelta { .. })) => {}
//!                         Some(Ok(PromptEvent::Completed(_))) => break Ok(()), // completion may win the race
//!                         Some(Err(error)) if error.kind() == agents_kit::error::ErrorKind::Cancelled => break Ok(()),
//!                         Some(Err(error)) => break Err(error),
//!                         None => break Err(agents_kit::KitError::protocol("cancellation terminal is missing")),
//!                     }
//!                 }
//!             }).await.map_err(|_| agents_kit::KitError::timeout("cancellation observation deadline"))?;
//!             terminal?;
//!             Ok::<(), agents_kit::KitError>(())
//!         }.await;
//!         let close = conversation.close().await; // also settles work after observation failure
//!         match (work, close) {
//!             (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!             (Err(error), _) | (_, Err(error)) => Err(error),
//!             (Ok(()), Ok(())) => Ok(()),
//!         }
//!     }.await;
//!     let close = workspace.close().await; // also runs if conversation creation failed
//!     match (work, close) {
//!         (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!         (Err(error), _) | (_, Err(error)) => Err(error),
//!         (Ok(()), Ok(())) => Ok(()),
//!     }
//! }.await;
//! let shutdown = agent.shutdown().await;
//! match (outcome, shutdown) {
//!     (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!     (Err(error), _) | (_, Err(error)) => Err(error),
//!     (Ok(()), Ok(())) => Ok(()),
//! }?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
use std::future::Future;
use std::path::PathBuf;
use tools_kit::ToolsetBackend;

use providers_kit::ProviderBackend;
use storage_kit::{DatabaseBackend, StateStoreBackend};

pub use crate::file::FileRead;

use crate::backends::{codex, custom, opencode};
use crate::prompt::PromptStream;
use crate::{
    ConversationId, ConversationNotificationReceiver, KitError, KnowledgeSource, NotificationId,
    NotificationResolution, PromptRequest, PromptResponse, WorkspaceDefaults, WorkspaceId,
};

/// Descriptive state for one portable conversation.
///
/// # Examples
///
/// ```
/// use agents_kit::conversation::ConversationInfo;
/// let id = "conversation_00000000000000000000000000000000".parse()?;
/// let workspace = "workspace_00000000000000000000000000000000".parse()?;
/// let info = ConversationInfo::new(id, workspace, Some("Review".into()))?;
/// assert_eq!(info.title(), Some("Review"));
/// # Ok::<(), agents_kit::KitError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversationInfo {
    id: ConversationId,
    workspace_id: WorkspaceId,
    title: Option<String>,
}

impl ConversationInfo {
    /// Validates and constructs descriptive conversation information.
    ///
    /// Titles, when present, must be nonempty and must not contain leading or
    /// trailing whitespace.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorKind::InvalidInput`] when `title` violates the portable
    /// naming contract.
    ///
    /// # Examples
    ///
    /// ```
    /// use agents_kit::{
    ///     conversation::ConversationInfo,
    ///     identity::{ConversationId, WorkspaceId},
    /// };
    ///
    /// let id: ConversationId =
    ///     "conversation_00000000000000000000000000000000".parse()?;
    /// let workspace_id: WorkspaceId =
    ///     "workspace_00000000000000000000000000000000".parse()?;
    /// let info =
    ///     ConversationInfo::new(id, workspace_id, Some("release".to_owned()))?;
    /// assert_eq!(info.title(), Some("release"));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(
        id: ConversationId,
        workspace_id: WorkspaceId,
        title: Option<String>,
    ) -> Result<Self, KitError> {
        Self::validate_title(title.as_deref())?;
        Ok(Self {
            id,
            workspace_id,
            title,
        })
    }

    /// Returns the portable conversation identifier.
    ///
    /// See [`ConversationInfo`] for a complete example.
    pub fn id(&self) -> &ConversationId {
        &self.id
    }

    /// Returns the portable identifier of the owning workspace.
    ///
    /// See [`ConversationInfo`] for a complete example.
    pub fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the configured title, when present.
    ///
    /// See [`ConversationInfo`] for a complete example.
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    fn validate_title(title: Option<&str>) -> Result<(), KitError> {
        if title.is_some_and(|value| value.is_empty() || value.trim() != value) {
            return Err(KitError::invalid_input(
                "conversation title must be nonempty and unpadded when present",
            ));
        }
        Ok(())
    }
}

/// Validated inputs for creating a conversation.
///
/// # Examples
///
/// ```
/// use agents_kit::conversation::NewConversation;
///
/// let request = NewConversation::new(
///     Some("incident review".to_owned()),
///     None,
///     Vec::new(),
/// )?;
/// assert_eq!(request.title(), Some("incident review"));
/// # Ok::<(), agents_kit::KitError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewConversation {
    title: Option<String>,
    defaults: Option<WorkspaceDefaults>,
    knowledge_sources: Vec<KnowledgeSource>,
}

impl NewConversation {
    /// Validates and constructs conversation-creation inputs.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an empty or padded title, an invalid knowledge policy/source, or duplicate source IDs. Construction performs no native I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use agents_kit::conversation::NewConversation;
    /// let request = NewConversation::new(Some("Review".into()), None, Vec::new())?;
    /// assert_eq!(request.title(), Some("Review"));
    /// assert!(request.defaults().is_none());
    /// assert!(request.knowledge_sources().is_empty());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(
        title: Option<String>,
        defaults: Option<WorkspaceDefaults>,
        knowledge_sources: Vec<KnowledgeSource>,
    ) -> Result<Self, KitError> {
        ConversationInfo::validate_title(title.as_deref())?;
        if let Some(defaults) = &defaults
            && let Some(policy) = defaults.knowledge_policy()
        {
            policy.validate()?;
        }
        let mut ids = std::collections::BTreeSet::new();
        for source in &knowledge_sources {
            source.validate()?;
            if !ids.insert(source.source_id()) {
                return Err(KitError::invalid_input(
                    "conversation knowledge source ids must be unique",
                ));
            }
        }
        Ok(Self {
            title,
            defaults,
            knowledge_sources,
        })
    }

    /// Returns the requested title, when present.
    ///
    /// See [`NewConversation`] for a complete example.
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns conversation-level default overrides, when present.
    ///
    /// See [`NewConversation`] for a complete example.
    pub fn defaults(&self) -> Option<&WorkspaceDefaults> {
        self.defaults.as_ref()
    }

    /// Returns the initial managed-knowledge sources.
    ///
    /// See [`NewConversation`] for a complete example.
    pub fn knowledge_sources(&self) -> &[KnowledgeSource] {
        &self.knowledge_sources
    }
}

/// Public implementation contract for one live portable conversation.
///
/// A caller workspace returns its own concrete conversation through
/// [`WorkspaceBackend::Conversation`](crate::WorkspaceBackend::Conversation).
/// Prompting reaches a terminal portable result, abort joins active work, and close
/// permanently closes the durable conversation. Runtime composition may use an
/// exhaustive caller-owned enum hierarchy.
///
/// # Runnable caller example
///
/// This small test double accepts one prompt and one file path. It demonstrates
/// portable consumption without a service or database; it is not a durable agent.
/// Notification and interaction methods return explicit Unsupported errors;
/// request assertions make the fixture's expected inputs visible. The streaming
/// response is caller-authored, not fabricated deltas from a supplied backend.
///
/// ```
/// use std::path::{Path, PathBuf};
/// use agents_kit::{ConversationBackend, KitError};
/// use agents_kit::conversation::{ConversationInfo, FileRead};
/// use agents_kit::identity::{ConversationId, NotificationId, WorkspaceId};
/// use agents_kit::notification::{ConversationNotificationReceiver, NotificationResolution};
/// use agents_kit::prompt::{PromptEvent, PromptRequest, PromptResponse, PromptStream};
///
/// struct ScriptedConversation {
///     id: ConversationId,
///     workspace: WorkspaceId,
/// }
/// impl ConversationBackend for ScriptedConversation {
///     fn id(&self) -> ConversationId { self.id.clone() }
///     async fn info(&self) -> Result<ConversationInfo, KitError> {
///         ConversationInfo::new(self.id(), self.workspace.clone(), Some("Example".into()))
///     }
///     async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, KitError> {
///         assert_eq!(request.text(), "Status?");
///         Ok(PromptResponse::new("ready".into(), None))
///     }
///     async fn prompt_stream(&self, request: PromptRequest) -> Result<PromptStream, KitError> {
///         assert_eq!(request.text(), "Status?");
///         Ok(PromptStream::from_stream(futures_util::stream::iter([
///             Ok(PromptEvent::OutputDelta { text: "ready".into() }),
///             Ok(PromptEvent::Completed(PromptResponse::new("ready".into(), None))),
///         ])))
///     }
///     async fn read_file(&self, path: PathBuf) -> Result<FileRead, KitError> {
///         assert_eq!(path, Path::new("status.txt"));
///         FileRead::from_bytes(path, b"ready\n".to_vec())
///     }
///     // No background work or durable interaction exists in this fixture.
///     async fn abort(&self) -> Result<(), KitError> { Ok(()) }
///     fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
///         Err(KitError::unsupported("this fixture has no notifications"))
///     }
///     async fn resolve_notification(&self, _: NotificationId, _: NotificationResolution)
///         -> Result<(), KitError> {
///         Err(KitError::unsupported("this fixture has no interactions"))
///     }
///     async fn close(self) -> Result<(), KitError> { Ok(()) }
/// }
/// # tokio::runtime::Builder::new_current_thread().build()?.block_on(async {
/// let conversation = ScriptedConversation {
///     id: "conversation_00000000000000000000000000000001".parse()?,
///     workspace: "workspace_00000000000000000000000000000001".parse()?,
/// };
/// let outcome = async {
///     assert_eq!(conversation.info().await?.id(), &conversation.id());
///     assert_eq!(conversation.prompt(PromptRequest::new("Status?")?).await?.output(), "ready");
///     let mut output = conversation.prompt_stream(PromptRequest::new("Status?")?).await?;
///     assert!(matches!(output.recv().await.transpose()?,
///         Some(PromptEvent::OutputDelta { text }) if text == "ready"));
///     assert!(matches!(output.recv().await.transpose()?,
///         Some(PromptEvent::Completed(response)) if response.output() == "ready"));
///     assert!(output.recv().await.is_none());
///     assert!(output.recv().await.is_none());
///     assert_eq!(conversation.read_file("status.txt".into()).await?.text(), Some("ready\n"));
///     conversation.abort().await?;
///     Ok::<(), KitError>(())
/// }.await;
/// let cleanup = conversation.close().await;
/// match (outcome, cleanup) {
///     (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
///     (Err(error), _) | (_, Err(error)) => Err(error),
///     (Ok(()), Ok(())) => Ok(()),
/// }
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait ConversationBackend: Send + Sync + 'static {
    /// Returns the durable portable conversation identity.
    ///
    /// See the runnable caller implementation on [`ConversationBackend`]. Supplied backends retain their own runtime and storage prerequisites.
    fn id(&self) -> ConversationId;

    /// Reads the current descriptive conversation state.
    ///
    /// # Errors
    ///
    /// Returns `Closed` for a terminal handle, `Conflict` for stale ownership/revision, or a typed storage/backend failure.
    ///
    /// See the runnable caller implementation on [`ConversationBackend`]. Supplied backends retain their own runtime and storage prerequisites.
    fn info(&self) -> impl Future<Output = Result<ConversationInfo, KitError>> + Send + '_;

    /// Runs one foreground prompt through its terminal portable result.
    ///
    /// Unary and incremental calls share one accepted prompt owner. Dropping the waiter does not cancel accepted work or free its slot. Success retains exact authored text; structured output is separately validated and usage is present only when sourceable.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput`/`Unsupported` before acceptance for invalid or unsupported requests, `Busy` when admission is occupied, or typed cancellation, deadline, conflict, storage, and backend failures. After ambiguous acceptance, do not blindly retry: cancellation/reconciliation retains the generation fence until settlement.
    ///
    /// See the runnable caller implementation on [`ConversationBackend`]. Supplied backends retain their own runtime and storage prerequisites.
    fn prompt(
        &self,
        request: PromptRequest,
    ) -> impl Future<Output = Result<PromptResponse, KitError>> + Send + '_;

    /// Starts real incremental observation of one foreground prompt.
    ///
    /// The default rejects unsupported streaming before accepting work. Override
    /// using public [`PromptStream`] and [`PromptEvent`](crate::prompt::PromptEvent)
    /// values; do not fabricate deltas from a terminal response. Dropping an
    /// observer does not cancel accepted work or release the conversation slot.
    ///
    /// # Errors
    ///
    /// Returns `Unsupported` when the backend has no genuine incremental route, or the same admission failures as `prompt`. After admission, the stream carries terminal failures; premature source EOF is a protocol failure.
    ///
    /// The runnable [`ConversationBackend`] example consumes real caller-stream events. [`PromptStream`] also demonstrates terminal validation; the [native interaction recipe](crate::conversation) requires a configured service.
    fn prompt_stream(
        &self,
        _request: PromptRequest,
    ) -> impl Future<Output = Result<PromptStream, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                "incremental prompt output is unavailable",
            ))
        }
    }

    /// Aborts the active prompt, or succeeds when no prompt is active.
    ///
    /// Cancellation targets only the active generation and joins its settlement. Natural completion may win; abort does not replace the prompt waiter or return its answer.
    ///
    /// # Errors
    ///
    /// Returns a typed cancellation, timeout, conflict, lifecycle, or backend failure. Ambiguous cancellation retains recovery ownership; an error does not prove remote work stopped.
    ///
    /// See the runnable caller implementation on [`ConversationBackend`]. Supplied backends retain their own runtime and storage prerequisites.
    fn abort(&self) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    /// Reads a validated workspace-relative file through the implementation.
    ///
    /// The path is interpreted against the admitted workspace cwd on the backend host. Absolute paths, dot/parent components and repeated separators are rejected rather than normalized.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for a nonportable relative path, `Unsupported` for a route unable to preserve exact bytes/policy, or a typed revision, lifecycle, storage, or native read failure. An untyped native rejection is not evidence of `NotFound`.
    ///
    /// See the runnable caller implementation on [`ConversationBackend`]. Supplied backends retain their own runtime and storage prerequisites.
    fn read_file(
        &self,
        path: PathBuf,
    ) -> impl Future<Output = Result<FileRead, KitError>> + Send + '_;

    /// Claims the conversation's exclusive durable notification receiver.
    ///
    /// # Errors
    ///
    /// Returns `Busy` while another receiver owns the lease, `Closed` for a closed handle, or a typed admission failure. Later durable delivery failures are yielded by `recv`.
    ///
    /// See [`ConversationNotificationReceiver`] for executable caller-stream delivery, and the [native interaction recipe](crate::conversation) for durable delivery.
    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError>;

    /// Submits a resolution for an unresolved portable notification.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an invalid resolution, `NotFound` for an unknown notification, or typed lifecycle, conflict, storage, and native resolution failures. Intent is durable before dispatch; known or ambiguous acceptance is never blindly resent.
    ///
    /// See [`NotificationResolution`] for resolution values and the [native interaction recipe](crate::conversation) for resolving an admitted interaction. Caller implementations are exercised by the [direct hierarchy tests](https://github.com/jecsand838/ai-kit/blob/main/agents-kit/tests/mock_hierarchy/mod.rs) without a service.
    fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    /// Permanently closes the conversation and consumes the implementation.
    ///
    /// Unlike parent workspace close or Agent shutdown, this permanently closes the durable conversation. Admission is sealed and active work, interactions, knowledge and owned resources are settled before completion.
    ///
    /// # Errors
    ///
    /// Returns a typed cancellation, reconciliation, storage, or cleanup failure. The handle remains consumed on failure; durable recovery obligations remain authoritative.
    ///
    /// See the runnable caller implementation on [`ConversationBackend`]. Supplied backends retain their own runtime and storage prerequisites.
    fn close(self) -> impl Future<Output = Result<(), KitError>> + Send
    where
        Self: Sized;
}

/// Opaque live handle for one portable conversation.
///
/// See [`ConversationBackend`] and the [native interaction recipe](crate::conversation) for usage.
pub struct Conversation<
    S: StateStoreBackend = storage_kit::StateStore,
    D: DatabaseBackend = storage_kit::Database,
    P: ProviderBackend = providers_kit::Provider,
    T: ToolsetBackend = tools_kit::Toolset,
> {
    inner: ConversationInner<S, D, P, T>,
}

enum ConversationInner<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    Codex(codex::conversations::CodexConversation<S, D, T>),
    OpenCode(opencode::conversations::OpenCodeConversation<S, D>),
    Custom(custom::CustomConversation<S, D, P, T>),
}

impl<S, D, P, T> Conversation<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    pub(crate) fn codex(backend: codex::conversations::CodexConversation<S, D, T>) -> Self {
        Self {
            inner: ConversationInner::Codex(backend),
        }
    }

    pub(crate) fn opencode(backend: opencode::conversations::OpenCodeConversation<S, D>) -> Self {
        Self {
            inner: ConversationInner::OpenCode(backend),
        }
    }

    pub(crate) fn custom(backend: custom::CustomConversation<S, D, P, T>) -> Self {
        Self {
            inner: ConversationInner::Custom(backend),
        }
    }
}

impl<S, D, P, T> ConversationBackend for ConversationInner<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    fn id(&self) -> ConversationId {
        match self {
            ConversationInner::Codex(backend) => backend.id(),
            ConversationInner::OpenCode(backend) => backend.id(),
            ConversationInner::Custom(backend) => backend.id(),
        }
    }

    async fn info(&self) -> Result<ConversationInfo, KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.info().await,
            ConversationInner::OpenCode(backend) => backend.info().await,
            ConversationInner::Custom(backend) => backend.info().await,
        }
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.prompt(request).await,
            ConversationInner::OpenCode(backend) => backend.prompt(request).await,
            ConversationInner::Custom(backend) => backend.prompt(request).await,
        }
    }

    async fn prompt_stream(&self, request: PromptRequest) -> Result<PromptStream, KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.prompt_stream(request).await,
            ConversationInner::OpenCode(backend) => backend.prompt_stream(request).await,
            ConversationInner::Custom(backend) => backend.prompt_stream(request).await,
        }
    }

    async fn abort(&self) -> Result<(), KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.abort().await,
            ConversationInner::OpenCode(backend) => backend.abort().await,
            ConversationInner::Custom(backend) => backend.abort().await,
        }
    }

    async fn read_file(&self, path: PathBuf) -> Result<FileRead, KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.read_file(path).await,
            ConversationInner::OpenCode(backend) => backend.read_file(path).await,
            ConversationInner::Custom(backend) => backend.read_file(path).await,
        }
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.notification_stream(),
            ConversationInner::OpenCode(backend) => backend.notification_stream(),
            ConversationInner::Custom(backend) => backend.notification_stream(),
        }
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.resolve_notification(id, resolution).await,
            ConversationInner::OpenCode(backend) => {
                backend.resolve_notification(id, resolution).await
            }
            ConversationInner::Custom(backend) => {
                backend.resolve_notification(id, resolution).await
            }
        }
    }

    async fn close(self) -> Result<(), KitError> {
        match self {
            ConversationInner::Codex(backend) => backend.close().await,
            ConversationInner::OpenCode(backend) => backend.close().await,
            ConversationInner::Custom(backend) => backend.close().await,
        }
    }
}

impl<S, D, P, T> ConversationBackend for Conversation<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    fn id(&self) -> ConversationId {
        self.inner.id()
    }

    async fn info(&self) -> Result<ConversationInfo, KitError> {
        self.inner.info().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::ConversationInfo)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, KitError> {
        self.inner.prompt(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::Prompt)
                .in_phase(crate::error::ErrorPhase::ModelGeneration)
        })
    }

    async fn prompt_stream(&self, request: PromptRequest) -> Result<PromptStream, KitError> {
        self.inner.prompt_stream(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::Prompt)
                .in_phase(crate::error::ErrorPhase::ModelGeneration)
        })
    }

    async fn abort(&self) -> Result<(), KitError> {
        self.inner.abort().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::Abort)
                .in_phase(crate::error::ErrorPhase::Cancellation)
        })
    }

    async fn read_file(&self, path: PathBuf) -> Result<FileRead, KitError> {
        self.inner.read_file(path).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::ReadFile)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        self.inner.notification_stream().map_err(|error| {
            error
                .at_operation(crate::error::Operation::NotificationStream)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        self.inner
            .resolve_notification(id, resolution)
            .await
            .map_err(|error| {
                error
                    .at_operation(crate::error::Operation::ResolveNotification)
                    .in_phase(crate::error::ErrorPhase::InteractionResolution)
            })
    }

    async fn close(self) -> Result<(), KitError> {
        self.inner.close().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::CloseConversation)
                .in_phase(crate::error::ErrorPhase::Lifecycle)
        })
    }
}
