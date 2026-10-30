//! Inputs for privately managed knowledge ingestion and retrieval policy.
//!
//! These values carry caller-authored sources and scope choices, not retrieval
//! internals or Database adapters. Nonempty ingestion atomically replaces the
//! addressed source set. An empty request is a **no-op after scope validation**;
//! it is not a deletion request. The receiving operation validates which scopes
//! it owns and whether the selected backend supports managed knowledge.
//!
//! Retrieval is private and happens during a prompt. [`KnowledgePolicy::only`]
//! narrows eligible owner scopes; it does not promise that a model will quote
//! every source or follow requested wording. Database scope is shared only when
//! callers deliberately supply the same Database domain. Agent and Workspace
//! shutdown preserve sources for reopen; Conversation close removes its owned
//! sources. Reopen must receive a compatible Database as well as control-plane
//! storage; missing or mismatched persisted knowledge is an error.
//!
//! # Local ingestion and scope validation
//!
//! This executes ingestion with embedded Lance StateStore and memory LanceDB
//! Database, without inference or an Ollama server. These are separate products:
//! the StateStore holds control-plane records; the Database holds source content.
//! The request carries sources, never private retrieval context or an adapter.
//!
//! ```standalone_crate
//! #![recursion_limit = "256"] // Nested concrete async futures need this on Linux.
//! use std::{num::NonZeroUsize, time::Duration};
//! use agents_kit::{AgentBackend, AgentBuilder, AgentStorage};
//! use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
//! use agents_kit::knowledge::{IngestKnowledgeRequest, KnowledgePolicy, KnowledgeScope, KnowledgeSource};
//! use agents_kit::{model::ModelSelector, tool::ToolSelection};
//! use providers_kit::{Provider, ProviderConfig,
//!     config::{OllamaConfig, ProviderKind, TransportConfig}};
//! use storage_kit::{Database, StateStore,
//!     database::config::{DatabaseConfig, LanceConfig},
//!     state::config::{LanceStateStoreConfig, StateStoreConfig}};
//! # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
//! # runtime.block_on(async {
//! let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
//! let database = Database::connect(DatabaseConfig::lance(
//!     LanceConfig::embedded_path("memory://"))).await?;
//! let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?,
//!     ToolSelection::None, CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
//! let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
//! let agent = AgentBuilder::create(config, AgentStorage::new(store))
//!     .with_provider(provider).with_knowledge_database(database).build().await?;
//! let outcome = async {
//!     agent.ingest_knowledge(IngestKnowledgeRequest::new(vec![
//!         KnowledgeSource::text("runbook", "Incident runbook", "Escalate service incidents.")?,
//!     ])?.with_scope(KnowledgeScope::Agent)).await?;
//!     agent.ingest_knowledge(IngestKnowledgeRequest::new(vec![])?
//!         .with_scope(KnowledgeScope::Agent)).await?; // retains the runbook
//!     let invalid = agent.ingest_knowledge(IngestKnowledgeRequest::new(vec![])?
//!         .with_scope(KnowledgeScope::Conversation)).await.unwrap_err();
//!     assert_eq!(invalid.kind(), agents_kit::error::ErrorKind::InvalidInput);
//!     let request = agents_kit::prompt::PromptRequest::new("How are incidents escalated?")?
//!         .with_knowledge_policy(KnowledgePolicy::only(vec![KnowledgeScope::Agent])?)?;
//!     assert_eq!(request.knowledge_policy(),
//!         Some(&KnowledgePolicy::Only(vec![KnowledgeScope::Agent])));
//!     // Sending this request through a Conversation requires the configured model.
//!     Ok::<(), agents_kit::KitError>(())
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

use std::collections::{BTreeMap, BTreeSet};

use crate::{AgentId, ConversationId, KitError, WorkspaceId};

const RETRIEVAL_SCOPE_ORDER: [KnowledgeScope; 4] = [
    KnowledgeScope::Conversation,
    KnowledgeScope::Workspace,
    KnowledgeScope::Agent,
    KnowledgeScope::Database,
];

/// Complete portable lineage for one semantic-knowledge owner.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub(crate) enum KnowledgeOwnerId {
    /// Shared data in the caller-supplied managed Database domain.
    Database,
    /// Data owned by one agent.
    Agent {
        /// Owning durable agent identity.
        agent_id: AgentId,
    },
    /// Data owned by one workspace and its agent.
    Workspace {
        /// Owning durable agent identity.
        agent_id: AgentId,
        /// Owning durable workspace identity.
        workspace_id: WorkspaceId,
    },
    /// Data owned by one conversation, workspace, and agent.
    Conversation {
        /// Owning durable agent identity.
        agent_id: AgentId,
        /// Owning durable workspace identity.
        workspace_id: WorkspaceId,
        /// Owning durable conversation identity.
        conversation_id: ConversationId,
    },
}

/// Positive limits for one structured knowledge response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KnowledgeBudget {
    chunks: usize,
    bytes: usize,
}

/// Validated semantic retrieval input.
#[derive(Clone, Debug)]
pub(crate) struct KnowledgeRetrievalRequest {
    query: String,
    owner: KnowledgeOwnerId,
    policy: KnowledgePolicy,
    budget: KnowledgeBudget,
}

/// Structured semantic context returned by a knowledge implementation.
#[derive(Clone, Debug)]
pub(crate) struct KnowledgeContext {
    snippets: Vec<KnowledgeSnippet>,
}

/// One ranked piece of structured semantic context.
#[derive(Clone, Debug)]
pub(crate) struct KnowledgeSnippet {
    source_id: String,
    title: String,
    text: String,
    owner: KnowledgeOwnerId,
    relevance_score: Option<f64>,
    provenance: KnowledgeProvenance,
    metadata: BTreeMap<String, serde_json::Value>,
}

/// Optional source location and structured provenance metadata.
#[derive(Clone, Debug, Default)]
pub(crate) struct KnowledgeProvenance {
    locator: Option<String>,
    metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
/// A portable ownership scope from which managed knowledge may be retrieved.
pub enum KnowledgeScope {
    /// Sources shared through the caller-supplied managed Database domain.
    Database,
    /// Sources owned by the agent.
    Agent,
    /// Sources owned by the current workspace.
    Workspace,
    /// Sources owned by the current conversation.
    Conversation,
}

/// Retrieval policy for managed knowledge.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgePolicy {
    /// Applies normal inherited retrieval behavior.
    Default,
    /// Excludes managed-knowledge retrieval.
    Disabled,
    /// Retrieves only from the selected ownership scopes.
    Only(
        /// A nonempty selection without duplicate scopes.
        Vec<KnowledgeScope>,
    ),
}

impl KnowledgePolicy {
    /// Validates and constructs an exact scope selection.
    ///
    /// # Errors
    /// Returns InvalidInput for no scopes or duplicate scopes.
    ///
    /// ```
    /// use agents_kit::knowledge::{KnowledgePolicy, KnowledgeScope};
    /// let policy = KnowledgePolicy::only(vec![KnowledgeScope::Workspace])?;
    /// assert_eq!(policy, KnowledgePolicy::Only(vec![KnowledgeScope::Workspace]));
    /// assert!(KnowledgePolicy::only(vec![]).is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn only(scopes: Vec<KnowledgeScope>) -> Result<Self, KitError> {
        let policy = Self::Only(scopes);
        policy.validate()?;
        Ok(policy)
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        let Self::Only(scopes) = self else {
            return Ok(());
        };
        if scopes.is_empty() {
            return Err(KitError::invalid_input(
                "knowledge scope selection must not be empty",
            ));
        }
        let mut sorted = scopes.clone();
        sorted.sort();
        sorted.dedup();
        if sorted.len() != scopes.len() {
            return Err(KitError::invalid_input(
                "knowledge scope selection must not contain duplicates",
            ));
        }
        Ok(())
    }
}

/// Caller-authored content for managed-knowledge ingestion.
///
/// Prefer [`KnowledgeSource::text`] and [`KnowledgeSource::bytes`] so source
/// identity and content are validated at construction.
///
/// # Examples
///
/// ```
/// use agents_kit::knowledge::{IngestKnowledgeRequest, KnowledgeSource};
///
/// let source = KnowledgeSource::text(
///     "operations-handbook",
///     "Operations handbook",
///     "Escalate service incidents.",
/// )?;
/// let request = IngestKnowledgeRequest::new(vec![source])?;
/// assert_eq!(request.sources()[0].source_id(), "operations-handbook");
/// # Ok::<(), agents_kit::KitError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KnowledgeSource {
    /// UTF-8 source content.
    Text {
        /// Stable caller-owned source identity.
        source_id: String,
        /// Human-readable source title.
        title: String,
        /// Nonempty UTF-8 source content.
        text: String,
    },
    /// Binary source content with an explicit MIME type.
    Bytes {
        /// Stable caller-owned source identity.
        source_id: String,
        /// Human-readable source title.
        title: String,
        /// Nonempty exact source bytes.
        bytes: Vec<u8>,
        /// Nonempty unpadded MIME type.
        mime_type: String,
    },
}

impl KnowledgeSource {
    /// Validates and constructs a text source.
    ///
    /// # Errors
    /// Returns InvalidInput for empty content, or empty/padded source identity or title. Text content itself is preserved exactly.
    ///
    /// ```
    /// use agents_kit::knowledge::KnowledgeSource;
    /// let source = KnowledgeSource::text("guide", "Guide", " Keep this spacing. ")?;
    /// assert_eq!(source.source_id(), "guide");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn text(
        source_id: impl Into<String>,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Self, KitError> {
        let source = Self::Text {
            source_id: source_id.into(),
            title: title.into(),
            text: text.into(),
        };
        source.validate()?;
        Ok(source)
    }

    /// Validates and constructs a binary source.
    ///
    /// # Errors
    /// Returns InvalidInput for empty bytes, or empty/padded source identity, title, or MIME type.
    ///
    /// ```
    /// use agents_kit::knowledge::KnowledgeSource;
    /// let source = KnowledgeSource::bytes("file", "File", vec![1, 2], "application/octet-stream")?;
    /// assert_eq!(source.title(), "File");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn bytes(
        source_id: impl Into<String>,
        title: impl Into<String>,
        bytes: Vec<u8>,
        mime_type: impl Into<String>,
    ) -> Result<Self, KitError> {
        let source = Self::Bytes {
            source_id: source_id.into(),
            title: title.into(),
            bytes,
            mime_type: mime_type.into(),
        };
        source.validate()?;
        Ok(source)
    }

    /// Returns the stable caller-owned source identity.
    ///
    /// ```
    /// use agents_kit::knowledge::KnowledgeSource;
    /// let source = KnowledgeSource::text("guide", "Guide", "contents")?;
    /// assert_eq!(source.source_id(), "guide");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn source_id(&self) -> &str {
        match self {
            Self::Text { source_id, .. } | Self::Bytes { source_id, .. } => source_id,
        }
    }

    /// Returns the human-readable title.
    ///
    /// ```
    /// use agents_kit::knowledge::KnowledgeSource;
    /// let source = KnowledgeSource::text("guide", "Guide", "contents")?;
    /// assert_eq!(source.title(), "Guide");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn title(&self) -> &str {
        match self {
            Self::Text { title, .. } | Self::Bytes { title, .. } => title,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        let (source_id, title) = match self {
            Self::Text {
                source_id,
                title,
                text,
            } => {
                if text.is_empty() {
                    return Err(KitError::invalid_input("knowledge text must not be empty"));
                }
                (source_id, title)
            }
            Self::Bytes {
                source_id,
                title,
                bytes,
                mime_type,
            } => {
                if bytes.is_empty() {
                    return Err(KitError::invalid_input("knowledge bytes must not be empty"));
                }
                Self::validate_text("knowledge MIME type", mime_type)?;
                (source_id, title)
            }
        };
        Self::validate_text("knowledge source id", source_id)?;
        Self::validate_text("knowledge source title", title)
    }

    fn validate_text(label: &str, value: &str) -> Result<(), KitError> {
        if value.is_empty() || value.trim() != value {
            return Err(KitError::invalid_input(format!(
                "{label} must be nonempty and unpadded"
            )));
        }
        Ok(())
    }
}

/// Validated source set for one managed-knowledge ingestion operation.
///
/// ```
/// use agents_kit::knowledge::{IngestKnowledgeRequest, KnowledgeSource};
/// let request = IngestKnowledgeRequest::new(vec![
///     KnowledgeSource::text("guide", "Guide", "Escalate incidents.")?,
/// ])?;
/// assert_eq!(request.sources().len(), 1);
/// # Ok::<(), agents_kit::KitError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestKnowledgeRequest {
    sources: Vec<KnowledgeSource>,
    scope: Option<KnowledgeScope>,
}

impl IngestKnowledgeRequest {
    /// Validates sources and rejects duplicate source identities.
    ///
    /// # Errors
    /// Returns InvalidInput for invalid sources or repeated source identities. An empty source set is valid and is a no-op after the receiving operation validates the selected scope; it never requests deletion.
    ///
    /// ```
    /// use agents_kit::knowledge::IngestKnowledgeRequest;
    /// let request = IngestKnowledgeRequest::new(vec![])?;
    /// assert!(request.sources().is_empty());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(sources: Vec<KnowledgeSource>) -> Result<Self, KitError> {
        let request = Self {
            sources,
            scope: None,
        };
        request.validate()?;
        Ok(request)
    }

    /// Selects an explicit scope instead of the receiving operation's default.
    #[must_use]
    ///
    /// ```
    /// use agents_kit::knowledge::{IngestKnowledgeRequest, KnowledgeScope};
    /// let request = IngestKnowledgeRequest::new(vec![])?.with_scope(KnowledgeScope::Workspace);
    /// assert_eq!(request.scope(), Some(KnowledgeScope::Workspace));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_scope(mut self, scope: KnowledgeScope) -> Self {
        self.scope = Some(scope);
        self
    }

    /// Borrows the validated sources in caller-provided order.
    ///
    /// ```
    /// use agents_kit::knowledge::{IngestKnowledgeRequest, KnowledgeSource};
    /// let request = IngestKnowledgeRequest::new(vec![KnowledgeSource::text("guide", "Guide", "contents")?])?;
    /// assert_eq!(request.sources()[0].source_id(), "guide");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn sources(&self) -> &[KnowledgeSource] {
        &self.sources
    }

    /// Returns the explicitly selected scope, when present.
    #[must_use]
    ///
    /// ```
    /// use agents_kit::knowledge::IngestKnowledgeRequest;
    /// assert_eq!(IngestKnowledgeRequest::new(vec![])?.scope(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn scope(&self) -> Option<KnowledgeScope> {
        self.scope
    }

    /// Consumes the request into validated sources and optional scope.
    #[must_use]
    ///
    /// ```
    /// use agents_kit::knowledge::IngestKnowledgeRequest;
    /// let (sources, scope) = IngestKnowledgeRequest::new(vec![])?.into_parts();
    /// assert!(sources.is_empty());
    /// assert_eq!(scope, None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn into_parts(self) -> (Vec<KnowledgeSource>, Option<KnowledgeScope>) {
        (self.sources, self.scope)
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        let mut ids = std::collections::BTreeSet::new();
        for source in &self.sources {
            source.validate()?;
            if !ids.insert(source.source_id()) {
                return Err(KitError::invalid_input(
                    "knowledge source ids must be unique within one request",
                ));
            }
        }
        Ok(())
    }
}

impl KnowledgeOwnerId {
    fn scope(&self) -> KnowledgeScope {
        match self {
            Self::Database => KnowledgeScope::Database,
            Self::Agent { .. } => KnowledgeScope::Agent,
            Self::Workspace { .. } => KnowledgeScope::Workspace,
            Self::Conversation { .. } => KnowledgeScope::Conversation,
        }
    }

    fn owner_for_scope(&self, scope: KnowledgeScope) -> Option<Self> {
        match (self, scope) {
            (Self::Database, KnowledgeScope::Database)
            | (Self::Agent { .. }, KnowledgeScope::Database)
            | (Self::Workspace { .. }, KnowledgeScope::Database)
            | (Self::Conversation { .. }, KnowledgeScope::Database) => Some(Self::Database),
            (Self::Agent { agent_id }, KnowledgeScope::Agent)
            | (Self::Workspace { agent_id, .. }, KnowledgeScope::Agent)
            | (Self::Conversation { agent_id, .. }, KnowledgeScope::Agent) => Some(Self::Agent {
                agent_id: agent_id.clone(),
            }),
            (
                Self::Workspace {
                    agent_id,
                    workspace_id,
                },
                KnowledgeScope::Workspace,
            )
            | (
                Self::Conversation {
                    agent_id,
                    workspace_id,
                    ..
                },
                KnowledgeScope::Workspace,
            ) => Some(Self::Workspace {
                agent_id: agent_id.clone(),
                workspace_id: workspace_id.clone(),
            }),
            (
                Self::Conversation {
                    agent_id,
                    workspace_id,
                    conversation_id,
                },
                KnowledgeScope::Conversation,
            ) => Some(Self::Conversation {
                agent_id: agent_id.clone(),
                workspace_id: workspace_id.clone(),
                conversation_id: conversation_id.clone(),
            }),
            _ => None,
        }
    }

    pub(crate) fn ingestion_owner(
        &self,
        selected: Option<KnowledgeScope>,
    ) -> Result<Self, KitError> {
        self.owner_for_scope(selected.unwrap_or_else(|| self.scope()))
            .ok_or_else(|| {
                KitError::invalid_input(
                    "selected knowledge scope is not applicable to this receiver",
                )
            })
    }

    pub(crate) fn retrieval_owners(&self, policy: &KnowledgePolicy) -> Result<Vec<Self>, KitError> {
        policy.validate()?;
        let selected = match policy {
            KnowledgePolicy::Default => None,
            KnowledgePolicy::Disabled => return Ok(Vec::new()),
            KnowledgePolicy::Only(scopes) => {
                for scope in scopes {
                    if self.owner_for_scope(*scope).is_none() {
                        return Err(KitError::invalid_input(
                            "selected knowledge scope is not applicable to this owner",
                        ));
                    }
                }
                Some(scopes.iter().copied().collect::<BTreeSet<_>>())
            }
        };
        Ok(RETRIEVAL_SCOPE_ORDER
            .into_iter()
            .filter(|scope| {
                selected
                    .as_ref()
                    .is_none_or(|selected| selected.contains(scope))
            })
            .filter_map(|scope| self.owner_for_scope(scope))
            .collect())
    }
}

impl KnowledgeBudget {
    /// Constructs positive chunk and UTF-8 text-byte limits.
    pub(crate) fn new(chunks: usize, bytes: usize) -> Result<Self, KitError> {
        if chunks == 0 || bytes == 0 {
            return Err(KitError::invalid_input(
                "knowledge chunk and byte budgets must be positive",
            ));
        }
        Ok(Self { chunks, bytes })
    }

    /// Returns the maximum number of snippets.
    #[must_use]
    pub(crate) fn chunks(&self) -> usize {
        self.chunks
    }

    /// Returns the maximum aggregate snippet-text bytes.
    #[must_use]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl KnowledgeRetrievalRequest {
    /// Validates and constructs one semantic retrieval request.
    pub(crate) fn new(
        query: String,
        owner: KnowledgeOwnerId,
        policy: KnowledgePolicy,
        budget: KnowledgeBudget,
    ) -> Result<Self, KitError> {
        let request = Self {
            query,
            owner,
            policy,
            budget,
        };
        request.validate()?;
        Ok(request)
    }

    /// Returns the exact caller-authored query.
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Returns the complete requested owner lineage.
    pub(crate) fn owner(&self) -> &KnowledgeOwnerId {
        &self.owner
    }

    /// Returns the validated retrieval policy.
    pub(crate) fn policy(&self) -> &KnowledgePolicy {
        &self.policy
    }

    /// Returns the response budget.
    #[must_use]
    pub(crate) fn budget(&self) -> KnowledgeBudget {
        self.budget
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.query.is_empty() || self.query.trim() != self.query {
            return Err(KitError::invalid_input(
                "knowledge query must be nonempty and unpadded",
            ));
        }
        self.owner.retrieval_owners(&self.policy).map(|_| ())
    }

    pub(crate) fn validate_context(&self, context: &KnowledgeContext) -> Result<(), KitError> {
        self.validate()?;
        context
            .validate()
            .map_err(|_| KitError::invalid_state("knowledge backend returned malformed context"))?;
        if context.snippets.len() > self.budget.chunks {
            return Err(KitError::invalid_state(
                "knowledge backend exceeded the requested chunk budget",
            ));
        }
        let allowed = self.owner.retrieval_owners(&self.policy)?;
        let mut previous_scope = None;
        let mut text_bytes = 0_usize;
        for snippet in &context.snippets {
            let scope = allowed
                .iter()
                .position(|owner| owner == &snippet.owner)
                .ok_or_else(|| {
                    KitError::invalid_state(
                        "knowledge backend returned an owner outside the requested lineage",
                    )
                })?;
            if previous_scope.is_some_and(|previous| scope < previous) {
                return Err(KitError::invalid_state(
                    "knowledge backend returned snippets outside policy precedence",
                ));
            }
            previous_scope = Some(scope);
            text_bytes = text_bytes.checked_add(snippet.text.len()).ok_or_else(|| {
                KitError::invalid_state("knowledge backend text-byte count overflowed")
            })?;
            if text_bytes > self.budget.bytes {
                return Err(KitError::invalid_state(
                    "knowledge backend exceeded the requested byte budget",
                ));
            }
        }
        Ok(())
    }
}

impl KnowledgeContext {
    /// Validates and constructs structured context.
    pub(crate) fn new(snippets: Vec<KnowledgeSnippet>) -> Result<Self, KitError> {
        let context = Self { snippets };
        context.validate()?;
        Ok(context)
    }

    /// Borrows snippets in semantic precedence and backend rank order.
    pub(crate) fn snippets(&self) -> &[KnowledgeSnippet] {
        &self.snippets
    }

    fn validate(&self) -> Result<(), KitError> {
        for snippet in &self.snippets {
            snippet.validate()?;
        }
        Ok(())
    }
}

impl KnowledgeSnippet {
    /// Validates and constructs one structured knowledge snippet.
    pub(crate) fn new(
        source_id: String,
        title: String,
        text: String,
        owner: KnowledgeOwnerId,
        relevance_score: Option<f64>,
        provenance: KnowledgeProvenance,
        metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<Self, KitError> {
        let snippet = Self {
            source_id,
            title,
            text,
            owner,
            relevance_score,
            provenance,
            metadata,
        };
        snippet.validate()?;
        Ok(snippet)
    }

    /// Returns the caller-owned source identity.
    pub(crate) fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Returns the human-readable source title.
    pub(crate) fn title(&self) -> &str {
        &self.title
    }

    /// Returns exact snippet text.
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    fn validate(&self) -> Result<(), KitError> {
        for (label, value) in [
            ("knowledge source id", self.source_id.as_str()),
            ("knowledge source title", self.title.as_str()),
        ] {
            if value.is_empty() || value.trim() != value {
                return Err(KitError::invalid_input(format!(
                    "{label} must be nonempty and unpadded"
                )));
            }
        }
        if self.text.trim().is_empty() {
            return Err(KitError::invalid_input(
                "knowledge snippet text must be nonblank",
            ));
        }
        if self
            .relevance_score
            .is_some_and(|score| !score.is_finite() || score < 0.0)
        {
            return Err(KitError::invalid_input(
                "knowledge relevance score must be finite and nonnegative",
            ));
        }
        self.provenance.validate()?;
        if self
            .metadata
            .keys()
            .any(|key| key.is_empty() || key.trim() != key)
        {
            return Err(KitError::invalid_input(
                "knowledge metadata keys must be nonempty and unpadded",
            ));
        }
        Ok(())
    }
}

impl KnowledgeProvenance {
    fn validate(&self) -> Result<(), KitError> {
        if self
            .locator
            .as_ref()
            .is_some_and(|locator| locator.is_empty() || locator.trim() != locator)
        {
            return Err(KitError::invalid_input(
                "knowledge provenance locator must be nonempty and unpadded",
            ));
        }
        if self
            .metadata
            .keys()
            .any(|key| key.is_empty() || key.trim() != key)
        {
            return Err(KitError::invalid_input(
                "knowledge provenance metadata keys must be nonempty and unpadded",
            ));
        }
        Ok(())
    }
}
