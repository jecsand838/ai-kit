use std::future::Future;
use tools_kit::ToolsetBackend;

use providers_kit::ProviderBackend;
use storage_kit::{DatabaseBackend, StateStoreBackend};

use crate::backends::{codex, custom, opencode};
use crate::{
    AgentId, DeleteWorkspaceRequest, IngestKnowledgeRequest, KitError, ModelInfo, NewWorkspace,
    UpdateWorkspaceRequest, Workspace, WorkspaceId, WorkspaceInfo, WorkspaceUpdateReadiness,
};

/// A descriptive snapshot of one durable agent identity.
///
/// Constructing this DTO validates its descriptive fields only; it does not
/// create an Agent or establish that the identity exists in storage.
///
/// # Examples
///
/// ```
/// use agents_kit::identity::AgentInfo;
/// let id = "agent_0123456789abcdef0123456789abcdef".parse()?;
/// let info = AgentInfo::new(id, Some("Reviewer".to_owned()))?;
/// assert_eq!(info.display_name(), Some("Reviewer"));
/// # Ok::<(), agents_kit::KitError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentInfo {
    id: AgentId,
    display_name: Option<String>,
}

impl AgentInfo {
    /// Validates and constructs descriptive agent information.
    ///
    /// Display names, when present, must be nonempty and must not contain
    /// leading or trailing whitespace.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorKind::InvalidInput`] when `display_name` violates the
    /// portable naming contract.
    ///
    /// # Examples
    ///
    /// ```
    /// use agents_kit::identity::AgentInfo;
    /// let id = "agent_0123456789abcdef0123456789abcdef".parse()?;
    /// assert!(AgentInfo::new(id, Some(" padded".to_owned())).is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(id: AgentId, display_name: Option<String>) -> Result<Self, KitError> {
        if display_name
            .as_deref()
            .is_some_and(|value| value.is_empty() || value.trim() != value)
        {
            return Err(KitError::invalid_input(
                "agent display name must be nonempty and unpadded when present",
            ));
        }
        Ok(Self { id, display_name })
    }

    /// Returns the portable durable identity.
    ///
    /// # Examples
    ///
    /// ```
    /// use agents_kit::identity::AgentInfo;
    /// let id = "agent_0123456789abcdef0123456789abcdef".parse()?;
    /// let info = AgentInfo::new(id, None)?;
    /// assert_eq!(info.id().as_str(), "agent_0123456789abcdef0123456789abcdef");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn id(&self) -> &AgentId {
        &self.id
    }

    /// Returns the configured display name, when present.
    ///
    /// # Examples
    ///
    /// ```
    /// use agents_kit::identity::AgentInfo;
    /// let id = "agent_0123456789abcdef0123456789abcdef".parse()?;
    /// let info = AgentInfo::new(id, None)?;
    /// assert_eq!(info.display_name(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }
}

/// Public implementation contract for a complete portable agent runtime.
///
/// Built-ins and caller implementations use this behavioral boundary directly.
/// A caller implementation returns its own concrete [`Workspace`](Self::Workspace),
/// whose associated Conversation remains concrete. Runtime selection may use an
/// exhaustive caller-owned enum hierarchy; no normalization constructor or
/// independent child type parameter is needed.
///
/// Accepted work must reach its documented durable terminal state even if a caller
/// drops the awaiting future. Shutdown seals new work, invalidates live descendants,
/// and settles accepted cleanup.
///
/// Start with the [local construction example](crate) and [`Self::new_workspace`].
/// For a complete caller-owned hierarchy, the [executable direct-trait examples](https://github.com/jecsand838/ai-kit/blob/main/agents-kit/tests/mock_hierarchy/mod.rs)
/// cover Agent, associated Workspace and Conversation, exact requests, lifecycle
/// and errors. Their process-local state is not native durability evidence.
/// The [Custom composition tests](https://github.com/jecsand838/ai-kit/blob/main/agents-kit/tests/custom_mock_composition.rs)
/// exercise the real Custom runtime with caller-owned lower implementations.
pub trait AgentBackend: Send + Sync + 'static {
    /// Concrete workspace implementation produced by this agent.
    type Workspace: crate::WorkspaceBackend;

    /// Returns the durable portable agent identity.
    ///
    /// The identity remains stable across runtime reopen. This value identifies
    /// the durable Agent; it does not expose backend routing or lower resources.
    ///
    /// See the [local construction example](crate) and [`Self::new_workspace`] for child ownership.
    fn id(&self) -> AgentId;

    /// Loads the current descriptive agent information.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] if the runtime is closed, committed state cannot be
    /// read, or stored facts contradict the portable record contract.
    ///
    /// See the [local construction example](crate) and [`Self::new_workspace`] for child ownership.
    fn info(&self) -> impl Future<Output = Result<AgentInfo, KitError>> + Send + '_;

    /// Creates and publishes a workspace from validated portable configuration.
    ///
    /// Publishes a handle only after complete context/defaults, backend attachment,
    /// and durable ownership agree. A new workspace begins at revision one.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for closed or busy admission, unsupported execution
    /// policy/integration composition, invalid input, dependency failure, or
    /// deadline expiry. Accepted native setup must remain durably attached or
    /// recovery-owned when an operation fails.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agents_kit::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agents_kit::{model::ModelSelector, tool::ToolSelection};
    /// # use providers_kit::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_kit::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agents_kit::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// let workspace = agent.new_workspace(
    ///     NewWorkspace::new("review", context, WorkspaceDefaults::empty())?,
    /// ).await?;
    /// use agents_kit::workspace::{UpdateWorkspaceRequest, WorkspaceUpdateReadiness, DeleteWorkspaceRequest};
    /// let observed = async {
    ///     let current = workspace.info().await?;
    ///     assert_eq!(current.revision(), 1);
    ///     let request = UpdateWorkspaceRequest::new(current.id().clone(), current.revision(),
    ///         "renamed", current.context().clone(), current.defaults().clone())?;
    ///     // Preflight observes the still-open workspace runtime.
    ///     assert_eq!(agent.check_workspace_update(&request).await?, WorkspaceUpdateReadiness::Ready);
    ///     // Readiness is advisory: it does not reserve or change the revision.
    ///     assert_eq!(agent.workspace_info(current.id().clone()).await?.revision(), current.revision());
    ///     Ok::<_, agents_kit::KitError>((current, request))
    /// }.await;
    /// let close = workspace.close().await;
    /// let (current, request) = match (observed, close) {
    ///     (Ok(value), Ok(())) => Ok(value),
    ///     (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
    ///     (Err(error), _) | (_, Err(error)) => Err(error),
    /// }?;
    /// // Revision-checked mutations also accept a quiescent, closed workspace.
    /// let updated = agent.update_workspace(request).await?;
    /// assert_eq!(updated.name(), "renamed");
    /// assert_eq!(updated.revision(), current.revision() + 1);
    /// agent.delete_workspace(DeleteWorkspaceRequest::new(updated.id().clone(), updated.revision())?).await?;
    /// assert!(agent.list_workspaces().await?.is_empty());
    /// # Ok::<(), agents_kit::KitError>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # match (outcome, cleanup) {
    /// #     (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
    /// #     (Err(error), _) | (_, Err(error)) => Err(error),
    /// #     (Ok(()), Ok(())) => Ok(()),
    /// # }?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn new_workspace(
        &self,
        request: NewWorkspace,
    ) -> impl Future<Output = Result<Self::Workspace, KitError>> + Send + '_;

    /// Lists the agent's durable workspaces.
    ///
    /// Inventory comes from committed records, not only currently opened handles.
    /// The result does not claim live ownership of those workspaces.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for a closed runtime, state-read failure, deadline
    /// expiry, or inconsistent durable inventory.
    ///
    /// See the [local construction example](crate) and [`Self::new_workspace`] for child ownership.
    fn list_workspaces(
        &self,
    ) -> impl Future<Output = Result<Vec<WorkspaceInfo>, KitError>> + Send + '_;

    /// Loads descriptive information for a workspace identity.
    ///
    /// This reads committed metadata without claiming a live workspace handle.
    /// Missing and foreign identities are both reported without revealing owners.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorKind::NotFound`] for a missing or foreign id,
    /// or the owning lifecycle, state-read, integrity, or deadline error.
    ///
    /// See the [local construction example](crate) and [`Self::new_workspace`] for child ownership.
    fn workspace_info(
        &self,
        id: WorkspaceId,
    ) -> impl Future<Output = Result<WorkspaceInfo, KitError>> + Send + '_;

    /// Reopens a durable workspace as a live handle.
    ///
    /// At most one live handle exists for a current workspace revision. Share it
    /// by reference; a second current-revision lookup is busy. Reopen verifies
    /// durable attachment and integration compatibility rather than inventing a
    /// replacement workspace.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for a missing/foreign id, an existing live claim,
    /// closed runtime, incompatible reopen state, dependency failure, or timeout.
    ///
    /// See [`WorkspaceBackend::new_conversation`](crate::WorkspaceBackend::new_conversation)
    /// for an inventory and reopen workflow,
    /// including release of the first workspace claim before lookup.
    fn get_workspace(
        &self,
        id: WorkspaceId,
    ) -> impl Future<Output = Result<Self::Workspace, KitError>> + Send + '_;

    /// Performs a read-only advisory preflight for a workspace replacement.
    ///
    /// A completed `Ready` reserves no later mutation right. The durable expected
    /// revision is checked before dynamic readiness; the preflight excludes its
    /// own transient admission from the activity it reports.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for invalid or unsupported replacement data, a
    /// missing/foreign workspace, stale revision, closed runtime, or an owning
    /// state/dependency/deadline failure. Dynamic activity is reported as
    /// [`WorkspaceUpdateReadiness::Busy`].
    ///
    /// See [`Self::new_workspace`] for a complete creation, revision-check, replacement and deletion workflow.
    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_;

    /// Atomically applies a revision-checked portable workspace replacement.
    ///
    /// This is a full replacement of name, context, and defaults, not a patch.
    /// The operation rechecks revision and activity atomically, preserves the
    /// backend attachment, increments the revision once, and invalidates prior
    /// workspace/conversation handles only after durable commit. Commit is success;
    /// postcommit cleanup cannot change it into an error.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for stale revision, busy or ambiguous descendant work,
    /// missing/foreign identity, unsupported replacement policy, closed runtime,
    /// or precommit dependency/deadline failures. Revision overflow is
    /// [`crate::error::ErrorKind::InvalidState`] and does not change state.
    ///
    /// See [`Self::new_workspace`] for a complete creation, revision-check, replacement and deletion workflow.
    fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> impl Future<Output = Result<WorkspaceInfo, KitError>> + Send + '_;

    /// Deletes a workspace after validating its expected revision.
    ///
    /// Requires quiescent descendants and the current revision; there is no force
    /// mode. Durable removal of workspace/child inventory is the success boundary.
    /// Postcommit native cleanup remains a recovery obligation and cannot reverse
    /// success.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for stale revision, active or ambiguous descendants,
    /// missing/foreign identity, closed runtime, or precommit state/dependency/
    /// deadline failure.
    ///
    /// See [`Self::new_workspace`] for a complete creation, revision-check, replacement and deletion workflow.
    fn delete_workspace(
        &self,
        request: DeleteWorkspaceRequest,
    ) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    /// Lists selectable models in stable canonical-selector order.
    ///
    /// The complete selectable inventory retains canonical selectors and supported
    /// reasoning/personality values. Listing does not reserve future availability;
    /// selection is revalidated in the eventual workspace/conversation context.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] when the inventory route is unsupported or incomplete,
    /// selected external data is malformed, the runtime is closed, or a dependency
    /// operation fails or times out. An unavailable inventory is not an empty success.
    ///
    /// Caller implementations need no service. The [executable caller hierarchy](https://github.com/jecsand838/ai-kit/blob/main/agents-kit/tests/mock_hierarchy/mod.rs) exercises this contract with direct trait mocks; supplied backends retain their own prerequisites.
    fn list_models(&self) -> impl Future<Output = Result<Vec<ModelInfo>, KitError>> + Send + '_;

    /// Replaces the agent-scoped managed-knowledge source set.
    ///
    /// An omitted scope selects Agent scope; an explicit Database scope follows
    /// the managed-knowledge contract. Nonempty ingestion replaces the selected source
    /// set atomically; an empty request is a no-op after scope validation, not deletion.
    /// It does not execute a prompt or create tools. See [`crate::knowledge`] for a
    /// complete local ingestion workflow.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for invalid source/scope data, missing or incompatible
    /// knowledge integrations, closed or busy admission, dependency failures, or
    /// timeout. Unsupported scope or backend semantics fail explicitly.
    ///
    /// Caller implementations need no service. The [executable caller hierarchy](https://github.com/jecsand838/ai-kit/blob/main/agents-kit/tests/mock_hierarchy/mod.rs) exercises this contract with direct trait mocks; supplied backends retain their own prerequisites.
    fn ingest_knowledge(
        &self,
        request: IngestKnowledgeRequest,
    ) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    /// Consumes the implementation, seals new work, and settles owned cleanup.
    ///
    /// Live descendants are invalidated after accepted work and cleanup settle.
    /// Quiescent durable workspaces and conversations remain available to a new
    /// parent runtime. Dropping an Agent provides no asynchronous cleanup guarantee.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] if consuming cleanup cannot fully settle within the
    /// lifecycle deadline or an owned resource fails cleanup. The owner is not
    /// restored; preserve the primary error and its supplementary cleanup facts
    /// and use durable state for later reconciliation.
    ///
    /// See the [local construction example](crate) and [`Self::new_workspace`] for child ownership.
    fn shutdown(self) -> impl Future<Output = Result<(), KitError>> + Send
    where
        Self: Sized;
}

/// A live handle to one durable agent and its workspace inventory.
///
/// Construct or reopen it with [`crate::AgentBuilder`] and import
/// [`AgentBackend`] for behavior. The handle is unique and non-Clone; its
/// workspace type preserves the same concrete storage/provider/tool types.
/// Explicit shutdown consumes it and settles owned work. Drop alone offers no
/// asynchronous cleanup guarantee.
///
/// See [`AgentBackend`] and the [local construction example](crate) for usage.
pub struct Agent<
    S: StateStoreBackend = storage_kit::StateStore,
    D: DatabaseBackend = storage_kit::Database,
    P: ProviderBackend = providers_kit::Provider,
    T: ToolsetBackend = tools_kit::Toolset,
> {
    inner: AgentInner<S, D, P, T>,
}

enum AgentInner<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> {
    Codex(Box<codex::agent::CodexAgent<S, D, T>>),
    OpenCode(Box<opencode::agent::OpenCodeAgent<S, D>>),
    Custom(Box<custom::CustomAgent<S, D, P, T>>),
}

impl<S, D, P, T> Agent<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    pub(crate) fn codex(backend: codex::agent::CodexAgent<S, D, T>) -> Self {
        Self {
            inner: AgentInner::Codex(Box::new(backend)),
        }
    }

    pub(crate) fn opencode(backend: opencode::agent::OpenCodeAgent<S, D>) -> Self {
        Self {
            inner: AgentInner::OpenCode(Box::new(backend)),
        }
    }

    pub(crate) fn custom(backend: custom::CustomAgent<S, D, P, T>) -> Self {
        Self {
            inner: AgentInner::Custom(Box::new(backend)),
        }
    }

    pub(crate) fn retain_storage_claim(&mut self) {
        match &mut self.inner {
            AgentInner::Codex(backend) => backend.retain_storage_claim(),
            AgentInner::OpenCode(backend) => backend.retain_storage_claim(),
            AgentInner::Custom(backend) => backend.retain_storage_claim(),
        }
    }
}

impl<S, D, P, T> AgentBackend for AgentInner<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    type Workspace = Workspace<S, D, P, T>;

    fn id(&self) -> AgentId {
        match self {
            AgentInner::Codex(backend) => backend.id(),
            AgentInner::OpenCode(backend) => backend.id(),
            AgentInner::Custom(backend) => backend.id(),
        }
    }

    async fn info(&self) -> Result<AgentInfo, KitError> {
        match self {
            AgentInner::Codex(backend) => backend.info().await,
            AgentInner::OpenCode(backend) => backend.info().await,
            AgentInner::Custom(backend) => backend.info().await,
        }
    }

    async fn new_workspace(
        &self,
        request: NewWorkspace,
    ) -> Result<Workspace<S, D, P, T>, KitError> {
        match self {
            AgentInner::Codex(backend) => {
                backend.new_workspace(request).await.map(Workspace::codex)
            }
            AgentInner::OpenCode(backend) => backend
                .new_workspace(request)
                .await
                .map(Workspace::opencode),
            AgentInner::Custom(backend) => {
                backend.new_workspace(request).await.map(Workspace::custom)
            }
        }
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        match self {
            AgentInner::Codex(backend) => backend.list_workspaces().await,
            AgentInner::OpenCode(backend) => backend.list_workspaces().await,
            AgentInner::Custom(backend) => backend.list_workspaces().await,
        }
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        match self {
            AgentInner::Codex(backend) => backend.workspace_info(id).await,
            AgentInner::OpenCode(backend) => backend.workspace_info(id).await,
            AgentInner::Custom(backend) => backend.workspace_info(id).await,
        }
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<Workspace<S, D, P, T>, KitError> {
        match self {
            AgentInner::Codex(backend) => backend.get_workspace(id).await.map(Workspace::codex),
            AgentInner::OpenCode(backend) => {
                backend.get_workspace(id).await.map(Workspace::opencode)
            }
            AgentInner::Custom(backend) => backend.get_workspace(id).await.map(Workspace::custom),
        }
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_ {
        let request = request.clone();
        async move {
            match self {
                AgentInner::Codex(backend) => backend.check_workspace_update(&request).await,
                AgentInner::OpenCode(backend) => backend.check_workspace_update(&request).await,
                AgentInner::Custom(backend) => backend.check_workspace_update(&request).await,
            }
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        match self {
            AgentInner::Codex(backend) => backend.update_workspace(request).await,
            AgentInner::OpenCode(backend) => backend.update_workspace(request).await,
            AgentInner::Custom(backend) => backend.update_workspace(request).await,
        }
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        match self {
            AgentInner::Codex(backend) => backend.delete_workspace(request).await,
            AgentInner::OpenCode(backend) => backend.delete_workspace(request).await,
            AgentInner::Custom(backend) => backend.delete_workspace(request).await,
        }
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        match self {
            AgentInner::Codex(backend) => backend.list_models().await,
            AgentInner::OpenCode(backend) => backend.list_models().await,
            AgentInner::Custom(backend) => backend.list_models().await,
        }
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        match self {
            AgentInner::Codex(backend) => backend.ingest_knowledge(request).await,
            AgentInner::OpenCode(backend) => backend.ingest_knowledge(request).await,
            AgentInner::Custom(backend) => backend.ingest_knowledge(request).await,
        }
    }

    async fn shutdown(self) -> Result<(), KitError> {
        match self {
            AgentInner::Codex(backend) => (*backend).shutdown().await,
            AgentInner::OpenCode(backend) => (*backend).shutdown().await,
            AgentInner::Custom(backend) => (*backend).shutdown().await,
        }
    }
}

impl<S, D, P, T> AgentBackend for Agent<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    type Workspace = Workspace<S, D, P, T>;

    fn id(&self) -> AgentId {
        self.inner.id()
    }

    async fn info(&self) -> Result<AgentInfo, KitError> {
        self.inner.info().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::AgentInfo)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    async fn new_workspace(
        &self,
        request: NewWorkspace,
    ) -> Result<Workspace<S, D, P, T>, KitError> {
        self.inner.new_workspace(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::NewWorkspace)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        self.inner.list_workspaces().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::ListWorkspaces)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        self.inner.workspace_info(id).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::WorkspaceInfo)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<Workspace<S, D, P, T>, KitError> {
        self.inner.get_workspace(id).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::GetWorkspace)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_ {
        let check = self.inner.check_workspace_update(request);
        async move {
            check.await.map_err(|error| {
                error
                    .at_operation(crate::error::Operation::CheckWorkspaceUpdate)
                    .in_phase(crate::error::ErrorPhase::InputValidation)
            })
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        self.inner.update_workspace(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::UpdateWorkspace)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        self.inner.delete_workspace(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::DeleteWorkspace)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        self.inner.list_models().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::ListModels)
                .in_phase(crate::error::ErrorPhase::InventoryVerification)
        })
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        self.inner.ingest_knowledge(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::IngestKnowledge)
                .in_phase(crate::error::ErrorPhase::KnowledgeWrite)
        })
    }

    async fn shutdown(self) -> Result<(), KitError> {
        self.inner.shutdown().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::Shutdown)
                .in_phase(crate::error::ErrorPhase::Lifecycle)
        })
    }
}
