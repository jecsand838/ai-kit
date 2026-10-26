use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tool_sdk::ToolsetBackend;

use provider_sdk::ProviderBackend;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::agent_storage::AgentStorageClaim;
use crate::backends::{codex, custom, opencode};
use crate::{
    Agent, AgentBackend, AgentConfig, AgentId, AgentInfo, AgentStorage, BackendConfig,
    KnowledgeOwnerId, ManagedKnowledge, SdkError, ToolSelection,
};
use storage_sdk::{Database, DatabaseBackend, StateStore, StateStoreBackend};

/// Configures creation or reopening of one durable [`Agent`].
///
/// The builder accepts already-built provider, tool, and storage values without
/// exposing their implementation details through `agent-sdk`.
///
/// The hierarchy preserves concrete `S`, `D`, `P` and `T` types, with no Clone
/// requirement on caller backends or `T::Handler`. New identity creation and
/// durable reopen use separate constructors; reopening resupplies integrations
/// instead of reconstructing credentials or lower configuration from records.
///
/// Native Codex/OpenCode can select `P` as static composition context without
/// owning a provider value. Custom requires a value through [`Self::with_provider`].
/// The builder owns supplied values, while [`Self::build`] accepts construction
/// and cleanup work. Await consuming [`AgentBackend::shutdown`] on the built Agent
/// while its runtime remains driven.
///
/// The local example uses memory Lance and a passively constructed Provider. It
/// selects an exact model without verifying a running service or generating output.
///
/// # Examples
///
/// ```
/// use std::{num::NonZeroUsize, time::Duration};
/// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
/// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
/// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
/// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
/// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// let storage = AgentStorage::new(store);
/// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
/// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
/// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
/// let provider = Provider::from_config(provider_config.clone())?;
/// // This constructs local owners and selects a model; it makes no generation request.
/// let agent = AgentBuilder::create(config, storage).with_provider(provider).build().await?;
/// assert!(agent.list_workspaces().await?.is_empty());
/// agent.shutdown().await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct AgentBuilder<
    S: StateStoreBackend = StateStore,
    D: DatabaseBackend = Database,
    P: ProviderBackend = provider_sdk::Provider,
    T: ToolsetBackend = tool_sdk::Toolset,
> {
    config: AgentConfig,
    storage: AgentStorage<S>,
    mode: AgentBuildMode,
    provider: Option<P>,
    toolsets: Vec<Arc<T>>,
    knowledge_database: Option<D>,
}

enum AgentBuildMode {
    Create,
    Reopen(AgentId),
}

impl<S: StateStoreBackend> AgentBuilder<S, Database, provider_sdk::Provider> {
    /// Configures a new durable agent over the required control-plane store.
    ///
    /// This records creation intent without opening a backend. [`Self::build`] creates
    /// a fresh durable identity. Storage is required up front and retains its exact
    /// associated transaction type. For an existing identity use [`Self::reopen`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let storage = AgentStorage::new(store);
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    ///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
    /// let provider = Provider::from_config(provider_config.clone())?;
    /// let builder = AgentBuilder::create(config, storage);
    /// let agent = builder.with_provider(provider).build().await?;
    /// assert!(agent.list_workspaces().await?.is_empty());
    /// agent.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create(config: AgentConfig, storage: AgentStorage<S>) -> Self {
        Self::create_for_backends::<provider_sdk::Provider, tool_sdk::Toolset>(config, storage)
    }

    /// Configures a new agent with fixed Provider and Toolset implementation types.
    ///
    /// Codex and OpenCode retain their native providers, so this selects only
    /// the static provider type shared with other agents in a caller-owned
    /// composition. Custom agents receive their provider value through
    /// [`Self::with_provider`]. Repeated [`Self::with_toolset`] calls append T2
    /// values to one homogeneous inventory without normalization or a Clone bound.
    ///
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let storage = AgentStorage::new(store);
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    ///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
    /// let provider = Provider::from_config(provider_config.clone())?;
    /// let builder = AgentBuilder::create_for_backends::<Provider, tool_sdk::Toolset>(config, storage);
    /// // Selecting P alone does not supply the provider required by Custom.
    /// let agent = builder.with_provider(provider).build().await?;
    /// assert!(agent.list_workspaces().await?.is_empty());
    /// agent.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create_for_backends<P2: ProviderBackend, T2: ToolsetBackend>(
        config: AgentConfig,
        storage: AgentStorage<S>,
    ) -> AgentBuilder<S, Database, P2, T2> {
        AgentBuilder {
            config,
            storage,
            mode: AgentBuildMode::Create,
            provider: None,
            toolsets: Vec::new(),
            knowledge_database: None,
        }
    }

    /// Configures reopening of an existing durable agent.
    ///
    /// Selects an existing identity; it never falls back to creating one. Build checks
    /// stored compatibility and the storage clone family's live-ownership claim.
    /// Resupply fresh provider/tool/database values required by that identity.
    /// An AgentStorage clone shares coordination without requiring `S: Clone`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let storage = AgentStorage::new(store);
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    ///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
    /// let provider = Provider::from_config(provider_config.clone())?;
    /// let agent = AgentBuilder::create(config.clone(), storage.clone())
    ///     .with_provider(provider).build().await?;
    /// let id = agent.id();
    /// agent.shutdown().await?;
    /// // Reuse the same storage family, and supply a fresh provider after shutdown.
    /// let reopened = AgentBuilder::reopen(config, storage, id.clone())
    ///     .with_provider(Provider::from_config(provider_config)?).build().await?;
    /// assert_eq!(reopened.id(), id);
    /// reopened.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn reopen(config: AgentConfig, storage: AgentStorage<S>, id: AgentId) -> Self {
        Self::reopen_for_backends::<provider_sdk::Provider, tool_sdk::Toolset>(config, storage, id)
    }

    /// Configures reopening with fixed Provider and Toolset implementation types.
    ///
    /// This is the reopening counterpart to [`Self::create_for_backends`]. It
    /// selects no provider value and performs no provider operation.
    ///
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let storage = AgentStorage::new(store);
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    ///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
    /// let provider = Provider::from_config(provider_config.clone())?;
    /// let agent = AgentBuilder::create(config.clone(), storage.clone())
    ///     .with_provider(provider).build().await?;
    /// let id = agent.id();
    /// agent.shutdown().await?;
    /// let builder = AgentBuilder::reopen_for_backends::<Provider, tool_sdk::Toolset>(
    ///     config, storage, id.clone(),
    /// );
    /// let reopened = builder.with_provider(Provider::from_config(provider_config)?).build().await?;
    /// assert_eq!(reopened.id(), id);
    /// reopened.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn reopen_for_backends<P2: ProviderBackend, T2: ToolsetBackend>(
        config: AgentConfig,
        storage: AgentStorage<S>,
        id: AgentId,
    ) -> AgentBuilder<S, Database, P2, T2> {
        let mut builder = Self::create_for_backends::<P2, T2>(config, storage);
        builder.mode = AgentBuildMode::Reopen(id);
        builder
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    AgentBuilder<S, D, P, T>
{
    /// Supplies the already-built provider required by the Custom backend.
    ///
    /// Consumes the provider and changes the builder's concrete provider parameter;
    /// accepted Toolsets and the Database remain attached. Replacing an earlier value
    /// drops that value, so complete any required lower lifecycle work before replacing
    /// it. Codex/OpenCode reject a supplied provider value during build; use typed
    /// construction to select their static provider parameter without a value.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let storage = AgentStorage::new(store);
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    ///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
    /// let provider = Provider::from_config(provider_config.clone())?;
    /// let agent = AgentBuilder::create(config, storage)
    ///     .with_provider(provider).build().await?;
    /// assert!(agent.list_workspaces().await?.is_empty());
    /// // Shutdown includes the transferred provider; do not reuse it afterward.
    /// agent.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    /// # Direct lower implementations and non-Clone tools
    ///
    /// These caller-owned enums deliberately expose only Provider selection and
    /// generation, and Local tool execution. Other Provider operations retain the
    /// trait's Unsupported defaults. They delegate actual transport/handler
    /// semantics, not fabricated success. Agent uses `LocalProvider::Model` and
    /// `LocalTools::Handler`; neither tool enum implements `Clone`.
    ///
    /// This example executes a real Local handler and builds Custom without model
    /// inference. A prompt would require the exact configured Ollama model. See
    /// [`provider_sdk::ProviderBackend`] and [`tool_sdk::ToolsetBackend`] for
    /// implementing those lower contracts rather than delegating them.
    ///
    /// ```
    /// use std::{convert::Infallible, num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Model, ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig};
    /// use provider_sdk::config::{OllamaConfig, ProviderKind, TransportConfig};
    /// use provider_sdk::generation::{GenerateEventStream, GenerateRequest, GenerateResponse, GenerationContinuation};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// use tool_sdk::{ToolError, ToolHandler, ToolHandlerBackend, Toolset, ToolsetBackend};
    /// use tool_sdk::declaration::{ToolInfo, ToolName};
    /// use tool_sdk::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult};
    /// use tool_sdk::{local::ToolRegistration, schema::JsonSchema};
    ///
    /// enum LocalProvider { Ollama(Provider) }
    /// enum LocalModel { Ollama(Model) }
    /// enum LocalTools { Functions(Toolset) }
    /// enum LocalHandler { Function(ToolHandler) }
    /// impl ProviderBackend for LocalProvider {
    ///     type Model = LocalModel;
    ///     fn model(&self, id: ModelId) -> Result<Self::Model, provider_sdk::SdkError> {
    ///         match self { Self::Ollama(value) => value.model(id).map(LocalModel::Ollama) }
    ///     }
    ///     async fn shutdown(&self) -> Result<(), provider_sdk::SdkError> {
    ///         match self { Self::Ollama(value) => value.shutdown().await }
    ///     }
    /// }
    /// impl ModelBackend for LocalModel {
    ///     type Continuation = GenerationContinuation;
    ///     type Session = Infallible;
    ///     fn model_id(&self) -> &ModelId {
    ///         match self { Self::Ollama(value) => value.model_id() }
    ///     }
    ///     async fn generate(&self, request: &GenerateRequest<Self::Continuation>)
    ///         -> Result<GenerateResponse<Self::Continuation>, provider_sdk::SdkError> {
    ///         match self { Self::Ollama(value) => value.generate(request).await }
    ///     }
    ///     async fn generate_stream(&self, request: &GenerateRequest<Self::Continuation>)
    ///         -> Result<GenerateEventStream<Self::Continuation>, provider_sdk::SdkError> {
    ///         match self { Self::Ollama(value) => value.generate_stream(request).await }
    ///     }
    /// }
    /// impl ToolsetBackend for LocalTools {
    ///     type Handler = LocalHandler;
    ///     async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
    ///         match self { Self::Functions(value) => value.list_tools().await }
    ///     }
    ///     async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
    ///         match self { Self::Functions(value) => value.tool_handler(name).await.map(LocalHandler::Function) }
    ///     }
    ///     async fn shutdown(&self) -> Result<(), ToolError> {
    ///         match self { Self::Functions(value) => value.shutdown().await }
    ///     }
    /// }
    /// impl ToolHandlerBackend for LocalHandler {
    ///     async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
    ///         match self { Self::Function(value) => value.step(input).await }
    ///     }
    /// }
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let tools = LocalTools::Functions(Toolset::local(vec![ToolRegistration::function(
    ///     ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type": "object"}))),
    ///     |_| async { Ok(ToolResult::json(serde_json::json!({"reply": "pong"}))) },
    /// )])?);
    /// let direct = async {
    ///     let mut handler = tools.tool_handler(ToolName::new("ping")?).await?;
    ///     let output = handler.step(ToolHandlerInput::Invoke(ToolInvocation {
    ///         call_id: Some("caller-ping".into()), arguments: serde_json::json!({}),
    ///     })).await?;
    ///     let ToolHandlerOutput::Completed(result) = output else {
    ///         return Err(ToolError::Unsupported { operation: "ping must complete without interaction".into() });
    ///     };
    ///     assert!(!result.is_error);
    ///     assert_eq!(result.structured_content, Some(serde_json::json!({"reply": "pong"})));
    ///     Ok::<(), ToolError>(())
    /// }.await;
    /// if let Err(error) = direct {
    ///     let primary = agent_sdk::SdkError::from(error);
    ///     let error = match tools.shutdown().await {
    ///         Ok(()) => primary,
    ///         Err(cleanup) => primary.with_supplementary(agent_sdk::SdkError::from(cleanup)),
    ///     };
    ///     return Err(Box::<dyn std::error::Error>::from(error));
    /// }
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::Auto,
    ///     CustomLoopLimits::new(4, 4, 1)?, CustomToolApproval::Allow)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider = LocalProvider::Ollama(Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?);
    /// let agent = AgentBuilder::create_for_backends::<LocalProvider, LocalTools>(config, AgentStorage::new(store))
    ///     .with_provider(provider).with_toolset(tools).build().await?;
    /// let outcome = agent.list_workspaces().await;
    /// let shutdown = agent.shutdown().await; // owns Provider and the non-Clone Toolset
    /// assert!(outcome?.is_empty());
    /// shutdown?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_provider<P2: ProviderBackend>(self, provider: P2) -> AgentBuilder<S, D, P2, T> {
        AgentBuilder {
            config: self.config,
            storage: self.storage,
            mode: self.mode,
            provider: Some(provider),
            toolsets: self.toolsets,
            knowledge_database: self.knowledge_database,
        }
    }

    /// Appends one already-built executable Toolset implementation.
    ///
    /// T remains fixed by construction. Changing the Provider or Database keeps
    /// every accepted Toolset; its unique Handler remains associated as `T::Handler`.
    ///
    /// Each call appends a value of the construction-selected `T`; use caller-owned
    /// exhaustive Toolset/Handler enums for heterogeneous sources. Custom executes the
    /// associated Handler directly. Build validates unique effective names and selected
    /// tools before publication. Native support is backend-specific: OpenCode rejects
    /// supplied executable toolsets. Builder transfer alone never invokes a handler.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let storage = AgentStorage::new(store);
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    ///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
    /// let provider = Provider::from_config(provider_config.clone())?;
    /// use tool_sdk::{Toolset, declaration::{ToolInfo, ToolName}, execution::ToolResult,
    ///     local::ToolRegistration, schema::JsonSchema};
    /// let tools = Toolset::local(vec![ToolRegistration::function(
    ///     ToolInfo::new(ToolName::new("ping")?, JsonSchema::strict(serde_json::json!({"type":"object"}))),
    ///     |_| async { Ok(ToolResult::text("pong")) },
    /// )])?;
    /// let agent = AgentBuilder::create(config, storage)
    ///     .with_toolset(tools).with_provider(provider).build().await?;
    /// assert!(agent.list_workspaces().await?.is_empty());
    /// // The accepted Toolset is retained across with_provider and closed by Agent shutdown.
    /// agent.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_toolset(mut self, toolset: T) -> Self {
        self.toolsets.push(Arc::new(toolset));
        self
    }

    /// Supplies the Database used by the private managed-knowledge owner.
    ///
    /// Consumes a direct Database implementation and changes only the builder's
    /// concrete `D` parameter. Existing Toolsets and Provider remain attached. This
    /// Database is separate from AgentStorage's control-plane records and exposes no
    /// public knowledge runtime. Reopen must resupply a compatible Database when the
    /// durable identity requires managed knowledge.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let storage = AgentStorage::new(store);
    /// let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    ///     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// let provider_config = ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
    /// let provider = Provider::from_config(provider_config.clone())?;
    /// use storage_sdk::{Database, database::config::{DatabaseConfig, LanceConfig}};
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// let agent = AgentBuilder::create(config, storage).with_provider(provider)
    ///     .with_knowledge_database(database).build().await?;
    /// assert!(agent.list_workspaces().await?.is_empty());
    /// agent.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_knowledge_database<D2: DatabaseBackend>(
        self,
        database: D2,
    ) -> AgentBuilder<S, D2, P, T> {
        AgentBuilder {
            config: self.config,
            storage: self.storage,
            mode: self.mode,
            provider: self.provider,
            toolsets: self.toolsets,
            knowledge_database: Some(database),
        }
    }

    /// Validates the complete configuration and creates or reopens the agent.
    ///
    /// Takes ownership of construction after acceptance. On failure, prepared resources
    /// are cleaned in reverse ownership order within the lifecycle deadline; the first
    /// failure remains primary and later failures remain supplementary. Losing the
    /// caller observation does not detach accepted build/cleanup ownership.
    ///
    /// On success the unique Agent owns its runtime and transferred integrations.
    /// Keep Tokio and lower runtimes driven through consuming shutdown. Custom model
    /// selection alone does not verify remote model availability; later provider
    /// operations own service validation.
    ///
    /// # Errors
    ///
    /// Returns InvalidConfig for invalid backend/integration combinations (including a
    /// missing Custom provider), Unsupported for a supplied integration the selected
    /// backend cannot represent, and Conflict/Busy for incompatible or already-owned
    /// reopen state. Missing identities use NotFound; malformed durable evidence uses
    /// InvalidState. Process, transport, protocol, storage and deadline failures retain
    /// their owning portable classification. Inspect [`SdkError::kind`] and
    /// [`SdkError::context`]; build and reopen carry distinct operation context.
    ///
    /// This example requires an installed, configured `codex` executable and its real
    /// App Server process. It does not submit a model prompt.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, CodexConfig};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let agent = AgentBuilder::create(config, AgentStorage::new(store)).build().await?;
    /// let observed = agent.info().await;
    /// let cleanup = agent.shutdown().await;
    /// let info = observed?;
    /// assert!(!info.id().as_str().is_empty());
    /// cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn build(self) -> Result<Agent<S, D, P, T>, SdkError> {
        let operation = match self.mode {
            AgentBuildMode::Create => crate::error::Operation::Build,
            AgentBuildMode::Reopen(_) => crate::error::Operation::Reopen,
        };
        AgentBuildPublication::<Agent<S, D, P, T>>::run(AgentBuildOwner::new(self))
            .await
            .map_err(|error| {
                error
                    .at_operation(operation)
                    .in_phase(crate::error::ErrorPhase::Admission)
            })
    }
}

struct AgentBuildOwner<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    config: Option<AgentConfig>,
    mode: AgentBuildMode,
    provider: Option<P>,
    toolsets: Vec<Arc<T>>,
    storage: Option<AgentStorage<S>>,
    knowledge_database: Option<D>,
    claim: Option<AgentStorageClaim>,
    agent: Option<Agent<S, D, P, T>>,
    cleanup_timeout: Duration,
    tracing_enabled: bool,
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    AgentBuildOwner<S, D, P, T>
{
    fn new(builder: AgentBuilder<S, D, P, T>) -> Self {
        let cleanup_timeout = builder.config.lifecycle_timeout();
        let tracing_enabled = builder.config.tracing_enabled();
        let AgentBuilder {
            config,
            storage,
            mode,
            provider,
            toolsets,
            knowledge_database,
        } = builder;
        Self {
            config: Some(config),
            mode,
            provider,
            toolsets,
            storage: Some(storage),
            knowledge_database,
            claim: None,
            agent: None,
            cleanup_timeout,
            tracing_enabled,
        }
    }

    async fn build(mut self) -> Result<Agent<S, D, P, T>, SdkError> {
        if let Err(error) = self.validate().await {
            return Err(self.cleanup_after_error(error).await);
        }
        let lifecycle_deadline = match tokio::time::Instant::now().checked_add(self.cleanup_timeout)
        {
            Some(deadline) => deadline,
            None => {
                return Err(self
                    .cleanup_after_error(SdkError::invalid_config(
                        "lifecycle timeout cannot be represented",
                    ))
                    .await);
            }
        };

        let storage = self
            .storage
            .as_ref()
            .expect("validated AgentStorage must be present")
            .clone();
        let reopen = matches!(&self.mode, AgentBuildMode::Reopen(_));
        let info = match &self.mode {
            AgentBuildMode::Reopen(id) => match storage.agent_info(id.clone()).await {
                Ok(info)
                    if self
                        .config
                        .as_ref()
                        .expect("build configuration must remain owned")
                        .display_name()
                        .is_some_and(|name| Some(name) != info.display_name()) =>
                {
                    return Err(self
                        .cleanup_after_error(SdkError::conflict(
                            "configured agent display name differs from durable identity",
                        ))
                        .await);
                }
                Ok(info) => info,
                Err(error) => return Err(self.cleanup_after_error(error).await),
            },
            AgentBuildMode::Create => match AgentInfo::new(
                AgentId::new(),
                self.config
                    .as_ref()
                    .expect("build configuration must remain owned")
                    .display_name()
                    .map(str::to_owned),
            ) {
                Ok(info) => info,
                Err(error) => return Err(self.cleanup_after_error(error).await),
            },
        };

        self.claim = match storage.claim_agent(info.id()) {
            Ok(claim) => Some(claim),
            Err(error) => return Err(self.cleanup_after_error(error).await),
        };
        let knowledge = self
            .knowledge_database
            .take()
            .map(|database| ManagedKnowledge::new(storage.clone(), database));
        if reopen {
            if let Err(error) = storage
                .settle_knowledge_forgets(knowledge.as_ref(), info.id(), lifecycle_deadline)
                .await
            {
                return Err(self.cleanup_after_error(error).await);
            }
            if let Some(knowledge) = &knowledge {
                let owner = KnowledgeOwnerId::Agent {
                    agent_id: info.id().clone(),
                };
                let verification = match tokio::time::timeout_at(
                    lifecycle_deadline,
                    knowledge.verify(owner),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(SdkError::timeout(
                        "knowledge reconciliation deadline elapsed",
                    )),
                };
                if let Err(error) = verification {
                    return Err(self.cleanup_after_error(error).await);
                }
            }
        }

        let config = self
            .config
            .take()
            .expect("build configuration must transfer exactly once");
        let backend = config.backend().clone();
        let claim = self
            .claim
            .take()
            .expect("validated live-agent claim must transfer exactly once");
        let built = match backend {
            BackendConfig::Codex(_) => {
                let toolsets = std::mem::take(&mut self.toolsets);
                codex::agent::CodexAgent::build(
                    info, config, storage, knowledge, claim, toolsets, reopen,
                )
                .await
                .map(Agent::codex)
            }
            BackendConfig::OpenCode(_) => opencode::agent::OpenCodeAgent::build(
                info, config, storage, knowledge, claim, reopen,
            )
            .await
            .map(Agent::opencode),
            BackendConfig::Custom(_) => {
                let composition = custom::CustomAgent::<S, D, P, T>::compose(
                    self.provider
                        .take()
                        .expect("validated provider must transfer exactly once"),
                    std::mem::take(&mut self.toolsets),
                );
                custom::CustomAgent::build(
                    info,
                    config,
                    storage,
                    knowledge,
                    claim,
                    composition,
                    reopen,
                )
                .await
                .map(Agent::custom)
            }
        };
        let agent = match built {
            Ok(agent) => agent,
            Err(error) => return Err(self.cleanup_after_error(error).await),
        };
        self.agent = Some(agent);

        Ok(self
            .agent
            .take()
            .expect("constructed Agent must publish exactly once"))
    }

    async fn validate(&mut self) -> Result<(), SdkError> {
        let agent_config = self
            .config
            .as_ref()
            .expect("build configuration must remain owned");
        agent_config.validate()?;
        if tokio::time::Instant::now()
            .checked_add(agent_config.lifecycle_timeout())
            .is_none()
        {
            return Err(SdkError::invalid_config(
                "lifecycle timeout cannot be represented",
            ));
        }
        let has_provider = self.provider.is_some();
        let has_tools = !self.toolsets.is_empty();
        match agent_config.backend() {
            BackendConfig::Codex(_) => {
                if has_provider {
                    return Err(SdkError::invalid_config(
                        "selected runtime does not accept a model runtime",
                    ));
                }
                codex::tools::CodexTools::<S, T>::validate_registrations(&self.toolsets).await?;
            }
            BackendConfig::OpenCode(_) => {
                if has_provider {
                    return Err(SdkError::invalid_config(
                        "selected runtime does not accept a model runtime",
                    ));
                }
                if has_tools {
                    return Err(SdkError::unsupported(
                        "selected runtime does not accept executable tools",
                    ));
                }
            }
            BackendConfig::Custom(config) => {
                if !has_provider {
                    return Err(SdkError::invalid_config(
                        "selected runtime requires a configured model runtime",
                    ));
                }
                let catalog = self.validate_catalog().await?;
                Self::validate_custom_selection(config.default_tool_selection(), &catalog)?;
            }
        }
        Ok(())
    }

    async fn validate_catalog(&self) -> Result<BTreeSet<String>, SdkError> {
        let mut executable_names = BTreeSet::new();
        for toolset in &self.toolsets {
            for tool in toolset.list_tools().await? {
                let name = tool.name.as_str().to_owned();
                if !executable_names.insert(name.clone()) {
                    return Err(SdkError::invalid_config(
                        "configured tool names must be unique",
                    ));
                }
            }
        }

        Ok(executable_names)
    }

    fn validate_custom_selection(
        selection: &ToolSelection,
        executable_names: &BTreeSet<String>,
    ) -> Result<(), SdkError> {
        selection.validate()?;
        match selection {
            ToolSelection::Auto | ToolSelection::None => Ok(()),
            ToolSelection::Required if executable_names.is_empty() => {
                Err(SdkError::invalid_config(
                    "required tool selection needs at least one configured tool",
                ))
            }
            ToolSelection::Required => Ok(()),
            ToolSelection::Allowed(names) | ToolSelection::RequiredOneOf(names) => {
                Self::require_known_names(names, executable_names, "default selected tool")
            }
        }
    }

    fn require_known_names(
        requested: &[String],
        available: &BTreeSet<String>,
        label: &str,
    ) -> Result<(), SdkError> {
        let missing = requested
            .iter()
            .filter(|name| !available.contains(*name))
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(SdkError::invalid_config(format!(
                "{label} names must exist in the supplied Toolsets"
            )))
        }
    }

    async fn cleanup_after_error(mut self, mut primary: SdkError) -> SdkError {
        let cleanup_deadline = tokio::time::Instant::now()
            .checked_add(self.cleanup_timeout)
            .unwrap_or_else(tokio::time::Instant::now);
        if let Some(agent) = self.agent.take()
            && let Err(cleanup_error) = agent.shutdown().await
        {
            if self.tracing_enabled {
                tracing::warn!(
                    cleanup_target = "agent",
                    "agent build cleanup did not complete successfully"
                );
            }
            primary = primary
                .with_supplementary(cleanup_error.at_phase(crate::error::ErrorPhase::Cleanup));
        }

        drop(self.claim.take());
        while let Some(toolset) = self.toolsets.pop() {
            let cleanup =
                tokio::time::timeout_at(cleanup_deadline, async { toolset.shutdown().await }).await;
            if !matches!(cleanup, Ok(Ok(()))) && self.tracing_enabled {
                tracing::warn!(
                    cleanup_target = "toolset",
                    "agent build cleanup did not complete successfully"
                );
            }
            let error = match cleanup {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(SdkError::from(error)),
                Err(_) => Some(
                    SdkError::timeout("tool cleanup deadline elapsed").with_detail(
                        crate::error::ErrorDetail::DeadlineElapsed {
                            kind: crate::error::DeadlineKind::Lifecycle,
                            timeout: Some(self.cleanup_timeout),
                        },
                    ),
                ),
            };
            if let Some(error) = error {
                primary =
                    primary.with_supplementary(error.at_phase(crate::error::ErrorPhase::Cleanup));
            }
        }
        if let Some(provider) = self.provider.take() {
            let cleanup =
                tokio::time::timeout_at(cleanup_deadline, async { provider.shutdown().await })
                    .await;
            if !matches!(cleanup, Ok(Ok(()))) && self.tracing_enabled {
                tracing::warn!(
                    cleanup_target = "provider",
                    "agent build cleanup did not complete successfully"
                );
            }
            let error = match cleanup {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(SdkError::from_provider(&error)),
                Err(_) => Some(
                    SdkError::timeout("provider cleanup deadline elapsed").with_detail(
                        crate::error::ErrorDetail::DeadlineElapsed {
                            kind: crate::error::DeadlineKind::Lifecycle,
                            timeout: Some(self.cleanup_timeout),
                        },
                    ),
                ),
            };
            if let Some(error) = error {
                primary =
                    primary.with_supplementary(error.at_phase(crate::error::ErrorPhase::Cleanup));
            }
        }
        primary
    }
}

struct AgentBuildPublication<A> {
    result: Mutex<Option<Result<A, SdkError>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    AgentBuildPublication<Agent<S, D, P, T>>
{
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            handle: Mutex::new(None),
        }
    }

    async fn run(owner: AgentBuildOwner<S, D, P, T>) -> Result<Agent<S, D, P, T>, SdkError> {
        let publication = Arc::new(Self::new());
        let task_publication = Arc::clone(&publication);
        let (start_sender, start_receiver) = oneshot::channel();
        let (ready_sender, ready_receiver) = oneshot::channel();
        let (claimed_sender, claimed_receiver) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_receiver.await.is_err() {
                let _ = owner
                    .cleanup_after_error(SdkError::invalid_state(
                        "agent build owner could not start",
                    ))
                    .await;
                task_publication.finish();
                return;
            }

            let result = owner.build().await;
            let successful = result.is_ok();
            task_publication.publish(result);
            if ready_sender.send(()).is_err() {
                if successful {
                    task_publication.cleanup_unclaimed().await;
                }
                task_publication.finish();
                return;
            }
            if successful && claimed_receiver.await.is_err() {
                task_publication.cleanup_unclaimed().await;
            }
            task_publication.finish();
        });
        publication.install(handle);
        start_sender
            .send(())
            .map_err(|_| SdkError::invalid_state("agent build owner could not start"))?;
        ready_receiver
            .await
            .map_err(|_| SdkError::invalid_state("agent build owner terminated unexpectedly"))?;

        let mut agent = publication.take()?;
        agent.retain_storage_claim();
        if claimed_sender.send(()).is_err() {
            let primary =
                SdkError::invalid_state("agent build publication terminated unexpectedly")
                    .with_detail(crate::error::ErrorDetail::TaskTerminated)
                    .in_phase(crate::error::ErrorPhase::Publication);
            return Err(match agent.shutdown().await {
                Ok(()) => primary,
                Err(cleanup) => {
                    primary.with_supplementary(cleanup.at_phase(crate::error::ErrorPhase::Cleanup))
                }
            });
        }
        Ok(agent)
    }

    fn install(&self, handle: JoinHandle<()>) {
        *self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
    }

    fn publish(&self, result: Result<Agent<S, D, P, T>, SdkError>) {
        *self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
    }

    fn take(&self) -> Result<Agent<S, D, P, T>, SdkError> {
        self.result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .ok_or_else(|| SdkError::invalid_state("agent build result was not published"))?
    }

    async fn cleanup_unclaimed(&self) {
        let result = self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(Ok(agent)) = result {
            let _ = agent.shutdown().await;
        }
    }

    fn finish(&self) {
        self.handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}
