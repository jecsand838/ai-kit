//! Portable agent runtimes over Codex App Server, OpenCode, and a
//! provider-backed Custom implementation.
//!
//! The public hierarchy is [`Agent`] → [`Workspace`] → [`Conversation`].
//! Supporting request and response values live in functional modules instead
//! of being flattened onto the crate root.
//!
//! Implement [`AgentBackend`], [`WorkspaceBackend`] and [`ConversationBackend`]
//! directly to supply a backend hierarchy. Associated children keep caller types
//! concrete; the supplied opaque handles implement those same contracts.
//!
//! Start with [`config`] and [`AgentBuilder`], supplying [`AgentStorage`] at
//! creation or explicit typed-ID reopen. [`workspace`] defines backend-host cwd,
//! roots, policy and revision-checked replacement. [`conversation`] and [`prompt`]
//! describe accepted work; [`notification`] covers durable acknowledgment and
//! interactions. [`identity`], [`model`], [`tool`] and [`knowledge`] hold passive
//! portable inputs, while [`error`] describes typed failure and cleanup context.
//!
//! Custom retains direct caller `ProviderBackend` and `ToolsetBackend` values,
//! using each toolset's associated Handler without a Clone requirement. Unary
//! and streaming prompts share one accepted-work owner. Dropping an observer
//! neither cancels that work nor releases its admission slot; use abort or the
//! consuming lifecycle operations and keep the async runtime alive through cleanup.
//!
//! # Walkthroughs
//!
//! | Goal | Start here |
//! | --- | --- |
//! | Build, close and reopen a supplied hierarchy | The local example below; [`AgentBuilder`] |
//! | Supply an exact native model and effort | [`model`] and [`config`] |
//! | Observe output, resolve durable interactions and cancel | [`conversation`] |
//! | Ingest local sources and choose retrieval scope | [`knowledge`] |
//! | Implement a caller-owned hierarchy | [`AgentBackend`]'s complete composition example |
//! | Supply concrete Provider and non-Clone Toolset children | [`AgentBuilder::with_provider`] |
//!
//! # Local construction, parent close and fresh-handle reopen
//!
//! This runs locally with an owned embedded Lance StateStore directory. Provider
//! construction is passive: submitting a prompt would require the configured
//! Ollama service and exact model. The directory and all runtime handles belong
//! to this example. Agent shutdown preserves quiescent durable records; closing
//! an individual conversation instead permanently terminates that conversation.
//!
//! Caller-built Local actions run in the caller process without Kit sandbox
//! enforcement. Custom therefore requires `FullAccess` and `Enabled` when actions
//! are configured, with read-write workspace roots; those roots do not confine the actions. This example's
//! `ping` action performs no filesystem or network I/O.
//!
//! ```
//! use std::{num::NonZeroUsize, time::Duration};
//! use agents_kit::{AgentBackend, AgentBuilder, AgentStorage, ConversationBackend, WorkspaceBackend};
//! use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
//! use agents_kit::conversation::NewConversation;
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
//!
//! let directory = std::env::temp_dir().join(format!("agent-rustdoc-{}", uuid::Uuid::new_v4()));
//! std::fs::create_dir(&directory)?;
//! let store_config = StateStoreConfig::Lance(
//!     LanceStateStoreConfig::directory(directory.clone()));
//! let provider_config = ProviderConfig::new(
//!     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default());
//! let config = AgentConfig::new(
//!     CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::Auto,
//!         CustomLoopLimits::new(4, 4, 1)?, CustomToolApproval::Deny)?.into(),
//!     Duration::from_secs(10), NonZeroUsize::MIN)?;
//! let fresh_tools = || -> Result<Toolset, tools_kit::ToolError> {
//!     Toolset::local(vec![ToolRegistration::function(
//!         ToolInfo::new(ToolName::new("ping")?,
//!             JsonSchema::strict(serde_json::json!({"type": "object"}))),
//!         |_| async { Ok(ToolResult::text("pong")) },
//!     )])
//! };
//! let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
//! let outcome = runtime.block_on(async {
//!     let store = StateStore::connect(store_config.clone()).await?;
//!     let agent = AgentBuilder::create(config.clone(), AgentStorage::new(store))
//!         .with_provider(Provider::from_config(provider_config.clone())?)
//!         .with_toolset(fresh_tools()?).build().await?;
//!     let agent_id = agent.id();
//!     let outcome = async {
//!         let cwd = std::env::current_dir()?;
//!         let context = WorkspaceContext::new(cwd.clone(),
//!             vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadWrite)?],
//!             WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::FullAccess,
//!                 WorkspaceNetworkAccess::Enabled))?;
//!         let workspace = agent.new_workspace(
//!             NewWorkspace::new("docs", context, WorkspaceDefaults::empty())?).await?;
//!         let conversation = workspace.new_conversation(
//!             NewConversation::new(Some("Review".into()), None, Vec::new())?).await?;
//!         let ids = (workspace.id(), conversation.id());
//!         assert_eq!(conversation.info().await?.title(), Some("Review"));
//!         workspace.close().await?; // invalidates runtime children, preserves records
//!         drop(conversation);
//!         Ok::<_, Box<dyn std::error::Error>>(ids)
//!     }.await;
//!     let shutdown = agent.shutdown().await;
//!     let (workspace_id, conversation_id) = outcome?;
//!     shutdown?;
//!
//!     // New handles: credentials, tools and storage are supplied again, not recovered.
//!     let store = StateStore::connect(store_config).await?;
//!     let reopened = AgentBuilder::reopen(config, AgentStorage::new(store), agent_id)
//!         .with_provider(Provider::from_config(provider_config)?)
//!         .with_toolset(fresh_tools()?).build().await?;
//!     let outcome = async {
//!         let workspace = reopened.get_workspace(workspace_id).await?;
//!         let conversation = workspace.get_conversation(conversation_id.clone()).await?;
//!         assert_eq!(conversation.id(), conversation_id);
//!         assert_eq!(conversation.info().await?.title(), Some("Review"));
//!         conversation.close().await?; // terminal conversation cleanup
//!         workspace.close().await?;
//!         Ok::<(), Box<dyn std::error::Error>>(())
//!     }.await;
//!     let shutdown = reopened.shutdown().await;
//!     outcome?;
//!     shutdown?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! });
//! // Retain data on failure; successful shutdown is required before deletion.
//! outcome?;
//! std::fs::remove_dir_all(directory)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Native Codex and OpenCode builders can select caller provider and tool types for
//! static upper composition with [`AgentBuilder::create_for_backends`] or
//! [`AgentBuilder::reopen_for_backends`]. Those methods retain no provider
//! value; Custom receives its concrete provider only through
//! [`AgentBuilder::with_provider`].
//!
//! Custom uses the selected Model's associated continuation type directly.
//! Returned assistant messages remain intact through one prompt's tool turns;
//! durable history contains only provider-neutral caller and final-assistant
//! text, so reopen does not reconstruct provider replay state.

mod agent;
mod agent_storage;
mod backends;
mod builder;
/// Agent construction settings and backend runtime policies.
pub mod config;
/// Conversation handles, backend contracts, and conversation requests and responses.
pub mod conversation;
mod core;
pub mod error;
mod file;
/// Validated identities and agent inventory information.
pub mod identity;
/// Knowledge sources, scopes, ingestion requests, and retrieval policies.
pub mod knowledge;
/// Model inventory, opaque selectors, and reasoning settings.
pub mod model;
/// Durable notifications, caller resolutions, and notification streams.
pub mod notification;
/// Prompt requests, responses, and incremental output streams.
pub mod prompt;
/// Portable tool selection and managed tool inventory.
pub mod tool;
/// Workspace handles, backend contracts, defaults, and workspace requests.
pub mod workspace;

pub use crate::agent::{Agent, AgentBackend};
pub use crate::agent_storage::AgentStorage;
pub use crate::builder::AgentBuilder;
pub use crate::conversation::{Conversation, ConversationBackend};
pub use crate::error::KitError;
pub use crate::workspace::{Workspace, WorkspaceBackend};

pub(crate) use crate::agent::AgentInfo;
pub(crate) use crate::config::{
    AgentConfig, BackendConfig, CodexConfig, CustomAgentConfig, CustomLoopLimits,
    CustomToolApproval, OpenCodeConfig,
};
pub(crate) use crate::conversation::{ConversationInfo, FileRead, NewConversation};
pub(crate) use crate::core::knowledge::ManagedKnowledge;
pub(crate) use crate::identity::{AgentId, ConversationId, NotificationId, WorkspaceId};
pub(crate) use crate::knowledge::{
    IngestKnowledgeRequest, KnowledgeBudget, KnowledgeContext, KnowledgeOwnerId, KnowledgePolicy,
    KnowledgeProvenance, KnowledgeRetrievalRequest, KnowledgeScope, KnowledgeSnippet,
    KnowledgeSource,
};
pub(crate) use crate::model::{ModelInfo, ModelSelector, PersonalitySelector, ReasoningEffort};
pub(crate) use crate::notification::{
    ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver,
    NotificationResolution, ProcessState,
};
pub(crate) use crate::prompt::{PromptRequest, PromptResponse, StructuredOutputRequest, UsageInfo};
pub(crate) use crate::tool::{ToolInfo, ToolSelection};
pub(crate) use crate::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceContext,
    WorkspaceDefaults, WorkspaceExecutionPolicy, WorkspaceInfo, WorkspaceNetworkAccess,
    WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode, WorkspaceUpdateReadiness,
};
