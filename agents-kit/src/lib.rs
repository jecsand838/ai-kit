//! Portable agent runtimes over Codex App Server, OpenCode, and a
//! provider-backed Custom implementation.
//!
//! The hierarchy is [`Agent`] → [`Workspace`] → [`Conversation`]. Implement
//! [`AgentBackend`], [`WorkspaceBackend`] and [`ConversationBackend`] directly to
//! supply your own concrete associated children. The supplied handles implement
//! those same traits.
//!
//! # Choose a task
//!
//! - Construct a supplied agent below, then create children with
//!   [`AgentBackend::new_workspace`] and [`WorkspaceBackend::new_conversation`].
//! - [Reopen a durable identity](AgentBuilder::reopen) with fresh integrations.
//! - [Consume a caller-owned conversation](ConversationBackend) without a service.
//! - [Stream native output and resolve interactions](conversation) with a configured backend.
//! - Build passive inputs in [`config`], [`workspace`], [`prompt`], [`model`],
//!   [`tool`] and [`knowledge`]; inspect failures through [`error`].
//!
//! Custom retains its direct Provider and Toolset implementations. Each toolset's
//! Handler stays associated and need not be Clone. Prompt streams observe accepted
//! work: dropping an observer neither cancels that work nor releases its slot.
//! Use abort or consuming lifecycle operations and keep the runtime alive through
//! cleanup. Native builders retain no provider value; Custom receives one through
//! [`AgentBuilder::with_provider`]. Exact Model continuations live only within an
//! active prompt loop; durable history contains provider-neutral text.
//!
//! # Construct and shut down a Custom agent
//!
//! This runs locally with memory Lance. Provider construction makes no model
//! request; prompting would require the configured Ollama service and exact model.
//! The result is inspected only after shutdown has been attempted.
//!
//! ```
//! use std::{num::NonZeroUsize, time::Duration};
//! use agents_kit::{AgentBackend, AgentBuilder, AgentStorage};
//! use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
//! use agents_kit::{model::ModelSelector, tool::ToolSelection};
//! use providers_kit::{Provider, ProviderConfig};
//! use providers_kit::config::{OllamaConfig, ProviderKind, TransportConfig};
//! use storage_kit::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
//! # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
//! let custom = CustomAgentConfig::new(
//!     ModelSelector::new("exact-model")?, ToolSelection::None,
//!     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny,
//! )?;
//! let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
//! let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default(),
//! ))?;
//! let agent = AgentBuilder::create(config, AgentStorage::new(store))
//!     .with_provider(provider).build().await?;
//! let id = agent.id();
//! let result = agent.info().await;
//! let cleanup = agent.shutdown().await;
//! let info = match (result, cleanup) {
//!     (Ok(info), Ok(())) => Ok(info),
//!     (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!     (Err(error), _) | (_, Err(error)) => Err(error),
//! }?;
//! assert_eq!(info.id(), &id);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

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
