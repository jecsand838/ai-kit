use crate::error::ErrorKind;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Component, PathBuf};
use std::time::Duration;

use crate::backends::opencode::protocol::requests::MAX_STRUCTURED_SCHEMA_BYTES;
use crate::backends::opencode::protocol::schemas::{
    OpenCodeMessageUpdatedEvent, OpenCodePermissionRequest, OpenCodeProcessEffect,
    OpenCodeQuestionRequest,
};
use crate::backends::opencode::tools::OpenCodeCatalogGeneration;
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::{AgentId, ConversationId, KitError, NotificationId, PromptResponse, WorkspaceId};

const OPENCODE_RECORD_VERSION: u32 = 1;
const OPENCODE_ATTACHMENT_VERSION: u32 = 2;
const OPENCODE_WORKSPACE_ROUTING_VERSION: u32 = 1;
const OPENCODE_EVENT_PROGRESS_VERSION: u32 = 4;
const OPENCODE_CATALOG_BINDING_VERSION: u32 = 1;
const OPENCODE_CATALOG_STATE_VERSION: u32 = 1;
const OPENCODE_PROMPT_RECORD_VERSION: u32 = 5;
const OPENCODE_PROMPT_NAMESPACE: &str = "agents_kit_opencode_prompts_v1";
const OPENCODE_SESSION_BINDING_VERSION: u32 = 2;
const MAX_CATALOG_BINDINGS: usize = 65_536;
const MAX_CATALOG_STATE_BYTES: usize = 2 << 20;
const MAX_RECENT_EVENTS: usize = 4_096;
const MAX_EVENT_PROGRESS_BYTES: usize = 2 << 20;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeAttachment {
    version: u32,
    agent_id: AgentId,
    mode: OpenCodeAttachmentMode,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OpenCodeAttachmentMode {
    Spawn { database: PathBuf },
    External { base_url: String },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeWorkspaceRouting {
    version: u32,
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    workspace_revision: u64,
    directory: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeCatalogState {
    version: u32,
    agent_id: AgentId,
    bindings: BTreeMap<WorkspaceId, OpenCodeCatalogBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeCatalogBinding {
    version: u32,
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    workspace_revision: u64,
    directory: PathBuf,
    generation: OpenCodeCatalogGeneration,
    complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeSessionBinding {
    version: u32,
    conversation_id: ConversationId,
    workspace_id: WorkspaceId,
    workspace_revision: u64,
    directory: PathBuf,
    state: OpenCodeSessionState,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "phase", deny_unknown_fields)]
enum OpenCodeSessionState {
    Creating {
        correlation: String,
    },
    Bound {
        correlation: String,
        session_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "owner", content = "id")]
pub(crate) enum OpenCodeCleanupOwner {
    Workspace(WorkspaceId),
    Conversation(ConversationId),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeCleanup {
    version: u32,
    agent_id: AgentId,
    owner: OpenCodeCleanupOwner,
    directory: PathBuf,
    root_session_ids: Vec<String>,
    session_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodePromptRecord {
    version: u32,
    conversation_id: ConversationId,
    #[serde(rename = "message_id")]
    generation_id: String,
    request_part_id: String,
    input_observed: bool,
    structured_root: Option<OpenCodePromptRootFact>,
    structured_part: Option<OpenCodePromptPartFact>,
    structured_parent_conflict: bool,
    structured_parent_recovery_claimed: bool,
    effective: OpenCodePromptEffective,
    timeout: Option<OpenCodePromptTimeout>,
    phase: OpenCodePromptPhase,
    cancellation: OpenCodeCancellation,
    waiter: OpenCodePromptWaiter,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodePromptRootFact {
    session_id: String,
    message_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodePromptPartFact {
    session_id: String,
    message_id: String,
    part_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodePromptEffective {
    model: Option<OpenCodePromptModel>,
    structured_schema: Option<serde_json::Value>,
    tool_mode: OpenCodePromptToolMode,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodePromptModel {
    provider_id: String,
    model_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum OpenCodePromptToolMode {
    Automatic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct OpenCodePromptTimeout {
    seconds: u64,
    nanoseconds: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "phase", deny_unknown_fields)]
enum OpenCodePromptPhase {
    Prepared,
    DispatchedAwaitingAdmission,
    Admitted,
    ResponseLostBeforeAdmission,
    ResponseLostAfterAdmission,
    Terminal { outcome: OpenCodePromptOutcome },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct OpenCodePromptOutput {
    text: String,
    structured: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "outcome", deny_unknown_fields)]
enum OpenCodePromptOutcome {
    Success {
        output: Option<OpenCodePromptOutput>,
    },
    Cancelled,
    Protocol,
    InvalidState,
    ReconciledInvalidState,
    Integration,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
pub(crate) enum OpenCodeCancellation {
    NotRequested,
    Requested,
    Reserved { owner: OpenCodeCancellationOwner },
    Dispatching { owner: OpenCodeCancellationOwner },
    Accepted { owner: OpenCodeCancellationOwner },
    Rejected { owner: OpenCodeCancellationOwner },
    Ambiguous { owner: OpenCodeCancellationOwner },
    Unnecessary,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub(crate) struct OpenCodeCancellationOwner(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum OpenCodePromptWaiter {
    Waiting,
    Delivering,
    TimedOut,
    Lost,
    Delivered,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeContinuation {
    version: u32,
    conversation_id: ConversationId,
    notification_id: NotificationId,
    request: OpenCodeContinuationRequest,
    generation_id: Option<String>,
    state: OpenCodeContinuationState,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OpenCodeContinuationRequest {
    Permission(OpenCodePermissionRequest),
    Question(OpenCodeQuestionRequest),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenCodeContinuationKind {
    Permission,
    Question,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenCodeNativeResolution {
    Answered,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OpenCodeContinuationState {
    Pending,
    Ambiguous,
    Accepted,
    Cancelled,
    CancellationRequired,
    CancellationDispatched { owner: OpenCodeCancellationOwner },
    Closing { owner: OpenCodeCancellationOwner },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeEventProgress {
    version: u32,
    agent_id: AgentId,
    high_water: BTreeMap<String, u64>,
    recent: VecDeque<OpenCodeEventIdentity>,
    active_processes: BTreeSet<OpenCodeProcessOwner>,
    retired_sessions: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProcessOwner {
    session_id: String,
    message_id: String,
    part_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "source", deny_unknown_fields)]
pub(crate) enum OpenCodeEventIdentity {
    Normal {
        id: String,
        event_type: String,
        data: serde_json::Value,
    },
    Durable {
        history: bool,
        id: String,
        aggregate_id: String,
        sequence: u64,
        event_type: String,
        data: serde_json::Value,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenCodeEventDisposition {
    Applied,
    RememberedDuplicate,
    Duplicate,
    Gap,
}

impl OpenCodeAttachment {
    pub(crate) fn new(agent_id: AgentId, mode: OpenCodeAttachmentMode) -> Result<Self, KitError> {
        let attachment = Self {
            version: OPENCODE_ATTACHMENT_VERSION,
            agent_id,
            mode,
        };
        attachment.validate()?;
        Ok(attachment)
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != OPENCODE_ATTACHMENT_VERSION {
            return Err(KitError::invalid_state(
                "stored attachment version is unsupported",
            ));
        }
        match &self.mode {
            OpenCodeAttachmentMode::Spawn { database } => {
                let mut components = database.components();
                if !matches!(components.next(), Some(Component::Normal(_)))
                    || components.next().is_some()
                {
                    return Err(KitError::invalid_state("spawn attachment is invalid"));
                }
            }
            OpenCodeAttachmentMode::External { base_url } => {
                let url = reqwest::Url::parse(base_url)
                    .map_err(|_| KitError::invalid_state("attachment URL is invalid"))?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || !url.path().ends_with('/')
                    || url.as_str() != base_url
                {
                    return Err(KitError::invalid_state("attachment URL is invalid"));
                }
            }
        }
        Ok(())
    }
}

impl OpenCodeWorkspaceRouting {
    pub(crate) fn new(workspace: &WorkspaceRecord, directory: PathBuf) -> Result<Self, KitError> {
        let routing = Self {
            version: OPENCODE_WORKSPACE_ROUTING_VERSION,
            agent_id: workspace.agent_id().clone(),
            workspace_id: workspace.id().clone(),
            workspace_revision: workspace.revision(),
            directory,
        };
        routing.verify_owner(workspace)?;
        Ok(routing)
    }

    pub(crate) fn directory(&self) -> &std::path::Path {
        &self.directory
    }

    pub(crate) fn verify_owner(&self, workspace: &WorkspaceRecord) -> Result<(), KitError> {
        if self.version != OPENCODE_WORKSPACE_ROUTING_VERSION
            || self.agent_id != *workspace.agent_id()
            || self.workspace_id != *workspace.id()
            || self.workspace_revision != workspace.revision()
            || crate::workspace::WorkspaceRoot::new(
                self.directory.clone(),
                crate::workspace::WorkspaceRootAccess::ReadWrite,
            )
            .is_err()
        {
            return Err(KitError::invalid_state(
                "workspace routing contradicts its durable owner",
            ));
        }
        Ok(())
    }

    pub(crate) fn key(
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(
            "agents_kit_opencode_workspace_routing_v1",
            format!("{}:{}", agent_id.as_str(), workspace_id.as_str()),
        )
        .map_err(|_| KitError::invalid_state("workspace routing key is invalid"))
    }

    pub(crate) fn record(&self) -> Result<storage_kit::state::record::StateRecord, KitError> {
        Ok(storage_kit::state::record::StateRecord::new(
            Self::key(&self.agent_id, &self.workspace_id)?,
            serde_json::to_value(self)
                .map_err(|_| KitError::invalid_state("workspace routing could not be encoded"))?,
        ))
    }

    pub(crate) fn decode(
        snapshot: storage_kit::state::record::StateRecordSnapshot,
        workspace: &WorkspaceRecord,
    ) -> Result<Self, KitError> {
        if snapshot.record.key != Self::key(workspace.agent_id(), workspace.id())? {
            return Err(KitError::invalid_state(
                "workspace routing contradicts its key",
            ));
        }
        let routing: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| KitError::invalid_state("workspace routing is malformed"))?;
        routing.verify_owner(workspace)?;
        Ok(routing)
    }
}

impl OpenCodeCatalogState {
    pub(crate) fn new(agent_id: AgentId) -> Self {
        Self {
            version: OPENCODE_CATALOG_STATE_VERSION,
            agent_id,
            bindings: BTreeMap::new(),
        }
    }

    pub(crate) fn bind(&mut self, binding: OpenCodeCatalogBinding) -> Result<(), KitError> {
        binding.validate()?;
        if binding.agent_id() != &self.agent_id {
            return Err(KitError::invalid_state(
                "catalog binding has a foreign agent owner",
            ));
        }
        if !self.bindings.contains_key(binding.workspace_id())
            && self.bindings.len() >= MAX_CATALOG_BINDINGS
        {
            return Err(KitError::integration(
                "catalog binding inventory exceeds its configured bound",
            ));
        }
        self.bindings
            .insert(binding.workspace_id().clone(), binding);
        if serde_json::to_vec(self).map_or(true, |encoded| encoded.len() > MAX_CATALOG_STATE_BYTES)
        {
            return Err(KitError::integration(
                "catalog binding state exceeds its configured bound",
            ));
        }
        self.validate(&self.agent_id)
    }

    pub(crate) fn binding(&self, workspace_id: &WorkspaceId) -> Option<&OpenCodeCatalogBinding> {
        self.bindings.get(workspace_id)
    }

    pub(crate) fn invalidate_all(&mut self) -> Result<(), KitError> {
        self.bindings.clear();
        self.validate(&self.agent_id)
    }

    pub(crate) fn remove(&mut self, workspace_id: &WorkspaceId) -> Result<(), KitError> {
        self.bindings.remove(workspace_id);
        self.validate(&self.agent_id)
    }

    pub(crate) fn validate(&self, agent_id: &AgentId) -> Result<(), KitError> {
        if self.version != OPENCODE_CATALOG_STATE_VERSION
            || &self.agent_id != agent_id
            || self.bindings.len() > MAX_CATALOG_BINDINGS
            || serde_json::to_vec(self)
                .map_or(true, |encoded| encoded.len() > MAX_CATALOG_STATE_BYTES)
        {
            return Err(KitError::invalid_state("stored catalog state is malformed"));
        }
        for (workspace_id, binding) in &self.bindings {
            binding.validate()?;
            if binding.agent_id() != agent_id || binding.workspace_id() != workspace_id {
                return Err(KitError::invalid_state(
                    "stored catalog binding contradicts its owner",
                ));
            }
        }
        Ok(())
    }
}

impl OpenCodeCatalogBinding {
    pub(crate) fn complete(
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        directory: PathBuf,
        generation: OpenCodeCatalogGeneration,
    ) -> Result<Self, KitError> {
        let binding = Self {
            version: OPENCODE_CATALOG_BINDING_VERSION,
            agent_id,
            workspace_id,
            workspace_revision,
            directory,
            generation,
            complete: true,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    pub(crate) fn matches_generation(&self, generation: &OpenCodeCatalogGeneration) -> bool {
        self.complete && &self.generation == generation
    }

    pub(crate) fn verify_owner(
        &self,
        agent_id: &AgentId,
        workspace: &WorkspaceRecord,
        routing: &OpenCodeWorkspaceRouting,
    ) -> Result<(), KitError> {
        routing.verify_owner(workspace)?;
        if &self.agent_id != agent_id
            || &self.workspace_id != workspace.id()
            || self.workspace_revision != workspace.revision()
            || self.directory != routing.directory()
        {
            return Err(KitError::invalid_state(
                "catalog binding contradicts its workspace owner",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != OPENCODE_CATALOG_BINDING_VERSION
            || self.workspace_revision == 0
            || crate::workspace::WorkspaceRoot::new(
                self.directory.clone(),
                crate::workspace::WorkspaceRootAccess::ReadWrite,
            )
            .is_err()
            || self.generation.as_str().is_empty()
            || self.generation.as_str().trim() != self.generation.as_str()
            || !self.complete
        {
            return Err(KitError::invalid_state(
                "stored catalog binding is malformed",
            ));
        }
        Ok(())
    }
}

impl OpenCodeSessionBinding {
    pub(crate) fn creating(
        conversation_id: ConversationId,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        directory: PathBuf,
        correlation: String,
    ) -> Result<Self, KitError> {
        let binding = Self {
            version: OPENCODE_SESSION_BINDING_VERSION,
            conversation_id,
            workspace_id,
            workspace_revision,
            directory,
            state: OpenCodeSessionState::Creating { correlation },
        };
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn bind(&self, session_id: String) -> Result<Self, KitError> {
        let correlation = match &self.state {
            OpenCodeSessionState::Creating { correlation } => correlation.clone(),
            OpenCodeSessionState::Bound { .. } => {
                return Err(KitError::invalid_state("session binding is already bound"));
            }
        };
        let binding = Self {
            version: self.version,
            conversation_id: self.conversation_id.clone(),
            workspace_id: self.workspace_id.clone(),
            workspace_revision: self.workspace_revision,
            directory: self.directory.clone(),
            state: OpenCodeSessionState::Bound {
                correlation,
                session_id,
            },
        };
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    pub(crate) fn workspace_revision(&self) -> u64 {
        self.workspace_revision
    }

    pub(crate) fn correlation(&self) -> &str {
        match &self.state {
            OpenCodeSessionState::Creating { correlation }
            | OpenCodeSessionState::Bound { correlation, .. } => correlation,
        }
    }

    pub(crate) fn session_id(&self) -> Option<&str> {
        match &self.state {
            OpenCodeSessionState::Creating { .. } => None,
            OpenCodeSessionState::Bound { session_id, .. } => Some(session_id),
        }
    }

    pub(crate) fn require_session_id(&self) -> Result<&str, KitError> {
        self.session_id()
            .ok_or_else(|| KitError::invalid_state("conversation session is not bound"))
    }

    pub(crate) fn is_bound(&self) -> bool {
        matches!(self.state, OpenCodeSessionState::Bound { .. })
    }

    pub(crate) fn directory(&self) -> &std::path::Path {
        &self.directory
    }

    pub(crate) fn with_workspace_revision(&self, revision: u64) -> Result<Self, KitError> {
        let mut binding = self.clone();
        binding.workspace_revision = revision;
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn verify_owner(
        &self,
        conversation: &ConversationRecord,
        workspace: &WorkspaceRecord,
        routing: &OpenCodeWorkspaceRouting,
    ) -> Result<(), KitError> {
        routing.verify_owner(workspace)?;
        if &self.conversation_id != conversation.id()
            || &self.workspace_id != conversation.workspace_id()
            || &self.workspace_id != workspace.id()
            || self.workspace_revision != conversation.workspace_revision()
            || self.workspace_revision != workspace.revision()
            || self.directory != routing.directory()
        {
            return Err(KitError::invalid_state(
                "session binding contradicts its portable owner",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != OPENCODE_SESSION_BINDING_VERSION
            || self.workspace_revision == 0
            || crate::workspace::WorkspaceRoot::new(
                self.directory.clone(),
                crate::workspace::WorkspaceRootAccess::ReadWrite,
            )
            .is_err()
            || self.correlation().is_empty()
            || self.correlation().trim() != self.correlation()
            || self
                .session_id()
                .is_some_and(|session_id| session_id.is_empty() || session_id.trim() != session_id)
        {
            return Err(KitError::invalid_state(
                "stored session binding is malformed",
            ));
        }
        Ok(())
    }
}

impl OpenCodeCleanup {
    pub(crate) fn workspace(
        agent_id: AgentId,
        id: WorkspaceId,
        directory: PathBuf,
        root_session_ids: Vec<String>,
        session_ids: Vec<String>,
    ) -> Result<Self, KitError> {
        Self::new(
            agent_id,
            OpenCodeCleanupOwner::Workspace(id),
            directory,
            root_session_ids,
            session_ids,
        )
    }

    pub(crate) fn conversation(
        agent_id: AgentId,
        id: ConversationId,
        directory: PathBuf,
        root_session_id: String,
        session_ids: Vec<String>,
    ) -> Result<Self, KitError> {
        Self::new(
            agent_id,
            OpenCodeCleanupOwner::Conversation(id),
            directory,
            vec![root_session_id],
            session_ids,
        )
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn directory(&self) -> &std::path::Path {
        &self.directory
    }

    pub(crate) fn root_session_ids(&self) -> &[String] {
        &self.root_session_ids
    }

    pub(crate) fn session_ids(&self) -> &[String] {
        &self.session_ids
    }

    pub(crate) fn key_fragment(&self) -> String {
        match &self.owner {
            OpenCodeCleanupOwner::Workspace(id) => {
                format!("{}:workspace:{}", self.agent_id.as_str(), id.as_str())
            }
            OpenCodeCleanupOwner::Conversation(id) => {
                format!("{}:conversation:{}", self.agent_id.as_str(), id.as_str())
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != OPENCODE_RECORD_VERSION
            || crate::workspace::WorkspaceRoot::new(
                self.directory.clone(),
                crate::workspace::WorkspaceRootAccess::ReadWrite,
            )
            .is_err()
            || self.root_session_ids.is_empty()
            || self.session_ids.is_empty()
        {
            return Err(KitError::invalid_state(
                "stored cleanup obligation is malformed",
            ));
        }
        let mut sorted_roots = self.root_session_ids.clone();
        sorted_roots.sort();
        sorted_roots.dedup();
        let mut sorted_sessions = self.session_ids.clone();
        sorted_sessions.sort();
        sorted_sessions.dedup();
        if sorted_roots != self.root_session_ids
            || sorted_sessions != self.session_ids
            || self
                .root_session_ids
                .iter()
                .chain(&self.session_ids)
                .any(|id| id.is_empty() || id.trim() != id)
            || !self
                .root_session_ids
                .iter()
                .all(|id| self.session_ids.binary_search(id).is_ok())
        {
            return Err(KitError::invalid_state(
                "stored cleanup obligation is malformed",
            ));
        }
        Ok(())
    }

    pub(crate) fn matches_workspace(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        bindings: &[OpenCodeSessionBinding],
    ) -> bool {
        if self.agent_id() != agent_id
            || !matches!(&self.owner, OpenCodeCleanupOwner::Workspace(id) if id == workspace_id)
            || bindings.is_empty()
            || bindings
                .iter()
                .any(|binding| binding.directory() != self.directory())
        {
            return false;
        }
        let mut roots = bindings
            .iter()
            .filter_map(|binding| binding.session_id().map(str::to_owned))
            .collect::<Vec<_>>();
        if roots.len() != bindings.len() {
            return false;
        }
        roots.sort();
        roots.dedup();
        roots == self.root_session_ids
    }

    pub(crate) fn matches_conversation(
        &self,
        agent_id: &AgentId,
        conversation_id: &ConversationId,
        binding: &OpenCodeSessionBinding,
    ) -> bool {
        self.agent_id() == agent_id
            && matches!(&self.owner, OpenCodeCleanupOwner::Conversation(id) if id == conversation_id)
            && self.directory() == binding.directory()
            && binding
                .session_id()
                .is_some_and(|session_id| self.root_session_ids == [session_id])
    }

    pub(crate) fn matches_creation(
        &self,
        agent_id: &AgentId,
        conversation_id: &ConversationId,
        creating: &OpenCodeSessionBinding,
        root_session_id: &str,
    ) -> bool {
        self.agent_id() == agent_id
            && matches!(&self.owner, OpenCodeCleanupOwner::Conversation(id) if id == conversation_id)
            && !creating.is_bound()
            && creating.conversation_id() == conversation_id
            && self.directory() == creating.directory()
            && self.root_session_ids == [root_session_id]
            && self
                .session_ids
                .binary_search_by(|candidate| candidate.as_str().cmp(root_session_id))
                .is_ok()
    }

    fn new(
        agent_id: AgentId,
        owner: OpenCodeCleanupOwner,
        directory: PathBuf,
        mut root_session_ids: Vec<String>,
        mut session_ids: Vec<String>,
    ) -> Result<Self, KitError> {
        root_session_ids.sort();
        root_session_ids.dedup();
        session_ids.sort();
        session_ids.dedup();
        let cleanup = Self {
            version: OPENCODE_RECORD_VERSION,
            agent_id,
            owner,
            directory,
            root_session_ids,
            session_ids,
        };
        cleanup.validate()?;
        Ok(cleanup)
    }
}

impl OpenCodePromptModel {
    pub(crate) fn new(provider_id: String, model_id: String) -> Result<Self, KitError> {
        let model = Self {
            provider_id,
            model_id,
        };
        model.validate()?;
        Ok(model)
    }

    fn validate(&self) -> Result<(), KitError> {
        if self.provider_id.is_empty()
            || self.provider_id.trim() != self.provider_id
            || self.model_id.is_empty()
            || self.model_id.trim() != self.model_id
        {
            return Err(KitError::invalid_state("stored prompt model is malformed"));
        }
        Ok(())
    }
}

impl OpenCodePromptEffective {
    pub(crate) fn new(
        model: Option<OpenCodePromptModel>,
        structured_schema: Option<serde_json::Value>,
    ) -> Result<Self, KitError> {
        let effective = Self {
            model,
            structured_schema,
            tool_mode: OpenCodePromptToolMode::Automatic,
        };
        effective.validate()?;
        Ok(effective)
    }

    fn validate(&self) -> Result<(), KitError> {
        if let Some(model) = &self.model {
            model.validate()?;
        }
        if let Some(schema) = &self.structured_schema {
            let encoded = serde_json::to_vec(schema)
                .map_err(|_| KitError::invalid_state("stored structured schema is malformed"))?;
            if encoded.len() > MAX_STRUCTURED_SCHEMA_BYTES {
                return Err(KitError::invalid_state(
                    "stored structured schema exceeds its configured bound",
                ));
            }
        }
        if self
            .structured_schema
            .as_ref()
            .is_some_and(|schema| schema.as_object().is_none_or(serde_json::Map::is_empty))
            || self.tool_mode != OpenCodePromptToolMode::Automatic
        {
            return Err(KitError::invalid_state(
                "stored effective prompt selection is malformed",
            ));
        }
        Ok(())
    }
}

impl OpenCodePromptTimeout {
    fn from_duration(timeout: Duration) -> Result<Self, KitError> {
        let timeout = Self {
            seconds: timeout.as_secs(),
            nanoseconds: timeout.subsec_nanos(),
        };
        timeout.validate()?;
        Ok(timeout)
    }

    fn validate(&self) -> Result<(), KitError> {
        if (self.seconds == 0 && self.nanoseconds == 0) || self.nanoseconds >= 1_000_000_000 {
            return Err(KitError::invalid_state(
                "stored prompt timeout is malformed",
            ));
        }
        Ok(())
    }
}

impl OpenCodePromptRootFact {
    fn new(session_id: &str, message_id: &str) -> Result<Self, KitError> {
        let fact = Self {
            session_id: session_id.to_owned(),
            message_id: message_id.to_owned(),
        };
        fact.validate()?;
        Ok(fact)
    }

    fn matches(&self, expected_session_id: &str, expected_message_id: &str) -> bool {
        self.session_id == expected_session_id && self.message_id == expected_message_id
    }

    fn validate(&self) -> Result<(), KitError> {
        if !self.session_id.starts_with("ses")
            || self.session_id.trim() != self.session_id
            || !self.message_id.starts_with("msg")
            || self.message_id.trim() != self.message_id
        {
            return Err(KitError::invalid_state(
                "stored structured-parent message fact is malformed",
            ));
        }
        Ok(())
    }
}

impl OpenCodePromptPartFact {
    fn new(session_id: &str, message_id: &str, part_id: &str) -> Result<Self, KitError> {
        let fact = Self {
            session_id: session_id.to_owned(),
            message_id: message_id.to_owned(),
            part_id: part_id.to_owned(),
        };
        fact.validate()?;
        Ok(fact)
    }

    fn same_owner(&self, root: &OpenCodePromptRootFact) -> bool {
        self.session_id == root.session_id && self.message_id == root.message_id
    }

    fn matches(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
        expected_part_id: &str,
    ) -> bool {
        self.session_id == expected_session_id
            && self.message_id == expected_message_id
            && self.part_id == expected_part_id
    }

    fn validate(&self) -> Result<(), KitError> {
        if !self.session_id.starts_with("ses")
            || self.session_id.trim() != self.session_id
            || !self.message_id.starts_with("msg")
            || self.message_id.trim() != self.message_id
            || !self.part_id.starts_with("prt")
            || self.part_id.trim() != self.part_id
        {
            return Err(KitError::invalid_state(
                "stored structured-parent part fact is malformed",
            ));
        }
        Ok(())
    }
}

impl OpenCodePromptRecord {
    pub(crate) fn key(
        conversation_id: &ConversationId,
    ) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(
            OPENCODE_PROMPT_NAMESPACE,
            conversation_id.as_str(),
        )
        .map_err(|_| KitError::invalid_state("prompt key is invalid"))
    }

    pub(crate) fn decode(
        snapshot: storage_kit::state::record::StateRecordSnapshot,
        conversation_id: &ConversationId,
    ) -> Result<Self, KitError> {
        let key = snapshot.record.key.key().to_owned();
        let record: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| KitError::invalid_state("stored prompt state is malformed"))?;
        record.validate()?;
        if record.conversation_id() != conversation_id || Self::key(conversation_id)?.key() != key {
            return Err(KitError::invalid_state(
                "stored prompt contradicts its key owner",
            ));
        }
        Ok(record)
    }

    pub(crate) fn prepared(
        conversation_id: ConversationId,
        generation_id: String,
        request_part_id: String,
        effective: OpenCodePromptEffective,
        timeout: Option<Duration>,
    ) -> Result<Self, KitError> {
        let record = Self {
            version: OPENCODE_PROMPT_RECORD_VERSION,
            conversation_id,
            generation_id,
            request_part_id,
            input_observed: false,
            structured_root: None,
            structured_part: None,
            structured_parent_conflict: false,
            structured_parent_recovery_claimed: false,
            effective,
            timeout: timeout
                .map(OpenCodePromptTimeout::from_duration)
                .transpose()?,
            phase: OpenCodePromptPhase::Prepared,
            cancellation: OpenCodeCancellation::NotRequested,
            waiter: OpenCodePromptWaiter::Waiting,
        };
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn generation_id(&self) -> &str {
        &self.generation_id
    }

    pub(crate) fn request_part_id(&self) -> &str {
        &self.request_part_id
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        matches!(
            self.phase,
            OpenCodePromptPhase::Terminal {
                outcome: OpenCodePromptOutcome::Cancelled
            }
        )
    }

    pub(crate) fn mark_dispatched_awaiting_admission(&mut self) -> Result<bool, KitError> {
        if matches!(&self.phase, OpenCodePromptPhase::Prepared) {
            if self.cancellation == OpenCodeCancellation::Requested {
                self.phase = OpenCodePromptPhase::Terminal {
                    outcome: OpenCodePromptOutcome::Cancelled,
                };
                self.cancellation = OpenCodeCancellation::Unnecessary;
                self.validate()?;
                return Ok(false);
            }
            self.phase = OpenCodePromptPhase::DispatchedAwaitingAdmission;
            self.validate()?;
            return Ok(true);
        }
        if matches!(
            &self.phase,
            OpenCodePromptPhase::DispatchedAwaitingAdmission
        ) {
            return Ok(true);
        }
        Err(KitError::conflict(
            "prompt dispatch boundary already advanced",
        ))
    }

    pub(crate) fn stop_before_dispatch(&mut self) -> Result<bool, KitError> {
        if matches!(
            &self.phase,
            OpenCodePromptPhase::Terminal {
                outcome: OpenCodePromptOutcome::Cancelled,
            }
        ) && self.cancellation == OpenCodeCancellation::Unnecessary
        {
            return self.validate().map(|()| true);
        }
        if !matches!(
            &self.phase,
            OpenCodePromptPhase::Prepared | OpenCodePromptPhase::DispatchedAwaitingAdmission
        ) {
            return Err(KitError::conflict(
                "prompt has already crossed its native dispatch boundary",
            ));
        }
        match &self.cancellation {
            OpenCodeCancellation::NotRequested => Ok(false),
            OpenCodeCancellation::Requested => {
                self.phase = OpenCodePromptPhase::Terminal {
                    outcome: OpenCodePromptOutcome::Cancelled,
                };
                self.cancellation = OpenCodeCancellation::Unnecessary;
                self.validate()?;
                Ok(true)
            }
            OpenCodeCancellation::Reserved { .. }
            | OpenCodeCancellation::Dispatching { .. }
            | OpenCodeCancellation::Accepted { .. }
            | OpenCodeCancellation::Rejected { .. }
            | OpenCodeCancellation::Ambiguous { .. }
            | OpenCodeCancellation::Unnecessary => Err(KitError::invalid_state(
                "prompt cancellation advanced before native admission",
            )),
        }
    }

    pub(crate) fn is_prepared(&self) -> bool {
        matches!(&self.phase, OpenCodePromptPhase::Prepared)
    }

    pub(crate) fn matches_prepared(&self, expected: &Self) -> bool {
        if !self.is_prepared()
            || !matches!(
                &self.cancellation,
                OpenCodeCancellation::NotRequested | OpenCodeCancellation::Requested
            )
        {
            return false;
        }
        let mut normalized = self.clone();
        normalized.cancellation = OpenCodeCancellation::NotRequested;
        normalized.waiter = OpenCodePromptWaiter::Waiting;
        normalized.input_observed = false;
        normalized.structured_root = None;
        normalized.structured_part = None;
        normalized.structured_parent_conflict = false;
        normalized.structured_parent_recovery_claimed = false;
        &normalized == expected
    }

    pub(crate) fn observe_prompt_message(
        &mut self,
        session_id: &str,
        message_id: &str,
    ) -> Result<bool, KitError> {
        if self.effective.structured_schema.is_none()
            || matches!(&self.phase, OpenCodePromptPhase::Terminal { .. })
        {
            return Ok(false);
        }
        if matches!(&self.phase, OpenCodePromptPhase::Prepared) {
            return Err(KitError::invalid_state(
                "structured-parent message predates native dispatch",
            ));
        }
        let fact = OpenCodePromptRootFact::new(session_id, message_id)?;
        if self
            .structured_part
            .as_ref()
            .is_some_and(|part| !part.same_owner(&fact))
        {
            return Ok(false);
        }
        let changed = match &self.structured_root {
            None => {
                self.structured_root = Some(fact.clone());
                true
            }
            Some(current) if current == &fact => false,
            Some(_) => {
                self.structured_root = Some(fact.clone());
                true
            }
        };
        self.validate()?;
        Ok(changed)
    }

    pub(crate) fn observe_input_part(
        &mut self,
        session_id: &str,
        message_id: &str,
        part_id: &str,
    ) -> Result<bool, KitError> {
        if self.request_part_id != part_id {
            return Ok(false);
        }
        if matches!(&self.phase, OpenCodePromptPhase::Prepared) {
            return Err(KitError::invalid_state(
                "prompt request part predates native dispatch",
            ));
        }
        if matches!(
            &self.phase,
            OpenCodePromptPhase::DispatchedAwaitingAdmission
                | OpenCodePromptPhase::ResponseLostBeforeAdmission
                | OpenCodePromptPhase::Admitted
                | OpenCodePromptPhase::ResponseLostAfterAdmission
        ) {
            let mut changed = !self.input_observed;
            self.input_observed = true;
            if self.effective.structured_schema.is_some() {
                let fact = OpenCodePromptPartFact::new(session_id, message_id, part_id)?;
                match &self.structured_part {
                    None => {
                        self.structured_part = Some(fact.clone());
                        changed = true;
                    }
                    Some(current) if current == &fact => {}
                    Some(_) => {
                        self.structured_parent_conflict = true;
                        changed = true;
                    }
                }
                if let Some(root) = &self.structured_root
                    && !fact.same_owner(root)
                {
                    self.structured_root = None;
                    changed = true;
                }
            }
            self.validate()?;
            return Ok(changed);
        }
        Ok(false)
    }

    pub(crate) fn structured_parent_proof(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
        expected_part_id: &str,
    ) -> Result<bool, KitError> {
        if self.effective.structured_schema.is_none() {
            return Err(KitError::invalid_state(
                "plain prompt requested structured-parent proof",
            ));
        }
        if self.structured_parent_conflict {
            return Err(KitError::invalid_state(
                "structured-parent evidence is contradictory",
            ));
        }
        if let Some(root) = &self.structured_root
            && !root.matches(expected_session_id, expected_message_id)
        {
            return Err(KitError::invalid_state(
                "structured parent belongs to another prompt",
            ));
        }
        if let Some(part) = &self.structured_part
            && !part.matches(expected_session_id, expected_message_id, expected_part_id)
        {
            return Err(KitError::invalid_state(
                "structured request part belongs to another prompt",
            ));
        }
        Ok(self.structured_root.is_some() && self.structured_part.is_some())
    }

    pub(crate) fn claim_structured_parent_recovery(&mut self) -> Result<bool, KitError> {
        if self.effective.structured_schema.is_none()
            || matches!(
                &self.phase,
                OpenCodePromptPhase::Prepared | OpenCodePromptPhase::Terminal { .. }
            )
        {
            return Err(KitError::invalid_state(
                "structured-parent recovery has no live owner",
            ));
        }
        if self.structured_parent_conflict {
            return Err(KitError::invalid_state(
                "structured-parent recovery has contradictory evidence",
            ));
        }
        if self.structured_parent_recovery_claimed {
            return Ok(false);
        }
        self.structured_parent_recovery_claimed = true;
        self.validate()?;
        Ok(true)
    }

    pub(crate) fn structured_parent_recovery_claimed(&self) -> bool {
        self.structured_parent_recovery_claimed
    }

    pub(crate) fn observe_active_status(&mut self) -> Result<bool, KitError> {
        self.observe_admission_evidence()
    }

    pub(crate) fn observe_interaction_admission(&mut self) -> Result<bool, KitError> {
        self.observe_admission_evidence()
    }

    fn observe_admission_evidence(&mut self) -> Result<bool, KitError> {
        if !self.input_observed || matches!(&self.phase, OpenCodePromptPhase::Terminal { .. }) {
            return Ok(false);
        }
        self.mark_admitted()?;
        Ok(true)
    }

    pub(crate) fn admission_observed(&self) -> bool {
        matches!(
            &self.phase,
            OpenCodePromptPhase::Admitted
                | OpenCodePromptPhase::ResponseLostAfterAdmission
                | OpenCodePromptPhase::Terminal { .. }
        )
    }

    pub(crate) fn mark_admitted(&mut self) -> Result<(), KitError> {
        if !self.input_observed {
            return Err(KitError::invalid_state(
                "native prompt admission lacks its exact input-part evidence",
            ));
        }
        self.phase = match &self.phase {
            OpenCodePromptPhase::DispatchedAwaitingAdmission => OpenCodePromptPhase::Admitted,
            OpenCodePromptPhase::ResponseLostBeforeAdmission => {
                OpenCodePromptPhase::ResponseLostAfterAdmission
            }
            OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission => {
                return Ok(());
            }
            OpenCodePromptPhase::Prepared | OpenCodePromptPhase::Terminal { .. } => {
                return Err(KitError::invalid_state(
                    "native prompt admission contradicts durable state",
                ));
            }
        };
        self.validate()
    }

    pub(crate) fn terminal(
        &mut self,
        result: &Result<PromptResponse, KitError>,
    ) -> Result<(), KitError> {
        let retain_output = matches!(
            self.waiter,
            OpenCodePromptWaiter::Waiting
                | OpenCodePromptWaiter::Delivering
                | OpenCodePromptWaiter::Delivered
        );
        let outcome = match result {
            Ok(response) => OpenCodePromptOutcome::Success {
                output: retain_output.then(|| OpenCodePromptOutput {
                    text: response.output().to_owned(),
                    structured: response.structured_output().cloned(),
                }),
            },
            Err(error) if error.kind() == ErrorKind::Cancelled => OpenCodePromptOutcome::Cancelled,
            Err(error) if error.kind() == ErrorKind::Protocol => OpenCodePromptOutcome::Protocol,
            Err(error) if error.kind() == ErrorKind::InvalidState => {
                OpenCodePromptOutcome::InvalidState
            }
            Err(_) => OpenCodePromptOutcome::Integration,
        };
        if let OpenCodePromptPhase::Terminal { outcome: current } = &self.phase {
            if current == &outcome {
                return self.validate();
            }
            return Err(KitError::invalid_state(
                "prompt terminal contradicts its committed outcome",
            ));
        }
        if !matches!(
            &self.phase,
            OpenCodePromptPhase::DispatchedAwaitingAdmission | OpenCodePromptPhase::Admitted
        ) {
            return Err(KitError::invalid_state(
                "prompt terminal arrived outside its live response owner",
            ));
        }
        self.phase = OpenCodePromptPhase::Terminal { outcome };
        if !matches!(
            &self.cancellation,
            OpenCodeCancellation::Dispatching { .. } | OpenCodeCancellation::Ambiguous { .. }
        ) {
            self.cancellation = OpenCodeCancellation::Unnecessary;
        }
        self.validate()
    }

    pub(crate) fn terminal_without_cancellation_dispatch(
        &mut self,
        owner: &OpenCodeCancellationOwner,
        result: &Result<PromptResponse, KitError>,
    ) -> Result<(), KitError> {
        if self.is_terminal() {
            return self.terminal(result);
        }
        if !matches!(
            &self.cancellation,
            OpenCodeCancellation::Reserved { owner: current } if current == owner
        ) {
            return Err(KitError::invalid_state(
                "prompt cancellation no-send proof changed ownership",
            ));
        }
        self.cancellation = OpenCodeCancellation::Requested;
        self.terminal(result)
    }

    pub(crate) fn lose_response(&mut self) -> Result<(), KitError> {
        self.phase = match &self.phase {
            OpenCodePromptPhase::DispatchedAwaitingAdmission => {
                OpenCodePromptPhase::ResponseLostBeforeAdmission
            }
            OpenCodePromptPhase::Admitted => OpenCodePromptPhase::ResponseLostAfterAdmission,
            OpenCodePromptPhase::ResponseLostBeforeAdmission
            | OpenCodePromptPhase::ResponseLostAfterAdmission => {
                let _ = self.begin_cancellation()?;
                return self.validate();
            }
            _ => {
                return Err(KitError::invalid_state(
                    "prompt response ownership is not live",
                ));
            }
        };
        let _ = self.begin_cancellation()?;
        self.validate()
    }

    pub(crate) fn mark_waiter_timed_out(&mut self) -> Result<bool, KitError> {
        if self.response_ready_for_delivery() {
            return Ok(false);
        }
        match self.waiter {
            OpenCodePromptWaiter::Waiting => {
                self.waiter = OpenCodePromptWaiter::TimedOut;
                self.discard_terminal_output();
                let _ = self.begin_cancellation()?;
                self.validate()?;
                Ok(true)
            }
            OpenCodePromptWaiter::TimedOut => {
                let _ = self.begin_cancellation()?;
                Ok(true)
            }
            OpenCodePromptWaiter::Delivering
            | OpenCodePromptWaiter::Lost
            | OpenCodePromptWaiter::Delivered => Ok(false),
        }
    }

    pub(crate) fn committed_timeout_result(&self) -> Option<bool> {
        if self.waiter == OpenCodePromptWaiter::TimedOut {
            Some(true)
        } else if self.response_ready_for_delivery() || self.waiter != OpenCodePromptWaiter::Waiting
        {
            Some(false)
        } else {
            None
        }
    }

    pub(crate) fn claim_waiter_delivery(&mut self) -> Result<bool, KitError> {
        if !self.response_ready_for_delivery() {
            return Ok(false);
        }
        match self.waiter {
            OpenCodePromptWaiter::Waiting => {
                self.waiter = OpenCodePromptWaiter::Delivering;
                self.validate()?;
                Ok(true)
            }
            OpenCodePromptWaiter::Delivering => Ok(true),
            OpenCodePromptWaiter::TimedOut
            | OpenCodePromptWaiter::Lost
            | OpenCodePromptWaiter::Delivered => Ok(false),
        }
    }

    pub(crate) fn mark_waiter_lost(&mut self) -> Result<bool, KitError> {
        match self.waiter {
            OpenCodePromptWaiter::Waiting | OpenCodePromptWaiter::Delivering => {
                self.waiter = OpenCodePromptWaiter::Lost;
                self.discard_terminal_output();
                self.validate()?;
                Ok(true)
            }
            OpenCodePromptWaiter::TimedOut
            | OpenCodePromptWaiter::Lost
            | OpenCodePromptWaiter::Delivered => Ok(false),
        }
    }

    pub(crate) fn settle_waiter_delivery(&mut self, delivered: bool) -> Result<(), KitError> {
        match (self.waiter, delivered) {
            (OpenCodePromptWaiter::Delivering, true) => {
                self.waiter = OpenCodePromptWaiter::Delivered;
                self.validate()
            }
            (OpenCodePromptWaiter::Delivering, false) => {
                self.waiter = OpenCodePromptWaiter::Lost;
                self.discard_terminal_output();
                self.validate()
            }
            (OpenCodePromptWaiter::Lost, false) | (OpenCodePromptWaiter::Delivered, true) => Ok(()),
            (OpenCodePromptWaiter::Waiting, _)
            | (OpenCodePromptWaiter::TimedOut, _)
            | (OpenCodePromptWaiter::Lost, true)
            | (OpenCodePromptWaiter::Delivered, false) => Err(KitError::conflict(
                "prompt waiter has a contradictory delivery outcome",
            )),
        }
    }

    pub(crate) fn cancellation_intent_recorded(&self) -> bool {
        self.cancellation != OpenCodeCancellation::NotRequested
    }

    pub(crate) fn begin_cancellation(&mut self) -> Result<bool, KitError> {
        if self.is_terminal() && self.cancellation_releasable() {
            return Ok(false);
        }
        if self.cancellation == OpenCodeCancellation::NotRequested {
            self.cancellation = OpenCodeCancellation::Requested;
            self.validate()?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn reserve_cancellation(
        &mut self,
        owner: &OpenCodeCancellationOwner,
    ) -> Result<bool, KitError> {
        if matches!(
            &self.phase,
            OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission
        ) && let OpenCodeCancellation::Reserved { owner: current } = &self.cancellation
        {
            return Ok(current == owner);
        }
        if self.cancellation != OpenCodeCancellation::Requested
            || !matches!(
                &self.phase,
                OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission
            )
        {
            return Ok(false);
        }
        self.cancellation = OpenCodeCancellation::Reserved {
            owner: owner.clone(),
        };
        self.validate()?;
        Ok(true)
    }

    pub(crate) fn cancellation_dispatch_pending(&self) -> bool {
        matches!(
            &self.cancellation,
            OpenCodeCancellation::Requested | OpenCodeCancellation::Reserved { .. }
        ) && matches!(
            &self.phase,
            OpenCodePromptPhase::DispatchedAwaitingAdmission
                | OpenCodePromptPhase::Admitted
                | OpenCodePromptPhase::ResponseLostBeforeAdmission
                | OpenCodePromptPhase::ResponseLostAfterAdmission
        )
    }

    pub(crate) fn reserve_recovered_cancellation(
        &mut self,
        owner: &OpenCodeCancellationOwner,
    ) -> Result<bool, KitError> {
        if matches!(
            &self.phase,
            OpenCodePromptPhase::DispatchedAwaitingAdmission
                | OpenCodePromptPhase::ResponseLostBeforeAdmission
        ) {
            self.mark_admitted()?;
        }
        if let OpenCodeCancellation::Reserved { owner: current } = &mut self.cancellation {
            *current = owner.clone();
            self.validate()?;
            return Ok(true);
        }
        self.reserve_cancellation(owner)
    }

    pub(crate) fn begin_cancellation_dispatch(
        &mut self,
        owner: &OpenCodeCancellationOwner,
    ) -> Result<bool, KitError> {
        if matches!(
            &self.cancellation,
            OpenCodeCancellation::Dispatching { owner: current } if current == owner
        ) {
            return Ok(true);
        }
        if !matches!(
            &self.cancellation,
            OpenCodeCancellation::Reserved { owner: current } if current == owner
        ) {
            return Ok(false);
        }
        self.cancellation = OpenCodeCancellation::Dispatching {
            owner: owner.clone(),
        };
        self.validate()?;
        Ok(true)
    }

    pub(crate) fn cancellation_is_requested(&self) -> bool {
        self.cancellation == OpenCodeCancellation::Requested
    }

    pub(crate) fn cancellation_is_reserved_by(&self, owner: &OpenCodeCancellationOwner) -> bool {
        matches!(
            &self.cancellation,
            OpenCodeCancellation::Reserved { owner: current } if current == owner
        )
    }

    pub(crate) fn cancellation_dispatch_is_owned_by(
        &self,
        owner: &OpenCodeCancellationOwner,
    ) -> bool {
        matches!(
            &self.cancellation,
            OpenCodeCancellation::Dispatching { owner: current } if current == owner
        )
    }

    pub(crate) fn release_cancellation_claim(
        &mut self,
        owner: &OpenCodeCancellationOwner,
    ) -> Result<bool, KitError> {
        if self.cancellation == OpenCodeCancellation::Requested {
            return Ok(true);
        }
        if matches!(
            &self.cancellation,
            OpenCodeCancellation::Reserved { owner: current } if current == owner
        ) {
            self.cancellation = OpenCodeCancellation::Requested;
            self.validate()?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn settle_cancellation(
        &mut self,
        outcome: OpenCodeCancellation,
    ) -> Result<(), KitError> {
        let outcome_owner = match &outcome {
            OpenCodeCancellation::Accepted { owner }
            | OpenCodeCancellation::Rejected { owner }
            | OpenCodeCancellation::Ambiguous { owner } => owner,
            OpenCodeCancellation::NotRequested
            | OpenCodeCancellation::Requested
            | OpenCodeCancellation::Reserved { .. }
            | OpenCodeCancellation::Dispatching { .. }
            | OpenCodeCancellation::Unnecessary => {
                return Err(KitError::invalid_state(
                    "prompt cancellation outcome is invalid",
                ));
            }
        };
        if self.is_terminal() && self.cancellation == OpenCodeCancellation::Unnecessary {
            return self.validate();
        }
        if self.cancellation == outcome {
            return self.validate();
        }
        if !matches!(
            &self.cancellation,
            OpenCodeCancellation::Dispatching { owner } if owner == outcome_owner
        ) {
            return Err(KitError::invalid_state(
                "cancellation dispatch is not in flight",
            ));
        }
        self.cancellation = outcome;
        self.validate()
    }

    pub(crate) fn cancellation_accepted(&self) -> bool {
        matches!(&self.cancellation, OpenCodeCancellation::Accepted { .. })
    }

    pub(crate) fn terminalize_accepted_cancellation(&mut self) -> Result<(), KitError> {
        if matches!(
            &self.phase,
            OpenCodePromptPhase::Terminal {
                outcome: OpenCodePromptOutcome::Cancelled,
            }
        ) && self.cancellation == OpenCodeCancellation::Unnecessary
        {
            return self.validate();
        }
        if !self.cancellation_accepted()
            || !matches!(
                &self.phase,
                OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission
            )
        {
            return Err(KitError::invalid_state(
                "accepted prompt cancellation contradicts durable state",
            ));
        }
        self.phase = OpenCodePromptPhase::Terminal {
            outcome: OpenCodePromptOutcome::Cancelled,
        };
        self.cancellation = OpenCodeCancellation::Unnecessary;
        self.validate()
    }

    pub(crate) fn reconcile_quiescent_cancellation(&mut self) -> Result<bool, KitError> {
        if self.is_terminal() {
            return match self.cancellation.clone() {
                OpenCodeCancellation::Unnecessary => self.validate().map(|()| false),
                OpenCodeCancellation::Accepted { .. }
                | OpenCodeCancellation::Rejected { .. }
                | OpenCodeCancellation::Ambiguous { .. } => {
                    self.cancellation = OpenCodeCancellation::Unnecessary;
                    self.validate().map(|()| true)
                }
                OpenCodeCancellation::NotRequested
                | OpenCodeCancellation::Requested
                | OpenCodeCancellation::Reserved { .. }
                | OpenCodeCancellation::Dispatching { .. } => Err(KitError::invalid_state(
                    "terminal prompt cancellation lacks an exact settled outcome",
                )),
            };
        }
        if self.cancellation_accepted() {
            self.terminalize_accepted_cancellation()?;
            return Ok(true);
        }
        if !matches!(
            &self.cancellation,
            OpenCodeCancellation::NotRequested
                | OpenCodeCancellation::Requested
                | OpenCodeCancellation::Rejected { .. }
                | OpenCodeCancellation::Ambiguous { .. }
        ) {
            return Err(KitError::invalid_state(
                "idle prompt reconciliation lacks a cancellation outcome",
            ));
        }
        if !matches!(
            &self.phase,
            OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission
        ) {
            return Err(KitError::invalid_state(
                "idle prompt reconciliation contradicts durable admission",
            ));
        }
        self.phase = OpenCodePromptPhase::Terminal {
            outcome: OpenCodePromptOutcome::ReconciledInvalidState,
        };
        self.cancellation = OpenCodeCancellation::Unnecessary;
        self.discard_terminal_output();
        self.validate()?;
        Ok(true)
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(&self.phase, OpenCodePromptPhase::Terminal { .. })
    }

    pub(crate) fn clearable(&self) -> bool {
        self.is_terminal()
            && self.cancellation_releasable()
            && matches!(
                self.waiter,
                OpenCodePromptWaiter::TimedOut
                    | OpenCodePromptWaiter::Lost
                    | OpenCodePromptWaiter::Delivered
            )
    }

    pub(crate) fn cancellation_releasable(&self) -> bool {
        self.cancellation == OpenCodeCancellation::Unnecessary
    }

    pub(crate) fn cancellation_recovery_owned(&self) -> bool {
        matches!(
            &self.cancellation,
            OpenCodeCancellation::Reserved { .. }
                | OpenCodeCancellation::Dispatching { .. }
                | OpenCodeCancellation::Accepted { .. }
                | OpenCodeCancellation::Rejected { .. }
                | OpenCodeCancellation::Ambiguous { .. }
        )
    }

    pub(crate) fn idle_cancellation_repairable(&self) -> bool {
        (self.is_terminal()
            && matches!(
                &self.cancellation,
                OpenCodeCancellation::Unnecessary
                    | OpenCodeCancellation::Accepted { .. }
                    | OpenCodeCancellation::Rejected { .. }
                    | OpenCodeCancellation::Ambiguous { .. }
            ))
            || (matches!(
                &self.phase,
                OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission
            ) && matches!(
                &self.cancellation,
                OpenCodeCancellation::NotRequested
                    | OpenCodeCancellation::Requested
                    | OpenCodeCancellation::Accepted { .. }
                    | OpenCodeCancellation::Rejected { .. }
                    | OpenCodeCancellation::Ambiguous { .. }
            ))
    }

    pub(crate) fn has_lost_response_after_admission(&self) -> bool {
        matches!(&self.phase, OpenCodePromptPhase::ResponseLostAfterAdmission)
    }

    pub(crate) fn interaction_cleanup_ready(&self) -> bool {
        ((self.has_lost_response_after_admission() && self.idle_cancellation_repairable())
            || (matches!(self.phase, OpenCodePromptPhase::Admitted)
                && self.cancellation_accepted()))
            && matches!(
                self.waiter,
                OpenCodePromptWaiter::TimedOut
                    | OpenCodePromptWaiter::Lost
                    | OpenCodePromptWaiter::Delivered
            )
    }

    pub(crate) fn delivered_ambiguous_response_loss(&self) -> bool {
        self.has_lost_response_after_admission()
            && self.waiter == OpenCodePromptWaiter::Delivered
            && matches!(&self.cancellation, OpenCodeCancellation::Ambiguous { .. })
    }

    fn response_ready_for_delivery(&self) -> bool {
        matches!(
            &self.phase,
            OpenCodePromptPhase::ResponseLostBeforeAdmission
                | OpenCodePromptPhase::ResponseLostAfterAdmission
        ) || self.is_terminal()
    }

    fn discard_terminal_output(&mut self) {
        if let OpenCodePromptPhase::Terminal {
            outcome: OpenCodePromptOutcome::Success { output },
        } = &mut self.phase
        {
            *output = None;
        }
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        self.effective.validate()?;
        if let Some(timeout) = self.timeout {
            timeout.validate()?;
        }
        if let Some(root) = &self.structured_root {
            root.validate()?;
        }
        if let Some(part) = &self.structured_part {
            part.validate()?;
        }
        let structured_parent_valid = if self.effective.structured_schema.is_some() {
            self.structured_part
                .as_ref()
                .is_none_or(|part| part.part_id == self.request_part_id)
                && (self.structured_parent_conflict
                    || self
                        .structured_root
                        .as_ref()
                        .zip(self.structured_part.as_ref())
                        .is_none_or(|(root, part)| part.same_owner(root)))
                && (!matches!(&self.phase, OpenCodePromptPhase::Prepared)
                    || (self.structured_root.is_none()
                        && self.structured_part.is_none()
                        && !self.structured_parent_conflict
                        && !self.structured_parent_recovery_claimed))
                && (!self.structured_parent_conflict
                    || self.structured_root.is_some()
                    || self.structured_part.is_some())
                && (self.input_observed == self.structured_part.is_some())
        } else {
            self.structured_root.is_none()
                && self.structured_part.is_none()
                && !self.structured_parent_conflict
                && !self.structured_parent_recovery_claimed
        };
        let phase_cancellation_valid = match &self.phase {
            OpenCodePromptPhase::Prepared
            | OpenCodePromptPhase::DispatchedAwaitingAdmission
            | OpenCodePromptPhase::ResponseLostBeforeAdmission => matches!(
                &self.cancellation,
                OpenCodeCancellation::NotRequested | OpenCodeCancellation::Requested
            ),
            OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission => {
                self.cancellation != OpenCodeCancellation::Unnecessary
            }
            OpenCodePromptPhase::Terminal { .. } => matches!(
                &self.cancellation,
                OpenCodeCancellation::Unnecessary
                    | OpenCodeCancellation::Dispatching { .. }
                    | OpenCodeCancellation::Accepted { .. }
                    | OpenCodeCancellation::Rejected { .. }
                    | OpenCodeCancellation::Ambiguous { .. }
            ),
        };
        let cancellation_owner_valid = self
            .cancellation
            .owner()
            .is_none_or(OpenCodeCancellationOwner::is_valid);
        let phase_waiter_valid = match &self.phase {
            OpenCodePromptPhase::Prepared
            | OpenCodePromptPhase::DispatchedAwaitingAdmission
            | OpenCodePromptPhase::Admitted => !matches!(
                self.waiter,
                OpenCodePromptWaiter::Delivering | OpenCodePromptWaiter::Delivered
            ),
            OpenCodePromptPhase::ResponseLostBeforeAdmission
            | OpenCodePromptPhase::ResponseLostAfterAdmission
            | OpenCodePromptPhase::Terminal { .. } => true,
        };
        let terminal_output_valid = match &self.phase {
            OpenCodePromptPhase::Terminal {
                outcome: OpenCodePromptOutcome::Success { output },
            } => {
                let retention_valid = match self.waiter {
                    OpenCodePromptWaiter::Waiting
                    | OpenCodePromptWaiter::Delivering
                    | OpenCodePromptWaiter::Delivered => output.is_some(),
                    OpenCodePromptWaiter::TimedOut | OpenCodePromptWaiter::Lost => output.is_none(),
                };
                retention_valid
                    && output.as_ref().is_none_or(|output| {
                        output.structured.is_some() == self.effective.structured_schema.is_some()
                    })
            }
            _ => true,
        };
        if self.version != OPENCODE_PROMPT_RECORD_VERSION
            || self.generation_id.is_empty()
            || self.generation_id.trim() != self.generation_id
            || !self.request_part_id.starts_with("prt")
            || self.request_part_id.trim() != self.request_part_id
            || (matches!(&self.phase, OpenCodePromptPhase::Prepared) && self.input_observed)
            || (matches!(
                &self.phase,
                OpenCodePromptPhase::Admitted | OpenCodePromptPhase::ResponseLostAfterAdmission
            ) && !self.input_observed)
            || !phase_cancellation_valid
            || !cancellation_owner_valid
            || !phase_waiter_valid
            || !terminal_output_valid
            || !structured_parent_valid
        {
            return Err(KitError::invalid_state("stored prompt state is malformed"));
        }
        Ok(())
    }
}

impl OpenCodeCancellationOwner {
    pub(crate) fn fresh() -> Self {
        Self(format!("abort_{}", uuid::Uuid::new_v4().simple()))
    }

    fn is_valid(&self) -> bool {
        !self.0.is_empty() && self.0.trim() == self.0
    }
}

impl OpenCodeCancellation {
    pub(crate) fn accepted(owner: &OpenCodeCancellationOwner) -> Self {
        Self::Accepted {
            owner: owner.clone(),
        }
    }

    pub(crate) fn rejected(owner: &OpenCodeCancellationOwner) -> Self {
        Self::Rejected {
            owner: owner.clone(),
        }
    }

    pub(crate) fn ambiguous(owner: &OpenCodeCancellationOwner) -> Self {
        Self::Ambiguous {
            owner: owner.clone(),
        }
    }

    fn owner(&self) -> Option<&OpenCodeCancellationOwner> {
        match self {
            Self::Reserved { owner }
            | Self::Dispatching { owner }
            | Self::Accepted { owner }
            | Self::Rejected { owner }
            | Self::Ambiguous { owner } => Some(owner),
            Self::NotRequested | Self::Requested | Self::Unnecessary => None,
        }
    }
}

impl OpenCodeContinuation {
    pub(crate) const NAMESPACE: &'static str = "agents_kit_opencode_continuations_v1";

    pub(crate) fn key(
        conversation_id: &ConversationId,
        request_id: &str,
    ) -> Result<storage_kit::state::record::StateKey, KitError> {
        if request_id.is_empty() || request_id.trim() != request_id {
            return Err(KitError::invalid_state(
                "continuation identity is malformed",
            ));
        }
        storage_kit::state::record::StateKey::new(
            Self::NAMESPACE,
            format!("{}:{}", conversation_id.as_str(), request_id),
        )
        .map_err(|_| KitError::invalid_state("continuation key is invalid"))
    }

    pub(crate) fn state_key(&self) -> Result<storage_kit::state::record::StateKey, KitError> {
        Self::key(&self.conversation_id, self.request_id())
    }

    pub(crate) fn decode(
        snapshot: storage_kit::state::record::StateRecordSnapshot,
        conversation_id: &ConversationId,
    ) -> Result<Self, KitError> {
        let key = snapshot.record.key.key().to_owned();
        let continuation: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| KitError::invalid_state("stored continuation is malformed"))?;
        continuation.validate()?;
        if continuation.conversation_id() != conversation_id
            || Self::key(conversation_id, continuation.request_id())?.key() != key
        {
            return Err(KitError::invalid_state(
                "stored continuation contradicts its key owner",
            ));
        }
        Ok(continuation)
    }

    pub(crate) fn new(
        conversation_id: ConversationId,
        notification_id: NotificationId,
        request: OpenCodeContinuationRequest,
        generation_id: Option<String>,
    ) -> Result<Self, KitError> {
        let continuation = Self {
            version: 2,
            conversation_id,
            notification_id,
            request,
            generation_id,
            state: OpenCodeContinuationState::Pending,
        };
        continuation.validate()?;
        Ok(continuation)
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }
    pub(crate) fn notification_id(&self) -> &NotificationId {
        &self.notification_id
    }
    pub(crate) fn session_id(&self) -> &str {
        self.request.session_id()
    }
    pub(crate) fn request_id(&self) -> &str {
        self.request.id()
    }
    pub(crate) fn request(&self) -> &OpenCodeContinuationRequest {
        &self.request
    }
    pub(crate) fn generation_id(&self) -> Option<&str> {
        self.generation_id.as_deref()
    }
    pub(crate) fn is_permission(&self) -> bool {
        self.kind() == OpenCodeContinuationKind::Permission
    }
    pub(crate) fn kind(&self) -> OpenCodeContinuationKind {
        self.request.kind()
    }
    pub(crate) fn question_count(&self) -> Option<usize> {
        self.request.question_count()
    }
    pub(crate) fn state(&self) -> OpenCodeContinuationState {
        self.state.clone()
    }

    pub(crate) fn is_unresolved(&self) -> bool {
        matches!(
            self.state,
            OpenCodeContinuationState::Pending
                | OpenCodeContinuationState::Ambiguous
                | OpenCodeContinuationState::Closing { .. }
        )
    }

    pub(crate) fn blocks_prompt_release(&self) -> bool {
        self.is_unresolved() || self.requires_native_cleanup()
    }

    pub(crate) fn requires_native_cleanup(&self) -> bool {
        matches!(
            self.state,
            OpenCodeContinuationState::CancellationRequired
                | OpenCodeContinuationState::CancellationDispatched { .. }
                | OpenCodeContinuationState::Closing { .. }
        )
    }

    pub(crate) fn retains_cancelled_generation(&self) -> bool {
        matches!(
            self.state,
            OpenCodeContinuationState::CancellationRequired
                | OpenCodeContinuationState::CancellationDispatched { .. }
        )
    }

    pub(crate) fn cancellation_owner(&self) -> Option<&OpenCodeCancellationOwner> {
        match &self.state {
            OpenCodeContinuationState::CancellationDispatched { owner }
            | OpenCodeContinuationState::Closing { owner } => Some(owner),
            _ => None,
        }
    }

    pub(crate) fn with_state(&self, state: OpenCodeContinuationState) -> Result<Self, KitError> {
        let mut continuation = self.clone();
        continuation.state = state;
        continuation.validate()?;
        Ok(continuation)
    }

    pub(crate) fn has_same_request(&self, other: &Self) -> bool {
        self.conversation_id == other.conversation_id
            && self.notification_id == other.notification_id
            && self.request == other.request
            && self.generation_id == other.generation_id
    }

    pub(crate) fn native_resolution(
        &self,
        outcome: OpenCodeNativeResolution,
    ) -> Result<Option<OpenCodeContinuationState>, KitError> {
        match &self.state {
            OpenCodeContinuationState::Pending | OpenCodeContinuationState::Ambiguous => {
                Ok(Some(OpenCodeContinuationState::Accepted))
            }
            OpenCodeContinuationState::Accepted => Ok(None),
            OpenCodeContinuationState::Cancelled => {
                if outcome == OpenCodeNativeResolution::Rejected {
                    Ok(None)
                } else {
                    Err(KitError::protocol(
                        "native answer contradicts cancelled interaction",
                    ))
                }
            }
            OpenCodeContinuationState::CancellationRequired
            | OpenCodeContinuationState::CancellationDispatched { .. }
            | OpenCodeContinuationState::Closing { .. } => {
                if outcome == OpenCodeNativeResolution::Rejected {
                    Ok(Some(OpenCodeContinuationState::Cancelled))
                } else {
                    Err(KitError::protocol(
                        "native answer contradicts interaction cancellation",
                    ))
                }
            }
        }
    }

    pub(crate) fn matches_native_resolution(
        &self,
        session_id: &str,
        request_id: &str,
        kind: OpenCodeContinuationKind,
        answer_count: Option<usize>,
    ) -> bool {
        self.session_id() == session_id
            && self.request_id() == request_id
            && self.kind() == kind
            && answer_count.is_none_or(|count| self.question_count() == Some(count))
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != 2
            || self
                .generation_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.trim() != id)
        {
            return Err(KitError::invalid_state(
                "stored interaction continuation version or generation is invalid",
            ));
        }
        self.request.validate()?;
        Ok(())
    }
}

impl OpenCodeContinuationRequest {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Permission(request) => request.id(),
            Self::Question(request) => request.id(),
        }
    }
    pub(crate) fn session_id(&self) -> &str {
        match self {
            Self::Permission(request) => request.session_id(),
            Self::Question(request) => request.session_id(),
        }
    }
    pub(crate) fn kind(&self) -> OpenCodeContinuationKind {
        match self {
            Self::Permission(_) => OpenCodeContinuationKind::Permission,
            Self::Question(_) => OpenCodeContinuationKind::Question,
        }
    }
    pub(crate) fn question_count(&self) -> Option<usize> {
        match self {
            Self::Permission(_) => None,
            Self::Question(request) => Some(request.questions().len()),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), KitError> {
        match self {
            Self::Permission(request) => {
                request.clone().validate_v1()?;
            }
            Self::Question(request) => {
                request.clone().validate_v1()?;
            }
        };
        Ok(())
    }
}

impl OpenCodeEventProgress {
    pub(crate) fn new(agent_id: AgentId) -> Self {
        Self {
            version: OPENCODE_EVENT_PROGRESS_VERSION,
            agent_id,
            high_water: BTreeMap::new(),
            recent: VecDeque::new(),
            active_processes: BTreeSet::new(),
            retired_sessions: BTreeSet::new(),
        }
    }

    pub(crate) fn high_water(&self) -> &BTreeMap<String, u64> {
        &self.high_water
    }

    pub(crate) fn retired_sessions(&self) -> &BTreeSet<String> {
        &self.retired_sessions
    }

    pub(crate) fn observe_process(
        &mut self,
        process: &OpenCodeProcessEffect,
    ) -> Result<bool, KitError> {
        let owner = OpenCodeProcessOwner::from_process(process)?;
        let changed = if process.is_running() {
            if self.active_processes.contains(&owner) {
                false
            } else {
                if self.active_processes.len() == MAX_RECENT_EVENTS {
                    return Err(KitError::integration(
                        "active process state exceeds its configured bound",
                    ));
                }
                self.active_processes.insert(owner)
            }
        } else if self.active_processes.remove(&owner) {
            true
        } else {
            return Err(KitError::invalid_state(
                "process exited without an active owner",
            ));
        };
        self.ensure_bound()?;
        Ok(changed)
    }

    pub(crate) fn observe(
        &mut self,
        event: &mut OpenCodeEventIdentity,
    ) -> Result<OpenCodeEventDisposition, KitError> {
        event.validate()?;
        if event.belongs_to_any(&self.retired_sessions) {
            return Ok(OpenCodeEventDisposition::Duplicate);
        }
        let disposition = match &*event {
            OpenCodeEventIdentity::Normal { id, .. } => {
                match self.recent.iter().find(|known| known.id() == id) {
                    Some(known) => {
                        known.require_same_semantic_event(event)?;
                        OpenCodeEventDisposition::RememberedDuplicate
                    }
                    None => {
                        self.remember(event.clone())?;
                        OpenCodeEventDisposition::Applied
                    }
                }
            }
            OpenCodeEventIdentity::Durable {
                id,
                aggregate_id,
                sequence,
                history,
                ..
            } => {
                let next = self
                    .high_water
                    .get(aggregate_id)
                    .map(|value| {
                        value.checked_add(1).ok_or_else(|| {
                            KitError::invalid_state("aggregate sequence is exhausted")
                        })
                    })
                    .transpose()?
                    .unwrap_or(0);
                if *sequence > next {
                    return Ok(OpenCodeEventDisposition::Gap);
                }
                let remembered =
                    if let Some(index) = self.recent.iter().position(|known| known.id() == id) {
                        let known = &self.recent[index];
                        known.require_same_semantic_event(event)?;
                        if let Some((known_aggregate, known_sequence)) = known.durable_position()
                            && (known_aggregate != aggregate_id || known_sequence != *sequence)
                        {
                            return Err(KitError::protocol(
                                "event identity has contradictory durable coordinates",
                            ));
                        }
                        Some(index)
                    } else {
                        None
                    };
                if self.recent.iter().any(|known| {
                    known.id() != id
                        && known.durable_position().is_some_and(
                            |(known_aggregate, known_sequence)| {
                                known_aggregate == aggregate_id && known_sequence == *sequence
                            },
                        )
                }) {
                    return Err(KitError::protocol(
                        "aggregate sequence was reused for another event",
                    ));
                }
                if *sequence < next {
                    if let Some(index) = remembered {
                        if *history
                            && !matches!(
                                &self.recent[index],
                                OpenCodeEventIdentity::Durable { history: true, .. }
                            )
                        {
                            self.recent.remove(index);
                            self.remember(event.clone())?;
                        }
                        OpenCodeEventDisposition::RememberedDuplicate
                    } else {
                        OpenCodeEventDisposition::Duplicate
                    }
                } else {
                    self.high_water.insert(aggregate_id.clone(), *sequence);
                    let known = remembered.and_then(|index| self.recent.remove(index));
                    let normal_was_applied =
                        matches!(&known, Some(OpenCodeEventIdentity::Normal { .. }));
                    let retained = known
                        .filter(|known| {
                            matches!(known, OpenCodeEventIdentity::Durable { history: true, .. })
                        })
                        .unwrap_or_else(|| event.clone());
                    self.remember(retained)?;
                    if normal_was_applied {
                        OpenCodeEventDisposition::RememberedDuplicate
                    } else {
                        OpenCodeEventDisposition::Applied
                    }
                }
            }
        };
        self.ensure_bound()?;
        if let Some(retained) = self.recent.iter().find(|known| known.id() == event.id()) {
            event.clone_from(retained);
        }
        Ok(disposition)
    }

    pub(crate) fn remembers_exact(&self, event: &OpenCodeEventIdentity) -> Result<bool, KitError> {
        event.validate()?;
        let Some(known) = self.recent.iter().find(|known| known.id() == event.id()) else {
            return Ok(false);
        };
        Ok(known == event)
    }

    pub(crate) fn validate(&self, agent_id: &AgentId) -> Result<(), KitError> {
        if self.version != OPENCODE_EVENT_PROGRESS_VERSION
            || &self.agent_id != agent_id
            || self
                .high_water
                .keys()
                .any(|id| id.is_empty() || id.trim() != id)
            || self.recent.len() > MAX_RECENT_EVENTS
            || self.active_processes.len() > MAX_RECENT_EVENTS
            || self.retired_sessions.len() > MAX_RECENT_EVENTS
            || serde_json::to_vec(self)
                .map_or(true, |encoded| encoded.len() > MAX_EVENT_PROGRESS_BYTES)
        {
            return Err(KitError::invalid_state(
                "stored event progress is malformed",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut positions = std::collections::BTreeSet::new();
        for event in &self.recent {
            event.validate()?;
            if !ids.insert(event.id())
                || event
                    .durable_position()
                    .is_some_and(|position| !positions.insert((position.0, position.1)))
            {
                return Err(KitError::invalid_state(
                    "stored event progress contains duplicate identity",
                ));
            }
        }
        for process in &self.active_processes {
            process.validate()?;
        }
        if self
            .retired_sessions
            .iter()
            .any(|id| !id.starts_with("ses") || id.trim() != id)
        {
            return Err(KitError::invalid_state(
                "stored retired session identity is malformed",
            ));
        }
        Ok(())
    }

    pub(crate) fn retire_aggregates(&mut self, ids: &[String]) -> Result<(), KitError> {
        let ids = ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
        if ids
            .iter()
            .any(|id| !id.starts_with("ses") || id.trim() != *id)
        {
            return Err(KitError::invalid_state(
                "retired session identity is malformed",
            ));
        }
        let new_sessions = ids
            .iter()
            .filter(|id| !self.retired_sessions.contains(**id))
            .count();
        if self.retired_sessions.len().saturating_add(new_sessions) > MAX_RECENT_EVENTS {
            return Err(KitError::integration(
                "retired session state exceeds its configured bound",
            ));
        }
        self.high_water.retain(|id, _| !ids.contains(id.as_str()));
        self.recent.retain(|event| !event.belongs_to_any(&ids));
        self.active_processes
            .retain(|process| !ids.contains(process.session_id.as_str()));
        self.retired_sessions
            .extend(ids.into_iter().map(str::to_owned));
        self.ensure_bound()
    }

    fn ensure_bound(&self) -> Result<(), KitError> {
        if serde_json::to_vec(self).map_or(true, |encoded| encoded.len() > MAX_EVENT_PROGRESS_BYTES)
        {
            return Err(KitError::integration(
                "event progress exceeds its configured bound",
            ));
        }
        Ok(())
    }
}

impl OpenCodeProcessOwner {
    fn from_process(process: &OpenCodeProcessEffect) -> Result<Self, KitError> {
        let owner = Self {
            session_id: process.session_id().to_owned(),
            message_id: process.message_id().to_owned(),
            part_id: process.part_id().to_owned(),
        };
        owner.validate()?;
        Ok(owner)
    }

    fn validate(&self) -> Result<(), KitError> {
        if !self.session_id.starts_with("ses")
            || self.session_id.trim() != self.session_id
            || !self.message_id.starts_with("msg")
            || self.message_id.trim() != self.message_id
            || !self.part_id.starts_with("prt")
            || self.part_id.trim() != self.part_id
        {
            return Err(KitError::invalid_state("stored process owner is malformed"));
        }
        Ok(())
    }
}

impl OpenCodeEventIdentity {
    pub(crate) fn normal(
        id: String,
        event_type: String,
        data: serde_json::Value,
    ) -> Result<Self, KitError> {
        let identity = Self::Normal {
            id,
            event_type,
            data,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn durable(
        id: String,
        aggregate_id: String,
        sequence: u64,
        history: bool,
        event_type: String,
        data: serde_json::Value,
    ) -> Result<Self, KitError> {
        let identity = Self::Durable {
            history,
            id,
            aggregate_id,
            sequence,
            event_type,
            data,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Normal { id, .. } | Self::Durable { id, .. } => id,
        }
    }

    pub(crate) fn event_type(&self) -> &str {
        match self {
            Self::Normal { event_type, .. } | Self::Durable { event_type, .. } => event_type,
        }
    }

    pub(crate) fn data(&self) -> &serde_json::Value {
        match self {
            Self::Normal { data, .. } | Self::Durable { data, .. } => data,
        }
    }

    pub(crate) fn durable_position(&self) -> Option<(&str, u64)> {
        match self {
            Self::Normal { .. } => None,
            Self::Durable {
                aggregate_id,
                sequence,
                ..
            } => Some((aggregate_id, *sequence)),
        }
    }

    fn session_id(&self) -> Option<&str> {
        match self {
            Self::Normal { data, .. } => data.get("sessionID").and_then(serde_json::Value::as_str),
            Self::Durable { aggregate_id, .. } => Some(aggregate_id),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        let invalid = !self.id().starts_with("evt_")
            || self.id().trim() != self.id()
            || self.event_type().is_empty()
            || self.event_type().trim() != self.event_type()
            || self.durable_position().is_some_and(|(aggregate, _)| {
                aggregate.is_empty() || aggregate.trim() != aggregate
            });
        if invalid {
            return Err(KitError::invalid_state(
                "stored event identity is malformed",
            ));
        }
        Ok(())
    }

    fn require_same_semantic_event(&self, event: &Self) -> Result<(), KitError> {
        let same_type =
            Self::canonical_type(self.event_type()) == Self::canonical_type(event.event_type());
        if same_type && self.data() == event.data() {
            return Ok(());
        }
        let same_origin = match (self, event) {
            (Self::Normal { .. }, Self::Normal { .. }) => true,
            (Self::Durable { history: left, .. }, Self::Durable { history: right, .. }) => {
                left == right
            }
            (Self::Normal { .. }, Self::Durable { .. })
            | (Self::Durable { .. }, Self::Normal { .. }) => false,
        };
        if same_type
            && !same_origin
            && [self, event].into_iter().all(|identity| match identity {
                Self::Normal { event_type, .. } => event_type == "message.updated",
                Self::Durable { event_type, .. } => event_type == "message.updated.1",
            })
            && event
                .data()
                .pointer("/info/role")
                .and_then(serde_json::Value::as_str)
                == Some("assistant")
        {
            // Incoming data has passed the selected event decoder. Validate the
            // stored projection too before disregarding any mutable field.
            let stored: OpenCodeMessageUpdatedEvent = serde::Deserialize::deserialize(self.data())
                .map_err(|_| KitError::protocol("stored Assistant event is malformed"))?;
            let stored = stored.validate()?;
            if stored.assistant_observation().is_some() {
                let mut before = self.data().clone();
                let mut after = event.data().clone();
                for value in [&mut before, &mut after] {
                    if let Some(time) = value
                        .pointer_mut("/info/time")
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        time.remove("completed");
                    }
                    if let Some(info) = value
                        .get_mut("info")
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        for field in ["cost", "tokens", "finish", "error", "structured"] {
                            info.remove(field);
                        }
                    }
                }
                if before == after {
                    return Ok(());
                }
            }
        }
        Err(KitError::protocol(
            "event identity was reused for contradictory content",
        ))
    }

    fn canonical_type(event_type: &str) -> &str {
        let Some((base, version)) = event_type.rsplit_once('.') else {
            return event_type;
        };
        if !base.is_empty()
            && !version.is_empty()
            && version.bytes().all(|byte| byte.is_ascii_digit())
        {
            base
        } else {
            event_type
        }
    }

    fn belongs_to_any<T>(&self, ids: &BTreeSet<T>) -> bool
    where
        T: std::borrow::Borrow<str> + Ord,
    {
        self.session_id().is_some_and(|id| ids.contains(id))
    }
}

impl OpenCodeEventProgress {
    fn remember(&mut self, event: OpenCodeEventIdentity) -> Result<(), KitError> {
        if self.recent.len() == MAX_RECENT_EVENTS {
            self.recent.pop_front();
        }
        self.recent.push_back(event);
        while serde_json::to_vec(&self.recent)
            .is_ok_and(|encoded| encoded.len() > MAX_EVENT_PROGRESS_BYTES)
            && self.recent.len() > 1
        {
            self.recent.pop_front();
        }
        if serde_json::to_vec(&self.recent)
            .map_or(true, |encoded| encoded.len() > MAX_EVENT_PROGRESS_BYTES)
        {
            return Err(KitError::integration(
                "event deduplication state exceeds its configured bound",
            ));
        }
        Ok(())
    }
}
