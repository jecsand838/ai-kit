use std::path::{Path, PathBuf};

use storage_sdk::StateStoreBackend;
use storage_sdk::state::record::{StateKey, StateKeyPrefix, StateListRequest, StateRecordSnapshot};

use crate::{
    AgentId, ConversationId, ConversationInfo, KnowledgePolicy, ModelSelector, NewConversation,
    NewWorkspace, PersonalitySelector, ReasoningEffort, SdkError, UpdateWorkspaceRequest,
    WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy, WorkspaceId, WorkspaceInfo,
    WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
};

const WORKSPACE_NAMESPACE: &str = "agent_sdk_workspaces_v1";
const CONVERSATION_NAMESPACE: &str = "agent_sdk_conversations_v1";
const RECORD_VERSION: u32 = 2;
const MAX_RECORD_PAGES: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum RecordLifecycle {
    Open,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct StoredDefaults {
    model: Option<ModelSelector>,
    personality: Option<PersonalitySelector>,
    reasoning_effort: Option<ReasoningEffort>,
    knowledge_policy: Option<KnowledgePolicy>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredWorkspaceRootAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct StoredWorkspaceRoot {
    path: PathBuf,
    access: StoredWorkspaceRootAccess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredWorkspaceSandboxMode {
    ReadOnly,
    WorkspaceWrite,
    FullAccess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredWorkspaceNetworkAccess {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct StoredWorkspaceExecutionPolicy {
    sandbox_mode: StoredWorkspaceSandboxMode,
    network_access: StoredWorkspaceNetworkAccess,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceRecord {
    version: u32,
    id: WorkspaceId,
    agent_id: AgentId,
    name: String,
    cwd: PathBuf,
    roots: Vec<StoredWorkspaceRoot>,
    execution_policy: StoredWorkspaceExecutionPolicy,
    defaults: StoredDefaults,
    revision: u64,
    lifecycle: RecordLifecycle,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConversationRecord {
    version: u32,
    id: ConversationId,
    workspace_id: WorkspaceId,
    workspace_revision: u64,
    title: Option<String>,
    defaults: Option<StoredDefaults>,
    lifecycle: RecordLifecycle,
}

impl StoredDefaults {
    fn from_public(defaults: &WorkspaceDefaults) -> Self {
        Self {
            model: defaults.model().cloned(),
            personality: defaults.personality().cloned(),
            reasoning_effort: defaults.reasoning_effort().cloned(),
            knowledge_policy: defaults.knowledge_policy().cloned(),
        }
    }

    fn to_public(&self) -> Result<WorkspaceDefaults, SdkError> {
        let defaults = WorkspaceDefaults::new(
            self.model.clone(),
            self.reasoning_effort.clone(),
            self.knowledge_policy.clone(),
        )
        .map_err(|_| SdkError::invalid_state("stored workspace defaults are malformed"))?;
        Ok(match &self.personality {
            Some(personality) => defaults.with_personality(personality.clone()),
            None => defaults,
        })
    }
}

impl StoredWorkspaceRoot {
    fn from_public(root: &WorkspaceRoot) -> Self {
        Self {
            path: root.path().to_path_buf(),
            access: match root.access() {
                WorkspaceRootAccess::ReadOnly => StoredWorkspaceRootAccess::ReadOnly,
                WorkspaceRootAccess::ReadWrite => StoredWorkspaceRootAccess::ReadWrite,
            },
        }
    }

    fn to_public(&self) -> Result<WorkspaceRoot, SdkError> {
        let access = match self.access {
            StoredWorkspaceRootAccess::ReadOnly => WorkspaceRootAccess::ReadOnly,
            StoredWorkspaceRootAccess::ReadWrite => WorkspaceRootAccess::ReadWrite,
        };
        WorkspaceRoot::new(self.path.clone(), access)
            .map_err(|_| SdkError::invalid_state("stored workspace root is malformed"))
    }
}

impl StoredWorkspaceExecutionPolicy {
    fn from_public(policy: &WorkspaceExecutionPolicy) -> Self {
        Self {
            sandbox_mode: match policy.sandbox_mode() {
                WorkspaceSandboxMode::ReadOnly => StoredWorkspaceSandboxMode::ReadOnly,
                WorkspaceSandboxMode::WorkspaceWrite => StoredWorkspaceSandboxMode::WorkspaceWrite,
                WorkspaceSandboxMode::FullAccess => StoredWorkspaceSandboxMode::FullAccess,
            },
            network_access: match policy.network_access() {
                WorkspaceNetworkAccess::Disabled => StoredWorkspaceNetworkAccess::Disabled,
                WorkspaceNetworkAccess::Enabled => StoredWorkspaceNetworkAccess::Enabled,
            },
        }
    }

    fn to_public(self) -> WorkspaceExecutionPolicy {
        let sandbox_mode = match self.sandbox_mode {
            StoredWorkspaceSandboxMode::ReadOnly => WorkspaceSandboxMode::ReadOnly,
            StoredWorkspaceSandboxMode::WorkspaceWrite => WorkspaceSandboxMode::WorkspaceWrite,
            StoredWorkspaceSandboxMode::FullAccess => WorkspaceSandboxMode::FullAccess,
        };
        let network_access = match self.network_access {
            StoredWorkspaceNetworkAccess::Disabled => WorkspaceNetworkAccess::Disabled,
            StoredWorkspaceNetworkAccess::Enabled => WorkspaceNetworkAccess::Enabled,
        };
        WorkspaceExecutionPolicy::new(sandbox_mode, network_access)
    }
}

impl WorkspaceRecord {
    pub(crate) fn new(agent_id: AgentId, id: WorkspaceId, request: &NewWorkspace) -> Self {
        Self {
            version: RECORD_VERSION,
            id,
            agent_id,
            name: request.name().to_owned(),
            cwd: request.context().cwd().to_path_buf(),
            roots: request
                .context()
                .roots()
                .iter()
                .map(StoredWorkspaceRoot::from_public)
                .collect(),
            execution_policy: StoredWorkspaceExecutionPolicy::from_public(
                request.context().execution_policy(),
            ),
            defaults: StoredDefaults::from_public(request.defaults()),
            revision: 1,
            lifecycle: RecordLifecycle::Open,
        }
    }

    pub(crate) fn id(&self) -> &WorkspaceId {
        &self.id
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub(crate) fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        if self.version != RECORD_VERSION || self.lifecycle != RecordLifecycle::Open {
            return Err(SdkError::invalid_state(
                "stored workspace lifecycle is malformed",
            ));
        }
        let roots = self
            .roots
            .iter()
            .map(StoredWorkspaceRoot::to_public)
            .collect::<Result<Vec<_>, _>>()?;
        let context =
            WorkspaceContext::new(self.cwd.clone(), roots, self.execution_policy.to_public())
                .map_err(|_| SdkError::invalid_state("stored workspace context is malformed"))?;
        WorkspaceInfo::new(
            self.id.clone(),
            self.name.clone(),
            context,
            self.defaults.to_public()?,
            self.revision,
        )
        .map_err(|_| SdkError::invalid_state("stored workspace state is malformed"))
    }

    pub(crate) fn replace(&self, request: &UpdateWorkspaceRequest) -> Result<Self, SdkError> {
        if request.id() != &self.id || request.expected_revision() != self.revision {
            return Err(SdkError::conflict("workspace revision is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision)
                .in_phase(crate::error::ErrorPhase::Admission));
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| SdkError::invalid_state("workspace revision is exhausted"))?;
        Ok(Self {
            version: RECORD_VERSION,
            id: self.id.clone(),
            agent_id: self.agent_id.clone(),
            name: request.name().to_owned(),
            cwd: request.context().cwd().to_path_buf(),
            roots: request
                .context()
                .roots()
                .iter()
                .map(StoredWorkspaceRoot::from_public)
                .collect(),
            execution_policy: StoredWorkspaceExecutionPolicy::from_public(
                request.context().execution_policy(),
            ),
            defaults: StoredDefaults::from_public(request.defaults()),
            revision,
            lifecycle: RecordLifecycle::Open,
        })
    }

    pub(crate) fn key(agent_id: &AgentId, id: &WorkspaceId) -> Result<StateKey, SdkError> {
        let key = format!("{}:{}", agent_id.as_str(), id.as_str());
        StateKey::new(WORKSPACE_NAMESPACE, key)
            .map_err(|_| SdkError::invalid_state("workspace state key is invalid"))
    }

    pub(crate) fn list_request(agent_id: &AgentId) -> Result<StateListRequest, SdkError> {
        let prefix = StateKeyPrefix::new(format!("{}:", agent_id.as_str()))
            .map_err(|_| SdkError::invalid_state("workspace state prefix is invalid"))?;
        StateListRequest::new(WORKSPACE_NAMESPACE)
            .map(|request| request.with_key_prefix(prefix))
            .map_err(|_| SdkError::invalid_state("workspace inventory request is invalid"))
    }

    pub(crate) async fn list(
        store: &impl StateStoreBackend,
        agent_id: &AgentId,
    ) -> Result<Vec<Self>, SdkError> {
        let mut records = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_RECORD_PAGES {
            let mut request = Self::list_request(agent_id)?
                .with_limit(StateListRequest::MAX_LIMIT)
                .map_err(|_| SdkError::invalid_state("workspace inventory limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request.start_after(next).map_err(|_| {
                    SdkError::invalid_state("workspace inventory cursor is invalid")
                })?;
            }
            let page = store.list(request).await.map_err(|error| {
                SdkError::from_state_store(error, "workspace inventory could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
            for snapshot in page.records {
                records.push(Self::decode(snapshot, agent_id, None)?);
            }
            let Some(next) = page.next_start_after else {
                records.sort_by(|left, right| left.id.cmp(&right.id));
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "workspace inventory exceeds its private page bound",
        ))
    }

    pub(crate) fn encode(&self) -> Result<serde_json::Value, SdkError> {
        serde_json::to_value(self)
            .map_err(|_| SdkError::invalid_state("workspace state could not be encoded"))
    }

    pub(crate) fn decode(
        snapshot: StateRecordSnapshot,
        agent_id: &AgentId,
        expected: Option<&WorkspaceId>,
    ) -> Result<Self, SdkError> {
        let record: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored workspace state is malformed"))?;
        if &record.agent_id != agent_id || expected.is_some_and(|id| id != &record.id) {
            return Err(SdkError::not_found("workspace does not exist"));
        }
        if Self::key(agent_id, &record.id)?.key() != snapshot.record.key.key() {
            return Err(SdkError::invalid_state(
                "stored workspace ownership contradicts its key",
            ));
        }
        record.info()?;
        Ok(record)
    }
}

impl ConversationRecord {
    pub(crate) fn new(
        id: ConversationId,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        request: &NewConversation,
    ) -> Self {
        Self {
            version: RECORD_VERSION,
            id,
            workspace_id,
            workspace_revision,
            title: request.title().map(str::to_owned),
            defaults: request.defaults().map(StoredDefaults::from_public),
            lifecycle: RecordLifecycle::Open,
        }
    }

    pub(crate) fn id(&self) -> &ConversationId {
        &self.id
    }

    pub(crate) fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    pub(crate) fn workspace_revision(&self) -> u64 {
        self.workspace_revision
    }

    pub(crate) fn defaults(&self) -> Result<Option<WorkspaceDefaults>, SdkError> {
        self.defaults
            .as_ref()
            .map(StoredDefaults::to_public)
            .transpose()
    }

    pub(crate) fn info(&self) -> Result<ConversationInfo, SdkError> {
        if self.version != RECORD_VERSION
            || self.lifecycle != RecordLifecycle::Open
            || self.workspace_revision == 0
        {
            return Err(SdkError::invalid_state(
                "stored conversation state is malformed",
            ));
        }
        let _ = self.defaults()?;
        ConversationInfo::new(
            self.id.clone(),
            self.workspace_id.clone(),
            self.title.clone(),
        )
        .map_err(|_| SdkError::invalid_state("stored conversation state is malformed"))
    }

    pub(crate) fn with_workspace_revision(&self, revision: u64) -> Result<Self, SdkError> {
        if revision == 0 {
            return Err(SdkError::invalid_state(
                "conversation workspace revision must be nonzero",
            ));
        }
        let mut record = self.clone();
        record.workspace_revision = revision;
        Ok(record)
    }

    pub(crate) fn key(
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<StateKey, SdkError> {
        let key = format!("{}:{}", workspace_id.as_str(), id.as_str());
        StateKey::new(CONVERSATION_NAMESPACE, key)
            .map_err(|_| SdkError::invalid_state("conversation state key is invalid"))
    }

    pub(crate) fn list_request(workspace_id: &WorkspaceId) -> Result<StateListRequest, SdkError> {
        let prefix = StateKeyPrefix::new(format!("{}:", workspace_id.as_str()))
            .map_err(|_| SdkError::invalid_state("conversation state prefix is invalid"))?;
        StateListRequest::new(CONVERSATION_NAMESPACE)
            .map(|request| request.with_key_prefix(prefix))
            .map_err(|_| SdkError::invalid_state("conversation inventory request is invalid"))
    }

    pub(crate) async fn list(
        store: &impl StateStoreBackend,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Self>, SdkError> {
        let mut records = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_RECORD_PAGES {
            let mut request = Self::list_request(workspace_id)?
                .with_limit(StateListRequest::MAX_LIMIT)
                .map_err(|_| SdkError::invalid_state("conversation inventory limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request.start_after(next).map_err(|_| {
                    SdkError::invalid_state("conversation inventory cursor is invalid")
                })?;
            }
            let page = store.list(request).await.map_err(|error| {
                SdkError::from_state_store(error, "conversation inventory could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
            for snapshot in page.records {
                records.push(Self::decode(snapshot, workspace_id, None)?);
            }
            let Some(next) = page.next_start_after else {
                records.sort_by(|left, right| left.id.cmp(&right.id));
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "conversation inventory exceeds its private page bound",
        ))
    }

    pub(crate) fn encode(&self) -> Result<serde_json::Value, SdkError> {
        serde_json::to_value(self)
            .map_err(|_| SdkError::invalid_state("conversation state could not be encoded"))
    }

    pub(crate) fn decode(
        snapshot: StateRecordSnapshot,
        workspace_id: &WorkspaceId,
        expected: Option<&ConversationId>,
    ) -> Result<Self, SdkError> {
        let record: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored conversation state is malformed"))?;
        if &record.workspace_id != workspace_id || expected.is_some_and(|id| id != &record.id) {
            return Err(SdkError::not_found("conversation does not exist"));
        }
        if Self::key(workspace_id, &record.id)?.key() != snapshot.record.key.key() {
            return Err(SdkError::invalid_state(
                "stored conversation ownership contradicts its key",
            ));
        }
        record.info()?;
        Ok(record)
    }
}
