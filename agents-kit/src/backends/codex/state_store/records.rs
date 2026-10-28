use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use super::failure::CodexStoredFailure;
use crate::config::{CodexConfig, CodexMode};
use crate::core::state::ConversationRecord;
use crate::error::{ErrorDetail, ErrorKind, ErrorPhase};
use crate::{
    AgentId, ConversationId, ConversationNotificationKind, KitError, ModelSelector, NotificationId,
    PersonalitySelector, PromptResponse, ReasoningEffort, WorkspaceId,
};

const CODEX_RECORD_VERSION: u32 = 1;
const CODEX_PROMPT_RECORD_VERSION: u32 = 4;
const CODEX_THREAD_CREATION_VERSION: u32 = 2;
const MAX_CODEX_PROMPT_SCHEMA_BYTES: usize = 64 << 10;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexAttachment {
    version: u32,
    agent_id: AgentId,
    mode: CodexAttachmentMode,
    instructions: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CodexAttachmentMode {
    Spawn,
    Tcp(SocketAddr),
    Unix(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "owner", content = "id")]
pub(crate) enum CodexCleanupOwner {
    Agent(AgentId),
    Workspace(WorkspaceId),
    Conversation(ConversationId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodexCleanupAction {
    Delete,
    Detach,
}

impl CodexCleanupAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Detach => "detach",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexCleanup {
    version: u32,
    agent_id: AgentId,
    owner: CodexCleanupOwner,
    action: CodexCleanupAction,
    thread_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexThreadCreation {
    version: u32,
    agent_id: AgentId,
    conversation: ConversationRecord,
    thread_source: String,
    native_name: String,
    state: CodexThreadCreationState,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
enum CodexThreadCreationState {
    PreparingKnowledge,
    ReadyToDispatch,
    DispatchMayBeAccepted { native_thread_id: Option<String> },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexNativeSelection {
    model: Option<ModelSelector>,
    reasoning_effort: Option<ReasoningEffort>,
    personality: Option<PersonalitySelector>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexNativeSelectionRecord {
    version: u32,
    conversation_id: ConversationId,
    selection: CodexNativeSelection,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexPromptRecord {
    version: u32,
    conversation_id: ConversationId,
    client_message_id: String,
    output_schema: Option<serde_json::Value>,
    previous_selection: CodexNativeSelection,
    accepted_selection: CodexNativeSelection,
    tool_selection: CodexPromptToolSelection,
    timeout: Option<CodexPromptTimeout>,
    state: CodexPromptState,
    cancellation: CodexCancellation,
    waiter: CodexPromptWaiter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CodexToolAdmission {
    Execute,
    Suppress,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
enum CodexPromptState {
    Prepared,
    MayBeAccepted,
    Running {
        turn_id: String,
        output: Option<String>,
    },
    Terminal {
        turn_id: String,
        outcome: CodexPromptOutcome,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "outcome", deny_unknown_fields)]
enum CodexPromptOutcome {
    Success {
        output: String,
        structured_output: Option<serde_json::Value>,
    },
    Failure {
        error: CodexStoredFailure,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CodexCancellation {
    NotRequested,
    Requested,
    Dispatching,
    Ambiguous,
    Settled,
    Unnecessary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CodexPromptWaiter {
    Waiting,
    Delivering,
    TimedOut,
    Lost,
    Delivered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CodexPromptToolSelection {
    Automatic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CodexPromptTimeout {
    seconds: u64,
    nanoseconds: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexContinuation {
    version: u32,
    agent_id: AgentId,
    conversation_id: ConversationId,
    notification_id: NotificationId,
    request_id: serde_json::Value,
    thread_id: String,
    turn_id: Option<String>,
    kind: CodexContinuationKind,
    state: CodexContinuationState,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub(crate) enum CodexContinuationKind {
    CommandApproval {
        denial: CodexCommandDenial,
        dismissible: bool,
    },
    FileApproval,
    PermissionsApproval {
        permissions: serde_json::Value,
    },
    UserInput {
        question_ids: Vec<String>,
    },
    McpForm,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodexContinuationState {
    Pending,
    Ambiguous,
    Accepted,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodexCommandDenial {
    Decline,
    Cancel,
}

impl CodexAttachment {
    pub(crate) fn new(
        agent_id: AgentId,
        config: &CodexConfig,
        instructions: Option<&str>,
    ) -> Result<Self, KitError> {
        config.validate()?;
        let mode = match config.mode() {
            CodexMode::Spawn { .. } => CodexAttachmentMode::Spawn,
            CodexMode::Tcp { address, .. } => CodexAttachmentMode::Tcp(*address),
            CodexMode::Unix { path, .. } => CodexAttachmentMode::Unix(path.clone()),
        };
        let attachment = Self {
            version: CODEX_RECORD_VERSION,
            agent_id,
            mode,
            instructions: instructions.map(str::to_owned),
        };
        attachment.validate()?;
        Ok(attachment)
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != CODEX_RECORD_VERSION {
            return Err(KitError::invalid_state(
                "stored attachment version is unsupported",
            ));
        }
        match &self.mode {
            CodexAttachmentMode::Spawn => {}
            CodexAttachmentMode::Tcp(address) if address.port() == 0 => {
                return Err(KitError::invalid_state(
                    "stored runtime attachment is invalid",
                ));
            }
            CodexAttachmentMode::Unix(path) if path.as_os_str().is_empty() => {
                return Err(KitError::invalid_state(
                    "stored runtime attachment is invalid",
                ));
            }
            CodexAttachmentMode::Tcp(_) | CodexAttachmentMode::Unix(_) => {}
        }
        if self.instructions.as_ref().is_some_and(|instructions| {
            instructions.is_empty() || instructions.trim() != instructions
        }) {
            return Err(KitError::invalid_state(
                "stored instruction policy is malformed",
            ));
        }
        Ok(())
    }
}

impl CodexCleanup {
    pub(crate) fn workspace(
        agent_id: AgentId,
        id: WorkspaceId,
        thread_ids: Vec<String>,
    ) -> Result<Self, KitError> {
        Self::new(
            agent_id,
            CodexCleanupOwner::Workspace(id),
            CodexCleanupAction::Delete,
            thread_ids,
        )
    }

    pub(crate) fn conversation(
        agent_id: AgentId,
        id: ConversationId,
        thread_id: String,
    ) -> Result<Self, KitError> {
        Self::new(
            agent_id,
            CodexCleanupOwner::Conversation(id),
            CodexCleanupAction::Delete,
            vec![thread_id],
        )
    }

    pub(crate) fn detach_agent(
        agent_id: AgentId,
        thread_ids: Vec<String>,
    ) -> Result<Self, KitError> {
        Self::new(
            agent_id.clone(),
            CodexCleanupOwner::Agent(agent_id),
            CodexCleanupAction::Detach,
            thread_ids,
        )
    }

    pub(crate) fn detach_workspace(
        agent_id: AgentId,
        id: WorkspaceId,
        thread_ids: Vec<String>,
    ) -> Result<Self, KitError> {
        Self::new(
            agent_id,
            CodexCleanupOwner::Workspace(id),
            CodexCleanupAction::Detach,
            thread_ids,
        )
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn thread_ids(&self) -> &[String] {
        &self.thread_ids
    }

    pub(crate) fn action(&self) -> CodexCleanupAction {
        self.action
    }

    pub(crate) fn matches_conversation_deletion(
        &self,
        conversation_id: &ConversationId,
        thread_id: &str,
    ) -> bool {
        self.action == CodexCleanupAction::Delete
            && matches!(
                &self.owner,
                CodexCleanupOwner::Conversation(owner) if owner == conversation_id
            )
            && self.thread_ids.as_slice() == [thread_id]
    }

    pub(crate) fn key_fragment(&self) -> String {
        match &self.owner {
            CodexCleanupOwner::Agent(id) => format!(
                "{}:{}:agent:{}",
                self.agent_id.as_str(),
                self.action.as_str(),
                id.as_str()
            ),
            CodexCleanupOwner::Workspace(id) => {
                format!(
                    "{}:{}:workspace:{}",
                    self.agent_id.as_str(),
                    self.action.as_str(),
                    id.as_str()
                )
            }
            CodexCleanupOwner::Conversation(id) => {
                format!(
                    "{}:{}:conversation:{}",
                    self.agent_id.as_str(),
                    self.action.as_str(),
                    id.as_str()
                )
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != CODEX_RECORD_VERSION
            || self.thread_ids.is_empty()
            || self
                .thread_ids
                .iter()
                .any(|id| id.is_empty() || id.trim() != id)
        {
            return Err(KitError::invalid_state(
                "stored cleanup obligation is malformed",
            ));
        }
        let mut sorted = self.thread_ids.clone();
        sorted.sort();
        sorted.dedup();
        if sorted.len() != self.thread_ids.len() {
            return Err(KitError::invalid_state(
                "stored cleanup obligation contains duplicate conversations",
            ));
        }
        Ok(())
    }

    fn new(
        agent_id: AgentId,
        owner: CodexCleanupOwner,
        action: CodexCleanupAction,
        mut thread_ids: Vec<String>,
    ) -> Result<Self, KitError> {
        thread_ids.sort();
        thread_ids.dedup();
        let cleanup = Self {
            version: CODEX_RECORD_VERSION,
            agent_id,
            owner,
            action,
            thread_ids,
        };
        cleanup.validate()?;
        Ok(cleanup)
    }
}

impl CodexThreadCreation {
    pub(crate) fn new(
        agent_id: AgentId,
        conversation: ConversationRecord,
        native_name: String,
    ) -> Result<Self, KitError> {
        let creation = Self {
            version: CODEX_THREAD_CREATION_VERSION,
            agent_id,
            conversation,
            thread_source: format!("agents-kit-create-{}", uuid::Uuid::new_v4().simple()),
            native_name,
            state: CodexThreadCreationState::PreparingKnowledge,
        };
        creation.validate()?;
        Ok(creation)
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn conversation(&self) -> &ConversationRecord {
        &self.conversation
    }

    pub(crate) fn thread_source(&self) -> &str {
        &self.thread_source
    }

    pub(crate) fn native_thread_id(&self) -> Option<&str> {
        match &self.state {
            CodexThreadCreationState::DispatchMayBeAccepted { native_thread_id } => {
                native_thread_id.as_deref()
            }
            CodexThreadCreationState::PreparingKnowledge
            | CodexThreadCreationState::ReadyToDispatch => None,
        }
    }

    pub(crate) fn native_name(&self) -> &str {
        &self.native_name
    }

    pub(crate) fn knowledge_ready(&self) -> bool {
        !matches!(self.state, CodexThreadCreationState::PreparingKnowledge)
    }

    pub(crate) fn dispatch_ready(&self) -> bool {
        matches!(self.state, CodexThreadCreationState::ReadyToDispatch)
    }

    pub(crate) fn dispatch_may_be_accepted(&self) -> bool {
        matches!(
            self.state,
            CodexThreadCreationState::DispatchMayBeAccepted { .. }
        )
    }

    pub(crate) fn with_knowledge_ready(&self) -> Result<Self, KitError> {
        let mut creation = self.clone();
        match creation.state {
            CodexThreadCreationState::PreparingKnowledge => {
                creation.state = CodexThreadCreationState::ReadyToDispatch;
            }
            CodexThreadCreationState::ReadyToDispatch
            | CodexThreadCreationState::DispatchMayBeAccepted { .. } => {}
        }
        creation.validate()?;
        Ok(creation)
    }

    pub(crate) fn with_dispatch_started(&self) -> Result<Self, KitError> {
        let mut creation = self.clone();
        match creation.state {
            CodexThreadCreationState::ReadyToDispatch => {
                creation.state = CodexThreadCreationState::DispatchMayBeAccepted {
                    native_thread_id: None,
                };
            }
            CodexThreadCreationState::DispatchMayBeAccepted { .. } => {}
            CodexThreadCreationState::PreparingKnowledge => {
                return Err(KitError::conflict(
                    "conversation creation knowledge is not durably ready",
                ));
            }
        }
        creation.validate()?;
        Ok(creation)
    }

    pub(crate) fn with_native_thread_id(&self, thread_id: &str) -> Result<Self, KitError> {
        let mut creation = self.clone();
        match &mut creation.state {
            CodexThreadCreationState::DispatchMayBeAccepted { native_thread_id } => {
                if let Some(current) = native_thread_id {
                    if current != thread_id {
                        return Err(KitError::invalid_state(
                            "conversation creation state matched multiple runtime conversations",
                        ));
                    }
                } else {
                    *native_thread_id = Some(thread_id.to_owned());
                }
            }
            CodexThreadCreationState::PreparingKnowledge
            | CodexThreadCreationState::ReadyToDispatch => {
                return Err(KitError::invalid_state(
                    "conversation creation was observed before runtime dispatch",
                ));
            }
        }
        creation.validate()?;
        Ok(creation)
    }

    pub(crate) fn matches_obligation(&self, expected: &Self, thread_id: &str) -> bool {
        self.version == expected.version
            && self.agent_id == expected.agent_id
            && self.conversation == expected.conversation
            && self.thread_source == expected.thread_source
            && self.native_name == expected.native_name
            && self.dispatch_may_be_accepted()
            && expected.dispatch_may_be_accepted()
            && self
                .native_thread_id()
                .is_none_or(|observed| observed == thread_id)
            && expected
                .native_thread_id()
                .is_none_or(|observed| observed == thread_id)
    }

    pub(crate) fn key_fragment(&self) -> &str {
        self.conversation.id().as_str()
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        let suffix = self
            .thread_source
            .strip_prefix("agents-kit-create-")
            .filter(|suffix| {
                suffix.len() == 32
                    && suffix
                        .bytes()
                        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            });
        if self.version != CODEX_THREAD_CREATION_VERSION
            || suffix.is_none()
            || self
                .native_thread_id()
                .is_some_and(|id| id.is_empty() || id.trim() != id)
            || self.native_name.is_empty()
            || self.native_name.trim() != self.native_name
        {
            return Err(KitError::invalid_state(
                "stored conversation creation is malformed",
            ));
        }
        self.conversation.info().map(|_| ())
    }
}

impl CodexNativeSelection {
    pub(crate) fn from_parts(
        model: Option<ModelSelector>,
        reasoning_effort: Option<ReasoningEffort>,
        personality: Option<PersonalitySelector>,
    ) -> Self {
        Self {
            model,
            reasoning_effort,
            personality,
        }
    }

    pub(crate) fn model(&self) -> Option<&ModelSelector> {
        self.model.as_ref()
    }

    pub(crate) fn reasoning_effort(&self) -> Option<&ReasoningEffort> {
        self.reasoning_effort.as_ref()
    }

    pub(crate) fn personality(&self) -> Option<&PersonalitySelector> {
        self.personality.as_ref()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.model.is_none() && self.reasoning_effort.is_none() && self.personality.is_none()
    }
}

impl CodexNativeSelectionRecord {
    pub(crate) fn new(conversation_id: ConversationId, selection: CodexNativeSelection) -> Self {
        Self {
            version: CODEX_RECORD_VERSION,
            conversation_id,
            selection,
        }
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn selection(&self) -> &CodexNativeSelection {
        &self.selection
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != CODEX_RECORD_VERSION {
            return Err(KitError::invalid_state(
                "stored runtime selection version is unsupported",
            ));
        }
        Ok(())
    }
}

impl CodexPromptRecord {
    pub(crate) fn prepared(
        conversation_id: ConversationId,
        client_message_id: String,
        output_schema: Option<serde_json::Value>,
        previous_selection: CodexNativeSelection,
        accepted_selection: CodexNativeSelection,
        timeout: Option<Duration>,
    ) -> Result<Self, KitError> {
        let record = Self {
            version: CODEX_PROMPT_RECORD_VERSION,
            conversation_id,
            client_message_id,
            output_schema,
            previous_selection,
            accepted_selection,
            tool_selection: CodexPromptToolSelection::Automatic,
            timeout: timeout.map(CodexPromptTimeout::from_duration),
            state: CodexPromptState::Prepared,
            cancellation: CodexCancellation::NotRequested,
            waiter: CodexPromptWaiter::Waiting,
        };
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn client_message_id(&self) -> &str {
        &self.client_message_id
    }

    pub(crate) fn output_schema(&self) -> Option<&serde_json::Value> {
        self.output_schema.as_ref()
    }

    pub(crate) fn previous_selection(&self) -> &CodexNativeSelection {
        &self.previous_selection
    }

    pub(crate) fn accepted_selection(&self) -> &CodexNativeSelection {
        &self.accepted_selection
    }

    pub(crate) fn turn_id(&self) -> Option<&str> {
        match &self.state {
            CodexPromptState::Prepared | CodexPromptState::MayBeAccepted => None,
            CodexPromptState::Running { turn_id, .. }
            | CodexPromptState::Terminal { turn_id, .. } => Some(turn_id),
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(self.state, CodexPromptState::Terminal { .. })
    }

    pub(crate) fn cancellation_releasable(&self) -> bool {
        matches!(
            self.cancellation,
            CodexCancellation::NotRequested
                | CodexCancellation::Settled
                | CodexCancellation::Unnecessary
        )
    }

    pub(crate) fn clearable(&self) -> bool {
        self.is_terminal()
            && self.cancellation_releasable()
            && matches!(
                self.waiter,
                CodexPromptWaiter::TimedOut
                    | CodexPromptWaiter::Lost
                    | CodexPromptWaiter::Delivered
            )
    }

    pub(crate) fn mark_may_be_accepted(&mut self) -> Result<(), KitError> {
        match self.state {
            CodexPromptState::Prepared => {
                self.state = CodexPromptState::MayBeAccepted;
                self.validate()
            }
            CodexPromptState::MayBeAccepted => Ok(()),
            CodexPromptState::Running { .. } | CodexPromptState::Terminal { .. } => Err(
                KitError::conflict("prompt acceptance boundary already advanced"),
            ),
        }
    }

    pub(crate) fn rejectable_without_turn(&self) -> bool {
        matches!(
            self.state,
            CodexPromptState::Prepared | CodexPromptState::MayBeAccepted
        )
    }

    pub(crate) fn request_cancellation(&mut self) -> Result<bool, KitError> {
        if self.is_terminal() {
            return Ok(false);
        }
        match self.cancellation {
            CodexCancellation::NotRequested => {
                self.cancellation = CodexCancellation::Requested;
                self.validate()?;
                Ok(true)
            }
            CodexCancellation::Requested
            | CodexCancellation::Dispatching
            | CodexCancellation::Ambiguous
            | CodexCancellation::Settled
            | CodexCancellation::Unnecessary => Ok(false),
        }
    }

    pub(crate) fn mark_waiter_timed_out(&mut self) -> Result<bool, KitError> {
        match self.waiter {
            CodexPromptWaiter::Waiting if self.is_terminal() => Ok(false),
            CodexPromptWaiter::Waiting => {
                self.waiter = CodexPromptWaiter::TimedOut;
                let _ = self.request_cancellation()?;
                self.validate()?;
                Ok(true)
            }
            CodexPromptWaiter::TimedOut => Ok(true),
            CodexPromptWaiter::Delivering
            | CodexPromptWaiter::Lost
            | CodexPromptWaiter::Delivered => Ok(false),
        }
    }

    pub(crate) fn claim_waiter_delivery(&mut self) -> Result<bool, KitError> {
        if !self.is_terminal() || !self.cancellation_releasable() {
            return Err(KitError::invalid_state(
                "prompt is not ready for terminal delivery",
            ));
        }
        match self.waiter {
            CodexPromptWaiter::Waiting => {
                self.waiter = CodexPromptWaiter::Delivering;
                self.validate()?;
                Ok(true)
            }
            CodexPromptWaiter::TimedOut
            | CodexPromptWaiter::Lost
            | CodexPromptWaiter::Delivered => Ok(false),
            CodexPromptWaiter::Delivering => Ok(true),
        }
    }

    pub(crate) fn mark_waiter_lost(&mut self) -> Result<(), KitError> {
        match self.waiter {
            CodexPromptWaiter::Waiting | CodexPromptWaiter::Delivering => {
                self.waiter = CodexPromptWaiter::Lost;
                self.validate()
            }
            CodexPromptWaiter::TimedOut | CodexPromptWaiter::Lost => Ok(()),
            CodexPromptWaiter::Delivered => Err(KitError::conflict(
                "delivered prompt waiter cannot become lost",
            )),
        }
    }

    pub(crate) fn mark_waiter_delivery(&mut self, delivered: bool) -> Result<(), KitError> {
        match (self.waiter, delivered) {
            (CodexPromptWaiter::Delivering, true) => {
                self.waiter = CodexPromptWaiter::Delivered;
                self.validate()
            }
            (CodexPromptWaiter::Delivering, false) => self.mark_waiter_lost(),
            (CodexPromptWaiter::Lost, false) | (CodexPromptWaiter::Delivered, true) => Ok(()),
            (CodexPromptWaiter::Waiting, _)
            | (CodexPromptWaiter::TimedOut, _)
            | (CodexPromptWaiter::Lost, true)
            | (CodexPromptWaiter::Delivered, false) => Err(KitError::conflict(
                "prompt waiter has a contradictory delivery outcome",
            )),
        }
    }

    pub(crate) fn claim_cancellation(
        &mut self,
        retry_ambiguous: bool,
    ) -> Result<Option<String>, KitError> {
        let Some(turn_id) = self.turn_id().map(str::to_owned) else {
            return Ok(None);
        };
        match self.cancellation {
            CodexCancellation::Requested => {
                self.cancellation = CodexCancellation::Dispatching;
                self.validate()?;
                Ok(Some(turn_id))
            }
            CodexCancellation::Dispatching if retry_ambiguous => Ok(Some(turn_id)),
            CodexCancellation::Ambiguous if retry_ambiguous => {
                self.cancellation = CodexCancellation::Dispatching;
                self.validate()?;
                Ok(Some(turn_id))
            }
            CodexCancellation::NotRequested
            | CodexCancellation::Dispatching
            | CodexCancellation::Ambiguous
            | CodexCancellation::Settled
            | CodexCancellation::Unnecessary => Ok(None),
        }
    }

    pub(crate) fn settle_cancellation(
        &mut self,
        turn_id: &str,
        ambiguous: bool,
    ) -> Result<(), KitError> {
        if self.turn_id() != Some(turn_id) {
            return Err(KitError::conflict(
                "cancellation belongs to another prompt generation",
            ));
        }
        let settled = if ambiguous {
            CodexCancellation::Ambiguous
        } else {
            CodexCancellation::Settled
        };
        match (self.cancellation, settled) {
            (CodexCancellation::Dispatching, settled) => {
                self.cancellation = settled;
                self.validate()
            }
            (current, settled) if current == settled => Ok(()),
            (CodexCancellation::Ambiguous, CodexCancellation::Settled) => {
                self.cancellation = CodexCancellation::Settled;
                self.validate()
            }
            (CodexCancellation::Settled, CodexCancellation::Ambiguous) => Ok(()),
            _ => Err(KitError::invalid_state(
                "cancellation dispatch is not in flight",
            )),
        }
    }

    pub(crate) fn accept(
        &mut self,
        client_message_id: &str,
        turn_id: &str,
    ) -> Result<(), KitError> {
        if self.client_message_id != client_message_id {
            return Err(KitError::conflict(
                "prompt execution belongs to another prompt generation",
            ));
        }
        self.observe_turn(turn_id)
    }

    pub(crate) fn observe_turn(&mut self, turn_id: &str) -> Result<(), KitError> {
        match &self.state {
            CodexPromptState::MayBeAccepted => {
                self.state = CodexPromptState::Running {
                    turn_id: turn_id.to_owned(),
                    output: None,
                };
                self.validate()
            }
            CodexPromptState::Running {
                turn_id: accepted, ..
            }
            | CodexPromptState::Terminal {
                turn_id: accepted, ..
            } if accepted == turn_id => Ok(()),
            CodexPromptState::Prepared => Err(KitError::invalid_state(
                "prompt execution arrived before the acceptance boundary",
            )),
            CodexPromptState::Running { .. } | CodexPromptState::Terminal { .. } => Err(
                KitError::conflict("prompt execution belongs to another prompt generation"),
            ),
        }
    }

    pub(crate) fn observe_tool_turn(
        &mut self,
        turn_id: &str,
    ) -> Result<CodexToolAdmission, KitError> {
        match &self.state {
            CodexPromptState::Prepared => Err(KitError::invalid_state(
                "dynamic tool work arrived before prompt acceptance",
            )),
            CodexPromptState::MayBeAccepted => {
                self.observe_turn(turn_id)?;
                self.tool_admission(turn_id)
            }
            CodexPromptState::Running {
                turn_id: accepted, ..
            }
            | CodexPromptState::Terminal {
                turn_id: accepted, ..
            } if accepted != turn_id => Ok(CodexToolAdmission::Suppress),
            CodexPromptState::Running { .. } | CodexPromptState::Terminal { .. } => {
                self.tool_admission(turn_id)
            }
        }
    }

    fn tool_admission(&self, turn_id: &str) -> Result<CodexToolAdmission, KitError> {
        match &self.state {
            CodexPromptState::Running {
                turn_id: accepted, ..
            } if accepted == turn_id => match self.cancellation {
                CodexCancellation::NotRequested => Ok(CodexToolAdmission::Execute),
                CodexCancellation::Requested
                | CodexCancellation::Dispatching
                | CodexCancellation::Ambiguous
                | CodexCancellation::Settled
                | CodexCancellation::Unnecessary => Ok(CodexToolAdmission::Suppress),
            },
            CodexPromptState::Terminal {
                turn_id: accepted, ..
            } if accepted == turn_id => Ok(CodexToolAdmission::Suppress),
            CodexPromptState::Prepared | CodexPromptState::MayBeAccepted => Err(
                KitError::invalid_state("dynamic tool work arrived before prompt acceptance"),
            ),
            CodexPromptState::Running { .. } | CodexPromptState::Terminal { .. } => Err(
                KitError::conflict("dynamic tool work belongs to another prompt generation"),
            ),
        }
    }

    pub(crate) fn observe_output(
        &mut self,
        turn_id: &str,
        text: &str,
        eligible: bool,
    ) -> Result<(), KitError> {
        self.observe_turn(turn_id)?;
        let CodexPromptState::Running {
            turn_id: accepted,
            output,
        } = &mut self.state
        else {
            return Err(KitError::invalid_state(
                "output arrived after terminal state",
            ));
        };
        if accepted != turn_id {
            return Err(KitError::conflict(
                "output belongs to another prompt generation",
            ));
        }
        if eligible && !text.trim().is_empty() {
            *output = Some(text.to_owned());
        }
        Ok(())
    }

    pub(crate) fn terminal(
        &mut self,
        turn_id: &str,
        result: &Result<PromptResponse, KitError>,
    ) -> Result<(), KitError> {
        let outcome = match result {
            Ok(response) => {
                let (output, structured_output, usage) = response.clone().into_parts();
                if usage.is_some() || structured_output.is_some() != self.output_schema.is_some() {
                    return Err(KitError::invalid_state(
                        "terminal response contradicts its durable request",
                    ));
                }
                CodexPromptOutcome::Success {
                    output,
                    structured_output,
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Cancelled
                        | ErrorKind::NotYetImplemented
                        | ErrorKind::Protocol
                        | ErrorKind::Integration
                ) =>
            {
                CodexPromptOutcome::Failure {
                    error: CodexStoredFailure::capture(error),
                }
            }
            Err(_) => {
                return Err(KitError::invalid_state(
                    "terminal used a nonterminal error category",
                ));
            }
        };
        match &self.state {
            CodexPromptState::MayBeAccepted => {}
            CodexPromptState::Running {
                turn_id: accepted, ..
            } if accepted == turn_id => {}
            CodexPromptState::Prepared => {
                return Err(KitError::invalid_state(
                    "terminal arrived before the acceptance boundary",
                ));
            }
            CodexPromptState::Running { .. } => {
                return Err(KitError::conflict(
                    "terminal belongs to another prompt generation",
                ));
            }
            CodexPromptState::Terminal {
                turn_id: accepted,
                outcome: existing,
            } if accepted == turn_id && existing == &outcome => return Ok(()),
            CodexPromptState::Terminal {
                turn_id: accepted,
                outcome: CodexPromptOutcome::Failure { error },
            } if accepted == turn_id && error.kind() == ErrorKind::NotYetImplemented => {}
            CodexPromptState::Terminal { .. } => {
                return Err(KitError::conflict("prompt is already terminal"));
            }
        };
        self.state = CodexPromptState::Terminal {
            turn_id: turn_id.to_owned(),
            outcome,
        };
        self.cancellation = match self.cancellation {
            CodexCancellation::Requested => CodexCancellation::Unnecessary,
            CodexCancellation::Dispatching | CodexCancellation::Ambiguous => {
                CodexCancellation::Settled
            }
            cancellation => cancellation,
        };
        self.validate()
    }

    pub(crate) fn output(&self) -> Option<&str> {
        match &self.state {
            CodexPromptState::Prepared | CodexPromptState::MayBeAccepted => None,
            CodexPromptState::Running { output, .. } => output.as_deref(),
            CodexPromptState::Terminal {
                outcome: CodexPromptOutcome::Success { output, .. },
                ..
            } => Some(output),
            CodexPromptState::Terminal { .. } => None,
        }
    }

    pub(crate) fn terminal_result(&self) -> Option<Result<PromptResponse, KitError>> {
        let CodexPromptState::Terminal { outcome, .. } = &self.state else {
            return None;
        };
        Some(match outcome {
            CodexPromptOutcome::Success {
                output,
                structured_output,
            } => Ok(PromptResponse::from_parts(
                output.clone(),
                structured_output.clone(),
                None,
            )),
            CodexPromptOutcome::Failure { error } => Err(error.restore()),
        })
    }

    pub(crate) fn check_version(value: &serde_json::Value) -> Result<(), KitError> {
        if value.get("version").and_then(serde_json::Value::as_u64)
            != Some(u64::from(CODEX_PROMPT_RECORD_VERSION))
        {
            return Err(
                KitError::invalid_state("stored prompt record version is unsupported")
                    .in_phase(ErrorPhase::StateRead)
                    .with_detail(ErrorDetail::InvalidData),
            );
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        let schema_valid = self.output_schema.as_ref().is_none_or(|schema| {
            schema.as_object().is_some_and(|object| !object.is_empty())
                && serde_json::to_vec(schema)
                    .is_ok_and(|encoded| encoded.len() <= MAX_CODEX_PROMPT_SCHEMA_BYTES)
        });
        let timeout_valid = self.timeout.is_none_or(CodexPromptTimeout::is_valid);
        let turn_valid = self
            .turn_id()
            .is_none_or(|turn_id| !turn_id.is_empty() && turn_id.trim() == turn_id);
        let terminal_output_valid = match &self.state {
            CodexPromptState::Terminal {
                outcome:
                    CodexPromptOutcome::Success {
                        structured_output, ..
                    },
                ..
            } => structured_output.is_some() == self.output_schema.is_some(),
            CodexPromptState::Terminal {
                outcome: CodexPromptOutcome::Failure { error },
                ..
            } => matches!(
                error.kind(),
                ErrorKind::Cancelled
                    | ErrorKind::NotYetImplemented
                    | ErrorKind::Protocol
                    | ErrorKind::Integration
            ),
            _ => true,
        };
        let state_cancellation_valid = !matches!(
            (&self.state, self.cancellation),
            (
                CodexPromptState::Prepared | CodexPromptState::MayBeAccepted,
                CodexCancellation::Dispatching
                    | CodexCancellation::Ambiguous
                    | CodexCancellation::Settled
                    | CodexCancellation::Unnecessary
            ) | (
                CodexPromptState::Running { .. },
                CodexCancellation::Unnecessary
            ) | (
                CodexPromptState::Terminal { .. },
                CodexCancellation::Requested
                    | CodexCancellation::Dispatching
                    | CodexCancellation::Ambiguous
            )
        );
        let waiter_cancellation_valid = match self.waiter {
            CodexPromptWaiter::Waiting => true,
            CodexPromptWaiter::Delivering | CodexPromptWaiter::Delivered => {
                self.is_terminal() && self.cancellation_releasable()
            }
            CodexPromptWaiter::TimedOut => {
                self.is_terminal() || self.cancellation != CodexCancellation::NotRequested
            }
            CodexPromptWaiter::Lost => true,
        };
        if self.version != CODEX_PROMPT_RECORD_VERSION
            || self.client_message_id.is_empty()
            || self.client_message_id.trim() != self.client_message_id
            || !schema_valid
            || !timeout_valid
            || !turn_valid
            || !terminal_output_valid
            || !state_cancellation_valid
            || !waiter_cancellation_valid
        {
            return Err(KitError::invalid_state("stored prompt state is malformed"));
        }
        let _ = self.tool_selection;
        Ok(())
    }
}

impl CodexPromptTimeout {
    fn from_duration(duration: Duration) -> Self {
        Self {
            seconds: duration.as_secs(),
            nanoseconds: duration.subsec_nanos(),
        }
    }

    fn is_valid(self) -> bool {
        self.nanoseconds < 1_000_000_000 && (self.seconds != 0 || self.nanoseconds != 0)
    }
}
impl CodexContinuation {
    pub(crate) fn new(
        agent_id: AgentId,
        conversation_id: ConversationId,
        notification_id: NotificationId,
        request_id: serde_json::Value,
        thread_id: String,
        turn_id: Option<String>,
        kind: CodexContinuationKind,
    ) -> Result<Self, KitError> {
        let continuation = Self {
            version: CODEX_RECORD_VERSION,
            agent_id,
            conversation_id,
            notification_id,
            request_id,
            thread_id,
            turn_id,
            kind,
            state: CodexContinuationState::Pending,
        };
        continuation.validate()?;
        Ok(continuation)
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn notification_id(&self) -> &NotificationId {
        &self.notification_id
    }

    pub(crate) fn request_id(&self) -> &serde_json::Value {
        &self.request_id
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    pub(crate) fn kind(&self) -> &CodexContinuationKind {
        &self.kind
    }

    pub(crate) fn state(&self) -> CodexContinuationState {
        self.state
    }

    pub(crate) fn is_unresolved(&self) -> bool {
        matches!(
            self.state,
            CodexContinuationState::Pending | CodexContinuationState::Ambiguous
        )
    }

    pub(crate) fn with_state(&self, state: CodexContinuationState) -> Result<Self, KitError> {
        let mut continuation = self.clone();
        continuation.state = state;
        continuation.validate()?;
        Ok(continuation)
    }

    pub(crate) fn has_same_request(&self, other: &Self) -> bool {
        self.agent_id == other.agent_id
            && self.conversation_id == other.conversation_id
            && self.notification_id == other.notification_id
            && self.request_id == other.request_id
            && self.thread_id == other.thread_id
            && self.turn_id == other.turn_id
            && self.kind == other.kind
    }

    pub(crate) fn key_fragment(&self) -> String {
        format!(
            "{}:{}",
            self.conversation_id.as_str(),
            self.notification_id.as_str()
        )
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.version != CODEX_RECORD_VERSION
            || self.thread_id.is_empty()
            || self.thread_id.trim() != self.thread_id
            || self.notification_id.as_str().is_empty()
            || self
                .turn_id
                .as_deref()
                .is_some_and(|turn_id| turn_id.is_empty() || turn_id.trim() != turn_id)
            || !(self.request_id.as_str().is_some() || self.request_id.as_i64().is_some())
        {
            return Err(KitError::invalid_state(
                "stored interaction continuation is malformed",
            ));
        }
        match &self.kind {
            CodexContinuationKind::PermissionsApproval { permissions }
                if !permissions.is_object() =>
            {
                Err(KitError::invalid_state(
                    "stored permission request is malformed",
                ))
            }
            CodexContinuationKind::UserInput { question_ids }
                if question_ids.is_empty()
                    || question_ids
                        .iter()
                        .any(|id| id.is_empty() || id.trim() != id)
                    || {
                        let mut unique = question_ids.clone();
                        unique.sort();
                        unique.dedup();
                        unique.len() != question_ids.len()
                    } =>
            {
                Err(KitError::invalid_state("stored input request is malformed"))
            }
            CodexContinuationKind::CommandApproval { .. }
            | CodexContinuationKind::FileApproval
            | CodexContinuationKind::PermissionsApproval { .. }
            | CodexContinuationKind::UserInput { .. }
            | CodexContinuationKind::McpForm => Ok(()),
        }
    }
}

impl CodexContinuationKind {
    pub(crate) fn validate_notification(
        &self,
        notification: &ConversationNotificationKind,
    ) -> Result<(), KitError> {
        let ConversationNotificationKind::Interaction {
            response_schema, ..
        } = notification
        else {
            return Err(KitError::protocol(
                "continuation did not produce an interaction",
            ));
        };
        let structured = matches!(self, Self::UserInput { .. } | Self::McpForm);
        if structured != response_schema.is_some() {
            return Err(KitError::protocol(
                "interaction schema contradicts its continuation",
            ));
        }
        if let (
            Self::CommandApproval { dismissible, .. },
            ConversationNotificationKind::Interaction {
                dismissible: actual,
                ..
            },
        ) = (self, notification)
            && dismissible != actual
        {
            return Err(KitError::protocol(
                "command decision set contradicts its interaction",
            ));
        }
        Ok(())
    }
}
