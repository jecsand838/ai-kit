use std::path::PathBuf;

use crate::backends::codex::adapters::conversation::CodexThread;
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::{ConversationId, SdkError, WorkspaceId};

const CODEX_THREAD_BINDING_VERSION: u32 = 2;
const CODEX_FOREGROUND_POLICY_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexThreadBinding {
    version: u32,
    thread_id: String,
    conversation_id: ConversationId,
    workspace_id: WorkspaceId,
    attachment: CodexThreadAttachment,
    cwd: PathBuf,
    instructions: Option<String>,
    foreground_policy_version: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum CodexThreadAttachment {
    Materialized {
        workspace_revision: u64,
    },
    RebindRequired {
        attached_revision: u64,
        target_revision: u64,
    },
}

impl CodexThreadBinding {
    pub(crate) fn new(
        thread: &CodexThread,
        conversation_id: ConversationId,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        instructions: Option<&str>,
        thread_source: &str,
    ) -> Result<Self, SdkError> {
        thread.require_thread_source(thread_source)?;
        let binding = Self {
            version: CODEX_THREAD_BINDING_VERSION,
            thread_id: thread.id().to_owned(),
            conversation_id,
            workspace_id,
            attachment: CodexThreadAttachment::Materialized { workspace_revision },
            cwd: thread.cwd().to_path_buf(),
            instructions: instructions.map(str::to_owned),
            foreground_policy_version: CODEX_FOREGROUND_POLICY_VERSION,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    pub(crate) fn workspace_revision(&self) -> u64 {
        match self.attachment {
            CodexThreadAttachment::Materialized { workspace_revision } => workspace_revision,
            CodexThreadAttachment::RebindRequired {
                target_revision, ..
            } => target_revision,
        }
    }

    pub(crate) fn requires_rebind(&self) -> bool {
        matches!(
            self.attachment,
            CodexThreadAttachment::RebindRequired { .. }
        )
    }

    pub(crate) fn require_materialized(&self) -> Result<(), SdkError> {
        if self.requires_rebind() {
            Err(SdkError::conflict(
                "conversation attachment requires workspace-policy refresh",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn with_rebind_required(&self, target_revision: u64) -> Result<Self, SdkError> {
        let attached_revision = match self.attachment {
            CodexThreadAttachment::Materialized { workspace_revision } => workspace_revision,
            CodexThreadAttachment::RebindRequired {
                attached_revision, ..
            } => attached_revision,
        };
        if target_revision <= self.workspace_revision() || target_revision <= attached_revision {
            return Err(SdkError::invalid_state(
                "conversation attachment target must advance its workspace revision",
            ));
        }
        let mut binding = self.clone();
        binding.attachment = CodexThreadAttachment::RebindRequired {
            attached_revision,
            target_revision,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn with_materialized_thread(
        &self,
        thread: &CodexThread,
        target_revision: u64,
    ) -> Result<Self, SdkError> {
        if thread.id() != self.thread_id {
            return Err(SdkError::invalid_state(
                "rebound conversation contradicts its durable identity",
            ));
        }
        match self.attachment {
            CodexThreadAttachment::Materialized { workspace_revision }
                if workspace_revision == target_revision => {}
            CodexThreadAttachment::RebindRequired {
                target_revision: expected,
                ..
            } if expected == target_revision => {}
            _ => {
                return Err(SdkError::conflict(
                    "conversation attachment changed during workspace-policy refresh",
                ));
            }
        }
        let mut binding = self.clone();
        binding.attachment = CodexThreadAttachment::Materialized {
            workspace_revision: target_revision,
        };
        binding.cwd = thread.cwd().to_path_buf();
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn verify_native(&self, thread: &CodexThread) -> Result<(), SdkError> {
        if thread.id() != self.thread_id || (!self.requires_rebind() && thread.cwd() != self.cwd) {
            return Err(SdkError::invalid_state(
                "conversation does not match its durable attachment",
            ));
        }
        Ok(())
    }

    pub(crate) fn verify_owner(
        &self,
        conversation: &ConversationRecord,
        workspace: &WorkspaceRecord,
        instructions: Option<&str>,
    ) -> Result<(), SdkError> {
        if &self.conversation_id != conversation.id()
            || &self.workspace_id != conversation.workspace_id()
            || &self.workspace_id != workspace.id()
            || self.workspace_revision() != conversation.workspace_revision()
            || self.workspace_revision() != workspace.revision()
            || self.instructions.as_deref() != instructions
        {
            return Err(SdkError::invalid_state(
                "conversation attachment contradicts its portable owner",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        let attachment_valid = match self.attachment {
            CodexThreadAttachment::Materialized { workspace_revision } => workspace_revision > 0,
            CodexThreadAttachment::RebindRequired {
                attached_revision,
                target_revision,
            } => attached_revision > 0 && target_revision > attached_revision,
        };
        if self.version != CODEX_THREAD_BINDING_VERSION
            || self.foreground_policy_version != CODEX_FOREGROUND_POLICY_VERSION
            || self.thread_id.is_empty()
            || self.thread_id.trim() != self.thread_id
            || !attachment_valid
            || !self.cwd.is_absolute()
            || self.instructions.as_deref().is_some_and(|instructions| {
                instructions.is_empty() || instructions.trim() != instructions
            })
        {
            return Err(SdkError::invalid_state(
                "stored conversation attachment is malformed",
            ));
        }
        Ok(())
    }
}
