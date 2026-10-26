use tool_sdk::ToolsetBackend;
pub(crate) mod runtime;
pub(crate) mod thread;

use std::path::PathBuf;

use storage_sdk::{DatabaseBackend, StateStoreBackend};

use crate::backends::codex::adapters::file::CodexFileRead;
use crate::backends::codex::adapters::prompt::{CodexPrompt, CodexStructuredOutput};
use crate::backends::codex::knowledge_base::CodexKnowledgeBase;
use crate::backends::codex::protocol::CodexProtocol;
use crate::conversation::ConversationBackend;
use crate::core::hierarchy::{HierarchyClaim, HierarchyRuntime};
use crate::core::state::ConversationRecord;
use crate::prompt::PromptStream;
use crate::{
    ConversationId, ConversationInfo, ConversationNotificationReceiver, FileRead, NotificationId,
    NotificationResolution, PromptRequest, PromptResponse, SdkError, WorkspaceDefaults,
};

pub(crate) struct CodexConversation<S: StateStoreBackend, D: DatabaseBackend, T: ToolsetBackend> {
    record: ConversationRecord,
    hierarchy: HierarchyRuntime,
    claim: HierarchyClaim,
    lifecycle_timeout: std::time::Duration,
    default_prompt_timeout: Option<std::time::Duration>,
    runtime: runtime::CodexConversationRuntime<S, T>,
    knowledge: CodexKnowledgeBase<S, D>,
}

impl<S, D, T> CodexConversation<S, D, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    T: ToolsetBackend,
{
    pub(crate) fn new(
        record: ConversationRecord,
        hierarchy: HierarchyRuntime,
        claim: HierarchyClaim,
        lifecycle_timeout: std::time::Duration,
        default_prompt_timeout: Option<std::time::Duration>,
        runtime: runtime::CodexConversationRuntime<S, T>,
        knowledge: CodexKnowledgeBase<S, D>,
    ) -> Self {
        Self {
            record,
            hierarchy,
            claim,
            lifecycle_timeout,
            default_prompt_timeout,
            runtime,
            knowledge,
        }
    }

    async fn current(&self) -> Result<ConversationRecord, SdkError> {
        self.claim.ensure_open()?;
        let workspace = self
            .runtime
            .state()
            .workspace(self.runtime.state().agent_id(), self.record.workspace_id())
            .await?;
        if workspace.revision() != self.record.workspace_revision() {
            return Err(SdkError::conflict("conversation handle is stale"));
        }
        let record = self
            .runtime
            .state()
            .conversation(self.record.workspace_id(), self.record.id())
            .await?;
        if record.workspace_revision() != workspace.revision() {
            return Err(SdkError::invalid_state(
                "conversation revision contradicts its workspace",
            ));
        }
        let binding = self
            .runtime
            .state()
            .binding(self.record.workspace_id(), self.record.id())
            .await?;
        if &binding != self.runtime.binding() {
            return Err(SdkError::invalid_state(
                "conversation binding changed while its handle was live",
            ));
        }
        binding.require_materialized()?;
        Ok(record)
    }

    async fn prepare_prompt(
        &self,
        request: PromptRequest,
        timeout: Option<std::time::Duration>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<CodexPrompt, SdkError> {
        let preparation = async {
            CodexPrompt::validate_request(&request)?;
            let structured_output = request
                .structured_output()
                .map(CodexStructuredOutput::compile)
                .transpose()?;
            let record = self.current().await?;
            let conversation_defaults = record.defaults()?;
            let workspace = self
                .runtime
                .state()
                .workspace(self.runtime.state().agent_id(), record.workspace_id())
                .await?;
            let workspace_info = workspace.info()?;
            let model = request
                .model()
                .cloned()
                .or_else(|| {
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::model)
                        .cloned()
                })
                .or_else(|| workspace_info.defaults().model().cloned());
            let reasoning_effort = request
                .reasoning_effort()
                .cloned()
                .or_else(|| {
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::reasoning_effort)
                        .cloned()
                })
                .or_else(|| workspace_info.defaults().reasoning_effort().cloned());
            let personality = request
                .personality()
                .cloned()
                .or_else(|| {
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::personality)
                        .cloned()
                })
                .or_else(|| workspace_info.defaults().personality().cloned());
            let cwd = match request.cwd() {
                Some(relative) => workspace_info.context().resolve(relative)?.1,
                None => workspace_info.context().cwd().to_path_buf(),
            };
            let previous_selection = self.runtime.state().native_selection(record.id()).await?;
            let selection = self
                .runtime
                .protocol()
                .resolve_prompt_selection(
                    model.as_ref(),
                    reasoning_effort.as_ref(),
                    personality.as_ref(),
                    previous_selection,
                )
                .await?;
            let text = self
                .knowledge
                .render_prompt(
                    workspace.id(),
                    record.id(),
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::knowledge_policy),
                    workspace_info.defaults().knowledge_policy(),
                    &request,
                )
                .await?;
            Ok::<CodexPrompt, SdkError>(CodexPrompt::new(
                self.runtime.binding(),
                text,
                cwd,
                selection,
                structured_output,
                timeout,
            ))
        };
        match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, preparation).await {
                Ok(result) => result,
                Err(_) => Err(SdkError::timeout(
                    "prompt deadline elapsed during preflight",
                )),
            },
            None => preparation.await,
        }
    }
}

impl<S, D, T> ConversationBackend for CodexConversation<S, D, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    T: ToolsetBackend,
{
    fn id(&self) -> ConversationId {
        self.record.id().clone()
    }

    async fn info(&self) -> Result<ConversationInfo, SdkError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = async { self.current().await?.info() }.await;
        admission.publish(result)
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, SdkError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let timeout = request.timeout().or(self.default_prompt_timeout);
        let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        match self.prepare_prompt(request, timeout, deadline).await {
            Ok(prompt) => {
                self.runtime
                    .prompt(prompt, admission, deadline, self.lifecycle_timeout)
                    .await
            }
            Err(error) => admission.publish(Err(error)),
        }
    }

    async fn prompt_stream(&self, request: PromptRequest) -> Result<PromptStream, SdkError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let timeout = request.timeout().or(self.default_prompt_timeout);
        let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        match self.prepare_prompt(request, timeout, deadline).await {
            Ok(prompt) => {
                self.runtime
                    .prompt_stream(prompt, admission, deadline, self.lifecycle_timeout)
                    .await
            }
            Err(error) => admission.publish(Err(error)),
        }
    }

    async fn abort(&self) -> Result<(), SdkError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
        let result = async {
            self.current().await?;
            self.runtime.abort(deadline).await
        }
        .await;
        admission.publish(result)
    }

    async fn read_file(&self, path: PathBuf) -> Result<FileRead, SdkError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = async {
            let current = self.current().await?;
            let workspace = self
                .runtime
                .state()
                .workspace(self.runtime.state().agent_id(), current.workspace_id())
                .await?
                .info()?;
            if workspace.context().execution_policy().sandbox_mode()
                != crate::WorkspaceSandboxMode::FullAccess
            {
                return Err(SdkError::unsupported(
                    "file reads require full workspace access",
                ));
            }
            let (relative, absolute) = workspace.context().resolve(&path)?;
            let request = CodexFileRead::new(relative, absolute)
                .map_err(|error| CodexProtocol::caller_safe_error(&error))?;
            self.runtime.protocol().read_file(request).await
        }
        .await;
        admission.publish(result)
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, SdkError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = self.claim.ensure_open().and_then(|()| {
            self.runtime.binding().require_materialized()?;
            self.runtime
                .state()
                .notifications()
                .receiver(self.record.workspace_id(), self.record.id())
                .map(ConversationNotificationReceiver::shared)
        });
        admission.publish(result)
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), SdkError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = async {
            self.current().await?;
            self.runtime.resolve_notification(id, resolution).await
        }
        .await;
        admission.publish(result)
    }

    async fn close(self) -> Result<(), SdkError> {
        let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
        let admission = self
            .hierarchy
            .admit_conversation_close(self.record.workspace_id(), self.record.id(), deadline)
            .await?;
        let result = match tokio::time::timeout_at(deadline, async {
            self.current().await.map_err(|error| match error {
                error if error.kind() == crate::error::ErrorKind::NotFound => {
                    SdkError::closed("conversation was deleted while close was admitted")
                }
                error => error,
            })?;
            self.runtime.abort(deadline).await?;
            self.runtime.cancel_tool_interactions(deadline).await?;
            self.runtime
                .state()
                .close_conversation(
                    self.runtime.state().agent_id(),
                    self.record.workspace_id(),
                    self.record.workspace_revision(),
                    self.record.id(),
                    deadline,
                )
                .await
                .map_err(|error| match error {
                    error if error.kind() == crate::error::ErrorKind::NotFound => {
                        SdkError::closed("conversation was deleted while close was admitted")
                    }
                    error => error,
                })
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(SdkError::timeout("conversation close deadline elapsed")),
        };
        if let Ok(cleanup) = &result {
            let owner = self
                .knowledge
                .conversation_owner(self.record.workspace_id(), self.record.id());
            let cleanup_attempt = async {
                let _ = tokio::join!(
                    async {
                        if self
                            .runtime
                            .protocol()
                            .settle_cleanup(cleanup)
                            .await
                            .is_ok()
                        {
                            let _ = self.runtime.state().finish_cleanup(cleanup).await;
                        }
                    },
                    self.knowledge.settle_forget(&owner, deadline),
                );
            };
            let _ = tokio::time::timeout_at(deadline, cleanup_attempt).await;
        }
        admission.publish(result.map(|_| ()))
    }
}
