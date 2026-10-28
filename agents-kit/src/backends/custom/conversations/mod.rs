use tools_kit::ToolsetBackend;
pub(crate) mod runtime;

use std::path::PathBuf;

use crate::backends::custom::knowledge_base::CustomKnowledgeBase;
use crate::backends::custom::notifications::CustomNotificationReceiver;
use crate::backends::custom::state_store::CustomStateStore;
use crate::conversation::ConversationBackend;
use crate::core::hierarchy::{HierarchyAdmission, HierarchyClaim, HierarchyRuntime};
use crate::core::state::ConversationRecord;
use crate::prompt::PromptStream;
use crate::{
    ConversationId, ConversationInfo, ConversationNotificationReceiver, FileRead, KitError,
    NotificationId, NotificationResolution, PromptRequest, PromptResponse,
};
use providers_kit::ProviderBackend;
use storage_kit::{DatabaseBackend, StateStoreBackend};

pub(crate) struct CustomConversation<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    record: ConversationRecord,
    state: CustomStateStore<S, D>,
    hierarchy: HierarchyRuntime,
    claim: HierarchyClaim,
    runtime: runtime::CustomConversationRuntime<S, D, P, T>,
    knowledge: CustomKnowledgeBase<S, D>,
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomConversation<S, D, P, T>
{
    pub(crate) fn new(
        record: ConversationRecord,
        state: CustomStateStore<S, D>,
        hierarchy: HierarchyRuntime,
        claim: HierarchyClaim,
        runtime: runtime::CustomConversationRuntime<S, D, P, T>,
        knowledge: CustomKnowledgeBase<S, D>,
    ) -> Self {
        Self {
            record,
            state,
            hierarchy,
            claim,
            runtime,
            knowledge,
        }
    }

    async fn current(&self) -> Result<ConversationRecord, KitError> {
        self.claim.ensure_open()?;
        let workspace = self.state.workspace(self.record.workspace_id()).await?;
        if workspace.revision() != self.record.workspace_revision() {
            return Err(KitError::conflict("conversation handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision)
                .in_phase(crate::error::ErrorPhase::Admission));
        }
        let record = self
            .state
            .conversation(self.record.workspace_id(), self.record.id())
            .await?;
        if record.workspace_revision() != workspace.revision() {
            return Err(KitError::invalid_state(
                "conversation revision contradicts its workspace",
            ));
        }
        Ok(record)
    }

    async fn complete_close(self, admission: HierarchyAdmission) -> Result<(), KitError> {
        let result = async {
            let record = self.current().await.map_err(|error| {
                if error.kind() == crate::error::ErrorKind::NotFound {
                    KitError::closed("conversation is already closed").with_cause(error)
                } else {
                    error
                }
            })?;
            self.runtime.begin_close().await?;
            admission.mark_conversation_terminal(record.workspace_revision())?;
            self.runtime.finish_close().await?;
            let deadline = self.runtime.lifecycle_deadline();
            self.state
                .close_conversation_before(
                    record.workspace_id().clone(),
                    record.workspace_revision(),
                    record.id().clone(),
                    self.knowledge.clone(),
                    deadline,
                    None,
                )
                .await
        }
        .await;
        admission.publish(result)
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    ConversationBackend for CustomConversation<S, D, P, T>
{
    fn id(&self) -> ConversationId {
        self.record.id().clone()
    }

    async fn info(&self) -> Result<ConversationInfo, KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = async { self.current().await?.info() }.await;
        admission.publish(result)
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        self.runtime
            .prompt(
                admission,
                self.record.workspace_id().clone(),
                self.record.workspace_revision(),
                request,
            )
            .await
    }

    async fn prompt_stream(&self, request: PromptRequest) -> Result<PromptStream, KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        self.runtime
            .prompt_stream(
                admission,
                self.record.workspace_id().clone(),
                self.record.workspace_revision(),
                request,
            )
            .await
    }

    async fn abort(&self) -> Result<(), KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = async {
            self.current().await?;
            self.runtime.abort().await
        }
        .await;
        admission.publish(result)
    }

    async fn read_file(&self, path: PathBuf) -> Result<FileRead, KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = async {
            let record = self.current().await?;
            let workspace = self.state.workspace(record.workspace_id()).await?.info()?;
            self.runtime.read_file(&workspace, path).await
        }
        .await;
        admission.publish(result)
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = self.claim.ensure_open().and_then(|()| {
            let receiver = self
                .state
                .notifications()
                .receiver(self.record.workspace_id(), self.record.id())?;
            let cleanup = self.state.terminal_drain(
                &self.record,
                self.knowledge.clone(),
                self.runtime.lifecycle_timeout(),
            );
            let receiver = CustomNotificationReceiver::new(receiver, cleanup);
            self.claim.ensure_open()?;
            Ok(ConversationNotificationReceiver::custom(receiver))
        });
        admission.publish(result)
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let result = async {
            self.current().await?;
            self.runtime.resolve(id, resolution).await
        }
        .await;
        admission.publish(result)
    }

    async fn close(self) -> Result<(), KitError> {
        let deadline = self.runtime.lifecycle_deadline();
        let admission = self
            .hierarchy
            .admit_conversation_close(self.record.workspace_id(), self.record.id(), deadline)
            .await?;
        let runtime = self.runtime.clone();
        runtime
            .own_close_before(
                deadline,
                Box::pin(async move { self.complete_close(admission).await }),
            )
            .await
    }
}
