use storage_sdk::{DatabaseBackend, StateStoreBackend};

use crate::{
    AgentId, AgentStorage, ConversationId, IngestKnowledgeRequest, KnowledgeBudget,
    KnowledgeContext, KnowledgeOwnerId, KnowledgePolicy, KnowledgeRetrievalRequest,
    ManagedKnowledge, PromptRequest, SdkError, WorkspaceId,
};

const MAX_MANAGED_CONTEXT_CHUNKS: usize = 32;
const MAX_MANAGED_CONTEXT_BYTES: usize = 64 << 10;
const MANAGED_CONTEXT_HEADING: &str = "Managed context:\n";
const EMPTY_MANAGED_CONTEXT: &str = "(none)\n";
const USER_REQUEST_HEADING: &str = "\nUser request:\n";

pub(crate) struct CodexKnowledgeBase<S: StateStoreBackend, D: DatabaseBackend> {
    knowledge: Option<ManagedKnowledge<S, D>>,
    storage: AgentStorage<S>,
    agent_id: AgentId,
}

struct CodexKnowledgeContext {
    context: KnowledgeContext,
}

impl<S, D> Clone for CodexKnowledgeBase<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    fn clone(&self) -> Self {
        Self {
            knowledge: self.knowledge.clone(),
            storage: self.storage.clone(),
            agent_id: self.agent_id.clone(),
        }
    }
}

impl<S, D> CodexKnowledgeBase<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    pub(crate) fn new(
        knowledge: Option<ManagedKnowledge<S, D>>,
        storage: AgentStorage<S>,
        agent_id: AgentId,
    ) -> Self {
        Self {
            knowledge,
            storage,
            agent_id,
        }
    }

    pub(crate) fn is_configured(&self) -> bool {
        self.knowledge.is_some()
    }

    pub(crate) fn agent_owner(&self) -> KnowledgeOwnerId {
        KnowledgeOwnerId::Agent {
            agent_id: self.agent_id.clone(),
        }
    }

    pub(crate) fn workspace_owner(&self, workspace_id: &WorkspaceId) -> KnowledgeOwnerId {
        KnowledgeOwnerId::Workspace {
            agent_id: self.agent_id.clone(),
            workspace_id: workspace_id.clone(),
        }
    }

    pub(crate) fn conversation_owner(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> KnowledgeOwnerId {
        KnowledgeOwnerId::Conversation {
            agent_id: self.agent_id.clone(),
            workspace_id: workspace_id.clone(),
            conversation_id: conversation_id.clone(),
        }
    }

    pub(crate) fn validate_policy(&self, policy: Option<&KnowledgePolicy>) -> Result<(), SdkError> {
        if let Some(policy) = policy {
            policy.validate()?;
            if self.knowledge.is_none() && matches!(policy, KnowledgePolicy::Only(_)) {
                return Err(SdkError::unsupported(
                    "selected knowledge scopes require a configured ManagedKnowledge",
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn verify(&self, owner_id: &KnowledgeOwnerId) -> Result<(), SdkError> {
        let Some(knowledge) = &self.knowledge else {
            return Ok(());
        };
        knowledge.verify(owner_id.clone()).await.map(|_| ())
    }

    pub(crate) async fn verify_before(
        &self,
        owner_id: &KnowledgeOwnerId,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        tokio::time::timeout_at(deadline, self.verify(owner_id))
            .await
            .map_err(|_| SdkError::timeout("knowledge verification deadline elapsed"))?
    }

    pub(crate) async fn ingest(
        &self,
        owner_id: KnowledgeOwnerId,
        request: IngestKnowledgeRequest,
    ) -> Result<(), SdkError> {
        request.validate()?;
        owner_id.ingestion_owner(request.scope())?;
        if request.sources().is_empty() {
            return Ok(());
        }
        let knowledge = self.knowledge.as_ref().ok_or_else(|| {
            SdkError::unsupported("knowledge ingestion requires a configured ManagedKnowledge")
        })?;
        knowledge.ingest(owner_id, request).await
    }

    pub(crate) async fn render_prompt(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
        conversation_policy: Option<&KnowledgePolicy>,
        workspace_policy: Option<&KnowledgePolicy>,
        request: &PromptRequest,
    ) -> Result<String, SdkError> {
        let policy = request
            .knowledge_policy()
            .or(conversation_policy)
            .or(workspace_policy)
            .cloned()
            .unwrap_or(KnowledgePolicy::Default);
        let retrieval = KnowledgeRetrievalRequest::new(
            request.text().to_owned(),
            self.conversation_owner(workspace_id, conversation_id),
            policy,
            KnowledgeBudget::new(MAX_MANAGED_CONTEXT_CHUNKS, MAX_MANAGED_CONTEXT_BYTES)?,
        )?;
        let context = if matches!(retrieval.policy(), KnowledgePolicy::Disabled) {
            KnowledgeContext::new(Vec::new())?
        } else if let Some(knowledge) = &self.knowledge {
            knowledge.retrieve(retrieval).await?
        } else if matches!(retrieval.policy(), KnowledgePolicy::Only(_)) {
            return Err(SdkError::unsupported(
                "selected knowledge scopes require a configured ManagedKnowledge",
            ));
        } else {
            KnowledgeContext::new(Vec::new())?
        };
        Ok(CodexKnowledgeContext::new(context).render(request.text()))
    }

    pub(crate) async fn settle_forget(
        &self,
        owner_id: &KnowledgeOwnerId,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        self.storage
            .settle_knowledge_forget(self.knowledge.as_ref(), owner_id, deadline, Some(deadline))
            .await
    }
}

impl CodexKnowledgeContext {
    fn new(context: KnowledgeContext) -> Self {
        Self { context }
    }

    fn render(&self, caller_text: &str) -> String {
        let mut managed = String::from(MANAGED_CONTEXT_HEADING);
        let mut rendered = 0_usize;
        for snippet in self.context.snippets() {
            if rendered == MAX_MANAGED_CONTEXT_CHUNKS {
                break;
            }
            let rendered_bytes = "- [".len()
                + snippet.source_id().len()
                + "] ".len()
                + snippet.title().len()
                + ": ".len()
                + snippet.text().len()
                + '\n'.len_utf8();
            if managed.len().saturating_add(rendered_bytes) > MAX_MANAGED_CONTEXT_BYTES {
                continue;
            }
            managed.push_str("- [");
            managed.push_str(snippet.source_id());
            managed.push_str("] ");
            managed.push_str(snippet.title());
            managed.push_str(": ");
            managed.push_str(snippet.text());
            managed.push('\n');
            rendered += 1;
        }

        if rendered == 0 {
            managed.push_str(EMPTY_MANAGED_CONTEXT);
        }
        managed.push_str(USER_REQUEST_HEADING);
        managed.push_str(caller_text);
        managed
    }
}
