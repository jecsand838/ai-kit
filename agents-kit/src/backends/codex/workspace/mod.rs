use crate::backends::codex::adapters::prompt::CodexPromptSelection;
use storage_kit::{DatabaseBackend, StateStoreBackend};
use tools_kit::ToolsetBackend;

use crate::backends::codex::conversations::CodexConversation;
use crate::backends::codex::conversations::runtime::CodexConversationRuntime;
use crate::backends::codex::conversations::thread::CodexThreadBinding;
use crate::backends::codex::knowledge_base::CodexKnowledgeBase;
use crate::backends::codex::protocol::CodexProtocol;
use crate::backends::codex::state_store::CodexStateStore;
use crate::backends::codex::state_store::records::{CodexCleanup, CodexThreadCreation};
use crate::backends::codex::tools::CodexTools;
use crate::core::hierarchy::{HierarchyClaim, HierarchyRuntime};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::error::ErrorPhase;
use crate::workspace::WorkspaceBackend;
use crate::{
    AgentConfig, ConversationId, ConversationInfo, IngestKnowledgeRequest, KitError,
    NewConversation, ToolInfo, WorkspaceId, WorkspaceInfo,
};

pub(crate) struct CodexWorkspace<S: StateStoreBackend, D: DatabaseBackend, T: ToolsetBackend> {
    record: WorkspaceRecord,
    state: CodexStateStore<S>,
    protocol: CodexProtocol,
    hierarchy: HierarchyRuntime,
    claim: HierarchyClaim,
    lifecycle_timeout: std::time::Duration,
    default_prompt_timeout: Option<std::time::Duration>,
    knowledge: CodexKnowledgeBase<S, D>,
    tools: CodexTools<S, T>,
}

impl<S, D, T> CodexWorkspace<S, D, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    T: ToolsetBackend,
{
    pub(crate) fn new(
        handle: (WorkspaceRecord, HierarchyClaim),
        state: CodexStateStore<S>,
        protocol: CodexProtocol,
        hierarchy: HierarchyRuntime,
        config: &AgentConfig,
        knowledge: CodexKnowledgeBase<S, D>,
        tools: CodexTools<S, T>,
    ) -> Self {
        let (record, claim) = handle;
        Self {
            record,
            state,
            protocol,
            hierarchy,
            claim,
            lifecycle_timeout: config.lifecycle_timeout(),
            default_prompt_timeout: config.default_prompt_timeout(),
            knowledge,
            tools,
        }
    }

    async fn current(&self) -> Result<WorkspaceRecord, KitError> {
        self.claim.ensure_open()?;
        let current = self
            .state
            .workspace(self.state.agent_id(), self.record.id())
            .await?;
        if current.revision() != self.record.revision() {
            return Err(KitError::conflict("workspace handle is stale"));
        }
        Ok(current)
    }
}

impl<S, D, T> WorkspaceBackend for CodexWorkspace<S, D, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    T: ToolsetBackend,
{
    type Conversation = CodexConversation<S, D, T>;

    fn id(&self) -> WorkspaceId {
        self.record.id().clone()
    }

    async fn info(&self) -> Result<WorkspaceInfo, KitError> {
        let admission = self.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let info = self.current().await?.info()?;
            self.tools.workspace_policy(info.context())?;
            Ok(info)
        }
        .await;
        admission.publish(result)
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<CodexConversation<S, D, T>, KitError> {
        let admission = self.hierarchy.admit_work(self.record.id())?;
        let workspace = match self.current().await {
            Ok(workspace) => workspace,
            Err(error) => return admission.publish(Err(error)),
        };
        let workspace_info = match workspace.info() {
            Ok(info) => info,
            Err(error) => return admission.publish(Err(error)),
        };
        if let Err(error) = CodexPromptSelection::validate_personality(
            request
                .defaults()
                .and_then(crate::WorkspaceDefaults::personality)
                .or_else(|| workspace_info.defaults().personality()),
        ) {
            return admission.publish(Err(error));
        }
        let workspace_policy = match self.tools.workspace_policy(workspace_info.context()) {
            Ok(policy) => policy,
            Err(error) => return admission.publish(Err(error)),
        };
        let policy = request
            .defaults()
            .and_then(|defaults| defaults.knowledge_policy())
            .or_else(|| workspace_info.defaults().knowledge_policy());
        if let Err(error) = self.knowledge.validate_policy(policy) {
            return admission.publish(Err(error));
        }
        let workspace_id = workspace.id().clone();
        let workspace_revision = workspace.revision();
        let record = ConversationRecord::new(
            ConversationId::new(),
            workspace_id.clone(),
            workspace_revision,
            &request,
        );
        let conversation_knowledge = self
            .knowledge
            .conversation_owner(&workspace_id, record.id());
        let knowledge_sources = request.knowledge_sources().to_vec();
        let native_name = request
            .title()
            .unwrap_or_else(|| record.id().as_str())
            .to_owned();
        let agent_id = self.state.agent_id().clone();
        let creation = match CodexThreadCreation::new(agent_id.clone(), record, native_name) {
            Ok(creation) => creation,
            Err(error) => return admission.publish(Err(error)),
        };
        let instructions = self.state.instructions().map(str::to_owned);
        let state = self.state.clone();
        let protocol = self.protocol.clone();
        let hierarchy = self.hierarchy.clone();
        let creation_workspace_id = workspace_id.clone();
        let lifecycle_timeout = self.lifecycle_timeout;
        let default_prompt_timeout = self.default_prompt_timeout;
        let knowledge = self.knowledge.clone();
        let tools = self.tools.clone();
        tokio::spawn(async move {
            let lifecycle_deadline = tokio::time::Instant::now() + lifecycle_timeout;
            let creation = async {
                let declarations = tools.thread_declarations()?;
                let knowledge_request = IngestKnowledgeRequest::new(knowledge_sources)?;
                state.prepare_thread_creation(&creation).await?;
                if let Err(error) = knowledge
                    .ingest(conversation_knowledge.clone(), knowledge_request)
                    .await
                {
                    let cleanup = state.abandon_thread_creation(&creation).await;
                    if cleanup.is_ok() {
                        let _ = knowledge
                            .settle_forget(&conversation_knowledge, lifecycle_deadline)
                            .await;
                    }
                    return Err(cleanup.err().unwrap_or(error));
                }
                let creation = match state.mark_thread_creation_knowledge_ready(&creation).await {
                    Ok(creation) => creation,
                    Err(error) => return Err(error),
                };
                let creation = state
                    .mark_thread_creation_dispatch_started(&creation)
                    .await?;
                let thread = match protocol
                    .start_thread(
                        &workspace_policy,
                        instructions.as_deref(),
                        creation.thread_source(),
                        declarations,
                    )
                    .await
                {
                    Ok(thread) => thread,
                    Err(error) if protocol.rejected_before_dispatch(&error) => {
                        let request_error = CodexProtocol::caller_safe_request_error(error);
                        let cleanup = state.abandon_thread_creation(&creation).await;
                        if cleanup.is_ok() {
                            let _ = knowledge
                                .settle_forget(&conversation_knowledge, lifecycle_deadline)
                                .await;
                        }
                        return Err(cleanup.err().unwrap_or(request_error));
                    }
                    Err(_) => {
                        let observed = state
                            .thread_creation_by_source(&agent_id, creation.thread_source())
                            .await?;
                        let observation = state.wait_for_thread_creation_observation(
                            &agent_id,
                            creation.thread_source(),
                            lifecycle_deadline,
                        );
                        protocol
                            .recover_thread_creation(
                                creation.thread_source(),
                                observed.native_thread_id(),
                                observation,
                            )
                            .await?
                    }
                };
                let cleanup = CodexCleanup::conversation(
                    agent_id.clone(),
                    creation.conversation().id().clone(),
                    thread.thread().id().to_owned(),
                )?;
                let binding = async {
                    state
                        .observe_thread_creation(
                            &agent_id,
                            creation.thread_source(),
                            thread.thread().id(),
                        )
                        .await?;
                    let creation = state
                        .thread_creation_by_source(&agent_id, creation.thread_source())
                        .await?;
                    thread.verify(&protocol, &workspace_policy).await?;
                    protocol
                        .materialize_thread(
                            thread.thread(),
                            creation.native_name(),
                            creation.thread_source(),
                        )
                        .await?;
                    let binding = CodexThreadBinding::new(
                        thread.thread(),
                        creation.conversation().id().clone(),
                        creation_workspace_id.clone(),
                        workspace_revision,
                        instructions.as_deref(),
                        creation.thread_source(),
                    )?;
                    state.complete_thread_creation(&creation, &binding).await?;
                    Ok::<(CodexThreadCreation, CodexThreadBinding), KitError>((creation, binding))
                }
                .await;
                let (creation, binding) = match binding {
                    Ok(materialized) => materialized,
                    Err(error) => {
                        if let Err(record_error) =
                            state.fail_thread_creation(&creation, &cleanup).await
                        {
                            hierarchy.invalidate();
                            return Err(error
                                .with_supplementary(record_error.in_phase(ErrorPhase::Cleanup)));
                        }
                        let _ = knowledge
                            .settle_forget(&conversation_knowledge, lifecycle_deadline)
                            .await;
                        if let Err(settlement_error) = protocol.settle_cleanup(&cleanup).await {
                            state.retain_cleanup(cleanup);
                            hierarchy.invalidate();
                            return Err(error.with_supplementary(
                                settlement_error.in_phase(ErrorPhase::Cleanup),
                            ));
                        }
                        if let Err(finish_error) = state.finish_cleanup(&cleanup).await {
                            state.retain_cleanup(cleanup);
                            hierarchy.invalidate();
                            return Err(error
                                .with_supplementary(finish_error.in_phase(ErrorPhase::Cleanup)));
                        }
                        return Err(error);
                    }
                };
                Ok((creation.conversation().clone(), binding))
            }
            .await;
            let creation = creation.and_then(|(record, binding)| {
                let claim = hierarchy.claim_conversation(
                    record.id().clone(),
                    workspace_id,
                    workspace_revision,
                )?;
                let runtime = CodexConversationRuntime::new(binding, state, protocol, tools);
                Ok(CodexConversation::new(
                    record,
                    hierarchy,
                    claim,
                    lifecycle_timeout,
                    default_prompt_timeout,
                    runtime,
                    knowledge,
                ))
            });
            admission.publish(creation)
        })
        .await
        .map_err(|_| KitError::integration("conversation creation task failed"))?
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, KitError> {
        let admission = self.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let workspace = self.current().await?;
            let info = workspace.info()?;
            self.tools.workspace_policy(info.context())?;
            self.state
                .conversations(workspace.id())
                .await?
                .into_iter()
                .map(|record| {
                    if record.workspace_revision() != workspace.revision() {
                        return Err(KitError::invalid_state(
                            "conversation revision contradicts its workspace",
                        ));
                    }
                    record.info()
                })
                .collect()
        }
        .await;
        admission.publish(result)
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, KitError> {
        let admission = self.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let workspace = self.current().await?;
            let info = workspace.info()?;
            self.tools.workspace_policy(info.context())?;
            let record = self.state.conversation(workspace.id(), &id).await?;
            if record.workspace_revision() != workspace.revision() {
                return Err(KitError::invalid_state(
                    "conversation revision contradicts its workspace",
                ));
            }
            record.info()
        }
        .await;
        admission.publish(result)
    }

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<CodexConversation<S, D, T>, KitError> {
        let admission = self.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let workspace = self.current().await?;
            let workspace_info = workspace.info()?;
            let workspace_policy = self.tools.workspace_policy(workspace_info.context())?;
            let record = self.state.conversation(workspace.id(), &id).await?;
            if record.workspace_revision() != workspace.revision() {
                return Err(KitError::invalid_state(
                    "conversation revision contradicts its workspace",
                ));
            }
            CodexPromptSelection::validate_personality(
                record
                    .defaults()?
                    .as_ref()
                    .and_then(crate::WorkspaceDefaults::personality)
                    .or_else(|| workspace_info.defaults().personality()),
            )?;
            CodexPromptSelection::validate_personality(
                self.state.native_selection(&id).await?.personality(),
            )?;
            let binding = self.state.binding(workspace.id(), &id).await?;
            binding.verify_owner(&record, &workspace, self.state.instructions())?;
            let claim = self.hierarchy.claim_conversation(
                id,
                workspace.id().clone(),
                workspace.revision(),
            )?;
            let thread = if binding.requires_rebind() {
                self.protocol
                    .rebind_thread(
                        binding.thread_id(),
                        &workspace_policy,
                        self.state.instructions(),
                    )
                    .await?
            } else {
                self.protocol
                    .verify_thread(
                        binding.thread_id(),
                        &workspace_policy,
                        self.state.instructions(),
                    )
                    .await?
            };
            binding.verify_native(&thread)?;
            let binding = if binding.requires_rebind() {
                self.state
                    .materialize_binding(
                        self.state.agent_id(),
                        workspace.id(),
                        record.id(),
                        &binding,
                        &thread,
                    )
                    .await?
            } else {
                binding
            };
            self.knowledge
                .verify(
                    &self
                        .knowledge
                        .conversation_owner(workspace.id(), record.id()),
                )
                .await?;
            let runtime = CodexConversationRuntime::new(
                binding,
                self.state.clone(),
                self.protocol.clone(),
                self.tools.clone(),
            );
            Ok(CodexConversation::new(
                record,
                self.hierarchy.clone(),
                claim,
                self.lifecycle_timeout,
                self.default_prompt_timeout,
                runtime,
                self.knowledge.clone(),
            ))
        }
        .await;
        admission.publish(result)
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, KitError> {
        let admission = self.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let info = self.current().await?.info()?;
            self.tools.workspace_policy(info.context())?;
            self.tools.list()
        }
        .await;
        admission.publish(result)
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        let admission = self.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let info = self.current().await?.info()?;
            self.tools.workspace_policy(info.context())?;
            self.knowledge
                .ingest(self.knowledge.workspace_owner(self.record.id()), request)
                .await
        }
        .await;
        admission.publish(result)
    }

    async fn close(self) -> Result<(), KitError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
        let result = match self
            .hierarchy
            .seal_workspace(self.record.id(), self.record.revision())
        {
            Ok(()) => {
                let prompt_cancellation = async {
                    let conversations = match tokio::time::timeout_at(
                        deadline,
                        self.state.conversations(self.record.id()),
                    )
                    .await
                    {
                        Ok(result) => result?,
                        Err(_) => {
                            return Err(KitError::timeout(
                                "workspace prompt cancellation deadline elapsed",
                            ));
                        }
                    };
                    let mut first_error = None;
                    for conversation in conversations {
                        match tokio::time::timeout_at(
                            deadline,
                            self.state.request_abort(conversation.id(), deadline),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) if first_error.is_none() => {
                                first_error = Some(error);
                            }
                            Ok(Err(_)) => {}
                            Err(_) => {
                                if first_error.is_none() {
                                    first_error = Some(KitError::timeout(
                                        "workspace prompt cancellation deadline elapsed",
                                    ));
                                }
                                break;
                            }
                        }
                    }
                    first_error.map_or(Ok(()), Err)
                };
                let (prompt_cancellation, tool_cancellation, quiescence) = tokio::join!(
                    prompt_cancellation,
                    self.tools.cancel_workspace(self.record.id(), deadline),
                    self.hierarchy
                        .await_workspace_quiescence(self.record.id(), deadline),
                );
                let prompt_cancellation_error = prompt_cancellation.err();
                let tool_cancellation_error = tool_cancellation.err();
                let quiescence_error = quiescence.err();
                self.state
                    .notifications()
                    .invalidate_workspace(self.record.id());
                let finalization_error = if quiescence_error.is_none() {
                    match tokio::time::timeout_at(deadline, async {
                        let current = self
                            .state
                            .workspace(self.state.agent_id(), self.record.id())
                            .await
                            .map_err(|error| match error {
                                error if error.kind() == crate::error::ErrorKind::NotFound => {
                                    KitError::closed(
                                        "workspace was deleted while close was admitted",
                                    )
                                    .with_cause(error)
                                }
                                error => error,
                            })?;
                        let bindings = self.state.bindings_for_workspace(current.id()).await?;
                        if bindings.is_empty() {
                            return Ok(());
                        }
                        let cleanup = CodexCleanup::detach_workspace(
                            self.state.agent_id().clone(),
                            current.id().clone(),
                            bindings
                                .into_iter()
                                .map(|binding| binding.thread_id().to_owned())
                                .collect(),
                        )?;
                        self.state.record_cleanup(&cleanup).await?;
                        if self.protocol.settle_cleanup(&cleanup).await.is_ok() {
                            let _ = self.state.finish_cleanup(&cleanup).await;
                        }
                        Ok(())
                    })
                    .await
                    {
                        Ok(result) => result.err(),
                        Err(_) => Some(KitError::timeout("workspace close deadline elapsed")),
                    }
                } else {
                    None
                };
                prompt_cancellation_error
                    .or(tool_cancellation_error)
                    .or(quiescence_error)
                    .or(finalization_error)
                    .map_or(Ok(()), Err)
            }
            Err(error) => Err(error),
        };
        admission.publish(result)
    }
}
