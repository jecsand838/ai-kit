use crate::backends::codex::adapters::prompt::CodexPromptSelection;
use crate::error::{ErrorKind, ErrorPhase};
use crate::workspace::WorkspaceDefaults;
use tools_kit::ToolsetBackend;
pub(crate) mod runtime;

use storage_kit::{DatabaseBackend, StateStoreBackend};

use crate::agent::AgentBackend;
use crate::agent_storage::AgentStorageClaim;
use crate::backends::codex::agent::runtime::CodexRuntime;
use crate::backends::codex::conversations::runtime::CodexConversationRuntime;
use crate::backends::codex::conversations::thread::CodexThreadBinding;
use crate::backends::codex::knowledge_base::CodexKnowledgeBase;
use crate::backends::codex::state_store::CodexStateStore;
use crate::backends::codex::state_store::records::{CodexAttachment, CodexCleanup};
use crate::backends::codex::tools::CodexTools;
use crate::backends::codex::workspace::CodexWorkspace;
use crate::core::hierarchy::HierarchyRuntime;
use crate::{
    AgentConfig, AgentId, AgentInfo, AgentStorage, BackendConfig, DeleteWorkspaceRequest,
    IngestKnowledgeRequest, KitError, ManagedKnowledge, ModelInfo, NewWorkspace,
    UpdateWorkspaceRequest, WorkspaceContext, WorkspaceId, WorkspaceInfo, WorkspaceUpdateReadiness,
};

pub(crate) struct CodexAgent<S: StateStoreBackend, D: DatabaseBackend, T: ToolsetBackend> {
    info: AgentInfo,
    runtime: CodexRuntime<S>,
    state: CodexStateStore<S>,
    hierarchy: HierarchyRuntime,
    _claim: AgentStorageClaim,
    config: AgentConfig,
    knowledge: CodexKnowledgeBase<S, D>,
    tools: CodexTools<S, T>,
}

impl<S, D, T> CodexAgent<S, D, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    T: ToolsetBackend,
{
    pub(crate) async fn build(
        info: AgentInfo,
        config: AgentConfig,
        storage: AgentStorage<S>,
        knowledge: Option<ManagedKnowledge<S, D>>,
        claim: AgentStorageClaim,
        toolsets: Vec<std::sync::Arc<T>>,
        reopen: bool,
    ) -> Result<Self, KitError> {
        let BackendConfig::Codex(backend) = config.backend() else {
            return Err(KitError::invalid_state(
                "agent configuration selection is contradictory",
            ));
        };
        let lifecycle_deadline = tokio::time::Instant::now()
            .checked_add(config.lifecycle_timeout())
            .ok_or_else(|| KitError::invalid_config("lifecycle timeout cannot be represented"))?;
        let attachment = CodexAttachment::new(info.id().clone(), backend, config.instructions())?;
        let knowledge = CodexKnowledgeBase::new(knowledge, storage.clone(), info.id().clone());
        let state = CodexStateStore::new(
            info.id().clone(),
            storage.clone(),
            knowledge.is_configured(),
            config.prompt_limit().get(),
            config.instructions().map(str::to_owned),
        )?;
        let persistence = if reopen {
            state.verify(&attachment).await
        } else {
            state.create(&info, &attachment).await
        };
        if let Err(error) = persistence {
            let error = match CodexTools::<S, T>::shutdown_registrations(
                toolsets,
                tokio::time::Instant::now() + config.lifecycle_timeout(),
            )
            .await
            {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            };
            return Err(error);
        }
        let tools = CodexTools::build(
            info.id().clone(),
            storage,
            state.notifications().clone(),
            toolsets,
            config.prompt_limit().get(),
            config.lifecycle_timeout(),
            reopen,
        )
        .await?;
        let workspace_policy_validation = async {
            for workspace in state.workspaces(info.id()).await? {
                let workspace_info = workspace.info()?;
                CodexPromptSelection::validate_personality(
                    workspace_info.defaults().personality(),
                )?;
                tools.workspace_policy(workspace_info.context())?;
                if reopen {
                    knowledge
                        .verify_before(
                            &knowledge.workspace_owner(workspace.id()),
                            lifecycle_deadline,
                        )
                        .await?;
                    for conversation in state.conversations(workspace.id()).await? {
                        CodexPromptSelection::validate_personality(
                            conversation
                                .defaults()?
                                .as_ref()
                                .and_then(WorkspaceDefaults::personality),
                        )?;
                        CodexPromptSelection::validate_personality(
                            state
                                .native_selection(conversation.id())
                                .await?
                                .personality(),
                        )?;
                        if let Some(prompt) = state.prompt_record(conversation.id()).await? {
                            CodexPromptSelection::validate_personality(
                                prompt.previous_selection().personality(),
                            )?;
                            CodexPromptSelection::validate_personality(
                                prompt.accepted_selection().personality(),
                            )?;
                        }
                        knowledge
                            .verify_before(
                                &knowledge.conversation_owner(workspace.id(), conversation.id()),
                                lifecycle_deadline,
                            )
                            .await?;
                    }
                }
            }
            Ok::<(), KitError>(())
        }
        .await;
        if let Err(error) = workspace_policy_validation {
            let error = match tools
                .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                .await
            {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            };
            return Err(error);
        }
        let hierarchy = HierarchyRuntime::default();
        let runtime = match CodexRuntime::connect(
            backend,
            info.id().clone(),
            state.clone(),
            hierarchy.clone(),
            tools.clone(),
        )
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let error = match tools
                    .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                    .await
                {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
                };
                return Err(error);
            }
        };
        let workspace_policy_validation = async {
            for workspace in state.workspaces(info.id()).await? {
                let workspace = workspace.info()?;
                let policy = tools.workspace_policy(workspace.context())?;
                runtime
                    .protocol()
                    .validate_workspace_policy(&policy)
                    .await?;
            }
            Ok::<(), KitError>(())
        }
        .await;
        if let Err(error) = workspace_policy_validation {
            let error = match runtime.shutdown(config.lifecycle_timeout()).await {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            };
            let error = match tools
                .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                .await
            {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            };
            return Err(error);
        }
        if reopen {
            let cleanups = match state.cleanup_obligations(info.id()).await {
                Ok(cleanups) => cleanups,
                Err(error) => {
                    let error = match runtime.shutdown(config.lifecycle_timeout()).await {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup))
                        }
                    };
                    let error = match tools
                        .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                        .await
                    {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup))
                        }
                    };
                    return Err(error);
                }
            };
            for cleanup in cleanups {
                if let Err(error) = runtime.protocol().settle_cleanup(&cleanup).await {
                    let error = match runtime.shutdown(config.lifecycle_timeout()).await {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup))
                        }
                    };
                    let error = match tools
                        .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                        .await
                    {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup))
                        }
                    };
                    return Err(error);
                }
                if let Err(error) = state.finish_cleanup(&cleanup).await {
                    let error = match runtime.shutdown(config.lifecycle_timeout()).await {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup))
                        }
                    };
                    let error = match tools
                        .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                        .await
                    {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup))
                        }
                    };
                    return Err(error);
                }
            }
            let creation_reconciliation = async {
                let mut pending_creations = state.thread_creations(info.id()).await?;
                pending_creations.sort_by_key(|creation| {
                    if creation.native_thread_id().is_some() {
                        0_u8
                    } else if creation.dispatch_ready() {
                        1
                    } else if !creation.knowledge_ready() {
                        2
                    } else {
                        3
                    }
                });
                let mut first_error: Option<KitError> = None;
                for pending in pending_creations {
                    let reconciliation = async {
                        let pending_owner = knowledge.conversation_owner(
                            pending.conversation().workspace_id(),
                            pending.conversation().id(),
                        );
                        if !pending.knowledge_ready() {
                            state.abandon_thread_creation(&pending).await?;
                            let _ = knowledge
                                .settle_forget(&pending_owner, lifecycle_deadline)
                                .await;
                            return Ok::<(), KitError>(());
                        }
                        let record = pending.conversation();
                        let workspace = state.workspace(info.id(), record.workspace_id()).await?;
                        if workspace.revision() != record.workspace_revision() {
                            return Err(KitError::conflict(
                                "pending conversation belongs to a stale workspace revision",
                            ));
                        }
                        let workspace_info = workspace.info()?;
                        CodexPromptSelection::validate_personality(
                            workspace_info.defaults().personality(),
                        )?;
                        CodexPromptSelection::validate_personality(
                            record
                                .defaults()?
                                .as_ref()
                                .and_then(WorkspaceDefaults::personality),
                        )?;
                        let policy = tools.workspace_policy(workspace_info.context())?;
                        let (creation, thread) = if pending.dispatch_ready() {
                            if let Err(error) = knowledge
                                .verify_before(&pending_owner, lifecycle_deadline)
                                .await
                            {
                                if error.kind() == ErrorKind::Timeout {
                                    return Err(error);
                                }
                                state.abandon_thread_creation(&pending).await?;
                                let _ = knowledge
                                    .settle_forget(&pending_owner, lifecycle_deadline)
                                    .await;
                                return Ok(());
                            }
                            let declarations = tools.thread_declarations()?;
                            let creation = state
                                .mark_thread_creation_dispatch_started(&pending)
                                .await?;
                            let thread = match runtime
                                .protocol()
                                .start_thread(
                                    &policy,
                                    state.instructions(),
                                    creation.thread_source(),
                                    declarations,
                                )
                                .await
                            {
                                Ok(thread) => thread,
                                Err(error)
                                    if runtime.protocol().rejected_before_dispatch(&error) =>
                                {
                                    state.abandon_thread_creation(&creation).await?;
                                    let _ = knowledge
                                        .settle_forget(&pending_owner, lifecycle_deadline)
                                        .await;
                                    return Ok(());
                                }
                                Err(_) => {
                                    let protocol = runtime.protocol();
                                    let observation = state.wait_for_thread_creation_observation(
                                        info.id(),
                                        creation.thread_source(),
                                        lifecycle_deadline,
                                    );
                                    protocol
                                        .recover_thread_creation(
                                            creation.thread_source(),
                                            creation.native_thread_id(),
                                            observation,
                                        )
                                        .await?
                                }
                            };
                            (creation, thread)
                        } else if pending.dispatch_may_be_accepted() {
                            let protocol = runtime.protocol();
                            let observation = state.wait_for_thread_creation_observation(
                                info.id(),
                                pending.thread_source(),
                                lifecycle_deadline,
                            );
                            let thread = protocol
                                .recover_thread_creation(
                                    pending.thread_source(),
                                    pending.native_thread_id(),
                                    observation,
                                )
                                .await?;
                            (pending, thread)
                        } else {
                            return Err(KitError::invalid_state(
                                "conversation creation state is inconsistent",
                            ));
                        };
                        let cleanup = CodexCleanup::conversation(
                            info.id().clone(),
                            creation.conversation().id().clone(),
                            thread.thread().id().to_owned(),
                        )?;
                        let publication = async {
                            state
                                .observe_thread_creation(
                                    info.id(),
                                    creation.thread_source(),
                                    thread.thread().id(),
                                )
                                .await?;
                            let creation = state
                                .thread_creation_by_source(info.id(), creation.thread_source())
                                .await?;
                            knowledge
                                .verify_before(
                                    &knowledge.conversation_owner(
                                        creation.conversation().workspace_id(),
                                        creation.conversation().id(),
                                    ),
                                    lifecycle_deadline,
                                )
                                .await?;
                            thread.verify(&runtime.protocol(), &policy).await?;
                            runtime
                                .protocol()
                                .materialize_thread(
                                    thread.thread(),
                                    creation.native_name(),
                                    creation.thread_source(),
                                )
                                .await?;
                            let record = creation.conversation();
                            let binding = CodexThreadBinding::new(
                                thread.thread(),
                                record.id().clone(),
                                record.workspace_id().clone(),
                                record.workspace_revision(),
                                state.instructions(),
                                creation.thread_source(),
                            )?;
                            state.complete_thread_creation(&creation, &binding).await
                        }
                        .await;
                        if publication.is_ok() {
                            return Ok(());
                        }
                        if let Err(error) = publication
                            && error.kind() == ErrorKind::Timeout
                        {
                            return Err(error);
                        }
                        state.fail_thread_creation(&creation, &cleanup).await?;
                        let _ = knowledge
                            .settle_forget(
                                &knowledge.conversation_owner(
                                    creation.conversation().workspace_id(),
                                    creation.conversation().id(),
                                ),
                                lifecycle_deadline,
                            )
                            .await;
                        if let Err(error) = runtime.protocol().settle_cleanup(&cleanup).await {
                            state.retain_cleanup(cleanup);
                            return Err(error);
                        }
                        state.finish_cleanup(&cleanup).await
                    }
                    .await;
                    if let Err(error) = reconciliation {
                        first_error = Some(match first_error.take() {
                            Some(primary) => {
                                primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                            }
                            None => error,
                        });
                    }
                }
                match first_error {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }
            .await;
            if let Err(error) = creation_reconciliation {
                let error = match runtime.shutdown(config.lifecycle_timeout()).await {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
                };
                let error = match tools
                    .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                    .await
                {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
                };
                return Err(error);
            }
            let reconciliation = async {
                if tokio::time::Instant::now() >= lifecycle_deadline {
                    return Err(KitError::timeout("agent reopen timed out"));
                }
                for workspace in state.workspaces(info.id()).await? {
                    for binding in state.bindings_for_workspace(workspace.id()).await? {
                        CodexConversationRuntime::new(
                            binding,
                            state.clone(),
                            runtime.protocol(),
                            tools.clone(),
                        )
                        .reconcile_reopen(lifecycle_deadline)
                        .await?;
                    }
                }
                Ok::<(), KitError>(())
            }
            .await;
            if let Err(error) = reconciliation {
                let error = match runtime.shutdown(config.lifecycle_timeout()).await {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
                };
                let error = match tools
                    .shutdown(tokio::time::Instant::now() + config.lifecycle_timeout())
                    .await
                {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
                };
                return Err(error);
            }
        }
        Ok(Self {
            info,
            runtime,
            state,
            hierarchy,
            _claim: claim,
            config,
            knowledge,
            tools,
        })
    }

    pub(crate) fn retain_storage_claim(&mut self) {
        self._claim.retain_until_shutdown();
    }

    async fn validate_workspace_policy(&self, context: &WorkspaceContext) -> Result<(), KitError> {
        let policy = self.tools.workspace_policy(context)?;
        self.runtime
            .protocol()
            .validate_workspace_policy(&policy)
            .await
    }
}

impl<S, D, T> AgentBackend for CodexAgent<S, D, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    T: ToolsetBackend,
{
    type Workspace = CodexWorkspace<S, D, T>;

    fn id(&self) -> AgentId {
        self.info.id().clone()
    }

    async fn info(&self) -> Result<AgentInfo, KitError> {
        let admission = self.hierarchy.admit_agent_work()?;
        admission.publish(Ok(self.info.clone()))
    }

    async fn new_workspace(
        &self,
        request: NewWorkspace,
    ) -> Result<CodexWorkspace<S, D, T>, KitError> {
        let admission = self.hierarchy.admit_agent_work()?;
        if let Err(error) =
            CodexPromptSelection::validate_personality(request.defaults().personality())
        {
            return admission.publish(Err(error));
        }
        if let Err(error) = self.validate_workspace_policy(request.context()).await {
            return admission.publish(Err(error));
        }
        if let Err(error) = self
            .knowledge
            .validate_policy(request.defaults().knowledge_policy())
        {
            return admission.publish(Err(error));
        }
        let state = self.state.clone();
        let agent_id = self.info.id().clone();
        let protocol = self.runtime.protocol();
        let hierarchy = self.hierarchy.clone();
        let knowledge = self.knowledge.clone();
        let tools = self.tools.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            let result = state
                .create_workspace(&agent_id, &request)
                .await
                .and_then(|record| {
                    let claim =
                        hierarchy.claim_workspace(record.id().clone(), record.revision())?;
                    Ok(CodexWorkspace::new(
                        (record, claim),
                        state,
                        protocol,
                        hierarchy,
                        &config,
                        knowledge,
                        tools,
                    ))
                });
            admission.publish(result)
        })
        .await
        .map_err(|_| KitError::integration("workspace creation task failed"))?
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let result = async {
            let mut workspaces = Vec::new();
            for record in self.state.workspaces(self.info.id()).await? {
                let info = record.info()?;
                self.validate_workspace_policy(info.context()).await?;
                workspaces.push(info);
            }
            Ok(workspaces)
        }
        .await;
        admission.publish(result)
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        let admission = self.hierarchy.admit_read(&id).await?;
        let result = async {
            let info = self.state.workspace(self.info.id(), &id).await?.info()?;
            self.validate_workspace_policy(info.context()).await?;
            Ok(info)
        }
        .await;
        admission.publish(result)
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<CodexWorkspace<S, D, T>, KitError> {
        let admission = self.hierarchy.admit_workspace_reopen(&id)?;
        let result = async {
            let record = self.state.workspace(self.info.id(), &id).await?;
            let info = record.info()?;
            CodexPromptSelection::validate_personality(info.defaults().personality())?;
            self.validate_workspace_policy(info.context()).await?;
            self.knowledge
                .verify(&self.knowledge.workspace_owner(&id))
                .await?;
            let claim = admission.claim_workspace(record.revision())?;
            Ok(CodexWorkspace::new(
                (record, claim),
                self.state.clone(),
                self.runtime.protocol(),
                self.hierarchy.clone(),
                &self.config,
                self.knowledge.clone(),
                self.tools.clone(),
            ))
        }
        .await;
        admission.publish(result)
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_
    {
        let request = request.clone();
        async move {
            let (admission, initially_busy) = self.hierarchy.admit_update_check(request.id())?;
            if let Err(error) =
                CodexPromptSelection::validate_personality(request.defaults().personality())
            {
                return admission.publish(Err(error));
            }
            if let Err(error) = self.validate_workspace_policy(request.context()).await {
                return admission.publish(Err(error));
            }
            if let Err(error) = self
                .knowledge
                .validate_policy(request.defaults().knowledge_policy())
            {
                return admission.publish(Err(error));
            }
            let result = async {
                let record = self.state.workspace(self.info.id(), request.id()).await?;
                if record.revision() != request.expected_revision() {
                    return Err(KitError::conflict("workspace revision is stale"));
                }
                if initially_busy
                    || self.hierarchy.workspace_update_busy(request.id())?
                    || self
                        .state
                        .workspace_has_unsettled_work(request.id())
                        .await?
                {
                    Ok(WorkspaceUpdateReadiness::Busy)
                } else {
                    Ok(WorkspaceUpdateReadiness::Ready)
                }
            }
            .await;
            admission.publish(result)
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        let admission = self.hierarchy.admit_mutation(request.id())?;
        if let Err(error) =
            CodexPromptSelection::validate_personality(request.defaults().personality())
        {
            return admission.publish(Err(error));
        }
        if let Err(error) = self.validate_workspace_policy(request.context()).await {
            return admission.publish(Err(error));
        }
        if let Err(error) = self
            .knowledge
            .validate_policy(request.defaults().knowledge_policy())
        {
            return admission.publish(Err(error));
        }
        let state = self.state.clone();
        let hierarchy = self.hierarchy.clone();
        let agent_id = self.info.id().clone();
        tokio::spawn(async move {
            let result = async {
                let info = state.update_workspace(&agent_id, &request).await?.info()?;
                hierarchy.advance_workspace_revision(info.id(), info.revision());
                state.notifications().invalidate_workspace(info.id());
                Ok(info)
            }
            .await;
            admission.publish(result)
        })
        .await
        .map_err(|_| KitError::integration("workspace update task failed"))?
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        let admission = self.hierarchy.admit_mutation(request.id())?;
        let state = self.state.clone();
        let protocol = self.runtime.protocol();
        let hierarchy = self.hierarchy.clone();
        let agent_id = self.info.id().clone();
        let knowledge = self.knowledge.clone();
        let lifecycle_timeout = self.config.lifecycle_timeout();
        tokio::spawn(async move {
            let result = async {
                let conversation_knowledge = state
                    .conversations(request.id())
                    .await?
                    .into_iter()
                    .map(|conversation| {
                        knowledge.conversation_owner(request.id(), conversation.id())
                    })
                    .collect::<Vec<_>>();
                let cleanup = state
                    .delete_workspace(&agent_id, request.id(), request.expected_revision())
                    .await?;
                hierarchy.invalidate_workspace(request.id());
                state.notifications().invalidate_workspace(request.id());
                let deadline = tokio::time::Instant::now() + lifecycle_timeout;
                let workspace_knowledge = knowledge.workspace_owner(request.id());
                let cleanup_attempt = async {
                    let _ = tokio::join!(
                        async {
                            if let Some(cleanup) = cleanup.as_ref()
                                && protocol.settle_cleanup(cleanup).await.is_ok()
                            {
                                let _ = state.finish_cleanup(cleanup).await;
                            }
                        },
                        async {
                            let _ = knowledge
                                .settle_forget(&workspace_knowledge, deadline)
                                .await;
                            for owner in conversation_knowledge {
                                let _ = knowledge.settle_forget(&owner, deadline).await;
                            }
                        },
                    );
                };
                let _ = tokio::time::timeout_at(deadline, cleanup_attempt).await;
                Ok(())
            }
            .await;
            admission.publish(result)
        })
        .await
        .map_err(|_| KitError::integration("workspace deletion task failed"))?
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let result = self.runtime.protocol().list_models().await;
        admission.publish(result)
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let result = self
            .knowledge
            .ingest(self.knowledge.agent_owner(), request)
            .await;
        admission.publish(result)
    }

    async fn shutdown(self) -> Result<(), KitError> {
        let Self {
            info,
            runtime,
            state,
            hierarchy,
            mut _claim,
            config,
            knowledge: _,
            tools,
        } = self;
        let deadline = tokio::time::Instant::now() + config.lifecycle_timeout();
        hierarchy.seal();
        let mut prompt_cancellation_error = None;
        match tokio::time::timeout_at(deadline, state.workspaces(info.id())).await {
            Ok(Ok(workspaces)) => {
                'workspaces: for workspace in workspaces {
                    match tokio::time::timeout_at(deadline, state.conversations(workspace.id()))
                        .await
                    {
                        Ok(Ok(conversations)) => {
                            for conversation in conversations {
                                match tokio::time::timeout_at(
                                    deadline,
                                    state.request_abort(conversation.id(), deadline),
                                )
                                .await
                                {
                                    Ok(Ok(_)) => {}
                                    Ok(Err(error)) if prompt_cancellation_error.is_none() => {
                                        prompt_cancellation_error = Some(error);
                                    }
                                    Ok(Err(_)) => {}
                                    Err(_) => {
                                        if prompt_cancellation_error.is_none() {
                                            prompt_cancellation_error = Some(KitError::timeout(
                                                "agent prompt cancellation deadline elapsed",
                                            ));
                                        }
                                        break 'workspaces;
                                    }
                                }
                            }
                        }
                        Ok(Err(error)) if prompt_cancellation_error.is_none() => {
                            prompt_cancellation_error = Some(error);
                        }
                        Ok(Err(_)) => {}
                        Err(_) => {
                            if prompt_cancellation_error.is_none() {
                                prompt_cancellation_error = Some(KitError::timeout(
                                    "agent prompt cancellation deadline elapsed",
                                ));
                            }
                            break 'workspaces;
                        }
                    }
                }
            }
            Ok(Err(error)) => prompt_cancellation_error = Some(error),
            Err(_) => {
                prompt_cancellation_error = Some(KitError::timeout(
                    "agent prompt cancellation deadline elapsed",
                ));
            }
        }
        let tool_cancellation_error = tools.cancel_all(deadline).await.err();
        state.invalidate_runtime();
        let quiescence_error = hierarchy.await_agent_quiescence(deadline).await.err();
        let protocol = runtime.protocol();
        let cleanup =
            async move {
                let mut first_error = [
                    prompt_cancellation_error,
                    tool_cancellation_error,
                    quiescence_error,
                ]
                .into_iter()
                .flatten()
                .reduce(|primary, error| {
                    primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                });
                for cleanup in state.take_retained_cleanups() {
                    let recording_error = state.record_cleanup(&cleanup).await.err();
                    let settlement_error = protocol.settle_cleanup(&cleanup).await.err();
                    let cleanup_error = match (recording_error, settlement_error) {
                        (None, None) => state.finish_cleanup(&cleanup).await.err(),
                        (Some(_), None) => None,
                        (None, Some(error)) => Some(error),
                        (Some(error), Some(settlement)) => {
                            state.retain_cleanup(cleanup);
                            Some(error.with_supplementary(settlement.in_phase(ErrorPhase::Cleanup)))
                        }
                    };
                    if let Some(error) = cleanup_error {
                        first_error = Some(match first_error.take() {
                            Some(primary) => {
                                primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                            }
                            None => error,
                        });
                    }
                }

                let mut thread_ids = Vec::new();
                match state.workspaces(info.id()).await {
                    Ok(workspaces) => {
                        for workspace in workspaces {
                            match state.bindings_for_workspace(workspace.id()).await {
                                Ok(bindings) => thread_ids.extend(
                                    bindings
                                        .into_iter()
                                        .filter(|binding| {
                                            protocol.is_thread_loaded(binding.thread_id())
                                        })
                                        .map(|binding| binding.thread_id().to_owned()),
                                ),
                                Err(error) => {
                                    first_error = Some(match first_error.take() {
                                        Some(primary) => primary.with_supplementary(
                                            error.in_phase(ErrorPhase::Cleanup),
                                        ),
                                        None => error,
                                    });
                                }
                            }
                        }
                    }
                    Err(error) => {
                        first_error = Some(match first_error.take() {
                            Some(primary) => {
                                primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                            }
                            None => error,
                        });
                    }
                }
                if !thread_ids.is_empty() {
                    match CodexCleanup::detach_agent(info.id().clone(), thread_ids) {
                        Ok(cleanup) => {
                            if let Err(error) = state.record_cleanup(&cleanup).await {
                                first_error = Some(match first_error.take() {
                                    Some(primary) => primary
                                        .with_supplementary(error.in_phase(ErrorPhase::Cleanup)),
                                    None => error,
                                });
                            } else if let Err(error) = protocol.settle_cleanup(&cleanup).await {
                                first_error = Some(match first_error.take() {
                                    Some(primary) => primary
                                        .with_supplementary(error.in_phase(ErrorPhase::Cleanup)),
                                    None => error,
                                });
                            } else if let Err(error) = state.finish_cleanup(&cleanup).await {
                                first_error = Some(match first_error.take() {
                                    Some(primary) => primary
                                        .with_supplementary(error.in_phase(ErrorPhase::Cleanup)),
                                    None => error,
                                });
                            }
                        }
                        Err(error) => {
                            first_error = Some(match first_error.take() {
                                Some(primary) => {
                                    primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                                }
                                None => error,
                            });
                        }
                    }
                }
                if let Err(error) = runtime
                    .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await
                {
                    first_error = Some(match first_error.take() {
                        Some(primary) => {
                            primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                        }
                        None => error,
                    });
                }
                if let Err(error) = tools.shutdown(deadline).await {
                    first_error = Some(match first_error.take() {
                        Some(primary) => {
                            primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                        }
                        None => error,
                    });
                }
                match first_error {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            };
        let result = tokio::time::timeout_at(deadline, cleanup)
            .await
            .unwrap_or_else(|_| Err(KitError::timeout("agent shutdown deadline elapsed")));
        _claim.complete_shutdown(result)
    }
}
