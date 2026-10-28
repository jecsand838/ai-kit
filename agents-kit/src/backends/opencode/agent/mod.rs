use crate::error::{ErrorDetail, ErrorPhase, Operation};
pub(crate) mod events;
pub(crate) mod runtime;

use crate::agent::AgentBackend;
use crate::agent_storage::AgentStorageClaim;
use crate::backends::opencode::adapters::OpenCodeModelInventory;
use crate::backends::opencode::conversations::runtime::{
    OpenCodeConversationRuntime, OpenCodePromptPolicy,
};
use crate::backends::opencode::knowledge_base::OpenCodeKnowledgeBase;
use crate::backends::opencode::protocol::requests::{OpenCodeCreationCorrelation, OpenCodeRouting};
use crate::backends::opencode::state_store::records::{OpenCodeAttachment, OpenCodeCleanup};
use crate::backends::opencode::state_store::{
    OpenCodeConversationMutationDisposition, OpenCodeStateStore, OpenCodeWorkspaceDeleteDisposition,
};
use crate::backends::opencode::tools::OpenCodeTools;
use crate::backends::opencode::workspace::{OpenCodeWorkspace, OpenCodeWorkspaceContext};
use crate::core::hierarchy::HierarchyRuntime;
use crate::{
    AgentConfig, AgentId, AgentInfo, AgentStorage, BackendConfig, DeleteWorkspaceRequest,
    IngestKnowledgeRequest, KitError, ManagedKnowledge, ModelInfo, NewWorkspace,
    UpdateWorkspaceRequest, WorkspaceContext, WorkspaceDefaults, WorkspaceId, WorkspaceInfo,
    WorkspaceNetworkAccess, WorkspaceRootAccess, WorkspaceSandboxMode, WorkspaceUpdateReadiness,
};

use events::{OpenCodeEventHandle, OpenCodeEventPolicy};
use runtime::OpenCodeRuntime;
use storage_kit::{DatabaseBackend, StateStoreBackend};

pub(crate) struct OpenCodeAgent<S: StateStoreBackend, D: DatabaseBackend> {
    info: AgentInfo,
    runtime: OpenCodeRuntime,
    events: OpenCodeEventHandle,
    state: OpenCodeStateStore<S>,
    prompt_policy: OpenCodePromptPolicy,
    hierarchy: HierarchyRuntime,
    _claim: AgentStorageClaim,
    config: AgentConfig,
    knowledge: OpenCodeKnowledgeBase<S, D>,
    tools: OpenCodeTools,
}

impl<S: StateStoreBackend, D: DatabaseBackend> OpenCodeAgent<S, D> {
    pub(crate) async fn build(
        info: AgentInfo,
        config: AgentConfig,
        storage: AgentStorage<S>,
        knowledge: Option<ManagedKnowledge<S, D>>,
        claim: AgentStorageClaim,
        reopen: bool,
    ) -> Result<Self, KitError> {
        let mut knowledge_error = false;
        let result = Self::construct(
            info,
            config,
            storage,
            knowledge,
            claim,
            reopen,
            &mut knowledge_error,
        )
        .await;
        result.map_err(|error| {
            error
                .at_operation(if reopen {
                    Operation::Reopen
                } else {
                    Operation::Build
                })
                .in_phase(ErrorPhase::Lifecycle)
        })
    }

    async fn construct(
        info: AgentInfo,
        config: AgentConfig,
        storage: AgentStorage<S>,
        knowledge: Option<ManagedKnowledge<S, D>>,
        claim: AgentStorageClaim,
        reopen: bool,
        knowledge_error: &mut bool,
    ) -> Result<Self, KitError> {
        let BackendConfig::OpenCode(backend) = config.backend() else {
            return Err(KitError::invalid_state(
                "received a contradictory backend selection",
            ));
        };
        let lifecycle_deadline = tokio::time::Instant::now()
            .checked_add(config.lifecycle_timeout())
            .ok_or_else(|| KitError::invalid_config("lifecycle timeout cannot be represented"))?;
        let attachment = OpenCodeAttachment::new(
            info.id().clone(),
            OpenCodeRuntime::attachment(backend, info.id())?,
        )?;
        let knowledge = OpenCodeKnowledgeBase::new(knowledge, storage.clone(), info.id().clone());
        let prompt_policy =
            OpenCodePromptPolicy::new(config.default_prompt_timeout(), config.instructions());
        let state = OpenCodeStateStore::new(
            info.id().clone(),
            storage,
            knowledge.is_configured(),
            config.prompt_limit().get(),
        )?;
        let tools = OpenCodeTools::new();
        if reopen {
            tokio::time::timeout_at(lifecycle_deadline, state.verify(&attachment))
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
            let generation = tools.generation();
            let workspaces =
                tokio::time::timeout_at(lifecycle_deadline, state.workspaces(info.id()))
                    .await
                    .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
            for workspace in workspaces {
                if let Err(error) = knowledge
                    .verify_before(
                        &knowledge.workspace_owner(workspace.id()),
                        lifecycle_deadline,
                    )
                    .await
                {
                    *knowledge_error = true;
                    return Err(error);
                }
                let conversations = tokio::time::timeout_at(
                    lifecycle_deadline,
                    state.conversations(workspace.id()),
                )
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
                for conversation in conversations {
                    if let Err(error) = knowledge
                        .verify_before(
                            &knowledge.conversation_owner(workspace.id(), conversation.id()),
                            lifecycle_deadline,
                        )
                        .await
                    {
                        *knowledge_error = true;
                        return Err(error);
                    }
                }
                state
                    .reconcile_catalog_binding(
                        info.id(),
                        workspace.id(),
                        workspace.revision(),
                        &generation,
                        lifecycle_deadline,
                    )
                    .await?;
            }
        } else {
            state.create(&info, &attachment, lifecycle_deadline).await?;
        }
        let mut runtime = OpenCodeRuntime::connect(
            backend,
            info.id(),
            lifecycle_deadline,
            config.lifecycle_timeout(),
        )
        .await?;
        let hierarchy = HierarchyRuntime::default();
        let initialization: Result<OpenCodeEventHandle, KitError> = async {
            if reopen {
                Self::reconcile_creations(
                    info.id(),
                    &state,
                    &runtime,
                    &knowledge,
                    lifecycle_deadline,
                    knowledge_error,
                )
                .await?;
            }
            if !reopen {
                tokio::time::timeout_at(
                    lifecycle_deadline,
                    runtime.http().verify_inventory_ownership(&[], &[]),
                )
                .await
                .map_err(|_| {
                    KitError::timeout("inventory verification deadline elapsed")
                        .at_phase(ErrorPhase::InventoryVerification)
                })??;
            } else {
                let cleanups = tokio::time::timeout_at(
                    lifecycle_deadline,
                    state.cleanup_obligations(info.id()),
                )
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
                let bindings = tokio::time::timeout_at(
                    lifecycle_deadline,
                    state.bindings_for_agent(info.id()),
                )
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
                tokio::time::timeout_at(
                    lifecycle_deadline,
                    runtime
                        .http()
                        .verify_inventory_ownership(&bindings, &cleanups),
                )
                .await
                .map_err(|_| {
                    KitError::timeout("inventory verification deadline elapsed")
                        .at_phase(ErrorPhase::InventoryVerification)
                })??;
                for cleanup in cleanups {
                    tokio::time::timeout_at(
                        lifecycle_deadline,
                        runtime.http().settle_cleanup(&cleanup),
                    )
                    .await
                    .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
                    if tokio::time::Instant::now() >= lifecycle_deadline {
                        return Err(KitError::timeout("agent construction deadline elapsed"));
                    }
                    let outcome = state.finish_cleanup(&cleanup).await;
                    if tokio::time::Instant::now() >= lifecycle_deadline {
                        return Err(KitError::timeout("agent construction deadline elapsed"));
                    }
                    outcome?;
                }
                tokio::time::timeout_at(
                    lifecycle_deadline,
                    runtime.http().verify_inventory_ownership(&bindings, &[]),
                )
                .await
                .map_err(|_| {
                    KitError::timeout("inventory verification deadline elapsed")
                        .at_phase(ErrorPhase::InventoryVerification)
                })??;
            }

            if reopen {
                tokio::time::timeout_at(lifecycle_deadline, async {
                    for workspace in state.workspaces(info.id()).await? {
                        let routing = state.workspace_routing(&workspace).await?;
                        runtime
                            .http()
                            .verify_directory(
                                workspace.cwd(),
                                routing.directory(),
                                lifecycle_deadline,
                            )
                            .await?;
                    }
                    Ok::<(), KitError>(())
                })
                .await
                .map_err(|_| {
                    KitError::timeout("workspace routing verification deadline elapsed")
                        .at_phase(ErrorPhase::InventoryVerification)
                })??;
            }

            let event_policy = OpenCodeEventPolicy::new(
                backend.mode().idle_stream_timeout(),
                lifecycle_deadline,
                config.lifecycle_timeout(),
            );
            runtime
                .start_events(
                    info.id().clone(),
                    state.clone(),
                    hierarchy.clone(),
                    tools.clone(),
                    event_policy,
                )
                .await?;
            let events = runtime.events()?;

            if reopen {
                let bindings = tokio::time::timeout_at(
                    lifecycle_deadline,
                    state.bindings_for_agent(info.id()),
                )
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
                for binding in bindings {
                    OpenCodeConversationRuntime::new(
                        info.id().clone(),
                        state.clone(),
                        runtime.http().clone(),
                        events.clone(),
                        prompt_policy.clone(),
                        binding,
                    )
                    .reconcile_reopen(lifecycle_deadline)
                    .await?;
                }
            }
            Ok(events)
        }
        .await;
        let events = match initialization {
            Ok(events) => events,
            Err(error) => {
                let cleanup_deadline = tokio::time::Instant::now() + config.lifecycle_timeout();
                let cleanup = runtime.shutdown(cleanup_deadline).await;
                return Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.at_phase(ErrorPhase::Cleanup)),
                });
            }
        };
        if tokio::time::Instant::now() >= lifecycle_deadline {
            let cleanup_deadline = tokio::time::Instant::now() + config.lifecycle_timeout();
            let error = KitError::timeout("agent publication deadline elapsed")
                .at_phase(ErrorPhase::Publication);
            let cleanup = runtime.shutdown(cleanup_deadline).await;
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.at_phase(ErrorPhase::Cleanup)),
            });
        }
        Ok(Self {
            info,
            runtime,
            events,
            state,
            prompt_policy,
            hierarchy,
            _claim: claim,
            config,
            knowledge,
            tools,
        })
    }

    fn retain_cleanup_failure(first: &mut Option<KitError>, error: KitError) {
        let error = error.in_phase(ErrorPhase::Cleanup);
        *first = Some(match first.take() {
            Some(primary) => primary.with_supplementary(error),
            None => error,
        });
    }

    pub(crate) fn retain_storage_claim(&mut self) {
        self._claim.retain_until_shutdown();
    }

    fn validate_workspace_configuration(
        &self,
        context: &WorkspaceContext,
        defaults: &WorkspaceDefaults,
    ) -> Result<(), KitError> {
        if context.execution_policy().sandbox_mode() != WorkspaceSandboxMode::FullAccess
            || context.execution_policy().network_access() != WorkspaceNetworkAccess::Enabled
            || context
                .roots()
                .iter()
                .any(|root| root.access() != WorkspaceRootAccess::ReadWrite)
        {
            return Err(KitError::unsupported(
                "requires full access, enabled network access, and read-write roots",
            ));
        }
        OpenCodeRouting::for_directory(context.cwd().to_path_buf())?;
        if defaults.reasoning_effort().is_some() || defaults.personality().is_some() {
            return Err(KitError::unsupported(
                "reasoning and personality selection are unsupported",
            ));
        }
        Ok(())
    }

    async fn model_inventory(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<OpenCodeModelInventory, KitError> {
        let providers = self.runtime.http().providers(routing).await?;
        OpenCodeModelInventory::from_native(providers)
    }

    async fn reconcile_creations(
        agent_id: &AgentId,
        state: &OpenCodeStateStore<S>,
        runtime: &OpenCodeRuntime,
        knowledge: &OpenCodeKnowledgeBase<S, D>,
        lifecycle_deadline: tokio::time::Instant,
        knowledge_error: &mut bool,
    ) -> Result<(), KitError> {
        let unresolved =
            tokio::time::timeout_at(lifecycle_deadline, state.unresolved_creations(agent_id))
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
        if unresolved.is_empty() {
            return Ok(());
        }
        let inventory =
            tokio::time::timeout_at(lifecycle_deadline, runtime.http().session_inventory())
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
        let mut correlations = std::collections::BTreeSet::new();
        for (record, creating) in unresolved {
            if !correlations.insert(creating.correlation().to_owned()) {
                return Err(KitError::invalid_state(
                    "creation correlation has multiple durable owners",
                ));
            }
            let correlation = OpenCodeCreationCorrelation::from_stored(creating.correlation())?;
            let matches = inventory.correlation_matches(&correlation);
            let [session_id] = matches.as_slice() else {
                return Err(if matches.is_empty() {
                    KitError::busy("session creation remains unresolved")
                } else {
                    KitError::invalid_state("session creation correlation is not unique")
                });
            };
            let routing = OpenCodeRouting::for_directory(creating.directory().to_path_buf())?;
            let native = tokio::time::timeout_at(
                lifecycle_deadline,
                runtime.http().session(&routing, session_id),
            )
            .await
            .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??
            .ok_or_else(|| KitError::busy("recovered session is not yet readable"))?;
            let native_validation = native
                .verify(session_id, creating.directory())
                .and_then(|()| native.verify_correlation(&correlation));
            if native_validation.is_err() {
                let sessions = tokio::time::timeout_at(
                    lifecycle_deadline,
                    runtime
                        .http()
                        .capture_subtree(std::slice::from_ref(session_id), creating.directory()),
                )
                .await
                .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
                let cleanup = OpenCodeCleanup::conversation(
                    agent_id.clone(),
                    record.id().clone(),
                    creating.directory().to_path_buf(),
                    session_id.clone(),
                    sessions,
                )?;
                match state
                    .replace_conversation_creation_with_cleanup(
                        agent_id,
                        &record,
                        &creating,
                        &cleanup,
                        session_id,
                        lifecycle_deadline,
                    )
                    .await?
                {
                    OpenCodeConversationMutationDisposition::Settled => {
                        let owner =
                            knowledge.conversation_owner(record.workspace_id(), record.id());
                        if let Err(settlement_error) =
                            knowledge.settle_forget(&owner, lifecycle_deadline).await
                        {
                            *knowledge_error = true;
                            return Err(settlement_error);
                        }
                        tokio::time::timeout_at(
                            lifecycle_deadline,
                            runtime.http().settle_cleanup(&cleanup),
                        )
                        .await
                        .map_err(|_| KitError::timeout("agent construction deadline elapsed"))??;
                        if tokio::time::Instant::now() >= lifecycle_deadline {
                            return Err(KitError::timeout("agent construction deadline elapsed"));
                        }
                        let finish = state.finish_cleanup(&cleanup).await;
                        if tokio::time::Instant::now() >= lifecycle_deadline {
                            return Err(KitError::timeout("agent construction deadline elapsed"));
                        }
                        finish?;
                        continue;
                    }
                    OpenCodeConversationMutationDisposition::RecoveryRequired(error) => {
                        return Err(error);
                    }
                }
            }
            if let Err(error) = knowledge
                .verify_before(
                    &knowledge.conversation_owner(record.workspace_id(), record.id()),
                    lifecycle_deadline,
                )
                .await
            {
                *knowledge_error = true;
                return Err(error);
            }
            let bound = creating.bind(session_id.clone())?;
            match state
                .complete_conversation_creation(
                    agent_id,
                    &record,
                    &creating,
                    &bound,
                    lifecycle_deadline,
                )
                .await?
            {
                OpenCodeConversationMutationDisposition::Settled => {}
                OpenCodeConversationMutationDisposition::RecoveryRequired(error) => {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend> AgentBackend for OpenCodeAgent<S, D> {
    type Workspace = OpenCodeWorkspace<S, D>;

    fn id(&self) -> AgentId {
        self.info.id().clone()
    }

    async fn info(&self) -> Result<AgentInfo, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_agent_work()?;
            admission.publish(Ok(self.info.clone()))
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::AgentInfo)
                .in_phase(ErrorPhase::StateRead)
        })
    }

    async fn new_workspace(
        &self,
        request: NewWorkspace,
    ) -> Result<OpenCodeWorkspace<S, D>, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_agent_work()?;
            if let Err(error) =
                self.validate_workspace_configuration(request.context(), request.defaults())
            {
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
            let http = self.runtime.http().clone();
            let events = self.events.clone();
            let prompt_policy = self.prompt_policy.clone();
            let hierarchy = self.hierarchy.clone();
            let knowledge = self.knowledge.clone();
            let tools = self.tools.clone();
            let lifecycle_timeout = self.config.lifecycle_timeout();
            let deadline = tokio::time::Instant::now() + lifecycle_timeout;
            tokio::spawn(async move {
                let result = async {
                    let directory = http.directory(request.context().cwd(), deadline).await?;
                    let record = state
                        .create_workspace(&agent_id, &request, directory, deadline)
                        .await?;
                    let claim =
                        hierarchy.claim_workspace(record.id().clone(), record.revision())?;
                    Ok(OpenCodeWorkspace::new(
                        record,
                        OpenCodeWorkspaceContext::new(
                            state,
                            http,
                            events,
                            prompt_policy,
                            hierarchy,
                            knowledge,
                            tools,
                        ),
                        claim,
                        lifecycle_timeout,
                    ))
                }
                .await;
                admission.publish(result)
            })
            .await
            .map_err(|_| KitError::integration("agent operation task failed"))?
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::NewWorkspace)
                .in_phase(ErrorPhase::Admission)
        })
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_agent_work()?;
            let result = async {
                self.state
                    .workspaces(self.info.id())
                    .await?
                    .into_iter()
                    .map(|record| record.info())
                    .collect()
            }
            .await;
            admission.publish(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ListWorkspaces)
                .in_phase(ErrorPhase::StateRead)
        })
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_read(&id).await?;
            let result = async { self.state.workspace(self.info.id(), &id).await?.info() }.await;
            admission.publish(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::WorkspaceInfo)
                .in_phase(ErrorPhase::StateRead)
        })
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<OpenCodeWorkspace<S, D>, KitError> {
        let result =
            async {
                let admission = self.hierarchy.admit_workspace_reopen(&id)?;
                let record = match self.state.workspace(self.info.id(), &id).await {
                    Ok(record) => record,
                    Err(error) => {
                        return admission.publish(Err(error));
                    }
                };
                let deadline = tokio::time::Instant::now() + self.config.lifecycle_timeout();
                let routing =
                    match tokio::time::timeout_at(deadline, self.state.workspace_routing(&record))
                        .await
                    {
                        Ok(Ok(routing)) => routing,
                        Ok(Err(error)) => return admission.publish(Err(error)),
                        Err(_) => {
                            return admission.publish(Err(KitError::timeout(
                                "workspace routing read deadline elapsed",
                            )
                            .at_phase(ErrorPhase::StateRead)));
                        }
                    };
                if let Err(error) = self
                    .runtime
                    .http()
                    .verify_directory(record.cwd(), routing.directory(), deadline)
                    .await
                {
                    return admission.publish(Err(error));
                }
                if let Err(error) = self
                    .knowledge
                    .verify(&self.knowledge.workspace_owner(&id))
                    .await
                {
                    return admission.publish(Err(error));
                }
                let result = (|| {
                    let claim = admission.claim_workspace(record.revision())?;
                    Ok(OpenCodeWorkspace::new(
                        record,
                        OpenCodeWorkspaceContext::new(
                            self.state.clone(),
                            self.runtime.http().clone(),
                            self.events.clone(),
                            self.prompt_policy.clone(),
                            self.hierarchy.clone(),
                            self.knowledge.clone(),
                            self.tools.clone(),
                        ),
                        claim,
                        self.config.lifecycle_timeout(),
                    ))
                })();
                admission.publish(result)
            }
            .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::GetWorkspace)
                .in_phase(ErrorPhase::Admission)
        })
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_
    {
        let request = request.clone();
        async move {
            let result = async {
                let (admission, initially_busy) =
                    self.hierarchy.admit_update_check(request.id())?;
                if let Err(error) =
                    self.validate_workspace_configuration(request.context(), request.defaults())
                {
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
                        return Err(KitError::conflict("workspace revision is stale")
                            .with_detail(ErrorDetail::StaleRevision));
                    }
                    if record.cwd() != request.context().cwd()
                        && !self.state.conversations(request.id()).await?.is_empty()
                    {
                        return Err(KitError::unsupported(
                            "cannot change cwd while conversations remain",
                        ));
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
            .await;
            result.map_err(|error: KitError| {
                error
                    .at_operation(Operation::CheckWorkspaceUpdate)
                    .in_phase(ErrorPhase::Admission)
            })
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_mutation(request.id())?;
            if let Err(error) =
                self.validate_workspace_configuration(request.context(), request.defaults())
            {
                return admission.publish(Err(error));
            }
            if let Err(error) = self
                .knowledge
                .validate_policy(request.defaults().knowledge_policy())
            {
                return admission.publish(Err(error));
            }
            let state = self.state.clone();
            let http = self.runtime.http().clone();
            let deadline = tokio::time::Instant::now() + self.config.lifecycle_timeout();
            let hierarchy = self.hierarchy.clone();
            let agent_id = self.info.id().clone();
            tokio::spawn(async move {
                let result = async {
                    let current =
                        tokio::time::timeout_at(deadline, state.workspace(&agent_id, request.id()))
                            .await
                            .map_err(|_| {
                                KitError::timeout("workspace update preflight deadline elapsed")
                                    .at_phase(ErrorPhase::StateRead)
                            })??;
                    if current.revision() != request.expected_revision() {
                        return Err(KitError::conflict("workspace revision is stale"));
                    }
                    let directory = http.directory(request.context().cwd(), deadline).await?;
                    let info = state
                        .update_workspace(&agent_id, &request, directory)
                        .await?
                        .info()?;
                    hierarchy.advance_workspace_revision(info.id(), info.revision());
                    Ok(info)
                }
                .await;
                admission.publish(result)
            })
            .await
            .map_err(|_| KitError::integration("agent operation task failed"))?
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::UpdateWorkspace)
                .in_phase(ErrorPhase::StateWrite)
        })
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        let result = async {
            let admission = self.hierarchy.admit_mutation(request.id())?;
            let state = self.state.clone();
            let http = self.runtime.http().clone();
            let hierarchy = self.hierarchy.clone();
            let agent_id = self.info.id().clone();
            let knowledge = self.knowledge.clone();
            let lifecycle_timeout = self.config.lifecycle_timeout();
            let deadline = tokio::time::Instant::now() + lifecycle_timeout;
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
                    let bindings = state.bindings_for_workspace(request.id()).await?;
                    let cleanup = http
                        .capture_workspace_cleanup(&agent_id, request.id(), &bindings)
                        .await?;
                    let deletion = state
                        .delete_workspace(
                            &agent_id,
                            request.id(),
                            request.expected_revision(),
                            cleanup.as_ref(),
                            deadline,
                        )
                        .await?;
                    hierarchy.invalidate_workspace(request.id());
                    if let OpenCodeWorkspaceDeleteDisposition::RecoveryRequired(error) = deletion {
                        return Err(error);
                    }
                    let workspace_knowledge = knowledge.workspace_owner(request.id());
                    let cleanup_attempt = async {
                        let _ = tokio::join!(
                            async {
                                if let Some(cleanup) = cleanup.as_ref()
                                    && http.settle_cleanup(cleanup).await.is_ok()
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
            .map_err(|_| KitError::integration("agent operation task failed"))?
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::DeleteWorkspace)
                .in_phase(ErrorPhase::StateWrite)
        })
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        let result = async {
            let admission = self.hierarchy.admit_agent_work()?;
            let result = self
                .knowledge
                .ingest(self.knowledge.agent_owner(), request)
                .await;
            admission.publish(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::IngestKnowledge)
                .in_phase(ErrorPhase::KnowledgeWrite)
        })
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_agent_work()?;
            let result = async {
                let routing = OpenCodeRouting::default();
                Ok(self.model_inventory(&routing).await?.portable())
            }
            .await;
            admission.publish(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ListModels)
                .in_phase(ErrorPhase::InventoryVerification)
        })
    }

    async fn shutdown(self) -> Result<(), KitError> {
        let result = async {
            let Self {
                info,
                runtime,
                events,
                state,
                prompt_policy,
                hierarchy,
                mut _claim,
                config,
                knowledge: _,
                tools: _,
            } = self;
            let shutdown_started = tokio::time::Instant::now();
            let lifecycle_timeout = config.lifecycle_timeout();
            let settlement_deadline = shutdown_started + lifecycle_timeout / 2;
            let deadline = shutdown_started + lifecycle_timeout;
            let http = runtime.http().clone();
            hierarchy.seal();
            let mut prompt_cancellation_error = None;
            match tokio::time::timeout_at(settlement_deadline, state.workspaces(info.id())).await {
                Ok(Ok(workspaces)) => {
                    'workspaces: for workspace in workspaces {
                        match tokio::time::timeout_at(
                            settlement_deadline,
                            state.conversation_bindings(workspace.id()),
                        )
                        .await
                        {
                            Ok(Ok(conversations)) => {
                                for (conversation, binding) in conversations {
                                    let ownership = async {
                                        let routing = state.workspace_routing(&workspace).await?;
                                        binding.verify_owner(&conversation, &workspace, &routing)
                                    };
                                    let ownership =
                                        tokio::time::timeout_at(settlement_deadline, ownership)
                                            .await
                                            .unwrap_or_else(|_| {
                                                Err(KitError::timeout(
                                                    "workspace cleanup routing deadline elapsed",
                                                ))
                                            });
                                    if let Err(error) = ownership {
                                        Self::retain_cleanup_failure(
                                            &mut prompt_cancellation_error,
                                            error,
                                        );
                                        continue;
                                    }
                                    let conversation_runtime = OpenCodeConversationRuntime::new(
                                        info.id().clone(),
                                        state.clone(),
                                        http.clone(),
                                        events.clone(),
                                        prompt_policy.clone(),
                                        binding,
                                    );
                                    match tokio::time::timeout_at(
                                        settlement_deadline,
                                        conversation_runtime.abort(settlement_deadline),
                                    )
                                    .await
                                    {
                                        Ok(Ok(_)) => {}
                                        Ok(Err(error)) => Self::retain_cleanup_failure(
                                            &mut prompt_cancellation_error,
                                            error,
                                        ),
                                        Err(_) => {
                                            Self::retain_cleanup_failure(
                                                &mut prompt_cancellation_error,
                                                KitError::timeout(
                                                    "agent prompt cancellation deadline elapsed",
                                                ),
                                            );
                                            break 'workspaces;
                                        }
                                    }
                                }
                            }
                            Ok(Err(error)) => {
                                Self::retain_cleanup_failure(&mut prompt_cancellation_error, error)
                            }
                            Err(_) => {
                                Self::retain_cleanup_failure(
                                    &mut prompt_cancellation_error,
                                    KitError::timeout("agent prompt cancellation deadline elapsed"),
                                );
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
            let quiescence = hierarchy.await_agent_quiescence(settlement_deadline).await;
            let prompt_tasks = state.shutdown_prompt_tasks(settlement_deadline).await;
            state.notifications().invalidate_all();
            if let Err(error) = prompt_tasks {
                Self::retain_cleanup_failure(&mut prompt_cancellation_error, error);
            }
            let mut first_error = quiescence.err();
            if let Some(error) = prompt_cancellation_error {
                Self::retain_cleanup_failure(&mut first_error, error);
            }
            let cleanup = async {
                match state.cleanup_obligations(info.id()).await {
                    Ok(cleanups) => {
                        for cleanup in cleanups {
                            if let Err(error) = http.settle_cleanup(&cleanup).await {
                                Self::retain_cleanup_failure(&mut first_error, error);
                                continue;
                            }
                            if let Err(error) = state.finish_cleanup(&cleanup).await {
                                Self::retain_cleanup_failure(&mut first_error, error);
                            }
                        }
                    }
                    Err(error) => Self::retain_cleanup_failure(&mut first_error, error),
                }
            };
            if tokio::time::timeout_at(settlement_deadline, cleanup)
                .await
                .is_err()
            {
                Self::retain_cleanup_failure(
                    &mut first_error,
                    KitError::timeout("agent shutdown deadline elapsed"),
                );
            }
            let runtime_result = runtime.shutdown(deadline).await;
            if let Err(error) = runtime_result {
                Self::retain_cleanup_failure(&mut first_error, error);
            }
            let result = match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            };
            _claim.complete_shutdown(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::Shutdown)
                .in_phase(ErrorPhase::Cleanup)
        })
    }
}
