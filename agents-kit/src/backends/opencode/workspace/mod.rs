use crate::backends::opencode::adapters::OpenCodeModelInventory;
use crate::backends::opencode::agent::events::OpenCodeEventHandle;
use crate::backends::opencode::conversations::OpenCodeConversation;
use crate::backends::opencode::conversations::runtime::{
    OpenCodeConversationRuntime, OpenCodePromptPolicy,
};
use crate::backends::opencode::knowledge_base::OpenCodeKnowledgeBase;
use crate::backends::opencode::protocol::http::{OpenCodeHttp, OpenCodeMutationError};
use crate::backends::opencode::protocol::requests::{
    OpenCodeCreationCorrelation, OpenCodeRouting, OpenCodeSessionCreateRequest,
};
use crate::backends::opencode::state_store::records::{OpenCodeCleanup, OpenCodeSessionBinding};
use crate::backends::opencode::state_store::{
    OpenCodeConversationMutationDisposition, OpenCodeStateStore,
};
use crate::backends::opencode::tools::OpenCodeTools;
use crate::core::hierarchy::{HierarchyClaim, HierarchyRuntime};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::error::{ErrorDetail, ErrorKind, ErrorPhase, Operation};
use crate::workspace::WorkspaceBackend;
use crate::{
    ConversationId, ConversationInfo, IngestKnowledgeRequest, KitError, NewConversation, ToolInfo,
    WorkspaceId, WorkspaceInfo,
};
use storage_kit::{DatabaseBackend, StateStoreBackend};

pub(crate) struct OpenCodeWorkspace<S: StateStoreBackend, D: DatabaseBackend> {
    record: WorkspaceRecord,
    context: OpenCodeWorkspaceContext<S, D>,
    claim: HierarchyClaim,
    lifecycle_timeout: std::time::Duration,
}

pub(super) struct OpenCodeWorkspaceContext<S: StateStoreBackend, D: DatabaseBackend> {
    state: OpenCodeStateStore<S>,
    http: OpenCodeHttp,
    events: OpenCodeEventHandle,
    prompt_policy: OpenCodePromptPolicy,
    hierarchy: HierarchyRuntime,
    knowledge: OpenCodeKnowledgeBase<S, D>,
    tools: OpenCodeTools,
}

impl<S: StateStoreBackend, D: DatabaseBackend> OpenCodeWorkspaceContext<S, D> {
    pub(super) fn new(
        state: OpenCodeStateStore<S>,
        http: OpenCodeHttp,
        events: OpenCodeEventHandle,
        prompt_policy: OpenCodePromptPolicy,
        hierarchy: HierarchyRuntime,
        knowledge: OpenCodeKnowledgeBase<S, D>,
        tools: OpenCodeTools,
    ) -> Self {
        Self {
            state,
            http,
            events,
            prompt_policy,
            hierarchy,
            knowledge,
            tools,
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend> OpenCodeWorkspace<S, D> {
    pub(super) fn new(
        record: WorkspaceRecord,
        context: OpenCodeWorkspaceContext<S, D>,
        claim: HierarchyClaim,
        lifecycle_timeout: std::time::Duration,
    ) -> Self {
        Self {
            record,
            context,
            claim,
            lifecycle_timeout,
        }
    }

    async fn current(&self) -> Result<WorkspaceRecord, KitError> {
        self.claim.ensure_open()?;
        let current = self
            .context
            .state
            .workspace(self.record.agent_id(), self.record.id())
            .await?;
        if current.revision() != self.record.revision() {
            return Err(KitError::conflict("workspace handle is stale"));
        }
        Ok(current)
    }

    async fn verify_created_session(
        http: &OpenCodeHttp,
        routing: &OpenCodeRouting,
        directory: &std::path::Path,
        session_id: &str,
        correlation: &OpenCodeCreationCorrelation,
    ) -> Result<(), KitError> {
        let native = http
            .session(routing, session_id)
            .await?
            .ok_or_else(|| KitError::busy("created session is not yet readable"))?;
        native.verify(session_id, directory)?;
        native.verify_correlation(correlation)
    }

    async fn recover_created_session(
        http: &OpenCodeHttp,
        routing: &OpenCodeRouting,
        directory: &std::path::Path,
        correlation: &OpenCodeCreationCorrelation,
    ) -> Result<String, KitError> {
        let matches = http
            .session_inventory()
            .await?
            .correlation_matches(correlation);
        let [session_id] = matches.as_slice() else {
            return Err(if matches.is_empty() {
                KitError::busy("session creation remains unresolved")
            } else {
                KitError::invalid_state("session creation correlation is not unique")
            });
        };
        Self::verify_created_session(http, routing, directory, session_id, correlation).await?;
        Ok(session_id.clone())
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend> WorkspaceBackend for OpenCodeWorkspace<S, D> {
    type Conversation = OpenCodeConversation<S, D>;

    fn id(&self) -> WorkspaceId {
        self.record.id().clone()
    }

    async fn info(&self) -> Result<WorkspaceInfo, KitError> {
        let result = async {
            let admission = self.context.hierarchy.admit_work(self.record.id())?;
            let result = async { self.current().await?.info() }.await;
            admission.publish(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::WorkspaceInfo)
                .in_phase(ErrorPhase::StateRead)
        })
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<OpenCodeConversation<S, D>, KitError> {
        let result = async {
            let admission = self.context.hierarchy.admit_work(self.record.id())?;
            let workspace = match self.current().await {
                Ok(workspace) => workspace,
                Err(error) => {
                    return admission.publish(Err(error));
                }
            };
            let workspace_info = match workspace.info() {
                Ok(info) => info,
                Err(error) => {
                    return admission.publish(Err(error));
                }
            };
            let reasoning_effort = request
                .defaults()
                .and_then(|defaults| defaults.reasoning_effort())
                .or_else(|| workspace_info.defaults().reasoning_effort());
            let personality = request
                .defaults()
                .and_then(|defaults| defaults.personality())
                .or_else(|| workspace_info.defaults().personality());
            if reasoning_effort.is_some() || personality.is_some() {
                return admission.publish(Err(KitError::unsupported(
                    "reasoning and personality selection are unsupported",
                )));
            }
            let policy = request
                .defaults()
                .and_then(|defaults| defaults.knowledge_policy())
                .or_else(|| workspace_info.defaults().knowledge_policy());
            if let Err(error) = self.context.knowledge.validate_policy(policy) {
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
                .context
                .knowledge
                .conversation_owner(&workspace_id, record.id());
            let knowledge_sources = request.knowledge_sources().to_vec();
            let agent_id = self.record.agent_id().clone();
            let state = self.context.state.clone();
            let http = self.context.http.clone();
            let events = self.context.events.clone();
            let prompt_policy = self.context.prompt_policy.clone();
            let hierarchy = self.context.hierarchy.clone();
            let creation_workspace_id = workspace_id.clone();
            let title = request.title().map(str::to_owned);
            let lifecycle_timeout = self.lifecycle_timeout;
            let knowledge = self.context.knowledge.clone();
            tokio::spawn(async move {
                let knowledge_deadline = tokio::time::Instant::now() + lifecycle_timeout;
                let creation = async {
                    let native_routing = tokio::time::timeout_at(
                        knowledge_deadline,
                        state.workspace_routing(&workspace),
                    )
                    .await
                    .map_err(|_| {
                        KitError::timeout("conversation routing read deadline elapsed")
                            .at_phase(ErrorPhase::StateRead)
                    })??;
                    http.verify_directory(
                        workspace.cwd(),
                        native_routing.directory(),
                        knowledge_deadline,
                    )
                    .await?;
                    let directory = native_routing.directory().to_path_buf();
                    let routing = OpenCodeRouting::for_directory(directory.clone())?;
                    let native_request = OpenCodeSessionCreateRequest::new(title);
                    let correlation = native_request.correlation()?;
                    let creating = OpenCodeSessionBinding::creating(
                        record.id().clone(),
                        creation_workspace_id.clone(),
                        workspace_revision,
                        directory.clone(),
                        correlation.as_str().to_owned(),
                    )?;
                    let knowledge_request = IngestKnowledgeRequest::new(knowledge_sources)?;
                    state
                        .begin_conversation_creation(
                            &agent_id,
                            &record,
                            &creating,
                            knowledge_deadline,
                        )
                        .await?;
                    if tokio::time::Instant::now() >= knowledge_deadline {
                        let error = KitError::timeout("conversation creation deadline elapsed")
                            .at_phase(ErrorPhase::StateCommit);
                        if let Err(cleanup) = state
                            .discard_conversation_creation(&record, &creating)
                            .await
                        {
                            hierarchy.invalidate();
                            return Err(
                                error.with_supplementary(cleanup.at_phase(ErrorPhase::Cleanup))
                            );
                        }
                        return Err(error);
                    }
                    if let Err(error) = knowledge
                        .ingest(conversation_knowledge.clone(), knowledge_request)
                        .await
                    {
                        let discarded = state
                            .discard_conversation_creation(&record, &creating)
                            .await;
                        if let Err(discard_error) = discarded {
                            hierarchy.invalidate();
                            return Err(error
                                .with_supplementary(discard_error.at_phase(ErrorPhase::Cleanup)));
                        }
                        let cleanup = knowledge
                            .settle_forget(&conversation_knowledge, knowledge_deadline)
                            .await;
                        return Err(error.with_cleanup_result(cleanup));
                    }
                    let (root, response_validation) =
                        match http.create_session(&routing, &native_request).await {
                            Ok(native) => {
                                let root = native.id().to_owned();
                                let validation = native
                                    .require_private()
                                    .and_then(|()| native.verify_correlation(&correlation));
                                (root, validation)
                            }
                            Err(OpenCodeMutationError::Rejected(error)) => {
                                if let Err(discard_error) = state
                                    .discard_conversation_creation(&record, &creating)
                                    .await
                                {
                                    hierarchy.invalidate();
                                    return Err(error.with_supplementary(
                                        discard_error.at_phase(ErrorPhase::Cleanup),
                                    ));
                                }
                                let cleanup = knowledge
                                    .settle_forget(&conversation_knowledge, knowledge_deadline)
                                    .await;
                                return Err(error.with_cleanup_result(cleanup));
                            }
                            Err(OpenCodeMutationError::Ambiguous(_)) => {
                                let root = Self::recover_created_session(
                                    &http,
                                    &routing,
                                    &directory,
                                    &correlation,
                                )
                                .await?;
                                (root, Ok(()))
                            }
                        };
                    let cleanup = match OpenCodeCleanup::conversation(
                        agent_id.clone(),
                        record.id().clone(),
                        directory.clone(),
                        root.clone(),
                        vec![root.clone()],
                    ) {
                        Ok(cleanup) => cleanup,
                        Err(error) => {
                            hierarchy.invalidate();
                            return Err(error);
                        }
                    };
                    let native_validation = async {
                        response_validation?;
                        Self::verify_created_session(
                            &http,
                            &routing,
                            &directory,
                            &root,
                            &correlation,
                        )
                        .await
                    }
                    .await;
                    let invalidate = native_validation.is_err();
                    let binding = native_validation.and_then(|()| creating.bind(root.clone()));
                    let binding = match binding {
                        Ok(binding) => {
                            let mut last_conflict = None;
                            loop {
                                if tokio::time::Instant::now() >= knowledge_deadline {
                                    let error =
                                        KitError::timeout("conversation binding deadline elapsed");
                                    return Err(match last_conflict {
                                        Some(conflict) => error.with_supplementary(conflict),
                                        None => error,
                                    });
                                }
                                match state
                                    .complete_conversation_creation(
                                        &agent_id,
                                        &record,
                                        &creating,
                                        &binding,
                                        knowledge_deadline,
                                    )
                                    .await
                                {
                                    Ok(OpenCodeConversationMutationDisposition::Settled) => {
                                        break binding;
                                    }
                                    Err(error)
                                        if error.kind() == ErrorKind::Conflict
                                            && error.detail()
                                                == Some(&ErrorDetail::TransactionConflict)
                                            && error.context().phase == ErrorPhase::StateCommit
                                            && error.cause().is_none()
                                            && error.http_status().is_none()
                                            && error.supplementary().is_empty()
                                            && !error.diagnostics_truncated() =>
                                    {
                                        // Only the binding transaction is re-authored.
                                        // Native creation, knowledge, and the immutable
                                        // Creating obligation retain their original owner.
                                        last_conflict = Some(error);
                                        tokio::time::sleep_until(
                                            (tokio::time::Instant::now()
                                                + std::time::Duration::from_millis(25))
                                            .min(knowledge_deadline),
                                        )
                                        .await;
                                    }
                                    Ok(
                                        OpenCodeConversationMutationDisposition::RecoveryRequired(
                                            error,
                                        ),
                                    )
                                    | Err(error) => {
                                        return Err(match last_conflict {
                                            Some(conflict) => error.with_supplementary(conflict),
                                            None => error,
                                        });
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            let handoff = state
                                .replace_conversation_creation_with_cleanup(
                                    &agent_id,
                                    &record,
                                    &creating,
                                    &cleanup,
                                    &root,
                                    knowledge_deadline,
                                )
                                .await;
                            if matches!(
                                handoff,
                                Ok(OpenCodeConversationMutationDisposition::Settled)
                            ) {
                                if http.settle_cleanup(&cleanup).await.is_ok() {
                                    let _ = state.finish_cleanup(&cleanup).await;
                                }
                                let _ = knowledge
                                    .settle_forget(&conversation_knowledge, knowledge_deadline)
                                    .await;
                            } else {
                                hierarchy.invalidate();
                            }
                            if invalidate {
                                hierarchy.invalidate();
                            }
                            return Err(error);
                        }
                    };
                    Ok((record, binding))
                }
                .await;
                let creation = creation.and_then(|(record, binding)| {
                    let claim = hierarchy.claim_conversation(
                        record.id().clone(),
                        workspace_id,
                        workspace_revision,
                    )?;
                    let runtime = OpenCodeConversationRuntime::new(
                        agent_id,
                        state,
                        http,
                        events,
                        prompt_policy,
                        binding,
                    );
                    Ok(OpenCodeConversation::new(
                        record,
                        hierarchy,
                        claim,
                        lifecycle_timeout,
                        runtime,
                        knowledge,
                    ))
                });
                admission.publish(creation)
            })
            .await
            .map_err(|_| KitError::integration("workspace operation task failed"))?
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::NewConversation)
                .in_phase(ErrorPhase::Admission)
        })
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, KitError> {
        let result = async {
            let admission = self.context.hierarchy.admit_work(self.record.id())?;
            let result = async {
                let workspace = self.current().await?;
                self.context
                    .state
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
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ListConversations)
                .in_phase(ErrorPhase::StateRead)
        })
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, KitError> {
        let result = async {
            let admission = self.context.hierarchy.admit_work(self.record.id())?;
            let result = async {
                let workspace = self.current().await?;
                let record = self.context.state.conversation(workspace.id(), &id).await?;
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
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ConversationInfo)
                .in_phase(ErrorPhase::StateRead)
        })
    }

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<OpenCodeConversation<S, D>, KitError> {
        let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
        let result = async {
            let admission = self.context.hierarchy.admit_work(self.record.id())?;
            let prepared = tokio::time::timeout_at(deadline, async {
                let workspace = self.current().await?;
                let record = self.context.state.conversation(workspace.id(), &id).await?;
                if record.workspace_revision() != workspace.revision() {
                    return Err(KitError::invalid_state(
                        "conversation revision contradicts its workspace",
                    ));
                }
                let binding = self.context.state.binding(workspace.id(), &id).await?;
                let native_routing = self.context.state.workspace_routing(&workspace).await?;
                binding.verify_owner(&record, &workspace, &native_routing)?;
                self.context
                    .http
                    .verify_directory(workspace.cwd(), native_routing.directory(), deadline)
                    .await?;
                let claim = self.context.hierarchy.claim_conversation(
                    id,
                    workspace.id().clone(),
                    workspace.revision(),
                )?;
                let routing = OpenCodeRouting::for_directory(binding.directory().to_path_buf())?;
                let session_id = binding.require_session_id()?.to_owned();
                let native = self
                    .context
                    .http
                    .session(&routing, &session_id)
                    .await?
                    .ok_or_else(|| KitError::conflict("bound session does not exist"))?;
                native.verify(&session_id, binding.directory())?;
                self.context
                    .http
                    .session_status(&routing)
                    .await?
                    .require_idle(&session_id)?;
                Ok::<_, KitError>((workspace, record, binding, claim))
            })
            .await
            .unwrap_or_else(|_| {
                Err(
                    KitError::timeout("conversation reopen preflight deadline elapsed")
                        .at_phase(ErrorPhase::StateRead),
                )
            });
            let (workspace, record, binding, claim) = match prepared {
                Ok(prepared) => prepared,
                Err(error) => {
                    return admission.publish(Err(error));
                }
            };
            if let Err(error) = self
                .context
                .knowledge
                .verify(
                    &self
                        .context
                        .knowledge
                        .conversation_owner(workspace.id(), record.id()),
                )
                .await
            {
                return admission.publish(Err(error));
            }
            let runtime = OpenCodeConversationRuntime::new(
                self.record.agent_id().clone(),
                self.context.state.clone(),
                self.context.http.clone(),
                self.context.events.clone(),
                self.context.prompt_policy.clone(),
                binding,
            );
            admission.publish(Ok(OpenCodeConversation::new(
                record,
                self.context.hierarchy.clone(),
                claim,
                self.lifecycle_timeout,
                runtime,
                self.context.knowledge.clone(),
            )))
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::GetConversation)
                .in_phase(ErrorPhase::Admission)
        })
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        let result = async {
            let admission = self.context.hierarchy.admit_work(self.record.id())?;
            let result = self
                .context
                .knowledge
                .ingest(
                    self.context.knowledge.workspace_owner(self.record.id()),
                    request,
                )
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

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, KitError> {
        let result = async {
            let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
            let admission = self.context.hierarchy.admit_work(self.record.id())?;
            let result = async {
                let workspace = self.current().await?;
                let info = workspace.info()?;
                let native_routing = tokio::time::timeout_at(
                    deadline,
                    self.context.state.workspace_routing(&workspace),
                )
                .await
                .map_err(|_| {
                    KitError::timeout("catalog routing read deadline elapsed")
                        .at_phase(ErrorPhase::StateRead)
                })??;
                self.context
                    .http
                    .verify_directory(workspace.cwd(), native_routing.directory(), deadline)
                    .await?;
                let routing =
                    OpenCodeRouting::for_directory(native_routing.directory().to_path_buf())?;
                let selected = if let Some(selector) = info.defaults().model() {
                    let providers = self.context.http.providers(&routing).await?;
                    Some(OpenCodeModelInventory::from_native(providers)?.select(selector)?)
                } else {
                    None
                };
                let catalog = self
                    .context
                    .tools
                    .discover(&self.context.http, &routing, selected.as_ref())
                    .await?;
                let catalog_generation = catalog.generation().clone();
                self.context
                    .state
                    .bind_complete_catalog(
                        self.record.agent_id(),
                        workspace.id(),
                        workspace.revision(),
                        &catalog,
                    )
                    .await?;
                if !self
                    .context
                    .state
                    .reconcile_catalog_binding(
                        self.record.agent_id(),
                        workspace.id(),
                        workspace.revision(),
                        &catalog_generation,
                        deadline,
                    )
                    .await?
                    || self.context.tools.generation() != catalog_generation
                {
                    return Err(KitError::conflict(
                        "tool catalog changed before publication",
                    ));
                }
                Ok(catalog.into_tools())
            }
            .await;
            admission.publish(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ListTools)
                .in_phase(ErrorPhase::InventoryVerification)
        })
    }

    async fn close(self) -> Result<(), KitError> {
        let result = async {
            let admission = self.context.hierarchy.admit_agent_work()?;
            let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
            let result = match self
                .context
                .hierarchy
                .seal_workspace(self.record.id(), self.record.revision())
            {
                Ok(()) => {
                    let prompt_cancellation = async {
                        let conversations = match tokio::time::timeout_at(
                            deadline,
                            self.context.state.conversation_bindings(self.record.id()),
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
                        let native_routing = tokio::time::timeout_at(
                            deadline,
                            self.context.state.workspace_routing(&self.record),
                        )
                        .await
                        .map_err(|_| {
                            KitError::timeout("workspace cleanup routing deadline elapsed")
                        })??;
                        let mut first_error = None;
                        for (conversation, binding) in conversations {
                            if let Err(error) =
                                binding.verify_owner(&conversation, &self.record, &native_routing)
                            {
                                if first_error.is_none() {
                                    first_error = Some(error);
                                }
                                continue;
                            }
                            let conversation_runtime = OpenCodeConversationRuntime::new(
                                self.record.agent_id().clone(),
                                self.context.state.clone(),
                                self.context.http.clone(),
                                self.context.events.clone(),
                                self.context.prompt_policy.clone(),
                                binding,
                            );
                            match tokio::time::timeout_at(
                                deadline,
                                conversation_runtime.abort(deadline),
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
                    let (prompt_cancellation, quiescence) = tokio::join!(
                        prompt_cancellation,
                        self.context
                            .hierarchy
                            .await_workspace_quiescence(self.record.id(), deadline),
                    );
                    let prompt_cancellation_error = prompt_cancellation.err();
                    let quiescence_error = quiescence.err();
                    self.context
                        .state
                        .notifications()
                        .invalidate_workspace(self.record.id());
                    let finalization_error = if quiescence_error.is_none() {
                        match tokio::time::timeout_at(deadline, async {
                            self.context
                                .state
                                .workspace(self.record.agent_id(), self.record.id())
                                .await
                                .map_err(|error| match error {
                                    error if error.kind() == ErrorKind::NotFound => {
                                        KitError::closed(
                                            "workspace was deleted while close was admitted",
                                        )
                                    }
                                    error => error,
                                })?;
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
                    [
                        prompt_cancellation_error,
                        quiescence_error,
                        finalization_error,
                    ]
                    .into_iter()
                    .flatten()
                    .reduce(|primary, secondary| primary.with_supplementary(secondary))
                    .map_or(Ok(()), Err)
                }
                Err(error) => Err(error),
            };
            admission.publish(result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::CloseWorkspace)
                .in_phase(ErrorPhase::Cleanup)
        })
    }
}
