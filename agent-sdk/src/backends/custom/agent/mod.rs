use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tool_sdk::ToolsetBackend;

use provider_sdk::ProviderBackend;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::agent::AgentBackend;
use crate::agent_storage::AgentStorageClaim;
use crate::backends::custom::adapters::{CustomProviderAdapter, CustomProviderOperation};
use crate::backends::custom::knowledge_base::CustomKnowledgeBase;
use crate::backends::custom::server::{CustomServer, CustomServerPrepublicationGuard};
use crate::backends::custom::server_requests::CustomServerRequests;
use crate::backends::custom::state_store::records::CustomCancellationIntent;
use crate::backends::custom::state_store::records::CustomCompatibilityFloor;
use crate::backends::custom::state_store::{
    CustomBuildClaimEscrow, CustomCascadeCancellation, CustomStateStore,
};
use crate::backends::custom::tools::{CustomToolCatalog, CustomTools};
use crate::backends::custom::workspace::CustomWorkspace;
use crate::core::hierarchy::HierarchyRuntime;
use crate::core::state::WorkspaceRecord;
use crate::{
    AgentConfig, AgentId, AgentInfo, AgentStorage, BackendConfig, ConversationId,
    CustomAgentConfig, DeleteWorkspaceRequest, IngestKnowledgeRequest, ManagedKnowledge, ModelInfo,
    NewWorkspace, SdkError, UpdateWorkspaceRequest, WorkspaceId, WorkspaceInfo,
    WorkspaceUpdateReadiness,
};
use storage_sdk::{DatabaseBackend, StateStoreBackend};

pub(crate) struct CustomAgent<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    info: AgentInfo,
    state: CustomStateStore<S, D>,
    server: CustomServer<S, D, P, T>,
    requests: CustomServerRequests<S, D, T>,
    hierarchy: HierarchyRuntime,
    claim: AgentStorageClaim,
    knowledge: CustomKnowledgeBase<S, D>,
}

pub(crate) struct CustomComposition<P: ProviderBackend, T: ToolsetBackend> {
    state: Mutex<CustomCompositionState<P, T>>,
    changed: Notify,
}

enum CustomCompositionState<P: ProviderBackend, T: ToolsetBackend> {
    Raw {
        provider: Arc<P>,
        toolsets: Vec<Arc<T>>,
    },
    Ready {
        provider: Arc<P>,
        tools: CustomTools<T>,
    },
    Task {
        handle: JoinHandle<Result<(), SdkError>>,
        phase: CustomCompositionCleanupPhase,
    },
    Transferred,
    Settled(Result<(), SdkError>),
    Poisoned,
}

enum CustomCompositionCleanupPhase {
    Running,
    Completed,
    Interrupted,
}

struct CustomCompositionCleanupGuard<P: ProviderBackend, T: ToolsetBackend> {
    owner: Arc<CustomComposition<P, T>>,
    state: CustomCompositionCleanupGuardState,
}

#[derive(Clone, Copy)]
enum CustomCompositionCleanupGuardState {
    Active,
    Complete,
}

impl<P: ProviderBackend, T: ToolsetBackend> CustomComposition<P, T> {
    fn new(provider: P, toolsets: Vec<Arc<T>>) -> Self {
        Self {
            state: Mutex::new(CustomCompositionState::Raw {
                provider: Arc::new(provider),
                toolsets,
            }),
            changed: Notify::new(),
        }
    }

    async fn prepare(&self) -> Result<(), SdkError> {
        let toolsets = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &*state {
                CustomCompositionState::Raw { toolsets, .. } => toolsets.clone(),
                _ => {
                    return Err(SdkError::invalid_state(
                        "composition preparation was repeated",
                    ));
                }
            }
        };
        let catalog = CustomToolCatalog::from_sources(&toolsets).await?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *state, CustomCompositionState::Poisoned);
        match previous {
            CustomCompositionState::Raw { provider, toolsets } => {
                *state = CustomCompositionState::Ready {
                    provider,
                    tools: CustomTools::new(toolsets, catalog),
                };
                Ok(())
            }
            previous => {
                *state = previous;
                Err(SdkError::invalid_state(
                    "composition changed during preparation",
                ))
            }
        }
    }

    fn compatibility(
        &self,
        config: &CustomAgentConfig,
    ) -> Result<CustomCompatibilityFloor, SdkError> {
        let (provider, floor) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let CustomCompositionState::Ready { provider, tools } = &*state else {
                return Err(SdkError::invalid_state(
                    "composition is unavailable before server transfer",
                ));
            };
            (provider.clone(), tools.floor().clone())
        };
        CustomProviderAdapter::validate_model(
            provider.as_ref(),
            config.default_model(),
            CustomProviderOperation::Build,
        )?;
        CustomCompatibilityFloor::new(
            config.default_model().as_str().to_owned(),
            floor,
            config.tool_approval(),
            config.default_tool_selection(),
            config.loop_limits(),
        )
    }

    fn transfer(&self) -> Result<(Arc<P>, CustomTools<T>), SdkError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *state, CustomCompositionState::Transferred);
        match previous {
            CustomCompositionState::Ready { provider, tools } => Ok((provider, tools)),
            previous => {
                *state = previous;
                Err(SdkError::invalid_state(
                    "composition cannot transfer its resources",
                ))
            }
        }
    }

    async fn cleanup_before(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
        primary: SdkError,
    ) -> SdkError {
        let cleanup = match self.start_cleanup() {
            Ok(()) => self.observe_cleanup_before(deadline).await,
            Err(error) => Err(error),
        };
        match cleanup {
            Ok(()) => primary,
            Err(error) => {
                primary.with_supplementary(error.at_phase(crate::error::ErrorPhase::Cleanup))
            }
        }
    }

    fn start_cleanup(self: &Arc<Self>) -> Result<(), SdkError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            CustomCompositionState::Raw { .. } | CustomCompositionState::Ready { .. } => {}
            CustomCompositionState::Task { .. }
            | CustomCompositionState::Transferred
            | CustomCompositionState::Settled(_) => return Ok(()),
            CustomCompositionState::Poisoned => {
                return Err(SdkError::invalid_state(
                    "composition cleanup owner is poisoned",
                ));
            }
        };
        let resources = std::mem::replace(&mut *state, CustomCompositionState::Poisoned);
        let owner = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let guard = CustomCompositionCleanupGuard {
                owner,
                state: CustomCompositionCleanupGuardState::Active,
            };
            let mut first_error = None;
            match resources {
                CustomCompositionState::Raw {
                    provider,
                    mut toolsets,
                } => {
                    while let Some(toolset) = toolsets.pop() {
                        if let Err(error) = toolset.shutdown().await {
                            SdkError::record_cleanup(
                                &mut first_error,
                                SdkError::from(error).at_phase(crate::error::ErrorPhase::Cleanup),
                            );
                        }
                    }
                    if let Err(error) = provider.shutdown().await {
                        SdkError::record_cleanup(
                            &mut first_error,
                            CustomProviderAdapter::map_shutdown_error(error),
                        );
                    }
                }
                CustomCompositionState::Ready { provider, tools } => {
                    if let Err(error) = tools.shutdown().await {
                        SdkError::record_cleanup(&mut first_error, error);
                    }
                    if let Err(error) = provider.shutdown().await {
                        SdkError::record_cleanup(
                            &mut first_error,
                            CustomProviderAdapter::map_shutdown_error(error),
                        );
                    }
                }
                _ => {
                    return Err(SdkError::invalid_state(
                        "composition cleanup resources are unavailable",
                    ));
                }
            }
            guard.complete();
            first_error.map_or(Ok(()), Err)
        });
        *state = CustomCompositionState::Task {
            handle,
            phase: CustomCompositionCleanupPhase::Running,
        };
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn observe_cleanup_before(&self, deadline: tokio::time::Instant) -> Result<(), SdkError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let should_yield = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match &*state {
                    CustomCompositionState::Task { handle, phase }
                        if !matches!(phase, CustomCompositionCleanupPhase::Running)
                            && handle.is_finished() =>
                    {
                        let previous =
                            std::mem::replace(&mut *state, CustomCompositionState::Poisoned);
                        if let CustomCompositionState::Task {
                            mut handle,
                            phase: CustomCompositionCleanupPhase::Completed,
                        } = previous
                        {
                            let waker = std::task::Waker::noop();
                            let mut context = Context::from_waker(waker);
                            if let Poll::Ready(Ok(result)) =
                                Pin::new(&mut handle).poll(&mut context)
                            {
                                *state = CustomCompositionState::Settled(result.clone());
                                return result;
                            }
                        }
                        return Err(SdkError::invalid_state(
                            "composition cleanup terminated unexpectedly",
                        ));
                    }
                    CustomCompositionState::Task {
                        phase:
                            CustomCompositionCleanupPhase::Completed
                            | CustomCompositionCleanupPhase::Interrupted,
                        ..
                    } => true,
                    CustomCompositionState::Task {
                        phase: CustomCompositionCleanupPhase::Running,
                        ..
                    } => false,
                    CustomCompositionState::Settled(result) => return result.clone(),
                    CustomCompositionState::Poisoned => {
                        return Err(SdkError::invalid_state(
                            "composition cleanup owner is poisoned",
                        ));
                    }
                    CustomCompositionState::Raw { .. }
                    | CustomCompositionState::Ready { .. }
                    | CustomCompositionState::Transferred => {
                        return Err(SdkError::invalid_state(
                            "composition cleanup was not started",
                        ));
                    }
                }
            };
            if should_yield {
                if tokio::time::Instant::now() >= deadline {
                    return Err(SdkError::timeout("composition cleanup timed out"));
                }
                tokio::task::yield_now().await;
            } else {
                tokio::time::timeout_at(deadline, changed)
                    .await
                    .map_err(|_| SdkError::timeout("composition cleanup timed out"))?;
            }
        }
    }

    fn finish_cleanup(&self, state: CustomCompositionCleanupGuardState) {
        let mut current = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *current, CustomCompositionState::Poisoned);
        *current = match previous {
            CustomCompositionState::Task {
                handle,
                phase: CustomCompositionCleanupPhase::Running,
            } => CustomCompositionState::Task {
                handle,
                phase: match state {
                    CustomCompositionCleanupGuardState::Active => {
                        CustomCompositionCleanupPhase::Interrupted
                    }
                    CustomCompositionCleanupGuardState::Complete => {
                        CustomCompositionCleanupPhase::Completed
                    }
                },
            },
            previous => previous,
        };
        drop(current);
        self.changed.notify_waiters();
    }
}

impl<P: ProviderBackend, T: ToolsetBackend> CustomCompositionCleanupGuard<P, T> {
    fn complete(mut self) {
        self.state = CustomCompositionCleanupGuardState::Complete;
    }
}

impl<P: ProviderBackend, T: ToolsetBackend> Drop for CustomCompositionCleanupGuard<P, T> {
    fn drop(&mut self) {
        self.owner.finish_cleanup(self.state);
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomAgent<S, D, P, T>
{
    pub(crate) fn compose(provider: P, toolsets: Vec<Arc<T>>) -> Arc<CustomComposition<P, T>> {
        Arc::new(CustomComposition::new(provider, toolsets))
    }

    pub(crate) async fn build(
        info: AgentInfo,
        config: AgentConfig,
        storage: AgentStorage<S>,
        knowledge: Option<ManagedKnowledge<S, D>>,
        claim: AgentStorageClaim,
        composition: Arc<CustomComposition<P, T>>,
        reopen: bool,
    ) -> Result<Self, SdkError> {
        let lifecycle_timeout = config.lifecycle_timeout();
        let lifecycle_deadline = tokio::time::Instant::now()
            .checked_add(lifecycle_timeout)
            .ok_or_else(|| SdkError::invalid_config("lifecycle timeout cannot be represented"))?;
        let BackendConfig::Custom(custom) = config.backend().clone() else {
            return Err(composition
                .cleanup_before(
                    lifecycle_deadline,
                    SdkError::invalid_state("agent configuration selection is contradictory"),
                )
                .await);
        };
        if let Err(error) = composition.prepare().await {
            return Err(composition.cleanup_before(lifecycle_deadline, error).await);
        }
        let compatibility = match composition.compatibility(&custom) {
            Ok(compatibility) => compatibility,
            Err(error) => {
                return Err(composition.cleanup_before(lifecycle_deadline, error).await);
            }
        };
        let build_claim = Arc::new(CustomBuildClaimEscrow::new(claim));
        let knowledge = CustomKnowledgeBase::new(
            knowledge,
            storage.clone(),
            info.id().clone(),
            Arc::clone(&build_claim),
        );
        let state = match CustomStateStore::new(
            info.id().clone(),
            storage,
            knowledge.is_configured(),
            config.prompt_limit().get(),
            config.default_prompt_timeout(),
            config.instructions().map(str::to_owned),
            Arc::clone(&build_claim),
        ) {
            Ok(state) => state,
            Err(error) => {
                return Err(composition.cleanup_before(lifecycle_deadline, error).await);
            }
        };
        if reopen {
            let verification =
                match tokio::time::timeout_at(lifecycle_deadline, state.verify(&compatibility))
                    .await
                {
                    Ok(result) => result,
                    Err(_) => Err(SdkError::timeout("agent reopen timed out")),
                };
            if let Err(error) = verification {
                return Err(composition.cleanup_before(lifecycle_deadline, error).await);
            }
        }
        if reopen {
            let reconciliation = state.reconcile_reopen(&knowledge, lifecycle_deadline).await;
            if let Err(error) = reconciliation {
                return Err(composition.cleanup_before(lifecycle_deadline, error).await);
            }
        }
        let (provider, tools) = match composition.transfer() {
            Ok(resources) => resources,
            Err(error) => {
                return Err(composition.cleanup_before(lifecycle_deadline, error).await);
            }
        };
        let requests = CustomServerRequests::new(state.clone(), tools.clone(), lifecycle_timeout);
        let server = CustomServer::build(
            provider,
            tools.clone(),
            requests.clone(),
            custom,
            lifecycle_timeout,
        );
        let mut publication = server.prepublication_guard(Arc::clone(&build_claim));
        if !reopen
            && let Err(error) = state
                .create_before(info.clone(), compatibility, lifecycle_deadline)
                .await
        {
            let mut primary = Some(error);
            if let Err(cleanup) = publication.start_cleanup() {
                SdkError::record_cleanup(&mut primary, cleanup);
            }
            if let Err(cleanup) = server.shutdown().await {
                SdkError::record_cleanup(&mut primary, cleanup);
            }
            return Err(primary.expect("primary failure exists"));
        }
        if reopen {
            let validation = match tokio::time::timeout_at(
                lifecycle_deadline,
                Self::validate_persisted_defaults(&state, &server),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(SdkError::timeout("agent reopen timed out")),
            };
            if let Err(error) = validation {
                let mut primary = Some(error);
                if let Err(cleanup) = publication.start_cleanup() {
                    SdkError::record_cleanup(&mut primary, cleanup);
                }
                if let Err(cleanup) = server.shutdown().await {
                    SdkError::record_cleanup(&mut primary, cleanup);
                }
                return Err(primary.expect("primary failure exists"));
            }
        }
        let claim = match build_claim.reclaim() {
            Ok(claim) => claim,
            Err(error) => {
                let mut primary = Some(error);
                if reopen {
                    if let Err(cleanup) = publication.start_cleanup() {
                        SdkError::record_cleanup(&mut primary, cleanup);
                    }
                    if let Err(cleanup) = server.shutdown().await {
                        SdkError::record_cleanup(&mut primary, cleanup);
                    }
                } else {
                    if let Err(cleanup) = Self::cleanup_failed_build(
                        &knowledge,
                        &server,
                        &mut publication,
                        lifecycle_deadline,
                    )
                    .await
                    {
                        SdkError::record_cleanup(&mut primary, cleanup);
                    }
                }
                return Err(primary.expect("primary failure exists"));
            }
        };
        publication.publish();
        Ok(Self {
            info,
            state,
            server,
            requests,
            hierarchy: HierarchyRuntime::default(),
            claim,
            knowledge,
        })
    }

    pub(crate) fn retain_storage_claim(&mut self) {
        self.claim.retain_until_shutdown();
    }

    async fn cleanup_failed_build(
        knowledge: &CustomKnowledgeBase<S, D>,
        server: &CustomServer<S, D, P, T>,
        publication: &mut CustomServerPrepublicationGuard<S, D, P, T>,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        let mut primary = None;
        if let Err(error) = publication.start_cleanup() {
            SdkError::record_cleanup(&mut primary, error);
        }
        let knowledge_cleanup = knowledge.settle_failed_build_before(deadline);
        let server_cleanup = server.shutdown();
        let (knowledge, server) = tokio::join!(knowledge_cleanup, server_cleanup);
        for result in [knowledge, server] {
            if let Err(error) = result {
                SdkError::record_cleanup(&mut primary, error);
            }
        }
        primary.map_or(Ok(()), Err)
    }

    async fn finish_workspace_delete(
        server: &CustomServer<S, D, P, T>,
        knowledge: &CustomKnowledgeBase<S, D>,
        workspace_id: &WorkspaceId,
        conversations: &[ConversationId],
    ) {
        let deadline = server.lifecycle_deadline();
        let route_cleanup = async {
            for conversation in conversations {
                let _ = server.remove_route_before(conversation, deadline).await;
            }
        };
        let mut owners = conversations
            .iter()
            .map(|conversation_id| knowledge.conversation_owner(workspace_id, conversation_id))
            .collect::<Vec<_>>();
        owners.push(knowledge.workspace_owner(workspace_id));
        let knowledge_cleanup = knowledge.settle_forgets_before(owners, deadline);
        let _ = tokio::join!(route_cleanup, knowledge_cleanup);
    }

    async fn validate_persisted_defaults(
        state: &CustomStateStore<S, D>,
        server: &CustomServer<S, D, P, T>,
    ) -> Result<(), SdkError> {
        for workspace in state.workspaces().await? {
            let info = workspace.info()?;
            server.validate_workspace(info.context())?;
            server.validate_defaults(info.defaults(), CustomProviderOperation::Inventory)?;
            for conversation in state.conversations(workspace.id()).await? {
                if conversation.workspace_revision() != workspace.revision() {
                    return Err(SdkError::invalid_state(
                        "conversation revision contradicts its workspace",
                    ));
                }
                let defaults = conversation.defaults()?;
                server.validate_conversation_defaults(
                    info.defaults(),
                    defaults.as_ref(),
                    CustomProviderOperation::Inventory,
                )?;
            }
        }
        Ok(())
    }

    fn validate_workspace_record(
        &self,
        record: &WorkspaceRecord,
    ) -> Result<WorkspaceInfo, SdkError> {
        let info = record.info()?;
        self.server.validate_workspace(info.context())?;
        self.server
            .validate_defaults(info.defaults(), CustomProviderOperation::Inventory)?;
        Ok(info)
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> AgentBackend
    for CustomAgent<S, D, P, T>
{
    type Workspace = CustomWorkspace<S, D, P, T>;

    fn id(&self) -> AgentId {
        self.info.id().clone()
    }

    async fn info(&self) -> Result<AgentInfo, SdkError> {
        let admission = self.hierarchy.admit_agent_work()?;
        admission.publish(Ok(self.info.clone()))
    }

    async fn new_workspace(
        &self,
        request: NewWorkspace,
    ) -> Result<CustomWorkspace<S, D, P, T>, SdkError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let state = self.state.clone();
        let server = self.server.clone();
        let requests = self.requests.clone();
        let hierarchy = self.hierarchy.clone();
        let knowledge = self.knowledge.clone();
        tokio::spawn(async move {
            let result = async {
                server.validate_workspace(request.context())?;
                server.validate_defaults(request.defaults(), CustomProviderOperation::Request)?;
                knowledge.validate_policy(request.defaults().knowledge_policy())?;
                let record = state.create_workspace(&request).await?;
                let claim = hierarchy.claim_workspace(record.id().clone(), record.revision())?;
                Ok(CustomWorkspace::new(
                    record, state, server, requests, hierarchy, claim, knowledge,
                ))
            }
            .await;
            admission.publish(result)
        })
        .await
        .map_err(|_| {
            SdkError::integration("workspace creation task failed")
                .with_detail(crate::error::ErrorDetail::TaskTerminated)
        })?
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, SdkError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let result = async {
            self.state
                .workspaces()
                .await?
                .into_iter()
                .map(|record| self.validate_workspace_record(&record))
                .collect()
        }
        .await;
        admission.publish(result)
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, SdkError> {
        let admission = self.hierarchy.admit_read(&id).await?;
        let result = async {
            let record = self.state.workspace(&id).await?;
            self.validate_workspace_record(&record)
        }
        .await;
        admission.publish(result)
    }

    async fn get_workspace(
        &self,
        id: WorkspaceId,
    ) -> Result<CustomWorkspace<S, D, P, T>, SdkError> {
        let admission = self.hierarchy.admit_workspace_reopen(&id)?;
        let deadline = self.server.lifecycle_deadline();
        let result = async {
            let record = tokio::time::timeout_at(deadline, self.state.workspace(&id))
                .await
                .map_err(|_| SdkError::timeout("workspace reopen timed out"))??;
            let _ = self.validate_workspace_record(&record)?;
            self.knowledge
                .verify_before(self.knowledge.workspace_owner(&id), deadline)
                .await?;
            self.server.reopen_workspace(&id)?;
            let claim = admission.claim_workspace(record.revision())?;
            Ok(CustomWorkspace::new(
                record,
                self.state.clone(),
                self.server.clone(),
                self.requests.clone(),
                self.hierarchy.clone(),
                claim,
                self.knowledge.clone(),
            ))
        }
        .await;
        admission.publish(result)
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, SdkError>> + Send + '_
    {
        let request = request.clone();
        async move {
            self.server.validate_workspace(request.context())?;
            self.server
                .validate_defaults(request.defaults(), CustomProviderOperation::Request)?;
            self.knowledge
                .validate_policy(request.defaults().knowledge_policy())?;
            let (admission, hierarchy_busy) = self.hierarchy.admit_update_check(request.id())?;
            let result = async {
                let current = self.state.workspace(request.id()).await?;
                if current.revision() != request.expected_revision() {
                    return Err(SdkError::conflict("workspace revision is stale"));
                }
                let durable_busy = self
                    .state
                    .workspace_has_unsettled_work(request.id())
                    .await?;
                Ok(if hierarchy_busy || durable_busy {
                    WorkspaceUpdateReadiness::Busy
                } else {
                    WorkspaceUpdateReadiness::Ready
                })
            }
            .await;
            admission.publish(result)
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, SdkError> {
        self.server.validate_workspace(request.context())?;
        self.server
            .validate_defaults(request.defaults(), CustomProviderOperation::Request)?;
        self.knowledge
            .validate_policy(request.defaults().knowledge_policy())?;
        let admission = self.hierarchy.admit_mutation(request.id())?;
        let state = self.state.clone();
        let server = self.server.clone();
        let hierarchy = self.hierarchy.clone();
        tokio::spawn(async move {
            let deadline = server.lifecycle_deadline();
            let result = async {
                let current = state.workspace(request.id()).await?;
                if current.revision() != request.expected_revision() {
                    return Err(SdkError::conflict("workspace revision is stale"));
                }
                state
                    .settle_terminal_drains_before(request.id(), deadline)
                    .await?;
                if state.workspace_has_unsettled_work(request.id()).await? {
                    return Err(SdkError::busy("workspace has unsettled descendant work"));
                }
                state.update_workspace(&request).await
            }
            .await;
            let (info, conversations) = match result {
                Ok(updated) => updated,
                Err(error) => return admission.publish(Err(error)),
            };
            hierarchy.advance_workspace_revision(info.id(), info.revision());
            for conversation in conversations {
                let _ = server.remove_route_before(&conversation, deadline).await;
            }
            admission.publish(Ok(info))
        })
        .await
        .map_err(|_| {
            SdkError::integration("workspace update task failed")
                .with_detail(crate::error::ErrorDetail::TaskTerminated)
        })?
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), SdkError> {
        let admission = self.hierarchy.admit_mutation(request.id())?;
        let state = self.state.clone();
        let server = self.server.clone();
        let hierarchy = self.hierarchy.clone();
        let knowledge = self.knowledge.clone();
        tokio::spawn(async move {
            let deadline = server.lifecycle_deadline();
            let result = async {
                let current = state.workspace(request.id()).await?;
                if current.revision() != request.expected_revision() {
                    return Err(SdkError::conflict("workspace revision is stale"));
                }
                state
                    .settle_terminal_drains_before(request.id(), deadline)
                    .await?;
                if state.workspace_has_unsettled_work(request.id()).await? {
                    return Err(SdkError::busy("workspace has unsettled descendant work"));
                }
                state
                    .delete_workspace(request.id(), request.expected_revision())
                    .await
            }
            .await;
            if let Ok(conversations) = &result {
                hierarchy.invalidate_workspace(request.id());
                Self::finish_workspace_delete(&server, &knowledge, request.id(), conversations)
                    .await;
            }
            admission.publish(result.map(|_| ()))
        })
        .await
        .map_err(|_| {
            SdkError::integration("workspace deletion task failed")
                .with_detail(crate::error::ErrorDetail::TaskTerminated)
        })?
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, SdkError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let result = self.server.models().await;
        admission.publish(result)
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), SdkError> {
        let admission = self.hierarchy.admit_agent_work()?;
        let result = self
            .knowledge
            .ingest(self.knowledge.agent_owner(), request)
            .await;
        admission.publish(result)
    }

    async fn shutdown(mut self) -> Result<(), SdkError> {
        let deadline = self.server.lifecycle_deadline();
        self.hierarchy.seal();
        self.server.seal_all_before(deadline);
        let workspaces = match tokio::time::timeout_at(deadline, self.state.workspaces()).await {
            Ok(result) => result,
            Err(_) => Err(SdkError::timeout("workspace inventory timed out")),
        };
        let mut first_error = None;
        match workspaces {
            Ok(workspaces) => {
                for workspace in workspaces {
                    if let Err(error) = self
                        .state
                        .settle_conversation_provisioning_before(workspace.id(), deadline)
                        .await
                    {
                        SdkError::record_cleanup(&mut first_error, error);
                    }
                    let conversations = match tokio::time::timeout_at(
                        deadline,
                        self.state.conversations(workspace.id()),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(SdkError::timeout("conversation inventory timed out")),
                    };
                    match conversations {
                        Ok(conversations) => {
                            for conversation in conversations {
                                let terminal = match self
                                    .state
                                    .request_cascade_cancellation_before(
                                        conversation.clone(),
                                        CustomCancellationIntent::AgentShutdown,
                                        deadline,
                                    )
                                    .await
                                {
                                    Ok(CustomCascadeCancellation::Active(generation)) => {
                                        let terminal = self.hierarchy.conversation_is_terminal(
                                            conversation.workspace_id(),
                                            conversation.id(),
                                        );
                                        if !terminal
                                            && let Err(error) =
                                                self.server.cancel(conversation.id(), generation)
                                        {
                                            SdkError::record_cleanup(&mut first_error, error);
                                        }
                                        terminal
                                    }
                                    Ok(CustomCascadeCancellation::Quiescent) => {
                                        self.hierarchy.conversation_is_terminal(
                                            conversation.workspace_id(),
                                            conversation.id(),
                                        )
                                    }
                                    Ok(CustomCascadeCancellation::Terminal) => true,
                                    Err(error) => {
                                        SdkError::record_cleanup(&mut first_error, error);
                                        false
                                    }
                                };
                                if terminal {
                                    continue;
                                }
                                if let Err(error) = self
                                    .requests
                                    .cancel_conversation(
                                        conversation.id(),
                                        deadline,
                                        Some(deadline),
                                    )
                                    .await
                                {
                                    SdkError::record_cleanup(&mut first_error, error);
                                }
                            }
                        }
                        Err(error) => SdkError::record_cleanup(&mut first_error, error),
                    }
                }
            }
            Err(error) => SdkError::record_cleanup(&mut first_error, error),
        }
        if let Err(error) = self.hierarchy.await_agent_quiescence(deadline).await {
            SdkError::record_cleanup(&mut first_error, error);
        }
        if let Err(error) = self.state.settle_all_owners_before(deadline).await {
            SdkError::record_cleanup(&mut first_error, error);
        }
        if let Err(error) = self.knowledge.settle_operations_before(deadline).await {
            SdkError::record_cleanup(&mut first_error, error);
        }
        self.state.notifications().invalidate_all();
        let server_result = self.server.shutdown().await;
        if let Err(error) = server_result {
            SdkError::record_cleanup(&mut first_error, error);
        }
        let result = first_error.map_or(Ok(()), Err);
        self.claim.complete_shutdown(result)
    }
}
