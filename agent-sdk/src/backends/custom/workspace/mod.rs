use crate::backends::custom::adapters::CustomProviderOperation;
use crate::backends::custom::conversations::CustomConversation;
use crate::backends::custom::conversations::runtime::CustomConversationRuntime;
use crate::backends::custom::knowledge_base::CustomKnowledgeBase;
use crate::backends::custom::server::{CustomRouteRegistration, CustomServer};
use crate::backends::custom::server_requests::CustomServerRequests;
use std::sync::{Arc, Mutex};
use tool_sdk::ToolsetBackend;

use tokio::task::JoinHandle;

use crate::backends::custom::state_store::records::CustomCancellationIntent;
use crate::backends::custom::state_store::{CustomCascadeCancellation, CustomStateStore};
use crate::core::hierarchy::{HierarchyAdmission, HierarchyClaim, HierarchyRuntime};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::workspace::WorkspaceBackend;
use crate::{
    ConversationId, ConversationInfo, IngestKnowledgeRequest, NewConversation, SdkError, ToolInfo,
    WorkspaceId, WorkspaceInfo,
};
use provider_sdk::ProviderBackend;
use storage_sdk::{DatabaseBackend, StateStoreBackend};

pub(crate) struct CustomWorkspace<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    record: WorkspaceRecord,
    runtime: CustomWorkspaceRuntime<S, D, P, T>,
    claim: HierarchyClaim,
}

struct CustomWorkspaceRuntime<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    state: CustomStateStore<S, D>,
    server: CustomServer<S, D, P, T>,
    requests: CustomServerRequests<S, D, T>,
    hierarchy: HierarchyRuntime,
    knowledge: CustomKnowledgeBase<S, D>,
}

struct CustomConversationProvisioning<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    workspace: WorkspaceRecord,
    record: ConversationRecord,
    request: NewConversation,
    runtime: CustomWorkspaceRuntime<S, D, P, T>,
    deadline: tokio::time::Instant,
    state: Mutex<CustomConversationProvisioningState>,
    result: Mutex<Option<CustomProvisioningResult<S, D, P, T>>>,
    observer: Mutex<CustomConversationProvisioningObserver>,
}

enum CustomConversationProvisioningState {
    Installing,
    Running {
        handle: JoinHandle<()>,
        phase: CustomConversationProvisioningPhase,
    },
    Poisoned,
}

enum CustomConversationProvisioningPhase {
    Running,
    Reconciled,
    Interrupted,
}

enum CustomConversationProvisioningObserver {
    Available {
        ready: tokio::sync::oneshot::Receiver<()>,
        claim: tokio::sync::oneshot::Sender<()>,
    },
    Claimed,
}

enum CustomProvisioningFailure<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    BeforeRoute(SdkError),
    WithRoute {
        error: SdkError,
        conversation: Box<CustomConversation<S, D, P, T>>,
        registration: CustomRouteRegistration<S, D, P, T>,
    },
}

enum CustomProvisioningResult<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    Ready {
        conversation: Box<CustomConversation<S, D, P, T>>,
        registration: CustomRouteRegistration<S, D, P, T>,
    },
    Failed(SdkError),
}

#[derive(Clone, Copy)]
enum CustomDurableProvisioningCleanup {
    Failed,
    Undelivered,
}

struct CustomConversationProvisioningGuard<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    owner: Arc<CustomConversationProvisioning<S, D, P, T>>,
    state: CustomConversationProvisioningGuardState,
}

#[derive(Clone, Copy)]
enum CustomConversationProvisioningGuardState {
    Active,
    Reconciled,
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomWorkspace<S, D, P, T>
{
    pub(crate) fn new(
        record: WorkspaceRecord,
        state: CustomStateStore<S, D>,
        server: CustomServer<S, D, P, T>,
        requests: CustomServerRequests<S, D, T>,
        hierarchy: HierarchyRuntime,
        claim: HierarchyClaim,
        knowledge: CustomKnowledgeBase<S, D>,
    ) -> Self {
        Self {
            record,
            claim,
            runtime: CustomWorkspaceRuntime {
                state,
                server,
                requests,
                hierarchy,
                knowledge,
            },
        }
    }

    async fn current(&self) -> Result<WorkspaceRecord, SdkError> {
        self.claim.ensure_open()?;
        self.runtime.current(&self.record).await
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Clone
    for CustomWorkspaceRuntime<S, D, P, T>
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            server: self.server.clone(),
            requests: self.requests.clone(),
            hierarchy: self.hierarchy.clone(),
            knowledge: self.knowledge.clone(),
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomWorkspaceRuntime<S, D, P, T>
{
    async fn current(&self, expected: &WorkspaceRecord) -> Result<WorkspaceRecord, SdkError> {
        let current = self.state.workspace(expected.id()).await?;
        if current.revision() != expected.revision() {
            return Err(SdkError::conflict("workspace handle is stale"));
        }
        let info = current.info()?;
        self.server.validate_workspace(info.context())?;
        self.server
            .validate_defaults(info.defaults(), CustomProviderOperation::Inventory)?;
        Ok(current)
    }

    fn validate_conversation_record(
        &self,
        workspace: &WorkspaceRecord,
        record: &ConversationRecord,
    ) -> Result<ConversationInfo, SdkError> {
        if record.workspace_revision() != workspace.revision() {
            return Err(SdkError::invalid_state(
                "conversation revision contradicts its workspace",
            ));
        }
        let workspace_info = workspace.info()?;
        let defaults = record.defaults()?;
        self.server.validate_conversation_defaults(
            workspace_info.defaults(),
            defaults.as_ref(),
            CustomProviderOperation::Inventory,
        )?;
        record.info()
    }

    async fn open_conversation(
        &self,
        workspace: &WorkspaceRecord,
        record: ConversationRecord,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            CustomConversation<S, D, P, T>,
            CustomRouteRegistration<S, D, P, T>,
        ),
        SdkError,
    > {
        let _ = self.validate_conversation_record(workspace, &record)?;
        let claim = self.hierarchy.claim_conversation(
            record.id().clone(),
            record.workspace_id().clone(),
            record.workspace_revision(),
        )?;
        if let Err(error) = self
            .knowledge
            .verify_before(
                self.knowledge
                    .conversation_owner(workspace.id(), record.id()),
                deadline,
            )
            .await
        {
            drop(claim);
            return Err(error);
        }
        if let Err(error) = self.requests.ensure_quiescent_reopen(record.id()) {
            drop(claim);
            return Err(error);
        }
        if let Err(error) = self
            .server
            .await_prompt_settlement_before(record.id(), deadline)
            .await
        {
            drop(claim);
            return Err(error);
        }
        if let Err(error) = self
            .state
            .ensure_quiescent_open_before(record.id().clone(), deadline)
            .await
        {
            drop(claim);
            return Err(error);
        }
        let registration = match self
            .server
            .create_route(record.workspace_id().clone(), record.id().clone(), deadline)
            .await
        {
            Ok(registration) => registration,
            Err(error) => {
                drop(claim);
                return Err(error);
            }
        };
        let runtime = CustomConversationRuntime::new(
            record.id().clone(),
            self.state.clone(),
            self.server.clone(),
            self.requests.clone(),
            self.knowledge.clone(),
        );
        Ok((
            CustomConversation::new(
                record,
                self.state.clone(),
                self.hierarchy.clone(),
                claim,
                runtime,
                self.knowledge.clone(),
            ),
            registration,
        ))
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomConversationProvisioning<S, D, P, T>
{
    async fn execute(
        &self,
    ) -> Result<
        (
            CustomConversation<S, D, P, T>,
            CustomRouteRegistration<S, D, P, T>,
        ),
        CustomProvisioningFailure<S, D, P, T>,
    > {
        let workspace =
            tokio::time::timeout_at(self.deadline, self.runtime.current(&self.workspace))
                .await
                .map_err(|_| {
                    CustomProvisioningFailure::BeforeRoute(SdkError::timeout(
                        "conversation creation timed out",
                    ))
                })?
                .map_err(CustomProvisioningFailure::BeforeRoute)?;
        let workspace_info = workspace
            .info()
            .map_err(CustomProvisioningFailure::BeforeRoute)?;
        self.runtime
            .server
            .validate_conversation_defaults(
                workspace_info.defaults(),
                self.request.defaults(),
                CustomProviderOperation::Request,
            )
            .map_err(CustomProvisioningFailure::BeforeRoute)?;
        let effective_policy = self
            .request
            .defaults()
            .and_then(|defaults| defaults.knowledge_policy())
            .or_else(|| workspace_info.defaults().knowledge_policy());
        self.runtime
            .knowledge
            .validate_policy(effective_policy)
            .map_err(CustomProvisioningFailure::BeforeRoute)?;
        if let Err(error) = self
            .runtime
            .state
            .create_conversation_before(self.record.clone(), self.deadline)
            .await
        {
            return Err(CustomProvisioningFailure::BeforeRoute(error));
        }
        let owner = self
            .runtime
            .knowledge
            .conversation_owner(self.record.workspace_id(), self.record.id());
        if let Err(error) = self
            .runtime
            .knowledge
            .ingest_sources_before(
                owner,
                self.request.knowledge_sources().to_vec(),
                self.deadline,
            )
            .await
        {
            return Err(CustomProvisioningFailure::BeforeRoute(error));
        }
        let (conversation, registration) = match self.open_conversation(self.deadline).await {
            Ok(opened) => opened,
            Err(error) => return Err(CustomProvisioningFailure::BeforeRoute(error)),
        };
        if let Err(error) = self
            .runtime
            .state
            .promote_conversation_before(self.record.clone(), self.deadline)
            .await
        {
            return Err(CustomProvisioningFailure::WithRoute {
                error,
                conversation: Box::new(conversation),
                registration,
            });
        }
        Ok((conversation, registration))
    }

    async fn open_conversation(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            CustomConversation<S, D, P, T>,
            CustomRouteRegistration<S, D, P, T>,
        ),
        SdkError,
    > {
        self.runtime
            .open_conversation(&self.workspace, self.record.clone(), deadline)
            .await
    }

    async fn publish_failure(
        &self,
        failure: CustomProvisioningFailure<S, D, P, T>,
        admission: HierarchyAdmission,
        ready: tokio::sync::oneshot::Sender<()>,
    ) -> bool {
        match failure {
            CustomProvisioningFailure::BeforeRoute(error) => {
                let result = admission.publish::<()>(Err(error));
                self.store_error(result);
                let _ = ready.send(());
                self.reconcile_durable_provisioning(
                    self.runtime.server.lifecycle_deadline(),
                    CustomDurableProvisioningCleanup::Failed,
                )
                .await
            }
            CustomProvisioningFailure::WithRoute {
                error,
                conversation,
                registration,
            } => {
                let result = admission.publish::<()>(Err(error));
                self.store_error(result);
                let _ = ready.send(());
                let route_reconciled = self.reconcile_route(registration).await;
                let durable_reconciled = if route_reconciled {
                    self.reconcile_durable_provisioning(
                        self.runtime.server.lifecycle_deadline(),
                        CustomDurableProvisioningCleanup::Failed,
                    )
                    .await
                } else {
                    false
                };
                drop(conversation);
                route_reconciled && durable_reconciled
            }
        }
    }

    fn store_error(&self, result: Result<(), SdkError>) {
        let error = result.err().unwrap_or_else(|| {
            SdkError::invalid_state("conversation provisioning failed without an error")
        });
        *self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(CustomProvisioningResult::Failed(error));
    }

    async fn reconcile_route(&self, registration: CustomRouteRegistration<S, D, P, T>) -> bool {
        if registration.settle_shutdown().await.is_err() {
            return false;
        }
        registration.detach().is_ok()
    }

    async fn reconcile_durable_provisioning(
        &self,
        deadline: tokio::time::Instant,
        cleanup: CustomDurableProvisioningCleanup,
    ) -> bool {
        let mut retry_delay = std::time::Duration::from_millis(10);
        loop {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            if self
                .runtime
                .state
                .settle_mutations_before(deadline)
                .await
                .is_err()
            {
                return false;
            }
            let result = match cleanup {
                CustomDurableProvisioningCleanup::Failed => {
                    self.runtime
                        .state
                        .discard_provisioning_before(
                            self.record.clone(),
                            self.runtime.knowledge.clone(),
                            deadline,
                        )
                        .await
                }
                CustomDurableProvisioningCleanup::Undelivered => {
                    self.runtime
                        .state
                        .discard_undelivered_conversation_before(
                            self.record.clone(),
                            self.runtime.knowledge.clone(),
                            deadline,
                        )
                        .await
                }
            };
            match result {
                Ok(()) => return true,
                Err(error) if error.kind() == crate::error::ErrorKind::NotFound => return true,
                Err(error)
                    if matches!(
                        error.kind(),
                        crate::error::ErrorKind::Integration
                            | crate::error::ErrorKind::Timeout
                            | crate::error::ErrorKind::Busy
                    ) => {}
                Err(_) => return false,
            }
            let Some(next) = tokio::time::Instant::now().checked_add(retry_delay) else {
                return false;
            };
            if next >= deadline {
                return false;
            }
            tokio::time::sleep_until(next).await;
            retry_delay = retry_delay
                .saturating_mul(2)
                .min(std::time::Duration::from_secs(1));
        }
    }

    async fn publish_success(
        &self,
        conversation: CustomConversation<S, D, P, T>,
        registration: CustomRouteRegistration<S, D, P, T>,
        admission: HierarchyAdmission,
        ready: tokio::sync::oneshot::Sender<()>,
        claim: tokio::sync::oneshot::Receiver<()>,
    ) -> bool {
        if let Err(error) = admission.publish(Ok(())) {
            self.store_error(Err(error));
            let _ = ready.send(());
            let route_reconciled = self.reconcile_route(registration).await;
            let durable_reconciled = if route_reconciled {
                self.reconcile_durable_provisioning(
                    self.runtime.server.lifecycle_deadline(),
                    CustomDurableProvisioningCleanup::Undelivered,
                )
                .await
            } else {
                false
            };
            drop(conversation);
            return route_reconciled && durable_reconciled;
        }
        *self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(CustomProvisioningResult::Ready {
                conversation: Box::new(conversation),
                registration,
            });
        let observer_ready = ready.send(()).is_ok();
        let claimed = if observer_ready {
            matches!(
                tokio::time::timeout_at(self.runtime.server.lifecycle_deadline(), claim).await,
                Ok(Ok(()))
            )
        } else {
            false
        };
        if claimed {
            return true;
        }
        let pending = self.release_unclaimed_result();
        match pending {
            Some(CustomProvisioningResult::Ready {
                conversation,
                registration,
            }) => {
                let route_published = registration.publish().is_ok();
                drop(conversation);
                route_published
            }
            Some(CustomProvisioningResult::Failed(_)) | None => true,
        }
    }

    fn start(
        workspace: WorkspaceRecord,
        request: NewConversation,
        runtime: CustomWorkspaceRuntime<S, D, P, T>,
        admission: HierarchyAdmission,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<Self>, SdkError> {
        let record = ConversationRecord::new(
            ConversationId::new(),
            workspace.id().clone(),
            workspace.revision(),
            &request,
        );
        runtime
            .state
            .begin_conversation_provisioning(record.workspace_id(), record.id())?;
        let (result_ready, ready_receiver) = tokio::sync::oneshot::channel();
        let (claim_sender, result_claimed) = tokio::sync::oneshot::channel();
        let provisioning = Arc::new(Self {
            workspace,
            record,
            request,
            runtime,
            deadline,
            state: Mutex::new(CustomConversationProvisioningState::Installing),
            result: Mutex::new(None),
            observer: Mutex::new(CustomConversationProvisioningObserver::Available {
                ready: ready_receiver,
                claim: claim_sender,
            }),
        });
        let (start, task_ready) = tokio::sync::oneshot::channel();
        let task = Arc::clone(&provisioning);
        let handle = tokio::spawn(async move {
            let guard = CustomConversationProvisioningGuard {
                owner: Arc::clone(&task),
                state: CustomConversationProvisioningGuardState::Active,
            };
            if task_ready.await.is_err() {
                return;
            }
            let reconciled = match task.execute().await {
                Ok((conversation, registration)) => {
                    task.publish_success(
                        conversation,
                        registration,
                        admission,
                        result_ready,
                        result_claimed,
                    )
                    .await
                }
                Err(failure) => task.publish_failure(failure, admission, result_ready).await,
            };
            guard.complete(reconciled);
        });
        provisioning.install(handle)?;
        start
            .send(())
            .map_err(|_| SdkError::invalid_state("conversation provisioning could not start"))?;
        Ok(provisioning)
    }

    fn install(&self, handle: JoinHandle<()>) -> Result<(), SdkError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !matches!(*state, CustomConversationProvisioningState::Installing) {
            *state = CustomConversationProvisioningState::Poisoned;
            return Err(SdkError::invalid_state(
                "conversation provisioning owner changed during task installation",
            ));
        }
        *state = CustomConversationProvisioningState::Running {
            handle,
            phase: CustomConversationProvisioningPhase::Running,
        };
        Ok(())
    }

    async fn observe(&self) -> Result<CustomConversation<S, D, P, T>, SdkError> {
        self.ensure_retained()?;
        let (ready, claim) = self.begin_observation()?;
        ready.await.map_err(|_| {
            SdkError::integration("conversation provisioning task failed")
                .with_detail(crate::error::ErrorDetail::TaskTerminated)
        })?;
        let result = self.claim_result()?;
        match result {
            CustomProvisioningResult::Ready {
                conversation,
                registration,
            } => {
                registration.publish()?;
                let _ = claim.send(());
                Ok(*conversation)
            }
            CustomProvisioningResult::Failed(error) => {
                let _ = claim.send(());
                Err(error)
            }
        }
    }

    fn ensure_retained(&self) -> Result<(), SdkError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            CustomConversationProvisioningState::Running {
                handle,
                phase:
                    CustomConversationProvisioningPhase::Running
                    | CustomConversationProvisioningPhase::Reconciled,
            } => {
                let _finished = handle.is_finished();
                Ok(())
            }
            CustomConversationProvisioningState::Installing
            | CustomConversationProvisioningState::Running {
                phase: CustomConversationProvisioningPhase::Interrupted,
                ..
            }
            | CustomConversationProvisioningState::Poisoned => Err(SdkError::invalid_state(
                "conversation provisioning task is not retained",
            )),
        }
    }

    fn begin_observation(
        &self,
    ) -> Result<
        (
            tokio::sync::oneshot::Receiver<()>,
            tokio::sync::oneshot::Sender<()>,
        ),
        SdkError,
    > {
        let mut observer = self
            .observer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(
            &mut *observer,
            CustomConversationProvisioningObserver::Claimed,
        );
        match previous {
            CustomConversationProvisioningObserver::Available { ready, claim } => {
                Ok((ready, claim))
            }
            previous => {
                *observer = previous;
                Err(SdkError::invalid_state(
                    "conversation provisioning already has an observer",
                ))
            }
        }
    }

    fn claim_result(&self) -> Result<CustomProvisioningResult<S, D, P, T>, SdkError> {
        self.result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .ok_or_else(|| {
                SdkError::invalid_state("conversation provisioning completed without a result")
            })
    }

    fn release_unclaimed_result(&self) -> Option<CustomProvisioningResult<S, D, P, T>> {
        self.result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomConversationProvisioningGuard<S, D, P, T>
{
    fn complete(mut self, reconciled: bool) {
        if reconciled {
            self.state = CustomConversationProvisioningGuardState::Reconciled;
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Drop
    for CustomConversationProvisioningGuard<S, D, P, T>
{
    fn drop(&mut self) {
        match self.state {
            CustomConversationProvisioningGuardState::Reconciled => {
                self.owner.runtime.state.finish_conversation_provisioning(
                    self.owner.record.workspace_id(),
                    self.owner.record.id(),
                );
            }
            CustomConversationProvisioningGuardState::Active => {
                self.owner
                    .runtime
                    .state
                    .require_conversation_provisioning_recovery(
                        self.owner.record.workspace_id(),
                        self.owner.record.id(),
                    );
            }
        }
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let CustomConversationProvisioningState::Running { phase, .. } = &mut *state
            && matches!(phase, CustomConversationProvisioningPhase::Running)
        {
            *phase = match self.state {
                CustomConversationProvisioningGuardState::Active => {
                    CustomConversationProvisioningPhase::Interrupted
                }
                CustomConversationProvisioningGuardState::Reconciled => {
                    CustomConversationProvisioningPhase::Reconciled
                }
            };
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    WorkspaceBackend for CustomWorkspace<S, D, P, T>
{
    type Conversation = CustomConversation<S, D, P, T>;

    fn id(&self) -> WorkspaceId {
        self.record.id().clone()
    }

    async fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        let admission = self.runtime.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let info = self.current().await?.info()?;
            self.runtime.server.validate_workspace(info.context())?;
            Ok(info)
        }
        .await;
        admission.publish(result)
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<CustomConversation<S, D, P, T>, SdkError> {
        let admission = self.runtime.hierarchy.admit_work(self.record.id())?;
        let deadline = self.runtime.server.lifecycle_deadline();
        let provisioning = CustomConversationProvisioning::start(
            self.record.clone(),
            request,
            self.runtime.clone(),
            admission,
            deadline,
        )?;
        provisioning.observe().await
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, SdkError> {
        let admission = self.runtime.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let workspace = self.current().await?;
            self.runtime
                .state
                .reopenable_conversations(workspace.id())
                .await?
                .into_iter()
                .map(|record| {
                    self.runtime
                        .validate_conversation_record(&workspace, &record)
                })
                .collect()
        }
        .await;
        admission.publish(result)
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, SdkError> {
        let admission = self.runtime.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let workspace = self.current().await?;
            let record = self.runtime.state.conversation(workspace.id(), &id).await?;
            self.runtime
                .validate_conversation_record(&workspace, &record)
        }
        .await;
        admission.publish(result)
    }

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<CustomConversation<S, D, P, T>, SdkError> {
        let admission = self.runtime.hierarchy.admit_work(self.record.id())?;
        let deadline = self.runtime.server.lifecycle_deadline();
        let result = match tokio::time::timeout_at(deadline, async {
            let workspace = self.current().await?;
            let record = self.runtime.state.conversation(workspace.id(), &id).await?;
            self.runtime
                .open_conversation(&workspace, record, deadline)
                .await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(SdkError::timeout("conversation reopen timed out")),
        };
        match result {
            Ok((conversation, registration)) => match admission.publish(Ok(())) {
                Ok(()) => {
                    registration.publish()?;
                    Ok(conversation)
                }
                Err(error) => {
                    drop(registration);
                    drop(conversation);
                    Err(error)
                }
            },
            Err(error) => match admission.publish::<()>(Err(error)) {
                Err(error) => Err(error),
                Ok(()) => Err(SdkError::invalid_state(
                    "conversation reopen failed without an error",
                )),
            },
        }
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, SdkError> {
        let admission = self.runtime.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let workspace = self.current().await?.info()?;
            self.runtime
                .server
                .validate_workspace(workspace.context())?;
            Ok(self.runtime.server.catalog())
        }
        .await;
        admission.publish(result)
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), SdkError> {
        let admission = self.runtime.hierarchy.admit_work(self.record.id())?;
        let result = async {
            let workspace = self.current().await?.info()?;
            self.runtime
                .server
                .validate_workspace(workspace.context())?;
            self.runtime
                .knowledge
                .ingest(
                    self.runtime.knowledge.workspace_owner(self.record.id()),
                    request,
                )
                .await
        }
        .await;
        admission.publish(result)
    }

    async fn close(self) -> Result<(), SdkError> {
        let admission = self.runtime.hierarchy.admit_agent_work()?;
        let deadline = self.runtime.server.lifecycle_deadline();
        let result = match self
            .runtime
            .hierarchy
            .seal_workspace(self.record.id(), self.record.revision())
        {
            Ok(()) => {
                let mut first_error = None;
                self.runtime
                    .server
                    .seal_workspace_before(self.record.id(), deadline);
                if let Err(error) = self
                    .runtime
                    .server
                    .settle_workspace_route_creations_before(self.record.id(), deadline)
                    .await
                {
                    SdkError::record_cleanup(&mut first_error, error);
                }
                if let Err(error) = self
                    .runtime
                    .state
                    .settle_conversation_provisioning_before(self.record.id(), deadline)
                    .await
                {
                    SdkError::record_cleanup(&mut first_error, error);
                }
                let conversations = match tokio::time::timeout_at(
                    deadline,
                    self.runtime.state.conversations(self.record.id()),
                )
                .await
                {
                    Ok(Ok(conversations)) => conversations,
                    Ok(Err(error)) => {
                        SdkError::record_cleanup(&mut first_error, error);
                        Vec::new()
                    }
                    Err(_) => {
                        SdkError::record_cleanup(
                            &mut first_error,
                            SdkError::timeout(
                                "workspace descendants could not be loaded before close",
                            ),
                        );
                        Vec::new()
                    }
                };
                for conversation in &conversations {
                    let terminal = match self
                        .runtime
                        .state
                        .request_cascade_cancellation_before(
                            conversation.clone(),
                            CustomCancellationIntent::WorkspaceClose,
                            deadline,
                        )
                        .await
                    {
                        Ok(CustomCascadeCancellation::Active(generation)) => {
                            let terminal = self.runtime.hierarchy.conversation_is_terminal(
                                conversation.workspace_id(),
                                conversation.id(),
                            );
                            if !terminal
                                && let Err(error) =
                                    self.runtime.server.cancel(conversation.id(), generation)
                            {
                                SdkError::record_cleanup(&mut first_error, error);
                            }
                            terminal
                        }
                        Ok(CustomCascadeCancellation::Quiescent) => {
                            self.runtime.hierarchy.conversation_is_terminal(
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
                        .runtime
                        .requests
                        .cancel_conversation(conversation.id(), deadline, Some(deadline))
                        .await
                    {
                        SdkError::record_cleanup(&mut first_error, error);
                    }
                }
                if let Err(error) = self
                    .runtime
                    .hierarchy
                    .await_workspace_quiescence(self.record.id(), deadline)
                    .await
                {
                    SdkError::record_cleanup(&mut first_error, error);
                }
                for conversation in conversations {
                    let error = match self
                        .runtime
                        .server
                        .remove_route_before(conversation.id(), deadline)
                        .await
                    {
                        Ok(()) => None,
                        Err(error) if error.kind() == crate::error::ErrorKind::Closed => None,
                        Err(error) => Some(error),
                    };
                    if let Some(error) = error {
                        SdkError::record_cleanup(&mut first_error, error);
                    }
                }
                self.runtime
                    .state
                    .notifications()
                    .invalidate_workspace(self.record.id());
                first_error.map_or(Ok(()), Err)
            }
            Err(error) => Err(error),
        };
        admission.publish(result)
    }
}
