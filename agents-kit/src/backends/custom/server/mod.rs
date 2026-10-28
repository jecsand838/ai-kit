use tools_kit::ToolsetBackend;
pub(crate) mod runtime;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use providers_kit::ProviderBackend;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use crate::backends::custom::adapters::files::CustomFileAdapter;
use crate::backends::custom::adapters::{
    CustomProviderAdapter, CustomProviderCall, CustomProviderOperation, CustomProviderRuntime,
};
use crate::backends::custom::server::runtime::{CustomProviderRun, CustomRoute};
use crate::backends::custom::server_requests::CustomServerRequests;
use crate::backends::custom::state_store::CustomBuildClaimEscrow;
use crate::backends::custom::tools::CustomTools;
use crate::{
    ConversationId, CustomAgentConfig, CustomLoopLimits, KitError, ModelInfo, ToolInfo,
    ToolSelection, WorkspaceContext, WorkspaceDefaults, WorkspaceId, WorkspaceNetworkAccess,
    WorkspaceSandboxMode,
};
use storage_kit::{DatabaseBackend, StateStoreBackend};

const MAX_LIVE_ROUTES: usize = 4_096;

#[derive(Clone)]
enum CustomCleanupOutcome {
    Success,
    Failure(KitError),
}

struct CustomServerOperationOwner {
    state: Mutex<CustomServerOperationState>,
    changed: Notify,
}

struct CustomServerCleanupOwner {
    operation: Arc<CustomServerOperationOwner>,
    first_error: Mutex<Option<CustomCleanupOutcome>>,
}

enum CustomServerOperationState {
    Ready,
    Installing,
    Task {
        handle: JoinHandle<()>,
        phase: CustomServerOperationPhase,
    },
    Settled(CustomCleanupOutcome),
    Poisoned,
}

enum CustomServerOperationPhase {
    Running,
    Completed(CustomCleanupOutcome),
    Interrupted,
}

enum CustomServerOperationObservation {
    Complete(CustomCleanupOutcome),
    Notify,
    Yield,
}

struct CustomServerOperationGuard {
    owner: Arc<CustomServerOperationOwner>,
    outcome: Option<CustomCleanupOutcome>,
}

pub(crate) struct CustomServer<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    inner: Arc<CustomServerInner<S, D, P, T>>,
}

pub(crate) struct CustomRouteRegistration<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    state: CustomRouteRegistrationState<S, D, P, T>,
}

enum CustomRouteRegistrationState<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    Creating {
        server: CustomServer<S, D, P, T>,
        creation_id: u64,
        conversation_id: ConversationId,
    },
    Registered {
        server: CustomServer<S, D, P, T>,
        creation_id: u64,
        conversation_id: ConversationId,
        route: Arc<CustomRoute<S, D, P, T>>,
    },
    Released,
}

pub(crate) struct CustomServerPrepublicationGuard<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    state: CustomServerPrepublicationState<S, D, P, T>,
}

enum CustomServerPrepublicationState<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    Guarded {
        server: CustomServer<S, D, P, T>,
        build_claim: Arc<CustomBuildClaimEscrow>,
    },
    Released,
}

enum CustomServerCleanupRetention {
    Runtime,
    BuildClaim(Arc<CustomBuildClaimEscrow>),
}

struct CustomServerInner<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    provider: Mutex<Option<Arc<P>>>,
    routes: Mutex<CustomRouteRegistry<S, D, P, T>>,
    tools: CustomTools<T>,
    requests: CustomServerRequests<S, D, T>,
    config: CustomAgentConfig,
    lifecycle_timeout: Duration,
    cleanup: Arc<CustomServerCleanupOwner>,
    route_creations_changed: Notify,
}

struct CustomRouteRegistry<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    routes: BTreeMap<ConversationId, Arc<CustomRoute<S, D, P, T>>>,
    active_creations: BTreeMap<u64, CustomRouteCreation>,
    next_creation_id: u64,
    sealed_workspaces: BTreeSet<WorkspaceId>,
    sealing: bool,
}

struct CustomRouteCreation {
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    registration_released: bool,
    state: CustomRouteCreationState,
}

enum CustomRouteCreationState {
    Installing,
    Task {
        handle: JoinHandle<()>,
        phase: CustomRouteCreationPhase,
    },
    Failed,
}

enum CustomRouteCreationPhase {
    Running,
    Reconciled,
    Interrupted,
}

struct CustomRouteCreationGuard<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    server: CustomServer<S, D, P, T>,
    creation_id: u64,
    state: CustomRouteCreationGuardState,
}

#[derive(Clone, Copy)]
enum CustomRouteCreationGuardState {
    Active,
    Reconciled,
}

impl CustomCleanupOutcome {
    fn capture(result: Result<(), KitError>) -> Self {
        match result {
            Ok(()) => Self::Success,
            Err(error) => Self::Failure(error),
        }
    }

    fn result(&self) -> Result<(), KitError> {
        match self {
            Self::Success => Ok(()),
            Self::Failure(error) => Err(error.clone()),
        }
    }
}

impl CustomServerOperationOwner {
    fn new() -> Self {
        Self {
            state: Mutex::new(CustomServerOperationState::Ready),
            changed: Notify::new(),
        }
    }

    fn start<F>(self: &Arc<Self>, future: F) -> Result<(), KitError>
    where
        F: Future<Output = Result<(), KitError>> + Send + 'static,
    {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &*state {
                CustomServerOperationState::Ready => {
                    *state = CustomServerOperationState::Installing;
                }
                CustomServerOperationState::Installing
                | CustomServerOperationState::Task { .. }
                | CustomServerOperationState::Settled(_) => return Ok(()),
                CustomServerOperationState::Poisoned => {
                    return Err(KitError::invalid_state(
                        "server operation owner is poisoned",
                    ));
                }
            }
        }
        let owner = Arc::clone(self);
        let (start, ready) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let guard = CustomServerOperationGuard {
                owner,
                outcome: None,
            };
            if ready.await.is_err() {
                return;
            }
            guard.complete(CustomCleanupOutcome::capture(future.await));
        });
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !matches!(*state, CustomServerOperationState::Installing) {
                *state = CustomServerOperationState::Poisoned;
                return Err(KitError::invalid_state(
                    "server operation owner changed during task installation",
                ));
            }
            *state = CustomServerOperationState::Task {
                handle,
                phase: CustomServerOperationPhase::Running,
            };
        }
        self.changed.notify_waiters();
        start
            .send(())
            .map_err(|_| KitError::invalid_state("server operation task could not start"))
    }

    async fn observe_before(&self, deadline: Instant) -> Result<CustomCleanupOutcome, KitError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            match self.observe()? {
                CustomServerOperationObservation::Complete(outcome) => return Ok(outcome),
                CustomServerOperationObservation::Notify => {
                    timeout_at(deadline, changed).await.map_err(|_| {
                        KitError::timeout("server operation timed out")
                            .with_detail(crate::error::ErrorDetail::DeadlineElapsed {
                                kind: crate::error::DeadlineKind::Observation,
                                timeout: None,
                            })
                            .in_phase(crate::error::ErrorPhase::Cleanup)
                    })?;
                }
                CustomServerOperationObservation::Yield => {
                    if Instant::now() >= deadline {
                        return Err(KitError::timeout("server operation timed out")
                            .with_detail(crate::error::ErrorDetail::DeadlineElapsed {
                                kind: crate::error::DeadlineKind::Observation,
                                timeout: None,
                            })
                            .in_phase(crate::error::ErrorPhase::Cleanup));
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    fn observe(&self) -> Result<CustomServerOperationObservation, KitError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            CustomServerOperationState::Ready => {
                Err(KitError::invalid_state("server operation was not started"))
            }
            CustomServerOperationState::Installing => Ok(CustomServerOperationObservation::Notify),
            CustomServerOperationState::Task {
                handle,
                phase: CustomServerOperationPhase::Completed(_),
            } if handle.is_finished() => {
                let previous = std::mem::replace(&mut *state, CustomServerOperationState::Poisoned);
                let CustomServerOperationState::Task {
                    mut handle,
                    phase: CustomServerOperationPhase::Completed(outcome),
                } = previous
                else {
                    return Err(KitError::invalid_state(
                        "server operation owner changed during settlement",
                    ));
                };
                if !Self::join_finished(&mut handle) {
                    return Err(KitError::invalid_state(
                        "server operation task terminated unexpectedly",
                    ));
                }
                *state = CustomServerOperationState::Settled(outcome.clone());
                Ok(CustomServerOperationObservation::Complete(outcome))
            }
            CustomServerOperationState::Task {
                handle,
                phase: CustomServerOperationPhase::Interrupted,
            } if handle.is_finished() => {
                let previous = std::mem::replace(&mut *state, CustomServerOperationState::Poisoned);
                let CustomServerOperationState::Task {
                    mut handle,
                    phase: CustomServerOperationPhase::Interrupted,
                } = previous
                else {
                    return Err(KitError::invalid_state(
                        "server operation owner changed during settlement",
                    ));
                };
                let _ = Self::join_finished(&mut handle);
                Err(KitError::invalid_state(
                    "server operation task terminated unexpectedly",
                ))
            }
            CustomServerOperationState::Task {
                phase:
                    CustomServerOperationPhase::Completed(_) | CustomServerOperationPhase::Interrupted,
                ..
            } => Ok(CustomServerOperationObservation::Yield),
            CustomServerOperationState::Task {
                phase: CustomServerOperationPhase::Running,
                ..
            } => Ok(CustomServerOperationObservation::Notify),
            CustomServerOperationState::Settled(outcome) => {
                Ok(CustomServerOperationObservation::Complete(outcome.clone()))
            }
            CustomServerOperationState::Poisoned => Err(KitError::invalid_state(
                "server operation owner is poisoned",
            )),
        }
    }

    fn finish(&self, outcome: Option<CustomCleanupOutcome>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *state, CustomServerOperationState::Poisoned);
        *state = match previous {
            CustomServerOperationState::Task {
                handle,
                phase: CustomServerOperationPhase::Running,
            } => CustomServerOperationState::Task {
                handle,
                phase: match outcome {
                    Some(outcome) => CustomServerOperationPhase::Completed(outcome),
                    None => CustomServerOperationPhase::Interrupted,
                },
            },
            previous => previous,
        };
        drop(state);
        self.changed.notify_waiters();
    }

    fn join_finished(handle: &mut JoinHandle<()>) -> bool {
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        matches!(Pin::new(handle).poll(&mut context), Poll::Ready(Ok(())))
    }
}

impl CustomServerCleanupOwner {
    fn new() -> Self {
        Self {
            operation: Arc::new(CustomServerOperationOwner::new()),
            first_error: Mutex::new(None),
        }
    }

    fn start<F>(self: &Arc<Self>, future: F) -> Result<(), KitError>
    where
        F: Future<Output = Result<(), KitError>> + Send + 'static,
    {
        self.operation.start(future)
    }

    async fn observe_before(&self, deadline: Instant) -> Result<(), KitError> {
        match self.operation.observe_before(deadline).await {
            Ok(outcome) => outcome.result(),
            Err(observation) => match self.first_error_result() {
                Some(Err(primary)) => Err(primary.with_supplementary(observation)),
                Some(Ok(())) | None => Err(observation),
            },
        }
    }

    fn record_first_error(&self, error: KitError) {
        let mut first_error = self
            .first_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let error = error.in_phase(crate::error::ErrorPhase::Cleanup);
        *first_error = Some(match first_error.take() {
            Some(CustomCleanupOutcome::Failure(primary)) => {
                CustomCleanupOutcome::Failure(primary.with_supplementary(error))
            }
            Some(CustomCleanupOutcome::Success) | None => CustomCleanupOutcome::Failure(error),
        });
    }

    fn first_error_result(&self) -> Option<Result<(), KitError>> {
        self.first_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(CustomCleanupOutcome::result)
    }

    fn result(&self) -> Result<(), KitError> {
        self.first_error_result().unwrap_or(Ok(()))
    }
}

impl CustomServerOperationGuard {
    fn complete(mut self, outcome: CustomCleanupOutcome) {
        self.outcome = Some(outcome);
    }
}

impl Drop for CustomServerOperationGuard {
    fn drop(&mut self) {
        self.owner.finish(self.outcome.take());
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomServerPrepublicationGuard<S, D, P, T>
{
    pub(crate) fn publish(mut self) {
        self.state = CustomServerPrepublicationState::Released;
    }

    pub(crate) fn start_cleanup(&mut self) -> Result<(), KitError> {
        let CustomServerPrepublicationState::Guarded {
            server,
            build_claim,
        } = &self.state
        else {
            return Ok(());
        };
        server.start_cleanup_holding(Arc::clone(build_claim))?;
        self.state = CustomServerPrepublicationState::Released;
        Ok(())
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Drop
    for CustomServerPrepublicationGuard<S, D, P, T>
{
    fn drop(&mut self) {
        if let CustomServerPrepublicationState::Guarded {
            server,
            build_claim,
        } = &self.state
        {
            let _ = server.start_cleanup_holding(Arc::clone(build_claim));
        }
        self.state = CustomServerPrepublicationState::Released;
    }
}

impl CustomServerCleanupRetention {
    fn release(self) {
        match self {
            Self::Runtime => {}
            Self::BuildClaim(build_claim) => drop(build_claim),
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomRouteCreationGuard<S, D, P, T>
{
    fn new(server: CustomServer<S, D, P, T>, creation_id: u64) -> Self {
        Self {
            server,
            creation_id,
            state: CustomRouteCreationGuardState::Active,
        }
    }

    fn complete(mut self) {
        self.state = CustomRouteCreationGuardState::Reconciled;
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Drop
    for CustomRouteCreationGuard<S, D, P, T>
{
    fn drop(&mut self) {
        self.server
            .finish_route_creation_task(self.creation_id, self.state);
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomRouteRegistration<S, D, P, T>
{
    fn creating(
        server: CustomServer<S, D, P, T>,
        creation_id: u64,
        conversation_id: ConversationId,
    ) -> Self {
        Self {
            state: CustomRouteRegistrationState::Creating {
                server,
                creation_id,
                conversation_id,
            },
        }
    }

    fn register(mut self, route: Arc<CustomRoute<S, D, P, T>>) -> Result<Self, KitError> {
        let state = std::mem::replace(&mut self.state, CustomRouteRegistrationState::Released);
        match state {
            CustomRouteRegistrationState::Creating {
                server,
                creation_id,
                conversation_id,
            } => {
                self.state = CustomRouteRegistrationState::Registered {
                    server,
                    creation_id,
                    conversation_id,
                    route,
                };
                Ok(self)
            }
            CustomRouteRegistrationState::Registered {
                server,
                creation_id,
                route: previous,
                ..
            } => {
                previous.seal(server.lifecycle_deadline());
                route.seal(server.lifecycle_deadline());
                server.release_route_creation_registration(creation_id);
                Err(KitError::invalid_state(
                    "route registration changed before installation completed",
                ))
            }
            CustomRouteRegistrationState::Released => Err(KitError::invalid_state(
                "route registration changed before installation completed",
            )),
        }
    }

    pub(crate) fn publish(mut self) -> Result<(), KitError> {
        let state = std::mem::replace(&mut self.state, CustomRouteRegistrationState::Released);
        match state {
            CustomRouteRegistrationState::Registered {
                server,
                creation_id,
                ..
            } => {
                server.release_route_creation_registration(creation_id);
                Ok(())
            }
            CustomRouteRegistrationState::Creating {
                server,
                creation_id,
                ..
            } => {
                server.release_route_creation_registration(creation_id);
                Err(KitError::invalid_state(
                    "route registration is not ready for publication",
                ))
            }
            CustomRouteRegistrationState::Released => Err(KitError::invalid_state(
                "route registration is not ready for publication",
            )),
        }
    }

    pub(crate) async fn settle_shutdown(&self) -> Result<(), KitError> {
        match &self.state {
            CustomRouteRegistrationState::Registered { route, .. } => route.settle_shutdown().await,
            CustomRouteRegistrationState::Creating { .. }
            | CustomRouteRegistrationState::Released => Err(KitError::invalid_state(
                "route registration is not available for shutdown",
            )),
        }
    }

    pub(crate) fn detach(mut self) -> Result<(), KitError> {
        let state = std::mem::replace(&mut self.state, CustomRouteRegistrationState::Released);
        let (server, creation_id, conversation_id, route) = match state {
            CustomRouteRegistrationState::Registered {
                server,
                creation_id,
                conversation_id,
                route,
            } => (server, creation_id, conversation_id, route),
            CustomRouteRegistrationState::Creating {
                server,
                creation_id,
                ..
            } => {
                server.release_route_creation_registration(creation_id);
                return Err(KitError::invalid_state(
                    "route registration is not available for detachment",
                ));
            }
            CustomRouteRegistrationState::Released => {
                return Err(KitError::invalid_state(
                    "route registration is not available for detachment",
                ));
            }
        };
        let removal_result = server.remove_registered_route(
            &conversation_id,
            &route,
            "conversation execution changed during provisioning cleanup",
        );
        server.release_route_creation_registration(creation_id);
        removal_result
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Drop
    for CustomRouteRegistration<S, D, P, T>
{
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, CustomRouteRegistrationState::Released);
        match state {
            CustomRouteRegistrationState::Creating {
                server,
                creation_id,
                ..
            } => server.release_route_creation_registration(creation_id),
            CustomRouteRegistrationState::Registered {
                server,
                creation_id,
                route,
                ..
            } => {
                route.seal(server.lifecycle_deadline());
                server.release_route_creation_registration(creation_id);
            }
            CustomRouteRegistrationState::Released => {}
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomRouteRegistry<S, D, P, T>
{
    fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
            active_creations: BTreeMap::new(),
            next_creation_id: 1,
            sealed_workspaces: BTreeSet::new(),
            sealing: false,
        }
    }

    fn route(&self, conversation_id: &ConversationId) -> Option<Arc<CustomRoute<S, D, P, T>>> {
        self.routes.get(conversation_id).cloned()
    }

    fn begin_creation(
        &mut self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<u64, KitError> {
        self.reap_finished_creations();
        self.ensure_workspace_open(workspace_id)?;
        if self
            .active_creations
            .values()
            .any(|creation| &creation.conversation_id == conversation_id)
        {
            return Err(KitError::busy(
                "conversation route creation is already active",
            ));
        }
        let reserved_routes = self
            .active_creations
            .values()
            .filter(|creation| !self.routes.contains_key(&creation.conversation_id))
            .count();
        if !self.routes.contains_key(conversation_id)
            && self.routes.len().saturating_add(reserved_routes) >= MAX_LIVE_ROUTES
        {
            return Err(KitError::busy(
                "live conversation execution capacity is saturated",
            ));
        }
        let creation_id = self.next_creation_id;
        self.next_creation_id = self
            .next_creation_id
            .checked_add(1)
            .ok_or_else(|| KitError::invalid_state("route creation identity exhausted"))?;
        self.active_creations.insert(
            creation_id,
            CustomRouteCreation {
                workspace_id: workspace_id.clone(),
                conversation_id: conversation_id.clone(),
                registration_released: false,
                state: CustomRouteCreationState::Installing,
            },
        );
        Ok(creation_id)
    }

    fn install_creation(
        &mut self,
        creation_id: u64,
        handle: JoinHandle<()>,
    ) -> Result<(), KitError> {
        let Some(creation) = self.active_creations.get_mut(&creation_id) else {
            handle.abort();
            return Err(KitError::invalid_state("route creation owner is missing"));
        };
        if !matches!(creation.state, CustomRouteCreationState::Installing) {
            handle.abort();
            creation.state = CustomRouteCreationState::Failed;
            return Err(KitError::invalid_state(
                "route creation owner changed during task installation",
            ));
        }
        creation.state = CustomRouteCreationState::Task {
            handle,
            phase: CustomRouteCreationPhase::Running,
        };
        Ok(())
    }

    fn finish_creation_task(&mut self, creation_id: u64, state: CustomRouteCreationGuardState) {
        if let Some(creation) = self.active_creations.get_mut(&creation_id) {
            let previous = std::mem::replace(&mut creation.state, CustomRouteCreationState::Failed);
            creation.state = match previous {
                CustomRouteCreationState::Task {
                    handle,
                    phase: CustomRouteCreationPhase::Running,
                } => CustomRouteCreationState::Task {
                    handle,
                    phase: match state {
                        CustomRouteCreationGuardState::Active => {
                            CustomRouteCreationPhase::Interrupted
                        }
                        CustomRouteCreationGuardState::Reconciled => {
                            CustomRouteCreationPhase::Reconciled
                        }
                    },
                },
                previous => previous,
            };
        }
        self.reap_finished_creations();
    }

    fn release_creation_registration(&mut self, creation_id: u64) {
        if let Some(creation) = self.active_creations.get_mut(&creation_id) {
            creation.registration_released = true;
        }
        self.reap_finished_creations();
    }

    fn creations_settled(&mut self, workspace_id: Option<&WorkspaceId>) -> Result<bool, KitError> {
        self.reap_finished_creations();
        let failed = self
            .active_creations
            .iter()
            .filter(|(_, creation)| {
                workspace_id.is_none_or(|expected| &creation.workspace_id == expected)
                    && matches!(creation.state, CustomRouteCreationState::Failed)
            })
            .map(|(creation_id, _)| *creation_id)
            .collect::<Vec<_>>();
        if !failed.is_empty() {
            for creation_id in failed {
                self.active_creations.remove(&creation_id);
            }
            return Err(KitError::invalid_state(
                "conversation route creation task terminated unexpectedly",
            ));
        }
        Ok(self.active_creations.values().all(|creation| {
            workspace_id.is_some_and(|expected| &creation.workspace_id != expected)
        }))
    }

    fn creations_finishing(&self, workspace_id: Option<&WorkspaceId>) -> bool {
        self.active_creations.values().any(|creation| {
            workspace_id.is_none_or(|expected| &creation.workspace_id == expected)
                && matches!(
                    creation.state,
                    CustomRouteCreationState::Task {
                        phase: CustomRouteCreationPhase::Reconciled
                            | CustomRouteCreationPhase::Interrupted,
                        ..
                    }
                )
        })
    }

    fn reap_finished_creations(&mut self) {
        let finished = self
            .active_creations
            .iter()
            .filter_map(|(creation_id, creation)| match &creation.state {
                CustomRouteCreationState::Task {
                    handle,
                    phase: CustomRouteCreationPhase::Reconciled,
                } if handle.is_finished() => {
                    Some((*creation_id, true, creation.registration_released))
                }
                CustomRouteCreationState::Task {
                    handle,
                    phase: CustomRouteCreationPhase::Interrupted,
                } if handle.is_finished() => {
                    Some((*creation_id, false, creation.registration_released))
                }
                CustomRouteCreationState::Installing
                | CustomRouteCreationState::Task { .. }
                | CustomRouteCreationState::Failed => None,
            })
            .collect::<Vec<_>>();
        for (creation_id, reconciled, registration_released) in finished {
            if reconciled && registration_released {
                self.active_creations.remove(&creation_id);
            } else if !reconciled
                && let Some(creation) = self.active_creations.get_mut(&creation_id)
            {
                creation.state = CustomRouteCreationState::Failed;
            }
        }
    }

    fn ensure_workspace_open(&self, workspace_id: &WorkspaceId) -> Result<(), KitError> {
        if self.sealing || self.sealed_workspaces.contains(workspace_id) {
            Err(KitError::closed("workspace runtime is closed"))
        } else {
            Ok(())
        }
    }

    fn ensure_capacity(&self, workspace_id: &WorkspaceId) -> Result<(), KitError> {
        self.ensure_workspace_open(workspace_id)?;
        if self.routes.len() >= MAX_LIVE_ROUTES {
            Err(KitError::busy(
                "live conversation execution capacity is saturated",
            ))
        } else {
            Ok(())
        }
    }

    fn insert(
        &mut self,
        workspace_id: &WorkspaceId,
        conversation_id: ConversationId,
        route: Arc<CustomRoute<S, D, P, T>>,
    ) -> Result<(), KitError> {
        self.ensure_capacity(workspace_id)?;
        match self.routes.entry(conversation_id) {
            Entry::Vacant(entry) => {
                entry.insert(route);
                Ok(())
            }
            Entry::Occupied(_) => Err(KitError::busy(
                "conversation execution was claimed concurrently",
            )),
        }
    }

    fn replace_current(
        &mut self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
        expected: &Arc<CustomRoute<S, D, P, T>>,
        route: Arc<CustomRoute<S, D, P, T>>,
    ) -> Result<(), KitError> {
        self.ensure_workspace_open(workspace_id)?;
        match self.routes.entry(conversation_id.clone()) {
            Entry::Occupied(mut entry) if Arc::ptr_eq(entry.get(), expected) => {
                entry.insert(route);
                Ok(())
            }
            Entry::Occupied(_) => Err(KitError::invalid_state(
                "conversation execution changed during reopen",
            )),
            Entry::Vacant(_) => Err(KitError::invalid_state(
                "conversation execution disappeared during reopen",
            )),
        }
    }

    fn remove_current(
        &mut self,
        conversation_id: &ConversationId,
        expected: &Arc<CustomRoute<S, D, P, T>>,
        changed_message: &'static str,
    ) -> Result<(), KitError> {
        match self.routes.get(conversation_id) {
            Some(current) if Arc::ptr_eq(current, expected) => {
                self.routes.remove(conversation_id);
                Ok(())
            }
            None => Ok(()),
            Some(_) => Err(KitError::invalid_state(changed_message)),
        }
    }

    fn seal_workspace(&mut self, workspace_id: &WorkspaceId, deadline: Instant) {
        self.sealed_workspaces.insert(workspace_id.clone());
        for route in self.routes.values() {
            if route.belongs_to(workspace_id) {
                route.seal(deadline);
            }
        }
    }

    fn reopen_workspace(&mut self, workspace_id: &WorkspaceId) -> Result<(), KitError> {
        if self.sealing {
            return Err(KitError::closed("agent runtime is closed"));
        }
        self.sealed_workspaces.remove(workspace_id);
        Ok(())
    }

    fn seal_all(&mut self, deadline: Instant) {
        self.sealing = true;
        for route in self.routes.values() {
            route.seal(deadline);
        }
    }

    fn snapshot(&self) -> BTreeMap<ConversationId, Arc<CustomRoute<S, D, P, T>>> {
        self.routes.clone()
    }

    fn clear_unchanged(
        &mut self,
        expected: &BTreeMap<ConversationId, Arc<CustomRoute<S, D, P, T>>>,
    ) -> Result<(), KitError> {
        let unchanged = self.routes.len() == expected.len()
            && expected.iter().all(|(id, route)| {
                self.routes
                    .get(id)
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, route))
            });
        if !unchanged {
            return Err(KitError::invalid_state(
                "conversation execution inventory changed during shutdown",
            ));
        }
        self.routes.clear();
        Ok(())
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Clone
    for CustomServer<S, D, P, T>
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomServer<S, D, P, T>
{
    pub(crate) fn build(
        provider: Arc<P>,
        tools: CustomTools<T>,
        requests: CustomServerRequests<S, D, T>,
        config: CustomAgentConfig,
        lifecycle_timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(CustomServerInner {
                provider: Mutex::new(Some(provider)),
                routes: Mutex::new(CustomRouteRegistry::new()),
                tools,
                requests,
                config,
                lifecycle_timeout,
                cleanup: Arc::new(CustomServerCleanupOwner::new()),
                route_creations_changed: Notify::new(),
            }),
        }
    }

    pub(crate) fn prepublication_guard(
        &self,
        build_claim: Arc<CustomBuildClaimEscrow>,
    ) -> CustomServerPrepublicationGuard<S, D, P, T> {
        CustomServerPrepublicationGuard {
            state: CustomServerPrepublicationState::Guarded {
                server: self.clone(),
                build_claim,
            },
        }
    }

    pub(crate) async fn create_route(
        &self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        deadline: Instant,
    ) -> Result<CustomRouteRegistration<S, D, P, T>, KitError> {
        let creation_id = {
            let mut routes = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            routes.begin_creation(&workspace_id, &conversation_id)?
        };
        self.inner.route_creations_changed.notify_waiters();
        let registration =
            CustomRouteRegistration::creating(self.clone(), creation_id, conversation_id.clone());
        let server = self.clone();
        let result = Arc::new(Mutex::new(None));
        let task_result = Arc::clone(&result);
        let (ready, observer) = oneshot::channel();
        let (claim, claimed) = oneshot::channel();
        let (start, started) = oneshot::channel();
        let guard = CustomRouteCreationGuard::new(server.clone(), creation_id);
        let creation_task = tokio::spawn(async move {
            let guard = guard;
            if started.await.is_err() {
                return;
            }
            let installed = match server.install_route(workspace_id, conversation_id).await {
                Ok(route) => registration.register(route),
                Err(error) => Err(error),
            };
            *task_result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(installed);
            let observer_ready = ready.send(()).is_ok();
            let claimed = observer_ready && claimed.await.is_ok();
            let reconciled = if claimed {
                true
            } else {
                let unclaimed = task_result
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take();
                match unclaimed {
                    Some(Ok(registration)) => {
                        if registration.settle_shutdown().await.is_err() {
                            false
                        } else {
                            registration.detach().is_ok()
                        }
                    }
                    Some(Err(_)) | None => false,
                }
            };
            if reconciled {
                guard.complete();
            }
        });
        self.install_route_creation_task(creation_id, creation_task)?;
        start
            .send(())
            .map_err(|_| KitError::invalid_state("conversation route creation could not start"))?;
        timeout_at(deadline, observer)
            .await
            .map_err(|_| KitError::timeout("conversation route creation timed out"))?
            .map_err(|_| {
                KitError::integration("conversation route creation task failed")
                    .with_detail(crate::error::ErrorDetail::TaskTerminated)
            })?;
        let installed = result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .ok_or_else(|| {
                KitError::invalid_state("conversation route creation completed without a result")
            })?;
        let _ = claim.send(());
        installed
    }

    async fn install_route(
        &self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Result<Arc<CustomRoute<S, D, P, T>>, KitError> {
        let existing = self
            .inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .route(&conversation_id);
        if existing.is_none() {
            self.inner
                .routes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .ensure_capacity(&workspace_id)?;
        }
        if let Some(existing) = existing.as_ref() {
            existing.prepare_reopen(&workspace_id, self.lifecycle_deadline())?;
            existing.settle_shutdown().await?;
        }
        let route = Arc::new(CustomRoute::new(
            workspace_id.clone(),
            CustomProviderRuntime::new(
                self.provider()?,
                self.inner.tools.clone(),
                self.inner.requests.clone(),
                self.inner.config.tool_approval(),
            ),
        ));
        let insertion = {
            let mut routes = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match existing.as_ref() {
                Some(existing) => routes.replace_current(
                    &workspace_id,
                    &conversation_id,
                    existing,
                    Arc::clone(&route),
                ),
                None => routes.insert(&workspace_id, conversation_id, Arc::clone(&route)),
            }
        };
        if let Err(error) = insertion {
            let _ = route.settle_shutdown().await;
            return Err(error);
        }
        Ok(route)
    }

    pub(crate) async fn await_route_result<O, F>(
        &self,
        conversation_id: &ConversationId,
        future: F,
    ) -> Result<O, KitError>
    where
        F: std::future::Future<Output = Result<O, KitError>>,
    {
        self.route(conversation_id)?.await_result(future).await
    }

    pub(crate) fn seal_route_before(
        &self,
        conversation_id: &ConversationId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        self.route(conversation_id)?.seal(deadline);
        Ok(())
    }

    pub(crate) fn seal_workspace_before(&self, workspace_id: &WorkspaceId, deadline: Instant) {
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .seal_workspace(workspace_id, deadline);
    }

    pub(crate) fn reopen_workspace(&self, workspace_id: &WorkspaceId) -> Result<(), KitError> {
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .reopen_workspace(workspace_id)
    }

    pub(crate) async fn settle_workspace_route_creations_before(
        &self,
        workspace_id: &WorkspaceId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        self.settle_route_creations_before(Some(workspace_id), Some(deadline))
            .await
    }

    pub(crate) fn seal_all_before(&self, deadline: Instant) {
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .seal_all(deadline);
    }

    pub(crate) fn spawn_prompt<F>(
        &self,
        conversation_id: &ConversationId,
        future: F,
    ) -> Result<tokio::sync::oneshot::Receiver<()>, KitError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.route(conversation_id)?.spawn(future)
    }

    pub(crate) fn acknowledge_prompt(
        &self,
        conversation_id: &ConversationId,
        generation: Option<u64>,
    ) -> Result<(), KitError> {
        self.route(conversation_id)?.acknowledge_prompt(generation)
    }

    pub(crate) async fn await_prompt_settlement_before(
        &self,
        conversation_id: &ConversationId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        let route = self
            .inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .route(conversation_id);
        match route {
            Some(route) => route.await_prompt_settlement(deadline).await,
            None => Ok(()),
        }
    }

    pub(crate) fn bind_prompt(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
    ) -> Result<(), KitError> {
        self.route(conversation_id)?.bind_task(generation)
    }

    pub(crate) fn start(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
        call: CustomProviderCall,
    ) -> Result<CustomProviderRun<S, D, P, T>, KitError> {
        self.route(conversation_id)?.start(generation, call)
    }

    pub(crate) fn decline(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
    ) -> Result<(), KitError> {
        self.route(conversation_id)?.decline(generation)
    }

    pub(crate) fn cancel(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
    ) -> Result<bool, KitError> {
        self.route(conversation_id)?.cancel(generation)
    }

    pub(crate) async fn remove_route_before(
        &self,
        conversation_id: &ConversationId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        let route = self
            .inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .route(conversation_id)
            .ok_or_else(|| KitError::closed("conversation execution is unavailable"))?;
        route.shutdown(deadline).await?;
        let mut routes = self
            .inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        routes.remove_current(
            conversation_id,
            &route,
            "conversation execution changed during close",
        )
    }

    fn remove_registered_route(
        &self,
        conversation_id: &ConversationId,
        route: &Arc<CustomRoute<S, D, P, T>>,
        changed_message: &'static str,
    ) -> Result<(), KitError> {
        match self
            .inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove_current(conversation_id, route, changed_message)
        {
            Err(error) if error.kind() == crate::error::ErrorKind::InvalidState => {
                Err(KitError::conflict(changed_message))
            }
            result => result,
        }
    }

    pub(crate) async fn models(&self) -> Result<Vec<ModelInfo>, KitError> {
        let provider = self.provider()?;
        CustomProviderAdapter::models(provider.as_ref()).await
    }

    pub(crate) fn validate_workspace(&self, context: &WorkspaceContext) -> Result<(), KitError> {
        CustomFileAdapter::validate_workspace_path_flavor(context.path_flavor())?;
        let exposes_unconfined_action = self.inner.tools.has_actions();
        if exposes_unconfined_action
            && (context.execution_policy().sandbox_mode() != WorkspaceSandboxMode::FullAccess
                || context.execution_policy().network_access() != WorkspaceNetworkAccess::Enabled)
        {
            return Err(KitError::unsupported(
                "configured actions require full filesystem and network access",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_defaults(
        &self,
        defaults: &WorkspaceDefaults,
        operation: CustomProviderOperation,
    ) -> Result<(), KitError> {
        if defaults.personality().is_some() {
            return Err(KitError::unsupported("personality is unsupported"));
        }
        self.model_selection(defaults.model(), operation)?;
        CustomProviderAdapter::validate_reasoning(defaults.reasoning_effort())
    }

    pub(crate) fn validate_conversation_defaults(
        &self,
        workspace: &WorkspaceDefaults,
        conversation: Option<&WorkspaceDefaults>,
        operation: CustomProviderOperation,
    ) -> Result<(), KitError> {
        let model = conversation
            .and_then(WorkspaceDefaults::model)
            .or_else(|| workspace.model());
        let personality = conversation
            .and_then(WorkspaceDefaults::personality)
            .or_else(|| workspace.personality());
        let reasoning = conversation
            .and_then(WorkspaceDefaults::reasoning_effort)
            .or_else(|| workspace.reasoning_effort());
        if personality.is_some() {
            return Err(KitError::unsupported("personality is unsupported"));
        }
        self.model_selection(model, operation)?;
        CustomProviderAdapter::validate_reasoning(reasoning)
    }

    pub(crate) fn model_selection(
        &self,
        selector: Option<&crate::ModelSelector>,
        operation: CustomProviderOperation,
    ) -> Result<providers_kit::ModelId, KitError> {
        let provider = self.provider()?;
        CustomProviderAdapter::validate_model(
            provider.as_ref(),
            selector.unwrap_or_else(|| self.inner.config.default_model()),
            operation,
        )
    }

    pub(crate) fn tool_selection(
        &self,
        requested: Option<&ToolSelection>,
    ) -> Result<ToolSelection, KitError> {
        let selection = self
            .inner
            .tools
            .select(requested, self.inner.config.default_tool_selection())?;
        if matches!(selection, ToolSelection::Required) && !self.inner.tools.has_actions() {
            return Err(KitError::invalid_input(
                "required tool selection needs at least one configured tool",
            ));
        }
        Ok(selection)
    }

    pub(crate) fn loop_limits(&self) -> CustomLoopLimits {
        self.inner.config.loop_limits()
    }

    pub(crate) fn catalog(&self) -> Vec<ToolInfo> {
        self.inner.tools.catalog()
    }

    pub(crate) fn lifecycle_deadline(&self) -> Instant {
        let now = Instant::now();
        now.checked_add(self.inner.lifecycle_timeout).unwrap_or(now)
    }

    pub(crate) fn lifecycle_timeout(&self) -> Duration {
        self.inner.lifecycle_timeout
    }

    fn install_route_creation_task(
        &self,
        creation_id: u64,
        handle: JoinHandle<()>,
    ) -> Result<(), KitError> {
        let result = self
            .inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .install_creation(creation_id, handle);
        self.inner.route_creations_changed.notify_waiters();
        result
    }

    fn finish_route_creation_task(&self, creation_id: u64, state: CustomRouteCreationGuardState) {
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .finish_creation_task(creation_id, state);
        self.inner.route_creations_changed.notify_waiters();
    }

    fn release_route_creation_registration(&self, creation_id: u64) {
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .release_creation_registration(creation_id);
        self.inner.route_creations_changed.notify_waiters();
    }

    async fn settle_route_creations_before(
        &self,
        workspace_id: Option<&WorkspaceId>,
        deadline: Option<Instant>,
    ) -> Result<(), KitError> {
        loop {
            let mut changed = std::pin::pin!(self.inner.route_creations_changed.notified());
            changed.as_mut().enable();
            let settled = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .creations_settled(workspace_id)?;
            if settled {
                return Ok(());
            }
            let finishing = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .creations_finishing(workspace_id);
            if finishing {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Err(KitError::busy(
                        "conversation route creation is still active",
                    ));
                }
                tokio::task::yield_now().await;
                continue;
            }
            match deadline {
                Some(deadline) => {
                    timeout_at(deadline, changed).await.map_err(|_| {
                        KitError::busy("conversation route creation is still active")
                    })?;
                }
                None => changed.await,
            }
        }
    }

    pub(crate) async fn shutdown(&self) -> Result<(), KitError> {
        let deadline = self.lifecycle_deadline();
        self.start_cleanup()?;
        self.inner.cleanup.observe_before(deadline).await
    }

    fn start_cleanup(&self) -> Result<(), KitError> {
        self.start_cleanup_with(CustomServerCleanupRetention::Runtime)
    }

    fn start_cleanup_holding(
        &self,
        build_claim: Arc<CustomBuildClaimEscrow>,
    ) -> Result<(), KitError> {
        self.start_cleanup_with(CustomServerCleanupRetention::BuildClaim(build_claim))
    }

    fn start_cleanup_with(&self, retention: CustomServerCleanupRetention) -> Result<(), KitError> {
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .seal_all(self.lifecycle_deadline());
        let server = self.clone();
        let cleanup = Arc::clone(&self.inner.cleanup);
        let task_cleanup = Arc::clone(&cleanup);
        cleanup.start(async move {
            let result = server.settle_cleanup(task_cleanup).await;
            retention.release();
            result
        })
    }

    async fn settle_cleanup(&self, cleanup: Arc<CustomServerCleanupOwner>) -> Result<(), KitError> {
        if let Err(error) = self.settle_route_creations_before(None, None).await {
            cleanup.record_first_error(error);
        }
        let routes = self
            .inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot();
        for route in routes.values() {
            if let Err(error) = route.settle_shutdown().await {
                cleanup.record_first_error(error);
            }
        }
        {
            let mut current = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Err(error) = current.clear_unchanged(&routes) {
                cleanup.record_first_error(error);
            }
        }

        if let Err(error) = self.inner.tools.clone().shutdown().await {
            cleanup.record_first_error(error);
        }

        let provider = self
            .inner
            .provider
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(provider) = provider
            && let Err(error) = provider.shutdown().await
        {
            cleanup.record_first_error(CustomProviderAdapter::map_shutdown_error(error));
        }

        cleanup.result()
    }

    fn provider(&self) -> Result<Arc<P>, KitError> {
        self.inner
            .provider
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| KitError::closed("model runtime is closed"))
    }

    fn route(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Arc<CustomRoute<S, D, P, T>>, KitError> {
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .route(conversation_id)
            .ok_or_else(|| KitError::closed("conversation execution is unavailable"))
    }
}
