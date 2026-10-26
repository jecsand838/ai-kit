use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use agent_sdk::AgentBackend;
use storage_sdk::StateStoreBackend;

use crate::workflow::{
    ActionBackend, ChildExecutor, ConditionBackend, LifecycleHooks, StateTyping,
};
use tokio::sync::{OwnedSemaphorePermit, watch};

use super::{
    MAX_WORKSPACES, WorkspaceAdmission, WorkspaceConfigRecord, WorkspaceRecord, WorkspaceRuntime,
    WorkspaceStateRecord,
};
use crate::app::FinalResult;
use crate::storage::AppMutation;
use crate::{AppError, WorkspaceId};

pub(super) enum WorkspaceOperation<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    Create {
        owner: WorkspaceRuntime<CS, A, X, C, T, H, E>,
        record: WorkspaceRecord,
    },
    Advance {
        owner: WorkspaceRuntime<CS, A, X, C, T, H, E>,
        fence: u64,
        storage_revision: u64,
        source: Box<WorkspaceRecord>,
        intended: WorkspaceRecord,
    },
    Delete {
        owner: WorkspaceRuntime<CS, A, X, C, T, H, E>,
        fence: u64,
        storage_revision: u64,
        source: WorkspaceRecord,
    },
    Recover {
        owner: WorkspaceRuntime<CS, A, X, C, T, H, E>,
        fence: u64,
    },
}

pub(crate) struct WorkspaceOperations {
    state: Mutex<WorkspaceOperationsState>,
}

enum WorkspaceOperationsState {
    Open(BTreeMap<WorkspaceId, WorkspaceObserver>),
    Closed,
}

#[derive(PartialEq)]
enum WorkspaceOperationIntent {
    Create {
        config: WorkspaceConfigRecord,
        targets: Vec<(String, String)>,
    },
    Replace {
        revision: u64,
        config: WorkspaceConfigRecord,
    },
    Delete,
}

#[derive(PartialEq)]
pub(crate) struct WorkspaceOperationIdentity {
    fence: u64,
    intent: WorkspaceOperationIntent,
}

struct WorkspaceObserver {
    identity: Arc<WorkspaceOperationIdentity>,
    completion: watch::Sender<Option<FinalResult>>,
}

pub(super) enum WorkspaceRegistration {
    Observing(watch::Receiver<Option<FinalResult>>),
    Starting {
        completion: watch::Receiver<Option<FinalResult>>,
        admission: Option<OwnedSemaphorePermit>,
    },
}

impl WorkspaceObserver {
    fn ensure_matches(&self, requested: &WorkspaceOperationIdentity) -> Result<(), AppError> {
        if self.identity.fence != requested.fence {
            return Err(AppError::Conflict(
                "workspace operation fence differs from its active owner".to_owned(),
            ));
        }
        if self.identity.intent != requested.intent {
            return Err(AppError::Conflict(
                "workspace operation intent differs from its active owner".to_owned(),
            ));
        }
        Ok(())
    }
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> WorkspaceOperation<CS, A, X, C, T, H, E>
{
    pub(super) fn create(
        owner: WorkspaceRuntime<CS, A, X, C, T, H, E>,
        record: WorkspaceRecord,
    ) -> Self {
        Self::Create { owner, record }
    }

    pub(super) fn advance(
        owner: WorkspaceRuntime<CS, A, X, C, T, H, E>,
        fence: u64,
        storage_revision: u64,
        source: WorkspaceRecord,
        intended: WorkspaceRecord,
    ) -> Self {
        Self::Advance {
            owner,
            fence,
            storage_revision,
            source: Box::new(source),
            intended,
        }
    }

    pub(super) fn delete(
        owner: WorkspaceRuntime<CS, A, X, C, T, H, E>,
        fence: u64,
        storage_revision: u64,
        source: WorkspaceRecord,
    ) -> Self {
        Self::Delete {
            owner,
            fence,
            storage_revision,
            source,
        }
    }

    pub(super) fn recover(owner: WorkspaceRuntime<CS, A, X, C, T, H, E>, fence: u64) -> Self {
        Self::Recover { owner, fence }
    }

    pub(super) fn workspace(&self) -> &WorkspaceId {
        match self {
            Self::Create { owner, .. }
            | Self::Advance { owner, .. }
            | Self::Delete { owner, .. }
            | Self::Recover { owner, .. } => owner.id(),
        }
    }

    fn fence(&self) -> u64 {
        match self {
            Self::Create { record, .. } => record.revision,
            Self::Advance { fence, .. }
            | Self::Delete { fence, .. }
            | Self::Recover { fence, .. } => *fence,
        }
    }

    pub(super) fn identity(&self) -> Result<Arc<WorkspaceOperationIdentity>, AppError> {
        let intent = match self {
            Self::Create { record, .. } => {
                let WorkspaceStateRecord::Creating { config, targets } = &record.state else {
                    return Err(AppError::Integration(
                        "workspace creation intent is invalid".to_owned(),
                    ));
                };
                WorkspaceOperationIntent::Create {
                    config: config.clone(),
                    targets: targets
                        .iter()
                        .flat_map(|(key, revisions)| {
                            revisions
                                .keys()
                                .map(move |revision| (key.clone(), revision.clone()))
                        })
                        .collect(),
                }
            }
            Self::Advance { intended, .. } => match &intended.state {
                WorkspaceStateRecord::Replacing {
                    next,
                    next_revision,
                    ..
                } => WorkspaceOperationIntent::Replace {
                    revision: *next_revision,
                    config: next.clone(),
                },
                WorkspaceStateRecord::Creating { config, targets } => {
                    WorkspaceOperationIntent::Create {
                        config: config.clone(),
                        targets: targets
                            .iter()
                            .flat_map(|(key, revisions)| {
                                revisions
                                    .keys()
                                    .map(move |revision| (key.clone(), revision.clone()))
                            })
                            .collect(),
                    }
                }
                _ => {
                    return Err(AppError::Integration(
                        "workspace advance intent is invalid".into(),
                    ));
                }
            },
            Self::Delete { .. } | Self::Recover { .. } => WorkspaceOperationIntent::Delete,
        };
        Ok(Arc::new(WorkspaceOperationIdentity {
            fence: self.fence(),
            intent,
        }))
    }

    pub(super) async fn execute(self) -> Result<(), AppError> {
        match self {
            Self::Create { owner, record } => {
                let (app_record, app_revision) = owner.app.admission_record().await?;
                owner.app.validate_workspace(&record, owner.id()).await?;
                let workspaces = owner.app.storage().list_workspaces().await?;
                for workspace in &workspaces {
                    owner
                        .app
                        .validate_workspace(workspace, &workspace.id)
                        .await?;
                }
                if workspaces.len() >= MAX_WORKSPACES {
                    return Err(AppError::Busy("workspace capacity is exhausted".to_owned()));
                }
                let mutation = AppMutation::admit_workspace(app_revision, &app_record, &record)?;
                owner
                    .commit_admission(WorkspaceAdmission::Create {
                        mutation,
                        expected_app_revision: app_revision,
                        intended: record,
                    })
                    .await?;
                owner.reconcile_owned().await
            }
            Self::Advance {
                owner,
                storage_revision,
                source,
                intended,
                ..
            } => {
                owner
                    .persist_admission(storage_revision, source, intended)
                    .await?;
                owner.reconcile_owned().await
            }
            Self::Delete {
                owner,
                storage_revision,
                source,
                ..
            } => {
                if matches!(&source.state, WorkspaceStateRecord::Active { .. }) {
                    let mut deleting = source.clone();
                    let WorkspaceStateRecord::Active {
                        materializations: remaining,
                        ..
                    } = deleting.state
                    else {
                        return Err(AppError::Busy("workspace is being reconciled".into()));
                    };
                    deleting.state = WorkspaceStateRecord::Deleting { remaining };
                    owner
                        .persist_admission(storage_revision, Box::new(source), deleting)
                        .await?;
                }
                owner.reconcile_owned().await
            }
            Self::Recover { owner, .. } => owner.reconcile_owned().await,
        }
    }
}

impl WorkspaceOperations {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(WorkspaceOperationsState::Open(BTreeMap::new())),
        }
    }

    pub(super) fn register<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        &self,
        operation: &WorkspaceOperation<CS, A, X, C, T, H, E>,
        identity: &Arc<WorkspaceOperationIdentity>,
        admission: Result<Option<OwnedSemaphorePermit>, AppError>,
    ) -> Result<WorkspaceRegistration, AppError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let WorkspaceOperationsState::Open(active) = &mut *state else {
            return Err(AppError::Closed(
                "application workspace operations are closed".to_owned(),
            ));
        };
        if let Some(registered) = active.get(operation.workspace()) {
            registered.ensure_matches(identity)?;
            return Ok(WorkspaceRegistration::Observing(
                registered.completion.subscribe(),
            ));
        }
        let admission = admission?;
        let (completion, observer) = watch::channel(None);
        active.insert(
            operation.workspace().clone(),
            WorkspaceObserver {
                identity: Arc::clone(identity),
                completion,
            },
        );
        Ok(WorkspaceRegistration::Starting {
            completion: observer,
            admission,
        })
    }

    pub(super) fn check_delete(
        &self,
        workspace: &WorkspaceId,
        fence: u64,
    ) -> Result<bool, AppError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let WorkspaceOperationsState::Open(active) = &*state else {
            return Err(AppError::Closed(
                "application workspace operations are closed".to_owned(),
            ));
        };
        if let Some(registered) = active.get(workspace) {
            registered.ensure_matches(&WorkspaceOperationIdentity {
                fence,
                intent: WorkspaceOperationIntent::Delete,
            })?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn finish(
        &self,
        workspace: &WorkspaceId,
        identity: &WorkspaceOperationIdentity,
        outcome: FinalResult,
    ) {
        let operation = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let WorkspaceOperationsState::Open(active) = &mut *state else {
                return;
            };
            active
                .get(workspace)
                .is_some_and(|operation| operation.identity.as_ref() == identity)
                .then(|| active.remove(workspace))
                .flatten()
        };
        if let Some(operation) = operation {
            let _ = operation.completion.send(Some(outcome));
        }
    }

    pub(crate) fn has_work(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        matches!(&*state, WorkspaceOperationsState::Open(active) if !active.is_empty())
    }

    pub(crate) fn close(&self) {
        let operations = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match std::mem::replace(&mut *state, WorkspaceOperationsState::Closed) {
                WorkspaceOperationsState::Open(active) => active,
                WorkspaceOperationsState::Closed => return,
            }
        };
        for operation in operations.into_values() {
            let _ = operation.completion.send(Some(FinalResult::Closed(
                "application workspace operation ownership was revoked".to_owned(),
            )));
        }
    }
}
