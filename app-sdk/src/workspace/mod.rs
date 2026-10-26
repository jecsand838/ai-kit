use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use agent_sdk::identity::WorkspaceId as AgentWorkspaceId;
use agent_sdk::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceContext,
    WorkspaceDefaults, WorkspaceExecutionPolicy, WorkspaceInfo, WorkspaceNetworkAccess,
    WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode, WorkspaceUpdateReadiness,
};
use agent_sdk::{AgentBackend, WorkspaceBackend};
use serde::{Deserialize, Serialize};
use storage_sdk::StateStoreBackend;

use crate::workflow::{
    ActionBackend, ChildExecutor, ConditionBackend, LifecycleHooks, StateTyping,
};

use crate::app::{AppRuntime, FinalResult, MAX_AGENTS};
use crate::storage::{AppCommitOutcome, AppMutation, EPOCH, StorageFailure};
use crate::workflow::WorkflowId;
use crate::workflow::definition::SchemaText;
use crate::workflow::run::WorkflowRuntime;
use crate::{AppError, WorkspaceConfig, WorkspaceId, WorkspaceSnapshot};

use self::MaterializationProgressRecord::{Applied, Pending, Unknown};

mod operations;

use operations::{WorkspaceOperation, WorkspaceRegistration};
pub(crate) use operations::{WorkspaceOperationIdentity, WorkspaceOperations};

const MAX_ROOT_BYTES: usize = 4_096;
const MAX_NAME_BYTES: usize = 256;
pub(crate) const MAX_WORKSPACES: usize = 10_000;
const MAX_WORKSPACE_INTENT_ATTEMPTS: usize = 8;

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct WorkspaceRecord {
    pub(crate) epoch: String,
    pub(crate) id: WorkspaceId,
    pub(crate) revision: u64,
    pub(crate) state: WorkspaceStateRecord,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct WorkspaceConfigRecord {
    pub(crate) name: Option<String>,
    pub(crate) root: String,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct MaterializationRecord {
    pub(crate) workspace_id: AgentWorkspaceId,
    pub(crate) revision: u64,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum WorkspaceStateRecord {
    Creating {
        config: WorkspaceConfigRecord,
        targets: BTreeMap<String, BTreeMap<String, MaterializationProgressRecord>>,
    },
    Active {
        config: WorkspaceConfigRecord,
        materializations: BTreeMap<String, BTreeMap<String, MaterializationRecord>>,
    },
    Replacing {
        prior: WorkspaceConfigRecord,
        next: WorkspaceConfigRecord,
        next_revision: u64,
        materializations: BTreeMap<String, BTreeMap<String, MaterializationRecord>>,
        remaining: Vec<(String, String)>,
    },
    Compensating {
        prior: WorkspaceConfigRecord,
        materializations: BTreeMap<String, BTreeMap<String, MaterializationRecord>>,
        remaining: Vec<(String, String)>,
    },
    Deleting {
        remaining: BTreeMap<String, BTreeMap<String, MaterializationRecord>>,
    },
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum MaterializationProgressRecord {
    Pending,
    Unknown,
    Applied {
        materialization: MaterializationRecord,
    },
}

pub(crate) struct WorkspaceRuntime<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
    id: WorkspaceId,
}

enum WorkspaceAdmission {
    Create {
        mutation: AppMutation,
        expected_app_revision: u64,
        intended: WorkspaceRecord,
    },
    Advance {
        mutation: AppMutation,
        storage_revision: u64,
        source: Box<WorkspaceRecord>,
        intended: WorkspaceRecord,
    },
}

struct LowerWorkspaceConfig {
    name: String,
    context: WorkspaceContext,
    defaults: WorkspaceDefaults,
}

impl From<&WorkspaceInfo> for MaterializationRecord {
    fn from(info: &WorkspaceInfo) -> Self {
        Self {
            workspace_id: info.id().clone(),
            revision: info.revision(),
        }
    }
}

impl MaterializationRecord {
    fn ensure_current(&self, info: &WorkspaceInfo) -> Result<(), AppError> {
        if info.id() != &self.workspace_id || info.revision() != self.revision {
            return Err(AppError::Conflict(
                "lower workspace revision changed".to_owned(),
            ));
        }
        Ok(())
    }
}

impl WorkspaceRecord {
    fn creating(
        id: &WorkspaceId,
        config: &WorkspaceConfig,
        agent_pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, AppError> {
        Ok(Self {
            epoch: EPOCH.to_owned(),
            id: id.clone(),
            revision: 1,
            state: WorkspaceStateRecord::Creating {
                config: WorkspaceConfigRecord::from_public(config)?,
                targets: agent_pairs.into_iter().fold(
                    BTreeMap::new(),
                    |mut targets, (key, revision)| {
                        targets
                            .entry(key)
                            .or_insert_with(BTreeMap::new)
                            .insert(revision, Pending);
                        targets
                    },
                ),
            },
        })
    }

    pub(crate) fn validate(
        &self,
        expected_id: &WorkspaceId,
        configured_agents: &BTreeSet<(&str, &str)>,
    ) -> Result<(), AppError> {
        let pairs = self.state.agent_pairs();
        let valid_config = |config: &WorkspaceConfigRecord| {
            LowerWorkspaceConfig::new(&self.id, &config.to_public()).is_ok()
        };
        let complete =
            |materializations: &BTreeMap<String, BTreeMap<String, MaterializationRecord>>| {
                materializations.values().all(|revisions| {
                    !revisions.is_empty() && revisions.values().all(|entry| entry.revision > 0)
                })
            };
        let valid = self.epoch == EPOCH && &self.id == expected_id && self.revision > 0
            && pairs.len() <= MAX_AGENTS
            && pairs.iter().all(|(key, revision)| SchemaText::node_or_state("agent key", key).is_ok()
                && SchemaText::revision(revision).is_ok())
            && match &self.state {
                WorkspaceStateRecord::Creating { config, targets } => valid_config(config)
                    && targets.values().all(|revisions| !revisions.is_empty()
                        && revisions.values().all(|item|
                            !matches!(item, Applied { materialization } if materialization.revision == 0))),
                WorkspaceStateRecord::Active { config, materializations } =>
                    valid_config(config) && complete(materializations),
                WorkspaceStateRecord::Replacing { prior, next, next_revision, materializations, remaining } => {
                    valid_config(prior) && valid_config(next)
                        && self.revision.checked_add(1) == Some(*next_revision)
                        && complete(materializations) && remaining.len() <= pairs.len()
                        && pairs.iter().skip(pairs.len() - remaining.len()).copied()
                            .eq(remaining.iter().map(|(key, revision)| (key.as_str(), revision.as_str())))
                }
                WorkspaceStateRecord::Compensating { prior, materializations, remaining } => {
                    valid_config(prior) && complete(materializations) && remaining.len() <= pairs.len()
                        && pairs.iter().take(remaining.len()).rev().copied()
                            .eq(remaining.iter().map(|(key, revision)| (key.as_str(), revision.as_str())))
                }
                WorkspaceStateRecord::Deleting { remaining } => complete(remaining),
            };
        if !valid {
            return Err(AppError::Integration("invalid workspace record".into()));
        }
        if !pairs.is_subset(configured_agents) {
            return Err(AppError::Conflict(
                "workspace agent revision is not configured".into(),
            ));
        }
        Ok(())
    }

    fn extension(
        &self,
        agent_pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Option<Self>, AppError> {
        let WorkspaceStateRecord::Active {
            config,
            materializations,
        } = &self.state
        else {
            return Err(AppError::Busy("workspace is reconciling".into()));
        };
        let mut targets = materializations
            .iter()
            .map(|(key, revisions)| {
                (
                    key.clone(),
                    revisions
                        .iter()
                        .map(|(revision, materialization)| {
                            (
                                revision.clone(),
                                Applied {
                                    materialization: materialization.clone(),
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut changed = false;
        for (key, revision) in agent_pairs {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                targets.entry(key).or_default().entry(revision)
            {
                entry.insert(Pending);
                changed = true;
            }
        }
        Ok(changed.then(|| Self {
            epoch: self.epoch.clone(),
            id: self.id.clone(),
            revision: self.revision,
            state: WorkspaceStateRecord::Creating {
                config: config.clone(),
                targets,
            },
        }))
    }

    pub(crate) fn has_agents(&self, agents: &BTreeMap<String, String>) -> Result<bool, AppError> {
        let materializations = self.materializations()?;
        Ok(agents.iter().all(|(key, revision)| {
            materializations
                .get(key)
                .is_some_and(|revisions| revisions.contains_key(revision))
        }))
    }

    pub(crate) fn has_recovery_agents(
        &self,
        agents: &BTreeMap<String, String>,
    ) -> Result<bool, AppError> {
        if matches!(&self.state, WorkspaceStateRecord::Creating { .. }) {
            return Ok(agents.iter().all(|(key, revision)| {
                self.state.claimed_materialization(key, revision).is_some()
            }));
        }
        self.has_agents(agents)
    }

    pub(crate) fn snapshot(&self) -> Result<WorkspaceSnapshot, AppError> {
        Ok(WorkspaceSnapshot {
            id: self.id.clone(),
            revision: self.revision,
            config: self.public_config()?,
        })
    }

    pub(crate) fn public_config(&self) -> Result<WorkspaceConfig, AppError> {
        match &self.state {
            WorkspaceStateRecord::Active { config, .. } => Ok(config.to_public()),
            WorkspaceStateRecord::Deleting { .. } => {
                Err(AppError::Closed("workspace is deleting".into()))
            }
            WorkspaceStateRecord::Creating { .. }
            | WorkspaceStateRecord::Replacing { .. }
            | WorkspaceStateRecord::Compensating { .. } => {
                Err(AppError::Busy("workspace is reconciling".into()))
            }
        }
    }

    pub(crate) fn materializations(
        &self,
    ) -> Result<&BTreeMap<String, BTreeMap<String, MaterializationRecord>>, AppError> {
        match &self.state {
            WorkspaceStateRecord::Active {
                materializations, ..
            }
            | WorkspaceStateRecord::Deleting {
                remaining: materializations,
            } => Ok(materializations),
            WorkspaceStateRecord::Creating { .. }
            | WorkspaceStateRecord::Replacing { .. }
            | WorkspaceStateRecord::Compensating { .. } => {
                Err(AppError::Busy("workspace is reconciling".into()))
            }
        }
    }

    fn observe_progress(
        &self,
        source: &Self,
        expected: u64,
        observed: Option<(Self, u64)>,
    ) -> Result<Option<u64>, AppError> {
        match observed {
            Some((record, seen)) if record == *self && seen > expected => Ok(Some(seen)),
            Some((record, seen)) if record == *source && seen == expected => Ok(None),
            Some(_) | None => Err(AppError::Conflict(
                "workspace progress was superseded".into(),
            )),
        }
    }
}

impl WorkspaceStateRecord {
    fn agent_pairs(&self) -> BTreeSet<(&str, &str)> {
        match self {
            Self::Creating { targets, .. } => targets
                .iter()
                .flat_map(|(key, revisions)| {
                    revisions
                        .keys()
                        .map(move |revision| (key.as_str(), revision.as_str()))
                })
                .collect(),
            Self::Active {
                materializations, ..
            }
            | Self::Replacing {
                materializations, ..
            }
            | Self::Compensating {
                materializations, ..
            }
            | Self::Deleting {
                remaining: materializations,
            } => materializations
                .iter()
                .flat_map(|(key, revisions)| {
                    revisions
                        .keys()
                        .map(move |revision| (key.as_str(), revision.as_str()))
                })
                .collect(),
        }
    }

    fn claimed_materialization(
        &self,
        agent: &str,
        revision: &str,
    ) -> Option<&MaterializationRecord> {
        match self {
            Self::Creating { targets, .. } => match targets.get(agent)?.get(revision)? {
                Applied { materialization } => Some(materialization),
                Pending | Unknown => None,
            },
            Self::Active {
                materializations, ..
            }
            | Self::Replacing {
                materializations, ..
            }
            | Self::Compensating {
                materializations, ..
            } => materializations.get(agent)?.get(revision),
            Self::Deleting { remaining } => remaining.get(agent)?.get(revision),
        }
    }
}

impl WorkspaceConfigRecord {
    fn from_public(config: &WorkspaceConfig) -> Result<Self, AppError> {
        Ok(Self {
            name: config.name.clone(),
            root: config
                .root
                .to_str()
                .ok_or_else(|| AppError::InvalidInput("workspace root is not UTF-8".into()))?
                .to_owned(),
        })
    }

    fn to_public(&self) -> WorkspaceConfig {
        WorkspaceConfig {
            name: self.name.clone(),
            root: PathBuf::from(&self.root),
        }
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
> WorkspaceRuntime<CS, A, X, C, T, H, E>
{
    pub(crate) fn new(app: Arc<AppRuntime<CS, A, X, C, T, H, E>>, id: WorkspaceId) -> Self {
        Self { app, id }
    }

    async fn run_operation(
        &self,
        operation: WorkspaceOperation<CS, A, X, C, T, H, E>,
        admission: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(), AppError> {
        let mut completion = self.start_operation(operation, admission)?;
        completion
            .wait_for(|result| result.is_some())
            .await
            .map_err(|_| AppError::Closed("workspace operation observation closed".to_owned()))?
            .as_ref()
            .ok_or_else(|| {
                AppError::Closed("workspace operation result is unavailable".to_owned())
            })?
            .to_public()
    }

    fn reattach_operation(
        &self,
        operation: WorkspaceOperation<CS, A, X, C, T, H, E>,
    ) -> Result<(), AppError> {
        self.start_operation(operation, None).map(|_| ())
    }

    fn start_operation(
        &self,
        operation: WorkspaceOperation<CS, A, X, C, T, H, E>,
        admission: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<tokio::sync::watch::Receiver<Option<FinalResult>>, AppError> {
        self.app.ensure_runtime_owned()?;
        let workspace = operation.workspace().clone();
        let identity = operation.identity()?;
        // A same-fence deletion may finish while its observer reads the native
        // record. Reserve outside registry exclusion; an existing owner ignores
        // this provisional result, while a replacement must accept it first.
        let admission = match admission {
            Some(admission) => Ok(Some(admission)),
            None if matches!(&operation, WorkspaceOperation::Delete { .. }) => {
                self.app.reserve_caller().map(Some)
            }
            None => Ok(None),
        };
        let registration = self
            .app
            .workspace_operations()
            .register(&operation, &identity, admission)?;
        let (completion, admission) = match registration {
            WorkspaceRegistration::Observing(completion) => return Ok(completion),
            WorkspaceRegistration::Starting {
                completion,
                admission,
            } => (completion, admission),
        };
        if !self
            .app
            .spawn_workspace(workspace, Arc::clone(&identity), admission, async move {
                operation.execute().await.map_or_else(
                    |error| FinalResult::from(&error),
                    |()| FinalResult::Succeeded,
                )
            })
        {
            self.app.workspace_operations().finish(
                &self.id,
                &identity,
                FinalResult::Closed(
                    "application workspace operation ownership was revoked".to_owned(),
                ),
            );
            return Err(AppError::Closed(
                "application workspace operation ownership was revoked".to_owned(),
            ));
        }
        self.app.mark_runtime_changed();
        Ok(completion)
    }

    pub(crate) async fn create(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        config: WorkspaceConfig,
    ) -> Result<Self, AppError> {
        let admission = app.reserve_caller()?;
        let id = WorkspaceId::generate();
        LowerWorkspaceConfig::new(&id, &config)?;
        let runtime = Self::new(app, id);
        let record = WorkspaceRecord::creating(
            &runtime.id,
            &config,
            runtime
                .app
                .agents()
                .await?
                .into_iter()
                .map(|(key, revision, _)| (key, revision)),
        )?;
        let owner = Self::new(Arc::clone(&runtime.app), runtime.id.clone());
        runtime
            .run_operation(WorkspaceOperation::create(owner, record), Some(admission))
            .await?;
        Ok(runtime)
    }

    pub(crate) async fn open(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        id: WorkspaceId,
    ) -> Result<Self, AppError> {
        let runtime = Self::new(app, id);
        runtime.readable_revision(None).await.map(|_| runtime)
    }

    pub(crate) async fn recover_inventory(
        app: &Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        records: &[WorkspaceRecord],
    ) -> Result<Vec<(WorkspaceId, u64)>, AppError> {
        let mut reconciling = Vec::new();
        for record in records {
            let id = record.id.clone();
            if matches!(record.state, WorkspaceStateRecord::Deleting { .. }) {
                reconciling.push((id, record.revision));
                continue;
            }
            let runtime = Self::new(Arc::clone(app), id.clone());
            runtime
                .reconcile()
                .await
                .map_err(StorageFailure::into_error)?;
            // Re-read after the new App claim and any recovered journal progress.
            // Only this fresh workspace fence authorizes deployment additions.
            let (source, storage_revision) = runtime.load_active().await?;
            let pairs = app
                .agents()
                .await?
                .into_iter()
                .map(|(key, revision, _)| (key, revision));
            if let Some(intended) = source.extension(pairs)? {
                let owner = Self::new(Arc::clone(app), id);
                runtime
                    .run_operation(
                        WorkspaceOperation::advance(
                            owner,
                            source.revision,
                            storage_revision,
                            source,
                            intended,
                        ),
                        None,
                    )
                    .await?;
            }
        }
        Ok(reconciling)
    }

    pub(crate) fn reattach_recovery(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        id: WorkspaceId,
        fence: u64,
    ) -> Result<(), AppError> {
        let runtime = Self::new(Arc::clone(&app), id.clone());
        let owner = Self::new(app, id);
        runtime.reattach_operation(WorkspaceOperation::recover(owner, fence))
    }

    pub(crate) fn id(&self) -> &WorkspaceId {
        &self.id
    }

    pub(crate) async fn snapshot(&self) -> Result<WorkspaceSnapshot, AppError> {
        self.app.ensure_open().await?;
        self.load_active().await?.0.snapshot()
    }

    pub(crate) async fn replace(
        &self,
        expected_revision: u64,
        config: WorkspaceConfig,
    ) -> Result<WorkspaceSnapshot, AppError> {
        let admission = self.app.reserve_caller()?;
        self.app.admit()?;
        let lower = LowerWorkspaceConfig::new(&self.id, &config)?;
        let (mut record, storage_revision) = self.load_active().await?;
        if record.revision != expected_revision {
            return Err(AppError::Conflict("workspace revision is stale".into()));
        }
        if self
            .app
            .has_active_runs(Some(&self.id))
            .await
            .map_err(StorageFailure::into_error)?
        {
            return Err(AppError::Busy("workspace has a nonterminal run".into()));
        }
        let next_revision = record
            .revision
            .checked_add(1)
            .ok_or_else(|| AppError::Conflict("workspace revision is exhausted".to_owned()))?;
        let source = record.clone();
        let (prior, materializations) = match record.state {
            WorkspaceStateRecord::Active {
                config,
                materializations,
            } => (config, materializations),
            _ => {
                return Err(AppError::Busy("workspace is being reconciled".into()));
            }
        };
        let next = WorkspaceConfigRecord::from_public(&config)?;
        let agents = self.app.agents().await?;
        for (key, revision, agent) in &agents {
            let materialization = materializations
                .get(key)
                .and_then(|revisions| revisions.get(revision))
                .ok_or_else(|| {
                    AppError::Integration("workspace materialization is incomplete".to_owned())
                })?;
            let request = lower.update_request(materialization)?;
            if !matches!(
                agent
                    .check_workspace_update(&request)
                    .await
                    .map_err(AppError::from)?,
                WorkspaceUpdateReadiness::Ready
            ) {
                return Err(AppError::Busy("lower workspace is not ready".into()));
            }
        }
        record.state = WorkspaceStateRecord::Replacing {
            prior,
            next,
            next_revision,
            materializations,
            remaining: agents
                .into_iter()
                .map(|(key, revision, _)| (key, revision))
                .collect(),
        };
        let owner = Self::new(Arc::clone(&self.app), self.id.clone());
        self.run_operation(
            WorkspaceOperation::advance(owner, expected_revision, storage_revision, source, record),
            Some(admission),
        )
        .await?;
        let (record, _) = self.load_active().await?;
        if record.revision != next_revision {
            return Err(AppError::Conflict("replacement was compensated".into()));
        }
        record.snapshot()
    }

    pub(crate) async fn workflow(
        &self,
        id: WorkflowId,
    ) -> Result<WorkflowRuntime<CS, A, X, C, T, H, E>, AppError> {
        self.app.ensure_open().await?;
        let revision = self.readable_revision(Some(&id)).await?;
        self.app.definition(&id)?;
        Ok(WorkflowRuntime::new(
            Arc::clone(&self.app),
            self.id.clone(),
            revision,
            id,
        ))
    }

    pub(crate) async fn delete(&mut self, expected_revision: u64) -> Result<(), AppError> {
        // Identical observation reuses its accepted owner's reservation. A
        // conflicting fence is rejected before competing for caller capacity.
        let reattached = self
            .app
            .workspace_operations()
            .check_delete(&self.id, expected_revision)?;
        let admission = if reattached {
            None
        } else {
            Some(self.app.reserve_caller()?)
        };
        self.app.ensure_open().await?;
        let (record, storage_revision) = self.load_readable().await?;
        if record.revision != expected_revision {
            return Err(AppError::Conflict("workspace revision is stale".into()));
        }
        let reattach = !matches!(&record.state, WorkspaceStateRecord::Active { .. });
        if !reattach {
            self.app.admit()?;
        }
        let owner = Self::new(Arc::clone(&self.app), self.id.clone());
        self.run_operation(
            WorkspaceOperation::delete(owner, expected_revision, storage_revision, record),
            admission,
        )
        .await
    }

    pub(crate) async fn reconcile_owned(self) -> Result<(), AppError> {
        let mut changes = self.app.runtime_changes();
        let mut delay = Duration::from_millis(25);
        loop {
            match self.reconcile().await {
                Err(failure)
                    if !failure.is_cleanup_failed()
                        && matches!(failure.error(), AppError::Busy(_) | AppError::Timeout(_)) => {}
                result => return result.map_err(StorageFailure::into_error),
            }
            tokio::select! {
                changed = changes.changed() => {
                    changed.map_err(|_| AppError::Closed("workspace observation closed".into()))?;
                    delay = Duration::from_millis(25);
                }
                _ = tokio::time::sleep(delay) => {
                    delay = delay.saturating_mul(2).min(Duration::from_secs(1));
                }
            }
        }
    }

    pub(crate) async fn lower_workspace(
        &self,
        agent_key: &str,
        agent_revision: &str,
    ) -> Result<AgentWorkspaceId, AppError> {
        let (record, _) = self.load_readable().await?;
        let materialization = record
            .materializations()?
            .get(agent_key)
            .and_then(|revisions| revisions.get(agent_revision))
            .ok_or_else(|| AppError::NotFound("agent materialization is missing".into()))?;
        Ok(materialization.workspace_id.clone())
    }

    fn reconcile(&self) -> Pin<Box<impl Future<Output = Result<(), StorageFailure>> + '_>> {
        Box::pin(async move {
            let mut cancellation_started = false;
            loop {
                if !self.app.owns_current_generation().await? {
                    return Err(AppError::Closed("workspace was superseded".into()).into());
                }
                let Some((record, storage_revision)) =
                    self.app.storage().load_workspace(&self.id).await?
                else {
                    return self.app.release_workspace_retention(&self.id).await;
                };
                self.app.current_generation_record().await?;
                self.app.validate_workspace(&record, &self.id).await?;
                if matches!(&record.state, WorkspaceStateRecord::Active { .. }) {
                    return Ok(());
                }
                if matches!(&record.state, WorkspaceStateRecord::Creating { .. }) {
                    self.reconcile_creation(record, storage_revision).await?;
                    continue;
                }
                if matches!(&record.state, WorkspaceStateRecord::Replacing { .. }) {
                    self.reconcile_replacement(record, storage_revision).await?;
                    continue;
                }
                if matches!(&record.state, WorkspaceStateRecord::Compensating { .. }) {
                    self.reconcile_compensation(record, storage_revision)
                        .await?;
                    continue;
                }
                let mut changes = self.app.runtime_changes();
                let active = if cancellation_started {
                    self.app.has_active_runs(Some(&self.id)).await?
                } else {
                    cancellation_started = true;
                    self.app.cancel_runs(Some(&self.id)).await?
                };
                if active {
                    changes
                        .changed()
                        .await
                        .map_err(|_| AppError::Closed("workspace observation closed".into()))?;
                    continue;
                }
                self.reconcile_deletion(record, storage_revision).await?;
            }
        })
    }

    fn reconcile_creation(
        &self,
        mut record: WorkspaceRecord,
        mut storage_revision: u64,
    ) -> Pin<Box<impl Future<Output = Result<(), StorageFailure>> + '_>> {
        Box::pin(async move {
            let mut source = record.clone();
            let WorkspaceStateRecord::Creating {
                mut config,
                mut targets,
            } = record.state
            else {
                return Err(AppError::Integration("creation journal changed".into()).into());
            };
            let Some((key, revision)) = targets.iter().find_map(|(key, revisions)| {
                revisions.iter().find_map(|(revision, progress)| {
                    (!matches!(progress, Applied { .. })).then(|| (key.clone(), revision.clone()))
                })
            }) else {
                let materializations = targets
                    .into_iter()
                    .map(|(key, revisions)| {
                        (
                            key,
                            revisions
                                .into_iter()
                                .filter_map(|(revision, progress)| match progress {
                                    Applied { materialization } => {
                                        Some((revision, materialization))
                                    }
                                    Pending | Unknown => None,
                                })
                                .collect(),
                        )
                    })
                    .collect();
                record.state = WorkspaceStateRecord::Active {
                    config,
                    materializations,
                };
                return self.persist(storage_revision, &source, &record).await;
            };
            let agent = self.app.agent(&key, &revision).await?;
            let lower = LowerWorkspaceConfig::new(&self.id, &config.to_public())?;
            let request = if matches!(
                targets
                    .get(&key)
                    .and_then(|revisions| revisions.get(&revision)),
                Some(Pending)
            ) {
                let request = lower.creation_request()?;
                targets
                    .entry(key.clone())
                    .or_default()
                    .insert(revision.clone(), Unknown);
                record.state = WorkspaceStateRecord::Creating { config, targets };
                storage_revision = self
                    .persist_creation_intent(&source, &record, storage_revision)
                    .await?;
                source.clone_from(&record);
                let WorkspaceStateRecord::Creating {
                    config: persisted_config,
                    targets: persisted_targets,
                } = record.state
                else {
                    return Err(AppError::Integration("creation journal changed".into()).into());
                };
                config = persisted_config;
                targets = persisted_targets;
                Some(request)
            } else {
                None
            };
            let (app_record, app_revision) = self.app.current_generation_record().await?;
            let mut claimed = BTreeSet::new();
            for workspace in self.app.storage().list_workspaces().await? {
                self.app
                    .validate_workspace(&workspace, &workspace.id)
                    .await?;
                if workspace.id != record.id
                    && let Some(materialization) =
                        workspace.state.claimed_materialization(&key, &revision)
                {
                    claimed.insert(materialization.workspace_id.clone());
                }
            }
            let returned = match request {
                Some(request) => agent.new_workspace(request).await.ok(),
                None => None,
            };
            let info = match returned {
                Some(workspace) => {
                    let info = workspace.info().await.map_err(AppError::from)?;
                    if claimed.contains(info.id()) {
                        return Err(AppError::Conflict("lower workspace is claimed".into()).into());
                    }
                    if !lower.matches(&info) {
                        return Err(
                            AppError::Integration("lower workspace is foreign".into()).into()
                        );
                    }
                    info
                }
                None => lower
                    .find_exact(
                        agent.list_workspaces().await.map_err(AppError::from)?,
                        &claimed,
                    )?
                    .ok_or_else(|| {
                        AppError::Integration("lower workspace is unresolved".to_owned())
                    })?,
            };
            targets.entry(key).or_default().insert(
                revision,
                Applied {
                    materialization: MaterializationRecord::from(&info),
                },
            );
            record.state = WorkspaceStateRecord::Creating { config, targets };
            self.app.validate_workspace(&record, &self.id).await?;
            let mutation = AppMutation::advance_workspace_with_app(
                app_revision,
                storage_revision,
                &app_record,
                &record,
            )?;
            self.commit_progress(mutation, storage_revision, &source, &record)
                .await
        })
    }

    fn reconcile_replacement(
        &self,
        mut record: WorkspaceRecord,
        storage_revision: u64,
    ) -> Pin<Box<impl Future<Output = Result<(), StorageFailure>> + '_>> {
        Box::pin(async move {
            let source = record.clone();
            let WorkspaceStateRecord::Replacing {
                prior,
                next,
                next_revision,
                mut materializations,
                mut remaining,
            } = record.state
            else {
                return Err(AppError::Integration("replacement journal changed".into()).into());
            };
            let processed =
                materializations.values().map(BTreeMap::len).sum::<usize>() - remaining.len();
            let Some((key, revision)) = remaining.first().cloned() else {
                record.revision = next_revision;
                record.state = WorkspaceStateRecord::Active {
                    config: next,
                    materializations,
                };
                return self.persist(storage_revision, &source, &record).await;
            };
            let agent = self.app.agent(&key, &revision).await?;
            let current = materializations
                .get(&key)
                .and_then(|revisions| revisions.get(&revision))
                .cloned()
                .ok_or_else(|| AppError::Integration("replacement journal is incomplete".into()))?;
            let info = agent
                .workspace_info(current.workspace_id.clone())
                .await
                .map_err(AppError::from)?;
            let next_lower = LowerWorkspaceConfig::new(&self.id, &next.to_public())?;
            let prior_lower = LowerWorkspaceConfig::new(&self.id, &prior.to_public())?;
            current.ensure_current(&info)?;
            if !prior_lower.matches(&info) {
                return Err(AppError::Conflict("lower configuration changed".into()).into());
            }
            match next_lower.apply_update(&self.app, &agent, &current).await {
                Ok(observed) => {
                    materializations
                        .entry(key.clone())
                        .or_default()
                        .insert(revision.clone(), MaterializationRecord::from(&observed));
                }
                Err(failure) => {
                    // A failed write has no returned mutation receipt. A diagnostic
                    // read can prove the leg unchanged, never claim a later revision.
                    let observed = agent
                        .workspace_info(current.workspace_id.clone())
                        .await
                        .map_err(|error| {
                            AppError::Conflict(format!(
                                "{failure}; lower reconciliation failed: {}",
                                AppError::from(error)
                            ))
                        })?;
                    current.ensure_current(&observed).map_err(|_| {
                        AppError::Conflict(format!(
                            "{failure}; lower workspace revision changed during reconciliation"
                        ))
                    })?;
                    if !prior_lower.matches(&observed) {
                        return Err(AppError::Conflict(format!(
                            "{failure}; lower configuration changed"
                        ))
                        .into());
                    }
                    let mut remaining = materializations
                        .iter()
                        .flat_map(|(key, revisions)| {
                            revisions
                                .keys()
                                .map(move |revision| (key.clone(), revision.clone()))
                        })
                        .take(processed)
                        .collect::<Vec<_>>();
                    remaining.reverse();
                    record.state = WorkspaceStateRecord::Compensating {
                        prior,
                        materializations,
                        remaining,
                    };
                    return self.persist(storage_revision, &source, &record).await;
                }
            }
            remaining.remove(0);
            record.state = WorkspaceStateRecord::Replacing {
                prior,
                next,
                next_revision,
                materializations,
                remaining,
            };
            self.persist(storage_revision, &source, &record).await
        })
    }

    fn reconcile_compensation(
        &self,
        mut record: WorkspaceRecord,
        storage_revision: u64,
    ) -> Pin<Box<impl Future<Output = Result<(), StorageFailure>> + '_>> {
        Box::pin(async move {
            let source = record.clone();
            let WorkspaceStateRecord::Compensating {
                prior,
                mut materializations,
                mut remaining,
            } = record.state
            else {
                return Err(AppError::Integration("compensation journal changed".into()).into());
            };
            let Some((key, revision)) = remaining.first().cloned() else {
                record.state = WorkspaceStateRecord::Active {
                    config: prior,
                    materializations,
                };
                return self.persist(storage_revision, &source, &record).await;
            };
            let agent = self.app.agent(&key, &revision).await?;
            let current = materializations
                .get(&key)
                .and_then(|revisions| revisions.get(&revision))
                .cloned()
                .ok_or_else(|| {
                    AppError::Integration("compensation journal is incomplete".into())
                })?;
            let info = agent
                .workspace_info(current.workspace_id.clone())
                .await
                .map_err(AppError::from)?;
            let prior_lower = LowerWorkspaceConfig::new(&self.id, &prior.to_public())?;
            current.ensure_current(&info)?;
            let info = prior_lower
                .apply_update(&self.app, &agent, &current)
                .await?;
            materializations
                .entry(key)
                .or_default()
                .insert(revision, MaterializationRecord::from(&info));
            remaining.remove(0);
            record.state = WorkspaceStateRecord::Compensating {
                prior,
                materializations,
                remaining,
            };
            self.persist(storage_revision, &source, &record).await
        })
    }

    fn reconcile_deletion(
        &self,
        mut record: WorkspaceRecord,
        storage_revision: u64,
    ) -> Pin<Box<impl Future<Output = Result<(), StorageFailure>> + '_>> {
        Box::pin(async move {
            let source = record.clone();
            let WorkspaceStateRecord::Deleting { mut remaining } = record.state else {
                return Err(AppError::Integration("deletion journal changed".into()).into());
            };
            let Some((key, revision, materialization)) =
                remaining.iter().find_map(|(key, revisions)| {
                    revisions.iter().next().map(|(revision, materialization)| {
                        (key.clone(), revision.clone(), materialization.clone())
                    })
                })
            else {
                record.state = WorkspaceStateRecord::Deleting { remaining };
                let app_revision = self.app.current_generation_revision().await?;
                let mutation =
                    AppMutation::delete_workspace(app_revision, storage_revision, &self.id)?;
                match self.app.storage().commit(mutation).await {
                    AppCommitOutcome::Applied => {}
                    AppCommitOutcome::Rejected(failure) if !failure.is_retryable_conflict() => {
                        return Err(failure);
                    }
                    AppCommitOutcome::Rejected(_) | AppCommitOutcome::Unknown(_) => {
                        match self.app.storage().load_workspace(&self.id).await? {
                            None => {}
                            Some((loaded, revision))
                                if loaded == record && revision == storage_revision =>
                            {
                                return Err(
                                    AppError::Busy("workspace deletion will retry".into()).into()
                                );
                            }
                            Some(_) => {
                                return Err(
                                    AppError::Conflict("deletion was superseded".into()).into()
                                );
                            }
                        }
                    }
                }
                self.app.release_workspace_retention(&self.id).await?;
                return Ok(());
            };
            let agent = self.app.agent(&key, &revision).await?;
            let id = materialization.workspace_id;
            self.app.current_generation_revision().await?;
            let current = agent
                .list_workspaces()
                .await
                .map_err(AppError::from)?
                .into_iter()
                .find(|info| info.id() == &id);
            if let Some(info) = current {
                if info.revision() != materialization.revision {
                    return Err(AppError::Conflict("lower deletion is stale".into()).into());
                }
                let request = DeleteWorkspaceRequest::new(id.clone(), info.revision())
                    .map_err(AppError::from)?;
                self.app.current_generation_revision().await?;
                if agent.delete_workspace(request).await.is_err() {
                    if !self.app.owns_current_generation().await? {
                        return Err(AppError::Closed("workspace was superseded".into()).into());
                    }
                    let current = agent
                        .list_workspaces()
                        .await
                        .map_err(AppError::from)?
                        .into_iter()
                        .find(|info| info.id() == &id);
                    match current {
                        None => {}
                        Some(info) if info.revision() == materialization.revision => {
                            return Err(AppError::Busy("lower deletion will retry".into()).into());
                        }
                        Some(_) => {
                            return Err(AppError::Conflict(
                                "lower deletion revision changed".into(),
                            )
                            .into());
                        }
                    }
                }
            }
            if let Some(revisions) = remaining.get_mut(&key) {
                revisions.remove(&revision);
                if revisions.is_empty() {
                    remaining.remove(&key);
                }
            }
            record.state = WorkspaceStateRecord::Deleting { remaining };
            self.persist(storage_revision, &source, &record).await
        })
    }

    async fn load_active(&self) -> Result<(WorkspaceRecord, u64), AppError> {
        let loaded = self
            .load_record()
            .await?
            .ok_or_else(|| AppError::NotFound("workspace does not exist".into()))?;
        loaded.0.public_config()?;
        Ok(loaded)
    }

    async fn load_readable(&self) -> Result<(WorkspaceRecord, u64), AppError> {
        let loaded = self
            .load_record()
            .await?
            .ok_or_else(|| AppError::NotFound("workspace does not exist".into()))?;
        loaded.0.materializations()?;
        Ok(loaded)
    }

    async fn readable_revision(
        &self,
        workflow: Option<&WorkflowId>,
    ) -> Result<Option<u64>, AppError> {
        let revision = match self.load_record().await? {
            Some((record, _)) => {
                record.materializations()?;
                Some(record.revision)
            }
            None => {
                if !self
                    .app
                    .storage()
                    .has_retained_history(&self.id, workflow)
                    .await?
                {
                    return Err(AppError::NotFound(
                        "workspace history does not exist".into(),
                    ));
                }
                None
            }
        };
        self.app.current_generation_record().await?;
        Ok(revision)
    }

    async fn load_record(&self) -> Result<Option<(WorkspaceRecord, u64)>, AppError> {
        self.app.current_generation_record().await?;
        let Some((record, revision)) = self.app.storage().load_workspace(&self.id).await? else {
            return Ok(None);
        };
        self.app.validate_workspace(&record, &self.id).await?;
        Ok(Some((record, revision)))
    }

    async fn persist_creation_intent(
        &self,
        prior: &WorkspaceRecord,
        intended: &WorkspaceRecord,
        storage_revision: u64,
    ) -> Result<u64, StorageFailure> {
        let mut delay = Duration::from_millis(25);
        let mut remaining = MAX_WORKSPACE_INTENT_ATTEMPTS;
        loop {
            let observed: Result<_, StorageFailure> = async {
                let app_revision = self.app.current_generation_revision().await?;
                let mutation =
                    AppMutation::advance_workspace(app_revision, storage_revision, intended)?;
                if let AppCommitOutcome::Rejected(failure) =
                    self.app.storage().commit(mutation).await
                    && !failure.is_retryable_conflict()
                {
                    return Err(failure);
                }
                self.app
                    .storage()
                    .load_workspace(&self.id)
                    .await
                    .map_err(StorageFailure::from)
            }
            .await;
            let observed = match observed {
                Err(failure) if failure.is_cleanup_failed() => return Err(failure),
                observed => observed.map_err(StorageFailure::into_error),
            };
            let exhausted = match observed.and_then(|loaded| {
                intended
                    .observe_progress(prior, storage_revision, loaded)
                    .map_err(|_| AppError::Integration("workspace intent is contradictory".into()))
            }) {
                Ok(Some(revision)) => return Ok(revision),
                Ok(None) => AppError::Conflict("workspace intent kept conflicting".into()),
                Err(AppError::Busy(_) | AppError::Timeout(_) | AppError::Integration(_)) => {
                    AppError::Integration("workspace intent is unproven".into())
                }
                Err(error) => return Err(error.into()),
            };
            remaining -= 1;
            if remaining == 0 {
                return Err(exhausted.into());
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(Duration::from_secs(1));
        }
    }

    async fn persist(
        &self,
        storage_revision: u64,
        source: &WorkspaceRecord,
        record: &WorkspaceRecord,
    ) -> Result<(), StorageFailure> {
        let app_revision = self.app.current_generation_revision().await?;
        self.app.validate_workspace(record, &self.id).await?;
        let mutation = AppMutation::advance_workspace(app_revision, storage_revision, record)?;
        self.commit_progress(mutation, storage_revision, source, record)
            .await
    }

    async fn persist_admission(
        &self,
        storage_revision: u64,
        source: Box<WorkspaceRecord>,
        intended: WorkspaceRecord,
    ) -> Result<(), AppError> {
        let (_, app_revision) = self.app.admission_record().await?;
        if matches!(&intended.state, WorkspaceStateRecord::Replacing { .. })
            && self
                .app
                .has_active_runs(Some(&self.id))
                .await
                .map_err(StorageFailure::into_error)?
        {
            return Err(AppError::Busy("workspace has a nonterminal run".to_owned()));
        }
        self.app.validate_workspace(&intended, &self.id).await?;
        let mutation = AppMutation::advance_workspace(app_revision, storage_revision, &intended)?;
        self.commit_admission(WorkspaceAdmission::Advance {
            mutation,
            storage_revision,
            source,
            intended,
        })
        .await
    }

    async fn commit_admission(&self, admission: WorkspaceAdmission) -> Result<(), AppError> {
        match admission {
            WorkspaceAdmission::Create {
                mutation,
                expected_app_revision,
                intended,
            } => match self.app.storage().commit(mutation).await {
                AppCommitOutcome::Applied => Ok(()),
                AppCommitOutcome::Rejected(failure) if failure.is_retryable_conflict() => {
                    let (_, app_revision) = self.app.current_generation_record().await?;
                    match self.app.storage().load_workspace(&self.id).await? {
                        Some((observed, _))
                            if observed == intended && app_revision > expected_app_revision =>
                        {
                            Ok(())
                        }
                        None => Err(failure.into_error()),
                        Some(_) => Err(AppError::Conflict(
                            "workspace admission was superseded".to_owned(),
                        )),
                    }
                }
                AppCommitOutcome::Unknown(failure) => {
                    self.observe_unknown_creation_admission(
                        expected_app_revision,
                        &intended,
                        failure,
                    )
                    .await
                }
                AppCommitOutcome::Rejected(failure) => Err(failure.into_error()),
            },
            WorkspaceAdmission::Advance {
                mutation,
                storage_revision,
                source,
                intended,
            } => self
                .commit_progress(mutation, storage_revision, &source, &intended)
                .await
                .map_err(StorageFailure::into_error),
        }
    }

    async fn observe_unknown_creation_admission(
        &self,
        expected_app_revision: u64,
        intended: &WorkspaceRecord,
        unknown: StorageFailure,
    ) -> Result<(), AppError> {
        let mut delay = Duration::from_millis(25);
        loop {
            self.app.ensure_runtime_owned()?;
            let observation: Result<_, StorageFailure> = async {
                let (_, app_revision) = self.app.current_generation_record_for_recovery().await?;
                let workspace = self
                    .app
                    .storage()
                    .load_workspace_for_recovery(&self.id)
                    .await?;
                Ok((app_revision, workspace))
            }
            .await;
            match observation {
                Ok((app_revision, Some((observed, _))))
                    if observed == *intended && app_revision > expected_app_revision =>
                {
                    return Ok(());
                }
                Ok((_, None)) => return Err(unknown.into_error()),
                Ok((_, Some(_))) => {
                    return Err(AppError::Conflict(
                        "workspace admission is contradictory".to_owned(),
                    ));
                }
                Err(failure) if failure.is_recoverable() => {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(Duration::from_secs(1));
                }
                Err(failure) => return Err(failure.into_error()),
            }
        }
    }

    async fn commit_progress(
        &self,
        mutation: AppMutation,
        storage_revision: u64,
        source: &WorkspaceRecord,
        intended: &WorkspaceRecord,
    ) -> Result<(), StorageFailure> {
        match self.app.storage().commit(mutation).await {
            AppCommitOutcome::Applied => Ok(()),
            AppCommitOutcome::Rejected(failure) if !failure.is_retryable_conflict() => Err(failure),
            AppCommitOutcome::Rejected(_) | AppCommitOutcome::Unknown(_) => {
                match intended.observe_progress(
                    source,
                    storage_revision,
                    self.app.storage().load_workspace(&self.id).await?,
                )? {
                    Some(_) => Ok(()),
                    None => Err(AppError::Busy("workspace progress will retry".into()).into()),
                }
            }
        }
    }
}

impl LowerWorkspaceConfig {
    fn new(id: &WorkspaceId, config: &WorkspaceConfig) -> Result<Self, AppError> {
        if config.name.as_deref().is_some_and(|name| {
            !(1..=MAX_NAME_BYTES).contains(&name.len())
                || name.trim() != name
                || name.chars().any(char::is_control)
        }) {
            return Err(AppError::InvalidInput("workspace name is invalid".into()));
        }
        let encoded = config.root.to_str().ok_or_else(|| {
            AppError::InvalidInput("workspace root must be valid UTF-8".to_owned())
        })?;
        if !(1..=MAX_ROOT_BYTES).contains(&encoded.len()) || encoded.chars().any(char::is_control) {
            return Err(AppError::InvalidInput("workspace root is invalid".into()));
        }
        let root = WorkspaceRoot::new(config.root.clone(), WorkspaceRootAccess::ReadWrite)
            .map_err(AppError::from)?;
        let context = WorkspaceContext::new(
            config.root.clone(),
            vec![root],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .map_err(AppError::from)?;
        Ok(Self {
            name: config.name.clone().unwrap_or_else(|| id.to_string()),
            context,
            defaults: WorkspaceDefaults::empty(),
        })
    }

    fn creation_request(&self) -> Result<NewWorkspace, AppError> {
        NewWorkspace::new(
            self.name.clone(),
            self.context.clone(),
            self.defaults.clone(),
        )
        .map_err(AppError::from)
    }

    fn update_request(
        &self,
        materialization: &MaterializationRecord,
    ) -> Result<UpdateWorkspaceRequest, AppError> {
        UpdateWorkspaceRequest::new(
            materialization.workspace_id.clone(),
            materialization.revision,
            self.name.clone(),
            self.context.clone(),
            self.defaults.clone(),
        )
        .map_err(AppError::from)
    }

    async fn apply_update<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        &self,
        app: &AppRuntime<CS, A, X, C, T, H, E>,
        agent: &A,
        current: &MaterializationRecord,
    ) -> Result<WorkspaceInfo, AppError> {
        let request = self.update_request(current)?;
        app.current_generation_revision().await?;
        let info = agent
            .update_workspace(request)
            .await
            .map_err(AppError::from)?;
        if info.id() != &current.workspace_id
            || Some(info.revision()) != current.revision.checked_add(1)
            || !self.matches(&info)
        {
            return Err(AppError::Integration(
                "lower workspace returned an invalid update receipt".to_owned(),
            ));
        }
        Ok(info)
    }

    fn matches(&self, info: &WorkspaceInfo) -> bool {
        info.name() == self.name
            && info.context() == &self.context
            && info.defaults() == &self.defaults
    }

    fn find_exact(
        &self,
        workspaces: Vec<WorkspaceInfo>,
        claimed: &BTreeSet<AgentWorkspaceId>,
    ) -> Result<Option<WorkspaceInfo>, AppError> {
        let mut exact = None;
        let mut claimed_exact = false;
        for info in workspaces {
            if !self.matches(&info) {
                continue;
            }
            if claimed.contains(info.id()) {
                claimed_exact = true;
                continue;
            }
            if exact.replace(info).is_some() {
                return Err(AppError::Conflict("lower claim is ambiguous".into()));
            }
        }
        if exact.is_none() && claimed_exact {
            Err(AppError::Conflict("lower workspace is claimed".into()))
        } else {
            Ok(exact)
        }
    }
}
