use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_sdk::AgentBackend;
use serde::{Deserialize, Serialize};
use storage_sdk::StateStoreBackend;

use crate::workflow::{
    ActionBackend, ChildExecutor, ConditionBackend, GraphDescription, LifecycleHooks, StateTyping,
};
use tokio::sync::{RwLock, oneshot, watch};

use crate::storage::{
    AppCommitOutcome, AppMigration, AppMutation, AppStore, EPOCH, StorageFailure,
};
use crate::workflow::definition::{CompiledDefinition, MAX_COLLECTION_ITEMS, SchemaText};
use crate::workflow::run::{
    RECONCILIATION_INITIAL_DELAY, RECONCILIATION_MAX_DELAY, RunKey, RunRegistry,
    RuntimeOperationFailure, ScheduleCause, WorkflowRunRuntime,
};
use crate::workflow::{RunStatus, WorkflowId};
use crate::workspace::{
    WorkspaceOperationIdentity, WorkspaceOperations, WorkspaceRecord, WorkspaceRuntime,
    WorkspaceStateRecord,
};
use crate::{
    AgentSpec, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId, WorkspaceSnapshot,
};

pub(crate) const MAX_AGENTS: usize = 256;
const MAX_WORKFLOWS: usize = 1_024;
pub(crate) const MAX_RUNS: usize = 100_000;
const MAX_LIST_LIMIT: usize = 1_000;
const MAX_DEFINITION_INVENTORY_BYTES: usize = 2 * 1_048_576;
const MAX_SHUTDOWN: Duration = Duration::from_secs(365 * 24 * 60 * 60);

mod inventory;
mod lifecycle;
mod tasks;

use inventory::AppInventory;
use lifecycle::{
    AppLifecycle, DrainPolicy, Finalization, FinalizationStage, ShutdownMode, ShutdownRequest,
};
use tasks::AppTasks;
pub(crate) use tasks::{DriverCommand, RunTaskKind};

/// Best-effort operational tracing, separate from functional lifecycle hooks.
/// This owner contains only a span; it never owns or retries workflow work.
pub(crate) struct AppTrace {
    span: Option<tracing::Span>,
}

impl AppTrace {
    pub(crate) fn span(create: impl FnOnce() -> tracing::Span) -> Self {
        Self {
            span: catch_unwind(AssertUnwindSafe(create)).ok(),
        }
    }

    pub(crate) fn observe<F: Future + Unpin>(
        self,
        mut future: F,
    ) -> impl Future<Output = F::Output> + Unpin {
        poll_fn(move |context| {
            let entered = self
                .span
                .as_ref()
                .and_then(|span| catch_unwind(AssertUnwindSafe(|| span.enter())).ok());
            // Preserve exactly one inner poll. A workflow panic is rethrown
            // after exiting observation; it is never swallowed or retried.
            let polled = catch_unwind(AssertUnwindSafe(|| Pin::new(&mut future).poll(context)));
            Self::event(|| drop(entered));
            match polled {
                Ok(polled) => polled,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        })
    }

    pub(crate) fn event(emit: impl FnOnce()) {
        let _ = catch_unwind(AssertUnwindSafe(emit));
    }
}

impl Drop for AppTrace {
    fn drop(&mut self) {
        // Span destruction invokes Subscriber::try_close, including when the
        // enclosing workflow future is cancelled or unwinds.
        Self::event(|| drop(self.span.take()));
    }
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct AppRecord {
    pub(crate) epoch: String,
    #[serde(default)]
    pub(crate) migration: AppMigration,
    pub(crate) generation: u64,
    pub(crate) run_count: usize,
    pub(crate) agents: BTreeMap<String, String>,
    pub(crate) definitions: BTreeMap<WorkflowId, String>,
    pub(crate) shutdown: ShutdownRecord,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum ShutdownRecord {
    Active,
    Draining,
    Cancelling,
    Closed,
}

pub(crate) struct AppRuntime<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    storage: AppStore<CS>,
    generation: u64,
    definitions: BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
    agents: RwLock<Vec<AgentSpec<A>>>,
    actions: Vec<X>,
    conditions: Vec<C>,
    typing: T,
    hooks: H,
    executor: E,
    lifecycle: Mutex<AppLifecycle>,
    shutdown_publication: tokio::sync::Mutex<()>,
    tasks: Arc<AppTasks<CS, A, X, C, T, H, E>>,
    runs: RunRegistry<A>,
    workspace_operations: WorkspaceOperations,
}

#[derive(Clone)]
pub(crate) enum FinalResult {
    Succeeded,
    Conflict(String),
    Busy(String),
    Closed(String),
    Timeout(String),
    Integration(String),
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> Drop for AppRuntime<CS, A, X, C, T, H, E>
{
    fn drop(&mut self) {
        self.runs.close();
        self.workspace_operations.close();
        self.tasks.stop_runtime();
        self.tasks.finish();
    }
}

impl AppRecord {
    fn new() -> Self {
        Self {
            epoch: EPOCH.to_owned(),
            migration: AppMigration::Ready,
            generation: 1,
            run_count: 0,
            agents: BTreeMap::new(),
            definitions: BTreeMap::new(),
            shutdown: ShutdownRecord::Active,
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
> AppRuntime<CS, A, X, C, T, H, E>
{
    pub(crate) async fn open(
        config: AppConfig<CS, A, X, C, T, H, E>,
    ) -> Result<Arc<Self>, AppError> {
        let AppConfig {
            state,
            agents,
            workflows,
            actions,
            conditions,
            typing,
            hooks,
            executor,
        } = config;
        let validation = if agents.len() > MAX_AGENTS
            || workflows.len() > MAX_WORKFLOWS
            || actions.len() > MAX_COLLECTION_ITEMS
            || conditions.len() > MAX_COLLECTION_ITEMS
        {
            Err(AppError::InvalidInput(
                "application configuration exceeds a private inventory bound".to_owned(),
            ))
        } else {
            Self::validate_agents(&agents).and_then(|()| {
                // Only borrowed caller declarations are inspected here. No App
                // runtime or storage mutation exists to resume after unwinding.
                catch_unwind(AssertUnwindSafe(|| {
                    Self::validate_actions(&actions)?;
                    Self::validate_conditions(&conditions)
                }))
                .unwrap_or_else(|_| {
                    Err(AppError::Integration(
                        "configured behavior declaration panicked".to_owned(),
                    ))
                })
            })
        };
        if let Err(error) = validation {
            if Self::shutdown_agents(agents).await.is_some() {
                AppTrace::event(
                    || tracing::warn!(target: "app_sdk", event = "app.open.cleanup_failed"),
                );
            }
            return Err(error);
        }

        let revisions = agents
            .iter()
            .map(|spec| (spec.key.clone(), spec.revision.clone()))
            .collect::<BTreeMap<_, _>>();
        let configured = agents;
        let prepared = async {
            let definitions = Self::validate_definitions(workflows)?;
            Self::validate_definition_inventory_size(&definitions)?;
            Self::validate_definition_agents(&definitions, &configured)?;
            catch_unwind(AssertUnwindSafe(|| {
                for definition in definitions.values().flatten() {
                    definition.validate_bindings(&actions, &conditions)?;
                }
                Ok::<(), AppError>(())
            }))
            .unwrap_or_else(|_| {
                Err(AppError::Integration(
                    "configured behavior declaration panicked".to_owned(),
                ))
            })?;
            let storage = AppStore::new(state);
            storage
                .migrate(
                    &definitions,
                    &configured
                        .iter()
                        .map(|spec| (spec.key.as_str(), spec.revision.as_str()))
                        .collect(),
                )
                .await?;
            storage.preflight_records().await?;
            let task_runtime = tokio::runtime::Handle::try_current().map_err(|_| {
                AppError::Integration("application requires an active Tokio runtime".to_owned())
            })?;
            let loaded = storage.load_app().await?;
            storage
                .validate_definition_inventory(
                    &definitions,
                    loaded.as_ref().map(|(record, _)| record),
                    &configured
                        .iter()
                        .map(|spec| (spec.key.as_str(), spec.revision.as_str()))
                        .collect(),
                )
                .await?;
            let (mut record, record_revision) = match loaded {
                Some((record, revision)) => (record, Some(revision)),
                None => (AppRecord::new(), None),
            };
            let definition_inventory = definitions
                .iter()
                .filter_map(|(id, revisions)| {
                    revisions
                        .last()
                        .map(|definition| (id.clone(), definition.revision().to_owned()))
                })
                .collect();
            record.agents = revisions;
            record.definitions = definition_inventory;
            storage.cleanup_receipts().await?;
            record.generation = record.generation.checked_add(1).ok_or_else(|| {
                AppError::Conflict("application generation is exhausted".to_owned())
            })?;
            let recovered_shutdown = record.shutdown.clone();
            if matches!(&recovered_shutdown, ShutdownRecord::Closed) {
                record.shutdown = ShutdownRecord::Active;
            }
            let mutation = match record_revision {
                Some(revision) => AppMutation::claim_app_revision(revision, &record, &definitions)?,
                None => AppMutation::claim_new_app(&record, &definitions)?,
            };
            match storage.commit(mutation).await {
                AppCommitOutcome::Applied => {}
                AppCommitOutcome::Rejected(failure) => return Err(failure.into_error()),
                AppCommitOutcome::Unknown(_) => {
                    return Err(AppError::Integration(
                        "application generation claim is unknown".to_owned(),
                    ));
                }
            }
            Ok::<_, AppError>((
                storage,
                task_runtime,
                record,
                recovered_shutdown,
                definitions,
            ))
        }
        .await;
        let (storage, task_runtime, record, recovered_shutdown, definitions) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                if Self::shutdown_agents(configured).await.is_some() {
                    AppTrace::event(
                        || tracing::warn!(target: "app_sdk", event = "app.open.cleanup_failed"),
                    );
                }
                return Err(error);
            }
        };
        AppTrace::event(|| {
            tracing::info!(
                target: "app_sdk",
                event = "app.open",
                generation = record.generation,
                agents = configured.len(),
                workflows = definitions.len(),
            )
        });

        let resume = match recovered_shutdown {
            ShutdownRecord::Draining => Some(false),
            ShutdownRecord::Cancelling => Some(true),
            ShutdownRecord::Active | ShutdownRecord::Closed => None,
        };
        let runtime = Arc::new(Self {
            storage,
            generation: record.generation,
            definitions,
            agents: RwLock::new(configured),
            actions,
            conditions,
            typing,
            hooks,
            executor,
            lifecycle: Mutex::new(AppLifecycle::Active),
            shutdown_publication: tokio::sync::Mutex::new(()),
            tasks: Arc::new(AppTasks::new(task_runtime)),
            runs: RunRegistry::new(),
            workspace_operations: WorkspaceOperations::new(),
        });
        runtime.tasks.start(Arc::downgrade(&runtime));
        let recovery = async {
            let inventory = AppInventory::load(&runtime).await?;
            let reconciling =
                WorkspaceRuntime::recover_inventory(&runtime, inventory.workspaces()).await?;
            let recovered_runs = inventory.recoverable_runs();
            for (workspace, fence) in reconciling {
                runtime.current_generation_revision().await?;
                WorkspaceRuntime::reattach_recovery(Arc::clone(&runtime), workspace, fence)?;
            }
            // Bind all child identities before making any parent runnable.
            // The parent's concrete executor owns nonterminal child polling.
            for record in &recovered_runs {
                WorkflowRunRuntime::attach_record(Arc::clone(&runtime), record)?;
            }
            for record in recovered_runs {
                if record.parent.is_some() && !record.checkpoint.is_terminal() {
                    continue;
                }
                let run = WorkflowRunRuntime::attach_record(Arc::clone(&runtime), &record)?;
                let cause = if resume == Some(true) && !record.checkpoint.is_terminal() {
                    ScheduleCause::CheckpointObserved(record.checkpoint.sequence)
                } else {
                    ScheduleCause::CheckpointReady(record.checkpoint.sequence)
                };
                run.schedule(cause);
            }
            let mut after = None;
            loop {
                let (deletions, next) = runtime.storage.pending_deletions(after.as_deref()).await?;
                for key in deletions {
                    runtime.current_generation_revision().await?;
                    let run = WorkflowRunRuntime::attach(Arc::clone(&runtime), key.clone())?;
                    let owner = Arc::clone(&runtime);
                    let (completed, completion) = oneshot::channel();
                    if !runtime.spawn_run(key, RunTaskKind::Operation, None, async move {
                        let result = match run.resume_deletion().await {
                            Ok(()) => FinalResult::Succeeded,
                            Err(error) => {
                                let failure = FinalResult::from(&error);
                                owner.tasks_failed(failure.clone());
                                failure
                            }
                        };
                        let _ = completed.send(result);
                    }) {
                        return Err(AppError::Closed(
                            "run deletion recovery admission is closed".into(),
                        ));
                    }
                    // Startup observes each original deletion owner. Dropping
                    // this receiver never cancels its admitted native cleanup.
                    completion
                        .await
                        .map_err(|_| {
                            AppError::Integration("run deletion recovery owner stopped".into())
                        })?
                        .to_public()?;
                }
                match next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            if let Some(cancel) = resume {
                runtime
                    .begin_shutdown(if cancel {
                        ShutdownRequest::Cancel
                    } else {
                        ShutdownRequest::Drain(DrainPolicy::Wait)
                    })
                    .await?;
            }
            Ok::<(), AppError>(())
        }
        .await;
        if let Err(error) = recovery {
            runtime.close_after_open_failure().await;
            return Err(error);
        }
        Ok(runtime)
    }

    pub(crate) async fn create_workspace(
        self: &Arc<Self>,
        config: WorkspaceConfig,
    ) -> Result<WorkspaceRuntime<CS, A, X, C, T, H, E>, AppError> {
        WorkspaceRuntime::create(Arc::clone(self), config).await
    }

    pub(crate) async fn workspace(
        self: &Arc<Self>,
        id: WorkspaceId,
    ) -> Result<WorkspaceRuntime<CS, A, X, C, T, H, E>, AppError> {
        self.ensure_open().await?;
        WorkspaceRuntime::open(Arc::clone(self), id).await
    }

    pub(crate) async fn list_workspaces(
        &self,
        after: Option<WorkspaceId>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WorkspaceSnapshot>, AppError> {
        self.ensure_open().await?;
        if limit.get() > MAX_LIST_LIMIT {
            return Err(AppError::InvalidInput(
                "workspace list limit exceeds the private bound".to_owned(),
            ));
        }
        let mut records = self.storage.list_workspaces().await?;
        for record in &records {
            self.validate_workspace(record, &record.id).await?;
        }
        records.sort_by(|left, right| left.id.cmp(&right.id));
        if let Some(after) = &after
            && !records.iter().any(|record| &record.id == after)
        {
            return Err(AppError::NotFound(
                "workspace cursor does not exist".to_owned(),
            ));
        }
        let mut output = Vec::new();
        for record in records {
            if !matches!(&record.state, WorkspaceStateRecord::Active { .. }) {
                continue;
            }
            if after.as_ref().is_some_and(|after| &record.id <= after) {
                continue;
            }
            output.push(record.snapshot()?);
            if output.len() == limit.get() {
                break;
            }
        }
        Ok(output)
    }

    pub(crate) async fn shutdown(self: &Arc<Self>, policy: Shutdown) -> Result<(), AppError> {
        let (request, timeout) = match policy {
            Shutdown::RejectActive => (
                ShutdownRequest::Drain(DrainPolicy::RejectIfActive),
                MAX_SHUTDOWN,
            ),
            Shutdown::Drain { timeout } => (
                ShutdownRequest::Drain(DrainPolicy::Wait),
                Self::validate_timeout(timeout)?,
            ),
            Shutdown::CancelAndDrain { timeout } => {
                (ShutdownRequest::Cancel, Self::validate_timeout(timeout)?)
            }
        };
        tokio::time::timeout(timeout, async {
            let mut completion = self.begin_shutdown(request).await?;
            let result = completion
                .wait_for(|result| result.is_some())
                .await
                .map_err(|_| {
                    AppError::Closed("application finalizer observation closed".to_owned())
                })?
                .clone()
                .ok_or_else(|| {
                    AppError::Closed("application finalizer result is unavailable".to_owned())
                })?;
            if self.finalization_closed() {
                self.tasks.join().await;
            }
            result.to_public()
        })
        .await
        .map_err(|_| AppError::Timeout("application shutdown observation timed out".to_owned()))?
    }

    pub(crate) fn storage(&self) -> &AppStore<CS> {
        &self.storage
    }

    pub(crate) fn run_registry(&self) -> &RunRegistry<A> {
        &self.runs
    }

    pub(crate) fn workspace_operations(&self) -> &WorkspaceOperations {
        &self.workspace_operations
    }

    pub(crate) async fn owns_current_generation(&self) -> Result<bool, AppError> {
        let (record, _) = self.generation_record().await?;
        if record.generation < self.generation {
            return Err(AppError::Integration(
                "application generation regressed".to_owned(),
            ));
        }
        Ok(record.generation == self.generation)
    }

    pub(crate) async fn current_generation_revision(&self) -> Result<u64, AppError> {
        self.current_generation_record()
            .await
            .map(|(_, revision)| revision)
    }

    pub(crate) async fn current_generation_record(&self) -> Result<(AppRecord, u64), AppError> {
        self.current_generation_record_for_recovery()
            .await
            .map_err(StorageFailure::into_error)
    }

    pub(crate) async fn current_generation_record_for_recovery(
        &self,
    ) -> Result<(AppRecord, u64), StorageFailure> {
        let Some((record, revision)) = self.storage.load_app_for_recovery().await? else {
            return Err(StorageFailure::permanent(AppError::Integration(
                "application generation record is missing".to_owned(),
            )));
        };
        if record.generation != self.generation {
            return Err(StorageFailure::permanent(AppError::Closed(
                "application generation has been superseded".to_owned(),
            )));
        }
        Ok((record, revision))
    }

    async fn generation_record(&self) -> Result<(AppRecord, u64), AppError> {
        let Some((record, revision)) = self.storage.load_app().await? else {
            return Err(AppError::Integration(
                "application generation record is missing".to_owned(),
            ));
        };
        Ok((record, revision))
    }

    pub(crate) async fn admission_record(&self) -> Result<(AppRecord, u64), AppError> {
        self.admit()?;
        let (record, revision) = self.current_generation_record().await?;
        if !matches!(&record.shutdown, ShutdownRecord::Active) {
            return Err(AppError::Closed(
                "application admission is durably sealed".to_owned(),
            ));
        }
        Ok((record, revision))
    }

    pub(crate) fn definition(&self, id: &WorkflowId) -> Result<Arc<CompiledDefinition>, AppError> {
        self.definitions
            .get(id)
            .and_then(|revisions| revisions.last())
            .cloned()
            .ok_or_else(|| {
                AppError::NotFound("workflow is not configured in this application".to_owned())
            })
    }

    pub(crate) fn definition_revision(
        &self,
        id: &WorkflowId,
        revision: &str,
    ) -> Result<Arc<CompiledDefinition>, AppError> {
        self.definitions
            .get(id)
            .and_then(|revisions| {
                revisions
                    .iter()
                    .find(|definition| definition.revision() == revision)
            })
            .cloned()
            .ok_or_else(|| {
                AppError::Conflict(
                    "durable run pins an unavailable workflow definition revision".into(),
                )
            })
    }

    pub(crate) fn definition_for(
        &self,
        record: &crate::workflow::run::RunRecord,
    ) -> Result<Arc<CompiledDefinition>, AppError> {
        let definition =
            self.definition_revision(&record.workflow_id, &record.definition_revision)?;
        if self.agent_revisions(
            &definition,
            &record.child_revisions,
            &record.agent_revisions,
        )? != record.agent_revisions
        {
            return Err(AppError::Integration(
                "durable workflow agent pins do not match reachable definitions".into(),
            ));
        }
        Ok(definition)
    }

    pub(crate) fn child_revisions(
        &self,
        definition: &CompiledDefinition,
    ) -> Result<BTreeMap<WorkflowId, String>, AppError> {
        let mut pending = vec![definition];
        let mut visited = BTreeSet::new();
        let mut selected = BTreeMap::new();
        while let Some(definition) = pending.pop() {
            if !visited.insert((definition.id(), definition.revision())) {
                continue;
            }
            if definition.has_dynamic_children() {
                return Ok(self
                    .definitions
                    .iter()
                    .filter_map(|(id, definitions)| {
                        definitions
                            .last()
                            .map(|child| (id.clone(), child.revision().to_owned()))
                    })
                    .collect());
            }
            for (workflow, revision) in definition.child_targets() {
                let id = WorkflowId::from_str(workflow)?;
                let definitions = self
                    .definitions
                    .get(&id)
                    .ok_or_else(|| AppError::NotFound("child workflow is not configured".into()))?;
                let child = match revision {
                    Some(revision) => definitions
                        .iter()
                        .find(|child| child.revision() == revision),
                    None => definitions.last(),
                }
                .ok_or_else(|| {
                    AppError::Conflict("child workflow revision is not configured".into())
                })?;
                if revision.is_none() {
                    selected.insert(id, child.revision().to_owned());
                }
                pending.push(child.as_ref());
            }
        }
        Ok(selected)
    }

    pub(crate) fn agent_revisions(
        &self,
        definition: &CompiledDefinition,
        children: &BTreeMap<WorkflowId, String>,
        selected: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, AppError> {
        let mut required = BTreeSet::new();
        let mut pending = vec![definition];
        let mut visited = BTreeSet::new();
        while let Some(definition) = pending.pop() {
            if !visited.insert((definition.id(), definition.revision())) {
                continue;
            }
            if definition.has_dynamic_children() {
                return Ok(selected.clone());
            }
            required.extend(definition.agent_keys());
            for (workflow, revision) in definition.child_targets() {
                let id = WorkflowId::from_str(workflow)?;
                let revision = revision
                    .or_else(|| children.get(&id).map(String::as_str))
                    .ok_or_else(|| {
                        AppError::Integration("required child definition pin is unavailable".into())
                    })?;
                let child = self
                    .definitions
                    .get(&id)
                    .and_then(|definitions| {
                        definitions
                            .iter()
                            .find(|child| child.revision() == revision)
                    })
                    .ok_or_else(|| {
                        AppError::Conflict("child workflow revision is not configured".into())
                    })?;
                pending.push(child.as_ref());
            }
        }
        required
            .into_iter()
            .map(|key| {
                let revision = selected.get(key).ok_or_else(|| {
                    AppError::Integration("required agent revision pin is unavailable".into())
                })?;
                Ok((key.to_owned(), revision.clone()))
            })
            .collect()
    }

    pub(crate) fn typing(&self) -> &T {
        &self.typing
    }

    pub(crate) fn hooks(&self) -> &H {
        &self.hooks
    }

    pub(crate) fn conditions(&self) -> &[C] {
        &self.conditions
    }

    pub(crate) fn graph(&self, definition: &CompiledDefinition) -> GraphDescription {
        definition.graph(&self.actions, &self.conditions)
    }

    pub(crate) fn action(&self, key: &str, revision: &str) -> Result<&X, AppError> {
        self.actions
            .iter()
            .find(|binding| {
                let declaration = binding.declaration();
                declaration.key == key && declaration.revision == revision
            })
            .ok_or_else(|| AppError::Integration("configured action binding is missing".into()))
    }

    pub(crate) fn child_executor(&self) -> &E {
        &self.executor
    }

    pub(crate) async fn agent(&self, key: &str, revision: &str) -> Result<Arc<A>, AppError> {
        self.current_generation_revision().await?;
        let agents = self.agents.read().await;
        agents
            .iter()
            .find(|spec| spec.key == key && spec.revision == revision)
            .map(|spec| Arc::clone(&spec.agent))
            .ok_or_else(|| {
                AppError::NotFound("workflow references an unavailable agent".to_owned())
            })
    }

    pub(crate) async fn agents(&self) -> Result<Vec<(String, String, Arc<A>)>, AppError> {
        self.current_generation_revision().await?;
        let mut agents = self
            .agents
            .read()
            .await
            .iter()
            .map(|spec| {
                (
                    spec.key.clone(),
                    spec.revision.clone(),
                    Arc::clone(&spec.agent),
                )
            })
            .collect::<Vec<_>>();
        agents.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        Ok(agents)
    }

    pub(crate) async fn validate_workspace(
        &self,
        record: &WorkspaceRecord,
        expected: &WorkspaceId,
    ) -> Result<(), AppError> {
        let agents = self.agents.read().await;
        // Finalization consumes this inventory. Its revocation is closure,
        // not evidence that the caller configured different Agent revisions.
        self.ensure_runtime_owned()?;
        record.validate(
            expected,
            &agents
                .iter()
                .map(|spec| (spec.key.as_str(), spec.revision.as_str()))
                .collect(),
        )
    }

    pub(crate) async fn has_active_runs(
        &self,
        workspace: Option<&WorkspaceId>,
    ) -> Result<bool, StorageFailure> {
        self.current_generation_record_for_recovery().await?;
        let mut active = false;
        for record in self.storage.list_all_runs_for_recovery().await? {
            if workspace.is_none_or(|workspace| &record.workspace_id == workspace)
                && !record.checkpoint.is_terminal()
            {
                if workspace.is_some() && record.checkpoint.has_unknown_effect() {
                    self.current_generation_record_for_recovery().await?;
                    return Err(StorageFailure::permanent(AppError::Conflict(
                        "workflow effect is unresolved".into(),
                    )));
                }
                active = true;
            }
        }
        self.current_generation_record_for_recovery().await?;
        Ok(active)
    }

    async fn is_quiescent(&self, active: Option<bool>) -> Result<bool, StorageFailure> {
        let active = match active {
            Some(active) => active,
            None => self.has_active_runs(None).await?,
        };
        let workspace_work = self.workspace_operations.has_work();
        if active || workspace_work || self.tasks.has_run_work(None) {
            return Ok(false);
        }
        for record in self
            .storage
            .list_workspaces()
            .await
            .map_err(StorageFailure::permanent)?
        {
            if !matches!(record.state, WorkspaceStateRecord::Active { .. }) {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "workspace reconciliation stopped before durable settlement".to_owned(),
                )));
            }
        }
        self.current_generation_record_for_recovery().await?;
        Ok(true)
    }

    pub(crate) async fn cancel_runs(
        self: &Arc<Self>,
        workspace: Option<&WorkspaceId>,
    ) -> Result<bool, StorageFailure> {
        self.current_generation_record_for_recovery().await?;
        let scoped = workspace.is_some();
        let mut active = false;
        for record in self.storage.list_all_runs_for_recovery().await? {
            if workspace.is_some_and(|workspace| &record.workspace_id != workspace)
                || record.checkpoint.is_terminal()
            {
                continue;
            }
            if scoped && record.checkpoint.has_unknown_effect() {
                self.current_generation_record_for_recovery().await?;
                return Err(StorageFailure::permanent(AppError::Conflict(
                    "workflow effect is unresolved".into(),
                )));
            }
            let outcome = async {
                crate::workflow::run::WorkflowRunRuntime::open(Arc::clone(self), record.key())
                    .await?
                    .cancel_unfenced()
                    .await
            }
            .await;
            let outcome = match outcome {
                Err(RuntimeOperationFailure::Storage(failure)) if failure.is_cleanup_failed() => {
                    return Err(failure);
                }
                outcome => outcome.map_err(RuntimeOperationFailure::into_error),
            };
            match outcome {
                Ok(snapshot)
                    if scoped
                        && snapshot.interactions.iter().any(|interaction| {
                            matches!(interaction, crate::workflow::Interaction::Unknown { .. })
                        }) =>
                {
                    return Err(StorageFailure::permanent(AppError::Conflict(
                        "workflow effect is unresolved".into(),
                    )));
                }
                Ok(snapshot) => match snapshot.status {
                    RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled => {}
                    _ => active = true,
                },
                Err(AppError::NotFound(_)) => {
                    self.current_generation_record_for_recovery().await?;
                }
                Err(error @ AppError::Conflict(_)) => {
                    let current = self
                        .storage
                        .load_run_for_recovery(&record.key())
                        .await?
                        .map(|(record, _)| record);
                    self.current_generation_record_for_recovery().await?;
                    match current {
                        Some(record) if record.checkpoint.is_terminal() => {}
                        Some(record) if record.checkpoint.is_stopping() => {
                            if let Ok(runtime) =
                                WorkflowRunRuntime::attach_record(Arc::clone(self), &record)
                            {
                                runtime.schedule(ScheduleCause::CheckpointReady(
                                    record.checkpoint.sequence,
                                ));
                            }
                            active = true;
                        }
                        _ => return Err(StorageFailure::permanent(error)),
                    }
                }
                Err(error) => return Err(StorageFailure::permanent(error)),
            }
        }
        self.current_generation_record_for_recovery().await?;
        Ok(active)
    }

    pub(crate) async fn release_workspace_retention(
        self: &Arc<Self>,
        workspace: &WorkspaceId,
    ) -> Result<(), StorageFailure> {
        self.current_generation_record_for_recovery().await?;
        for record in self.storage.list_all_runs_for_recovery().await? {
            if &record.workspace_id == workspace && record.checkpoint.is_terminal() {
                record.validate_recovery(self.definition_for(&record)?.as_ref())?;
                WorkflowRunRuntime::attach_record(Arc::clone(self), &record)?
                    .schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
            }
        }
        Ok(())
    }

    pub(crate) fn execution_handle(&self) -> Result<tokio::runtime::Handle, AppError> {
        self.ensure_runtime_owned()?;
        Ok(self.tasks.runtime().clone())
    }

    pub(crate) fn has_run_work(&self, key: &RunKey) -> bool {
        self.tasks.has_run_work(Some(key))
    }

    pub(crate) fn has_control_work(&self, key: &RunKey, sequence: u64) -> bool {
        self.tasks.has_control_work(key, sequence)
    }

    pub(crate) fn retarget_control(
        &self,
        id: tokio::task::Id,
        key: &RunKey,
        prior: u64,
        next: u64,
    ) -> Result<(), AppError> {
        self.ensure_runtime_owned()?;
        if self.tasks.retarget_control(id, key, prior, next) {
            Ok(())
        } else {
            Err(AppError::Closed(
                "native control admission ownership changed".into(),
            ))
        }
    }

    pub(crate) fn has_child_work(&self, key: &RunKey) -> bool {
        self.tasks.has_child_work(key)
    }

    pub(crate) fn has_resolution_work(&self, key: &RunKey, control: Option<u64>) -> bool {
        self.tasks.has_resolution_work(key, control)
    }

    pub(crate) fn runtime_changes(&self) -> watch::Receiver<()> {
        self.tasks.changes()
    }

    pub(crate) fn is_draining_accepted_work(&self) -> bool {
        matches!(
            &*self
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            AppLifecycle::Draining {
                policy: DrainPolicy::Wait,
                ..
            } | AppLifecycle::Cancelling { .. }
        )
    }

    pub(crate) fn spawn_workspace<F>(
        &self,
        workspace: WorkspaceId,
        identity: Arc<WorkspaceOperationIdentity>,
        admission: Option<tokio::sync::OwnedSemaphorePermit>,
        future: F,
    ) -> bool
    where
        F: std::future::Future<Output = FinalResult> + Send + 'static,
    {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !lifecycle.accepts_runtime_work() {
            return false;
        }
        self.tasks
            .spawn_workspace(workspace, identity, admission, future)
    }

    pub(crate) fn spawn_run<F>(
        self: &Arc<Self>,
        key: RunKey,
        kind: RunTaskKind,
        admission: Option<tokio::sync::OwnedSemaphorePermit>,
        future: F,
    ) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !lifecycle.accepts_runtime_work() {
            return false;
        }
        let Some(owner) = WorkflowRunRuntime::existing(Arc::clone(self), key.clone()) else {
            return false;
        };
        self.tasks.spawn_run(owner, kind, admission, future)
    }

    pub(crate) fn spawn_driver(
        self: &Arc<Self>,
        key: RunKey,
        start_after: Duration,
        command: DriverCommand,
    ) -> bool {
        let outcome = {
            let lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !lifecycle.accepts_runtime_work() {
                return false;
            }
            let Some(owner) = WorkflowRunRuntime::existing(Arc::clone(self), key.clone()) else {
                return false;
            };
            self.tasks
                .spawn_driver(Arc::downgrade(self), key, owner, start_after, command)
        };
        match outcome {
            Ok(accepted) => accepted,
            Err(failure) => {
                self.tasks_failed(failure);
                false
            }
        }
    }

    pub(crate) fn mark_runtime_changed(&self) {
        self.tasks.mark_changed();
    }

    pub(crate) fn reserve_caller(&self) -> Result<tokio::sync::OwnedSemaphorePermit, AppError> {
        self.ensure_runtime_owned()?;
        self.tasks.reserve_caller()
    }

    pub(crate) fn admit(&self) -> Result<(), AppError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if lifecycle.is_active() {
            Ok(())
        } else {
            Err(AppError::Closed(
                "application admission is sealed".to_owned(),
            ))
        }
    }

    pub(crate) async fn ensure_open(&self) -> Result<(), AppError> {
        let closed = {
            let lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lifecycle.is_closed()
        };
        if closed {
            return Err(AppError::Closed(
                "application generation is closed".to_owned(),
            ));
        }
        self.current_generation_revision().await.map(|_| ())
    }

    pub(crate) fn ensure_runtime_owned(&self) -> Result<(), AppError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !lifecycle.accepts_runtime_work() {
            Err(AppError::Closed(
                "application runtime ownership is revoked".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_snapshot_read(&self) -> Result<(), AppError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if lifecycle.allows_snapshot_read() {
            Ok(())
        } else {
            Err(AppError::Closed(
                "application snapshot observation is revoked".to_owned(),
            ))
        }
    }

    pub(crate) fn request_drop(self: &Arc<Self>) {
        let completion = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let AppLifecycle::Draining {
                policy: DrainPolicy::RejectIfActive,
                root_dropped,
                ..
            } = &mut *lifecycle
            {
                *root_dropped = true;
                return;
            }
            if !lifecycle.is_active() {
                return;
            }
            let (sender, receiver) = watch::channel(None);
            *lifecycle = AppLifecycle::Finalizing {
                mode: ShutdownMode::Cancel,
                completion: receiver,
                progress: Finalization::new(true),
            };
            sender
        };
        self.runs.close();
        self.workspace_operations.close();
        self.tasks.stop_runtime();
        let runtime = Arc::clone(self);
        self.tasks
            .spawn_finalizer(Arc::clone(self), completion, true, async move {
                runtime.finalize_cleanup().await
            });
    }

    pub(super) fn tasks_failed(self: &Arc<Self>, failure: FinalResult) {
        let completion = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &mut *lifecycle {
                AppLifecycle::Finalizing { progress, .. } => {
                    progress.failure.get_or_insert(failure);
                    None
                }
                AppLifecycle::Closed { .. } => return,
                AppLifecycle::Active => {
                    let (sender, receiver) = watch::channel(None);
                    let mut progress = Finalization::new(false);
                    progress.failure = Some(failure);
                    *lifecycle = AppLifecycle::Finalizing {
                        mode: ShutdownMode::Cancel,
                        completion: receiver,
                        progress,
                    };
                    Some(sender)
                }
                AppLifecycle::Draining {
                    completion,
                    root_dropped,
                    ..
                } => {
                    let mut progress = Finalization::new(*root_dropped);
                    progress.failure = Some(failure);
                    *lifecycle = AppLifecycle::Finalizing {
                        mode: ShutdownMode::Drain,
                        completion: completion.clone(),
                        progress,
                    };
                    None
                }
                AppLifecycle::Cancelling { completion } => {
                    let mut progress = Finalization::new(false);
                    progress.failure = Some(failure);
                    *lifecycle = AppLifecycle::Finalizing {
                        mode: ShutdownMode::Cancel,
                        completion: completion.clone(),
                        progress,
                    };
                    None
                }
            }
        };
        self.runs.close();
        self.workspace_operations.close();
        self.tasks.stop_runtime();
        if let Some(completion) = completion {
            let runtime = Arc::clone(self);
            self.tasks
                .spawn_finalizer(Arc::clone(self), completion, false, async move {
                    runtime.finalize_cleanup().await
                });
        }
        self.tasks.mark_changed();
    }

    async fn begin_shutdown(
        self: &Arc<Self>,
        requested: ShutdownRequest,
    ) -> Result<watch::Receiver<Option<FinalResult>>, AppError> {
        let mut publication = None;
        loop {
            let transition = {
                let mut lifecycle = self
                    .lifecycle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let current = std::mem::replace(&mut *lifecycle, AppLifecycle::Active);
                let transition = match current {
                    AppLifecycle::Active => {
                        let (sender, receiver) = watch::channel(None);
                        let runtime = Arc::clone(self);
                        self.tasks
                            .spawn_finalizer(Arc::clone(self), sender, false, async move {
                                runtime.finalize_inner().await
                            });
                        let next = match requested {
                            ShutdownRequest::Cancel => AppLifecycle::Cancelling {
                                completion: receiver.clone(),
                            },
                            ShutdownRequest::Drain(policy) => AppLifecycle::Draining {
                                policy,
                                root_dropped: false,
                                completion: receiver.clone(),
                            },
                        };
                        Some((next, receiver, false))
                    }
                    AppLifecycle::Draining {
                        policy,
                        completion,
                        root_dropped,
                    } => {
                        let receiver = completion.clone();
                        let next = match requested {
                            ShutdownRequest::Cancel => AppLifecycle::Cancelling { completion },
                            ShutdownRequest::Drain(DrainPolicy::Wait) => AppLifecycle::Draining {
                                policy: DrainPolicy::Wait,
                                root_dropped,
                                completion,
                            },
                            ShutdownRequest::Drain(DrainPolicy::RejectIfActive)
                                if policy == DrainPolicy::RejectIfActive =>
                            {
                                AppLifecycle::Draining {
                                    policy,
                                    root_dropped,
                                    completion,
                                }
                            }
                            ShutdownRequest::Drain(DrainPolicy::RejectIfActive) => {
                                *lifecycle = AppLifecycle::Draining {
                                    policy,
                                    root_dropped,
                                    completion,
                                };
                                return Err(AppError::Conflict(
                                    "application shutdown mode cannot de-escalate".to_owned(),
                                ));
                            }
                        };
                        Some((next, receiver, false))
                    }
                    AppLifecycle::Cancelling { completion } => {
                        if requested != ShutdownRequest::Cancel {
                            *lifecycle = AppLifecycle::Cancelling { completion };
                            return Err(AppError::Conflict(
                                "application shutdown mode cannot de-escalate".to_owned(),
                            ));
                        }
                        let receiver = completion.clone();
                        Some((AppLifecycle::Cancelling { completion }, receiver, false))
                    }
                    AppLifecycle::Finalizing {
                        mode: current,
                        completion,
                        progress,
                    } => {
                        if current == ShutdownMode::Drain
                            && requested == ShutdownRequest::Cancel
                            && publication.is_none()
                        {
                            *lifecycle = AppLifecycle::Finalizing {
                                mode: current,
                                completion,
                                progress,
                            };
                            None
                        } else {
                            let (mode, publish_escalation) = match (current, requested) {
                                (ShutdownMode::Cancel, ShutdownRequest::Cancel) => {
                                    (ShutdownMode::Cancel, false)
                                }
                                (ShutdownMode::Cancel, _) => {
                                    *lifecycle = AppLifecycle::Finalizing {
                                        mode: current,
                                        completion,
                                        progress,
                                    };
                                    return Err(AppError::Conflict(
                                        "application shutdown mode cannot de-escalate".to_owned(),
                                    ));
                                }
                                (ShutdownMode::Drain, ShutdownRequest::Cancel) => {
                                    (ShutdownMode::Cancel, true)
                                }
                                (ShutdownMode::Drain, ShutdownRequest::Drain(_)) => {
                                    (ShutdownMode::Drain, false)
                                }
                            };
                            let receiver = completion.clone();
                            Some((
                                AppLifecycle::Finalizing {
                                    mode,
                                    completion,
                                    progress,
                                },
                                receiver,
                                publish_escalation,
                            ))
                        }
                    }
                    AppLifecycle::Closed { outcome, dropping } => {
                        let receiver = watch::channel(Some(outcome.clone())).1;
                        Some((AppLifecycle::Closed { outcome, dropping }, receiver, false))
                    }
                };
                match transition {
                    Some((next, completion, publish_escalation)) => {
                        *lifecycle = next;
                        self.tasks.mark_changed();
                        Some((completion, publish_escalation))
                    }
                    None => None,
                }
            };
            let Some((completion, publish_escalation)) = transition else {
                publication = Some(self.shutdown_publication.lock().await);
                continue;
            };
            if publish_escalation {
                debug_assert!(publication.is_some());
                self.publish_shutdown(ShutdownRequest::Cancel)
                    .await
                    .map_err(StorageFailure::into_error)?;
            }
            return Ok(completion);
        }
    }

    fn revert_rejected_shutdown(&self) -> bool {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = std::mem::replace(&mut *lifecycle, AppLifecycle::Active);
        match current {
            AppLifecycle::Draining {
                policy: DrainPolicy::RejectIfActive,
                completion,
                root_dropped,
            } => {
                if root_dropped {
                    *lifecycle = AppLifecycle::Finalizing {
                        mode: ShutdownMode::Drain,
                        completion,
                        progress: Finalization::new(true),
                    };
                    false
                } else {
                    true
                }
            }
            current => {
                *lifecycle = current;
                false
            }
        }
    }

    fn finalization_dropping(&self) -> bool {
        matches!(
            &*self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            AppLifecycle::Finalizing {
                progress: Finalization { dropping: true, .. },
                ..
            }
        )
    }

    fn transition_to_finalizing(&self) -> Result<ShutdownMode, FinalResult> {
        self.lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .transition_to_finalizing()
    }

    fn try_graceful_finalization(&self) -> Result<Option<ShutdownMode>, FinalResult> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Admission takes the lifecycle lock before spawning accepted work.
        // An earlier empty sample cannot revoke that work or its result.
        if matches!(
            &*lifecycle,
            AppLifecycle::Draining { .. } | AppLifecycle::Cancelling { .. }
        ) && (self.tasks.has_run_work(None) || self.workspace_operations.has_work())
        {
            return Ok(None);
        }
        lifecycle.transition_to_finalizing().map(Some)
    }

    async fn publish_shutdown(&self, request: ShutdownRequest) -> Result<(), StorageFailure> {
        let mode = request.mode();
        loop {
            let (mut record, revision) = self.current_generation_record_for_recovery().await?;
            match (&record.shutdown, mode) {
                (ShutdownRecord::Draining, ShutdownMode::Drain)
                | (ShutdownRecord::Cancelling, _) => return Ok(()),
                (ShutdownRecord::Closed, _) => {
                    return Err(StorageFailure::permanent(AppError::Closed(
                        "application generation is already closed".to_owned(),
                    )));
                }
                (ShutdownRecord::Active, ShutdownMode::Drain) => {
                    if request.rejects_active() && !self.is_quiescent(None).await? {
                        return Err(StorageFailure::permanent(AppError::Busy(
                            "application still owns accepted work".to_owned(),
                        )));
                    }
                    record.shutdown = ShutdownRecord::Draining;
                }
                (ShutdownRecord::Active | ShutdownRecord::Draining, ShutdownMode::Cancel) => {
                    record.shutdown = ShutdownRecord::Cancelling;
                }
            }
            let mutation =
                AppMutation::advance_lifecycle(revision, self.generation, record.shutdown.clone())
                    .map_err(StorageFailure::permanent)?;
            match self.storage.commit(mutation).await {
                AppCommitOutcome::Applied => return Ok(()),
                AppCommitOutcome::Rejected(failure) if failure.is_retryable_conflict() => {
                    tokio::task::yield_now().await
                }
                AppCommitOutcome::Unknown(_) => match self.storage.load_app().await {
                    Ok(Some((observed, _))) => {
                        if observed.generation == record.generation
                            && observed.shutdown == record.shutdown
                        {
                            return Ok(());
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(None) => {
                        return Err(StorageFailure::permanent(AppError::Integration(
                            "application shutdown receipt lost its durable record".to_owned(),
                        )));
                    }
                    Err(error) => return Err(StorageFailure::permanent(error)),
                },
                AppCommitOutcome::Rejected(failure) => {
                    return Err(if failure.is_cleanup_failed() {
                        failure
                    } else {
                        StorageFailure::permanent(failure.into_error())
                    });
                }
            }
        }
    }

    async fn finalize_inner(self: &Arc<Self>) -> FinalResult {
        let mut changes = self.tasks.changes();
        let mut published_mode = None;
        let mut retry_delay = RECONCILIATION_INITIAL_DELAY;
        let mut superseded = false;
        let mut failure = 'finalizing: loop {
            let request = {
                let lifecycle = self
                    .lifecycle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if matches!(&*lifecycle, AppLifecycle::Finalizing { .. }) {
                    break 'finalizing None;
                }
                let Some(request) = lifecycle.shutdown_request() else {
                    break 'finalizing Some(FinalResult::Conflict(
                        "application finalizer lost lifecycle ownership".to_owned(),
                    ));
                };
                request
            };
            let mode = request.mode();
            let current = if published_mode != Some(mode) {
                let published = match self.publish_shutdown(request).await {
                    Err(StorageFailure::Permanent(error @ AppError::Busy(_)))
                        if request.rejects_active() =>
                    {
                        if self.revert_rejected_shutdown() {
                            return FinalResult::from(&error);
                        }
                        if self.finalization_dropping() {
                            return self.finalize_cleanup().await;
                        }
                        continue;
                    }
                    published => published,
                };
                match published {
                    Ok(()) if mode == ShutdownMode::Cancel => {
                        self.cancel_runs(None).await.map(Some)
                    }
                    result => result.map(|()| None),
                }
            } else {
                Ok(None)
            };
            let observation = match current {
                Ok(active) => {
                    published_mode = Some(mode);
                    let observed_mode = {
                        let lifecycle = self
                            .lifecycle
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        lifecycle.shutdown_mode()
                    };
                    if observed_mode != Some(mode) {
                        continue;
                    }
                    self.is_quiescent(active).await
                }
                Err(failure) => Err(failure),
            };
            let quiescent = match observation {
                Ok(quiescent) => {
                    retry_delay = RECONCILIATION_INITIAL_DELAY;
                    quiescent
                }
                Err(failure) if failure.is_recoverable() => {
                    // Retry read-only inventory movement under this same
                    // accepted finalizer. The caller's timeout only bounds
                    // observation; permanent failures and uncertain writes
                    // do not enter this retry path.
                    tokio::select! {
                        changed = changes.changed() => {
                            if changed.is_err() {
                                break 'finalizing Some(FinalResult::Closed(
                                "application runtime observation closed".into()));
                            }
                        }
                        _ = tokio::time::sleep(retry_delay) => {}
                    }
                    retry_delay = retry_delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                    continue;
                }
                Err(failure) => {
                    let primary = FinalResult::from(failure.error());
                    let _ = self.transition_to_finalizing();
                    self.retain_finalization_failure(primary.clone());
                    superseded = matches!(self.owns_current_generation().await, Ok(false));
                    break 'finalizing (!superseded).then_some(primary);
                }
            };
            if quiescent {
                match self.try_graceful_finalization() {
                    Ok(Some(owned)) if owned == mode => break 'finalizing None,
                    Ok(Some(_)) => continue,
                    Ok(None) => {}
                    Err(error) => break 'finalizing Some(error),
                }
            }
            if changes.changed().await.is_err() {
                break Some(FinalResult::Closed(
                    "application runtime observation closed".to_owned(),
                ));
            }
        };
        if let Err(error) = self.transition_to_finalizing() {
            failure.get_or_insert(error);
        }
        {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let AppLifecycle::Finalizing { progress, .. } = &mut *lifecycle {
                progress.published_mode = published_mode;
                progress.superseded = superseded;
                if let Some(failure) = failure {
                    progress.failure.get_or_insert(failure);
                }
            }
        }
        self.finalize_cleanup().await
    }

    pub(super) fn finalization_closed(&self) -> bool {
        self.lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_closed()
    }

    fn retain_finalization_failure(&self, failure: FinalResult) {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let AppLifecycle::Finalizing { progress, .. } = &mut *lifecycle {
            progress.failure.get_or_insert(failure);
        }
    }

    fn advance_finalization(&self, stage: FinalizationStage) {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let AppLifecycle::Finalizing { progress, .. } = &mut *lifecycle {
            progress.stage = stage;
        }
    }

    pub(super) fn resume_finalizer(
        self: &Arc<Self>,
        completion: watch::Sender<Option<FinalResult>>,
        dropping: bool,
    ) {
        let _ = self.transition_to_finalizing();
        self.retain_finalization_failure(FinalResult::Integration(
            "application owned task panicked during cleanup".to_owned(),
        ));
        let runtime = Arc::clone(self);
        self.tasks
            .spawn_finalizer(Arc::clone(self), completion, dropping, async move {
                runtime.finalize_cleanup().await
            });
    }

    async fn finalize_cleanup(&self) -> FinalResult {
        loop {
            let (stage, dropping, published_mode, superseded) = {
                let lifecycle = self
                    .lifecycle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match &*lifecycle {
                    AppLifecycle::Finalizing { progress, .. } => (
                        progress.stage,
                        progress.dropping,
                        progress.published_mode,
                        progress.superseded,
                    ),
                    AppLifecycle::Closed { outcome, .. } => return outcome.clone(),
                    _ => {
                        return FinalResult::Conflict(
                            "application finalizer lost lifecycle ownership".to_owned(),
                        );
                    }
                }
            };
            match stage {
                FinalizationStage::Tasks => {
                    self.workspace_operations.close();
                    self.runs.close();
                    self.tasks.close().await;
                    self.advance_finalization(FinalizationStage::Agents);
                }
                FinalizationStage::Agents => {
                    let spec = {
                        let mut agents = self.agents.write().await;
                        let next = agents
                            .iter()
                            .enumerate()
                            .min_by(|(_, left), (_, right)| {
                                left.key
                                    .cmp(&right.key)
                                    .then_with(|| left.revision.cmp(&right.revision))
                            })
                            .map(|(index, _)| index);
                        next.map(|index| agents.remove(index))
                    };
                    if let Some(spec) = spec {
                        if let Some(error) = Self::shutdown_agent(spec).await {
                            self.retain_finalization_failure(FinalResult::from(&error));
                        }
                    } else {
                        self.advance_finalization(FinalizationStage::Publication);
                    }
                }
                FinalizationStage::Publication if !dropping => {
                    self.advance_finalization(FinalizationStage::Publishing);
                    self.finalize_publication(published_mode, superseded).await;
                    self.advance_finalization(FinalizationStage::Complete);
                }
                FinalizationStage::Publication | FinalizationStage::Publishing => {
                    // A panicking durable publication has unknown effect. Cleanup has
                    // completed; retrying this same phase cannot add safe evidence.
                    self.advance_finalization(FinalizationStage::Complete);
                }
                FinalizationStage::Complete => {
                    let mut lifecycle = self
                        .lifecycle
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let current = std::mem::replace(&mut *lifecycle, AppLifecycle::Active);
                    let AppLifecycle::Finalizing { progress, .. } = current else {
                        *lifecycle = current;
                        return FinalResult::Conflict(
                            "application finalizer lost lifecycle ownership".to_owned(),
                        );
                    };
                    let outcome = progress.failure.unwrap_or_else(|| {
                        if progress.dropping {
                            FinalResult::Closed(
                                "application handle was dropped before shutdown".to_owned(),
                            )
                        } else {
                            FinalResult::Succeeded
                        }
                    });
                    *lifecycle = AppLifecycle::Closed {
                        outcome: outcome.clone(),
                        dropping: progress.dropping,
                    };
                    return outcome;
                }
            }
        }
    }

    async fn finalize_publication(
        &self,
        mut published_mode: Option<ShutdownMode>,
        superseded: bool,
    ) {
        let _publication = self.shutdown_publication.lock().await;
        loop {
            let mode = {
                let lifecycle = self
                    .lifecycle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                lifecycle.shutdown_mode()
            };
            let Some(mode) = mode else {
                self.retain_finalization_failure(FinalResult::Conflict(
                    "application finalizer lost lifecycle ownership".to_owned(),
                ));
                return;
            };
            if published_mode != Some(mode) {
                if let Err(failure) = self.publish_shutdown(mode.into()).await {
                    self.retain_finalization_failure(FinalResult::from(failure.error()));
                    break;
                }
                published_mode = Some(mode);
                continue;
            }
            tokio::task::yield_now().await;
            let confirmed = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .shutdown_mode();
            if confirmed == Some(mode) {
                break;
            }
        }
        if !superseded && let Err(error) = self.publish_closed().await {
            self.retain_finalization_failure(FinalResult::from(&error));
        }
        // Keep the publication gate through live Closed so a late mode change
        // cannot be accepted after durable Closed already linearized.
        self.advance_finalization(FinalizationStage::Complete);
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = std::mem::replace(&mut *lifecycle, AppLifecycle::Active);
        if let AppLifecycle::Finalizing { progress, .. } = current {
            let outcome = progress.failure.unwrap_or(FinalResult::Succeeded);
            *lifecycle = AppLifecycle::Closed {
                outcome,
                dropping: progress.dropping,
            };
        } else {
            *lifecycle = current;
        }
    }

    async fn shutdown_agent(spec: AgentSpec<A>) -> Option<AppError> {
        match Arc::try_unwrap(spec.agent) {
            Ok(agent) => {
                match tokio::time::timeout(Duration::from_secs(30), agent.shutdown()).await {
                    Ok(Ok(())) => None,
                    Ok(Err(_)) | Err(_) => Some(AppError::Integration(
                        "application agent shutdown failed".to_owned(),
                    )),
                }
            }
            Err(_) => Some(AppError::Integration(
                "application agent ownership changed during shutdown".to_owned(),
            )),
        }
    }

    async fn publish_closed(&self) -> Result<(), AppError> {
        loop {
            let (mut record, revision) = self.generation_record().await?;
            if record.generation > self.generation {
                return Ok(());
            }
            if record.generation < self.generation {
                return Err(AppError::Integration(
                    "application generation regressed during finalization".to_owned(),
                ));
            }
            if matches!(&record.shutdown, ShutdownRecord::Closed) {
                return Ok(());
            }
            record.shutdown = ShutdownRecord::Closed;
            let mutation =
                AppMutation::advance_lifecycle(revision, self.generation, record.shutdown.clone())?;
            match self.storage.commit(mutation).await {
                AppCommitOutcome::Applied => return Ok(()),
                AppCommitOutcome::Rejected(failure) if failure.is_retryable_conflict() => {
                    tokio::task::yield_now().await
                }
                AppCommitOutcome::Unknown(_) => tokio::task::yield_now().await,
                AppCommitOutcome::Rejected(failure) => return Err(failure.into_error()),
            }
        }
    }

    async fn close_after_open_failure(&self) {
        self.workspace_operations.close();
        self.runs.close();
        self.tasks.close().await;
        let agents = std::mem::take(&mut *self.agents.write().await);
        if Self::shutdown_agents(agents).await.is_some() {
            AppTrace::event(
                || tracing::warn!(target: "app_sdk", event = "app.open.cleanup_failed"),
            );
        }
        self.tasks.finish();
        self.tasks.join().await;
    }

    async fn shutdown_agents(mut agents: Vec<AgentSpec<A>>) -> Option<AppError> {
        agents.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.revision.cmp(&right.revision))
        });
        let mut failure = None;
        for spec in agents {
            if let Some(error) = Self::shutdown_agent(spec).await {
                failure.get_or_insert(error);
            }
        }
        failure
    }

    fn validate_agents(agents: &[AgentSpec<A>]) -> Result<(), AppError> {
        let mut bindings = BTreeSet::new();
        for agent in agents {
            if SchemaText::node_or_state("agent key", &agent.key).is_err()
                || SchemaText::revision(&agent.revision).is_err()
            {
                return Err(AppError::InvalidInput(
                    "configured agent key or revision is invalid".to_owned(),
                ));
            }
            if !bindings.insert((&agent.key, &agent.revision)) {
                return Err(AppError::InvalidInput(
                    "configured agent key and revision must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_actions(actions: &[X]) -> Result<(), AppError> {
        let mut bindings = BTreeSet::new();
        for action in actions {
            let declaration = action.declaration();
            declaration.validate()?;
            if !bindings.insert((&declaration.key, &declaration.revision)) {
                return Err(AppError::InvalidInput(
                    "configured action key and revision must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_conditions(conditions: &[C]) -> Result<(), AppError> {
        let mut bindings = BTreeSet::new();
        for condition in conditions {
            let declaration = condition.declaration();
            declaration.validate()?;
            if !bindings.insert((&declaration.key, &declaration.revision)) {
                return Err(AppError::InvalidInput(
                    "configured condition key and revision must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_definitions(
        workflows: Vec<crate::workflow::WorkflowDefinition>,
    ) -> Result<BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>, AppError> {
        let mut output: BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>> = BTreeMap::new();
        for workflow in workflows {
            let compiled = workflow.into_compiled();
            let id = WorkflowId::from_str(compiled.id())?;
            let revisions = output.entry(id).or_default();
            if let Some(existing) = revisions
                .iter()
                .find(|item| item.revision() == compiled.revision())
            {
                return Err(if existing.fingerprint() == compiled.fingerprint() {
                    AppError::InvalidInput(
                        "configured workflow id and revision must be unique".into(),
                    )
                } else {
                    AppError::Conflict(
                        "configured workflow revision has different canonical content".into(),
                    )
                });
            }
            revisions.push(Arc::new(compiled));
        }
        let selected = output
            .iter()
            .filter_map(|(id, revisions)| {
                revisions
                    .last()
                    .map(|definition| (id.clone(), definition.revision().to_owned()))
            })
            .collect::<BTreeMap<_, _>>();
        for definitions in output.values() {
            if let Some(definition) = definitions.last() {
                definition.validate_child_relationships(&output, &selected)?;
            }
        }
        Ok(output)
    }

    fn validate_definition_agents(
        definitions: &BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
        agents: &[AgentSpec<A>],
    ) -> Result<(), AppError> {
        if definitions
            .values()
            .flatten()
            .flat_map(|definition| definition.agent_keys())
            .any(|key| !agents.iter().any(|spec| spec.key == key))
        {
            Err(AppError::InvalidInput(
                "workflow references an unconfigured agent".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn validate_definition_inventory_size(
        definitions: &BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
    ) -> Result<(), AppError> {
        definitions
            .values()
            .flatten()
            .try_fold(0usize, |total, definition| {
                total
                    .checked_add(definition.id().len())
                    .and_then(|total| total.checked_add(definition.fingerprint().len()))
            })
            .filter(|total| *total <= MAX_DEFINITION_INVENTORY_BYTES)
            .ok_or_else(|| {
                AppError::InvalidInput(
                    "workflow definition inventory exceeds the private bound".to_owned(),
                )
            })?;
        Ok(())
    }

    fn validate_timeout(timeout: Duration) -> Result<Duration, AppError> {
        if timeout.is_zero() || timeout > MAX_SHUTDOWN {
            Err(AppError::InvalidInput(
                "shutdown timeout is outside the private bound".to_owned(),
            ))
        } else {
            Ok(timeout)
        }
    }
}

impl FinalResult {
    pub(crate) fn from(error: &AppError) -> Self {
        match error {
            AppError::Conflict(message) => Self::Conflict(message.clone()),
            AppError::Busy(message) => Self::Busy(message.clone()),
            AppError::Closed(message) => Self::Closed(message.clone()),
            AppError::Timeout(message) => Self::Timeout(message.clone()),
            AppError::InvalidInput(message)
            | AppError::NotFound(message)
            | AppError::Integration(message) => Self::Integration(message.clone()),
        }
    }

    pub(crate) fn to_public(&self) -> Result<(), AppError> {
        match self {
            Self::Succeeded => Ok(()),
            Self::Conflict(message) => Err(AppError::Conflict(message.clone())),
            Self::Busy(message) => Err(AppError::Busy(message.clone())),
            Self::Closed(message) => Err(AppError::Closed(message.clone())),
            Self::Timeout(message) => Err(AppError::Timeout(message.clone())),
            Self::Integration(message) => Err(AppError::Integration(message.clone())),
        }
    }
}
