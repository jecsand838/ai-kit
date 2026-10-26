use std::collections::BTreeMap;

use agent_sdk::AgentBackend;
use storage_sdk::StateStoreBackend;

use crate::workflow::{
    ActionBackend, ChildExecutor, ConditionBackend, LifecycleHooks, StateTyping,
};

use crate::AppError;
use crate::app::{AppRuntime, MAX_RUNS};
use crate::storage::StorageFailure;
use crate::workflow::run::RunRecord;
use crate::workspace::{MAX_WORKSPACES, WorkspaceRecord};

pub(crate) struct AppInventory {
    workspaces: Vec<WorkspaceRecord>,
    runs: Vec<RunRecord>,
}

impl AppInventory {
    pub(crate) async fn load<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        runtime: &AppRuntime<CS, A, X, C, T, H, E>,
    ) -> Result<Self, AppError> {
        runtime.current_generation_revision().await?;
        let runs = runtime.storage().list_all_runs().await?;
        let workspaces = runtime.storage().list_workspaces().await?;
        let (application, _) = runtime.current_generation_record().await?;
        let inventory = Self { workspaces, runs };
        inventory.validate(runtime, &application).await?;
        runtime.current_generation_revision().await?;
        Ok(inventory)
    }

    async fn validate<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        &self,
        runtime: &AppRuntime<CS, A, X, C, T, H, E>,
        application: &crate::app::AppRecord,
    ) -> Result<(), AppError> {
        if self.workspaces.len() > MAX_WORKSPACES {
            return Err(AppError::Integration(
                "durable workspace inventory exceeds the private bound".to_owned(),
            ));
        }
        if self.runs.len() > MAX_RUNS {
            return Err(AppError::Integration(
                "durable run inventory exceeds the private bound".to_owned(),
            ));
        }
        if application.run_count != self.runs.len() {
            return Err(AppError::Integration(
                "durable run-capacity accounting is inconsistent".to_owned(),
            ));
        }
        let run_index = self
            .runs
            .iter()
            .map(|record| (record.key(), record))
            .collect::<BTreeMap<_, _>>();
        let workspace_index = self
            .workspaces
            .iter()
            .map(|workspace| (&workspace.id, workspace))
            .collect::<BTreeMap<_, _>>();
        if run_index.len() != self.runs.len() || workspace_index.len() != self.workspaces.len() {
            return Err(AppError::Integration(
                "durable app inventory contains duplicate identities".to_owned(),
            ));
        }
        for workspace in &self.workspaces {
            runtime.validate_workspace(workspace, &workspace.id).await?;
        }
        for record in &self.runs {
            let definition = runtime.definition_for(record)?;
            record.validate_recovery(&definition)?;
            // Capacity tracks active runs. An active parent can still own a
            // terminal child whose capacity has already been released.
            let mut retained = Vec::new();
            let mut children = BTreeMap::new();
            for key in record.admitted_child_keys() {
                if let Some(child) = run_index.get(key) {
                    children.insert(child.key(), *child);
                    continue;
                }
                let (child, _) = runtime.storage().load_run(key).await?.ok_or_else(|| {
                    AppError::Integration("admitted child record is missing".into())
                })?;
                if !child.checkpoint.is_terminal()
                    || runtime
                        .storage()
                        .run_is_active(&child)
                        .await
                        .map_err(StorageFailure::into_error)?
                {
                    return Err(AppError::Integration(
                        "admitted child is missing from active capacity inventory".into(),
                    ));
                }
                child.validate_recovery(runtime.definition_for(&child)?.as_ref())?;
                retained.push(child);
            }
            for child in &retained {
                children.insert(child.key(), child);
            }
            record.validate_admitted_children(&definition, &children)?;
            if !record.checkpoint.is_terminal() {
                let workspace = workspace_index.get(&record.workspace_id).ok_or_else(|| {
                    AppError::Integration("active durable run workspace is unavailable".to_owned())
                })?;
                if workspace.revision != record.workspace_revision
                    || !workspace.has_recovery_agents(&record.agent_revisions)?
                {
                    return Err(AppError::Conflict(
                        "active durable run workspace materialization is unavailable".to_owned(),
                    ));
                }
                if let Some(parent) = &record.parent {
                    let parent_record = run_index
                        .get(&crate::workflow::run::RunKey::new(
                            record.workspace_id.clone(),
                            parent.workflow_id.clone(),
                            parent.run_id.clone(),
                            parent.occurrence.clone(),
                        ))
                        .ok_or_else(|| {
                            AppError::Integration(
                                "active child run parent is unavailable".to_owned(),
                            )
                        })?;
                    record.validate_child(
                        parent_record,
                        runtime.definition_for(parent_record)?.as_ref(),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn workspaces(&self) -> &[WorkspaceRecord] {
        &self.workspaces
    }

    pub(crate) fn recoverable_runs(self) -> Vec<RunRecord> {
        self.runs
            .into_iter()
            .filter(|record| record.checkpoint.recoverable())
            .collect()
    }
}
