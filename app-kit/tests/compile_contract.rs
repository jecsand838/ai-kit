//! These generic consumers are checked by Cargo using its exact dependency graph.
//! They intentionally add no concrete supplied handle or Clone bound.

use agents_kit::AgentBackend;
use app_kit::workflow::{
    ActionBackend, ActionContext, ChildExecutor, ConditionBackend, LifecycleHooks, RunId,
    RunSelector, StateTyping, Workflow, WorkflowDefinition, WorkflowId, WorkflowRun,
};
use app_kit::{AgentSpec, App, AppConfig, AppError, Workspace, WorkspaceId};
use storage_kit::StateStoreBackend;

pub struct DirectComposition;

impl DirectComposition {
    pub fn action_context_copy(context: &ActionContext) -> ActionContext {
        ActionContext {
            workspace: context.workspace.clone(),
            workflow: context.workflow.clone(),
            run: context.run.clone(),
            occurrence: context.occurrence.clone(),
            checkpoint: context.checkpoint,
            action_ordinal: context.action_ordinal,
            node: context.node.clone(),
            attempt: context.attempt,
            mode: context.mode,
            parent: context.parent.clone(),
            parent_occurrence: context.parent_occurrence.clone(),
        }
    }

    pub async fn open_config<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        config: AppConfig<CS, A, X, C, T, H, E>,
    ) -> Result<App<CS, A, X, C, T, H, E>, AppError> {
        App::open(config).await
    }

    pub async fn workspace<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        app: &App<CS, A, X, C, T, H, E>,
        id: WorkspaceId,
    ) -> Result<Workspace<CS, A, X, C, T, H, E>, AppError> {
        app.workspace(id).await
    }

    pub async fn workflow<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        workspace: &Workspace<CS, A, X, C, T, H, E>,
        id: WorkflowId,
    ) -> Result<Workflow<CS, A, X, C, T, H, E>, AppError> {
        workspace.workflow(id).await
    }

    pub async fn run<
        CS: StateStoreBackend,
        A: AgentBackend,
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        workflow: &Workflow<CS, A, X, C, T, H, E>,
        id: RunId,
    ) -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError> {
        workflow.open_run(RunSelector::Latest(id)).await
    }

    pub async fn open<CS: StateStoreBackend, A: AgentBackend>(
        state: CS,
        agent: A,
        definitions: Vec<WorkflowDefinition>,
    ) -> Result<App<CS, A>, AppError> {
        App::open(AppConfig::new(
            state,
            vec![AgentSpec::new("worker".into(), "revision-1".into(), agent)],
            definitions,
        ))
        .await
    }
}
