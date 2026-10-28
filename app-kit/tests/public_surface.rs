//! Guards the exact adopted public surface and its crate boundaries.
//! The separate core_contract target preserves direct seven-parameter composition;
//! action_contract, execution_controls and child_executor_contract still require
//! the remaining execution behavior. This is not a completion gate.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt::{Debug, Display},
    fs,
    future::Future,
    hash::Hash,
};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    str::FromStr,
};

use agents_kit::notification::{ConversationNotification, NotificationResolution};
use agents_kit::{Agent, AgentBackend, ConversationBackend, WorkspaceBackend};
use app_kit::workflow::{
    ActionBackend, ActionContext, ActionDeclaration, ActionEvent, ActionMode, ActionOutcome,
    ActionRequest, Checkpoint, CheckpointKind, CheckpointSelector, ChildBatch, ChildCompletion,
    ChildExecutor, ConditionBackend, ConditionDeclaration, ExecutionEvent, ExecutionOptions,
    GraphComposition, GraphConnection, GraphDescription, GraphEdge, GraphFormat, GraphImport,
    GraphNode, InlineExecutor, Interaction, JsonState, LifecycleEvent, LifecycleHooks, NoActions,
    NoConditions, NoHooks, Resolution, ResumePosition, RunId, RunLineage, RunSelector, RunSnapshot,
    RunStatus, StateChange, StateSerialization, StateTyping, SyncExecution, Workflow,
    WorkflowDefinition, WorkflowFailure, WorkflowId, WorkflowRun, WorkflowState,
};
use app_kit::{
    AgentSpec, App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId, WorkspaceSnapshot,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use storage_kit::{StateStore, StateStoreBackend};

const PUBLIC_TYPES: [&str; 57] = [
    "ActionContext",
    "ActionDeclaration",
    "ActionEvent",
    "ActionMode",
    "ActionOutcome",
    "ActionRequest",
    "AgentSpec",
    "App",
    "AppConfig",
    "AppError",
    "Checkpoint",
    "CheckpointKind",
    "CheckpointSelector",
    "ChildBatch",
    "ChildCompletion",
    "ConditionDeclaration",
    "Execution",
    "ExecutionEvent",
    "ExecutionOptions",
    "GraphComposition",
    "GraphConnection",
    "GraphDescription",
    "GraphEdge",
    "GraphFormat",
    "GraphImport",
    "GraphNode",
    "HistoryCursor",
    "HistoryPage",
    "InlineExecutor",
    "Interaction",
    "JsonState",
    "LifecycleEvent",
    "NoActions",
    "NoConditions",
    "NoHooks",
    "Resolution",
    "RestoreOptions",
    "ResumePosition",
    "RunId",
    "RunLineage",
    "RunSelector",
    "RunSnapshot",
    "RunStatus",
    "Shutdown",
    "StateChange",
    "StateSerialization",
    "SyncExecution",
    "Workflow",
    "WorkflowDefinition",
    "WorkflowFailure",
    "WorkflowId",
    "WorkflowRun",
    "WorkflowState",
    "Workspace",
    "WorkspaceConfig",
    "WorkspaceId",
    "WorkspaceSnapshot",
];

const ROOT_TYPES: [&str; 9] = [
    "AgentSpec",
    "App",
    "AppConfig",
    "AppError",
    "Shutdown",
    "Workspace",
    "WorkspaceConfig",
    "WorkspaceId",
    "WorkspaceSnapshot",
];

const PUBLIC_TRAITS: [&str; 5] = [
    "ActionBackend",
    "ChildExecutor",
    "ConditionBackend",
    "LifecycleHooks",
    "StateTyping",
];

const RUNTIME_METHODS: [&str; 34] = [
    "abort",
    "abort",
    "cancel",
    "checkpoint",
    "create_workspace",
    "delete",
    "delete",
    "execution",
    "finish",
    "finish",
    "history",
    "id",
    "id",
    "id",
    "list_runs",
    "list_workspaces",
    "next",
    "open",
    "open",
    "open_run",
    "prepare",
    "replace",
    "resolve",
    "restore",
    "shutdown",
    "snapshot",
    "snapshot",
    "start",
    "step",
    "step",
    "try_next",
    "wait",
    "workflow",
    "workspace",
];

struct Repository {
    root: PathBuf,
    boundary: String,
}

impl Repository {
    fn assert_documented_action_context(&self) {
        let documented = Self {
            root: self.root.clone(),
            boundary: Self::read(&self.root.join("specs/SPECIFICATIONS.md")),
        };
        let fields = |repository: &Self| {
            repository
                .declaration_body("pub struct ActionContext {")
                .split("pub ")
                .skip(1)
                .map(|field| {
                    let (name, rest) = field.split_once(':').expect("public DTO field type");
                    let ty = rest.split(',').next().expect("field terminator");
                    (
                        name.trim().to_owned(),
                        ty.split_whitespace().collect::<String>(),
                    )
                })
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(
            fields(&documented),
            fields(self),
            "the exact ActionContext ledger must describe every exported field and its type"
        );
    }

    fn load() -> Self {
        let mut repository = Self {
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            boundary: String::new(),
        };
        repository.boundary = repository.production().join("\n");
        repository
    }

    fn read(path: &Path) -> String {
        fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
    }

    fn production(&self) -> Vec<String> {
        let mut pending = vec![self.root.join("src")];
        let mut paths = Vec::new();
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory)
                .unwrap_or_else(|error| panic!("failed to list {}: {error}", directory.display()))
            {
                let path = entry.expect("source entry must be readable").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    paths.push(path);
                }
            }
        }
        paths.sort();
        paths
            .iter()
            .map(|path| {
                Self::read(path)
                    .lines()
                    // Rustdoc examples describe caller code, outside App's production boundary.
                    .filter(|line| {
                        let line = line.trim_start();
                        !line.starts_with("//!")
                            && (!line.starts_with("///") || line.starts_with("////"))
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect()
    }

    fn assert_public_contract(&self) {
        let mut public_types = BTreeSet::new();
        let mut public_methods = Vec::new();
        let mut public_modules = BTreeSet::new();
        let mut public_traits = BTreeSet::new();
        for line in self.boundary.lines() {
            let line = line.trim_start();
            if let Some(tail) = line
                .strip_prefix("pub struct ")
                .or_else(|| line.strip_prefix("pub enum "))
            {
                let name = tail
                    .split(|character: char| {
                        character.is_ascii_whitespace()
                            || matches!(character, '<' | '{' | '(' | ';')
                    })
                    .next()
                    .expect("a public declaration must have a name");
                public_types.insert(name.to_owned());
            }
            if let Some(tail) = line
                .strip_prefix("pub async fn ")
                .or_else(|| line.strip_prefix("pub fn "))
            {
                let method = tail
                    .split(['<', '('])
                    .next()
                    .expect("a public method must have a name")
                    .trim();
                public_methods.push(method.to_owned());
            }
            if let Some(tail) = line.strip_prefix("pub mod ") {
                let module = tail
                    .split(';')
                    .next()
                    .expect("a public module must have a name")
                    .trim();
                public_modules.insert(module.to_owned());
            }
            if let Some(tail) = line.strip_prefix("pub trait ") {
                public_traits.insert(
                    tail.split([':', '<', ' ', '{'])
                        .next()
                        .expect("trait name")
                        .to_owned(),
                );
            }
            assert!(
                !line.starts_with("pub type ")
                    && !line.starts_with("pub const ")
                    && !line.starts_with("pub static ")
                    && line != "#[macro_export]",
                "unlisted public item: {line}"
            );
        }
        assert_eq!(
            public_types,
            PUBLIC_TYPES.into_iter().map(str::to_owned).collect()
        );
        assert_eq!(public_modules, BTreeSet::from(["workflow".to_owned()]));
        assert_eq!(
            public_traits,
            PUBLIC_TRAITS.into_iter().map(str::to_owned).collect()
        );
        assert_eq!(
            Self::namespace_items(&Self::read(&self.root.join("src/lib.rs"))),
            ROOT_TYPES
                .into_iter()
                .chain(["workflow"])
                .map(str::to_owned)
                .collect(),
            "crate-root exports must remain primary App construction and handles"
        );
        assert_eq!(
            Self::namespace_items(&Self::read(&self.root.join("src/workflow/mod.rs"))),
            PUBLIC_TYPES
                .into_iter()
                .filter(|name| !ROOT_TYPES.contains(name))
                .chain(PUBLIC_TRAITS)
                .map(str::to_owned)
                .collect(),
            "supporting behavior and DTOs belong in the workflow namespace"
        );
        let mut expected_methods = RUNTIME_METHODS
            .into_iter()
            .chain([
                "new",
                "new",
                "parse",
                "new",
                "values",
                "apply",
                "serialize",
                "deserialize",
                "global",
                "register_field",
                "register_tag",
                "graph",
                "export",
                "compose",
                "tracking_record",
                "tracking_records",
                "with_actions",
                "with_conditions",
                "with_state_typing",
                "with_hooks",
                "with_child_executor",
                "len",
                "is_empty",
                "into_tasks",
                "mode",
                "ordinal",
            ])
            .map(str::to_owned)
            .collect::<Vec<_>>();
        public_methods.sort();
        expected_methods.sort();
        assert_eq!(public_methods, expected_methods);
    }

    fn namespace_items(source: &str) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        let mut lines = source.lines().map(str::trim_start);
        while let Some(line) = lines.next() {
            if let Some(tail) = line.strip_prefix("pub use ") {
                let mut statement = tail.split("//").next().expect("source line").to_owned();
                while !statement.contains(';') {
                    statement.push(' ');
                    statement.push_str(
                        lines
                            .next()
                            .expect("public re-export must end")
                            .split("//")
                            .next()
                            .expect("source line"),
                    );
                }
                names.extend(Self::reexported_names(&statement));
            } else if let Some(tail) = line
                .strip_prefix("pub struct ")
                .or_else(|| line.strip_prefix("pub enum "))
                .or_else(|| line.strip_prefix("pub trait "))
                .or_else(|| line.strip_prefix("pub mod "))
            {
                names.insert(
                    tail.split(|character: char| {
                        character.is_whitespace()
                            || matches!(character, '<' | '{' | '(' | ';' | ':')
                    })
                    .next()
                    .expect("public namespace declaration name")
                    .to_owned(),
                );
            }
        }
        names
    }

    fn reexported_names(statement: &str) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        let mut remaining = statement;
        while !remaining.is_empty() {
            remaining = remaining.trim_start_matches(|character: char| {
                character.is_whitespace() || matches!(character, ':' | '{' | '}' | ',' | ';')
            });
            if remaining.is_empty() {
                break;
            }
            let length = remaining
                .bytes()
                .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                .count();
            assert!(
                length > 0,
                "public re-exports must name exact items, without globs: {statement}"
            );
            let (name, rest) = remaining.split_at(length);
            assert_ne!(
                name, "as",
                "public re-export renaming introduces an unlisted alias"
            );
            assert_ne!(
                name, "self",
                "public module re-exports require an explicit namespace contract"
            );
            remaining = rest.trim_start();
            if !remaining.starts_with("::") {
                names.insert(name.to_owned());
            }
        }
        names
    }

    fn assert_private_fields(&self, name: &str) {
        let declaration = format!("pub struct {name} {{");
        let body = self.declaration_body(&declaration);
        assert!(
            body.lines()
                .map(str::trim_start)
                .all(|line| !line.starts_with("pub ") && !line.starts_with("pub(")),
            "{name} must keep its fields private"
        );
    }

    fn assert_public_generics(&self, kind: &str, name: &str, expected: &[&str]) {
        let declaration = format!("pub {kind} {name}<");
        let header = self
            .boundary
            .split_once(&declaration)
            .unwrap_or_else(|| panic!("missing generic public declaration: {declaration}"))
            .1
            .split_once('{')
            .map(|(header, _)| header)
            .expect("the generic public declaration must have a body");
        let compact = header.split_whitespace().collect::<String>();
        let parameters = compact
            .strip_suffix('>')
            .expect("the generic declaration must close")
            .split(',')
            .filter(|parameter| !parameter.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(
            parameters, expected,
            "{name} has the wrong generic contract"
        );
    }

    fn assert_public_members(&self, declaration: &str, expected: &str) {
        let members = self
            .declaration_body(declaration)
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                if declaration.starts_with("pub struct ") {
                    return line
                        .strip_prefix("pub ")?
                        .split_once(':')
                        .map(|(field, _)| field.to_owned());
                }
                let variant = line.split(['{', '(', ',']).next()?.trim();
                variant
                    .chars()
                    .next()
                    .is_some_and(char::is_uppercase)
                    .then(|| variant.to_owned())
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            members,
            expected.split_whitespace().map(str::to_owned).collect()
        );
    }

    fn declaration_body(&self, declaration: &str) -> String {
        let source = &self.boundary;
        let body = source
            .split_once(declaration)
            .map(|(_, body)| body)
            .unwrap_or_else(|| {
                let prefix = declaration
                    .strip_suffix(" {")
                    .unwrap_or_else(|| panic!("missing public declaration: {declaration}"));
                let tail = source
                    .split_once(&format!("{prefix}<"))
                    .unwrap_or_else(|| panic!("missing public declaration: {declaration}"))
                    .1;
                let (parameters, body) = tail.split_once('{').unwrap_or_else(|| {
                    panic!("public declaration does not have a body: {declaration}")
                });
                let parameters = parameters.trim();
                assert!(
                    parameters.ends_with('>'),
                    "public declaration has unexpected syntax: {declaration}{parameters}"
                );
                body
            });
        let mut depth = 1usize;
        let end = body
            .char_indices()
            .find_map(|(index, character)| match character {
                '{' => {
                    depth += 1;
                    None
                }
                '}' => {
                    depth -= 1;
                    (depth == 0).then_some(index)
                }
                _ => None,
            })
            .expect("the declaration must close");
        body[..end].to_owned()
    }

    fn assert_private_tuple_field(&self, name: &str) {
        let declaration = format!("pub struct {name}(");
        let source = &self.boundary;
        let field = source
            .split_once(&declaration)
            .and_then(|(_, tail)| tail.split_once(')'))
            .map(|(field, _)| field)
            .unwrap_or_else(|| panic!("missing public tuple struct: {name}"));
        assert!(
            !field.trim_start().starts_with("pub"),
            "{name} must keep its field private"
        );
    }

    fn manifest_keys(manifest: &str) -> BTreeSet<String> {
        manifest
            .split_once("[dependencies]")
            .expect("the manifest must contain dependencies")
            .1
            .lines()
            .take_while(|line| !line.starts_with('['))
            .filter_map(|line| line.split_once('=').map(|(key, _)| key.trim().to_owned()))
            .collect()
    }
}

#[test]
fn action_context_documentation_matches_exported_fields() {
    Repository::load().assert_documented_action_context();
}

fn assert_identity<T>()
where
    T: Clone
        + Debug
        + Display
        + Eq
        + Ord
        + Hash
        + FromStr<Err = AppError>
        + Serialize
        + for<'de> Deserialize<'de>,
{
}

fn assert_error<T: Debug + Display + Error>() {}

fn root_shapes(shutdown: Shutdown, workspace: WorkspaceConfig, snapshot: WorkspaceSnapshot) {
    if let Shutdown::Drain { timeout } | Shutdown::CancelAndDrain { timeout } = shutdown {
        let _: std::time::Duration = timeout;
    }
    let _: (Option<String>, PathBuf) = (workspace.name, workspace.root);
    let _: (WorkspaceId, u64, WorkspaceConfig) = (snapshot.id, snapshot.revision, snapshot.config);
}

fn static_composition_shapes<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
>(
    construction: (CS, AgentSpec<A>, T, H, E),
    config: AppConfig<CS, A, X, C, T, H, E>,
    app: App<CS, A, X, C, T, H, E>,
    workspace: app_kit::Workspace<CS, A, X, C, T, H, E>,
    workflow: Workflow<CS, A, X, C, T, H, E>,
    run: WorkflowRun<CS, A, X, C, T, H, E>,
) {
    let (state, agent, typing, hooks, executor) = construction;
    fn assert_workspace<W: WorkspaceBackend>() {
        fn assert_conversation<C: ConversationBackend>() {}
        assert_conversation::<W::Conversation>();
    }

    assert_workspace::<A::Workspace>();
    let _: AppConfig<CS, A, X, C, T, H, E> = AppConfig::new(state, Vec::new(), Vec::new())
        .with_actions(Vec::<X>::new())
        .with_conditions(Vec::<C>::new())
        .with_state_typing(typing)
        .with_hooks(hooks)
        .with_child_executor(executor);
    let _: AgentSpec<A> = agent;
    let _: AppConfig<CS, A, X, C, T, H, E> = config;
    let _: App<CS, A, X, C, T, H, E> = app;
    let _: app_kit::Workspace<CS, A, X, C, T, H, E> = workspace;
    let _: Workflow<CS, A, X, C, T, H, E> = workflow;
    let mut run: WorkflowRun<CS, A, X, C, T, H, E> = run;
    if let Ok(mut execution) = SyncExecution::open(&mut run, ExecutionOptions::default()) {
        let _: Result<Option<ExecutionEvent>, AppError> = execution.try_next();
        let _: Result<RunSnapshot, AppError> = execution.abort();
    }
}

fn caller_behavior_shapes<
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
>(
    action: &X,
    condition: &C,
    hooks: &H,
    request: ActionRequest,
    state: &WorkflowState,
    event: &LifecycleEvent<'_>,
) {
    let _: &ActionDeclaration = action.declaration();
    let _: ActionContext = request.context.clone();
    let _: ExecutionOptions = ExecutionOptions::default().clone();
    let _: X::Stream = action.start(request);
    let _: &ConditionDeclaration = condition.declaration();
    let _: Result<bool, AppError> = condition.evaluate(state);
    let _: Option<T::Data> = None;
    let _: Result<(), AppError> = hooks.observe(event);
    drop(hooks.observe_async(event));
}

fn child_behavior_shapes<'a, E, F>(
    executor: &'a E,
    batch: ChildBatch<F>,
    transferred: ChildBatch<F>,
    completion: &ChildCompletion,
) where
    E: ChildExecutor,
    F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
{
    let _: usize = batch.len();
    let _: bool = batch.is_empty();
    let _: ActionMode = batch.mode();
    let _: usize = completion.ordinal();
    let execution: E::Execution<'a, F> = executor.execute(batch);
    drop(execution);
    let _: Result<Vec<std::pin::Pin<Box<F>>>, AppError> = ChildBatch::into_tasks(transferred);
}

fn homogeneous_agent_inventory<A: AgentBackend>(
    state: StateStore,
    first: A,
    second: A,
) -> AppConfig<StateStore, A> {
    AppConfig::new(
        state,
        vec![
            AgentSpec::new("first".to_owned(), "v1".to_owned(), first),
            AgentSpec::new("second".to_owned(), "v1".to_owned(), second),
        ],
        Vec::<WorkflowDefinition>::new(),
    )
}

fn workflow_shapes(snapshot: RunSnapshot, status: RunStatus, interaction: Interaction) {
    let _: String = snapshot.occurrence;
    let _: Option<u64> = snapshot.last_action_ordinal;
    let _: Vec<ActionOutcome> = snapshot.outcomes;
    let _: Vec<Interaction> = snapshot.interactions;
    let _: (RunId, u64, RunStatus, WorkflowState) = (
        snapshot.id,
        snapshot.checkpoint,
        snapshot.status,
        snapshot.state,
    );
    match status {
        RunStatus::Paused | RunStatus::Running | RunStatus::Succeeded | RunStatus::Cancelled => {}
        RunStatus::Failed(failure) => {
            let _: WorkflowFailure = failure;
        }
        _ => {}
    }
    match interaction {
        Interaction::Input {
            sequence,
            generation,
            message,
            schema,
        } => {
            let _: (u64, u64, String, Option<Value>) = (sequence, generation, message, schema);
        }
        Interaction::Notification {
            sequence,
            generation,
            notification,
        } => {
            let _: (u64, u64, std::sync::Arc<ConversationNotification>) =
                (sequence, generation, notification);
        }
        Interaction::Unknown {
            sequence,
            generation,
            retry_safe,
        } => {
            let _: (u64, u64, bool) = (sequence, generation, retry_safe);
        }
        Interaction::Inputs {
            sequence,
            generation,
            required,
        } => {
            let _: (u64, u64, Vec<String>) = (sequence, generation, required);
        }
        _ => {}
    }
    let _: fn(Value) -> Resolution = Resolution::Input;
    let _: fn(Map<String, Value>) -> Resolution = Resolution::Inputs;
    let _: fn(NotificationResolution) -> Resolution = Resolution::Notification;
    let _: fn(Map<String, Value>) -> Resolution = Resolution::Accept;
    let _: Resolution = Resolution::Fail;
    let _: Resolution = Resolution::Retry;
    let _: [fn(String) -> WorkflowFailure; 7] = [
        WorkflowFailure::Evaluation,
        WorkflowFailure::State,
        WorkflowFailure::Agent,
        WorkflowFailure::Child,
        WorkflowFailure::Execution,
        WorkflowFailure::Timeout,
        WorkflowFailure::Limit,
    ];
    let _: [fn(String) -> AppError; 7] = [
        AppError::InvalidInput,
        AppError::NotFound,
        AppError::Conflict,
        AppError::Busy,
        AppError::Closed,
        AppError::Timeout,
        AppError::Integration,
    ];
}

async fn operations(
    config: AppConfig<StateStore, Agent>,
    workspace_config: WorkspaceConfig,
    workspace_id: WorkspaceId,
    workflow_id: WorkflowId,
    run_id: RunId,
    notification: NotificationResolution,
) -> Result<(), AppError> {
    let mut app = App::open(config).await?;
    let _ = app.create_workspace(workspace_config).await?;
    let _ = app.list_workspaces(None, NonZeroUsize::MIN).await?;
    let mut workspace = app.workspace(workspace_id).await?;
    let _: &WorkspaceId = workspace.id();
    let current = workspace.snapshot().await?;
    let _ = workspace.replace(current.revision, current.config).await?;
    let workflow: Workflow<StateStore, Agent> = workspace.workflow(workflow_id).await?;
    let _: &WorkflowId = workflow.id();
    let _: WorkflowRun<StateStore, Agent> = workflow.start(ExecutionOptions::default()).await?;
    let _: WorkflowRun<StateStore, Agent> = workflow.prepare(WorkflowState::default()).await?;
    let _ = workflow.list_runs(None, NonZeroUsize::MIN).await?;
    let run = workflow.open_run(RunSelector::Latest(run_id)).await?;
    let _: &RunId = run.id();
    let snapshot = run.snapshot().await?;
    let _ = run.wait().await?;
    let _ = run.cancel(snapshot.checkpoint).await?;
    let _ = run
        .resolve(
            0,
            snapshot.checkpoint,
            Resolution::Notification(notification),
        )
        .await?;
    workspace.delete(current.revision).await?;
    app.shutdown(Shutdown::RejectActive).await
}

#[test]
fn adopted_public_surface_has_exact_names_and_namespaces() {
    let _ = root_shapes;
    let _ = static_composition_shapes::<
        StateStore,
        Agent,
        NoActions,
        NoConditions,
        JsonState,
        NoHooks,
        InlineExecutor,
    >;
    let _ = caller_behavior_shapes::<NoActions, NoConditions, JsonState, NoHooks>;
    let _ = child_behavior_shapes::<
        InlineExecutor,
        std::future::Pending<Result<ChildCompletion, AppError>>,
    >;
    let _ = homogeneous_agent_inventory::<Agent>;
    let _ = workflow_shapes;
    let _ = operations;
    let _ = (
        None::<ActionContext>,
        None::<ActionDeclaration>,
        None::<ActionEvent>,
        None::<ActionMode>,
        None::<ActionOutcome>,
        None::<ActionRequest>,
        None::<Checkpoint>,
        None::<CheckpointKind>,
        None::<ChildCompletion>,
        None::<ConditionDeclaration>,
        None::<GraphComposition>,
        None::<GraphConnection>,
        None::<GraphDescription>,
        None::<GraphEdge>,
        None::<GraphFormat>,
        None::<GraphImport>,
        None::<GraphNode>,
        None::<InlineExecutor>,
        None::<JsonState>,
        None::<LifecycleEvent<'static>>,
        None::<NoActions>,
        None::<NoConditions>,
        None::<NoHooks>,
        None::<RunLineage>,
        None::<StateChange>,
        None::<StateSerialization>,
        None::<WorkflowState>,
    );
    let repository = Repository::load();
    repository.assert_public_contract();
}

#[test]
fn constructors_privacy_and_traits_match_the_contract() {
    let repository = Repository::load();
    let composition = [
        "CS:StateStoreBackend",
        "A:AgentBackend",
        "X:ActionBackend=NoActions",
        "C:ConditionBackend=NoConditions",
        "T:StateTyping=JsonState",
        "H:LifecycleHooks=NoHooks",
        "E:ChildExecutor=InlineExecutor",
    ];
    for name in ["App", "AppConfig", "Workspace", "Workflow", "WorkflowRun"] {
        repository.assert_public_generics("struct", name, &composition);
    }
    repository.assert_public_generics("struct", "AgentSpec", &["A:AgentBackend"]);
    repository.assert_public_generics("struct", "ChildBatch", &["F"]);
    repository.assert_public_generics("enum", "LifecycleEvent", &["'a"]);
    for name in [
        "AgentSpec",
        "App",
        "AppConfig",
        "ChildBatch",
        "ChildCompletion",
        "Execution",
        "HistoryCursor",
        "Workflow",
        "WorkflowDefinition",
        "WorkflowRun",
        "WorkflowState",
        "StateSerialization",
        "SyncExecution",
        "Workspace",
    ] {
        repository.assert_private_fields(name);
    }
    for name in ["RunId", "WorkflowId", "WorkspaceId"] {
        repository.assert_private_tuple_field(name);
    }
    for (name, fields) in [
        ("WorkspaceConfig", "name root"),
        ("WorkspaceSnapshot", "config id revision"),
        (
            "RunSnapshot",
            "checkpoint id occurrence state status interactions last_action_ordinal outcomes",
        ),
        ("GraphDescription", "edges entry id nodes revision"),
        (
            "GraphNode",
            "description id kind name optional_inputs reads required_inputs source tags writes",
        ),
        ("GraphEdge", "condition from to"),
        ("GraphImport", "definition prefix"),
        ("GraphConnection", "from to"),
        ("GraphComposition", "connections entry id imports revision"),
        (
            "ActionDeclaration",
            "key revision reads writes required_inputs optional_inputs bound_inputs default_inputs tags source mode",
        ),
        ("ActionRequest", "state inputs context"),
        (
            "ActionContext",
            "workspace workflow run occurrence checkpoint action_ordinal node attempt mode parent parent_occurrence",
        ),
        ("ConditionDeclaration", "key revision reads"),
        ("ActionOutcome", "node ordinal result"),
        (
            "Checkpoint",
            "run occurrence sequence kind workflow revision action_ordinal position state status outcomes lineage",
        ),
        (
            "RunLineage",
            "source source_occurrence checkpoint action_ordinal",
        ),
        ("HistoryPage", "checkpoints next"),
        (
            "RestoreOptions",
            "source checkpoint overrides position fallback",
        ),
        (
            "ExecutionOptions",
            "state inputs halt_before halt_after max_steps",
        ),
    ] {
        repository.assert_public_members(&format!("pub struct {name} {{"), fields);
    }
    for (name, variants) in [
        ("Shutdown", "CancelAndDrain Drain RejectActive"),
        (
            "AppError",
            "Busy Closed Conflict Integration InvalidInput NotFound Timeout",
        ),
        (
            "RunStatus",
            "Cancelled Cancelling Failed Halted Paused Running Succeeded",
        ),
        ("Interaction", "Input Inputs Notification Unknown"),
        ("GraphFormat", "Dot Json Mermaid"),
        ("ActionMode", "Synchronous Asynchronous Both"),
        (
            "ActionEvent",
            "Partial Completed SpanStarted SpanAttribute SpanFinished",
        ),
        ("NoActions", ""),
        ("NoConditions", ""),
        ("CheckpointKind", "Initial Control Action Failed Terminal"),
        ("CheckpointSelector", "Latest Sequence Action"),
        ("RunSelector", "Latest Exact"),
        ("ResumePosition", "AfterCompleted AtSaved Entrypoint"),
        ("ExecutionEvent", "Partial Committed Halted"),
        (
            "LifecycleEvent",
            "Registered ChildFinished ExecuteStarted ExecuteFinished StepStarted StepFinished StreamStarted StreamItem StreamFinished SpanStarted SpanAttribute SpanFinished",
        ),
        ("Resolution", "Accept Fail Input Inputs Notification Retry"),
        (
            "WorkflowFailure",
            "Action Agent Child Evaluation Execution Hook Limit State Timeout",
        ),
    ] {
        repository.assert_public_members(&format!("pub enum {name} {{"), variants);
    }
    let _: fn(String, String, Agent) -> AgentSpec<Agent> = AgentSpec::new;
    let _: fn(&str) -> Result<WorkflowDefinition, AppError> = WorkflowDefinition::parse;
    assert_identity::<WorkspaceId>();
    assert_identity::<WorkflowId>();
    assert_identity::<RunId>();
    assert_error::<AppError>();
}

#[test]
fn production_and_manifest_keep_the_reduced_boundary() {
    let repository = Repository::load();
    let sources = repository.production();
    let production = sources.join("\n");
    for forbidden in [
        "#[cfg(test)]",
        "#[cfg(not(",
        "#[path",
        "#[allow(",
        "#[expect(",
        "include!(",
        "include_str!(",
        "include_bytes!(",
        "macro_rules!",
        "#[macro_export]",
        "unsafe ",
        "todo!(",
        "unimplemented!(",
        "std::env::",
        "env!(",
        "option_env!(",
        "pub type ",
        "pub(crate) type ",
        "pub(super) type ",
        "tools_kit::",
        "agents_kit::backends",
        "agents_kit::state",
        "#[serde(alias",
    ] {
        assert!(
            !production.contains(forbidden),
            "forbidden production construct remains: {forbidden}"
        );
    }
    let mut private_codecs = 0;
    for source in &sources {
        let owns_serialization = source.contains("impl StateSerialization {");
        for line in source.lines() {
            if line.chars().next().is_some_and(char::is_whitespace) {
                continue;
            }
            let declaration = line
                .strip_prefix("pub ")
                .or_else(|| line.split_once(") ").map(|(_, tail)| tail))
                .unwrap_or(line);
            // The process-global field/tag registry is the sole private
            // callable erasure. Recognize its owner and Value-only contract,
            // without freezing the private trait, method or parameter names.
            let private_codec = owns_serialization && line.starts_with("trait ") && {
                private_codecs += 1;
                let tail = source.split_once(line).unwrap().1;
                let bounds = line.split_once(':').map(|(_, bounds)| {
                    bounds
                        .trim_end_matches('{')
                        .split('+')
                        .map(str::trim)
                        .collect::<BTreeSet<_>>()
                });
                let body = tail
                    .split_once('}')
                    .map(|(body, _)| body)
                    .unwrap_or_default();
                let compact = body.split_whitespace().collect::<String>();
                let signature = compact
                    .strip_prefix("fn")
                    .and_then(|method| method.split_once('('))
                    .map(|(_, signature)| signature);
                private_codecs == 1
                    && bounds == Some(BTreeSet::from(["Send", "Sync"]))
                    && signature.is_some_and(|signature| {
                        signature
                            .split_once(',')
                            .is_some_and(|(receiver, argument)| {
                                receiver == "&self"
                                    && argument.split_once(':').is_some_and(|(_, ty)| {
                                        ty == "&Value)->Result<Value,AppError>;"
                                    })
                            })
                    })
            };
            assert!(
                (!declaration.starts_with("trait ")
                    || private_codec
                    || (line.starts_with("pub trait ")
                        && declaration
                            .strip_prefix("trait ")
                            .is_some_and(|tail| PUBLIC_TRAITS.contains(
                                &tail.split([':', '<', ' ', '{']).next().expect("trait name")
                            ))))
                    && !declaration.starts_with("type ")
                    && !declaration.starts_with("fn ")
                    && !declaration.starts_with("async fn ")
                    && !declaration.starts_with("const fn "),
                "forbidden standalone production item: {line}"
            );
        }
    }
    assert!(production.contains("\"app/v7\""));
    assert!(
        production.contains("\"app/v6\""),
        "the one supported cold migration remains explicit"
    );
    assert!(!production.contains("\"app/v5\""));
    assert!(!production.contains("\"app/v4\""));
    assert!(production.contains("\"app-kit.workflow/v2\""));
    assert!(!production.contains("\"app-kit.workflow/v1\""));
    for forbidden in [
        "StateStore::connect",
        "StateStoreConfig",
        "pub enum AppState",
        "StateStore::from_backend",
        "dyn StateStoreBackend",
        "DatabaseBackend",
        "implementation_id",
        "ErasedStateStoreBackend",
        "ErasedStateTransactionBackend",
        "providers_kit::",
        "ProviderBackend",
        "AS: StateStoreBackend",
        "KD: DatabaseBackend",
        "P: ProviderBackend",
        "agents_kit::Agent<",
        "ErasedAgentBackend",
        "ErasedWorkspaceBackend",
        "ErasedConversationBackend",
        "Agent::from_backend",
        "dyn AgentBackend",
        "dyn WorkspaceBackend",
        "dyn ConversationBackend",
        "A: Clone",
        "AgentBackend + Clone",
        "dyn ActionBackend",
        "dyn ConditionBackend",
        "dyn StateTyping",
        "dyn LifecycleHooks",
        "dyn ChildExecutor",
        "X: Clone",
        "C: Clone",
        "T: Clone",
        "H: Clone",
        "E: Clone",
        "ActionBackend + Clone",
        "ConditionBackend + Clone",
        "StateTyping + Clone",
        "LifecycleHooks + Clone",
        "ChildExecutor + Clone",
    ] {
        assert!(
            !production.contains(forbidden),
            "forbidden lower-crate route remains: {forbidden}"
        );
    }

    let manifest = Repository::read(&repository.root.join("Cargo.toml"));
    for forbidden in [
        "[features]",
        "[package.metadata.docs.rs]",
        "build =",
        "[build-dependencies]",
        "[dev-dependencies]",
        "[target.",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "forbidden manifest entry: {forbidden}"
        );
    }
    assert_eq!(
        Repository::manifest_keys(&manifest),
        "agents-kit futures-util jsonschema serde serde_json sha2 storage-kit tokio tracing uuid"
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    );
    assert!(!manifest.contains("providers-kit"));
    assert!(manifest.contains("storage-kit = { workspace = true, default-features = false }"));
}

#[test]
fn run_ids_accept_the_exact_pinned_child_digest_without_reformatting() {
    // Fixed rurr parallelism/child_context.rs:5–54 and support/parallelism/
    // graphs.rs:182–184: SHA-256 of the UTF-8 bytes `parent-app:0-0`.
    let pinned = "83ee2cdbea3265d2f2768e4c7d36864eddbdcb77b1410ac31425af1b0ddc42bd";
    let parsed: RunId = pinned
        .parse()
        .expect("pinned child identity is public RunId text");
    assert_eq!(parsed.to_string(), pinned);
    let encoded = serde_json::to_string(&parsed).unwrap();
    assert_eq!(encoded, format!("\"{pinned}\""));
    assert_eq!(serde_json::from_str::<RunId>(&encoded).unwrap(), parsed);
}

#[test]
fn run_ids_keep_generated_root_uuid_format_and_reject_noncanonical_child_text() {
    let root = "run_00000000-0000-4000-8000-000000000000";
    let parsed: RunId = root.parse().expect("existing root format stays valid");
    assert_eq!(parsed.to_string(), root);
    let pinned = "83ee2cdbea3265d2f2768e4c7d36864eddbdcb77b1410ac31425af1b0ddc42bd";
    for invalid in [
        pinned.to_uppercase(),
        pinned[..63].to_owned(),
        format!("{pinned}0"),
        format!("run_{pinned}"),
        format!(" {pinned}"),
        format!("{pinned}\n"),
        format!("{}g", &pinned[..63]),
    ] {
        assert!(
            matches!(invalid.parse::<RunId>(), Err(AppError::InvalidInput(_))),
            "{invalid:?}"
        );
        assert!(serde_json::from_value::<RunId>(serde_json::Value::String(invalid)).is_err());
    }
}

#[test]
fn passive_checkpoint_selectors_use_the_authored_initializer_wire_shapes() {
    for (selector, encoded) in [
        (
            CheckpointSelector::Latest,
            serde_json::json!({"kind":"latest"}),
        ),
        (
            CheckpointSelector::Sequence(7),
            serde_json::json!({"kind":"sequence","value":7}),
        ),
        (
            CheckpointSelector::Action(3),
            serde_json::json!({"kind":"action","value":3}),
        ),
    ] {
        assert_eq!(serde_json::to_value(selector.clone()).unwrap(), encoded);
        let decoded: CheckpointSelector = serde_json::from_value(encoded).unwrap();
        assert!(decoded == selector);
    }
    for invalid in [
        serde_json::json!({"kind":"sequence"}),
        serde_json::json!({"kind":"action","value":-1}),
        serde_json::json!({"kind":"sequence","value":1.5}),
        serde_json::json!({"kind":"action","value":"3"}),
        serde_json::json!({"kind":"unknown"}),
        serde_json::json!({"kind":"latest","unknown":true}),
    ] {
        assert!(serde_json::from_value::<CheckpointSelector>(invalid).is_err());
    }
    for (position, encoded) in [
        (ResumePosition::AfterCompleted, "after_completed"),
        (ResumePosition::AtSaved, "at_saved"),
        (ResumePosition::Entrypoint, "entrypoint"),
    ] {
        assert_eq!(serde_json::to_value(position.clone()).unwrap(), encoded);
        let decoded: ResumePosition = serde_json::from_value(serde_json::json!(encoded)).unwrap();
        assert!(decoded == position);
    }
    for invalid in [
        serde_json::json!("AfterCompleted"),
        serde_json::json!({"position":"at_saved"}),
        serde_json::Value::Null,
    ] {
        assert!(serde_json::from_value::<ResumePosition>(invalid).is_err());
    }
}
