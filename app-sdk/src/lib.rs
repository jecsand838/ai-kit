//! Durable applications over caller-owned storage and configured agents.
//!
//! Start with [`AppConfig`] and [`App::open`], create a [`Workspace`], then select
//! a [`workflow::Workflow`]. The [`App`] example runs a local state workflow with
//! caller-created embedded Lance StateStore and no Agent value or external service.
//!
//! [`workflow`] contains definitions, execution controls, state operations,
//! graph inspection, immutable checkpoints, and the five concrete App behavior
//! seams. [`AgentSpec`] transfers an already-built Agent; App never constructs
//! providers, tools or lower storage from declarations. Exact associated lower
//! Workspace, Conversation and StateTransaction types remain attached.
//!
//! Call [`App::shutdown`] while the captured Tokio runtime and lower runtimes
//! are alive and driven. Run handles observe accepted work; [`workflow::Execution`]
//! owns lazy advancement. Dropping an execution can revoke uncommitted work,
//! while observation loss cannot revoke an admitted native commit.
//!
//! Public supporting types live under [`workflow`]. The state serialization
//! registry stores only field/tag codecs; it is not a runtime behavior registry.

//!
//! # Choose a workflow
//!
//! - Application builders: the [local workflow](#local-workflow),
//!   [explicit execution and retained history](workflow), then
//!   [durable reopen and migration prerequisites](App::open).
//! - Custom implementers: [configure concrete behavior](AppConfig#configure-behavior)
//!   with the five workflow traits. Their examples run locally; lower
//!   [`agent_sdk::AgentBackend`] and [`storage_sdk::StateStoreBackend`]
//!   implementations remain direct, statically selected values.
//! - Agent-backed workflows: transfer already-built values through [`AgentSpec`].
//!   App owns eventual consuming Agent shutdown, without rebuilding credentials,
//!   tools, Providers or persistence from declarations.
//!
//! # Local workflow
//!
//! ```
//! use agent_sdk::Agent;
//! use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
//! use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
//!     RunStatus, WorkflowDefinition, WorkflowState};
//! use storage_sdk::StateStore;
//! use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
//!
//! let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
//! runtime.block_on(async {
//! let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
//! let definition = WorkflowDefinition::parse(r#"{
//!     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
//!     "state":{"count":0}, "required":["count"],
//!     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
//!     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
//!         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
//!     ]}}], "routes":{}
//! }"#)?;
//! let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
//! let mut app = App::open(config).await?;
//! let mut workspace = app.create_workspace(WorkspaceConfig {
//!     name: Some("local counter".into()), root: std::env::current_dir()?,
//! }).await?;
//! let workflow = workspace.workflow("counter".parse()?).await?;
//! let run = workflow.start(ExecutionOptions::default()).await?;
//! let snapshot = run.wait().await?;
//! assert!(matches!(snapshot.status, RunStatus::Succeeded));
//! assert_eq!(snapshot.state.values()["count"], 1);
//! #
//! app.shutdown(Shutdown::Drain { timeout: std::time::Duration::from_secs(5) }).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use agent_sdk::AgentBackend;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use storage_sdk::StateStoreBackend;
use uuid::{Uuid, Variant, Version};

use crate::workflow::{
    ActionBackend, ChildExecutor, ConditionBackend, InlineExecutor, JsonState, LifecycleHooks,
    NoActions, NoConditions, NoHooks, StateTyping, Workflow, WorkflowDefinition, WorkflowId,
};

mod app;
mod storage;
mod workspace;

/// Opaque workflow definitions and runtime handles.
pub mod workflow;

/// An open application generation.
///
/// Owns one durable application generation, its transferred agents and accepted work.
///
/// Supply a concrete [`storage_sdk::StateStoreBackend`] through [`AppConfig`].
/// Each [`AgentSpec`] preserves its concrete [`agent_sdk::AgentBackend`] and its
/// associated Workspace/Conversation types. An empty agent inventory can execute
/// state-only workflows; the explicit `Agent` type below constructs no Agent value.
///
/// Keep the Tokio runtime and any caller-owned lower runtimes alive and driven
/// through [`Self::shutdown`]. Root drop revokes admission and requests supervised
/// best-effort cleanup; it cannot substitute for awaited shutdown. Workspace and
/// run observers do not keep admission alive after the App is revoked.
///
/// # Example
///
/// ```
/// use agent_sdk::Agent;
/// use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
/// use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
///     RunStatus, WorkflowDefinition, WorkflowState};
/// use storage_sdk::StateStore;
/// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
///
/// let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// runtime.block_on(async {
/// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// let definition = WorkflowDefinition::parse(r#"{
///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#)?;
/// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
/// let mut app = App::open(config).await?;
/// let mut workspace = app.create_workspace(WorkspaceConfig {
///     name: Some("local counter".into()), root: std::env::current_dir()?,
/// }).await?;
/// let workflow = workspace.workflow("counter".parse()?).await?;
/// let run = workflow.start(ExecutionOptions::default()).await?;
/// let snapshot = run.wait().await?;
/// assert!(matches!(snapshot.status, RunStatus::Succeeded));
/// assert_eq!(snapshot.state.values()["count"], 1);
/// #
/// app.shutdown(Shutdown::Drain { timeout: std::time::Duration::from_secs(5) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct App<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend = NoActions,
    C: ConditionBackend = NoConditions,
    T: StateTyping = JsonState,
    H: LifecycleHooks = NoHooks,
    E: ChildExecutor = InlineExecutor,
> {
    runtime: Arc<app::AppRuntime<CS, A, X, C, T, H, E>>,
}

/// Complete application configuration.
///
/// App does not offer a separate built-in storage construction path:
///
/// ```compile_fail
/// let _ = app_sdk::AppState::<storage_sdk::StateStore>::Ephemeral;
/// ```
///
/// Owns caller-built storage, a homogeneous agent inventory, and workflow definitions.
///
/// The consuming `with_*` methods change their concrete App behavior parameter.
/// They do not erase backends or alter already admitted runs. Heterogeneous agent
/// inventories use a caller-owned exhaustive hierarchy. Dropping configuration
/// before open does not perform asynchronous Agent shutdown.
///
/// # Configure behavior
///
/// Start with ready storage, ready Agents, and parsed definitions. Apply the
/// consuming configuration methods before [`App::open`]; they retain exact
/// implementation types, including associated lower children and transaction
/// ownership. None registers a runtime callback or erases a backend.
///
/// | Behavior | Complete implementation example | Attach before open |
/// | --- | --- | --- |
/// | Produce lazy action events and writes | [`workflow::ActionBackend`] | [`Self::with_actions`] |
/// | Evaluate declared routing predicates | [`workflow::ConditionBackend`] | [`Self::with_conditions`] |
/// | Construct and validate application state | [`workflow::StateTyping`] | [`Self::with_state_typing`] |
/// | Observe authoritative lifecycle events | [`workflow::LifecycleHooks`] | [`Self::with_hooks`] |
/// | Drive concrete local child futures | [`workflow::ChildExecutor`] | [`Self::with_child_executor`] |
///
/// The `with_*` examples exercise the complete path from a concrete implementation
/// to opening App. Action and condition declarations must match the workflow's
/// exact logical keys and revisions. A custom child executor must return every
/// admitted completion exactly once; it never creates child identities. App's
/// built-in `NoActions`, `NoConditions`, `JsonState`, `NoHooks` and `InlineExecutor`
/// are suitable defaults only where their actual behavior satisfies the workflow.
///
/// Lower composition is independent: `A::Workspace` fixes its Conversation,
/// while `CS::Transaction` fixes consuming commit/rollback. For heterogeneous
/// Agent inventories implement matching caller-owned exhaustive hierarchies;
/// do not substitute public trait objects or independent child parameters.
///
/// # Example
///
/// ```
/// # use agent_sdk::Agent;
/// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_sdk::StateStore;
/// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// #
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
/// #     "state":{"count":0}, "required":["count"],
/// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
/// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
/// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
/// #     ]}}], "routes":{}
/// # }"#)?;
/// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
/// let mut app = App::open(config).await?;
/// app.shutdown(Shutdown::RejectActive).await?;
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct AppConfig<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend = NoActions,
    C: ConditionBackend = NoConditions,
    T: StateTyping = JsonState,
    H: LifecycleHooks = NoHooks,
    E: ChildExecutor = InlineExecutor,
> {
    state: CS,
    agents: Vec<AgentSpec<A>>,
    workflows: Vec<WorkflowDefinition>,
    actions: Vec<X>,
    conditions: Vec<C>,
    typing: T,
    hooks: H,
    executor: E,
}

/// Declares one configured lower agent.
///
/// The key and revision are caller-owned logical binding metadata, validated by
/// [`App::open`]. Keep the same revision only while the configuration is compatible.
/// App never derives compatibility from the opaque lower Agent identity. A
/// successful open takes responsibility for consuming Agent shutdown.
///
/// # Example
///
/// This example requires an already-running OpenCode service at the shown endpoint.
///
/// ```no_run
/// use std::{num::NonZeroUsize, time::Duration};
/// use agent_sdk::{Agent, AgentBuilder, AgentStorage};
/// use agent_sdk::config::{AgentConfig, OpenCodeConfig};
/// use app_sdk::{AgentSpec, App, AppConfig, Shutdown};
/// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// let agent_store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// let backend = OpenCodeConfig::external("http://127.0.0.1:4096", Duration::from_secs(10),
///     Duration::from_secs(60), Duration::from_secs(10))?;
/// let agent_config = AgentConfig::new(backend.into(), Duration::from_secs(10),
///     NonZeroUsize::new(1).unwrap())?;
/// let agent: Agent = AgentBuilder::create(agent_config, AgentStorage::new(agent_store)).build().await?;
/// let spec = AgentSpec::new("reviewer".into(), "r1".into(), agent);
/// let app_store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// let mut app = App::open(AppConfig::new(app_store, vec![spec], Vec::new())).await?;
/// // App now owns consuming shutdown of the transferred Agent.
/// app.shutdown(Shutdown::Drain { timeout: Duration::from_secs(30) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct AgentSpec<A: AgentBackend> {
    key: String,
    revision: String,
    agent: Arc<A>,
}

/// Selects application shutdown behavior.
///
/// Drain timeouts limit observation only. Once admission is sealed, a timed-out
/// or dropped shutdown observer does not cancel the finalizer or reopen App.
/// Keep all owning runtimes driven until shutdown completes.
pub enum Shutdown {
    /// Rejects shutdown while accepted work remains.
    RejectActive,
    /// Seals admission and drains accepted work.
    Drain {
        /// Bounds this caller's observation of finalization.
        timeout: Duration,
    },
    /// Seals admission, cancels accepted work, and drains it.
    CancelAndDrain {
        /// Bounds this caller's observation of finalization.
        timeout: Duration,
    },
}

/// An application workspace handle.
///
/// A typed projection into the owning App generation. Dropping it does not delete
/// or close lower workspaces. Replacement uses a complete configuration and a
/// revision fence; successful deletion revokes this projection. Retained terminal
/// history can still be opened through its workflow after workspace deletion.
///
/// # Example
///
/// ```
/// # use agent_sdk::Agent;
/// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_sdk::StateStore;
/// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// #
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
/// #     "state":{"count":0}, "required":["count"],
/// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
/// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
/// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
/// #     ]}}], "routes":{}
/// # }"#)?;
/// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
/// # let mut app = App::open(config).await?;
/// # let mut workspace = app.create_workspace(WorkspaceConfig {
/// #     name: Some("local counter".into()), root: std::env::current_dir()?,
/// # }).await?;
/// let snapshot = workspace.snapshot().await?;
/// assert_eq!(&snapshot.id, workspace.id());
/// assert_eq!(snapshot.revision, 1);
/// #
/// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Workspace<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend = NoActions,
    C: ConditionBackend = NoConditions,
    T: StateTyping = JsonState,
    H: LifecycleHooks = NoHooks,
    E: ChildExecutor = InlineExecutor,
> {
    runtime: workspace::WorkspaceRuntime<CS, A, X, C, T, H, E>,
}

/// A validated application workspace identity.
///
/// Parsing and deserialization require `appws_` followed by a canonical lowercase,
/// hyphenated UUIDv4. Identity equality does not establish lower-agent compatibility.
///
/// # Example
///
/// ```
/// use app_sdk::WorkspaceId;
/// let id: WorkspaceId = "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?;
/// assert_eq!(id.to_string(), "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc");
/// assert!("appws_not-a-uuid".parse::<WorkspaceId>().is_err());
/// # Ok::<(), app_sdk::AppError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceId(String);

/// Complete caller-authored workspace configuration.
///
/// The root is an absolute, lexical UTF-8 path, 1–4,096 bytes without control
/// characters. Validation does not create directories or resolve symlinks. A present
/// name is trimmed and must be nonempty, control-free, and at most 256 UTF-8 bytes.
///
/// # Example
///
/// ```
/// use app_sdk::WorkspaceConfig;
/// let config = WorkspaceConfig { name: Some("review".into()), root: std::env::current_dir()? };
/// assert!(config.root.is_absolute());
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct WorkspaceConfig {
    /// Optionally names the lower workspace.
    pub name: Option<String>,
    /// Selects the absolute workspace root.
    pub root: PathBuf,
}

/// The current caller-visible workspace truth.
///
/// # Example
///
/// ```
/// # use agent_sdk::Agent;
/// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_sdk::StateStore;
/// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// #
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
/// #     "state":{"count":0}, "required":["count"],
/// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
/// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
/// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
/// #     ]}}], "routes":{}
/// # }"#)?;
/// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
/// # let mut app = App::open(config).await?;
/// # let mut workspace = app.create_workspace(WorkspaceConfig {
/// #     name: Some("local counter".into()), root: std::env::current_dir()?,
/// # }).await?;
/// let snapshot = workspace.snapshot().await?;
/// let updated = workspace.replace(snapshot.revision, WorkspaceConfig {
///     name: Some("renamed".into()), root: snapshot.config.root,
/// }).await?;
/// assert_eq!(updated.revision, snapshot.revision + 1);
/// #
/// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[non_exhaustive]
pub struct WorkspaceSnapshot {
    /// Identifies the workspace.
    pub id: WorkspaceId,
    /// Fences complete replacement.
    pub revision: u64,
    /// Contains the committed configuration.
    pub config: WorkspaceConfig,
}

/// A safe caller-visible application error.
///
/// Operational messages are bounded and redact lower payloads and identities.
/// Errors supplied by explicitly registered codecs or lifecycle hooks retain
/// caller context at that opt-in boundary. Match variants (with a fallback for
/// this non-exhaustive enum), rather than parsing Display text. An error from an
/// observer does not by itself prove an accepted native operation rolled back.
#[non_exhaustive]
#[derive(Debug)]
pub enum AppError {
    /// Caller-authored input is invalid.
    InvalidInput(String),
    /// Exact scoped evidence proves a resource absent.
    NotFound(String),
    /// A durable fact, fence, or transition conflicts.
    Conflict(String),
    /// Finite capacity or an exclusive owner prevents progress.
    Busy(String),
    /// Revocation or terminal invalidation rejects new work.
    Closed(String),
    /// An application-owned deadline elapsed.
    Timeout(String),
    /// Caller behavior, a lower public operation, or a private durable record failed.
    Integration(String),
}

enum IdentitySyntax {
    Named(&'static str),
    Uuid {
        name: &'static str,
        prefix: &'static str,
    },
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> App<CS, A, X, C, T, H, E>
{
    /// Opens or reconciles one application generation.
    ///
    /// Validates bindings and retained records, claims one durable generation, and
    /// reconstructs recovery ownership before returning. A successful newer opener
    /// fences handles from the previous generation. Configuration is consumed.
    ///
    /// # Durable reopen and deployment
    ///
    /// Reopen with a fresh handle to the same caller-owned StateStore, the same
    /// compatible workflow definitions, and freshly constructed or reopened lower
    /// integrations. Never reuse a consumed Agent or infer a new logical identity
    /// from its display representation. The state-only example below needs no Agent
    /// value and demonstrates that shutdown retains history until explicit deletion.
    ///
    /// Existing App v6 records require a **cold, one-way v6-to-v7 migration**.
    /// Stop older clients, settle all their owners and take a whole-store physical
    /// backup before opening with this version. Opening performs the supported
    /// migration before claiming the new generation. Mixed-version operation,
    /// in-place downgrade, reset and corruption repair are not supported. Restore
    /// the physical backup for a rollback deployment; do not open migrated data
    /// with an older client. Unsupported epochs and corrupt current records fail
    /// rather than falling back to another decoder.
    ///
    /// After Agent ownership transfer, ordinary failure awaits consuming Agent cleanup
    /// and preserves the primary failure. Cancelling this open future during transfer
    /// does not promise graceful cleanup: reopen the durable lower products and App to
    /// reconcile their retained facts.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::InvalidInput`] for invalid or conflicting construction
    /// metadata, [`AppError::Conflict`] for incompatible retained revisions or lost
    /// ownership, and [`AppError::Integration`] for unsupported durable versions,
    /// invalid retained records, or lower failures. Recovery can also report capacity,
    /// closure, or deadline errors; no live App is returned on failure.
    ///
    /// # Example
    ///
    /// ```
    /// use agent_sdk::Agent;
    /// use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// use app_sdk::workflow::{ExecutionOptions, RunSelector, RunStatus, WorkflowDefinition};
    /// use storage_sdk::StateStore;
    /// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// use std::time::Duration;
    /// let directory = std::env::temp_dir().join(format!("ai-kit-app-doc-{}", uuid::Uuid::new_v4()));
    /// std::fs::create_dir(&directory)?;
    /// let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// let result = runtime.block_on(async {
    ///     let definition_source = r#"{
    ///         "schema":"app-sdk.workflow/v2","id":"counter","revision":"r1",
    ///         "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
    ///         "entry":"increment","nodes":[{"id":"increment","step":{"kind":"state",
    ///         "mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}}],
    ///         "routes":{}
    ///     }"#;
    ///     let config = StateStoreConfig::lance(LanceStateStoreConfig::directory(&directory));
    ///     let store = StateStore::connect(config.clone()).await?;
    ///     let mut app = App::open(AppConfig::<_, Agent>::new(store, Vec::new(), vec![WorkflowDefinition::parse(definition_source)?])).await?;
    ///     let first = async {
    ///         let workspace = app.create_workspace(WorkspaceConfig { name: None, root: directory.clone() }).await?;
    ///         let workflow = workspace.workflow("counter".parse()?).await?;
    ///         let run = workflow.start(ExecutionOptions::default()).await?;
    ///         let terminal = run.wait().await?;
    ///         assert!(matches!(terminal.status, RunStatus::Succeeded));
    ///         Ok::<_, app_sdk::AppError>((workspace.id().clone(), run.id().clone()))
    ///     }.await;
    ///     let shutdown = app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(5) }).await;
    ///     drop(app);
    ///     let (workspace_id, run_id) = first?;
    ///     shutdown?;
    ///
    ///     let fresh_store = StateStore::connect(config).await?;
    ///     let mut reopened = App::open(AppConfig::<_, Agent>::new(fresh_store, Vec::new(), vec![WorkflowDefinition::parse(definition_source)?])).await?;
    ///     let readback = async {
    ///         let workspace = reopened.workspace(workspace_id).await?;
    ///         let workflow = workspace.workflow("counter".parse()?).await?;
    ///         let mut run = workflow.open_run(RunSelector::Latest(run_id.clone())).await?;
    ///         let terminal = run.snapshot().await?;
    ///         assert!(matches!(terminal.status, RunStatus::Succeeded));
    ///         assert_eq!(terminal.state.values()["count"], 1);
    ///         run.delete(terminal.checkpoint).await?;
    ///         assert!(matches!(workflow.open_run(RunSelector::Latest(run_id)).await,
    ///             Err(app_sdk::AppError::NotFound(_))));
    ///         Ok::<(), app_sdk::AppError>(())
    ///     }.await;
    ///     let shutdown = reopened.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(5) }).await;
    ///     drop(reopened);
    ///     readback?;
    ///     shutdown?;
    ///     Ok::<(), Box<dyn std::error::Error>>(())
    /// });
    /// // A shutdown timeout bounds the waiter, not necessarily the owned finalizer.
    /// // Remove the directory only after both generations demonstrably settled.
    /// if result.is_ok() {
    ///     std::fs::remove_dir_all(&directory)?;
    /// } else {
    ///     eprintln!("Retain {} for cleanup reconciliation", directory.display());
    /// }
    /// result?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn open(config: AppConfig<CS, A, X, C, T, H, E>) -> Result<Self, AppError> {
        Ok(Self {
            runtime: app::AppTrace::span(|| tracing::info_span!(target: "app_sdk", "app.open"))
                .observe(std::pin::pin!(app::AppRuntime::open(config)))
                .await?,
        })
    }

    /// Creates and materializes one workspace.
    ///
    /// Materializes the complete configured Agent key/revision inventory before
    /// publishing the App workspace. Durable intent precedes each lower operation;
    /// unresolved partial progress remains recoverable rather than a visible success.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::InvalidInput`] for invalid configuration, [`AppError::Busy`]
    /// when admission is full, or [`AppError::Closed`] after revocation. Conflicting
    /// materialization evidence, lower/storage failures, and bounded waits report
    /// `Conflict`, `Integration`, or `Timeout` respectively.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// let workspace = app.create_workspace(WorkspaceConfig {
    ///     name: Some("review".into()), root: std::env::current_dir()?,
    /// }).await?;
    /// assert_eq!(workspace.snapshot().await?.revision, 1);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn create_workspace(
        &self,
        config: WorkspaceConfig,
    ) -> Result<Workspace<CS, A, X, C, T, H, E>, AppError> {
        Ok(Workspace {
            runtime: self.runtime.create_workspace(config).await?,
        })
    }

    /// Reopens one workspace by typed identity.
    ///
    /// A retained terminal head can establish a history-only scope after coordination
    /// is deleted. That handle permits retained-run lookup, not fresh admission.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::NotFound`] when neither active coordination nor readable
    /// retained history exists in this scope. Revocation, incompatible lifecycle
    /// progress, and unreadable durable evidence report `Closed`, `Conflict`, or
    /// `Integration`.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// let id = workspace.id().clone();
    /// let reopened = app.workspace(id.clone()).await?;
    /// assert_eq!(reopened.id(), &id);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn workspace(
        &self,
        id: WorkspaceId,
    ) -> Result<Workspace<CS, A, X, C, T, H, E>, AppError> {
        Ok(Workspace {
            runtime: self.runtime.workspace(id).await?,
        })
    }

    /// Lists workspaces in canonical identity order.
    ///
    /// The cursor is exclusive and pages contain active workspaces only. Limits are
    /// 1–1,000; larger requests are rejected rather than silently clamped.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::InvalidInput`] for an excessive limit, [`AppError::NotFound`]
    /// for an unknown cursor, and `Closed` or `Integration` for revoked or unreadable
    /// application state.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// let page = app.list_workspaces(None, std::num::NonZeroUsize::new(1).unwrap()).await?;
    /// assert_eq!(&page[0].id, workspace.id());
    /// let next = app.list_workspaces(Some(page[0].id.clone()), std::num::NonZeroUsize::new(1).unwrap()).await?;
    /// assert!(next.is_empty());
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn list_workspaces(
        &self,
        after: Option<WorkspaceId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<WorkspaceSnapshot>, AppError> {
        self.runtime.list_workspaces(after, limit).await
    }

    /// Finalizes this application according to `policy`.
    ///
    /// `RejectActive` leaves admission open if work remains. Drain policies seal
    /// admission; `CancelAndDrain` also requests explicit cancellation. Successful
    /// completion joins App-owned work and consumes each transferred Agent once.
    /// The timeout bounds this observer, not the continuing finalizer. Cancelling the
    /// observer or timing out never reopens admission or detaches accepted cleanup.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Busy`] when `RejectActive` encounters accepted work,
    /// [`AppError::InvalidInput`] for an invalid timeout, or [`AppError::Timeout`] when
    /// observation expires. Durable conflicts and lower cleanup failures remain
    /// `Conflict` or `Integration`; cleanup does not erase an earlier failure.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// app.shutdown(Shutdown::Drain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// #
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn shutdown(&mut self, policy: Shutdown) -> Result<(), AppError> {
        self.runtime.shutdown(policy).await
    }
}

impl<CS: StateStoreBackend, A: AgentBackend> AppConfig<CS, A> {
    /// Constructs configuration from a caller-owned control-plane store and agents.
    ///
    /// The store is required even for process-local operation. App preserves its
    /// concrete transaction type and never constructs lower products. Ownership
    /// transfers to App when [`App::open`] is called.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// let mut app = App::open(config).await?;
    /// app.shutdown(Shutdown::RejectActive).await?;
    /// #
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn new(state: CS, agents: Vec<AgentSpec<A>>, workflows: Vec<WorkflowDefinition>) -> Self {
        Self {
            state,
            agents,
            workflows,
            actions: Vec::new(),
            conditions: Vec::new(),
            typing: JsonState,
            hooks: NoHooks,
            executor: InlineExecutor,
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
> AppConfig<CS, A, X, C, T, H, E>
{
    /// Replaces the complete caller-owned action inventory before opening App.
    ///
    /// Values retain their concrete type and need not implement `Clone`. App
    /// validates their stable declarations before accepting any workflow work.
    /// Replacing an inventory drops its previous values without invoking them.
    ///
    /// # Example
    ///
    /// ```
    /// use agent_sdk::Agent;
    /// use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    ///     RunStatus, WorkflowDefinition, WorkflowState};
    /// use storage_sdk::StateStore;
    /// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// use app_sdk::{AppError, workflow::{ActionBackend, ActionDeclaration, ActionEvent,
    ///     ActionMode, ActionRequest, StateChange}};
    /// use serde_json::{Map, Value};
    /// struct Increment { declaration: ActionDeclaration }
    /// impl ActionBackend for Increment {
    ///     type Stream = futures_util::stream::Iter<std::vec::IntoIter<Result<ActionEvent, AppError>>>;
    ///     fn declaration(&self) -> &ActionDeclaration { &self.declaration }
    ///     fn start(&self, request: ActionRequest) -> Self::Stream {
    ///         let count = request.state.values()["count"].as_i64().unwrap() + 1;
    ///         futures_util::stream::iter(vec![Ok(ActionEvent::Completed {
    ///             result: Map::from_iter([("new_count".into(), Value::from(count))]),
    ///             writes: vec![StateChange::Increment { values: vec![("count".into(), Value::from(1))] }],
    ///         })])
    ///     }
    /// }
    /// let action = Increment { declaration: ActionDeclaration {
    ///     key: "increment".into(), revision: "r1".into(), reads: vec!["count".into()],
    ///     writes: vec!["count".into()], required_inputs: Vec::new(), optional_inputs: Vec::new(),
    ///     bound_inputs: Map::new(), default_inputs: Map::new(), tags: vec!["counter".into()],
    ///     source: Some("increment count by one".into()), mode: ActionMode::Both,
    /// } };
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let definition = WorkflowDefinition::parse(r#"{
    ///  "schema":"app-sdk.workflow/v2","id":"counter","revision":"r1",
    ///  "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
    ///  "entry":"increment","nodes":[{"id":"increment","step":{"kind":"action",
    ///  "action":"increment","revision":"r1"}}],"routes":{}
    /// }"#)?;
    /// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// let config = config.with_actions(vec![action]);
    /// let mut app = App::open(config).await?;
    /// let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir()? }).await?;
    /// let workflow = workspace.workflow("counter".parse()?).await?;
    /// let run = workflow.start(ExecutionOptions::default()).await?;
    /// assert_eq!(run.wait().await?.state.values()["count"], 1);
    /// app.shutdown(Shutdown::Drain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// #
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_actions<N: ActionBackend>(
        self,
        actions: Vec<N>,
    ) -> AppConfig<CS, A, N, C, T, H, E> {
        AppConfig {
            state: self.state,
            agents: self.agents,
            workflows: self.workflows,
            actions,
            conditions: self.conditions,
            typing: self.typing,
            hooks: self.hooks,
            executor: self.executor,
        }
    }

    /// Replaces the complete caller-owned condition inventory before opening App.
    ///
    /// App validates stable logical keys and revisions without evaluating the
    /// predicates. Replacing an inventory drops its previous values.
    ///
    /// # Example
    ///
    /// ```
    /// use agent_sdk::Agent;
    /// use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    ///     RunStatus, WorkflowDefinition, WorkflowState};
    /// use storage_sdk::StateStore;
    /// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// use app_sdk::{AppError, workflow::{ConditionBackend, ConditionDeclaration}};
    /// struct Ready { declaration: ConditionDeclaration }
    /// impl ConditionBackend for Ready {
    ///     fn declaration(&self) -> &ConditionDeclaration { &self.declaration }
    ///     fn evaluate(&self, state: &WorkflowState) -> Result<bool, AppError> {
    ///         let count = state.values().get("count").and_then(serde_json::Value::as_i64)
    ///             .ok_or_else(|| AppError::InvalidInput("count must be an integer".into()))?;
    ///         Ok(count >= 3)
    ///     }
    /// }
    /// let condition = Ready { declaration: ConditionDeclaration {
    ///     key: "ready".into(), revision: "r1".into(), reads: vec!["count".into()],
    /// } };
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let definition = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{"increment":{"mode":"first","branches":[
    ///         {"when":{"kind":"custom","condition":"ready","revision":"r1"},"target":{"kind":"end"}},
    ///         {"target":{"kind":"node","id":"increment"}}
    ///     ]}}
    /// }"#)?;
    /// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// let config = config.with_conditions(vec![condition]);
    /// let mut app = App::open(config).await?;
    /// let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir()? }).await?;
    /// let workflow = workspace.workflow("counter".parse()?).await?;
    /// let terminal = workflow.start(ExecutionOptions::default()).await?.wait().await?;
    /// assert!(matches!(terminal.status, RunStatus::Succeeded));
    /// assert_eq!(terminal.state.values()["count"], 3);
    /// app.shutdown(Shutdown::Drain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// #
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_conditions<N: ConditionBackend>(
        self,
        conditions: Vec<N>,
    ) -> AppConfig<CS, A, X, N, T, H, E> {
        AppConfig {
            state: self.state,
            agents: self.agents,
            workflows: self.workflows,
            actions: self.actions,
            conditions,
            typing: self.typing,
            hooks: self.hooks,
            executor: self.executor,
        }
    }

    /// Replaces the caller-owned workflow typing policy before opening App.
    ///
    /// The policy remains concrete and need not implement `Clone`. Its data
    /// values are local to validation or execution and need not be `Send`.
    ///
    /// # Example
    ///
    /// ```
    /// use agent_sdk::Agent;
    /// use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    ///     RunStatus, WorkflowDefinition, WorkflowState};
    /// use storage_sdk::StateStore;
    /// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// use app_sdk::{AppError, workflow::{StateTyping, StateChange}};
    /// use serde_json::{Map, Value};
    /// struct Counter;
    /// impl StateTyping for Counter {
    ///     type Data = i64;
    ///     fn construct_data(&self, state: &WorkflowState) -> Result<i64, AppError> {
    ///         state.values().get("count").and_then(Value::as_i64)
    ///             .ok_or_else(|| AppError::InvalidInput("count must be an integer".into()))
    ///     }
    ///     fn construct_state(&self, count: &i64) -> Result<WorkflowState, AppError> {
    ///         WorkflowState::new(Map::from_iter([("count".into(), Value::from(*count))]))
    ///     }
    ///     fn validate_pre(&self, _state: &WorkflowState, change: &StateChange) -> Result<(), AppError> {
    ///         if let StateChange::Increment { values } = change {
    ///             if values.iter().any(|(_, delta)| delta.as_i64().is_some_and(|value| value < 0)) {
    ///                 return Err(AppError::InvalidInput("count cannot decrease".into()));
    ///             }
    ///         }
    ///         Ok(())
    ///     }
    ///     fn validate_post(&self, before: &WorkflowState, after: &WorkflowState, _change: &StateChange) -> Result<(), AppError> {
    ///         if self.construct_data(after)? < self.construct_data(before)? {
    ///             return Err(AppError::InvalidInput("count cannot decrease".into()));
    ///         }
    ///         Ok(())
    ///     }
    ///     fn validate_state(&self, state: &WorkflowState) -> Result<(), AppError> {
    ///         if self.construct_data(state)? < 0 {
    ///             return Err(AppError::InvalidInput("count must be nonnegative".into()));
    ///         }
    ///         Ok(())
    ///     }
    /// }
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let definition = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#)?;
    /// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// let config = config.with_state_typing(Counter);
    /// let mut app = App::open(config).await?;
    /// let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir()? }).await?;
    /// let workflow = workspace.workflow("counter".parse()?).await?;
    /// let invalid = WorkflowState::new(serde_json::from_str(r#"{"count":-1}"#)?)?;
    /// assert!(workflow.prepare(invalid).await.is_err());
    /// app.shutdown(Shutdown::RejectActive).await?;
    /// #
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_state_typing<N: StateTyping>(self, typing: N) -> AppConfig<CS, A, X, C, N, H, E> {
        AppConfig {
            state: self.state,
            agents: self.agents,
            workflows: self.workflows,
            actions: self.actions,
            conditions: self.conditions,
            typing,
            hooks: self.hooks,
            executor: self.executor,
        }
    }

    /// Replaces the caller-owned lifecycle observer before opening App.
    ///
    /// Replacement drops the prior observer without invoking it. The new
    /// observer retains its concrete type and need not implement `Clone`.
    ///
    /// # Example
    ///
    /// ```
    /// use agent_sdk::Agent;
    /// use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    ///     RunStatus, WorkflowDefinition, WorkflowState};
    /// use storage_sdk::StateStore;
    /// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// use app_sdk::{AppError, workflow::{LifecycleEvent, LifecycleHooks}};
    /// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    /// struct CountEvents(Arc<AtomicUsize>);
    /// impl LifecycleHooks for CountEvents {
    ///     fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
    ///         if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
    ///             self.0.fetch_add(1, Ordering::SeqCst);
    ///         }
    ///         Ok(())
    ///     }
    ///     fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>)
    ///         -> impl std::future::Future<Output = Result<(), AppError>> + Send + 'a {
    ///         async move {
    ///             if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
    ///                 self.0.fetch_add(10, Ordering::SeqCst);
    ///             }
    ///             Ok(())
    ///         }
    ///     }
    /// }
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let definition = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#)?;
    /// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// let count = Arc::new(AtomicUsize::new(0));
    /// let config = config.with_hooks(CountEvents(Arc::clone(&count)));
    /// let mut app = App::open(config).await?;
    /// let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir()? }).await?;
    /// let workflow = workspace.workflow("counter".parse()?).await?;
    /// let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// assert_eq!(count.load(Ordering::SeqCst), 11);
    /// app.shutdown(Shutdown::Drain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// #
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_hooks<N: LifecycleHooks>(self, hooks: N) -> AppConfig<CS, A, X, C, T, N, E> {
        AppConfig {
            state: self.state,
            agents: self.agents,
            workflows: self.workflows,
            actions: self.actions,
            conditions: self.conditions,
            typing: self.typing,
            hooks,
            executor: self.executor,
        }
    }

    /// Replaces the caller-owned admitted child executor before opening App.
    ///
    /// The executor remains concrete. Its associated execution futures are
    /// created only for admitted child work and need not implement `Send`.
    ///
    /// # Example
    ///
    /// ```
    /// use agent_sdk::Agent;
    /// use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    ///     RunStatus, WorkflowDefinition, WorkflowState};
    /// use storage_sdk::StateStore;
    /// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let definition = WorkflowDefinition::parse(r#"{
    ///  "schema":"app-sdk.workflow/v2","id":"counter","revision":"r1",
    ///  "state":{"sum":0},"required":["sum"],"reducers":{"sum":"sum_integer"},
    ///  "entry":"child","nodes":[{"id":"child","step":{"kind":"child","workflow":"child",
    ///  "input":{"count":{"kind":"literal","value":4}},
    ///  "reductions":[{"source":"count","target":"sum","reducer":"sum_integer"}]}}],"routes":{}
    /// }"#)?;
    /// let child = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"child", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#)?;
    /// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition, child])
    ///     .with_child_executor(app_sdk::workflow::InlineExecutor);
    /// let mut app = App::open(config).await?;
    /// let mut workspace = app.create_workspace(WorkspaceConfig {
    ///     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// }).await?;
    /// let workflow = workspace.workflow("counter".parse()?).await?;
    /// let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// assert_eq!(terminal.state.values()["sum"], 5);
    /// #
    /// app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_child_executor<N: ChildExecutor>(
        self,
        executor: N,
    ) -> AppConfig<CS, A, X, C, T, H, N> {
        AppConfig {
            state: self.state,
            agents: self.agents,
            workflows: self.workflows,
            actions: self.actions,
            conditions: self.conditions,
            typing: self.typing,
            hooks: self.hooks,
            executor,
        }
    }
}

impl<A: AgentBackend> AgentSpec<A> {
    /// Constructs one configured lower-agent entry.
    ///
    /// The key and revision are caller-owned logical binding metadata, validated by
    /// [`App::open`]. Keep the same revision only while the configuration is compatible.
    /// App never derives compatibility from the opaque lower Agent identity. A
    /// successful open takes responsibility for consuming Agent shutdown.
    ///
    /// # Example
    ///
    /// This example requires an already-running OpenCode service at the shown endpoint.
    ///
    /// ```no_run
    /// use std::{num::NonZeroUsize, time::Duration};
    /// use agent_sdk::{Agent, AgentBuilder, AgentStorage};
    /// use agent_sdk::config::{AgentConfig, OpenCodeConfig};
    /// use app_sdk::{AgentSpec, App, AppConfig, Shutdown};
    /// use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let agent_store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let backend = OpenCodeConfig::external("http://127.0.0.1:4096", Duration::from_secs(10),
    ///     Duration::from_secs(60), Duration::from_secs(10))?;
    /// let agent_config = AgentConfig::new(backend.into(), Duration::from_secs(10),
    ///     NonZeroUsize::new(1).unwrap())?;
    /// let agent: Agent = AgentBuilder::create(agent_config, AgentStorage::new(agent_store)).build().await?;
    /// let spec = AgentSpec::new("reviewer".into(), "r1".into(), agent);
    /// let app_store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// let mut app = App::open(AppConfig::new(app_store, vec![spec], Vec::new())).await?;
    /// // App now owns consuming shutdown of the transferred Agent.
    /// app.shutdown(Shutdown::Drain { timeout: Duration::from_secs(30) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn new(key: String, revision: String, agent: A) -> Self {
        Self {
            key,
            revision,
            agent: Arc::new(agent),
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
> Drop for App<CS, A, X, C, T, H, E>
{
    fn drop(&mut self) {
        self.runtime.request_drop();
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
> Workspace<CS, A, X, C, T, H, E>
{
    /// Returns this workspace's stable identity.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// let id = workspace.id().clone();
    /// assert_eq!(workspace.snapshot().await?.id, id);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn id(&self) -> &WorkspaceId {
        self.runtime.id()
    }

    /// Loads the current workspace snapshot.
    ///
    /// # Errors
    ///
    /// Returns `Closed` after revocation or sealing, `NotFound` for absent coordination,
    /// `Conflict` during unresolved lifecycle progress, or `Integration` for unreadable
    /// committed evidence. A history-only workspace has no configuration snapshot.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// let snapshot = workspace.snapshot().await?;
    /// assert_eq!(snapshot.revision, 1);
    /// assert_eq!(snapshot.config.name.as_deref(), Some("local counter"));
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn snapshot(&self) -> Result<WorkspaceSnapshot, AppError> {
        self.runtime.snapshot().await
    }

    /// Atomically replaces the complete workspace configuration.
    ///
    /// This is full replacement, including the name and root. Preflight checks all
    /// lower workspaces before fan-out. A partial lower failure retains the old App
    /// revision and durable reverse compensation; preflight never reserves a revision.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` for a stale revision, a nonterminal run pinned to the current
    /// revision, or partial lower fan-out (even if compensation succeeds). Invalid
    /// configuration, capacity, revocation, storage/lower failures and bounded waits
    /// report `InvalidInput`, `Busy`, `Closed`, `Integration`, or `Timeout`.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// let before = workspace.snapshot().await?;
    /// let after = workspace.replace(before.revision, WorkspaceConfig {
    ///     name: Some("reviewed".into()), root: before.config.root,
    /// }).await?;
    /// assert_eq!(after.revision, before.revision + 1);
    /// assert_eq!(after.config.name.as_deref(), Some("reviewed"));
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn replace(
        &self,
        expected_revision: u64,
        config: WorkspaceConfig,
    ) -> Result<WorkspaceSnapshot, AppError> {
        self.runtime.replace(expected_revision, config).await
    }

    /// Opens a configured workflow in this workspace.
    ///
    /// Selects the current configured definition for new work. A deleted workspace
    /// may retain a history-only workflow when exact terminal evidence still exists.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` for an unavailable definition or retained scope, `Closed`
    /// for revoked admission, or `Integration` for inconsistent durable evidence.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// let workflow = workspace.workflow("counter".parse()?).await?;
    /// assert_eq!(workflow.id().to_string(), "counter");
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn workflow(
        &self,
        id: WorkflowId,
    ) -> Result<Workflow<CS, A, X, C, T, H, E>, AppError> {
        Ok(Workflow::from_runtime(self.runtime.workflow(id).await?))
    }

    /// Deletes this workspace after proving its revision fence.
    ///
    /// Seals new admission, requests cancellation, and deletes lower workspaces only
    /// after every run is terminal with no unresolved effect. Progress is durable and
    /// resumable; terminal run history is retained until explicit run deletion.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` for a stale revision or incompatible deletion intent, `Busy`
    /// while unresolved work prevents deletion, and `Closed` for revoked ownership.
    /// Invalid fences, storage/lower failures, or expired observation return
    /// `InvalidInput`, `Integration`, or `Timeout`.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// let revision = workspace.snapshot().await?.revision;
    /// workspace.delete(revision).await?;
    /// assert!(app.list_workspaces(None, std::num::NonZeroUsize::new(10).unwrap()).await?.is_empty());
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn delete(&mut self, expected_revision: u64) -> Result<(), AppError> {
        self.runtime.delete(expected_revision).await
    }
}

impl WorkspaceId {
    pub(crate) fn generate() -> Self {
        Self(format!("appws_{}", Uuid::new_v4().hyphenated()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl IdentitySyntax {
    fn parse(&self, value: &str) -> Result<String, AppError> {
        match self {
            Self::Named(name) => {
                let valid = !value.is_empty()
                    && value.len() <= 128
                    && value.bytes().enumerate().all(|(index, byte)| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || (index > 0 && matches!(byte, b'_' | b'-'))
                    });
                if valid {
                    Ok(value.to_owned())
                } else {
                    Err(AppError::InvalidInput(format!(
                        "{name} must match [a-z0-9][a-z0-9_-]{{0,127}}"
                    )))
                }
            }
            Self::Uuid { name, prefix } => {
                let Some(encoded) = value.strip_prefix(prefix) else {
                    return Err(AppError::InvalidInput(format!(
                        "{name} has an invalid prefix"
                    )));
                };
                let parsed = Uuid::parse_str(encoded)
                    .map_err(|_| AppError::InvalidInput(format!("{name} is not a UUIDv4")))?;
                if parsed.get_version() != Some(Version::Random)
                    || parsed.get_variant() != Variant::RFC4122
                    || parsed.hyphenated().to_string() != encoded
                {
                    return Err(AppError::InvalidInput(format!(
                        "{name} must contain a canonical lowercase UUIDv4"
                    )));
                }
                Ok(value.to_owned())
            }
        }
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for WorkspaceId {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        IdentitySyntax::Uuid {
            name: "WorkspaceId",
            prefix: "appws_",
        }
        .parse(value)
        .map(Self)
    }
}

impl Serialize for WorkspaceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for WorkspaceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(formatter, "invalid input: {message}"),
            Self::NotFound(message) => write!(formatter, "not found: {message}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::Busy(message) => write!(formatter, "busy: {message}"),
            Self::Closed(message) => write!(formatter, "closed: {message}"),
            Self::Timeout(message) => write!(formatter, "timeout: {message}"),
            Self::Integration(message) => write!(formatter, "integration: {message}"),
        }
    }
}

impl std::error::Error for AppError {}
