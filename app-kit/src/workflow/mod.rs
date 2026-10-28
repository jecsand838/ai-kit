//! Workflow authoring, execution, immutable state, and retained history.
//!
//! Parse strict versioned JSON with
//! [`WorkflowDefinition::parse`](crate::workflow::WorkflowDefinition::parse), inspect its
//! [`GraphDescription`](crate::workflow::GraphDescription), and transfer definitions
//! through [`crate::AppConfig`].
//! [`Workflow::start`](crate::workflow::Workflow::start) drives accepted work in the background;
//! [`Workflow::prepare`](crate::workflow::Workflow::prepare) admits checkpoint zero for explicit
//! [`WorkflowRun::execution`](crate::workflow::WorkflowRun::execution) or
//! [`SyncExecution`](crate::workflow::SyncExecution). Synchronous calls require an outside thread and a runtime
//! that stays driven elsewhere. Both lanes share the same commit authority.
//!
//! [`ExecutionEvent::Partial`](crate::workflow::ExecutionEvent::Partial) never commits state.
//! [`ExecutionEvent::Committed`](crate::workflow::ExecutionEvent::Committed)
//! identifies a durable boundary; action ordinals and checkpoint sequences are
//! different coordinates. [`WorkflowRun::history`](crate::workflow::WorkflowRun::history)
//! reads a fixed cutoff, and [`Workflow::restore`](crate::workflow::Workflow::restore)
//! copies clean retained state into a new paused run.
//! History remains until explicit [`WorkflowRun::delete`](crate::workflow::WorkflowRun::delete).
//!
//! Concrete behavior belongs to [`ActionBackend`],
//! [`ConditionBackend`], [`StateTyping`],
//! [`LifecycleHooks`] and [`ChildExecutor`].
//! The child executor's
//! associated future can remain local/non-Send without future erasure.
//! [`StateSerialization`](crate::workflow::StateSerialization) separately registers JSON field/tag codecs only.
//! Supporting DTOs describe inputs, results and controls; runtime ownership
//! stays private. Run observers do not cancel work when dropped; execution owners
//! revoke uncommitted advancement while preserving admitted native commits.

//! # Execute, observe, restore and delete
//!
//! This local workflow admits a paused run, explicitly drives its lazy execution,
//! and then reads immutable history. Merely obtaining a run snapshot does not
//! advance a prepared run. Restore copies a retained checkpoint into another paused
//! run; deletion removes only the selected terminal history, not its independent
//! copy. The example needs no credentials, Agent value or subprocess.
//!
//! ```
//! use agents_kit::Agent;
//! use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
//! use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions,
//!     RestoreOptions, ResumePosition, RunSelector, RunStatus, WorkflowDefinition, WorkflowState};
//! use storage_kit::StateStore;
//! use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
//! use std::{num::NonZeroUsize, time::Duration};
//! # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
//! # runtime.block_on(async {
//! let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
//! let definition = WorkflowDefinition::parse(r#"{
//!     "schema":"app-kit.workflow/v2","id":"counter","revision":"r1",
//!     "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
//!     "entry":"increment","nodes":[{"id":"increment","step":{"kind":"state",
//!     "mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}}],
//!     "routes":{}
//! }"#)?;
//! let mut app = App::open(AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition])).await?;
//! let outcome = async {
//!     let workspace = app.create_workspace(WorkspaceConfig {
//!         name: Some("history example".into()), root: std::env::current_dir()?,
//!     }).await?;
//!     let workflow = workspace.workflow("counter".parse()?).await?;
//!     let mut run = workflow.prepare(WorkflowState::default()).await?;
//!     assert!(matches!(run.snapshot().await?.status, RunStatus::Paused));
//!     assert_eq!(run.checkpoint(CheckpointSelector::Sequence(0)).await?.state.values()["count"], 0);
//!     let mut execution = run.execution(ExecutionOptions::default()).await?;
//!     let mut committed_actions = 0;
//!     while let Some(event) = execution.next().await? {
//!         if let ExecutionEvent::Committed { outcomes, .. } = event {
//!             committed_actions += outcomes.len();
//!         }
//!     }
//!     let terminal = execution.finish().await?;
//!     assert!(matches!(terminal.status, RunStatus::Succeeded));
//!     assert_eq!(terminal.state.values()["count"], 1);
//!     assert_eq!(committed_actions, 1);
//!
//!     let first = run.history(None, NonZeroUsize::new(1).unwrap()).await?;
//!     assert_eq!(first.checkpoints[0].sequence, 0);
//!     let cursor = first.next.expect("the action has a later checkpoint");
//!     let later = run.history(Some(cursor), NonZeroUsize::new(10).unwrap()).await?;
//!     assert!(later.checkpoints.iter().any(|checkpoint| checkpoint.action_ordinal == Some(0)));
//!     let mut copy = workflow.restore(RestoreOptions {
//!         source: RunSelector::Latest(run.id().clone()), checkpoint: CheckpointSelector::Action(0),
//!         overrides: WorkflowState::default(), position: ResumePosition::AfterCompleted, fallback: None,
//!     }).await?;
//!     assert_ne!(copy.id(), run.id());
//!     assert!(matches!(copy.snapshot().await?.status, RunStatus::Paused));
//!     let copied = copy.checkpoint(CheckpointSelector::Sequence(0)).await?;
//!     assert_eq!(copied.state.values()["count"], 1);
//!     assert_eq!(&copied.lineage.as_ref().unwrap().source, run.id());
//!     let copied_terminal = copy.execution(ExecutionOptions::default()).await?.finish().await?;
//!     assert!(matches!(copied_terminal.status, RunStatus::Succeeded));
//!     copy.delete(copied_terminal.checkpoint).await?;
//!     let original_id = run.id().clone();
//!     run.delete(terminal.checkpoint).await?;
//!     assert!(matches!(workflow.open_run(RunSelector::Latest(original_id)).await, Err(AppError::NotFound(_))));
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! }.await;
//! let cleanup = app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(5) }).await;
//! outcome?; // An execution error remains primary if shutdown also reports an error.
//! cleanup?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! A real Agent-backed workflow can stop at an actionable interaction rather than
//! success: inspect [`crate::workflow::RunSnapshot::interactions`] and [`crate::workflow::RunStatus`] before choosing
//! an explicit resolution. Caller actions own their external effects; a discarded
//! uncommitted workflow state change does not undo external I/O.
//!
//! # Configure caller behavior
//!
//! [`crate::AppConfig`]'s configuration map links the complete implementations of
//! all five behavior traits and their consuming attachment methods. Actions and
//! conditions use exact declared keys/revisions; typing validates initial and
//! resulting state; hooks observe lifecycle boundaries; child executors drive the
//! already-admitted concrete child futures. None changes the lower Agent or
//! StateStore associated child types. See [`crate::App::open`] for durable reopen
//! and the cold migration boundary.

use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use agents_kit::AgentBackend;
use agents_kit::notification::{ConversationNotification, NotificationResolution};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use storage_kit::StateStoreBackend;
use uuid::Uuid;

use crate::{AppError, IdentitySyntax};

mod action;
mod child;
pub(crate) mod definition;
mod execution;
pub(crate) use execution::{MAX_FRONTIER, StagedOutcomeRecord};
mod graph;
mod history;
pub(crate) mod interaction;
mod lifecycle;
pub(crate) mod run;
mod serialization;
mod state;

pub use action::{
    ActionBackend, ActionContext, ActionDeclaration, ActionEvent, ActionMode, ActionRequest,
    ConditionBackend, ConditionDeclaration, NoActions, NoConditions,
};
pub use child::{ChildBatch, ChildCompletion, ChildExecutor, InlineExecutor};
pub use history::{
    ActionOutcome, Checkpoint, CheckpointKind, CheckpointSelector, HistoryCursor, HistoryPage,
    RestoreOptions, ResumePosition, RunLineage,
};
pub use lifecycle::{LifecycleEvent, LifecycleHooks, NoHooks};
pub use run::session::{Execution, SyncExecution};

/// Immutable, ordered workflow values validated at each operation boundary.
///
/// ```
/// use app_kit::workflow::{JsonState, StateChange, WorkflowState};
/// use serde_json::{Map, Value};
/// let before = WorkflowState::new(Map::from_iter([("count".into(), Value::from(1))]))?;
/// let after = before.apply(&StateChange::Increment {
///     values: vec![("count".into(), Value::from(1))],
/// }, &JsonState::default())?;
/// assert_eq!(before.values()["count"], 1);
/// assert_eq!(after.values()["count"], 2);
/// # Ok::<(), app_kit::AppError>(())
/// ```
///
/// Changes go through the validated state operations:
///
/// ```compile_fail
/// use app_kit::workflow::WorkflowState;
/// let mut state = WorkflowState::default();
/// state.values.clear();
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WorkflowState {
    values: Map<String, Value>,
}

/// Process-global field encoders, field decoders and tagged JSON decoders.
///
/// Registrations are exact names and replacements affect the next operation.
/// Each operation retains one registry snapshot and invokes callbacks without
/// holding its lock. The only construction entrypoint is [`Self::global`].
///
/// ```
/// use app_kit::workflow::StateSerialization;
/// StateSerialization::global().register_tag("text".into(), |value| {
/// value.get("value").cloned().ok_or("missing text value")
/// })?;
/// # Ok::<(), app_kit::AppError>(())
/// ```
///
/// Only field/tag JSON codecs enter this registry; actions, hooks, child executors
/// and lower backends remain concrete configuration values. Registration does not
/// persist callable identity; callers version their own portable codec payloads.
pub struct StateSerialization {
    registry: RwLock<Arc<serialization::SerializationRegistry>>,
}

/// A resolved immutable state operation; expressions remain private syntax.
///
/// Each payload is one atomic operation with at most 4,096 entries. Repeated
/// Append/Extend/Increment keys apply in order; any later failure discards the
/// whole candidate. Merge and Subset use full-state validation rather than
/// primitive transition callbacks. Results never mutate the original state.
pub enum StateChange {
    /// Replaces values atomically in payload order, preserving existing positions.
    Assign {
        /// Complete values to replace or insert.
        values: Map<String, Value>,
    },
    /// Appends one item per entry, creating arrays for missing keys.
    Append {
        /// Ordered entries; repeated keys append repeatedly within one operation.
        values: Vec<(String, Value)>,
    },
    /// Extends arrays atomically without recursively flattening their items.
    Extend {
        /// Ordered entries; repeated keys preserve every supplied item.
        values: Vec<(String, Vec<Value>)>,
    },
    /// Adds signed integer deltas to integer values, or zero for missing keys.
    Increment {
        /// Ordered deltas, each representable as i64; repeated keys fold in order.
        values: Vec<(String, Value)>,
    },
    /// Removes keys atomically, preserving the order of other keys.
    Remove {
        /// Missing and repeated names are harmless.
        keys: Vec<String>,
    },
    /// Keeps keys in their existing order; typing observes resolved Remove keys.
    Wipe {
        /// Keys to retain; missing and repeated names are harmless.
        keep: Vec<String>,
    },
    /// Replaces overlapping values and appends new keys in incoming order.
    Merge {
        /// Values to merge into the current state.
        state: WorkflowState,
    },
    /// Selects present keys in the requested order.
    Subset {
        /// Ordered keys to select; missing keys are ignored.
        keys: Vec<String>,
    },
}

/// Caller-owned typed construction and state validation.
///
/// Data is created and consumed locally and need not be Clone, Send or serializable.
/// The default roundtrip checks successful reconstruction, not raw equality.
///
/// # Example
///
/// ```
/// use app_kit::{AppError, workflow::{StateTyping, StateChange, WorkflowState}};
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
/// let before = Counter.construct_state(&2)?;
/// let change = StateChange::Increment { values: vec![("count".into(), Value::from(1))] };
/// let after = before.apply(&change, &Counter)?;
/// assert_eq!(Counter.construct_data(&after)?, 3);
/// assert!(Counter.validate_state(&Counter.construct_state(&-1)?).is_err());
/// # Ok::<(), AppError>(())
/// ```
pub trait StateTyping: Send + Sync + 'static {
    /// Caller-specific data produced from workflow values.
    type Data;

    /// Constructs caller data from a validated state.
    ///
    /// # Errors
    ///
    /// Return an error when the state cannot be represented as `Self::Data`.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{StateTyping, StateChange, WorkflowState}};
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
    /// let before = Counter.construct_state(&2)?;
    /// let change = StateChange::Increment { values: vec![("count".into(), Value::from(1))] };
    /// let after = before.apply(&change, &Counter)?;
    /// assert_eq!(Counter.construct_data(&after)?, 3);
    /// # Ok::<(), AppError>(())
    /// ```
    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError>;
    /// Reconstructs workflow values from caller data.
    ///
    /// # Errors
    ///
    /// Return an error when caller data cannot produce a valid workflow state.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{StateTyping, StateChange, WorkflowState}};
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
    /// let before = Counter.construct_state(&2)?;
    /// let change = StateChange::Increment { values: vec![("count".into(), Value::from(1))] };
    /// let after = before.apply(&change, &Counter)?;
    /// let reconstructed = Counter.construct_state(&3)?;
    /// assert_eq!(reconstructed, after);
    /// # Ok::<(), AppError>(())
    /// ```
    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError>;
    /// Validates an operation before any candidate mutation.
    ///
    /// # Errors
    ///
    /// Return an error to reject the complete primitive operation before candidate
    /// mutation. The default accepts it; Merge and Subset do not invoke this method.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{StateTyping, StateChange, WorkflowState}};
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
    /// let before = Counter.construct_state(&2)?;
    /// let change = StateChange::Increment { values: vec![("count".into(), Value::from(1))] };
    /// let after = before.apply(&change, &Counter)?;
    /// Counter.validate_pre(&before, &change)?;
    /// let decreasing = StateChange::Increment { values: vec![("count".into(), Value::from(-1))] };
    /// assert!(Counter.validate_pre(&before, &decreasing).is_err());
    /// assert_eq!(before.values()["count"], 2);
    /// # Ok::<(), AppError>(())
    /// ```
    fn validate_pre(&self, _state: &WorkflowState, _change: &StateChange) -> Result<(), AppError> {
        Ok(())
    }
    /// Validates the original and candidate states after the operation.
    ///
    /// # Errors
    ///
    /// Return an error to reject the complete candidate primitive transition. The
    /// original state remains unchanged. The default accepts it; Merge and Subset
    /// do not invoke this method.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{StateTyping, StateChange, WorkflowState}};
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
    /// let before = Counter.construct_state(&2)?;
    /// let change = StateChange::Increment { values: vec![("count".into(), Value::from(1))] };
    /// let after = before.apply(&change, &Counter)?;
    /// Counter.validate_post(&before, &after, &change)?;
    /// let decreasing = Counter.construct_state(&1)?;
    /// assert!(Counter.validate_post(&before, &decreasing, &change).is_err());
    /// assert_eq!(after.values()["count"], 3);
    /// # Ok::<(), AppError>(())
    /// ```
    fn validate_post(
        &self,
        _before: &WorkflowState,
        _after: &WorkflowState,
        _change: &StateChange,
    ) -> Result<(), AppError> {
        Ok(())
    }
    /// Validates all candidate values after transition validation.
    ///
    /// # Errors
    ///
    /// Return an error when any full-state invariant is violated. The default accepts
    /// the state; this method also runs for reconstructed Merge/Subset snapshots.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{StateTyping, StateChange, WorkflowState}};
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
    /// let before = Counter.construct_state(&2)?;
    /// let change = StateChange::Increment { values: vec![("count".into(), Value::from(1))] };
    /// let after = before.apply(&change, &Counter)?;
    /// assert!(Counter.validate_state(&Counter.construct_state(&-1)?).is_err());
    /// # Ok::<(), AppError>(())
    /// ```
    fn validate_state(&self, _state: &WorkflowState) -> Result<(), AppError> {
        Ok(())
    }
    /// Verifies both caller conversions succeed; a different reconstruction is allowed.
    ///
    /// # Errors
    ///
    /// The default propagates either conversion error. Successful reconstruction is
    /// sufficient; byte-for-byte or value equality is not required.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{StateTyping, StateChange, WorkflowState}};
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
    /// let before = Counter.construct_state(&2)?;
    /// let change = StateChange::Increment { values: vec![("count".into(), Value::from(1))] };
    /// let after = before.apply(&change, &Counter)?;
    /// Counter.validate_roundtrip(&after)?;
    /// assert_eq!(Counter.construct_data(&after)?, 3);
    /// # Ok::<(), AppError>(())
    /// ```
    fn validate_roundtrip(&self, state: &WorkflowState) -> Result<(), AppError> {
        let data = self.construct_data(state)?;
        self.construct_state(&data).map(|_| ())
    }
}

/// Raw JSON typing with no additional conversion or validation policy.
///
/// # Example
///
/// ```
/// use app_kit::workflow::{JsonState, StateTyping, WorkflowState};
/// let state = WorkflowState::new(serde_json::from_str(r#"{"count":2}"#)?)?;
/// let data = JsonState.construct_data(&state)?;
/// assert_eq!(data["count"], 2);
/// assert_eq!(JsonState.construct_state(&data)?, state);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonState;

impl StateTyping for JsonState {
    type Data = Map<String, Value>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        Ok(state.values.clone())
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        WorkflowState::new(data.clone())
    }

    fn validate_roundtrip(&self, _state: &WorkflowState) -> Result<(), AppError> {
        Ok(())
    }
}

impl WorkflowState {
    /// Validates ordered JSON values and takes ownership of them.
    ///
    /// ```
    /// use app_kit::workflow::WorkflowState;
    /// let values = serde_json::from_str(r#"{"first":1,"second":2}"#)?;
    /// let state = WorkflowState::new(values)?;
    /// assert_eq!(state.values().keys().collect::<Vec<_>>(), ["first", "second"]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for invalid state keys or exceeded JSON byte, nesting,
    /// collection, or text bounds. Ownership is accepted only after full validation.
    pub fn new(values: Map<String, Value>) -> Result<Self, AppError> {
        definition::JsonBoundary::validate_map(&values)?;
        Ok(Self { values })
    }

    /// Borrows values in their insertion order.
    ///
    /// ```
    /// use app_kit::workflow::WorkflowState;
    /// let state = WorkflowState::default();
    /// assert!(state.values().get("optional_input").is_none());
    /// ```
    pub fn values(&self) -> &Map<String, Value> {
        &self.values
    }

    /// Produces a new state with the pinned operation-specific validation order.
    ///
    /// Primitive changes run pre/post transition validation, then full-state
    /// validation and roundtrip. Merge and subset construct new snapshots and
    /// run only full-state validation and roundtrip.
    /// Any failure leaves this state and its retained clones unchanged.
    ///
    /// ```
    /// use app_kit::workflow::{JsonState, StateChange, WorkflowState};
    /// use serde_json::{Map, Value};
    /// let state = WorkflowState::new(Map::from_iter([
    /// ("count".into(), Value::from("unknown")),
    /// ]))?;
    /// let change = StateChange::Increment {
    /// values: vec![("count".into(), Value::from(1))],
    /// };
    /// assert!(state.apply(&change, &JsonState).is_err());
    /// assert_eq!(state.values()["count"], "unknown");
    /// # Ok::<(), app_kit::AppError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for incompatible operand types, signed-integer delta or
    /// range errors, invalid state, or excessive operation payloads. Caller typing
    /// errors propagate unchanged; failure never mutates the original state.
    ///
    /// # Panics
    ///
    /// Panics if the caller-provided typing implementation panics. Direct state
    /// operations do not contain caller panics; App admission/execution has its own
    /// boundary for classifying them.
    pub fn apply<T: StateTyping>(
        &self,
        change: &StateChange,
        typing: &T,
    ) -> Result<Self, AppError> {
        if let StateChange::Wipe { keep } = change {
            StateChange::validate_count(keep.len())?;
            let keys = self
                .values
                .keys()
                .filter(|key| !keep.contains(key))
                .cloned()
                .collect();
            return self.apply(&StateChange::Remove { keys }, typing);
        }
        let snapshot = matches!(
            change,
            StateChange::Merge { .. } | StateChange::Subset { .. }
        );
        if !snapshot {
            typing.validate_pre(self, change)?;
        }
        let candidate = Self::new(change.apply_values(&self.values)?)?;
        if !snapshot {
            typing.validate_post(self, &candidate, change)?;
        }
        candidate.validate_typing(typing)?;
        Ok(candidate)
    }

    fn validate_typing<T: StateTyping>(&self, typing: &T) -> Result<(), AppError> {
        typing.validate_state(self)?;
        typing.validate_roundtrip(self)
    }
}

/// A workflow selected in one application workspace.
///
/// # Example
///
/// ```
/// # use agents_kit::Agent;
/// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_kit::StateStore;
/// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// #
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
/// # let workflow = workspace.workflow("counter".parse()?).await?;
/// let run = workflow.start(ExecutionOptions::default()).await?;
/// assert!(matches!(run.wait().await?.status, RunStatus::Succeeded));
/// #
/// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Workflow<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend = NoActions,
    C: ConditionBackend = NoConditions,
    T: StateTyping = JsonState,
    H: LifecycleHooks = NoHooks,
    E: ChildExecutor = InlineExecutor,
> {
    runtime: run::WorkflowRuntime<CS, A, X, C, T, H, E>,
}

/// A durable workflow-run handle.
///
/// This handle observes durable state and requests explicit controls. Dropping it
/// or a `wait`/snapshot future does not cancel accepted work. [`Self::execution`]
/// borrows it exclusively and returns an owner whose drop does revoke uncommitted
/// advancement. Use [`Self::cancel`] for durable, monotonic cancellation.
///
/// # Example
///
/// ```
/// # use agents_kit::Agent;
/// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_kit::StateStore;
/// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// #
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
/// # let workflow = workspace.workflow("counter".parse()?).await?;
/// # let mut run = workflow.prepare(WorkflowState::default()).await?;
/// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
/// assert!(matches!(terminal.status, RunStatus::Succeeded));
/// assert_eq!(run.snapshot().await?.state.values()["count"], 1);
/// #
/// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct WorkflowRun<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend = NoActions,
    C: ConditionBackend = NoConditions,
    T: StateTyping = JsonState,
    H: LifecycleHooks = NoHooks,
    E: ChildExecutor = InlineExecutor,
> {
    runtime: run::WorkflowRunRuntime<CS, A, X, C, T, H, E>,
}

/// One opaque, parsed `app-kit.workflow/v2` definition.
///
/// Construct definitions through [`Self::parse`]; their internals cannot be
/// copied through struct update syntax:
///
/// ```compile_fail
/// use app_kit::workflow::WorkflowDefinition;
/// fn reconstruct(definition: WorkflowDefinition) -> WorkflowDefinition {
/// WorkflowDefinition { ..definition }
/// }
/// ```
///
/// Only the strict versioned language is accepted. Unknown/duplicate fields,
/// invalid expressions, graph references and finite-bound violations fail before
/// runtime work. Inspection returns passive metadata; composition runs the same
/// compiler rather than exposing a mutable executable AST.
///
/// # Example
///
/// ```
/// use app_kit::workflow::WorkflowDefinition;
///
/// let definition = WorkflowDefinition::parse(r#"{
///     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#)?;
/// assert_eq!(definition.graph().entry, "increment");
/// # Ok::<(), app_kit::AppError>(())
/// ```
pub struct WorkflowDefinition {
    compiled: definition::CompiledDefinition,
}

/// A deterministic read-only graph export format.
pub enum GraphFormat {
    /// Portable graph metadata as JSON, without runtime state.
    Json,
    /// Graphviz DOT text; rendering requires no subprocess in this crate.
    Dot,
    /// Mermaid flowchart text with escaped caller labels.
    Mermaid,
}

/// Passive graph metadata in authored node and transition order.
///
/// # Example
///
/// ```
/// use app_kit::workflow::WorkflowDefinition;
///
/// let definition = WorkflowDefinition::parse(r#"{
///     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#)?;
/// let graph = definition.graph();
/// assert_eq!(graph.entry, "increment");
/// assert_eq!(graph.nodes.len(), 1);
/// # Ok::<(), app_kit::AppError>(())
/// ```
#[derive(Serialize)]
pub struct GraphDescription {
    /// Logical workflow identity.
    pub id: WorkflowId,
    /// Caller-selected definition revision.
    pub revision: String,
    /// First node's logical identity.
    pub entry: String,
    /// Authored node metadata, without executable bindings.
    pub nodes: Vec<GraphNode>,
    /// Ordered outgoing transitions, grouped by authored source-node order.
    pub edges: Vec<GraphEdge>,
}

/// One passive node declaration in a graph projection.
///
/// # Example
///
/// ```
/// use app_kit::workflow::WorkflowDefinition;
///
/// let definition = WorkflowDefinition::parse(r#"{
///     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#)?;
/// let graph = definition.graph();
/// let node = &graph.nodes[0];
/// assert_eq!(node.kind, "state");
/// assert_eq!(node.writes, ["count"]);
/// # Ok::<(), app_kit::AppError>(())
/// ```
#[derive(Serialize)]
pub struct GraphNode {
    /// Graph-local node identity.
    pub id: String,
    /// Optional caller-facing label.
    pub name: Option<String>,
    /// Optional authored description.
    pub description: Option<String>,
    /// Authored selector tags, in declaration order.
    pub tags: Vec<String>,
    /// Closed workflow step kind rendered as text.
    pub kind: String,
    /// Top-level state keys read by this node.
    pub reads: Vec<String>,
    /// Top-level state keys written by this node.
    pub writes: Vec<String>,
    /// Required transient input names.
    pub required_inputs: Vec<String>,
    /// Optional transient input names.
    pub optional_inputs: Vec<String>,
    /// Actual caller source or canonical authored built-in definition.
    pub source: Option<String>,
}

/// One ordered transition; a missing destination denotes path exhaustion.
///
/// # Example
///
/// ```
/// use app_kit::workflow::GraphEdge;
/// let end = GraphEdge { from: "increment".into(), to: None, condition: None };
/// assert!(end.to.is_none());
/// # Ok::<(), app_kit::AppError>(())
/// ```
#[derive(Serialize)]
pub struct GraphEdge {
    /// Source node identity.
    pub from: String,
    /// Destination node identity, or explicit end.
    pub to: Option<String>,
    /// Display expression, or the default true condition when absent.
    pub condition: Option<String>,
}

/// An owned graph piece and its graph-local node-name prefix.
///
/// # Example
///
/// ```
/// use app_kit::workflow::WorkflowDefinition;
/// const SOURCE: &str = r#"{
///     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#;
/// let definition = WorkflowDefinition::parse(SOURCE)?;
/// use app_kit::workflow::{GraphComposition, GraphConnection, GraphImport};
/// let composition = GraphComposition {
///     id: "composed".parse()?, revision: "r1".into(), entry: "a_increment".into(),
///     imports: vec![
///         GraphImport { definition, prefix: "a_".into() },
///         GraphImport { definition: WorkflowDefinition::parse(SOURCE)?, prefix: "b_".into() },
///     ],
///     connections: vec![GraphConnection { from: "a_increment".into(), to: "b_increment".into() }],
/// };
/// let composed = WorkflowDefinition::compose(composition)?;
/// assert_eq!(composed.graph().nodes.len(), 2);
/// assert_eq!(composed.graph().entry, "a_increment");
/// # Ok::<(), app_kit::AppError>(())
/// ```
pub struct GraphImport {
    /// Validated definition to import.
    pub definition: WorkflowDefinition,
    /// Prefix applied to nodes and their graph-local references.
    pub prefix: String,
}

/// A default transition appended to an assembled graph.
///
/// # Example
///
/// ```
/// use app_kit::workflow::WorkflowDefinition;
/// const SOURCE: &str = r#"{
///     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#;
/// let definition = WorkflowDefinition::parse(SOURCE)?;
/// use app_kit::workflow::{GraphComposition, GraphConnection, GraphImport};
/// let composition = GraphComposition {
///     id: "composed".parse()?, revision: "r1".into(), entry: "a_increment".into(),
///     imports: vec![
///         GraphImport { definition, prefix: "a_".into() },
///         GraphImport { definition: WorkflowDefinition::parse(SOURCE)?, prefix: "b_".into() },
///     ],
///     connections: vec![GraphConnection { from: "a_increment".into(), to: "b_increment".into() }],
/// };
/// let composed = WorkflowDefinition::compose(composition)?;
/// assert_eq!(composed.graph().nodes.len(), 2);
/// assert_eq!(composed.graph().entry, "a_increment");
/// # Ok::<(), app_kit::AppError>(())
/// ```
pub struct GraphConnection {
    /// Already-prefixed source node identity.
    pub from: String,
    /// Already-prefixed destination node identity.
    pub to: String,
}

/// Definitions and explicit connections for one ordinary compiler invocation.
///
/// # Example
///
/// ```
/// use app_kit::workflow::WorkflowDefinition;
/// const SOURCE: &str = r#"{
///     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#;
/// let definition = WorkflowDefinition::parse(SOURCE)?;
/// use app_kit::workflow::{GraphComposition, GraphConnection, GraphImport};
/// let composition = GraphComposition {
///     id: "composed".parse()?, revision: "r1".into(), entry: "a_increment".into(),
///     imports: vec![
///         GraphImport { definition, prefix: "a_".into() },
///         GraphImport { definition: WorkflowDefinition::parse(SOURCE)?, prefix: "b_".into() },
///     ],
///     connections: vec![GraphConnection { from: "a_increment".into(), to: "b_increment".into() }],
/// };
/// let composed = WorkflowDefinition::compose(composition)?;
/// assert_eq!(composed.graph().nodes.len(), 2);
/// assert_eq!(composed.graph().entry, "a_increment");
/// # Ok::<(), app_kit::AppError>(())
/// ```
pub struct GraphComposition {
    /// Identity of the assembled workflow.
    pub id: WorkflowId,
    /// Revision of the assembled workflow.
    pub revision: String,
    /// Already-prefixed entrypoint.
    pub entry: String,
    /// Graph pieces, preserving their declaration order.
    pub imports: Vec<GraphImport>,
    /// Ordered default transitions appended after imported transitions.
    pub connections: Vec<GraphConnection>,
}

/// A validated caller-authored workflow identity.
///
/// Parsing and deserialization accept only ASCII `[a-z0-9][a-z0-9_-]{0,127}`.
///
/// # Example
///
/// ```
/// use app_kit::workflow::WorkflowId;
/// let id: WorkflowId = "counter".parse()?;
/// assert_eq!(id.to_string(), "counter");
/// assert!("invalid identity".parse::<WorkflowId>().is_err());
/// # Ok::<(), app_kit::AppError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkflowId(String);

/// A validated root UUID or stable child SHA-256 identity.
///
/// Root identities use `run_` followed by a canonical lowercase UUIDv4. Child
/// identities also accept exactly 64 lowercase hexadecimal digits. One logical
/// identity may have multiple independent occurrences; use [`RunSelector::Exact`]
/// when the occurrence matters.
///
/// # Example
///
/// ```
/// use app_kit::workflow::RunId;
/// let id: RunId = "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?;
/// assert_eq!(id.to_string(), "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc");
/// assert!("invalid identity".parse::<RunId>().is_err());
/// # Ok::<(), app_kit::AppError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(String);

/// Selects an exact occurrence or the latest native observation of a logical run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunSelector {
    /// Last natively saved observation in this workflow and workspace scope.
    Latest(RunId),
    /// One admitted occurrence; an absent occurrence never falls back to Latest.
    Exact {
        /// Stable logical identity.
        id: RunId,
        /// Validated at method ingress as 32 lowercase hexadecimal digits.
        occurrence: String,
    },
}

impl RunSelector {
    pub(crate) fn validate(&self) -> Result<(), AppError> {
        match self {
            Self::Latest(_) => Ok(()),
            Self::Exact { occurrence, .. } => RunId::validate_occurrence(occurrence),
        }
    }

    pub(crate) fn id(&self) -> &RunId {
        match self {
            Self::Latest(id) | Self::Exact { id, .. } => id,
        }
    }
}

/// Initial state, transient inputs and boundaries selected for one execution.
///
/// `state` patches a new run only through [`Workflow::start`]; an already admitted
/// run requires an empty state in its execution options. Inputs are transient and
/// do not become state automatically. Selectors are node ids or `@tag` names and
/// must match. `max_steps` counts committed action boundaries, not partial results
/// or control checkpoints; private finite safety bounds always remain.
///
/// # Example
///
/// ```
/// use app_kit::workflow::{ExecutionOptions, WorkflowState};
/// let options = ExecutionOptions {
///     state: WorkflowState::new(serde_json::from_str(r#"{"count":4}"#)?)?,
///     halt_after: vec!["increment".into()],
///     ..ExecutionOptions::default()
/// };
/// assert_eq!(options.state.values()["count"], 4);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Default)]
pub struct ExecutionOptions {
    /// Initial patch for a new run; must be empty on an existing run.
    pub state: WorkflowState,
    /// Transient inputs reused by successive actions.
    pub inputs: Map<String, Value>,
    /// Node or tag selectors to stop before invocation.
    pub halt_before: Vec<String>,
    /// Node or tag selectors to stop after a committed boundary.
    pub halt_after: Vec<String>,
    /// Optional maximum number of committed action boundaries.
    pub max_steps: Option<NonZeroUsize>,
}

impl ExecutionOptions {
    pub(crate) fn resolve_selectors(&mut self, graph: &GraphDescription) -> Result<(), AppError> {
        for selectors in [&mut self.halt_before, &mut self.halt_after] {
            let mut resolved = Vec::new();
            for selector in selectors.iter() {
                let mut matches = graph
                    .nodes
                    .iter()
                    .filter(|node| {
                        selector
                            .strip_prefix('@')
                            .map_or(node.id == *selector, |tag| {
                                node.tags.iter().any(|candidate| candidate == tag)
                            })
                    })
                    .peekable();
                if matches.peek().is_none() {
                    return Err(AppError::InvalidInput(
                        "execution selector has no matching node".into(),
                    ));
                }
                for node in matches {
                    if !resolved.contains(&node.id) {
                        resolved.push(node.id.clone());
                    }
                }
            }
            *selectors = resolved;
        }
        Ok(())
    }
}

/// One transient result or committed execution boundary.
#[non_exhaustive]
pub enum ExecutionEvent {
    /// A source result which has not changed workflow state.
    Partial {
        /// Node whose source emitted this item.
        node: String,
        /// Transient output independent of workflow state.
        result: Map<String, Value>,
    },
    /// A durable action boundary with its complete ordered outcomes.
    Committed {
        /// Exact durable boundary sequence.
        checkpoint: u64,
        /// Complete results in canonical node order.
        outcomes: Vec<ActionOutcome>,
    },
    /// A selected boundary parked for a later execution.
    Halted {
        /// Selected next node or last committed node.
        node: String,
        /// Present only for a committed after-boundary, including an empty map.
        result: Option<Map<String, Value>>,
    },
}

/// The current caller-visible run truth.
///
/// # Example
///
/// ```
/// # use agents_kit::Agent;
/// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_kit::StateStore;
/// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// #
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
/// # let workflow = workspace.workflow("counter".parse()?).await?;
/// # let mut run = workflow.prepare(WorkflowState::default()).await?;
/// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
/// assert_eq!(terminal.state.values()["count"], 1);
/// assert_eq!(terminal.last_action_ordinal, Some(0));
/// #
/// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[non_exhaustive]
pub struct RunSnapshot {
    /// Identifies the run.
    pub id: RunId,
    /// Exact admitted execution occurrence of this logical run.
    pub occurrence: String,
    /// Fences explicit controls.
    pub checkpoint: u64,
    /// Projects lifecycle; actionable targets remain in `interactions`.
    pub status: RunStatus,
    /// Contains the current committed workflow state.
    pub state: WorkflowState,
    /// Canonical currently actionable controls.
    pub interactions: Vec<Interaction>,
    /// Last successfully committed action ordinal; controls do not advance it.
    pub last_action_ordinal: Option<u64>,
    /// Results from the latest committed wave, retained through later controls.
    pub outcomes: Vec<ActionOutcome>,
}

/// The current stable run status.
///
/// Inspect [`crate::workflow::RunSnapshot::interactions`] alongside this lifecycle projection.
/// Halted retains the selected before/after boundary and exact optional result;
/// Paused denotes an ordinary parked control or prepared run. Neither status
/// alone means that every external effect is safe to retry.
#[non_exhaustive]
pub enum RunStatus {
    /// Execution can still advance without caller input.
    Running,
    /// A committed boundary is parked for explicit execution.
    Paused,
    /// An explicitly selected node boundary is retained.
    Halted {
        /// Selected next node or last committed node.
        node: String,
        /// Present only for a committed after-boundary, including an empty map.
        result: Option<Map<String, Value>>,
    },
    /// Explicit cancellation has committed and owned cleanup is still running.
    Cancelling,
    /// The terminal frontier succeeded.
    Succeeded,
    /// The run terminated with a durable failure.
    Failed(WorkflowFailure),
    /// Explicit cancellation converged.
    Cancelled,
}

/// A caller-actionable workflow interaction.
#[derive(Clone)]
#[non_exhaustive]
pub enum Interaction {
    /// Requests one schema-checked caller value.
    Input {
        /// Identifies this run-scoped control.
        sequence: u64,
        /// Fences this input reservation independently of sibling checkpoints.
        generation: u64,
        /// Describes the requested value.
        message: String,
        /// Optionally constrains the response.
        schema: Option<Value>,
    },
    /// Exposes one exact lower-agent notification.
    Notification {
        /// Identifies this run-scoped control.
        sequence: u64,
        /// Fences this exact notification target independently of sibling checkpoints.
        generation: u64,
        /// Carries the exact portable lower notification.
        notification: Arc<ConversationNotification>,
    },
    /// Retains an external effect whose outcome has not been established.
    Unknown {
        /// Identifies this run-scoped interaction target.
        sequence: u64,
        /// Fences this target independently of sibling checkpoints.
        generation: u64,
        /// Reports whether authoritative evidence permits another attempt.
        retry_safe: bool,
    },
    /// Requests transient inputs for an action that has not started.
    Inputs {
        /// Identifies this run-scoped interaction target.
        sequence: u64,
        /// Fences this binding independently of sibling checkpoints.
        generation: u64,
        /// Names the exact missing input keys in canonical order.
        required: Vec<String>,
    },
}

/// Settles one current interaction or unknown effect.
///
/// Use the interaction sequence and its target generation from the same observed
/// item. Sibling checkpoints do not invalidate that reservation. Unknown effects
/// are never automatically retried; Retry requires authoritative retry-safe
/// evidence, and Accept is an explicit caller settlement.
pub enum Resolution {
    /// Supplies one caller-input value.
    Input(Value),
    /// Binds exact missing inputs in the retained execution, without persisting values.
    Inputs(Map<String, Value>),
    /// Resolves one exact lower notification.
    Notification(NotificationResolution),
    /// Accepts an unknown effect and stages ordinary state contribution.
    Accept(Map<String, Value>),
    /// Fails an unknown effect without claiming its external outcome.
    Fail,
    /// Retries only after authoritative evidence proves retry safe.
    Retry,
}

/// A stable terminal workflow-failure category.
#[non_exhaustive]
pub enum WorkflowFailure {
    /// Expression or routing evaluation failed.
    Evaluation(String),
    /// State validation or deterministic reduction failed.
    State(String),
    /// A lower agent operation failed safely.
    Agent(String),
    /// A caller-owned action source returned an error.
    Action(String),
    /// An opt-in lifecycle hook failed, retaining any prior execution failure.
    Hook {
        /// The caller-supplied hook error context.
        message: String,
        /// The execution failure observed before the hook failed.
        cause: Option<Box<WorkflowFailure>>,
    },
    /// A child or map relationship failed, or a child executor returned an error.
    ///
    /// An error returned by [`ChildExecutor::execute`]'s future has this category
    /// regardless of its [`AppError`] variant. Partial child results do not reduce
    /// the parent, and descendant cleanup retains ownership before publication.
    Child(String),
    /// Execution could not continue, including a contained child-executor panic.
    ///
    /// A panic while constructing or polling [`ChildExecutor::execute`]'s future
    /// has this category. It does not reduce the parent from partial child results;
    /// descendant cleanup retains ownership before terminal publication.
    Execution(String),
    /// A durable deadline expired.
    Timeout(String),
    /// A fixed private safety bound was exhausted.
    Limit(String),
}

impl WorkflowDefinition {
    /// Parses and validates one strict versioned workflow document.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::InvalidInput`] for malformed JSON, unsupported schema,
    /// unknown or duplicate members, invalid identities or expressions, inconsistent
    /// state/reducers, graph errors, or exceeded source/collection/depth limits.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::workflow::WorkflowDefinition;
    ///
    /// let definition = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#)?;
    /// assert_eq!(definition.graph().id.to_string(), "counter");
    /// assert!(WorkflowDefinition::parse(r#"{"schema":"app-kit.workflow/v1"}"#).is_err());
    /// # Ok::<(), app_kit::AppError>(())
    /// ```
    pub fn parse(source: &str) -> Result<Self, AppError> {
        Ok(Self {
            compiled: definition::CompiledDefinition::parse(source)?,
        })
    }

    pub(crate) fn into_compiled(self) -> definition::CompiledDefinition {
        self.compiled
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
> Workflow<CS, A, X, C, T, H, E>
{
    pub(crate) fn from_runtime(runtime: run::WorkflowRuntime<CS, A, X, C, T, H, E>) -> Self {
        Self { runtime }
    }

    /// Returns this workflow's stable identity.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// assert_eq!(workflow.id().to_string(), "counter");
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn id(&self) -> &WorkflowId {
        self.runtime.id()
    }

    /// Atomically admits one run at checkpoint zero.
    ///
    /// Schedules background advancement after admission. Dropping the returned run
    /// observer does not stop it. [`WorkflowRun::wait`] returns at an actionable or
    /// terminal boundary, so inspect status and interactions before assuming success.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for invalid initial state/options or missing bindings,
    /// `Busy` for exhausted admission/capacity, `Closed` for sealed ownership, or
    /// `Conflict` for stale workspace/revision evidence. Lower/storage failures use
    /// `Integration`; accepted execution failures appear in run status.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// let run = workflow.start(ExecutionOptions::default()).await?;
    /// let snapshot = run.wait().await?;
    /// assert_eq!(snapshot.state.values()["count"], 1);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn start(
        &self,
        options: ExecutionOptions,
    ) -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError> {
        Ok(WorkflowRun {
            runtime: self.runtime.start(options).await?,
        })
    }

    /// Admits checkpoint zero without executing the entry node.
    ///
    /// The initial patch is validated and atomically stored using the same
    /// workspace and App admission fences as background execution. Observers
    /// can read the parked run, and cancellation uses its checkpoint fence.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for invalid initial state or missing bindings,
    /// `Busy` for exhausted admission/capacity, `Closed` for sealed ownership, or
    /// `Conflict` for stale workspace/revision evidence or exhausted bounded
    /// transaction-contention retries. Concurrent preparations are not guaranteed
    /// to all succeed. Lower/storage failures use `Integration`; accepted
    /// execution failures appear in run status.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// let run = workflow.prepare(WorkflowState::default()).await?;
    /// let checkpoint = run.checkpoint(CheckpointSelector::Sequence(0)).await?;
    /// assert_eq!(checkpoint.state.values()["count"], 0);
    /// assert!(checkpoint.action_ordinal.is_none());
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn prepare(
        &self,
        state: WorkflowState,
    ) -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError> {
        Ok(WorkflowRun {
            runtime: self.runtime.prepare(state.values).await?,
        })
    }

    /// Copies a clean retained boundary into a fresh, paused run.
    ///
    /// Source selection and new admission commit atomically. The source remains
    /// unchanged; actions run only after explicit execution of the returned run.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` for absent source/checkpoint without fallback, `InvalidInput`
    /// for invalid overrides or selectors, `Busy` for exhausted capacity, and
    /// `Conflict` when the selected boundary is unsafe to copy. Sealing/revocation
    /// returns `Closed`; inconsistent history or storage failures return `Integration`.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// use app_kit::workflow::{RestoreOptions, ResumePosition};
    /// let options = RestoreOptions {
    ///     source: RunSelector::Latest(run.id().clone()),
    ///     checkpoint: CheckpointSelector::Action(0),
    ///     overrides: WorkflowState::default(), position: ResumePosition::AfterCompleted,
    ///     fallback: None,
    /// };
    /// let copy = workflow.restore(options).await?;
    /// assert_ne!(copy.id(), run.id());
    /// let copied = copy.checkpoint(CheckpointSelector::Sequence(0)).await?;
    /// assert_eq!(copied.state.values()["count"], 1);
    /// assert_eq!(&copied.lineage.as_ref().unwrap().source, run.id());
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn restore(
        &self,
        options: RestoreOptions,
    ) -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError> {
        Ok(WorkflowRun {
            runtime: self.runtime.restore(options).await?,
        })
    }

    /// Reopens one run by typed identity.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for a malformed exact occurrence, `NotFound` when the
    /// selected run/occurrence is absent, or `Closed` for sealed scope. Exact selection
    /// never falls back to Latest; inconsistent retained evidence is `Integration`.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let id = run.id().clone();
    /// let reopened = workflow.open_run(RunSelector::Latest(id.clone())).await?;
    /// assert_eq!(reopened.id(), &id);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn open_run(
        &self,
        selector: RunSelector,
    ) -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError> {
        Ok(WorkflowRun {
            runtime: self.runtime.open_run(selector).await?,
        })
    }

    /// Lists runs by logical identity, then exact occurrence token.
    ///
    /// An Exact cursor continues after that occurrence; a Latest cursor skips
    /// every occurrence of its logical identity. This inventory order does not
    /// represent the native save order used when opening Latest.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for a limit above 1,000 or malformed cursor, `NotFound`
    /// for an unavailable cursor, and `Closed` or `Integration` for revoked or
    /// unreadable scope.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let runs = workflow.list_runs(None, std::num::NonZeroUsize::new(10).unwrap()).await?;
    /// assert_eq!(runs.len(), 1);
    /// assert_eq!(&runs[0].id, run.id());
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn list_runs(
        &self,
        after: Option<RunSelector>,
        limit: NonZeroUsize,
    ) -> Result<Vec<RunSnapshot>, AppError> {
        self.runtime.list_runs(after, limit).await
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
> WorkflowRun<CS, A, X, C, T, H, E>
{
    /// Returns this run's stable identity.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// assert_eq!(run.snapshot().await?.id, *run.id());
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn id(&self) -> &RunId {
        self.runtime.id()
    }

    /// Acquires exclusive caller-local advancement without constructing a source.
    ///
    /// A live execution keeps that borrow until its last use:
    ///
    /// ```compile_fail
    /// use agents_kit::AgentBackend;
    /// use app_kit::{AppError, workflow::{ExecutionOptions, RunSnapshot, WorkflowRun}};
    /// use storage_kit::StateStoreBackend;
    /// async fn overlapping<CS: StateStoreBackend, A: AgentBackend>(
    /// run: &mut WorkflowRun<CS, A>,
    /// ) -> Result<RunSnapshot, AppError> {
    /// let execution = run.execution(ExecutionOptions::default()).await?;
    /// run.snapshot().await?;
    /// execution.finish().await
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for a nonempty options state or invalid inputs/selectors,
    /// `Busy` if advancement or admission is already owned, `Conflict` for stale
    /// authority, and `Closed` after revocation. Storage failures use `Integration`.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// assert_eq!(run.snapshot().await?.checkpoint, terminal.checkpoint);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn execution(
        &mut self,
        options: ExecutionOptions,
    ) -> Result<Execution<'_, CS, A, X, C, T, H, E>, AppError> {
        Execution::open(self, options, ActionMode::Asynchronous).await
    }

    /// Loads the current run snapshot.
    ///
    /// # Errors
    ///
    /// Returns `Closed` for revoked App ownership or sealed/deleted scope, `NotFound`
    /// for absent retained evidence, or `Integration` for inconsistent or unreadable
    /// records. A newer App generation can invalidate this handle.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let snapshot = run.snapshot().await?;
    /// assert_eq!(snapshot.checkpoint, 0);
    /// assert!(matches!(snapshot.status, RunStatus::Paused));
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn snapshot(&self) -> Result<RunSnapshot, AppError> {
        self.runtime.snapshot().await
    }

    /// Reads one immutable checkpoint without advancing the workflow.
    ///
    /// An action selector returns the latest successful or failed observation
    /// of that ordinal. Missing or explicitly deleted history returns NotFound.
    ///
    /// # Errors
    ///
    /// Returns `Closed` for revoked App ownership or sealed/deleted scope, `NotFound`
    /// for absent retained evidence, or `Integration` for inconsistent or unreadable
    /// records. A newer App generation can invalidate this handle.
    /// An explicit missing sequence/action is `NotFound`; action ordinals are distinct
    /// from checkpoint sequences and select the latest matching saved observation.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// let initial = run.checkpoint(CheckpointSelector::Sequence(0)).await?;
    /// let action = run.checkpoint(CheckpointSelector::Action(0)).await?;
    /// assert_eq!(initial.state.values()["count"], 0);
    /// assert_eq!(action.state.values()["count"], 1);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn checkpoint(&self, selector: CheckpointSelector) -> Result<Checkpoint, AppError> {
        self.runtime.checkpoint(selector).await
    }

    /// Reads a bounded page from a fixed retained-history cutoff.
    ///
    /// Limits are 1 through 1,000. A returned cursor preserves its cutoff and
    /// is valid only for this run creation and App generation. Reading a page
    /// never invokes actions, state typing, codecs or lower-agent recovery.
    ///
    /// # Errors
    ///
    /// Returns `Closed` for revoked App ownership or sealed/deleted scope, `NotFound`
    /// for absent retained evidence, or `Integration` for inconsistent or unreadable
    /// records. A newer App generation can invalidate this handle.
    /// An excessive page limit or a cursor from another run/occurrence/generation is
    /// `InvalidInput`. A captured cutoff never includes later appended checkpoints.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// let page = run.history(None, std::num::NonZeroUsize::new(1).unwrap()).await?;
    /// assert_eq!(page.checkpoints[0].sequence, 0);
    /// let next = run.history(page.next, std::num::NonZeroUsize::new(100).unwrap()).await?;
    /// assert!(next.checkpoints.iter().all(|checkpoint| checkpoint.sequence > 0));
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn history(
        &self,
        after: Option<HistoryCursor>,
        limit: NonZeroUsize,
    ) -> Result<HistoryPage, AppError> {
        self.runtime.history(after, limit).await
    }

    /// Deletes terminal history through bounded, resumable pages.
    ///
    /// The expected checkpoint fences the terminal boundary. Dropping this
    /// operation's observer preserves admitted deletion and native commits.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` for a stale checkpoint, `Busy` for nonterminal/unknown work
    /// or a child still referenced by an active parent, and `Closed` after sealing or
    /// revocation. Storage failures use `Integration`. A successful seal survives
    /// observer loss and deletion resumes in bounded pages after reopen.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// let id = run.id().clone();
    /// run.delete(terminal.checkpoint).await?;
    /// assert!(matches!(workflow.open_run(RunSelector::Latest(id)).await, Err(app_kit::AppError::NotFound(_))));
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn delete(&mut self, expected_checkpoint: u64) -> Result<(), AppError> {
        self.runtime.delete(expected_checkpoint).await
    }

    /// Waits for the next actionable or terminal snapshot.
    ///
    /// This observes accepted work; it does not acquire execution ownership or resume
    /// a prepared run. A caller interaction or unknown effect can return before a
    /// terminal result. Dropping the observation future never requests cancellation.
    ///
    /// # Errors
    ///
    /// Returns `Closed` for revoked App ownership or sealed/deleted scope, `NotFound`
    /// for absent retained evidence, or `Integration` for inconsistent or unreadable
    /// records. A newer App generation can invalidate this handle.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// let background = workflow.start(ExecutionOptions::default()).await?;
    /// let observed = background.wait().await?;
    /// assert!(matches!(observed.status, RunStatus::Succeeded));
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn wait(&self) -> Result<RunSnapshot, AppError> {
        self.runtime.wait().await
    }

    /// Commits explicit checkpoint-fenced cancellation.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` for a stale checkpoint, `Busy` for unavailable admission,
    /// `Closed` after revocation, or `Integration` for unreadable/failed storage.
    /// Cancellation admission is durable; observer loss does not reverse it.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"counter", "revision":"r1",
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
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let before = run.snapshot().await?;
    /// run.cancel(before.checkpoint).await?;
    /// let cancelled = run.wait().await?;
    /// assert!(matches!(cancelled.status, RunStatus::Cancelled));
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn cancel(&self, expected_checkpoint: u64) -> Result<RunSnapshot, AppError> {
        self.runtime.cancel(expected_checkpoint).await
    }

    /// Settles one exact interaction using its original target generation.
    ///
    /// Sibling checkpoints do not invalidate that generation. An identical
    /// decision replays its retained receipt; a different decision conflicts.
    /// Transient inputs replay only while their execution retains the values.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for a mismatched resolution or invalid schema/input,
    /// `Conflict` for a stale target generation, a different repeated decision, or an
    /// unsafe retry. Capacity, revocation, lower/storage failure and observation
    /// deadlines report `Busy`, `Closed`, `Integration`, or `Timeout`.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #  "schema":"app-kit.workflow/v2","id":"counter","revision":"r1",
    /// #  "state":{"answer":""},"required":["answer"],"reducers":{"answer":"reject_concurrent"},
    /// #  "entry":"ask","nodes":[{"id":"ask","step":{"kind":"input",
    /// #  "message":[{"kind":"text","text":"Review topic?"}],"target":"answer"}}],"routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// use app_kit::workflow::{Interaction, Resolution};
    /// let run = workflow.start(ExecutionOptions::default()).await?;
    /// let waiting = run.wait().await?;
    /// let Interaction::Input { sequence, generation, .. } = waiting.interactions[0] else {
    ///     panic!("the input workflow must request a value");
    /// };
    /// run.resolve(sequence, generation, Resolution::Input(serde_json::json!("ownership"))).await?;
    /// assert_eq!(run.wait().await?.state.values()["answer"], "ownership");
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn resolve(
        &self,
        sequence: u64,
        generation: u64,
        resolution: Resolution,
    ) -> Result<RunSnapshot, AppError> {
        self.runtime.resolve(sequence, generation, resolution).await
    }
}

impl WorkflowId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl RunId {
    pub(crate) fn validate_occurrence(occurrence: &str) -> Result<(), AppError> {
        if occurrence.len() == 32
            && occurrence
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(())
        } else {
            Err(AppError::InvalidInput(
                "run occurrence must contain 32 lowercase hexadecimal digits".into(),
            ))
        }
    }

    pub(crate) fn generate() -> Self {
        Self(format!("run_{}", Uuid::new_v4().hyphenated()))
    }

    fn child(&self, task_key: &str) -> Self {
        let mut digest = Sha256::new();
        digest.update(self.as_str().as_bytes());
        digest.update(b":");
        digest.update(task_key.as_bytes());
        Self(format!("{:x}", digest.finalize()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for WorkflowId {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        IdentitySyntax::Named("WorkflowId").parse(value).map(Self)
    }
}

impl Serialize for WorkflowId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for WorkflowId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RunId {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Ok(Self(value.to_owned()));
        }
        IdentitySyntax::Uuid {
            name: "RunId",
            prefix: "run_",
        }
        .parse(value)
        .map(Self)
    }
}

impl Serialize for RunId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RunId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
