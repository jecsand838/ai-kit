use futures_util::Stream;
use futures_util::stream::Empty;
use serde_json::{Map, Value};

use crate::workflow::definition::{JsonBoundary, MAX_COLLECTION_ITEMS, MAX_TEXT_BYTES, SchemaText};
use crate::workflow::execution::MAX_RUNTIME_BYTES;
use crate::workflow::{RunId, StateChange, WorkflowId, WorkflowState};
use crate::{AppError, WorkspaceId};

/// A caller-owned action whose lazy source produces results and explicit writes.
///
/// The declaration remains stable for its logical key and revision. App retains
/// the exact implementation without requiring `Clone` or normalizing it through
/// an erased handle. The returned source begins external work only when polled.
///
/// An explicit source error is an action failure, including
/// [`AppError::Integration`]. A captured construction or polling panic is an
/// execution failure; an App activation deadline is a timeout failure. EOF
/// without a completed result succeeds with no output. Before storage commit
/// admission, these outcomes, owning drop and abort discard staged workflow
/// state and preserve the prior action position. None automatically retries.
///
/// The caller owns the source's internal I/O, cancellation, independently spawned
/// tasks and external reconciliation. Workflow-state rollback does not undo
/// those effects; a later explicit execution can repeat them. App does not infer
/// an Unknown effect merely from polling the source. Separately recorded
/// App-owned lower-operation or admitted-storage uncertainty retains its own
/// reconciliation authority after source observation ends.
///
/// # Example
///
/// ```
/// use app_kit::{AppError, workflow::{ActionBackend, ActionDeclaration, ActionEvent,
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
/// use app_kit::workflow::{ActionContext, WorkflowState};
/// let request = ActionRequest {
///     state: WorkflowState::new(Map::from_iter([("count".into(), Value::from(2))]))?,
///     inputs: Map::new(), context: ActionContext {
///     workspace: "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
///     workflow: "counter".parse()?,
///     run: "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
///     occurrence: "00000000000040008000000000000001".into(),
///     checkpoint: 0, action_ordinal: 0, node: "increment".into(), attempt: 0,
///     mode: ActionMode::Asynchronous, parent: None, parent_occurrence: None,
/// },
/// };
/// use futures_util::StreamExt;
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// let mut source = action.start(request);
/// let ActionEvent::Completed { result, writes } = source.next().await.unwrap()? else {
///     panic!("increment emits a completed result");
/// };
/// assert_eq!(result["new_count"], 3);
/// assert_eq!(writes.len(), 1);
/// # Ok::<(), app_kit::AppError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait ActionBackend: Send + Sync + 'static {
    /// The exact caller source, which may be local to one task and need not be Send.
    ///
    /// App stops polling at the first Completed event or error. A source that
    /// ends without either leaves the attempted action uncommitted.
    type Stream: Stream<Item = Result<ActionEvent, AppError>> + 'static;

    /// Returns the stable declaration validated when App binds the inventory.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{ActionBackend, ActionDeclaration, ActionEvent,
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
    /// assert_eq!(action.declaration().reads, ["count"]);
    /// assert_eq!(action.declaration().revision, "r1");
    /// ```
    fn declaration(&self) -> &ActionDeclaration;

    /// Constructs the source for one attempt without performing external work.
    ///
    /// The source may be local and non-Send. Construct it without starting external
    /// I/O: the first poll accepts that work. Emit partial results for observation and
    /// one Completed result with explicit state writes; EOF before Completed commits
    /// no action. The action owns cancellation and reconciliation of its own I/O and
    /// spawned tasks. State rollback cannot undo caller external effects.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{ActionBackend, ActionDeclaration, ActionEvent,
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
    /// use app_kit::workflow::{ActionContext, WorkflowState};
    /// let request = ActionRequest {
    ///     state: WorkflowState::new(Map::from_iter([("count".into(), Value::from(2))]))?,
    ///     inputs: Map::new(), context: ActionContext {
    ///     workspace: "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
    ///     workflow: "counter".parse()?,
    ///     run: "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
    ///     occurrence: "00000000000040008000000000000001".into(),
    ///     checkpoint: 0, action_ordinal: 0, node: "increment".into(), attempt: 0,
    ///     mode: ActionMode::Asynchronous, parent: None, parent_occurrence: None,
    /// },
    /// };
    /// use futures_util::StreamExt;
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// let mut source = action.start(request);
    /// let ActionEvent::Completed { result, writes } = source.next().await.unwrap()? else {
    ///     panic!("increment emits a completed result");
    /// };
    /// assert_eq!(result["new_count"], 3);
    /// assert_eq!(writes.len(), 1);
    /// # Ok::<(), app_kit::AppError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn start(&self, request: ActionRequest) -> Self::Stream;
}

/// Execution modes supported by a caller action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionMode {
    /// Supports execution driven by the synchronous session.
    Synchronous,
    /// Supports execution driven by an asynchronous session.
    Asynchronous,
    /// Supports either mode; an invocation context selects one concrete mode.
    Both,
}

/// Caller-authored metadata and transient input rules for an action binding.
///
/// Read and write names describe state keys; they do not hide other values or
/// restrict explicit writes. Input names are data, except the reserved `__*`
/// namespace. Repeated declarations are accepted. Defaults apply only to
/// optional, non-required inputs; bound values may satisfy required inputs and
/// cannot be overridden by runtime input, even with an equal value.
///
/// # Example
///
/// ```
/// use app_kit::workflow::{ActionDeclaration, ActionMode};
/// use serde_json::Map;
/// let declaration = ActionDeclaration {
///     key: "increment".into(), revision: "r1".into(), reads: vec!["count".into()],
///     writes: vec!["count".into()], required_inputs: Vec::new(), optional_inputs: Vec::new(),
///     bound_inputs: Map::new(), default_inputs: Map::new(), tags: vec!["counter".into()],
///     source: Some("increment count by one".into()), mode: ActionMode::Both,
/// };
/// assert!(declaration.required_inputs.is_empty());
/// assert_eq!(declaration.writes, ["count"]);
/// ```
pub struct ActionDeclaration {
    /// Canonical logical binding key, using the workflow identity grammar.
    pub key: String,
    /// Nonempty caller revision identifying this stable behavior.
    pub revision: String,
    /// Declared state reads, retaining caller order.
    pub reads: Vec<String>,
    /// Declared state writes, retaining caller order.
    pub writes: Vec<String>,
    /// Input names that must be present after defaults, bindings and runtime input.
    pub required_inputs: Vec<String>,
    /// Input names eligible for defaults.
    pub optional_inputs: Vec<String>,
    /// Fixed input values; undeclared input names are permitted.
    pub bound_inputs: Map<String, Value>,
    /// Defaults for optional inputs, merged before fixed bindings.
    pub default_inputs: Map<String, Value>,
    /// Caller tags preserved for graph inspection and selectors.
    pub tags: Vec<String>,
    /// Optional caller source text for inspection.
    pub source: Option<String>,
    /// Invocation modes this action supports.
    pub mode: ActionMode,
}

/// One action attempt's immutable state, resolved transient inputs and identity.
///
/// # Example
///
/// ```
/// use app_kit::{AppError, workflow::{ActionBackend, ActionDeclaration, ActionEvent,
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
/// use app_kit::workflow::{ActionContext, WorkflowState};
/// let request = ActionRequest {
///     state: WorkflowState::new(Map::from_iter([("count".into(), Value::from(2))]))?,
///     inputs: Map::new(), context: ActionContext {
///     workspace: "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
///     workflow: "counter".parse()?,
///     run: "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
///     occurrence: "00000000000040008000000000000001".into(),
///     checkpoint: 0, action_ordinal: 0, node: "increment".into(), attempt: 0,
///     mode: ActionMode::Asynchronous, parent: None, parent_occurrence: None,
/// },
/// };
/// use futures_util::StreamExt;
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// let mut source = action.start(request);
/// let ActionEvent::Completed { result, writes } = source.next().await.unwrap()? else {
///     panic!("increment emits a completed result");
/// };
/// assert_eq!(result["new_count"], 3);
/// assert_eq!(writes.len(), 1);
/// # Ok::<(), app_kit::AppError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ActionRequest {
    /// The full immutable base state, independent of transient inputs and results.
    pub state: WorkflowState,
    /// Resolved defaults, fixed bindings and all caller runtime inputs.
    pub inputs: Map<String, Value>,
    /// Application identity and execution position for this attempt.
    pub context: ActionContext,
}

/// Passive application context; it contains no runtime or lower backend handle.
///
/// # Example
///
/// ```
/// use app_kit::workflow::{ActionContext, ActionMode};
/// let context = ActionContext {
///     workspace: "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
///     workflow: "counter".parse()?,
///     run: "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?,
///     occurrence: "00000000000040008000000000000001".into(),
///     checkpoint: 0, action_ordinal: 0, node: "increment".into(), attempt: 0,
///     mode: ActionMode::Asynchronous, parent: None, parent_occurrence: None,
/// };
/// assert_eq!(context.action_ordinal, 0);
/// assert!(context.parent.is_none());
/// # Ok::<(), app_kit::AppError>(())
/// ```
#[derive(Clone)]
pub struct ActionContext {
    /// Workspace containing the workflow run.
    pub workspace: WorkspaceId,
    /// Exact logical workflow identity.
    pub workflow: WorkflowId,
    /// Run receiving this action's staged result.
    pub run: RunId,
    /// Exact admitted execution occurrence.
    pub occurrence: String,
    /// Committed base checkpoint sequence.
    pub checkpoint: u64,
    /// Attempted action ordinal; failed attempts do not advance committed state.
    pub action_ordinal: u64,
    /// Node being invoked.
    pub node: String,
    /// Attempt identity within this activation.
    pub attempt: u64,
    /// Actual invocation mode, always Synchronous or Asynchronous, never Both.
    pub mode: ActionMode,
    /// Parent run when invoked as a child workflow.
    pub parent: Option<RunId>,
    /// Exact occurrence of the parent, when this action belongs to a child.
    pub parent_occurrence: Option<String>,
}

/// Output from the single lazy source owned by an action attempt.
pub enum ActionEvent {
    /// Transient partial output that does not commit workflow state.
    Partial {
        /// Caller result fields for this partial observation.
        result: Map<String, Value>,
    },
    /// The first authoritative terminal result and its staged state operations.
    Completed {
        /// Transient final result, distinct from durable workflow state.
        result: Map<String, Value>,
        /// Ordered state operations to validate and reduce at the commit barrier.
        writes: Vec<StateChange>,
    },
    /// Begins an explicitly addressed tracing span within this action attempt.
    SpanStarted {
        /// Caller-local span identity.
        id: String,
        /// Display name of the span.
        name: String,
        /// Optional parent span identity within the attempt.
        parent: Option<String>,
        /// Preceding spans on which this span depends.
        dependencies: Vec<String>,
    },
    /// Attaches an attribute to an addressed span or the action's root span.
    SpanAttribute {
        /// Target span, or None for the action's root span.
        span: Option<String>,
        /// Attribute name.
        key: String,
        /// Attribute value.
        value: Value,
        /// Caller tags attached to this attribute.
        tags: Map<String, Value>,
    },
    /// Finishes an explicitly addressed span.
    SpanFinished {
        /// The previously started span identity.
        id: String,
    },
}

impl ActionEvent {
    pub(crate) fn validate_span(&self) -> Result<(), AppError> {
        let bytes = match self {
            Self::SpanStarted {
                id,
                name,
                parent,
                dependencies,
            } => {
                if dependencies.len() > MAX_COLLECTION_ITEMS
                    || [id.as_str(), name.as_str()]
                        .into_iter()
                        .chain(parent.as_deref())
                        .chain(dependencies.iter().map(String::as_str))
                        .any(|text| text.len() > MAX_TEXT_BYTES)
                {
                    return Err(AppError::InvalidInput(
                        "caller span metadata exceeds its boundary".into(),
                    ));
                }
                dependencies.iter().fold(
                    id.len()
                        .saturating_add(name.len())
                        .saturating_add(parent.as_ref().map_or(0, String::len)),
                    |bytes, dependency| bytes.saturating_add(dependency.len()),
                )
            }
            Self::SpanAttribute {
                span,
                key,
                value,
                tags,
            } => {
                if key.len() > MAX_TEXT_BYTES
                    || span
                        .as_ref()
                        .is_some_and(|span| span.len() > MAX_TEXT_BYTES)
                    || tags.len() > MAX_COLLECTION_ITEMS
                    || tags.values().any(|value| {
                        value
                            .as_str()
                            .is_none_or(|value| value.len() > MAX_TEXT_BYTES)
                    })
                {
                    return Err(AppError::InvalidInput(
                        "caller span attribute exceeds its boundary".into(),
                    ));
                }
                JsonBoundary::validate(value)?;
                JsonBoundary::validate_map(tags)?;
                let value_bytes = serde_json::to_vec(value)
                    .map_err(|_| AppError::InvalidInput("caller span attribute is invalid".into()))?
                    .len();
                let tag_bytes = serde_json::to_vec(tags)
                    .map_err(|_| AppError::InvalidInput("caller span tags are invalid".into()))?
                    .len();
                key.len()
                    .saturating_add(span.as_ref().map_or(0, String::len))
                    .saturating_add(value_bytes)
                    .saturating_add(tag_bytes)
            }
            Self::SpanFinished { id } => {
                if id.len() > MAX_TEXT_BYTES {
                    return Err(AppError::InvalidInput(
                        "caller span identity exceeds its boundary".into(),
                    ));
                }
                id.len()
            }
            Self::Partial { .. } | Self::Completed { .. } => {
                return Err(AppError::Integration(
                    "action event is not span metadata".into(),
                ));
            }
        };
        if bytes > MAX_RUNTIME_BYTES {
            return Err(AppError::InvalidInput(
                "caller span event exceeds its byte boundary".into(),
            ));
        }
        Ok(())
    }
}

/// A caller-owned predicate evaluated against the full immutable workflow state.
///
/// App validates declared reads before evaluating the predicate, including
/// reads within composed conditions that would otherwise short-circuit.
///
/// # Example
///
/// ```
/// use app_kit::{AppError, workflow::{ConditionBackend, ConditionDeclaration, WorkflowState}};
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
/// let state = WorkflowState::new(serde_json::from_str(r#"{"count":3}"#)?)?;
/// assert!(condition.evaluate(&state)?);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait ConditionBackend: Send + Sync + 'static {
    /// Returns the stable declaration for one logical predicate revision.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{ConditionBackend, ConditionDeclaration, WorkflowState}};
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
    /// assert_eq!(condition.declaration().key, "ready");
    /// assert_eq!(condition.declaration().reads, ["count"]);
    /// ```
    fn declaration(&self) -> &ConditionDeclaration;

    /// Evaluates the predicate without advancing workflow state or action ordinals.
    ///
    /// # Errors
    ///
    /// Return an error when evaluation cannot produce a valid boolean. App validates
    /// declared reads before calling this method; caller failures stop that routing
    /// decision rather than being interpreted as false.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{ConditionBackend, ConditionDeclaration, WorkflowState}};
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
    /// let state = WorkflowState::new(serde_json::from_str(r#"{"count":3}"#)?)?;
    /// assert!(condition.evaluate(&state)?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn evaluate(&self, state: &WorkflowState) -> Result<bool, AppError>;
}

/// Caller-authored binding identity and declared reads for a predicate.
///
/// # Example
///
/// ```
/// use app_kit::workflow::ConditionDeclaration;
/// let declaration = ConditionDeclaration {
///     key: "ready".into(), revision: "r1".into(), reads: vec!["count".into()],
/// };
/// assert_eq!(declaration.reads, ["count"]);
/// ```
pub struct ConditionDeclaration {
    /// Canonical logical binding key, using the workflow identity grammar.
    pub key: String,
    /// Nonempty caller revision identifying this stable behavior.
    pub revision: String,
    /// Top-level state keys required before predicate evaluation.
    pub reads: Vec<String>,
}

/// Uninhabited default action type for an empty App action inventory.
///
/// An empty inventory needs no placeholder action or declaration:
/// ```
/// use app_kit::workflow::NoActions;
/// let actions: Vec<NoActions> = Vec::new();
/// assert!(actions.is_empty());
/// ```
pub enum NoActions {}

/// Uninhabited default predicate type for an empty App condition inventory.
///
/// ```
/// use app_kit::workflow::NoConditions;
/// let conditions: Vec<NoConditions> = Vec::new();
/// assert!(conditions.is_empty());
/// ```
pub enum NoConditions {}

impl ActionBackend for NoActions {
    type Stream = Empty<Result<ActionEvent, AppError>>;

    fn declaration(&self) -> &ActionDeclaration {
        match *self {}
    }

    fn start(&self, _request: ActionRequest) -> Self::Stream {
        match *self {}
    }
}

impl ConditionBackend for NoConditions {
    fn declaration(&self) -> &ConditionDeclaration {
        match *self {}
    }

    fn evaluate(&self, _state: &WorkflowState) -> Result<bool, AppError> {
        match *self {}
    }
}

pub(crate) enum ActionInputs {
    Ready(Map<String, Value>),
    Required(Vec<String>),
}

impl ActionInputs {
    pub(crate) fn into_values(self) -> Result<Map<String, Value>, AppError> {
        match self {
            Self::Ready(values) => Ok(values),
            Self::Required(_) => Err(AppError::InvalidInput(
                "required action input is missing".into(),
            )),
        }
    }
}

impl ActionDeclaration {
    pub(crate) fn result_projection(&self, result: &Map<String, Value>) -> StateChange {
        let mut values = Map::new();
        for key in &self.writes {
            if let Some(value) = result.get(key) {
                values.insert(key.clone(), value.clone());
            }
        }
        StateChange::Assign { values }
    }

    pub(crate) fn resolve_inputs(
        &self,
        runtime: &Map<String, Value>,
        mode: ActionMode,
    ) -> Result<ActionInputs, AppError> {
        if !matches!(
            (mode, self.mode),
            (
                ActionMode::Synchronous,
                ActionMode::Synchronous | ActionMode::Both
            ) | (
                ActionMode::Asynchronous,
                ActionMode::Asynchronous | ActionMode::Both
            )
        ) {
            return Err(AppError::InvalidInput(
                "action does not support the selected execution mode".into(),
            ));
        }
        JsonBoundary::validate_inputs(runtime)?;
        let mut resolved = self.default_inputs.clone();
        for (key, value) in &self.bound_inputs {
            resolved.insert(key.clone(), value.clone());
        }
        for (key, value) in runtime {
            if self.bound_inputs.contains_key(key) {
                return Err(AppError::InvalidInput(
                    "runtime input cannot override a bound action input".into(),
                ));
            }
            resolved.insert(key.clone(), value.clone());
        }
        JsonBoundary::validate_map(&resolved)?;
        let mut required = self
            .required_inputs
            .iter()
            .filter(|key| !resolved.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        required.sort_unstable();
        required.dedup();
        if required.is_empty() {
            Ok(ActionInputs::Ready(resolved))
        } else {
            Ok(ActionInputs::Required(required))
        }
    }

    pub(crate) fn validate_reads(&self, state: &WorkflowState) -> Result<(), AppError> {
        if self
            .reads
            .iter()
            .any(|key| !state.values().contains_key(key))
        {
            return Err(AppError::InvalidInput(
                "declared action state read is missing".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), AppError> {
        SchemaText::node_or_state("action binding key", &self.key)?;
        SchemaText::revision(&self.revision)?;
        if [
            self.reads.len(),
            self.writes.len(),
            self.required_inputs.len(),
            self.optional_inputs.len(),
            self.bound_inputs.len(),
            self.default_inputs.len(),
            self.tags.len(),
        ]
        .into_iter()
        .any(|length| length > MAX_COLLECTION_ITEMS)
        {
            return Err(AppError::InvalidInput(
                "action declaration exceeds the private collection bound".into(),
            ));
        }
        for key in self.reads.iter().chain(&self.writes) {
            JsonBoundary::validate_key(key)?;
        }
        for key in self
            .required_inputs
            .iter()
            .chain(&self.optional_inputs)
            .chain(self.bound_inputs.keys())
            .chain(self.default_inputs.keys())
        {
            JsonBoundary::validate_key(key)?;
            if key.starts_with("__") {
                return Err(AppError::InvalidInput(
                    "action input uses the reserved runtime namespace".into(),
                ));
            }
        }
        if self
            .default_inputs
            .keys()
            .any(|key| self.required_inputs.contains(key) || !self.optional_inputs.contains(key))
        {
            return Err(AppError::InvalidInput(
                "action defaults require optional, non-required inputs".into(),
            ));
        }
        if self.tags.iter().any(|tag| tag.len() > MAX_TEXT_BYTES)
            || self
                .source
                .as_ref()
                .is_some_and(|source| source.len() > MAX_TEXT_BYTES)
        {
            return Err(AppError::InvalidInput(
                "action declaration text exceeds the private byte bound".into(),
            ));
        }
        JsonBoundary::validate_map(&self.bound_inputs)?;
        JsonBoundary::validate_map(&self.default_inputs)?;
        JsonBoundary::validate_size(&(
            &self.key,
            &self.revision,
            &self.reads,
            &self.writes,
            &self.required_inputs,
            &self.optional_inputs,
            &self.bound_inputs,
            &self.default_inputs,
            &self.tags,
            &self.source,
        ))
    }
}

impl ConditionDeclaration {
    pub(crate) fn validate(&self) -> Result<(), AppError> {
        SchemaText::node_or_state("condition binding key", &self.key)?;
        SchemaText::revision(&self.revision)?;
        if self.reads.len() > MAX_COLLECTION_ITEMS {
            return Err(AppError::InvalidInput(
                "condition declaration exceeds the private collection bound".into(),
            ));
        }
        for key in &self.reads {
            JsonBoundary::validate_key(key)?;
        }
        JsonBoundary::validate_size(&(&self.key, &self.revision, &self.reads))
    }
}
