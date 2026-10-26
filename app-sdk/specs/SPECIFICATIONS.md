# app-sdk public contract

This is the sole exact caller-facing signature and type ledger. Focused specs
own behavior. The 2026-09-13 completion goal explicitly replaces the former
20-type/22-method limit and adopts pinned rurr **core observable semantics**.
This is a target contract; passing tests and review establish implementation
status. No paragraph here claims that a declared capability is implemented.

## Authority and boundaries

The user-approved completion plan and pinned rurr commit
`005bfe3ece631e64b06dac2d42ef84235f727a4f` resolve competing core semantics.
`workflows/RESEARCH.md` records exact provenance and scope. The owning lower
crate ledgers govern their traits and portable DTOs. App consumes only
`CS: storage_sdk::StateStoreBackend` and `A: agent_sdk::AgentBackend`; it uses
`CS::Transaction`, `A::Workspace` and its associated Conversation unchanged.
No supplied lower handle is a default generic argument or required value.
App has no provider-sdk/tool-sdk dependency, lower reexport, public erased
backend, lower factory, private lower import or native payload inspection.

Only the crate root and `app_sdk::workflow` are public namespaces. Supporting
implementation modules remain private even when their types are reexported
from `workflow`. There are five App-owned static behavioral seams:
`ActionBackend`, `ConditionBackend`, `StateTyping`, `LifecycleHooks`, and
`ChildExecutor`. Their implementations are concrete caller values; heterogeneous
inventories use caller-owned exhaustive enums. No behavior requires Clone.
The ChildExecutor associated future is the sole App-only GAT return exception.
`StateSerialization` is the sole narrowly scoped process-global callable
registration owner; private field/tag function erasure cannot store behaviors
or lower backends.

## Root inventory and generics

Root types: `App`, `AppConfig`, `AgentSpec`, `Shutdown`, `Workspace`,
`WorkspaceId`, `WorkspaceConfig`, `WorkspaceSnapshot`, `AppError`.
`AppState` is deleted, including its ephemeral construction path.

`App`, `AppConfig`, `Workspace`, `Workflow`, `WorkflowRun`, `Execution`, and
`SyncExecution` preserve parameters in this order:

```rust
CS: StateStoreBackend,
A: AgentBackend,
X: ActionBackend = NoActions,
C: ConditionBackend = NoConditions,
T: StateTyping = JsonState,
H: LifecycleHooks = NoHooks,
E: ChildExecutor = InlineExecutor,
```

CS and A have no defaults. The default types belong under `workflow`.
NoActions/NoConditions are uninhabited exhaustive enums without Default;
their empty inventories represent absence of callable behavior. Their trait
methods eliminate the impossible receiver with an exhaustive match, so there
is no fabricated declaration, global metadata initialization or panic-only
default binding. JsonState, NoHooks and InlineExecutor are concrete unit
structs with derived Default: JsonState provides raw JSON typing, NoHooks
performs no callback, and InlineExecutor applies the admitted batch's actual
synchronous or asynchronous scheduling semantics.
These defaults make a state/agent-only App usable without invented lower
implementations.
Runtime/configuration owners and validated identities have private fields.
AgentSpec owns its non-Clone A through eventual consuming shutdown; it has no
AgentId accessor or configuration reconstruction path.

The following signatures use the same full seven-parameter impl bounds above;
writing the parameter list once does not create a Rust item type alias.

```rust
impl<CS, A> AppConfig<CS, A>
where CS: StateStoreBackend, A: AgentBackend {
    pub fn new(state: CS, agents: Vec<AgentSpec<A>>,
               workflows: Vec<WorkflowDefinition>) -> Self;
}
impl<CS, A, X, C, T, H, E> AppConfig<CS, A, X, C, T, H, E> {
    pub fn with_actions<N: ActionBackend>(self, actions: Vec<N>)
        -> AppConfig<CS, A, N, C, T, H, E>;
    pub fn with_conditions<N: ConditionBackend>(self, conditions: Vec<N>)
        -> AppConfig<CS, A, X, N, T, H, E>;
    pub fn with_state_typing<N: StateTyping>(self, typing: N)
        -> AppConfig<CS, A, X, C, N, H, E>;
    pub fn with_hooks<N: LifecycleHooks>(self, hooks: N)
        -> AppConfig<CS, A, X, C, T, N, E>;
    pub fn with_child_executor<N: ChildExecutor>(self, executor: N)
        -> AppConfig<CS, A, X, C, T, H, N>;
}
impl<A: AgentBackend> AgentSpec<A> {
    pub fn new(key: String, revision: String, agent: A) -> Self;
}
impl<CS, A, X, C, T, H, E> App<CS, A, X, C, T, H, E> {
    pub async fn open(config: AppConfig<CS, A, X, C, T, H, E>) -> Result<Self, AppError>;
    pub async fn create_workspace(&self, config: WorkspaceConfig)
        -> Result<Workspace<CS, A, X, C, T, H, E>, AppError>;
    pub async fn workspace(&self, id: WorkspaceId)
        -> Result<Workspace<CS, A, X, C, T, H, E>, AppError>;
    pub async fn list_workspaces(&self, after: Option<WorkspaceId>, limit: NonZeroUsize)
        -> Result<Vec<WorkspaceSnapshot>, AppError>;
    pub async fn shutdown(&mut self, policy: Shutdown) -> Result<(), AppError>;
}
impl<CS, A, X, C, T, H, E> Workspace<CS, A, X, C, T, H, E> {
    pub fn id(&self) -> &WorkspaceId;
    pub async fn snapshot(&self) -> Result<WorkspaceSnapshot, AppError>;
    pub async fn replace(&self, expected_revision: u64, config: WorkspaceConfig)
        -> Result<WorkspaceSnapshot, AppError>;
    pub async fn workflow(&self, id: WorkflowId)
        -> Result<Workflow<CS, A, X, C, T, H, E>, AppError>;
    pub async fn delete(&mut self, expected_revision: u64) -> Result<(), AppError>;
}
```

with_actions/with_conditions replace the respective construction inventory in
one move; they do not normalize values, create a runtime registry, or permit
changing an accepted run's bindings. Their declarations carry logical key and
revision, so no redundant ActionSpec/ConditionSpec wrapper is exposed.
Each authored Action/custom Condition reference includes its required exact
revision, under the syntax and retained-pin rules in WORKFLOW.md. Distinct
revisions may coexist; inventory order never supplies a missing selector.

```rust
pub enum Shutdown {
    RejectActive,
    Drain { timeout: Duration },
    CancelAndDrain { timeout: Duration },
}
pub struct WorkspaceConfig { pub name: Option<String>, pub root: PathBuf }
#[non_exhaustive]
pub struct WorkspaceSnapshot {
    pub id: WorkspaceId, pub revision: u64, pub config: WorkspaceConfig,
}
#[non_exhaustive]
pub enum AppError {
    InvalidInput(String), NotFound(String), Conflict(String), Busy(String),
    Closed(String), Timeout(String), Integration(String),
}
```

## Workflow inventory and receiver operations

In addition to the five traits/defaults, `workflow` exports only:
`Workflow`, `WorkflowRun`, `WorkflowDefinition`, `WorkflowId`, `RunId`, `RunSelector`,
`WorkflowState`, `StateChange`, `StateSerialization`, `ActionDeclaration`,
`ActionMode`, `ActionRequest`, `ActionContext`, `ActionEvent`,
`ConditionDeclaration`, `LifecycleEvent`, `ChildBatch`, `ChildCompletion`,
`Execution`, `SyncExecution`, `ExecutionOptions`, `ExecutionEvent`,
`ActionOutcome`, `RunSnapshot`, `RunStatus`, `WorkflowFailure`, `Interaction`,
`Resolution`, `Checkpoint`, `CheckpointKind`, `HistoryCursor`, `HistoryPage`,
`CheckpointSelector`, `ResumePosition`, `RestoreOptions`, `RunLineage`,
`GraphDescription`, `GraphNode`, `GraphEdge`, `GraphFormat`, `GraphImport`,
`GraphConnection`, and `GraphComposition`.

```rust
impl<CS, A, X, C, T, H, E> Workflow<CS, A, X, C, T, H, E> {
    pub fn id(&self) -> &WorkflowId;
    pub async fn start(&self, options: ExecutionOptions)
        -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError>;
    pub async fn prepare(&self, state: WorkflowState)
        -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError>;
    pub async fn open_run(&self, selector: RunSelector)
        -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError>;
    pub async fn list_runs(&self, after: Option<RunSelector>, limit: NonZeroUsize)
        -> Result<Vec<RunSnapshot>, AppError>;
    pub async fn restore(&self, options: RestoreOptions)
        -> Result<WorkflowRun<CS, A, X, C, T, H, E>, AppError>;
}
impl<CS, A, X, C, T, H, E> WorkflowRun<CS, A, X, C, T, H, E> {
    pub fn id(&self) -> &RunId;
    pub async fn snapshot(&self) -> Result<RunSnapshot, AppError>;
    pub async fn wait(&self) -> Result<RunSnapshot, AppError>;
    pub async fn cancel(&self, expected_checkpoint: u64) -> Result<RunSnapshot, AppError>;
    pub async fn resolve(&self, sequence: u64, expected_generation: u64,
                         resolution: Resolution) -> Result<RunSnapshot, AppError>;
    pub async fn execution(&mut self, options: ExecutionOptions)
        -> Result<Execution<'_, CS, A, X, C, T, H, E>, AppError>;
    pub async fn checkpoint(&self, selector: CheckpointSelector) -> Result<Checkpoint, AppError>;
    pub async fn history(&self, after: Option<HistoryCursor>, limit: NonZeroUsize)
        -> Result<HistoryPage, AppError>;
    pub async fn delete(&mut self, expected_checkpoint: u64) -> Result<(), AppError>;
}
impl<'a, CS, A, X, C, T, H, E> Execution<'a, CS, A, X, C, T, H, E> {
    pub async fn next(&mut self) -> Result<Option<ExecutionEvent>, AppError>;
    pub async fn step(&mut self) -> Result<Option<ExecutionEvent>, AppError>;
    pub async fn finish(self) -> Result<RunSnapshot, AppError>;
    pub async fn abort(self) -> Result<RunSnapshot, AppError>;
}
impl<'a, CS, A, X, C, T, H, E> SyncExecution<'a, CS, A, X, C, T, H, E> {
    pub fn open(run: &'a mut WorkflowRun<CS, A, X, C, T, H, E>, options: ExecutionOptions)
        -> Result<Self, AppError>;
    pub fn try_next(&mut self) -> Result<Option<ExecutionEvent>, AppError>;
    pub fn step(&mut self) -> Result<Option<ExecutionEvent>, AppError>;
    pub fn finish(self) -> Result<RunSnapshot, AppError>;
    pub fn abort(self) -> Result<RunSnapshot, AppError>;
}
```

Execution and SyncExecution are non-Clone, must_use owners. SyncExecution owns
the same Execution and a captured runtime handle; it creates no thread, channel
or second kernel. Blocking calls reject an entered Tokio runtime context before
blocking, while source construction, polling and Drop remain on the calling thread.
`Execution::next` and `SyncExecution::try_next` advance
lazily to one partial/boundary event; `step` drains partials to one committed
boundary; `finish` drains to halt/exhaustion; `abort` finalizes the current
uncommitted boundary. `prepare` admits checkpoint zero without executing;
`start` atomically admits using options.state, then uses the same driver in
background mode. The supervised execution-worker lane constructs and polls ChildExecutor's
associated future locally; no unexpressible higher-ranked Send bound over all F
is imposed on background admission. Only Send intent/owners/results cross that
worker boundary. The same kernel serves inline and worker execution; no public
ChildTask, erased future or duplicate child trait is added. Worker ownership and
local-future integration remain explicit regression and review obligations in
`workflows/TESTING.md`; the `worker_contract` and `child_executor_contract`
targets exercise those boundaries.

The existing methods select observable halt precedence. `next` maps to pinned
streaming iteration, testing halt-after-prior before halt-before-next; `step`,
`finish` and background `start` map to ordinary iteration/run, testing
halt-before-next first. The method committing a boundary selects its order,
including when `step` or `finish` drains a source previously polled by `next`.
A retained halt decision is never reclassified by a later method. Both lanes
execute their first current-session action before checking either halt against
historical position; a partial or no-output EOF does not count as a commit.
The exact source anchors and acceptance cases belong to WORKFLOW.md's halting
contract; all methods retain one transition and commit kernel.

`ExecutionEvent::Halted` and `RunStatus::Halted` project the same retained halt
decision: halt-before identifies the pending node with no result; halt-after
identifies the completed node with its exact result, including an empty map.
`finish`, background observation and reopen expose that tuple through snapshot
status. A plain Paused snapshot plus committed outcomes cannot preserve this
rurr run-output distinction, so Paused remains reserved for ordinary parked
controls. The data-carrying status variant extends the existing DTO rather than
adding a result wrapper or inventing an uncommitted ActionOutcome. Snapshot
outcomes and action ordinals continue to describe only committed work.

```rust
#[derive(Clone, Default)]
pub struct ExecutionOptions {
    pub state: WorkflowState,
    pub inputs: Map<String, Value>,
    pub halt_before: Vec<String>,
    pub halt_after: Vec<String>,
    pub max_steps: Option<NonZeroUsize>,
}
#[non_exhaustive]
pub enum ExecutionEvent {
    Partial { node: String, result: Map<String, Value> },
    Committed { checkpoint: u64, outcomes: Vec<ActionOutcome> },
    Halted { node: String, result: Option<Map<String, Value>> },
}
#[non_exhaustive]
pub struct ActionOutcome {
    pub node: String, pub ordinal: u64, pub result: Map<String, Value>,
}
#[non_exhaustive]
pub struct RunSnapshot {
    pub id: RunId, pub occurrence: String, pub checkpoint: u64, pub status: RunStatus,
    pub state: WorkflowState, pub interactions: Vec<Interaction>,
    pub last_action_ordinal: Option<u64>, pub outcomes: Vec<ActionOutcome>,
}
#[non_exhaustive]
pub enum RunStatus {
    Running, Paused,
    Halted { node: String, result: Option<Map<String, Value>> },
    Cancelling, Succeeded, Failed(WorkflowFailure), Cancelled,
}
#[non_exhaustive]
pub enum Interaction {
    Input { sequence: u64, generation: u64, message: String, schema: Option<Value> },
    Notification { sequence: u64, generation: u64,
        notification: Arc<agent_sdk::notification::ConversationNotification> },
    Unknown { sequence: u64, generation: u64, retry_safe: bool },
    Inputs { sequence: u64, generation: u64, required: Vec<String> },
}
pub enum Resolution {
    Input(Value), Notification(agent_sdk::notification::NotificationResolution),
    Accept(Map<String, Value>), Inputs(Map<String, Value>), Fail, Retry,
}
#[non_exhaustive]
pub enum WorkflowFailure {
    Evaluation(String), State(String), Agent(String), Action(String), Child(String),
    Execution(String), Timeout(String), Limit(String),
    Hook { message: String, cause: Option<Box<WorkflowFailure>> },
}
```

Options.state is an explicit initial-state patch at new-run admission; on
already-admitted execution it must be empty. Inputs never implicitly become
state and are reused by successive actions according to declared bindings.
Default execution options use empty state/inputs/selectors and no caller
step limit; private finite runtime bounds always remain. Snapshot status is
lifecycle; a canonical actionable collection can represent several waiting,
unknown, and input-rebinding targets simultaneously.

## Actions, conditions and immutable state

The public Stream bound is `futures_util::Stream`. App may use the existing
pinned workspace futures-util package directly; no new package/version or
general dependency exception is authorized.

```rust
pub trait ActionBackend: Send + Sync + 'static {
    type Stream: Stream<Item = Result<ActionEvent, AppError>> + 'static;
    fn declaration(&self) -> &ActionDeclaration;
    fn start(&self, request: ActionRequest) -> Self::Stream;
}
pub enum ActionMode { Synchronous, Asynchronous, Both }
pub struct ActionDeclaration {
    pub key: String, pub revision: String,
    pub reads: Vec<String>, pub writes: Vec<String>,
    pub required_inputs: Vec<String>, pub optional_inputs: Vec<String>,
    pub bound_inputs: Map<String, Value>, pub default_inputs: Map<String, Value>,
    pub tags: Vec<String>, pub source: Option<String>, pub mode: ActionMode,
}
pub struct ActionRequest {
    pub state: WorkflowState, pub inputs: Map<String, Value>, pub context: ActionContext,
}
#[derive(Clone)]
pub struct ActionContext {
    pub workspace: WorkspaceId, pub workflow: WorkflowId, pub run: RunId,
    pub occurrence: String, pub checkpoint: u64, pub action_ordinal: u64, pub node: String, pub attempt: u64,
    pub mode: ActionMode,
    pub parent: Option<RunId>,
    pub parent_occurrence: Option<String>,
}
pub enum ActionEvent {
    Partial { result: Map<String, Value> },
    Completed { result: Map<String, Value>, writes: Vec<StateChange> },
    SpanStarted { id: String, name: String, parent: Option<String>, dependencies: Vec<String> },
    SpanAttribute { span: Option<String>, key: String, value: Value, tags: Map<String, Value> },
    SpanFinished { id: String },
}
pub trait ConditionBackend: Send + Sync + 'static {
    fn declaration(&self) -> &ConditionDeclaration;
    fn evaluate(&self, state: &WorkflowState) -> Result<bool, AppError>;
}
pub struct ConditionDeclaration {
    pub key: String, pub revision: String, pub reads: Vec<String>,
}
pub enum StateChange {
    Assign { values: Map<String, Value> },
    Append { values: Vec<(String, Value)> },
    Extend { values: Vec<(String, Vec<Value>)> },
    Increment { values: Vec<(String, Value)> },
    Remove { keys: Vec<String> },
    Wipe { keep: Vec<String> },
    Merge { state: WorkflowState },
    Subset { keys: Vec<String> },
}
impl WorkflowState {
    pub fn new(values: Map<String, Value>) -> Result<Self, AppError>;
    pub fn values(&self) -> &Map<String, Value>;
    pub fn apply<T: StateTyping>(&self, change: &StateChange, typing: &T)
        -> Result<Self, AppError>;
    pub fn serialize(&self) -> Result<Map<String, Value>, AppError>;
    pub fn deserialize(values: Map<String, Value>) -> Result<Self, AppError>;
}
pub trait StateTyping: Send + Sync + 'static {
    type Data;
    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError>;
    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError>;
    fn validate_pre(&self, state: &WorkflowState, change: &StateChange) -> Result<(), AppError>;
    fn validate_post(&self, before: &WorkflowState, after: &WorkflowState,
                     change: &StateChange) -> Result<(), AppError>;
    fn validate_state(&self, state: &WorkflowState) -> Result<(), AppError>;
    fn validate_roundtrip(&self, state: &WorkflowState) -> Result<(), AppError>;
}
```

WorkflowState is an invariant-bearing ordered immutable state owner with
private fields, Clone/Debug/PartialEq/Default. Data has no Send, Clone or
serialization bound. Validation methods have success defaults; roundtrip's
default invokes both conversions and checks successful reconstruction, not
raw equality. JsonState overrides the roundtrip fast path only. All state
production, including forks and child overrides, uses the same validation
order in WORKFLOW.md; no unchecked engine state-mutator is a second contract.
StateChange carries resolved values, never a public expression AST. Primitive
changes run pre/post transition validation before full-state validation and
roundtrip; Merge/Subset reconstruct snapshots and run only the latter two,
following the pinned state operations. Each primitive payload is one atomic
operation: pre/post observe the complete batch once. Assign preserves map order;
Append/Extend/Increment preserve every ordered payload entry, including repeated
keys. Increment validates each resolved JSON delta as signed i64. Remove accepts
repeated and missing names. Payload/selector counts share the existing 4,096-entry App collection bound
(including repeated names), an explicit finite-resource restriction relative to
pinned rurr; exceeding it returns InvalidInput without changing original state; no singular twin API
is retained. A later entry failure discards the entire candidate.
ActionEvent span variants provide the caller action's trace context through the
same stream without a second callback/command owner. Span nesting is bounded;
attributes/finish address an open span and invalid order is an execution error.
Each activation permits at most 4,096 distinct ids, including closed spans,
with at most 64 open ancestors including the addressed span. Span text uses the
existing 65,536-byte bound; dependency/tag collections use the 4,096-entry bound.
Each event's content and the activation's retained id bytes are separately
bounded by 1 MiB. Event content counts UTF-8 text fields plus encoded attribute
value/tag JSON, without a second serialized event envelope. Attribute value/tag
JSON also obeys the existing depth/collection limits and 128-byte object-key
bound; that includes tag keys. The SpanAttribute key field remains span text.
Metadata events feed LifecycleHooks and never safe tracing content automatically.
WorkflowDefinition.graph remains an authored, unbound projection. Registered
uses the same GraphDescription resolved against exact declaration pairs, with
actual source and input/read/write metadata. Unresolved callable source is None
and cannot be exported as a successful tracking record. For callable nodes,
omitted node tags inherit declaration tags; explicit tags replace them,
including an empty replacement. These are behavior rules on existing owners,
not additional graph models, public binding constructors or revision DTOs.

```rust
impl StateSerialization {
    pub fn global() -> &'static Self;
    pub fn register_field<S, D, SE, DE>(&self, field: String, serialize: S, deserialize: D)
        -> Result<(), AppError>
        where S: Fn(&Value) -> Result<Value, SE> + Send + Sync + 'static,
              D: Fn(&Value) -> Result<Value, DE> + Send + Sync + 'static,
              SE: Display, DE: Display;
    pub fn register_tag<D, DE>(&self, tag: String, deserialize: D) -> Result<(), AppError>
        where D: Fn(&Value) -> Result<Value, DE> + Send + Sync + 'static, DE: Display;
}
```

No reset, unregister, public dyn callable, general service lookup or Any API
exists. Error messages preserve caller codec context as required by rurr but
are not copied into default operational tracing. Global handler snapshots are
immutable per serialization operation; handlers run with no registry lock.

## Lifecycle and admitted child execution

```rust
pub trait LifecycleHooks: Send + Sync + 'static {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError>;
    fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>)
        -> impl Future<Output = Result<(), AppError>> + Send + 'a;
}
pub enum LifecycleEvent<'a> {
    Registered { workspace: &'a WorkspaceId, run: &'a RunId, occurrence: &'a str,
                 graph: &'a GraphDescription, parent: Option<&'a ActionContext>,
                 lineage: Option<&'a RunLineage> },
    ChildFinished { parent: &'a ActionContext, snapshot: &'a RunSnapshot },
    ExecuteStarted { run: &'a RunId, occurrence: &'a str },
    ExecuteFinished { snapshot: &'a RunSnapshot, failure: Option<&'a WorkflowFailure> },
    StepStarted { context: &'a ActionContext, state: &'a WorkflowState,
                  inputs: &'a Map<String, Value> },
    StepFinished { context: &'a ActionContext,
                   outcome: Option<Result<&'a ActionOutcome, &'a WorkflowFailure>>,
                   checkpoint: &'a Checkpoint },
    StreamStarted { context: &'a ActionContext, span: Option<&'a str> },
    StreamItem { context: &'a ActionContext, span: Option<&'a str>, index: usize,
                 result: &'a Map<String, Value> },
    StreamFinished { context: &'a ActionContext, span: Option<&'a str>, items: usize,
                     failure: Option<&'a WorkflowFailure> },
    SpanStarted { context: &'a ActionContext, id: &'a str, name: &'a str,
                  parent: Option<&'a str>, dependencies: &'a [String] },
    SpanAttribute { context: &'a ActionContext, span: Option<&'a str>, key: &'a str,
                    value: &'a Value, tags: &'a Map<String, Value> },
    SpanFinished { context: &'a ActionContext, id: &'a str },
}
impl LifecycleEvent<'_> {
    pub fn tracking_records(&self, observed_at: SystemTime) -> Result<Vec<Value>, AppError>;
}
pub trait ChildExecutor: Send + Sync + 'static {
    type Execution<'a, F>: Future<Output = Result<Vec<ChildCompletion>, AppError>> + 'a
        where Self: 'a, F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
        where F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
}
impl<F> ChildBatch<F> {
    pub fn mode(&self) -> ActionMode;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn into_tasks(self) -> Result<Vec<Pin<Box<F>>>, AppError>;
}
impl ChildCompletion { pub fn ordinal(&self) -> usize; }
impl<F> Future for ChildBatch<F>
where F: Future<Output = Result<ChildCompletion, AppError>> {
    type Output = Result<Vec<ChildCompletion>, AppError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
}
```

Hook methods have success defaults. Async execution observes sync then async;
SyncExecution invokes only the synchronous hook method, including finish/abort.
The internal async lower-operation lane never changes that caller-visible mode.
StepFinished runs for both success and failure: success borrows the committed
outcome/state; failure borrows the failure and unchanged committed base, never
an invented empty result. Normal EOF, explicit abort and owning execution drop
without a completed or failed action carry no outcome. Their checkpoint is an
App-native view of the unchanged authoritative base; it does not fabricate a
pinned tracking end entry. Projecting such a StepFinished through tracking_records
returns an error before invoking codecs. ActionContext.action_ordinal is the attempted semantic
ordinal and is independent of control-checkpoint sequence. Repeated span names
are legal; context mode is the actual Synchronous or Asynchronous invocation,
never the declaration-only Both value. Bounded caller span ids uniquely address
one activation's spans.
An explicit parent names an open span and dependencies retain authored order,
including repeated opaque dependency names; they do not require open-span
membership. Attributes with no span are action-scoped, and an unaddressed
Partial does not acquire the most recently opened span. Attribute tag values
must be strings. Unknown/closed ids, any id reuse within an activation and a
parent finish while its child is open fail without fabricating span events.
The no-reuse rule maps the pin's action ordinal and monotonic child sequence
identity (`visibility/span.rs:70–85,131–140`) to caller-supplied event ids.
Completion, source failure, EOF and abort close remaining spans in reverse
start order, children before parents, before StepFinished. Owning Drop uses
only synchronous closure after any admitted callback settles. This automatic
closure is the App-native adaptation of structured visibility scopes, not a
claim that a raw pinned span handle has a Drop finalizer. A Completed event
cannot stage its result until span closure succeeds; an end-hook error rejects
that uncommitted result. Exact lane, receipt and cleanup rules are owned by
OBSERVABILITY.md.

Event variants form an exhaustive borrowed vocabulary, not individually
registered callbacks. ChildBatch and
ChildCompletion are non-Clone with private fields/constructors; only admitted
child futures produce completions. App validates exact membership and reduces
in original order independently of executor polling order. ChildBatch retains
the actual invocation ActionMode, exposed by mode(): Synchronous or
Asynchronous, never the declaration-only Both value. Its Future polls the
complete admitted vector concurrently in asynchronous mode, awaits all child
results even when one fails, then collects Results in original input order.
In synchronous mode it polls children sequentially and stops at the first
error. These are the pinned `parallelism/executor.rs:89–110` defaults.
InlineExecutor's associated future is this same concrete owner, not a leaked
private type, another public future wrapper or an associated opaque erasure.
ChildBatch pins its
concrete child futures once; it never requires F: Unpin or F: Send. Before its
first poll, into_tasks transfers those exact pins to a custom executor, which
may select different polling order. After polling begins, extraction returns
Conflict and revokes the consumed batch without moving pinned values or
silently dropping completed membership evidence. This container refinement
requires downstream compile/runtime proof with local non-Unpin children before
the signature is closed. Core spans/events
are caller-visible through the hook vocabulary; exporter transport is excluded.
Pipeline is synchronous-only: asynchronous invocation rejects before child
admission or effects, while synchronous pipeline executes state handoff through
the same kernel sequentially and bypasses ChildExecutor. This follows pinned
`parallelism/executor.rs:119–136,149–169`; there is no alternate pipeline engine.

## Retained history, selection and restore

```rust
pub enum CheckpointKind {
    Initial, Control, Action,
    Failed { node: String, ordinal: u64 },
    Terminal,
}
#[non_exhaustive]
pub struct Checkpoint {
    pub run: RunId, pub occurrence: String, pub sequence: u64, pub kind: CheckpointKind,
    pub workflow: WorkflowId, pub revision: String,
    pub action_ordinal: Option<u64>, pub position: Option<String>,
    pub state: WorkflowState, pub status: RunStatus,
    pub outcomes: Vec<ActionOutcome>, pub lineage: Option<RunLineage>,
}
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub enum CheckpointSelector { Latest, Sequence(u64), Action(u64) }
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub enum ResumePosition { AfterCompleted, AtSaved, Entrypoint }
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunSelector {
    Latest(RunId),
    Exact { id: RunId, occurrence: String },
}
pub struct RestoreOptions {
    pub source: RunSelector, pub checkpoint: CheckpointSelector,
    pub overrides: WorkflowState, pub position: ResumePosition,
    pub fallback: Option<WorkflowState>,
}
#[non_exhaustive]
pub struct RunLineage {
    pub source: RunId, pub source_occurrence: String,
    pub checkpoint: u64, pub action_ordinal: Option<u64>,
}
#[non_exhaustive]
pub struct HistoryPage { pub checkpoints: Vec<Checkpoint>, pub next: Option<HistoryCursor> }
```

CheckpointSelector and ResumePosition are passive serializable policy enums.
CheckpointSelector uses the adjacent `kind`/`value` representation, with canonical
forms `{"kind":"latest"}`, `{"kind":"sequence","value":N}` and
`{"kind":"action","value":N}`; N is a u64 and unknown members reject.
ResumePosition uses its snake_case string variants. Authored initializer syntax
adds its exact field-presence validation before converting to these enums,
including rejecting a `value` member on `latest`. This reuses the same policy
values across authored and durable child tasks without a second selector enum.
It does not add serde to WorkflowState or RestoreOptions.

RunSelector is the only additional public occurrence selector. Its Exact token
uses the existing creation-token syntax: exactly 32 lowercase hexadecimal ASCII
characters. Validate before storage reads; malformed input returns InvalidInput.
It is passive request data, not a backend identity, handle, revision or new
wrapper family. RunSnapshot/Checkpoint and registration/action lifecycle data
expose this same token so a caller can select each independently admitted child.
Stable RunId remains unchanged; handles bind one exact occurrence when opened.
The focused CHECKPOINTS contract owns Latest selection, listing, deletion and
recovery. Tracking's rurr app_id remains the stable RunId; the occurrence token
is App control-plane metadata, not a salted tracking identity.

HistoryCursor is an opaque validated serialized cursor carrying a committed
head cutoff and deletion/generation scope; Clone/Debug/Serialize/Deserialize
are public commitments. It is not a native StateStore cursor or public tuple.
Action selection resolves the latest matching successful or failed action
observation at the captured head. An Action checkpoint matches every ordinal in
its committed outcomes; Failed matches its attempted ordinal. Initial, Control
and Terminal checkpoints create no action observation. Failed carries the
attempted node/ordinal without inventing an ActionOutcome: Checkpoint.position
and action_ordinal still describe the last successful commit, its state remains
committed state, and its outcomes are empty. The owning CHECKPOINTS contract
defines canonical attribution for a failed BSP wave and preserves the serial
pinned selector behavior. Explicit missing selection fails unless RestoreOptions supplies fallback;
fallback does not inherit the requested sequence. Restore creates a fresh run,
copies only clean committed state, and records lineage; it never overwrites the
source. Same-run continuation uses open_run/execution. These operations together
provide resume, rewind via new identity, and fork without duplicate APIs.
RunLineage.action_ordinal identifies the selected source observation: an explicit
Action(n) keeps n even when the selected BSP row also contains later siblings;
other selectors use a Failed row's attempted ordinal, otherwise its last
committed ordinal. The copied Checkpoint.action_ordinal/position remain the
source's successful committed metadata. CHECKPOINTS documents this resolved
pointer translation separately from the pin's requested load-target pointer.

## Read-only graph description and composition

```rust
impl WorkflowDefinition {
    pub fn parse(source: &str) -> Result<Self, AppError>;
    pub fn graph(&self) -> GraphDescription;
    pub fn export(&self, format: GraphFormat) -> Result<String, AppError>;
    pub fn compose(composition: GraphComposition) -> Result<Self, AppError>;
}
pub enum GraphFormat { Json, Dot, Mermaid }
pub struct GraphDescription {
    pub id: WorkflowId, pub revision: String, pub entry: String,
    pub nodes: Vec<GraphNode>, pub edges: Vec<GraphEdge>,
}
impl GraphDescription {
    pub fn tracking_record(&self) -> Result<Value, AppError>;
}
pub struct GraphNode {
    pub id: String, pub name: Option<String>, pub description: Option<String>,
    pub tags: Vec<String>, pub kind: String,
    pub reads: Vec<String>, pub writes: Vec<String>,
    pub required_inputs: Vec<String>, pub optional_inputs: Vec<String>,
    pub source: Option<String>,
}
pub struct GraphEdge { pub from: String, pub to: Option<String>, pub condition: Option<String> }
pub struct GraphImport { pub definition: WorkflowDefinition, pub prefix: String }
pub struct GraphConnection { pub from: String, pub to: String }
pub struct GraphComposition {
    pub id: WorkflowId, pub revision: String, pub entry: String,
    pub imports: Vec<GraphImport>, pub connections: Vec<GraphConnection>,
}
```

Graph projections contain metadata and display expressions, not executable AST
or runtime owners. Composition prefixes imported node references while
retaining original logical behavior binding keys and authored route/tag order;
connections append explicit transitions and the ordinary compiler validates
collisions, terminal reachability and state/binding compatibility. No mutable
graph builder or alternate parser/evaluator is introduced.

GraphDescription.tracking_record produces the pinned rurr application graph
JSON object. Registration supplies resolved action declaration metadata;
inspection before binding may lack it, and export fails explicitly for missing
required source/metadata rather than inventing callback code. Built-in nodes
use their canonical authored definition as source. Existing JSON/DOT/Mermaid
inspection export remains separate from the tracking wire projection. An end
branch has no action destination and adds no tracking transition, matching
pinned graph exhaustion; it never creates a synthetic terminal action.

LifecycleEvent.tracking_records emits a bounded ordered list of pinned rurr
metadata, log and child JSON objects. Registration emits graph/metadata and,
for a child, its parent relationship. ChildFinished retains the terminal child
snapshot in the hook and emits no tracking record: the pin defines only fork
and spawn-start relationships. Step, span, attribute and stream variants emit
their matching log records; nontracking execute boundaries and nonfirst stream items return
an empty list. The supplied observation time is sampled once by the caller for
that event and rendered in the pinned RFC3339 form; repeated serialization with
the same time is deterministic. The host chooses destination and JSONL framing.
No file, network, exporter lifecycle, record registry, or second tracking truth
is introduced. OBSERVABILITY.md defines exact source schema mapping, content
policy and golden proof; private app control records use app/v7.

## Identities, errors and wire versions

WorkspaceId and newly generated root RunId use validated lowercase hyphenated
UUIDv4 strings with `appws_` and `run_` prefixes. RunId additionally accepts the
exact unprefixed 64-character lowercase hexadecimal child identity required by
the pinned public action-context/tracking fixtures. Child identity is the full
SHA-256 digest of the UTF-8 parent RunId, one `:` byte, then the resolved task key;
no workspace, workflow, action, revision, input, ordinal, checkpoint or occurrence
salt is added. Repeated identity still admits independent exact occurrences as
specified in CHECKPOINTS.md. WorkflowId is caller-authored ASCII
`[a-z0-9][a-z0-9_-]{0,127}`. They have private fields and implement Clone, Debug,
PartialEq, Eq, PartialOrd, Ord, Hash, Display, FromStr<Err=AppError>, and
validated Serialize/Deserialize. AppError implements Debug, Display and Error.
No lower identity may be parsed/formatted/compared to infer compatibility.

App errors and operational failure classifications are bounded and redacted;
no prompts, credentials, paths, private lower identities/records or raw lower
errors enter safe error strings. Explicit caller-codec/hook errors preserve
necessary user error context at their opt-in boundary, separate from safe
tracing. A Hook failure surfaces the hook error with the primary workflow
failure retained as structured cause; cleanup cannot erase the primary cause.
ExecuteFinished.failure reports that Hook value after a functional hook error:
its cause contains the prior execution failure, or is None when the action
succeeded. The completion observation preserves the authoritative state and
committed outcomes. A post-hook error after an action failure leaves that
action's durable failed observation intact; its structured Hook cause belongs
to the execution-completion observation and creates no duplicate failed action.

`WorkflowDefinition::parse` accepts only `app-sdk.workflow/v2`. Private durable
ready records accept only `app/v7`. `STATE_STORE.md` authorizes the one-way,
cold-cutover migration from `app/v6`, before normal generation claim or lower
effects. Other versions and initial corruption fail read-only. There is no
public migration API, runtime dual-format fallback, reset, downgrade, or
unrequested history deletion. `WORKFLOW.md` owns the
complete schema/behavior contract; `docs/schema/workflow-v2.schema.json` is its
hand-maintained JSON Schema projection, draft 2020-12 with local references.

## Explicit excluded surface

No public mutable AST, public native record/transaction, public backend handle,
Provider/Tool/Knowledge construction, generic repository/conformance product,
public dyn extension, scheduler tuning family, service server/UI/CLI, exporter
client or automatic telemetry. Core actions, local/synchronous execution,
streaming, typed/global serialized state, hooks, history, restore/fork,
inspection, child executors and pipelines are required and cannot be described
as covered merely by current snapshots or tracing.
