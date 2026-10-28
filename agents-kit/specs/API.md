# Agent Kit Public API Ledger

This file is the sole exact inventory of caller-facing agents-kit types and
runtime operations. Other specifications own behavior without adding
signatures.

## Namespace

The crate root exports:

- `AgentStorage`
- `AgentBuilder`
- `Agent`
- `AgentBackend`
- `Workspace`
- `WorkspaceBackend`
- `Conversation`
- `ConversationBackend`
- `KitError`

Supporting public values live in these modules:

### `error`

- `KitError` (also re-exported at the root)
- `ErrorKind`
- `Operation`
- `ErrorPhase`
- `ErrorContext`
- `ErrorDetail`
- `DeadlineKind`

### `config`

- `AgentConfig`
- `BackendConfig`
- `CodexConfig`
- `OpenCodeConfig`
- `CustomAgentConfig`
- `CustomLoopLimits`
- `CustomToolApproval`

### `identity`

- `AgentInfo`
- `AgentId`
- `WorkspaceId`
- `ConversationId`
- `NotificationId`

### `workspace`

- `WorkspaceInfo`
- `NewWorkspace`
- `WorkspaceContext`
- `WorkspaceRoot`
- `WorkspaceRootAccess`
- `WorkspaceExecutionPolicy`
- `WorkspaceSandboxMode`
- `WorkspaceNetworkAccess`
- `WorkspaceDefaults`
- `UpdateWorkspaceRequest`
- `WorkspaceUpdateReadiness`
- `DeleteWorkspaceRequest`

### `conversation`

- `ConversationInfo`
- `NewConversation`
- `FileRead`

### `prompt`

- `PromptRequest`
- `StructuredOutputRequest`
- `PromptResponse`
- `PromptEvent`
- `PromptStream`
- `UsageInfo`

### `notification`

- `ConversationNotificationReceiver`
- `ConversationNotification`
- `ConversationNotificationKind`
- `ProcessState`
- `NotificationResolution`

### `model`

- `ModelInfo`
- `ModelSelector`
- `PersonalitySelector`
- `ReasoningEffort`

### `tool`

- `ToolInfo`
- `ToolSelection`

### `knowledge`

- `KnowledgeSource`
- `KnowledgePolicy`
- `KnowledgeScope`
- `IngestKnowledgeRequest`

This inventory is exhaustive. In particular, the public surface contains no
factory or conformance family, `KnowledgeBase`, `KnowledgeBaseBackend`,
knowledge owner/retrieval/context/snippet/provenance/verification result
family, background execution family, protocol DTO, lower-crate config mirror,
or compatibility alias.

Passive DTO methods are limited to construction, validation, serialization or
conversion, and simple accessors. Those methods do not add runtime operations.

## Handle And Trait Topology

Import `AgentBackend`, `WorkspaceBackend`, and `ConversationBackend` when calling
behavior on supplied handles. These traits are the sole behavioral contracts;
the handles have no same-name inherent forwarding methods.

```rust
pub trait AgentBackend: Send + Sync + 'static {
    type Workspace: WorkspaceBackend;

    fn id(&self) -> identity::AgentId;

    fn info(
        &self,
    ) -> impl Future<Output = Result<identity::AgentInfo, KitError>> + Send + '_;

    fn new_workspace(
        &self,
        request: workspace::NewWorkspace,
    ) -> impl Future<Output = Result<Self::Workspace, KitError>> + Send + '_;

    fn list_workspaces(
        &self,
    ) -> impl Future<Output = Result<Vec<workspace::WorkspaceInfo>, KitError>>
        + Send
        + '_;

    fn workspace_info(
        &self,
        id: identity::WorkspaceId,
    ) -> impl Future<Output = Result<workspace::WorkspaceInfo, KitError>> + Send + '_;

    fn get_workspace(
        &self,
        id: identity::WorkspaceId,
    ) -> impl Future<Output = Result<Self::Workspace, KitError>> + Send + '_;

    fn check_workspace_update(
        &self,
        request: &workspace::UpdateWorkspaceRequest,
    ) -> impl Future<
        Output = Result<workspace::WorkspaceUpdateReadiness, KitError>,
    > + Send
        + '_;

    fn update_workspace(
        &self,
        request: workspace::UpdateWorkspaceRequest,
    ) -> impl Future<Output = Result<workspace::WorkspaceInfo, KitError>> + Send + '_;

    fn delete_workspace(
        &self,
        request: workspace::DeleteWorkspaceRequest,
    ) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    fn list_models(
        &self,
    ) -> impl Future<Output = Result<Vec<model::ModelInfo>, KitError>> + Send + '_;

    fn ingest_knowledge(
        &self,
        request: knowledge::IngestKnowledgeRequest,
    ) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    fn shutdown(self) -> impl Future<Output = Result<(), KitError>> + Send
    where
        Self: Sized;
}

pub trait WorkspaceBackend: Send + Sync + 'static {
    type Conversation: ConversationBackend;

    fn id(&self) -> identity::WorkspaceId;

    fn info(
        &self,
    ) -> impl Future<Output = Result<workspace::WorkspaceInfo, KitError>> + Send + '_;

    fn new_conversation(
        &self,
        request: conversation::NewConversation,
    ) -> impl Future<Output = Result<Self::Conversation, KitError>> + Send + '_;

    fn list_conversations(
        &self,
    ) -> impl Future<Output = Result<Vec<conversation::ConversationInfo>, KitError>>
        + Send
        + '_;

    fn conversation_info(
        &self,
        id: identity::ConversationId,
    ) -> impl Future<Output = Result<conversation::ConversationInfo, KitError>>
        + Send
        + '_;

    fn get_conversation(
        &self,
        id: identity::ConversationId,
    ) -> impl Future<Output = Result<Self::Conversation, KitError>> + Send + '_;

    fn list_tools(
        &self,
    ) -> impl Future<Output = Result<Vec<tool::ToolInfo>, KitError>> + Send + '_;

    fn ingest_knowledge(
        &self,
        request: knowledge::IngestKnowledgeRequest,
    ) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    fn close(self) -> impl Future<Output = Result<(), KitError>> + Send
    where
        Self: Sized;
}

pub trait ConversationBackend: Send + Sync + 'static {
    fn id(&self) -> identity::ConversationId;

    fn info(
        &self,
    ) -> impl Future<Output = Result<conversation::ConversationInfo, KitError>>
        + Send
        + '_;

    fn prompt(
        &self,
        request: prompt::PromptRequest,
    ) -> impl Future<Output = Result<prompt::PromptResponse, KitError>> + Send + '_;

    fn prompt_stream(
        &self,
        request: prompt::PromptRequest,
    ) -> impl Future<Output = Result<prompt::PromptStream, KitError>> + Send + '_;

    fn abort(&self) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    fn read_file(
        &self,
        path: std::path::PathBuf,
    ) -> impl Future<Output = Result<conversation::FileRead, KitError>> + Send + '_;

    fn notification_stream(
        &self,
    ) -> Result<notification::ConversationNotificationReceiver, KitError>;

    fn resolve_notification(
        &self,
        id: identity::NotificationId,
        resolution: notification::NotificationResolution,
    ) -> impl Future<Output = Result<(), KitError>> + Send + '_;

    fn close(self) -> impl Future<Output = Result<(), KitError>> + Send
    where
        Self: Sized;
}
```

`AgentBackend::Workspace` and `WorkspaceBackend::Conversation` are concrete
associated child implementations. For supplied handles,
`Agent<S, D, P, T>::Workspace = Workspace<S, D, P, T>` and
`Workspace<S, D, P, T>::Conversation = Conversation<S, D, P, T>`. Caller
implementations remain concrete or caller-owned exhaustive enum hierarchies;
agents-kit never normalizes them into closed Codex/OpenCode/Custom handles.

`AgentStorage<S>`, `AgentBuilder<S, D, P, T>`, `Agent<S, D, P, T>`,
`Workspace<S, D, P, T>`, and `Conversation<S, D, P, T>` use
`S: storage_kit::StateStoreBackend`; every type except `AgentStorage` also uses
`D: storage_kit::DatabaseBackend`, `P: providers_kit::ProviderBackend`, and
`T: tools_kit::ToolsetBackend`. `S`
defaults to `storage_kit::StateStore`, `D` defaults to
`storage_kit::Database`, `P` defaults to `providers_kit::Provider`, and `T`
defaults to `tools_kit::Toolset`. These defaults never require normalization.
`Agent`, `Workspace`, and `Conversation` are `Send + Sync` and intentionally
not `Clone`; associated children preserve the same `S, D, P, T` types.
Neither lower implementations nor `T::Handler` require Clone; Handler is not
an independent Agent parameter.
`AgentStorage<S>` is `Clone` without requiring `S: Clone`.
`ConversationNotificationReceiver` is `Send`, not `Clone`, and advances
through `&mut self`.

## Construction

`AgentBuilder` constructs supplied Codex, OpenCode, or Custom Agents only; it
does not accept caller `AgentBackend` implementations.

```rust
impl<S: storage_kit::StateStoreBackend> AgentStorage<S> {
    pub fn new(store: S) -> Self;

    pub fn list_agents(
        &self,
    ) -> impl Future<Output = Result<Vec<identity::AgentInfo>, KitError>> + Send + '_;

    pub fn agent_info(
        &self,
        id: identity::AgentId,
    ) -> impl Future<Output = Result<identity::AgentInfo, KitError>> + Send + '_;
}

impl<S: storage_kit::StateStoreBackend>
    AgentBuilder<S, storage_kit::Database, providers_kit::Provider, tools_kit::Toolset>
{
    pub fn create(config: config::AgentConfig, storage: AgentStorage<S>) -> Self;

    pub fn create_for_backends<P2: providers_kit::ProviderBackend, T2: tools_kit::ToolsetBackend>(
        config: config::AgentConfig,
        storage: AgentStorage<S>,
    ) -> AgentBuilder<S, storage_kit::Database, P2, T2>;

    pub fn reopen(
        config: config::AgentConfig,
        storage: AgentStorage<S>,
        id: identity::AgentId,
    ) -> Self;

    pub fn reopen_for_backends<P2: providers_kit::ProviderBackend, T2: tools_kit::ToolsetBackend>(
        config: config::AgentConfig,
        storage: AgentStorage<S>,
        id: identity::AgentId,
    ) -> AgentBuilder<S, storage_kit::Database, P2, T2>;
}

impl<S, D, P, T> AgentBuilder<S, D, P, T>
where
    S: storage_kit::StateStoreBackend,
    D: storage_kit::DatabaseBackend,
    P: providers_kit::ProviderBackend,
    T: tools_kit::ToolsetBackend,
{
    pub fn with_provider<P2: providers_kit::ProviderBackend>(
        self,
        provider: P2,
    ) -> AgentBuilder<S, D, P2, T>;

    pub fn with_toolset(self, toolset: T) -> Self;

    pub fn with_knowledge_database<D2: storage_kit::DatabaseBackend>(
        self,
        database: D2,
    ) -> AgentBuilder<S, D2, P, T>;

    pub fn build(self) -> impl Future<Output = Result<Agent<S, D, P, T>, KitError>> + Send;
}
```

`create_for_backends::<P2, T2>` and `reopen_for_backends::<P2, T2>` fix the tool
implementation type and select the static
provider type for a native Codex or OpenCode Agent without constructing,
retaining, or invoking a `P2` value. This permits those native Agents to share
one caller-owned provider type with Custom Agents in a homogeneous downstream
inventory. Custom still receives its concrete provider value exclusively
through `with_provider`; selecting a type does not satisfy that requirement.
The unqualified constructors continue to select the supplied
`providers_kit::Provider` and `tools_kit::Toolset` defaults. Repeated
`with_toolset` appends to the fixed homogeneous inventory; changing Provider
or Database preserves every accepted tool. Callers use exhaustive enums for
heterogeneous Toolsets and associated Handlers. No tool implementation is
normalized or inspected through a supplied concrete handle.

There is no `from_config`, `with_agent_storage`, `with_agent_id`,
`with_knowledge_base`, `with_knowledge_sources`, provider-hosted declaration
ingress, or Agent or Provider normalization constructor. Agent, Provider,
Toolset, and storage implementations remain direct
through their public traits; no supplied-handle normalization, erased adapter,
implementation identity, or compatibility route exists. `P::Model`,
`P::Model::Continuation`, and the Model-associated Session remain provider
associated types and are not independent Agent parameters. The continuation
is prompt-run-local Custom state, not another Agent generic or durable value.
Initial knowledge is ingested through the built Agent or Workspace after its
final durable identity exists.

## Custom Configuration

`CustomAgentConfig` has private fields and requires:

- one default `ModelSelector`;
- one `ToolSelection`;
- one `CustomLoopLimits`;
- one `CustomToolApproval`.

`CustomLoopLimits` validates nonzero model turns and parallelism plus a bounded
total tool-action count. `CustomToolApproval` is a small agent-owned enum for
allow, deny, or durable interaction. Shell execution is a caller-built ordinary
tool with explicit cwd, argument validation, cancellation, output limits, and
process cleanup owned by that tool. Agent does not construct a shell or carry
a shell-only policy, registration, or task-local working-directory bridge.

The config contains no provider credentials, Provider/Model/Session handle,
Toolset, Database, callback, driver, hook, mount, or runtime state.

## Knowledge Inputs

`KnowledgeScope` is exactly `Database | Agent | Workspace | Conversation`.
`KnowledgePolicy` is exactly `Default | Disabled | Only(Vec<KnowledgeScope>)`.
`KnowledgeSource` contains validated text or bytes plus caller source identity
and title. `IngestKnowledgeRequest` owns a vector of sources and optional
scope.

Knowledge retrieval, owner lineage, budgets, snippets, provenance,
verification, and formatted prompt context are private implementation models.
An empty ingestion vector is a successful no-op after scope validation.

## Notification Receiver

```rust
impl notification::ConversationNotificationReceiver {
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: futures_util::Stream<
                Item = Result<notification::ConversationNotification, KitError>,
            > + Send
            + 'static;

    pub fn recv(
        &mut self,
    ) -> impl Future<
        Output = Option<Result<notification::ConversationNotification, KitError>>,
    > + Send + '_;
}
```

`from_stream` is the sole downstream construction boundary for the concrete
receiver required by `ConversationBackend`. It adapts notification values, not
a backend implementation, and does not create an independent behavioral seam.
agents-kit owns all normalization for built-in receivers.

## Prompt Output

```rust
pub enum prompt::PromptEvent {
    OutputDelta { text: String },
    Completed(prompt::PromptResponse),
}

impl prompt::PromptStream {
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: futures_util::Stream<Item = Result<prompt::PromptEvent, KitError>>
            + Send
            + 'static;

    pub fn recv(
        &mut self,
    ) -> impl Future<Output = Option<Result<prompt::PromptEvent, KitError>>> + Send + '_;
}
```

PromptStream is an opaque non-Clone, Send observation value. Its constructor
accepts non-Unpin streams and adapts values only; it is not a Conversation
implementation ingress. Exactly one Completed or error ends observation;
premature source EOF yields Protocol once, and all later recv calls yield None.
The complete response remains authoritative, including structured output and
whole-prompt usage.

ConversationBackend supplies an honest Unsupported default for prompt_stream.
This preserves terminal-only caller implementations; supplied Codex, OpenCode,
and Custom backends override the operation using actual incremental output.
Ordinary prompt remains supported for caller Models that implement only
generate. Neither operation introduces another prompt slot or execution owner.
[`RUNTIME.md`](RUNTIME.md) owns bounds, cancellation and durable publication.

## Constructibility

Invariant-bearing backend results remain publicly constructible:

- `AgentInfo::new`
- `WorkspaceInfo::new`
- `ConversationInfo::new`
- `ConversationNotification::new`
- `FileRead::from_bytes`

Typed ids have validated constructors/serde and private representation.
Relative file paths reject roots, drive prefixes, NUL, empty/dot/parent
components, and repeated/trailing separators without normalization.

## Errors

`KitError` is opaque, cloneable, and deliberately nonserializable. `ErrorKind`
retains these closed categories:

- `InvalidConfig`
- `InvalidInput`
- `NotFound`
- `Conflict`
- `Closed`
- `Busy`
- `Cancelled`
- `Timeout`
- `Unsupported`
- `NotYetImplemented`
- `Protocol`
- `InvalidState`
- `Integration`

Every category has a public safe-message constructor. `new(kind, safe_message)`
supports direct caller implementations. Inspection uses `kind()`, `message()`,
`context()`, `detail()`, `http_status()`, `cause()`, `supplementary()`, and
`diagnostics_truncated()`. Consuming enrichment uses `with_context(operation,
phase)`, `with_detail(detail)`, `with_http_status(status)`, `with_cause(error)`,
and `with_supplementary(error)`.

`ErrorContext` is a passive DTO with public `operation: Operation` and
`phase: ErrorPhase`. All supporting enums are closed, cloneable, and comparable.
`Operation` contains Unspecified, Configure, ValidateInput, Build, Reopen,
ListAgents, AgentInfo, NewWorkspace, ListWorkspaces, WorkspaceInfo, GetWorkspace,
CheckWorkspaceUpdate, UpdateWorkspace, DeleteWorkspace, ListModels, ListTools,
IngestKnowledge, CloseWorkspace, NewConversation, ListConversations,
ConversationInfo, GetConversation, Prompt, Abort, ReadFile, NotificationStream,
ResolveNotification, CloseConversation, Shutdown. Prompt includes accepted
unary and streamed work.

`ErrorPhase` contains Unspecified, Configuration, InputValidation, Admission,
ClientSetup, ProcessStart, ServerVerification, InventoryVerification,
EventReady, EventSynchronization, ModelSelection, ModelGeneration, ToolExecution,
CompletionValidation, KnowledgeRead, KnowledgeWrite, RequestBuild, RequestSend,
ResponseRead, ResponseDecode, StateRead, StateWrite, StateCommit, Reconciliation,
InteractionResolution, Cancellation, Publication, Settlement, Cleanup, Lifecycle.

`ErrorDetail` contains AuthenticationRejected, PermissionDenied, InvalidRequest,
ResourceMissing, RemoteConflict, RequestTooLarge, PreconditionFailed,
ResourceExhausted, RateLimited, QuotaExceeded, BillingRequired,
ServiceUnavailable, RemoteInternal, RemoteCancelled, RemoteDeadlineExceeded,
RemoteRejected, TransportFailure, DependencyClosed, DependencyCancelled,
TaskTerminated, OutputLimit, ContextLimit, SessionLimit, ContentFiltered,
PolicyRejected, ContinuationRequired, MalformedData, MissingData, InvalidData,
UnknownDiscriminator, CorrelationMismatch, InvalidSequence, IncompleteStream,
StructuredOutputMismatch, InvalidToolOutput, StaleRevision, AttachmentMismatch,
TransactionConflict, AcceptanceUnknown, OwnershipConflict,
`ResponseLimit { limit: usize }`, and
`DeadlineElapsed { kind: DeadlineKind, timeout: Option<Duration> }`.
`DeadlineKind` contains Lifecycle, Request, Operation, Observation.

Causes and supplementary failures are bounded Agent-owned `KitError` values;
`std::error::Error::source()` exposes only the normalized cause. No error
carries a backend label, native id, route, storage key, secret, raw payload,
lower-crate error value, or retry token. Arbitrary native text is never safe
merely because it was truncated. `ERRORS.md` owns bounds and precedence.

## Operation Rules

- Inventory vectors are deterministic and contain only live or reopenable
  records.
- Workspace preflight is read-only and advisory. Update/delete recheck revision
  atomically.
- Prompt is async foreground work; accepted work remains runtime-owned after
  caller-future drop.
- Abort joins only the active generation.
- Notification delivery is exclusive and at-least-once.
- Agent shutdown and Workspace close preserve quiescent descendants for
  reopen; Conversation close is terminal.
- Every backend/provider/tool/storage limitation maps to the smallest honest
  portable operation and never expands the public type inventory.
