# Agent Runtime

This document owns runtime hierarchy, information, lifecycle, prompts, models,
files, and tools; exact public surface comes only from [`API.md`](API.md).

## Portable Information

- `AgentInfo` contains `id: AgentId` and `display_name: Option<String>`.
- `WorkspaceInfo` contains `id: WorkspaceId`, a validated nonempty
  `name: String`, complete `context: WorkspaceContext`, complete
  `defaults: WorkspaceDefaults`, and `revision: u64`.
- `ConversationInfo` contains `id: ConversationId`,
  `workspace_id: WorkspaceId`, and `title: Option<String>`.

Information DTOs are committed portable snapshots. Terminal or deleted records
are operation failures or absent inventory; later reads reflect current state.

## Runtime Hierarchy And Concurrency

An `Agent` owns shared runtime and workspaces. A `Workspace` owns portable
context and conversations. A `Conversation` owns one foreground prompt slot,
notification delivery, interaction resolution, and terminal close. All three
are opaque over private backend dispatch.

One parent runtime publishes at most one live public handle for each current
workspace or conversation record. A repeated current-revision lookup returns
`Busy`; callers share that handle by reference when needed. Dropping a
Workspace or Conversation releases only its process-local handle claim. A
prior-revision claim is already stale and does not block current lookup;
consuming close invalidates the claimed handle and descendants.

The nonzero agent config bound limits prompts active across conversations.
Each conversation permits one. Admission reserves both limits before backend
acceptance and releases them after durable terminalization. Saturation returns
`Busy` without beginning backend work.

Every async operation on a live `Agent`, `Workspace`, or `Conversation` handle
admits against that open runtime and, when workspace-bound, the current
workspace revision. `AgentStorage` discovery and consuming
`AgentBuilder::build` establish or inspect runtime ownership and therefore do
not require a preexisting open runtime.
Policy-sensitive reads and knowledge mutations remain admitted until publication or commit, so update and delete see them as busy.
Cancelling an uncommitted policy-sensitive read publishes nothing, releases its transient admission, and cannot leave the workspace permanently `Busy`.
Close and shutdown seal new admission and settle or cancel accepted work within the lifecycle timeout.
Except for the committed terminal notification drain owned by
[`NOTIFICATIONS.md`](NOTIFICATIONS.md), no live-handle operation publishes
after its runtime closes or its workspace revision becomes stale.

## Cascade lifecycle

- `Agent::shutdown` seals new work and cascade-invalidates descendant
  workspace, conversation, and receiver runtime.
- `Workspace::close` seals the workspace and cascade-invalidates conversation
  and receiver runtime.
- `Conversation::close` durably terminalizes the conversation, removes it from
  reopenable inventory, and starts terminal record/resource reclamation.

Agent shutdown preserves its durable agent, workspaces, and quiescent
conversations. Workspace close preserves its durable workspace and quiescent
conversations. A new parent runtime may reopen them; parent cascade does not
mark quiescent conversations closed.

Cascade reconciles accepted work, persists required terminal/recovery facts,
then ends runtime delivery. Descendants never keep a closed parent transport
alive. Consuming cleanup attempts independent responsibilities within the
lifecycle timeout and records recovery evidence on failure. Dropping a handle
provides no async or durable cleanup guarantee.

Sealing completes every outstanding prompt waiter, blocked receiver, and
notification-resolution caller with the result of the first durable terminal
transition or its owning lifecycle error. Close and shutdown leave no such
caller parked after their lifecycle timeout completes.

The close order is seal, preserve routing, cancel active work, settle
interactions, persist terminal or recovery state, release tool/provider/backend
resources, commit the final notification backlog, then seal delivery.
[`NOTIFICATIONS.md`](NOTIFICATIONS.md) owns drain, replay, and terminal owner
deletion. Parent cascade runtime-detaches quiescent descendants; it does not
invoke their public close operation or terminalize them. Failure after a
durable commit remains a cleanup obligation and cannot reverse that commit.

## Workspace And Conversation Creation

`NewWorkspace` contains a validated nonempty `String` name, complete
`WorkspaceContext`, and `WorkspaceDefaults`. A new workspace starts at revision
one.

`WorkspaceContext` contains one normalized absolute backend-host cwd, a
nonempty validated `Vec<WorkspaceRoot>`, and one
`WorkspaceExecutionPolicy`. The cwd and every root use backend-host path
semantics and are never resolved against the SDK process cwd or silently
reinterpreted as SDK-host paths across an external connection.

`WorkspaceRoot` contains one normalized absolute `PathBuf` and one
`WorkspaceRootAccess`, exactly `ReadOnly` or `ReadWrite`. Roots are
deduplicated, their overlaps cannot produce contradictory access, and the cwd
must be contained by at least one root. These are caller-declared workspace
roots, not filesystem-existence claims during passive validation.

`WorkspaceExecutionPolicy` contains one `WorkspaceSandboxMode` and one
`WorkspaceNetworkAccess`. Sandbox mode is exactly `ReadOnly`,
`WorkspaceWrite`, or `FullAccess`; network access is exactly `Disabled` or
`Enabled`. The policy governs every model-visible action owned or installed by
the backend, including native shell/file behavior, executable Toolsets,
backend-native tools, provider-hosted actions, and MCP execution. ReadOnly
forbids writes through all such paths. WorkspaceWrite permits writes only
inside roots declared ReadWrite. FullAccess permits the selected backend's
unrestricted filesystem behavior, while portable `read_file` still uses its
workspace-relative path contract. Disabled network forbids model-visible
arbitrary network access through all such paths; it does not disable the
configured transport needed to reach the selected backend or Provider.
Enabled permits model-visible network access. There is no backend-default
policy that silently changes meaning.

Passive policy coherence is exact: ReadOnly requires every root to be
ReadOnly; WorkspaceWrite requires the root containing cwd to be ReadWrite and
permits additional ReadOnly or ReadWrite roots; FullAccess requires every root
to be ReadWrite because a declared root-level write restriction would
contradict unrestricted filesystem behavior. A contradictory combination is
`InvalidInput`, not a backend capability decision.

The concrete backend validates the complete installed callable composition and
enforces the complete policy before commit or native work. A backend cannot
claim confinement merely because its native shell is sandboxed while another
model-visible tool path remains unrestricted. A selected backend or installed
composition that cannot prove the complete policy returns `Unsupported`; it
never weakens, drops, or approximates a field.

`WorkspaceDefaults` contains `model: Option<ModelSelector>`,
`personality: Option<PersonalitySelector>`,
`reasoning_effort: Option<ReasoningEffort>`, and
`knowledge_policy: Option<KnowledgePolicy>`. Absence follows the field-specific
precedence below.

`NewConversation` contains `title: Option<String>`,
`defaults: Option<WorkspaceDefaults>`, and
`knowledge_sources: Vec<KnowledgeSource>`. Missing overrides inherit workspace policy.
Initial sources are conversation-scoped. Creation prepares portable state
before commit; accepted native setup is attached durably or represented by
recovery evidence, never hidden behind empty success.

List operations need no request wrappers and return only live or reopenable
durable inventory in the deterministic order fixed by [`API.md`](API.md).
Direct lookup validates ownership without revealing a foreign owner.
Conversation reopen follows [`STATE.md`](STATE.md).

## Revisioned Workspace Update

`UpdateWorkspaceRequest` contains `id: WorkspaceId`,
`expected_revision: u64`, validated nonempty `name: String`, complete
`context: WorkspaceContext`, and complete `defaults: WorkspaceDefaults`. It is
a full replacement, not a patch. `WorkspaceUpdateReadiness` is exactly `Ready`
or `Busy`.

Preflight is read-only and advisory. It first validates request shape and backend representability; an unrepresentable replacement is `Unsupported`.
While reading durable state and evaluating readiness, it holds a transient workspace-scoped read admission.
The durable expected revision is compared before dynamic readiness, so a stale request returns `Conflict` even when current-revision activity would otherwise be `Busy`.
Its own admission does not make its result `Busy`; mutation observes it as admitted activity, and an admitted policy-sensitive read makes the result `WorkspaceUpdateReadiness::Busy`.
Publication or cancellation releases the transient admission; sealing prevents publication, and completed `Ready` reserves no later mutation right.
Other busy activity covers descendant prompt, cancellation, pending or resolving interaction, admitted file read, workspace/conversation knowledge ingestion,
setup, close, recovery, and backend-owned background work.

Mutation repeats ownership, revision, and busy checks atomically; replaces
only name/context/defaults; preserves backend attachment; increments revision
once; and returns committed `WorkspaceInfo`. Durable commit is success.
The expected-revision check ensures that exactly one of two competing updates
can commit.
Revision `u64::MAX` cannot increment: mutation returns `InvalidState` without
changing portable or native state and never wraps to zero.

After commit, every workspace and conversation handle bound to the prior
revision is deterministically invalidated. There is no in-place refresh
promise. Those stale handles detect revision mismatch before using old policy and
must be reopened by typed id. Postcommit invalidation/cleanup is idempotent
recovery and cannot change success to `Err`.

## Safe Workspace Delete

`DeleteWorkspaceRequest` contains `id: WorkspaceId` and
`expected_revision: u64`.
Delete rejects stale revision and any busy descendant; it has no force or
cascade-policy selector.

Safe delete atomically removes workspace inventory and quiescent conversation
records, invalidates opened descendants, and records backend-native cleanup
obligations. Active or ambiguous work is never relabeled deleted. Durable
commit is success; postcommit native cleanup is retryable private recovery and
cannot change success to an error.

## Foreground Prompt

`PromptRequest` contains `text: String` plus `cwd: Option<PathBuf>`,
`model: Option<ModelSelector>`, `reasoning_effort: Option<ReasoningEffort>`,
`personality: Option<PersonalitySelector>`, `instructions: Option<String>`,
`timeout: Option<std::time::Duration>`, and
`knowledge_policy: Option<KnowledgePolicy>`,
`structured_output: Option<StructuredOutputRequest>`, and
`tool_selection: Option<ToolSelection>`. Text is nonempty.

`StructuredOutputRequest` contains exactly `schema: serde_json::Value`. The
schema is a nonempty JSON object that compiles as a self-contained JSON Schema
Draft 2020-12 document. Remote or external references are invalid; local
references remain within the same document. The SDK validates and compiles it
before native/provider acceptance. Portable structured output always requests
strict schema adherence; there is no caller name, strictness toggle, hidden
retry count, fallback-to-text mode, or capability-query object.

Custom's provider adapter additionally requires the schema root to declare an
object before provider I/O because provider-sdk's `JsonSchema` contract is
object-shaped. Caller input that violates this selected boundary is
`InvalidInput`; contradictory persisted state is `InvalidState`. This does not
narrow the Codex or OpenCode structured-output contract.

`ToolSelection` is exactly:

- `Auto`: make the complete effective portable catalog eligible and let the
  backend/provider decide whether to call a tool;
- `None`: make no effective portable tool eligible;
- `Required`: make the complete effective portable catalog eligible and
  require at least one call before final output;
- `Allowed(Vec<String>)`: make only the named portable tools eligible and let
  the backend/provider decide whether to call one; and
- `RequiredOneOf(Vec<String>)`: make only the named portable tools eligible
  and require at least one of them before final output.

Names are canonical `ToolInfo` names. Both named vectors are nonempty,
duplicate-free, and resolve against the complete current Workspace catalog
before native/provider acceptance. `Required` also requires that catalog to be
nonempty; an empty required set is `InvalidInput`. A backend with effective catalog entries it
cannot control for the prompt returns `Unsupported` for a non-`Auto`
selection rather than claiming a partial policy. Absent selection uses the
backend's configured/default policy; explicit `Auto` means the complete
portable catalog is eligible. A backend never implements this request by
mutating persistent native permissions, snapshotting/restoring a session, or
rewriting the user text.

The optional cwd is a normalized path relative to the workspace context cwd.
Absolute paths, traversal, and any value outside the active context are
`InvalidInput`; it is never interpreted relative to the SDK process.

Precedence is field-specific. Model, personality, and reasoning effort use
prompt, conversation defaults, workspace defaults, then backend/provider
default. Knowledge policy uses prompt, conversation, workspace, then
`KnowledgePolicy::Default`. Instructions use prompt, `AgentConfig`
instructions, then backend policy. Timeout uses prompt, `AgentConfig`, then no
caller prompt deadline. Cwd uses the prompt override or workspace context cwd
captured at admission. Knowledge rendering is backend-owned and does not
mutate the request. Structured output has no inherited default. Tool selection
uses the prompt override, then the concrete backend's configured/default
selection.

`Conversation::prompt` is an async foreground operation whose awaited future
reaches a terminal portable result. Backend acceptance transfers ownership to
conversation runtime. Dropping the caller future neither cancels work nor
releases admission; runtime continues to durable terminal state.

A supplied prompt timeout is nonzero and starts after admission. If it elapses
before backend acceptance, no backend work is accepted. If it elapses after
acceptance, the Conversation durably records timeout intent and uses the same
generation-fenced cancellation owner as `abort`; it never replays the prompt
or fabricates a terminal success. The foreground caller receives `Timeout`
when the deadline contract is reached, while the generation remains `Busy`
until terminal/reconciliation evidence proves that delayed cancellation cannot
affect later work. A timeout is distinct from caller-future drop, backend
transport timeout, and lifecycle cleanup timeout.

Success returns `PromptResponse { output: String, structured_output:
Option<serde_json::Value>, usage: Option<UsageInfo> }`. Without a structured
request, `structured_output` is `None`. With one, the SDK requires the selected
native/provider structured result, parses it, validates it locally against the
same compiled schema, and returns it as `Some`; malformed JSON, absent required
structured state, or schema nonconformance is `Protocol` with no partial
success. `output` remains the exact backend/provider-authored terminal text and
may be empty when the selected native structured result has no text form. The
SDK never reserializes the parsed value to manufacture output or silently
downgrades a structured request to plain text.

`UsageInfo` contains required `input_tokens: u64` and `output_tokens: u64` and
no backend metadata. When present, it covers every native model call owned by
that foreground prompt; a session-cumulative or final-step-only counter yields
`None` rather than a misleading partial value. Cancellation,
timeout, malformed terminal state, and integration failure use `SdkError`, not
a public prompt-status family.

Custom requires an explicit normalized finish reason from every complete
response supplied by a caller's Model, in both foreground `generate` results
and streamed `Completed` events. The reason and function-call shape must pass
the matrix in [`custom/CONVERSATION.md`](custom/CONVERSATION.md) before tools
run or a final answer commits. Partial text cannot authorize success when the
completion reason is absent, failed, incomplete, paused, or contradictory.

## Incremental Prompt Observation

Streaming admits the same foreground prompt generation and uses the same
backend execution, interaction, cancellation and terminal commit owners as
ordinary prompt. The stream observes real source text deltas in source order;
it never splits a completed response to simulate progress. Custom selects
ModelBackend::generate_stream for streaming and generate for ordinary prompts
inside its one loop. A selected Model that cannot stream returns Unsupported;
the ordinary generate-only path remains available.

OutputDelta is transient assistant text. It excludes reasoning, tool input and
output, commands, native identifiers and protocol frames. Intermediate
assistant text may precede tool calls or differ from the terminal response.
Completed carries the exact authoritative PromptResponse only after terminal
durability. Deltas do not commit transcript or workflow state, cannot be
replayed after process loss, and do not become durable notifications.

Each supplied observer queues at most 64 text fragments of at most 16 KiB
each, bounding queued text at 1 MiB. A larger source delta may be split only
at UTF-8 boundaries, preserving its exact text and order. Publication never
blocks the protocol reader or prompt owner on consumer capacity. Overflow
ends observation with an explicit Integration error; the accepted prompt
continues to process interactions, cancellation and durable terminalization. An observer
cannot miss fragments and then report Completed as if delivery were complete.
Caller-constructed streams own equivalent bounded production and lifecycle
responsibilities; the value adapter owns terminal/error fusion and premature
EOF detection, not the caller's execution task.

Dropping the stream, cancelling recv, or losing observation does not abort an
accepted prompt or release admission. Abort, close and parent shutdown still
target that same generation and unblock its observer with the first terminal
result or owning lifecycle error. A later generation cannot use the prior
observer, and actionable notifications remain independently resolvable while
output is streaming. A known unsupported selected capability fails honestly
without substituting a terminal-only stream.

## Abort

`abort()` targets the conversation's one prompt. With no active prompt it is a successful no-op;
otherwise it captures only the currently active private prompt generation. Concurrent abort calls
join the same target, and cancellation is dispatched at most once. Abort returns success after that
generation reaches any valid durable terminal and its cancellation dispatch is settled or proven
unnecessary. The prompt waiter alone receives the terminal outcome; natural success or failure may
win. A later prompt is never part of the abort target. Sealing before target capture returns
`Closed`. Timeout or ambiguous cancellation retains the fence and leaves admission `Busy` until
reconciliation proves delayed cancellation cannot affect later work.

Parent close and shutdown use the same cancellation ownership with their distinct durable meanings.
Cancellation terminalizes pending interactions owned by the same prompt. If durable state says the
prompt is running but the backend proves no matching work exists, the operation returns
`InvalidState` and reconciliation repairs the record rather than silently freeing the slot.

## Models And Reasoning

`ModelSelector` is an opaque validated nonempty model selection. A simple
caller string resolves only when unambiguous. Backends that need multiple
native coordinates keep them private; callers round-trip the selector returned
by model inventory.

`ReasoningEffort` is an opaque open newtype over a validated nonempty `String` with `low`,
`medium`, and `high` constructors. Advertised future values round-trip exactly.

`PersonalitySelector` is an opaque validated nonempty selection. It
round-trips values advertised by model inventory without exposing backend or
provider coordinates. A simple caller string resolves only when unambiguous.

`ModelInfo` contains `selector: ModelSelector`,
`display_name: Option<String>`, `reasoning_efforts: Vec<ReasoningEffort>`,
`personalities: Vec<PersonalitySelector>`, and `is_default: bool`.
`is_default` is true only for a sourced agent-wide default that applies across
workspaces. At most one entry is true; zero means the backend default remains
opaque. `list_models` returns selectable models in stable canonical-selector
order, and canonical selectors are unique. Duplicate selectors are an
integrity/protocol failure rather than duplicate rows. Every paged backend
detects a repeated cursor, enforces fixed page and item bounds, and fails
honestly instead of looping or truncating. Malformed inventory is `Protocol`,
and unsupported listing is `Unsupported`, not an empty list.

An empty reasoning or personality vector means the selected inventory cannot
advertise supported values for that model; it is not permission to ignore a
caller-supplied value. A backend may accept a caller-constructed value only
when its own contract defines an exact preacceptance mapping and validation
path. Otherwise a non-absent value is `Unsupported` before native/provider
acceptance.

## File Reads

`FileRead` is an opaque result with private text/binary content. Backends
construct it only through the fallible `FileRead::from_bytes(path, bytes)`
boundary; callers observe it through `path`, `text`, `bytes`, and `mime_type`
accessors, so public enum variants cannot forge an invalid path. Input is a
validated path relative to the admitted current `WorkspaceContext.cwd`.
Before network or provider work, its lexical backend-host join must remain in
a declared readable root. Workspace policy is enforced before read, and the
returned path preserves the valid relative input spelling. Absolute paths,
lexical traversal, platform-specific absolute prefixes, empty/dot/parent
components, repeated or trailing separators, and NUL are rejected without
normalization.

That lexical check is the portable minimum, not a claim that every selected
native route supplies a filesystem sandbox. Restricted access additionally
requires the concrete backend to confine native resolution atomically against
symlink or equivalent escape. A backend that cannot do so returns
`Unsupported` before the request. FullAccess intentionally permits the native
filesystem to resolve the lexically admitted backend-host path; its backend
contract must say when the selected route has no stronger sandbox.
The result is byte-exact: valid UTF-8 is exposed as text without trimming or
newline normalization; other bytes remain binary, and all non-UTF-8 bytes use
`application/octet-stream`. A proven structured missing-path result is
`NotFound`. A selected backend whose definite rejection cannot distinguish
missing from another filesystem failure uses its backend contract's honest
portable category and must not parse human text to manufacture `NotFound`.
A backend route that cannot preserve bytes, distinguish missing from empty, or
enforce a restricted active context atomically returns `Unsupported`. Backend
documents define each narrow selected-version limitation.

## Tool Inventory

Executable toolsets enter through the builder. `ToolInfo`
contains a unique validated nonempty `String` name, optional `String`
description, and optional `serde_json::Value` input schema.
Its neutral name does not coerce provider-hosted or backend-native identity.

Inventory is the complete portable catalog the concrete backend owns and can
truthfully expose for that workspace. Mounted executable registrations and
backend-native hosted registrations may both appear; their provenance and
native identity remain private. Every returned item is callable in the active
workspace under its validated contract.

Results sort by name. Duplicate portable names fail the owning setup or
inventory operation. No tools yields an empty vector only when the backend can
prove the complete catalog is empty. A selected backend that cannot enumerate
a complete catalog returns `Unsupported` or `NotYetImplemented`; it never
silently returns a partial set as complete.

## Structured failure publication

Detectors preserve the most precise safe phase and evidence; supplied public
operations supply their operation. Unary and streamed prompts share `Prompt`.
Streams publish the same structured terminal failure once and then EOF. An
observer failure does not change accepted execution ownership. Full-error
clones preserve causes, supplementary failures, and truncation state.

An expired recovery deadline is primary, with the last failed recovery attempt
as supplementary context, not the timer's cause. Cleanup preserves the original
failure and appends later failures in observed order. Structured classification
never widens replay admission or changes native transaction commit authority.
