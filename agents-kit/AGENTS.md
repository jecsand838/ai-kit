# agents-kit AGENTS

This file adds agent-specific rules to the workspace
[`AGENTS.md`](../AGENTS.md).

## Scope

`agents-kit` exposes one portable hierarchy over Codex, OpenCode, and Custom:

```text
Agent -> Workspace -> Conversation
```

The crate owns portable identities, lifecycle, revisions, notifications,
interaction resolution, and private managed knowledge. Backend protocols,
provider messages, tool runtimes, StateStore records, Database payloads, and
native ids remain private.

[`specs/API.md`](specs/API.md) is the sole exact public inventory.

## Public Architecture

- `AgentBackend`, `WorkspaceBackend`, and `ConversationBackend` are the only
  public behavioral traits in this crate.
- `AgentBackend` has an associated `Workspace: WorkspaceBackend`.
  `WorkspaceBackend` has an associated
  `Conversation: ConversationBackend`.
- `Agent`, `Workspace`, and `Conversation`, every concrete built-in, and each
  private dispatcher implement their corresponding trait.
- Caller Agent, Workspace, and Conversation implementations remain their own
  concrete associated hierarchy or a caller-owned exhaustive enum hierarchy.
- `AgentInner`, `WorkspaceInner`, and `ConversationInner` are closed exhaustive
  dispatchers over Codex, OpenCode, and Custom. They have no erased adapter,
  `Extension` arm, or normalization constructor.
- Do not add public or private `KnowledgeBase` behavior, factories, production
  conformance suites, `from_backend` constructors, one-field extension
  wrappers, trait objects, registries, aliases, macros, generated dispatch,
  compatibility surfaces, or duplicate behavioral traits.

## Public Modules

Primary handles, backend traits, the builder, and `KitError` stay at the crate
root. Supporting public values live in:

```text
config
identity
workspace
conversation
prompt
notification
model
tool
knowledge
error
```

Passive request/response DTOs may expose public fields when direct data
construction is their purpose. Runtime owners, validated identities, configs
with invariants, and lifecycle owners keep private fields.

## Construction And Cross-Crate Boundaries

`AgentBuilder` constructs supplied Codex, OpenCode, or Custom Agents only and
is not an ingress for caller `AgentBackend` implementations. Construction
starts with exactly one of:

```rust
AgentBuilder::create(AgentConfig, AgentStorage)
AgentBuilder::reopen(AgentConfig, AgentStorage, AgentId)
AgentBuilder::create_for_backends::<P, T>(AgentConfig, AgentStorage)
AgentBuilder::reopen_for_backends::<P, T>(AgentConfig, AgentStorage, AgentId)
```

The `for_backends` forms fix the tool implementation `T` and static `P` context for native Codex
and OpenCode Agents; they do not accept or retain a provider value. Custom
continues to receive its concrete `P` through `with_provider`. This permits
native and Custom Agents to preserve one caller-owned provider enum type in a
homogeneous downstream inventory without provider erasure.

Custom builder composition accepts a direct Provider implementation, while
all builders accept already-built Toolset trait implementations and direct storage trait
implementations:

- `with_provider<P: providers_kit::ProviderBackend>(P)`
- repeated `with_toolset(T)` with one fixed `T: tools_kit::ToolsetBackend`
- `with_knowledge_database<D: storage_kit::DatabaseBackend>(D)`

`AgentStorage<S>::new` accepts `S: storage_kit::StateStoreBackend` directly.
The resulting `Agent<S, D, P, T> -> Workspace<S, D, P, T> -> Conversation<S, D, P, T>`
hierarchy preserves all four implementation types; supplied lower-crate handles
are ergonomic defaults and never required. `T::Handler` remains associated and
neither `T` nor its Handler needs Clone. Mixed tool inventories use caller-owned
exhaustive Toolset/Handler enums. Provider and Database replacement preserve
the accepted tool vector. `P::Model`, its
associated `Continuation`, and its associated Session remain children of `P`,
never independent Agent generic parameters. Custom retains the exact
continuation only inside one active prompt/tool loop; durable history remains
provider-neutral caller and final-assistant text. agents-kit does not own lower-crate credentials, declaration
registries, factories, Agent/storage/provider erasure, or normalization. Reopen
callers resupply the required values; no lower-crate configuration or
credentials are persisted.

Custom requires a `P: ProviderBackend` used directly. Runtime selection may use
a caller-owned exhaustive Provider/Model/Session enum hierarchy; agents-kit
does not require such enums or normalize them into supplied handles. Codex and
OpenCode retain their native providers.
Toolset and knowledge support remain optional where a selected backend can
honestly represent them.

## Custom Runtime

Custom owns one private prompt-loop domain using:

```text
P::model -> P::Model::{generate, generate_stream}
```

Session remains the Model-associated provider child and is not used or
parameterized by Agent.

The loop is generic over `P::Model::Continuation` at the call boundary. It
appends each returned assistant message unchanged during that prompt, never
inspects or serializes its continuation, and reconstructs continuation-free
messages from durable history after reopen.

The loop, not providers-kit, owns multi-turn agent orchestration:

- bounded model turns and tool actions;
- selected executable tool declarations;
- handler construction and bounded parallel stepping;
- durable approvals through the single notification interaction path;
- shell as an ordinary local function tool;
- transcript advancement and final-response detection;
- cancellation, lifecycle deadlines, and Toolset cleanup.

Validate each complete model response's finish reason and call shape before
constructing handlers or advancing the transcript. Preserve the error matrix
in `specs/custom/CONVERSATION.md` for both unary and streaming execution.
Reject empty completed tool results rather than inventing model-visible text;
valid structured-only results and `is_error` values remain supported.

Do not recreate `LoopDriver`, mounts, provider hooks, loop-error recovery
families, generic retry controllers, provider shell policy types, or
provider-specific driver copies.

`CustomAgentConfig` owns one required default `ModelSelector`, existing
`ToolSelection`, one small loop-limit value, one agent-owned tool approval
enum. Callers construct ordinary shell tools with explicit cwd and process
policy; Agent does not construct shell tools or inject task-local cwd. Workspace or prompt model
selection may override the default.

## Knowledge

Managed knowledge is a private agent owner composed from required
`AgentStorage<S>` and an optional caller-supplied
`D: storage_kit::DatabaseBackend`.

The public surface retains only caller input:

- `KnowledgeSource`
- `KnowledgeScope`
- `KnowledgePolicy`
- `IngestKnowledgeRequest`

Owner lineage, retrieval budgets, context/snippets/provenance, verification,
and Database storage models remain private. Internal verification returns
unit. Knowledge is never a public implementation seam, StateStore substitute,
or automatic model-visible tool.

## Lifecycle And State

- `Agent`, `Workspace`, and `Conversation` are unique non-Clone handles.
- Agent shutdown and Workspace close cascade runtime invalidation while
  preserving quiescent durable records for direct-id reopen.
- Conversation close is terminal.
- Prompt is async foreground work owned by runtime after acceptance; dropping
  its caller future does not cancel it or release admission early.
- PromptStream is bounded transient observation of that same prompt owner,
  with real text deltas and an authoritative terminal response after commit.
  Receiver drop or overflow does not abort accepted work. Terminal-only caller
  Conversations retain the default honest Unsupported streaming operation.
- Abort joins the captured prompt generation's durable terminalization and
  settled cancellation independently of unary reply consumption. Preserve
  generation fencing, the prompt waiter's acknowledgement, and full
  close/shutdown quiescence as distinct obligations.
- Notification delivery is one exclusive durable at-least-once receiver.
  Interactions have one resolution path and no polling or producer-specific
  API.
- The receiver's generic `from_stream` value constructor is permitted so a
  downstream `ConversationBackend` can return the concrete receiver required
  by the trait. It is not a backend-ingress or lifecycle seam.
- Workspace update is revision-checked and atomic. Durable commit precedes
  stale-handle invalidation; postcommit cleanup cannot convert success to
  failure.
- Preserve exact `TransactionConflict` from StateStore paths. Retry or
  reconcile only where an operation contract permits it, within the existing
  lifecycle deadline; surface every non-conflict error unchanged.
- No synchronization guard or live StateStore transaction crosses provider,
  tool, Database, process, notification, or network I/O.

## Backend And Protocol Ownership

- Codex and OpenCode keep selected protocol DTOs and transport behavior
  private and typed.
- Known events decode into closed backend enums and receive an exhaustive
  portable/private/unsupported/not-yet-implemented outcome.
- Custom uses normalized provider messages and never exposes a provider wire
  envelope.
- Backend-native tool installation and inventory remain backend-owned.
- StateStore records, keys, revisions, attachments, recovery facts, and
  cleanup obligations remain private.
- Public errors contain safe caller context and no provider/backend label,
  native id, key, route, raw payload, or lower-crate error value.

## Error Ownership

`KitError` is opaque, cloneable, and nonserializable. Closed public inspection
enums live in `error`; keep the exact vocabulary in `specs/API.md`. Public
operations attach their operation, detectors attach a precise known phase,
and boundaries enrich or clone the whole error instead of reconstructing a
message-only category. Normalize lower sources into Agent-owned errors and
preserve bounded supplementary cleanup failures. Do not infer retry safety,
durable cancellation, or local transaction conflict from remote classifications.

Codex prompt records use a private version-4 failure snapshot containing only
safe structured facts. Reject other versions without writes or migration.
OpenCode root attachment validation rejects older formats before native
initialization or recovery writes, without defaults or migration. The owning
[`specs/opencode/STATE_STORE.md`](specs/opencode/STATE_STORE.md) defines the
required native-routing evidence and backend record formats.

## Tests And Documentation

- Production `src/**` contains no test-only code.
- `/tests` owns downstream trait implementations, deterministic lifecycle and
  state contracts, source contracts, and strict-live.
- Ordinary private test helpers replace public conformance suites.
- All real StateStore and managed-knowledge fixtures use embedded Lance.
  Backend-specific Storage tests and alternative service provisioning remain
  in storage-kit; caller-defined trait probes remain valid.
- Source contracts protect the public module allowlist, private enum dispatch,
  deleted symbols, and protocol privacy. They do not lock private helper
  names, exact files, record graphs, match syntax, or call choreography.
- Strict-live alone proves Codex, OpenCode, provider, tool, storage, process,
  and protocol behavior.
- Strict-live Kit correctness is unconditional. Require exact untouched terminal
  output where the prompt requests an exact answer, except for the two explicitly
  approved OpenCode Question cases identified in `docs/STRICT_TESTING.md`.
  Those cases retain blocking native interaction, output-fidelity, lifecycle and
  cleanup checks; exact raw answer and both option-description copy grades are
  separately and mandatorily reported, with a false grade visibly failed but
  nonblocking. Actual descriptions must remain unchanged across native input
  observations; all other typed Question requirements remain blocking. This exception
  permits no model substitution, output normalization, retry or other relaxation.
  Require the configured model role without inventory-default or first-entry
  fallback. A test failure must still settle its runtime owners before reporting
  the original failure.

## Validation

Run the relevant deterministic targets before and after each implementation
slice, recording meaningful red evidence first. The final crate gate is:

```bash
source agents-kit/strict-tests.env
bash agents-kit/scripts/precheck_strict_live.sh
bash agents-kit/scripts/verify_strict_live.sh
```

The verifier requires exactly one valid version-2 Question-grade receipt for each
of the two approved cases, emitted only after successful Kit checks and cleanup.
Missing, duplicate, malformed or Kit-failed receipts fail the gate. A failed raw
model-copy grade remains visible and cannot be described as model-quality
acceptance. Native Cargo and log-capture failures remain blocking; report parsing
cannot erase them. The retained strict-live log is private under `target/`.

The verifier runs `opencode_event_ownership` with `--test-threads=1` so
independent loopback fixtures do not compete during startup. Keep each case's
concurrent operations and deadline assertions intact; do not increase operation
timeouts to compensate for parallel fixture scheduling.

Exact non-live and live ownership is documented in
[`docs/STRICT_TESTING.md`](docs/STRICT_TESTING.md). Update specs, docs, scripts,
source contracts, and env templates together when the public or validation
contract changes.

## Authority And Research

- [`specs/SPECIFICATIONS.md`](specs/SPECIFICATIONS.md) selects pinned backend
  sources.
- [`specs/API.md`](specs/API.md) owns exact public shape.
- Other root specs own behavior; backend specs own sourced deltas.
- Ground Codex/OpenCode claims in selected vendored sources, official docs, or
  strict-live proof. Never infer protocol behavior from architectural
  symmetry.

## Primary mock CI

The direct-trait targets are required by `bash scripts/check_ci.sh` and can be
scoped with `bash scripts/check_mock_contracts.sh --crate agents-kit`. Follow
[the mock test contract](../docs/mock-testing.md). Keep these credential-free
targets separate from existing integration and strict-live verification; the
existing gates above remain required. Production regressions stay enabled and
are recorded in [the bug ledger](../docs/mock-testing-production-bugs.md).
