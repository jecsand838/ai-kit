# ai-kit Architecture

`ai-kit` is a five-crate Rust workspace of independently usable provider,
storage, tool, agent, and application SDKs. The workspace root owns shared
policy and validation, not runtime orchestration. Historical artifacts under
`tmp/**` are immutable records, not current architecture.

## Workspace Model

```text
provider-sdk: Provider -> Model -> Session

storage-sdk:  Database
              StateStore -> StateTransaction

tool-sdk:     Toolset -> ToolHandler

agent-sdk:    Agent -> Workspace -> Conversation
                    |
                    +-- private Custom composition:
                        Provider -> Model
                        Toolset -> ToolHandler
                        Database-backed managed knowledge
                        StateStore-backed durable control state

app-sdk:      App -> Workspace -> Workflow -> WorkflowRun
              preserves caller-built Agent and storage parameters statically
```

The workspace has no application registry, public factory layer, public
conformance framework, global backend map, serialized trait object, or
cross-crate lifecycle framework.

## Public Trait and Private Dispatch Boundary

The lower products have eleven public behavioral traits:

| Crate | Trait | Opaque handle | Parent |
| --- | --- | --- | --- |
| provider | `ProviderBackend` | `Provider` | direct static implementation or built-in handle |
| provider | `ModelBackend` | `Model` | `ProviderBackend::Model` |
| provider | `SessionBackend` | `Session` | `ModelBackend::Session` |
| storage | `DatabaseBackend` | `Database` | direct static implementation or built-in handle |
| storage | `StateStoreBackend` | `StateStore` | direct static implementation or built-in handle |
| storage | `StateTransactionBackend` | `StateTransaction` | caller store's associated transaction or built-in handle |
| tools | `ToolsetBackend` | `Toolset` | sole public ingress root |
| tools | `ToolHandlerBackend` | `ToolHandler` | `ToolsetBackend::Handler` |
| agents | `AgentBackend` | `Agent` | direct static implementation or built-in handle |
| agents | `WorkspaceBackend` | `Workspace` | `AgentBackend::Workspace` |
| agents | `ConversationBackend` | `Conversation` | `WorkspaceBackend::Conversation` |

App's `workflow` module additionally owns five narrow core seams:
`ActionBackend`, `ConditionBackend`, `StateTyping`, `LifecycleHooks`, and
`ChildExecutor`. They preserve concrete caller behavior and local futures;
they do not replace or erase any lower product. `ChildExecutor` uses a generic
associated future to retain the concrete admitted batch. Other async contracts
use RPITIT. The App specification owns their exact signatures.

Each parent trait names its concrete child with an associated type. A Model
also names its opaque replay ownership as `ModelBackend::Continuation`.
Toolset
privately normalizes its ToolHandler after the approved root ingress.
Provider, StateStore, and Agent keep caller-defined children as associated
concrete types without normalization.

Only one handle exposes public erased extension construction:

```text
Toolset::from_backend
```

ToolHandler has no public `from_backend`; its private normalizer is an
implementation detail of Toolset. Provider, Storage, and Agent have no public
`from_backend` at any level: callers use concrete trait implementations or
optional caller-owned enums.

Every supplied lower-crate handle is an opaque struct over one private exhaustive enum,
and every built-in remains a concrete enum variant. The two Tool dispatchers
have one private `Extension` arm; only that arm uses the existing object-safe
erased adapter. The nine Provider, Storage, and Agent dispatchers are closed
over built-ins and have no `Extension` arm or erased adapter. Their public
traits are direct static contracts for caller implementations.

Built-ins never cross erasure merely for symmetry. One-field generic extension
wrappers, duplicate private behavioral traits, bare trait-object handles,
macro dispatch, generated routing, wildcard implementation-family matches,
and parallel forwarding facades are forbidden.

## Public Namespace Shape

Primary handles, backend traits, and errors remain easy to find at crate roots.
Supporting data is grouped by functional domain:

```text
provider_sdk::{
    config, generation, embedding, hosted, files, batch, models, session,
    runtime, error
}

storage_sdk::{
    database::{config, catalog, data, query, index},
    state::{config, record, transaction}
}

tool_sdk::{declaration, execution, interaction, local, mcp, schema}

agent_sdk::{
    config, identity, workspace, conversation, prompt, notification, model,
    tool, knowledge, error
}

app_sdk::{workflow}
```

These are caller modules, not public implementation trees. Provider clients,
storage managers, tool source runtimes, agent protocols, dispatcher enums,
erased adapters, tasks, channels, and native payloads remain private.

Passive DTO fields may be public. Invariant-bearing identifiers, configs with
validation, runtime owners, lifecycle owners, and opaque handles keep private
fields and receiver-owned behavior.

## Provider Ownership

`provider-sdk` owns:

- OpenAI, Anthropic, Gemini, and Ollama configuration and credentials;
- provider resource discovery, files, batch lifecycle, and runtime health;
- validated provider-local `ModelId` lookup;
- selected-model generation, streaming, embeddings, batch submission, and
  session creation;
- normalized messages, function calls/results, and portable hosted-tool
  configuration;
- provider wire rendering/parsing, continuation metadata, transport policy,
  and shared provider shutdown.

`Provider` and `Model` are cloneable shared handles. `Session` is unique and
mutable. A Model retains its Provider runtime; provider shutdown seals model
handles and drains admitted work. Model has no separate lifecycle.

Model selection is explicit and exact. There are no configured modality slots,
fallback aliases, provider-wide capability matrix, or request-local model
field. Calling an unsupported operation returns
`SdkError` classified as `ErrorKind::Unsupported`.

`FunctionCall` and `FunctionResult` are normalized values with validated
correlation. `Message<C>` carries a Model-associated continuation; the supplied
`GenerationContinuation` hides its native replay contents. A caller continues
a turn by preserving the assistant message and appending a validated
function-result message, never by handling a native provider envelope.

`HostedTool` is a closed portable configuration enum carried by
`ProviderToolSpec::Hosted`. The selected model owns validation and rendering.
There is no hosted-tools handle, opaque `Any` payload, hosted extension seam,
or provider loop driver.

## Tool Ownership

`tool-sdk` owns executable Local and MCP-client behavior. One `ToolInfo`
describes a tool's schema, annotations, icons, and interaction capabilities.
`ToolRegistration` binds that information to a private Local handler.

Built-in construction is direct:

```text
Toolset::local(Vec<ToolRegistration>)
Toolset::mcp(McpClientConfig)
```

Toolset listing returns `Vec<ToolInfo>`. A Toolset creates one associated
unique handler by validated `ToolName`. Toolset shutdown is borrowed,
idempotent, exactly once across clones, and seals future listing/handler
admission.

Accepted model-visible execution failures are completed error results. Outer
errors describe invalid input/state, transport, protocol, cancellation, or
lifecycle failure.

MCP serving is a separate non-Clone `McpServer`. It accepts an `Arc<T>` for any
`T: ToolsetBackend`, starts eagerly, owns and joins its task,
and has consuming shutdown. Server lifecycle never shuts down the Toolset.

## Storage Ownership

`storage-sdk` contains two independent products:

- `Database`: portable data, collection, query, and index behavior;
- `StateStore`: transactional revisioned control-plane state.

Database always supports LanceDB, with opt-in SurrealDB and Qdrant. A portable
operation remains only when at least two implementation families provide the
same semantics. The retained families are collection create/open/list/drop
and schema alteration; insert/lookup/scan/update/delete/merge/atomic partition
replacement/count; nearest/search-by-id/text/scroll/random sample; and index
create/list/drop/wait. One compact `CapabilitySet` reports genuine optional
behavior, while operation calls remain authoritative.

Realtime, backend-native envelopes, aliases, snapshots, versions,
import/export, optimizer controls, prewarm, sparse-only operations, and other
single-family controls are not portable Database APIs.

StateStore always supports embedded Lance. Redb, PostgreSQL, and SQLite/Turso
are opt-in implementations. The default feature set is empty. SQLite/Turso and
Qdrant cannot be enabled together because their pinned dependency features
conflict. Default Storage strict-live covers both Lance products; direct backend
and shared-abstraction changes add the relevant alternative gates. All real
downstream persistence and managed-knowledge test fixtures use Lance.

State transactions preserve native ownership:

- bounded admission and command queues;
- FIFO Redb writer admission;
- dedicated SQLite/PostgreSQL transaction actors;
- read-your-writes and tombstones;
- atomic consuming commit or rollback;
- exact `TransactionConflict`;
- rollback on drop;
- cancellation-safe terminal publication;
- deterministic task and OS-thread settlement.

No common actor/lifecycle framework obscures differing native semantics.

## Agent Ownership

`agent-sdk` owns Codex, OpenCode, and Custom implementations of the same
`Agent -> Workspace -> Conversation` contract.

Agent construction requires `AgentConfig` and `AgentStorage<S>` at creation or
reopen. Custom callers supply already-built runtime values:

```text
ProviderBackend implementation
ToolsetBackend implementation
DatabaseBackend implementation (optional managed knowledge)
```

Agent-sdk statically retains its caller-provided `S: StateStoreBackend`,
optional `D: DatabaseBackend`, `P: ProviderBackend`, and `T: ToolsetBackend`.
The associated `T::Handler` remains concrete; neither tool value needs Clone.
Supplied Agent handles are closed over Codex, OpenCode, and
Custom; caller hierarchies remain concrete or caller-owned exhaustive enums.
Agent-sdk does not erase Agent, provider, or storage implementations or persist
lower-crate credentials/factory declarations. Reopen requires callers to
resupply the same runtime categories. Native Codex/OpenCode construction may
select a caller `P` solely as static upper-composition context; those runtimes
never construct, retain, invoke, or shut down a `P` value.

Managed knowledge is a private agent owner backed by `AgentStorage<S>` and an
optional `D: DatabaseBackend`. Callers can supply validated knowledge sources,
scopes, policies, and ingestion requests. Retrieval context, ownership
lineage, verification, and database lowering stay private.

Custom owns one private multi-turn prompt loop. It resolves an exact Model,
advances transcript messages, renders portable tool definitions, executes
associated ToolHandlers with bounded concurrency, publishes durable
interactions, applies agent-owned approvals and loop limits, recognizes final
responses, and settles cleanup under the existing lifecycle deadline. Shell is
an ordinary caller-built Local or foreign tool with explicit cwd/input policy;
Agent has no implicit shell construction or task-local cwd injection.
Incremental output and ordinary prompts share this accepted-work owner;
observer drop does not cancel the prompt. The selected Model's associated continuation remains
typed inside that active loop and is not persisted in Agent durable history.
Custom abort observes cancellation and durable terminalization of its captured
generation independently of unread unary replies. Reply acknowledgement and
full close/shutdown quiescence remain separate obligations under the owning
[Custom conversation contract](agent-sdk/specs/custom/CONVERSATION.md).

Codex/OpenCode protocol ownership remains backend-private. Known events decode
to typed backend enums and receive one exhaustive portable, private-state,
unsupported, or not-yet-implemented classification.

Durable state remains backend-blind. Notification ownership, at-least-once
delivery, conflict reconciliation, accepted-work ownership, stale revisions,
conversation terminality, workspace/agent cascading invalidation, and
reopenability remain unchanged in principle.

## Cross-Crate Dependencies

The intended dependency direction is:

```text
provider-sdk -----> agent-sdk -----> app-sdk
storage-sdk  --+--> agent-sdk
               +-------------------> app-sdk
tool-sdk ---------> agent-sdk
```

`provider-sdk` does not depend on `tool-sdk`. Provider models describe
function calls and results; `agent-sdk` owns the tool execution loop.

`storage-sdk` does not depend on `tool-sdk`. Model-visible storage tools are
caller-authored Toolsets.

`tool-sdk` remains provider-neutral and storage-neutral. Callers supply MCP
authentication through connection configuration, including HTTP headers or
explicit stdio environment entries. Provider credentials remain owned by
`provider-sdk`.

`agent-sdk` consumes direct ProviderBackend and ToolsetBackend implementations,
and static StateStoreBackend and DatabaseBackend
implementations. It never inspects lower
private enums, native ids, protocol envelopes, source identity, storage keys,
or credentials.

`app-sdk` statically preserves a direct `A: AgentBackend` and its own
`CS: StateStoreBackend`. It uses `A::Workspace` and that Workspace's associated
Conversation without independent child parameters, persists app-owned keys
and caller revisions, and never invokes lower provider behavior or
reconstructs or inspects lower resources.

Callers supply App storage directly even for process-local use; there is no
App-owned built-in storage constructor or lower default generic requirement.
The pinned rurr core supplies overlapping workflow semantics. App owns one
transition engine for automatic, stepped, iterated and streamed execution,
control sets, immutable checkpoint history and explicit resumable deletion.
Its serialization-only global field/tag registry may privately erase captured
codec functions, with a snapshot per operation; this is not a general backend
or runtime registry. Server, UI, CLI and observability exporter products remain
outside the core crate.

## Testing Architecture

Production crates contain no public conformance framework. Behavioral reuse in
tests stays inside ordinary crate integration-test helpers.

The primary deterministic lane uses test-local structs and exhaustive caller
enums implementing all sixteen public behavioral traits directly. Seven
integration targets exercise those contracts and the real Custom Agent and App
with concrete lower implementations. Associated children and continuation types
remain exact; permitted local App values and child futures remain concrete.
There is no extension ingress or shared mock crate in this lane. Process-local
StateStore reconstruction does not prove native durability, and scripted
Database responses do not implement a database engine. The
[mock-testing guide](docs/mock-testing.md) owns commands and evidence rules.

The proof layers are:

1. compile contracts for public modules, associated child types, trait
   implementations, privacy, ownership, Send/Sync, and rejected child ingress;
2. deterministic integration tests for validation, Tool erasure, direct
   Provider/Storage/Agent traits, lifecycle, cancellation, cleanup, redaction,
   conflict, and continuation;
3. provider wire/model normalization tests;
4. storage transaction-actor tests;
5. MCP client/server tests;
6. agent durable lifecycle and notification tests;
7. app workflow, checkpoint, recovery, and ownership tests;
8. strict-live tests for real configured implementations only.

Tests do not freeze private helper names, exact internal file topology,
match-arm spelling, or architecture prose. Small source guards may distinguish
the two open Tool dispatchers from the nine closed Provider/Storage/Agent
dispatchers and require exact absence of deleted public symbols.

## Shared Tooling and Documentation

- `scripts/check_no_path_wiring.sh` enforces the workspace path-dependency
  boundary.
- `scripts/check_no_type_aliases.sh` bans Rust item type aliases.
- `scripts/check_ci.sh` runs the primary deterministic checks;
  `scripts/check_mock_contracts.sh` selects its seven behavioral targets.
- `scripts/qualify_mock_runtime.py` separately qualifies credential-free,
  network-denied execution after dependency preparation.
- `scripts/check_docs_site.sh` checks active documentation inventory and local
  file links, then builds the five-crate rustdoc site and runs its doctests.
- `scripts/verify_workspace_strict.sh` runs the five crate-owned strict-live
  gates after their environment files are sourced.
- `scripts/vendor_update.py` and crate-local vendor metadata own pinned-source
  refreshes.

The integrated registry graph uses LanceDB 0.40.0 and Lance 13.0.0 for the
local Database and StateStore products. Qdrant Edge remains at the explicitly
selected 0.6.1 release so its geometry requirements coexist with SurrealDB
3.3.0 without a Cargo patch or vendored dependency override. Remote Qdrant uses
1.19.2. Consumer packages must resolve independently of workspace configuration
and source trees; dependency guards have no third-party source exception.

LanceDB 0.40.0 enables its single approved `remote` feature because published
job decoding references error variants guarded by that feature. The adapter
contract remains local filesystem/memory storage. This build exception brings
the native HTTP/Flight/OAuth dependencies into the registry graph; it does not
expose LanceDB Cloud or enable cloud object-store features. Direct dependency
default features remain disabled. The separate approved macOS/Qdrant registry
constraint and its shared-resolution limits belong to
[Storage's feature guidance](storage-sdk/README.md#features).

Root docs explain workspace ownership once. Crate READMEs teach downstream
workflows, crate architecture files describe private ownership, crate AGENTS
files own exact validation, and focused API/spec documents own exact public
contracts. `docs/documentation-ledger.toml` records canonical owners and active,
historical, template, and excluded document families. Workflow guidance applies
when explicitly selected. Historical plans and execution results retain their
recorded scope and do not replace current contracts or establish a current pass.
