# agent-sdk

`agent-sdk` provides one portable runtime hierarchy over Codex App Server,
OpenCode Server, and Custom provider-backed agents:

```text
Agent -> Workspace -> Conversation
```

Each supplied opaque handle implements its public backend trait as a closed
Codex/OpenCode/Custom dispatcher. Callers use their concrete `AgentBackend`
hierarchy directly or compose matching caller-owned exhaustive enums; Workspace
and Conversation remain parent-associated child types.

## Construction

Managed runtimes start with required durable storage:

```rust,no_run
use agent_sdk::{config::AgentConfig, Agent, AgentBuilder, AgentStorage, SdkError};
use storage_sdk::StateStoreBackend;

async fn create<S: StateStoreBackend>(
    config: AgentConfig,
    storage: AgentStorage<S>,
) -> Result<Agent<S>, SdkError> {
    AgentBuilder::create(config, storage).build().await
}
```

Reopen uses `AgentBuilder::reopen(config, storage, agent_id)` or its typed
native counterpart. Custom callers resupply a direct
`P: provider_sdk::ProviderBackend` implementation; all callers resupply their
already-built `T: tool_sdk::ToolsetBackend` implementations, direct `StateStoreBackend`
implementation, and optional direct `DatabaseBackend` implementation.
Agent-sdk preserves `S`, `D`, `P`, and `T` statically and never persists lower
credentials or reconstructs integrations through factories.

Examples and real persistence tests use Lance: `LanceStateStoreConfig::memory()`
for process-local state, `LanceStateStoreConfig::directory(path)` for durable
state, and `LanceConfig::embedded_path(path)` for managed knowledge. Alternative
Storage implementations require caller-enabled Storage features; Agent does
not enable them or select a backend on the caller’s behalf.

Concrete Provider, Toolset, and storage implementations are the primary composition
path. A caller may define its own exhaustive Provider/Model/Session, Toolset/Handler, or storage
enum hierarchy when runtime selection is needed; agent-sdk does not require a
wrapper or normalize those implementations into supplied SDK handles.
Native Codex/OpenCode construction selects that enum only as a static type
with `AgentBuilder::create_for_backends::<FleetProvider, FleetTools>(...)` or the matching
reopen method. Custom installs the real value with
`AgentBuilder::create_for_backends::<FleetProvider, FleetTools>(...).with_provider(FleetProvider::Custom(...))`. Both
paths produce the same `Agent<S, D, FleetProvider, FleetTools>` type without making native
runtimes own provider lifecycle.

Each `with_toolset(T)` appends to the fixed inventory; Provider and Database
changes preserve every accepted tool. Neither T nor `T::Handler` requires
Clone. Caller-built shell tools carry explicit cwd and process policy through
their ordinary tool inputs; Agent has no implicit shell configuration.

## Public Modules

- `config` — backend and Custom runtime construction
- `identity` — typed Agent, Workspace, Conversation, and Notification ids
- `workspace` — context, defaults, revisions, and update requests
- `conversation` — conversation creation and snapshots
- `prompt` — foreground prompt requests, transient output streams, and results
- `notification` — durable delivery and interaction resolution
- `model` — portable model selection and inventory
- `tool` — source-neutral tool selection and inventory
- `knowledge` — source, scope, policy, and ingestion inputs
- `error` — closed failure classifications, operation/phase context, and safe evidence

Primary handles, backend traits, `AgentBuilder`, `AgentStorage`, and `SdkError`
remain at the crate root.

## Boundaries

- Custom retains the caller's `P` and calls `P::Model::generate` for unary
  delivery or `generate_stream` for incremental delivery through the same
  multi-turn prompt/tool owner. Session remains the Model-associated
  provider child and is not used or parameterized by Agent. The Model's
  associated continuation stays typed inside one active prompt; durable
  history is provider-neutral and reopen reconstructs messages without it.
- Tool execution stays in `tool-sdk`.
- Control-plane state stays in the caller-supplied
  `storage_sdk::StateStoreBackend` implementation.
- Managed knowledge is private agent behavior over a caller-supplied
  `storage_sdk::DatabaseBackend` implementation.
- Codex/OpenCode protocol values and provider envelopes never become public
  agent types.

There are no public factories, conformance runners, KnowledgeBase backend,
LoopDriver integration, Agent normalization constructors, compatibility API,
or runtime trait objects.

Custom accepts final prompt success only from a valid completed model turn.
Provider failure, truncation, filtering, paused continuation, and malformed
completion remain errors; partial output never becomes a successful durable
answer. A caller ToolHandler must return actual content or structured content,
including for model-visible failures. Empty results are rejected instead of
receiving SDK-authored replacement text.

## Documentation

- [Public API ledger](specs/API.md)
- [Architecture](ARCHITECTURE.md)
- [Specification index](specs/SPECIFICATIONS.md)
- [Strict testing](docs/STRICT_TESTING.md)
- [Contributor rules](AGENTS.md)

## Inspecting failures

`SdkError` is an opaque, cloneable error. Match `error.kind()` for broad
control flow and inspect `detail()`, `context()`, and `http_status()` for the
reason and detection phase. `cause()` contains normalized Agent errors;
`supplementary()` retains cleanup failures without replacing the primary.
These fields do not establish retry safety or durable cancellation.

```rust
use agent_sdk::{SdkError, error::{ErrorDetail, ErrorKind, ErrorPhase, Operation}};
let error = SdkError::integration("authentication rejected")
    .with_context(Operation::Prompt, ErrorPhase::ModelGeneration)
    .with_detail(ErrorDetail::AuthenticationRejected)
    .with_http_status(401);
assert_eq!(error.kind(), ErrorKind::Integration);
assert_eq!(error.http_status(), Some(401));
```

Caller implementations must supply already-redacted messages. Supplied
implementations never publish native prose through errors. Diagnostic trees
are bounded; check `diagnostics_truncated()` before treating retained evidence
as complete. See [the error contract](specs/ERRORS.md). Codex prompt records use
version 4; older records are rejected unchanged and are not migrated.

## Direct public-trait tests

Run `bash scripts/check_mock_contracts.sh --crate agent-sdk` from the workspace
root for the credential-free mock lane. The workspace primary CI entrypoint is
`bash scripts/check_ci.sh`. See [mock testing](../docs/mock-testing.md) for
targets, evidence, runtime qualification, and the separate release/live gates.
