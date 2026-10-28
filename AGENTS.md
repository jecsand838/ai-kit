# ai-kit AGENTS

Operating contract for the five compiled `ai-kit` crates.

## Scope

- Workspace crates: `providers-kit`, `storage-kit`, `tools-kit`, `agents-kit`,
  and `app-kit`.
- `tmp/**` contains immutable historical planning artifacts. Do not edit,
  validate, stage, or treat those files as current architecture.
- Do not create an app substitute, compatibility crate, or shared runtime
  framework.

## Public Architecture

Only these public behavioral traits are allowed:

- provider: `ProviderBackend`, `ModelBackend`, `SessionBackend`;
- storage: `DatabaseBackend`, `StateStoreBackend`,
  `StateTransactionBackend`;
- tools: `ToolsetBackend`, `ToolHandlerBackend`;
- agents: `AgentBackend`, `WorkspaceBackend`, `ConversationBackend`;
- applications, under `workflow`: `ActionBackend`, `ConditionBackend`,
  `StateTyping`, `LifecycleHooks`, `ChildExecutor`.

Public hierarchies: `Provider -> Model -> Session`, `Database`,
`StateStore -> StateTransaction`, `Toolset -> ToolHandler`,
`Agent -> Workspace -> Conversation`.

Required parent associated types/bounds:

| Parent | Associated type and bound |
| --- | --- |
| `ProviderBackend` | `Model: ModelBackend` |
| `ModelBackend` | `Session: SessionBackend`; `Continuation: Clone + Send + Sync + 'static` |
| `StateStoreBackend` | `Transaction: StateTransactionBackend` |
| `ToolsetBackend` | `Handler: ToolHandlerBackend` |
| `AgentBackend` | `Workspace: WorkspaceBackend` |
| `WorkspaceBackend` | `Conversation: ConversationBackend` |

Exact signatures belong to the owning portable API docs; do not duplicate
signature ledgers in workspace docs.

`Toolset::from_backend` is the sole public erased extension constructor.

ToolHandler has no public `from_backend`; Toolset normalizes its child privately.
Provider/Storage/Agent prohibit normalization ingress; callers implement public
traits directly with their own associated children.

Supplied Provider/Storage/Tool/Agent handles implement public traits as opaque
structs over private exhaustive enums of concrete built-ins. Each Tool dispatcher
(two total) has exactly one private `Extension` arm; the sole root extension
constructor creates only that arm. Built-in constructors create concrete
variants directly. The nine Provider/Storage/Agent dispatchers contain built-ins
only: no `Extension`, erased adapter, identity carrier or normalization constructor.

At each approved seam, the public trait is the sole behavioral contract for
built-ins, private dispatchers, and supplied handles.
Async methods use RPITIT except App's `ChildExecutor`: its generic associated
future supports local, non-Send child batches without boxing or erasing the
returned future. `ChildBatch` may pin concrete futures in `Box<F>` for safe,
unerased extraction. App owns the exact signature; this exception does not
widen lower-crate traits. Tool's private object-safe adapter serves only one
`Extension` payload. Provider/Storage/Agent callers use concrete static types or
caller-owned exhaustive composition enums. No duplicate private behavioral
traits, one-field generic extension wrappers, public `dyn` backends, bare erased
roots, dispatch macros, generated routing tables, wildcard dispatcher arms or
same-name inherent forwarding contracts.

## Tight Public Namespaces

Crate roots expose primary handles, backend traits, errors, and few frequent
construction owners. Supporting types belong in functional public modules:

- `providers-kit`: `config`, `generation`, `embedding`, `hosted`, `files`,
  `batch`, `models`, `session`, `runtime`, `error`;
- `storage-kit`: `database::{config,catalog,data,query,index}` and
  `state::{config,record,transaction}`;
- `tools-kit`: `declaration`, `execution`, `interaction`, `local`, `mcp`,
  `schema`;
- `agents-kit`: `config`, `identity`, `workspace`, `conversation`, `prompt`,
  `notification`, `model`, `tool`, `knowledge`, `error`.
- `app-kit`: `workflow`.

Do not flatten advanced DTOs onto roots or bury primary handles.
Rustdoc/examples use documented public modules, never private source paths.

Fields default private; passive request/response/config/serialization/protocol/
persistence DTOs may expose fields. Runtime/synchronization/lifecycle owners,
validated identities and opaque handles keep fields private. No accessors merely
to hide passive data; use domain methods instead of exposed cross-module fields.

## Deleted Product Surfaces

Forbidden in workspace API and implementation:

- public conformance owners, inputs, failures, or production conformance
  modules;
- durable lower-crate factory traits or factory request DTOs;
- `LoopDriver` and provider-owned agent-loop, shell, approval, retry, mount,
  or tool-runtime frameworks;
- `ProviderHostedToolsBackend`, `ProviderHostedTools`, opaque hosted payload
  downcasting, and the hosted-tools dispatcher;
- public `KnowledgeBase`, `KnowledgeBaseBackend`, public retrieval context,
  or public knowledge conformance/factory surfaces;
- storage-to-tool adapters and the storage `toolsets` feature;
- portable database realtime and backend-native operation envelopes;
- direct child `from_backend` entrypoints;
- topology tests that freeze helper names, exact private files, or
  implementation choreography.

No removed-symbol aliases, deprecated twins, shims, wrapper-only DTOs,
tombstone modules, feature stubs, or parallel routes.

## Crate Boundaries

### Provider

`providers-kit` owns credentials, discovery/resources, validated `ModelId`
selection, normalized generation/streaming, embeddings, batches, sessions,
portable hosted-tool configuration, wire adaptation and shared shutdown.

`Provider -> Model -> Session` is a resource hierarchy. `Model` owns exact
selected-model stateless calls; `Session` owns provider-supported stateful
interaction. Model owns no agent loop, transcript, tool runtime, approvals,
shell execution, retry controller, or independent shutdown. Provider-owned
bounded transport retries do not authorize agent-loop or generation-retry
policy; those remain caller-owned.

Normalized `FunctionCall`/`FunctionResult` cross the boundary.
`ModelBackend::Continuation` associates exact selected-Model replay. Supplied
`GenerationContinuation` keeps native contents opaque inside returned assistant
messages; caller Models may define their own type. Never expose native envelopes.

The selected model validates and renders closed `HostedTool` values carried
by `ProviderToolSpec`; unsupported combinations return the owning portable
unsupported error.

### Tools

`tools-kit` owns executable Local/MCP client toolsets, schema validation,
source-neutral `ToolInfo`, unique handlers, resumable interactions and shared
shutdown. Construct built-ins directly with `Toolset::local`/`Toolset::mcp`.

Separate non-Clone `McpServer` accepts caller `Arc<T>` for any
`T: ToolsetBackend`, starts eagerly, tracks its task and consumes self on
shutdown without shutting down the Toolset. `ToolsetBackend` has no `serve`/`stop`.

Accepted model-visible failures are completed `ToolResult` values with
`is_error = true`. Reserve outer `ToolError` for input, state, protocol,
transport, cancellation, lifecycle and closure failures.

### Storage

`Database` owns portable data/queries; `StateStore` owns transactional
control-plane records. Keep them separate.

Public Database operations require matching honest semantics from at least two
implementation families. Backend-specific controls, realtime, native payloads,
aliases, snapshots, versions, import/export, optimizer controls, sparse-only
operations, and other single-family behavior stay private or are deleted.
Operation calls are authoritative; one compact `CapabilitySet` describes only
retained optional behavior.

StateStore transactions stay owned, lifetime-free and non-Clone. Their actors
preserve bounded admission, exact conflicts, rollback-on-drop, cancellation
safety, terminal authority and deterministic task/thread joins. Never consolidate
backend guards with differing native transaction semantics.

Storage default build/strict-live uses Lance for both products. Alternatives
require explicit `redb`, `postgres`, `surrealdb` or `sqlite` features.
Qdrant support is temporarily removed, including its feature and validation
lanes. Never use Storage `--all-features`. Direct backend
and shared-abstraction changes require relevant alternative strict-live coverage.
Downstream persistence/knowledge fixtures use Lance; backend-specific proof
stays in Storage.

The approved LanceDB 0.40.0 dependency exception enables only its `remote`
feature to compile the published job-error paths. Keep direct dependency
defaults and cloud object-store features disabled. This does not authorize
remote supplied Storage routes, additional public behavior or unrelated
dependency features; the local filesystem/memory boundary remains required.

External Rust dependencies must resolve from crates.io. Cargo patches,
replacements, and external path overrides are forbidden; the dependency checker
permits only the five local workspace packages. No upstream source tree is
exempt from the Kit path or alias guards.
The temporary advisory exceptions in `scripts/security_exceptions.toml` require
complete, individually reviewed RustSec IDs. Preserve unfiltered findings and
fail on new vulnerabilities or unsoundness outside that list. Remove exceptions
as soon as compatible upstream fixes are available. Risk acceptance does not
establish remediation or waive behavioral validation. Follow
[the dependency security policy](docs/security-dependencies.md).

### Agents

`agents-kit` owns portable `Agent -> Workspace -> Conversation`, durable
control-plane state, notifications, conflict reconciliation, cancellation and
Codex/OpenCode/Custom adaptation.

Construction retains caller-selected `P: ProviderBackend`, `T: ToolsetBackend`, required
`AgentStorage<S>` and optional `D: DatabaseBackend`. Custom owns one direct `P`,
already-built `T` values and only `T::Handler` children; neither `T` nor Handler
requires Clone. Native Codex/OpenCode may select `P` solely for static
upper composition: never construct, retain, invoke or shut down a provider value.
Never persist lower credentials or reconstruct lower products from declarations.
Creation/reopen are distinct builder entrypoints requiring storage up front.
Agent traits remain direct static contracts: supplied handles dispatch only
Codex/OpenCode/Custom; caller hierarchies stay concrete or use matching
exhaustive enums.

Managed knowledge stays private. Public inputs cover only caller-used source,
scope, policy, and ingestion requests; retrieval context, ownership lineage,
verification, and database adaptation remain private.

Custom owns the multi-turn prompt loop: advance transcripts through
`Provider -> Model`, execute `Toolset -> ToolHandler`, publish durable
interactions, bound loop/tool limits, and preserve deadline, cancellation,
conflict, cleanup and notification semantics. Exact `P::Model::Continuation`
stays within one prompt loop; durable history is provider-neutral and reopen
reconstructs messages without it. Portable shell is a caller-built Local or
foreign tool with explicit cwd/input policy; Agent neither constructs shell
tools nor injects task-local cwd. Incremental output shares the accepted prompt
owner; observer drop does not cancel accepted work.

### Applications

`app-kit` owns application workspaces, workflow validation/execution, checkpoints,
recovery and app control-plane state. It consumes ready `A: AgentBackend` via
`AgentSpec<A>` plus direct `CS: StateStoreBackend`; no providers-kit/tools-kit
dependency.

App uses `A::Workspace` and its associated Conversation, never independent child
parameters. Inventories are homogeneous in `A`; heterogeneity uses caller-owned
exhaustive Agent/Workspace/Conversation enums. Callers supply CS directly,
including process-local storage. App never constructs supplied StateStore,
requires supplied lower defaults, parses/formats/persists/compares `AgentId`, or
reconstructs lower configuration. Recovery stores only public workspace/conversation IDs.

The pinned rurr core governs overlapping observable workflow semantics.
App-only action, condition, typing, lifecycle and child-executor traits own
core behavior absent from lower crates. Keep runtime values concrete and DTOs
in `workflow`; no public mutable AST or generalized callback/runtime registry.
Only the process-global field/tag serialization registry may privately erase
captured codec functions; no Any payload or lower-backend normalization ingress.

Execution streams own uncommitted action work; observation streams do not.
Preserve native transaction commit authority after admission. Delete retained
history only explicitly, boundedly and resumably. Server, UI, CLI, concrete
integration adapters and observability exporters remain separate future
products outside App core completion.

## Rust Structure and Safety

- No Rust item type aliases at any visibility; trait associated types remain
  required and valid.
- No unsafe code, `async_trait`, generated source, `include!`, `#[path]`,
  negative conditional compilation, or production test branches.
- No `macro_rules!`, proc macros, generated dispatch, or codegen to hide
  repetition; `tools_kit::tool!` is the sole narrow ergonomic exception.
- No standalone library functions/loose helpers absent an exact human-written
  spec requirement. Put behavior on the smallest honest domain owner, builder,
  data-carrying enum or trait.
- Prefer one owner plus a data-carrying state enum over `Started`, `Completed`,
  `Failed`, `Inputs`, `Task`, or similar same-root carrier families.
- No `#[cfg(test)]` in production `src/**`; executable tests belong in each
  crate's `tests/` tree.
- Use `mod.rs` for directory-backed modules and explicit imports; no `use super::*`.
- Never read environment variables in production `src/**`; callers supply
  validated configuration.
- No compatibility layers, dead code, lint/formatting suppressions, new
  dependencies/crates, or synthetic portable success.

Async owners track every task/thread, define cancellation before spawn, avoid
guards across unrelated awaits, bound external waits and publish terminal results
only after cleanup. Cleanup errors never erase primary failures; handle drop
never silently detaches API-owned work.

## Testing and Validation

Test public behavior, not private spelling:

- Compile contracts: public modules/signatures, associated children, privacy,
  ownership, auto traits, child-ingress rejection.
- Primary direct-trait mocks: test-local structs and exhaustive caller enums
  for all sixteen public traits, plus real Custom Agent/App consumers. Preserve
  exact associated children and permitted local App futures; no extension
  ingress, supplied lower-backend construction, or shared mock crate in this lane.
- Deterministic integration: validation, Tool private erasure, direct
  Provider/Storage/Agent composition, lifecycle, cancellation, redaction,
  conflict, cleanup.
- Provider wire/model: normalization/replay; Storage actors: transactions;
  MCP: client/independent server; App: workflow/checkpoint/recovery/lower ownership.
- Strict-live alone: real providers, services, processes and persistence.

Share ordinary test-local helpers only within a crate. No public conformance
API, production fixture, callback framework, macro, or shared conformance crate.

Only the two OpenCode interaction cases named in Agent's strict guide and two
OpenAI Realtime cases named in Provider's strict guide separate Kit/scenario
correctness from verbatim-copy quality. Exact native delivery where independently
observed, correlation, output fidelity, recovery, streaming and cleanup stay
blocking. Within the two Question cases, answer copying and each option
description's exact copying have separate mandatory grades; actual native
description fidelity and every other typed Question requirement remain blocking.
Realtime requires new audio and nonblank transcription after successful
function-result submission; public-session evidence does not independently prove
native parity or semantic result use. Report every raw copy grade, including
failures; they do not determine Kit completion. Missing/invalid grades fail
validation. These exceptions change no other acceptance rule and prove neither
model-quality acceptance nor statistical reliability.

Before completion-work production changes, add/change meaningful tests and
record red evidence. Require independent subagent review of the actual diff
before closing an area. Check existing owners for extension/reduction before
adding abstractions. Run one Cargo command at a time.

App may directly use pinned workspace futures-util and serde_json preserve_order
only when its approved core contract requires them. Record/review the Cargo graph
effect; no new package version.
App may add exact locked sha2 0.10.9 without default features solely for the pinned
public child identity digest; no other hash API, package version, dependency
expansion or generated code.

Tool may add locked `futures-core =0.3.32` directly, optional without default
features, enabled only by existing `mcp-server`, solely for native Stream bounds
owning MCP session/worker cleanup. No public trait, package version or other
dependency is permitted.

Run exact current crate gates. Workspace completion requires formatting,
non-Storage workspace Clippy plus every supported Storage lane, docs/doctests,
no-path/no-alias checks, all five strict-live verifiers, the five-crate wrapper
and locked clean-checkout proof. Partial/deterministic results never prove live
behavior.

The workspace wrapper defaults full. Only explicit `--skip-long-provider-batches`
permits seven Provider exclusions: four long OpenAI batch cases plus Anthropic
inline, mixed-row and bounded-results completion. Cancellation, missing-resource,
unsupported-input, every other strict-live case and gate stay required. Final
receipts must say qualified coverage and list exclusions. An environment toggle
alone never authorizes partial workspace completion.

An explicitly requested release continuation may use reviewed, hash-verified
unchanged-source receipts through the canonical wrapper
`--resume BUNDLE` path. Follow [the continuation contract](docs/release-qualification.md):
retain original failures, require complete command/source/build/runtime identity,
label reuse explicitly, and keep prerequisites, App prepare proof, artifact
publication and final clean-checkout proof fresh. Missing provenance never
authorizes omission of a gate. An explicit `--skip-strict-live` invocation is a
qualified local pass with all five strict-live suites listed as deferred; it is
not workspace release qualification. Normal wrapper invocation remains full execution.

Release-gate dependency resolution uses native Cargo `--locked`: missing/stale
locks fail, never update. This does not force offline mode or change feature
lanes, service prerequisites or separate locked clean-checkout proof.

## Documentation Ownership

- Root `README.md`: orientation/entrypoints.
- Root `CONTRIBUTING.md`: contributor onboarding, local test setup, and issue/PR workflow.
- Root `ARCHITECTURE.md`: hierarchy, ownership, public modules, crate boundaries.
- Root `AGENTS.md`: operating rules/validation baseline.
- Root `SKILLS.md`: compact skill catalog.
- Crate `README.md`: downstream workflows.
- Crate `ARCHITECTURE.md`: private modules/runtime ownership.
- Crate `AGENTS.md`: crate rules/exact gates.
- Focused docs/specs: one exact public contract/behavior details.

Public-surface, module-boundary, lifecycle-promise, or validation-command changes
require owning-doc and behavioral-test updates. Historical refactor teaching is
never current authority.

Select workflows explicitly; presence does not activate them. Preserve scope
restrictions and approved workspace coordination. Consolidation must retain every
rule, prohibition, bound and approved exception explicitly in its owner; remove
repetition, never obligations. The ledger separates active guidance, history and
templates. Historical results prove only recorded source/invocation, never a
current pass or standing gate waiver.

## Primary Deterministic CI

`bash scripts/check_ci.sh` runs the direct public-trait mock lane.
`bash scripts/check_mock_contracts.sh` runs its seven behavioral targets;
`--crate NAME` reports scoped evidence only. Follow
[the mock test contract](docs/mock-testing.md) for inventory, log, feature,
compile-only, and runtime-independence requirements. Preserve all existing
crate and workspace release gates. Production bugs remain enabled required
regressions and are recorded in [the bug ledger](docs/mock-testing-production-bugs.md).
Document any missing committed regression coverage explicitly; supplemental
local probes do not satisfy this requirement. Production freezes remain binding
unless the user authorizes a separate fix. Historical receipts and scoped passes
never replace a required failing or incomplete gate.
