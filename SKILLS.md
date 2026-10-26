# ai-kit Skills

Repository skills live under `.agents/skills/`. Project-scoped custom agents
live under `.codex/agents/`.

## Public Architecture

All crate-specific skills must preserve the reduced architecture:

```text
Provider -> Model -> Session
Database
StateStore -> StateTransaction
Toolset -> ToolHandler
Agent -> Workspace -> Conversation
```

The eleven lower-crate backend traits and App's five `workflow` traits
(`ActionBackend`, `ConditionBackend`, `StateTyping`, `LifecycleHooks`, and
`ChildExecutor`) are the only public behavioral contracts. Async methods use
RPITIT except for App's exact `ChildExecutor` associated-future contract.
App's process-global serialization registry may privately erase field/tag
codec functions only; no runtime or lower-backend value enters it.
ToolHandler enters through its Toolset associated type, and public
`from_backend` exists only on Toolset. Provider, Storage, and Agent traits use
direct static implementations and associated children;
skills must not prescribe a public factory/conformance
framework, LoopDriver, hosted-tools dispatcher, public KnowledgeBase backend,
storage tool adapter, portable realtime, direct child ingress, or
compatibility path.

## Workflow Skills

- `ai-kit-audit`: scoped read-only findings before implementation.
- `orchestrate-workflow`: isolated disjoint worker ownership and integration.
- `ai-kit-change-workflow`: plan, implementation, and validation sequencing.
- `ai-kit-refactor-plan`: decision-complete reductive refactor planning.
- `ai-kit-resolve-review-findings`: plan fixes for accepted built-in `/review`
  findings, with tests-first ordering, owning repairs, docs, and exact gates;
  implementation runs through the separate change workflow.
- `ai-kit-delete-first-pass`: deletion-focused follow-up cleanup.
- `ai-kit-net-reduction-check`: net reduction and bloat measurement.
- `ai-kit-diff-optimization-audit`: diff-level redundancy and regression
  review.
- `ai-kit-validation-matrix`: primary deterministic CI, scoped evidence, and
  separate workspace/crate release and strict-live sequencing.
- `audit-workspace-for-shims` / `remove-workspace-shims`: detect and delete
  compatibility or synthetic behavior.
- `audit-workspace-test-coverage` / `expand-workspace-test-coverage`:
  direct public-trait and real-consumer proof analysis, separate live coverage,
  and scoped expansion.
- `refresh-workspace-agentic-documentation`: root/crate docs, skill/agent
  guidance and metadata, ledger, and docs-contract refresh.
- `refresh-workspace-rustdocs`: focused canonical workflow rustdoc and
  doctest refresh.
- `vendor-refresh-campaign` / `vendor-release-update`: pinned vendor source
  maintenance.

## Contract Skills

- `ai-kit-public-surface-contract`: tight functional public modules, opaque
  supplied handles, intentional passive DTOs, associated child types, one
  erased Tool ingress, direct static Provider/Storage/Agent traits, private enum
  dispatch, and cross-crate ownership.
- `provider-sdk-change-contract`: Provider/Model/Session ownership, normalized
  provider messages and Model-associated function continuation, portable hosted-tool values,
  model/session lifecycle, and provider validation.
- `storage-sdk-change-contract`: reduced honest Database operations,
  StateStore/StateTransaction ownership, actor correctness, feature lanes, and
  storage validation.
- `tool-sdk-change-contract`: Toolset/ToolHandler behavior, direct Local/MCP
  construction, independent MCP server ownership, interaction state, and tool
  validation.
- `agent-sdk-change-contract`: Agent/Workspace/Conversation behavior, static
  Provider/Storage/Toolset composition with associated, non-Clone handlers,
  private managed knowledge, prompt-local associated continuation, one Custom
  unary/streaming prompt owner, durable provider-neutral state, notifications, and
  backend validation.

## Research Skills

- `provider-sdk-research`: official OpenAI, Anthropic, Gemini, and Ollama
  behavior, model support, continuation metadata, and strict-live facts.
- `storage-sdk-research`: official/vendored LanceDB, SurrealDB, Qdrant, Redb,
  SQLite/Turso, PostgreSQL, transaction, feature, and strict-live facts.
- `tool-sdk-research`: MCP client/server protocol, Local/MCP portability, and
  runtime ownership facts.
- `agent-sdk-research`: pinned Codex App Server and OpenCode protocol plus
  strict-live behavior.
- `app-sdk-research`: app workflow semantics, the checksum-fixed rurr snapshot,
  lower-crate composition contracts, and current official graph sources.

Research distinguishes sourced facts from implementation inference. It never
uses an existing adapter as proof of dependency behavior.

## Rust Practice Skills

- `use-rust-best-practices-core`: baseline Rust and Cargo hygiene.
- `use-rust-best-practices-architecture`: functional module/crate ownership
  and enum-dispatch placement.
- `use-rust-best-practices-abstractions`: concrete-first simplification,
  public RPITIT/private erasure at the approved Tool seam, direct static
  Provider/Storage/Agent traits, and handwritten exhaustive dispatch.
- `rust-code-structuring-best-practices`: domain structs, data-carrying enums,
  receiver methods, associated construction, and elimination of loose
  helpers.
- `use-rust-best-practices-async`: task/thread ownership, bounded work,
  cancellation, terminal publication, and graceful cleanup.
- `use-rust-best-practices-testing`: compile, direct public-trait mocks, real
  consumer composition, protocol, actor, and separate strict-live proof placement.
- `use-rust-best-practices-networking`: clients, transports, timeouts,
  backpressure, and connection ownership.
- `use-rust-best-practices-dependencies-platforms`: dependency, feature,
  security, and portability hygiene.

Workspace Rust policy bans item type aliases, unsafe code, generated dispatch,
compatibility shims, and macro-based reduction. `tool_sdk::tool!` remains the
only narrow ergonomic macro exception.

## Documentation and Validation Skills

- `rust-public-api-docs`: focused public rustdoc and doctestable examples.
- `strict-live-audit`: real-service test topology, environment contracts, and
  docs/script alignment.

Primary tests use ordinary crate-local struct and exhaustive enum mocks of all
16 public behavioral traits and exercise the real Custom Agent/App. Follow
[the mock test contract](docs/mock-testing.md); this lane does not prove supplied
backend compatibility or native durability. Skills must not recreate a public
conformance API, production test fixture, callback framework, or shared
conformance crate.

## Custom Agents

- `provider-docs-researcher`: provider behavior and portability research.
- `agent-server-researcher`: Codex App Server and OpenCode protocol research.
- `strict-test-reviewer`: strict-live environment, topology, and proof review,
  preserving separation from primary deterministic evidence.

## Usage

Load the narrowest relevant crate research/change-contract skill plus Rust
core. Add async, testing, abstraction, architecture, structuring, networking,
or dependency skills when that concern is material.

Public-surface changes always pair with `ai-kit-public-surface-contract`.
App changes load `app-sdk/AGENTS.md`, its owning specifications, and
`ai-kit-public-surface-contract`; add `app-sdk-research` when workflow or
external graph semantics matter. Protocol/backend claims always pair with the
owning research skill. When
context is interrupted, reload the current governing docs and applicable
skills before resuming.

`tmp/**` remains immutable historical context.
