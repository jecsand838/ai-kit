---
name: storage-sdk-change-contract
description: "Apply storage-sdk crate rules, tight portable boundaries, actor ownership, documentation triggers, and validation expectations for Database and StateStore work."
---

# storage-sdk Change Contract

## Trigger

Use this skill for storage-sdk production, public API, tests, docs, feature,
backend, transaction, or validation changes.

## Prerequisites

- Read [`storage-sdk/AGENTS.md`](../../../storage-sdk/AGENTS.md).
- Read [`storage-sdk/ARCHITECTURE.md`](../../../storage-sdk/ARCHITECTURE.md)
  when public shape or module ownership changes.
- Load `storage-sdk-research` for backend facts.
- Load `ai-kit-public-surface-contract` for visibility/reexport work.
- Load the Rust core, async, testing, abstraction, architecture, and
  structuring skills.

## Public Boundary

1. Keep exactly three public behavioral traits:
   `DatabaseBackend`, `StateStoreBackend`, and
   `StateTransactionBackend`.
2. `StateStoreBackend` owns an associated
   `Transaction: StateTransactionBackend`.
3. `Database`, `StateStore`, and `StateTransaction` are the closed built-in
   family. Callers implement the public traits directly on their own concrete
   types or enums; storage-sdk provides no backend-ingress constructor.
4. The built-in handles, their private dispatcher enums, and every concrete
   built-in implement the corresponding trait. A caller-owned StateStore enum
   names its own associated transaction enum.
5. Private dispatchers contain only concrete built-in variants. Reject
   `Extension` arms, erased adapters, backend identity/provenance values,
   one-field wrappers, and duplicate behavioral traits.
6. Keep primary handles, traits, and errors at the crate root. Put supporting
   types under `database::{config,catalog,data,query,index}` and
   `state::{config,record,transaction}`.
7. Do not expose factories, production conformance suites, builders, public
   trait objects, registries, aliases, compatibility surfaces, generated
   dispatch, raw backend handles, SQL, transport payloads, or actor internals.

## Database Rules

- Built-ins use `Database::connect(DatabaseConfig)`.
- `DatabaseConfig` is one enum carrying Lance, Surreal, or feature-gated
  Qdrant configuration.
- Retain only catalog, data, query, and index behavior with honest semantics
  in at least two implementation families.
- Schema evolution belongs to catalog alteration. Delete standalone admin,
  realtime, namespace administration, alias, single-family query/admin,
  snapshot/import/version/optimizer/statistics, and raw trace/debug surfaces.
- One reduced `CapabilitySet` describes optional retained operations. Calls
  remain authoritative.
- Unsupported behavior fails before mutation; never synthesize ids, counts,
  scores, acknowledgements, plans, or success.
- Storage owns no model-visible tool adapter or tool-sdk dependency.

## StateStore Rules

- Built-ins use `StateStore::connect(StateStoreConfig)`.
- `StateStoreConfig` is one enum carrying Lance, Redb, PostgreSQL, or feature-gated
  SQLite/Turso configuration.
- `StateTransaction` is lifetime-free, non-Clone, and consumed by commit or
  rollback. Commit returns unit.
- Preserve read-your-writes, tombstones, atomic commit, typed revisions,
  pagination, and exact `TransactionConflict`.
- Preserve bounded channels, terminal authority, rollback-on-drop, caller-loss
  cleanup, and deterministic task/thread joins.
- Redb remains an owned OS-thread boundary with FIFO logical transaction
  admission. PostgreSQL/SQLite remain dedicated-connection transaction actors.
- Do not flatten distinct backend cancellation/admission rules into a generic
  lifecycle framework. Only identical task-join bookkeeping may be shared.

## Features And Dependencies

- Lance is always compiled for both products; `default = []`.
- Alternatives require explicit `redb`, `postgres`, `surrealdb`, `sqlite`, or
  `qdrant` features. Reject SQLite/Qdrant together. Never use Storage
  `--all-features`, a `state-store` umbrella, or the deleted `toolsets` feature.
- Preserve unconditional configuration DTOs and return existing `InvalidConfig`
  before I/O for disabled native construction. Keep existing SQLite/Qdrant
  conditional public availability. Downstream real persistence uses Lance.
- Storage secrets remain validated storage-sdk values and never cross into
  provider-sdk.

## Tests And Docs

- Production `src/**` remains test-free.
- Ordinary private test helpers replace public conformance owners.
- Downstream tests prove direct trait implementations and associated
  transactions without normalization.
- Source contracts protect stable public/privacy/dispatcher boundaries and
  deleted symbols, not helper names or private file topology.
- Strict-live alone proves real remote services; deterministic fixtures never
  substitute.
- Update AGENTS, architecture, README, capability/testing guides, scripts,
  source contracts, and env templates together when their contract moves.

## Output

- Prefer deletion and receiver-owned domain modeling.
- Preserve explicit exhaustive dispatcher matches and receiver syntax.
- Report only validation actually run.
- Do not invent a reduced substitute for a required gate.

## References

- [`storage-sdk/AGENTS.md`](../../../storage-sdk/AGENTS.md)
- [`storage-sdk/ARCHITECTURE.md`](../../../storage-sdk/ARCHITECTURE.md)
- [`storage-sdk/docs/testing.md`](../../../storage-sdk/docs/testing.md)
