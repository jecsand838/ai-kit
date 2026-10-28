---
name: storage-kit-research
description: "Research storage-kit databases, state stores, or both against authoritative docs plus vendored/runtime sources. Covers LanceDB and SurrealDB Database behavior, plus embedded Lance, SQLite/Turso, Redb, and PostgreSQL StateStore behavior. Default to researching both unless the user clearly scopes the request."
---

# storage-kit Research

## Purpose

Ground `storage-kit` changes in authoritative database-backend and StateStore
behavior before portable APIs, capability reporting, dependency choices, or
strict-live expectations move.

## Trigger Boundary

Use this skill when `storage-kit` work depends on database facts, state-store
facts, remote-vs-embedded semantics, dependency behavior, vendor drift, or
strict-live assumptions.

## Scope Routing

Choose one research scope before collecting sources:

- `databases`: use when the user names `Database`, `Database::connect`,
  LanceDB, LanceDB default location, LanceDB `memory://`, SurrealDB,
  backend modes, retained query/catalog/data/index behavior, database
  capabilities, or strict-live database behavior.
- `state stores`: use when the user names `StateStore`, state store,
  `StateStore::begin_transaction`, `StateTransaction::load`,
  `StateTransaction::put`, `StateTransaction::delete`, `StateTransaction::commit`,
  `StateTransaction::rollback`, live transactions, execution checkpointing,
  active executions, revisions, transactional state, SQLite, Turso, Redb,
  embedded Lance StateStore, embedded StateStore, in-memory StateStore,
  persisted Redb StateStore, ACID,
  savepoints, durability, PostgreSQL, Postgres, `tokio-postgres`, owned clients,
  Docker strict-live proof, or StateStore feature policy.
- `both`: use by default when the request says only `storage-kit`, storage,
  persistence, vendor drift, architecture, or when database and state-store
  concerns are mixed or unclear.

If the user explicitly scopes to databases or state stores, stay in that scope
unless the implementation would cross the other boundary.
For StateStore work, keep the boundary explicit: it is transactional
control-plane state, not a `Database` backend and not a model-visible tool path.

## Prerequisites

- Read the source list in [`storage-kit/AGENTS.md`](../../../storage-kit/AGENTS.md).
- Pair with `storage-kit-change-contract` when the task also changes code,
  tests, docs, or validation.
- Read [references/state-stores.md](references/state-stores.md) whenever the
  selected scope is `state stores` or `both`.
- Read [references/redb-state-stores.md](references/redb-state-stores.md)
  when the selected StateStore implementation is Redb, including its opt-in
  in-memory or persisted paths, transactions, savepoints and durability.
  Generic embedded, in-memory or no-`sqlite` StateStore work does not select
  Redb; Lance is the unconditional default.
- Read [references/postgres-state-stores.md](references/postgres-state-stores.md)
  whenever PostgreSQL StateStore, owned Postgres clients,
  `tokio-postgres`, live transactions, atomic upserts, timeouts, or Postgres
  strict-live proof is in scope.
- Read [references/strict-live.md](references/strict-live.md) whenever database
  strict-live behavior or validation claims are in scope.

## Workflow

1. State the selected scope: `databases`, `state stores`, or `both`.
2. For database research, check LanceDB and SurrealDB behavior against
   official docs, vendored sources, runtime sources, and strict-live proof.
   Promote behavior to the portable Database surface only when at least two
   implementation families can preserve the same semantics. Single-backend
   realtime, administration, query, tracing, or optimizer operations remain
   private rather than becoming portable request families.
3. For embedded Lance StateStore research, bind native versions from
   `Cargo.lock` and `storage-kit/Cargo.toml`, inspect the matching Cargo registry
   sources and the existing Lance StateStore owner, and follow the Lance source
   routing in [references/state-stores.md](references/state-stores.md). Separate
   native Dataset guarantees from the adapter's transaction, durability,
   reclamation and owned-worker obligations and their recorded platform proof.
4. For SQLite StateStore research, inspect the active vendored Turso source
   under `storage-kit/vendor/turso-*` and official Turso docs or GitHub
   sources.
5. For Redb StateStore research, inspect the active vendored Redb source under
   `storage-kit/vendor/redb-*` or the pinned zip, then check official Redb
   docs.rs pages and GitHub sources before recommending in-memory, file-backed,
   transaction, savepoint, durability, or async-boundary behavior.
   The current target is a private worker-thread async boundary for Redb's
   synchronous API, with bounded command queues and typed replies; do not
   recommend `spawn_blocking`, `block_on`, `block_in_place`, `Semaphore`, or
   direct Redb calls on Tokio worker threads unless a new sourced Redb/Tokio
   fact invalidates that target and the plan is explicitly revised.
6. For PostgreSQL StateStore research, check official PostgreSQL docs plus
   `tokio-postgres` docs before recommending transactions, owned-client
   shape, locking, timeouts, atomic upserts, or dependency features.
7. Confirm whether the behavior belongs on the portable surface or must stay
   backend-specific.
8. For public StateStore transaction construction, use owned static types. The
   built-in `StateStore::begin_transaction(&self)` returns the closed built-in
   `StateTransaction`. A caller-owned store returns its own associated
   `StateStoreBackend::Transaction`; storage-kit does not normalize or erase
   it. Transactions expose `load(StateKey)`, `put(StateRecord)`,
   `delete(StateKey)`, `commit(self)`, and `rollback(self)`. Do not recommend
   backend-specific transaction exposure.
9. Check whether database behavior is honest across LanceDB and SurrealDB,
   and whether StateStore behavior is grounded in embedded Lance
   source and platform evidence, Turso-backed local persistence facts, Redb
   embedded facts, or Postgres strict-live facts.
10. Identify strict-live env, embedded-path, remote endpoint, state-file,
   dependency-feature, or mode fallout. Validate explicit supported feature
   lanes; never use Storage `--all-features`. Qdrant support is temporarily
   removed and is not part of the current research or validation matrix.
11. Classify uncertain behavior explicitly as sourced fact or implementation
   inference.
12. Use strict-live reproducibility to decide whether database or remote
   StateStore behavior may stay on the portable surface.
13. Choose `UnsupportedCapability` vs `NotYetImplemented` only after backend-doc
   research plus strict-live confirmation.
14. Reject silent portability degradation, temporary shims, synthetic responses,
   macro shims, synthetic local abstractions, and non-live-only assumptions.
15. Feed the confirmed behavior into concrete structs, closed enums, methods,
   owning docs, tests, and public API contracts.
16. Avoid macro-based abstraction as a substitute for modeling backend or
   state-store behavior and mode differences directly.
17. Reject public factory, conformance, builder, tool-adapter, realtime,
   child-ingress, raw trace, and backend-debug replacements. The reduction is
   intentional and has no compatibility period.

## Output Rules

- Start with selected scope and sources consulted.
- Separate sourced facts from implementation inference.
- Include validation impact and whether strict-live, source-contract, docs, or
  dependency guards must move.
- Prefer crate-root portable behavior over backend-private leakage.
- Behavior that is not reproducible in strict-live is not a valid portable
  success contract.
- Embedded Lance claims require the matching native-source, public-API,
  durability and platform evidence; historical receipts retain their original
  source and execution scope. Native versioning alone does not prove the
  supplied transaction or filesystem-durability contract.
- Redb embedded StateStore behavior is proved by local integration tests,
  source contracts, dependency guards, and feature-lane compilation, not by a
  remote strict-live service.
- Research recommendations should lead to struct-and-method designs, not macro
  helpers, compatibility layers, or synthetic local behavior.

## References

- [`storage-kit/AGENTS.md`](../../../storage-kit/AGENTS.md)
- [references/strict-live.md](references/strict-live.md)
- [references/state-stores.md](references/state-stores.md)
- [references/redb-state-stores.md](references/redb-state-stores.md)
- [references/postgres-state-stores.md](references/postgres-state-stores.md)
- [LanceDB releases](https://github.com/lancedb/lancedb/releases)
- [SurrealDB releases](https://github.com/surrealdb/surrealdb/releases)
- [Turso docs introduction](https://docs.turso.tech/introduction)
- [Turso releases](https://github.com/tursodatabase/turso/releases)
- [Turso Rust client docs](https://docs.turso.tech/sdk/rust/quickstart)
- [Turso GitHub](https://github.com/tursodatabase/turso)
- [Redb docs.rs](https://docs.rs/redb/latest/redb/)
- [Redb GitHub](https://github.com/cberner/redb)
- [Redb releases](https://github.com/cberner/redb/releases)

Bind the actual target feature graph and registry versions when researching
dependency behavior. External Rust dependencies must resolve from crates.io;
Cargo patches, replacements and external path overrides are forbidden.
[Storage's feature guidance](../../../storage-kit/README.md#features) records
supported backend lanes. [The dependency security policy](../../../docs/security-dependencies.md)
owns the explicit temporary advisory exceptions, preservation of raw findings,
and requirement to remove each exception when compatible upstream fixes become
available. Acceptance of an advisory is not proof of remediation or backend
behavior. Historical Qdrant sources and results do not authorize restoring the
removed feature or its dependency constraints.
