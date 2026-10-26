# storage-sdk AGENTS

This file adds storage-specific rules to the workspace
[`AGENTS.md`](../AGENTS.md).

## Scope

`storage-sdk` owns two independent products:

- `Database` provides portable catalog, data, query, and index behavior over
  always-available LanceDB and opt-in SurrealDB/Qdrant.
- `StateStore -> StateTransaction` provides revisioned transactional
  control-plane state over always-available embedded Lance and opt-in
  Redb/PostgreSQL/SQLite.

Neither product exposes backend-native clients, SQL, wire payloads, storage
keys, actor state, tool runtimes, or application reconstruction policy.

## Public Architecture

- `DatabaseBackend`, `StateStoreBackend`, and `StateTransactionBackend` are
  the only public behavioral traits in this crate.
- `StateStoreBackend` has an associated `Transaction:
  StateTransactionBackend`.
- `Database`, `StateStore`, and `StateTransaction` implement their
  corresponding traits as the crate-provided built-in family. Callers may
  implement the traits directly on their own concrete types or enums.
- `DatabaseInner`, `StateStoreInner`, and `StateTransactionInner` remain
  private exhaustive enums containing only concrete built-in variants.
- Caller implementations remain caller-owned and statically dispatched. Do
  not add `from_backend`, `Extension` variants, erased adapters, public `dyn`
  backends, duplicate behavioral traits, wrappers, aliases, macros, generated
  dispatch, compatibility paths, or a registry.

## Public Modules

Keep primary handles, backend traits, and errors at the crate root. Supporting
types live under functional modules:

```text
database::{
    config,
    catalog,
    data,
    query,
    index,
}
state::{
    config,
    record,
    transaction,
}
```

Do not flatten the complete DTO inventory onto the crate root. Passive serde
DTOs may expose public fields; invariant-bearing values and runtime owners keep
private fields and validating constructors.

## Database Contract

- `Database::connect(DatabaseConfig)` is the built-in construction path.
  `DatabaseBuilder`, `DatabaseBackendConfig`, and parallel shared-config
  wrappers do not exist.
- `DatabaseConfig` is one data-carrying enum over Lance, Surreal, and
  feature-gated Qdrant configs. Backend-specific configs remain in
  `database::config`.
- `DatabaseBackend` exposes one reduced `CapabilitySet` and the retained
  catalog, data, query, and index operations. It exposes no backend identity,
  provenance, kind, label, or implementation discriminator.
  Operation calls remain authoritative when an optional capability is absent.
- Retain only portable operations with honest semantics in at least two
  implementation families:
  - create, open, list, drop, and alter collections;
  - insert, lookup, scan, update, delete, merge, atomic partition
    replacement, and count;
  - nearest, search-by-id, text, scroll, and random sample;
  - create, list, drop, and wait for indexes.
- Schema evolution is a catalog alteration. There is no standalone admin
  family.
- Do not expose realtime, namespace administration, aliases, hybrid or
  multivector search, group/recommendation/discovery/facet queries, version
  restore, consistency controls, import/export, snapshots, prewarm, optimizer
  controls, native statistics/rebuild, sparse-vector-only behavior, raw plans,
  debug traces, or generic native escape hatches.
- Backend-specific behavior that cannot satisfy the retained contract returns
  an explicit `UnsupportedCapability` or `NotYetImplemented` error before
  mutation. Never synthesize success.
- `Database` owns persistence and retrieval only. Model-visible tools are
  caller-authored in `tool-sdk`; storage-sdk has no `toolsets` feature,
  tool-sdk dependency, adapter DTO, or `Database::build_toolset`.

## StateStore Contract

- `StateStore::connect(StateStoreConfig)` is the built-in construction path.
  `StateStoreBuilder` and `StateStoreBackendConfig` do not exist.
- `StateStoreConfig` is one data-carrying enum over Lance, Redb,
  PostgreSQL plus feature-gated SQLite/Turso configuration.
- `StateStore` is a cloneable shared owner. `StateTransaction` is a unique,
  lifetime-free, non-`Clone` owner.
- `begin_transaction(&self)` returns the implementation's associated
  transaction. The built-in `StateStore` returns `StateTransaction`; a
  caller-owned store enum returns its caller-owned transaction enum.
  Transaction methods are `load`, `put`, `delete`, consuming `commit`, and
  consuming `rollback`; commit returns `Result<()>`.
- Transactional reads observe earlier writes and tombstones. Commit atomically
  applies all mutations. Exact revision conflicts remain
  `StateStoreError::TransactionConflict`.
- Retain `StateRecord` as caller write input and `StateRecordSnapshot` as the
  revision-bearing observed value. Keep typed keys, namespaces, prefixes,
  revisions, pagination, and cloneable data-owned errors.
- PostgreSQL and SQLite/Turso use private transaction actors with dedicated
  native connections. Redb uses bounded per-transaction channels and FIFO
  logical admission to one native writer.
- Preserve terminal authority, rollback-on-drop, bounded queues, deterministic
  task/thread joining, and the rule that admitted native finalization is never
  detached merely because a caller stops waiting.
- Do not consolidate backend-specific transaction actors, rollback guards,
  native sessions, or admission rules into a generic lifecycle framework.
  Only the structurally identical PostgreSQL/SQLite task-join registry may be
  shared privately.

## Backend And Secret Ownership

- Database backend resources remain under `src/databases/`; StateStore backend
  resources remain under `src/state_stores/`.
- Concrete built-ins implement the public trait in their owning subtree. Root
  built-in dispatcher matches are explicit and exhaustive and call receiver
  methods.
- Qdrant API keys and SurrealDB passwords use storage-sdk validated secret
  wrappers. Absence means no authentication; blank strings never do.
- Feature-enabled Redb uses owned OS threads
  with bounded channels for its synchronous engine. Do not use
  `spawn_blocking`, `block_on`, `block_in_place`, semaphores, or direct Redb
  calls on Tokio workers.
- Feature-enabled PostgreSQL uses `tokio-postgres` with private
  parameterized SQL and backend-native timeouts, and does not expose clients,
  transactions, SQL, pools, or TLS internals.
- Lance is always compiled for both products; `default = []`. Alternatives use
  explicit `redb`, `postgres`, `surrealdb`, `sqlite`, and `qdrant` features.
  Reject `sqlite` plus `qdrant`; never use Storage `--all-features`. Preserve
  unconditional configuration DTO availability and return existing typed
  configuration errors before I/O when the implementation is disabled.
  Do not add `toolsets` or a `state-store` umbrella feature.

The integrated local Lance family is frozen to LanceDB 0.40.0, exact
`lance`, `lance-io`, and `lance-table` 13.0.0, exact `object_store` 0.14.2,
and Unix `rustix` 1.1.4 filesystem support, with default features disabled.
Only LanceDB's `remote` feature is enabled as an approved compilation workaround:
0.40.0's unconditional job decoder refers to `Error::Http`, which that feature
gates. Do not enable `aws`, `gcs`, `azure` or `oss`, or expose native cloud
configuration. Supplied configuration accepts only local filesystem and memory
storage; resolve filesystem paths before native dispatch. The private ObjectStore,
ObjectStoreProvider and CommitHandler seams add no public behavior. Directory
qualification remains scoped to macOS and Linux.

For macOS builds with `qdrant`, retain the approved optional, exact
`webbrowser =1.2.1` constraint with its direct default features disabled. This
constrains LanceDB's transitive browser selection to avoid the Objective-C
feature combination that triggers Edge 0.6.1's native `E0275` compile failure;
SDK code does not call the added dependency. It is a registry-only compilation
workaround, not a native patch or new public capability. A shared lock may select
that version for other lanes too; review the complete graph and extracted
consumer resolution. Another dependency can independently enable the same
`objc2` features, so this constraint does not guarantee all downstream graphs
compile. Keep those limits explicit; require current graph and runtime evidence
before claiming qualification.

The supplied StateStore has one direct Lance 13 implementation. Preserve its
private schema version 1, Lance file format 2.0 and disabled stable row IDs;
do not add an older-engine implementation or compatibility harness. Prove current
Lance 13 cold reopen with exact records, revisions, tombstones and namespace
isolation through the existing public test owners. Validate all retained native
references before reclamation; unsupported overlays, foreign bases or new file
layouts fail closed. The local/memory commit handler treats absent versions as
definitive while preserving errors after landed publication. Disable native
CommitBuilder timeouts: the admitted StateStore owner must finish and reconcile
native finalization even when its caller stops waiting. This does not extend
caller admission, scenario or cleanup deadlines. No implicit native-format
migration or older-engine compatibility is promised.
Historical prototype/platform evidence remains at its original source identity;
see the [evidence ledger](docs/lancedb-state-store-prototype.md).

The exact portable operation contract lives in
[`docs/PORTABLE_API.md`](docs/PORTABLE_API.md). Keep isolation, revision,
pagination and ambiguous-finalization obligations there rather than duplicating
signature ledgers in architecture prose.

## Testing And Documentation

- Production `src/**` contains no test-only code.
- `/tests` owns public API, downstream implementation, transaction-actor,
  local backend, source-contract, and strict-live coverage.
- Public conformance owners and durable factory traits do not exist. Shared
  test cases remain private ordinary test helpers.
- Source contracts protect public modules/privacy, the three closed built-in
  dispatchers, absence of erasure/identity symbols, and backend-detail
  isolation. They do not lock helper names, private files, actor type names,
  or match spelling.
- Strict-live proves every supported Database mode and all StateStore variants,
  including Redb and SQLite/Turso memory/file modes.
  Deterministic tests prove local validation, actor ownership, and downstream
  trait implementations without claiming remote built-in behavior.

## Validation

Follow the approved tests-first implementation plan: run focused tests before
production edits, record meaningful red evidence, then require passing tests
and independent code review for each completed area. Coordinate Cargo commands
serially. The default strict verifier runs the Lance-only non-live gate and
both embedded Lance products. Alternative live coverage is explicit or selected
by direct backend/shared abstraction changes:

```bash
bash storage-sdk/scripts/check.sh
bash storage-sdk/scripts/verify_strict_live.sh
bash storage-sdk/scripts/verify_strict_live.sh --changed-since BASE
# After sourcing selected alternative prerequisites:
bash storage-sdk/scripts/verify_strict_live.sh --backend postgres
bash storage-sdk/scripts/verify_strict_live.sh --all-backends
```

The full request runs eight behavioral configurations, compiles all 24 legal
feature subsets, and proves rejection of all eight SQLite/Qdrant unions.
Downstream actual persistence and knowledge fixtures use Lance; alternative
backend-specific evidence remains owned by Storage.
The exact expanded commands belong to [`docs/testing.md`](docs/testing.md).
Update that document, this file, source contracts, env templates, and scripts
together when the matrix changes.

`check.sh` also runs `lancedb_native_feasibility`,
`lancedb_durability_feasibility`, and `lancedb_state_store` in every supported
lane. The focused embedded verifier runs those ordinary real-engine,
public-API, and supervised-process checks with `--no-default-features`.
Missing prerequisites fail instead of producing skipped success. Receipts
report scoped PASS, FAIL or INCOMPLETE execution, preserve actual command
errors, and identify source, lockfile, compiler, filesystem and target logs.
The user has accepted embedded feasibility; a verifier must not hardcode an
aggregate product qualification verdict or invent unexecuted platform results.
It does not replace the broader strict-live verifier or full feature matrix.
Production completion preserves public API/configuration availability; backend
implementation opt-ins follow the feature policy above. Acceptance uses a measured capacity
envelope, real consumer integration and ordinary release evidence. No automatic
Redb migration or public maintenance/backup surface is introduced.
The Lance orphan supplement must validate all retained references before native
history/data reclamation in both cleanup paths, refresh surviving references
after full GC, hold exclusive snapshot protection through owned joining, and
delete only supported unreferenced files through the existing durable owner.
Pinned layout/filename assumptions require qualification on dependency or
format changes; never infer general preservation of foreign files inside the
reserved native table directories.

The user explicitly withdrew the prototype's privileged fault campaign.
Do not build or run GDB/MI, ptrace, added tracing capabilities, relaxed container
security, syscall interception/modification, or equivalent workarounds for this
backend. Preserve ordinary owned-child cancellation and crash/reopen tests.
Earlier fault-harness receipts are historical records, not current gates or
proof of the supplied adapter's remaining finalization obligations.

## Research Sources

Ground backend claims in the active vendored sources and official LanceDB,
SurrealDB, Qdrant, Turso, Redb, PostgreSQL, and `tokio-postgres`
documentation. The storage research skill owns the curated source list.
Strict-live reproducibility is required before claiming a portable success
contract.

## Primary mock CI

The direct-trait targets are required by `bash scripts/check_ci.sh` and can be
scoped with `bash scripts/check_mock_contracts.sh --crate storage-sdk`. Follow
[the mock test contract](../docs/mock-testing.md). Keep these credential-free
targets separate from existing integration and strict-live verification; the
existing gates above remain required. Production regressions stay enabled and
are recorded in [the bug ledger](../docs/mock-testing-production-bugs.md).
