# storage-kit StateStore Research

Use this file when the selected `storage-kit-research` scope is `state stores`
or `both`.

## Current Boundary

- `StateStore` is the crate-provided closed built-in family. Caller-owned
  stores implement `StateStoreBackend` directly and retain their static type.
- Embedded Lance research covers the unconditional default StateStore,
  including its separate memory and directory configurations. Its transactional
  control-plane table is distinct from LanceDB Database knowledge collections.
- Turso research is for the SQLite StateStore path only, not the
  `storage_kit::Database` backend family.
- Redb research is for embedded StateStore work only, including its opt-in
  in-memory StateStore path, not the `storage_kit::Database` backend family.
  Use [redb-state-stores.md](redb-state-stores.md) for detailed Redb
  in-memory, file-backed, transaction, savepoint, durability, dependency, and
  async-boundary research.
- PostgreSQL research is for the remote durable StateStore path only,
  not the `storage_kit::Database` backend family.
- StateStore is for transactional control-plane state: conversations,
  active executions, execution checkpoints, revisions, approvals, and recovery
  records.
- Built-in `StateStore::begin_transaction(&self)` returns its owned
  `StateTransaction` with `load(StateKey)`, `put(StateRecord)`,
  `delete(StateKey)`, `commit(self)`, and `rollback(self)`. A caller store
  returns its own associated transaction implementation without storage-kit
  normalization or erasure.
- StateStore operation timeout policy belongs to backend configuration and
  should be passed into backend-native controls where available. PostgreSQL
  StateStore uses `tokio-postgres` `connect_timeout`, session-level and
  transaction-local `statement_timeout` and `lock_timeout`, and
  `SERIALIZABLE` live transactions; SQLite/Turso uses configured
  `busy_timeout`. Do not wrap PostgreSQL StateStore work in
  `tokio::time::timeout`, because canceling native database futures can make
  the database outcome ambiguous.
- Database remains the data, knowledge, search, vector, and document storage
  family. Callers may separately author Toolsets that use it.
- Public API, dependency features, and production guarantees must stay grounded
  in official sources, locked Lance runtime sources and recorded platform
  evidence, local vendored Turso code for SQLite, local vendored Redb code for
  Redb StateStore work, and Docker-backed PostgreSQL strict-live proof for remote
  StateStore behavior.

## Current Architecture Constraints

- Keep StateStore as a separate crate-root built-in family, not a
  `DatabaseBackendConfig` variant or a database mode. Caller-owned store enums
  remain outside that closed built-in dispatcher.
- Lance StateStore is unconditional and the recommended default. Redb,
  PostgreSQL, and SQLite/Turso require `redb`, `postgres`, and `sqlite` features.
  The Database alternative uses `surrealdb`. Default features are empty.
- Validate explicit feature lanes and every supported combination; never use
  Storage `--all-features`. Qdrant support is temporarily removed.
- Default strict-live covers both embedded Lance products. Add backend coverage
  for direct module or shared-abstraction changes; the verifier supports
  `--backend`, `--changed-since`, and explicit `--all-backends` selection.
- Real downstream persistence and knowledge fixtures use Lance. Alternative
  native locking/admission proofs remain in Storage. Caller-defined pure trait
  probes remain valid but do not establish built-in engine behavior.
- Keep `state_stores::*`, SQLite and PostgreSQL implementation structs, Turso
  connection types, PostgreSQL client/transaction types, SQL helpers, and
  implementation managers private.
- Use a StateStore-specific error boundary; do not depend on `DatabaseKind` for
  StateStore errors.
- Do not expose backend-specific handles through StateStore builder APIs. Redb
  write transactions, Turso/SQLite transactions, PostgreSQL
  `tokio-postgres` transactions, SQL helpers, timeout behavior, and savepoints
  stay private execution details behind the opaque owned `StateTransaction`.
- Redb live transaction recovery uses one private initial savepoint before any
  table is opened. After a failed `put` or `delete`, the transaction is
  terminal: commit is rejected and the caller must roll back or drop it.
- Cover local SQLite/Turso and Redb embedded behavior with integration tests,
  source contracts, and dependency guards rather than storage strict-live
  remote-service env.
- Cover remote PostgreSQL behavior with Docker-backed strict-live StateStore
  tests. Use [postgres-state-stores.md](postgres-state-stores.md) for detailed
  PostgreSQL StateStore research.

## Required Local Sources

For embedded Lance, bind the exact native versions and registry checksums from
`Cargo.lock` and the direct dependency declarations in `storage-kit/Cargo.toml`.
Inspect those matching Cargo registry sources, including `lance`, `lance-table`,
`lance-io`, `object_store` and the Unix `rustix` dependency. The LanceDB vendor
archive alone does not supply all native StateStore source authority. Prioritize
Dataset snapshot/merge/commit behavior, manifest publication, ObjectStore
conditional writes, native format and file reclamation, and filesystem locking
and synchronization used by the adapter.

Read `storage-kit/src/state_stores/lance/`, `storage-kit/ARCHITECTURE.md` and
`storage-kit/docs/PORTABLE_API.md` for the existing adapter and its owning
contract. The prototype/evidence ledger and qualification reports preserve
recorded proof and limitations; they neither authorize a rerun nor replace the
current source binding.

Inspect the other active StateStore vendor versions from:

- `storage-kit/vendor/vendor-lock.toml`
- `storage-kit/vendor/vendors.toml`
- `storage-kit/vendor/turso-*/`
- `storage-kit/vendor/redb-*/`
- `storage-kit/vendor/redb-*.zip`

For Turso, prioritize these vendored paths:

- `bindings/rust/src`
- `bindings/rust/tests`
- `bindings/rust/examples`
- `bindings/rust/Cargo.toml`
- `core`
- `sync`
- `docs`
- `CHANGELOG.md`
- `Cargo.toml`

For Redb, prioritize these vendored paths:

- `README.md`
- `Cargo.toml`
- `CHANGELOG.md`
- `docs/design.md`
- `src/db.rs`
- `src/transactions.rs`
- `src/backends.rs`
- `src/tree_store/page_store/backends.rs`
- `src/error.rs`
- `tests/basic_tests.rs`
- `tests/integration_tests.rs`
- `tests/multithreading_tests.rs`
- `tests/backward_compatibility.rs`
- `src`
- `tests`

Treat the vendored source as local code truth for the version pinned by
`vendor-lock.toml`.

## Required Official Sources

For embedded Lance research, start with the [official Lance repository](https://github.com/lance-format/lance)
and [Lance documentation](https://lance.org/), then locate the exact release
sources corresponding to the locked crates. Use current documentation as
context, not as authority to silently upgrade native guarantees or dependencies.
Keep native transaction/versioning claims separate from the supplied adapter's
portable revisions, ownership, filesystem durability and qualified platforms.

For SQLite StateStore research, start with official Turso sources:

- [Turso docs introduction](https://docs.turso.tech/introduction)
- [Turso docs index](https://docs.turso.tech/llms.txt)
- [Turso Rust quickstart](https://docs.turso.tech/sdk/rust/quickstart)
- [Turso Rust reference](https://docs.turso.tech/sdk/rust/reference)
- [Turso GitHub repository](https://github.com/tursodatabase/turso)
- [Turso stable releases](https://github.com/tursodatabase/turso/releases)

Use stable release tags only for dependency updates; ignore prerelease
tags unless the user explicitly asks for prerelease research.

For PostgreSQL StateStore research, read
[postgres-state-stores.md](postgres-state-stores.md), then check the official
PostgreSQL and `tokio-postgres` sources linked there.

For Redb StateStore research, use the active vendored Redb source and
[redb-state-stores.md](redb-state-stores.md). That reference owns detailed
docs.rs and GitHub source routing, including `InMemoryBackend`, `FileBackend`,
`Builder`, `Database`, `ReadableDatabase`, `WriteTransaction`,
`ReadTransaction`, `Savepoint`, `Durability`, and `StorageBackend`.
[Redb stable releases](https://github.com/cberner/redb/releases) remain the
release source. Stable release tracking uses semver tags with the configured
`v` prefix and excludes prerelease tags unless the user explicitly asks for
prerelease research.

## Research Focus

For embedded Lance StateStore changes, check:

- immutable snapshot identity, exact next-version fencing and conditional
  publication, including conflict versus ambiguous finalization
- permanent tombstones, revision increments, read-your-writes and list ordering
- memory versus directory ownership, worker admission, observer loss and joined
  finalization; native filesystem waits are not cancellation guarantees
- pinned format, native executor ownership, filesystem synchronization and
  exclusive lease protection during reclamation
- cold whole-directory backup/reopen and the actual macOS/Linux proof scope;
  no public backup, import, retention or maintenance API is implied
- local real-engine, supervised-process and platform evidence distinct from
  remote-service, performance or capacity proof; preserve withdrawn fault work

For SQLite StateStore changes, check:

- local embedded operation and file-path behavior
- async Rust APIs, connection behavior, prepared statements, and transactions
- revision computation inside live transactions
- SQLite compatibility claims and documented limitations
- beta or production-data caveats
- sync feature boundaries and why sync stays out unless explicitly planned
- dependency feature risk, default features, C toolchain assumptions, and
  cross-platform support
- validation implications for source contracts, dependency guards, and local
  integration tests

For PostgreSQL StateStore changes, check:

- owned async client behavior with `tokio-postgres`
- transaction, rollback, timeout, and connection-task behavior
- row-lock semantics and backend-level contention behavior
- `INSERT ... ON CONFLICT ... RETURNING` behavior for tombstone and revision
  writes
- Docker-backed strict-live service setup and required env
- Cargo feature policy: PostgreSQL requires `postgres`, SQLite requires
  `sqlite`, and Lance remains unconditional; validate the explicit supported
  feature combinations
- source-contract guards preventing Postgres driver types or SQL surface from
  leaking through the public StateStore API

For Redb StateStore changes, check:

- in-memory and file-backed backend APIs
- transaction, rollback, savepoint, durability, and read concurrency behavior
- file-format and upgrade notes
- synchronous API behavior and async-boundary implications before implementing
  async `StateStore` methods; the current target is private worker threads
  with bounded command queues and typed replies, not `spawn_blocking`,
  `block_on`, `block_in_place`, `Semaphore`, or direct Redb calls on Tokio
  worker threads
- dependency feature risk, default features, MSRV, and cross-platform support
- validation implications for source contracts, dependency guards, and local
  integration tests

## Output Requirements

State-store research output must include:

- selected scope: `state stores` or `both`
- locked Lance/native dependency versions, registry identities, source paths
  and relevant platform receipts when embedded Lance is in scope
- local vendored Turso version and paths inspected when SQLite/Turso is in
  scope
- local vendored Redb version and paths inspected when Redb is in scope
- official Lance documentation and matching native release sources consulted
  when embedded Lance is in scope
- official Turso docs or GitHub pages consulted when SQLite/Turso is in scope
- official PostgreSQL docs plus `tokio-postgres` docs
  consulted when PostgreSQL is in scope
- sourced facts
- implementation inference
- validation impact
- unresolved risks or behavior that still needs live/local proof

## Integrated Lance dependency boundary

The current supplied Database and StateStore share LanceDB 0.40.0 and the exact
Lance/lance-io/lance-table 13.0.0 family, using ObjectStore 0.14.2. Crates.io's
stable release identities were checked on 2026-10-23; prerelease tags are not
selected. Keep defaults disabled. The sole enabled LanceDB `remote` feature is
an approved workaround for 0.40.0's unconditional job decoder referring to the
feature-gated `Error::Http`. It does not authorize remote configuration or the
`aws`, `gcs`, `azure` and `oss` features. Verify filesystem paths stay local before
native URI dispatch. Preserve StateStore schema 1, file format 2.0 and
disabled stable row IDs. Only the direct Lance 13 StateStore is in scope; do not
add an older-engine implementation, cross-version harness or support promise.
Current-engine cold reopen, exact revisions/tombstones, namespace isolation and
retained-reference cleanup require executable proof. Use the current supported
native CommitHandler, ObjectStore and cleanup APIs: the local/memory resolver's
version absence is definitive, and custom-handler barrier errors remain errors.
Explicitly disable CommitBuilder's native timeout so it cannot drop admitted
finalization; the existing StateStore owner retains completion and reconciliation
authority. The positive-timeout versus disabled-timeout native control establishes
that cancellation policy, not a reproduction of the default 1,800-second expiry.
Caller admission, scenario and cleanup deadlines remain unchanged. Native cleanup
retains reference/manifest authority, and
the StateStore's owned-file boundary must protect unknown payloads. Native 13
already scans all data with `delete_unverified`; only transaction/deletion
listings still need the existing timestamp-independent orphan supplement.
Readable manifests alone do not prove safe mutation or maintenance.
