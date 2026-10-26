# PostgreSQL StateStore Research

Use this file when `storage-sdk-research` scope is `state stores` or `both`
and the work involves PostgreSQL, Postgres, owned clients, live transactions,
atomic upserts, timeouts, or strict-live proof for remote durable StateStore
behavior.

## Boundary

- PostgreSQL StateStore is the remote durable transactional state-store path.
- PostgreSQL is not a `Database` backend and must not reuse
  `DatabaseConfig`, `DatabaseKind`, `DatabaseBackend`, `DatabaseManager`, or
  `StorageError`.
- PostgreSQL StateStore requires the explicit `postgres` feature. Lance is
  unconditional and used by downstream real persistence fixtures.
- Optional implementations use `redb`, `postgres`, `sqlite`, `surrealdb`,
  and `qdrant`; Lance is always compiled.
- The `sqlite`/`qdrant` split is temporary. It exists because compiling both
  currently reaches `parking_lot`'s
  `compile_error!("the \`send_guard\` and \`deadlock_detection\` features cannot be used together")`;
  qdrant-edge enables `deadlock_detection` while Turso's SDK stack enables
  `send_guard`.
- Do not add a `state-store` umbrella feature.
- Keep `state_stores::*`, `PostgresStateStore`, `PostgresSchema`,
  `PostgresStateTransaction`, the owned client, transaction handles, SQL,
  timeout details, and driver errors must stay private.
- `StateStore::begin_transaction(&self)` is the portable write entry point.
  It returns an opaque owned `StateTransaction` backed by a private
  dedicated-connection actor and must not expose
  `tokio-postgres` transaction handles, SQL, or any other PostgreSQL execution
  detail.

## Required Official Sources

Check these before recommending PostgreSQL StateStore behavior:

- [`tokio-postgres` crate docs](https://docs.rs/tokio-postgres/latest/tokio_postgres/)
- [`tokio-postgres::Client`](https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Client.html)
- [`tokio-postgres::Transaction`](https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Transaction.html)
- [PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
- [PostgreSQL explicit locks](https://www.postgresql.org/docs/current/explicit-locking.html)
- [PostgreSQL `INSERT`](https://www.postgresql.org/docs/current/sql-insert.html)
- [Postgres Docker Official Image](https://hub.docker.com/_/postgres/)

## Dependency Guidance

Use the direct `tokio-postgres` dependency for one primary client per built
StateStore plus one dedicated client/driver per owned transaction actor:

```toml
tokio-postgres = { version = "0.7.18", default-features = false, features = ["runtime"], optional = true }
```

[tokio-postgres 0.7.18](https://docs.rs/crate/tokio-postgres/0.7.18) requires
`postgres-protocol` 0.6.12 and `postgres-types` 0.2.14 as its minimum versions.
[Protocol 0.6.12](https://docs.rs/crate/postgres-protocol/0.6.12) rejects SCRAM
server challenges above 100,000 iterations before password derivation.
[PostgreSQL's default](https://www.postgresql.org/docs/18/runtime-config-connection.html#GUC-SCRAM-ITERATIONS)
is 4096; the count belongs to the role's stored password verifier, so changing
`scram_iterations` alone does not update existing passwords. Authentication
failure remains an existing redacted backend-operation error before the owned
driver starts. Do not add a cap override, alter caller credentials, or weaken
server authentication to bypass this constraint.

Backend constraints:

- Use `tokio_postgres::NoTls` only in the first PostgreSQL backend.
- Keep the primary `tokio_postgres::Client` and its connection future owned by
  `PostgresStateStore` for shared loads and lists. Retain validated private
  connection settings so each transaction actor can open and drive its own
  dedicated client/connection pair.
- Fail fast during `connect` by connecting the client and initializing schema.
- Set `tokio-postgres` `Config::connect_timeout` from the StateStore
  operation timeout before connecting.
- Configure session-level timeouts with parameterized
  `SELECT set_config('statement_timeout', $1, false)` and
  `SELECT set_config('lock_timeout', $1, false)` before schema initialization,
  direct loads, and list scans.
- On each actor-owned dedicated connection, start live read-write work with
  `BEGIN ISOLATION LEVEL SERIALIZABLE`; no lifetime-bearing native transaction
  may escape the actor.
- Configure transaction-local timeouts with parameterized
  `SELECT set_config('statement_timeout', $1, true)` and
  `SELECT set_config('lock_timeout', $1, true)`.
- Do not use SQLx, Diesel, `postgres` sync client, custom pools, query macros,
  migration frameworks, `tokio-postgres-rustls`, `native-tls`,
  `postgres-openssl`, or `tokio-postgres/with-serde_json-1` in the initial
  implementation.

## Implementation Research Checklist

Confirm these details before implementation or review:

- `PostgresStateStore` owns a long-lived primary `tokio_postgres::Client`, its
  connection task handle, validated connection settings, and operation
  timeout. Built `StateStore` handles do not expose backend identity; backend
  choice is visible only in construction config.
- Shared loads/lists use the primary client. Each transaction start opens a
  dedicated client whose actor owns transaction operations and finalization.
  PostgreSQL StateStore work must not be wrapped in `tokio::time::timeout`,
  because canceling native database futures can make the database outcome
  ambiguous. Once commit is admitted, waiter cancellation does not cancel the
  actor; a lost post-commit response is reconciled through reopened revisioned
  state instead of a blind retry.
- `PostgresSchema` owns private schema creation for a state-record table.
- Store JSON values as `TEXT`, not `jsonb`, because StateStore is not a JSON
  query API and SQLite/Turso already stores serialized JSON text.
- `PostgresStateTransaction` owns live transaction read and write behavior,
  accumulated mutation outcomes, failure state, commit, and rollback.
- Portable writes run inside a real PostgreSQL `SERIALIZABLE` transaction
  opened by the private actor after `StateStore::begin_transaction(&self)`.
- Configure session-level and transaction-local `statement_timeout` and
  `lock_timeout` when proving timeout behavior, including commit and rollback
  finalization behavior.
- Use database-side atomic revision increments in parameterized
  `INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING` statements for both puts
  and tombstones.
- Keep all SQL parameterized. Do not interpolate user state, keys, namespace,
  JSON, or limits into SQL strings.
- Map driver, timeout, serialization, uniqueness, and server errors into
  `StateStoreError`; do not expose `tokio_postgres` error types publicly.

## Strict-Live Research Checklist

PostgreSQL StateStore behavior must be proved against a real Docker Postgres
service, not mocks or synthetic paths.

Check or plan:

- `storage-sdk/docker-compose.strict.yml` includes a pinned Postgres service.
- `storage-sdk/strict-tests.env` defines required Postgres host, port,
  database, username, and password.
- `storage-sdk/scripts/verify_strict_live.sh` starts only selected alternative services in an invocation-owned project;
  PostgreSQL does not imply SurrealDB or Qdrant. Exit cleanup removes that
  invocation's named volumes with
  `docker compose down --volumes`; cleanup authority begins only after
  environment and deterministic validation succeed, immediately before the
  first service mutation. Preserve the primary gate failure and report cleanup
  failures separately; cleanup failure also fails an otherwise successful gate.
- The default gate is Lance-only; select PostgreSQL with
  `bash storage-sdk/scripts/verify_strict_live.sh --backend postgres`.
  Focused PostgreSQL filters do not prove the complete live regression set.
- Bare tests require already-running configured services. Follow the
  [owning testing guide](../../../../storage-sdk/docs/testing.md#strict-live)
  for wrapper readiness bounds and Compose project, port and embedded-path
  isolation; there is no shared 60-second bare-target readiness gate.
- Source contracts must prove dedicated actor connection and task ownership.
  PostgreSQL StateStore strict-live must prove primary-client connect, schema
  initialization, two retained transaction handles, serializable live
  transaction commit, rollback and cleanup, tombstones, list pagination,
  sequential same-key revision outcomes, failed transaction behavior,
  serialization conflict behavior, and real timeout mapping through lock
  contention.

## Output Requirements

PostgreSQL StateStore research output must include:

- selected scope: `state stores` or `both`
- official PostgreSQL docs consulted
- `tokio-postgres` docs consulted
- sourced facts about transactions, locks, owned clients, and dependency
  features
- implementation inference for StateStore semantics
- validation impact for strict-live, source contracts, docs, and Cargo feature
  policy
- unresolved behavior that still needs Docker-backed proof
