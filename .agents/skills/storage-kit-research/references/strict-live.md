# storage-kit Strict Live

Use this file when planning or reviewing `storage-kit` strict-live work.

## Canonical Commands

Run from the workspace root. For focused non-live validation:

```bash
bash storage-kit/scripts/check.sh
```

The default gate runs Lance-only deterministic and embedded live coverage
without alternative credentials or service provisioning:

```bash
bash storage-kit/scripts/verify_strict_live.sh
```

Complete Database and StateStore live targets against already-running services:

```bash
source storage-kit/strict-tests.env && cargo test --locked -p storage-kit --test strict_live --no-default-features --features redb,postgres,surrealdb,sqlite -- --nocapture --test-threads=1
```

Focused PostgreSQL round-trip case; this is not complete PostgreSQL coverage:

```bash
source storage-kit/strict-tests.env && cargo test --locked -p storage-kit --test strict_live --no-default-features --features postgres postgres_state_store_is_live -- --exact --nocapture --test-threads=1
```

The focused command omits independent collation, conflict, timeout,
lost-waiter and cleanup-receipt regressions. Direct test invocations do not
provision services or replace the canonical verifier.

## Current Matrix

The default verifier runs Lance-only deterministic and real embedded Database /
StateStore tests. `--backend NAME` adds one alternative; `--changed-since BASE`
selects direct backend/shared-abstraction coverage; `--all-backends` runs the
complete matrix. The four optional backend features are `redb`, `postgres`,
`surrealdb` and `sqlite`. Validate explicit feature lanes and their supported
combinations; never use Storage `--all-features`.

The live target covers these implementation families:

- LanceDB memory and filesystem;
- SurrealDB memory, RocksDB, SurrealKV, remote WebSocket and remote HTTP;
- Redb memory and file StateStore;
- SQLite/Turso memory and file StateStore when `sqlite` is selected;
- PostgreSQL StateStore against Docker Postgres.

The [owning testing guide](../../../../storage-kit/docs/testing.md) records the
current operation and regression cases and their exact proof requirements.

Qdrant support is temporarily removed. Historical sources and receipts do not
provide current backend or validation coverage.

## Current Contract

- The canonical public target is `strict_live`.
- Strict-live suites must fail on missing or invalid env instead of skipping.
- Bare tests require already-running configured services. They neither start
  services nor provide a shared 60-second readiness gate. The verifier performs
  readiness checks after startup. Each HTTP endpoint permits 60 attempts with
  a 2-second connection limit, 10-second transfer limit and 1-second failure
  delay: about 660 seconds for a persistently stalled endpoint, plus scheduling
  overhead. This does not bound Compose commands or the whole verifier. The
  [testing guide](../../../../storage-kit/docs/testing.md#strict-live) owns
  readiness and orchestration details.
- Selected strict-live lanes use explicit compatible features; the default
  Lance-only lane enables no alternatives. Every invocation uses fresh owned
  child directories below configured embedded roots; caller data is preserved.
  Service endpoints must match the selected invocation-owned Compose services.
  SurrealDB readiness uses `/ready`; native versions and image identities are
  recorded after readiness and before live cases.
- `storage-kit/scripts/verify_strict_live.sh` starts bundled SurrealDB and
  PostgreSQL only when selected, under a fresh invocation-owned Compose
  project. Exit cleanup removes that project's named volumes with
  `docker compose down --volumes`. Cleanup authority begins immediately before
  the first service mutation, after environment and deterministic validation
  succeed. A cleanup failure fails an otherwise successful gate; when both fail,
  retain the gate's primary failure and report cleanup failure separately.
  Follow the testing guide's port and embedded-path isolation requirements;
  the verifier owns its fresh Compose project name.
- PostgreSQL live coverage requires `--features postgres`;
  the focused no-default PostgreSQL command above does not change that gate.
- Surreal realtime is backend-specific and is not part of the portable
  Database strict-live contract.
- Source-contract coverage must keep removed backend routes, raw/native
  escape hatches, duplicate backend public capability bitsets, synthetic vector
  storage, and silent request-field drops from returning.

## Proof Boundary

- The canonical matrix covers real embedded engines plus docker-backed remote
  services.
- PostgreSQL StateStore source contracts must prove dedicated actor connection
  and task ownership. Real Docker Postgres proof must cover primary-client
  connect, retained transaction handles, schema initialization, serializable
  transaction conflict and cleanup behavior, tombstones, list pagination, and
  timeout mapping through lock contention.
- Strict-live proves legal backend behavior. It does not prove malformed
  impossible wire shapes.
- Deterministic local decode tests own rejection of malformed backend-private
  inputs that real services cannot legally emit.
- Behavior that is not reproducible in the matrix is not a valid portable
  success contract.
