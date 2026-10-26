# storage-sdk

`storage-sdk` provides two focused Rust APIs:

- `Database` for portable data, retrieval, and indexes with always-available LanceDB
  and opt-in SurrealDB/Qdrant.
- `StateStore -> StateTransaction` for revisioned control-plane state with
  always-available embedded Lance and opt-in Redb/PostgreSQL/SQLite.

The crate-provided handles use closed built-in dispatch. Callers implement
`DatabaseBackend`, `StateStoreBackend`, and `StateTransactionBackend` directly
on their own concrete types or enums. A caller-owned StateStore enum returns
its caller-owned transaction enum through `StateStoreBackend::Transaction`;
storage-sdk performs no erasure or normalization.

## Construction

Built-ins use direct configuration:

```rust,no_run
use storage_sdk::{database::config::DatabaseConfig, Database, StorageError};

async fn open(config: DatabaseConfig) -> Result<Database, StorageError> {
    Database::connect(config).await
}
```

```rust,no_run
use storage_sdk::{
    state::config::StateStoreConfig, StateStore, StateStoreError,
};

async fn open(config: StateStoreConfig) -> Result<StateStore, StateStoreError> {
    StateStore::connect(config).await
}
```

There are no builders, durable factories, public conformance owners, storage
tool adapters, or public realtime API.

The default embedded Lance StateStore is selected explicitly:

```rust,no_run
use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{StateKey, StateRecord};

async fn save() -> Result<(), storage_sdk::StateStoreError> {
    let store = StateStore::connect(StateStoreConfig::Lance(
        LanceStateStoreConfig::directory("application-data"),
    )).await?;
    let mut transaction = store.begin_transaction().await?;
    transaction.put(StateRecord::new(
        StateKey::new("workflow", "checkpoint")?, serde_json::json!({"step": 1}),
    )).await?;
    transaction.commit().await
}
```

Use `LanceStateStoreConfig::memory()` for an independent process-local store;
clones of its connected handle share that store. Directory mode targets macOS
and Linux. A Lance Database may use the same directory for knowledge collections;
the private state collection is reserved, including ASCII case variants.
The default 30-second timeout bounds admission and observation, not accepted
commit authority or final filesystem joining. Read preconditions inside the
transaction; reconcile a durable application receipt after ambiguous errors.

The [evidence ledger](docs/lancedb-state-store-prototype.md) records the embedded
backend's macOS/APFS and Linux/ext4 validation, consumer and packaged-build
checks, and measured capacity. Those results apply to the recorded source,
locked dependency graph, and execution environments.
The [focused verifier](docs/testing.md#embedded-lance-statestore)
runs ordinary real-engine, public-API, and owned-process checks. The privileged
fault campaign remains excluded. JSON values retain the existing public
representation; the private codec does not impose a nesting limit.
Each ordinary read opens a fresh native view and validates its identity before
reusing the worker's decoded image. Cold connections and externally changed
identities still require a complete scan and decode. Maintenance also scans and
validates every native row, but exact previously validated payload bytes can
reuse their decoded values. Each private JSON owner retains its encoded bytes
in addition to the parsed value; unchanged clones share both. Transactions at
one image can share it; transactions retaining different versions retain
separate complete images. Multipart writes buffer their payloads. These costs
have no implicit transaction expiry, payload-size or record-count cap.

Private maintenance reclaims obsolete native history and unpublished files
only while every supplied snapshot lease is absent. It retains the current
version and every tombstone, preserving exact logical revisions. Long-lived
transactions can defer reclamation indefinitely, and tombstones still grow
with the set of deleted keys. Connection-triggered orphan cleanup also scans
retained manifest metadata before deleting unpublished files. These memory,
storage and contention costs belong in the measured capacity envelope and any
deployment sizing.
Hosted/external Lance storage and Windows qualification are outside the supported
embedded scope.

### Recovery and cold backups

An exact `TransactionConflict` means that transaction published nothing. An
observation timeout or other finalization error can leave a committed result.
For a retryable application operation, put its durable receipt and data changes
in the same transaction. After an uncertain result, read that receipt before
authoring the operation again; a missing receipt while the original commit is
still running is not a rollback acknowledgment.

```rust,no_run
use storage_sdk::{StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend};
use storage_sdk::state::record::{StateKey, StateRecord};

async fn checkpoint(store: &StateStore, operation_id: &str)
    -> Result<(), StateStoreError>
{
    let receipt = StateKey::new("workflow-receipts", operation_id)?;
    let checkpoint = StateKey::new("workflow", "checkpoint")?;
    let mut transaction = store.begin_transaction().await?;
    if transaction.load(receipt.clone()).await?.is_some() {
        return transaction.rollback().await;
    }
    let step = match transaction.load(checkpoint.clone()).await? {
        Some(snapshot) => snapshot.record.value.get("step")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| StateStoreError::invalid_request(
                "checkpoint.step must be an unsigned integer",
            ))?,
        None => 0,
    };
    let next_step = step.checked_add(1)
        .ok_or_else(|| StateStoreError::invalid_request("checkpoint step overflow"))?;
    transaction.put(StateRecord::new(
        checkpoint, serde_json::json!({"step": next_step}),
    )).await?;
    transaction.put(StateRecord::new(
        receipt, serde_json::json!({"step": next_step}),
    )).await?;
    transaction.commit().await
}
```

Callers own the retry policy and stable operation identifier. A newly authored
transaction reads both the receipt and current checkpoint before deriving its
next step, then publishes the checkpoint and receipt together. Ordinary polling
alone does not fence a retry against concurrent publication. The SDK never
automatically replays an authored transaction after uncertainty.

For a cold backup, stop new work in every process using the directory, settle
accepted Agent/App work, and release every transaction, StateStore, and
same-directory Database handle. Final handle release joins its accepted native
work. Copy the **entire directory** only after all writers and owners have
settled. This preserves tombstones, revisions, manifests, retained versions,
and knowledge collections. A live recursive copy is not a supported coherent
backup, and a StateStore transaction does not include Database effects.

Restore into a new empty directory, open it using the ordinary constructors,
and verify application receipts and records before resuming work. Keep the
original backup until validation succeeds. Do not repair a corrupt private
table by deleting manifests, tombstones, or the sentinel, and do not overwrite
an active directory. Cold-copy/restore coverage and its current execution
receipt belong in the evidence ledger.

The current native family is LanceDB 0.40.0 with Lance 13.0.0. The supplied
configuration remains local-only. LanceDB's `remote` feature is enabled solely
to compile its 0.40.0 job decoder; this does not expose a remote deployment or
enable its cloud object-store features.

For an SDK upgrade, first retain a cold copy and validate the candidate against
that copy. Stop all older processes before starting the candidate: older
prototype clients do not participate in the new private snapshot leases and
cannot safely share maintenance with it. The private table still requires
schema version 1, Lance file format 2.0, and disabled stable row IDs. Native
maintenance or a format upgrade outside the supplied adapter is unsupported.
The private reclamation rules depend on the pinned native layout and filenames;
a dependency upgrade must requalify those rules before use. Both maintenance
paths validate retained references and sweep recognized unreferenced files
without assuming that filesystem and manifest clocks agree. Cut over only
after application reopen and receipt checks pass, and resume a rollback copy only while all
writers are stopped. The portable list/put API cannot migrate exact hidden
tombstone revisions from another backend; existing Redb files are never
automatically converted or reinterpreted as Lance directories.

For SurrealDB 3.3 embedded memory, RocksDB, and SurrealKV, deep native queries
in unoptimized builds may need an 8 MiB stack on the thread polling the query
future. The upstream [debug-test configuration](https://github.com/surrealdb/surrealdb/blob/v3.3.0/.cargo/config.toml)
sets that size, and the crate's embedded regression workloads need it. When
using a current-thread runtime or polling the query directly through
`Runtime::block_on`, configure the enclosing OS thread with
`std::thread::Builder::stack_size(8 * 1024 * 1024)`. Tokio's
[`thread_stack_size`](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.thread_stack_size)
configures runtime-created worker threads; it does not resize the calling
thread. Remote HTTP/WebSocket clients do not execute the embedded engine
locally.

## Public Modules

- `database::config` — Lance, Surreal, Qdrant, and shared construction data
- `database::catalog` — collections and schema alteration
- `database::data` — records and portable mutation requests
- `database::query` — retained cross-backend search operations
- `database::index` — portable index lifecycle
- `state::config` — Redb, PostgreSQL, SQLite/Turso, and Lance construction
- `state::record` — keys, namespaces, records, revisions, and pagination
- `state::transaction` — transaction-domain supporting values

Primary handles, backend traits, and errors remain at the crate root.

## Portability

The Database surface contains only behavior with honest semantics in at least
two backend families. Unsupported operations return explicit errors before
mutation. Live handles and backend traits expose no reusable implementation
identity or provenance discriminator. Backend clients, native requests, raw
plans, trace/debug payloads, SQL, table layouts, and actor state stay private.

State transactions are owned, lifetime-free, and consumed by commit or
rollback. Redb uses a bounded FIFO native-writer owner; PostgreSQL and
SQLite/Turso use dedicated per-transaction connection actors. Transaction
drop never detaches cleanup or commits implicitly.

`StateStore` is for SDK control-plane state. `Database` is for application data
and managed retrieval. Model-visible tools are caller-authored in `tool-sdk`;
storage-sdk has no tool runtime dependency.

## Features

LanceDB for `Database` and embedded Lance for `StateStore` are always compiled,
including with `default-features = false`. The default feature list is empty.
Construction remains explicit through the existing configuration types; no runtime fallback
or automatic migration opens an existing Redb store as Lance.

| Optional feature | Implementation |
| --- | --- |
| `redb` | Redb StateStore |
| `postgres` | PostgreSQL StateStore |
| `sqlite` | SQLite/Turso StateStore |
| `surrealdb` | SurrealDB Database |
| `qdrant` | Qdrant Database |

Existing users of these implementations must add the corresponding feature to
their dependency. Redb, PostgreSQL and Surreal configuration DTOs stay available
without their implementation feature; valid disabled construction returns the
existing `InvalidConfig` error before native I/O. SQLite/Qdrant retain their
existing feature-conditional configuration availability. Public traits and
caller-defined static implementations are unchanged.

On macOS, `qdrant` also activates an optional exact registry constraint on
`webbrowser =1.2.1`. This keeps LanceDB's browser dependency from enabling the
Objective-C trait combination that causes the selected Edge 0.6.1 to fail
compilation. Storage exposes no browser API or separate browser feature.
Cargo resolves optional workspace features and target tables when generating
the lockfile, so this constraint can also select 1.2.1 for other platforms and
feature lanes. Consumers requiring a newer incompatible browser constraint
may fail dependency resolution. Independently enabling the same Objective-C
features can still expose the native Edge compiler limitation; this mitigation
does not establish compatibility with arbitrary downstream dependency graphs.
See the [dependency policy](AGENTS.md#backend-and-secret-ownership) for its
validation boundaries.

`sqlite` and `qdrant` cannot be enabled together: their pinned dependencies
require incompatible `parking_lot` features. Cargo combines features across
workspace dependents, so placing these features in different crates does not
isolate the conflict. Never use Storage `--all-features`.

Run `bash storage-sdk/scripts/verify_strict_live.sh` for default Lance coverage.
Use `--backend NAME`, `--changed-since BASE`, or `--all-backends` for additional
coverage; see [testing](docs/testing.md). Alternative services and their
credentials are required only when selected.

## More

- [Portable API and transaction contract](docs/PORTABLE_API.md)
- [Architecture](ARCHITECTURE.md)
- [Capability matrix](docs/capability-matrix.md)
- [Testing](docs/testing.md)
- [Contributor rules](AGENTS.md)

## Direct public-trait tests

Run `bash scripts/check_mock_contracts.sh --crate storage-sdk` from the workspace
root for the credential-free mock lane. The workspace primary CI entrypoint is
`bash scripts/check_ci.sh`. See [mock testing](../docs/mock-testing.md) for
targets, evidence, runtime qualification, and the separate release/live gates.
