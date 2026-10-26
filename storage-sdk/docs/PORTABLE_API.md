# Storage portable API

This document owns the behavior required of `DatabaseBackend`,
`StateStoreBackend`, and `StateTransactionBackend`, including caller-written
implementations. The crate's Rust declarations and functional-module rustdoc
own DTO fields and validation bounds. [Architecture](../ARCHITECTURE.md) owns
private resource management; [testing](testing.md) owns the executable gates.

## Static composition

All three behavioral traits are implemented directly. A backend need not be
`Clone`. `DatabaseBackend` and `StateStoreBackend` require `Send + Sync +
'static`; each method returns a `Send` future borrowing that backend. A
`StateTransactionBackend` requires `Send + 'static`, has no lifetime parameter,
and need not be `Sync` or `Clone`. Its `commit` and `rollback` consume it.

The supplied `Database`, `StateStore`, and `StateTransaction` implement exactly
those contracts. The first two supplied handles are cloneable shared owners;
the transaction is unique. Their private dispatchers contain built-ins only.
There is no normalization constructor, public trait object, backend identity,
factory, implementation registry, or second inherent behavioral interface.
Bring the owning trait into scope to call its methods on a supplied handle.
Construction remains an inherent operation: `Database::connect(DatabaseConfig)`
or `StateStore::connect(StateStoreConfig)`.

A caller may use its own exhaustive store enum with its own associated
transaction enum. A consumer keeps these exact static types; it cannot assume
that a transaction is the supplied `StateTransaction` or require a custom
backend to construct one.

## Database

```rust,ignore
pub trait DatabaseBackend: Send + Sync + 'static {
    fn capabilities(&self) -> CapabilitySet;
    fn catalog(&self, request: CatalogRequest)
        -> impl Future<Output = Result<CatalogResponse, StorageError>> + Send + '_;
    fn data(&self, request: DataRequest)
        -> impl Future<Output = Result<DataResponse, StorageError>> + Send + '_;
    fn query(&self, request: QueryRequest)
        -> impl Future<Output = Result<QueryResponse, StorageError>> + Send + '_;
    fn index(&self, request: IndexRequest)
        -> impl Future<Output = Result<IndexResponse, StorageError>> + Send + '_;
}
```

The supporting types are in `database::{catalog,data,query,index}` and
`database::CapabilitySet`. The retained inventory is catalog creation,
opening, listing, dropping and alteration; data insertion, lookup, scanning,
update, deletion, merge, partition replacement and count; nearest, stored-id,
text, scroll and sample queries; and index creation, listing, dropping and
readiness waiting. The [capability matrix](capability-matrix.md) specifies the
optional operations provided by the supplied families.

An operation is authoritative. A capability bit is not an acknowledgement that
an arbitrary request is supported. Invalid configuration and invalid requests
fail through the owning error. An unsupported optional operation fails before
mutation with `UnsupportedCapability` or the documented
`NotYetImplemented` category; an adapter cannot silently discard fields or
manufacture ids, counts, scores, records or completion.

Surreal collection removal keeps table removal and partition-coordination cleanup
in one native transaction. It retries only a confirmed rolled-back transaction:
a typed native transaction conflict, with any other failed statements marked
not executed. `CommonConfig.max_retries` bounds those retries, and the original
request deadline includes context selection, all attempts, and backoff. Transport
failures, indeterminate commits, mixed errors, and not-executed-only responses
are not replayed. Completion requires a successful native commit.

For `SchemaEvolutionRequest::AddColumns`, the supplied Lance implementation
eagerly fills existing rows from each `ComputedColumn.expression_sql` before
returning `CatalogResponse::Completed`. The derived values are immediately
readable and persist across filesystem database reopening. The portable
`ComputedColumn` request does not declare an upstream deferred computed column
or promise automatic recomputation after subsequent writes.

Both supplied Lance and Surreal implementations reject a present
`AddColumnsRequest.read_columns`, including `Some(vec![])`, with
`StorageError::UnsupportedCapability` (`SCHEMA_EVOLUTION_READ_COLUMNS`) before
mutation. Use `None` so the expressions determine their source columns.
`SCHEMA_EVOLUTION` indicates operation support; it does not guarantee every
request option.

The supplied Lance implementation rejects an alteration batch that combines
any `cast_to` with changing a currently nullable column to `nullable: Some(false)`.
It returns `UnsupportedCapability` (`SCHEMA_EVOLUTION_CAST_NULLABILITY`) before
applying any alteration, preserving the rows, schema, and indexes. Cast first,
then make the nullable column required in a separate request; each completes
independently. A cast combined with `nullable: Some(false)` on an already
non-nullable column remains supported. The adapter never splits one request
into separately visible native commits.

LanceDB supports scalar equality (`Eq`), non-equality (`Ne`) and membership
(`In`) predicates on Json fields. Range comparisons (`Gt`, `Gte`, `Lt`, `Lte`, `Between`) and pattern
predicates (`Like`, including its case-insensitive or negated forms) on those
fields return `UnsupportedCapability` before executing the query or mutation.
The supplied JSON encoding does not expose native logical ordering or pattern
matching. Ordinary String fields retain the backend's native scalar and pattern
semantics. This is an exact field/operator limitation, not absence of filtering.

`StorageLocator` is the portable logical namespace/database/collection address.
Its meaning is not an implementation discriminator or native connection
configuration. Unsupported locator depth or request combinations must be
rejected, not silently redirected to a different collection.

Public response DTOs permit caller construction. `CollectionInfo` may omit a
schema or count when it cannot honestly report that information. A
`QueryResponse` carries records, optional raw scores with their explicit
`ScoreKind`, and an optional caller/backend-owned `PageToken`. Consumers return
that token unchanged to the same applicable query path and cannot parse it as
a universally numeric offset. Distances, similarities and relevance scores
are not mutually interchangeable. Absence of a raw score must remain absence.
The selected [Lance 13 scanner](https://docs.rs/crate/lance/13.0.0/source/src/dataset/scanner.rs)
uses live-row metadata for eligible unfiltered limit/offset planning and
defaults to deterministic scan order. The supplied Lance regression
compares pages with an independently verified same-state scan after deletions,
including after filesystem reopen; it adds no portable insertion-order or
cross-reopen ordering guarantee. [Testing](testing.md) owns that proof's bounds.
Database operations do not provide the transactional control-plane promises
below; no StateStore transaction can include Database effects.

`WriteMode::Overwrite` replaces the collection's records, including clearing
its contents for an empty replacement. It preserves the collection schema and
indexes; catalog overwrite instead recreates a collection. Merge upserts only
its selected identities and preserves unrelated rows. Empty Qdrant Append/Merge
is an identity operation after verifying that the collection exists.

Qdrant REST replacement preflights all native inputs, confirms the upsert with
`wait=true` and a `completed` result, then removes the complement. A failed or
unconfirmed upsert never triggers deletion. Ordinary Database writes are not
rollback-capable multi-request transactions: a timeout or native runtime failure
can have an uncertain or partial outcome and is not automatically replayed.
`ReplacePartition` retains its stronger explicit atomic contract. CommonConfig's
portable operation deadline includes internal preflight requests and retries.

Qdrant REST index creation and deletion likewise request `wait=true` and require
an explicit `completed` result before returning `IndexResponse::Completed`.
A queued acknowledgement or missing completion evidence is an error; the
mutation is not automatically replayed. `WaitForIndexRequest.timeout_ms` bounds
the complete readiness observation, including its response body, in addition to
the enclosing CommonConfig operation deadline. An already-ready index returns
immediately; an absent index remains an explicit readiness failure.

Qdrant REST and Edge retain named dense vectors. An explicit `vector_field`
selects that name; omission selects `QdrantConfig.default_vector_name`.
SearchById retrieves the seed from the selected vector and excludes the source
identity even when the caller's filter admits it. Projections independently
select payload fields and vector names; lookup omits missing identities.
Scan and Scroll tokens remain opaque, including UUID cursors: return each token
unchanged. The initial numeric offset applies only before a token is present.
Numeric vector inputs use lossless conversion to f32; cosine readback may be
normalized and does not preserve the caller's numeric variant.

## StateStore and associated transaction

```rust,ignore
pub trait StateStoreBackend: Send + Sync + 'static {
    type Transaction: StateTransactionBackend;

    fn load(&self, key: StateKey)
        -> impl Future<Output = Result<Option<StateRecordSnapshot>, StateStoreError>>
            + Send + '_;
    fn list(&self, request: StateListRequest)
        -> impl Future<Output = Result<StateListResponse, StateStoreError>> + Send + '_;
    fn begin_transaction(&self)
        -> impl Future<Output = Result<Self::Transaction, StateStoreError>> + Send + '_;
}

pub trait StateTransactionBackend: Send + 'static {
    fn load(&mut self, key: StateKey)
        -> impl Future<Output = Result<Option<StateRecordSnapshot>, StateStoreError>>
            + Send + '_;
    fn put(&mut self, record: StateRecord)
        -> impl Future<Output = Result<(), StateStoreError>> + Send + '_;
    fn delete(&mut self, key: StateKey)
        -> impl Future<Output = Result<(), StateStoreError>> + Send + '_;
    fn commit(self) -> impl Future<Output = Result<(), StateStoreError>> + Send
        where Self: Sized;
    fn rollback(self) -> impl Future<Output = Result<(), StateStoreError>> + Send
        where Self: Sized;
}
```

The request/response values are in `state::record`. A `StateRecord` is a caller
write input with a validated fully qualified key and a public
`Arc<serde_json::Value>` payload. `StateRecord::new(key, value)` accepts an owned
JSON Value and shares it; cloning a record keeps its content independently
editable through `Arc::make_mut(&mut record.value)`. Changes to that caller-owned
copy do not change retained observations or stored content unless explicitly
published through a transaction. Separately returned observations need not share
an allocation; pointer identity is not a backend, revision, or publication
contract. `StateRecordSnapshot` adds the observed positive `StateRevision`. Revisions are
backend-owned counters for individual keys, not workflow sequence numbers,
timestamps, globally ordered commit ids or selectable historical snapshots.

### Isolation and publication

A successful transaction atomically publishes every staged mutation. Its
transactional reads and writes must be consistent with a serial ordering of
successfully committed transactions, including reads of absent or tombstoned
keys. Implementations may serialize admission or detect a conflict; callers
must not assume optimistic admission or that any particular concurrent
transaction wins. An observed native serialization/write conflict is preserved
as `StateStoreError::TransactionConflict`.

A read is protected only when performed through the same transaction used for
the conditional write. A preceding `StateStoreBackend::load` or `list` is not
a fence and cannot justify a later overwrite. To enforce an application
revision, begin a transaction, load its record, check that exact revision,
stage all related writes, then commit. A read-only transaction need not fail
merely because another serializable transaction later changes a read key.
The guarantee is serializability, not a promise that every overlapping change
produces an error.

Transactional reads see that transaction's earlier puts and deletes. Other
callers see only committed data. A delete makes `load` return `None`, including
inside the deleting transaction. Multiple writes to a key within a transaction
are permitted and retain their authored operation order. A mutation failure
invalidates the transaction: it may be rolled back or dropped, and must not
subsequently publish an earlier partial mutation set through `commit`.

The transaction contains known-key operations, not a scan or list method.
There is no portable snapshot token, history/fork operation, arbitrary SQL,
watch interface or externally reconstructible native transaction. App and
Agent own their record formats, receipts, histories and reconciliation.

### Exact revision rules

- The first successful put **or delete** for a previously untouched key stages
  revision 1. Deleting an absent key records a tombstone; it is not a no-op.
- Every subsequent put or delete stages the preceding revision plus exactly
  one, including repeated equal-value puts and repeated deletes in one
  transaction. Transactional loads expose the latest staged present revision.
- A committed tombstone retains that revision. Recreating the key increments it;
  deletion cannot reset the counter or admit an ABA equality check.
- Rollback, failed transactions and dropped uncommitted transactions consume no
  committed revisions. Their staged increments disappear with their writes.
- Revisions never wrap or saturate. An implementation reports a typed failure
  at its supported numeric boundary and cannot publish a partial transaction.
  The supplied SQL stores have a signed 64-bit native revision range; the
  public positive `u64` representation does not promise every built-in can
  persist its full range.

### Listing and pagination

`StateListRequest` selects one exact namespace, an optional literal
case-sensitive key prefix, a bounded page size, and an optional namespace-local
exclusive `start_after` key. The public [state::record](../src/state/record.rs)
module owns validation bounds and request defaults. Prefix wildcard characters
have no special meaning.

Each response is ordered by canonical ascending key order: lexicographic UTF-8
bytes, independent of process locale or database collation. It contains at
most the requested number of present records, all in the requested namespace
and prefix and strictly after the cursor. Tombstones never appear.
`next_start_after` is the last returned key when another matching record
exists in that page's read view; otherwise it is `None`. An empty result has
no continuation. Cursors always advance; they are keys, not backend offsets.

Each individual page uses one consistent read view. A sequence of separate
pages **is not a historical snapshot**: concurrent insertion, update or
pruning can change later pages. `None` does not promise that no future write
will add a record. Consumers requiring stable inventory use their own
transactionally checked generation/head plus immutable records, upper bounds
and bounded revalidation. They cannot infer a portable snapshot from a native
`StateRevision` or impose a new storage snapshot interface to implement App
history.

### Cancellation, finalization and errors

Commit and rollback transfer the unique transaction into finalization. `Ok(())`
from commit means all staged mutations committed; success is delivered after
the owning native operation and cleanup. Rollback success means none committed
and rollback cleanup completed. Dropping an uncommitted transaction never
commits it; its owner must settle rollback and release native resources.

Admission and waiting are separate. Once a commit is admitted to its native
owner, dropping the waiting future must not cancel or detach finalization.
The transaction can therefore commit after its caller loses the reply. A
cancelled or failed nonterminal operation invalidates further writes/commit
and drives rollback cleanup. Task/thread ownership, bounded admission and
native waits remain the implementation's responsibility, with explicit
configuration and no production environment-variable lookup.

`TransactionConflict` means the transaction did not publish its mutations. An
invalid request or serialization error before commit similarly cannot justify
partial publication. `TimedOut`, `Unavailable`, a transport/backend finalization
error, a lost waiter or process loss **does not by itself prove commit rolled
back**. Preserve the primary error and any uncertainty. An upper consumer
must reconcile its own durable receipt in a fresh read/transaction before
retrying a potentially committed mutation; storage does not invent portable
success or replay non-idempotent work.

No portable error exposes a native driver, SQL, credentials or transport
payload. Configuration and operation errors remain distinct. A native lock
wait may produce `TimedOut`; a serializability failure produces
`TransactionConflict`. Concurrency is not a reason to remap every timeout or
backend failure to conflict.

## Validation and source grounding

[Testing](testing.md) owns the executable proof lanes and validation gates.

The supplied implementations rely on distinct native mechanisms:

- The embedded Lance StateStore uses the pinned Lance 13.0.0 Dataset APIs beneath
  LanceDB 0.40.0. It prepares a delta against one immutable snapshot, then guards
  the exact next manifest version before conditional publication. A private
  sentinel, permanent tombstones, owned worker and local durability wrapper
  preserve the contracts above without changing their public surface. Private
  decoded-image reuse validates a fresh native identity, and private native-file
  reclamation requires exclusive snapshot protection; neither permits changing
  logical revisions or expiring caller transactions. The
  [historical prototype ledger](lancedb-state-store-prototype.md) separates
  design reasoning, tests, native probes and platform execution for its original
  candidate; it does not qualify the current native dependency graph.
  [Cold-copy recovery guidance](../README.md#recovery-and-cold-backups) uses
  settled filesystem ownership and adds no portable backup or import operation.
- Redb 4.3.0 retains one native writer behind bounded FIFO logical admission.
  Its synchronous transaction runs on the owning OS thread, with native
  commit/abort and tombstones. See the repository-only research archive
  `vendor/redb-4.3.0.zip` (excluded from the published crate), members
  `src/db.rs` and `src/transactions.rs`; no newer Redb guarantee is implied.
- Turso 0.8.2 backs SQLite with dedicated actor connections and native
  immediate transactions. Pinned `vendor/turso-0.8.2.zip` members under
  `bindings/rust/src/` (`connection.rs`, `rows.rs`, `lib.rs`, and `transaction.rs`)
  ground its native operations: batches acquire exclusive connection access,
  while query rows retain shared access until exhaustion, error, or drop.
  Storage keeps rows lexical within each operation. The official
  [Rust reference](https://docs.turso.tech/sdk/rust/reference) provides general
  API context; SDK cancellation, revisions, and finalization authority are
  separately established by local and strict-live tests. Clean file reopen
  proves ordinary persistence, not crash-before-checkpoint durability.
- PostgreSQL uses dedicated serializable transaction connections and atomic
  per-key increments. Native committed-outcome semantics come from
  [PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html).
  Listing compares and orders keys after [conversion to UTF-8 bytes](https://www.postgresql.org/docs/18/functions-binarystring.html),
  so the cursor and page order use the same representation regardless of the
  database's text collation or encoding.
  The async transport/finalization boundary uses the
  [tokio-postgres transaction contract](https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Transaction.html);
  SDK tests must additionally prove its own actor ownership and error mapping.

These native facts support the design; they do not replace local or strict-live
proof. Keep each native actor's admission and cleanup logic separate.
