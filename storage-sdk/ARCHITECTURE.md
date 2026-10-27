# storage-sdk Architecture

`storage-sdk` contains two separately owned portable domains: a multi-backend
`Database` for application data and retrieval, and a transactional
`StateStore -> StateTransaction` hierarchy for control-plane state.

## Public Boundary

| Public trait | Built-in handle | Closed dispatcher |
|---|---|---|
| `DatabaseBackend` | `Database` | `DatabaseInner` |
| `StateStoreBackend` | `StateStore` | `StateStoreInner` |
| `StateTransactionBackend` | `StateTransaction` | `StateTransactionInner` |

Each built-in payload, private dispatcher, and built-in handle implements its
public trait. The dispatchers contain only concrete built-in variants. Callers
implement the traits directly on their own concrete types or enums and retain
their own static types. A caller-owned StateStore enum names its own associated
transaction enum; storage-sdk performs no normalization or erasure.

The crate has no `from_backend` ingress, extension variant, erased adapter,
public trait-object handle, factory, conformance suite, builder, registry,
compatibility layer, or duplicate behavioral contract.

## Public Module Layout

```text
storage_sdk
├── Database
├── DatabaseBackend
├── StateStore
├── StateStoreBackend
├── StateTransaction
├── StateTransactionBackend
├── StorageError
├── StateStoreError
├── database
│   ├── config
│   ├── catalog
│   ├── data
│   ├── query
│   └── index
└── state
    ├── config
    ├── record
    └── transaction
```

Primary runtime owners and errors remain convenient at the root. Supporting
DTOs and configuration are grouped by functional domain instead of being
flattened wholesale.

## Database

`Database::connect(DatabaseConfig)` selects a built-in concrete variant.
`DatabaseConfig` is one enum carrying:

- Lance embedded configuration;
- Surreal embedded or remote configuration;
- feature-gated Qdrant Edge or REST configuration.

The [portable API](docs/PORTABLE_API.md#database) owns the retained operation
contract. The [capability matrix](docs/capability-matrix.md) records which
optional operations the supplied families support.

Schema alteration is part of catalog ownership; there is no one-variant admin
envelope. Optional capability reporting describes only retained behavior.
Calling the operation remains authoritative and may return an honest
unsupported error.

The portable boundary excludes realtime, raw plans/traces, backend notes,
native status messages, generic located wrappers, namespace administration,
aliases, single-family search and administration operations, sparse-only
models, and backend optimizer controls. Backend-native APIs may still exist
privately where their concrete implementation needs them.

Qdrant index operations live on the existing manager under its request deadline;
index readiness retains its additional caller-selected wait bound. Edge nearest
requests own their native search-parameter conversion. Collection creation fully
encodes configuration and points before any overwrite; the larger native Edge
configuration is boxed within that private prepared-creation enum.

Qdrant Edge keeps native shards under the shared connection owner. Its payload
placement is explicitly in memory; the native engine owns persistence. The file
regression closes every Database owner before reopening and proves inserts,
payload updates, deletions, named-vector selection and dimension validation
survive normal close/reopen.
This does not prove crash or power-loss durability. Native Drop-flush logging
cannot supply a successful public flush receipt or propagate a flush failure
through an acknowledged SDK shutdown operation.

## StateStore And Transactions

The exact trait signatures and portable isolation, revision, pagination and
finalization obligations live in [the portable API](docs/PORTABLE_API.md).
`StateStore::connect(StateStoreConfig)` selects the always-available embedded Lance StateStore or the
opt-in Redb, PostgreSQL, and SQLite/Turso implementations. Its associated transaction is the supplied unique
`StateTransaction`; caller stores return their own static transaction types.
The SDK never normalizes caller stores or transactions. Commit and rollback
consume the transaction, and drop never commits it.

`StateRecord` is caller write input with a shared immutable JSON payload;
`StateRecordSnapshot` pairs observed data with its per-key revision. Caller-owned
copies use standard Arc copy-on-write without changing stored observations. Upper crates own their own immutable history,
mutable heads and receipts through these existing operations. Native revision
values are fences, not application checkpoint sequences. Multiple list calls
do not create a snapshot.

### Backend ownership

- Lance owns one OS worker and a bounded queue of 32 commands. Each job uses an
  isolated current-thread native runtime. Transactions retain a validated
  decoded image, its immutable native Dataset, snapshot protection, and an overlay. Nonterminal
  operations contain no suspension; errors discard prior staging. An admitted
  commit transfers all state to the worker and survives observer loss. Final
  owner drop closes admission, drains accepted work, and joins without a cycle.
  Native filesystem waits can extend joining beyond observation deadlines.
- Lance owns JSON container traversal with explicit stacks for encoding,
  decoding and disposal. The pinned serde_json scalar parser retains string,
  escaping, number and exact floating-point behavior; container traversal adds
  no nesting limit. Adapter-owned values use one private last-owner domain,
  including discarded overlays, malformed partial values and replaced object
  entries. Worker load/list replies keep private record/page owners until the
  observer receives them; public Arc values are created synchronously after
  receipt. Losing an observer therefore cannot leave an unobserved public
  payload to be recursively destroyed on the worker. The public DTO and trait
  signatures are unchanged.
- Lance state files explicitly use the pinned engine's supported 2.0 format
  without stable row IDs. That encoder schedules child work on the owned
  runtime. The native default format can schedule encoding on a private global
  CPU executor whose jobs cannot be joined by this owner. Every opened state
  image checks the required format and row-ID mode before scanning; incompatible
  datasets fail rather than silently selecting a different lifecycle path.
- Lance delta publication updates a sentinel and the authored rows with native
  merge preparation, then commits against the original Dataset with zero
  retries. A foreign native CommitHandler rejects any target other than the
  original V+1 before conditional manifest creation. Transaction properties are
  assigned directly; the handler is installed on Dataset loading. Read-only
  transactions publish nothing. Permanent tombstones preserve exact revisions.
- Every ordinary read/begin opens a fresh Dataset and checks its native
  version/transaction identity. An unchanged identity may reuse the latest
  immutable decoded image; the Dataset and snapshot protection are never
  cached. A cache miss validates and decodes the complete persisted image.
  Transactions at the same version share that image, while retained distinct
  versions retain distinct full images. A successful exact-version delta commit
  derives a candidate image from the validated original image and final authored
  overlay, preserving every revision and tombstone with the new native version
  and sentinel UUID. The worker promotes it only after native children drain,
  their runtime is destroyed, and durable cleanup succeeds. Publication or
  cleanup uncertainty discards the candidate and invalidates cached state;
  UUID readback alone never promotes it. Old transactions retain their original
  immutable images. Initialization and external identity changes still scan
  and decode the complete persisted image. Maintenance also scans every native
  row and retains all schema, identity, sentinel, nullability, key, revision,
  presence and duplicate-record checks. Only after a matching key, revision,
  presence and exact byte comparison may it reuse a previously validated
  immutable JSON value. Changed bytes always pass through the iterative parser.
  Reclamation retains only the decoded image for this comparison; the native
  Dataset and snapshot are released at the same cleanup boundary.

  Each private JSON owner shares a lazy encoded-byte memo across its clones.
  Newly authored values use the existing iterative encoding. Decoded values
  retain their exact validated bytes, including whitespace or duplicate object
  keys; the public value keeps the same last-key-wins behavior. This changes no
  persisted format and performs no migration. Arrow batches still own their
  native byte buffers, and complete native scans remain required.

  Candidate construction clones O(N) record/key-index metadata for N retained
  logical records and temporarily clones O(W) overlay metadata for W authored
  keys. JSON payloads share the existing iterative-disposal Arc owners rather
  than being recursively cloned. The encoded-byte memo adds resident payload
  bytes and one shared memo allocation per private JSON clone family, without
  an encoded-size cap. These and the structural allocations remain part of the
  capacity cost, including when long-lived transactions retain old images.
  Listing scans the image while holding
  only limit-plus-one borrowed candidates, then clones the requested page.
  Delta batches move at most 128 rows each; maintenance batches borrow at most
  128 rows from the validated image. This bounds adapter row buffers, not
  record bytes, native query-plan memory or buffered multipart payloads.
- Lance directory writes use `file-object-store` and one native wrapping entry
  point shared with the isolated memory route. Directory wrappers, list/delete
  streams and buffered uploads retain the same `Arc<DurableIo>`; that owner holds
  the canonical paths and mutex-protected per-job error/cleanup ledger directly.
  It has no reference back to the engine or worker. Owned atomic writes retain
  file and ancestor-directory barriers. Internal write policies represent only
  preparation, conditional creation and replacement; unsupported native modes
  are rejected before filesystem effects. Recognized
  native data, deletion and transaction preparation files under the exact
  canonical state table are installed without replacement. Their owned staging
  name is removed and the complete ancestor chain synchronized before the final
  retained-file barrier, including macOS F_FULLFSYNC. Preparation collisions
  remain failures in the job ledger. Manifests, hints, unknown destinations and
  copy retain both existing file barriers; rename and ordinary deletion retain
  their immediate protocols. Publication and staging-cleanup errors retain their
  original precedence while the final file barrier always executes; a later
  failure is retained in the same ledger. Immediate fail-fast synchronization
  remains distinct from reclamation's attempt-all settlement. Upstream local
  `with_fsync` does not replace deletion durability, owned multipart cleanup or
  native-task draining.
  This measured preparation protocol is retained. The
  [qualification report](docs/feature-split-qualification.md) separates current
  feature-split status from historical performance and capacity evidence.
  Native ignored errors remain failures; own-UUID readback never invents
  durable success. Independent initializers coordinate through an owned
  bootstrap file lock; missing manifests in an existing nonempty table fail
  rather than resetting history. The lock carries no recovery state or journal.
- Exclusive Lance maintenance can coalesce directory synchronization for at
  most 64 recognized obsolete native files sharing one exact parent. The
  existing I/O ledger owns descriptors before unlink. Each group synchronizes
  the complete existing ancestor chain, then every file; no file barrier is
  removed. Groups flush on the 64th unlink, parent changes, excluded deletions
  (including manifests), native cleanup completion and supplemental orphan
  completion. Hint removal stays immediately durable before reclamation.
  A barrier failure rejects later deletion and remains sticky through the
  final sweep after native-child drain and runtime destruction. That finite
  sweep closes descriptors under exclusive protection and preserves all
  failures. Completed cleanup attempts do not establish successful durability.
  Batching is disarmed before staging cleanup, final reopen and validation.
  Memory mode, writes, publication barriers and maintenance selection are
  unchanged. This measured change is retained; the
  [qualification report](docs/feature-split-qualification.md) records its source
  proof, completed comparisons, historical live and capacity failures, and
  current feature-split status.
- The Lance state table is separate from knowledge collections in the same
  directory and is hidden/rejected by supplied Database operations, including
  ASCII case aliases. Direct native edits are unsupported. Schema, sentinel,
  keys, revisions, JSON, duplicates and native transaction identity are checked.
  Automatic native indexing and unconstrained compaction remain disabled.
  Private maintenance instead uses snapshot leases: a memory counter for one
  worker, or independent shared file-lock descriptors for every directory
  snapshot across handles and processes. Protection survives native child
  draining and cleanup. An exclusive lock/counter transition permits maintenance
  only when all supplied snapshots have released their leases.

  Scheduling estimates request maintenance at 64 retained versions; exact
  retention and a fresh complete image are checked under exclusive protection.
  At that threshold a full-row delta reuses guarded publication, preserves
  logical revisions/tombstones, and settles before reclamation. Cleanup retains
  the current manifest, removes obsolete history and unreachable files, and
  validates the remaining image. Orphan cleanup is requested on connection and
  after 64 failed native mutation preparations. A blocked or failed attempt
  waits another 32 completed jobs before retrying. Version hints are removed
  before deletion so process loss cannot leave a hint into partially removed
  history. A failed maintenance operation is reported through its initiating
  public job and invalidates cached decoded state; it does not revoke an
  earlier acknowledged commit.

  Both cleanup paths open all retained versions, validate each schema, storage
  format, transaction identity and data/deletion/transaction reference, and
  reject unsupported layouts before native history or data reclamation. The
  supplement retains only deletion and transaction-file paths; native 13 owns
  data-file reference discovery and scans data without an age cutoff under
  `delete_unverified`. Durable version-hint removal intentionally happens first.
  Orphan-only policy `before_version = 0` retains every committed manifest;
  full cleanup re-enumerates the manifests actually surviving native GC before
  collecting its final reference union. Native `Dataset::versions` performs a
  fresh listing rather than using the Dataset's earlier manifest inventory.
  During directory-backed reclamation, the existing ObjectStore wrapper limits
  data/deletion/transaction candidates to recognized direct-child native names,
  hides unsupported index payloads and temporary-manifest candidates, and preserves
  real manifest/reference discovery and listing errors. Unknown payload names
  remain untouched regardless of age; empty caller directories are not a
  preservation contract. Native cleanup's deletion/transaction age
  filters can still miss newer or clock-skewed unpublished files. Both paths
  then collect only unreferenced direct-child deletion/transaction names matching
  the pinned native formats and delete them through the same wrapped ObjectStore,
  without comparing timestamps. Scanning after native cleanup avoids deleting
  a path already removed by that cleanup. Exclusive protection stays outside
  the job future through native child draining, runtime joining and cleanup.

  Retained-history inspection adds metadata work to connection-triggered
  cleanup. The private scanner is deliberately limited to the pinned StateStore
  layout; branches, detached manifests, external base paths, indexes and
  unsupported row metadata fail before native history/data reclamation. Future
  dependency or format changes require requalification. The filename boundary
  applies to both native cleanup and the supplemental scan; an unreferenced file
  that matches a supported native filename remains eligible for reclamation.
  The reserved table is not general-purpose file storage; sibling/root user files
  remain outside these reclamation trees.

  Long-held snapshot leases can defer reclamation indefinitely. Permanent
  tombstones, complete decoded images and buffered multipart writes still
  determine the capacity envelope. Older prototype clients and direct native
  writers do not participate in leases; upgrades require cold cutover with all
  writers stopped. Owned staging files, buffers and live work must settle
  before replies regardless of later native-file reclamation.
  The [evidence ledger](docs/lancedb-state-store-prototype.md) separates pinned
  source reasoning, ordinary public-handle tests, native supported-hook probes,
  and platform execution. Historical receipts do not qualify later changes;
  accepted design reasoning does not invent results for an unexecuted platform.
  The ledger records measured capacity and actual consumer and platform results.
  No debugger or syscall-interception campaign is an active gate.
  A coherent cold backup requires every store, transaction and same-directory
  Database owner in every writer process to settle before copying the whole
  directory. Restore and upgrade validation use a separate directory; native
  format changes and in-place repair are not part of the public abstraction.
- Redb owns a bounded FIFO writer command queue and a bounded channel for each
  logical transaction. Logical handles may coexist; one native writer proceeds
  at a time. Dropped queued sessions cancel reservation, dropped admitted
  sessions roll back, and shutdown settles queued sessions. The existing shared
  read owner retains at most four decoded present bodies within a 16 MiB bound
  on their complete native encoded bytes. Every operation still reads its native
  transaction/table first; only an exact key and whole-body byte match reuses a
  decoded body. Ordinary point reads alone populate this memo; transactional
  reads only look up, lists decode directly, and puts never prime it. Eligible
  point-read decoding and admission share the memo lock. Transaction misses,
  oversize bodies and unavailable memo locks decode outside it. Parsed values,
  key storage and transient allocations are separate from the encoded-byte cap.
- PostgreSQL owns one primary client for direct reads plus one dedicated
  client/connection actor per transaction. Native transaction finalization
  remains actor-owned after admission. A failed nonterminal operation preserves
  its primary error separately from the actual native rollback receipt. The
  transaction's later consuming rollback reports that receipt; the primary
  error category alone never confirms cleanup. Lost actor replies remain
  uncertain.
- SQLite/Turso owns its retained database plus one dedicated connection actor
  per transaction. Busy timeout is native SQLite policy.

Every task and OS thread remains tracked through terminal cleanup. A timeout
that forgets a live join handle is not cleanup. Backend-specific cancellation
and rollback state stays separate; only identical task-join bookkeeping may be
shared.

## Internal Ownership

- Root dispatcher modules own the only cross-backend matches.
- Concrete database implementations remain under `src/databases/`.
- Concrete state implementations remain under `src/state_stores/`.
- Portable DTO modules import no backend SDK.
- A small concrete private `DatabaseRuntime` may own genuinely shared timeout,
  capability, and sanitized error behavior. There is no generic runtime or
  manager behavioral trait.
- Backend managers own native resources and receiver methods directly.
- Redb worker threads, PostgreSQL/SQLite transaction actors, SQL, tables,
  codecs, savepoints, transport payloads, and backend debug details never cross
  the public boundary.

### Lance mutation ownership

The embedded Lance adapter accepts filesystem paths and the exact `memory://`
selector. Native remote/file URI routing is rejected during configuration
validation. All catalog, data and index mutations share a bounded, private
worker owned by the supplied Database. The worker has no ownership reference
back to its joining handle. Each admitted operation gets an independent Tokio
runtime and retains the process/path and filesystem coordination guards until
that runtime has drained. The original deadline covers admission, lock waiting
and native execution; an uncertain timeout is never replayed.

Each connection uses distinct native sessions for namespace metadata and data
tables. Native table registry keys retain namespace accessors; sharing one
session would create a strong ownership cycle through those keys. Memory
connections register one private retained object store in both registries, so
table-specific options cannot split the database or discard its bytes when a
table handle closes. Clones share that connection; independently opened memory
connections remain isolated. The native ownership suite records the unsafe
shared-session counterexample separately from the supplied Database behavior.

The existing `LanceManager` owns catalog methods and native namespace lookup.
Admitted collection creation creates missing namespace ancestors through native
operations; ordinary reads never create namespace metadata. A successful native
listing must prove absence before the adapter returns an empty result or creates
a child. Native listing failures retain their error category. Schema operations
remain single native publications; unsupported combinations fail before mutation
as specified by the portable API.

Dropping an observation does not release the mutation guards. Final Database
owner drop closes admission and joins the worker without requiring progress on
the caller's Tokio runtime. Native blocking filesystem cleanup can extend that
join beyond the observer deadline. Upstream process-global CPU encoders are
not individually joinable; the inspected encoding closures retain data rather
than storage mutation authority. This ownership applies to the supported local
file/memory stores and does not claim coordination with external native writers
that ignore the adapter's advisory filesystem lock.

## Portability Floor

Portable success requires semantics implemented honestly in at least two
backend families. A single-backend capability is not promoted merely because
the native SDK exposes it. Unsupported behavior fails before mutation; the
adapter never creates synthetic ids, counts, scores, acknowledgements, or
traces.

`CapabilitySet` reports only optional retained operations. Neither the live
`Database` handle nor `DatabaseBackend` exposes backend identity or provenance,
and capabilities do not duplicate construction-mode selection.

## Cross-Crate Boundary

`StateStore` supplies durable control-plane state. `Database` supplies
application data and private agent-managed knowledge. Model-visible tools are
authored in `tool-sdk`; storage-sdk does not depend on tool-sdk. The built-in
handles and caller-owned trait implementations remain separate static types.

## Validation Ownership

- downstream tests prove trait signatures, associated transaction ownership,
  root/module exports, privacy, and absence of backend-ingress constructors;
- the [primary mock lane](../docs/mock-testing.md) proves direct trait behavior
  with process-local StateStore transactions and typed Database scripts;
  retained-state reconstruction is same-process evidence, not durability;
- deterministic adapter tests prove validation, native transaction actor
  ownership, cancellation, rollback, terminal joins, and redaction;
- local integration tests exercise Redb, SQLite/Turso, and embedded Lance
  StateStore behavior; Lance platform qualification is tracked separately;
- strict-live verification covers selected real engines/services, including
  supplied Lance products, supported Database modes, and alternative StateStores;
- source contracts guard only stable public/private architecture and deleted
  symbols, never incidental helper or filesystem topology.

Exact commands live in [`AGENTS.md`](AGENTS.md) and
[`docs/testing.md`](docs/testing.md).

Lance record batches retain the incoming field set for native validation while
using declared JSON field metadata to encode present JSON values. Scalar
assignments and equality, inequality and membership predicates use that same
metadata, so JSON-encoded
UTF8 and plain String fields preserve their distinct portable values. These
private codec and predicate choices add no public native-schema surface.

Range and pattern predicates reject Json fields through that same private
expression owner before executing native queries or writes. Applying UTF8
ordering to JSON storage bytes would not preserve the logical request; this
limitation does not change ordinary String predicates or Json equality.

## Build selection

Lance is unconditional for both products. The empty default feature set omits
alternative native backends. `redb`, `postgres`, `surrealdb`, `sqlite`, and
`qdrant` select concrete private dispatcher variants; they add no behavioral
trait or normalization route. Existing unconditional configuration DTOs remain
available, with typed construction errors when disabled. SQLite/Qdrant retain
their current feature-conditional public configuration surface. Their dependency
conflict is rejected at compile time and by verifier selection before building.

The local Lance native family is shared at LanceDB 0.40.0 / Lance 13.0.0 with
ObjectStore 0.14.2. LanceDB's `remote` feature is enabled only because 0.40.0's
unconditional job decoder references its gated `Error::Http`; the supplied
configuration remains local-only and no cloud object-store features are enabled.
The StateStore keeps
its established schema and exact file format 2.0; this dependency transition
requires cold-reopen and cleanup proof, not a new public or persisted API.
Remote Qdrant is pinned to server 1.19.2; Edge uses crate 0.9.0. These are
distinct native implementations of the retained portable operations. The
approved macOS/Qdrant browser version constraint adds no runtime route; its
resolver and downstream limits are documented in [feature guidance](README.md#features).
Edge applies nearest-query offsets internally; the adapter does not skip rows
a second time. Its configuration builders retain cached payload memory and
existing defaults, and configuration guards are released before helpers may
reacquire them. The seven workspace source patches and their nontransitive
downstream requirements are owned by the [dependency security policy](../docs/security-dependencies.md).
