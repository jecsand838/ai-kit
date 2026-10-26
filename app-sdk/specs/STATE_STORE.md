# Private Persistence Boundary

App-sdk exposes no storage handle, record, key, transaction, backend identity,
capability, repository, CAS, batch, or persistence policy. It composes only
the crate-root `storage_sdk::StateStoreBackend` and associated
`StateTransactionBackend` contracts.

## Ownership

One private `AppStore<CS>` consumes the caller-built
`CS: StateStoreBackend` and is the direct receiver for app records, keys,
codecs, scans, and closed app mutations. It awaits the portable trait contract
on the caller's active async runtime.
It creates no second runtime, OS thread, actor, queue, command envelope,
oneshot proxy, or storage-specific shutdown protocol. Backend thread affinity,
bounded native admission, rollback-on-drop, and transaction task joining stay
owned by storage-sdk, where LanceDB and the opt-in Redb, PostgreSQL, and SQLite/Turso implementations can preserve
their different native requirements.

`AppStore` is not an application lifecycle owner. Dropping it releases the
app's concrete store; it neither revokes application work nor attempts
to reproduce lower-store cleanup. Concurrent app operations rely on exact
transaction fences rather than serialization through an app-owned queue.

AppStore retains CS directly. The built-in/durable store sum and associated
transaction forwarding sum are deleted. Caller-built non-Clone CS and its
owned non-Clone Transaction remain exact through every operation; no concrete
storage implementation is imported by production App code.

Exact reads use the portable typed key operation. Prefix traversal constructs
`StateListRequest` with a validated `StateKeyPrefix`, advances its exclusive
`start_after` cursor from each response's `next_start_after`, and stops only at
an absent next cursor. Revision fences extract the numeric value only through
`StateRevision::get`. Each mutation obtains one owned, lifetime-free
associated transaction and consumes it by commit or rollback; transactions
are never cloned, shared, erased, or reconstructed.

All inventory and history scans use the same bounded page validation. Every
returned row must match the requested namespace and optional prefix, follow
the exclusive cursor in strict key order, and belong to a page within the
requested size.
A continuation cursor must identify the page's last row; empty pages cannot
continue. Whole-namespace preflight omits the prefix. Invalid page evidence
fails with Integration. Evidence found during preclaim startup scans is
rejected before generation claim or lower effects; validation itself leaves
native records and revisions intact.

A requested page size is an upper bound: a valid store may return shorter
nonempty pages. No fixed count of physical pages limits retained history,
preflight or definition-pin validation; valid traversal continues to EOF.
Those scans retain one bounded page and their finite configured inventories.
Workspace and active-run materialization enforce their existing actual record
counts while collecting, rather than imposing an accidental smaller limit on
stores which return short pages.

Workflow subdomains own invariant-bearing state and closed transitions.
Storage owns:

- versioned keys and bounded codecs;
- live transaction execution;
- exact reads and bounded prefix listings through the portable StateStore
  contract;
- mutation identity and receipt reconciliation; and
- transaction-pure execution of domain-owned terminal deletion.

Storage does not own an anemic public workflow model, a generic repository, or
domain decisions reconstructed from optional fields.

## Record Epoch

Ready App records use private epoch `app/v7`. The approved one-way migration
from `app/v6` runs inside the existing AppStore before the normal generation
claim. No other old epoch is accepted, and invalid v7 data never falls back to
legacy decoding. Public types and persisted logical identities are unchanged.

Deployment requires a cold cutover: stop older clients, settle all owners, and
take a whole-store physical backup. Revision and epoch fences cannot undo lower
effects already dispatched by an older client. The backup is the downgrade
path; there is no in-place downgrade, reset, or corruption repair.

Migration first performs bounded read-only validation of the entire supported
legacy inventory. Initial corruption, unsupported epochs, and applicable
revision overflow leave values and revisions unchanged. It then fences the App
root into Migrating, retaining its logical fields and recording one migration
identity, phase, and bounded progress. Only that exact state permits mixed v6/v7
records. Already sealed deletions finish before new page keys are introduced.
Each checkpoint, its newly introduced pages, and progress publish atomically;
unchanged pages reuse their converted predecessor's references. Other records
convert in bounded batches. Every batch fences the root and selected source
revisions; listing alone is never publication authority. Uncertain publication
requires exact identity/progress and intended-row reconciliation before replay.
A complete converted-inventory audit precedes Ready and the existing generation
increment. Interruption leaves a resumable, non-runnable migration. Rewritten
native StateStore revisions advance normally; unrelated keys and tombstones are
untouched. Downtime never replenishes activation deadlines.

Preflight revision accounting covers observable present records, including the
root's remaining publications and the existing cleanup of temporary mutation
receipts. The StateStore contract does not expose an absent key's tombstone
revision. A foreign, exhausted tombstone at a future page key therefore fails
that conversion transaction atomically; it cannot be discovered by read-only
preflight. Migration remains non-runnable and resumable, without resetting the
revision or treating uncertain publication as rejection.

The shared codec remains the sole record-format boundary. It owns one canonical
hydrated RunRecord/CheckpointRecord and one migration-only v6 decoder, not
parallel runtime schemas. Ready-v7 mixed epochs reject before lower effects.

Each checkpoint persists a settlement count and dense page-publication references.
An immutable occurrence-owned page contains at most 64 entries; at most 64 pages
represent the unchanged 4,096-entry maximum. Every non-final page is full. The
page key is beneath the occurrence's existing history prefix and identifies its
index and publication sequence, which must not exceed the checkpoint sequence.
A transition writes only changed pages and retains unchanged references; all
changed pages, checkpoint, head, indexes, and mutation receipt publish in the
same transaction. Pending, Intended, and Applied/Uncertain remain separate
publication boundaries. Copies and restores retain their existing semantics,
including clearing settlement evidence, and never share pages across occurrences.

Current and predecessor hydration reads their distinct pages within the existing
coherent transaction, validates their complete identities and contents, and then
performs the existing contextual checks and fresh-head fence. The four-record
memo may reuse intrinsic decoding only when freshly read root and all leaf
identities match; it stores no head, generation, or contextual authority.

Each settlement page preserves the original inline entry representation. Each
entry is exactly `[sequence, generation, control_sequence, target, state]`; the first three
fields retain their unsigned 64-bit semantics. The closed nested arrays are:

| Owner | Exact array |
| --- | --- |
| Target: Input | `[0]` |
| Target: Notification | `[1, attempt_number, notification_id, [seconds, nanoseconds], title, body, response_schema, dismissible]` |
| Target: Unknown | `[2, attempt_number]` |
| Target: Inputs | `[3]` |
| State: Pending | `[0]` |
| State: Intended | `[1, decision]` |
| State: Applied | `[2, decision, checkpoint]` |
| State: Uncertain | `[3, decision]` |
| Decision: Input | `[0, value]` |
| Decision: Allow | `[1]` |
| Decision: Deny | `[2, reason]` |
| Decision: Submit | `[3, content]` |
| Decision: Dismiss | `[4, reason]` |
| Decision: Accept | `[5, contribution]` |
| Decision: Fail | `[6]` |
| Decision: Retry | `[7]` |
| Decision: Inputs | `[8, keys, app_generation, execution_generation]` |

Every position is present, including a null absent response schema. Tags are
unsigned 8-bit integers; attempt numbers and nanoseconds retain unsigned 32-bit
bounds, and seconds, checkpoint and generation fields retain unsigned 64-bit
bounds. SystemTime serialization still rejects dates before the Unix epoch;
decoding preserves the existing checked seconds/nanoseconds normalization and
platform range checks. Object alternatives, incorrect arity/types and unknown
tags reject as corrupt. There is no old-object or dual-format decoder.

Strings, opaque JSON values, ordered user maps and key vectors retain their
original contents and insertion order. Payloads are not text-encoded; their occurrence-owned page is the only
normalized settlement entity. Full target, decision, notification and current/predecessor
semantic validation still runs before projection or publication. Array decoding
alone never proves a control, generation, clock, definition or native fence.
The private encoding does not add a persistence owner, registry or public API.

The unchanged 4 MiB limit measures the reconstructed inline-equivalent complete
record, not the smaller checkpoint descriptor or individual pages. Both the
replacement Unknown-row charge and the late Applied-state growth charge use
these same owning array encodings, including separators and maximum integer
widths. Non-ledger control/staging/phase reserves remain in their existing
encodings. The aggregate 4,096-target-generation limit and all deadlines remain
unchanged. Removing fixed field names cannot reduce logical payload capacity;
the exact historical payload size at which byte admission rejects can increase.
Native keys, revisions, atomic receipts, replay, cancellation, explicit deletion
and independent copies retain their existing contracts.

## Canonical Records

The smallest durable owner set is:

- application lifecycle, run count, the exact logical-agent-key to
  caller-revision map, and compact workflow revision selections;
- immutable workflow catalog entries containing the exact canonical definition
  once per workflow id and caller revision;
- workspace lifecycle and materialization progress;
- run identity and immutable workspace/definition/parent pins;
- immutable checkpoint rows and bounded indexes plus one mutable run head; and
- bounded mutation receipts.

One checkpoint owns sequence, semantic kind, action position/ordinal,
superstep/base sequence, ordered state, frontier, staged siblings, join ordinals,
one canonical control map and a data-carrying lifecycle phase. No duplicated
inline current/previous state or independent durable tracking truth remains.
The head retention object is closed for every variant: Active and Retained carry
only their state discriminator, while Deleting additionally carries its progress
cursor. Unexpected fields reject as corrupt durable state, including on variants
with no payload.
Run rows and their compact heads retain matching workflow and child revision
pins, never another copy of the canonical graph. During open, head pages
establish required revision bindings and every readable head receives bounded
semantic validation of its exact latest checkpoint and predecessor, when one
exists. This does not hydrate older unrelated checkpoints or promote unrelated
retained runs into execution inventory. Explicit history selection validates the
older rows it reads. New generation claims check/insert configured catalog
entries in the same existing transaction as the application claim. Exact
canonical content, including ordered user JSON, cannot change for a reused
id/revision. UTF-8 revision bytes are encoded in catalog keys without path
interpretation; the payload and key must agree. Catalog rows remain immutable
even when no retained run needs them, preserving future drift detection.
Callers need to supply only their configured revisions and the revisions pinned
by readable retained/active runs; old unused catalog entries are not runtime
bindings. Sealed deletion needs no executable definition to finish cleanup.

History projections and graph/step/child tracking readers derive from these
committed records. Runtime resources and lower records never enter storage.

## Typed Mutations

Every write is prepared by a functional owner as one closed `AppMutation`.
The mutation owns one unique identity and exactly one legal shape:

- claim a missing or revision-fenced application record for one new
  generation;
- advance only the shutdown field of one revision-fenced application
  generation;
- atomically admit an application/workspace pair;
- advance one revision-fenced workspace, with or without an application
  record update;
- delete one revision-fenced workspace;
- atomically admit an application/workspace/run triple;
- append one immutable checkpoint and its indexes while advancing one fenced run head;
- atomically update application, workspace, and parent run while admitting a
  nonempty unique child-run set; or
- copy a validated clean source boundary and admit a fresh restored run with
  lineage/capacity/checkpoint zero;
- release active capacity after terminal cleanup without deleting history;
- seal a terminal run and record bounded deletion progress;
- delete one bounded owned history/index page while advancing its progress; or
- finalize sealed run removal after all owned rows are proven absent.

There is no general mutation batch, arbitrary fence/change vocabulary, or
publicly composable put/delete builder. Generation claim and domain-admission
constructors accept typed records and captured revisions; lifecycle
advancement accepts only the generation and next shutdown state. Every
record-carrying variant derives physical keys and validates encoded key/value
identity before opening a transaction. Lifecycle advancement instead decodes
the exact fenced app record in its transaction and changes only `shutdown`.
An existing-generation claim transactionally requires the next generation,
unchanged epoch, required retained pins, and active count, and only the specified Closed to
Active reopen transition; a missing claim requires active state and a zero run
count at the canonical initial generation.
Cross-record workspace/run/parent-child identity is validated wherever a
variant spans records. Functional owners still validate higher workflow
invariants and retain exact prior/intended evidence for replay comparison.

Run admission, child admission, readmission and terminal capacity release may
rebase only a changed App `run_count` inside their original native transaction.
They load the current App record, require exact equality of epoch, generation,
shutdown, Agent revisions and definition revisions with the captured authority,
then apply only the variant's fixed count delta to the current count. A current
native revision equal to the captured revision also requires the exact captured
count transition; an older native revision is rejected. Underflow and the
existing App run-capacity limit remain enforced. No other changed App field is
accepted.

An earlier active-run observation and a later zero App count do not alone prove
corruption. Terminal cleanup revalidates that observation before failing App:
concurrent retirement or readmission remains nonfatal, while an active occurrence
with an unchanged zero-count App revision remains an Integration failure.

Checkpoint advancement uses the same App count-only comparison with a zero
count delta. Successful child capacity transfer or reduction decrements that
rebased native count only for the exact verified children whose active markers
are removed in the same transaction. Already retained children do not decrement
it again. Advancement writes the App record only when this count actually
changes; otherwise it leaves the App record and its revision unchanged.
Every other mutation retains its exact App revision fence.

These run-capacity mutations retain the exact workspace native revision fence
where present, together with the owning workflow's logical workspace revision.
They do not rewrite an unchanged workspace record. Exact parent/run/source
identity, native head/value/missing fences, once-only active markers and the
original mutation receipt still govern the whole transaction. Count contention
does not create a second retry owner, increase the existing retry budget or
authorize replay after an Unknown outcome.

One storage-owned epilogue executes every mutation:

1. encode and identity-check every caller-supplied intended record;
2. begin a live StateTransaction;
3. prove the mutation receipt is absent;
4. load every variant-owned revision or missing-record fence, permitting only
   the App count rebase specified above;
5. apply only the variant's transaction-pure puts or deletes;
6. write the mutation receipt in the same transaction;
7. commit or rollback;
8. after an uncertain backend commit, exact-read the mutation receipt before
   classifying the storage result; and
9. return one `Applied | Rejected(StorageFailure) |
   Unknown(StorageFailure)` outcome.

`Rejected` means the mutation did not commit. `Unknown` preserves the failure
classification but never authorizes blind replay. The functional owner exact
reads its intended domain records before deciding whether a retry is legal.
Receipts are bounded cleanup evidence, not a second history model. Only receipts
whose mutation has been proved `Applied` enter the AppStore retirement page.
At 256 entries, the owner takes the page and deletes eligible receipts through
one separate best-effort transaction, retaining epoch and identity validation.
There are at most 255 queued IDs plus 256 per in-flight cleanup. A partial idle
page may remain until subsequent mutations or reopening; it never includes an
unresolved mutation's evidence. Cleanup failure or cancellation does not change
the already-applied outcome and may leave durable receipts for the existing
startup sweep. Cleanup is never folded into another domain mutation.
The startup sweep also holds only one returned native page, deletes that page
through one bounded transaction, releases its values, then advances the validated
exclusive cursor. It never materializes the complete receipt inventory or keeps
all returned receipt values alive. A deleted cursor key remains an exclusive
lexicographic bound for the next portable list request.

Run admission retries a definitely rejected revision conflict within a bounded
budget. Each attempt retains the run identity and checkpoint-zero intent while
resampling app/workspace storage fences and revalidating lifecycle, capacity,
and the caller's logical workspace revision. An unknown outcome still requires
exact reconciliation before activation.

Exhausting that budget returns the definite conflict without publishing the
rejected run or consuming its capacity. Concurrent caller admission does not
guarantee that every caller wins publication. Only a proven rejected caller
operation may be newly authored after contention settles; an uncertain outcome
still requires exact receipt and domain reconciliation before any resubmission.

Generation claim is stricter because an app record contains no claimant
identity. An exact mutation receipt is the only proof that this opener claimed
the generation. An `Unknown` generation claim never authorizes App publication:
equality between the intended and observed app records cannot distinguish two
concurrent claimants that constructed the same next generation.

While a StateTransaction is live, only its own `load`, `put`, `delete`,
`commit`, and `rollback` operations or transitively transaction-pure receiver
methods may be awaited. External calls, timers, capacity waits, channels,
unrelated storage operations, tracing, and lower crates are forbidden.

A failed transaction cannot be committed. An unknown commit is never repeated
until exact reads establish the prior state.

## External Effects

Durable intent commits before a lower effect. The terminal result or truthful
unknown observation commits afterward. Lower calls never run under storage
mutation authority.

A reservation generation fences split control. Identical replay reattaches;
conflicting targets conflict; unrelated owners receive `Busy`; cancellation
may monotonically replace control authority. A stale generation can never
commit.

Child admission uses the same `Applied | Rejected | Unknown` commit outcome.
Only `Applied` with the exact parent relation, control, map identity,
definition, materialization, and child checkpoint-zero evidence may activate a
child. `Unknown` retains capacity and recovery ownership but cannot activate,
blindly requeue, or infer success without exact durable evidence.

## History, retention and serializable fences

Retain terminal state/results/history until explicit deletion. Active capacity
releases independently. Immutable sequence rows plus a captured head cutoff
provide App snapshot pagination over ordinary ordered exclusive StateStore
lists; list traversal itself is not a native snapshot. No new storage history,
snapshot or transactional-list method is permitted solely for App convenience.

Stable task identity and exact occurrence use existing control-plane owners.
An occurrence head/history/active key includes the admitted creation token;
all parent membership, mutation and deletion fences include that same token.
The logical RunId App-checkpoint and sequence-match links advance atomically
with each eligible App checkpoint, including original ordinal order for
checkpoint-zero admissions within one batch. Saved-action and action-match
links advance only for actual Action/Failed observations; control, terminal
and admission checkpoints never replace those saved-action matches. Mutable index
entries retain exact neighboring references for bounded unlinking; deletion
updates affected links/tails in its same page transaction. Readers never order
random tokens or scan all history to find the latest remaining match. No new
counter, generic index framework or permanent retired-key tombstone is added.
The existing mutation owner validates all touched references/record-byte and
child-count bounds before native admission; one logical publication is never
split across transactions. These links never replace another occurrence's
head or rows. The v7 normalization preserves this behavior; migration is one-way and never dual-writes formats.
A history append atomically writes row/index/head/receipt. A fork atomically
reads/fences its clean source, copies state, admits capacity and writes lineage.
The head's bounded unresolved-failure ordinal-to-sequence evidence changes in
that same append transaction: record the latest actual failed observation,
preserve it through retry/control changes, and prune an ordinal only when a
successful commit reaches it. Its size and ordinal distance use the existing
MAX_FRONTIER bound; fresh/restored local histories start empty. Readers combine
this sparse evidence with bounded binary search of monotone committed
positions to validate the existing observation index. They never load all
history or add a second generic index owner. Sealing and deletion preserve the
same head evidence and revision fence until final removal; no deletion page
reinterprets failed observations or requires their removed rows to remain.
A child policy's copied-result deletion authorization is part of the existing
parent barrier transaction and fences each exact child occurrence. The same
bounded child count and record/transaction limits apply before admission; do not
split one logical parent barrier across commits to fit a policy. Reuse the
existing terminal-head seal directly whenever cleanup fences permit it. If the
head must retain an authorization before sealing, it contains only the original
parent occurrence/checkpoint proof, and startup attaches the existing bounded
deletion owner. No storage trait, transaction-list API, separate retention queue
or periodic garbage collector is added. State and outcomes remain private to
their existing run records; the authorization does not copy result payloads.
A terminal deletion atomically seals admission, then deletes bounded pages with
progress/receipt updates, then removes the final head/scope indexes. Each page
is restartable and exact-receipt reconciled; no giant all-history mutation or
per-reader pin registry. Closed seals reject new reads/forks; new reads after
completed removal return NotFound. Concurrent mutations use exact transaction fences.

App relies on the owning StateStore guarantees for serializable read/write and
missing-key conflicts, read-own-writes, exact revision/tombstone semantics,
atomic multi-key commits, cancellation safety and rollback-on-drop. The lower
suite must prove these with non-Clone public consumers and actual native lanes,
including PostgreSQL. App cannot synthesize stronger lower semantics or expose
native transactions to compensate for missing proof.

## Codecs And Privacy

Record consolidation precedes codec consolidation. Each functional
persistence owner defines one versioned encoding. Storage retains one bounded
JSON/key primitive; it does not add macros, generated tables, generic
serialization frameworks, or parallel authoritative record projections. The
explicit global field/tag codec exception belongs to WorkflowState; App record
codecs sample it only at documented user-state serialization boundaries.

Records may contain app identities, caller workflow state, safe
classifications, public lower workspace or conversation ids required for
scoped recovery, and finite counters. `AppRecord` stores each logical agent
key with only its caller revision. No record stores an Agent id. Records never
contain credentials, lower private records, native ids, raw errors, prompts
copied outside the canonical definition/control, tool or provider payloads, or
runtime resources.
