# Embedded Lance StateStore production evidence ledger

This ledger records the implementation before the subsequent
[feature split](feature-split-qualification.md). Its default-backend and feature
lane statements describe those recorded candidates; current feature selection
and commands belong to [testing](testing.md).

The embedded StateStore feasibility result remains accepted as **proven**.
The production implementation and its scoped macOS/APFS and Linux/ext4 evidence
are complete for the recorded candidates. See [production completion](#production-completion--2026-10-09)
for current results, source identities, tooling continuations and evidence
limitations. Earlier prototype, incomplete-platform and intermediate production
receipts below are historical; their original failures and source scopes remain
unchanged. Public APIs, defaults and backend feature policy are preserved.

The existing [portable contract](PORTABLE_API.md#state-store) is authoritative.
This backend preserves it without upstream patches, dependency upgrades,
new behavioral traits, or a recovery journal. Existing defaults and features
remain unchanged. Qualification covers memory and local directories on macOS
and Linux; hosted services and Windows are outside scope.

## Maintenance directory-sync batching — retained

The reviewed durability proof, focused semantic checks and fixed comparisons
support retaining this measured change. The
[current report](feature-split-qualification.md) separates completed scoped
feature-split qualification from the historical Agent live failure and failed
capacity screen. The subsequent withdrawal of those test scenarios does not
turn either historical failure into a pass or demonstrate a production repair.
The change affects only obsolete-file
deletion settlement during exclusive directory maintenance. App, memory mode,
public contracts, formats, reference selection, scheduling, publication guards,
deadlines and workload counts are unchanged.

After immediate durable hint removal, fresh opening and retained-reference
validation, the existing I/O ledger batches only recognized direct-child data,
deletion and transaction files under the canonical private table. It adopts each
descriptor before unlink. At most 64 descriptors share one exact parent; the
64th unlink flushes the group. Parent changes and excluded deletions flush first,
including old manifests. Native cleanup flushes before surviving-reference
enumeration; the orphan supplement flushes and disarms before staging cleanup
and final reopen. Every flush retains the complete ancestor walk and every
file's `sync_all`, including the pinned macOS file barrier. This descriptor
bound is independent of the unchanged 64-version maintenance threshold.

Every ancestor and retained-file barrier is attempted even after a prior error.
Failure stops later deletes before opening or unlinking another file. Failed
descriptors remain ledger-owned through early return or native unwind. After
children drain and the runtime is destroyed, the existing finalizer makes one
finite sweep, closes descriptors while exclusive protection is held and resets
batching without clearing failures. Joined attempts do not turn uncertain
durability into success. Existing poisoned-ledger handling stays fail-closed.

The proof assumes valid retained history and cooperating lease owners. Current
and tagged references remain protected by existing validation and deletion
selection. Pinned native cleanup already deletes unreferenced payloads before
old manifests; flushing before manifest deletion preserves this ordering. An
interruption may leave obsolete files or incomplete old history. It does not
promise arbitrary historical checkout during interrupted GC. Source proof,
native extension-point tests, supplied-adapter tests, process recovery and
hardware power loss are different evidence categories; the last is untested.
No new fixture, durability counter, worker, journal, cache or runtime owner is
introduced. Both complete App ABBA comparisons and the Storage complete-cost
gate passed. The unchanged growth screen returned 1,449.344 seconds, above
1,200; no 4,096-delivery execution followed.

## Preparation-file durability candidate — 2026-10-13

This section retains the original preparation-only qualification status.
Subsequent fixes and execution are recorded in the current report above;
the results here apply only to their original source and invocation.

This later working-tree candidate is **measured and retained; scoped release
qualification remains incomplete**. Storage platform gates, scoped live coverage
and packaging pass, while App deterministic failures and Clippy findings remain. The
[current execution report](feature-split-qualification.md#preparation-file-durability--measured-release-qualification-incomplete)
records passing focused checks in both profiles, the outcome-neutral timeout
correction, preparation and combined-candidate comparisons, and their tradeoffs.
The original missing optimized-profile timeout witness remains historical failed
evidence; actual timeout coverage is credited separately to its unchanged
production inputs. Both growth runs joined successfully, but their
**1,816.833-second** screen prohibits a 4,096-delivery attempt. Historical
two-barrier descriptions and receipts below describe their original candidates;
they do not qualify this protocol or close capacity.

`DurableIo` stores the exact native state-table path derived from the canonical
configured directory and the existing table constant. Its native-file grammar
is shared with orphan selection. Only recognized native data, deletion and
transaction paths receive the preparation policy; names select eligible roles,
but successful non-replacing creation establishes ownership. Update requests
are rejected before classification. Copy explicitly keeps the existing policy.

Preparation writes create a same-directory staging file with `create_new`,
write the complete payload, hard-link it to the destination without replacement,
unlink their owned staging name, synchronize the complete existing ancestor
chain and synchronize the retained file descriptor before returning success.
The staging-cleanup walk supplies that one directory walk. Manifests, hints,
unknown writes and copy keep their initial and final file barriers. Rename and
deletion are unchanged. Buffered multipart completion uses the same classified
write path; abort releases only unpublished memory.

The ownership proof assumes valid retained history and cooperating writers
following the existing lease protocol. An existing preparation destination is
never overwritten, adopted, truncated or removed after a collision. A newly
installed preparation cannot enter a successful manifest publication until its
write returns after the final barrier. Before that barrier, a crash may leave an
incomplete unreferenced orphan. Failed installed preparations remain for the
existing reference-validated maintenance; no eager deletion compensation is
added. All successful preparation barriers precede authoritative publication.

Preparation collisions enter the existing failure ledger; manifest collision
handling is unchanged. The final file barrier is attempted even after another
failure. Primary errors retain their type, supplementary cleanup/barrier errors
remain recorded, and later cleanup cannot erase them. Failed staging cleanup
remains owned. There is no suspension in this filesystem sequence; native-child
draining, runtime destruction and protection release retain their original
ordering.

Pinned Lance assigns transaction-sidecar cleanup ownership only after its write
succeeds. A failed create therefore cannot arm cleanup for the existing winner's
sidecar. A later native error can remove this attempt's successfully created
sidecar even after uncertain publication. Inline manifest transaction readback
remains available; neither own-UUID readback nor cleanup alone proves
nonpublication or turns a recorded failure into success.

Rust 1.94.1's `File::sync_all` uses F_FULLFSYNC on macOS and fsync on Linux.
[Apple's synchronization interface](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html)
and the [Linux fsync contract](https://man7.org/linux/man-pages/man2/fsync.2.html)
inform this ordering. Source reasoning and ordinary recovery tests do not
establish hardware power-loss behavior or every filesystem implementation.
Native extension-point probes, public adapter execution and source review remain
separate evidence categories.

## Image advancement during feature-split qualification

The later Agent/App continuation advances the existing decoded-image cache
after a successful local commit. This change is outside the recorded production
candidates below; their receipts remain historical. Current execution evidence
belongs to the [feature-split qualification report](feature-split-qualification.md).

`LanceEngine::commit` retains the final authored overlay while the unchanged
batch iterator consumes cheap record clones. Only successful guarded publication
at the original version plus one permits `LanceImage::committed` to derive a
candidate from the validated immutable image and that overlay. Existing keys
are replaced, new keys appended, and tombstones retained with their exact
authored revisions. The candidate records the same version and UUID used by
publication; it contains no Dataset or snapshot lease.

The worker installs that candidate only after `LanceCommand::run` has drained
native children, destroyed their runtime and checked durable cleanup. The
original snapshot protection remains held through those steps. A failed job or
cache promotion discards the candidate and invalidates cached state; finding the
attempt's UUID after an uncertain publication error cannot manufacture
success or justify promotion. Read-only commits do not create an image. Older
transactions keep their immutable images unchanged, and replaced or discarded
values retain the existing iterative last-owner disposal.

Every ordinary load, list and begin still opens a fresh Dataset and validates
schema, storage format, row-ID mode, expected version and transaction UUID
before reusing an image. External identity changes force full decoding and
validation; failed freshness resolution never returns cached success.
Initialization and maintenance continue full persisted-image validation rather
than deriving an image from a prior cache.

The optimization avoids a postcommit full decode but adds O(N) record/key-index
metadata cloning for N retained logical rows and O(W) temporary overlay metadata
for W authored keys. JSON payloads share Arc owners. These allocations and
retained old images are real capacity costs, not a claim of constant memory or
bounded record size. The ordinary macOS workload has no available RSS counters,
so its memory effect remains unmeasured. Baseline/candidate measurements must
compare combined commit-plus-first-observation cost, separate first load from
first transactional observation, witness maintenance, and report available
memory metrics and regressions. No performance result for the changed candidate
is inferred from the older receipts in this ledger.

## Ordered implementation gates

1. Prove native original-snapshot publication, a reproducible unguarded write
   skew, exact-version guarded delta publication, transaction UUID readback,
   durable object-store interception, and cleanup behavior.
2. Add selectable construction and run the shared StateCases against memory
   and persistent supplied handles.
3. Exercise serializability, revision/tombstone rules, failures, cancellation,
   accepted commit authority, and independent connections/processes.
4. Protect the private state table from supplied Database operations and
   exercise same-directory knowledge storage.
5. Complete ordinary public-handle cancellation, caller-runtime loss,
   contention, filesystem-failure and crash/reopen checks; run the supported
   platform/feature lanes and record measurements and remaining evidence gaps.

The privileged debugger/syscall campaign is withdrawn. Its executables,
fixtures and active gates were removed. Do not rebuild, run or replace it with
tracing privileges, relaxed container security, syscall interception, or an
equivalent workaround. Ordinary supported native ObjectStore hooks, disposable
local datasets and supervised test-owned processes remain in scope. Native
hook probes establish engine assumptions; they never substitute for public
supplied-handle tests.

Whole-image overwrite is a test reference only. Native reference tests do not
qualify the supplied adapter. Conflict means proven nonpublication; other
finalization errors preserve uncertainty. Finding our UUID never manufactures
durability or success. Process-crash recovery, synchronization evidence, and
machine/power-loss evidence are distinct.

[Tweetxvault's single-table batching design](https://github.com/lhl/tweetxvault/blob/main/docs/PLAN.md)
informs the one-publication write set. [dlt's destination state recovery](https://dlthub.com/docs/general-usage/state)
and its [LanceDB state support](https://dlthub.com/docs/dlt-ecosystem/destinations/lancedb)
inform the fresh-read/recovery checks. These projects are design references,
not evidence that this Kit's serializability, revision, error or lifecycle
contracts hold. The supplied-adapter tests below own that evidence.

Each implementation stage requires tests first, recorded red/baseline evidence,
focused green checks, and independent review. Record missing execution as
incomplete evidence for that invocation. A required upstream or abstraction
change remains outside the accepted implementation boundaries.

## Environment and dependency record

- Original prototype checkout: clean, macOS host, Rust/Cargo 1.94.1. This
  continuation preserved the existing dirty implementation and untracked tests.
- Native package versions: Lance and lance-table 7.0.0, object_store 0.13.2,
  rustix 1.1.4. LanceDB remains 0.30.0.
- The initial approved manifest changes added four direct edges to
  already-locked packages with default features disabled. That initial lockfile
  diff changed only Storage's edges. The separately reviewed production cloud
  feature pruning below subsequently removed unreachable graph entries;
  retained package versions, sources and checksums remain unchanged.
- The existing serde_json dependency additionally enables `float_roundtrip`,
  an empty feature in pinned serde_json 1.0.149 that adds no package or
  dependency. The initial `unbounded_depth` attempt was removed after the
  acknowledged deep-value regression; the current private codec traverses
  containers iteratively and delegates scalar parsing to serde_json. Default
  features are unchanged.
- The first isolated Linux build followed the repository's floating `stable`
  channel to Rust 1.99.0 and failed in already-locked `ethnum 1.5.2` with E0512
  (`TryFromIntError` layout). It did not compile or test the prototype. The
  qualification image now explicitly selects Rust 1.94.1, matching the host
  and the crate's declared Rust 1.94 minimum. No dependency was patched or
  upgraded to work around the newer compiler. Rust 1.99 remains unqualified.
- The first resumed Rust 1.94.1 image lacked `google/protobuf/empty.proto`;
  the pinned Lance build stopped before compiling the adapter. Debian's
  `libprotobuf-dev` is now an explicit qualification-image prerequisite beside
  `protobuf-compiler`. This changes test tooling only. The incomplete build is
  retained under `/private/tmp/lance-state-qualification-20261008/combined-red/`.
- `cargo metadata --offline --format-version 1` resolved the new edges, then
  failed because `antithesis_sdk 0.2.8` was not cached. This is dependency
  availability evidence, not a behavioral test or prototype failure.
- During the initial continuation, ordinary Docker access failed with permission denied
  on its Unix socket. No access escalation or security change was attempted.
  Linux execution was unavailable for that receipt. The later approved override
  rerun below targets macOS only; no historical Linux controller receipt fills
  the remaining Linux qualification gap.

## Execution evidence

The historical macOS checks below passed at their recorded source states.
They are not a receipt for the current checkout. Complete qualification
requires the current public-behavior/native checks, ordinary Linux execution,
the supported feature lanes, independent diff review and resolution of every
remaining contract gap. Partial evidence does not imply a complete-contract
claim.

### Test controls and review

- Added public tests before adding construction/backend code. The first
  `cargo test --locked -p storage-kit --no-default-features --test
  lancedb_state_store -- --nocapture --test-threads=1` failed as expected with
  E0432 (missing Lance configuration) and E0599 (missing Lance variant), before
  production implementation. This is API compilation red evidence, not a
  behavioral failure. Full log: `/private/tmp/lancedb-state-store-red.log`.
  Local resource settings are `CARGO_PROFILE_DEV_DEBUG=0`,
  `CARGO_PROFILE_TEST_DEBUG=0`, `CARGO_INCREMENTAL=0`, `CARGO_BUILD_JOBS=2`.
  Subsequent canonical checks use `CARGO_BUILD_JOBS=1`; Cargo commands run
  serially, including across host and container builds.
- Independent source review found the native transaction probes correctly
  retain original snapshots, forward affected-row information, disable retries
  and cleanup, and use fresh exact-version UUID readback. These remain native
  assumptions, not supplied-adapter evidence.
- Independent review of the public tests found coverage for existing shared
  cases, reopening, present/absent/tombstone write skew, ABA, lost updates and
  initialization races. Failure injection and lifecycle qualification remain
  separate requirements.
- A scripted comparison of both lockfiles confirms identical package names,
  versions, sources and checksums, with only storage-kit dependency edges
  changed.
- Native baseline command: `cargo test --locked -p storage-kit
  --no-default-features --test lancedb_native_feasibility
  --test lancedb_durability_feasibility -- --nocapture --test-threads=1`, using
  the resource settings above. **16 passed, zero failed/ignored** on macOS.
  Six transaction probes include the unsafe unguarded counterexample and
  guarded original-version commits. Ten durability probes demonstrate actual
  wrapper interception, barrier errors, runtime-abort cancellation, explicit
  cleanup draining, native staging leakage after failed completion, and ignored
  abort/version-hint errors. Full log: `/private/tmp/lancedb-native-baseline.log`.

### Supplied adapter: macOS/APFS

- Platform: macOS 26.7 (25G229), Apple Silicon, APFS (`/dev/disk3s1`),
  Rust/Cargo 1.94.1.
- The first implementation passed 11 supplied behavior cases. Further tests
  produced actual behavioral failures: the Database catalog exposed the
  private table, supplied Database creation could overwrite it, and removing
  all manifests could reinitialize existing state. Fixes reserve every supplied
  Database operation and serialize initialization with a safe filesystem lock
  while refusing to recreate a nonempty private dataset. A separate failing
  test proved case-insensitive aliases needed the same reservation on APFS.
- Red logs: `/private/tmp/lancedb-state-boundaries-red.log` and
  `/private/tmp/lancedb-state-casefold-red.log`. The following focused run passed
  21 cases: `/private/tmp/lancedb-state-boundaries-green.log`.
- Final expanded command at this stage: `cargo test --locked -p storage-kit
  --no-default-features --test lancedb_state_store --test strict_env_contract
  --test source_contract -- --nocapture --test-threads=1`. **33 StateStore,
  10 verifier-contract, and 4 source-contract tests passed**, zero failures or
  ignored tests. Log: `/private/tmp/lancedb-state-macos-final.log`.
- Those 33 include two clean-I/O overwrite-reference cases, shared delta and
  reference concurrency scenarios, independently opened stores and supervised
  processes, actual post-acknowledgment process kill/reopen, 12 MiB JSON,
  malformed persisted-state fixtures, full-u64 revision overflow, maintenance
  interference, exact repeated-operation increments, owner retention,
  unpolled futures, rollback/drop, and StateStore/Database coexistence.
- A prior run passed 30 cases but failed the workload's macOS RSS measurement
  because the sandbox denied `ps`. The same test passed with process-inspection
  permission; the measurement was not skipped or replaced with a constant.
- A test-first verifier change failed until `scripts/check.sh` included the
  three new embedded/native targets in every existing supported lane. The
  verifier-contract test now passes. The no-path guard separately rejected a
  redundant explicit integration-target path; using Cargo's conventional
  target discovery fixed it without changing the guard. Both no-path and
  no-type-alias guards pass.
- `cargo clippy --locked -p storage-kit --all-targets --no-default-features
  -- -D warnings` passed; log: `/private/tmp/lancedb-clippy-first.log`.

### Native encoder ownership correction

Independent final review found a material gap in the initial implementation:
Lance 7's default 2.1 structural encoder dispatches work to a private global
CPU runtime. Draining the job runtime cannot join that executor's remaining
encoding work after an early failure. A small batch size is not an ownership
proof, and the global executor exposes no supported drain or injection API.

The pinned engine also supports native format **2.0**. Its previous primitive
encoder uses `tokio::task::spawn` on the calling runtime and propagates encoding
JoinErrors. It supports every column in the private state schema. Delta writes
inherit the original dataset's format. Stable row IDs are disabled to avoid a
separate detached split-stream path. This is an existing native configuration,
not a patch, dependency change, schema-limit reduction, or public abstraction
change.

Two added tests failed before the fix: newly created state had native format
2.1, and a fresh supplied read accepted an incompatible-format fixture. Log:
`/private/tmp/lancedb-format-red.log`. Creation now explicitly selects 2.0 and
disables stable row IDs. Every opened image validates both settings before
scanning, including reconciliation readback; incompatible datasets fail without
migration or repair. The full supplied target subsequently passed **35 tests**,
zero failures or ignored tests: `/private/tmp/lancedb-format-green.log`.

Two additional native probes exercise an actual multipart child returning an
error or panicking after delegated local I/O, then require error propagation,
owned cleanup, runtime destruction, fresh readback and subsequent progress.
They qualify native child handling; they do not exercise a private encoder
panic or prove every shipped-adapter cleanup path. Independent reviewers checked
both the configuration/source reasoning and the actual added tests.

### Historical first JSON codec correction

Independent review produced four more public behavioral regressions before
changing production. Memory and disk stores acknowledged a programmatically
constructed depth-140 JSON value, then failed fresh reads because the default
parser rejects nesting beyond 128 levels. Other valid values changed a floating
point bit when parsed: for example `51.248178375505404` changed from
`0x40499fc44f1b2f60` to `0x40499fc44f1b2f61`.

The three-lane check reached the Qdrant lane with 35 supplied cases passing and
these four failing. Logs: `/private/tmp/lancedb-json-red.log` and the complete
`/private/tmp/lancedb-storage-three-lanes.log`. Earlier default/no-default lane
passes therefore do not constitute a final three-lane qualification.

That first correction used the pinned parser's exact float-roundtrip option
and an explicit deserializer with its default recursion limit disabled. A final `end`
check still rejects trailing data. Four public regressions exercise fresh reads,
list values, unrelated progress and disk reopen. No new package, version, value
size limit or depth limit was introduced. This removes the artificial parser
cap; recursive JSON values remain subject to process stack and memory resources.
The focused serial no-default-feature run then passed **39 supplied tests**,
zero failures or ignored tests: `/private/tmp/lancedb-json-green.log`.
Independent review passed. The final feature-lane rerun was pending at that
historical receipt; current continuation results appear below.

### Withdrawn historical tooling

Earlier Linux controller-only smoke receipts were recorded under
`/private/tmp/lancedb-gdb-smoke/run1/` and `run9/`. They are historical provenance
only: they did not qualify the supplied adapter, and the campaign is no longer
an active gate or permitted continuation path. No current PASS may rely on
these controls. The withdrawn shipped-adapter scenarios were not completed.

The supplied adapter already preserves primary errors when staging cleanup or
the final retained-file synchronization also fails. The durability ledger keeps
the secondary error, and `LanceCommand::cleanup_context` appends it while
retaining the primary portable variant. This is existing source behavior,
not a pending implementation change or an executed combined-failure receipt.

### Continuation: acknowledged deep JSON regression

Two public-handle regressions isolate memory and directory stores in ordinary
owned child processes. Each constructs a depth-1536 JSON value, requires commit
acknowledgment, then reads/lists it, checks unrelated progress, and reopens the
directory store. Both reached their acknowledgment marker and then aborted on
the worker's recursive decoding path before completing. This is behavioral
red evidence: removing serde_json's default depth limit fixed depth 140 but
did not make deeper acknowledged values safe to read. It is not a compiler,
environment or upstream failure. The children disable core files and are
reaped by their parents; no debugger or process instrumentation is involved.

The old 39-test receipt predates these two regressions. The continuation replaces
recursive container encoding/decoding with explicit traversal stacks and uses
one private last-owner domain for iterative cleanup. Load/list replies retain
private owners until synchronous conversion after receipt, so observer loss
cannot recursively drop unobserved public payloads. Partially decoded and
duplicate-replaced values receive the same cleanup. The public Arc-based DTOs,
scalar parser behavior, float-roundtrip feature and object ordering remain
unchanged; `unbounded_depth` is removed. No new record-depth limit, enlarged
worker stack, dependency upgrade or public API change is used. The continuation
receipts below record focused green execution and the additional unpolled-put
red/green correction.

### Ordinary native outcome probes added for continuation

Four new supported-ObjectStore tests distinguish an error after publishing
the authored UUID, a stale writer whose exact target contains another UUID,
unavailable exact-version readback despite an actual authored publication,
and a genuine conditional-create collision with a separate cleanup-hook error.
Every successful filesystem operation delegates to real local I/O; the
read-error control uses only Lance's supported application-level hook. These
are native baseline probes, not deliberately failing production regressions.
All fourteen native durability cases pass in the continuation lanes. These
results establish engine assumptions; source mapping separately supports the
supplied adapter's reconciliation and combined cleanup branches.

### Representative workload measurements

The serial macOS run in `/private/tmp/lancedb-json-green.log` authored 32 multi-key state transactions alongside 64 knowledge
inserts. Each state step includes a 256 KiB checkpoint, 64 KiB notification,
and 8 KiB history value. Four quiescent checkpoints measure retained-version
growth; all 58 retired keys are subsequently recreated inside rolled-back
transactions to check revision continuity without removing their tombstones.

| State transactions | Retained tombstones | Shared-directory bytes | Files | Authored JSON bytes |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 0 | 5,093 | 13 | 0 |
| 8 | 10 | 2,737,202 | 49 | 2,698,478 |
| 16 | 26 | 5,481,515 | 92 | 5,396,980 |
| 24 | 42 | 8,232,722 | 135 | 8,095,490 |
| 32 | 58 | 10,990,825 | 178 | 10,794,000 |

Observed state transaction latency was 100.0 ms median and 113.1 ms p95;
throughput was 10.1 transactions/second. Four contention rounds produced four
commits and four conflicts with no partial receipts. Whole-process RSS changed
from 175,152 to 182,528 KiB. After contention the shared directory held 11,028,904
bytes in 198 files.

Payloads are compressible. At step 32 the physical-growth/authored-value-byte
ratio was 1.0178; it includes knowledge writes, metadata, and old versions and
is **not** a device-write amplification measurement. RSS includes native caches
and the integration process. These are bounded functional workload samples,
not production capacity estimates or a new acceptance threshold.
Linux separately records process `write_bytes`, `cancelled_write_bytes` and
`wchar` at the same quiescent checkpoints. Those counters measure kernel I/O
accounting for this process and waited-for children, not device durability or
isolated StateStore writes. They are explicitly unavailable on macOS; that
platform's physical-growth ratio does not replace a device-write measurement.

## Contract evidence matrix

This matrix summarizes the prototype's macOS/APFS results, extended by the
production receipts below, and the pinned source proofs. Linux execution
remains unqualified for every applicable
platform obligation. Direct supplied-adapter tests, native probes and source
reasoning are distinct evidence categories; none is silently substituted for
another. The earlier socket-fixture failures remain recorded as historical
failures; the approved ordinary macOS rerun subsequently passed the complete
three-lane Storage gate without source changes.

| Obligation | Direct supplied-adapter evidence | Native evidence and implementation reasoning |
| --- | --- | --- |
| Unchanged static API and unique transaction | Public API, downstream backend and source-contract targets pass in all three lanes; construction remains additive | Traits, associated types, errors and DTOs are unchanged; concrete exhaustive dispatch and unique transaction ownership are retained |
| Serializable read-modify-write | Present/absent/tombstone skew, lost update, ABA, memory clones, separate connections/processes, three-way namespace cycles and subsequent progress pass | Original immutable V, unconditional sentinel, exact V+1 guard and atomic conditional creation prevent successful stale publication; native unsafe counterexample and guarded reference cases pass |
| Exact revisions and tombstones | Repeated equal puts/deletes, rollback/drop, absent/deleted recreation, MAX-1 to MAX, overflowing put/delete and complete-overlay invalidation pass | One checked increment per authored mutation, permanent positive revisions, no physical tombstone removal or automatic replay |
| Atomic cross-namespace publication | One fresh verification transaction observes complete data/deletion/receipt sets; losing sets remain absent | One table and one authoritative manifest publish the overlay atomically |
| Fresh loads and exact list pages | Portable namespace/prefix/UTF-8 ordering, exclusive cursor, limit continuation and stale/fresh view cases pass | New Dataset/Session per ordinary read; validated immutable page image; Rust byte ordering; pinned stale-hint successor probing/full-list fallback |
| JSON and observation ownership | Depth-4096 arrays/objects/mixed values, deep cleanup/error paths, abandoned private replies, existing byte representation and scalar differential cases pass; the production preserve_order run passes the complete 85-case target, extending the earlier 10-case JSON selection | Explicit heap stacks, scalar/key parsing by pinned serde_json, Arc::into_inner last-owner disposal, eager public put forwarding, private reply owners and synchronous public conversion; no new depth limit or format migration |
| Corruption and no repair | Schema/control/revision/JSON corruption, incompatible format/row IDs, missing manifests and creation races fail explicitly; ordinary filesystem obstruction publishes nothing and recovers after fixture restoration | Full-image validation, guarded initialization, nonempty-dataset protection and retained history; fixture restoration is test-owned, never production repair |
| Exact conflicts and uncertain finalization | Public conflicting receipts stay absent; real filesystem failure remains BackendOperation with no revision consumption | Fourteen native durability probes cover data/manifest failures, own/other/unreadable UUID outcomes and separate collision/hook errors. Source mapping preserves uncertainty, and retains the primary variant plus cleanup context. Private combined unlink/barrier branches are source-reviewed, not directly injected |
| Accepted commit authority | Actual Pending observer cancellation, caller-runtime loss, final-owner joining, parent reopen after reaping, actual observation timeout, bootstrap timeout and 96-observer pressure pass | Complete command transfers into bounded queue; observer loss cannot revoke worker authority; timeout limits observation/admission rather than native completion |
| Settled cleanup and native task ownership | Public rollback/drop, retained owners, joined child completion and subsequent progress pass | Native part-child error/panic and multipart cleanup probes plus selected scan/decode/merge/DataFusion source proof; per-job runtime drains children, joins blocking work and checks ledger before reply |
| Durable local acknowledgment | APFS ordinary writes, reopen, real I/O obstruction and acknowledged process-kill/reopen pass | File, directory and ancestor barriers surround conditional publication; failed barriers prevent acknowledgment. No machine-power-loss or Linux execution claim |
| Process-local memory | Portable memory cases, isolated connections, shared clones and concurrent transactions pass | One retained InMemory object-store identity per owner; no persistence claim |
| Database coexistence and reservation | Same-directory state/knowledge use and all reserved-table operation families pass; the earlier 21 focused Lance Database regressions are followed by the production complete 68-case Storage strict-live gate including Lance | Private state table stays outside supplied Database catalog/data/query/index access, including ASCII case variants |

The candidate uses no upstream change, dependency version upgrade, public
abstraction weakening, journal or secondary database. The remaining two-platform
qualification gap is Linux execution, not a currently demonstrated implementation
defect. Full consumer/release gates remain separately scoped; see the invocation
receipts below for their actual results and outstanding work.

## Review record and limitations

Historical independent agents reviewed the engine/record, durability, owner,
configuration/dispatch and test diffs at their recorded source states. The
withdrawn tooling's review is not a current gate or supplied-adapter receipt.
Every continuation change requires independent review of its actual diff.
Reviews are source evidence, not substitutes for the execution matrix.

No upstream code, dependency version, public behavioral trait, portable record
or error contract, default backend, or feature policy was changed. Native
foreign-trait implementations use the boxed futures required by those existing
upstream extension points; no new behavioral trait or public erased backend was
introduced. The bootstrap lock contains no recovery state and is not a journal.

Windows, external object stores, hosted `db://`, paired Database/StateStore
memory construction and direct native mutation of the private table remain
outside scope. The original prototype also excluded version reclamation and
production capacity qualification: every acquisition decoded a complete scan,
and native versions accumulated. The production continuation below adds image
reuse, lease-governed private history reclamation and measured capacity.
Automatic native indexing/unconstrained compaction remain disabled; logical
tombstones stay permanent, changed images still require complete scans, and
multipart buffers consume memory. The timeout bounds admission/observation,
not forced native filesystem interruption or final joining.

Actual machine/power-loss testing has not been performed. A killed process,
executed synchronization path, injected syscall error, and actual device power
loss are distinct evidence categories. Unrelated remote services and complete
workspace strict-live gates have not been claimed.

### Implementation decisions from executed probes

The durable wrapper must own disk writes and buffered multipart staging;
delegating to native local uploads loses cleanup errors and paths. A per-job
ledger must retain even errors ignored by native version-hint handling. The
manifest guard checks that ledger before publication, and final acknowledgement
checks it again after child cleanup and runtime joining. Buffered multipart
uploads trade memory for explicit cleanup ownership in this prototype.

Each transaction retains a fully validated, decoded snapshot plus its native
Dataset and an overlay. Its nonterminal operations contain no suspension
point: unpolled futures perform no work, while polled operations finish or
invalidate atomically. This avoids outstanding native transaction reads.
Snapshot acquisition and ordinary reads/listing cost O(all retained records),
including tombstones. This is an explicit performance limitation, not a new
record count, payload limit, transaction expiry, or portable semantic change.

### Pinned-source continuation review

The following locations were independently read in the already-locked 7.0.0
registry sources. They explain the implementation; they do not claim runtime
execution of otherwise unexercised paths.

| Concern | Pinned native source | Conclusion and limit |
| --- | --- | --- |
| Encoder selection | `lance-encoding-7.0.0/src/encoder.rs:290` and `src/previous/encoder.rs:161` | Format 2.0 selects the previous core strategy. The private schema's scalar, UTF-8 and binary fields select its primitive encoder; they do not select the structural 2.1 encoder. |
| Encoding-task ownership | `lance-encoding-7.0.0/src/previous/encodings/logical/primitive.rs:424` | Encoding uses `tokio::task::spawn` on the current job runtime and maps its JoinError into a native error. The writer awaits ordered encoding results in `lance-file-7.0.0/src/writer.rs:363`. After an early parent error, the adapter must still drain the runtime because dropping remaining join handles alone does not join their tasks. |
| Excluded global encoder | `lance-encoding-7.0.0/src/encodings/logical/primitive.rs:5193` and `lance-core-7.0.0/src/utils/tokio.rs:111` | Structural encoding calls `spawn_cpu`, which submits to a separate global runtime. Pinning format 2.0 removes this specific unowned encoding route; it is not a proof about every possible native query task or a private encoder-panic execution receipt. |
| Commit ownership | `lance-7.0.0/src/dataset/write/commit.rs:183` | A Dataset-backed builder takes the handler from the Dataset. Installing the handler when loading the original snapshot is required; a later builder setter is insufficient. The transaction properties are assigned directly to the prepared transaction before execution. |
| Rebase before the first attempt | `lance-7.0.0/src/io/commit.rs:927` and `:966` | Zero retries still permits native merge preflight to load newer transactions and rebase before its one publication attempt. The supplied exact-target guard is essential even when sentinel rows do not overlap a metadata or maintenance commit. It rejects the resulting later target before native publication. |
| Atomic publication and hints | `lance-table-7.0.0/src/io/commit.rs:1396` and `:349` | The conditional handler publishes one manifest with `PutMode::Create`; only AlreadyExists/Precondition become native commit collisions. The later version hint is best effort. The supplied guard checks its own I/O ledger around the handler so an ignored hint failure cannot become successful acknowledgment. |
| Native multipart cleanup | `lance-io-7.0.0/src/object_writer.rs:323` | Dropping an unfinished multipart writer spawns abort work on the current runtime and ignores its returned error. The supplied wrapper owns buffered parts, upload counts, staging paths and error receipts; the owner drains native children before destroying the runtime and checking the ledger. |

The supplied publication path retains the original snapshot, stages a sentinel
with the complete overlay, forwards affected-row metadata and sets zero native
commit retries. The handler rejects any manifest other than the recorded next
version before conditional creation. Every successful mutating transaction
therefore claims exactly that next version; a competing state or maintenance
commit cannot cause authored decisions to replay against a later snapshot.
Read-only transactions can serialize at their original snapshot.

Ordinary load/list and transaction construction each use a new Dataset builder,
without a supplied version, manifest or Session. Native
`lance-7.0.0/src/dataset/builder.rs:633` creates fresh metadata/index caches;
`:782` resolves the latest location before reading its manifest. The selected
`file-object-store` route is not the optimized local route
(`lance-io-7.0.0/src/object_store.rs:543`), and its provider marks listing as
unordered (`src/object_store/providers/local.rs:30`). Therefore native latest
resolution consults the optional hint and probes successive manifest versions
with fresh HEAD requests (`lance-table-7.0.0/src/io/commit.rs:372` and `:413`).
It cannot trust a stale hint alone: it checks each successor until NotFound.
Missing/malformed/ahead hints and non-NotFound probe errors fall back to a full
maximum-version listing (`:552` and `:612`). Local reads obtain current file
metadata through `object_store-0.13.2/src/local.rs:421`, not a hint cache.

This fresh-read reasoning depends on the supported local filesystem and
contiguous immutable manifest history. The adapter publishes only the next
version and disables version cleanup, so a failed or reordered hint write
cannot create a history gap. Direct private-table mutation/deletion remains
unsupported. A read concurrent with publication may observe the prior version
and serialize before that publication; a manifest acknowledged before the read
begins is present for resolution. Memory mode uses the retained InMemory store
and its ordered listing, without version hints. Native
`LANCE_USE_VERSION_HINT` is sampled once, defaults on, and accepts `0`, `false`
or `off` to choose plain listing instead (`lance-table-7.0.0/src/io/commit.rs:308`).
Correctness does not require that variable to be unset, but the hint-failure
probe requires it unset to establish that it actually exercised the hint path.

The shipped error mapping distinguishes prepublication native conflicts from
ambiguous finalization. A manifest-collision error becomes a portable conflict
only when fresh exact-version readback identifies a different transaction.
An own UUID, missing UUID or unreadable version does not prove success or
nonpublication. Other native errors remain backend errors. At the worker
boundary, successful publication plus cleanup failure returns an error; a
primary conflict plus cleanup failure retains the conflict and appends the
cleanup detail. The wrapper excludes ordinary AlreadyExists collisions from
its failure ledger while separately retaining secondary cleanup/barrier errors.
These conclusions are source evidence; native-hook probes do not exercise the
private supplied wrapper's combined-error branches.

The directory wrapper writes and synchronizes a new staging file, conditionally
hard-links or renames it into place, synchronizes ancestor directories, removes
owned staging state and synchronizes the retained file descriptor again before
acknowledgment. No suspension point can discard that synchronous publication
sequence. Copy, rename and deletion also propagate their synchronization
failures. Runtime shutdown and final owner joining may wait for filesystem
work; observation deadlines cannot safely force it to stop. Ordinary local
tests and source inspection establish neither device power-loss behavior nor
every filesystem's barrier implementation. Those limits remain explicit in
the final verdict.


## 2026-10-08 continuation receipts

These receipts apply to the continuation implementation, not the historical
attempts above. Host: macOS 26 / Darwin 25.6.0, arm64, APFS (local, journaled).
Compiler: `rustc 1.94.1 (e408947bf 2026-03-25)`. Commands use `--locked`,
`CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`, and dev/test debug information off.
Lance 7.0.0, lance-table 7.0.0, LanceDB 0.30.0, object_store 0.13.2 and
serde_json 1.0.149 remain unchanged. The lockfile diff adds only four direct
Storage dependency edges; no package identity/version/source/checksum changes.

### JSON red, correction, and focused green

The iterative codec first corrected the original depth-1536 decode abort.
Independent review then found that the two opaque `put` forwarding layers still
captured the raw `StateRecord` inside unpolled async futures. An added public
small-stack caller test reproduced that destructor overflow in both memory
and disk child processes: **0 passed, 2 failed**, each aborting in
`small-stack-public-put`. Receipt:
`/private/tmp/lancedb-continuation-unpolled-red.log`.

Both forwarding layers now construct the concrete Lance future immediately;
the backend adopts the incoming Arc before first polling or validation. The
returned future still performs no mutation until polled. A concrete `Either`
keeps static dispatch without boxing, erasure, an API change or a new trait.
The same supervised tests now pass (**2 passed**) with depth-4096 arrays,
objects and mixed shapes; iterative test assertions/disposal keep the test
harness from hiding adapter failures. Receipt:
`/private/tmp/lancedb-continuation-json-green.log`.

Coverage includes transaction reads, replacement, deletion, rollback/drop,
reopen, rejected deep input after earlier staging, malformed partial roots and
trailing data, duplicate-key displacement, abandoned load/list/begin replies
with actual Pending witnesses, and final owner cleanup. Shallow differential
cases compare accepted/rejected grammar and encoded bytes with pinned
serde_json, including number boundaries, exact floats, escaping, Unicode and
object iteration order. Both ordinary-map and App preserve-order execution
passed; the final validation receipt below records their separate invocations.

### First focused verifier result

`RUSTUP_TOOLCHAIN=1.94.1 bash storage-kit/scripts/verify_lancedb_state_store.sh
/private/tmp/lancedb-continuation-embedded` passed **54 StateStore tests,
8 native transaction/format tests, and 13 native durability tests**, zero
failures or ignored cases. Its log is
`/private/tmp/lancedb-continuation-embedded/embedded.log`; summary and host
metadata are beside it. Child-entry tests are included in target counts;
their parent tests actually invoke the supervised scenarios.

The supplied adapter produced actual Pending witnesses for admitted observer
cancellation, caller-runtime destruction, and last-owner destruction. The
parent reaped the final-owner child before independently reopening and checking
the atomic receipt, exact revisions, retained tombstones, and subsequent
progress. The configured 100ms commit observer timed out on the first 8MiB
ordinary workload; final joining followed by fresh readback found the complete
receipt. The ordinary bootstrap lock timed out and recovered after release.
Of 96 observers, all were Pending at inspection, 32 were cancelled, and 64
completed; subsequent work and final joins succeeded. These observations do
not claim knowledge of an internal I/O phase or exact queue occupancy.

New public transaction cases passed for memory clones, independent processes
with absent/tombstoned decision keys, a three-way cross-namespace mixed
put/delete/receipt cycle, and present/tombstoned `MAX-1` revisions followed by
`MAX`, overflow invalidation and reopen continuity. Verification uses one
fresh transaction for multi-record atomicity; losing writes and receipts stay
absent.

The measured 32-transaction workload completed in 2.956s (10.82 transactions/s),
median 94.091ms and p95 114.127ms. It checked 58 retained tombstone revisions,
64 concurrent knowledge inserts and four conflict pairs. Final shared storage
was 11,028,841 bytes in 198 files. This is a bounded compressible workload,
not a capacity threshold or isolated device-write measurement. macOS optional
process-memory and Linux-specific I/O counters were reported unavailable;
all behavioral assertions still ran.

A fourteenth native durability case was subsequently added to cover a genuine
conditional-create collision together with a separate hook failure. Its final
execution receipt belongs to the full lane runs below. This native hook test
must not be presented as execution of the shipped adapter's private
staging-unlink/final-descriptor combined-error branch.

### Independent continuation review

Two reviewers inspected the actual codec, private reply, transaction, and
forwarding changes; both identified the forwarding defect before its
correction and found no remaining blocker after correction. Separate review
covered the transaction/lifecycle tests and the exact-version source proof.
The proof above identifies the pinned mechanisms; reviewer agreement alone
is not qualification evidence. No privileged instrumentation was used.

### Historical qualification scope after prototype continuation

The three compilation/lint/documentation lanes, preserve-order checks, focused
real embedded Database regressions, locked isolated-candidate build, and pinned
source analysis completed. The final validation receipt records exact results.
The first full test-gate attempt failed because existing TCP fixtures could not
bind in the sandbox. The user then approved an ordinary macOS test-command
sandbox override, and the complete canonical gate passed; its receipt is below.
Linux was not rerun as part of that macOS retry. Overall two-platform
qualification remains **UNPROVEN** pending Linux.


### Selected native read/merge executor ownership

Independent inspection also traced the selected query path, with Lance 7.0.0,
Object Store 0.13.2 and DataFusion 53.1.0. It found no task transfer to an
unowned global executor on this configuration:

| Selected path | Pinned source and ownership mechanism |
| --- | --- |
| Full scan | `lance/src/dataset/scanner.rs:1953` calls `execute_plan`; `lance/src/io/exec/scan.rs:287` spawns fragment readers on the current job Tokio runtime. |
| Format-2.0 decode | `lance-encoding/src/decoder.rs:1480–1511` spawns decode tasks on the current runtime, awaits handles and propagates JoinErrors. Scheduling at `:2180–2210` is inline or current-runtime spawn; `:2019–2049` awaits completion and aborts on early stream drop. |
| Native I/O | `lance-io/src/scheduler.rs:423–439,583–610` starts loops/reads on the current runtime. `:803–819` closes the queue on scheduler drop. The adapter's drain settles remaining tasks. `object_store.rs:608–650,659–712` routes `file-object-store` through wrapped readers, excluding the separate `file+uring` infrastructure. Local reader blocking tasks belong to the current runtime. |
| Merge | `lance/src/dataset/write/merge_insert.rs:1550–1670` selects the full-schema fast plan, with the join/projection at `:1372–1459`. `merge_insert/exec/write.rs:277–355` uses a polled map with stable IDs disabled, excluding the split-stream branch at `:589`. Applying deletions in `merge_insert/exec.rs:64–107` uses bounded polled futures. |
| Repartition | `datafusion-physical-plan/src/repartition/mod.rs:390–408` owns producers/error-forwarders through `SpawnedTask`; `datafusion-common-runtime/src/common.rs:40–60` delegates to current-runtime Tokio spawn/blocking spawn and `:108–110` aborts on drop. |
| Coalescing | `datafusion-physical-plan/src/stream.rs:63–165` owns a current-runtime JoinSet, propagates task failures and cancels producers on stream drop. |

The cached DataFusion SessionContext in `lance-datafusion/src/exec.rs:405–456`
contains resource managers, not an independent Tokio executor. `execute_plan`
at `:596–620` executes in the calling context. The static decoder `WAITER_RT`
belongs to the blocking take/decode API; this path uses asynchronous
`schedule_and_decode` in `lance-file/src/reader.rs:915–959`. Global `spawn_cpu`
routes in structural 2.1 encoding and vector/index/FTS operations are excluded
by the validated scalar schema, native format 2.0, plain scans, disabled stable
row IDs and `use_index(false)`.

Consequently, returned/error streams release their producer owners, the worker
drains remaining job-runtime tasks, and runtime destruction joins blocking
work before the ledger check and response. Revisit this proof if any of those
settings or pinned dependencies change. This is source-supported ownership,
not an execution claim for every internal failure, a forced-I/O deadline, or
power-loss/Linux qualification.


### Completed unpublished files versus owned cleanup

This section records the earlier prototype before production maintenance.
The current orphan-only correction and its evidence are documented
[below](#orphan-reclamation-correction-and-source-proof).

The portable contract requires settled native operations, rollback/resource
cleanup and task/thread joining, not immediate physical space reclamation.
Lance 7.0.0 `dataset/write/merge_insert/exec/write.rs:877–952` can finish fragment
and deletion files before returning the metadata transaction. Native
`io/commit.rs:1110–1130` removes the attempt's transaction file on commit failure;
`skip_auto_cleanup` disables the successful-commit version-GC hook at
`:1084–1091`. `dataset/cleanup.rs:9–32` separately classifies abandoned
transaction data/deletion files as unreferenced material for later GC.

Because this prototype disables automatic GC, completed unreferenced files may
remain indefinitely after contention or failure, alongside retained versions
and tombstones. This is a real disk-growth/write-amplification cost, not a
promise to clean native history online. It does not relax ownership of Kit
`.state-stage` files, multipart buffers, cleanup errors or live tasks/threads;
those must settle before terminal receipts. No unverified reclamation was added.

### Continuation validation receipt before the approved sandbox override

Final implementation, tests, manifests, lockfile and Storage scripts have source
digest `077b35c7bcba687969b3e50bacaf938e6ff53a46b8859725acaf08f4321d4534`.
The manifest `/private/tmp/lancedb-continuation-gates/final-source.json` records
all 154 paths and per-file SHA256 values, plus the digest algorithm (sorted
relative path, NUL, file SHA256, LF). The original checkout remains at base
`1a6b722051e23dadf0168674df599fcb4f288f1a` with its existing and continuation
changes uncommitted. No upstream source, dependency version, public trait,
portable error or default backend was changed.

All commands used Rust 1.94.1 and the low-memory settings recorded above;
dependency-resolving commands used `--locked`. Commands were serialized.

| Invocation / scope | Current result |
| --- | --- |
| `bash storage-kit/scripts/verify_lancedb_state_store.sh /private/tmp/lancedb-continuation-final-embedded` | PASS: 55 supplied StateStore tests, 8 native transaction/format tests, 14 native durability tests; 77 total, zero failures or ignored cases |
| `cargo test --locked -p storage-kit --test lancedb_state_store -- --nocapture --test-threads=1` | PASS: all 55 default-feature cases, including the final ordinary filesystem-failure regression |
| All-target compile (`cargo test --locked -p storage-kit --all-targets --no-run`) in default, no-default, and no-default + Qdrant lanes | PASS in all three lanes |
| Full deterministic target set in all three lanes, using `--no-fail-fast` | Incomplete gate: all Lance and other non-socket targets passed; existing TCP-fixture failures remain, detailed below |
| Public API / caller StateStore backend / source contracts | PASS: 8 / 3 / 4 tests in each lane |
| Caller Database backend / local existing StateStores | PASS: 2 Database tests per lane; 12 local StateStore tests in default, 8 in each other lane |
| `cargo clippy --locked -p storage-kit --all-targets -- -D warnings` with each lane's feature flags | PASS in all three lanes |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked -p storage-kit --no-deps` with each lane's feature flags | PASS in all three lanes |
| `cargo test --locked -p storage-kit --doc` with each lane's feature flags | PASS: 250 default, 240 no-default, 246 Qdrant doctests |
| No-default `lancedb_state_store` target with `--features serde_json/preserve_order` and filter `json` | PASS: 10 tests; 45 intentionally outside this focused JSON invocation |
| No-default `strict_live` target with filter `database_cases::lancedb_` | PASS: 21 real embedded Database tests; 34 outside this focused invocation |
| `cargo fmt --all --check`, `git diff --check`, `bash scripts/check_no_path_wiring.sh`, `bash scripts/check_no_type_aliases.sh` | PASS |
| Fresh isolated-candidate all-target no-default build with a new target directory | PASS: locked compilation and test-executable linking completed in 19m39s |
| Ordinary Linux execution | Unavailable: Docker Unix-socket access denied; no Linux filesystem or behavioral result can be reported |

The canonical `storage-kit/scripts/check.sh` was invoked and stopped at the
existing Surreal TCP fixtures. Subsequent explicit serialized lane commands
ran every deterministic target with `--no-fail-fast`, followed by each lane's
Clippy, rustdoc and doctests. This is evidence for those individual commands,
**not** a passing canonical wrapper. Each lane's `strict_env_contract` had
8 passed / 2 failed, and `surreal_partition_deadline` had 0 passed / 4 failed.
The Qdrant lane additionally had 0 passed / 27 failed in
`qdrant_rest_completion`; that target correctly contains zero tests when its
feature is disabled. All these failures occurred at ordinary local TCP bind
with `PermissionDenied` / OS error 1 before the intended fixture interaction.
They were not skipped or converted into success. The default lane initially
ran 54 Lance cases; the final standalone 55-case invocation above includes the
subsequently added filesystem obstruction test.

That last public test obstructs only its own disposable dataset's data path
with an ordinary file. It observes `BackendOperation`, unchanged native version,
and absent data/receipt together in a fresh transaction. After restoring the
path, a new write succeeds at revision 1 and survives reopen. It neither
intercepts syscalls nor claims to execute every private synchronization branch.

Gate commands, exit codes, timings and logs live in
`/private/tmp/lancedb-continuation-gates/`; `results.json` records the three-lane
commands individually. Final focused evidence lives in
`/private/tmp/lancedb-continuation-final-embedded/`, including compiler/platform
metadata and the verifier's scoped PASS / overall UNPROVEN summary. The final
default test receipt is `final-default-state.log` in the gate directory.

The isolated candidate at `/private/tmp/lancedb-continuation-candidate` includes
tracked and untracked prototype files; immutable historical `tmp/**` is excluded.
Its fresh-build snapshot was `6c5e748856fdebbb09155b96a940b90eeb09e9e1`, and
all 154 source/build/test paths match the working tree byte-for-byte. The
candidate was clean before and after compilation. Final documentation is
included in the subsequent clean snapshot recorded by
`final-clean-candidate.json`, with a locked cached build recheck. This creates
no commit in the user's original checkout. Build logs are `clean-build.log`
and `final-clean-build.log` in the gate directory.

**Verdict at this earlier receipt: UNPROVEN.** The demonstrated deep-JSON defects are corrected, all
available embedded public tests pass, and independent diff/source reviews have
no outstanding correctness finding. The selected evidence standard combines
direct supplied-adapter execution, clearly separated native probes, and the
pinned mechanism proofs above. It does not justify a 100% macOS-and-Linux
contract claim while Linux execution and required full test-gate success remain
unavailable. There was no privileged fault campaign, actual power-loss test,
broader remote-service qualification or workspace release qualification.
Redb remains the default. No further implementation correction was identified
at that receipt. The next qualification work was to rerun the
documented ordinary gates in permitted macOS/Linux environments and investigate
any real failures. The following receipt closes the macOS gate gap.

### Approved ordinary macOS qualification rerun — 2026-10-08

The user explicitly approved sandbox overrides for this rerun. An initial
ordinary loopback bind still returned `PermissionDenied`; the standard Storage
check script then ran with the approved command-level override and exited
**0**. It ran only normal builds, tests, linting and documentation tools. No
debugger, syscall interception, tracing capability, relaxed container security
or withdrawn fault campaign was built, restored or executed.

Host: **macOS 26.7, Darwin 25.6.0, arm64, APFS (local, journaled)**. Compiler:
`rustc 1.94.1 (e408947bf 2026-03-25)`. The 154-file source/build/test digest
remains `077b35c7bcba687969b3e50bacaf938e6ff53a46b8859725acaf08f4321d4534`;
no production code, test, manifest, lockfile or script changed for this rerun.

```bash
RUSTUP_TOOLCHAIN=1.94.1 CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 \
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 RUST_TEST_THREADS=1 \
  bash storage-kit/scripts/check.sh
```

| Canonical lane | Integration tests | Doctests | Build / Clippy / rustdoc |
| --- | --- | --- | --- |
| Default SQLite/Turso | 120 passed | 250 passed | PASS |
| No default features | 116 passed | 240 passed | PASS |
| No default features + Qdrant | 143 passed | 246 passed | PASS |

Every lane ran all **55 supplied Lance StateStore**, **14 native durability**,
and **8 native transaction/format** tests. All 4 Surreal TCP cases and all 10
strict-environment cases passed in every lane; all 27 Qdrant TCP cases passed
in the enabled lane. There were no failed or ignored cases. Formatting passed.
Cargo commands stayed serial and dependency resolution stayed locked; no
Storage all-features lane was used.

Full output: `/private/tmp/lancedb-macos-qualification-retry-20261008.log`.
The adjacent `.json` receipt records the command, exit code, compiler/platform,
source and log hashes, and per-target results. This successful canonical
wrapper supersedes the earlier macOS socket-blocked gate result, while
preserving that earlier invocation as historical evidence.

The matching-source 10-case preserve-order JSON run, 21 focused real embedded
Database regressions, architecture guards, fresh locked clean-candidate build,
and independent implementation/source reviews remain applicable. Independent
validation review identified no other unexecuted macOS-only obligation in the
approved prototype plan. Documentation changes only record this new result.

**macOS/APFS verdict: PASS** for embedded process-local memory and directory
storage in the tested environment under the selected evidence standard. Public
behavior, native engine probes and pinned-source reasoning retain their
separate labels; this does not claim direct execution of every private combined
error branch, selected native I/O-boundary cancellation, actual power loss,
all macOS versions/filesystems, remote-service qualification or workspace
release qualification. **Overall macOS + Linux verdict: UNPROVEN pending
Linux.** Redb remains the default and all public abstractions remain unchanged.

## Production continuation — 2026-10-08

The user accepted core feasibility as proven after the macOS rerun. This stage
preserves every public abstraction, configuration type, current Cargo default
and backend feature. It does not implement the separate proposal to make Lance
the default or change backend selection and strict-live policy. Production
acceptance adds measured capacity, actual Agent/App consumers, cold recovery,
packaging and ordinary platform release execution to the accepted design.

### Verifier TDD and execution scope

Seven meaningful public script-behavior cases were added to
`tests/strict_env_contract.rs` before changing the focused verifier. The
recorded red invocation was:

```bash
cargo test --locked -p storage-kit --no-default-features \
  --test strict_env_contract embedded_verifier_ -- --nocapture --test-threads=1
```

Result: **1 passed, 6 failed**. Evidence:
`/private/tmp/lancedb-production-20261008/verifier-red.log`. The old verifier
already refused to overwrite a receipt directory. It failed the new scoped
receipt, per-target error preservation, pinned compiler, nonempty-suite,
unchanged-source and interruption-receipt obligations. These are actual
orchestration failures, not adapter failures or fabricated native failures.

The corrected verifier runs each of the three real embedded targets separately,
with `--locked` and `--no-default-features`. A new evidence directory owns
per-target logs and a terminal JSON receipt after owned-process cleanup.
`PASS`, `FAIL` and `INCOMPLETE` describe that execution only; the former
hardcoded aggregate `prototype_qualification: UNPROVEN` field is removed.
Source manifests include untracked implementation and test files, before and
after execution. Rust 1.94.1, lockfile hash, actual platform and available
temporary-dataset filesystem evidence are recorded. Missing metadata remains
explicitly unavailable. Rust tests stub command tools only to validate this
orchestration; those stubs are never native-engine or supplied-adapter evidence.

Independent review then identified omitted compiler flags/wrappers in the
receipt and the test watchdog's need to own the verifier's separate command
group on failure. The actual verifier was exercised with fixture executables:
it first omitted supplied `RUSTFLAGS` and `RUSTC_WRAPPER`, then recorded them
after correction. Red/green logs are
`/private/tmp/lancedb-production-20261008/verifier-build-metadata-{red,green}.log`.
No real Cargo invocation or engine build occurred in this orchestration check.
The interruption test now terminates and observes that owned command group if
its independent watchdog fires, while still reaping its direct verifier child.
Independent re-review confirmed those findings are addressed and found no
remaining identified correctness issue in the verifier changes. At that point
the Rust execution receipt was still outstanding. The later complete Storage
gate passed all 17 `strict_env_contract` cases in each of its three lanes,
including the seven new verifier cases. Those are orchestration results, not
additional native-adapter tests. Historical earlier receipts do not qualify
changed files; the final candidate's actual executions are recorded below.

### Release prerequisites and operational documentation

The first production-phase ordinary daemon inspection succeeded and reported:
Docker Desktop 29.8.1, Linux kernel 7.0.14-linuxkit, aarch64, 16 CPUs and about
7.75 GiB daemon memory. No container was started or built by that inspection.
The daemon reports an overlayfs storage driver; this is not evidence of the
filesystem holding a future test dataset. The Linux lane must record that
filesystem inside its actual ordinary execution environment, without added
capabilities or security changes. The host remains macOS 26.7 / arm64 / APFS,
with Rust 1.94.1 installed.

A subsequent root-side sandbox probe could not access the Docker socket.
Daemon availability and this caller's sandbox access are distinct prerequisites.
No Linux build or execution is claimed by either probe; any ordinary access
override remains separate from the prohibited instrumentation and container
security changes.

The package manifest excludes `vendor/**`, retaining repository research
archives outside the published crate. These five archives total roughly
37 MiB and are not referenced by Storage source or tests. At this initial
stage, package-list, packaged verification and downstream construction checks
were still outstanding; the metadata change alone was not a downstream pass.
Their subsequent executed results appear in the package receipts below.

The README now documents transactional receipt reconciliation, whole-directory
cold backup and restore, and cold upgrade/cutover using existing owners and
constructors. Every writer, transaction and same-directory Database must settle
before copying. There is no public backup API, live-copy coherence claim,
automatic replay or Redb-to-Lance migration. Logical tombstones remain permanent; leases govern retained native history.
Capacity evidence must include both costs.

### Implemented production costs and maintenance ownership

The candidate now opens a fresh Dataset and validates its version/transaction
identity before reusing a decoded image. It never caches the Dataset or lease.
Distinct retained versions still retain complete images, and changed images
still require a full scan/decode. Listing retains at most limit-plus-one
borrowed candidates, with O(N log L) selection rather than sorting/cloning the
whole image. Delta encoding moves up to 128 rows per batch; full-image rewrite
encoding borrows up to 128 rows. This limits adapter row buffers, not bytes per
record or native plan/multipart memory.

Private maintenance uses shared snapshot protection and an exclusive
maintenance lease, including independent directory connections and processes.
At 64 retained native versions, it validates the full image under exclusion,
publishes a guarded full-row delta preserving every revision/tombstone, settles
native work, then reclaims prior history while retaining the current version.
Orphan cleanup is requested on connection and after 64 failed native mutation
preparations. Blocked/failed maintenance is deferred for another 32 completed
jobs. Hint removal precedes deletion; cleanup reopens and validates the
remaining image. There is no public maintenance API or caller transaction
expiry. Long-held leases can defer reclamation indefinitely.

These mechanisms replace the earlier prototype's disabled-GC and every-read
full-decode costs. Historical measurements above describe that earlier code.
The candidate's actual release envelope must come from matched source and
build-profile runs. All older prototype processes must stop before upgrade,
because they do not participate in the new snapshot leases.

### Initial production-stage execution receipt

The preliminary capacity target passed **15/15 cases in 425.88 seconds**:
`/private/tmp/lancedb-production-20261008/performance-baseline-1.log`. This is
not the final paired baseline: explicit mutation warmups were added afterward.
It cannot be compared as if it had those later inputs. The target covers eight
capacity profiles, five contention/knowledge scenarios, retained transaction
images at 1/8/32, and a fixed 16 MiB payload split over 1/16/128/1024 keys.
The less-compressible profile repeats one pseudo-random 16 KiB row payload and
therefore still favors compression across rows. Finalization throughput is
measured after owned work joins; retained file bytes are not device write
amplification. macOS process memory metrics remain explicitly unavailable.

The initial App Lance deletion baseline hit the existing 20-second fixture
deadline in all four selected cases. This is a recorded timeout result, not a
demonstrated semantic failure; optimization and rerun evidence was still needed.
The focused optimization red run recorded **3 passed, 1 failed** before its
production correction. At this initial receipt, the candidate's four focused
targets were running under root coordination in
`/private/tmp/lancedb-production-20261008/candidate-focused-1.log`. The subsequent
complete focused results and App rerun appear in the current receipts below.

Disposable release fixtures were prepared at
`/private/tmp/lancedb-production-20261008/release-fixtures/`. They include a
then-uncompiled packaged consumer, an existing-version-only lock seed, source
snapshot tooling and ordinary Linux commands. The consumer exercises memory,
directory conflicts, atomic receipts, tombstone continuity, reopen,
same-directory knowledge and direct caller traits. Preparation ran no Cargo,
Docker build or container. Later package verification and locked downstream
execution passed in their separate recorded invocations; source freezing and
Linux runs remain independently evidenced gates.


### Unreachable cloud dependency pruning

The candidate retains `lancedb = "0.30.0"` and changes only its enabled native
features to `default-features = false`. This does not change Storage's public
feature/default policy or configuration surface. Existing `LanceConfig`
validation permits filesystem paths and `memory://`, rejecting native cloud,
`db://`, HTTP and `file:` URI routes before connection. The StateStore uses its
existing memory/directory location enum and private `file-object-store` route.
Pinned `lance-io 7.0.0` registers memory, file and file-object-store providers
unconditionally (`src/object_store/providers.rs:290`); cloud providers are
feature-gated. Pinned LanceDB and namespace implementation dependencies already
disable the native Lance/IO cloud defaults. No upstream edits are needed.

The first locked graph check correctly reported a stale lockfile. The explicit
offline graph-maintenance invocation pruned the lock, then failed while trying
to fetch uncached `antithesis_sdk`; that invocation is not a passed Cargo gate.
The separate structural graph check passed: **1087 to 1022 packages**, 65
removed, no new package identity, version, source or checksum, no new resolved
dependency edge, and every pre-existing workspace dependency edge preserved.
The manifest requirement strings remain unchanged. Detailed before/after
receipts are under
`/private/tmp/lancedb-production-20261008/release-fixtures/cloud-pruning/`.
All subsequent release checks use `--locked`; successful compilation and
behavioral execution must independently qualify this feature reduction. Independent
review found no defect in the manifest scope, native provider selection or
structural graph checker; that review is not runtime qualification.

The disposable consumer lock seed was rebuilt from this pruned root lock and
contains 995 existing package identities plus its fixture root. It is still a
manually prepared seed, not a Cargo resolution or downstream pass. Its receipt
records both lock hashes, and its prior seed is retained beside the graph
baseline.

Source inspection also found a pre-existing Database path edge case in pinned
LanceDB: `ConnectBuilder::execute` checks `uri.starts_with("db")` before local
routing (`lancedb 0.30.0/src/connection.rs:911`). A relative directory beginning
with `db` therefore deserves a targeted accepted-path regression. The
StateStore's directory construction does not use this LanceDB builder. No
routing production change or success claim is made for that existing Database
edge case as part of dependency pruning.


### Orphan reclamation correction and source proof

The production candidate initially left recent unpublished native files after
orphan-only maintenance. The first focused run recorded **82 passed, 1 failed**
in `candidate-trim-focused.log`; the new test created real unpublished
fragment/deletion files and a native transaction file, then required removal
through cold public connection. A second regression recorded **0 passed,
1 failed** in `orphan-history-red.log`, showing that unsupported retained-history
metadata was not rejected before the original cleanup path. These were
behavioral failures, not compiler failures or missing prerequisites.

The first correction passed the **8 selected maintenance recovery tests in
24.39 seconds** in `orphan-maintenance-green.log`. The command also reported two
deprecated `Path::child` warnings; those calls were subsequently replaced with
`join`. This is historical provenance for the first correction.

The ordinary clock-skew regression then recorded **0 passed, 1 failed in
4.34 seconds** in `full-gc-clock-red.log`: a future-dated real native orphan
survived full GC. The final correction applies the supplement to both paths
and passes **9 selected maintenance recovery tests in 29.23 seconds**, with
no warnings, in `full-gc-clock-green.log`. The nine include the supervised
process-entry helper; they are not nine distinct recovery scenarios. Both
independent engine reviewers found no blocking defect in the final patch.
No complete feature-matrix, platform or release pass is inferred from these
focused receipts. The unsupported-history test is now named
`maintenance_recovery_rejects_unsupported_history_before_history_file_deletion`;
earlier receipt names remain historical provenance. Mandatory durable hint
removal intentionally precedes reference validation; the preflight guarantee
concerns data/history-file reclamation, not every filesystem change.

| Obligation | Pinned mechanism and supplied owner | Reasoning and evidence scope |
| --- | --- | --- |
| Find recent unpublished files | `lance-7.0.0/src/dataset/cleanup.rs:346` passes the oldest retained manifest timestamp into `lance-io-7.0.0/src/object_store.rs:98`; the latter filters out newer objects before native cleanup decisions | `delete_unverified` does not remove this listing cutoff. Changing only native policy cannot find these recent files while retaining every committed version. Both supplied cleanup paths supplement native cleanup by listing directly through the same wrapped store, without timestamp assumptions. |
| Establish complete retained references | [LanceEngine::retained_files](../src/state_stores/lance/engine.rs) uses native `versions`, `checkout_version`, concrete manifest metadata and the public deletion-path helper | Before native data/history reclamation in either path, every retained version must have the expected schema, format, identity and supported reference layout. Branches/detached manifests, external references, indexes and unsupported row metadata fail before that reclamation. Orphan-only cleanup reuses this complete reference union. Full cleanup re-enumerates surviving manifests through pinned `lance-7.0.0/src/dataset.rs:2101` (`Dataset::versions` freshly lists/reads manifests), so removed versions cannot incorrectly retain obsolete files. Historical record values are not all decoded by this metadata scan; the current image is separately fully validated. |
| Bound deletion to known native files | [LanceEngine::remove_recent_orphans and native_file](../src/state_stores/lance/engine.rs); pinned `lance-7.0.0/src/dataset/fragment/write.rs:43`, `src/io/commit.rs:127`, and `lance-table-7.0.0/src/io/deletion.rs:37` | Direct children must match the pinned data, deletion or transaction filename spelling and be absent from the retained union. Candidate collection completes before this supplement starts deletion. Scanning after native cleanup avoids a second deletion becoming a false NotFound failure. It changes no timestamps, manifest bytes or upstream source. |
| Preserve durability and exclusion | [LanceCommand::maintain](../src/state_stores/lance/mod.rs) retains the exclusive protection outside each job future; [LanceEngine](../src/state_stores/lance/engine.rs) deletes through `dataset.object_store(None).inner` | The same directory durability wrapper or retained memory store owns deletion. Native children, runtime shutdown and cleanup settle before exclusion is released. Deleting only an unreferenced subset cannot invalidate committed history if process loss interrupts the supplement; this is source reasoning, separate from process-crash execution. |
| Fail conservatively and permit progress | Public maintenance recovery tests in [lancedb_state_maintenance_recovery](../tests/lancedb_state_maintenance_recovery/mod.rs) | The recent-orphan case preserves all manifests, referenced file bytes, historical batches, exact current revision/value and fresh unknown names, then proves subsequent progress. The unsupported-history case checks failure before data/history reclamation. The clock-skew case requires full GC to remove a future-dated real native orphan while preserving current state and subsequent progress. Existing cleanup failures remain backend errors and do not manufacture commit success. |

The supplement now covers both orphan-only and full-history cleanup. It
ignores unknown filenames, but no blanket preservation promise is made for
arbitrary old unknown files inside the preceding native cleanup's reserved
managed directories. Root and sibling user files remain outside the table's
reclamation trees.

Both paths inspect metadata for every retained manifest, including on
connection-triggered cleanup; full GC also scans the surviving manifest set. Long-held snapshots can defer reclamation and
increase that metadata work as well as retained disk/image costs. The collector
and filename recognizers depend on the pinned native representation; a future
dependency or format change must requalify them. None of this introduces a
public GC control, transaction expiry, recovery journal or upstream patch.


### App consumer supervision update before final capacity freeze

The first candidate App focused run passed one preparation and two workspace
cases, while the four Lance deletion cases again exceeded the existing
20-second workflow wait (`app-candidate-focused.log`). This matches the earlier
Lance baseline's supervision limit; the timeout alone is not a demonstrated
functional failure or a measured production latency SLO.

The existing test owner now allows 120 seconds for Lance's workflow terminal
wait and 135 seconds for its outer watchdog. Redb remains at 20/35 seconds.
All 130 workflow steps, more than 256 retained rows, pagination, forks,
lost-receipt checks and existing 6/4-second shutdown assertions remain intact.
The fixture prints actual elapsed/completion observations. This changes test
supervision only, not production behavior or acceptance assertions. The second
attempt (`app-candidate-focused-2.log`) failed compilation because an older
Redb child-policy initializer lacked the newly required timeout field. That
initializer now explicitly retains 20 seconds; this compile correction is not
behavioral red evidence. The third run passed, as recorded below.


### Current production candidate execution receipts

The final focused embedded run completed on the recorded macOS/APFS host with
Rust 1.94.1 and the existing low-memory test profile:

| Target | Passing tests | Execution seconds |
| --- | ---: | ---: |
| `lancedb_durability_feasibility` | 14 | 0.45 |
| `lancedb_native_feasibility` | 10 | 0.90 |
| `lancedb_state_store` | 85 | 84.64 |

Each target reported zero failures, ignored or filtered top-level tests. Nested
supervised-child output is not counted again. The receipt is
`/private/tmp/lancedb-production-20261008/candidate-final-focused.log`, SHA-256
`11ffe6c33510645e8d83a90b029f0bc21329602288a685487ed6682c1d8bf582`.
The dedicated final recovery selection additionally passed 9/9 in 29.23 seconds
without warnings (`full-gc-clock-green.log`, SHA-256
`b5fa69a22a74b75018d8857804a3fc9be95aded3acf92bcca2316e31c2f954d4`).
It overlaps the full adapter target and is not additional distinct coverage.

The App consumer rerun passed four deletion, one preparation and two workspace
cases in `app-candidate-focused-3.log`, SHA-256
`628c800fc1839fbf387b4a46446cbbb9f9d9b60e101f9287e45e6dd809d5128e`.
These are selected deterministic consumer cases, not a complete App strict-live
pass. Their actual workflow observations include `workflow.start` plus terminal
observation, not just `run.wait` or an individual StateStore commit:

| Lance deletion fixture | Observed seconds | Completed |
| --- | ---: | --- |
| Directory, bounded pages/forks | 69.794 | yes |
| Directory, lost receipt | 75.676 | yes |
| Memory, bounded pages/forks | 24.655 | yes |
| Memory, lost receipt | 24.623 | yes |

These costs describe the 130-step test workload under the recorded build
profile; the 120-second supervision bound is not a production latency SLO.
The candidate capacity source remains frozen separately at digest
`a6dc335f2137b37a4ce692761d805801270bcbd18aef13ca2f7b3bb60772be0a`.
The paired capacity report owns its complete measurement provenance; later
reporting-document changes do not rewrite that frozen source.

The current-git no-path and no-type-alias guards passed. Agent's ordinary Python
precheck contract suite passed 11/11 in 83.336 seconds and its grade-contract
suite passed 9/9 in 24.294 seconds under the approved ordinary TCP access.
Initial sandbox TCP failures were environment failures, not production red
results. These validate gate orchestration, not real provider behavior.
App's real prerequisite precheck passed with selected OpenCode 1.17.18,
Ollama daemon 0.40.0/client 0.34.2, and its separately owned PostgreSQL on
27934. A prerequisite pass is not the full App gate.

The full canonical Storage strict verifier subsequently **passed** (parent-owned
process exit 0), including all three current feature lanes: default SQLite/Turso,
no-default always-compiled backends, and no-default Qdrant. Each lane completed
target compilation, its deterministic tests, Clippy, rustdoc and doctests;
workspace formatting also passed. The Lance adapter target passed all 85 tests
in every lane. The full real Database/PostgreSQL strict-live target passed
68/68 in 29.45 seconds with zero failures, ignored, measured or filtered tests.
The verifier then removed its own three service containers, network and volumes
successfully. No Storage `--all-features` invocation was used.

The complete receipt is
`/private/tmp/lancedb-production-20261008/storage-strict-live.log`, SHA-256
`24dea37cc2a56c4ee65bc3e8b5b581daf56dca5a68e29d06df93686a933a5412`.
This closes the earlier macOS broader-Storage-gate environment gap for this
executed candidate. It does not establish another crate's full gate or Linux
execution. The post-gate source comparison recorded at that stage is in
`storage-gate-source-comparison.json` beside the log: Storage source, tests,
host verification scripts, root Cargo/lock/toolchain inputs and workspace
member manifests matched the frozen candidate file hashes. The only difference
within the compared Storage scripts was the Linux Dockerfile's added pinned
rustfmt/Clippy prerequisites, which this macOS gate did not execute. That is a
post-execution source comparison, not an invented before-run snapshot. Reporting
documents continue to evolve separately from the immutable measurement source.

The full Agent prerequisite precheck also **passed all eight phases**, including
its selected native endpoints, exact OpenCode overlay/privacy checks and actual
Ollama context/GPU-residency checks. Its receipt is `agent-full-precheck.log`,
SHA-256 `f32be37af4467f05abcd8618f71233a9140556944a43f439d27cdd37f19a267f`.
The subsequent native primary-residency observation in
`agent-precheck-model-residency.json`, SHA-256
`2a1f1782262ab23916d4f550bed5cab08b1796cef979dfd05a0d64171523c0a3`, records
`gemma4:e4b-it-qat`, context 131072, and equal positive `size` / `size_vram` of
6700002179 bytes. This is actual prerequisite evidence, not just installed-model
inventory. It does not prove the Gemini generation case or the complete Agent
strict suite. Gemini inputs come directly from the user-authorized Provider
environment; no credential values are copied into the evidence.

The [paired capacity report](lancedb-state-store-capacity.md) is complete for
its recorded macOS directory-storage matrix: two baseline and two candidate
runs, each passing all 15 scenarios. Independent reviewers checked the report
against all four logs, sample counts, calculation rules and source identities,
with no remaining identified reporting defect. The report retains observed
cold-open, write-tail and mixed-knowledge regressions alongside improved
repeated-read results. Its finite operating points, unoptimized test profile,
uncontrolled filesystem cache/thermal conditions and unavailable memory/device
I/O metrics do not establish a universal capacity limit, production SLO or
Linux performance. Reviewer agreement is not additional execution evidence.

The separate complete `serde_json/preserve_order` adapter run **passed 85/85**
in 81.51 seconds, with zero failures, ignored, measured or filtered tests.
Its receipt is `preserve-order-state.log`, SHA-256
`961f76f8d0012c1539ee2dd074258859de6e2c12ea377e501d6c221bf3ed7b2b`.
This includes deep disposal and cache cases whose names do not contain `json`.

### Package and downstream locked-graph receipts

Both `cargo package --locked -p storage-kit --no-default-features --allow-dirty
--list` and the actual packaging/build-verification command **passed**. Cargo
packaged 170 files, 2.0 MiB uncompressed and 425.5 KiB compressed, then rebuilt
the extracted Storage crate successfully in 7.18 seconds; `--no-verify` was not
used. The retained archive is
`/private/tmp/lancedb-production-20261008/release-fixtures/storage-kit-0.1.0.crate`,
SHA-256 `31c6a73eaf49d9dc26f6526553b65c1b591483da169f9850ea26b1e6e0c22fd9`.
Its unique regular-file inventory exactly matches `package-list.txt`, excludes
`vendor/**` and live `.env` files, and retains the nonsecret
`strict-tests.env.example` template. It includes all six private Lance
implementation files plus the construction configuration. An independent byte comparison
matched every packaged `src/**` file and `Cargo.toml.orig` against the current
source and manifest. Later document or test edits do not alter that archive.

The standalone consumer's initial `--locked` attempt correctly rejected the
manually prepared seed's lock shape. Explicit offline metadata normalization
then pruned 203 packages from the 996-package seed, leaving 793 including the
consumer root. `downstream-graph-comparison.json` and the separately recomputed
`package-independent-review.json` establish no new dependency identity,
version, source, checksum or retained dependency edge against both the seed and
the workspace lock. The sole added root is the disposable consumer itself.
This lock-shape preparation is not a Kit behavioral correction or a dependency
upgrade. The subsequent actual consumer execution used `--locked` and **passed**
with process exit 0 after a 10.10-second compile. It imports the extracted
package and exercises public memory/directory construction, shared clones,
independent-connection conflicts, complete winning/losing receipts, tombstone
revision continuity through reopen, same-directory Database knowledge and
reserved-table protection, and a direct caller implementation of
`StateStoreBackend` with its associated `StateTransactionBackend`.

The owning receipts under `release-fixtures/` are:

| Receipt | SHA-256 |
| --- | --- |
| `package-verification.log` | `3799b67d38142b7d00c9e9f169eb7612d1d6c3ee62f60fddefc764106f97efa2` |
| `downstream-final.log` | `e79b2447e89549cbfc18715e0ff8da6a89644d7722b49a44d24a65b69a62b0d9` |
| Normalized `consumer/Cargo.lock` | `216020344b94f30962e24451e87903bc6b87645f190990d7d6974dedc2e89dbf` |

Cargo reported four already-locked registry versions as yanked:
`chacha20 0.10.0`, `core2 0.4.0`, `fastrand 2.4.0`, and `spin 0.9.8`.
The package also retains the pre-existing missing documentation/homepage/
repository metadata warning. Neither warning was hidden or fixed through an
unauthorized version or metadata change. These package/downstream receipts
qualify the recorded locked resolution; they do not establish a fresh unlocked
dependency resolution. Cargo excludes yanked versions from new resolution
without an existing lockfile, while existing lockfiles can retain them. See
the [Cargo yanking contract](https://doc.rust-lang.org/cargo/commands/cargo-yank.html).
Requalify a deliberately updated graph separately; this delivery preserves
the existing requirements, public surface, defaults and feature policy.

### Clean-workspace Agent prerequisite and ordinary Linux preflight

The first full Agent verifier stopped at its `source_contract` target with
11 passes and two failures in 0.20 seconds. The two selected wire-fact tests
could not read files from the ignored Codex/OpenCode extraction directories.
The tracked, pinned archives were present; this was missing local source
materialization, not a version mismatch or demonstrated Lance defect.
The failed receipt remains `agent-full-strict-live.log`, SHA-256
`10e671895d26999a1d4586565d6aa7801b459989b45336b03840b6f16c84f9bc`.

The parent then verified both archives against the existing Agent vendor lock,
used the existing `VendorUpdater._extract_archive_to_path` owner to materialize
their missing directories, and checked every extracted file against the ZIP.
Codex `rust-v0.144.1` verified 5,389 files with archive SHA-256
`51d5acab11f375d3dd85c29c970f1e83010f65ba242de736b55ccaec8f8a6c7f`;
OpenCode `v1.17.18` verified 6,230 files with archive SHA-256
`03f7dd032afaeb8695f7a610fa93be22caeb8bb21783226363cd9b758ff9edf6`.
Archive hashes and file counts were independently rechecked. The materialization
receipt is `agent-vendor-materialization.log`, SHA-256
`35e2352faca5dfd9f7b140a9efe4a91a3cc25ac06328e9bc20a5c2439fe0fbb4`.
No selected version, tracked source, dependency or archive changed, and no
download or vendor-update workflow ran. Fresh Agent checkouts/exports need this
prerequisite because the extraction directories are ignored. The reproduction
procedure is retained in [the testing guide](testing.md#reproducing-an-ordinary-linux-candidate).
The complete Agent verifier was restarted with its own
`agent-full-strict-live-2.log`; a started rerun is not a pass.

The ordinary Linux image/volume preflight recorded all five installed
`aarch64-unknown-linux-gnu` components: Cargo, rustc, rust-std, rustfmt and
Clippy. The actual `/workspace` volume is ext4 according to its Linux mountinfo
entry on `/dev/vda1`; `stat` prints the broader `ext2/ext3` filesystem-magic
label for this family. The mount entry identifies the actual filesystem.
This uses the owned default-security container and volume, without tracing
capabilities or other relaxed security settings. Its receipt is
`linux-environment-preflight.log`, SHA-256
`2b0d964b1a94e486ac5e4f433f96b0542815b6d65178994d89256f6c2fea1c8b`.
It establishes prerequisites, not a Linux build, test pass or dataset durability
result. The real run must still record its dataset mount and source identity.

The subsequent test-only addition,
`memory_snapshots_preserve_state_across_maintenance_threshold_and_release`,
**passed its exact selection** with process exit 0 in 0.81 seconds: one passed,
zero failed/ignored/measured and 85 intentionally filtered cases, after a
15.72-second compile. It exercises a held memory snapshot through head revision
70, across 69 subsequent writes, stale transaction conflict with no partial write set, a valid old read-only
transaction, release/progress and exact tombstone revisions. Internal memory
GC invocation is not exposed by the public API; source reasoning separately
supports that mechanism. Its `memory-maintenance-coverage.log` SHA-256 is
`e82c36b09898535bd950f9ee163916ef071115ea68594e6c30d374ae7abd4062`;
the sibling exit receipt SHA-256 is
`462a33c4574d2f68dc98d86ecce32f6dae5118a64fd17e7c17119a52182659ed`.
No production code changed. The adapter now has 86 tests; every earlier 85-case
receipt above retains its original count and source scope. The changed
maintenance test is not covered by the older source-comparison capture or
package test inventory. Complete final 86-case lanes, `preserve_order` and the
new final source identity still require separate receipts.

### Completed Agent gate before the final cleanup correction

The full canonical Agent rerun **passed with process exit 0** for its recorded
source, before the final prompt-cleanup correction described below. Its source
contracts passed 13/13, static audit 3/3 and public construction/lifecycle
contracts 22/22. Subsequent terminal
subtarget results include downstream compile contracts 4/4 in 975.89 seconds,
Custom prompt runtime 55/55 in 39.36 seconds (including four Lance cases),
Codex prompt protocol 46/46 in 24.44 seconds (including two Lance cases), and
OpenCode event ownership 280/280 in 720.85 seconds. Each of these subtargets
reported zero failures, ignored, measured or filtered cases. The complete real
strict-live target then **passed 23/23 in 623.89 seconds**, also with zero
failed, ignored, measured or filtered tests. Rustdoc, 266 doctests across their
two reported groups, Clippy with warnings denied and formatting completed
successfully. Compile-fail doctests emitted their expected compiler errors;
their harness results passed.

The final log is `agent-full-strict-live-2.log`, SHA-256
`3d2b043f99471b0c66bc75a78f0ec03f855d88a7d9f37a89d64703a6376f5704`.
The sibling `agent-full-strict-live-2.receipt.json` records the exact canonical
command, exit 0 and grade outcomes; its SHA-256 is
`c2c7141e2e5bb5e32a25c8cffb971438e7aa40de13bd881de649d6cc441d1fea`.
The private strict-only log at
`target/agents-kit-strict-live/question-grades-v65M0c4g/strict-live.log` has
SHA-256 `e8d5ea221df65372d09a3ed81813b527dad52713ed5564c0f4222128437b6a76`.
The post-execution `agent-gate-source-comparison.json` matches Agent production,
tests and scripts, its lower production dependencies and Cargo/lock/toolchain
inputs to the frozen candidate file hashes. Selected protocol archive/lock
hashes are recorded separately because that measurement manifest excluded
vendor research files. This is not a fabricated pre-run capture or a new
whole-workspace freeze.

Both required Question Kit receipts passed with `sdk_contract: true`, while
**both raw exact-copy grades FAILED** (`model_exact_copy: false`):

| Exact strict-live case | Kit receipt | Raw model exact-copy grade |
| --- | --- | --- |
| `strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains` | PASS | **FAILED** |
| `strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build` | PASS | **FAILED** |

Both used selected OpenCode 1.17.18, Ollama and the unchanged configured shell
role `ministral-3:8b-instruct-2512-q4_K_M`. The approved Agent contract makes only
these two raw-copy grades nonblocking; exact Kit/native output fidelity,
interaction correlation, lifecycle, cleanup and valid grade reporting remain
blocking. This result is not model-quality acceptance or reliability evidence.
The full verifier log also includes a deterministic grader-control receipt;
it is outside the strict-live phase and is not a third real Question result.

### App deterministic evidence and real live-gate failure

The full App canonical verifier completed with exit 101 because of the real
live-case failures recorded below. Its earlier completed deterministic phase
reported
**816 passing runtime tests across 35 executable targets**, plus the intentional
compile-only `compile_contract` target, whose generic consumer functions compile
but register zero runtime tests. Each parent target footer reported zero failures,
ignored, measured or filtered cases. Supervised-child summaries are not counted
again. Both doctest groups also passed: 120 in 5.31 seconds and 7 in 1.31 seconds.

The partial receipt `app-deterministic-progress.json` records all 36 target
counts/times and both doctest groups. It binds only the observed log prefix
through the second doctest footer: 117,178 bytes, SHA-256
`aa908615f44947364a10d6c33eb602c4ea8faf9027daaa63e1c3e6a8cc41d1f6`.
This is not a final whole-log hash, process-exit receipt or source freeze.
Relevant completed parent targets include:

| App target | Passing tests | Seconds | Lance coverage included |
| --- | ---: | ---: | --- |
| `deletion_contract` | 24 | 220.48 | Four memory/directory bounded-deletion and lost-receipt cases |
| `prepare_contract` | 5 | 10.80 | One Lance recovery case covering four process-loss boundaries |
| `workspace_contract` | 19 | 4.17 | Two memory/directory revision-fencing and reopen cases |
| `workflow_execution` | 153 | 1902.99 | Existing general App execution coverage; not a Lance-specific capacity measurement |

All seven selected Lance cases passed within these complete targets. The four
Lance deletion workflow observations were 70.937 seconds (directory, bounded
pages/forks), 69.361 (directory, lost receipt), 25.253 (memory, bounded
pages/forks) and 23.931 (memory, lost receipt). As in the earlier focused run,
these measure `workflow.start` plus terminal observation for the full fixture,
not individual commit latency or a production SLO. The recovery case exercised
staged action, staged input, before-barrier and after-barrier process loss; its
four child runs count as one parent case.

The existing `settlement_count_reserves_the_last_unknown_row_before_admitting_another_target`
case completed its full 4,096-receipt bound, reporting
`received/calls/returns = (4096, 4095, 4095)` and 1021.277618041 seconds of
monotonic elapsed time. It passed within the 153-case execution target; this is
an ordinary App contract observation, not a new Lance benchmark or a shortened
substitute for that bound.

The real `workflow_live` phase completed with **16 passed, 2 failed**, zero
ignored, measured or filtered tests, in 414.91 seconds. Both
`lance_state_directory_two_real_agents_preserve_tool_and_knowledge_isolation`
and the existing Redb case
`two_real_agents_parallel_wave_preserves_tool_and_knowledge_isolation` failed
at the same primary final-JSON assertion. In each case, the returned primary
state contained its own knowledge UUID in both `tool` and `knowledge`, instead
of its distinct tool UUID. Before that assertion, both actual native tool
receipts had matched their respective canaries, both process exits and parked
questions were observed, and both branches reached successful terminal status.
The failures establish a final response-content mismatch; those receipts alone
do not identify native generation, Agent adaptation, App mapping or persistence
as its cause, and do not establish cross-agent data leakage. The existing Redb
and new Lance cases share this exact assertion and prompt. Both failures remain
blocking; the two approved Agent Question exact-copy exceptions do not apply.
Owned App and both service cleanup observations succeeded for each failed case.

The completed canonical log `app-full-strict-live.log` has SHA-256
`c8f9971a5dd5b5237a1912f8345b1abc034e11c3c9137d7c8fb43395c5a12747`.
Its parent-owned exit receipt `app-full-strict-live.receipt.json` has SHA-256
`341c049c3958f9dfb534bc1dc2fa75e65157b4be1b01d1272e3e2e4074db6d47`.
The canonical command stopped before its separate PostgreSQL crash selection.
That exact tail subsequently **passed as a separate continuation with exit 0**:
`staged_action_and_atomic_barrier_survive_forced_process_loss` passed once in
3.52 seconds, with zero failed/ignored/measured cases and four intentionally
filtered cases. Its one parent case covers all four PostgreSQL process-loss
boundaries; child headers are not additional cases. The continuation script's
body from crash-directory creation onward matches the canonical verifier and
preserves its test-owned database creation, non-forced cleanup trap and `wip`
check. Exit 0 includes successful cleanup. Its log
`app-postgres-crash-continuation.log` has SHA-256
`b17e0be4859ab43376972c3df5575317245bce5f127bc5a6660c238246f8575c`;
the sibling receipt has SHA-256
`1d1ef5dd6659be83611ae425619f0adce340fe1f2db99b8debb00d49ec4a5763`.
This separate pass cannot turn the failed canonical invocation into a pass.
Earlier successful deterministic/doctest observations retain their original
scope.

A subsequent test-only diagnostic addition to the existing `ForwardingControl`
now captures the actual lower portable response's two UUID marker fields before
returning it unchanged to App. It retains only bounded valid UUID strings or
fixed missing/invalid labels, and prints both agents' projections only when the
unchanged final-state assertion would fail. It adds no native payload dump,
production hook, public API or relaxed assertion. Independent review confirmed
that ownership, response forwarding and cleanup remain unchanged. The first
focused diagnostic run then completed with exit 101: the existing Redb case
passed, while the Lance case returned `Conflict("app state transaction conflicted")`
before the final marker assertion. Its subsequent App shutdown observation
timed out, and an execution thread printed a Tokio runtime-shutdown panic during
the next case. The target reported 1 passed, 1 failed and 16 filtered in 82.93
seconds (`app-isolation-diagnostic.log`, SHA-256
`91fe4834788d8d39ae4208ad146c407b5d6d7966a5af93d471cff347e266789e`).
This failed run supplies no lower-marker comparison for Lance and is not a
successful isolation result. Test-local fixed phase labels and existing counter
observations were subsequently added to identify which public operation
propagates the conflict, without replaying resolutions or changing assertions.
The phase-only exact Lance rerun then passed with exit 0: one passed, zero
failed/ignored/measured and 17 filtered in 40.30 seconds, after a 20.71-second
compile. Its App and both service cleanup observations succeeded. The receipt
is `app-lance-isolation-phase.log`, SHA-256
`53ee7720e83fcae6b69bed17f014abe23292411a755d435e760feb02fe15a829`;
the sibling exit receipt SHA-256 is
`e6123b45f40ffccad25f03f3dffafb5d28ee1b7c1780eb043e6eaba16fd2dae8`.
It did not reproduce the conflict, so the earlier failing operation's exact
phase remains unknown. This focused success does not relabel the failed
canonical invocation or establish reliability.

The fixture was subsequently corrected to follow App's documented public
conflict and shutdown-observation rules. Only this isolation case now rereads
the public snapshot after `Conflict` before reissuing the identical notification
decision, with at most eight attempts inside the original 120-second scenario
bound. A changed generation, identity or kind, including Unknown, is rejected;
a disappeared target only permits exact retained-receipt replay, never assumed
success. Other errors are not retried. Both lower-resolution counts must still
be exactly one, and every original marker and native receipt assertion remains.
Its shutdown observer now reattaches to the same mode on `Timeout`, retaining
App and its native services inside the original 120-second cleanup supervision.
Primary failures and secondary cleanup outcomes remain reported. These changes
are test-local. The first final
live-validation attempt stopped in Clippy with compiler error E0004 because the
public `Interaction` enum is non-exhaustive; no live test executed in that attempt
(`app-live-final.log`). Its consumer match now conservatively rejects any future
variant before a retry. This is a compile correction, not behavioral red or a
private-dispatch wildcard. Independent review confirmed that the added fallback
cannot authorize an unknown interaction. The earlier conflict is still an
observation, and the shared-table explanation is source reasoning rather than a
reproduced operation-stage finding.

The complete corrected 18-case live target subsequently returned exit 101:
**17 passed, one failed**, zero ignored/measured/filtered, in 451.95 seconds.
Formatting and the selected `workflow_live` Clippy gate passed; the subsequent
architecture guards were not reached. The remaining Lance failure occurred
during terminal observation after both native prompts, answers and process exits
had each been observed exactly once. Its forwarded Agent error was the precise
`Conflict` / `StateCommit` / `TransactionConflict`, with the safe message
`prompt cleanup could not commit`. App terminal supervision then expired; App
and both owned native service cleanup observations succeeded. This is a new,
identified cleanup failure, distinct from the original final-JSON mismatch.
The log `app-live-final-2.log` has SHA-256
`f91cbbe5376d7977fd99c33e1faaa3f2607b8f536e2c5e062064119e7c846c02`;
its sibling receipt binds unchanged before/after inputs for both live test files
and the recorded Cargo inputs. The failed target remains blocking.

Source inspection identified an Agent consumer gap: final prompt-record cleanup
did not retry a definite consuming-commit conflict, although the preceding
generation claim and waiter-settlement transitions already used the bounded
generation policy. Lance's approved whole-table serialization can expose that
conflict even when another writer changes a different namespace. This is not a
StateStore atomicity or serializability failure. The existing cleanup owner
performs only fresh control-state reads and deletion; it does not redispatch a
native prompt or interaction.

Two new deterministic tests reuse Agent's existing protocol fixture and caller
StateStore fault owner. A real question and its answer settle before the exact
terminal deletion receives a rolled-back commit conflict. The transient case
requires the unchanged native response, exact retained continuation receipt,
one native prompt/answer, no republished interaction and subsequent prompt
progress. The persistent case requires bounded retries under the existing
five-second lifecycle deadline, retained terminal state and no further retry
admission after expiry. Before production changes, both failed behaviorally:
zero passed, two failed and 280 filtered in 0.15 seconds. Both returned the first
typed conflict immediately. The log `agent-terminal-clear-conflict-red.log` has
SHA-256 `87a2b96140e65ef4c64a96f1b1c995e619502ca9b794777a58e686d0f303d3d8`;
its receipt records the unchanged production runtime hash for that red.

The narrowly scoped correction now adds the existing generation policy to the
final cleanup guard only for `StateCommit`: an exact typed transaction conflict
without cleanup failure may re-read and re-author the deletion within the
existing deadline. The former non-conflict policy and all precommit-conflict
handling are unchanged. No new retry budget, native replay, public API or
Storage semantic change is introduced. The two focused regressions then passed:
two passed, zero failed/ignored/measured and 280 filtered in 6.43 seconds after
a 34.37-second compile, exit 0. The log
`agent-terminal-clear-conflict-green.log` has SHA-256
`7c6469babe608c26397fd9c7008cf12be3d1e2dd0d2cf88c9b4722a822eaf7eb`.
This is direct public Agent behavior through a caller-owned StateStore fault
implementation, not a strict-live execution. The exact real Lance App isolation
case then also passed: one passed, zero failed/ignored/measured and 17 filtered
in 37.78 seconds after a 37.45-second compile, exit 0. App and both native service
cleanup observations succeeded. Its log `app-lance-after-cleanup-fix.log` has
SHA-256 `0e874bcb9f93a245e4631b95c29b24b6a13d208b13cd4bbcf3073f289fde0dbe`;
the sibling receipt has SHA-256
`3d2e807c148d80a1c993eceb32a3bd071ba51a9d11b7e04acc907a67b6e9866d`.
This is the real shared-directory App/Agent/knowledge case with all original
assertions, not a claim of statistical reliability or a changed canonical
failure receipt. Independent review of the actual implementation, tests and
contract found no actionable issue: the unchanged legacy classifier remains
the first guard arm, the added arm admits only the exact consuming-commit
conflict, each new attempt rechecks the generation and clearability, and the
original admitted completion future remains owned across observer expiry.
Review also confirmed that the fault returns only after the backing transaction
rolls back. Previous Agent/App receipts do not qualify the changed production
runtime by themselves.

### Completed App live continuation and owned fixture cleanup

The final complete App `workflow_live` continuation passed **18/18**, with zero
failed, ignored, measured or filtered cases, in 367.54 seconds and process exit
0. Both the Lance and existing Redb isolation cases passed their unchanged
final-JSON, native tool, knowledge, interaction and cleanup assertions. The same
invocation completed workspace formatting, Clippy for the changed live target
with warnings denied, and both root no-path/no-alias guards. The log
`app-live-final-3.log` has SHA-256
`b8376db16b5a689f15efa6ab99d6ec0cdaece7f8d03eef3b66f84c892e6fa51a`;
its sibling receipt has SHA-256
`415824ee6873ae24928a3ecd189695b2a200b3adb670020c577e3ed850a80528`.
The recorded eight input hashes are identical before and after execution and
still match the inspected files, including both App live test files, the
corrected Agent runtime and its regression target, and the four recorded Cargo
inputs. This is an explicitly scoped continuation: the earlier 816 deterministic
App passes, 127 doctests and separate four-boundary PostgreSQL recovery receipt
retain their own source and invocation scope. The original canonical App
invocation still has exit 101; no combined result rewrites that history.

Parent-owned App fixture cleanup is complete. Its exact OpenCode foreground
session 81049 was joined after intentional Ctrl-C, recording exit 1 for that
requested termination. The first Compose cleanup failed before mutation because
required interpolation variables were missing. After sourcing the owned fixture
configuration, the second cleanup exited 0 and removed only the App project's
PostgreSQL container, volume and network. The exact test environment symlink was
also removed. These outcomes are recorded in `app-owned-process-cleanup.json`
(SHA-256 `f8d14af92b888316657550619a765806ca05ab16db172232af5a9d48d45ef980`).
The failed prerequisite log `app-fixture-cleanup.log` has SHA-256
`0f58d56007a7bc3440d239cf2e375f497cc34ee3da4b3dd6995b7b9f19e97ea8`;
the successful `app-fixture-cleanup-2.log` has SHA-256
`ab427442597c697bf8045e81baee34dc23b035e41cc7d522d3751d0268f29d2e`.

The fresh full canonical Agent verifier is running as
`agent-full-strict-live-final.log` against the corrected private runtime and
fresh owned `codex-final` / `opencode-final` homes. Started services, prechecks
or partial target output are not a terminal result; no final Agent pass is
claimed until that invocation and its required grades complete.

Matching lower and stored markers would distinguish response content from App
persistence, but would not by itself distinguish native model output from Agent
adaptation. The conflict and cleanup observations are not dismissed as model
quality, and no public or production behavior was weakened.
Parent-owned Agent service teardown is separate from the verifier result and
must preserve any cleanup outcome. Linux execution and a separate locked
clean-candidate build remain outstanding release receipts; Cargo's package
verification is not a fresh build of every dependency or a substitute for that
whole-candidate gate. No unexecuted gate or platform is reported as a pass, and
this work does not claim unrelated workspace-wide completion. Public StateStore
APIs, defaults and backend feature policy remain unchanged; the later feature
split is outside this delivery.

### Remaining production completion checklist

This table is a historical checkpoint, superseded by the
[completed production receipts](#production-completion--2026-10-09) below.

The continuation audit identified no additional unimplemented requirement or
unresolved demonstrated StateStore defect in the approved embedded scope. The
JSON codec/ownership correction, exact-version publication, permanent revision
and tombstone semantics, image reuse, bounded adapter batches, snapshot leases,
both maintenance paths, cold recovery/backup guidance, scoped verifier and
dependency pruning are implemented and have the earlier named behavioral or
source evidence. The final Agent cleanup gap has meaningful red/green tests,
two independent source reviews and a passing full real App live continuation.
The following items are execution or evidence closure, not authority to alter
the public API, defaults, feature policy or accepted serialization design.

| Remaining item | Current state | Required completion receipt |
| --- | --- | --- |
| Final canonical Agent gate | Running against the cleanup correction; no terminal pass inferred | Complete command exit, all target/static/doc results, both mandatory Kit grades and raw copy grades, exact source/lock/compiler identity, followed by settlement of the exact owned native processes and service project |
| Final macOS Storage gate | Earlier 85-case source passed; the added memory case passed separately | Final three-lane canonical verifier, real Database/service suite and cleanup, then the complete 86-case `preserve_order` target; keep the earlier source counts unchanged |
| Final package and downstream consumer | Earlier locked package/consumer passed; final archive refresh pending | Actual `cargo package --locked` verification, archive listing/content checks including the new test, no vendor archives or live env files, and locked execution of the consumer against that extracted archive |
| Ordinary Linux Storage execution | Only installed-toolchain and ext4-volume prerequisites established | Current-source focused verifier; all three existing Storage lanes; nonempty `lancedb_` Database selection; complete `preserve_order` adapter target; actual native compiler, dataset filesystem, source digest, parent counts, exit and cleanup |
| Ordinary Linux consumer execution | Scripts prepared; no behavioral pass claimed | Four Custom and two Codex Lance cases, four App deletion/one four-boundary recovery/two workspace cases, plus the two new ordinary OpenCode terminal-cleanup regressions; retain separate target summaries and intentional filters |
| Linux capacity and workload evidence | Two candidate runs and analysis pending | Two complete 15-case candidate matrices with all sample/measurement receipts; parse the existing adapter workload's one process-I/O receipt and four growth checkpoints after target completion, preserving scope and arithmetic |
| Final clean-candidate/source closure | Fresh export/build pending | Include untracked implementation/tests in a verified immutable source archive, refuse an existing extraction, use a fresh Linux source/target tree and locked build, retain exact archive/file/log hashes and recheck code/build inputs; run Git-inventory guards in the actual checkout |

The Linux consumer runner now includes the final Agent correction's two local
protocol-fixture regressions; it needs no native CLI, model or external service
for those selections. Its existing six Agent and seven App cases are unchanged.
Both disposable runner copies passed shell syntax validation, and nine ordinary
parser controls accepted the valid exact-count summary while rejecting empty,
failed, ignored, measured, absent, wrong-target, duplicate-target and child-only
success cases. Those controls qualify result parsing only, not Linux execution.

The macOS paired capacity matrix is complete for its recorded unchanged Storage
implementation and harness. Its measured cold-open/write/contention regressions
remain visible; acceptance is the finite measured envelope, with no invented
latency threshold or universal maximum. Memory-mode capacity, production-profile
performance, independent random-per-row compression, native rewrite counts and
device write amplification remain outside those measurements. Linux kernel
process-I/O counters can supplement the existing workload report but cannot be
relabeled device durability or isolated Lance write amplification. No missing
optional metric excuses a behavioral failure.

The selected evidence standard continues to combine actual public behavior,
ordinary native probes and identified source mechanisms. It does not require
reviving the prohibited instrumentation or inventing machine-power-loss proof.
The pre-existing Database relative-path `db` routing edge is a separate follow-up;
it is not a demonstrated StateStore construction defect. Fresh unlocked
dependency resolution, hosted/external storage, Windows, paired memory
Database/StateStore construction and default/feature-policy migration retain
their previously disclosed scope. When final receipts arrive, update this
checklist and the overall production result without rewriting historical
failures or claiming unrelated whole-workspace qualification.

### Final source-review receipt

The final independent review found no actionable unresolved defect in the
requested cache, lease or reclamation paths. It inspected engine SHA-256
`11883360c08246a1dfbd384938bb44d035a01276f871932c8c7a994924e219c6`,
independently rehashed when recording this receipt. This was read-only source
review: no Cargo, services or platform tests ran, and the outstanding execution
checklist above is unchanged.

- **Freshness:** `LanceEngine::snapshot/read_image` opens a fresh Dataset and
  validates its schema, native format, row-ID mode, version and transaction
  identity before reusing an image. `LanceImage::matches_identity` requires both
  version and UUID; the cache owns neither Dataset nor lease. Failed job outcomes
  invalidate it. Relevant owners are `engine.rs:333–363`, `records.rs:77–116,290`
  and `mod.rs:473–500`.
- **Lifetime:** `coordination.rs` uses independent directory descriptors without
  upgrade or duplication; the single memory worker controls snapshot admission.
  Commit retains its snapshot outside the native future until child draining,
  runtime destruction and cleanup finish (`mod.rs:431–437`). Maintenance holds
  exclusive protection across inspection, rewrite, reclamation and final
  validation (`mod.rs:440–470`); final owner drop only drains and joins.
- **Reclamation:** `engine.rs:673–872` preflights every retained reference before
  native data/history deletion and re-enumerates surviving references after full
  GC. Its supplemental sweep removes only recognized unreferenced direct-child
  files through the existing durable store. Durable hint removal intentionally
  precedes that reference validation.

| Pinned native mechanism | Source and implication |
| --- | --- |
| Full-row replacement can retire old fragments | `lance-7.0.0/src/dataset/write/merge_insert.rs:1762–1863` removes replaced row addresses and reports completely deleted fragments through `removed_fragment_ids`; the supplied guarded full-row delta preserves logical revisions and tombstones. |
| Cleanup retains the current version and enforces tag protection | `lance-7.0.0/src/dataset/cleanup.rs:170–265,911–933` uses a strict `< before_version` cutoff; default tag checks remain enabled and recursive branch cleanup remains disabled. |
| A supplemental orphan scan is required | Native `cleanup.rs:259,346` and `lance-io-7.0.0/src/object_store.rs:98` still apply the earliest-retained-manifest timestamp filter despite `delete_unverified`. `lance-7.0.0/src/dataset.rs:2101–2122` freshly lists manifests for the Kit's reference union. Pinned filename sources are `fragment/write.rs:43`, `lance-table-7.0.0/src/io/deletion.rs:37` and `lance-7.0.0/src/io/commit.rs:127`. |
| Deletion and interrupted-history handling remain owned | Native `cleanup.rs:436–442` awaits deletions; Kit `durability.rs:492,588` synchronizes directories and retains error receipts. `lance-table-7.0.0/src/io/commit.rs:279,372–424` probes contiguous versions from hints, which the Kit durably removes before history deletion. Inline manifest transactions (`lance-table…/io/manifest.rs:153–158`, `lance…/dataset.rs:1044–1066`) remain the preferred identity source after interrupted removal of old external transaction files. |

The reasoning assumes the documented cold cutover and cooperative lease
protocol. It preserves the existing distinction between ordinary process-loss
tests, source reasoning and actual power-loss evidence, and supplies no new
executed release or platform verdict.

## Production completion — 2026-10-09

The approved embedded production implementation is complete. Its recorded
runtime candidate passed the scoped macOS/APFS and Linux/ext4 validation below,
with no unresolved demonstrated StateStore defect found by the independent
implementation, scenario-inventory and evidence reviews. This conclusion uses
the accepted public-behavior, ordinary native-probe and pinned-source standard;
it does not claim execution of every internal combined-failure branch or actual
machine-power-loss testing.

The implemented scope includes validated immutable-image reuse, bounded list
selection, stale-write preflight, lazy delta batches, coordinated full-row
maintenance, version/orphan reclamation, permanent revision/tombstone history,
iterative JSON ownership, and cancellation-safe admitted work. Cold backup,
recovery, receipt reconciliation, deployment cutover and measured operating
costs are documented. Actual Agent/App consumers and extracted-package consumers
exercise the existing public abstractions. No upstream patch, public API change,
new dependency package, backend-default change, or feature-policy migration was
required. Redb remains the default. External/hosted storage, Windows and paired
memory Database/StateStore construction remain outside this delivery.

### Source and evidence continuity

The execution candidate includes all 976 tracked and untracked source files,
excluding immutable root `tmp/**`. Its compact-JSON inventory digest is
`3edeee0f81e407d80627a801e6a380b8a73a8decd8a1dc32227681ea3c2d0239`;
the manifest file hash is
`c1352c4d1ddee077f64f2896fbceb240189577b0dcbdaf660f2ae49175b944f2`.
The lockfile hash is
`7ef9b5f1511c54bf743df27c8af6e4084c84287e321b9f13ae3e54f38ecddc34`.
Each resumed Linux step rechecked the full exported source before and after
execution. The initial export/build used a clean Linux source/target; after the
interruption, the source remained identical and compiled artifacts survived.
The resumed run used that compiler cache and is not a newly empty-target build.

The earlier `/private/tmp/lancedb-production-20261008` evidence directory
disappeared during the interruption. Original macOS raw logs, frozen capacity
sources, machine-readable receipts and package artifacts are unavailable for a
fresh audit. Their previously observed outcomes, hashes and independent reviews
remain historical records here and in the capacity report. No historical raw
logs or success receipts were recreated. Recomputing an identical source
manifest establishes source continuity, not recovery of lost execution output.

Fresh Linux logs, analyzed measurements, package artifacts, source manifests,
runner identities and negative parser controls are preserved in the checkout's
ignored `target/lancedb-production-evidence-20261009` directory. Preserve this
directory before cleaning build output. The final owning-document and rustdoc
refresh changes prose only; it has a separate `final-documentation` source
manifest and doc/package receipts in that evidence directory. Its files must
not be described as byte-identical to the earlier runtime candidate.

### Completed execution matrix

| Scope | Observed result and boundary |
| --- | --- |
| Final macOS Storage canonical verifier | PASS: all three existing lanes, each including native 10, durability 14 and supplied adapter 86; complete compile/contracts/format/Clippy/rustdoc gates and 250/240/246 doctests. Real-service suite 68/68 in 25.99s. These earlier raw artifacts are now unavailable. |
| macOS deep-JSON feature union | PASS: complete `serde_json/preserve_order` adapter target, 86/86 in 86.67s; separate from the default feature lanes. |
| Fresh Linux focused verifier | PASS: native 10, durability 14, supplied adapter 86, without filters or ignored cases. The focused source/environment receipt is separately bound to the complete exported candidate. |
| Fresh Linux Storage lanes | PASS: default SQLite 160 deterministic + 250 doctests; no-default 156 + 240; Qdrant 183 + 246. Full compilation, formatting, Clippy and rustdoc included. The two disabled Qdrant test targets provide zero behavior coverage in their disabled lanes. Repeated cases across lanes are not distinct tests. |
| Linux real embedded Database | PASS: 24 selected `lancedb_` cases; 31 other cases intentionally filtered. This does not qualify unrelated remote-service cases on Linux. |
| Linux deep-JSON feature union | PASS: all 86 adapter cases with `serde_json/preserve_order`. |
| Linux Agent consumers | PASS: two Codex, four Custom and two OpenCode terminal-cleanup regressions. The cleanup regressions use the ordinary local protocol fixture and caller-owned conflict injection; they are not a claim of live Lance collision injection. |
| Linux App consumers | PASS: four deletion cases, one process-loss recovery parent covering four boundaries, and two workspace cases. All seven parent cases passed; child output is not double-counted. |
| Capacity | Complete macOS baseline/candidate pairs and two additional Linux candidate repetitions, 15/15 cases per run. Linux repetitions took 139.80/139.23s. The [capacity report](lancedb-state-store-capacity.md) preserves regressions, raw-counter limits and the finite operating envelope. |
| Extracted Linux package | PASS after the tooling continuations below: actual locked package verification, 168 regular archive files, 152 source/test/original-manifest byte comparisons, no vendor archives or live environment files. The extracted consumer passed memory/directory/clone/conflict/tombstone/reopen/same-root knowledge/direct-trait behavior. |

Linux used native Rust 1.94.1 (`e408947bf`, LLVM 21.1.8),
`aarch64-unknown-linux-gnu`, one Cargo job, disabled incremental compilation and
disabled dev/test debug info. `/workspace/test-tmp` and build output used the
Linux-owned volume on `/dev/vda1`, mounted as ext4 at `/workspace`; the raw
`stat -f` label was `ext2/ext3`. Docker retained default security settings and
no added capabilities. No privileged instrumentation was built or run.

Fresh root receipts are `linux-resume-receipt.json` and
`linux-gates-summary.json`; the consumer scope is
`linux-consumer-summary.json`; the two measurement runs are bound by
`linux-capacity-completion.json` and `linux-capacity-analysis.json`. Independent
review checked terminal counts, intentional filters, hashes, source and compiler
identity, filesystem observations and workload arithmetic. The analysis hash is
`d767cdda059f19eb9aa88bbaf6dac70d3f5cf507334ac590243926bf214250ed`.

The first Linux finish invocation remains **FAIL**: package compilation passed,
then its disposable extraction helper called a Python `tarfile` keyword absent
from the image. An isolated retry used validated regular-file copying into a
fresh owned directory. That retry's package/extraction passed, but offline
metadata stopped on an uncached `android_system_properties 0.1.5`. Its lockfile
had already normalized to the seeded graph. The continuation first proved every
nonconsumer package identity, checksum and retained dependency edge was a subset
of the existing root graph, then used ordinary `cargo metadata --locked` to
fetch missing packages and `cargo run --locked` for the consumer. It passed and
joined in 35.28s; neither earlier failed receipt was rewritten.

The package is identified by SHA-256
`b2127413418ea325b13191c4bdc941010b0db295b87bd6972bceacc117d2d0d3`.
Its consumer resolved 793 packages from a 1,023-entry seed, pruning 230; no
nonconsumer version/source/checksum or retained edge was added. Consumer lock
SHA-256 is
`216020344b94f30962e24451e87903bc6b87645f190990d7d6974dedc2e89dbf`.
`package-retry/linux-package-receipt.json` binds the preparation script and
package/consumer bytes; its separate
`linux-package-continuation-receipt.json` binds the executed continuation and
zero exit. Existing yanked-package warnings remain disclosed. This qualifies
the tested locked resolution, not arbitrary fresh dependency resolution.

### Agent/App release continuations retain their failure history

The final Agent canonical invocation passed 503 deterministic cases across 14
targets, including all 282 OpenCode ownership cases, then failed its live phase
with 21/23 passing: a PostgreSQL conversation-creation conflict and a required
managed-knowledge output mismatch. That command remains exit 101. An unchanged
canonical live/static continuation subsequently passed 23/23 in 410.40s, 266
doctests, rustdoc, Clippy and formatting; all 501 recorded input hashes matched.
Both required Question Kit grades were true. Raw model-copy grades were
**true / false**, with the native-idle case false, nonblocking only under the
existing two-case Agent contract. This exception proves no model-quality or
statistical-reliability claim.

The original App canonical invocation passed 816 deterministic cases, 127
doctests and the recorded static/example gates before its live phase failed
with 16/18 passing. It remains exit 101. Following the reviewed fixture
corrections and narrowly scoped Agent terminal-cleanup conflict correction, the
complete App live continuation passed 18/18 in 367.54s, together with formatting,
changed-target Clippy and architecture guards. The separate canonical
PostgreSQL crash-test tail passed its one parent covering four boundaries.
Those separate invocations preserve their own source and coverage scope; they
do not relabel the original canonical command as successful.

Previously observed macOS owned-process, Compose volume/network and temporary
credential-home cleanup completed; the user's Ollama service was left alone.
The fresh Linux command supervisors settle each owned process group before
acknowledging completion. Final container/volume removal is a separate closure
receipt, not inferred from a test pass. Public APIs and current feature lanes
remain unchanged. This delivery claims the approved embedded StateStore work,
not unrelated whole-workspace completion or the subsequent feature split.
