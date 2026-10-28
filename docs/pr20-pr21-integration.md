# PR #20 / PR #21 integration record

Recorded 2026-10-23; updated through 2026-10-24. **Status: INCOMPLETE.**
The scoped Storage matrix passed. Both canonical workspace attempts stopped
before live execution; the second consumed the single newly authorized attempt.
Its compile-fixture isolation repair and the requested test/documentation cleanup
are independently reviewed and passed focused validation. No further canonical
retry has been authorized. Workspace/live, clean-candidate and scoped
distribution gates remain pending.
This records the user-authorized integration, not a replacement for owning
specifications or proof that the combined candidate passed.

## Source and authority

The integration starts from PR #21
`81d34036525506bdcb30ba10fbdd72ba5d622940` and merges main
`de908e0616186b1fcbe8b5d7caf0307798ca602d` in an isolated worktree. Both parent
histories remain intact. The original PR #21 worktree and qualification report
remain unchanged. No integration commit has been created.

The original requirement is latest stable LanceDB **and** Lance for the separate
Database and StateStore products. The October 23 registry recheck selected exact
LanceDB 0.40.0 and Lance/lance-io/lance-table 13.0.0. The selected Rust toolchain
is 1.99.0; Arrow resolves to 58.3.0 and direct ObjectStore to 0.14.2. The remaining
selected native families are SurrealDB 3.3.0, Turso 0.8.2, Redb 4.3.0,
tokio-postgres 0.7.18, remote Qdrant 1.19.2, and embedded Qdrant Edge 0.6.1.
Codex remains 0.163.0 and OpenCode remains 1.19.3.

The initial registry-only graph resolves without a Geo patch. Edge 0.6.1 is an
explicit selection to avoid the incompatible geometry requirements of Edge
0.8.0 and SurrealDB 3.3.0. Geo 0.32.0 resolves from crates.io with its published
checksum. The removed override's source, patch and provenance remain in main's
Git history. This resolution alone does not prove compilation or extracted
consumer compatibility.

The first minimal-feature build fails inside published LanceDB 0.40.0:
`TerminalResult::decode` references `Error::Http`, while that variant requires
its `remote` feature. The compiler failure agrees with the tagged
[job decoder](https://github.com/lancedb/lancedb/blob/v0.40.0/rust/lancedb/src/job.rs)
and [error definition](https://github.com/lancedb/lancedb/blob/v0.40.0/rust/lancedb/src/error.rs).
That failed build remains retained under its original inputs.

The user then approved enabling **only `lancedb/remote`** as a compilation
workaround, while preserving disabled direct defaults, disabled cloud
object-store features, latest Lance versions and patch-free consumers. The
[native feature declaration](https://github.com/lancedb/lancedb/blob/v0.40.0/rust/lancedb/Cargo.toml)
activates ten optional direct dependencies and `lance-namespace-impls/rest`.
The recorded offline resolution adds 17 registry packages without removing or
advancing an existing locked version. Metadata confirms `lancedb=[remote]`,
`lance-namespace-impls=[rest]`, empty Lance/lance-io/lance-table feature sets,
and no cloud object-store feature in that resolved default workspace lane.
These are dependency-resolution facts, not a compilation or behavior pass.
No Cargo patch, version downgrade or registry-cache edit was introduced.

Independent source review found that native connection selection treats any
`db`-prefixed string as remote, including accepted relative filesystem paths.
The public relative-path regression demonstrated that misrouting; the existing
LanceDB connection owner now resolves the filesystem location before native URI
dispatch. The same exact regression then passed, including absolute-path reopen.
The approved feature does not authorize a new public Storage route or cloud
qualification. Exact approval and metadata identities are retained in
`remote-feature-approval.json` and `remote-feature-metadata.json`; behavioral
red/green evidence is in `storage/relative-path-behavior-red.log` and
`storage/relative-path-green.log` under the new evidence directory.

## Reconciliation owners and required proof

| Area | Retained behavior and reduction | Validation status |
| --- | --- | --- |
| Lance StateStore | One StateStore over one Lance 13 family; explicit format 2.0, no stable row IDs, original sentinel/revisions/tombstones, commit-error authority, bounded transactions and cleanup. Both storage wrappers retain intercepted pagination. Retention must account for overlays before deletion. | Focused red/green and 124 local controls retain their recorded scope. The later V6 matrix passed all eight Storage lanes; see the separate candidate and cleanup accounting below. |
| LanceDB Database | PR #21 memory ownership and Session-cycle repair combined with main's namespace/catalog and eager schema-backfill corrections. | Relative-path red/green and four Session controls passed on recorded candidates. V6 later completed the selected Database/StateStore matrix; no downstream distribution or workspace pass is inferred. |
| Alternative Storage | Main SurrealDB and remote-Qdrant corrections; the approved Edge 0.6.1 selection retains three corrected native calls. Empty defaults, explicit alternatives and SQLite/Qdrant exclusion remain. | The isolated compiler red→green, approved-graph check and retained focused query/index pass are distinct. V6 passed current selected feature/service lanes. Normalized package and extracted-consumer proof remain pending. |
| Storage verifier | Existing feature-policy owner incorporates main's owned endpoints, service readiness and isolated directories. | Endpoint/isolation and bounded process-cleanup red→green retained; 39 script controls passed. V6 completed eight behavioral lanes, 24 legal subsets and eight prohibited combinations with settled ownership. |
| Codex | Main passive item/metadata modules incorporate PR #21's .163 fields; inline duplicates removed. Preserve reasoning reset, partial-answer exclusion, read state, historical terminals, metadata ownership, permission/personality checks, callback redaction and transactional publication. | Exact 29-case combined live inventory verified statically. Reconciled Codex controls passed 137 tests, then a separately captured object-shape regression went red→green. The final current target passed all 138 controls. Live gates remain pending. |
| OpenCode | PR #21 response-future retirement and bound ownership combined with main's unbound creation intent, definite-conflict handling, late discard and primary-error preservation. | The first combined target passed 329 controls and failed one native capability-shape check. That demonstrated defect and separate rejection/cleanup-reporting defects received focused red→green proof below. The final current target passed all 334 controls; live proof remains pending. |
| Custom diagnostics | One existing concrete Provider/Model observation path, with case-specific passive evidence; rollout, native-history and managed-knowledge boundaries remain distinct. | Custom runtime 57 and strict-live assertion 64 controls passed at their recorded source identities; live qualification remains pending. |
| App | PR #21 v7 persistence and independent cold one-way v6→v7 migration retained. Main compiler-compatible assertions and caller-dispatcher scope applied to existing test owners. | Incremental App/Tool merge review found no issue. Current Lance 13 cold reopen of App v7 checkpoint/history passed once; remaining App, existing v6 migration and canonical crash gates remain required. |
| Provider / Tool | PR #21 supplied behavior retained. Stateless OpenAI audio and supplied Gemini Sessions remain unsupported; Realtime and status-read-only bounded retries retained. Tool lifecycle and public rustdocs retained. | Source continuity observed; changed dependency graph still requires fresh qualification. |
| Vendor workflow | Main VendorUpdater is the sole owner. Original gate/interruption failure stays primary through lock-restoration failure; all remaining archive restoration is attempted. No additional retry/lifecycle owner. | Meaningful deterministic red→green, 76 vendor controls, 14 wiring controls and independent actual-diff review passed. |
| Documentation / packaging | Active Geo override machinery removed after registry proof; ordinary path/alias guards have no vendor exception. Current guidance reconciled; historic reports not edited. | The first canonical attempt passed docs inventory and 1,466 doctest checks; generated-output review found a documentation-order issue. The corrected full docs gate then passed separately; scoped patch-free distribution proofs remain pending. |

The selected foreground-only Codex policy supersedes main's older passive
asynchronous-output expectation. OpenCode reopen keeps PR #21's single-attempt
policy; main's distinct recovered public-Abort reconciliation remains. Its
unique repaired-Abort case raises the Agent inventory from 28 to 29. The stdin
approval checks are combined, and Lance conflict composition is retained
instead of adding a duplicate PostgreSQL scenario. These are explicit policy
selections, not silent assertion removal.

## Reduction and public-boundary review

The intermediate source review against both parents found no additional actionable
owner duplication or accidental public-surface expansion. Provider files and
Tool production remain unchanged from PR #21. At that intermediate review, App production changes were only
three compiler-compatible rustdoc link corrections. Later generated-documentation
findings and the reviewed ordering correction are recorded separately below;
App source remains byte-identical to the pre-correction snapshot. Agent protocol additions
are private DTOs moved into existing owning modules; the observation tap remains
one concrete Provider/Model owner. Storage changes extend the existing database,
commit and durable object-store owners. Its opaque future forwarding retains the
same public trait contract and validates data/index requests in the sole private
dispatcher before backend routing. No new public declaration was introduced
relative to PR #21. Dependency changes comprise the explicitly selected native versions, approved
remote-feature compilation workaround and subsequently approved macOS/Qdrant
browser constraint. Each has its own graph and validation evidence.

The following tracked line counts distinguish categories; they are not a
judgment that retained tests or historical proof are redundant. Rustdoc inside
`src` is counted with production source. Renames are counted as removal/addition;
binary archive changes have no line count. The new, untracked integration record
is excluded. The table below records the intermediate review in
`shared/integration-size-public-boundary-v4.json`; later focused regressions and
repairs are additional changes, so these are not final candidate totals. Earlier
size records remain retained with their original classification limitations.

The versioned record separates production/rustdoc, tests, documentation,
validation scripts, manifests and vendored material. Against PR #21 it records
2,339 added / 1,486 removed production-source lines and 13,023 / 2,423 test
lines; against main it records 12,269 / 3,440 and 45,323 / 7,156 respectively.
These include retained work from the other parent, not just new integration
implementation. The obsolete Geo source accounts for most vendor deletion.

Immutable fixture provenance/data, manifests/locks, examples and validation
scripts are separate categories in that receipt. Their retained evidence is not
production growth. In particular, main's removed vendored lines are the obsolete
Geo override; PR #21's migration fixtures remain intact. This source review does
not replace the pending compiler, public-contract, live or package gates.

## Evidence and execution boundaries

New local evidence is under
`target/pr21-integration-20261023` in the preserved original worktree. It includes
both tracked source trees, source/configuration/environment/lock/compiler
identities, historical and diagnostic hashes, the release freeze, registry
resolution, deterministic logs, and independent source reviews. No credentials
are included in this report.

An interrupted build with mismatched debug settings is recorded as incomplete,
not a behavioral failure. Its owned process group was settled before the
low-memory baseline build. The latter reached the native LanceDB compilation
failure. Neither execution ran a live test or produced qualification evidence.

The user subsequently narrowed the implementation and qualification scope to
**Lance 13 only** for the StateStore, with the selected LanceDB 0.40 Database.
The proposed Lance 7→13 creator/reopen campaign is withdrawn; no active old-version
branch, compatibility harness or old/new qualification obligation remains in the
candidate. Any already-recorded build or fixture observations retain their
original identities as historical evidence, not current compatibility claims.
The independent App persisted-schema v6→v7 migration contract and immutable v6
fixtures remain unchanged. Current-version persistence, directory reopen,
retention and crash recovery still require their owning gates; no mixed-version
writers, downgrade support or relaxed durability is implied.

Focused integration results are recorded at their individual source identities:

| Boundary | Retained result and meaning |
| --- | --- |
| Retained native overlays | The original public maintenance control failed before the guard and passed after it. Unsupported overlays are rejected before reclamation; this is not support for overlay-backed StateStore data. See `storage/overlay-red.log` and `storage/overlay-green.log`. |
| Unknown cleanup payloads | The initial StateStore run remains **90 passed / 1 failed**. A strengthened exact control also failed, then passed after reclamation-only listing restriction. It covers current/aged/nested unknown files, index and temporary-manifest sentinels, actual native-orphan removal and retained-history readability. See `storage/native13-unknown-files-strengthened-red.log` and `storage/native13-unknown-files-green.log`. |
| Native durability policy | The first native feasibility run remains **13 passed / 3 failed**. Selecting the supported custom-handler error-preservation policy made all **16** controls pass. This is test-local proof of the required policy, not a claim that Lance's built-in ambiguous-error recovery is inherently defective. See `storage/native13-focused-controls.log` and `storage/native13-current-controls.log`. |
| Exact prepublication errors | The later 13-case native run remains **11 passed / 2 failed**. Enhanced diagnostics showed valid prepublication `InvalidInput` and exact-version conflict errors being wrapped as `CommitStatusUnknown`. Explicit definitive absence for the actual local/memory resolver passed both controls. The public preflight-conflict pass does not reproduce the narrower post-preflight race. See `storage/native13-exact-guard-diagnostic-red.log` and `storage/native13-exact-guard-green.log`. |
| Commit timeout authority | One event-gated native control passed: a positive timeout drops the held post-publication future; disabled timeout preserves it through release. Fresh readbacks bind both landed transaction UUIDs. This is baseline-green native-policy evidence, not a reproduction of the default 1,800-second expiry. Both production commit builders explicitly disable that extra timer; existing admission/observation deadlines and joined finalization remain authoritative. See `storage/native13-timeout-authority.log`. |
| Current-engine StateStore and Session | The subsequent current-only run passed **90 StateStore controls and four Session controls**. The count excludes the newly withdrawn old-version fixture; no retained current-engine assertion was waived. This receipt predates the final commit-policy changes and does not substitute for their remaining validation. See `storage/native13-current-state-session.log`. |
| Final current-engine local controls | After the final cleanup and commit-policy corrections, **17 durability, 13 native API, four Session and 90 StateStore controls passed: 124 parent tests, zero failures**, exit 0 with the process reaped. A nested child summary is excluded from the parent total. At that receipt's original comparison, only a later portable-contract prose edit differed. Subsequent Qdrant adapter, feature-scope and dependency changes mean this is not a whole-current-candidate continuity claim. See `storage/native13-final-focused-controls.json`. |
| Invocation and verifier owners | Focused endpoint/isolation and cleanup controls went red→green; **39** Storage script controls passed, including real disposable-child settlement. The prepared canonical runner's **22** offline accounting controls passed against synthetic and saved historical logs. These are control tests, not execution of Storage services or the canonical qualification wrapper. The final source-bound script refresh passed 39 controls; its omitted lock binding was corrected by a separate rerun of the one lock-reading control. See `storage/script-contracts-final-offline.json`, `storage/script-contracts-final-lock-control.json` and `shared/workspace-provisioning-corrected-review.json`. |

The reclamation restriction remains inside the existing StateStore object-store
owner and only while its exclusive reclamation lease is active. It preserves
listing errors and native manifest/reference authority; ordinary listing is
unchanged. Native Lance 13 already scans all data when unverified deletion is
selected, so the supplemental scan now covers only transactions and deletions.
All data-reference validation remains, without an unused data-path retention
set. Native cleanup can remove empty index directories; preservation of empty
caller directories is not claimed. Independent source reviews cover the actual
cleanup, commit-policy and timeout-control diffs under `shared/`; no new runtime
owner or second cleanup planner was introduced.

The subsequent focused App build stopped at compiler error `E0425`: the
integrated Codex prompt adapter used `PersonalitySelector` without importing it.
No App case executed in that failed build. After the independently reviewed
one-line existing-type import correction, recompilation and the exact
`cold_lance_directory_reopen_preserves_v7_checkpoint_history` case passed:
**one passed, zero failed, zero ignored, 43 filtered**, exit 0 with the child
reaped. The source-bound receipt is
`shared/app-current13-cold-reopen-result.json`; its execution log is
`shared/app-current13-cold-reopen-after-import.log`. This proves the exercised
current Lance 13 cold reopen through fresh handles, not the remaining App,
canonical crash or v6 migration gates. The original compiler failure remains
preserved in `shared/app-current13-cold-reopen.log`.

Focused Agent results now include the following distinct receipts; none is a
live execution or a replacement for the pending 29-case inventory:

- Reconciled Codex protocol: **137 passed**; strict-live assertion controls:
  **64 passed**. The older four failures remain recorded: relative-path classification was a
  demonstrated defect; ownership/history/cleanup expectations were reconciled
  to the selected policy. Relative permission locations classify as Unsupported;
  foreign passive metadata does not acquire ownership; asynchronous output
  remains unsupported under the selected foreground policy.
- Custom runtime: **57 passed**; native cleanup: **19 parent tests passed**.
  Nested child results are recorded separately. The first OpenCode target was
  **329 passed / one failed**, not a current full-target pass.
- The OpenCode capability decoder accepted a positional array where the pinned
  native schema requires an object. The exact red control failed; the smallest
  private object-only decoding correction passed five related inventory
  controls. Actual resolved Serde is **1.0.229**. The versioned authority
  correction preserves earlier reviews that mistakenly named 1.0.228; it does
  not relabel their source claims.
- A separate Codex metadata control reproduced acceptance of positional arrays.
  The existing decoder now requires object-valued metadata and read state;
  omitted/null optional fields retain their valid behavior. The same control
  passed after correction.
- OpenCode native creation rejection was replaced by a failed creation-intent
  discard in its red control. The correction preserves the original native
  failure and attaches cleanup as supplementary. Ingestion rejection instead
  retained Unsupported but omitted the discard failure; its separate red and
  corrected green retain that distinction. Both subsequent knowledge-settlement
  read-error controls reproduced omitted supplementary errors. Their reviewed
  correction passed all four focused rejection-cleanup controls; no nonempty
  knowledge-deletion proof is claimed by these no-Database controls. Final full
  protocol targets passed **138 Codex and 334 OpenCode** controls, with no
  failures, ignored cases or filtered cases. These are deterministic protocol
  results, not the pending 29-case real-runtime qualification.
- Agent backend, compile-fail, final-audit and public-API targets passed **4, 5,
  3 and 22** controls respectively. Source contracts initially failed four
  missing-extraction checks; materializing only the already selected,
  checksum-verified archives made all **15** source controls pass. Locks and
  archives were unchanged; no vendor discovery or update ran.

Receipts and exact source hashes are retained under `agent/`, including
`codex-reconciled-and-assertion-controls-after-borrow.json`,
`remaining-focused-integration-controls.json`,
`opencode-interleaved-green.json`,
`opencode-interleaved-serde229-authority-correction-v1.json`,
`codex-metadata-object-green.json`, `opencode-create-discard-green.json`,
`opencode-ingestion-discard-red.json`, `opencode-rejection-cleanup-green.json`,
`final-protocol-controls.json`, and
`source-contract-after-materialization.json`. Independent incremental reviews
are retained beside them. The focused Cargo processes were reaped. The later
Agent continuity review matches its 210-file source scope and finds no new public
API issue, but the 138 Codex, 334 OpenCode, 64 assertion, 15 source-contract and
Clippy receipts bind their earlier dependency lock. They are not fresh Agent
qualification against the later `9d679aee...f529a1` graph.

The subsequent Rust 1.99 Clippy gate rejected a nested `if` in Lance
initialization error classification (`collapsible_match`). The equivalent match
arm guard retains the same four error variants, canonical UUID comparison and
error precedence; its lint red is preserved separately. Focused initialization
controls passed all four cases. Subsequent Agent Clippy reached strict-live
fixture compilation and exposed omitted main-side helpers plus unused overlapping
recovery diagnostics. The reviewed correction restores the exact one-second
quiet and 30-second idle helpers, removes orphaned duplicate-stdin helpers, and
retains the stronger recovery `prompt_output` owner. Its duplicate 124-line
history reader was removed; four existing assertion controls now exercise the
surviving owner, explicitly rejecting stale-parent or unknown-terminal attribution.
The exact live prompt, assertions, request count and deadlines are unchanged.
All **64 assertion controls** and the final Agent all-targets/all-features Clippy
check passed. These compiler and assertion checks executed no live workload.

A separate PostgreSQL-only compile check confirmed that file-only shared fixture
methods were unnecessarily included in that target. The reviewed correction moves
those methods onto the same `StateCases` owner in a selectively loaded module;
fields stay private, both handles are dropped before reopen, and all assertions
and three 30-second connection bounds remain intact. PostgreSQL-only Clippy and
the existing exact Lance cold-reopen case passed. The latter emitted a macOS linker
unwind-table size warning, retained without suppression in its log; the test
completed successfully. Receipts include `agent/final-clippy-green.json`,
`agent/assertions-after-readback-consolidation.json`,
`storage/postgres-live-fixture-clippy-green.json`,
`storage/file-case-module-cold-reopen-green.json`, and
`storage/recovery-and-file-fixture-applied-review-v1.json`.

A disposable supervisor composition control exposed an additional admission
issue: the outer 20-second forced shutdown can terminate the Storage verifier
before its owned, separately grouped child settles. Its first diagnostic
fallback could not establish exact group absence, so that receipt remains
**UNRESOLVED**; later process/file-descriptor observations do not retroactively
supply the missing identity. A separate exact-identity red control reproduced
that failure and then settled all its owned processes. The reviewed direct-Storage
delegated-cleanup correction passed four controls and was applied to the ignored
invocation owner. It preserves operation deadlines and gives the existing inner
owner time to perform its already bounded cleanup.

The separate canonical-wrapper controls also reproduced premature outer settlement
and false settlement reporting. Its reviewed candidate passed eight disposable
controls plus one pre-Storage interruption variant (nine successful invocations). All exact fixture groups
settled and children were reaped. The reviewed owner was subsequently installed
after V6 settlement, with the prior owner preserved, before the separately
admitted canonical attempt described below. These controls used no real Cargo,
service or qualification workload. Exact receipts are
`storage/scoped-cleanup-red-v2/receipt.json`,
`storage/scoped-cleanup-green-v2/receipt.json`,
`storage/scoped-storage-delegation-applied.json`, and
`storage/canonical-cleanup-controls-complete-v1.json`.

Two user-authorized cache reclamations recovered a recorded **44.47 GiB** in
aggregate. They removed only inactive docs-codegen/debug and original-worktree
`.rlib`/`.rmeta` products. A third removal deleted 5,383 inactive static-library
and metadata files, totaling 16.19 GiB logically, but measured only about
**79.55 MiB additional immediate free space**. Logical file size is not claimed
as physical space recovered. Executables, dynamic libraries, source, packages,
archives and historical evidence were retained. Receipts are
`shared/project-cache-reclamation-01.json`, `-02.json` and `-03.json`; this is
resource settlement, not qualification evidence.

The first complete Storage matrix invocation stopped in its prerequisite gate:
`strict_env_contract` reported **17 passed / 2 failed** because two assertions
still expected Rust 1.94.1 while the fixture and verifier already selected the
approved Rust 1.99.0. No alternative service or behavioral lane was started.
The failed receipt remains unchanged at `scoped-live-20261023-153946`:
exit 101, stable source and input hashes, no retained service ownership or cleanup
errors, and reaped child and absent process group. The independently reviewed
two-string correction preserves the deliberately unsupported 1.94.1 fixture,
rejection-before-Cargo assertion and all other checks. The focused target then
passed **19/19**. This is a compiler-expectation repair, not new backend evidence;
see `storage/strict-env-compiler-pin-red.json`,
`storage/strict-env-compiler-pin-green.json`, and
`storage/strict-env-compiler-expectation-independent-review-v1.json`.

The subsequent compiler-policy audit found the same stale guard in App's routine
workload owner. Its existing mocked routine control produced meaningful red
against the 1.94.1 guard; the owner now requires the selected 1.99.0 compiler.
The first whole offline run exposed a separate fixture mismatch: its synthetic
full-execution baseline still supplied 1.94.1. That failed result is retained.
The corrected current-policy fixture explicitly supplies 1.99.0, while historical
baseline fixtures remain 1.94.1 and an added drift subcase rejects cross-compiler
reuse. All **35 offline controls** passed. No App or capacity workload was run;
execution selections, assertions, deadlines, cleanup and real historical receipts
are unchanged. Evidence is in `shared/app-compiler-policy/`.

Remaining qualification, after the focused reporting and documentation fixes
passed and received their source comparisons:

1. Preserve the successful scoped Storage V6 result and its frozen identities.
   Review later test/reporting/docs-only differences explicitly; do not relabel
   the old receipt as an execution against newly edited files or repeat batches
   through an unrelated wrapper.
2. The user explicitly authorized one new canonical attempt on the repaired
   candidate; it has not started at this report cutoff. Resume only with fresh
   reviewed source/configuration/resource admission:
   Provider 137, Agent 29, configured Tool service, and App 18 workflow plus four
   prepare cases. The first attempt below ran no live cases. App's canonical crash
   case executes once through its owning prepare gate. Long batches run only in
   the canonical Provider phase, preserving both independent 24-hour waits.
   Existing operation, shutdown and 777,600-second outer bounds remain fixed.
3. Complete the remaining current-candidate public/source, clean-candidate and
   distribution gates. Documentation/Clippy prefix passes are recorded evidence,
   not a replacement for mandatory resumed-wrapper gates or clean proof.
   The user-approved distribution scope proves unmodified extracted Provider,
   Tool and Storage packages independently, including Storage's SurrealDB/Qdrant
   composition, and uses Cargo's native multi-package verification for Agent/App.
   No workspace patches, Kit manifest rewrites or publishing are authorized.
   Agent/App standalone distribution remains unproved because their candidate
   siblings are unpublished and the registry's `agents-kit 0.1.0` is a different
   package. This limitation is not a successful standalone-consumer result;
   approval and reviewed preparation remain in
   `shared/package-proof-scope-approval-v1.json` and
   `shared/package-owner-three-roots-native-overlay-proposal-v3.json`.

The prepared package owner's initial 85 offline controls and later 160-control
preservation extension are preparation evidence only. Metadata's 16 MiB and
feature-tree's 1 MiB bounds apply before parsing, after the inherited command
owner reads its private log; they are not new whole-log disk/memory bounds.
Before any package cleanup, a newly reviewed combined executable/dylib closure
must be preserved outside cleaned caches and bound to the full package identity.
The immutable Storage-only preservation set below cannot by itself substitute
for the later complete package closure or full admission identity. No package
or extracted-consumer execution has occurred.

Every terminal result must separate test assertions, remote resource cleanup,
Kit shutdown, child reaping and process-group settlement. A mandatory failure or
incomplete cleanup stops escalation; no unchanged live retry is authorized.

## October 24 matrix and Qdrant findings

The interrupted October 23 matrix remains incomplete. Its historical outer exit
and reaping evidence were unavailable; later process/resource absence is separate
reconciliation evidence, not a reconstructed success or exit status. A new
October 24 invocation retained the same frozen inputs. Its default and Redb
non-live lanes completed, then independent source review identified three missed
adaptations from main's Edge 0.8 API to the selected Edge 0.6.1. The existing
supervisor received one controlled stop during PostgreSQL prerequisite compilation,
before service startup. It returned 143 with **INCOMPLETE**, unchanged source/input
identities, no cleanup errors or retained command ownership, a reaped child and
absent owned groups. See `storage/full-matrix-v4-controlled-settlement.json`.
No live Storage lane or full-matrix pass is claimed for that interrupted V4
invocation. The separate later V6 result is recorded below.

The focused candidate Qdrant build failed earlier inside published Edge 0.6.1,
with `E0275` in `anonymize_collection_values_opt`; it did not reach the ai-kit
adapter. The locked macOS feature tree binds LanceDB's required `remote` feature
to `webbrowser` 1.2.4 and Objective-C features shared into Edge through `sysinfo`.
Rust's [existing trait-solver report](https://github.com/rust-lang/rust/issues/136856)
describes the same recursive `IntoIterator` mechanism. The newer published Edge
0.8/0.9 source supplies explicit generic arguments, but those releases retain the
separate, unresolved Geo requirement conflict with the selected Surreal version.
No native source, dependency patch or compiler flag was changed.

A private full-workspace production-source copy selected published `webbrowser`
1.2.1, which satisfies LanceDB's existing version requirement and predates its
macOS Objective-C switch. The copied source and native Cargo resolution are bound
separately. After initial private-preparation failures were retained, the locked
control compiled Edge and exposed the three expected ai-kit errors: an unavailable
`RetrieveRequest` and two erroneous `map_err` calls on infallible `ShardInfo`.
The existing owners now use Edge 0.6.1's three-argument retrieval and infallible
metadata reads, preserving result mapping and all tests, APIs and deadlines.
The same private control then compiled successfully. See
`storage/edge061-browser121-workspace-compile-v2.json`,
`storage/edge061-api-correction.patch` and
`storage/edge061-api-green-private-graph.json`.

This is isolated compiler red/green evidence, **not qualification of the current
candidate**. The adapter correction left the integration's dependency manifest
and lock unchanged at that point.

The user subsequently approved an optional exact `webbrowser =1.2.1` dependency
for macOS builds with `qdrant`, with direct default features disabled. The
package manifest now declares that constraint instead of relying on a
workspace-only lock pin; preservation in normalized packages remains to be
verified. Its scope and limitations are owned by
[Storage's dependency policy](../storage-kit/AGENTS.md#backend-and-secret-ownership).
Native Cargo resolution succeeded and selected `webbrowser` 1.2.1 in the shared
lock, replacing 1.2.4 and removing `dispatch2` 0.3.1 and `objc2-app-kit` 0.3.2.
It also removed the implicated Objective-C dependency edges and rebound
`oauth2` 5.0.0 from the already-locked `base64` 0.21.7 to already-locked 0.22.1;
no other package version was added. The full delta and registry checksums are
recorded in `storage/edge061-browser121-candidate-graph-v1.json`. This is a
resolution result, not a build or runtime pass.

The shared lock can also change selection in other lanes. Independently enabled
Objective-C features can still expose the native compiler limitation; this is
not a general fix to Edge or Rust. The revision requires current graph validation
and extracted-consumer proof and does not itself establish native behavior.
The subsequent approved candidate check passed under this graph with settled
child ownership; that is compiler evidence. The exact embedded
`qdrant_edge_query_index` case separately passed **one case, zero failed or
ignored, 54 filtered**, including 11 subcases. Its original wrapper remains
**INCOMPLETE/exit 1**, with Cargo exit 0: fixture diagnostics separated the test
start from `ok`, exposing a same-line accounting assumption. Independent review
accepted the retained complete test result and cleanup without another execution.
See `storage/edge061-approved-candidate-check.json`,
`storage/edge061-approved-query-index.json` and
`storage/edge061-query-index-accounting-independent-review-v1.json`.
Normalized package preservation remains pending. Historical receipts and both
parent histories remain intact.

## Settled Storage V6 and later resource preparation

V5 then stopped with **FAIL/101** during SQLite-only compilation, before its
service phase: two Redb-only controls lacked their feature gates. The reviewed
correction adds only two `cfg(feature = "redb")` attributes; test bodies,
assertions, deadlines and cleanup are unchanged. The SQLite-only **nine passed**
remain a valid prefix of their separately retained **INCOMPLETE** invocation.
The completed v2 control then passed Redb **10** and combined Redb/SQLite **19**,
plus formatting and diff checks, with stable source and settled children. See
`storage/state-store-local-feature-scope-independent-review-v1.json` and
`storage/state-store-local-feature-scope-green-v2.json`.

The separately admitted V6 matrix at `scoped-live-20261024-132217` completed
with **PASS/exit 0**, on candidate
`b8b03c263f7bb4916f5c4a1720ccfce36b9314ef0b4ac65d869c4d3c885b7927`
and lock `9d679aee125445ed05d4f717564e7e7ab6588a882a766fcbde64f10893f529a1`.
All 181 frozen input bindings and before/after source identities matched.

| Storage lane | Passed | Failed / ignored / filtered |
| --- | ---: | --- |
| Default Lance | 35 | 0 / 0 / 0 |
| Redb | 37 | 0 / 0 / 0 |
| PostgreSQL | 43 | 0 / 0 / 0 |
| SurrealDB | 65 | 0 / 0 / 0 |
| SQLite | 37 | 0 / 0 / 0 |
| Qdrant | 55 | 0 / 0 / 0 |
| Redb + PostgreSQL + SurrealDB + SQLite | 77 | 0 / 0 / 0 |
| Redb + PostgreSQL + SurrealDB + Qdrant | 95 | 0 / 0 / 0 |

This is **444 case executions across eight lanes and 97 unique case names**;
each lane's exact ordered inventory matched its execution. The same invocation
completed all **24 legal feature subsets and eight prohibited SQLite/Qdrant
unions**, required deterministic targets, compilation, Clippy, rustdoc and
doctests. Negative temporary diagnostics follow the existing cleanup policy;
no separately retained per-union log is claimed. Native service observations
were PostgreSQL 18.6, SurrealDB 3.3.0 and remote Qdrant 1.19.2; the host proof is
`aarch64-apple-darwin`, with service containers `linux/arm64`, not other-host
qualification. Its 108,000-second command and existing delegated cleanup bounds
were unchanged.

Scenario cleanup passed. All three owned Compose containers, three volumes and
one network have matching removal acknowledgements. Per-lane dataset directories
were removed; four empty configured base directories remain and are not claimed
deleted. The child was reaped, owned group settled, no nested ownership remained,
and supplementary cleanup errors were empty. Storage has no Provider-style
workspace shutdown API; no additional shutdown is invented. See
`storage/full-matrix-v6-terminal-independent-review-v1.json` and the original
`matrix-summary.json`. V3's historical missing outer-exit proof, V4 interruption
and V5 failure remain intact. This pass qualifies the selected Storage scope,
not Provider, Agent, Tool, App, packages or the full workspace.

Later resource work is separate from qualification. The earlier cache tiers
retain their original statements and receipts; subsequent operations were:

| Operation | Preserved scope | Observed free-space delta |
| --- | --- | ---: |
| Cache 04 | 5,307 regenerable static artifacts; source/history/executables retained | +2,074,238,976 bytes |
| Cache 05 | 1,996 regenerable static artifacts; no capacity execution or result-body access | +2,869,927,936 bytes |
| Cache 06 | 1,991 regenerable static artifacts; executable and evidence identities retained | +9,842,688 bytes |
| Archive 01 | Nine inactive executables, losslessly archived and fully restore-verified before unlink | +92,557,312 bytes |
| Archive 02 | Eight inactive final-Cargo executables, losslessly archived and restore-verified | −3,688,112,128 bytes during concurrent compilation |
| Archive 03 | Eight exact integration-cache duplicates, separately user-approved; reused Archive 02 files, all eight scratch restores verified before any unlink | +7,118,409,728 bytes (**6.6295 GiB**) |
| Cache 07 | 650 regular nonexecutable `.rlib`/`.rmeta` files for 82 registry versions absent the current lock, proved with 436 retained dep-info files | +1,223,483,392 bytes (**1.139 GiB**) |

Logical lengths, APFS sharing and concurrent allocations do not establish
exclusive physical savings or guarantee future headroom. Archive 03's new
integration-path restoration map preserves the archive bytes and protected
source/history; original paths are absent, scratch directories empty. Restoration
must refuse overwrite and reverify content/metadata before original-path use;
inode/ctime/birthtime preservation is not claimed. Cache 07 leaves dep-info,
fingerprints, every current-lock product, executable, dylib, archive and qualified
artifact untouched. Each operation's durable receipt and independent settlement
review remain under `shared/`; none modifies historical test outcomes.

Before later clean/package work, **98 current Storage-qualified artifacts** were
preserved as independent regular APFS COW copies outside `target/build`, with
source/copy hashes equal, distinct inodes, before/after source inventory equal,
and no hardlink, symlink or full-copy fallback. The 45,152,754,560 logical bytes
are preservation scope, not newly allocated or reclaimed disk space. The first
attempt's filesystem-preflight failure remains recorded; the reviewed existing
owner correction preceded the successful copy. See
`shared/qualified-artifact-preservation-storage-v6-20261024-v1.json` and its
independent output review. This immutable Storage set retains its three-field
source identity; later package admission needs its own complete combined closure
and full identity, without rewriting this receipt.

The same reviewed owner then seeded the separate docs build cache using COW:
**49,704 files**, 39,773,256,508 logical bytes, **COMPLETE/exit 0**, with identical
source inventories and no fallback or unresolved copies. The operation took
67.88 seconds; its observed free-space delta was −26,800,128 bytes. Cargo freshness
remains authoritative. The seed does not qualify code or promise sufficient space
for later linking or packages. Canonical launch received a fresh resource and
process check; package capacity remains a later separate admission. Evidence:
`shared/docs-cache-seeding-result.json` and
`shared/canonical-workspace-readiness-audit-v6.json`.

## First canonical attempt and focused reporting repair

The one canonical attempt at `scoped-live-20261024-150649` first verified
inventories **Agent 29, Provider 137, Tool 1, App workflow 18 and prepare 4**
with `--list`; these commands executed no cases. Native Codex and two OpenCode
prerequisites then started under their existing owners.

The wrapper passed root hygiene, path/alias guards, formatting, the non-Storage
workspace Clippy invocation and all eight Storage Clippy lanes. Documentation
inventory passed **10/10**. Rustdoc/doctests completed with **1,466 passing
checks**, zero failures/ignored: Agent 271, App 128, Provider 250, Tool 66,
and Storage 245 default, 255 SQLite composition and 251 Qdrant composition.
Those totals count separate feature-lane checks, not unique examples. They
include **21 compile-fail controls**; `no_run` examples are compilation-only.
The count does not claim every example body executed.

Agent's deterministic precheck process controls passed **13/13**. Its Question
reporting process controls then finished **eight passed / one failed**:
`deeply_nested` rejected the malformed receipt with sanitized `ValueError`,
while the test required `RecursionError`. The wrapper command exited **1** before
actual strict-live prechecks or any live suite. **Zero live cases, zero batches
and no App prepare/crash case ran.** The later exact-case accounting check could
not find an unexecuted live target and recorded `ValueError`; the enclosing
receipt is **INCOMPLETE**, retaining the original command exit 1.

All three provisioned native children were reaped and their groups settled;
outer command and nested ownership settled, cleanup errors were empty, and
source/input/credential-source hashes were unchanged. This is process cleanup,
not a claim of Kit shutdown for scenarios that never ran. No live-owned batch
or file cleanup was needed. The independent terminal review is
`shared/canonical-workspace-terminal-independent-review-v1.json`.

The cause investigation confirmed that Python 3.11 rejects the 1,500-level JSON
array during parsing, whereas Python 3.14 parses it and the reporter rejects its
nonobject shape. The reporter implementation is unchanged. The narrow test fix
accepts exactly one sanitized `RecursionError` or `ValueError` line, adds exact
exit 1 and no PASS-banner checks, and preserves payload, redaction, cleanup,
deadlines and live-policy assertions. Both interpreter runs passed all **nine
control groups**, serially, at `scoped-live-20261024-152718`, with stable source,
empty cleanup errors and exit 0. Source `aa8e7d32...459748a2` differs from the
canonical source only within the separately reviewed repair scope; its receipt
is not a replacement for the earlier failed invocation. See
`shared/canonical-reporting-control-repair-independent-review-v1.json`.

Generated-output review also identified a build-order issue: App was documented
before Storage pages existed, so cross-crate storage anchors were not rendered
as intended. Re-rendering the **original App source** after Storage docs existed
produced the expected anchors; changing valid public API links was not the
necessary correction. An initial explicit-link edit failed rustdoc's redundant-link lint and was
reverted; that failed attempt remains recorded. The reviewed fix moves only the
existing **Agent and App documentation block** after the final Storage default
render, preserving all eight documentation commands, seven doctest commands,
warning flags, compatible feature lanes, staging and publication behavior.
Original App source and the Question reporter remain byte-identical to their
pre-correction snapshots. Independent actual-diff reviews are
`shared/canonical-reporting-control-repair-independent-review-v1.json` and
`shared/canonical-reporting-control-repair-v1/docs-render-order-actual-diff-independent-review-v1.json`.

The corrected **full docs gate passed** at `scoped-live-20261024-153128` on
candidate `7ac8d05d7aeee1ef0db7e4495e0d777c21aa1c4355271b7574045e0e9a9eb610`,
with the unchanged `9d679aee...f529a1` lock. It reran all **10 documentation
controls and 1,466 doctest checks**, zero failed/ignored, with the same
per-crate/feature-lane counts as the first canonical prefix. Formatting and
`git diff --check` also returned 0. Each command's before/after source identity
matched, no timeout or cleanup error occurred, and the owned processes settled.
These are documentation and deterministic-control results, not live qualification.

A fresh continuity comparison checks all 181 Storage V6 input bindings: **180 are
byte-identical**, and the sole changed ignored path is the separately reviewed
installation of the canonical cleanup policy in `run_scoped_live.py`. Storage
source, tests, manifest, toolchain, dependency lock, original selected lanes and
assertions are unchanged. Provider's tracked tree remains byte-identical to PR
#21 `81d34036`; the reviewed reporting/docs changes do not modify Provider paths.
The reviewed post-`b8b03c` correction changes only the Question reporting test and
docs generation order; this new report is the proposed third changed path.
The comparison preserves Storage's recorded scoped applicability without
rewriting its PASS/source hashes. Provider's old 118+19 receipts remain historical
because the integrated dependency graph already differs from their graph;
source continuity does not satisfy pending fresh Provider qualification.

The user explicitly approved **one new canonical attempt** ("Run the repaired
candidate once") after the failure and focused repairs. Authorization is not an
execution receipt: that attempt remains **not started** at this report cutoff,
and fresh root admission and eventual terminal results remain required. No
extra standalone batches, captures, crash run or capacity workload are included.

Full workspace/live, clean-candidate and scoped distribution qualification remain
pending. Historical Provider and Agent/App receipts below are not current passes
for this newly integrated graph or these later documentation/control edits.

## Second canonical attempt and subsequent review corrections

The historical sections above retain their recorded candidate and cutoff. The
newly authorized attempt ran once at `scoped-live-20261024-154455` on candidate
`ef9fd2690cf7ade1e323b780fb67a102255da0bfa499c44a36746f4ef360893f`,
with the unchanged `9d679aee...f529a1` lock. Its command exited **101** at
Agent's compile-contract target: **0 passed / 5 failed**. Under the owned
in-repository `TMPDIR`, each generated downstream package was incorrectly
associated with the ancestor workspace before reaching its intended Rust
compiler diagnostic. The existing diagnostic guard correctly rejected that
failure; it does not demonstrate a public API regression.

The enclosing runner remains **INCOMPLETE**: its later exact-case accounting
reported a missing, unexecuted live target while preserving the original 101.
**No live case, batch, App preparation or crash case ran.** All three provisioned
native children were reaped and their groups settled; outer and nested command
ownership settled, cleanup errors were empty, and source/input/credential-source
hashes remained unchanged. No live-scenario Kit shutdown or remote-resource
cleanup is inferred from prerequisite-process settlement. See
`shared/canonical-workspace-terminal-independent-review-v2.json`.

Focused controls at `scoped-live-20261024-155455` reproduced the same isolation
failure in Provider (**zero passed / one failed**) and Tool (**zero passed /
six failed**), with settled processes and no temporary children left behind.
The smallest repair adds an empty `[workspace]` table to each existing Agent,
Provider and Tool generated consumer manifest. Original sources, required Rust
diagnostics, dependencies, copied lockfiles, commands, supervision and temporary
cleanup remain unchanged. This is a test-fixture correction, not a Kit runtime
repair. Its actual diff is independently reviewed in
`shared/compile-fixture-workspace-isolation-independent-review-v1.json`.

The requested duplication review consolidated two redundant personality tests
into `codex_retired_personality_cannot_mutate_native_state`. It retains all-model
inventory checks, Unsupported rejection for all three retired values, absence
of queued turns and native mutations, workspace/conversation admission and
revision checks, explicit close of unexpectedly accepted handles, and the
existing bounded `finish_release` cleanup with primary-error preservation.
Reasoning reset and persisted-personality reopen tests are unchanged. The
Codex deterministic inventory decreases **138 to 136**, confirmed by the completed
owning target below; the historical
138-pass receipt retains its original outcome, and the **29-case strict-live
inventory is unchanged**. This removes the identified duplication, not a claim
that all workspace duplication has been eliminated. See
`shared/personality-consolidation-independent-review-v1.json`.

The three validated Storage documentation findings are also corrected: README
now distinguishes LanceDB Database from embedded Lance StateStore; architecture
states the current directory-backed native-filename reclamation boundary; and
testing guidance separates the strengthened orphan-only regression from shared
full-cleanup source reasoning and labels the older proof historical. Unknown
names are preserved regardless of age, but unreferenced native-shaped names can
be reclaimed and empty directories have no preservation guarantee. These prose
changes do not alter format 2.0/schema 1 constraints or any implementation. See
`shared/storage-current-lance-doc-applied-independent-review-v2.json`.

Focused validation at `scoped-live-20261024-155813` completed **PASS/exit 0** on
candidate `985b7c2c68bbb1312f2c2ac1559e2da51a5c134c2d37f094e37cd341073ad5ae`,
with the unchanged `9d679aee...f529a1` lock. All **11 commands passed**:
compile-contract controls Agent **5**, Provider **1** and Tool **6**; the complete
Codex target **136**; Agent source contracts **15**; three affected Clippy
commands; documentation contracts **10**; formatting; and diff checks. Rust
summaries report zero failed, ignored or filtered. Each command's source identity
remained stable, no timeout or cleanup error occurred, processes settled, and
the owned temporary directory is empty. These are deterministic/compilation and
documentation results, not live qualification. The retained `receipts.json` and
logs bind their exact sources; the earlier canonical and red failures remain
unchanged. This focused pass does not authorize another canonical attempt.

Only deterministic test fixtures, the reviewed test consolidation and active
Storage prose changed in this follow-up; production source, public APIs,
dependencies, live assertions, models, deadlines and historical evidence did
not. The Storage V6 PASS remains attached to its recorded scope and identity.
Provider's original 118+19 receipts remain historical under their older graph;
there is still no current integrated Provider or workspace qualification.

## Preserved historical qualification

The original Provider qualification remains **118 fresh nonbatch passes plus
19 reused historical batch passes**, under its original source and dependency
identities. It is not current evidence for this changed graph. Original
Agent/App/package outcomes and all earlier failures retain their recorded
criteria. The five withdrawn audio/session cases remain withdrawn. The two
OpenCode Question and two OpenAI Realtime grading exceptions remain exactly
scoped; raw copy grades remain visible and all other acceptance rules stand.

Preserve the Anthropic capture limitation. The recorded Gemini generated-output
retention exception remains **undeleted, expiration unverified**. Historical
capacity results remain historical; this integration authorizes no capacity
execution. No push or GitHub merge is included. A local two-parent integration
commit is conditional on all required gates and review findings settling.

## Lance 13 ownership reduction and scoped validation

This follow-up reduces the existing Lance 13 StateStore adapter; it does not
replace its durability contract with native defaults. The starting candidate is
`997d50141fec65356c55434893f491c4a33a915926ce46d95e496cad352dfa60`;
the reviewed implementation and owning-guide candidate is
`71cfeba337a1d40ea98bd0d4b30a9de8150d6cb298f1625bfa889038543cbc44`,
with the unchanged `9d679aee...f529a1` lock.

The two production files have a net reduction of **15 lines**. `LanceObjects`
now supplies the single native `WrappingObjectStore` ingress. Directory wrappers,
streams and uploads share its existing `Arc<DurableIo>`, whose ledger is a direct
mutex rather than a separately shared allocation. The write policy represents
only preparation, conditional creation and overwrite; unsupported updates remain
rejected before filesystem effects. One pairwise settlement method replaces two
identical error-composition matches, preserving primary error types, supplementary
labels and the unconditional final file barrier. Copy retains its separate policy
mapping; multipart completion retains preparation-file classification.

Necessary machinery remains: durable deletion and reclamation barriers,
memory-owned multipart terminal state, staging/upload/failure accounting,
child-task draining before runtime teardown, pagination fallback through the
wrapped store, and retained-reference validation before cleanup. Native
`object_store` fsync omits standalone deletion durability and discards some
cleanup failures ([pinned 0.14.2 implementation](https://github.com/apache/arrow-rs-object-store/blob/v0.14.2/src/local.rs));
Lance's writer can spawn abort work on drop
([pinned Lance 13 writer](https://github.com/lance-format/lance/blob/v13.0.0/rust/lance-io/src/object_writer.rs)). Those differences
do not justify delegating these responsibilities away. Schema 1, format 2.0,
exact conflicts, post-publication error authority, admitted-work completion,
public APIs, dependencies, assertions and deadlines remain unchanged. The
actual incremental source and owning-guide reviews found no unresolved issue;
see `lance13-reduction/reduction-actual-diff-independent-review-v1.json` and
`ownership-and-documentation-independent-review-v2.json`.

The pre-edit baseline at `scoped-live-20261024-161715` passed **17 native
feasibility controls plus five exact supplied-StateStore cases**, with stable
source identities and empty cleanup-error lists. This is passing baseline
evidence, not an invented defect reproduction. After the reduction,
`scoped-live-20261024-161945` passed three exact supplied cases: filesystem
obstruction without publication followed by progress; transaction ownership
after the last store handle drops; and large JSON commit/reopen. Each reported
**1 passed, 0 failed, 0 ignored, 89 filtered**, exit 0 and no cleanup errors on
candidate `71cfeba...cbc44`.

Native feasibility establishes native extension-point behavior, not execution of
the Kit's private combined staging-cleanup/final-barrier failure branches. Those
branches retain a source-equivalence review; supplied public cases establish only
their exercised behavior. This macOS evidence does not establish Linux durability.

The scoped invocation at `scoped-live-20261024-162252` settled **FAIL/exit
101**, with unchanged source `71cfeba...cbc44`, no timeouts and no recorded
cleanup errors. Its default Storage gate passed: **165 deterministic tests**,
**39 feature-policy controls**, **245 doctests**, and **35 embedded strict-live
cases**, plus formatting, Clippy and rustdoc. The five local Agent targets
passed **22 + 9 + 57 + 136 + 334 = 558 tests**. These use local/scripted lower
boundaries and do not establish native model behavior.

App completed **630 tests across 33 ordinary contract targets**, all **four
preparation cases**, **180 routine workflow cases plus 38 worker cases**,
**128 doctests** and all **seven executable examples**. The canonical crash
case executed once as one parent with four owned process-loss boundaries.
Routine inventories retained their explicit **10 workflow and one worker
exclusions**; no capacity measurement ran. App then failed its original
warnings-as-errors Clippy command on two deprecated `AtomicUsize::fetch_update`
calls in test code. App rustdoc and alias/path guards, and the enclosing
documentation/diff commands, were not reached. This failed receipt is retained;
it is not relabeled as an uninterrupted App-gate pass.

Pinned Rust 1.99 source establishes that the deprecated method directly forwards
to `try_update` with the same arguments. The subsequent independently reviewed
test-only correction replaces those two names without changing orderings,
closures, counters, assertions, deadlines or cleanup. It is separate from the
Lance production reduction. The limited follow-up at
`scoped-live-20261024-175502` on source
`e995814fc41f09155dc36b94dd80ae3c71d4707579f7fe8b3a9a6e2696702010`
settled **PASS/exit 0** with all **10 commands passing**. A fresh inventory
contains 190 workflow cases. The two named counter/conflict controls each report
**1 passed, 0 failed, 0 ignored, 189 filtered**; the exact previously failed
Clippy command, strict App rustdoc, alias/path guards, **10 documentation
contracts**, formatting and diff checks passed. Source and lock identities stayed
stable, no timeout or cleanup error occurred, and process ownership settled.
This did not repeat `check.sh`, the crash case, routine suite or external
qualification. The static follow-up is a separate receipt, not a rewrite of the
original failure.

The terminal interpretation is recorded in
`lance13-reduction/scoped-gates-terminal-independent-review-v1.json`;
the narrow test correction and source equivalence are recorded in
`app-atomic-correction-independent-review-v1.json`. The successful follow-up has
its own `scoped-static-followup-terminal-independent-review-v1.json`.

Public test cleanup assertions passed in their exercised scenarios. The outer
and nested process owners recorded no cleanup failures, and a separate process
snapshot found no survivors in **35 recorded owned groups** across these scoped
invocations. This pass created no external Provider resources; process settlement
does not establish cleanup of historical remote resources. Nonfatal linker
warnings remain in the original logs.

The final pre-report comparison found exactly **five changed files** against
1,579 frozen source files: the two reduced production owners, two active Storage
guides and the two-call App test correction. All **36 frozen non-build ignored
bindings** remained unchanged, including environment and historical receipts;
three explicitly mutable build products were rebuilt and are not treated as
immutable receipts. The append-only report is the sixth changed path. Public
API owners, persistence schema, format constraints, dependencies and all other
tests remain unchanged. See
`lance13-reduction/settled-source-and-process-comparison-v1.json`.

The default Lance gate is fresh evidence for this private adapter reduction.
Alternative Storage lanes were not rerun; their earlier receipts remain evidence
for their recorded candidates and scopes. The local Agent/App controls above
are fresh scoped evidence, not the 29 native Agent cases, 18 App workflow-live
cases, distribution checks or a workspace verifier. The independent continuity
review preserves applicability only where unchanged source/dependencies support
it; it does not fill the integration campaign's outstanding qualification gaps.

This pass makes no complete workspace qualification claim. Storage V6 and all
older Provider, Agent/App/package and failure receipts retain their original
source identities and limitations; their hashes are not refreshed to describe
this reduction. Provider's historical **118 fresh nonbatch plus 19 reused batch**
passes remain historical. No capacity workload, external-model generation,
redundant batch or unrelated live suite belongs to this scoped pass.


## Reduced-candidate qualification continuation — October 24, 2026

The user authorized resuming only the remaining integration gates, reusing
applicable receipts and avoiding repeated passed workloads. The reviewed
continuation ran on candidate
`700b9b352c561d372b8f072d5bdfa35758889d130450abe1259c54e2419c1b87`, with
lock `9d679aee125445ed05d4f717564e7e7ab6588a882a766fcbde64f10893f529a1`,
Rust 1.99.0, native Cargo `--locked`, one build job and serial Cargo commands.
This is a continuation with explicitly reviewed receipt reuse, not a fresh
uninterrupted canonical workspace-wrapper pass.

Only the existing ignored invocation owner and its offline controls changed.
The new explicit continuation admission binds source, original receipts,
reviewed successful prefixes of failed runs, exact verifier-derived native
commands, graders, prerequisites and cleanup. Diagnostic baseline-red controls
preceded the narrow change; all **55 offline controls** passed. Two independent
actual-diff reviews approved the continuation before execution. Their bindings
and applicability decisions are retained under
`resume-reduced-lance13/` in the integration evidence root. No production,
public API, dependency, assertion, model or deadline changed.

The applicability review retains the earlier Storage V6 alternative-backend and
feature-policy proofs together with the fresh reduced-Lance default gate. It
also retains the five completed local Agent targets and App's completed local,
preparation, canonical crash and routine prefixes, followed by the reviewed
static correction. The original `162252` failure remains failed. Combined
Storage doctest lanes retain only explicitly compared example applicability;
this is not a fresh eight-lane run or new Linux durability evidence. No Storage
workload, App preparation/crash case, routine suite or already-proven static gate
was repeated. See `storage-app-applicability-review-v1.json`,
`storage-combined-doc-applicability-addendum-v1.json`, and
`provider-tool-docs-receipt-applicability-v2.json` in that directory.

The invocation at **`scoped-live-20261024-183550`** settled **FAIL/exit 101**.
All four compiled inventories matched: **Agent 29, Provider 137, Tool 1 and App
18**. Of 39 recorded commands, 38 exited zero: four inventories, initial hygiene,
32 remaining deterministic commands and Agent doctests. Fresh local results were
**103 Agent, 623 Provider and 111 Tool tests**, plus **271 Agent doctests**.
The Agent total excludes two nested cleanup-child summaries; the earlier
progress count of 105 included those children. The Provider and Tool library
commands each correctly contained zero tests. No timeout or command-cleanup
error was recorded.

The Agent live suite completed **26 passed, 3 failed, 0 ignored, 0 filtered**.
Its 29 unique root cases include three existing nested-child executions, which
are not extra inventory cases. All Codex cases, Custom Anthropic/OpenAI
continuation, managed knowledge and OpenCode structured output passed. Three
OpenCode scenarios remain blocking:

- `opencode_question_delivery_replays_resolves_once_and_drains`: the native
  pending Question descriptions were `" Continue"` and `" Stop"`, with leading
  spaces, instead of the exact requested `"Continue"` and `"Stop"`. Public
  interaction delivery, replay and schema checks had passed. The typed-question
  comparison failed before answer submission, terminal fidelity and grading.
  It is outside the final-answer copy-quality exception.
- `opencode_reopen_cancels_recovered_pending_interaction_before_publication`:
  the child received an empty successful terminal response before the required
  pending interaction. The child failed and was reaped; the parent consequently
  never reached the intended recovery/follow-up journey. Its recovery-output
  diagnostic is explicitly incomplete with `followup_unavailable`.
- `opencode_repaired_abort_releases_generation_and_later_abort_is_idle`:
  an empty successful terminal response arrived before the required admission
  point. The repair/abort behavior was therefore not exercised.

For the latter two cases, the bounded native-history snapshots show a matching
original prompt, terminal `stop`, no text parts and no tool calls. These snapshots
are complete for their stated readback boundary; they are not raw model-service
captures and do not establish model/native authorship or a Kit adaptation
cause. No Kit repair or assertion relaxation is justified by this attempt.

The native-idle Question case emitted a valid successful Kit receipt with
**exact-copy grade true**. The failed Question-delivery case emitted no successful
receipt, making its grade unavailable. The required two-receipt grader correctly
failed; Cargo's original exit 101 remained primary. These are not three waived
copy-quality failures. Provider Realtime grades were not produced because its
native phase was not reached.

Cleanup and qualification remain separate. Successful cases retain their own
cleanup assertions. Failed parent paths ran their bounded cleanup without
recorded phase errors; the recovery parent's scenario cleanup explicitly
succeeded, but graceful Kit shutdown in its failed child is not established and
no reopened handles existed. Receiver-drain and other success-only assertions
that were not reached are not reported as passing. All three owned prerequisite
services were reaped and their process groups settled; no outer cleanup error
was recorded. A separate post-settlement process snapshot found no survivors in
**42 recorded groups**. This is not a new remote-resource absence check.

Before this report-only append, all **1,579 frozen source files**, **52 bound
inputs**, the lock and all four pre-live executable hashes/metadata remained
unchanged. Historical receipts retain their original bytes and identities.
The final source/artifact/process check is
`settled-source-artifact-process-check-v1.json`; terminal interpretation is
independently reviewed in `continuation-terminal-independent-review-v1.json` and
`continuation-storage-app-reuse-terminal-review-v1.json`.

The mandatory failure stopped escalation. **Provider 137 (including batches),
Tool live, App's 18 workflow-live cases, final hygiene and clean-candidate/package
proofs were not reached.** No additional attempt, redundant batch, capacity
workload, commit, push or merge occurred. Earlier Provider **118 fresh nonbatch
plus 19 reused historical batch** evidence remains historical; the integrated
HTTP graph still requires its own qualifying evidence. The five withdrawn
cases, retained Realtime support, Anthropic capture limitation and historical
Gemini generated-output exception remain unchanged: **undeleted, expiration
unverified**. No complete integration qualification or merge readiness is claimed.


## Question description grades and two native captures — October 24, 2026

The user approved separating Question description copying from Kit checks and
capturing the two previously failed empty-completion scenarios. This pass
preserves the preceding **26 passed / 3 failed** result under its original
criteria. It does not upgrade that receipt or reopen broader qualification.
No production, public API, dependency, model, prompt, generation limit or
deadline changed. The only acceptance revision is the explicitly authorized
Question description-copy split within the existing two graded cases.

`QuestionEvidence` now keeps three independent model-copy observations per
successful approved case: the unchanged whole terminal answer, the Yes option's
description and the No option's description. Descriptions are compared without
trimming, extraction or normalization. Their actual native pending values must
still match the running and completed tool inputs exactly. Header, question,
labels, order, cardinality, flags, original prompt/model, call correlation,
answer submission, native/Kit output fidelity and cleanup remain blocking.
Malformed or missing descriptions still fail. The bounded version-2 reporter
requires both named successful Kit receipts and all six boolean grades; false
quality grades remain visible and nonblocking. Historical version-1 receipts
remain unchanged and cannot supply the new description grades. Neither Question
case was rerun, so current version-2 live grades remain unavailable.

Meaningful red evidence recorded the old check rejecting an otherwise faithful
leading-space description and the old reporter rejecting the new independent
grades. Fresh green validation passed **66 assertion controls**, **11 process-level
reporter controls**, **15 Agent source-contract tests**, affected Agent Clippy,
formatting, documentation contracts and diff checks. The reporter control's
complete unittest `OK` log is retained; its original tool-level exit metadata was
not retained separately. Later process inspection found no surviving control.
Rust **1.99.0**, native Cargo `--locked`, one build job and serial Cargo execution
were preserved. Existing observer controls and added request/projection/runner
failure controls passed, including real local TLS, byte forwarding, bounds,
redaction and joined cleanup. Independent actual-diff reviews approved both the
grade boundary and capture owner. A demonstrated diagnostic finalization issue
was corrected before execution: export failures now preserve the Kit error,
mark evidence incomplete and cannot bypass observer settlement.

The reviewed capture extended only the existing ignored `KnowledgeRequest` and
`KnowledgeWireRun` owners. It identifies foreground traffic by the exact original
prompt independently of tool presence, records ordered SSE/tool fragments and
separates native title traffic. The original local endpoint receives one request
for each observed OpenCode request. Only the invocation's
`provider.ollama.options.baseURL` changes; reversing that overlay reproduces the
original configuration. Authoritative environment bytes remain unchanged.

Invocation **`scoped-live-20261024-205300`** ran on candidate
`950714413580403a7c8756fd22be1dde6302eaf5104e19ade0060b32c0900f09`, lock
`9d679aee125445ed05d4f717564e7e7ab6588a882a766fcbde64f10893f529a1`, and
Agent executable SHA-256
`d97f273ce7f17c0302cc8a6fa0b044d516b0d6ce007e66247b5c327587844021`.
It verified the full **29-case** inventory, then selected each failed case once
with **1 selected, 0 ignored and 28 filtered**. The existing nested recovery child
is not an additional root case. Each command retained its **7,200-second**
supervisor and all existing scenario, prompt and cleanup budgets.

Read-only identity checks recorded OpenCode **1.19.3**, Ollama **0.40.10**, and
`ministral-3:8b-instruct-2512-q4_K_M` with digest
`100cf353f4fb9447e81485bc81f54ef3ddcc54bbb38d6bd3601d5ff5a02c5250`.
The HTTP server-version/model readback does not independently bind the serving
process's binary. Both initial native requests contained the exact 774-byte
original user prompt, the selected model, a valid Question declaration,
`tool_choice: "auto"`, a 4,096-token allowance and no explicit sampling overrides.
Each capture observed one complete foreground HTTP/SSE exchange, with no
additional title or auxiliary request.

- **Recovery remained failed.**
  `opencode_reopen_cancels_recovered_pending_interaction_before_publication`
  returned 319 bytes of fenced Question JSON as ordinary text, with `stop`,
  no tool-call fragments and complete `[DONE]` framing. The native assembled
  text and unmodified Kit output both hash to
  `30ff49ebbe695ef8b3b1d59057a642b51cd266a12d7ed1cb4489d00f7646780d`.
  The separately retained native-history snapshot shows the matching original
  user, one successful terminal assistant, 319 text bytes and no tools. The
  required interaction never occurred, so the intended recovery and follow-up
  assertions were not reached. This establishes missing tool execution at the
  observed Ollama-compatible boundary for this attempt, not Kit text loss.
- **Repaired abort passed unchanged.**
  `opencode_repaired_abort_releases_generation_and_later_abort_is_idle`
  received a genuine `question` call with valid captured arguments and
  `tool_calls` terminal framing. Its admission, repaired-abort release and
  later-idle assertions completed. This qualifies that exact recorded journey;
  it is non-reproduction of its earlier empty completion, not a Kit fix.

Neither historical empty response recurred. The projected system/user content,
tool schemas, model and selected settings agree across the two requests, but
the complete request bodies have different lengths and hashes; the retained
projections do not locate that difference. This is not a controlled identical-
request experiment. These captures do not determine whether the earlier
emptiness originated in model generation or lower native processing, and do not
establish statistical reliability. The recovery failure
now has a complete native/Kit text comparison; no demonstrated ai-kit request
or normalization defect warrants a production correction. Tool choice remains
caller/native policy as before; no prompt change, required-tool override,
output repair, generation retry or additional execution was introduced.

The invocation settled **FAIL / exit 101**, retaining recovery's primary error.
Both observers reported complete forwarding/projection, joined handlers/workers,
clean exit, child reaping and settled groups. Recovery's parent explicitly
reported successful scenario cleanup; its failed crash child's graceful Kit
shutdown remains **unestablished**. Repaired abort's cleanup owner returned
without a cleanup-error diagnostic. An independent post-settlement process
snapshot found no survivors in **11 owned groups**. These facts do not convert
socket forwarding into proof of application delivery; the recovery text hash
and Kit result supply their separate observed boundary comparison. No remote
resource deletion or absence claim is added.

Before this append, all **1,614 frozen source/diagnostic/history paths**, **47
bound inputs**, the lock and artifact identity were unchanged throughout live
execution. Evidence, reviews and the source-continuity comparison are retained
under `question-grades-empty-captures/` in the integration evidence root. The
pre-live freeze, `settled-source-artifact-process-check-v1.json` and
`receipt-applicability-source-continuity-v1.json` preserve original hashes.
The report's preceding bytes and every historical receipt remain intact.

The unchanged production/dependency paths preserve the reviewed applicability
of scoped Storage, local Agent/App, crash and unaffected deterministic receipts.
They do not make the former failed Agent run pass. The prior Question checks
and version-1 grades are historical; the new assertion controls apply only to
the revised checker. Provider's historical **118 fresh nonbatch plus 19 reused
batch** passes remain tied to their original graph. The integrated Provider,
Tool/App live and distribution gates remain pending; no passed workload or
batch was repeated. The five withdrawn cases, Realtime support, Anthropic capture
limitation and Gemini output exception remain: **undeleted, expiration
unverified**. No complete workspace qualification, commit, push or merge is
claimed. No capacity workload or unrelated live campaign ran.

## Recovery-only sampling experiment: 2026-10-24

The user authorized one invocation-only experiment after the preceding recovery
capture returned Question-shaped text without a tool call. The sampling
hypothesis was explicit: the selected model declares temperature `0.15`, while
Ollama 0.40.10's OpenAI-compatible adapter supplies `1.0` when temperature is
omitted. That source fact did not establish the historical failure's cause.
No ai-kit production defect was demonstrated or repaired in this pass.

Only the existing ignored `KnowledgeRequest` / `KnowledgeWireRun` diagnostic
and invocation owner, and its existing self-check script, changed. The new
recovery-only selection cannot invoke the already-passed repaired-abort case.
The invocation merges exactly three configuration paths: the owned loopback
`provider.ollama.options.baseURL`, the selected model's boolean `temperature`
capability, and `agent.build.temperature = 0.15`. Reversing the overlay restores
the original configuration, including originally absent fields. Authoritative
environment files remain byte-identical. No plugin was added or opted in; the
declared external-plugin list remains empty and native default/auth plugin
behavior is unchanged. No forced tool selection, prompt change, model alias,
retry or output repair was added.

The observer now records bounded canonical whole-request and ordered complete
message/tool hashes, with description lengths/hashes. It checks numeric `0.15`
on every reached foreground request and separates native title traffic.
Independent review identified and resolved two additional evidence gaps:
retained ordered tool/schema and initial-message comparisons are now required,
and incomplete title traffic cannot be silently excluded from completeness.
Meaningful red evidence, the intervening failed self-check receipts, and final
**41-group PASS / exit 0** are retained. Both independent final actual-diff
reviews passed. Raw prompts, credentials, tool arguments and answer text are
not included in the new projections.

The pre-live comparison bound **1,614 source/diagnostic paths**, **49 inputs**,
**1,680 historical files**, compiler, lock and executable. Only the two ignored
diagnostic scripts changed. No compiled input changed, so the existing Rust and
artifact receipts were reused explicitly; no build or passed Rust/live workload
was repeated. The exact runner hash is
`7878559c8129efc1cc255cb5286862c48ddd7bd48bd87389b0d8994536fdc94e`;
the self-check script hashes to
`3ab7746f55a8dc0149ff32c11355c5f5069246f8a73ad580d78f1baf2b16c6b0`.

Invocation **`scoped-live-20261024-212819`** used candidate
`c30865360e984069a026433c4990968e8c50d2aa47745ce3933dc5e3a7253ea7`,
the unchanged lock
`9d679aee125445ed05d4f717564e7e7ab6588a882a766fcbde64f10893f529a1`,
and the previously built Agent executable
`d97f273ce7f17c0302cc8a6fa0b044d516b0d6ce007e66247b5c327587844021`.
It verified **29 cases**, then ran only
`opencode_reopen_cancels_recovered_pending_interaction_before_publication`:
**1 passed, 0 failed, 0 ignored, 28 filtered**, with one separately accounted
nested crash-child start. The 600-second initial prompt/admission, 900-second
child, 1,290-second scenario, 180-second follow-up, bounded native-history
readback, 90-second cleanup phases and 7,200-second supervisor were unchanged.

Read-only preflight rebound OpenCode **1.19.3**, Ollama **0.40.10**, and the same
`ministral-3:8b-instruct-2512-q4_K_M` digest recorded above. The native OpenCode
binary, configuration sources and override presence/absence were bound.
Ollama's server version/model are HTTP observations; its serving binary remains
not independently bound. The model-settings readback confirms declared `0.15`,
not the internal sampler's state.

The completed capture independently records:

- Two foreground requests, both with numeric temperature **0.15**, the unchanged
  selected model, `tool_choice: "auto"`, 11 retained tool declarations/schemas,
  and the 4,096-token allowance. No title or unclassified traffic occurred.
- A genuine `question` call with valid captured arguments, no text substitute,
  native `tool_calls` finish and `[DONE]`. The Kit's required Question admission
  succeeded, allowing the abrupt child exit and reopen reconciliation to run.
- The existing follow-up in the same native session returned **54 bytes**, with
  `stop`, no extra call and `[DONE]`. Its SHA-256 is
  `407c82831e816e9f4ec859d88df89626d2b7d8bfa4f7f805a50734e6d20cf556`.
  The wire text equals the requested token, and the independent bounded native
  history snapshot confirms native = portable Kit = expected, without trimming
  or unescaping. All original blocking assertions passed.

Both HTTP/SSE exchanges were forwarded unchanged with complete framing and
projection. Model-stream termination is recorded separately from OpenCode turn
cancellation; no disconnect or cancellation was substituted for `[DONE]`.
Successful socket forwarding alone is not proof of Kit delivery; the Kit result
and native-history comparison supply their separate evidence. New complete
request hashes cannot reconstruct the 12-byte difference in the historical
requests or establish their full semantic equality.

The invocation settled **PASS / exit 0**. The parent explicitly reported
successful scenario cleanup. The observer joined its worker and handlers,
exited cleanly, and was reaped. All command owners settled without timeout or
cleanup error; a separate read-only process snapshot found no survivors in
**11 owned groups**. Graceful Kit shutdown of the deliberately abrupt crash
child remains **unestablished** and is not implied by parent cleanup or child
reaping. No new remote-resource deletion claim is made.

Before this report append, every pre-live source/input/history binding and the
artifact were unchanged after settlement. Evidence and independent reviews are
retained under `recovery-sampling-015/` in the integration evidence root,
including `observer-selfchecks-final-v1.json`, `prelive-continuity-v1.json` and
`settled-source-artifact-process-check-v1.json`. Historical report bytes and
receipts are preserved verbatim.

This pass qualifies this **changed-sampling recovery journey only**. It does
not prove that omission of temperature caused the prior printed JSON or empty
responses, establish statistical reliability, or make this sampling policy a
permanent product/default change. The repaired-abort targeted pass and other
applicable receipts remain separate. Question cases, Provider batches and all
other passed workloads were not repeated. Broader qualification remains
pending; historical Provider evidence remains **118 fresh nonbatch plus 19
reused batch** passes under its original graph, not current integrated Provider
qualification. The five withdrawn cases, retained Realtime support, Anthropic
capture limitation and Gemini retained-output exception remain unchanged:
**undeleted, expiration unverified**. No commit, push, merge, capacity workload
or complete workspace qualification is claimed.

## 2026-10-25 — Interrupted Provider continuation and exact OpenAI reconciliation

The approved continuation `scoped-live-20261024-235540` ran against candidate
`678d447f3f596b424f763e0f27ccb2cbc4e4cf5f16be9aabb53e0c8cd5ced8d3`, with the
unchanged lock and previously bound Agent/Provider/Tool/App artifacts. It did
not repeat the passed Storage, local Agent, App prepare or canonical crash gates.
The one fresh Agent Question-delivery case passed **1/0/0, 28 filtered**. Its
Kit grade is true; answer-copy is false; both option-description grades are
true. Agent coverage is **one fresh plus 28 reviewed retained passes**, not a
fresh 29-case pass. The earlier 26/3 result, repaired-abort pass, changed-sampling
0.15 recovery pass and original v1 native-idle evidence retain their original
identities and limitations. Graceful Kit shutdown of the abruptly exited
recovery child remains unestablished.

Provider recorded **15 explicit passes, one failure and one interrupted case**;
there is no final 137-case summary. The mixed-row case failed before generation
or batch submission: readiness FileInfo and cleanup DeleteFile both returned
HTTP 503 with an upstream connection-refusal diagnostic. The Kit retained the
HTTP rejection as primary and malformed error-body JSON as supplementary. This
establishes an HTTP-path failure, not the origin of the refusal or a Kit defect.
These file operations intentionally remain fail-fast; the separate bounded
batch-status retry policy is unchanged. This case's bounded Provider shutdown
succeeded, while one acknowledged 89-byte upload remained unresolved.

The following result-limit case had acknowledged one batch, with its hidden
input and later artifacts not yet discovered. The user explicitly authorized
stopping the long wait and reconciling its exact resources. The existing outer
supervisor received one SIGTERM; its final disposition is **INCOMPLETE / 143**,
and the Provider command exited **-15**, without a timeout. Its interrupted
Kit shutdown and test-owned remote cleanup are not established. The supervisor
settled all command/service children and groups; independent read-only OS
inspection found none of the 12 recorded owner/group identities remaining.
Cleanup error lists are empty. This process settlement does not establish
native batch terminality or remote deletion.

Final receipts preserve matching before/after candidate, all 80 input bindings,
22 provisioned source bindings and four executable hashes. Copies and hashes
were frozen before operational diagnostic edits, including the original report
bytes. The prior frozen report remains a byte-identical prefix of this append.
The two exact OpenAI ownership receipts, final logs/exits and environment chain
are bound separately under the original integration evidence root's
`openai-operational-reconciliation/` directory. Resource identifiers and native
responses remain private.

The ignored operational invocation and its existing offline accounting owner
were extended with explicit OpenAI admission. No Rust, compiled input, public
API, dependency, model, test assertion or test deadline changed. Meaningful
red controls covered absent decision logic, identity contradictions and
error-precedence gaps; the final **183/183 offline controls passed**,
including the 81 unchanged baseline controls. Independent actual-diff reviews
bound the final scripts, admission and controls before native execution. The
existing `ScopedLiveRun` remained the process owner. One-shot parent/child claims
prevent this attempt from being launched again. TLS, exact-resource scope,
15-second connection, 60-second request and 4 MiB response bounds remained in
force, with no automatic request retries or redirects.

The one operational attempt `scoped-live-20261025-032617` ended
**INCOMPLETE / exit 1** after **664.589 seconds**. Its outer 900-second supervision
bound did not expire. Within the approved 840-second aggregate bound, the exact
batch GET reported `in_progress` with two total, zero completed and zero failed
rows. One cancellation POST returned HTTP 200 and acknowledged `cancelling`.
Ten subsequent exact-ID GETs continued to report `cancelling`; the unchanged
660-second cancellation window expired without terminal proof. This is the last
observed state, not an assertion of the batch's current state. The exact input
file was discovered, but no batch file was admitted for deletion before a
terminal state. No output/error file was reported in those nonterminal snapshots;
that does not establish a final artifact inventory or absence.

The independent exact lookup of the previously acknowledged 89-byte upload then
returned HTTP 503. Its 190-byte non-JSON body matched both original Kit FileInfo
and DeleteFile error bodies byte-for-byte. A native HTTP request outside the Kit
therefore reproduced the same service-path rejection without generation or model
execution. The refusing upstream component and originating cause remain unknown;
no Kit defect or production correction is established. HTTP 503 remained primary
and JSON decoding supplementary. No DELETE followed this lookup, and the file's
current existence and absence remain unverified.

There were **13 native requests: 11 exact batch GETs, one cancellation POST and
one exact file GET; zero DELETEs**. Every response hash, length and exact resource
relationship was independently checked. The cancellation timeout and file GET
failure remain separate retained errors; neither is converted to operational
success. Batch terminality, its input-file cleanup and any eventual output/error
artifacts remain unresolved. The 89-byte upload also remains unresolved. No
additional reconciliation attempt or qualification escalation was performed.

All 13 curl children were reaped. The operational command child and its process
group settled with no recorded cleanup error; independent read-only OS inspection
found no surviving process in its recorded group. This was operational native
cleanup, not Kit execution. It cannot establish the interrupted result-limit
case's Kit shutdown. The mixed-row case's previously established successful
Provider shutdown retains its separate scope. Original local settlement, current
operational settlement and remote-resource reconciliation are distinct outcomes.

The operational supervisor recorded the same candidate before and after the
attempt. A fresh post-settlement rehash confirmed all four original executable
payloads unchanged. All frozen historical snapshots and original receipts remain
byte-identical. Among their bound original files, only the active status metadata
and this report append changed.
The private evidence directory retains the red/green controls, versioned actual
diffs and reviews, final admission, one-attempt claims, native response hashes,
operational receipt, supervision receipt and independent outcome/settlement
reviews. The monitor was retired to prevent further automatic requests.

These results do not replace the historical failures or constitute complete
workspace qualification. Tool, App workflow-live and package escalation remain
blocked. Provider's earlier **118 fresh nonbatch plus 19 reused historical batch**
result remains evidence for its original candidate, not a current integrated
pass. The five withdrawn cases, retained Realtime support, raw-copy grades and
Anthropic capture limitation are unchanged. The separately permitted Gemini
generated output remains **undeleted, expiration unverified**; it is outside this
OpenAI reconciliation scope. This reconciliation repeated no passed workload or failed batch and performed
no generation, commit, push, merge or capacity work.

## 2026-10-25 — Authorized exact OpenAI recheck; file cleanup still incomplete

The user authorized a fresh exact-resource recheck and qualification resumption
using applicable receipts, without repeating passed workloads. Before requests,
source continuity confirmed that the sole change from the previously settled
candidate was the preceding report append: current candidate
`97968a75a58758147146a192db5c8326e793f515b31c21df86118edf7af4f653`, unchanged
lock and four original executable payloads. The prior report, source bindings, prior versions of the operational script and
checker, admissions, claims and receipts were preserved.

The existing ignored cleanup invocation gained an explicit recheck selection,
using the same `ScopedLiveRun` and native-request owner. Its versioned admission
binds the preceding settled operation and all 13 native response bodies. The
previous cancellation is carried into the mutation guard. A recheck performs
one batch GET, cannot repeat cancellation or restart the exhausted cancellation
wait, and admits batch-linked files only from fresh matching terminal metadata.
The first 11 new decision/transport controls recorded meaningful red evidence;
final validation passed **253/253 offline controls**, retaining the original
183 baseline controls unchanged. Additional admission and evidence-rejection
controls are passing diagnostic proof, not invented Kit-defect evidence.
Independent actual-diff and admission review found no blocking issue. A stale
checker binding was rejected before any claim or request while the checker was
being finalized; its rejected payload remains preserved separately.

The one recheck `scoped-live-20261025-035750` settled **INCOMPLETE / exit 1**
after **5.851 seconds**. All request/connect/body limits and the 840-second
aggregate and 900-second supervisor bounds remained unchanged; neither was exceeded.
No cancellation wait was restarted. The batch GET returned HTTP 200 and
**`cancelled`**, with the original input reference, an exact error-file reference
and no output-file reference. Native counts were two total, zero completed and
zero failed. This establishes operational terminality, not successful batch
processing or the interrupted Kit result-limit assertion.

| Exact owned resource | Current operation | Established result and limit |
| --- | --- | --- |
| Independent 89-byte upload | GET returned HTTP 503 | No DELETE followed; current existence and absence remain unverified. |
| Batch input file | GET 200; DELETE 200; final GET 503 | Exact matching `object=file`, `deleted=true` acknowledgement; independent absence remains unverified. |
| Batch error file | GET 200; DELETE 503; final GET 200 | No deletion was acknowledged; matching metadata establishes that the file was still present at the final observation. |

All three 503 response bodies match the earlier 190-byte upstream connection
refusal diagnostic byte-for-byte. These native requests again establish the
service-path rejection outside Kit/model execution, not the refused component
or underlying infrastructure cause. HTTP errors remain primary; malformed JSON
remains supplementary. The acknowledged input deletion is not relabeled as an
absence observation. The failed error-file deletion was not repeated.

There were **eight native requests: six GETs and two DELETEs**, with no POST,
new batch, generation, listing or content download. Exact identifiers, response
hashes and relationships were independently reviewed. All eight curl children
were reaped; the command child and owned process group settled without recorded
cleanup errors. An independent read-only process check found no surviving owned
process. The same current candidate was recorded before and after execution.
This operation did not execute the Kit. The original mixed-row case's successful
Provider shutdown and interrupted result-limit case's unestablished Kit shutdown
remain separate historical facts; the abrupt recovery child's graceful Kit
shutdown also remains unestablished.

Qualification did **not** resume because remote cleanup remains incomplete.
The read-only continuation audit identifies **122 remaining Provider cases plus
15 potentially reusable passes**, subject to per-case cleanup/source admission;
this is not a fresh 137-case result. Agent remains one fresh plus 28 retained
passes. Tool's one case, App's 18 workflow cases and distribution gates remain
pending. The existing continuation owner would repeat passed Agent/Provider
work, so it was not executed or modified. Its narrow versioned update remains
pending cleanup resolution; no passed Storage, local Agent, App prepare or crash
workload was repeated.

Evidence is retained under `openai-operational-recheck/` in the original
integration evidence root, including versioned controls, rejected/final admission,
one-shot claims, source/artifact continuity, both independent outcome reviews and
the qualification-resumption audit. Earlier failed and incomplete receipts remain
unchanged. The five withdrawn cases, Realtime/raw-copy scope, Anthropic capture
limitation and Gemini generated-output exception remain unchanged: **undeleted,
expiration unverified**. No production correction, retry, commit, push, merge or
capacity workload followed this recheck. No complete workspace qualification is
claimed.
