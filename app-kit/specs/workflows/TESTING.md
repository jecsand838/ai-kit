# Test-first proof and review contract

The completion goal requires every A01–A31/C01–C12 acceptance row, each adopted
lower-crate prerequisite, complete docs/examples and release proof. An excluded
server/CLI/exporter integration is explicitly labeled; a missing core case
never becomes an exclusion during implementation. The acceptance matrix and
EXECUTION_LOG hold evidence/status, not competing public signatures.

## Red, implementation, review, green

For every bounded area:

1. Inspect existing owners/tests for extension and deletion opportunities.
2. Add/change the smallest public compile or behavioral cases **before**
   production. Record the actual failing case, command and diagnostic. A new
   missing-proof test that already passes is labeled as such, not invented red.
3. Implement through existing structs/enums/methods, remove superseded paths,
   and update owning behavior/docs. Do not weaken assertions to match current
   output or add always-failing placeholder tests.
4. Run focused tests then the affected crate/feature gates centrally, one Cargo
   command at a time.
5. Obtain independent subagent review of the actual diff and test evidence;
   resolve actionable findings and rerun affected checks. Passing tests without
   review, or review without passing tests, does not close an area.

All executable tests live under tests/; src has no cfg(test), environment reads,
fixtures or production test branches. Deterministic foreign implementations are
valid public-boundary probes, explicitly not built-in or strict-live evidence.
No new shared conformance/test crate, public fixture API or test dependency is
needed. New targets are added to the owning check script before area completion.

## Downstream and structure proof

compile_contract exercises actual external compilation for direct CS/A with
associated children, no Clone/default-handle requirement, concrete Action/
Condition/StateTyping/Hooks/ChildExecutor, local Rc-bearing streams/Data and
public construction of portable DTOs. Cargo compiles positive integration contracts against its exact selected
dependency graph; no rustc artifact-directory/mtime guessing or nested Cargo
harness is permitted. Negative contracts are rustdoc compile_fail examples on
the owning public type/module, adjacent to a positive example establishing
imports and intended context. Removed AppState and private child completion/AST
construction are negative probes. Never treat an unresolved import or a
filename containing private as proof of field privacy. ChildExecutor's GAT must support genuine custom polling order.

The public source audit discovers exported types and inherent impls throughout
src, including private modules that implement exported handles. It enforces
only the exact ledger, namespace/ownership/privacy and prohibited constructs;
it never freezes AppStoreInner names, filenames or routing choreography.
No aliases, unsafe, generated source, macros, dyn backend ingress, lower-private
imports or env reads are permitted. The approved serialization-only callable
erasure and static App seams are precisely scoped exceptions.

## Required behavioral targets

The following targets organize distinct domains; names do not replace coverage.
Existing workflow_execution/workspace_contract cases remain where they fit.

| Domain | Required proof |
|---|---|
| mock_contracts | Direct struct and exhaustive enum implementations of all five App workflow traits, exact associated futures, local/borrowed/non-Unpin values, every enum arm, and fixture integrity |
| workflow_mock_composition | Real App execution over caller Agent hierarchies and process-local StateStore transactions: preflight, revisions/compensation, workflows, ownership, uncertain effects, checkpoints, reconstruction and shutdown |
| compile_contract/public_surface | Exact direct traits/defaults, associated types, local/Send ownership, deleted ingress, privacy and approved narrow exceptions |
| workspace_contract | Exact forward/compensation revision, foreign r3 preservation, receipt loss/reopen, full preflight, reverse cleanup, deletion/shutdown |
| workflow_execution/worker_contract | Whole-activation deadline during preparation; separate synchronized lower-method entry with a finite activation budget, panic/JoinError supervision, driver release, primary cause and Unknown |
| expression_parity/condition_contract/state_contract/execution_controls | Every pinned numeric/condition/state/input/halt behavior with archive/member/expected-value provenance; no expectations copied from App output |
| state_contract | Insertion order, operation/reducer truth tables, full pre/post/state/roundtrip sequence, local Data, global field/tag codecs and cross-App replacement |
| graph_contract | Strict schema/parser agreement, unknown/duplicate fields, routes/tags/cycles/read/write/input validation, compose/export escaping |
| action_contract | Sync/async/local terminal and partial actions, lazy polling, declarations/defaults/binding, result/state separation, errors/panics |
| execution_controls/action_contract/native_streaming_contract/bsp_execution_contract | One exclusive driver, entry halt semantics, serial/BSP boundaries, partial-before-terminal, drop/EOF/error/commit-admission/lost receipt, explicit finish/abort |
| checkpoint_history | Immutable row/head/index/receipt atomicity, more-than-page cutoff traversal, old/mixed epoch read-only rejection, >1s results, restore/fork/copy/delete races |
| child_executor_contract/child_composition | Full batch custom reversed polling, local futures, missing/duplicate/foreign completion by occurrence, stable task_key with fresh changed-input effects across parent actions, equal-key concurrent occurrences, same-occurrence recovery without replay, dynamic inventory/pipelines/initializer policies |
| lifecycle_contract/observability_contract | Guaranteed safe spans/redaction, exact sync/async hooks and drop lanes, failure precedence, graph/step/child reader outcomes |
| hook_execution_contract | Real caller-local async hook delivery: exact lane order, committed versus partial/failed state, pre/item/post callback failure, no-output rollback, and synchronous-only Drop after retained callback/publication settlement |
| observability_contract | Captured host subscriber, applicable operational spans, native commit-before-event witnesses, safe fields, poll scope, worker dispatcher propagation and subscriber-failure isolation |
| tracking_checkpoint_contract | Real public history/restore values in StepFinished and registration exports; attempted versus committed metadata, one codec snapshot, callback/output failures and distinct fork/spawn pointers |
| docs_examples | Every JSON example parses, deterministic ones execute, public examples compile/run, all guides/schema/navigation links complete |
| performance/recovery workloads | Measured linear/wide/nested/history workloads, active capacity reuse, bounded task/thread/memory growth and fresh-process crash recovery |

The two mock targets belong to the primary deterministic CI lane described in
[the workspace mock testing contract](../../../docs/mock-testing.md). They use
test-local implementations directly, without supplied lower backends, extension
ingress, backend processes, sockets, or credentials. Reopening retained
process-local state proves same-process reconstruction only. Their results do
not replace real-Lance, process-recovery, strict-live, or capacity obligations.
The compile-only `compile_contract` target remains a separate positive proof;
new negative cases still belong on owning public rustdoc, and missing coverage
under a source freeze remains an explicit limitation.

The 25 ms whole-activation case permits expiration during real storage preparation
and requires public Timeout, no lower dispatch, and joined cleanup. Lower-entry
and panic cases use a separate 200 ms finite activation budget and retain their
mandatory entry witnesses, original outcome observers, no-replay assertions, and
cleanup. Their budget is sampled at activation; entering a lower method never
restarts it. Reaching that method within 25 ms is not a separate public guarantee.

Child identity proof must observe the same public identity with two actual fresh
effects and semantic ordinal zero for each uninitialized occurrence, while exact
earlier App checkpoints remain readable. Equal keys in one batch must preserve
both input indices and results; the async case observes both starts before either
is released. A separate lost-receipt/reopen arm proves that one already admitted
occurrence does not rerun. Pipeline tests distinguish handed-off defaults from
loaded initializer state, loaded-state overrides and missing-state fallback.
Deletion tests hold the original receipt, require copied parent results before
deletion, then prove a fresh same-key invocation and honest deleted-source
initialization behavior. The pin's source-derived duplicate-key behavior must
not be labeled an executed differential result.

The first `checkpoint_history` target exercises public selection of successful
and failed observations, explicit same-ordinal retry, serialized cursor cutoff
and scope, traversal beyond one native storage page, retained results beyond the
former expiry and fresh App generation, parked restore, fallback, source deletion
after copy and distinct selector/committed lineage ordinals. BSP failure and
restored-wave attribution are explicit App extensions. Its real LanceDB fixture
does not prove individual write/commit/lost-receipt or process-crash boundaries;
those remain required follow-up cases in the table. Missing-history API compiler
failure is an integration obligation, not runtime evidence for these assertions.

Every source-derived parity fixture records archive SHA, member/anchor, inputs,
expected value/type/error/order and action/commit trace. Comparing those fixtures
is deterministic parity evidence. Optional authorized installed-oracle execution
is separately labeled; neither implies real lower services were exercised.

The checkpoint tracking target obtains Checkpoint and RunLineage through real
StateStore-backed runs and public history/restore operations, never synthetic
non-exhaustive constructors or a production fixture ingress. It drains those
Apps before installing reserved-key codecs. Fixed global codec replacements
run in isolated, bounded child processes whose parent always reaps them; no
reset/unregister API is added. Metadata-handler inputs, unchanged authoritative
checkpoints, codec failures, inclusive encoded-state bytes and the separate
complete-record bound are independent assertions. Actual restore lineage and
an optional context from a different committed run exercise pointer ordering;
this pure projection case does not claim runtime spawning-parent delivery.
The public history/restore access used by this target is implemented. A missing-API
compile failure remains an integration failure, not passing tracking or
lifecycle-hook evidence; preserve the actual StateStore-backed runtime assertions.

The hook execution target uses the public asynchronous Execution owner,
a local Rc-bearing source, a non-Clone hook and real LanceDB. It verifies borrowed
data on actual callbacks, including post-step before post-stream ordering,
synchronous callback failure preventing that event's asynchronous callback,
pre-dispatch rejection, committed state retained after post-hook failure and
no source replay. EOF/abort/Drop carry no post-step outcome: tracking projection
must fail before codec invocation, while the callback still sees App's committed
base. Drop uses only synchronous callbacks and App cleanup cannot later repeat
or upgrade those closed lanes. The source oracle is fixed
`lifecycle/guard.rs:20–71,104–193,206–271`,
`application/step_txn.rs:258–276`, and
`application/streaming/step_core.rs:310–375,401–421,656–678`.
The target also requires structured Hook cause at completion, native State
pre-hooks before primitive typing, complete foreground/background native event
delivery, registration rejection with retained discoverable failure, and lost
admission observation while registration is held. Native Map assertions use
per-child identity and happens-before edges without ordering siblings. These
runtime callback assertions do not replace direct trait/borrow tests, tracking
wire/schema tests, synchronous execution, action span tests or captured
operational-subscriber proof.
An applied-admission receipt gate holds the original transaction's response
after real native commit. The visible checkpoint-zero run must reject a
competing Execution before registration begins, then advance only after the
receipt and registration settle. This isolates the previsibility ownership
reservation from missing-callback failures.
Native Input cases separately prove the App-native control lifetime: successful
finish followed by resolution/resume preserves one activation guard across two
execution sessions; owning Drop closes it synchronously with no outcome, and
later settlement cannot reopen it. Both use real checkpoint-fenced resolution,
exact committed result/state and bounded retries only for rejected Busy
acquisition, without permitting extra lifecycle calls from those rejections.
Held registration and post-completion callbacks must independently prove the
private 30-second lifecycle ceiling, including the single shared cleanup
budget across one step, stream and execution finalization/unwind chain, without
carrying an earlier completed chain's deadline into later actions or caller idle
after a returned Committed event. Composed Drop cases hold an admitted successful
item callback across a source destructor panic, and hold an async StepFinished
across owner Drop. They require retained failure or committed outcome respectively,
with no later asynchronous guard lane. Synchronous callbacks remain cooperative;
these tests do not claim forced interruption of synchronous code.
Successful EOF cleanup also holds its admitted StepFinished callback while the
caller cancels only `next`. Resumption must return the retained None before a
separate advancement retries the saved position; finish after observed EOF
must not replay the source. Each source closes its Step/Stream guards once,
while the same exclusive Execution retains one Execute pair and unchanged
state/committed position. Release that callback before joined failure cleanup.
An earlier successful native sibling must execute before a caller source
reaches EOF, as witnessed by its typing callback and both original Step
contexts. Both uncommitted Step guards then close with None before
the complete wave is discarded; explicit same-owner retry repeats both real
starts while preserving the last committed base, outcomes and ordinal.
Background EOF must become durable Paused without automatic source replay;
fresh explicit execution may retry the unchanged position. After a genuine EOF
None and settled Step/Stream guards, a separate real 31-second caller idle
interval must admit no callbacks, and finish must complete within a fresh
cleanup budget with only the original source and no action outcome. A failure
before that idle interval is not timing-branch evidence.
Caller-span runtime cases require exact authored ids, repeated names, ordered
opaque dependencies, explicit attribute targets and sync-then-async delivery.
Separate malformed-finish cases require unknown/closed ids, open-child parent
finish and closed-id reuse to fail before any action commit without emitting
fabricated end events. Remaining spans close child before parent on successful
completion, source error, EOF, explicit abort and owning Drop. Span end-hook
errors must reject both explicit and automatic pre-stage completion, while
enclosing cleanup still runs. Held attribute cases distinguish cancelling one
`next` wait from dropping its Execution: the former retains exactly one receipt
and resumes, while the latter retains any failure and defers sync-only closure
until that same admitted callback settles. These are runtime proof obligations;
the pure lifecycle event/serialization tests do not prove source forwarding.
The BSP hook-primary case must first observe a real earlier native State failure
in the durable staged wave while the later caller's item or span callback is
held. Both ready activations and their equal proposed ordinals remain visible.
After the later Hook error, retained failure/history still names the canonical
native activation and original State message; execution completion carries the
later Hook with that State failure as its cause. A missing later callback or
unreached staging witness is an execution-progress failure, not evidence that
primary attribution was tested.
Background declaration rejection covers bound-input override and unsupported
execution mode after real run admission. Wait and snapshot must retain an
Execution failure with the exact stable declaration message, unchanged state
and no action position/outcomes, source construction or step/stream hook
dispatch. Foreground rejection remains InvalidInput before dispatch. Missing required transient inputs
remain a separate input-rebinding obligation; these failure cases do not stand
in for Interaction::Inputs. JSON allocation lifetime has no public Drop witness;
use stale-input behavior on resume/reopen and source review rather than private
runtime-field assertions.
The resource matrix checks below, at and above the existing count/depth/text
and byte bounds, including closed ids in retained count/bytes. Exact callback
counts prove where the first over-limit event is rejected. The 4,096-id cases
have an outer budget proportional to their real metadata receipts; a timeout
cannot satisfy the required Execution failure or unchanged-state assertions.
The structured-scope anchors are `visibility/span.rs:70–85,131–140`,
`visibility/factory/session.rs:84–137`,
`visibility/factory/handle.rs:119–197`, and
`visibility/trace_api.rs:165–183,237–255`; reverse-open automatic closure and
synchronous owning Drop are explicit App-native adaptations.
Foreground condition_contract cases separately establish containment of a
native typing panic and a post-commit caller-condition panic on the local owner;
background Driver panic supervision is not evidence for that boundary.

The native streaming target uses the same direct Agent hierarchy as native
result tests. It requires exact lower deltas, terminal-only state/result
authority, cancelled `next` observation without lost completion, explicit
nonstreaming dispatch, and uncertainty after an Unsupported stream-opening
error. The opening case proves no unary fallback or automatic replay, explicit
caller failure settlement, and redaction without inferring acceptance from the
error category or absent stream handle.
Accepted stream error or premature EOF retains Unknown until explicit caller
settlement, including Unsupported, InvalidInput, Closed, and Cancelled items
received after acceptance while App has not requested cancellation. Their error
variants do not prove a terminal lower result. A stalled consumer must not eagerly drain the complete lower
source; explicit public cancellation still reaches lower abort, drops the
actual observation source once, and joins cleanup without committing targets.
The delayed-return case holds a real constructed lower stream before returning
it to App, then drops its owning Execution. Existing guards must remain owned
until that publication settles; no new asynchronous stream callbacks may begin.
Terminal observation or abort reconciliation is permitted, with one accepted
prompt/source Drop, bounded nonduplicate abort and unchanged committed state.
These deterministic fixtures prove App composition and ownership, not a native
provider or process stream. Lower notification interleaving remains a separate
proof case.

Mandatory C01–C12 include all four original correctness bugs; independent ready
sibling controls; retry policies/counters; complete reducers; structured output
success/failure; retention/capacity; honest lost-effect evidence; complete public
compile/structure auditing; and exact limits/tracing/performance proof. Limit
cases use limit-1/limit/limit+1 with units, byte/depth/overflow and cancelled
capacity waits; success at a constant does not prove a performance envelope.

## Local, synchronous and worker proof

SyncExecution creates no worker or command channel. Prove caller-thread source
construction/polling/Drop, non-Send values across a pending await, rejection from
an entered Tokio runtime context, a separately driven current-thread lower
runtime, synchronous-only hooks, mode propagation, halt/state behavior and
receipt-preserving cancellation/panic cleanup. Facade-specific thread creation,
queue admission, thread exit and cancelled asynchronous close-waiter scenarios
are superseded as inapplicable, not passing evidence. Those obligations still
apply to the actual background/child worker below; App never joins a caller thread.

Preserve these shared execution-worker regression and review requirements:
non-Send streams and Data are constructed/polled/dropped only on the owning
thread; custom child futures truly run in selected order; no recursive worker
admission creates a bounded-pool deadlock; current-thread caller runtimes remain
driven; foreign runtime-bound StateStore and real lower Agents operate correctly;
worker count/queues are bounded; cancellation is registered before spawn; every
panic/normal/drop/cancelled-close path retains and joins the OS handle. No
block_on/runtime construction occurs in Drop, no self-join or detached finalizer,
and stalled child work still services control/cancellation.

The full 1,024 lazy-execution-owner case uses a dedicated Input definition with
the existing finite default deadline. Setup/observation guards remain separate
from authored deadlines. Check every retained Input immediately before and after
independent progress, before releasing any execution owner; a preparation count does not prove
simultaneously live ownership. Keep all 4,097 run
heads and 1,450 actions in their separate capacity scenarios.
Routine validation and maximum-capacity qualification have distinct execution
scopes within the existing `workflow_execution` and `worker_contract` Cargo
targets. No test is ignored or compiled out. The routine
`notification_delivery_routine_preserves_receipts_replay_and_cleanup` case uses
64 actual deliveries, receipt replay, public cancellation, and joined cleanup.
`delivery_capacity_prefix_measurement` and `delivery_capacity_growth_measurement`
are separately selected 256- and 1,024-delivery diagnostics. The
maximum-capacity cases retain all 1,024 owners,
4,097 heads or 1,450 actions, their authored deadlines and
assertions. Their explicit `capacity` gate retains every original per-case outer
budget. The App check explicitly excludes these cases, both diagnostics and
the four instrumentation-calibration tests named in `docs/STRICT_TESTING.md`.
Diagnostic scope runs those four exact calibrators as mandatory preflight, outside
timed workload samples, with the same verified artifact and profile. Missing,
ignored, filtered or failed preflight cases prevent measurement. Routine scope
verifies nonempty inventories and exact execution for the remaining cases.
See `docs/STRICT_TESTING.md` for the exact names, budgets and commands.
Routine checks retain the test profile. Dedicated capacity execution uses the
workspace optimized `capacity` profile with debug assertions and overflow checks;
diagnostics explicitly select either profile. Compile before measurement and verify
effective artifact settings. Profile, platform and filesystem remain part of every claim.
The optional native version-hint experiment uses fresh processes and permits
exact `LANCE_USE_VERSION_HINT=0` only in the 256-delivery prefix diagnostic.
Record that policy separately; retain runtime-identity checks. Routine, growth,
capacity and candidate qualification require the setting unset. This diagnostic
changes neither the production storage contract nor the acceptance criteria.

Maximum-capacity qualification is a separate task outside the current Storage
feature-split completion scope. A routine or diagnostic pass never qualifies the
maximum boundary or erases earlier full-count failures. Historical capacity
receipts retain their recorded source and execution scope; the 4,096-delivery
unoptimized timeout remains failed historical capacity evidence permanently.
A later complete optimized pass qualifies its recorded profile and environment;
it does not establish an unoptimized pass or universal throughput.

If a cleanup observer expires, keep the originating runtime driven through fresh
Drain observers until the accepted finalizer settles. Preserve the first timeout
as failed evidence even if later observation joins. Retry only an exact shutdown
observation timeout, not terminal cleanup errors. A supervised outer process
budget bounds the complete workload; expiry is incomplete execution evidence.

Foreground asynchronous Child tests hold a genuine Rc-bearing caller executor
after exact atomic child admission and before any child effect. Cancelling only
`next` must retain one construction and zero Drops until the gate opens; resuming
the same Execution must return the parent Committed event and exact reduced
state. Owning Execution Drop keeps competing acquisition Busy while that accepted
batch is held, then permits a paused parent to resume from the same completed
children without repeating effects. Assert the same worker thread at executor
construction, every poll and Drop, exact once counters and child membership,
and unconditional gate release plus joined App cleanup on every failure path.
These tests complement the transitive Agent revision completion proof; they do
not by themselves close deeper nesting, all child policies or worker capacity.

## Real integration and process evidence

At least one real workflow executes Agent A → committed structured state →
Agent B, plus a parallel A/B ready wave. Tests assert each actual backend entry,
state handoff, isolated agent conversations/tools/knowledge, shutdown ownership,
durable reopen and forced process-loss recovery. A config containing two Agents
and zero workflows proves none of this. Real sequential approvals exercise two
notification targets during one prompt.

Strict-live uses real configured providers, native Codex/OpenCode processes,
Ollama and each owning storage/tool lane with unique test-owned durable domains.
The exact verifier env/service/version/model contracts are authoritative.
Missing prerequisites fail; no skip, fake server, recording, scripted substitute,
terminal-text chunking or synthetic success satisfies live evidence. Never print
or stage ignored strict-tests.env or delete unrelated service data.

The recovered-notification authority cases reuse the real killed-process Pending
notification journey with controlled App-store read faults. Their concrete test
store forwards every normal operation and native transaction to the original
StateStore; only one selected read is held or returns a typed storage failure.
These are real integration proofs with controlled fault injection, distinct from
undisturbed strict-live evidence. Original lower identities, payloads, timestamps
and native rows are never fabricated, and recovery must not replay a prompt.
OpenCode Agent reopen cancels the killed process's lost lower continuation.
The recoverable-read case therefore proves exact retained App Pending projection,
Busy without a lower decision, and explicit App cancellation through the original
lower owner; it does not claim the cancelled notification can accept Submit.

App tests construct separate real Agent/App StateStores outside the library and
pass CS directly. Fresh-process directory-backed LanceDB crash tests synchronize
at public effect/commit boundaries and terminate only test-owned processes.
Close/open within one process is useful but not process-crash proof. Lower-crate
trait changes run every affected consumer integration; foreign custom proofs and
supplied native service proofs complement each other.

## Final ladder and accounting

Run exact current crate gates centrally/sequentially, including formatting,
all changed deterministic suites, correct storage feature lanes, clippy warnings
denied, rustdoc/doctests/examples, docs-site/source/alias/path/portability checks,
all five strict-live verifiers, then the workspace strict wrapper. Finish with a
locked clean-checkout proof of the exact final source state and measured
performance/recovery workloads specified in the completion plan. Do not run an
invalid all-features storage combination or call a narrower command equivalent.

EXECUTION_LOG records phase, acceptance IDs, red/green command results,
reductions, review findings/dispositions and blockers. COMPLETION_REPORT records
exact final test/service/version/commit evidence and remaining proof limits.
Missing evidence, unrun command, unresolved review, stale doc/empty guide,
placeholder, secret/runtime artifact or incomplete acceptance row prevents
completion. External blockers leave affected work explicitly open while
independent work continues; goal completion requires every in-scope item proven.

## Test simplification and evidence categories

Keep one execution of each behavioral journey and apply every distinct assertion
to it. The canonical 96-delivery history case always validates immutable settlement
pages at 1/63/64/65 entries, replay, complete history, restore, deletion and joined
cleanup; its optional cold export follows joining. No frozen v6 migration
fixtures are shipped; current tests do not establish legacy migration coverage.
Canonical memory-backed deletion cases retain their 130-step workloads; directory
counterparts exercise directory-backed persistence and ownership cleanup.
Separate recovery cases prove reopen.
One successful tracing journey checks both poll-scoped spans and redacted,
commit-ordered events. Subscriber-panic callbacks remain separate cases.

Keep evidence categories explicit. Real App execution with Lance proves App
behavior; test-local failures exercise specific error boundaries. The two
`OwnedChild` cases, receipt-page lifetime probe and native conflict-controller
case validate fixture machinery used by those behavioral or live tests. The four
measurement calibrators validate diagnostics. None substitutes for real-Agent
strict-live tests, disk/process recovery or maximum-capacity qualification.

`compile_contract` has no runtime cases; all-target checking and Clippy must
continue to compile it. The strict verifier credits the current invocation's
explicit four-case `prepare_contract` inventory and successful execution from
`check.sh`, including the canonical four-boundary crash case, rather than running
that journey twice. Standalone scoped live qualification still runs the exact
canonical crash case. Historical receipts cannot supply this execution credit.

Protect the complete saved-order child-composition stack regression, including
its shared scenario, `SavedOrder` branch, runtime and default stack. Similar setup
or private helper size does not establish redundant coverage. Remove a fixture
control only after mapping all consumers and preserving their assertions.
