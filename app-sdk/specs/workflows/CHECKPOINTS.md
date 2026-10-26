# Checkpoints, recovery and retained history

Private immutable records and one mutable run head are the only durable truth.
Public Checkpoint/HistoryPage are bounded read projections, not storage handles
or a second tracking database. Exact shapes live in ../SPECIFICATIONS.md.

## Atomic history model

Checkpoint zero records identity, workspace/definition/action revision pins,
initial ordered state/frontier, optional parent lineage and active capacity
atomically with admission. Every later workflow checkpoint mutation writes a fresh immutable row,
updates the head and indexes, and writes its exact receipt in one transaction.
A head never points at a missing/uncommitted row; unreferenced uncommitted rows
are never visible history. Do not retain permanently duplicated inline current
and previous checkpoint state after immutable rows become authoritative.
Capacity release, retention and deletion maintenance preserve the head's
checkpoint sequence and create no workflow boundary or action observation.

Prepared admission records a parked checkpoint zero in the same transaction
and under the same App/workspace fences as background admission. It performs
no entry-node work. Reading, listing, waiting on or reattaching that run never
converts the parked boundary into automatic execution; recovery preserves it.
Only an explicitly acquired execution session can advance it. Ordinary fenced
cancellation may instead publish cancellation intent and then terminal cleanup,
without running the entry node or changing its initial user state. Cancellation
returns the accepted current snapshot; callers use wait to observe terminal
cleanup rather than assuming the cancellation call itself already drained it.

A row owns monotonic run sequence, semantic kind (initial/control/action/
failed observation/terminal), current superstep/base sequence, committed state/frontier, staged
siblings, join ordinals, exact definition revision, action position/ordinal,
one bounded canonical control map and a data-carrying lifecycle phase. Controls
are not copied into Active/Cancelling/Failing alternatives. Each control owns
its activation, target generation, sampled deadline and input/attempt/sleep/
child/rebinding state. Repeated notification targets receive distinct durable
settlement identities. Resolution and abort evidence remain independent.

Before each native publication, the record's bounded encoding accounts for
retained interaction evidence plus the possible Unknown replacements reserved
by its accepted lower work. The prospective row and byte charge is derived
from that same control map and settlement vector; it is not another durable
owner or counter. Unknown encoding reserves full integer widths and the
associated control/sequence metadata changes, so later unrelated publication
cannot strand a promised replacement at the record limit. The interaction
contract owns admission/consumption of this reserved capacity. Recovery rejects
records whose accepted ownership lacks the required prospective capacity.

One immutable base supplies the complete BSP ready set. Control checkpoints
may add/settle ownership or evidence while preserving state, superstep, action
ordinal and all unrelated staged/control data. Barrier commit reduces staged
siblings exactly once and atomically publishes the next state/frontier/outcomes.
Checkpoint sequence includes control transitions; rurr action ordinal starts
at zero and advances only for committed actions. A failed attempt may retain
the same attempted position/ordinal across several control rows. Only its
durably published Failed observation creates a failed action-selector entry;
intent, cleanup and terminal transitions do not manufacture repeated attempts.

Failed observations carry the attempted node and ordinal in CheckpointKind.
They preserve committed state and the last successful Checkpoint.position and
action_ordinal, expose no committed outcomes, and retain the owning failure in
status. This separates the attempted identity needed for selection/resume from
the successful identity used by state metadata and tracking serialization.
An explicit later retry may successfully commit that same ordinal; neither
failure persistence nor history reads authorize automatic effect replay.

For a failed BSP wave, no sibling consumes an ordinal or publishes state or
outcomes. The failed observation names the first failed activation in canonical
node/lineage order and retains its sampled attempted ordinal. An earlier
admitted, unresolved caller source reserves a potential ordinal before a later
sibling starts. Every source request and lifecycle event retains that original
base and ordinal, even if the earlier source subsequently fails or reaches EOF.
A native primitive already known to have failed before the next activation's
start consumes no reservation; the next native pre-hook can therefore share
its attempted ordinal. These are attempted identities, not committed outcomes.
For example, after committed ordinal zero, pending `a` and later `z` retain
attempted ordinals one and two. If `z` fails and `a` then succeeds without a
wave commit or reaches EOF, the failed selector remains `z` at two; no entry
at one is invented. If `a` also fails, canonical primary selection names `a`
at one. Validation preserves the sampled failure identity and its bounded
canonical reservation proof instead of renumbering it from subsequently
completed stages. A reducer or completed-candidate typing failure belongs to the
whole barrier: its representative is the first canonical contributing
activation with that activation's proposed ordinal. These BSP attribution rules
are App extensions, not rurr schemas or evidence of parallel execution in the
pin. They reuse the existing failure phase and action-position representation;
there is no fabricated ActionOutcome or separate failure-position DTO.

The reservation proof first validates both retained frontiers: bounded count,
known nodes, bounded structural lineages and unique node/lineage pairs. It does
not assume serialized frontier order. The failed node and attempted ordinal
must resolve to one eligible activation in canonical order, excluding blocked
joins, suppressed JoinFirst entries and already failed/deferred predecessors.
An earlier staged failure must remain the canonical primary. Retaining a
failed observation across control checkpoints preserves that exact activation's
lineage as well as its public node/ordinal; equal public tuples alone cannot
authorize a different activation. A merely in-range ordinal or a known node in
a future wave is insufficient evidence.

An action barrier publishes committed state, position/outcomes and the exact
selected completed frontier entries awaiting outgoing-route evaluation. It
removes those completed activations from the remaining frontier and clears
their staged writes. The existing lifecycle phase owns these routing sources;
there is no parallel result store or second execution owner. A later routing
control transition evaluates only those retained sources against committed
state, publishes the routed frontier, and preserves the action base sequence,
state, position and outcomes. A routing failure retains the successful action
or whole BSP wave. Reopen at this boundary resumes routing without rerunning
the completed action or re-filtering committed JoinFirst winners. Serial step
returns at the action boundary; the next execution advance evaluates outgoing
conditions. Pinned authority is `application/session.rs:746–754,873–909`,
`application/state_meta.rs:85–91`, `application/step_txn.rs:710–744` and
`graph.rs:368–376`; BSP wave publication is App's explicit extension of that
ordering.

Before projection or recovery, validate retained action metadata together with
its predecessor and exact definition. After a wave commits in this run, its
ordinary action/control/terminal records retain both position and latest
outcomes identifying the same final node/ordinal. A fresh initial boundary has
neither; a restored initial boundary instead validates inherited position and
ordinal against its selected source/lineage contract and does not fabricate a
newly committed wave or outcomes. A local wave has consecutive checked ordinals
in canonical node/lineage order, and each named node exists in the pinned
definition. Repeated node names may represent distinct lineages and are not
deduplicated. Ordinary control transitions preserve the retained metadata
exactly, including result values, and only an action commit advances it. Reject
contradictory or forged control metadata even when its individual tuples are
well formed. Recovery retains the existing maximum of 4,096 activations per
wave and the 1 MiB bound for each staged activation's writes/result encoding,
in addition to the owning record's aggregate byte bound. Child input/control
admission and staged writes/results retain their separate original byte charges.
Admission stores the ordered exact child references once; successful staging
moves that same compact membership and control sequence into the staged
activation while dropping its resolved state-input vector. It does not copy or
recharge those inputs as staged output. All retained references still count
against the unchanged aggregate durable-record bound and prospective settlement
reserve before each native publication. These finite App resource restrictions
must be checked on untrusted durable data as well as new writes.

JoinFirst settlement counters use absolute visits of the join node in the
retained structural lineage, including any ancestor prefix. Execution and
recovery select the exact completed sibling set using the same pure predicate
over the owning checkpoint and compiled definition. Validation rejects both
foreign sources and coordinated omissions of sources and matching outcomes;
it does not rerun actions, conditions, reducers or caller typing hooks. An
ancestor contributes the same constant to both sides of every visit comparison,
so retaining absolute counts preserves nested and cyclic join behavior without
reconstructing ancestry solely to validate a selected set. Parent/child identity
and membership validation remain mandatory. This is private record semantics
retained by the normalized `app/v7` format, not a new public counter.
Public regression proof covers repeated JoinFirst visits and
late sibling suppression beneath one and two ancestors with overlapping node
names; tests must not freeze private counter spelling or encoding.

Every descriptive timestamp/deadline is sampled once by its owning mutation;
receipt reconciliation and restart reuse it. Same-process elapsed work may use
monotonic time; wall clock never selects route, identity, ordinal or reduction
order. Restart deadline translation is conservative and never grants a fresh
activation budget. Tests separate generated ids/timestamps from semantic order.

## Independent occurrences of one stable identity

Pinned child identity is SHA-256(parent identity + ':' + task_key), while each
SubGraphTask constructs a fresh independent Application. Its full unprefixed
lowercase 64-hex text is publicly observable: child_context.rs:5–54 returns the
public ActionContext identity as child state, and tracking_and_persistence.rs:
69–73,163–167 compares the tracking identity. Their oracle is the exact digest
in support/parallelism/graphs.rs:182–184. App child RunId adopts that format;
new root RunIds keep their existing prefixed UUIDv4 form. The occurrence token
remains a separate validated address and never changes the stable digest. The default async
executor accepts equal keys without deduplication or serialization. Repeated
child persistence reuses app_id and semantic sequence zero; InMemoryPersister
appends those observations and selects the most recently saved matching row.
Sources are parallelism/runtime.rs:103–107,253–275,317–345,
parallelism/executor.rs:100–108, tracking_and_persistence.rs:669–705 and
rurr-persistence/src/in_memory.rs:51–58,94–106. App's interactive controls and
immutable history additionally require an exact durable occurrence address;
the pin does not supply App's open_run/Input control plane. Do not claim that
a latest-only lookup makes concurrent equal-key Input ownership recoverable.

Every root/child admission owns one unique existing-format creation token.
Each occurrence has its own head, checkpoint zero, monotonic sequence, action
ordinal, capacity, lifecycle/control fences and bounded deletion scope. Root
admission normally creates a new RunId; repeated child admission preserves its
stable RunId and changes only the occurrence token. Parent relationships,
admitted batches, local runtime keys, completions, lineage, history cursors,
active inventory and all native fences identify the exact occurrence. The
existing RunRecord/RunHead/control/ParentRecord owners carry this dimension;
there is no second workflow engine, public child registry or identity wrapper.

For open_run, RunSelector::Latest follows the last natively committed observation in
that workflow/workspace/RunId scope, including checkpoint-zero admission. Every
occurrence publication updates the logical saved-observation link atomically
with its immutable row, head, observation indexes and receipt. A completion's
poll/return order, token spelling and clock are not publication authority. For
equal-key checkpoint-zero admissions in one atomic batch, original source order
defines their saved order and the greatest ordinal is latest. Native pointer
fences serialize only publication; equal-key executions remain concurrent.
A returned WorkflowRun pins the selected exact occurrence permanently. Later
publication by another occurrence cannot retarget that handle's snapshots,
execution, resolution, cancellation, checkpoint or history calls.

Opening a run and checkpoint/history lookup use App's control-plane projection:
checkpoint zero and ordinary control/terminal checkpoints are real App saved
boundaries. Restore and child initializer instead use the pin's saved-action
projection for CheckpointSelector::Latest and Action(n). Latest selects the
latest actual Action or Failed observation, and Action(n) additionally requires
a matching semantic action ordinal under the existing action-selector rules.
Initial, Control and Terminal rows never create a new saved-action observation.
A freshly admitted, unstarted occurrence B therefore cannot hide occurrence A's
actual saved state merely by writing checkpoint zero. App control rows never
become fabricated pinned action saves; the in-flight guard below still applies. Pinned authority is
application/step_txn.rs:489–515 and tracking/client/persistence_loader.rs:79–101.
These are two projections on the same existing selector/index owner, not another
public selector, lifecycle value or history product.

For a restore/initializer source RunSelector::Latest, the saved-action match
ranges across every retained occurrence. Do not first select the newest App
control head and thereby miss another occurrence's matching action. Exact
instead scopes the match to its one occurrence. No saved-action match is absence:
root restore requires its existing explicit fallback; a child initializer uses
its existing explicit fallback or resolved child default, without loaded-state
overrides. A prepared checkpoint-zero source is copied only through the explicit
App capability Sequence(0), not by manufacturing a pinned save. Sequence(n)
selects an actual immutable App checkpoint, including control/initial rows;
Exact scopes it to one occurrence and Latest selects the latest saved matching
local sequence across retained occurrences. WorkflowRun::checkpoint(Latest)
continues to mean that exact handle's latest App checkpoint.

Saved-action lookup does not authorize silently skipping current in-flight or
Unknown work. With CheckpointSelector::Latest, apply the existing clean-source
rule to the current head of the matched occurrence before copying its saved
row. RunSelector::Exact only narrows occurrence scope; Exact plus Latest is not
explicit authorization to rewind that occurrence past unsettled work. For an
aggregate RunSelector::Latest lookup, any retained occurrence in that logical
workspace/workflow/RunId scope with current in-flight/Unknown work blocks the
implicit historical copy. A later clean occurrence cannot hide a still-owned
uncertain occurrence. This guard also runs when no saved-action match exists:
uncertainty cannot become initializer defaults or a root fallback. Exact plus
Latest guards only its named occurrence. A fresh unstarted checkpoint-zero
boundary remains clean; an Input, child or Agent control still obeys the existing
in-flight source rule.

An explicit CheckpointSelector::Sequence(n) or Action(n) authorizes selection of
that named older clean boundary under the existing historical-restore rule.
Exact scopes that authorization to one occurrence; aggregate Latest chooses the
latest saved matching named boundary across occurrences. The selected boundary
itself must still be clean. This never settles, cancels, resets or infers success
for the original uncertain occurrence, whose controls remain independently owned.

The existing logical occurrence index also records which current occurrence
heads fail the clean-source rule, using the same bounded previous/next link
ownership as its saved-observation projections. Each normal native checkpoint
publication inserts/removes that exact occurrence's index membership atomically
with the head change. The logical unresolved tail is empty exactly when the
aggregate scope is clean. This is a projection of current durable controls and
staging, not another lifecycle state, task registry, ordering counter or timer.
Neither enumerating every historical row nor checking only the last-saved App
head establishes absence of unresolved occurrences.

Selection captures the appropriate logical index links and selected immutable
source row/head fences together. Native copy or fallback admission rechecks all
of them: aggregate Latest requires the unresolved tail still absent, and Exact
plus Latest requires its original current head still clean. An admission/effect
transition between selection and copy conflicts/reconciles instead of bypassing
the guard. Tail absence is an exact native fence, not a cached boolean. The
captured source and guard pointers are never retargeted during ambiguous receipt
reconciliation; unavailable exact native evidence preserves uncertainty.

RunSelector::Exact requires the requested workspace/workflow/RunId/token tuple.
A well-formed absent tuple returns NotFound, a sealed exact occurrence returns
Closed until final removal, and a stale mutation fence remains Conflict.
A token belonging to a different scope never selects or reveals that record.
Malformed tokens return InvalidInput before storage access. Exact restore and child
initializer select one occurrence first and apply their existing checkpoint/
absence-fallback rules only within it. Latest uses the aggregate matching rule
above. An exact missing occurrence may use an explicitly permitted fallback,
but never silently changes to Latest. Lineage
records the actual source occurrence, and a copied fork remains independent of
source deletion. Reopen preserves exact tokens and selects no replacement for
a missing admitted child.

list_runs returns each occurrence, ordered by (RunId, token) for bounded stable
inventory pagination; lexical token order is only inventory order, not Latest.
An Exact after selector continues strictly after that tuple. Latest(id) as an
after selector excludes every occurrence at or before that logical id. Returned
snapshots expose their exact token. Each history cursor remains scoped to its
exact occurrence and captured immutable cutoff. A cursor/handle cannot switch
to a newer equal-key occurrence, and its seal/generation fences remain unchanged.

The existing mutable history/index records own saved-order links. A logical
tail selects the latest App checkpoint; saved-action and matching sequence/action
tails select their latest eligible observations without scanning every occurrence
or its history. The index entry carries exact previous/next references where needed
for bounded deletion. Each publication appends/splices those links with its
normal native row/head/receipt mutation. This is run-history indexing, not a
generic list framework, new clock, public ordering counter or runtime registry.

Deletion removes only the selected occurrence and unlinks each of its indexed
observations under the existing bounded page transaction. The same transaction
repairs neighboring links and each affected logical/matching tail. If the
selected tail is removed, the prior still-retained saved match becomes latest;
never return absence while such a match remains. A sealed occurrence is already
unavailable for new source admission. Selection encountering its tail follows
the existing bounded deletion owner to repair that scope before admitting a
new lookup; it may return the established Closed/Busy observation while cleanup
is pending, but cannot silently copy sealed state. A final empty scope removes
its index links; no permanent retired-key tombstone or ever-increasing
occurrence counter is needed. Native receipt recovery preserves the original
link mutations and does not publish the same observation twice.

Required proof includes two equal-key children concurrently parked at real
Input, independently resolved through exact public selectors, with ordered
results and one original executor batch; same identity reentry with changed
inputs; exact history/fork of both occurrences; reopen before resolution;
newer admission racing an older deletion; missing/foreign/malformed selectors;
original handles/cursors after later admission; and lost batch receipt recovery
without another token or effect. Tests distinguish admission order from saved order: the earlier-admitted
child saving last becomes Latest, and restore/initializer Latest+Action finds an
older occurrence's matching action when the newest control-plane occurrence has
no such observation. Completed A followed by fresh unstarted B still initializes
from A's actual save. No prior saved action exercises the unchanged explicit
root fallback and child default/fallback rules; Exact+Sequence(0) still copies
a prepared App boundary. Add same-occurrence clean-save then Unknown, cross-
occurrence A-clean/B-Unknown, A-clean/B-Unknown/fresh-C, and no-prior-save/
B-Unknown/fresh-C cases: aggregate implicit Latest must reject. Exact plus Latest
rejects when its named occurrence is unsettled; clean Exact(A) remains usable
despite independently unsettled B. Explicitly named older clean Sequence/Action
may copy without altering the uncertain source. Race the source
control/head or unresolved index after selection but before native copy/fallback and
require exact-fence rejection/reconciliation. Existing root Latest tests must
adopt this distinction before production changes instead of retaining a false
pin-equivalence.
Deleting the current latest match exposes its preceding retained saved match;
removing an unrelated occurrence cannot change the tail. Same-batch initial
order is tested before either child advances.

## Bounded readers and selection

Exact checkpoint lookup is direct and scoped. Action-ordinal selection is a
separate projection selecting the latest matching action observation at its
captured committed head cutoff; it is not a StateRevision lookup. A successful
Action row matches each of its committed outcome ordinals, including an earlier
sibling in a BSP wave. A Failed row matches its attempted ordinal. Select the
matching observation with greatest immutable checkpoint sequence at or below
the cutoff, including failure observations; Initial, Control and Terminal rows
never supersede it merely because they retain the same successful position.
The index therefore retains both ordinal and observation sequence, not one
mutable successful-ordinal slot. A failed observation and a later explicit
successful retry remain independently addressable by checkpoint sequence.

Action-index integrity distinguishes absent local observations from damaged
indexes. Checkpoint zero's inherited position creates no local observation.
Successful local ordinals are contiguous and their first committed boundary
is found with at most 64 binary-search probes over immutable sequences; the
exact ordinal index must identify that boundary. Sparse failed BSP ordinals
are not monotone across explicit retries. The existing run head therefore
retains only each unresolved local failed ordinal's latest observation
sequence. A proposed failure is less than MAX_FRONTIER positions beyond the
next committed ordinal, so this evidence has the same finite frontier bound.
Control changes and retry admission preserve it; a committed wave removes
only entries whose ordinal it has successfully reached. Fresh and restored
checkpoint-zero admission start with empty local failure evidence. Readers
validate these sequence bounds and exact Failed row/ordinal identities, and
require the existing index to match the latest expected observation. A missing
or stale required index is Integration, even if an older failed index remains.
This bounded head evidence is private index integrity, not another outcome or
position ledger, all-history cache, or permission to scan history as fallback.

Pinned authority is `rurr-persistence/src/in_memory.rs:51–56`,
`rurr-persistence/src/sql/query_plan.rs:145–152` and the decisive
`rurr-testkit/src/persistence_cases.rs:615–671` duplicate-identity case: a later
Failed record at sequence 7 supersedes an earlier Completed record at sequence
7. `rurr-core/src/tracking/client/persistence_loader.rs:80–101` records attempted
sequence separately from prior-step state, and
`rurr-core/src/application/build/initializer.rs:243–259` resumes Failed at its
saved action while Completed resumes after it. App checkpoint sequence is its
observation order; its action ordinal maps the pin's semantic sequence. The
BSP matching/attribution rules above are explicit extensions of this serial
behavior. Missing
explicit selectors return NotFound. Restore's explicit fallback option applies
when either the source run or selected checkpoint is absent. It uses its
defaults unchanged, without applying overrides intended for loaded state or
stamping the missing requested sequence into new state. This follows pinned
`application/build/initializer.rs:221–242`: the missing-state branch returns
before the loaded-state override branch.

History traversal captures a head cutoff, pages immutable records through
existing StateStore list operations, and validates cursor run/generation/
deletion scope. App-owned cursor serialization is lossless through public DTOs.
The requested history limit is nonzero and at most 1,000, reusing the App run
inventory page limit. A larger request returns InvalidInput before storage
reads or caller callbacks; it is never silently capped. Physical StateStore
fetches may be smaller while assembling the requested bounded page.
A native list is not a snapshot; immutable sequence keys plus the cutoff provide
stable append-insensitive pages. New committed rows do not shift prior pages.
Corrupt rows, broken head/index/predecessor references, invalid cursor scope
and missing pinned definitions fail explicitly. Opening App pages active
inventory and loads current recovery facts; it does not materialize every
retained checkpoint into RAM. An active parent may reference terminal children
whose execution capacity has already been released. Startup loads those exact
referenced children for validation, retaining at most one parent's child batch
alongside the active inventory; unrelated retained runs do not enter execution
inventory. Child admission, execution, cancellation and recovery all use the
revision selections captured by the parent, including children first reached
after reopen with newer configured defaults. Persist a dynamic batch's resolved
selector, stable task key, original ordinal, state input, initializer selection
and history policy once in its existing child control/relationship. Transient
`inputs` values remain only on the concrete execution owner and use the existing
rebinding contract after recovery. No recovery path reevaluates a dynamic tasks
expression or silently substitutes a newer omitted revision.

## Resume, restore and fork

Opening the same run reattaches its existing fences/effects; exclusive execution
continues its saved position after required input rebinding. Restore creates a
fresh run identity from a selected source boundary, explicit overrides and
resume position. It validates/fences the source, pins exact revisions, copies
committed state/frontier into checkpoint zero, reserves capacity and writes
receipt/lineage atomically. Source state/history remains unchanged.
Lineage records the actual selected source checkpoint sequence. Its semantic
action pointer preserves explicit Action(n), including an earlier sibling in a
committed BSP wave; for Latest/Sequence selectors it uses a Failed observation's
attempted ordinal, otherwise the row's last committed ordinal. The copied
checkpoint retains the source's successful position/ordinal independently of
that pointer. This is App's resolved-pointer translation: pinned
`application/build/initializer.rs:193–213` preserves the requested load target's
sequence (including None for latest) in ParentPointer, and
`tracking/client/registration.rs:80–104` emits that pointer. App always records
its resolved source checkpoint and must not conflate selector identity with the
last sibling in that row. Missing-selector fallback creates no invented source
checkpoint or lineage.
The restored run is durably Paused at checkpoint zero. Restore itself does not
poll a caller action, invoke lower effects or schedule entry-node work; only
explicit execution advances the copied clean boundary. Same-run explicit
execution may likewise retry a proven-safe Failed observation at its saved
attempted node/ordinal, without consuming another ordinal before success.

Completed boundaries resume after saved action; proven-safe failed boundaries
may retry the attempted position carried by Failed, rather than the prior
successful Checkpoint.position. This is the serial rule. A failed BSP tuple is
only its observation representative: same-run retry and AtSaved restore retain
the complete uncommitted wave/frontier and retry all rolled-back contributors,
allocating proposed ordinals from the unchanged committed position. They never
silently replace that wave with the representative node or replay an earlier
committed wave. Successful AtSaved replays the selected activation with its
original cycle/join lineage and the successful observation's pre-routing
remainder, including unrelated pending frontier entries. Replaying a selected
JoinFirst reopens only that exact completed visit; every unrelated settled join
mark remains authoritative. For an explicitly
selected later Control/terminal checkpoint, state and source lineage still come
from that selected checkpoint; only activation/frontier proof comes from its
original successful Action observation. The proof is resolved under the same
captured source authority, never by inventing lineage from a node name. An
inherited checkpoint-zero position is provenance, not a new local Action save;
AtSaved retains that prepared frontier when no local observation exists.
Explicit entrypoint restart clears prior-action selection. Overrides pass codec/typing/required-key validation before admission.
No Agent identity/handle, provider continuation, credential, transient runtime
input or live notification target transfers into the new run. Source lineage
is metadata; deleting the source after a successful copy cannot strand a fork.

An in-flight/Unknown checkpoint cannot be forked into a new undispatched attempt.
Reject it until effect settlement. A currently Unknown source may have an older
clean checkpoint; explicitly selecting that older state creates a new authorized
run and may deliberately repeat later effects, with visible source lineage.
Never silently reset uncertainty or infer success from structural origin.

Definitions and behavior revisions remain caller supplied. Reopen accepts the
retained revisions required by history plus newer revisions for new runs;
ambiguous same-key/revision content or missing required versions rejects before
lower dispatch. Global codec handlers are sampled per operation; their portable
payload version is caller-defined, not inferred from runtime function identity.

## Recovery ownership

All durable record epochs are preflighted before generation mutation or lower
effects. Unsupported app/v5 or older epochs fail read-only. The one-way v6
migration in `STATE_STORE.md` is the only exception to ready-v7 storage and
the only authority for mixed epochs. A ready-v7 reopen claims exactly one new
generation using the exact receipt, then
reattaches every run/control, staged sibling, admitted child, Unknown effect,
workspace lifecycle operation and sealed deletion with reconstructed capacity.
Timeout is not a distributed lease or proof the previous process is dead.

Callers reopen/create Agents before AppConfig. App reacquires only their exact
associated Workspace/Conversation through public ids. Recovery never persists
or revives tasks, futures, locks, channels, lower records/handles or transactions.
Workspace creation/replacement/compensation and previously sealed run-history
cleanup settle before App publication. Sealed heads are paged and each original
tracked cleanup owner is observed before the next is admitted; neither all heads
nor all deletion owners accumulate in memory. Accepted native cleanup remains
owned if the open observer drops, and permanent failure prevents publication.
Run/effect/workspace-deletion convergence may continue afterward only under a
reattached tracked owner and its durable fence. Every driver panic/JoinError is
classified, watchers wake, and stale generations cannot commit.

Lost receipt is not lost intent. Reconcile exact mutation receipts and public
lower evidence before replay. Missing prompt output cannot be inferred from
Conversation info, and lower uncertainty remains Unknown until authoritative
truth or caller Accept/Fail/proven-safe Retry. Missing transient inputs create
an actionable rebinding target for unstarted work, never persist arbitrary
secret input or redispatch an already possibly accepted effect.

## Retention and deletion

Terminal result/history is retained until explicit terminal-run deletion.
There is no one-second expiry or hidden pressure-driven eviction. Active
execution capacity releases after terminal cleanup independently of retained
history; completing runs does not create a lifetime run-count quota. Retained
bytes/pages remain bounded per operation and storage exhaustion reports an
explicit error rather than deleting user history. Sequences never renumber.
Successful terminal runtime ownership is separate from that durable retention:
once drivers, controls, lifecycle/child observations and completion supervision
settle, dropping the last actual owner reclaims the cell and its registry entry
without deleting any history. A later observer reconstructs the saved terminal
view without replay. Unresolved work and retained transient lifecycle failures
keep their existing owner and error authority. Valid short pages do not impose
a lifetime physical-page quota on history or startup scans.

Deletion first fences and seals the terminal head against new read/fork
admission and persists progress. It removes bounded history/index pages through
the existing mutation receipt path, atomically advancing progress per page.
Only after all owned rows are gone does it remove head/final scope indexes.
Reopen resumes a sealed deletion. No giant all-history transaction, generic GC,
observer-pin family or source-fork retention graph is necessary.

Read/fork versus seal has a defined linearization: a pre-seal transaction may
return a complete read/copy; post-seal admission returns Closed while the sealed
head remains, then NotFound after final removal. Stale mutation fences return
Conflict. No successful partial history or half-created fork is permitted.
An in-flight observation that already selected a readable head returns Closed
if that exact head disappears during checkpoint or predecessor rereads. This
revokes the observation without closing App. Missing required rows under a
still-readable head remain Integration failures.
Every page and finalization is idempotently receipt-reconciled after lost reply;
unrelated runs and already copied forks are preserved. Unknown/nonterminal
runs reject deletion before sealing.

A terminal child also returns Busy while a nonterminal parent still references
its exact admitted occurrence. Successful staging is still a parent dependency:
its copied writes/result have not committed. Child `delete_after_reduction`
releases that dependency only at the barrier which actually commits that staged
child outcome. The original child relationship supplies its authored policy;
the original staged activation retains exact admitted occurrence membership
until that barrier. A selected successful stage may use that child's immutable
Succeeded checkpoint as its terminal authority: Succeeded has no readmission
path, and the staged dependency prevents deletion/replacement of its exact
occurrence. The native seal still checks its original parent relationship/base
and exact Succeeded row/head fence. Failed children may be retried, so their
current terminal status never substitutes for successful copied-result proof.
Terminal status, a vanished control, a later
parent status, or a task-key match alone never authorizes automatic deletion.
Explicit parent deletion does not cascade to independently retained children.
Observing a terminal child may finish from its own validated row after the parent
is deleted. This admits no new child work and does not authorize a parent
reduction; those retain their exact relationship and completion checks.

Publish each selected child's deletion authorization in the same native
transaction as the parent's copied result and outcome. Prefer sealing the
existing terminal retained child head directly in that transaction when all
its effect, lifecycle and capacity-release fences have settled. That existing
Deleting head already owns bounded progress and restart discovery; it needs
no additional pending-deletion queue. If an intermediate authorization is
necessary to preserve existing cleanup ownership, it belongs on that exact
child head, names the committed parent occurrence/checkpoint and is recovered
by the existing deletion owner. It is not another task, public DTO, timer,
result store or unbounded parent history list. The choice must be justified
by the actual mutation/cleanup fence review before implementation.

Deletion authorization never deletes a child from an uncommitted, failed,
cancelled or discarded wave. An authorization remains valid after the parent
advances, terminates or is explicitly deleted, because the authorizing native
transaction already fenced and copied the original result. The seal still
fences the exact terminal child head, its existing capacity-release status and
all remaining effect ownership; capacity is released exactly once. Receipt
loss or restart resumes the existing bounded deletion without invoking an
executor, action, reducer or callback again. Explicit retain and omitted history
remain readable after the same successful parent barrier.

Ordinary public child deletion without such committed authorization retains
its parent dependency check. Once that dependency is released, sealing checks
the observed parent head revision and immutable row in the same transaction;
an absent parent is fenced as absent. Concurrent parent readmission cannot
invalidate membership between observation and sealing. Deleting one occurrence
never seals another occurrence sharing its stable task identity.

## Required proofs

The history/head/index/receipt mutation is exercised at each write, commit,
lost-reply and process-crash boundary. More-than-one-page tests cover readers,
retention deletion, source deletion after fork and append-versus-read cutoffs.
Delayed observation after the former one-second deadline and App shutdown/reopen
must retrieve terminal outcomes. Multiple simultaneous controls and staged
siblings survive crash independently. Old/mixed epoch preflight proves zero
writes and zero lower calls. Refer to TESTING.md for review/release accounting.

Child-policy tests cover direct Child, Map and synchronous Pipeline; exact
retained history before reduction; explicit retain afterward; one child done
while a sibling still awaits real Input; failure/cancellation before the barrier;
parent advancement/deletion afterward; lost barrier/seal/page receipts; and
restart between authorization and the first deletion page. Retained parent
state/outcome and the original executor/effect counts prove copied authority.
Tests must not require a particular redundant read or helper choreography.

Selector tests retain initial failure, failure after a successful action, and an
explicit same-ordinal retry as separate observations; state-preserving controls
must not become selector winners. BSP tests select an earlier successful sibling
and attribute both an activation failure and a barrier-wide reducer/typing
failure in canonical order, with complete-wave rollback. Restore tests distinguish
failed attempted position from prior success and keep the original history
unchanged. These are required runtime proofs after the public APIs compile;
source-derived expected values alone do not certify persistence behavior.
