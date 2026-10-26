# Application Ownership

[`SPECIFICATIONS.md`](SPECIFICATIONS.md) is the sole exact public ledger. This
file owns application configuration, opening, lower-runtime composition,
admission, revocation, errors, and shutdown.

## Configuration

AppConfig owns one caller-built CS, a homogeneous Agent inventory, revisioned
workflow definitions and concrete App-owned action, condition, typing, hook
and child-executor values. Exact signatures/defaults are defined only in
SPECIFICATIONS.md. There is no AppState, implicit Redb constructor, supplied
lower generic default, lower config envelope or erased backend normalization.
A process-local caller constructs an ordinary in-memory StateStore itself.

AgentSpec holds a validated logical key, nonempty bounded caller revision and
one fully built A: AgentBackend. Its fields remain private. Revisions cover
caller-owned configuration without App inspection; reusing a revision for
incompatible configuration violates the caller contract. Multiple revisions
needed by retained runs may coexist with a new revision selected for new work;
duplicate key/revision entries and absent required revisions reject before
lower effects. Construction inventories preserve caller order: the last supplied
revision for a logical key/id selects new admissions, while earlier distinct
revisions remain available to existing pins. Revisions are opaque caller strings,
never semver-sorted or compared by lower identity. Shutdown visits every
transferred key/revision once in canonical key/revision order. App never parses, formats, persists or compares AgentId.

App preserves exact associated Workspace/Conversation types and CS::Transaction.
Owners are retained under Arc only where sharing is necessary, without Clone
bounds on caller backends. Consuming Agent shutdown remains App-owned after
transfer. Providers/tools/knowledge and Agent storage remain below Agent.

AppConfig's consuming with_* operations replace only their own pre-open
construction inventory and type parameter. No separate ActionSpec wrapper,
AppExtensions god trait, public dyn registry, generic service factory or
configuration getter family exists. Empty inventories use only honest App-owned
defaults. Unconstrained A for a state-only application may require an explicit
caller type annotation; this does not justify a fabricated supplied Agent.

Dropping configuration before ownership transfer cannot promise asynchronous
lower cleanup. The caller uses owning lower APIs to reconstruct/reopen as
needed. Successful App open takes responsibility for every transferred Agent.

## Open

`App::open`:

1. validates every app-owned scalar and relationship;
2. parses and compiles each workflow definition and validates the complete
   child graph without a lower effect;
3. consumes the caller-built StateStoreBackend without constructing a lower product;
4. exact-loads the private app record or prepares a missing-record intent without writing;
5. preflights the root and every referenced workspace/run/history record epoch;
   ready operation requires `app/v7`. The one-way cold-cutover `app/v6` migration
   follows `STATE_STORE.md` before generation claim. Unsupported epochs and
   initial corruption fail read-only, before lower effects;
6. verifies configured logical-key/revision bindings against the retained revision
   inventory, preserving old pins while permitting new definitions for new runs,
   without reading or storing lower Agent ids;
7. takes ownership of the already-live Agents, with canonical key/revision cleanup;
8. claims exactly one new durable application generation;
9. validates complete durable ownership relationships, reconstructs one shared
   owner for each recoverable run and in-progress workspace operation,
   reattaches the exact durable fences, and settles previously sealed run-history
   deletions one owner at a time; and
10. publishes one live generation after ownership reconstruction succeeds.

The exact mutation receipt is the only proof that this opener owns the claimed
generation. If generation claim is `Unknown`, open fails without publication.
An observed app record equal to the intended record is insufficient because a
concurrent opener can commit the same record without proving this opener's
claim.

Open synchronously settles recovered workspace creation, replacement, and
compensation before publication. Recovered workspace deletion instead reattaches
its exact durable fence and owner without waiting for lower deletion to converge.
Previously sealed run-history deletion has no lower effect: open pages those
heads, admits one tracked cleanup owner, and observes its original completion
before admitting the next. A cleanup failure prevents App publication and retains
the existing failure classification and cleanup path. Dropping the observation
does not drop the accepted native deletion future. Recovered workflows, lower
effects, workspace deletion, cancellation, and ordinary retention maintenance may
continue under the reattached App-owned runtime after publication.

Validation precedes lower effects. No lower operation or channel wait
occurs while a synchronous lock or StateTransaction is held. If ordinary open
failure follows Agent ownership transfer, app-sdk awaits consuming shutdown
of every supplied `A` in canonical key/revision order and
preserves any durable recovery intent. A cleanup failure does not erase the
primary failure. Dropping an open future after ownership transfer has begun
does not promise graceful cleanup or install a cancellation guard; the caller
reconstructs or reopens the durable Agent implementations and the next App
open reconciles durable app facts.

The durable app record stores only:

- app schema epoch, lifecycle generation, shutdown state, and active capacity;
- current logical Agent and workflow revision selections, with no opaque runtime
  value. Exact historical Agent bindings belong to workspace journals and run
  heads; action and condition references belong to versioned definitions.

The immutable definition catalog retains versioned workflow fingerprints, and
compact run heads retain exact executable pins.

It never stores an Agent id, lower builder/config, credential, private record,
native id, protocol envelope, runtime handle, task, lock, or channel.

## Fixed Private Limits

The implementation owns finite, documented constants for active and pending
runs, node attempts, external calls, graph size, state size/depth, prompt
bytes, child depth/fan-out, map items, retries, interactions, transaction
record changes, and retention work. There is no public scheduler
tuning API and no private `None` meaning unbounded.

Active capacity is acquired before owned work becomes reachable and retained
until terminal cleanup or truthful reconciliation. Retained results/history do
not consume active-run capacity, and history pages are not loaded wholesale.
Worker, action, control, graph/state and history-page bounds have explicit units,
limit-1/limit/limit+1 cases and documented errors; no tuning API hides unbounded
work. A bound is not a production performance claim until workloads prove it. Recovery reconstructs capacity
for every durable nonterminal fact, including admitted children and unknown
external effects. Before per-record reconstruction, recovery rejects complete
durable workspace or run inventories that exceed their respective private
finite bounds; equality to either bound is not itself a capacity failure.

One shared private limit admits at most 1,024 concurrent caller operations per
App. Prepare, start, restore, execution acquisition, run resolution/cancellation/
deletion, and workspace creation/replacement/deletion reserve nonblocking
capacity before storage access, new run-cell ownership, or caller state typing.
Exhaustion returns `Busy` without starting that work. The bound counts accepted
operations, independently of durable active-run capacity. Dropping their caller
observers does not release capacity while accepted work or its completion
supervision is unresolved. Definite rejection and completed supervision release
the slot for reuse in the same App.

Execution acquisition holds capacity through its admitted initial publication
and supervision handoff. Returned lazy Execution/SyncExecution owners, parked
inputs and ordinary read/wait observers do not retain caller-operation capacity
for their lifetimes. Internal children, reconciliation, cleanup, recovery and
shutdown use their existing owners and bounds without acquiring this caller
quota; saturation cannot prevent the work required to release it. An observer
reattaching to an already registered identical workspace deletion shares that
operation's slot. A different registered intent or stale fence still returns
`Conflict`, rather than consuming another slot or hiding the conflict as Busy.

## Handles And Admission

`App`, `Workspace`, `Workflow`, and `WorkflowRun` are opaque generation-pinned
handles. They cache identities and owners, not mutable persistence records or
lower handles. Dropping an ordinary Workspace, Workflow, WorkflowRun, or
observation future never cancels accepted work and performs no asynchronous
cleanup. The exclusive App root has the revocation and retained cleanup
semantics below. Exclusive Execution/SyncExecution
owners revoke uncommitted advancement on drop under WORKFLOW.md; this does not
change observer-handle semantics.

Run admission and all workspace lifecycle changes pass through their
functional owners. Workspace coordination does not own run admission.

One private `RunRegistry` coordinates one `RunCell` per typed
workspace/workflow/run occurrence. Active or unresolved work retains its cell,
including queued drivers and completion supervision. A successfully settled
terminal cell no longer requires strong registry ownership; after all actual
owners and observers drop, both its allocation and unused registry entry are
reclaimed while App remains alive. Durable history is unchanged and a later
observer can reconstruct its terminal view. Reopening an observed terminal run
does not replay actions or lifecycle callbacks. Transient lifecycle failures
retain their existing same-App observation authority until successful execution
reacquisition; pending child observation, controls and publication cannot be
retired as successful terminal work.

`WorkflowRunRuntime` owns that key and drives itself
without identity parameters. Wakeups, live interactions, retry disposition,
checkpoint observation, and release are receiver operations on that run owner.
One closed schedule-cause vocabulary is the only route into its driver.
Asynchronous foreground Child continuation uses that same supervised worker,
queue and task registry. Its command carries the exact exclusive generation and
original Session publication receipt; the local executor future is constructed
only on the worker. Accepted work remains an existing Foreground operation until
receipt settlement and generation-exact cleanup, including cancelled waits and
owning Execution Drop. No second run driver or retry counter is created.

One private `WorkspaceOperations` owner admits closed creation, advancement,
deletion, and recovery operations. Each typed `WorkspaceOperation` owns its
intent and execution data. AppRuntime hands its concrete execution future to
AppTasks for supervised lifetime tracking under the exact operation identity.
Requests
with the same closed intent, workspace identity, and fence reattach to one
shared completion regardless of observer lifetime. The recovery form carries
the durable deletion intent it reattaches. Creation identity includes its
configuration and canonical lower target set; replacement identity includes
its intended next revision and configuration. A different intent, target, or
fence conflicts.

Once deletion intent is durable, its one registered owner remains responsible
across exact-safe `Busy` and `Timeout` observations. It exits only after
settlement, a permanent contradiction, generation supersession, or lifecycle
revocation. Retry admission stays bounded by one tracked task, one lower call
at a time, and capped backoff; a fixed attempt count never abandons durable
deletion ownership.

Every accepted run pins:

- its workspace identity and revision;
- the materialized lower workspace identities/revisions;
- the exact logical Agent key/revision pairs required by its definition and
  captured child definitions, before the first Agent node executes. Descendants
  and restored runs inherit those selections. An unrelated state-only run does
  not make unused Agent revisions a historical supply obligation;
- its exact workflow revision and compact child revision selections; the
  immutable App definition catalog checks canonical content once per pair.
  Selections for implicit child references are captured at admission, inherited
  by descendants and restored runs, and cannot switch to newer caller defaults
  when an older parent resumes before reaching its child node. If the reachable
  graph contains dynamic child selection or a resolved tasks source, this
  selection includes each workflow id's current default; purely static graphs
  keep only their reachable selections. Every potentially selectable configured
  definition's Agent key uses the same captured root Agent revision, including
  explicit historical workflow revisions; actual target schema compatibility
  is checked only for the resolved batch. This is the unavoidable inventory
  dependency of an unrestricted dynamic selector, not a reason to pin unused
  Agents for static state-only roots. Explicit revisions remain exact immutable
  catalog references. Each definition pins its own action/condition references.

Agent deployment uses the current selection in the App record, exact pairs in
workspace journals and compact retained run-head pins. There is no Agent
configuration catalog: the caller owns revision meaning. Before receipt cleanup,
generation claim or operational lower calls, opening checks every workspace
journal pair and readable run-head pair against the supplied inventory. Sealed
history deletion needs no executable Agent binding; unfinished workspace
deletion still requires the owners of its remaining lower workspaces. Missing
required pairs return Conflict without changing the generation.

After an exact generation claim, opening re-reads and fences each affected
workspace through its existing operation owner. It first reconciles prior
workspace operations, then journals missing configured materializations before
lower calls. It preserves old materializations and the logical workspace
revision. New materializations must settle before recovered runs are scheduled
or the App is published. A preflight read alone never authorizes overwriting a
workspace journal that progressed before the claim.

## Errors

The seven categories in the public ledger have these meanings:

| Category | Meaning |
|---|---|
| `InvalidInput` | invalid caller data or relationship rejected before an effect |
| `NotFound` | exact scoped evidence proves a requested public resource absent |
| `Conflict` | stale fence, incompatible durable fact, or invalid transition |
| `Busy` | finite capacity or an exclusive owner prevents progress |
| `Closed` | revocation, deletion, or terminal invalidation rejects new work |
| `Timeout` | an app-owned deadline expires without fabricating a result |
| `Integration` | a lower public operation, host integration, corrupt private record, or unsupported private epoch fails |

Dynamic messages are bounded and redacted according to
[`SPECIFICATIONS.md`](SPECIFICATIONS.md). Durable run failure and cancellation
are `RunStatus` data; actionable waiting and unknown effects appear in
`RunSnapshot.interactions`, rather than as operation errors. Caller typing
validation that panics during root start or
prepare, before its first run record is published, returns a redacted
Integration error rather than Closed. The App remains usable and publishes no
run for that rejection. Panics after run admission follow the owned execution
failure and recovery contract below.

Required execution-authority reads preserve their typed failure disposition in
one finite pure-read pass. Only recoverable reads retry within that pass;
permanent unreadability or read expiry invokes fatal App cleanup before another
effect. Authoritative Closed/Conflict fences remain unchanged. Public snapshot
and history reads are observation operations whose errors do not themselves
close the App. Admission, native commit, Unknown receipt and lifecycle-callback
failures retain their separate policies.

An original execution owner keeps successful pre-hooks, proof that lower
conversation creation never began, and actual lower errors or notifications
while authority reads retry. A recoverable read cannot turn known no-effect
preparation into lower-effect uncertainty. Recovered Pending notification work
retains its acquired conversation and receiver through the post-setup read, then
its delivered notification through the following read. The same ownership
applies before the first prompt, after a notification wake, and when observing
a returned lower error or notification; readable authority determines the
existing failure or uncertainty disposition without resubmitting the prompt.

Read passes retain earlier original deadlines. The attempt deadline continues
to bound lower setup, valid notification waiting and required reads while live.
After that deadline elapses and its timeout publication owner returns, abort
observation has a fresh finite read pass without restarting the prompt or
attempt. EOF callbacks and pre-publication reads share their existing cleanup
budget. A background terminal close shares the remaining lifecycle budget
between its closing callback and subsequent generation proof. Original parent
activation deadlines continue to cap child observation.

Halt, Pause, explicit failed-run readmission, EOF, source Stage and Failed publications
retain their exact native checkpoint receipts, including abort joining an
already-admitted source publication. Returning a retained receipt still proves
the owning App generation in a finite pass sampled after that receipt; it does
not require decoding the same checkpoint again. Accepted native failure,
timeout, notification, abort and retention mutations, including Unknown receipt
reconciliation, remain outside pure-read timers. Their latency does not consume
a subsequent post-receipt read pass or renew a callback budget. Dropping an
observer does not re-admit the operation or abandon its accepted cleanup.

Retention may advance the terminal head revision, so stopping and consuming
release require a fresh current-head observation after retention returns.
Likewise, a rejected terminal stopping mutation supplies no terminal receipt
and requires fresh authority after the native attempt returns. A rejected child
result's disposition read also uses a finite pass after its original rejection
handling returns. Terminal settlement's
head/active-marker/head-consistency proof and generation observation
share one finite read pass. These owned execution reads remain separate from a
consuming caller's later public snapshot.

An error returned after a possible external effect never authorizes automatic
retry. Durable intent, exact lower evidence, and reservation generation decide
whether an effect can run again.

## Shutdown

`RejectActive` leaves an active App unsealed when accepted work exists.
Otherwise it begins finalization. `Drain` seals new admission and waits for
accepted work. `CancelAndDrain` first commits monotonic cancellation intent for
every nonterminal run, then drains.

The duration variants use nonzero durations no greater than 365 days. The
duration bounds one caller's observation of the entire process: runtime drain,
direct app-store receipt reconciliation, `A::shutdown`, and retained
outcome publication. Timeout never cancels the App-owned finalizer.

A retention commit may remove an active marker after shutdown has scanned it.
Run snapshot reads may also lose serializability when an owned run transition
changes an observed revision. A transaction-conflict result from committing that
read-only observation discards its snapshot and starts a fresh transaction within
the existing finite observation loop. Exhaustion remains a recoverable read
disposition; non-conflict failures retain their existing classification.
The existing finalizer retries only the typed recoverable read disposition,
waiting for task changes or bounded reconciliation backoff before observing
again. There is no fixed total retry count for a valid stream of terminal
capacity releases. Permanent corruption, semantic Busy/Conflict and uncertain
native-write outcomes retain their existing failure/receipt handling. These
read retries neither replay native writes nor add a finalizer deadline: the
caller's timeout bounds observation, while the same accepted owner continues
until cleanup converges or a terminal failure is retained. Each independent
terminal-capacity release prepares its existing native mutation under one 30-second observation
budget shared by the run, marker, App-count and generation reads. Required
capacity preparation is not an optional terminal observation that normal
shutdown can discard. Expiry destroys the original pure read before fatal App
cleanup. The native retention mutation and its accepted receipt remain outside
this read budget; known deletion or generation closure stays an ordinary fence.

Terminal children release active capacity after their own lifecycle cleanup,
including a completed prefix whose siblings are still waiting. Retained history
remains protected by the original parent membership. At a successfully completed
child group's original Stage receipt, those members' capacity is released. The
native Stage may retain any remaining active members under exact admitted
membership, terminal-row and active-marker fences; an already retained member
is not released again. Its original receipt owns uncertainty and replay. Staged
child history remains referenced until the parent barrier or cleanup permits
deletion. Failed, cancelled, or incomplete groups keep their existing descendant
and individual terminal cleanup owners.

One private `AppLifecycle` data-carrying state enum owns active, draining,
cancelling, finalizing, and closed state. Draining carries its exact
RejectIfActive or Wait policy; finalizing carries its exact Drain
or Cancel mode. Task admission, application admission, finalizer escalation,
and synchronous root-drop revocation all derive from this one mutex-protected
state. There is no second atomic lifecycle or revocation flag. One tracked,
cancellation-safe finalizer owns:

1. admission sealing;
2. cancellation convergence when requested;
3. draining all registered run, workspace-operation, and bounded task owners;
4. exact receipt reconciliation and final-mode reobservation;
5. consuming `A::shutdown` in canonical key/revision order while retaining the
   first cleanup error;
6. publishing durable Closed; and
7. publishing the retained live outcome to every observer.

Graceful quiescence includes accepted run operations and foreground admission
or publication owners, even before capacity exists or after explicit deletion
removes its final head. The lifecycle owner rechecks those tracked owners under
the same lifecycle-to-task lock order used by admission before entering
Finalizing. Result publication remains owned until the operation completes;
forced failure and root-drop revocation retain their existing behavior.

During accepted `Drain` or `CancelAndDrain`, a background execution may stop
its pure terminal observation read after receiving the original terminal
publication receipt and completing that checkpoint's lifecycle callbacks.
The existing run owner must have no pending scheduled continuation or child
completion callback. Work scheduled concurrently remains on that owner.
This permission covers only the terminal observation read; native write
receipts, uncertain-outcome reconciliation and retention work remain owned
through settlement. A panic while dropping the selected read reaches the
existing fatal App cleanup path and retains a redacted Integration failure.

A primary finalization failure does not leave a draining or cancelling live
owner behind. The finalizer still enters Finalizing, performs the owned cleanup
and durable close sequence, installs live Closed, and replays the retained
failure to later shutdown observers.

App persistence calls the caller-owned `StateStoreBackend` directly through a
narrow private record/mutation codec. AppStore holds CS directly and invokes exactly CS::Transaction. There is no
App-owned supplied-store dispatcher or duplicate transaction forwarding enum. It introduces no
storage actor, erasure, thread, queue, or independent shutdown lifecycle;
transaction fencing, bounded admission, and cancellation safety remain
backend responsibilities. `AppStore<CS>` and its caller-supplied store remain
retained until `AppRuntime` itself is dropped; application shutdown does not
invent a lower storage shutdown step.

An identical shutdown reattaches. `Drain` may escalate to
`CancelAndDrain`; no mode de-escalates or conflicts silently. Caller future
drop cannot detach the finalizer or lose its outcome. Root handle drop seals
local admission and revokes owned tasks synchronously only when no graceful
finalizer already owns cleanup. The existing task owner then retains a drop
finalizer that joins revoked work and consumes remaining Agents in canonical
key/revision order. This bounded best-effort cleanup runs only while the caller's Tokio
runtime continues to be driven; root drop does not await it, publish durable
Closed, settle uncertain external effects, or claim graceful shutdown success.
A cancelled App::open future has not handed a root to the caller and does not
promise graceful Agent shutdown. It still closes and wakes any runtime task
owner it already constructed, so its idle supervisor can terminate. An unpolled
opening future has spawned no App work.

`RejectActive` synchronously installs its draining policy before it checks
quiescence, so concurrent admissions cannot cross the check/commit boundary.
If accepted work is found, it returns to Active only while the same reject
policy still owns lifecycle state and the App root still exists; a concurrent
Drain or Cancel upgrade wins. If the root was dropped during the reject scan,
the same lifecycle owner retains that intent and advances into best-effort drop
cleanup instead of reopening admission or waiting for active work to complete.
Mode escalation remains observable while Finalizing and is republished before
durable Closed. A narrow shutdown-publication gate linearizes those two writes:
a Finalizing Drain-to-Cancel request acquires the gate before changing the live
mode and retains it through durable Cancelling publication, while the finalizer
retains the same gate across its last mode check, durable Closed publication,
and the live Closed transition. An escalation that acquires first is therefore
published; a finalizer that acquires first closes without accepting an
unpublished cancellation, and the later caller observes the shared Closed
outcome.


## Runtime deadlines, panic and local execution workers

Every complete activation uses one sampled deadline, including lower setup,
subscription, execution, settlement and recovery. Cleanup uses separate bounded
grace and cannot erase the original failure. AppTasks supervises every admitted
task/thread and inspects every join result; driver release and watcher wakeup
do not depend on normal return from a possibly panicking body. Unwinding before
possible effect is Execution failure; possible acceptance is retained Unknown.
panic=abort is process loss and is proved through fresh-process recovery.

Requesting another bounded drive turn does not require a separate authority
read. The tracked driver obtains current bounded authority before admitting
any effect; already accepted native receipts remain with their original owners.

An admitted execution's pure authority observation pass has one 30-second
ceiling, shortened by any earlier original activation or cleanup deadline.
Initial root observation and background driver bootstrap have this bound before
opening execution hooks or constructing an effect. The first current-authority
read after an owned task panic has the same bounded pass; subsequent native
failure or uncertainty reconciliation retains its original task and receipt
ownership. Recoverable reads retain the original pass; permanent
unreadability or read expiry invokes fatal App cleanup. A control watchdog
shares its pass across the initial read and notifier-fenced recheck. These
read budgets are separate from waiting on a valid Input, Sleep or Retry deadline
and never cancel an admitted native transition. Agent preparation bounds its
required observations before and after the shared lower preparation lease, and
on a control wake, with the same finite pure-read policy. A valid lease wait
remains inside the original activation or abort phase and outside these read
passes. A wake read failure preserves possible lower acceptance; it cannot
claim that a conversation was never created or replay preparation. After a valid Retry wait, the
current-authority observation starts one new bounded read pass; the original
activation deadline still governs native timeout settlement before another
attempt can be admitted. Retired controls and validated
terminal rows admit no further work. An admitted child's completion observation
likewise caps its pure own-row and parent-authority reads at 30 seconds or the
remaining original parent activation deadline, whichever is earlier. Retries
retain that sampled read budget, including after the caller executor is dropped;
unreadable expiry cannot leave the original completion publication stranded.
The executor's returned completion vector retains one such budget across all
ordered child proofs and the final parent/ancestor observation. When original
child admission or an incomplete executor result is rejected, its failure owner
retains the original rejection while the required parent-authority read retries
in a finite pass. Permanent unreadability or read expiry invokes fatal cleanup;
the following fenced native failure publication retains its own receipt owner. A parent
watchdog bounds each read pass and its retries separately from a valid wait
on the original child deadline; a notification starts the next finite pass.
Native completion and lifecycle finalization retain their existing owners.
When resuming a Child control, the ordered inventory observation retains its
validated parent receipt and shares one such read budget across all admitted
children and recoverable retries. No child executor is admitted until the whole
inventory has been validated; native child admission stays outside this budget.

Finalization retains its first failure and current cleanup phase on the
existing lifecycle owner. A panic during Agent shutdown advances past that
consumed Agent and preserves the remaining inventory; cleanup never restarts
an already consumed Agent. A panicked durable publication phase is not entered
again, and its unknown write outcome remains a failure rather than fabricated
Closed evidence. Each consuming Agent shutdown has one 30-second cooperative
cleanup bound, independent of the caller's shutdown observation timeout.
Observers obtain terminal shutdown results only after the finalizer is joined;
cancelling an observer waiting for the supervisor join cannot detach that join.

The shared supervised execution worker owns local ChildExecutor futures in
background mode. Only Send intent/Arc owners/results cross its bounded channel;
streams/Data/child futures are constructed and polled on the worker using the
captured still-driven runtime.
This removes an unexpressible generic Send bound and adds no public ChildTask,
trait twin or second engine. Preserve compile/runtime and independent review
evidence for bounded workers, no recursive worker-admission deadlock, a
still-driven current-thread runtime, local-value isolation and joining after
facade/root drop and cancelled close observation.
Background stream acceptance adds no Send bound: the source stays on the worker.
Its mandatory positive proof must execute an Rc-bearing source and verify
construction, polling and Drop on that worker through completion and cancellation.
The `worker_contract` target executes this Rc-bearing source through completion,
cancellation and shutdown; native-only runs configured with that stream type
would prove compilation only. Keep the runtime assertions and independent
review requirements when changing the worker.
Caller-driven local Execution keeps local futures on its caller task.
SyncExecution::open borrows the run exclusively and constructs a blocking facade
over that same Execution on the calling thread, using the App's captured
Handle::block_on at synchronous entrypoints. Its fallible try_next method
advances to the same event boundary as asynchronous Execution::next.
It creates no runtime, worker, command channel or additional execution engine.
Sources and typed values are constructed, polled and dropped on that caller
thread. The captured runtime and lower runtimes remain alive and driven elsewhere.
Acquisition and each blocking method reject an entered Tokio runtime context
before blocking. Owning Drop delegates synchronous revocation to Execution;
it never blocks or claims to join a caller-created thread. App retains its
existing admitted publication, native-effect and child cleanup ownership.

The background Driver lane uses one lazily started dedicated OS worker per App.
Its typed run-identity/delay queue and active driver set reuse the existing
100,000-run bound; it introduces no unbounded admission or thread per run.
RunCell remains the generation owner, and drivers poll the same execution
engine. Operation and reconciliation tasks remain on their existing Tokio
lanes. Pending drivers are multiplexed so parked controls do not monopolize the
worker. Caller-local values are constructed and destroyed on their polling
thread. This Driver routing is separate from the remaining ChildExecutor and
foreground execution integration proofs.

Cancellation is registered before thread creation and before a driver intent
enters the queue. Task supervision retains each driver completion and the
worker join. Closing admission signals the worker and its drivers before
waiting for non-finalizer tasks; it never aborts the worker's join observer.
Only after worker exit notification does the completed-thread join enter the
caller runtime's blocking pool, preserving that pool for lower operations.
Worker creation, thread or join failure seals runtime admission and uses the
existing Finalization first-failure state and cleanup phases. A caught per-run
panic retains the existing run reconciliation path. The captured runtime must
remain alive and driven through App shutdown; no unconditional join is promised
after the caller has destroyed it, and Drop performs no blocking or async join.

Public run wait observations require live runtime ownership. Ordinary root
drop revokes waiting and advancement but preserves read-only committed
snapshots during and after successful best-effort cleanup. The same lifecycle
owner retains its drop intent through Closed; any retained cleanup failure,
fatal worker stop, or explicit shutdown revokes snapshot reads. Snapshot checks
that authority before loading and before returning its projection, and its
existing durable generation check fences the old handle after another App
opens. Private recovery reads remain available to the existing finalizer while
it reconciles and closes the generation.
