# OpenCode Control-Plane State Domain

This file defines OpenCode's backend-private use of the required
caller-supplied `S: storage_sdk::StateStoreBackend`.
[`../STATE.md`](../STATE.md) owns the shared durable floor, revision rules,
reopen contract, ambiguity policy, and cleanup semantics. Managed source
content remains in the separate caller-supplied
`D: storage_sdk::DatabaseBackend` under
[`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md).

OpenCode control-plane state is current-state recovery, not an execution
history, transcript database, event-sourcing layer, generic command bus,
optional external mirror, or replacement for native server inventory.

The StateStore is not the command path to the Agent event owner. A request for
gap or structured-parent history remains runtime coordination; only its
ownership-checked results and resulting high-water or prompt facts enter
durable state.

## Private Attachment And Durable Floor

Root Agent, Workspace, and Conversation records remain backend-blind portable
authority. OpenCode stores private attachments and records alongside them for:

- selected release and non-secret server-compatibility facts;
- workspace routing context, full-access/network policy compatibility,
  normalized-root continuity, revision observation, privacy validation, and
  bounded native root-session ownership;
- conversation-to-session binding and validated descendant closure;
- one bounded private native session-creation correlation and ambiguous-create
  obligation until exact binding or cleanup is proven;
- native catalog generation and completeness state;
- managed-knowledge source identity and integrity references across Database,
  Agent, Workspace, and Conversation scopes, never content;
- one current prompt generation with admission, dispatch, acceptance,
  effective model, enforced absence of unsupported reasoning/personality,
  timeout/structured-schema/tool-selection fencing, cancellation, terminal,
  waiter, output-discard, structured-parent event correlation, and
  reconciliation facts;
- one interaction owner per permission or question with pending, resolving
  private attempt correlation, acceptance, and terminal state;
- portable notification order, delivery acknowledgment, and resolution;
- per-aggregate contiguous high-water and bounded overlap/dedup identity;
- setup, native deletion, event cleanup, knowledge cleanup, and other precise
  uncompleted-effect obligations.

Native session, message, part, permission, question, aggregate, provider, and
tool ids may appear only where required by those private records. They never
enter root portable DTOs, public errors, app coordination records, or generic
shared hierarchy state. Portable title and complete defaults remain in the
portable Conversation record and are not duplicated from native state.

The OpenCode root attachment uses version 2. Version 1 snapshots are rejected
before native runtime initialization or recovery writes; there is no migration,
serde default, or inferred native directory. In particular, a version 1 Creating
correlation does not establish which canonical native directory it owned.
Cleanup records retain their independent version 1 format.

A private workspace routing record binds the existing Agent/Workspace ids and
portable revision to one verified native directory. The portable Workspace
record alone retains caller cwd and roots. Workspace creation obtains bounded
native `/path` evidence before its transaction and commits both records
atomically. The routing record survives catalog invalidation and removal of the
last conversation. Descriptive/default updates preserve its native identity;
an explicit, supported cwd change replaces it with newly observed identity in
the same workspace-update transaction. Workspace deletion removes it atomically
while retained cleanup records independently preserve their original directory.
Missing, foreign, stale, or contradictory private routing evidence fails closed.

## Keys, Encoding, And Transactions

OpenCode owns its private key construction and typed versioned record
decoding. Keys are derived from stable SDK ownership plus the minimum native
identity needed for reconciliation. No generic repository, whole-state blob,
agent-owned batch API, CAS compatibility shim, backend-label dispatcher, or
raw JSON record family is introduced.

The prompt-state owner retains its storage and notification dependencies.
Registration returns an owned generation bound once to conversation and
generation identity; exact prompt start, structured-parent proof/claim, abort,
terminalization, and recovered commit are receiver methods on that generation.
Prompt and continuation records derive and validate their own durable keys,
while inventory scans and notification reconciliation remain prompt-state
process behavior rather than passive-record behavior.

Every mutation uses the live StateStore transaction surface. The owning method
loads current records, validates ownership, revision, lifecycle, generation,
and legal transition in the same transaction, writes only the affected typed
records, records cleanup obligations atomically with the portable transition
that creates them, and commits once. Failed conditional transitions roll back.

No StateStore transaction or hierarchy synchronization guard crosses HTTP,
SSE, Database, provider, tool, process, cancellation, receiver, or timer waits.
Non-idempotent native work is bracketed by short durable intent/acceptance
transactions rather than performed inside one.

## Sequencing Ownership

StateStore methods are typed mutation primitives; they do not decide when a
prompt, interaction, notification, recovery, or cleanup transition occurs.
Conversation runtime owns cross-domain sequencing after domain locks are
released:

- prompt admission records the current generation before foreground dispatch;
- terminal response, cancellation, ambiguity, waiter delivery/discard, and
  slot release update only the matching generation;
- notification insertion and stable ordering commit before receiver return;
- interaction resolution records Resolving before HTTP and acceptance or
  rollback afterward;
- event application, semantic dedup, and contiguous high-water advancement
  commit as one outcome;
- native inventory/history verification clears cleanup obligations only after
  both selected absence proofs succeed.

Workspace creation retains one allocated identity and request across definite
transaction conflicts. Each retry repeats the complete local transaction under
the Agent operation's original lifecycle deadline. An admitted transaction
settles before another attempt; a successful commit remains successful after
deadline expiry. Identity collisions and non-conflict failures are not retried,
including uncertain commit results.

Conversation creation retains its allocated portable identity and private native
correlation while preparing the unbound intent. Only exact StateStore transaction
conflicts from admission, reads, writes, or commit permit repeating that complete
local transaction under the creation operation's original lifecycle deadline.
Every admitted read/write or validation failure explicitly rolls back before
return or retry. A rollback failure supplements the primary failure and prevents
retry; identity collisions, stale workspace revisions, and other logical conflicts
remain terminal. Each retry checks the same deadline before admission, without
cancelling admitted storage work. Exhausting that deadline returns `Timeout` in
`StateWrite`, retaining the last exact conflict and its original phase as bounded
supplementary evidence. Uncertain commit failures are never replayed. A confirmed
intent commit that settles after the same deadline remains authoritative: the
workspace reports `Timeout` in `StateCommit` only after the existing exact
intent-discard transaction settles, before knowledge ingestion or native session
creation. Successful discard permits reopen without an unresolved creation.
Failed discard invalidates the hierarchy and preserves the timeout with a
`Cleanup` supplementary error beneath the existing `Closed`/`Publication` error;
it does not claim a recovered or discarded intent. An unbound intent is not yet
the accepted bound-conversation success described below. The native
session-create request runs only after a timely confirmed intent commit, outside
this retry loop, at most once.

An event mutation makes one transaction attempt and returns a typed conflict
without retrying or creating a deadline. The Agent event owner retains the
already-normalized identity and effect, retries only that transaction attempt
under its existing absolute deadline, and publishes postcommit effects once.
Deadline observation may cancel read-only preparation, HTTP discovery, and
backoff, but never an admitted StateStore mutation. An admitted event or
pending-reconciliation mutation reaches its terminal commit observation and
publishes its required postcommit effect before an elapsed episode deadline is
reported. A non-conflict ambiguous commit retries only read-only exact
observation after transient storage failures; it never replays the mutation or
redelivers its postcommit effect. Simultaneous SSE failure likewise cannot cancel that admitted owner;
the stream failure is retained until mutation settlement completes. Graceful
event-owner stop is observed only between admission episodes; the already-owned
shutdown deadline remains the final abort and join authority.
Startup readiness observation ends at the Agent construction deadline without
dropping the event owner. The runtime retains that owner so failure cleanup can
use its one lifecycle-bounded shutdown deadline to await settlement or finally
abort and join a persistently unobservable admitted mutation and its sole SSE.
Construction setup and catalog reconciliation follow the same boundary: they
check the caller-owned deadline before transaction admission, explicitly
commit or roll back admitted work, reconcile a non-conflict commit failure by
exact durable reads, and only then report an elapsed deadline. A late confirmed
setup commit remains complete reopenable recovery state and cannot authorize
runtime connection or Agent publication.
Reopen prompt reconciliation uses that same construction deadline and owns each
admitted prompt transaction through explicit rollback or terminal commit.
Definite conflicts remain conflicts; every other commit failure is accepted
only after an exact intended prompt record or exact prompt deletion is read.
Required prompt-generation wake and release effects publish after confirmed
commit, and a late confirmed mutation returns `Timeout` before abort recovery or
Agent publication can begin another construction step.
Normal conversation creation also checks its lifecycle deadline before binding
transaction admission and settles an admitted commit by exact durable reads.
After a definite consuming-commit conflict, the existing creation owner may
re-author only the binding transaction under that original deadline. Every
attempt rechecks the same immutable Creating obligation, portable conversation,
workspace revision and previously verified native binding. Native creation,
knowledge ingestion and prompt dispatch are never repeated. Precommit failures,
failed rollback and uncertain commits do not enter this conflict retry; uncertain
commits retain exact-read observation only. Deadline exhaustion reports Timeout
with the last conflict as supplementary evidence and retains Creating recovery
ownership. Reopen remains single-attempt on a definite binding conflict. This is
a bounded robustness measure for conservative storage conflicts, not a promise
that conversation construction cannot fail.
Unlike Agent construction, a confirmed bound conversation is the operation's
accepted success: it proceeds to route ownership and handle publication even
when settlement crosses the deadline, and never enters precommit native cleanup.
Transient exact-read failures retry only through the operation deadline. If
durable truth is still unavailable, `Busy` leaves the creating-or-bound intent
and native session recovery-owned; neither native cleanup nor durable discard
may run. Exact unchanged intent retains the original integration failure, while
mixed durable state is contradictory.
An applied catalog-invalidated effect advances the process-local catalog
generation only after that transaction commits and before an already-elapsed
episode deadline is reported. A remembered duplicate,
ordinary duplicate, or gap publishes no process-local catalog invalidation;
durable event progress is the sole semantic deduplication authority.
Pending-interaction and lost-continuation reconciliation preserve definite
transaction conflicts. After any other commit error, the owner exact-reads the
continuation, notification, and when changed prompt records and accepts only
the complete intended mutation; the complete original mutation is an
integration failure and every mixed or foreign observation is contradictory
state. Confirmed late commits publish the
same receiver wake, prompt-change, continuation-release, and terminal-prompt
effects exactly once before an already-elapsed episode deadline is reported.
The typed result preserves a conflict from every operation in that attempt,
including effect-owned catalog, interaction, and notification operations; no
nested transaction helper may collapse it into an integration failure.
Terminal-prompt cleanup after an accepted continuation has its own bounded local
transaction attempts under that same episode deadline. Missing or nonclearable
prompt reads explicitly roll back. After confirmed absence and rollback,
cleanup may release a retained local reservation only when the current durable
continuation inventory contains the same immutable request and generation in
Accepted or Cancelled state and contains no continuation blocking prompt release.
This handles a committed deletion whose caller disappeared before local release;
it cannot release a newer generation or an unresolved prospective Abort deletion.
A present nonclearable prompt does not authorize this absent-record release.
Read or delete conflicts retry only after confirmed rollback; a failed rollback
retains the primary error and its cleanup diagnostic and forbids another attempt.
Consuming commit conflicts permit fresh local recomputation. Other commit errors enter read-only deletion observation:
only exact absence confirms success, while unchanged or foreign state retains
the original commit failure without another deletion. Confirmed deletion releases
the exact prompt generation before reporting an elapsed deadline. Exhaustion
retains the last typed conflict. An unconfirmed terminal cleanup is a terminal
failure of that event application or pending-reconciliation episode, even when
its normalized error is Integration; automatic history recovery may not replay
the already accepted event or restage that deletion. Simultaneous SSE failure
and elapsed-deadline handling preserve this disposition and its complete error.
An event-ownership admission race remains an event-runtime recovery concern:
it uses same-stream history under the live application deadline and does not
authorize the StateStore to retry or mint time authority.

Backend-private prompt admission is owned by one prompt-state owner containing
the concurrency permits, generation registry, status/change signals, and
reservations. Agent prompt instructions and default timeout are runtime policy
owned by the conversation path; they are neither persistence nor admission
state and are not fields on the StateStore owner.

A bound prompt-generation operation carries its exact conversation and
generation identities together with only the prompt-state, persistence, and
notification owners needed to settle that generation. That operation owns the
complete abort reconciliation: it checks exact in-memory progress, loads and
repairs or removes the matching durable prompt in one fenced transaction,
settles typed continuations, publishes the exact abort outcome, and releases
only after every durable effect is verified. An absent matching prompt is an
explicit reconciliation branch, not a separate identity-heavy helper.
Continuation storage keys are derived from each typed continuation at the
mutation boundary; keys are not paired with records in tuple carriers. A
recovered-prompt commit first validates the bound generation, distinguishes
transaction conflict from integration failure, commits once, and publishes
its change signal only after successful commit.

Protocol HTTP/SSE decoding, event classification, catalog discovery,
knowledge retrieval/rendering, and request-state validation do not write
StateStore directly.

## Prompt Generation And Output

One conversation has at most one current prompt generation. Durable state
distinguishes local admission, possibly or definitely accepted foreground
work, cancellation intent and dispatch outcome, exact terminal response
observation, and recovery requirement.

Only a matching generation may terminalize or release the prompt slot.
Concurrent abort joins the same generation; delayed cancellation cannot target
a later generation. Natural completion, abort, timeout reconciliation, caller
future loss, parent close, and process loss compete through one durable first
terminal transition.

In-memory abort progress retains a prospective Complete or RepairedInvalidState
deletion outcome before invoking consuming commit. It is distinct from the
published Settling and Settled states. A confirmed rejected commit may clear
only its matching prospective attempt; uncertain commit or observer loss keeps
it fenced, including the full observed commit failure when available. Only an
exact absence receipt may promote that intended outcome into Settling, then
Settled after continuation confirmation. A separately owned standalone reopen
retains its single attempt and receipt on its construction stack; it does not
create a prompt reservation merely to run this transition.
Settling means the exact terminal classification is published but required
continuation reconciliation is not yet proven. The reservation becomes
releasable only at Settled, and a contradictory outcome is invalid state.
Observed native prompt activity is likewise typed as Active or Idle; absence
of an observation is not silently treated as Idle.

Each reservation also has one exhaustive runtime state. A live reservation
owns local admission and has cancellation Available or Requested. A detached
reservation retains that local owner while cancellation is Available,
Requested, or held by one exact cancellation owner. A recovery reservation has
no live local owner and its requested cancellation is either unclaimed or held
by one exact recovery owner. Durability and abort-joiner count are orthogonal
facts; contradictory boolean and optional-owner products are not representable.

Prompt state owns every task that can outlive its initiating caller. The task
owner is either Open with its tracked prompt and abort-persistence tasks or
Sealed. Agent shutdown seals it only after the hierarchy quiescence attempt,
observes all task exits under the existing lifecycle deadline, and aborts and
joins any remainder before notification and event teardown. Durable prompt and
abort progress remains recovery authority; task completion alone is never a
synthetic durable outcome. An aborted tracked task closes its live waiter
before registration detaches, while the durable prompt remains available to
recovery. Work that reaches task admission after sealing is rejected, closes
its waiter, and never executes inline outside the tracked owner.

Exact output has one live waiter and no public recovery API. After terminal
commit it is delivered to that waiter, or waiter loss is durably recorded and
the output discarded. Only then, and after notification and cleanup obligations
settle, may terminal output compact and the prompt slot release. Reopen treats
every prior-process waiter as absent and never reconstructs lost output from
native history.

## Event Progress And Recovery

Event sequencing is per aggregate. The first selected native sequence is zero;
the internal no-progress state is not serialized as a fabricated cursor. A
semantic event observed through normal, live-sync, and history forms applies
once. Its portable/private effects, overlap identity, and contiguous
high-water advancement are crash-atomic.

For an active structured prompt, private event application retains bounded
typed User and text-part ownership observations needed to join the eventual
POST Assistant parent id with the exact session, User root shape, part message
owner, and private request-part id. A missed live observation is recovered
through the same bounded `/sync/history` normalization and deduplication path.
The proof is generation-scoped, releases with terminal reconciliation, and
never stores Assistant output or creates a replay token. Plain prompts retain
the exact-message read and need no event-parent proof.

Event-progress encoding version 4 retains the actual normal/live-sync/history
origin required by the selected cross-origin Assistant rule in
[`PROTOCOL.md`](PROTOCOL.md). Older or missing versions/origin evidence are
rejected without migration, inferred defaults, or writes; the existing private
namespace and ownership keys remain unchanged. This does not change other
record versions or the bounded recent-event and encoded-byte limits.

History promotion and its exact retained content commit in the existing event
transaction. The commit receipt is the exact identity retained by the intended
mutation, including stronger history evidence that a later live observation
must preserve. After an ambiguous result, an exact read must match that
retained origin, raw content, and durable coordinates; semantic cross-origin
equivalence alone cannot prove a promotion committed. Unchanged older live
evidence retains the commit failure, while confirmed promotion publishes the
existing postcommit behavior once. Fresh reads, native transaction admission,
recovery deadlines, and generation ownership are unchanged.

A gap does not advance past the missing sequence. Conflicting duplicate ids
are corruption, not harmless replay. Bounded overlap identities compact after
they can no longer suppress a captured duplicate; no historical id set grows
without bound. An event belonging outside all validated native closures seals
the runtime and advances none of the foreign aggregate.

Reopen verifies attachment, native session status, prompt and interaction
state, catalog binding, knowledge integrity, notification backlog, event
progress, and cleanup obligations. It performs at most the bounded
reconciliation allowed by the root state contract. A lost exact foreground
response never becomes success; contradictory facts are repaired to a
non-success terminal state when safe and otherwise return `Busy`,
`InvalidState`, `Conflict`, or `Integration` honestly.

An ambiguous session creation is reconciled only through the durable private
metadata correlation, bounded-complete global inventory, and an exact session
read. It is never replayed or matched by caller title, cwd, timestamps,
inventory order, or active status. The correlation is removed only after the
native binding or its cleanup obligation is durably established. A verified
valid session remains `Creating` after a failed binding transaction so reopen
can bind it. An invalid recovered session instead changes exact matching
conversation and unbound-binding records into one existing cleanup obligation,
including the knowledge-forget obligation, in a single transaction. Native
deletion begins only after the exact handoff is confirmed. Exact unchanged
truth retains `Creating`; exact applied truth retains cleanup. Still-unobservable
truth returns recovery-required, revokes local authority, performs no deletion,
and leaves whichever atomic durable state committed for reopen without creating
a second authority. A confirmed handoff during reopen settles
the staged knowledge forget and native cleanup, verifies absence, finishes the
cleanup record, and continues that same construction under its original
deadline. It does not require a second reopen. Every definite conflict from
the handoff's transaction begin, exact loads, knowledge staging, deletes, put,
or commit remains `Conflict`.

## Workspace Delete And Terminal Close

Workspace deletion atomically removes portable workspace/conversation
inventory and stages every existing private knowledge and native-subtree
cleanup obligation in the same committed transaction. Network and Database
cleanup runs afterward. Failure cannot resurrect portable inventory or turn a
committed delete into an error. A non-conflict commit failure is reconciled by
exact reads of the deleted workspace key: the admitted owner always awaits an
exact read, retries only a transient read failure while its original operation
deadline remains, and never replays the mutation. Confirmed absence publishes
the same notification invalidation, hierarchy invalidation, and cleanup
postcommit effects once, while a surviving record retains the original
integration failure. A definite transaction conflict is never relabeled by
observation.

Terminal conversation close commits its nonreopenable lifecycle and exact
native/knowledge cleanup obligations. Its application episode owns one
deadline through bounded FIFO event-command capacity, event-owner
synchronization, and transaction admission. Capacity pressure expires as
`Timeout`, not `Busy`; no StateStore transaction is held while synchronization
performs native or event I/O. After admission, the transaction owner reaches
explicit rollback or commit without outer cancellation. A definite transaction
conflict remains `Conflict`.
Every other commit error starts one distinct receiver-owned
terminal-reconciliation episode. That episode creates one absolute
lifecycle-bounded deadline when exact observation begins and may
only read conversation, binding, cleanup, continuation, and notification truth
and publish required postcommit effects; it cannot readmit or replay the close
mutation or extend its application authority. Transient read failures reuse
that reconciliation deadline. Exact confirmed close publishes notification
delivery closure and the hierarchy terminal marker once and returns success
even after the application deadline. Exact unchanged truth retains the original
error. Contradictory or still-unobservable truth retains the original error
while invalidating local delivery and hierarchy ownership so durable reopen is
the only recovery path.
Parent Agent shutdown or Workspace close instead preserves
quiescent durable descendants and unacknowledged backlog for typed-id reopen.
Notification owner reclamation follows
[`../NOTIFICATIONS.md`](../NOTIFICATIONS.md).

An OpenCode deletion response is not sufficient evidence: native session
inventory and sync-history aggregates must both prove every recorded root and
descendant absent before the obligation clears.

## Required Invariants

- StateStore is mandatory control-plane persistence; Database is separate
  managed knowledge.
- Durable state models current ownership and recovery, not optional history.
- Native facts remain in concrete OpenCode records; shared core stays
  backend-blind.
- Non-idempotent native acceptance is recorded and never blindly replayed.
- Every cursor, binding, interaction, generation, and cleanup transition is
  ownership-checked and typed.
- Prompt policy is conversation runtime state; prompt admission and abort
  progress are prompt-state ownership; event commands are event ownership.
- No storage or synchronization critical section crosses unrelated async work.
