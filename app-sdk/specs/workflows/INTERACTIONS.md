# Interaction, unknown effects and cancellation

`RunSnapshot.interactions` is the canonical collection of actionable truth.
RunStatus describes lifecycle and never hides simultaneous sibling controls.
Input, exact Agent notifications, Unknown effects and transient-input rebinding
share one Interaction vocabulary; timers/retry/child waits stay private.
Exact signatures and passive payloads live in ../SPECIFICATIONS.md.

## Independent target fencing

Each target exposes run-scoped sequence and its own generation. Resolution
fences sequence, target identity, reservation generation, current open
membership and prior settlement evidence. It does **not** require the latest
whole-run checkpoint: resolving sibling A cannot invalidate surviving sibling
B. Cancellation and global state/admission changes still use the checkpoint
fence. Generation changes only when the target's settlement authority changes.
A stale/conflicting answer mutates nothing; an exact duplicate replays its
retained applied outcome without repeating a lower decision.

Input schema validation precedes staging. Agent notifications carry the exact
portable ConversationNotification and settle via NotificationResolution on the
associated Conversation. Acquire the exclusive notification receiver before
prompt submission. Durable at-least-once delivery never becomes protocol-envelope
cloning, synthetic notification production or backend-specific App logic.

One prompt may yield N1, wait, then N2 and later N3. A new distinct notification
gets a fresh target generation/settlement cell under the same attempt; N1's
Applied state cannot settle N2. Duplicate delivery of the same live target
reattaches; duplicate after settlement uses retained evidence. Reopening between
settlement and delivery preserves target-specific receipt identity. Bound both
pending and settled-target evidence; overflow fails truthfully with retained
accepted ownership rather than dropping an actionable notification.
The aggregate retained settlement bound is 4,096 target-generation rows per
run, sufficient for a complete maximum-size Input frontier. Admission charges
both retained rows and the replacement Unknown rows that already accepted
lower work may require. This prospective charge is derived from the existing
controls and settlement evidence. Initial Attempt admission, notification
reservation and explicit retry must leave that capacity available before new
lower work begins; publishing the reserved replacement consumes its existing
charge. Input and transient Inputs binding do not reserve lower-effect rows.

The same prospective check reserves encoded bytes for the exact possible
Unknown evidence, including separators, changed control metadata and maximum
integer widths. Unrelated sibling publication cannot consume this reserved
space. Both checks use the existing record bound and reject new admission with
a truthful limit failure while preserving already accepted ownership through
cleanup. No settled evidence or live target is evicted to make room, and no
separate durable capacity counter or receipt cache is introduced.

Settlement commits intended evidence before lower resolution, records exact
applied/result/contribution truth after it, removes only its own target, and
writes one state-preserving control checkpoint. Routing/reducers wait for the
ordinary barrier. Resolution cannot directly overwrite committed workflow state.
Input/rebinding values remain transient unless an explicit state target writes
them; reopening unstarted actions may request fresh values without persisting
secret runtime inputs.

Transient `Inputs` binding has execution-scoped replay authority. Its durable
evidence records required keys and the binding generation, never supplied
values or value-derived digests. While the original execution-options owner
retains those values, an identical binding reattaches and a changed binding
conflicts. Disposing that owner or reopening revokes the transient generation;
an old binding request then conflicts, and an unstarted action exposes a fresh
`Inputs` target when values are required. App cannot attest equality of values
it deliberately did not retain. This App-native rebinding rule does not narrow
immutable applied replay for Input, Notification or Unknown settlement, whose
explicit decisions have durable evidence.

Notification reservation retains the exact delivered portable interaction
identity, insertion timestamp and Interaction fields in its existing settlement
evidence. These fields are immutable and obey the record byte bound. Historical
receipt projection reconstructs that same observed value through the public
validated notification constructor; it does not manufacture a new delivery or
look up content from a later live attempt. Live resolution still invokes only
the original lower owner. This permits exact applied-snapshot replay after a
sibling notification has advanced or the App has reopened without a parallel
notification cache or new lower-crate serialization API.

Until lower recovery delivers the exact Pending notification, resolution returns
Busy before admitting resolution work. Readiness captures the original attempt
and validates the delivered target; accepted resolution still owns its deadline,
native settlement and generation fences before invoking the lower decision.

## Lost caller and lower receipts

Loss of a caller response after the App settlement committed replays the exact
applied result. Loss of lower resolution receipt retains reservation, capacity,
notification correlation and reconciliation authority. Only authoritative public
lower evidence can prove acceptance/completion; a timeout, error string,
redelivery or disappearance alone is insufficient. If uncertainty remains,
replace that target's actionable projection with Unknown without losing its
identity or inventing another notification. Other siblings remain actionable.

Reconciliation must not assume every lower backend offers terminal prompt
history. Public info metadata is not a prompt result. If the portable contract
cannot establish truth, preserve Unknown and require explicit caller settlement.

## Unknown effects

Unknown represents a possibly accepted operation with no authoritative terminal
truth for which App owns recorded attempt or publication evidence. Conversation
creation, prompt, notification resolution, abort and App observation persistence
retain that private evidence; the public projection exposes only sequence,
generation and evidence-based retry_safe. No generic timeout implies retry
safety.

After a Conversation prompt has been dispatched, `Busy`, `InvalidConfig`,
`InvalidInput`, `Unsupported` and `NotYetImplemented` do not prove rejection
before acceptance. This applies to unary prompt errors and errors returned
while opening a stream before a stream handle exists. App preserves those
outcomes as Unknown and never automatically replays them. Failures in App's
workspace/conversation preparation before prompt dispatch may still use the
existing proven-safe retry policy.

A caller Action source's explicit terminal error is authoritative failure of
that source, even when the error is AppError::Integration. EOF is successful
no-output rollback. Neither source polling nor knowledge that a caller may
perform I/O creates an App-owned Unknown attempt. The pinned
`application/streaming/step_core.rs:157–167,358–365` distinguishes these source
outcomes without an external-effect classification. A caller owns its internal
I/O, independently spawned tasks, idempotency and external reconciliation; App
cannot infer acceptance or undo those effects by rolling back workflow state.
Source failure, panic, timeout, abort or drop never authorizes automatic replay.
Separately recorded App-owned lower-operation or admitted-storage uncertainty
survives those outcomes until exact evidence or caller settlement resolves it.

The same resolve operation accepts Accept(state contribution), Fail or Retry.
Accept uses ordinary validation/reducers/barrier rules; Fail records safe caller
settlement without asserting external outcome. For a Prepared run, Input,
Accept and Fail commit their control settlement but require a newly acquired
explicit execution to advance the ordinary complete barrier; reading or waiting
does not advance staged state, results or failure. Retry is legal only when exact
evidence proves the prior effect not live and retry_safe is true, and consumes
the same retry count/deadline policy as automatic proven-safe retries. Wrong
resolution variants conflict. Same payload replay is idempotent, changed replay
conflicts. Unknown effects never automatically redispatch.

## Complete deadline and abort ownership

One sampled activation deadline covers workspace/conversation lookup/creation,
notification subscription, prompt or stream opening, prompt execution,
resolution, abort and recovery. Setup and retries never restart the budget.
After a lower recovery operation fails, the finite current-authority read remains
interruptible by that original deadline and control change throughout its retries.
Abort reconciliation observes at most the remaining activation budget capped
at one second; if the activation has already expired, one separately bounded
one-second cleanup grace permits the exact abort/recovery operation. Expiry
preserves Unknown rather than claiming abort acceptance. This bounds workspace/
conversation reacquisition and abort together, not each leg independently. A caller wait timeout affects observation only; accepted
mutation/cleanup authority remains supervised.

Resolution and abort can be simultaneously reachable and retain independent
target-specific cells on one attempt. Abort commits intent, invokes the exact
public operation, reconciles authoritative evidence, and retains uncertainty
when evidence is incomplete. A stale generation never commits. Cancellation
cannot silently discard a notification or staged sibling whose accepted effect
has not been accounted for. Cleanup failures preserve the primary failure.

## Cancellation

Cancellation is explicit, monotonic and checkpoint-fenced. First cancellation
of an already successful/failed run without retained intent conflicts. An exact
retained cancellation is idempotent after its terminal result; conflicting
requests mutate nothing. Committing intent stops new work and propagates to
children in canonical order, then observes every accepted effect/descendant.
If cancellation wins before a complete wave's state publication, it discards
that wave's staged writes/results and retains the last committed state,
action position and outcomes. It does not run the wave reducer as a cancellation
operation. Settling an accepted external effect is still required; it does not
authorize publishing a partial wave or undo that external effect.

Terminal Cancelled requires no new dispatch, drained or truthfully settled
accepted work, reconciled reservations and exactly-once capacity release.
An observer waits while recorded abort intent or definite abort settlement is
still being reconciled. An uncertain abort outcome remains actionable Unknown
and may keep Cancelling nonterminal. Observer/WorkflowRun drop does not
cancel. Dropping an execution-owning stream only revokes its uncommitted step
and later admission, following WORKFLOW.md's commit-admission distinction;
it cannot undo external effects or force native storage rollback.

## Bounded ownership and proof

Every control/receiver/queue/task is bounded and tracked from durable admission
through settlement, reconstructed on reopen and counted by shutdown. Wakeup
channels are hints; StateStore truth is authoritative. No lower call/timer/
channel/capacity wait occurs under a synchronous lock or live transaction.
Required tests cover N1→N2→N3, simultaneous input/agent/Unknown siblings,
duplicate/conflicting/stale answers, lost caller/lower receipts, deadline at
each public operation, cancellation races and real sequential approvals.

An accepted prompt error, including a streamed error, remains buffered while
an original notification resolution receipt is pending. Neither its category
nor its arrival after a newer notification can replace that receipt. The
original stopping-cancellation settlement authority is retained. After receipt
settlement, the normal terminal/Unknown classification applies without an
automatic replay of dispatched lower work.
