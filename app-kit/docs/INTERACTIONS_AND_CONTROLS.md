# Resolve workflow interactions

Treat a run snapshot as committed state plus a collection of independently
actionable targets. Read `RunSnapshot.interactions` for those targets. The run's
lifecycle status answers whether it is running, parked, cancelling or terminal;
it does not replace the interaction collection when several siblings need
attention.

## Drive a caller response loop

Keep one observer loop for the run and route each target to the application
component that can answer it:

1. Call `run.wait()` to obtain an actionable or terminal snapshot. An outer
   caller timeout may bound this observation; it does not cancel the run.
2. Inspect every interaction in that snapshot. Retain its run identity,
   snapshot occurrence, sequence and target generation together with the response
   you intend to submit. If the observer must be reopened, use that exact
   occurrence rather than selecting the latest logical run. Two sibling targets
   can be outstanding at the same time.
3. Validate or collect an answer appropriate to that exact target. An Input
   requests one value; Inputs requests transient action inputs; a Notification
   carries the actual portable Agent notification. Unknown needs an explicit
   decision about an uncertain effect.
4. Call `run.resolve(sequence, generation, resolution)` with the saved target
   generation. Use the returned snapshot to update the displayed committed
   state and outstanding targets. An applied replay returns its original
   snapshot; reload for the current target inventory. Settling one target does
   not invalidate a different sibling's still-current generation.
5. On a conflict, reload the snapshot and reconcile the exact target. Do not
   silently apply an old response to whichever interaction is now displayed.
   Continue until no response is needed or the run reaches its terminal result.

A snapshot may expose a parked execution boundary as well as interactions.
Resolution settles its target; explicit caller-controlled execution still needs
its appropriate continuation. It does not implicitly grant another caller
exclusive execution ownership.

Retain the exact response until its outcome is known. For Input, Notification
and Unknown settlement, repeating the same applied response is idempotent;
changing it conflicts. A lost caller response must not become another lower
approval operation.

Transient `Inputs` replay lasts only while the original execution-options owner
retains the supplied values. Disposing that owner or reopening revokes the old
generation. An unstarted action then requests any missing values through a fresh
`Inputs` target; App does not persist those values or their hashes for replay.

## Choose the matching resolution

The complete [Input definition](examples/input.json) asks for a nonempty string
and explicitly targets `answer`. `Resolution::Input(json!("API design"))`
stages that answer; `json!("")` fails its schema before any staging.

| Interaction | Caller response | State effect |
|---|---|---|
| Input | `Resolution::Input(value)` | Validate the optional schema; stage a write only where the node explicitly declares a state target. |
| Inputs | `Resolution::Inputs(values)` | Rebind transient inputs required by an unstarted action; do not persist them as workflow state. |
| Notification | `Resolution::Notification(answer)` | Resolve the exact associated Conversation notification through the Agent trait. |
| Unknown | `Resolution::Accept(contribution)` | Stage a caller-approved contribution under ordinary state validation and reduction. |
| Unknown | `Resolution::Fail` | Settle the workflow attempt as failed without asserting what happened externally. |
| Unknown with `retry_safe: true` | `Resolution::Retry` | Consume the ordinary retry/deadline policy only after authoritative evidence permits it. |

Submitting the wrong resolution kind conflicts. A schema failure does not
stage part of an Input answer. Resolving a target writes state-preserving
control history; its contribution enters workflow state only at the ordinary
barrier after sibling work is accounted for. Input collection and approval
handling cannot overwrite the committed state behind other siblings.

Use the public Agent notification payload to decide what can be answered.
Do not inspect native protocol envelopes, synthesize replacement notifications,
or assume all notifications accept a boolean. Agent owns protocol adaptation
and the supported `NotificationResolution` vocabulary.

## Handle several notifications in one prompt

One accepted Agent prompt can yield a notification, continue after its answer,
then yield another notification. Each distinct notification has its own target
generation and settlement evidence. The first approval cannot settle the
second, even though both belong to the same prompt attempt.

Duplicate delivery of an open target reattaches to that target. Duplicate
delivery after settlement uses its retained evidence. App acquires the exclusive
notification receiver before submitting the prompt, then preserves correlation
through response loss and reopen. Your response loop should preserve these
identities instead of deduplicating by message text or resetting after the first
approval.

The per-run bound is 4,096 retained target-generation rows. Admission must also
leave room within that count and the 4 MiB encoded-record bound for Unknown
evidence that already accepted lower work may require. New work can therefore
hit a limit before the current rows fill either bound. App retains accepted
ownership through cleanup and evicts neither live targets nor settled evidence.
The [interaction contract](../specs/workflows/INTERACTIONS.md#independent-target-fencing)
owns the prospective accounting rules.

## Distinguish a lost response from an unknown effect

If App committed settlement but your caller lost the response, retrying the
same exact request can return the retained applied result. App does not repeat
the lower decision.

If the lower resolution response is lost, App retains the reservation,
notification correlation and reconciliation authority. Only authoritative
evidence from public lower operations can prove acceptance or completion.
An error string, timeout, notification disappearance or duplicate delivery
alone cannot establish that truth. When the available evidence is insufficient,
that target becomes Unknown while surviving siblings remain actionable.

`retry_safe` reports evidence about the prior operation; it is not a suggestion
based on elapsed time. Unknown work never redispatches automatically. Even a
safe `Resolution::Retry` must be permitted by the Agent node's policy and uses
the original activation deadline, attempt count and elapsed budget. It grants
no policy exception: omitted, null or `never` retry permits no extra attempt.
`Accept` is your explicit settlement decision and
`Fail` is a workflow decision; neither claims an external transaction was
rolled back.

Caller Action streams have a different ownership boundary. Their explicit
errors are source failures, and EOF without a terminal event rolls back the
uncommitted workflow contribution. App does not infer an Unknown external
attempt merely because arbitrary caller code could have performed I/O. The
source owns that I/O's idempotency and reconciliation. See
[caller actions](ACTIONS_AND_INPUTS.md).

## Cancel with the run checkpoint

Cancellation uses `run.cancel(snapshot.checkpoint)`. This checkpoint fence is
different from the target generation used by resolution. Reload after a stale
checkpoint conflict; do not substitute the current checkpoint for an old
interaction generation.

Cancellation commits intent before stopping new dispatch and propagating to
children. If it wins before a wave publishes state, App discards that wave's
staged contributions and retains the previous committed state and outcomes.
It still accounts for accepted external work. Cancellation cannot undo an
Agent's already accepted effect or publish half a sibling wave to make cleanup
appear complete.

Cancelled is terminal only after accepted work, descendants and reservations
are drained or truthfully settled and capacity is released once. Unknown can
therefore keep a run Cancelling. Repeating the retained exact cancellation is
idempotent; initiating cancellation on an already successful or failed run
without such an intent conflicts.

Dropping a run handle or wait observer does not cancel. An execution-owning
stream has separate uncommitted-admission rules and cannot roll back an already
admitted native commit. Keep the runtime driven through explicit App shutdown
and accepted-effect reconciliation; see
[lifecycle and operations](LIFECYCLE_AND_OPERATIONS.md).

## Keep one activation budget

Lower setup, execution, resolution and recovery share the original activation
deadline; retries do not restart it. `max_attempts` includes the first attempt.
A policy's elapsed ceiling includes saved backoff and does not reset on reopen;
App checks it again before dispatch after a delayed wake or recovery. Manual
`Resolution::Retry` consumes this same policy budget.

Automatic repetition requires proof that setup or initial prompt admission
failed before acceptance. Possible acceptance, an accepted stream, a pending
notification or App resolution work prevents automatic replay; an error category
alone is not enough. See the owning
[attempt policy](../specs/workflows/WORKFLOW.md#attempts-retries-children-and-failure).
The [deadline and abort rules](../specs/workflows/INTERACTIONS.md#complete-deadline-and-abort-ownership)
bound cleanup and preserve uncertainty when acceptance is unproven.

These rules belong to the [interaction contract](../specs/workflows/INTERACTIONS.md).
The [public ledger](../specs/SPECIFICATIONS.md) owns the exact DTOs and signatures.
[Runtime interaction tests](../tests/workflow_execution.rs) exercise repeated
delivery, settlement and fault cases; the
[real workflow suite](../tests/workflow_live.rs) exercises configured services.
Their results must identify which simultaneous-target, receipt-loss and
process-recovery cases actually ran. A single approval journey does not prove
every interaction race.
