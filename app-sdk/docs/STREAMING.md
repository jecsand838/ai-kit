# Consume partial results and committed outcomes

Use `Execution::next` when a caller needs incremental output. Each Partial
event contains a node identity and transient result fields. It does not update
workflow state, consume an action ordinal or create committed result history.
The eventual Committed event follows validation and durable publication of
the complete required boundary.

A partial is useful for progress display, but it is not a state patch or a
promise about the final answer. Keep provisional output separate from the
committed snapshot shown elsewhere in the application.

## Drive one lazy source

One Action source serves incremental and terminal consumers. App does no
external source work when execution is merely acquired. A caller Action's
`start` constructs a lazy source; external work begins when that source is
polled. The associated stream remains concrete and can hold local non-Send
values on a caller-local execution lane.

`next` advances to one event. `step` drains partials until one committed
boundary. `finish` drains the remaining execution to a halt, actionable park
or exhaustion. Draining is explicit caller intent; it does not create another
action implementation or prompt loop. In a parallel workflow, publication
still waits for the complete required wave.

The first Action `Completed` event supplies its exact final result and ordered
state writes. App stops polling and drops that source. A later terminal item in
the unconsumed tail cannot add another commit. An empty final result map is a
real completed outcome; EOF is a different event with different semantics.

State changes pass validation and ordinary sibling reduction. Result projection,
when explicitly enabled, uses the rules in
[actions and inputs](ACTIONS_AND_INPUTS.md). A partial never performs that
projection. After an action commits, evaluating its outgoing route can fail
without rolling back its committed result or executing it again.

## Request native Agent streaming explicitly

Set `"stream": true` on an Agent step to use the associated Conversation's
`prompt_stream` method. The default is false and uses ordinary `prompt`.
App never slices a terminal response into pretend chunks or calls both methods
to obtain the same answer.

Each actual lower `OutputDelta` produces a Partial result with one
`output_delta` string. Do not concatenate those deltas to manufacture the final
result: a multi-turn Agent may stream earlier assistant messages that are not
part of the terminal answer.

The actual lower Completed response supplies the final result:

- `output` contains the exact terminal text.
- `structured_output` is present only when the response contains it. A present
  JSON null remains present.
- `usage`, when supplied, contains the exact input and output token counts.

These fields retain that order. Only explicitly configured text and structured
targets write state. An untargeted result can remain available in the action
outcome without becoming workflow state. App does not expose provider wire
payloads, tool events or reasoning as generic output deltas.

Notification observation is acquired before prompt submission. Incremental
output does not replace the independent Input/approval loop. Resolve the exact
targets through [interactions](INTERACTIONS_AND_CONTROLS.md) while the existing
Agent attempt retains prompt, deadline, abort and settlement ownership.

## Bound output without blocking controls

Caller Action streams are polled lazily by their execution owner. Native Agent
output crosses a bounded handoff from the accepted attempt to that owner.
The runtime retains a finite pending item rather than collecting the entire
lower stream when a consumer stops requesting output.

Backpressure must still permit notification, cancellation, deadline and cleanup
progress. A slow consumer does not grant a fresh activation deadline. Internal
output bounds are not a promise that the lower provider itself buffers nothing;
each lower implementation owns its documented resources.

Default tracing never receives partial contents. Opt-in stream-item hooks do
receive them, including when terminal consumption drains the stream. Callback
errors and observation deadlines remain functional. See
[observability](OBSERVABILITY.md) for callback order, content policy and the
shared cleanup budget.

## Handle each kind of interruption

| Boundary | Required outcome |
|---|---|
| Caller Action EOF before Completed | Successful no-output rollback: no new state, result or ordinal. A later explicit execution may try that action again. |
| Caller Action returns an explicit error | Action failure; discard its uncommitted contribution. The AppError variant does not turn it into a generic lower-effect Unknown. |
| Caller source construction, polling or destruction panics | Contained execution failure; preserve an earlier primary failure if destruction also fails. |
| Caller Action's App activation deadline expires | Timeout under the existing once-sampled deadline; admitted publication and separately recorded effects retain cleanup authority. |
| Dispatched Agent stream opening returns Busy, InvalidConfig, InvalidInput, Unsupported or NotYetImplemented | Preserve Unknown; the category and missing stream handle do not prove rejection before effects. Do not automatically replay, invent deltas or invoke a unary fallback. |
| Accepted Agent stream errors or ends before terminal truth | Preserve Unknown until authoritative terminal or explicit settlement evidence exists. Even Unsupported, InvalidInput, Closed or Cancelled item spelling alone does not prove preacceptance rejection. |
| Completed action followed by a post-hook error | Keep the committed result and state; surface the hook error and resume later without replaying that action. |

For an accepted native Agent attempt, deadline expiry retains Unknown when
terminal or abort settlement evidence is incomplete. A timeout alone cannot
turn that recorded uncertainty into an ordinary failed source or safe retry.

EOF from arbitrary caller code does not prove that its internal I/O had no
effect. Workflow-state rollback cannot undo a request that the action already
sent. The caller source owns idempotency, external reconciliation and any tasks
it creates. App-owned Agent attempts and admitted StateStore mutations have
their own recorded acceptance and uncertainty protocols.

## Separate observation loss from owning Drop

Dropping a read-only WorkflowRun handle or wait future does not cancel accepted
work. A lower PromptStream is also an observation handle for its accepted Agent
prompt. The App Execution owner has a stronger role: dropping it revokes
uncommitted advancement and prevents later action admission for that session.

Owning Drop discards uncommitted caller Action staging. Accepted native child
results retain their staged evidence for later explicit collection, while the
parent state, results and action ordinal remain at the previous barrier. The
original child worker stages that evidence before releasing its receipt.
After commit admission, the supervised publication retains authority to reconcile
its exact receipt/head. Losing the consumer cannot force native rollback,
authorize a duplicate commit, or make the accepted effect safe to replay.

App-owned external work is aborted or reconciled through its existing attempt
owner. Uncertainty stays explicit where evidence is incomplete. A cancelled
caller wait is not proof that lower work was cancelled. Keep the runtime driven
while those accepted tasks and receipts are being settled.

Drop invokes only synchronous lifecycle observation and performs no asynchronous
I/O, runtime creation or blocking join. `finish` and `abort` are the explicit
paths for awaited finalization. App shutdown retains ownership of outstanding
admitted work. Caller-local stream values are destroyed on their own lane;
they are not erased or moved across threads to make cleanup appear complete.

Choose halt behavior deliberately when mixing consumers. If `next` yields a
partial and `step` later commits that source, the committing method selects
ordinary halt precedence. A halt already retained at a committed boundary
cannot be reclassified. See [execution controls](EXECUTION_CONTROLS.md).

The [streaming contract](../specs/workflows/WORKFLOW.md),
[caller-source tests](../tests/action_contract.rs),
[native streaming tests](../tests/native_streaming_contract.rs) and
[hook tests](../tests/hook_execution_contract.rs) identify the separate proof
obligations. Actual Agent/process streaming is established only by the relevant
configured real-service tests, not by deterministic deltas or terminal-text
comparison.
