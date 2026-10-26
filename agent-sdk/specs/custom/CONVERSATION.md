# Custom Conversation

This document owns the provider-backed Custom prompt lifecycle.

## Foreground execution

`Conversation::prompt` admits one foreground generation. Concurrent prompt
admission is `Busy`; there is no public background execution handle.

After admission, Custom:

1. resolves the effective `ModelSelector`, reasoning, instructions, structured
   output, knowledge policy, and `ToolSelection`;
2. selects the exact `P::Model` from the retained `P: ProviderBackend`;
3. loads the bounded portable transcript and private managed-knowledge context;
4. materializes that transcript as `Message<P::Model::Continuation>` values
   without continuation and constructs one typed `GenerateRequest`;
5. calls `ModelBackend::generate` for terminal delivery or consumes actual
   `generate_stream` events for incremental observation inside the same loop;
6. validates the returned finish reason and function-call shape before
   constructing handlers or advancing the transcript, then executes accepted
   `FunctionCall` values through the selected `Toolset -> ToolHandler` routes,
   bounded by `CustomLoopLimits`;
7. appends the returned assistant `Message<P::Model::Continuation>` unchanged
   plus one validated Tool message containing the ordered `FunctionResult`
   values and repeats until a final assistant response;
8. durably commits the caller/final-assistant turn and terminal waiter state.

The Custom loop owns transcript advancement, parallel action bounds, durable
approval notifications, final-response detection, cancellation, and the
existing lifecycle deadline. Provider-sdk owns only provider/model rendering
and normalized call/result correlation.

Explicit reasoning selections map exactly from `minimal`, `low`, `medium`,
`high`, and `maximum` to Provider's corresponding portable effort values.
Other strings, including native-only names such as `none`, `xhigh`, `max`, and
`ultra`, return `Unsupported` before generation. Omission leaves Provider
reasoning configuration absent; a prior prompt's override does not persist.
The selected Provider Model owns the model-specific interpretation and may
reject a portable effort that it cannot honor.

Every complete model response, whether returned by `generate` or carried by
streamed `Completed`, follows this classification:

| Finish reason and returned calls | Custom outcome |
|---|---|
| `Stop`, no function calls | Validate the terminal text/structured result and any required-tool obligation, then permit prompt success. |
| `ToolCall`, one or more function calls | Validate and execute the selected calls, preserve the assistant message and continuation, and continue the loop. |
| `Stop` with calls, or `ToolCall` without calls | `Protocol`; the completion contradicts its call shape. |
| `Error`, `Length`, or `ContentFilter` | `Integration`; failed, truncated, or filtered output is not a completed prompt. |
| `Paused` | `NotYetImplemented`; Custom does not yet resume a provider-managed paused turn. |
| Absent finish reason | `Protocol`; a completed response must classify its completion. |

Rejected completions never construct or execute a ToolHandler, advance the
prompt transcript, or commit a successful durable caller/assistant turn.
Already observed streaming deltas remain observations and cannot replace the
failed terminal result. Diagnostics carry safe classification, context, normalized causes and supplementary
failures, never
provider payloads, partial generated text, tool output, or private identities.

Completed ToolHandler results preserve actual content and `is_error`. A result
with no content and no structured content is malformed integration output and
returns `Integration`; Custom never manufactures success or failure text.
Structured content alone is valid, including a present JSON `null`, and is
converted through the existing normalized function-result path. A valid
model-visible tool failure remains an `is_error = true` function result that
the next model turn can consume.

`ToolSelection` applies to the complete effective catalog. Named selections
must resolve exactly. `Required` needs at least one selected executable tool.
Every returned function-call batch is checked against that effective selection
and the installed catalog before any approval, handler creation, or execution
from the batch. An unknown or excluded call rejects the complete batch as
`Protocol` in `CompletionValidation` with `InvalidData`; no partial batch is
executed. This validation applies to every model turn and to both unary and
streamed completed responses. Prior accepted tool effects are not replayed,
and a failed prompt contributes no successful conversation history.
A selected model may reject an otherwise valid tool or structured output
request with the provider's unchanged `Unsupported` error. Custom additionally
requires structured-output and function-tool schemas to have a direct object
root before provider I/O; Codex and OpenCode retain their own broader native
schema contracts.

Usage aggregates complete input/output counters across every model turn. If
any turn lacks a complete counter, portable aggregate usage is absent.

Streaming forwards only ContentDelta text; Started, thinking, native data and
tool fragments cannot become assistant text. The authoritative Completed
GenerateResponse supplies each model turn's exact message and continuation to
the existing function loop. Stream EOF without that response is Protocol, and
no model turn or tool continuation is fabricated from partial text. The public
PromptStream publishes `Completed` only after the Custom prompt's terminal
durable commit.
The existing generation owner retains bounded observation state; dropping or
overflowing observation cannot abort accepted provider/tool work. Incremental
observation does not install a foreground response waiter: the existing
terminal waiter is settled as discarded before `Completed` is published.
Cleanup never waits for the observer to read its queued terminal;
ordinary prompt keeps its existing response acknowledgment semantics.
Abort joins durable terminalization independently of that acknowledgement.
It may return while the unary reply remains unread; this does not mark the
reply delivered or release its waiter. Dropping the reply still records waiter
loss, and reading it retains the existing acknowledgement path. An abort that
captures work before its terminal transition observes that transition even if
a later prompt starts before the abort caller resumes. Existing reconciliation
of an already-recorded waiter settlement remains required.
Failed waiter settlement ends stream observation with its error, while an
earlier execution error remains primary. The request deadline remains active
through waiter settlement. Expiry ends observation with that earlier error or
`Timeout`, while the same prompt task retains any admitted storage operation
through its original receipt. A late successful settlement cannot replace the
stream's terminal error; deadline expiry wins when settlement is ready at the
same observation boundary. The existing conversation fence retains
the exact generation and waiter disposition for bounded reconciliation by a
subsequent prompt, abort, or close; a failed observer does not erase that cleanup
obligation. Agent-only shutdown may retain this already-reported terminal
waiter failure as durable pending evidence after joining the old task. Fresh
reopen discards that pending waiter before admission, retains committed
provider-neutral history, and never replays its model turn or old observer.
The provider stream drain cooperates
with the executor even when every source event is immediately ready, so output
overflow and ignored thinking events cannot starve deadline or abort progress.

## Cancellation and durability

Dropping the caller future does not cancel admitted work. Timeout or abort
records cancellation intent, signals only its captured generation, and retains
admission until terminal reconciliation. After recording intent, abort checks
the generation fence and dispatches cancellation synchronously while that exact
generation remains active. If it has advanced toward its terminal or a successor
has started, abort only joins the captured terminal notification. The captured
prompt owner settles its own interactions before durable terminalization; an
abort observer cannot cancel a successor's interactions. A cancellation-intent
conflict is reconciled only when the captured terminal notification positively
proves durable completion. A closed notification source is not that proof, and
non-conflict failures remain errors.

Once Custom acquires a tool handler, an interaction validation, publication, or
wait failure cancels that handler exactly once before returning the original
error. A cancellation failure is supplementary `Cleanup` evidence and cannot
replace the primary error.

Definite storage conflicts may be
retried only within the existing lifecycle deadline. An ambiguous commit may
also retry transient exact-read unavailability without replaying its mutation;
other non-conflict failures surface unchanged. A durable interaction
publication retains one identity, kind, insertion time, and absolute deadline
across those retries. A confirmed publication remains successful even when
confirmation completes after its deadline.
Resolution-intent persistence likewise retains one private attempt and one
absolute lifecycle deadline. A non-conflict commit enters read-only exact
observation; it cannot dispatch the retained handler response until the same
attempt is confirmed. An unconfirmed deadline outcome stays recovery-owned and
cannot create another attempt or handler dispatch.

Only the bounded caller text and final assistant text are durable public
history. Provider continuation metadata and intermediate function messages
remain typed run-local state for that one prompt and are discarded after its
terminal result. Reopen reconstructs plain messages without continuation.
Custom never trims an oversized turn or reports success without durable
continuity.

## Notifications and files

`notification_stream` and `resolve_notification` are the only public
interaction path. Tool approvals and source-neutral tool interactions enter
that durable owner; prompt terminal results remain on the prompt return path.

File reads use the current committed workspace context and reject absolute
paths, traversal, stale revisions, and containment escapes. Returned
`FileRead` preserves exact bytes and accepted relative spelling.

## Close

Conversation close is consuming and terminal. It seals admission, cancels or
reconciles admitted work, settles pending interactions, removes the live
route, commits terminal state, and deletes the durable conversation according
to the receiver-drain rules. No lock or StateStore transaction crosses a
provider, tool, database, notification, process, or timer await.

The begin, runtime-settlement, and final-deletion phases each sample their
existing lifecycle budget once. A failed phase is never restarted or given a
fresh budget. A settled dependency timeout remains that phase's failure; only
an exact transaction conflict permits the existing bounded conflict retry.

The caller's close observation has its own original deadline. Expiry or caller
loss does not cancel the admitted close task. That task retains the original
result receiver for each admitted state mutation and knowledge cleanup until
settlement, even after its observer expires. A confirmed late commit remains
authoritative; an uncommitted late failure stops its phase without replay.
Later operations within that phase still require admission before its original
deadline. A distinct following phase may sample its own first budget after the
preceding phase succeeds. Staged terminal state remains recoverable if the
remaining cleanup cannot start. A late result does not replace a timeout
already delivered to the caller.

Retained knowledge cleanup admits the complete forget operation, including
Database deletion and its manifest reconciliation, before the original phase
deadline. Once admitted, that operation runs to its exact result. Removing the
separate durable forget obligation requires a subsequent transaction admitted
before that same deadline; once admitted, its load, delete, and commit remain
one retained transaction. If that admission expires after successful knowledge
forget, the obligation remains for recovery rather than starting a late
transaction. Pending-interaction cancellation's state mutation likewise keeps
its original result receiver inside owned close.

## Failure evidence

Complete-response validation uses `Prompt/CompletionValidation`: absent reasons
are MissingData, contradictory shapes InvalidSequence, truncation OutputLimit,
filtering ContentFiltered, and provider-managed pauses ContinuationRequired.
Malformed tool output is Integration/InvalidToolOutput; malformed model-authored
tool arguments are Protocol/InvalidData. Dependency closure/cancellation do not
claim Agent closure or durable cancellation. Provider evidence and native status
are normalized without exposing foreign prose or raw source errors.

State timeouts retain their supplied duration and detecting read/write/commit
phase. Confirmed durable outcomes remain authoritative even when a commit receipt
fails. Before success is proven, retained commit failures and reconciliation
failures remain inspectable. Prompt stream settlement and cancellation cleanup
retain the primary failure and supplementary failures without synthesizing a
second execution outcome.
