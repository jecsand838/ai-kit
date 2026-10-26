# Conversation Notifications

This document owns the portable stream, delivery, backpressure, interactions,
and receiver shutdown; exact public surface comes only from [`API.md`](API.md).

## Portable Shape

`ConversationNotification` contains opaque `NotificationId`, the SDK
durable-insertion `SystemTime`, and one `ConversationNotificationKind`.
Actionability is derived from kind; the
envelope has no response flag, backend/producer label, route, provider/tool
payload, raw protocol value, recovery marker, or metadata map.

The compact kind enum is:

- `Message { text: String }`
- `FileChanged { path: PathBuf, summary: Option<String> }`
- `Process { label: String, state: ProcessState }`
- `Progress { label: String, detail: Option<String> }`
- `Warning { message: String }`
- `Error { message: String }`
- `Interaction { title: String, body: String, response_schema: Option<serde_json::Value>, dismissible: bool }`

Message is complete text, not a public started/delta/completed family.
FileChanged identifies a path to refresh, not native watcher/diff facts.
`ProcessState` is exactly `Running` or
`Exited { exit_code: Option<i32> }`. Process identifies a caller-visible
backend-host process lifecycle without exposing native process ids, command
lines, environment, routes, or backend-specific state. A process execution
failure may additionally produce one Error fact when it is asynchronous.
Progress is advisory and may be coalesced. Warning, Error, and Interaction are
never silently dropped.

`PromptResponse` does not contain notifications. Ordinary prompt returns its
terminal result; streaming prompt observes transient OutputDelta values and
the same authoritative terminal result through PromptStream. Neither produces
durable token notifications. Independently meaningful passive events and
actionable interactions use this receiver.
Prompt-returned failures are not duplicated as Error notifications. Error
notifications report asynchronous failures unrelated to that prompt result.

## Interaction

Approval, backend question, tool input, form input, and MCP elicitation all
use Interaction and `NotificationResolution`. Producer identity creates no
public subtype. Provider and tool failures remain on the foreground error path
unless they generated one admitted actionable request.

Codex, OpenCode, and Custom implement this same portable interaction and
delivery contract. Codex JSON-RPC requests, OpenCode permission/question
routes, and Custom tool continuations differ only behind their typed backend
request owners. A backend may not omit notifications or requests merely
because its ingress is embedded rather than wire-based.

No response schema means approval. A schema requests JSON content and is a
self-contained JSON Schema Draft 2020-12 document; external or remote
references are invalid; local references stay within that document. The schema
is validated before durable publication of the notification. Malformed known
wire schemas are `Protocol`; malformed SDK-owned integration schemas fail the
originating operation as `Integration`. Text uses a string schema and forms use
an object schema. Sensitive fields use standard schema annotations instead of
widget DTOs. Values for `writeOnly` or otherwise secret response fields are
never persisted in cleartext. A crash before proven native acceptance
re-prompts only when the native continuation is safely reconstructable;
otherwise the interaction is terminally cancelled. Proven or ambiguous
acceptance persists only nonsecret acceptance state and never resends the
response.

Resolution is exactly `Allow`, `Deny { reason: String }`,
`Submit { content: serde_json::Value }`, or `Dismiss { reason: String }`.
Allow/Deny require approval; Submit is schema-checked; Dismiss requires
`dismissible`. Reasons are nonempty, and wrong decision kinds are
`InvalidInput`. Submitted content that fails the published schema is also
`InvalidInput`. Allow always maps to the least-privilege one-shot native
acceptance; persistent/session-wide native choices are not exposed.

There is one private interaction state per notification:
`Pending -> Resolving { attempt, acceptance } -> Terminal(outcome)`. Each
resolution creates one bounded typed private attempt correlation before the
intent transaction. Delivery is a view of that owner, not a second lifecycle.
The attempt, native continuation, and acceptance evidence stay private.

## Exclusive At-Least-Once Delivery

Each live conversation permits one exclusive receiver. A second acquisition is
`Busy`. While the conversation remains reopenable, dropping it releases the
process-local lease but acknowledges nothing. There is no polling, cursor,
topic, producer selector, or raw backend stream.

Durable journal mutation and process-local delivery ownership are separate.
The journal owns StateStore reads, transactions, and exact commit observation;
the delivery owner owns the exclusive lease, wake state, and tracked terminal
reclamation task. Receiver progress is either ready at one delivery cursor or
reconciling one exact receipt at that cursor. Receipt presence is therefore
not an independent optional flag, and a terminal task owns its handle and one
exhaustive exit result through join.

Read-only delivery views settle their transaction without creating a mutation
receipt. An acknowledgment receipt proves removal of its exact prefix and
preservation of the retained notifications' immutable identities, order,
timestamps, and payloads. Later appends and valid interaction/lifecycle
advancement do not contradict that receipt. Each appended item must account
for exactly one consecutive sequence increment. Failed transaction rollback
stops receipt retries and preserves the primary conflict with cleanup evidence.
Confirmation then reads current
delivery state rather than returning a saved waiting state. Missing journals
confirm only the exact terminal deletion receipt. The separate resolution
intent and settlement authorities below keep their exact attempt checks.

The supplied public receiver retains its pending inner receive future when a
caller cancels one `recv` observation. Its admitted commit and registered wake
remain owned until receiving resumes or the receiver itself is dropped.

`recv` returns `Some(Ok(item))`, `Some(Err(error))`, or `None`. `Timeout` and
`Integration` leave the receiver and backlog unchanged so a later call may
retry. `Conflict`, `Closed`, `Unsupported`, `NotYetImplemented`, `Protocol`,
or `InvalidState` are returned once and terminate that receiver instance; its
next call returns `None`. Termination releases the process-local receiver lease
without acknowledgment and leaves durable state for owning reconciliation
before another receiver can be acquired. Normal delivery returns `None` only
after delivery closes and committed final items drain. Cancellation while
waiting cannot discard durable state.

Delivery is ordered and at-least-once per conversation. Id and order are
durable before return. Receiver drop, crash, or ambiguous acknowledgment may
duplicate an item with the same id while its conversation remains reopenable.

The next `recv` acknowledges the prior passive item before waiting. Drop/crash
before that commit replays it. An unresolved Interaction blocks later items;
successful `resolve_notification` acknowledges it. Receiver advancement alone
cannot acknowledge an Interaction.

Acknowledged passive items are compacted and never replayed. Durable
unacknowledged items remain until acknowledgment or explicit owning-record
deletion; there is no capacity- or time-based eviction.

All backend/provider/tool/lifecycle producers enter the same conversation
order. They do not own independent public channels.

## Resolution Boundary

Resolution verifies ownership, pending state, kind, and content, then durably
enters resolving before the non-idempotent native response outside storage and
synchronization critical sections.

Every intent-commit outcome converges on an exact read through the retained
exclusive access to the same real StateStore; the commit result alone neither
authorizes nor rejects continuation. Only the same notification in unconfirmed
Resolving state with the same attempt correlation proves that this attempt may
continue. Every other observation fails before native response dispatch.
Intent-owned settlement requires that same attempt correlation; authoritative
native reconciliation remains a separate typed recovery transition.
The backend operation samples one absolute lifecycle deadline before intent
persistence. Definite transaction conflicts may replay only that mutation with
the same attempt. After any non-conflict commit result, transient exact-read
conflict or unavailability retries only the read under that deadline and never
replays the mutation. Exact confirmation remains successful when its admitted
read completes after the deadline. If observation cannot settle by the
deadline, durable resolving state remains recovery-owned and no native response
is dispatched with a new attempt.
Real strict-live proof exercises this shared exact-read continuation path;
static contracts prove only result-path convergence and attempt guards.

Proven pre-acceptance failure returns to pending. Known or ambiguous acceptance
is never blindly resent; recovery-required state blocks later delivery and
returns `Conflict` until reconciled. `InvalidState` is reserved for
contradictory durable/runtime facts; `Integration` is a definite dependency or
reconciliation execution failure that does not claim acceptance was
unambiguous.

Success requires native acceptance plus durable terminalization. Duplicate,
stale, wrong-kind, foreign, and terminal calls fail without native behavior.
Exactly-once continuation is an internal invariant, not an exactly-once
network claim.

Resolution, cancellation, and close races use the first durable terminal
transition. Close cancels a pending interaction but does not relabel an
already resolving interaction; its acceptance evidence decides recovery.

## Backpressure And Close

Durable state is backlog authority; bounded in-memory wake/backpressure state only wakes delivery.
Essential terminal, warning, error, file, and interaction facts are persisted
or fail their producer honestly. Progress may coalesce before publication but
never after its id is returned or across interaction/terminal boundaries.

Storage-capacity failure pauses delivery and surfaces as `Integration`; it
never drops an item. The durable store is the queue, while bounded in-memory
state only wakes a receiver and applies backpressure.

A slow receiver creates neither unbounded tasks/memory nor waits inside a
storage transaction. Protocol readers never execute caller resolutions.

Conversation close stops publication, cancels pending interactions where
possible, commits final items, and closes delivery without waiting on caller
receipt. A receiver held at close becomes drain-only, receives the committed
backlog, then returns `None`; reaching `None` deletes the terminal owner and
its drained state. Dropping that drain-only receiver declares the terminal
consumer abandoned and deletes the owner plus its unreachable remainder. If no
receiver is held when close commits, the terminal owner and unreachable
backlog are deleted in the close cleanup path. No receiver can be acquired
after terminal close.

Terminal close is the explicit owning-record-deletion exception to durable
retention; deleting its backlog is not an acknowledgment. The at-least-once
replay promise applies while the owning conversation remains reopenable or a
terminal drain receiver remains held.

This terminal deletion rule does not apply to parent cascade. Agent shutdown
or workspace close preserves the quiescent conversation and durable backlog
for typed-id reopen. Parent cascade invalidates and releases an already-held
receiver without acknowledging it; the receiver ends, and a receiver acquired
after reopen replays the durable backlog.

Lost continuations are durably cancelled/interrupted, never presented again as
resolvable.

## Typed Mapping

Known backend events decode to typed backend enums before mapping. Portable
facts map to the kinds above; state-only facts update private owners;
impossible behavior is `Unsupported`; adapter gaps are `NotYetImplemented`;
malformed known input is `Protocol`.

Unknown future input may be retained privately. It cannot become a raw public
kind, acquire an invented conversation, or silently count as success.
