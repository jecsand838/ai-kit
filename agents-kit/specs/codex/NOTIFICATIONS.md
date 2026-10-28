# Codex Notification Domain

This document owns the Codex `notifications/` domain after
[`PROTOCOL.md`](PROTOCOL.md) has directionally decoded and typed an inbound App
Server fact. The portable envelope, kinds, exclusive receiver, interaction,
acknowledgment, replay, and drain semantics remain in
[`../NOTIFICATIONS.md`](../NOTIFICATIONS.md).

This contract assigns behavior, not an exact bus, router, topic, subscriber,
frame, file, enum, or helper topology.

## Sources And Classification

The selected known notification inventory is owned and source-anchored by
[`PROTOCOL.md`](PROTOCOL.md). Every known method decodes into a typed Codex
event and receives exactly one exhaustive semantic classification before any
portable mapping:

- portable notification fact;
- actionable interaction fact;
- private prompt, request, lifecycle, attachment, or cleanup fact;
- valid behavior that cannot be represented portably; or
- a sourced adapter gap.

Malformed payloads for known methods are `Protocol`. A known method never
falls into raw, unknown-future, silent, no-event, generic-map, or wildcard
handling. Unknown future methods may retain complete private method, payload,
and routing context for diagnosis, but cannot become invented portable success
or acquire a guessed Conversation.

Classification is owned once. Protocol decode, notification conversion, and
conversation runtime must not maintain competing switches over the same known
method inventory.

## Typed Scope And Ownership

The protocol domain assigns connection, thread, or turn scope before this
domain receives a fact. Thread/turn facts resolve through durable native
bindings and verified descendant ancestry. A connection-global event remains
global unless sourced aggregate evidence names an owner; it is never routed to
whichever conversation is active.

The notification domain validates the selected owning conversation and
produces a typed private outcome. It does not select routes, execute JSON-RPC
responses, mutate server-request lifecycle, write control-plane state, or
complete prompt waiters directly.

## Portable Mapping

Only facts matching the compact root kind semantics become public
notifications. Mapping preserves meaningful complete text and normalized
paths while excluding backend method names, ids, routes, phases, raw payloads,
provider/tool DTOs, and native capability metadata.

Codex approvals, tool user input, permission requests, MCP elicitation, and
handler-requested tool interactions all become the one portable Interaction.
No Codex-specific approval, form, MCP, input, or dynamic-tool notification
variant exists. Published response schemas are self-contained and validated
before durable insertion.

Prompt-returned failure is not duplicated as an Error notification. Typed
asynchronous warning/error facts unrelated to a prompt result may map to the
portable kinds when the selected semantics are complete. Selected
thread-scoped `item/started` and `item/completed` facts carrying
`ThreadItem::CommandExecution`, with verified conversation ownership, map to
portable `Process` transitions without native process ids or
command/environment leakage. The separate connection-scoped `process/spawn`,
`process/outputDelta`, and `process/exited` family is keyed only by a caller
process handle and cannot independently acquire a Conversation owner. Progress may
coalesce only before durable publication; essential message, file, warning,
error, terminal, and interaction facts are never silently dropped.

## Runtime-Only Facts

`turn/completed` becomes a typed terminal turn fact for the owning conversation
runtime. For a current generation, that runtime validates generation/thread/turn,
commits terminal state, and only then completes or discards its waiter result.
A delayed terminal after authoritative readback and Kit record compaction is
reconciled read-only against complete native history for the exclusively owned
bound root thread. Exact client/turn identity, chronological relation to any
current generation, status, and normalized native error must agree. It never
publishes another result or changes a newer prompt. Unreadable, incomplete,
foreign, ambiguous or contradictory history fails closed. The protocol or
notification classifier does not terminalize the prompt.

This historical reconciliation applies only to typed terminal facts. The pinned
legacy history reconstructs message item IDs; it does not establish identity
equality with live canonical output items. Late output items retain their
existing fail-closed behavior, without text-only identity substitution.

`item/agentMessage/delta` becomes a typed transient output fact carrying the
selected private thread, turn, item, and text fields. The conversation checks
the exact generation before offering text to its existing bounded prompt
observer. It is never a durable Message notification; reasoning, plan, command,
and tool deltas remain outside assistant output. The classifier does not infer
final-answer phase from an incomplete item.

`serverRequest/resolved` becomes a typed request-reconciliation fact for the
server-request owner. `thread/deleted` becomes a typed attachment/cleanup fact.
Creation metadata such as `thread/name/updated` remains private. None is a
generic public status notification, and `thread/closed` remains excluded as
specified by the protocol because native unloading is not portable closure.

The release additions have the scope and disposition in the protocol table.
Attachment, project, environment, strict-review, raw-response, OAuth, and auth
recovery metadata remain private after typed validation. Raw-response completion
and authentication recovery never complete a prompt or infer whole-prompt usage.
`thread/readState/changed` is also private thread metadata. Its required
`readState` object contains an opaque string revision and an optional/null unread
position, either thread start or a historical turn identity. Validate the nested
shape and thread ownership without requiring the unread turn to be active.
Repeated receipts neither publish portable notifications nor acknowledge Kit
delivery, resolve an interaction, or complete a prompt. Even after final
assistant output, only the independent terminal fact can settle that prompt.
Queue, prediction, MCP event-stream, and realtime-item behavior is
`Unsupported`. Reversion of an owned root or descendant history invalidates the
attachment with `InvalidState`/`AttachmentMismatch`; foreign thread facts never
acquire the active Conversation. Metadata carrying a current root turn must
match its owned generation; verified descendants retain their own turn scope.

Function-call output items validate their text or typed content arrays and stay
private. Agent-message delivery and question fields are validated on both item
lifecycles and in history. Asynchronous delivery or any non-null question set
is `Unsupported`, including a final-phase item; it cannot become an answer or
an invented Interaction. The added `partial_answer` phase is valid private item
data, like commentary; it cannot replace the final answer. Only final-answer
or absent-phase text is eligible in both live and historical completion.

## Durable Publication And Delivery

The owning conversation inserts each portable fact into the durable ordered
notification owner before waking delivery. One bounded in-memory mechanism may
wake the exclusive receiver and apply backpressure; it is not queue authority.
Unbounded fan-out, independent producer channels, and ignored sends are
invalid.

The conversation sequences durable insertion, delivery preparation,
acknowledgment, interaction resolution, replay, and terminal drain under the
root contract. If durable publication or delivery recording fails, the item
remains retryable or its producer fails honestly; no classifier advances it
optimistically.

Turn terminal facts, interaction terminal facts, and notification publication
must retain one deterministic conversation order. Slow or absent receivers do
not block the protocol reader inside storage or synchronization critical
sections.

## Interactions And Close

Actionable facts are published only after their private continuation and
durable pending state exist. The notification stores portable presentation;
[`SERVER_REQUESTS.md`](SERVER_REQUESTS.md) or [`CATALOG.md`](CATALOG.md) owns
the private continuation. Resolution and cancellation use the first durable
terminal transition and never resurrect completed work.

Conversation close stops new publication, settles interactions, commits final
items, and transitions the receiver to the root drain semantics. Parent
cascade ends the live receiver without acknowledgment and preserves backlog
for reopen. Notification state never keeps a closed transport or parent
runtime alive.

## Required Boundaries

- No protocol-owned notification manager, delivery queue, waiter registry, or
  StateStore writer.
- No exact private topology, generated inventory/dispatch, macro, wildcard,
  generic event box, item alias, or compatibility event pipeline.
- No public backend, producer, method, route, raw payload, or native identity.
- No notification classifier executes a response, invokes a tool, mutates an
  interaction, or calls a prompt waiter.
- Runtime owners keep fields private; any exposed event/persistence fields are
  passive DTO boundaries only.

## Validation Obligations

Static source contracts compare the selected known inventory to typed
classification coverage while allowing private implementation changes. Real
strict-live tests prove scoped routing, portable mapping, interaction
publication, terminal prompt sequencing, request reconciliation, durable
ordering, at-least-once replay, receiver exclusivity/drop, backpressure,
parent cascade, terminal drain, malformed-known failure, and foreign/future
event handling against the configured selected App Server.

## Selected Metadata And Terminal Changes

The exact new-method classification table in [`PROTOCOL.md`](PROTOCOL.md) owns
thread/project/environment metadata, authentication recovery, raw response
completion, strict review control, OAuth and MCP subscription events. Private
metadata has no public publication or prompt-terminal authority. Thread/turn
ownership is exact and untrimmed; a verified descendant retains its own native
turn scope. Global events remain global even with incidental payload fields.

Owned root or descendant history reversion invalidates the runtime with
InvalidState/AttachmentMismatch, preserving durable history and cleanup evidence.
Unrelated history is not adopted. Queues, realtime items and MCP event streams
remain explicit Unsupported outcomes after typed validation.

Async delivery or non-null questions is Unsupported, including completed
`final_answer` items. The foreground owner neither publishes async text as a
passive Message nor substitutes it for final output. Function-call output is
private typed non-assistant data. Malformed selected metadata is Protocol in
both live and history paths.

Interrupted terminal errors remain Cancelled with the normalized Integration
cause, preserving the same operation, phase, safe detail and redaction as
recovered history. Prompt failure is not duplicated as a notification; terminal
publication still follows durable settlement and cleanup.
