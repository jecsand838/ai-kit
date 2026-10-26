# OpenCode Notification Domain

This file defines how selected OpenCode events become the one portable
conversation delivery stream. [`../NOTIFICATIONS.md`](../NOTIFICATIONS.md)
owns public shape, exclusive receiver behavior, at-least-once delivery,
interaction resolution, backpressure, and terminal drain. This file owns only
OpenCode event transport, typed classification, routing, and private recovery
context.

## Sole Transport

`GET /global/event` is the sole live OpenCode subscription. There is no
instance `/event` subscription, per-conversation worker, notification polling,
periodic pending-request poll, or prompt-returned notification list. One Agent
owns the long-lived SSE response, and typed routing selects the owning native
root closure and portable Conversation.

That Agent ownership is one tracked event task with one clonable command
handle, one monotonic one-shot stop owner, and one join. Its explicit
Starting/Live/Recovering phase determines whether the current response exists;
startup, reconnect, gap, and caller-command reasons determine the permitted
recovery action. A Conversation may ask this owner to synchronize before an
existing absolute deadline, but cannot open, replace, stop, or independently
consume the stream. StateStore is persistence authority, not the event command
channel.

The bounded command channel admits synchronization requests in FIFO order by
reserving capacity under that supplied absolute deadline. Capacity pressure is
backpressure and expires as `Timeout`, never `Busy`. Cancellation before
reservation sends no command. For ordinary synchronization, cancellation after
enqueue closes the result owner so an unstarted command is discarded. Admitted
Close commands retain cleanup ownership after their result observer is dropped.
Event-owner shutdown closes the channel and wakes capacity waiters as `Closed`.

The SSE parser and the three selected event-envelope shapes are fixed by
[`PROTOCOL.md`](PROTOCOL.md). Normal events, live durable sync envelopes, and
history rows are decoded by distinct typed paths before normalization.
Reconnect subscribes first, buffers within fixed limits, retrieves bounded
history, merges by semantic identity, and advances per-aggregate contiguous
high-water only after the resulting durable side effects commit.

A detected gap or structured-parent proof uses that same open stream while
history is read and overlap is captured. Only failure of the current response
permits replacement, and the failed response is dropped before its replacement
is opened. Startup/reconnect subscribe-buffer-history and same-stream
synchronization each consume one caller- or lifecycle-supplied absolute
deadline across retries and backoff. The SSE silence deadline remains a
distinct transport-liveness bound.
Expiry of a caller command on a healthy current response fails the
command and returns the owner to live delivery; it does not invalidate or
replace that response. Owner-scoped gap and transport recovery retain their
terminal deadline and invariant semantics.

The synthetic selected `server.connected` and `server.heartbeat` events are
stream-liveness facts. They do not create portable notifications, complete a
prompt, select a conversation, or advance a durable aggregate. A complete
valid SSE event resets the silence deadline; partial bytes and comments do not.

## Typed Classification

Every event type in the selected OpenCode manifest decodes into a typed
OpenCode event and has one exhaustive outcome:

- a portable notification fact;
- a portable Interaction fact;
- a backend-private state or recovery fact;
- an honest `Unsupported` capability result; or
- an honest `NotYetImplemented` adapter gap.

Malformed known payloads are `Protocol`. A selected known type never falls
through an unknown-future bucket, generic raw value, provider payload, silent
no-event case, or string-prefix classifier. Unknown future types may retain
their complete envelope privately for diagnostics but cannot invent portable
state or success.

Portable mapping is semantic rather than one-to-one with native event names:

- complete independently sourced asynchronous user-visible text may produce
  `Message`; text parts owned by the current foreground response remain
  correlation/output state and never duplicate `PromptResponse`;
- session-owned `session.diff` or PatchPart facts may produce `FileChanged`
  after path and conversation validation; workspace-only `file.edited` or
  watcher events never choose an arbitrary conversation;
- a selected session-owned shell ToolPart maps its running state to portable
  `ProcessState::Running` and its completed or error state to
  `ProcessState::Exited`, using a sourced exit code only after typed range
  validation; pending state remains private, and global PTY lifecycle events
  have no session owner and remain workspace/runtime facts;
- bounded nonterminal execution facts may produce coalescible `Progress`;
- asynchronous recoverable concerns may produce `Warning`;
- asynchronous terminal failures not already returned by a prompt may produce
  `Error`; and
- pending permission and question facts produce `Interaction` through
  [`SERVER_REQUESTS.md`](SERVER_REQUESTS.md).

Native deltas, part fragments, tool internals, provider/model events, session
metadata, sharing facts, installation state, and lifecycle transitions remain
private unless a complete portable semantic fact is defined. Prompt-returned
errors are not duplicated as Error notifications. Tool-like rendered text is
not a native interaction.

Typed `message.updated` and `message.part.updated` facts for an active
structured prompt also remain private. They may satisfy the conversation's
parent-ownership proof only after exact session, User root shape, returned
parent id, part message owner, and private request-part id correlation. Their
live and history forms share one typed deduplicated state path and never become
portable notifications or terminal prompt output. The same selected typed facts
may establish generation/message/part ownership for transient `PromptStream`
text as specified by [`CONVERSATION.md`](CONVERSATION.md). Native deltas never
become durable notification history or `Message` notifications.

Process mapping uses a safe portable label and typed state only. Native PTY
ids, process ids, raw command arguments, environment, tool input/output,
provider payloads, and terminal bytes remain private. A separately meaningful
safe error may become Warning or Error without expanding the Process shape.
An exit value outside portable `i32` range is `InvalidState`; it is never
truncated or wrapped.

File mapping requires a selected file value, normalizes it against the owning
backend-host workspace context, and emits only the portable path plus a safe
summary derived from typed status/count facts. Native patch text, snapshots,
watcher provenance, and paths outside the validated context remain private. A
session diff entry without a file cannot invent a FileChanged notification.

File paths use the verified native directory's backend-host path syntax, never
SDK-host filesystem interpretation. Native relative and absolute paths are
lexically normalized and must identify a nonempty descendant of that directory
by complete path components. Windows drive and UNC anchors remain distinct;
slash-separated native Patch paths and backslash-separated Windows paths have
the same lexical meaning. A foreign anchor, root underflow, final escape, or
workspace-root-only value is `InvalidState` and cannot publish a file event.
This normalization neither queries a filesystem nor proves symlink isolation;
portable Workspace context and strict passive path validation are unchanged.

## Routing And Ownership

The global wrapper's directory, project, workspace, payload identity, and any
session-bearing typed fields form private routing evidence. A bound root
session owns its source-validated descendant closure. A descendant acquires no
public Conversation, but its events, interactions, aggregate progress, and
dedup state route to the root owner after parent-chain validation.

A global event that needs no conversation may be handled as a runtime fact.
Missing conversation ownership for a conversation-required event is an
explicit routing failure. An aggregate outside every owned closure proves the
exclusive-server contract false, seals new admission, and enters bounded
recovery; it is never ignored or attached to an arbitrary active conversation.

Protocol decoding performs no StateStore, receiver, interaction-response, or
prompt-terminal side effect. Conversation runtime is the first owner allowed
to atomically apply typed notification insertion, private state transition,
interaction reconciliation, high-water advancement, and waiter wakeup in the
required order.

## Durable Delivery

Before a portable item can be returned, its stable notification id, insertion
time, conversation order, kind, and interaction state are committed through
[`STATE_STORE.md`](STATE_STORE.md). The durable store is backlog authority;
bounded in-memory state only wakes the receiver and applies backpressure.

Passive items acknowledge according to the root receiver contract. An
Interaction blocks later delivery until its durable terminal resolution.
Receiver drop, parent cascade, stream reconnect, or process loss acknowledges
nothing. Replay uses the same notification id. Progress may coalesce only
before durable publication and never across an Interaction or terminal fact.

SSE loss does not terminate an open receiver while bounded recovery can
continue. A terminal stream or protocol error is delivered according to the
root receiver error taxonomy and leaves durable unacknowledged work for owning
reconciliation. A receiver remains pending after prompt completion while the
Conversation remains open.

If consuming Conversation close fails while preparing interaction cleanup
through the event command owner, its existing open receiver wakes with a terminal
`Closed` error at `NotificationStream` / `Lifecycle`. The close caller retains
the original failure. This receiver failure acknowledges no durable item and
requires owning reconciliation before reacquisition; it neither terminalizes
the conversation nor cancels admitted cleanup. Other conversations, replacement
receiver leases, and committed terminal drains are unaffected.

## Request Boundary

Pending and terminal permission/question events are typed here and handed to
the request owner. This notification layer does not construct HTTP replies,
transition resolving state by itself, call pending-list routes, validate caller
response content, or expose native request ids. Pending-list reconciliation
after open or reconnect enters the same durable interaction owner directly and
does not fabricate an SSE event, event id, envelope, or high-water advance.

## Required Invariants

- One global SSE stream feeds one durable portable receiver path.
- One Agent event owner alone opens, replaces, stops, and joins that stream.
- Gap and caller-command history preserve a healthy current stream and one
  absolute recovery deadline.
- Every selected known event is typed and exhaustively classified.
- Raw transport and native provenance remain private.
- Delivery state commits before return and is never replaced by polling.
- Protocol, notification, request, and StateStore responsibilities remain
  distinct even though conversation runtime sequences their outcomes.
