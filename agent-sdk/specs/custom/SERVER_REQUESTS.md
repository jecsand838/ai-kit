# Custom Actionable Requests

This document owns Custom tool approvals and source-neutral tool interactions.

## Approval

`CustomToolApproval` applies uniformly to model-requested executable tools,
including the ordinary local shell tool:

- `Allow` executes admitted calls without publishing an approval;
- `Deny` returns a model-visible failed `FunctionResult`;
- `Interaction` publishes one durable, non-dismissible approval and waits for
  `Allow` or `Deny`.

Approval never replaces workspace confinement checks. Safe presentation uses
the visible tool name and arguments only after the owning ToolInfo schema is
validated. Values that cannot be presented safely are omitted. Provider ids,
native call ids, credentials, environment values, and private aliases never
enter notifications.

## Tool interaction

A source-neutral text interaction publishes a JSON-string response contract.
A form interaction publishes its self-contained schema. Submitted content is
validated at the agent boundary, then resumed with the private original
interaction identity. Dismissal becomes the tool interaction cancellation
response. The public API exposes no parallel MCP, input, approval, backend, or
provider-specific request family.

## Ownership and recovery

One private durable owner covers pending, resolving, and terminal states.
Resolution commits intent before resuming a handler. Definite preacceptance
failure may return to pending; proven or ambiguous acceptance is never resent.
The request owner samples one absolute lifecycle deadline before intent
persistence. Exact transaction conflicts retry with the same private attempt.
Every commit outcome is confirmed by an exact read before the retained handler
response is dispatched; transient read conflict or unavailability retries only
that read. Late exact confirmation succeeds. Observation that cannot settle by
the deadline leaves the interaction recovery-owned and blocks a second attempt
or handler dispatch until durable reopen reconciliation.

That same deadline remains authoritative across retained-handler response
dispatch and durable settlement. Once settlement has produced any non-conflict
commit result, only exact observation may retry; neither the original request
nor its settlement mutation is dispatched again. A later lifecycle recovery
continues that read-only observation and admits settlement only when an exact
unchanged observation proves that the prior mutation did not apply.

The request owner creates one absolute deadline when durable publication
starts. Parallel requests append through the shared notification journal by
retrying only exact transaction conflicts with the same notification identity,
kind, and insertion time. A non-conflict commit failure is reconciled by an
exact read and never causes blind publication replay. Transient exact-read
unavailability stays in that read-only reconciliation episode until the same
deadline.

Abort, timeout, Conversation close, parent cascade, and runtime loss wake
blocked waiters with the first durable terminal outcome. Reopen never
fabricates a lost callback. Tool/provider DTOs, continuation senders, process
configuration, routes, and sensitive submitted values remain private.
