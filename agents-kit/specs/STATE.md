# Agent Control-Plane State

This document owns minimum durable state, recovery, and reopen.

## Storage Boundary

`AgentStorage<S>` owns one caller-supplied
`S: storage_kit::StateStoreBackend`. It is the only agents-kit control-plane
ingress. agents-kit exposes no keys, records, backend identity, config,
normalization constructor, or erased storage route through its public surface.
Every transaction is exactly `S::Transaction`.

One AgentStorage clone family coordinates process-local live ownership without
requiring `S: Clone`; shared ownership is internal. The
contract does not claim cross-process exclusion for one AgentId; callers
isolate or serialize shared domains externally.

Database-backed managed knowledge is separate. Model-visible behavior is a
Toolset. Neither substitutes for StateStore.

## Durable Floor

Persist only:

- Agent identity, display name, backend attachment, and safe integration
  compatibility facts;
- Workspace identity, context, defaults, revision, lifecycle, and attachment;
- Conversation identity, ownership, defaults, lifecycle, attachment, and
  private knowledge integrity references;
- one current prompt owner with admission, acceptance, cancellation, terminal,
  and reconciliation facts;
- one current interaction owner;
- unacknowledged notifications and acknowledgment/resolution state;
- selected-backend recovery progress;
- cleanup obligations.

Do not persist Provider, Model, Session, Toolset, ToolHandler, Database, native
clients, credentials, provider wire envelopes, callbacks, or task handles.

Custom persists a bounded sequence of committed portable caller and final
assistant turns sufficient for reopen. Provider-private intermediate function
calls, function results, thought signatures, replay metadata, hosted output,
and other causality remain run-local. The next prompt rebuilds the selected
model conversation as continuation-free messages through providers-kit's
public generic message contract.

## Transactions And Conflicts

State mutation uses storage-kit live transactions. A transaction contains only
its own load/put/delete/commit/rollback work; no provider, tool, Database,
notification, process, network, or timer await occurs inside it.

`StateTransactionBackend::commit` returns unit. When an agent operation needs
an observed revision or terminal fact, it performs the exact reload or
reconciliation required by that operation.

Preserve `StateStoreError::TransactionConflict` distinctly. Retry only a
definite conflict and only where the operation is safe to replay, bounded by
the existing lifecycle deadline. Surface all non-conflict errors unchanged.
After an admitted ambiguous commit, reconcile exact durable state rather than
blindly retrying.

Workspace update/delete commit is the public success boundary. Postcommit
runtime invalidation or native cleanup cannot change success into an error.

## Reopen

Reopen receives fresh `AgentConfig`, `AgentStorage`, and all required lower
opaque handles. It:

1. verifies durable identity and process-local ownership;
2. verifies backend and safe integration compatibility;
3. reconciles cleanup or ambiguous work once within the lifecycle deadline;
4. verifies referenced private knowledge through the supplied Database;
5. publishes a live Agent only when state is quiescent and coherent.

Active or ambiguous work remains `Busy` or an integrity failure; it is not
fabricated as idle. Closed conversations remain nonreopenable. Parent shutdown
or close preserves quiescent descendants.

## Notification State

Notification id/order, passive acknowledgment, interaction state, and terminal
drain facts are durable. Receiver leases are process-local and acknowledge
nothing on drop or parent invalidation. Resolution records one attempt before
non-idempotent continuation and never blindly resends known or ambiguous
acceptance.

## Cleanup And Privacy

Cleanup retains tracked ownership until tasks, processes, provider/model work,
tool handlers, and storage actors settle or a durable recovery obligation is
recorded. Losing a caller does not detach cleanup.

Malformed durable state or contradictory ownership/revision/lifecycle facts
are `InvalidState`. Errors and tracing expose no storage key, provider/native
id, route, payload, or secret.
