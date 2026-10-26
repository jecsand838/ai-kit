# Private Managed Knowledge

This document owns agent-managed source ingestion and prompt retrieval.
[`API.md`](API.md) lists the only public knowledge inputs.

## Boundary

Managed knowledge is a private agent domain built from:

- required `AgentStorage<S>` for source manifests and integrity facts;
- optional caller-supplied `D: storage_sdk::DatabaseBackend` for source
  content, chunks, and retrieval.

Agent-sdk holds the database behind private shared ownership and does not
require `D: Clone`.

There is no public `KnowledgeBase`, backend trait, custom semantic execution
seam, factory, conformance owner, owner-lineage enum, retrieval request,
budget, context, snippet, provenance, or verification result.

The only public values are `KnowledgeSource`, `KnowledgeScope`,
`KnowledgePolicy`, and `IngestKnowledgeRequest`.

## Sources, Scope, And Policy

`KnowledgeSource` is validated caller text or bytes with a source id and title.
Sources contain no live handle, storage key, locator, embedding, chunk,
callback, stream, backend plan, metadata escape hatch, or render mode.

`KnowledgeScope` is Database, Agent, Workspace, or Conversation.
`KnowledgePolicy` is Default, Disabled, or a validated nonempty duplicate-free
set of scopes. Scope applicability is validated against the receiving Agent,
Workspace, or Conversation.

An empty source vector is a no-op after scope validation. Nonempty ingestion
atomically replaces the addressed owner/source set. Failure leaves the prior
published set authoritative.

## Private Ownership

The implementation derives full typed owner lineage privately from the live
hierarchy. Database namespaces and manifests are private and versioned.
Database-scope sharing occurs only when callers deliberately supply equivalent
access to the same Database domain.

Retrieval:

1. derives a private bounded query from the admitted prompt;
2. resolves effective policy through prompt, conversation, workspace, then
   default;
3. requests private bounded results from Database;
4. validates owner lineage, size, score, and source integrity;
5. gives typed private context to the selected backend prompt owner.

The backend owns final provider/native formatting. Shared code never creates
provider messages.

## Reopen And Lifecycle

Initial Agent and Conversation sources are committed before the owner becomes
usable. Reopen verifies every referenced source against the supplied Database.
Missing or mismatched well-formed records are `Conflict`; contradictory
internal facts are `InvalidState`; execution failure is `Integration`. Missing
knowledge never becomes empty success.

Agent shutdown and Workspace close preserve knowledge for reopen.
Conversation close removes Conversation-scoped sources or records cleanup
obligations. Workspace deletion removes Workspace/Conversation sources without
touching Agent or shared Database scope.

Durable cleanup errors preserve the detecting storage phase: transaction begin,
put, and delete use `StateWrite`; load uses `StateRead`; commit uses `StateCommit`.
Typed dependency timeouts retain their supplied duration, and exact transaction
conflicts retain `Conflict/TransactionConflict` plus that phase. The caller's
operation remains authoritative. Notification closure within conversation
cleanup preserves the same detecting phases. Preserving these facts does not
authorize a new retry or change commit reconciliation and recovery ownership.

No StateStore transaction remains open while awaiting Database work. Managed
knowledge is not control-plane StateStore data and never becomes an automatic
model-visible tool.
