# Custom Managed Knowledge

[`../KNOWLEDGE.md`](../KNOWLEDGE.md) owns source validation, scope, policy,
ingestion, retrieval budgets, integrity, and lineage. This file owns only
Custom's private coordination and prompt rendering.

When supplied, `D: storage_sdk::DatabaseBackend` backs one private
managed-knowledge owner inside the Agent. Custom never exposes that owner as a
public behavioral handle, substitutes StateStore for semantic data, constructs
a default Database, or inspects the Database's private implementation.

Agent creation/reopen verifies configured source facts after the final
`AgentId` is known. Workspace and Conversation creation materialize their
scopes atomically with the durable hierarchy transition. Prompt admission
retrieves bounded context only after model, tool, and knowledge policy resolve;
Custom renders source-neutral context without exposing native rows, vectors,
queries, locators, or credentials.

Reopen verifies durable integrity references through the private managed owner.
Close and delete remove only the exact owned lineage. No hierarchy lock,
prompt lock, or StateStore transaction crosses a Database await. The caller
supplies the Database needed to reopen and retains no public knowledge runtime
surface.
