# OpenCode Managed Knowledge

[`../KNOWLEDGE.md`](../KNOWLEDGE.md) owns portable sources, scope, policy,
ingestion, retrieval, integrity, and lineage. This file owns only OpenCode's
private coordination and native prompt rendering.

When supplied, `D: storage_sdk::DatabaseBackend` backs one private
managed-knowledge owner inside the Agent. OpenCode never exposes that owner
publicly, inspects the Database's private implementation, substitutes
StateStore, constructs a default Database, or persists lower-crate credentials.

Build and reopen validate source facts after durable Agent identity is known.
Workspace and Conversation creation materialize their exact scope with the
hierarchy transition. Before a prompt, OpenCode retrieves bounded
source-neutral context and maps it into the selected native prompt shape.
Native rows, vectors, locators, query payloads, and credentials never enter
portable DTOs.

Reopen verifies every durable integrity reference through the private managed
owner before publishing a handle. Close and delete remove only the exact owned
lineage. No hierarchy guard, prompt lock, or StateStore transaction crosses a
Database await. Callers resupply the Database required by reopen.
