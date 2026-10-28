# OpenCode Agent Domain

This file defines the OpenCode implementation behind the opaque root `Agent`.
[`../API.md`](../API.md) remains the sole public operation and type inventory.
[`BACKEND.md`](BACKEND.md) owns the selected OpenCode realization and its
operation-level capability limits, while
[`PROTOCOL.md`](PROTOCOL.md) owns wire authority.

The OpenCode agent owns one selected OpenCode runtime, its reusable HTTP
client, its sole global SSE connection, backend-private compatibility and
attachment state, the workspace registry, native catalog discovery, managed
knowledge adaptation, recovery coordination, and shutdown. Native identifiers,
route names, provider metadata, protocol handles, storage keys, and server
configuration never become public identity or capability surfaces.

## Construction And Reopen

Construction validates all passive caller input before starting or contacting
the backend. It then establishes the mode selected by the caller:

- spawned mode starts the selected server, owns bounded readiness and later
  process shutdown, chooses an isolated absolute OpenCode database, disables
  native sharing and background subagents, and preserves the normal data home
  for authentication and configuration;
- external mode attaches to a caller-owned, exclusively assigned server and
  database and never assumes process-lifecycle ownership.

Both modes verify the exact selected release, disabled background subagents,
disabled sharing, and isolated native inventory before publishing the Agent.
The detailed checks and failure classifications are fixed by
[`BACKEND.md`](BACKEND.md). No compatibility mode, best-effort capability
projection, or degraded Agent may be published after a failed check.

Construction samples one absolute Agent lifecycle deadline before durable
setup. External health and capability verification consume that deadline.
Spawned process readiness and capability verification consume the earlier of
that deadline and the configured startup deadline; neither a transport request
timeout nor a nested readiness step grants a fresh window. Native inventory,
cleanup reconciliation, initial event synchronization, and reopen
reconciliation consume the same construction deadline. If verification expires
after a process starts, the runtime owner still terminates and joins the process
and its output drains before returning the original timeout.
Failure cleanup samples one lifecycle-bounded absolute deadline; event,
process, child-reap, and output-drain teardown cannot extend it.
Errors retain the public build or reopen operation and the stage that detected
the failure: client setup, server verification, inventory verification, or
event readiness/synchronization.

Client setup runs its finite synchronous HTTP client constructor on Tokio's
blocking pool. The existing builder owner retains and awaits the original join
even if the caller drops the build or reopen future. The same absolute deadline
is checked before dispatch, when the worker starts, and after its join. A worker
that starts after expiry skips construction. Once construction starts, expiry
does not interrupt or detach it; its owned settlement may finish after the
lifecycle deadline. A late result remains a timeout in client setup and cannot
start a server process, issue HTTP requests, or publish an Agent. Off-thread
execution preserves the client's existing proxy and TLS configuration.

Recovery expiry retains its last normalized failure as supplementary evidence,
and cleanup failures remain supplementary to the original failure.
Read-only durable preparation may be cancelled when construction expires. An
admitted durable transaction instead remains owned through commit or rollback; a
commit observed after expiry is retained as durable recovery state but never
authorizes publication or another construction step.

Durable Agent identity is Kit authority. Backend-private attachment state may
record only the non-secret facts required to reconnect to the same selected
runtime and verify its ownership. Reopen verifies those facts and current
server state; it does not create replacement native sessions, adopt foreign
inventory, infer ownership from a base URL, or reconstruct identity from
OpenCode records.

Partial construction remains an owned cleanup obligation. Spawned processes,
HTTP/SSE resources, newly created native state, and durable setup facts are
released or recorded according to the last committed boundary while the
original failure remains the returned error.

## Agent-Owned Operations

The agent domain implements the OpenCode side of Agent identity and info,
workspace creation, inventory, reopen, update, deletion, model inventory,
agent-scoped knowledge ingestion, and shutdown. Workspace and conversation
operations remain on their owning domains; the agent must not become a generic
dispatcher for descendant behavior.

Workspace inventory is derived from Kit durable records, never from native
session inventory. Native inventory is verification and recovery evidence
only. Workspace create, reopen, update, and delete follow
[`WORKSPACE.md`](WORKSPACE.md) and the root lifecycle requirements in
[`../RUNTIME.md`](../RUNTIME.md) and [`../STATE.md`](../STATE.md).

## Model Inventory And Selection

The selected provider inventory route supports portable model discovery.
Every `list_models` request performs a fresh typed read using the stable
agent-level routing context established at construction. The adapter:

- considers only providers reported as connected;
- preserves the exact provider/model pair in a private selector rather than
  treating a model name as globally unique;
- maps the selected model name to portable display information;
- reports empty reasoning-effort and personality inventories because selected
  native variant and agent names have no sourced portable semantic mapping;
- sorts the portable result deterministically; and
- reports no model as the portable agent-wide default.

The native provider response contains a provider-relative `default` map, while
the server's effective model choice also depends on configuration and recent
state. Neither is proof of one stable agent-wide default, so every portable
`is_default` value is false. The adapter must not guess a default, expose
provider credentials or costs, or flatten two distinct provider/model pairs
into one identity.

Model inventory is advisory. Prompt execution revalidates a supplied or
inherited selector in the workspace-routed context before sending work. A
model that disappeared or is no longer connected fails honestly; it is not
silently replaced by the server default. Every non-absent effective portable
reasoning or personality value returns backend-neutral `Unsupported` before
native mutation. The adapter never projects arbitrary native variant keys or
native agent names into those source-neutral public domains and does not call
the native agent inventory route for portable discovery. Absence omits both
wire fields and leaves config-owned/default behavior to the server. Native
variant option maps, agent names, prompts, permissions, temperature, steps,
provider defaults, and modes remain backend-private.

## Knowledge Ownership

Agent knowledge ingestion uses the caller-supplied
`D: storage_kit::DatabaseBackend` directly through the shared
managed-knowledge contract. The
request may select deliberate Database scope or Agent scope; absence selects
Agent. The agent validates receiver-applicable scope and delegates validation,
atomic publication, integrity, retrieval, and cleanup to the knowledge owner
described in
[`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md). It never substitutes StateStore,
creates an implicit database, inspects the database backend, or shuts down a
caller-owned database.

## Shutdown

Shutdown atomically seals new admission before awaiting backend work. The
OpenCode owner then generation-fences cancellation of accepted prompts,
settles interactions and durable cleanup obligations, waits for descendants
to quiesce within the configured lifecycle boundary, stops the global stream,
releases HTTP state, and terminates only a spawned process it owns.

External servers, caller-owned databases, caller-owned tool services, and
authentication state are never shut down by the agent. A dropped handle alone
does not promise asynchronous cleanup. Durable records remain available for
reopen according to the root state contract.

## Required Invariants

- One Agent owns one compatible, isolated OpenCode runtime and one global live
  stream.
- Kit ids remain portable authority; native ids are private bindings.
- Native inventory verifies ownership but never creates portable inventory.
- Model discovery is live, complete for connected providers, and advisory.
- No provider, route, server, or database identity leaks through public data.
- No Agent operation bypasses workspace, conversation, catalog, knowledge, or
  StateStore ownership to mutate their state directly.
