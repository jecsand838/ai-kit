# Codex Agent Domain

This document owns the Codex `agent/` domain behind the opaque root `Agent`.
[`../API.md`](../API.md) remains the sole exact public ledger,
[`../BUILD_CONFIG.md`](../BUILD_CONFIG.md) owns caller configuration, and
[`BACKEND.md`](BACKEND.md) owns the selected Codex realization and its
operation-level capability limits.

## Functional Ownership

The agent domain owns one live Codex backend runtime for one durable
`AgentId`. Its responsibilities are:

- the one initialized, long-lived App Server protocol connection;
- an SDK-spawned App Server process when the selected config requests one;
- backend-private attachment compatibility and live route ownership;
- workspace and conversation aggregate coordination;
- the Codex tool, knowledge, StateStore, notification, and server-request
  domain roots;
- model inventory and agent-scoped knowledge ingress; and
- sealing, descendant invalidation, recovery, and shutdown coordination.

Native thread ids, turn ids, JSON-RPC ids, transport handles, route keys,
StateStore keys, dynamic-tool payloads, and recovery records remain private.
They never become public identities, capability objects, errors, tracing
fields, or documentation imports.

The agent domain implements the Codex payload side of the private root Agent
dispatch contract. It does not introduce another public facade, backend-kind
query, catalog dispatcher, or shared native-runtime abstraction.

## Construction And Durable Publication

Build first validates passive Codex configuration and all cross-input
compatibility described by [`../BUILD_CONFIG.md`](../BUILD_CONFIG.md). It then
establishes the selected transport, performs initialization and exact version
verification, prepares backend-private attachment state, validates managed
tool registration compatibility, opens managed knowledge when supplied, and
reconciles the durable agent record.

Success publishes an Agent only after all of the following are true:

- the required caller-supplied `AgentStorage` contains discoverable agent
  identity;
- the initialized connection has the selected protocol identity and required
  experimental behavior;
- exclusive caller or SDK process ownership is established;
- the supplied tool and knowledge integrations agree with durable attachment
  facts; and
- background runtime owners can accept routed input and participate in
  bounded shutdown.

A failure after native or durable acceptance preserves the primary error,
attempts bounded cleanup of resources owned by that build, and records an
idempotent recovery obligation when cleanup cannot be proved. Once a new
durable identity is visible, it is either removed completely or remains
discoverable by typed id for reconciliation; hidden durable identities and
fallback Agents are invalid.

## Reopen And Inventory

Reopen uses `AgentStorage` inventory as authority, preserves the requested SDK
identity, and performs the one bounded reconciliation attempt required by
[`../STATE.md`](../STATE.md). It verifies:

- Codex attachment and configured connection compatibility;
- selected server identity and exclusive ownership;
- the complete source-neutral managed tool registration descriptor;
- every referenced managed-knowledge record through the supplied opaque
  Database; and
- cleanup, prompt, interaction, and native binding state needed before work
  can be admitted.

Reopen never substitutes a fresh native thread for a missing bound thread,
silently drops managed registrations, or treats absent knowledge as empty.
Active or ambiguous native work remains unavailable under the root recovery
rules.

Agent, workspace, and conversation inventories come from durable SDK records,
not route maps or App Server thread listings. Native listings are used only to
verify or reconcile backend-private attachment state. Missing and foreign
typed ids fail without revealing their actual owner.

## Workspace, Model, And Knowledge Coordination

The agent domain is the sole Codex workspace factory and lookup coordinator.
It delegates workspace behavior to [`WORKSPACE.md`](WORKSPACE.md), preserves
backend attachment state across revisioned updates, and records cleanup
obligations at safe delete commit before native thread deletion proceeds.

Model inventory is sourced from the selected App Server and normalized under
[`BACKEND.md`](BACKEND.md). The agent domain owns pagination, reasoning and
personality capability mapping, and validation but does not expose Codex preset
ids or provider payloads.

Agent- and explicit Database-scoped ingestion enter the shared
managed-knowledge implementation through the Codex knowledge domain. The agent
owner does not construct a
Database, inspect its backend, or move knowledge into StateStore or automatic
model-visible tools.

## Runtime And Shutdown

Inbound protocol work is decoded and scoped by [`PROTOCOL.md`](PROTOCOL.md),
then delegated to the owning conversation, notification, server-request, or
lifecycle domain. The agent does not perform protocol method switches or
conversation side effects in its connection reader.

Shutdown consumes the public Agent, seals new admission, cascade-invalidates
live workspace and conversation runtime, and settles accepted work under the
root lifecycle deadline. It then attempts independent tool, knowledge,
notification, transport, task, and SDK-owned process cleanup without holding
a synchronization guard or StateStore transaction across an await.

Caller-managed TCP or Unix servers are disconnected but never terminated.
Only an SDK-spawned process is terminated. Quiescent durable workspaces and
conversations remain reopenable; shutdown does not relabel them terminal.
Postcommit cleanup failure remains durable recovery work and cannot reverse an
earlier successful mutation.

## Required Boundaries

- Conversation creation, prompting, file reads, receiver acquisition, and
  interaction resolution remain workspace or conversation responsibilities.
- Protocol owns transport and typed wire decode, not notification delivery or
  durable lifecycle mutation.
- StateStore owns control-plane facts; Database owns managed knowledge;
  tool-sdk owns executable handler behavior.
- Concrete Codex domain owners keep private fields and expose invariants
  through receiver methods. Non-private fields are limited to intentional
  passive DTOs.
- Lifecycle state uses one owner with data-carrying states, not parallel
  started/completed/cancelled carrier families.
- No lock or StateStore transaction crosses network, process, tool, knowledge,
  notification, cancellation, or timer waits.

## Validation Obligations

Static contracts verify the private root dispatch boundary, absence of public
Codex implementation data, and domain separation without phrase-locking leaf
topology. Strict-live coverage against the configured selected App Server and
real storage/tool integrations proves construction, reopen, inventory,
version/exclusivity rejection, workspace lifecycle, model inventory,
knowledge ingress, failure cleanup, and shutdown.
