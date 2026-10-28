# Codex Workspace Domain

This document owns the Codex `workspace/` domain behind the opaque root
`Workspace`. Root workspace shape, update, delete, and cascade semantics live
in [`../RUNTIME.md`](../RUNTIME.md); exact operations remain solely in
[`../API.md`](../API.md).

## Functional Ownership

A live Codex workspace owns its Kit identity, observed nonzero revision, link
to the agent runtime, workspace-scoped knowledge and managed-tool state, and
access to the Codex conversation and persistence domains. It never exposes or
accepts a native thread id, protocol handle, storage key, route key, or
backend-host implementation identity.

Durable workspace state is authority for name, context, defaults, lifecycle,
revision, and backend-private attachment. A live handle may retain the
minimum validated snapshot needed for admission, but every policy-sensitive
operation proves that its observed revision is current before using it. A
stale handle does not refresh itself in place.

Only the Codex agent domain creates or reopens workspace handles. App-owned
records, route registries, native thread inventories, and caller-supplied
protocol values are not alternate construction paths.

## Information And Revision Admission

Information and inventory operations read owning durable records and return
the portable snapshots defined by [`../RUNTIME.md`](../RUNTIME.md). The live
route registry is an optimization for delivery, never inventory authority.

Every admitted workspace operation is bound to the handle's observed
revision. Revision mismatch is detected before native work or use of stale
cwd/default policy. Update preflight, mutation, deletion, and cancellation of
policy-sensitive reads follow the shared hierarchy rules; the Codex workspace
does not add a second lock, reservation, or revision system.

Codex context uses App Server host paths. The workspace domain validates the
complete cwd, runtime roots, per-root access, sandbox mode, and network policy
against the selected representation in [`BACKEND.md`](BACKEND.md) and does not
reinterpret paths relative to the Kit process. Effective native policy is
reverified when a conversation is created or reopened; silent downgrade,
broader access, or ignored roots are not accepted.

Representability includes every installed model-visible action, not only the
native shell sandbox. Restricted filesystem/network policy rejects mounted
opaque Toolsets, ambient MCP servers, mixed-access WorkspaceWrite roots, or an
effective web-search setting that cannot meet [`BACKEND.md`](BACKEND.md).

## Conversation Creation

Conversation creation coordinates existing domain owners in this semantic
order, without requiring a fixed helper graph:

1. validate the request, current workspace revision, defaults, and initial
   knowledge sources;
2. prepare shared conversation-scoped knowledge and the workspace's managed
   tool installation contract;
3. durably record one unique backend-private native-creation correlation and
   reconciliation obligation;
4. start and materialize one durable native root thread with the exact
   representable workspace and foreground-only policy;
5. durably bind the Kit conversation record, workspace revision, native
   attachment, managed-source references, and cleanup obligations; and
6. open the conversation runtime and publish its live route before returning
   the opaque handle.

Native acceptance that cannot be rolled back is recorded for reconciliation.
The selected private correlation is sent only through native `threadSource`.
After a lost `thread/start` response, bounded recovery exhausts paginated
native inventory, requires exactly one matching correlation, and verifies that
thread before binding or cleanup. Creation never replays `thread/start`, guesses
from a coarse thread attribute, or exposes the correlation through public API
or caller configuration.
Creation never returns a conversation whose dynamic tools, knowledge,
foreground policy, or durable binding silently fell back to empty or default
state.

Portable conversation defaults are resolved from the complete validated
conversation and workspace values. Native request overrides are derived
views, not durable authority, and do not duplicate model, personality,
reasoning, or knowledge policy in a second record family.

## Conversation Reopen

Reopen first validates typed Kit ownership, current workspace revision,
nonterminal lifecycle, managed source integrity, and managed tool registration
compatibility. Only then may it resume and inspect the bound native thread,
verify exact effective workspace policy and disabled background features,
reconcile active work, and install a live route.

It never changes the bound native identity, fabricates a replacement thread,
replays a non-idempotent prompt or interaction response, or treats native
thread existence as proof of a terminal prompt. A missing, foreign,
contradictory, active, or ambiguous binding follows the root recovery error
rules.

Setup failure cleans only resources actually acquired by that attempt,
preserves the primary error, and leaves accepted durable/native effects as
explicit cleanup obligations. It must not leak a route or retain a parallel
tool/runtime cleanup owner.

## Tools And Knowledge

Tool inventory delegates to [`CATALOG.md`](CATALOG.md) and returns the compact
source-neutral inventory of managed registrations installed for this
workspace. It does not require a live conversation and excludes implicit
server-native tools.

Workspace-scoped ingestion delegates to [`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md)
under the scope and publication rules of [`../KNOWLEDGE.md`](../KNOWLEDGE.md).
Prompt retrieval remains conversation-owned. Neither path writes knowledge to
StateStore or mounts it as a tool.

## Close, Update, And Delete

Workspace close consumes the handle, atomically seals admission, and then
allows the concrete backend to dispatch generation-fenced prompt cancellation,
settle interactions, and clean background terminals before bounded
quiescence. Synchronization is released before any protocol or timer await.

Close cascade-invalidates live conversations and receivers while preserving
quiescent durable conversation records and notification backlog for typed-id
reopen. It does not call public conversation close, terminalize quiescent
conversations, or delete the workspace.

Revisioned update and safe delete are agent-scoped mutations. Update preserves
the Codex attachment and invalidates all prior-revision handles after durable
commit. Delete records cleanup for every bound root thread at portable commit,
removes quiescent durable inventory, and then uses sourced native deletion.
Native cleanup cannot become the durable success boundary or expose thread ids
in the result.

## Required Boundaries

- No permanent independently mutable workspace context is authoritative over
  the durable workspace record.
- No root/shared Codex-specific update, delete, runtime, or cleanup dispatcher
  is introduced.
- No setup wrapper, compatibility adapter, generic command bus, or parallel
  lifecycle owner replaces direct collaboration among the domain owners.
- Tool, knowledge, storage, and protocol work occurs outside hierarchy guards
  and outside unrelated StateStore transactions.
- Conversation ownership is validated before tool installation, native
  resume, or route publication.

## Validation Obligations

Strict-live coverage proves durable information and inventory, exact revision
admission, every directly representable workspace policy, conversation create/reopen, setup-failure
cleanup, tool and knowledge scope, update invalidation, cascade close,
background-terminal quiescence, and safe delete against the selected App
Server and real configured dependencies. Static coverage protects the public
boundary and domain ownership without freezing private helper or file layout.
