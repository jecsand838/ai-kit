# Codex Tool Catalog Domain

This document owns the Codex `tools/` domain: managed executable registration,
native dynamic-tool installation, runtime execution, source-neutral inventory,
and teardown. Exact builder and inventory operations remain in
[`../API.md`](../API.md); [`../BUILD_CONFIG.md`](../BUILD_CONFIG.md) owns the
portable Toolset ingress.

`CATALOG.md` names a functional domain, not a required private
`CodexToolCatalog` type or leaf-module graph.

## Cross-Crate Boundary

Caller-supplied `tools_kit::ToolsetBackend` implementations remain opaque
owning-crate registrations; the exported `Toolset` is one valid implementation.
tools-kit owns Local/MCP source behavior, handler construction,
tool execution state, result semantics, and any caller-owned MCP serving.
Codex owns only its native installation and invocation adapter around those
registrations.

The Codex domain must not expose or duplicate tools-kit source DTOs, provider
hosted declarations, protocol request payloads, MCP internals, or runtime
handles through agents-kit. It does not create another public registration API,
backend-specific tool config, source/provenance inventory, or root catalog
runtime dispatcher.

## Build Validation And Durable Compatibility

Build validates every supplied executable registration before publishing an
Agent. Effective managed names are nonempty and unique across all supplied
Toolsets. Description and behavior-affecting input schema are validated and
lowered into the private Codex dynamic-tool declaration required by the
selected App Server.

Selected dynamic-tool names match `^[a-zA-Z0-9_-]+$`, are at most 128
characters, and reject `mcp` plus the reserved `mcp__` prefix. Every input
schema passes the selected App Server tool-schema parser before publication.
Duplicate effective names fail in their applicable scope. Namespace names and
descriptions obey their separate selected bounds and reserved-name rules; they
are not validated as ordinary function names. Any violation is
`InvalidConfig` before Agent publication rather than a deferred conversation
failure.

Because Toolset execution is a client callback outside the native shell
sandbox and Toolset exposes no portable confinement capability, a nonempty
managed catalog is representable only for `FullAccess` with network `Enabled`.
Restricted workspaces fail `Unsupported` before native thread creation rather
than installing an execution path that bypasses policy.

The source-neutral managed registration descriptor defined by
[`../BUILD_CONFIG.md`](../BUILD_CONFIG.md) is the durable compatibility fact.
It contains no source label or live handler. Reopen reconstructs handlers from
the integrations supplied by the caller and requires their normalized public
registration contract to match durable state before native resume.

Codex initialization and thread setup must prove the required experimental
dynamic-tool support. Unsupported composition fails before the Agent or
Conversation is published; a failed installation never falls back to an empty
catalog.

## Conversation Installation

For each new conversation, the tools domain prepares one complete managed
registration set before native thread materialization. Thread creation supplies
the selected dynamic declarations, and the resulting conversation runtime owns
the executable handler state needed to answer `item/tool/call`.

For reopen, App Server may restore persisted native declarations from session
metadata but does not provide a complete inspection surface. Durable managed
registration compatibility plus the caller-supplied Toolsets is therefore
required before resume. Contradictory later native invocation is `Protocol` or
`InvalidState`; it is never accepted as an unregistered replacement.

Partial preparation is cleaned by the owner that acquired it. After a
conversation accepts the completed runtime, no workspace or agent component
keeps a parallel teardown path or shared-ownership escape hatch.

## Dynamic Tool Execution

The selected App Server sends dynamic tool work as a typed server request. The
server-request domain validates native request ownership and delegates the
call to the installed tools domain; it does not execute tool handlers itself.

Execution enters tools-kit through its public handler contract. A completed
tool result becomes the selected Codex dynamic-tool response. A tool execution
error becomes a failed tool response, not a JSON-RPC transport failure.
Malformed native payload, lost runtime ownership, cancellation, and broken
continuation are Kit errors with the portable category defined by
[`../ERRORS.md`](../ERRORS.md).

When a handler suspends for caller input, the tools domain asks the owning
conversation to publish the one portable Interaction described by
[`../NOTIFICATIONS.md`](../NOTIFICATIONS.md). Resolution resumes exactly that
handler continuation. Backend/tool ids and tools-kit interaction envelopes stay
private. Close or unrecoverable continuation loss terminally cancels the
interaction and must not fabricate a successful tool result.

A live managed callback retains the exact original prompt owner through handler
acquisition and every handler step. Before a newly acquired handler is invoked,
before an interaction is published, and before a resolved interaction resumes
the handler, the tools owner rechecks that prompt's original deadline and effect
permission. A public timeout can precede its durable cancellation intent; delayed
intent does not authorize another tool effect.

When that owner no longer permits effects, the unique handler receives one
`Cancel` under the existing lifecycle cleanup bound. A completed result or typed
Tool cancellation settles cleanup; another interaction or another error does
not. No native callback result is emitted. A real cleanup failure remains with
the existing tracked worker and runtime shutdown, supplementary to the original
prompt failure; it never replaces an already delivered public timeout or proves
remote native settlement. No new worker, handler reconstruction, or retry is
introduced.

The protocol reader remains responsive while handler work executes. Handler
work is runtime-owned, bounded by the owning prompt/lifecycle, and not awaited
while holding route, notification, hierarchy, or StateStore guards.

## Portable Inventory

Workspace tool inventory contains exactly the agents-kit-managed executable
registrations installed under the build contract. It is deterministic, sorted
by validated name, and source-neutral. No managed registrations yields an
empty inventory.

Implicit Codex tools, native MCP configuration not supplied through the
portable builder, protocol route details, and dynamic invocation state remain
private and do not appear as invented `ToolInfo` entries.

## Teardown And Recovery

Conversation close and parent cascade stop new tool work, settle or cancel
handler interactions, and release the conversation-owned executable runtime
before delivery ends. An accepted native response is never resent after an
ambiguous connection result. Durable interaction acceptance and cleanup facts
remain with the conversation StateStore owner.

Cancellation remains actionable when passive notifications precede an unresolved
managed Interaction in the journal. Lifecycle cancellation removes the exact
local pending responder under a short guard and transfers its complete owner,
notification, request, original deadline, and observation receipt to its already
registered managed worker. That worker is marked Retiring under the same task
lock before handoff; revocation cannot race a fresh handoff into an aborted task.
The public abort waiter observes the receipt but does not own the admitted
retirement future. Dropping it neither drops the unique Handler nor reexecutes
its tool.

The worker drives one Handler Cancel concurrently with durable retirement, then
joins both before completing its receipt. Completed and typed Cancelled are
settled local cleanup; a new Interaction or another failure is retained as
cleanup failure. One transaction verifies the complete persisted binding and
Interaction content, cancels only its still-Pending notification, and removes
only its matching tool obligation. Earlier unread facts retain their identities,
contents, timestamps, and ordering. A raced Resolving or contradictory record is
not rewritten into success. Public caller resolution remains head-ordered.
Persisted binding, kind, and Pending checks retain StateRead attribution through
abort, close, and shutdown; admitted commit failures retain StateCommit. A later
Toolset shutdown failure remains supplementary to the retained retirement error.

The original cleanup deadline bounds new retirement stages, Handler cancellation,
and confirmation. An admitted commit remains owned until its result is known,
even when its observation receipt is delayed past that deadline. A definite
conflict is returned without replay; an unknown commit permits only exact
before/expected journal observation plus matching obligation absence to prove
atomic retirement. Unproven retirement seals the existing conversation admission
and retains its exact owner/error in the existing generation fence; a new Agent
reopen reconciles the durable obligation. The same task/mutation fence blocks
new native dispatch while retirement is owned, including after native foreground
completion. Prompt admission checks this exact conversation both initially and
before dispatch; unrelated conversations remain independent. No sender, tool
effect, native callback decision, or retirement mutation is blindly replayed.

The existing event reader owns and joins every managed worker. Reader exit and
awaited Agent shutdown revoke Executing work but retain Retiring work through
its accepted cancellation and commit receipt. The reader removes a Retiring task
only after consuming its JoinSet result. Awaited shutdown preserves the original
deadline failure and subsequent cleanup evidence, and closes the reader before
releasing Toolset resources; an outer native-cleanup timeout cannot drop that
owner. Native cleanup keeps its original bound. These guarantees concern
ordinary observers and explicit awaited cleanup: destructive root-handle loss,
including dropping the consuming shutdown future, retains the portable runtime
contract's lack of asynchronous/durable cleanup guarantees. It cannot establish
native rollback or successful retirement; persisted obligations require later
reconciliation. No additional root task, runtime framework, or persistence
version is introduced.

## Required Boundaries

- No macro, generated table, trait-object router, wildcard adapter, item type
  alias, or loose helper replaces explicit typed Codex ownership.
- No providers-kit hosted declaration or tools-kit source DTO becomes an
  agents-kit public or Codex-native DTO.
- No server-request route directly owns handler execution, notification
  persistence, or lifecycle mutation.
- No private source-kind or backend-kind value is reused as public inventory,
  diagnostics, capability, or error context.
- Runtime owners keep fields private; only passive protocol/serialization DTOs
  may expose fields within their intentional boundary.

## Validation Obligations

Static contracts verify direct static storage composition, opaque
Provider/Toolset ingress, and absence of public
source/protocol leakage. Strict-live tests with the selected real App Server
and real Toolsets prove validation, native installation, inventory, dynamic
execution, tool errors, caller interaction/resumption, protocol-reader
responsiveness, reopen compatibility, partial-setup cleanup, and consuming
teardown.

## Native Model Inventory

`model/list` remains the authoritative selected-provider catalog. Rejection by
native provider configuration is a safe failure and never authorizes a bundled,
stale, guessed, or alternate-model fallback. Exact configured model selection
and sourced defaults remain required. The deprecated `supportsPersonality`
field no longer advertises support: portable personality inventories are empty,
and explicit or persisted personality selections are Unsupported before native
mutation. Other backends' public personality types are unaffected.
