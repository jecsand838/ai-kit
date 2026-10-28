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

Agent shutdown releases remaining agent-owned registration resources after
descendant work settles. Cleanup is bounded and does not hold synchronization
or storage transactions across tool waits.

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
