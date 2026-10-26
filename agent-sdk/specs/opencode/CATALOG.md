# OpenCode Native Catalog Domain

This file defines the backend-private catalog used by OpenCode workspaces and
conversations. It does not create a public catalog API; [`../API.md`](../API.md)
owns the portable model and tool operations. The selected server owns execution
of its built-in tools, configured custom tools, and configured MCP tools.
`agent-sdk` discovers, validates, projects, and binds that native inventory but
does not reimplement it.

## Capability Boundary

Caller-supplied executable `T: tool_sdk::ToolsetBackend` values are `Unsupported` for
OpenCode because the selected release exposes no client callback that would let the SDK
execute them through the native planner. The SDK must not register a fake
native tool, install a callback shim, render tool calls as prompts, or claim
that a caller-owned MCP service is mounted merely because a Toolset exists.

OpenCode-visible built-in, custom, and MCP tools may be configured outside
`agent-sdk`. Their execution remains wholly server-native. Native permission
and question continuations produced during that execution enter the portable
interaction path; tool protocol envelopes and implementation provenance remain
private.

## Discovery And Completeness

The selected experimental tool-id and tool-description routes expose the
built-in/custom native registry. Discovery obtains a bounded, typed id set and
uses the selected provider/model context required by the description route.
The id route is a candidate inventory, not proof that every id is callable for
an unknown effective model. A portable inventory therefore requires a concrete
revalidated Workspace model and the provider/model-sensitive description
route. Without one, `list_tools` returns `NotYetImplemented` rather than
guessing the server's opaque default model or presenting candidate ids as
callable tools.
The adapter maps the validated native name, optional description, and selected
JSON Schema parameters into portable tool information and validates all
selected response shapes. The parameter schema does not become identity or
provenance. Provider details, internal execution metadata, and route identities
remain private.

Portable names are validated nonempty and unique and results sort
deterministically by that neutral name. A malformed selected description or
input schema is `Protocol`; duplicate or contradictory inventory is
`InvalidState`. An unavailable selected read fails the operation rather than
being treated as an empty catalog.

MCP tools are merged separately by the selected session tool resolver. The
selected MCP route exposes connection status, and its tools-changed event names
the server whose tools changed, but the selected server exposes no route that
proves a complete MCP tool-definition inventory. Therefore:

- with no connected MCP contribution, the built-in/custom inventory may be
  returned as the complete native catalog;
- when connected MCP state can contribute tools, `list_tools` returns
  `NotYetImplemented` rather than an empty or known-partial vector; and
- a tools-changed event invalidates the affected private catalog binding and
  requires fresh validation before later prompt admission.

The implementation must never report an unconditional empty list, silently
omit MCP tools from a claimed complete inventory, scrape configuration files,
or infer definitions from model output. If a later selected server exposes a
complete typed MCP inventory, that new capability requires new selected-source
authority before adoption.

## Workspace And Conversation Binding

A workspace owns the current native catalog projection for its routing and
model context. A conversation stores a private binding to the validated
catalog generation used at creation or reopen. “Installation” means validating
and binding the existing server-owned catalog to that conversation; it does not
mutate external OpenCode configuration, allocate synthetic native ids, copy
MCP credentials, or transfer execution ownership to the SDK.

Portable inventory completeness is a `list_tools` concern, not a prerequisite
for server-native prompt execution. A tools-changed event invalidates cached
inventory and any catalog observation used for later listing; it does not
disable the server's own planner or block a prompt that otherwise leaves model
and tool selection to OpenCode. Conversation creation and reopen validate only
configured compatibility and direct-Toolset rejection, not a fabricated
complete catalog snapshot.

When `list_tools` has no concrete revalidated effective provider/model, it must
not choose an arbitrary provider-relative default. Connected MCP contribution
also remains `NotYetImplemented` because complete identity and definition
inventory is then unprovable. Neither limitation blocks server-native prompt
execution.

## Interaction And Lifecycle Ownership

Tool-produced permission and question requests follow
[`SERVER_REQUESTS.md`](SERVER_REQUESTS.md) and the sole receiver in
[`NOTIFICATIONS.md`](NOTIFICATIONS.md). The catalog does not create a second
tool-interaction API, execute HTTP replies, own portable notification state, or
write StateStore directly.

Workspace update, conversation close, workspace close, and agent shutdown
release only SDK-owned catalog bindings and discovery state. They never shut
down server-native built-ins, caller-configured MCP servers, provider services,
or an external OpenCode process.

## Required Invariants

- Native execution remains server-owned.
- Portable inventory is complete or fails honestly; it is never known-partial.
- Executable Toolsets are rejected without compatibility shims.
- Catalog bindings contain no secrets and create no public backend identity.
- Catalog code discovers and validates; it does not own prompt, interaction,
  persistence, or process lifecycle sequencing.
