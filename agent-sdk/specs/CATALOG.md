# Agent Tool Catalog

This document owns the portable catalog contract and concrete backend
installation boundary. Exact public types remain in [`API.md`](API.md).

## Portable Inventory

`Workspace::list_tools` returns every effective tool owned by the Agent and
callable in that Workspace. The projection contains validated names,
descriptions, schemas, annotations, icons, and interaction capabilities from
the caller-supplied `T: tool_sdk::ToolsetBackend` values and backend-native tools that
the concrete backend can prove are installed.

Source identity, native ids, MCP transport state, handlers, installation
records, provider envelopes, and runtime health remain private. A backend that
cannot prove completeness fails honestly instead of returning a fabricated or
partial inventory.

## Construction Boundary

`AgentBuilder` accepts already-built values of one fixed
`T: tool_sdk::ToolsetBackend`; `tool_sdk::Toolset` is one supplied implementation.
Handlers remain the associated `T::Handler`, and neither type requires Clone.
Shared code may validate source-neutral callable contracts and retain the
values needed by the selected backend, but it does not inspect private source
variants, normalize them into supplied handles, or create a root runtime
dispatcher. Mixed inventories use caller-owned exhaustive Toolset/Handler enums.

Effective portable names are unique within one Workspace. Duplicate names
fail before Agent publication or before a native catalog change is accepted.
Durable compatibility records contain only safe behavior-affecting data, never
live handlers, credentials, or source provenance.

## Concrete Ownership

- Codex owns its selected dynamic-tool installation, inventory, invocation
  adaptation, and teardown.
- OpenCode owns discovery and mapping of its built-in, custom, and MCP catalog.
  Unsupported direct Toolset composition fails explicitly.
- Custom obtains declarations and handlers through the public Toolset
  behavior, performs bounded execution in its private prompt loop, and treats
  portable shell support as an ordinary local function tool.

Tool approvals and source-neutral text/form interactions use the one durable
notification-resolution lifecycle. Conversation close releases its runtime
catalog state; Workspace close releases live state while preserving reopen
records; Agent shutdown settles all owned tool work. No lock or StateStore
transaction crosses a tool or network wait.
