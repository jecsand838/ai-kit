# Behavior Proof Policy

`tools-kit` exposes executable Local and MCP behavior only.

## Proof Levels

1. MCP specifications and rmcp sources establish protocol semantics.
2. Deterministic tests prove Local behavior, MCP mapping, lifecycle, and the
   independent server product.
3. Downstream fixtures prove direct public-trait composition with exact
   associated handlers. The [primary mock lane](../../docs/mock-testing.md)
   uses caller-owned structs and exhaustive enums without normalization;
   existing extension tests separately prove `Toolset::from_backend` routing.
   Neither establishes an independent MCP service's behavior.
4. Strict-live against an independently configured MCP service proves native
   client behavior.

Unsupported or unproven behavior returns an explicit ToolError. Do not invent
outputs, retries, source identities, or successful cleanup.

## Invocation Results

Once an invocation is accepted, tool-authored success or failure is a
Completed ToolResult. `is_error` distinguishes model-visible failure. Outer
ToolError means the handler could not carry out a valid invocation step.

Interaction requests remain source-neutral and resume the same unique Handler.
Form validation is explicit. URL elicitation remains unsupported unless a
separate public contract and proof are approved.

## MCP Client

The client preserves native names privately, supports empty inventories, maps
all supported MCP content honestly, and settles request/session cleanup.
Form-enabled sessions serialize calls when reliable per-call elicitation
correlation is unavailable.

## MCP Server

McpServer is independent from Toolset source identity. It serves any direct
`T: ToolsetBackend` through inventory and associated-handler behavior, retaining
`Arc<T>` without a Clone requirement on `T` or its handler. It owns a tracked
task and has one consuming shutdown authority. Server cleanup cannot detach a
live task or terminate the underlying shared toolset.

## Prohibitions

Do not add provider-hosted behavior, storage adapters, agent registration,
Toolset serving state, endpoint discovery, factories/conformance owners,
source-id routing, public protocol DTOs, compatibility facades, macro
dispatch, or synthetic success.
