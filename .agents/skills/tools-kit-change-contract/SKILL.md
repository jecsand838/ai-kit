---
name: tools-kit-change-contract
description: "Apply tools-kit Toolset→ToolHandler ownership, associated-child private normalization, direct Local/MCP construction, independent McpServer lifecycle, public-module, and validation rules."
---

# tools-kit Change Contract

## Purpose

Keep tool changes inside the reduced executable-tool boundary.

## Prerequisites

- Read [`tools-kit/AGENTS.md`](../../../tools-kit/AGENTS.md).
- Load `tools-kit-research` for MCP, schema, interaction, lifecycle, or
  strict-live facts.
- Load `ai-kit-public-surface-contract` for import/reexport/visibility changes.
- Load relevant Rust core, async, testing, abstraction, architecture, and
  structuring skills.

## Workflow

1. Preserve `ToolsetBackend` and `ToolHandlerBackend` as the only public
   behavioral contracts.
2. Preserve `Toolset -> ToolHandler` with
   `ToolsetBackend::Handler: ToolHandlerBackend`. Toolset is the sole public
   generic ingress; Handler products normalize privately.
3. Keep Local/MCP concrete owners, private dispatchers, and public handles
   implementing the same traits. Each dispatcher has explicit built-ins plus
   one private erased Extension arm. Built-ins never use erasure.
4. Construct built-ins directly with `Toolset::local(Vec<ToolRegistration>)`
   and `Toolset::mcp(McpClientConfig)`.
5. Keep ToolInfo as the complete declaration and return `Vec<ToolInfo>`.
   Unify function and interactive registration under ToolRegistration.
6. Keep `ToolHandlerBackend::step` as the only execution method.
   Tool-authored failure is Completed ToolResult with `is_error`; outer
   ToolError is reserved for invalid input/state, protocol, transport,
   cancellation, timeout, unsupported, or closed runtime.
7. Preserve shared exactly-once Toolset shutdown, sealed admission, tracked
   cleanup, cancellation-safe terminal publication, and no drop cleanup
   promise. Registration copy-on-write Arc unwrapping is not lifecycle.
8. Keep one validated McpClientConfig with connection, optional namespace,
   timeout, and form-validation policy. Preserve private native-name mapping,
   empty inventory, honest content mapping, and bounded cleanup.
9. Keep McpServer separate and non-Clone. Start eagerly with a caller
   `Arc<T>` for `T: ToolsetBackend` plus McpServerConfig; preserve `T::Handler`
   without requiring Clone or Toolset normalization. Consuming shutdown joins
   the tracked owner. Drop or cancellation stops serving while retaining the
   cleanup owner; never abort the supervisor that owns accepted cleanup.
   A shutdown timeout is an error, not a transfer or discard of unfinished
   work. Server shutdown never shuts down the shared Toolset.
10. Organize supporting API into `declaration`, `execution`, `interaction`,
    `local`, `mcp`, and `schema`. Keep root exports tight and do not reexport
    schemars.
11. Update docs/source contracts with implementation and run validation only
    at the authorized final validation stage.

## Removed Boundaries

Do not restore factories/conformance, ToolsetBuilder, ToolsetConfig/source
wrappers, ToolHandler child ingress, ToolList, ToolDeclaration, source ids,
Toolset serve/stop, McpSourceConfig serving, restartable server actors,
duplicated registration/handler families, intent/decision carriers,
ToolHandlerOutput::Error, ToolError::Execution, result call/name fields,
provider-hosted/composite/empty runtime, or compatibility APIs.

## Output Rules

- No aliases, macros except the existing narrow `tool!` helper, unsafe, shims,
  generic frameworks, public dyn backends, or duplicate contracts.
- Keep source implementations and protocol DTOs private.
- Use explicit receiver-method dispatch and domain owners.
- Return honest errors and preserve lifecycle terminal authority.
