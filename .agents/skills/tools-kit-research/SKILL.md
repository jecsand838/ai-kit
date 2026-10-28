---
name: tools-kit-research
description: "Research tools-kit Local/MCP Toolset behavior, associated ToolHandler execution, MCP client/server protocol semantics, schemas, interactions, and strict-live requirements."
---

# tools-kit Research

## Purpose

Ground executable Local/MCP behavior and the independent MCP server product in
authoritative protocol and runtime sources.

## Prerequisites

- Read root `AGENTS.md` and `SKILLS.md`.
- Read [`tools-kit/AGENTS.md`](../../../tools-kit/AGENTS.md).
- Read [`tools-kit/docs/PORTABLE_API.md`](../../../tools-kit/docs/PORTABLE_API.md)
  for public-surface changes.
- Read [`tools-kit/docs/STRICT_TESTING.md`](../../../tools-kit/docs/STRICT_TESTING.md)
  for validation changes.
- Pair with `tools-kit-change-contract`, `ai-kit-public-surface-contract`, and
  relevant Rust skills for implementation or docs changes.

## Workflow

1. Classify the behavior as Local Toolset, MCP client Toolset, source-neutral
   ToolHandler, independent MCP server, or outside tools-kit.
2. Verify MCP lifecycle, transport, tools/list pagination, tools/call results,
   interaction, cancellation, and server semantics against the current MCP
   specification and rmcp source.
3. Preserve `Toolset -> ToolHandler` with associated Handler output and sole
   public Toolset ingress.
4. Keep model-visible tool failure in Completed ToolResult. Distinguish it
   from transport, protocol, cancellation, or state-machine errors.
5. Keep MCP client source state private. Preserve native names for protocol
   calls while exposing only validated ToolName values.
6. Treat McpServer as an independent tracked owner serving any Toolset. Reject
   Toolset serve/stop, endpoint discovery, and detached cleanup.
7. Identify deterministic fixture, strict-live, docs, and source-contract
   fallout. Scripted repository services never count as strict-live.
8. Feed sourced facts into the owning contract and implementation without
   exposing native protocol DTOs.

## Guardrails

- Do not restore ToolsetBuilder/config layering, factories, production
  conformance, child ingress, ToolList/ToolDeclaration, source ids, provider
  hosted behavior, composite runtime, or Toolset serving state.
- Prefer narrow honest errors over hidden fallback.
- Distinguish sourced facts from implementation inference.

## References

- [references/local-backend.md](references/local-backend.md)
- [references/mcp-backend.md](references/mcp-backend.md)
- [references/provider-hosted-backends.md](references/provider-hosted-backends.md)
- [references/future-validation.md](references/future-validation.md)
