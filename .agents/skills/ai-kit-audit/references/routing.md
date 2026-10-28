# Routing

Use this file after the user provides the audit topic and before inspecting
the repository.

## Always Load

- Root `AGENTS.md`
- The nearest crate `AGENTS.md` files for the requested scope
- [scope-routing.md](scope-routing.md)
- [output-schema.md](output-schema.md)

## Add Skills By Audit Topic

- Public imports, crate-root API, docs-taught paths, or module visibility:
  `ai-kit-public-surface-contract`
- Validation, strict-live, docs gates, or command sequencing:
  `ai-kit-validation-matrix` and `strict-live-audit`
- Crate behavior or public surface in `agents-kit`: `agents-kit-change-contract`
- Crate behavior or public surface in `providers-kit`:
  `providers-kit-change-contract`
- Crate behavior or public surface in `storage-kit`:
  `storage-kit-change-contract`
- Crate behavior or public surface in `tools-kit`: `tools-kit-change-contract`
- Crate behavior or public surface in `app-kit`: read its `AGENTS.md` and
  owning specs and load `ai-kit-public-surface-contract`
- Rust design, tests, async, networking, architecture, abstraction, or
  dependency questions: the matching `use-rust-best-practices-*` skills

## Add Research Skills Only When Needed

- App graph/workflow semantics, checkpoint/recovery behavior, rurr evidence,
  or lower-crate composition facts matter: `app-kit-research`
- `agents-kit-research`: Codex App Server, OpenCode, transport, capability, or
  strict-live behavior.
- `providers-kit-research`: provider API, model support, hosted-tool behavior,
  or strict-live behavior.
- `storage-kit-research`: backend capability, embedded-vs-remote behavior, or
  strict-live behavior.
- `tools-kit-research`: MCP, local/composite tool behavior, provider-hosted
  tool behavior, or strict-live behavior.

## Boundary

This skill is read-only. Do not edit files, add planning comments, create
branches, create worktrees, spawn sub-agents, or implement fixes.
