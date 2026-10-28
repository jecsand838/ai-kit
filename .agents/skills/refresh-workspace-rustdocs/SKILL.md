---
name: refresh-workspace-rustdocs
description: "Refresh ai-kit public rustdoc end to end across agents-kit, providers-kit, storage-kit, tools-kit, and app-kit. Use when public Rust APIs, crate or module docs, doctestable examples, source-contract expectations, or workspace rustdoc validation need a coordinated refresh against current HEAD. Do not use it for markdown-only documentation refreshes or narrow single-item rustdoc edits; use `refresh-workspace-agentic-documentation` for markdown-only work and `rust-public-api-docs` for focused item-level rustdoc work."
---

# Refresh Workspace Rustdocs

## Purpose

Refresh the public rustdoc surface, doctestable examples, and rustdoc-contract
coverage across the `ai-kit` workspace.

## Trigger Boundary

Use this skill for workspace-wide rustdoc refreshes that span one or more
crates, rustdoc contract tests, or the shared docs gate. Do not use it for
markdown-only documentation refreshes; route that work to
`refresh-workspace-agentic-documentation`.

## Prerequisites

- Read [references/routing.md](references/routing.md).
- Read [references/workflow.md](references/workflow.md).
- Start from current `HEAD` and the in-scope public surface.
- Load the relevant crate `*-change-contract` skills before editing crate-owned
  rustdoc or rustdoc-contract tests. For App, read its `AGENTS.md` and owning
  specs and load `ai-kit-public-surface-contract`.

## Workflow

1. Inventory the rustdoc-visible public surface for every crate in scope before
   writing edits.
2. If the request is made from `/plan`, produce a decision-complete plan first,
   then continue into execution only when the surrounding Codex flow permits
   file edits.
3. Browse the official rustdoc writing guide live when example structure,
   crate-root docs, or rustdoc sections need external confirmation.
4. Refresh crate-root and module `//!` docs first, then public item `///` docs,
   examples, and required sections such as `# Errors` or `# Panics`.
5. Tighten rustdoc contract helpers and docs-site validation together whenever
   expectations change.
6. Run the documented rustdoc and source-contract gates before closing the
   task.

## Output Rules

- Keep examples runnable unless the item truly requires live services,
  credentials, or external processes.
- Never suppress rustdoc warnings, lints, or contract failures.
- Route markdown-only documentation refreshes to
  `refresh-workspace-agentic-documentation`.
- Keep narrow item-level rustdoc work on `rust-public-api-docs` when the task
  does not require a workspace-wide coordination pass.

## References

- [references/routing.md](references/routing.md)
- [references/workflow.md](references/workflow.md)
- [references/examples.md](references/examples.md)
