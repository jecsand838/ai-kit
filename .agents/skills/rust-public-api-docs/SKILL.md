---
name: rust-public-api-docs
description: "Add or review targeted rustdoc for ai-kit public APIs, including doctestable examples for public structs and methods, and validate workspace docs gates under supported feature lanes. Use when one public Rust type, method, crate-root example, or a small crate-local public rustdoc surface needs a focused refresh. Do not use it for workspace-wide rustdoc campaigns across multiple crates; use `refresh-workspace-rustdocs` for those."
---

# Rust Public API Docs

## Purpose

Keep public rustdoc and crate-root-first examples accurate, useful, and
doctestable.

## Trigger Boundary

Use this skill when one public Rust type, method, crate-root example, or a
small crate-local public rustdoc surface needs a refresh. Do not use it for
workspace-wide rustdoc campaigns across multiple crates; route that work to
`refresh-workspace-rustdocs`.

## Prerequisites

- Load `ai-kit-public-surface-contract` when public imports or visibility
  change.
- Identify the owning crate docs that teach the same surface.

## Workflow

1. Update rustdoc on the public item first.
2. Keep examples crate-root-first and doctestable.
3. Use `no_run` only when the example would otherwise require a live service.
4. Keep the stable workspace docs index aligned with the current crate map.
5. Validate the docs gate before closing the task.

## Output Rules

- Every public struct and public method needs rustdoc with a doctestable
  example.
- Keep field, enum, variant, and type-level docs concrete instead of
  placeholder text.
- Keep this skill focused on narrow item-level or crate-local rustdoc work, not
  workspace-wide campaign orchestration.

## References

- [`docs/rustdoc-index.html`](../../../docs/rustdoc-index.html)
- [`AGENTS.md`](../../../AGENTS.md)
