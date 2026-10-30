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
2. Use documented public imports and one canonical, doctestable example per
   meaningful workflow. Link related types and simple methods to that example
   instead of repeating its setup.
3. Prefer pure values, local implementations, or small direct-trait mocks for
   portable behavior. Instantiate and invoke the example, and assert its outcome.
   Use `no_run` for genuine service, credential, or external-process requirements;
   an uncalled async helper is only a compile proof.
4. Keep the stable workspace docs index aligned with the current crate map.
5. Validate the docs gate before closing the task.

## Output Rules

- Every public struct and method needs useful rustdoc. A link to an applicable
  canonical example satisfies example coverage; add a separate example only
  when it teaches distinct behavior. Preserve operation-specific errors,
  cancellation, ownership, and cleanup guarantees when consolidating prose.
- Keep introductions focused on one caller task. Put complete implementer and
  native integration walkthroughs in linked advanced examples. Hide incidental
  imports/runtime setup, not repeated application lifecycles.
- Keep mocks private to the example or owning crate's tests. Do not export
  fixtures, copy full test harnesses into every snippet, or claim mock execution
  proves native compatibility or durability.
- In examples that acquire runtime owners, settle required cleanup before
  propagating an operation failure. Preserve the primary error and report
  supplementary cleanup failures.
- Keep field, enum, variant, and type-level docs concrete instead of
  placeholder text.
- Keep this skill focused on narrow item-level or crate-local rustdoc work, not
  workspace-wide campaign orchestration.

## References

- [`docs/rustdoc-index.html`](../../../docs/rustdoc-index.html)
- [`AGENTS.md`](../../../AGENTS.md)
