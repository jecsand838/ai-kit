---
name: use-rust-best-practices-core
description: "Apply this on any Rust task that creates, edits, reviews, refactors, debugs, or plans Rust code. Respect the repository's pinned toolchain, edition, and MSRV policy first; otherwise use current stable Rust conventions, thin binaries over reusable libraries, borrow-first data flow, modern Cargo/workspace hygiene, docs/lints/tests review, and escalation to the specialized Rust skills when architecture, testing, async, networking, abstractions, dependencies, or cross-platform issues appear."
---

# Rust Core Best Practices

## Purpose

Provide the baseline Rust quality checklist for every Rust task.

## Trigger Boundary

Use this skill on any task that creates, edits, reviews, debugs, or plans Rust
code, manifests, or test and lint behavior.

## Prerequisites

- Inspect repo-local toolchain, edition, and `rust-version` policy first.
- Load the specialized Rust skills when the task is primarily about
  architecture, testing, async, networking, abstractions, or dependencies.
- Load `rust-code-structuring-best-practices` when the task involves
  organizing code around structs, enums, builders, receiver methods, repeated
  parameter groups, stringly typed state, Rust item type aliases, stateful
  suffix struct families, or eliminating standalone functions and loose
  helpers.
- Load `use-rust-best-practices-abstractions` for enum-dispatcher work,
  especially closed implementation families that need private trait contracts
  or the eleven lower-crate backend seams enumerated by root `AGENTS.md`
  that require a public trait over a retained private dispatcher. App's five
  additional workflow traits stay concrete; only `ChildExecutor` uses its
  exact associated-future exception. Its serialization registry may erase
  field/tag codecs only, never runtime or lower-backend values.

## Workflow

1. Preserve repo-local conventions before applying generic defaults.
2. Prefer library-first design and keep binaries thin.
3. Prefer borrowing, slices, iterators, and focused refactors before adding
   `clone`, `Arc`, or duplicate logic.
4. Prefer typed errors in libraries and ergonomic composition at executable
   boundaries.
5. Reject Rust item type aliases at every visibility and same-root stateful
   suffix struct families before writing code, specs, tests, or docs. Use
   concrete structs, enums, newtypes, direct concrete types, receiver methods,
   or trait associated types instead.
6. Review formatting, linting, test scope, dependency impact, platform impact,
   and unsafe boundaries before finishing.

## Output Rules

- Keep public API additions minimal and intentional.
- Reuse existing domain code before creating a new crate.
- Avoid hidden panics in production paths unless the invariant is truly
  impossible and documented.
- Do not use type aliases or duplicated lifecycle/state carrier structs as
  compatibility shims, convenience names, or migration aids.

## References

- `references/core-checklist.md`
- `assets/templates/baseline-package/Cargo.toml`
- `assets/templates/member-Cargo.toml`
- Companion skills:
  `use-rust-best-practices-architecture`,
  `use-rust-best-practices-testing`,
  `use-rust-best-practices-async`,
  `use-rust-best-practices-networking`,
  `use-rust-best-practices-abstractions`,
  `use-rust-best-practices-dependencies-platforms`,
  `rust-code-structuring-best-practices`
