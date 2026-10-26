---
name: ai-kit-change-workflow
description: "Run scoped ai-kit change workflows for features, bug fixes, refactors, optimization findings, code-review fixes, docs, and test hardening. Use directly or inside orchestrated workers to enforce inline planning comments, research refinement, implementation gates, scope boundaries, validation, and ai-kit architecture rules."
---

# ai-kit Change Workflow

## Purpose

Enforce a scoped, phased change workflow for ai-kit tasks that need planning
comments, research refinement, and implementation discipline.

## Trigger Boundary

Use this skill when the user asks to run a change workflow in ai-kit, either
directly in the current worktree or through `orchestrate-workflow` workers.

Use `ai-kit-audit` instead when the user wants only an initial read-only
findings backlog.

## Required Inputs

- Task type: `optimization`, `code-review-fix`, `feature`, `bug-fix`,
  `refactor`, `docs`, `test-hardening`, or a user-provided equivalent.
- Scope mode: `workspace`, `crates`, `crate`, `paths`, or `module`.
- Phase: `INLINE_PLANNING`, `RESEARCH_REFINEMENT`, or `IMPLEMENTATION`.
- TODO marker: user-provided marker or default `AI-KIT-CHANGE-TODO(<scope>)`.

## Prerequisites

- Read root `AGENTS.md`.
- Read nearest crate `AGENTS.md` files for the scope.
- Read [references/routing.md](references/routing.md).
- Read [references/scope-routing.md](references/scope-routing.md).
- Read the phase reference matching the requested phase.
- Load relevant change-contract, research, Rust practice, validation, docs, or
  strict-live skills based on the task type and scope.

## Hard Rules

- Do not use macro-based reductive refactors. The only macro exception is
  `tool-sdk::tool!`.
- Do not add shims, hacks, escape hatches, synthetic behavior, deterministic
  output shortcuts, or backwards-compatibility bloat.
- Preserve private exhaustive enum dispatch. At the root-approved Tool seam,
  use the public RPITIT behavioral contract plus a private erased adapter only
  in the enum's `Extension` variant. Provider, Storage, and Agent public traits
  remain direct and their supplied dispatchers remain closed over built-ins.
  Do not retain a duplicate private behavioral trait or replace an enum with
  bare trait-object storage.
- Keep backend-, provider-, database-, agent-, runtime-, and source-specific
  behavior out of public portable APIs.
- Prefer reductive structs, closed enums, typed spec data, helper methods, and
  inherent methods over additive abstraction bloat.
- Treat out-of-scope required work as a dependent issue unless the user or
  orchestrator explicitly expands scope.

## Workflow

1. Resolve scope, phase, TODO marker, and allowed paths.
2. Load the phase reference:
   - `INLINE_PLANNING`: [inline-planning-phase.md](references/inline-planning-phase.md)
   - `RESEARCH_REFINEMENT`: [research-refinement-phase.md](references/research-refinement-phase.md)
   - `IMPLEMENTATION`: [implementation-phase.md](references/implementation-phase.md)
3. Execute only the requested phase.
4. Validate the phase-specific completion gate in
   [validation-and-done.md](references/validation-and-done.md).
5. Report changed files, remaining blockers, validation results, and dependent
   out-of-scope issues.

## Output Rules

- State the task type, scope mode, phase, and TODO marker used.
- Report only the requested phase outcome.
- Report changed files, validation commands and results, remaining scoped TODO
  markers, blockers, and out-of-scope dependent issues.
- Do not claim the workflow is complete while scoped TODO markers remain.

## References

- [references/routing.md](references/routing.md)
- [references/scope-routing.md](references/scope-routing.md)
- [references/inline-planning-phase.md](references/inline-planning-phase.md)
- [references/research-refinement-phase.md](references/research-refinement-phase.md)
- [references/implementation-phase.md](references/implementation-phase.md)
- [references/validation-and-done.md](references/validation-and-done.md)
- [references/examples.md](references/examples.md)
