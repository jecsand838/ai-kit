# Routing

Use this file after the user names the task type, scope, phase, and optional
TODO marker.

## Always Load

- Root `AGENTS.md`
- Nearest crate `AGENTS.md` files for the assigned scope
- [scope-routing.md](scope-routing.md)
- The reference for the requested phase:
  - [inline-planning-phase.md](inline-planning-phase.md)
  - [research-refinement-phase.md](research-refinement-phase.md)
  - [implementation-phase.md](implementation-phase.md)
- [validation-and-done.md](validation-and-done.md)

## Phase Routing

- `INLINE_PLANNING`: add or refine planning comments only.
- `RESEARCH_REFINEMENT`: expand planning comments with sourced guidance only.
- `IMPLEMENTATION`: implement scoped TODOs and remove each only after it is
  complete, tested, and documented when needed.

## Skill Routing

- Always consider `use-rust-best-practices-core` for Rust changes.
- Add `use-rust-best-practices-testing` when tests, doctests, source
  contracts, strict-live, or validation strategy are in scope.
- Add `use-rust-best-practices-abstractions` for enum-dispatch, API shape,
  duplicated logic, traits, errors, or lifetimes.
- Add `use-rust-best-practices-architecture` for crate boundaries, module
  ownership, target layout, or test placement.
- Add `use-rust-best-practices-async`, `use-rust-best-practices-networking`,
  or `use-rust-best-practices-dependencies-platforms` when those domains
  dominate the work.
- Add the relevant crate `*-change-contract` skill for every crate in scope.
  For App, read `app-kit/AGENTS.md` and owning specs and load
  `ai-kit-public-surface-contract`; add `app-kit-research` when workflow,
  graph, recovery, or lower-crate composition facts matter.
- Add research skills only when external provider, backend, protocol, MCP,
  runtime, or strict-live facts affect the planned or implemented behavior.

## Boundary

Execute only the requested phase. Treat required out-of-scope work as a
dependent issue unless the user or orchestrator explicitly expands scope.
