# IMPLEMENTATION Phase

Use this phase to implement the work described by scoped inline TODO comments.

## Starting Rules

- Locate all scoped TODO markers before editing.
- Read each TODO and surrounding code before changing it.
- Execute TODOs in a coherent order, usually tests first, then production
  changes, then docs and validation.
- Do not remove a TODO until all work it describes is complete.

## Implementation Rules

- Prefer reductive refactors that delete obsolete logic and reduce duplication.
- Do not lift and shift code without improving structure when the TODO calls
  for refactoring.
- Do not preserve backwards compatibility unless the user explicitly asks.
- Do not add shims, hacks, escape hatches, synthetic behavior, deterministic
  shortcuts, or compatibility bloat.
- Do not use macro-based reductive refactors. The only macro exception is
  `tools-kit::tool!`.
- Keep backend-, provider-, database-, agent-, runtime-, and source-specific
  behavior behind owning crate boundaries.
- Preserve explicit receiver-method dispatch through the owning behavioral
  contract: the public RPITIT trait at a root-approved seam and a private trait
  for other closed families. Retain the private exhaustive enum; the approved
  Tool seam adds exactly one privately erased `Extension` arm, while Provider,
  Storage, and Agent dispatchers remain closed over built-ins.
- At a root-approved open seam, every concrete built-in payload, the private enum, and
  the exported opaque handle implement the same public trait. Generic
  Toolset's root `from_backend` ingress constructs only `Extension`; built-in
  construction creates concrete arms directly, while child implementations
  enter through associated types. Delete the superseded private behavioral trait and
  any parallel forwarding/dispatch path in the same vertical.

## TODO Removal Rule

Remove a TODO only when:

- all described code changes are implemented
- relevant tests or source contracts are updated
- owning docs are updated when behavior, public API, validation, or workflow
  changed
- validation for that scope passes or a concrete external blocker is reported

## Completion Gate

The implementation is not complete while scoped TODO markers remain.

Before final response:

- Run `rg "<TODO marker>"` for the scoped marker.
- Run relevant crate validation from `AGENTS.md`.
- Run strict-live or workspace gates when required by the task.
- Report changed files, removed TODOs, validation results, and blockers.
