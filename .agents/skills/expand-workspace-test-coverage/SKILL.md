---
name: expand-workspace-test-coverage
description: "Plan and implement ai-kit workspace test coverage improvements from concrete findings, including unit-level, primary public-trait mocks, integration, strict-live, docs, and completion-gate changes, then run the authorized validation gates and address exposed production defects within the task scope. Use when the user explicitly wants to execute previously identified workspace test coverage work across agent-sdk, provider-sdk, storage-sdk, tool-sdk, and app-sdk. Prefer plan mode; do not use for audit-only requests or when no concrete findings exist."
---

# Expand Workspace Test Coverage

## Purpose

Turn concrete workspace test-coverage findings into completed test, docs, and
validation changes, with production fixes only within the authorized scope.

## Trigger Boundary

Use this skill when the user explicitly wants to execute previously identified
test coverage work across `agent-sdk`, `provider-sdk`, `storage-sdk`,
`tool-sdk`, and `app-sdk`. Do not use it for a fresh audit, vague brainstorming,
or requests that do not start from concrete findings or a bounded coverage goal.

## Prerequisites

- Start from concrete findings or a bounded coverage goal.
- Read [references/routing.md](references/routing.md).
- Read [references/examples.md](references/examples.md).
- Always load `use-rust-best-practices-core` and
  `use-rust-best-practices-testing`.
- Load the matching crate change-contract, research, and Rust best-practice
  skills before changing code, tests, docs, or validation flows.

## Workflow

1. Start from the accepted findings or bounded goal, not a generic request to
   add more tests.
2. Produce a decision-complete plan first when the surrounding Codex flow
   supports planning.
3. Add the narrowest honest tests first in the owning crate's `tests/` tree:
   direct public-trait struct/enum mocks, real Custom Agent/App composition,
   other public API integration tests, strict-live expansions, and docs-contract
   or verify-script coverage as needed. Keep production `src/**` free of tests
   and test-only branches. Follow [the mock contract](../../../docs/mock-testing.md)
   for the primary lane; retain separate built-in and strict-live targets.
4. Fix production code only when a new test exposes a real defect and the task
   authorizes production changes. Under a source freeze, keep its required
   regression enabled, document the defect in the owning ledger, and continue
   independent test work. Do not weaken fixtures or hide failures to obtain green.
5. Update owning docs, skill docs, env contracts, and validation fallout in the
   same change.
6. Run the exact primary, workspace, and crate gates from
   `ai-kit-validation-matrix`, including the canonical strict completion path
   when expansion scope is workspace-wide. Primary CI remains separate evidence
   and never replaces the required release gates.

## Output Rules

- Use ordinary crate-local structs and exhaustive enums for direct public-trait
  mocks. No extension ingress, backend erasure, generated dispatch, shared mock
  crate, or public mock API in the primary lane.
- No mocks or recorded payloads as strict-live or built-in service evidence.
  No warning suppressions, dead code, test-only production branches, or new crates.
- Plan before implementing. In Plan Mode, stop at the plan; otherwise continue
  the authorized implementation when the active mode permits edits. A writable
  sandbox alone never overrides Plan Mode.
- If there are no concrete findings, redirect to
  `audit-workspace-test-coverage`.

## References

- [references/routing.md](references/routing.md)
- [references/examples.md](references/examples.md)
