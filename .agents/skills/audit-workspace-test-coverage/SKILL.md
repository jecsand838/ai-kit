---
name: audit-workspace-test-coverage
description: "Audit the ai-kit workspace for missing or weak unit-level, primary public-trait mock, integration, strict-live, docs, and completion-gate coverage, then return a detailed prioritized findings list without editing code. Use when the user asks for a workspace test coverage audit, missing-test analysis, strict-live coverage review, or validation-coverage recommendations across agents-kit, providers-kit, storage-kit, tools-kit, and app-kit."
---

# Audit Workspace Test Coverage

## Purpose

Produce a detailed workspace test-coverage gap report without starting
implementation.

## Trigger Boundary

Use this skill when the user wants a findings list, coverage audit, or test-gap
analysis across `agents-kit`, `providers-kit`, `storage-kit`, `tools-kit`, and `app-kit`. Do
not use it when the user already wants the implementation plan or code changes.

## Prerequisites

- Inspect the actual workspace state, tests, docs, and verify scripts before
  judging coverage.
- Load `use-rust-best-practices-testing`.
- Read every in-scope crate's `AGENTS.md` and owning public/behavior specs.
  Load its change-contract skill where one exists; App uses its AGENTS/specs
  plus `ai-kit-public-surface-contract`.
- Load `agents-kit-research`, `providers-kit-research`, `storage-kit-research`,
  `tools-kit-research`, and `app-kit-research` for the owning scope when coverage
  conclusions depend on upstream, workflow, or runtime facts.
- Load `ai-kit-validation-matrix` and `strict-live-audit` when strict-live,
  env-contract, or completion-gate coverage is in scope.

## Workflow

1. Compare each crate's public surface, critical helper logic, primary mock
   lane, strict-live flow, and completion gates against the tests that exist today.
2. Separate unit-level, direct public-trait struct/enum, and real consumer
   coverage from other deterministic integration, process/service, strict-live,
   docs/env, and runner coverage. Use [the mock contract](../../../docs/mock-testing.md) for
   complete inventory and fixture-integrity requirements; a mock pass does not
   prove built-in behavior or native durability.
3. Prefer narrow, actionable findings with concrete file references and a short
   rationale.
4. Call out production defects or blind spots discovered during the audit, but
   stop short of planning fixes.
5. Return `no material gaps found` if the workspace coverage is already strong.

## Output Rules

- Report findings first, ordered by impact.
- Keep implementation ideas secondary and high-level.
- Do not edit code or produce the follow-up implementation plan unless the user
  explicitly asks for it.

## References

- `use-rust-best-practices-testing`
- `agents-kit-research`
- `providers-kit-research`
- `storage-kit-research`
- `tools-kit-research`
- `app-kit-research`
- `ai-kit-validation-matrix`
- `strict-live-audit`
