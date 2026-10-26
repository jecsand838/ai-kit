---
name: ai-kit-delete-first-pass
description: "Plan or execute a delete-first reductive pass by starting from known growth targets, targeting files responsible for bloat, and preferring helper deletion, branch collapse, and abstraction simplification over additive cleanup. Use when the user asks for a follow-up cleanup pass, net-reduction pass, or bloat-removal pass after a refactor, not when they only want audit findings or diff measurement."
---

# ai-kit Delete-First Pass

## Purpose

Drive a cleanup pass that is explicitly reductive instead of additive.

## Trigger Boundary

Use this skill when the user wants a follow-up cleanup, bloat-removal, or
delete-first pass after a refactor. Do not use it for a findings-only audit or
for a diff-measurement-only request.

## Prerequisites

- Capture a baseline diff stat.
- Load the relevant crate contract, research, and Rust best-practice skills for
  the files in scope.

## Workflow

1. Identify the files still driving positive growth.
2. Delete redundant helpers, duplicate branches, placeholder wrappers, and
   stale scaffolding before adding new abstraction.
3. Keep only abstractions that remove more code than they add in the same scope.
4. Re-measure the diff after the pass.
5. Rerun the relevant validation before closing.

## Output Rules

- Separate production-code growth from docs or test growth before choosing the
  next target.
- Prefer stopping once the branch is smaller and green instead of widening
  scope.

## References

- `ai-kit-net-reduction-check`
- `ai-kit-validation-matrix`
