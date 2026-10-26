---
name: ai-kit-diff-optimization-audit
description: "Audit the current diff against a chosen base for delete-first refactor opportunities, code bloat, duplicated logic, public-surface drift, performance or stability risks, and validation or docs gaps, then return findings only. Use when the user asks for an audit, optimization review, cleanup review, or findings list before refactoring, not when they already want the implementation plan."
---

# ai-kit Diff Optimization Audit

## Purpose

Produce findings-first optimization or cleanup review output without starting
implementation or planning.

## Trigger Boundary

Use this skill when the user wants an audit, optimization review, cleanup
review, or findings list before refactoring.

## Prerequisites

- Compare the current diff against the requested base.
- Load the relevant crate contract, research, and Rust best-practice skills for
  the touched areas.

## Workflow

1. Inspect the actual changed code before judging it.
2. Prioritize delete-first findings: duplicated logic, redundant helpers, leaky
   abstractions, stale docs/tests, and avoidable overhead.
3. Report findings in severity order with concrete file references.
4. Stop short of the implementation plan.
5. Return `no opportunities found` if the diff is already clean.

## Output Rules

- Prefer actionable findings over broad summaries.
- Call out validation, docs, and source-contract fallout whenever a finding
  would require them to change.

## References

- `ai-kit-public-surface-contract`
- `ai-kit-validation-matrix`
