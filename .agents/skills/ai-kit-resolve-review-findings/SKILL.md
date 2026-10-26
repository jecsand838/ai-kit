---
name: ai-kit-resolve-review-findings
description: "Turn accepted Codex `/review` findings into a decision-complete ai-kit resolution plan with TDD ordering, crate-specific validation, docs fallout, and clean-architecture constraints. Use when the user asks to plan fixes for built-in code review findings before code changes, not when they want a fresh review, a generic refactor or optimization plan, or direct implementation."
---

# ai-kit Resolve Review Findings

## Purpose

Turn accepted built-in `/review` findings into a decision-complete fix plan
before any edits.

## Trigger Boundary

Use this skill when the user wants to plan fixes for accepted built-in
`/review` findings. Do not use it for a fresh review or a generic optimization
plan.

## Prerequisites

- Confirm the accepted findings are present and concrete enough to map to
  owning files or crates.
- Read [references/routing.md](references/routing.md).
- Always load `use-rust-best-practices-core` and
  `use-rust-best-practices-testing`.

## Workflow

1. Treat the provided findings as the accepted bug list.
2. Derive the affected crates and ownership from each finding.
3. Load the matching crate contract, research, and Rust best-practice skills.
4. Produce a decision-complete tests-first plan with implementation approach,
   docs fallout, exact validation, and completion criteria.
5. Resolve all accepted findings in one plan unless the user explicitly narrows
   scope.

## Output Rules

- Plan only. Do not edit code.
- Keep the plan tied to the cited findings.
- State assumptions explicitly when a finding leaves a real gap.

## References

- [references/routing.md](references/routing.md)
- [references/examples.md](references/examples.md)
