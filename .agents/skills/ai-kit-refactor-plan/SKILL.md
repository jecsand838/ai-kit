---
name: ai-kit-refactor-plan
description: "Turn optimization-audit findings or a cleanup goal into a decision-complete implementation plan that preserves behavior, covers validation, and prefers net-reductive refactors over additive scaffolding. Use when the user asks for an audit-driven refactor plan, optimization plan, or cleanup plan before code changes, not when they want a fresh findings list, built-in `/review` bug-resolution planning, or direct implementation."
---

# ai-kit Refactor Plan

## Purpose

Produce a decision-complete cleanup or optimization plan before any edits.

## Trigger Boundary

Use this skill when the user wants an audit-driven refactor plan, optimization
plan, or cleanup plan before code changes. Do not use it for a fresh findings
list or for accepted `/review` findings.

## Prerequisites

- Start from actual findings or a concrete cleanup goal.
- Read [references/routing.md](references/routing.md).
- Load the relevant crate contract, research, and Rust best-practice skills.

## Workflow

1. Start from the provided findings or cleanup goal, not a generic template.
2. Produce a decision-complete plan covering behavior, implementation changes,
   tests, docs, validation, and assumptions.
3. Prefer reductive simplification over compatibility shims or additive
   scaffolding.
4. Ban warning suppression, dead-code preservation, and new crates unless the
   user explicitly approved them.
5. Order the plan around tests first, then production changes, then docs and
   validation.

## Output Rules

- Plan only. Do not edit code.
- Leave no material implementation decision unresolved for the next agent.
- Use the exact validation blocks from the relevant crate `AGENTS.md` files.

## References

- [references/routing.md](references/routing.md)
- [references/examples.md](references/examples.md)
