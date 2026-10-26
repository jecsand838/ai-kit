---
name: ai-kit-audit
description: "Perform scoped read-only ai-kit audits and return a structured findings backlog or \"no findings identified.\" Use when the user specifies a crate, set of crates, path scope, or workspace-wide ai-kit audit topic and wants findings before branches, worktrees, sub-agents, comments, plans, or implementation."
---

# ai-kit Audit

## Purpose

Produce a findings-first backlog for a user-specified ai-kit scope and audit
topic without changing repository state.

## Trigger Boundary

Use this skill when the user asks to audit ai-kit for architectural,
optimization, review, testing, documentation, boundary, strict-live, or
workflow issues and expects findings to select from later.

Do not use this skill for the later inline planning, research refinement, or
implementation pass. Use `ai-kit-change-workflow` for that.

## Prerequisites

- Read root `AGENTS.md`.
- Read the nearest crate `AGENTS.md` files for the requested scope.
- Read [references/routing.md](references/routing.md).
- Read [references/scope-routing.md](references/scope-routing.md).
- Read [references/output-schema.md](references/output-schema.md).
- Load relevant contract, research, Rust practice, docs, or validation skills
  based on the requested scope and audit topic.

## Workflow

1. Identify the audit topic, scope mode, target crates or paths, and any
   requested severity threshold.
2. Inspect only. Do not edit files, create branches, create worktrees, spawn
   sub-agents, run implementation commands, or add inline comments.
3. Derive findings from repository evidence, source contracts, docs, tests,
   scripts, and authoritative references when the audit depends on external
   behavior.
4. Separate confirmed issues from speculative opportunities. Omit weak findings
   unless they create a concrete risk.
5. Return the structured backlog from `output-schema.md`, or exactly state
   `No findings identified.` when the audit finds nothing actionable.

## Output Rules

- Lead with findings, ordered by severity.
- Include affected ownership, expected impact, overlap risk, parallelization
  status, and recommended validation commands for each finding.
- Keep recommendations actionable enough for `orchestrate-workflow` or
  `ai-kit-change-workflow` to consume later.
- Never perform the follow-up workflow during this audit pass.

## References

- [references/routing.md](references/routing.md)
- [references/scope-routing.md](references/scope-routing.md)
- [references/output-schema.md](references/output-schema.md)
- [references/examples.md](references/examples.md)
