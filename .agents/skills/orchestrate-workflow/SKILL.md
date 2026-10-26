---
name: orchestrate-workflow
description: "Create isolated worktree branches for selected findings, spawn scoped Codex workers using a specified workflow skill, review each worker result, merge only clean validated work, and clean up successful worktrees. Use after the user selects findings and explicitly asks for parallel or worktree-based execution."
---

# Orchestrate Workflow

## Purpose

Turn selected findings into isolated worktree tasks, delegate independent work
to scoped workers, and integrate only reviewed and validated results.

## Trigger Boundary

Use this skill only after the user provides selected findings or issues and
asks to orchestrate worktrees, sub-agents, workers, or parallel execution.

Do not use it for a fresh audit. Use `ai-kit-audit` first.

## Prerequisites

- Read root `AGENTS.md`.
- Read [references/routing.md](references/routing.md).
- Read [references/orchestration-flow.md](references/orchestration-flow.md).
- Read [references/worker-contract.md](references/worker-contract.md).
- Read [references/review-merge-cleanup.md](references/review-merge-cleanup.md).
- Read the workflow skill named by the user, such as
  `ai-kit-change-workflow`, before composing worker prompts.

## Workflow

1. Confirm selected findings, base branch or base commit, worktree root,
   workflow skill, workflow phase, and merge policy.
2. Inspect current git status, current branch, and existing worktrees.
3. Classify findings by file ownership and overlap risk.
4. Create one branch and worktree per independent issue. Keep overlapping
   issues in a serial queue.
5. Spawn one worker per independent issue with the worker contract customized
   for that issue, worktree, branch, scope, and workflow phase.
6. Review completed worker diffs and validation results.
7. Auto-merge only clean, reviewed, validated work. Stop and report conflicts,
   failed validation, unsafe scope drift, or unclear worker output.
8. Clean up worktrees and branches only after successful merge.

## Output Rules

- Report created branches and worktrees.
- Report worker assignment, queued serial issues, review outcome, merge status,
  cleanup status, and blockers.
- Do not hide failed worker results behind a partial success summary.
- Do not use force checkout, force branch deletion, destructive cleanup, or
  same-branch writable worktrees.

## References

- [references/routing.md](references/routing.md)
- [references/orchestration-flow.md](references/orchestration-flow.md)
- [references/worker-contract.md](references/worker-contract.md)
- [references/review-merge-cleanup.md](references/review-merge-cleanup.md)
- [references/examples.md](references/examples.md)
