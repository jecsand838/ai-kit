---
name: vendor-release-update
description: "Plan or execute one ai-kit vendor refresh from updater-generated dossier inputs for a single crate, vendor, and target tag. Use when `scripts/vendor_update.py` hands Codex a generated `dossier.md` and optional `plan.md` for one vendor refresh, not when the user wants a top-level workspace or crate refresh campaign, current-branch landing, or multi-vendor orchestration."
---

# Vendor Release Update

## Purpose

Handle the single-vendor dossier workflow emitted by `scripts/vendor_update.py`.

## Trigger Boundary

Use this skill only for a single crate, vendor, and target tag described by an
updater-generated `dossier.md` and optional `plan.md`.

## Prerequisites

- Read [references/routing.md](references/routing.md),
  [references/dossier-contract.md](references/dossier-contract.md), and
  [references/update-workflow.md](references/update-workflow.md).
- Determine whether the prompt is planning mode or execution mode.

## Workflow

1. Load the relevant crate `*-change-contract` skill.
2. Load `ai-kit-validation-matrix`.
3. Load the matching `*-research` skill when upstream behavior or vendored
   source facts matter.
4. Follow the repo and crate `AGENTS.md` workflow for tests, docs, and
   validation.
5. Leave vendor archive cleanup, lockfile finalization, branch management,
   commits, and pushes to `scripts/vendor_update.py`.
6. When the dossier selects no-regression completion, read its completion-gate
   report before planning or editing. Treat baseline failures as recorded debt,
   not as permission to audit or implement unrelated specification backlog.

## Output Rules

- Stay leaf-scoped to the single vendor refresh described by the dossier.
- Limit production and spec changes to sourced vendor drift and mechanical
  cleanup in touched paths. Do not expand into unrelated backlog merely to
  reduce baseline failures.
- Complete the updater-delegated dossier in the current Codex execution. Do
  not spawn collaboration subagents or delegate parts of the dossier.
- Do not take over top-level campaign orchestration.
- Keep the worktree ready for the updater to rerun completion gates or finish
  the mechanical steps.

## References

- [references/routing.md](references/routing.md)
- [references/dossier-contract.md](references/dossier-contract.md)
- [references/update-workflow.md](references/update-workflow.md)
- [references/examples.md](references/examples.md)
