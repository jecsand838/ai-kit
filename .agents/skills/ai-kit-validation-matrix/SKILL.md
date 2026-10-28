---
name: ai-kit-validation-matrix
description: "Run or plan the exact ai-kit workspace and crate validation matrix, including primary deterministic CI, strict-live prerequisites, verify scripts, Docker-backed storage services, compile-fail artifact cleanup, and docs gates. Use when a task needs canonical validation, strict-live verification, or repo-accurate command sequencing."
---

# ai-kit Validation Matrix

## Purpose

Provide the canonical `ai-kit` primary CI and release validation sequences,
with distinct evidence and strict-live prerequisites.

## Trigger Boundary

Use this skill when a task needs exact repo validation commands, strict-live
verification, or repo-accurate command sequencing.

## Prerequisites

- Read [references/validation.md](references/validation.md).
- Identify which crates are in scope.

## Workflow

1. Select authority from explicit user direction and the selected workflow
   before choosing validation scope. A checked-in workflow is not active merely
   because its file exists. Normally start with root `AGENTS.md`. When
   `agents-kit/.codex/development/PROCESS.md` is selected, preserve its
   agents-kit-only boundary for ordinary runs: do not run the workspace
   verifier or another crate's validation. An explicitly approved coordinated
   workspace plan owns its named cross-crate phases and gates; dependencies
   or a workflow file's presence alone never broaden scope.
2. Select the primary deterministic lane, scoped iteration, or required release
   gates explicitly. Run their exact commands and the relevant crate `AGENTS.md`
   blocks; a mock or focused pass never substitutes for full release proof.
3. Respect strict-live prerequisites, including sourced env files and service
   startup behavior.
4. Confirm compile-fail and verification flows leave crate-owned `wip/`
   directories clean.
5. Report any missing prerequisite instead of silently skipping it.

## Output Rules

- Report the exact commands used or proposed.
- Keep primary deterministic evidence separate from crate-level strict-live
  details and full workspace completion. Preserve FAIL and INCOMPLETE receipts;
  do not combine historical runs or retry a failure into a pass claim.

## References

- [references/validation.md](references/validation.md)
- [`AGENTS.md`](../../../AGENTS.md)
