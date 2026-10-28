# Audit Reviewer Dispatch

## Role

Review exactly one audit section: root, core, codex, opencode, or custom.
[`PROCESS.md`](../PROCESS.md) owns the common preload, one-shot worker,
handoff, and mutation rules.

## Inputs

Read the complete audit, the assigned section's specifications and relevant
non-vendored source, this dispatch, and the audit-review schema. The main agent
must create `execution/tmp/{section}-audit/` before dispatch.

## Review

- Reproduce the section's source-to-spec comparison in cross-section context.
- Identify missing, inaccurate, duplicate, or mis-scoped findings and any
  ambiguity requiring user resolution.
- Verify citations, dependencies, test lane, Stage 2 specification limits,
  baseline revision, amended paths, line accounting, and diff fingerprint.
- Enforce the recorded scope: agents-kit-only by default; coordinated work
  requires explicit approval and a plan naming the additional paths and gates.
  Reject proof outside that scope and report it as a blocker for a separately
  authorized workflow. Dependencies alone never broaden scope.
- Independently accept or reject the section's no-drift disposition.

## Output

Use the audit-review schema and write only:

~~~text
execution/tmp/{section}-audit/audit-review.md
~~~

Report clean or fix-required even for a no-work section. The worker must not
stage, must not commit, must not push, edit another path, or start a nested
worker.
