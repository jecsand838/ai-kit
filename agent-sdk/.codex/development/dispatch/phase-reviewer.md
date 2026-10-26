# Phase Reviewer Dispatch

## Role

Review one completed implementation phase and one numbered review cycle.
[`PROCESS.md`](../PROCESS.md) owns common preload, one-shot worker, handoff,
marker, and review-loop rules.

## Inputs

Read the audit, immutable plan, vertical analysis and research, owning specs,
remaining phase markers, complete phase diff, affected files, and recorded
validation. The main agent creates the phase-review parent before dispatch.

## Review

- Prove every phase task is implemented in the planned vertical and ordered
  test, deletion, extension, then addition.
- Review correctness, errors, ownership, async safety, backend boundaries,
  public-surface impact, maintainability, and cleanup.
- Verify every extension and addition has its linked test. Verify every
  behavior-changing deletion has linked test evidence proving the retained
  behavior or intentional removal.
- Require meaningful pre-production red evidence, distinguishing compile-red,
  reached behavioral red and already-passing missing-proof additions.
- Keep `source-contract` and `compile-contract` proof distinct from deterministic
  `public-contract` and real `strict-live`. Direct foreign implementations prove
  only Agent-owned behavior; reject substitutes for built-in/service proof and
  any validation weakening.
- Enforce the recorded scope: agent-sdk-only by default, or only additional
  paths/packages/gates named by an explicitly approved coordinated plan. Reject
  commands or evidence outside it; transitive compilation and Agent strict-live
  use of real dependencies do not authorize broader validation.
- Verify immutable inputs and marker text are unchanged. Every whole-file
  deletion target must be absent while its surviving marker host remains.
- For the last phase, ordinary runtime completion requires
  `bash agent-sdk/scripts/verify_strict_live.sh`. Accept additional gates only
  from the explicitly approved coordinated plan. Documentation-only work uses
  PROCESS Section 1's non-live gates and makes no runtime proof claim.

## Output

Use the phase-review schema and write only:

~~~text
execution/tmp/phase-reviews/phase-{phase-count}/review-{review-count}.md
~~~

Return clean only with no remaining finding. The worker must not stage, must
not commit, must not push, edit another path, or start a nested worker.
