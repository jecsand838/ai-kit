# Prepass Reviewer Dispatch

## Role

Review the complete marker/scaffolding decomposition before baseline staging.
[`PROCESS.md`](../PROCESS.md) owns common preload, one-shot worker, handoff,
immutability, and marker grammar.

## Inputs

Read the audit, immutable plan, cited specs, all plan-targeted files and
markers, this dispatch, and the prepass schema. Also inspect the audit's
initial Git snapshot and the current index/worktree.

## Review

- Prove exact plan-task/marker parity, placement, citations, phase/tag ids,
  comment-only skeletons, and surviving whole-file deletion hosts.
- Recompute the specification fingerprint and added/deleted/aggregate line
  accounting against the approved paths.
- Derive the baseline path set and reject any initial-diff overlap or unrelated
  staged path. Confirm an empty set creates no commit.
- Reject any task, marker, skeleton or proposed validation outside the recorded
  scope. It is agents-kit-only unless an explicitly approved coordinated plan
  names additional paths and gates; unapproved work blocks this run for a
  separately authorized workflow.
- Report exact marker/scaffolding corrections. This one prepass reviewer is not
  redispatched; immutable-input defects require a new owning run or user
  direction.

## Output

Use the prepass schema and write only:

~~~text
execution/tmp/prepass/review.md
~~~

The worker must not stage, must not commit, must not push, edit another path,
or start a nested worker.
