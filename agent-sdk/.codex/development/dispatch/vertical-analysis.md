# Vertical Analysis Dispatch

## Role

Convert the complete reviewed audit into the smallest non-overlapping set of
functional verticals. [`PROCESS.md`](../PROCESS.md) owns common preload,
one-shot worker, handoff, and mutation rules.

## Inputs

Read the complete audit, every cited spec, relevant non-vendored source, this
dispatch, and the vertical-analysis schema.

## Analysis

- Assign each finding to exactly one behavior- or contract-rooted vertical.
- Order dependencies, give cross-cutting work one owner, and define each
  vertical's objective, root anchor, scope, proof lane, exclusions, overlap
  risks, and completion signal.
- Prove finding coverage and non-overlap. When the audit reports no drift,
  record zero verticals and do not invent validation work.
- Keep verticals agent-sdk-only unless an explicitly approved coordinated
  plan names the additional crate work. Any dependency outside that recorded
  scope is a blocker for a separately authorized workflow, never implicit work
  this workflow may absorb.

## Output

Use the vertical-analysis schema and write only:

~~~text
execution/tmp/planning/verticals.md
~~~

The worker must not stage, must not commit, must not push, edit another path,
or start a nested worker.
