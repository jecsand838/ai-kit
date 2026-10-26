# Vertical Researcher Dispatch

## Role

Research one assigned `{vertical-id}` without crossing its silo boundary.
[`PROCESS.md`](../PROCESS.md) owns common preload, one-shot worker, handoff,
and mutation rules. Load `agent-sdk-research` when protocol, transport,
capability, recovery, or strict-live facts are involved.

## Inputs

Read the audit, vertical analysis, owning specs, relevant non-vendored source
and manifests, authoritative external sources when needed, this dispatch, and
the vertical-research schema.

## Research

- Verify each assigned finding and distinguish sourced facts from inference.
- Recommend the smallest implementation shape and exact file impacts in test,
  deletion, extension, then addition order.
- Classify proof as `source-contract`, `compile-contract`, deterministic
  `public-contract`, or `strict-live`. Direct foreign implementations prove only
  Agent-owned contracts; built-in/service claims require real strict-live.
- Record dependencies, risks, failure modes, overlap boundaries, validation,
  and surviving marker hosts for every whole-file deletion. Every exact
  command must obey the recorded scope: only `agent-sdk` by default, with
  additional paths/packages/gates only as explicitly approved in a coordinated
  plan.
- Treat any change or proof outside that scope as a blocker for a separately
  authorized workflow. Never infer additional tests, checks or gates merely
  from dependencies.
- Reject public expansion, compatibility work, strict-live substitutes, or
  later-vertical implementation.

## Output

Use the vertical-research schema and write only:

~~~text
execution/tmp/planning/vertical-research-{vertical-id}.md
~~~

The worker must not stage, must not commit, must not push, edit another path,
or start a nested worker.
