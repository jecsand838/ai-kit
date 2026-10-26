---
name: remove-workspace-shims
description: "Plan and remove accepted shim-audit findings for one target ai-kit crate with a delete-first, strict-live-first workflow. Use when the user explicitly wants the follow-up shim-removal pass for `agent-sdk`, `provider-sdk`, `storage-sdk`, `tool-sdk`, or `app-sdk` after an audit has already produced accepted findings or `DELETE(crate shim audit)` markers. Prefer Plan Mode; do not use for fresh audits or generic cleanup requests."
---

# Remove Workspace Shims

## Purpose

Turn accepted shim findings for one target crate into a delete-first,
strict-live-first repair pass.

## Trigger Boundary

Use this skill only for explicit follow-up shim removal on `agent-sdk`,
`provider-sdk`, `storage-sdk`, `tool-sdk`, or `app-sdk` after an audit already produced
accepted findings or `DELETE(<crate> shim audit)` markers.
Do not use it for a fresh audit, generic cleanup request, or vague request to
remove technical debt.
If the target crate is missing or unknown, ask for the missing target crate
instead of inferring one.

## Prerequisites

- Start from accepted findings or `DELETE(<crate> shim audit)` markers, not a
  fresh audit.
- Read [references/routing.md](references/routing.md).
- Read [references/examples.md](references/examples.md).
- Always load `use-rust-best-practices-core` and
  `use-rust-best-practices-testing`.
- Load the matching crate `*-change-contract` skill before touching code,
  tests, docs, or validation flows.
  For App, read its `AGENTS.md` and owning specs and load
  `ai-kit-public-surface-contract`.
- Load the matching research skill only when upstream server, provider, or
  backend facts matter to the fix.
- In Plan Mode, keep the initial phase non-mutating and produce the
  decision-complete removal plan first.
- Outside Plan Mode, execute only when the accepted audit or plan already
  exists in the thread; otherwise stop at the plan and ask for the planning
  phase first.

## Workflow

1. Start from `DELETE(<crate> shim audit)` markers or accepted findings, not a
   fresh audit.
2. In Plan Mode, do the non-mutating grounding pass and write the
   decision-complete removal plan.
3. After the thread leaves Plan Mode, delete every marked shim in an initial
   pre-pass before any repair work.
4. Run the target crate's strict source-of-truth suites first from its
   `AGENTS.md`; extend strict-live coverage until every removed shim has failing
   proof, or delete the conflicting non-live tests when that proof is
   impossible.
5. Fix failures TDD-style with honest production behavior only; prefer
   `UnsupportedCapability`, `UnsupportedByModel`, or `not implemented` instead
   of new shims.
6. Align or delete conflicting unit, source-contract, docs, or validation
   tests.
7. Run the exact target-crate completion gate from its `AGENTS.md`.

## Output Rules

- No new shims, no synthetic behavior, no output shaping, and no provider or
  backend leakage into public APIs.
- Prefer explicit unsupported or not-yet-implemented errors when honest
  portability does not exist.
- If there are no accepted audit findings or markers, redirect to
  `audit-workspace-for-shims`.
- Preserve the honest Plan Mode split: Plan Mode is for the decision-complete
  removal plan; execution begins only after the thread leaves Plan Mode.

## References

- [references/routing.md](references/routing.md)
- [references/examples.md](references/examples.md)
