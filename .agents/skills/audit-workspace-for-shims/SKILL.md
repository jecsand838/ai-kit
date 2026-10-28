---
name: audit-workspace-for-shims
description: "Audit one target ai-kit crate for hacks, shims, synthetic behavior, strict-live output shaping, and non-live expectations that lack strict-live or official-doc proof. Use when the user asks for a shim audit, synthetic fallback review, strict-live shaping review, or unproven test-assumption audit for `agents-kit`, `providers-kit`, `storage-kit`, `tools-kit`, or `app-kit`, not when they already want the follow-up removal pass."
---

# Audit Workspace For Shims

## Purpose

Produce a findings-first shim audit for one target crate and annotate confirmed
issues for deletion when edits are allowed.

## Trigger Boundary

Use this skill for explicit shim, hack, synthetic-behavior, or strict-live
output-shaping audits of `agents-kit`, `providers-kit`, `storage-kit`,
`tools-kit`, or `app-kit`.
Do not use it for the follow-up removal pass. Redirect follow-up cleanup to
`remove-workspace-shims`.
If the target crate is missing or unknown, ask for the missing target crate
instead of inferring one.

## Prerequisites

- Inspect the actual crate state, docs, tests, and validation scripts before
  judging anything.
- Read [references/routing.md](references/routing.md).
- Read [references/examples.md](references/examples.md).
- Always load `use-rust-best-practices-core`,
  `ai-kit-validation-matrix`, and `strict-live-audit`.
- Load the matching crate `*-change-contract` skill before judging production,
  docs, tests, or public-surface fallout.
  For App, read its `AGENTS.md` and owning specs and load
  `ai-kit-public-surface-contract`.
- Load the matching research skill only when upstream server, provider, or
  backend facts matter to the audit conclusion.
- If edits are not allowed, including Plan Mode or read-only execution, keep
  the audit honest and do not claim the deletion markers were written.

## Workflow

1. Scope the audit to the requested crate only.
2. Audit production code for hacks, shims, deterministic outputs, silent
   fallbacks, synthetic ids, synthetic paths, synthetic text, compatibility
   rewrites, and output shaping.
3. Audit non-live tests for provider, backend, or server expectations that do
   not have strict-live or official-doc proof.
4. Audit strict-live helpers and assertions for reshaping, inferred ids,
   substituted events, downgraded failures, or forced expected shapes.
5. Annotate every confirmed issue with language-appropriate comment syntax and
   the marker `DELETE(<crate> shim audit): <reason>` when edits are allowed.
6. When edits are not allowed, return the same findings and say the deletion
   markers were not written because mutation was unavailable.
7. Return findings first, ordered by impact, with concrete file references and
   a short summary of what should be removed next.

## Output Rules

- No implementation plan, no removal pass, and no cleanup strategy beyond the
  audit markers.
- Report findings first, ordered by impact.
- Say `no material shim findings found` when the target crate is clean.
- Say `deletion markers were not written` when edits were not allowed.
- Never pretend read-only or Plan Mode runs wrote `DELETE(<crate> shim audit)`
  markers.

## References

- [references/routing.md](references/routing.md)
- [references/examples.md](references/examples.md)
