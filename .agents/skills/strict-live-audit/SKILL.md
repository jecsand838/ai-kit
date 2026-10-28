---
name: strict-live-audit
description: "Audit ai-kit strict-live suites for public /tests placement, real-service execution, feature-correct validation commands, fail-fast env handling, and docs or script alignment without skips or mocks. Use when changing live tests, strict env contracts, verification scripts, or README and AGENTS validation guidance."
---

# Strict Live Audit

## Purpose

Audit strict-live topology, env contracts, docs, and scripts for one or more
`ai-kit` crates.

## Trigger Boundary

Use this skill when changing live tests, strict env contracts, verification
scripts, or README and `AGENTS.md` validation guidance.

## Prerequisites

- Load `ai-kit-validation-matrix`.
- Load the relevant crate `*-change-contract` skill. For App, read its
  `AGENTS.md` and owning specs and load `ai-kit-public-surface-contract`.

## Workflow

1. Confirm every canonical command uses its owning crate's supported feature
   lane. Use `--all-features` only where the crate contract permits it; keep
   Storage Lance-only plus explicit supported alternative lanes; never use
   Storage `--all-features`. Verify actual nonempty feature-selected inventories.
2. Confirm strict-live suites live under crate `/tests`.
3. Confirm strict-live suites use real services and fail on missing env instead
   of skipping.
4. Confirm strict-live helpers and assertions do not normalize, infer, reshape,
   downgrade, case-fold, substring-match, substitute event types, infer ids, or
   rewrite provider output into an expected shape.
5. Confirm non-live tests do not assert backend/provider behavior unless that
   behavior is proven by strict-live, official docs, or vendored schemas.
6. Confirm unproven local substitute-service expectations are deleted or
   refactored into crate-local invariants; never preserve them with production
   compatibility code.
7. Confirm scripts and docs name the real test targets and prerequisite steps.
8. Confirm test-mode selection happens through explicit test targets and
   scripts, not Cargo features.
9. Confirm production code does not keep silent success fallbacks, synthetic
   portable data, compatibility shims, or deterministic provider behavior that
   exists only to satisfy tests.
10. Confirm honestly unmappable behavior surfaces the crate's explicit
    unsupported or not-yet-implemented error instead of a fabricated portable
    event, response, id, status, text, output, summary, or capability.
11. Confirm known Codex/OpenCode events are decoded into typed backend event
    enums and exhaustively classified before portable mapping. Strict-live
    should prove typed portable events, honest unsupported errors for behavior
    proved impossible to represent portably, or honest not-yet-implemented
    stream items, never a generic raw, silent no-event, or known-event
    catch-all.
12. Confirm crate docs, workspace docs, and source-contract tests agree.
13. Preserve the exact qualifications in root `AGENTS.md` and the owning strict
    guides: seven explicitly selected Provider batch exclusions, two named
    OpenCode Question cases and two named OpenAI Realtime cases with separate
    raw-copy grades, and Provider's narrowly eligible File Search and retention
    evidence limits. OpenCode requires independently observed native/portable
    terminal fidelity and unchanged actual Question descriptions; answer and
    each description copy grade are separately required. Realtime requires post-result audio and nonblank
    transcription but does not independently prove native parity or semantic
    result use. Both retain blocking scenario, correlation, terminal, cleanup
    and valid-receipt checks. Report all untouched whole-string grades, including
    nonblocking failures, without claiming model-quality acceptance or reliability.
    Do not extend any exception to another case, model, operation, or failure;
    knowledge, pending-interaction recovery and all other exact-output assertions
    remain blocking.

## Output Rules

- No mocks, deterministic shims, recorded responses, or hidden bypasses in
  strict-live suites.
- No output reshaping, inferred ids, substituted events, expected-shape
  rewrites, or hidden normalization in strict-live helpers or assertions.
- No output-shaping helpers or non-live backend expectations that make
  unproven provider behavior look portable.
- No unproven local substitute-service expectations, non-live-only behavior
  contracts, synthetic portable outputs, or silent success fallbacks justified
  by tests.
- No production hacks, shims, compatibility fallbacks, or deterministic
  provider behavior created to satisfy tests.
- No test updates that force live provider output into an expected shape.
- No strict-live acceptance of generic raw fallback for valid known
  Codex/OpenCode provider events.
- Prefer deleting unproven non-live tests over adding production behavior that
  exists only to make those tests pass.
- No hidden `#[ignore]` behavior for the canonical strict targets unless docs
  and scripts explicitly require `--ignored`.

## References

- `ai-kit-validation-matrix`
- [`AGENTS.md`](../../../AGENTS.md)
