---
name: use-rust-best-practices-testing
description: "Apply this when adding, moving, fixing, or reviewing Rust tests. Use it to choose between unit, integration, doctest, binary-smoke, and end-to-end tests; to place tests correctly in source files, crate-level tests/, or dedicated workspace e2e crates; and to separate deterministic public-contract fixtures from built-in and live-service integration evidence. Respect repository-specific primary mock lanes and use real dependencies for the infrastructure behavior being claimed."
---

# Rust Testing Best Practices

## Purpose

Guide test placement, scope, and live-dependency strategy for Rust code.

## Trigger Boundary

Use this skill when the task adds, moves, fixes, or reviews Rust tests or test
strategy.

## Prerequisites

- Load `use-rust-best-practices-architecture` when the question is mostly about
  where tests should live.
- Load `use-rust-best-practices-async` when async timing or shutdown dominates.
- Load `use-rust-best-practices-abstractions` when tests guard
  enum-dispatch trait contracts or implementation-family parity.

## Workflow

1. Prefer the narrowest test level that proves the behavior.
2. In `ai-kit`, keep production `src/**` free of `#[cfg(test)]`; express
   owner-local semantics through the owning crate's `/tests` tree. In other
   repositories, follow their local placement policy.
3. Keep integration tests under the crate being tested and use the public API.
   Ai-kit primary tests use crate-local structs and exhaustive enums implementing
   all 16 public behavioral traits directly, including App workflow values, and
   exercise the real Custom Agent/App with caller-owned lower implementations.
   Existing Tool extension tests remain separate; no extension ingress or backend
   erasure belongs in the primary lane. Neither lane proves a built-in service.
4. Add doctests for public APIs that benefit from executable examples.
5. Use existing app boundaries for full-system tests in `ai-kit`; do not add
   a new e2e or conformance crate. Outside `ai-kit`, follow the owning
   repository's policy for dedicated workspace e2e crates.
6. At the open Tool seam, use narrow source guards for private erasure and the
   sole root ingress. For Provider, Storage, and Agent, prove direct non-Clone
   caller implementations through compile/behavior tests and use source guards
   only to reject erasure, `Extension` arms, and normalization ingress.

## Output Rules

- Deterministic public-contract fixtures prove the owning public semantics,
  including direct composition, workflow execution, cancellation, conflicts,
  cleanup, and redaction. Preserve exact associated children, non-Clone owners,
  continuation bounds, and App's local/borrowed/non-Unpin concrete child future.
  Keep Tool extension routing/child normalization in its existing separate lane.
- Use [the primary mock contract](../../../docs/mock-testing.md) for fixture
  integrity, complete inventories, runtime qualification, and evidence rules.
  Same-process reconstruction never proves native durability.
- Integration tests should not mock dependencies they claim to test.
- E2E tests should validate the external contract, not internal details.
- Source guards may reject prohibited erasure, macro-generated replacements,
  catch-all dispatcher arms, and stale restoration TODO markers; they must not
  freeze private names, files, impl placement, or match-arm spelling.
- Strict-live tests use real built-ins and real configured services; mocks,
  recordings, scripted servers, and deterministic substitutes never satisfy a
  strict-live claim.

## References

- `references/testing.md`
- `assets/templates/unit_test_with_fake.rs`
- `assets/templates/bin_smoke_test.rs`
- `assets/templates/doctest_examples.rs`
