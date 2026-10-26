---
name: use-rust-best-practices-architecture
description: "Apply this when creating or restructuring Rust packages, workspaces, crates, modules, lib.rs/main.rs boundaries, target layout, or test placement. Use it when deciding whether to add a new crate or extend an existing one, when choosing between src/bin and a separate package, when replacing outdated module layouts, and when organizing code to minimize clone-heavy flows and borrow-checker friction."
---

# Rust Architecture Best Practices

## Purpose

Guide Rust package, crate, module, target, and test-tree ownership decisions.

## Trigger Boundary

Use this skill when the task changes project layout, crate boundaries, module
structure, binary/library boundaries, or test placement.

## Prerequisites

- Load `use-rust-best-practices-testing` when the task is mostly about test
  implementation details.
- Load `use-rust-best-practices-abstractions` when the task is mostly about
  trait, generic, error, or lifetime design.
- Read `use-rust-best-practices-abstractions/references/enum-dispatch-trait-pattern.md`
  before moving enum-dispatch contract modules or implementation subtrees.

## Workflow

1. Keep binaries thin and move reusable behavior into libraries.
2. Organize by functional domain and behavior, not vague buckets.
3. Add a new crate only when it introduces a real ownership, dependency,
   deployment, reuse, or release boundary.
4. Put tests at the narrowest level that still exercises the behavior you need.
5. Prefer clone-minimizing ownership transitions at parsing, persistence,
   network, and task-spawn edges.
6. For enum-dispatch contract modules, keep shared modules provider-agnostic
   and put concrete trait impls in the owning implementation subtree.
   Dispatcher modules keep explicit exhaustive `match` arms and no wildcard or
   catch-all arms for implementation-family variants. Match arms call
   receiver methods on payloads implementing the same trait; do not use
   dispatch-contract UFCS as a structural shortcut. Public root handle methods
   may use `<Self as Trait>::method(self, ...)` only to bridge into the
   handle's own owning trait impl. At the root-approved Tool seam, the public
   RPITIT trait is the contract and one private erased adapter serves only the
   enum's `Extension` arm. Provider, Storage, and Agent public RPITIT traits
   are direct static contracts and their supplied dispatchers remain closed
   over built-ins. App's five workflow traits retain concrete runtime values;
   `ChildExecutor` alone uses its exact associated-future contract, and only
   field/tag serialization codecs may enter its erased registry. Neither
   exception widens lower-crate contracts. Only exported dispatchers need an
   opaque
   public struct wrapper; private internal dispatch enums can remain plain
   private enums, but they must not become reusable implementation-family
   identity, provenance, capability, diagnostic-label, or error-label helpers.
7. Keep implementation-family matches over provider backends, agent backends,
   database backends, or toolset runtime/source families in root `{crate}/src`
   dispatcher files only. Shared core modules, contract modules, and helper
   subtrees may match ordinary domain enums and state-machine enums, but must
   not match across different backend families.

## Output Rules

- Prefer one coherent domain crate over many tiny pseudo-layer crates.
- Use `src/bin/` before introducing a separate package for small extra binaries
  that share the same domain and dependencies.
- Do not centralize backend-, provider-, model-, database-, runtime-, or
  source-specific enum-dispatch impls in shared contract modules.
- Do not write enum-dispatch arms as `Trait::method(receiver, args)`; fix the
  receiver/type structure so `receiver.method(args)` resolves to the owning
  public-or-private trait method.
- Keep `<Self as Trait>::method(self, ...)` limited to root public handles;
  do not use it inside payload-dispatch match arms.
- Do not add shared backend/provider/source kind or provenance utilities as
  shortcuts around module ownership; route behavior through the owning trait
  (public only at a seam enumerated by root `AGENTS.md`) and keep
  implementation facts in the owning subtree.
- Do not hide missing implementation-family variants behind wildcard or
  catch-all dispatcher arms.

## References

- `references/architecture.md`
- `assets/templates/workspace-root/Cargo.toml`
- `assets/templates/thin-lib-bin/src/lib.rs`
- `assets/templates/thin-lib-bin/src/main.rs`
