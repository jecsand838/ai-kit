---
name: use-rust-best-practices-abstractions
description: "Apply this when refactoring Rust code to remove duplication, designing or extending structs and methods, choosing between concrete code, generics, traits, trait objects, or enums, handling errors and panics, introducing or simplifying lifetimes, or implementing enum-dispatcher style backends. Use it when Codex must decide whether to keep code concrete, extract a reusable abstraction, or redesign an API to be more idiomatic and maintainable."
---

# Rust Abstractions Best Practices

## Purpose

Guide decisions about duplication removal, API shape, polymorphism, lifetimes,
errors, and safe abstraction boundaries.

## Trigger Boundary

Use this skill when a Rust task changes APIs, data structures, traits,
generics, lifetimes, error types, panic policy, or duplicated domain logic.

## Prerequisites

- Load `rust-code-structuring-best-practices` first when the task is primarily
  about struct and enum organization, receiver methods, associated
  constructors, primitive soup, repeated parameter groups, stringly typed
  state, or eliminating standalone functions and loose helpers.
- Load `use-rust-best-practices-architecture` when the task is mostly about
  package or module structure.
- Load `use-rust-best-practices-async` or
  `use-rust-best-practices-networking` when runtime behavior dominates.
- Read `references/enum-dispatch-trait-pattern.md` before adding, removing,
  or refactoring enum-dispatch behavior.

## Workflow

1. Keep code concrete until duplication or extension pressure is real.
2. Refactor repeated data plus behavior into structs with methods.
3. Do not replace duplication with Rust item type aliases or same-root
   stateful suffix carrier structs. Collapse repeated lifecycle/state carriers
   into one owner struct plus a data-carrying state enum, introduce a real
   newtype/struct only when it owns invariants, or use trait associated types
   only inside trait/impl contracts.
4. For `ai-kit` reductive refactors, do not replace duplication with
   `macro_rules!`, proc-macro indirection, generated dispatch, generated
   tables, or codegen-style hidden abstraction.
   The single exception is `tools-kit::tool!`, which is a public typed-tool
   ergonomic helper and not a reductive-refactor abstraction.
5. For `ai-kit` enum dispatch over agent, provider, provider-model, database,
   tool-runtime, or tool-source implementation families, keep one hand-written
   provider-agnostic behavioral contract. At the eleven root-approved
   lower-crate backend seams it is the public RPITIT trait; other closed
   families keep a private contract. App's five workflow traits remain static;
   `ChildExecutor` alone uses the exact GAT future in the App specification.
   Only App's field/tag serialization registry may erase codec functions;
   runtime values and lower backends must not enter it. The dispatcher must
   route through that trait and must not rely on
   inherent-method parity by convention. The two approved open Tool
   dispatchers retain concrete built-ins plus one private erased `Extension`
   variant. The nine Provider, Storage, and Agent dispatchers retain concrete
   built-ins only; callers implement those traits statically or define their
   own exhaustive enum hierarchies.
   Shared contract modules own only provider-agnostic trait definitions,
   shared handles, and
   default helpers; concrete impls live in the owning implementation subtree.
   Public root handles should be opaque structs over private inner enums whose
   variants wrap concrete structs. This struct wrapper rule is only for
   exported dispatchers; crate-private internal dispatch enums can remain plain
   private enums. Private dispatch and config-selection enums are routing
   mechanisms only; do not reuse them as implementation-family identity,
   provenance, capability, diagnostic-label, or error-label helpers.
   Dispatcher methods should use explicit exhaustive `match` arms that
   delegate with receiver-method syntax on values implementing the same trait.
   Prefer
   `backend.method(args).await`; `Trait::method(backend, args).await` is an
   invalid dispatch shape in this workspace. Put root-handle behavior in its
   owning trait implementation; callers import that trait to use its methods.
   Do not add same-name inherent forwarding methods.
6. Introduce traits at the usage boundary when multiple implementations or test
   doubles are genuinely useful.
7. Choose enums for closed sets, generics for static polymorphism, and trait
   objects for open runtime-extensible sets.
8. Use explicit lifetimes only when the borrow relationship cannot be elided.
9. Return `Result` for recoverable failures and reserve panic for bugs or truly
   impossible invariants.

## Output Rules

- Avoid speculative traits or generics.
- Prefer concrete structs, closed enums, typed data, helper methods, and
  inherent methods over macro-based deduplication in this workspace.
- Reject item type aliases and lifecycle/state suffix carrier families as
  abstraction mechanisms. They are guardrail bypasses, not simplifications.
- Preserve mandatory private enum dispatch; do not delete, bypass, publicize,
  or macro-generate it. Publicize a behavioral trait only at a root-approved
  seam and delete the superseded duplicate private trait.
- Require every concrete variant payload, the private dispatcher enum, and the
  exported root handle to implement the same owning dispatch trait. The
  private enum owns the exhaustive receiver-syntax match; the handle delegates
  through that enum or its runtime owner instead of creating a second match
  path.
- Reject reusable implementation-family kind/provenance helpers such as
  backend/provider/source display-name utilities, capability-label routers, or
  diagnostic/error labels. Backend-specific facts belong in the owning
  concrete payload or documented data/config type, not in a shared shortcut.
- Keep constructors and other legitimate inherent domain methods, but do not
  duplicate trait behavior with same-name inherent forwarders. Root-handle
  trait implementations delegate through the private dispatcher or its runtime
  owner; payload dispatch arms retain receiver-method calls.
- Keep concrete enum-dispatch impls in the owning backend, provider, model,
  database, runtime, or source subtree instead of the shared contract module.
- Treat `tools-kit::tool!` as the only macro exception; do not generalize it
  into dispatch, routing, source, schema, test, or docs-contract patterns.
- In `ai-kit`, reject `unsafe` entirely. Outside repositories with that ban,
  keep unavoidable `unsafe` small and wrap it behind safe APIs.

## References

- `references/abstractions.md`
- `references/enum-dispatch-trait-pattern.md`
- `assets/templates/enum_dispatch.rs`
- `assets/templates/typed_error.rs`
