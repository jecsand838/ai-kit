---
name: rust-code-structuring-best-practices
description: "Apply this when structuring, refactoring, reviewing, or planning Rust code around domain structs, enums, impl blocks, receiver methods, associated constructors, builders, and strict standalone-function removal. Use when a task involves primitive-soup parameters, repeated argument groups, stringly typed state, Option-heavy state, god structs, enum variants carrying data, moving behavior from free functions to methods, or enforcing that standalone functions are off-limits except for main or explicit human-written spec carveouts."
---

# Rust Code Structuring Best Practices

## Purpose

Use Rust's type system to make domain ownership explicit. Group data into
meaningful structs, model mutually exclusive cases with data-carrying enums,
and put behavior on the type that owns the invariant.

This workspace is intentionally stricter than general Rust guidance:
standalone functions and loose private helpers are off-limits except for
`main` or an exact human-written spec carveout.

## Functional Domain Field Privacy

Struct fields are private by default. Any non-private field visibility,
including `pub`, `pub(crate)`, `pub(super)`, and `pub(in ...)`, is allowed only
at an intentional DTO boundary.

A DTO is passive request, response, config, protocol, serialization, or
persistence data. It must not own a runtime resource, synchronization,
lifecycle transition, or domain behavior. DTO methods are limited to
construction, validation, serialization or conversion, and simple accessors.

Non-DTO domains keep fields private and expose state and invariants through
receiver methods or the owning trait contract. Public behavioral traits are
permitted only at the eleven lower-crate backend seams and five App workflow
seams enumerated by root `AGENTS.md`; internal dispatch remains private.
App's `ChildExecutor` associated-future and serialization-only field/tag
registry exceptions do not authorize another trait or erased runtime owner.
Cross-module field access is not an acceptable substitute for a domain interface. The purpose is
to preserve functional and contextual ownership boundaries, not merely to
apply field encapsulation mechanically.

Apply this rule immediately to all new or modified structs. Existing
violations remain existing production debt and must be cleaned up in the
production vertical that owns them. Documentation-only work may add the rule
and its source contract without claiming current-source conformance.

## Core Rules

1. Put read-only behavior on `&self`, mutation on `&mut self`, and consuming
   transitions on `self`.
2. Put construction on `Type::new`, `Type::build`, `Type::from_*`, `TryFrom`,
   or a builder owned by the domain.
3. Replace repeated argument groups and primitive soup with named domain or
   DTO structs.
4. Replace string tags, boolean modes, and mutually exclusive `Option<T>`
   fields with data-carrying enums.
5. Split god structs by responsibility, lifecycle, invariant, and method
   surface. Do not split one logical object into a same-root suffix family.
6. Never use Rust item type aliases. Use the concrete type, a real
   struct/newtype, a data-carrying enum, or a trait associated type.
7. Keep free-standing helpers out of library code. If logic seems neutral,
   find the smallest honest struct, enum, builder, or real dispatch trait that
   owns it. General Rust examples permitting tiny helpers do not override this
   workspace rule.
8. Before widening a field, prove the owner is a passive DTO and its methods
   stay within the DTO limits above.

## Lifecycle And State Modeling

Do not create `FooStarted`, `FooCompleted`, `FooFailed`, `FooCancelled`,
`FooInputs`, `FooExecution`, `FooPlan`, `FooTask`, or similar carriers for one
logical object. Repeated identity fields across those shapes indicate a
missing owner and state enum.

Bad:

```rust
pub struct AgentRunStarted {
    id: AgentRunId,
    started_at: SystemTime,
}

pub struct AgentRunCompleted {
    id: AgentRunId,
    started_at: SystemTime,
    output: AgentOutput,
}
```

Good:

```rust
pub struct AgentRun {
    id: AgentRunId,
    state: AgentRunState,
}

pub enum AgentRunState {
    Started { started_at: SystemTime },
    Completed {
        started_at: SystemTime,
        output: AgentOutput,
    },
    Failed {
        started_at: SystemTime,
        error: KitError,
    },
}

impl AgentRun {
    pub fn complete(self, output: AgentOutput) -> Self {
        let started_at = match self.state {
            AgentRunState::Started { started_at } => started_at,
            AgentRunState::Completed { started_at, .. }
            | AgentRunState::Failed { started_at, .. } => started_at,
        };
        Self {
            state: AgentRunState::Completed { started_at, output },
            ..self
        }
    }
}
```

State enum variants such as `AgentRunState::Started` are valid. Same-root
carrier structs such as `AgentRunStarted` are the prohibited shape.

## Domain And DTO Boundaries

Public fields are acceptable only when the type is intentionally passive:

```rust
// Intentional caller request DTO.
pub struct RunRequest {
    pub user: UserId,
    pub prompt: String,
}

// Runtime domain: fields remain private.
pub struct AgentRun {
    request: RunRequest,
    steps: Vec<AgentStep>,
}

impl AgentRun {
    pub fn record(&mut self, step: AgentStep) {
        self.steps.push(step);
    }

    pub fn steps(&self) -> &[AgentStep] {
        &self.steps
    }
}
```

A DTO that acquires tasks, locks, channels, sockets, open files, lifecycle
transitions, orchestration, or rich domain methods is no longer a DTO. Move
that behavior and state to an opaque owner instead of widening more fields.

## Enum Modeling

Bad tag-plus-options state:

```rust
pub struct ToolStatus {
    status: String,
    output: Option<String>,
    error: Option<String>,
}
```

Good closed state:

```rust
pub enum ToolStatus {
    Pending,
    Running,
    Succeeded(String),
    Failed(ToolError),
}

impl ToolStatus {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Succeeded(_) | Self::Failed(_))
    }
}
```

Use an enum when exactly one known case is active. At the approved Tool public
seam, retain concrete variants and add one private erased extension variant
behind the public trait. Use a closed built-in dispatcher plus direct static
caller implementations for the nine Provider, Storage, and Agent traits. Use
a separate flag-set model when cases may coexist.
Use a trait only for a real
polymorphic or closed dispatch contract, not to make unrelated types look
symmetrical.

## Refactoring Workflow

1. Name the domain owners and passive DTO boundaries.
2. Group repeated parameters and validate them at construction.
3. Convert stringly or option-heavy state into data-carrying enums.
4. Collapse same-object suffix families into one owner plus state enum.
5. Move construction and behavior onto receiver or associated methods.
6. Replace cross-module field access with the smallest honest domain method.
7. Delete type aliases and loose helpers rather than routing through them.
8. Review field visibility, ownership, borrowing, async boundaries, tests, and
   docs before accepting the structure.

## Research Reference

`references/structs-enums-receiver-methods.md` is background source material,
not workspace policy and not a byte-identical copy of this skill. Its
permissive standalone-function discussion and unlabelled public-field examples
are superseded by the hard rules above.

## Companion Skills

- Load `use-rust-best-practices-core` for any Rust task.
- Load `use-rust-best-practices-abstractions` for traits, generics,
  enum-dispatch, errors, or macro-free duplication removal.
- Load `use-rust-best-practices-architecture` when module, crate, or package
  structure dominates.
- Load the async, networking, dependency/platform, or testing Rust skill when
  that concern dominates the design.
