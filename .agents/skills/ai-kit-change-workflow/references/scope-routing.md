# Scope Routing

Use this reference before running any phase.

## Scope Modes

- `workspace`: all ai-kit crates plus root docs, scripts, skills, source
  contracts, and validation.
- `crates`: multiple named crates and their shared public surfaces.
- `crate`: one named crate and root-owned docs or scripts that define its
  workflow.
- `paths`: only supplied paths and their direct owning docs or tests.
- `module`: one module plus direct callers/callees needed for correctness.

## Scope Contract

For every run, establish:

- allowed paths
- forbidden paths
- target crate or crates
- task type
- phase
- TODO marker

If implementation requires work outside the allowed paths, record a dependent
issue and stop or ask the orchestrator to expand scope.

## Crate Profiles

### `agents-kit`

- Root: `agents-kit/`
- Required instructions: root `AGENTS.md`, `agents-kit/AGENTS.md`
- Usually relevant skills: `agents-kit-change-contract`,
  `agents-kit-research`

### `providers-kit`

- Root: `providers-kit/`
- Required instructions: root `AGENTS.md`, `providers-kit/AGENTS.md`
- Usually relevant skills: `providers-kit-change-contract`,
  `providers-kit-research`

### `storage-kit`

- Root: `storage-kit/`
- Required instructions: root `AGENTS.md`, `storage-kit/AGENTS.md`
- Usually relevant skills: `storage-kit-change-contract`,
  `storage-kit-research`

### `tools-kit`

- Root: `tools-kit/`
- Required instructions: root `AGENTS.md`, `tools-kit/AGENTS.md`
- Usually relevant skills: `tools-kit-change-contract`, `tools-kit-research`

### `app-kit`

- Root: `app-kit/`
- Required instructions: root `AGENTS.md`, `app-kit/AGENTS.md`, and owning specs
- Usually relevant skills: `ai-kit-public-surface-contract`,
  `app-kit-research`, Rust best-practice skills as needed

## Rust Skill Routing

Always consider `use-rust-best-practices-core` for Rust tasks.

Add:

- `use-rust-best-practices-testing` for tests, doctests, source contracts, or
  validation strategy.
- `use-rust-best-practices-abstractions` for enum dispatch, API shape,
  duplicated logic, errors, traits, or lifetimes.
- `use-rust-best-practices-architecture` for crate boundaries, module
  ownership, test placement, or target layout.
- `use-rust-best-practices-async` for async functions, streams, task spawning,
  locks, cancellation, or shutdown.
- `use-rust-best-practices-networking` for HTTP, gRPC, database clients,
  retries, timeouts, pools, or backpressure.
- `use-rust-best-practices-dependencies-platforms` for Cargo, features,
  dependencies, paths, processes, or platform behavior.
