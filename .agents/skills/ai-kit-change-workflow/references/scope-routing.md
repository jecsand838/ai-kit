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

### `agent-sdk`

- Root: `agent-sdk/`
- Required instructions: root `AGENTS.md`, `agent-sdk/AGENTS.md`
- Usually relevant skills: `agent-sdk-change-contract`,
  `agent-sdk-research`

### `provider-sdk`

- Root: `provider-sdk/`
- Required instructions: root `AGENTS.md`, `provider-sdk/AGENTS.md`
- Usually relevant skills: `provider-sdk-change-contract`,
  `provider-sdk-research`

### `storage-sdk`

- Root: `storage-sdk/`
- Required instructions: root `AGENTS.md`, `storage-sdk/AGENTS.md`
- Usually relevant skills: `storage-sdk-change-contract`,
  `storage-sdk-research`

### `tool-sdk`

- Root: `tool-sdk/`
- Required instructions: root `AGENTS.md`, `tool-sdk/AGENTS.md`
- Usually relevant skills: `tool-sdk-change-contract`, `tool-sdk-research`

### `app-sdk`

- Root: `app-sdk/`
- Required instructions: root `AGENTS.md`, `app-sdk/AGENTS.md`, and owning specs
- Usually relevant skills: `ai-kit-public-surface-contract`,
  `app-sdk-research`, Rust best-practice skills as needed

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
