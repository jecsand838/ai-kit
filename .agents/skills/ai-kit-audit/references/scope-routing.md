# Scope Routing

Use this reference before auditing so the findings are scoped tightly enough
for later worktree or change-workflow execution.

## Scope Modes

- `workspace`: inspect all ai-kit crates and root docs, scripts, skills, and
  source contracts relevant to the audit topic.
- `crates`: inspect only the named crates and cross-crate surfaces they expose
  or consume.
- `crate`: inspect one named crate plus root docs or scripts only when they
  define that crate's workflow.
- `paths`: inspect only the supplied paths and any owning docs or validation
  references needed to understand them.
- `module`: inspect the named module and direct callers/callees needed to
  decide whether a finding is real.

## Crate Profiles

### `agent-sdk`

- Root: `agent-sdk/`
- Always read: root `AGENTS.md`, `agent-sdk/AGENTS.md`
- Usually relevant skills: `agent-sdk-change-contract`,
  `agent-sdk-research`, Rust best-practice skills as needed

### `provider-sdk`

- Root: `provider-sdk/`
- Always read: root `AGENTS.md`, `provider-sdk/AGENTS.md`
- Usually relevant skills: `provider-sdk-change-contract`,
  `provider-sdk-research`, Rust best-practice skills as needed

### `storage-sdk`

- Root: `storage-sdk/`
- Always read: root `AGENTS.md`, `storage-sdk/AGENTS.md`
- Usually relevant skills: `storage-sdk-change-contract`,
  `storage-sdk-research`, Rust best-practice skills as needed

### `tool-sdk`

- Root: `tool-sdk/`
- Always read: root `AGENTS.md`, `tool-sdk/AGENTS.md`
- Usually relevant skills: `tool-sdk-change-contract`,
  `tool-sdk-research`, Rust best-practice skills as needed

### `app-sdk`

- Root: `app-sdk/`
- Required instructions: root `AGENTS.md`, `app-sdk/AGENTS.md`, and owning specs
- Usually relevant skills: `ai-kit-public-surface-contract`,
  `app-sdk-research`, Rust best-practice skills as needed

## Audit Topic Routing

- Public imports, crate-root API, docs-taught paths, or module visibility:
  load `ai-kit-public-surface-contract`.
- Validation, strict-live, docs gates, or command sequencing: load
  `ai-kit-validation-matrix` and crate-local `AGENTS.md` files.
- Async lifecycle, streams, task spawning, or cancellation: load
  `use-rust-best-practices-async`.
- Networking, HTTP, gRPC, database clients, retries, or timeouts: load
  `use-rust-best-practices-networking`.
- Enum dispatch, API shape, duplicated logic, or error structure: load
  `use-rust-best-practices-abstractions`.
- Module ownership, crate boundaries, target layout, or test placement: load
  `use-rust-best-practices-architecture`.
- Dependency, feature, Cargo, or platform behavior: load
  `use-rust-best-practices-dependencies-platforms`.
- Tests, source contracts, doctests, or live test topology: load
  `use-rust-best-practices-testing`.

Use research skills only when the finding depends on external protocol,
provider, backend, or strict-live facts.
