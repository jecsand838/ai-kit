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

### `agents-kit`

- Root: `agents-kit/`
- Always read: root `AGENTS.md`, `agents-kit/AGENTS.md`
- Usually relevant skills: `agents-kit-change-contract`,
  `agents-kit-research`, Rust best-practice skills as needed

### `providers-kit`

- Root: `providers-kit/`
- Always read: root `AGENTS.md`, `providers-kit/AGENTS.md`
- Usually relevant skills: `providers-kit-change-contract`,
  `providers-kit-research`, Rust best-practice skills as needed

### `storage-kit`

- Root: `storage-kit/`
- Always read: root `AGENTS.md`, `storage-kit/AGENTS.md`
- Usually relevant skills: `storage-kit-change-contract`,
  `storage-kit-research`, Rust best-practice skills as needed

### `tools-kit`

- Root: `tools-kit/`
- Always read: root `AGENTS.md`, `tools-kit/AGENTS.md`
- Usually relevant skills: `tools-kit-change-contract`,
  `tools-kit-research`, Rust best-practice skills as needed

### `app-kit`

- Root: `app-kit/`
- Required instructions: root `AGENTS.md`, `app-kit/AGENTS.md`, and owning specs
- Usually relevant skills: `ai-kit-public-surface-contract`,
  `app-kit-research`, Rust best-practice skills as needed

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
