# Routing Matrix

Use this file after the campaign goal is understood and before running the
refresh.

## Always Load

- `use-rust-best-practices-core`
- `use-rust-best-practices-testing`
- `ai-kit-validation-matrix`

## Add Rust Best-Practice Skills When

- abstraction, data-model, or error-surface drift dominates:
  `use-rust-best-practices-abstractions`
- module ownership or test placement drift dominates:
  `use-rust-best-practices-architecture`
- async or live runtime behavior dominates:
  `use-rust-best-practices-async`
- HTTP, websocket, gRPC, timeouts, retries, or readiness dominate:
  `use-rust-best-practices-networking`
- dependencies, Cargo, or path/process portability dominate:
  `use-rust-best-practices-dependencies-platforms`

## Add Crate Skills When

- `agents-kit/**`: `agents-kit-change-contract`
- `storage-kit/**`: `storage-kit-change-contract`
- `providers-kit/**`: `providers-kit-change-contract` only if a future vendor
  workflow or campaign scope actually reaches that crate

Current manifest-backed vendor workflows are owned by `agents-kit` and
`storage-kit`.

Before execution, confirm any in-scope crate with strict-live completion gates
has a populated crate-local `strict-tests.env` file. Today that means
`agents-kit/strict-tests.env` and `storage-kit/strict-tests.env`.

## Add Research Skills Only When

- provider documentation or runtime behavior matters: `providers-kit-research`
- backend documentation or strict-live behavior matters: `storage-kit-research`
- Codex/OpenCode protocol or runtime behavior matters: `agents-kit-research`

When the campaign includes `storage-kit/turso`, load
`storage-kit-change-contract`, `storage-kit-research`, and
`use-rust-best-practices-dependencies-platforms` for that vendor. Turso uses
the GitHub releases feed with stable semver tags under the configured `v`
prefix; prerelease tags such as `v0.7.0-pre.*` must stay out of the stable
queue. Turso is vendored for SQLite StateStore research and implementation
work only; do not route it as a `Database` backend.

When the campaign includes `storage-kit/redb`, load
`storage-kit-change-contract`, `storage-kit-research` with `state stores`
scope, and `use-rust-best-practices-dependencies-platforms` for that vendor.
Redb uses the GitHub releases feed with stable semver tags under the configured
`v` prefix; prerelease tags must stay out of the stable queue. Redb is vendored
for embedded StateStore research and implementation work only; do not route it
as a `Database` backend. If release drift touches transaction, savepoint,
durability, storage backend, in-memory, or file-backed behavior, review
`storage-kit-research/references/redb-state-stores.md` before planning code.

## Output Contract

- Execute the campaign unless the user explicitly asks for planning only.
- Stay on the starting branch and use
  `python3 scripts/vendor_update.py update --crate <crate> --vendor <vendor> --git-mode current-branch`
  for each queued vendor.
- Add `--completion-policy no-regression` only when the user explicitly
  permits unchanged baseline failures while forbidding broad backlog
  implementation. Preflight failures and newly introduced diagnostics remain
  blocking.
- Report missing crate-local strict-live env prerequisites before the first
  updater run.
- Do not push or open a PR unless the user explicitly asks.
