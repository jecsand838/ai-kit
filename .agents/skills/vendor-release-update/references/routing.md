# Routing Matrix

Use this file after the vendor refresh request is understood and before acting
on the dossier.

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

- `agent-sdk/**`: `agent-sdk-change-contract`
- `storage-sdk/**`: `storage-sdk-change-contract`
- `provider-sdk/**`: `provider-sdk-change-contract` if a future dossier targets
  that crate

Current manifest-backed vendor workflows are owned by `agent-sdk` and
`storage-sdk`.

## Add Research Skills Only When

- provider docs or runtime behavior matter: `provider-sdk-research`
- backend docs or strict-live behavior matter: `storage-sdk-research`
- Codex/OpenCode protocol or runtime behavior matter: `agent-sdk-research`

For `storage-sdk/turso` dossier work, load `storage-sdk-change-contract`,
`storage-sdk-research`, and `use-rust-best-practices-dependencies-platforms`.
Treat the Turso GitHub releases feed as the release source and keep stable
tracking limited to semver tags with the configured `v` prefix and no
prerelease suffix. Turso is vendored for SQLite StateStore research and
implementation work only; do not route it as a `Database` backend.

For `storage-sdk/redb` dossier work, load `storage-sdk-change-contract`,
`storage-sdk-research` with `state stores` scope, and
`use-rust-best-practices-dependencies-platforms`. Treat the Redb GitHub
releases feed as the release source and keep stable tracking limited to semver
tags with the configured `v` prefix and no prerelease suffix. Redb is vendored
for embedded StateStore research and implementation work only; do not route it
as a `Database` backend. If the release changes transaction, savepoint,
durability, storage backend, in-memory, or file-backed behavior, review
`storage-sdk-research/references/redb-state-stores.md` before planning code.

## Output Contract

- Stay scoped to the single crate, vendor, and target tag described by the
  dossier.
- Leave cleanup, lockfile finalization, branch management, commits, and pushes
  to `scripts/vendor_update.py`.
