# Routing Matrix

Use this file after the accepted findings are understood and before writing the
plan.

## Always Load

- `use-rust-best-practices-core`
- `use-rust-best-practices-testing`

## Add Rust Best-Practice Skills When

- abstraction, API, enum-dispatch, error, or lifetime work dominates:
  `use-rust-best-practices-abstractions`
- module ownership, file splits, test placement, or crate boundaries dominate:
  `use-rust-best-practices-architecture`
- async, streams, cancellation, or bounded concurrency dominate:
  `use-rust-best-practices-async`
- HTTP, websocket, gRPC, retries, timeouts, or readiness dominate:
  `use-rust-best-practices-networking`
- dependencies, Cargo features, path/process portability, or platform behavior
  dominate:
  `use-rust-best-practices-dependencies-platforms`

## Add Crate Skills When

- `provider-sdk/**` or findings about provider capabilities, routing, live
  behavior, auth, or provider public types are in scope:
  `provider-sdk-change-contract`
- `storage-sdk/**` or findings about backend behavior, grouping, scoring,
  readiness, embedded-vs-remote behavior, or storage public types are in
  scope:
  `storage-sdk-change-contract`
- `agent-sdk/**` or findings about Codex/OpenCode transport, capability
  mapping, or public agent surfaces are in scope:
  `agent-sdk-change-contract`
- `tool-sdk/**` or findings about Local/MCP executable runtime behavior,
  hosted/composite deletion boundaries, validation, event deletion, or public
  tool surfaces are in scope:
  `tool-sdk-change-contract`
- `app-sdk/**`: read `app-sdk/AGENTS.md` and the owning App specifications;
  load `ai-kit-public-surface-contract`. App has no separate change-contract
  skill; preserve its five static workflow seams and exact exceptions.

Load every contract skill that matches the accepted findings.

## Add Research Skills Only When

- App graph/workflow semantics, checkpoint/recovery behavior, rurr evidence,
  or lower-crate composition facts matter: `app-sdk-research`
- a `provider-sdk` finding depends on provider docs or runtime behavior:
  `provider-sdk-research`
- a `storage-sdk` finding depends on backend docs, capability behavior,
  embedded-vs-remote semantics, or strict-live assumptions:
  `storage-sdk-research`
- an `agent-sdk` finding depends on Codex/OpenCode protocol, runtime, or
  strict-live facts:
  `agent-sdk-research`
- a `tool-sdk` finding depends on MCP protocol, provider-hosted tool behavior,
  local runtime semantics, composite routing, or strict-live tool facts:
  `tool-sdk-research`

Do not load research for purely internal issues whose semantics are already
local and well-defined.

## Output Contract

- Treat the provided `/review` findings as the accepted bug list.
- Resolve all accepted findings in one plan unless the prompt explicitly splits
  scope.
- Order the plan tests first, then production changes, then docs and
  validation.
- Use the exact command blocks from the owning crate `AGENTS.md` files.
