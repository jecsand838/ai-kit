# Routing Matrix

Use this file after the optimization findings or cleanup goal are understood
and before writing the plan.

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

- `provider-sdk/**` or provider capability, routing, live, auth, or provider
  public types are in scope:
  `provider-sdk-change-contract`
- `storage-sdk/**` or backend behavior, grouping, scoring, readiness,
  embedded-vs-remote behavior, or storage public types are in scope:
  `storage-sdk-change-contract`
- `agent-sdk/**` or Codex/OpenCode transport, capability mapping, or public
  agent surfaces are in scope:
  `agent-sdk-change-contract`
- `tool-sdk/**` or Local/MCP executable runtime behavior, hosted/composite
  deletion boundaries, validation, event deletion, or public tool surfaces are
  in scope:
  `tool-sdk-change-contract`
- `app-sdk/**`: read `app-sdk/AGENTS.md` and the owning App specifications;
  load `ai-kit-public-surface-contract`. App has no separate change-contract
  skill; preserve its five static workflow seams and exact exceptions.

Load every contract skill that matches the requested scope.

## Add Research Skills Only When

- App graph/workflow semantics, checkpoint/recovery behavior, rurr evidence,
  or lower-crate composition facts matter: `app-sdk-research`
- the plan depends on provider docs or runtime behavior:
  `provider-sdk-research`
- the plan depends on backend docs, capability behavior, embedded-vs-remote
  semantics, or strict-live assumptions:
  `storage-sdk-research`
- the plan depends on Codex/OpenCode protocol, runtime, or strict-live facts:
  `agent-sdk-research`
- the plan depends on MCP protocol, provider-hosted tool behavior, local
  runtime semantics, composite routing, or strict-live tool facts:
  `tool-sdk-research`

Do not load research for purely internal cleanup such as helper collapse, local
validation tightening, deterministic ordering fixes already encoded in ai-kit
types, or module moves that do not depend on upstream behavior.

## Output Contract

- Produce one decision-complete plan.
- Order it tests first, then production changes, then docs and validation.
- Use the exact command blocks from the owning crate `AGENTS.md` files.
