# Routing Matrix

Use this file after the accepted findings and target crate are known and before
writing the plan or starting implementation.

## Always Load

- `use-rust-best-practices-core`
- `use-rust-best-practices-testing`
- `ai-kit-validation-matrix`
- `strict-live-audit`

## Add The Matching Crate Skills

- `agent-sdk`:
  load `agent-sdk-change-contract`;
  add `agent-sdk-research` when Codex App Server or OpenCode facts, runtime
  behavior, or live-validation assumptions matter
- `provider-sdk`:
  load `provider-sdk-change-contract`;
  add `provider-sdk-research` when provider docs, model support, files, or
  strict-live provider facts matter
- `storage-sdk`:
  load `storage-sdk-change-contract`;
  add `storage-sdk-research` when backend behavior, embedded-vs-remote
  behavior, or strict-live backend facts matter
- `tool-sdk`:
  load `tool-sdk-change-contract`;
  add `tool-sdk-research` when MCP protocol, provider-hosted tool, local
  runtime, composite-source, or strict-live tool facts matter
- `app-sdk`:
  read `app-sdk/AGENTS.md` and its owning specs; load
  `ai-kit-public-surface-contract`; add `app-sdk-research` when workflow,
  checkpoint, recovery, rurr, or lower-crate composition facts matter

## Add Rust Specialty Skills When

- abstraction, enum-dispatch, public-surface error design, or shared helper
  collapse dominates:
  `use-rust-best-practices-abstractions`
- module ownership, docs-contract fallout, or workspace-boundary questions
  dominate:
  `use-rust-best-practices-architecture`
- async runtimes, streaming, cancellation, subprocess control, or socket flows
  dominate:
  `use-rust-best-practices-async`
- transport behavior, retries, timeouts, sockets, or live prechecks dominate:
  `use-rust-best-practices-networking`
- shell scripts, env files, dependency policy, or cross-platform fallout
  dominate:
  `use-rust-best-practices-dependencies-platforms`

## Output Contract

- Start from accepted findings or `DELETE(<crate> shim audit)` markers.
- Keep the workflow delete-first and strict-live-first.
- Prefer honest errors over synthetic behavior.
- Use the exact target-crate validation blocks from the relevant `AGENTS.md`.
- Do not add new shims, output shaping, or provider or backend leakage into the
  public surface.
