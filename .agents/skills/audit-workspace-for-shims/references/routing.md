# Routing Matrix

Use this file after the target crate is known and before starting the audit.

## Always Load

- `use-rust-best-practices-core`
- `ai-kit-validation-matrix`
- `strict-live-audit`

## Add The Matching Crate Skills

- `agents-kit`:
  load `agents-kit-change-contract`;
  add `agents-kit-research` when Codex App Server or OpenCode facts, runtime
  behavior, or live-validation assumptions matter
- `providers-kit`:
  load `providers-kit-change-contract`;
  add `providers-kit-research` when provider docs, model support, files, or
  strict-live provider facts matter
- `storage-kit`:
  load `storage-kit-change-contract`;
  add `storage-kit-research` when backend behavior, embedded-vs-remote
  behavior, or strict-live backend facts matter
- `tools-kit`:
  load `tools-kit-change-contract`;
  add `tools-kit-research` when MCP protocol, provider-hosted tool, local
  runtime, composite-source, or strict-live tool facts matter
- `app-kit`:
  read `app-kit/AGENTS.md` and its owning specs; load
  `ai-kit-public-surface-contract`; add `app-kit-research` when workflow,
  checkpoint, recovery, rurr, or lower-crate composition facts matter

## Add Rust Specialty Skills When

- abstraction, enum-dispatch, honest-error design, or shared helper drift
  dominates:
  `use-rust-best-practices-abstractions`
- module ownership, source-contract fallout, or workspace-boundary questions
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

- Require an explicit target crate: `agents-kit`, `providers-kit`, `storage-kit`,
  `tools-kit`, or `app-kit`.
- Keep the audit crate-scoped; do not widen into unrelated workspace churn.
- Report findings first.
- Write `DELETE(<crate> shim audit): <reason>` markers only when edits are
  allowed.
- Stop at the audit. Do not produce the removal plan or start implementation.
