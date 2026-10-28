# Routing Matrix

Use this file after the findings or bounded coverage goal are understood and
before writing the plan or starting implementation.

## Always Load

- `use-rust-best-practices-core`
- `use-rust-best-practices-testing`
- `ai-kit-validation-matrix`

## Add Rust Best-Practice Skills When

- abstraction, enum-dispatch, error design, or shared test helper extraction
  dominates: `use-rust-best-practices-abstractions`
- test placement, module ownership, docs-contract layout, or workspace
  boundaries dominate: `use-rust-best-practices-architecture`
- async runtimes, streaming, cancellation, subprocess control, or readiness
  dominate: `use-rust-best-practices-async`
- transport behavior, retries, timeouts, sockets, or live prechecks dominate:
  `use-rust-best-practices-networking`
- Cargo, shell scripts, env files, dependency policy, or platform behavior
  dominate: `use-rust-best-practices-dependencies-platforms`

## Add Crate Contract Skills When

- `agents-kit/**` or agent control, approval, command, prompt, runtime, or
  strict-live flows are in scope: `agents-kit-change-contract`
- `providers-kit/**` or provider normalization, files, live, models, runtime, or
  strict-live flows are in scope: `providers-kit-change-contract`
- `storage-kit/**` or backend codecs, pagination, query, readiness, admin, or
  strict-live flows are in scope: `storage-kit-change-contract`
- `tools-kit/**` or local execution, MCP runtime, provider-hosted declarations,
  composite routing, validation, events, or strict-live flows are in scope:
  `tools-kit-change-contract`
- `app-kit/**`: read `app-kit/AGENTS.md` and the owning App specifications;
  load `ai-kit-public-surface-contract`. App has no separate change-contract
  skill; preserve its five static workflow seams and exact exceptions.

Load every contract skill that matches the requested scope.

## Add Research Skills Only When

- App graph/workflow semantics, checkpoint/recovery behavior, rurr evidence,
  or lower-crate composition facts matter: `app-kit-research`
- Codex App Server or OpenCode facts, protocol semantics, or live-validation
  behavior matter: `agents-kit-research`
- provider API behavior, model support, files, live, or strict-live assumptions
  matter: `providers-kit-research`
- LanceDB, SurrealDB, embedded-vs-remote behavior, or strict-live
  assumptions matter: `storage-kit-research`
- MCP protocol, provider-hosted tool behavior, local runtime semantics,
  composite-source routing, or strict-live tool assumptions matter:
  `tools-kit-research`

Do not load research for purely repo-local test cleanup that does not depend on
upstream behavior.

## Add Validation Skills When

- strict-live suites, env contracts, verify scripts, or docs-script-test
  alignment are in scope: `strict-live-audit`
- exact command sequencing, workspace done gates, or crate strict completion
  flows are in scope: `ai-kit-validation-matrix`

## Output Contract

- Start from concrete findings or a bounded coverage goal.
- Produce a decision-complete plan first.
- Add tests first, then production fixes only within the authorized scope,
  followed by docs and validation fallout. Under a production freeze, retain
  required enabled regressions, record confirmed defects in the owning ledger,
  and continue independent work without weakening tests or claiming a pass.
- Use the exact command blocks from the relevant `AGENTS.md` files.
