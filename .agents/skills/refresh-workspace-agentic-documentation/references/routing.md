# Routing Matrix

Use this file after the documentation refresh goal is understood and before
writing the plan or editing files.

## Select Authority First

Before inspecting `HEAD`, the branch diff, implementation docs, tests, or
source, record the governing authority in this order:

1. explicit user direction;
2. the active workflow's selected specs and instructions;
3. current repository surfaces as implementation-status evidence.

In a spec-led workflow, current source, tests, docs, and rustdoc may reveal
drift but do not displace the selected specification authority. The workflow
may authorize an authority-only preimplementation reconciliation. Keep every
implementation-status delta explicit for the later implementation work, and do
not describe the target as already implemented.

## Always Load

- `ai-kit-validation-matrix`

## Add Rust Best-Practice Skills When

- the refresh needs a workspace-wide wording, review, or invariants pass:
  `use-rust-best-practices-core`
- public examples, doctests, or validation-flow wording are in scope:
  `use-rust-best-practices-testing`

## Add Contract Skills When

- root docs or `agents-kit/**` docs or tests are in scope:
  `agents-kit-change-contract`
- `providers-kit/**` docs or tests are in scope:
  `providers-kit-change-contract`
- `storage-kit/**` docs or tests are in scope:
  `storage-kit-change-contract`
- `tools-kit/**` docs or tests are in scope:
  `tools-kit-change-contract`
- `app-kit/**`: read `app-kit/AGENTS.md` and the owning App specifications;
  load `ai-kit-public-surface-contract`. App has no separate change-contract
  skill; preserve its five static workflow seams and exact exceptions.

Load every crate contract skill that matches the requested scope.

## Add Docs Skills When

- rustdoc ownership, docs-site validation, or public API examples are in scope:
  `rust-public-api-docs`
- strict-live docs, verify scripts, or env-contract guidance are in scope:
  `strict-live-audit`

## Add Research Skills Only When

- App graph/workflow semantics, checkpoint/recovery behavior, rurr evidence,
  or lower-crate composition facts matter: `app-kit-research`
- provider docs, model behavior, or strict-live facts depend on upstream docs:
  `providers-kit-research`
- backend docs, capability behavior, or strict-live facts depend on upstream
  docs:
  `storage-kit-research`
- MCP protocol, provider-hosted tool behavior, local runtime semantics,
  composite routing, or strict-live tool facts depend on upstream docs:
  `tools-kit-research`
- Codex App Server, OpenCode, or AGENTS-layering assumptions depend on upstream
  docs:
  `agents-kit-research`

Do not load research skills for purely repo-local wording cleanup that is
already settled by the selected authority and its recorded repository
evidence.

## Output Contract

- Start with a decision-complete plan when the request is made from `/plan`.
- Continue into edits and validation when execution is allowed by the
  surrounding Codex flow.
- Refresh owning docs and docs-contract tests together.
- When an active workflow authorizes authority-only preimplementation work,
  limit edits to its declared authority surfaces and retain implementation
  drift as later work.
- Redirect rustdoc-only work to `refresh-workspace-rustdocs` or
  `rust-public-api-docs`.
