# Routing Matrix

Use this file after the rustdoc refresh goal is understood and before writing
the plan or editing files.

## Always Load

- `rust-public-api-docs`
- `ai-kit-public-surface-contract`
- `ai-kit-validation-matrix`
- `use-rust-best-practices-core`
- `use-rust-best-practices-testing`

## Add Contract Skills When

- `agents-kit/**` public rustdoc or contract tests are in scope:
  `agents-kit-change-contract`
- `providers-kit/**` public rustdoc or contract tests are in scope:
  `providers-kit-change-contract`
- `storage-kit/**` public rustdoc or contract tests are in scope:
  `storage-kit-change-contract`
- `tools-kit/**` public rustdoc or contract tests are in scope:
  `tools-kit-change-contract`
- `app-kit/**`: read `app-kit/AGENTS.md` and the owning App specifications;
  load `ai-kit-public-surface-contract`. App has no separate change-contract
  skill; preserve its five static workflow seams and exact exceptions.

Load every crate contract skill that matches the requested scope.

## Add Research Skills Only When

- App graph/workflow semantics, checkpoint/recovery behavior, rurr evidence,
  or lower-crate composition facts matter: `app-kit-research`
- provider behavior, model support, or live examples depend on upstream facts:
  `providers-kit-research`
- backend behavior, capability behavior, or live examples depend on upstream
  facts:
  `storage-kit-research`
- Codex App Server or OpenCode behavior affects the documented public surface:
  `agents-kit-research`
- MCP protocol, provider-hosted tool behavior, local runtime semantics, or
  composite routing affects the documented public surface:
  `tools-kit-research`

Do not load research skills for purely repo-local rustdoc cleanup that current
tests and source already settle.

## Output Contract

- Start with a decision-complete plan when the request is made from `/plan`.
- Continue into edits and validation when execution is allowed by the
  surrounding Codex flow.
- Update rustdoc contract helpers, docs-site validation, and public examples in
  the same change when their expectations move together.
- Redirect markdown-only documentation refreshes to
  `refresh-workspace-agentic-documentation`.
