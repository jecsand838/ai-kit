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

- `agent-sdk/**` public rustdoc or contract tests are in scope:
  `agent-sdk-change-contract`
- `provider-sdk/**` public rustdoc or contract tests are in scope:
  `provider-sdk-change-contract`
- `storage-sdk/**` public rustdoc or contract tests are in scope:
  `storage-sdk-change-contract`
- `tool-sdk/**` public rustdoc or contract tests are in scope:
  `tool-sdk-change-contract`
- `app-sdk/**`: read `app-sdk/AGENTS.md` and the owning App specifications;
  load `ai-kit-public-surface-contract`. App has no separate change-contract
  skill; preserve its five static workflow seams and exact exceptions.

Load every crate contract skill that matches the requested scope.

## Add Research Skills Only When

- App graph/workflow semantics, checkpoint/recovery behavior, rurr evidence,
  or lower-crate composition facts matter: `app-sdk-research`
- provider behavior, model support, or live examples depend on upstream facts:
  `provider-sdk-research`
- backend behavior, capability behavior, or live examples depend on upstream
  facts:
  `storage-sdk-research`
- Codex App Server or OpenCode behavior affects the documented public surface:
  `agent-sdk-research`
- MCP protocol, provider-hosted tool behavior, local runtime semantics, or
  composite routing affects the documented public surface:
  `tool-sdk-research`

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
