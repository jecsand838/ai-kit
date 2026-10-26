---
name: refresh-workspace-agentic-documentation
description: "Refresh ai-kit workspace documentation across AGENTS.md, ARCHITECTURE.md, README.md, SKILLS.md, focused guides, and repo skill/agent guidance and metadata. Use when selected user or workflow authority, or a branch change to behavior, ownership, validation, skills, or documentation contract tests, requires reconciliation. Do not use it for public-rustdoc-only refreshes; use `refresh-workspace-rustdocs` or `rust-public-api-docs` for those."
---

# Refresh Workspace Agentic Documentation

## Purpose

Refresh repo-owned `ai-kit` markdown docs, skill/agent metadata, and matching
docs-contract coverage as one workspace change.

## Trigger Boundary

Use this skill for workspace-wide documentation refreshes that span root docs,
crate docs, focused guides, skill/agent guidance or metadata, or docs-contract
tests. Do not use it for narrow rustdoc-only work; route that to
`refresh-workspace-rustdocs` or
`rust-public-api-docs`.

## Prerequisites

- Read [references/routing.md](references/routing.md).
- Read [references/workflow.md](references/workflow.md).
- Select authority before inspecting current `HEAD` or the diff against `main`:
  explicit user direction first, then the active workflow's selected specs and
  instructions.
- Inspect current `HEAD` and the diff only after that selection, and record
  implementation-status conflicts as drift against the selected authority.
- Load the relevant crate `*-change-contract` skills before editing crate-owned
  docs or docs-contract tests. For App, read its `AGENTS.md` and owning specs
  and load `ai-kit-public-surface-contract`; no App change-contract skill exists.

## Workflow

1. Start by recording the explicit user direction and active workflow authority
   that govern the refresh.
2. Identify the in-scope docs and skill/agent metadata from that authority,
   the diff against `main`, the current skill inventory, and the existing
   documentation ledger.
3. If the request is made from `/plan`, produce a decision-complete plan first,
   then continue into execution only when the surrounding Codex flow permits
   file edits.
4. Browse the official `AGENTS.md`, `ARCHITECTURE.md`, and Codex AGENTS docs
   live when structure or instruction-layering guidance matters.
5. Refresh workspace docs, crate docs, skill/agent guidance and metadata, and
   the owning docs-contract tests together instead of treating them as separate
   tasks.
   An active spec-led workflow may instead authorize an authority-only
   preimplementation reconciliation; in that case, keep implementation-status
   surfaces recorded as drift for the later implementation work.
6. Update `docs/documentation-ledger.toml` whenever the in-scope inventory or
   adoption scope changes. Keep frozen adopted requirements distinct from
   active instructions and historical execution evidence.
7. Run the documented validation gates before closing the task.

## Output Rules

- Keep role ownership clear across `README.md`, `ARCHITECTURE.md`, `AGENTS.md`,
  focused guides, and skill docs.
- Do not rewrite source, tests, implementation-status docs, or rustdoc to imply
  that an accepted preimplementation target is already complete.
- Never suppress warnings, doc-test failures, or docs-contract failures.
- Route rustdoc-only refreshes to `refresh-workspace-rustdocs` or
  `rust-public-api-docs` instead of absorbing them here.
- Keep `SKILLS.md` compact and leave long operating detail in the owning skill
  docs and references.

## References

- [references/routing.md](references/routing.md)
- [references/workflow.md](references/workflow.md)
- [references/examples.md](references/examples.md)
