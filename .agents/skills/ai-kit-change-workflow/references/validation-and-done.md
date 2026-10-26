# Validation And Done

Use this reference at the end of each phase.

## Shared ai-kit Baseline

When a change reaches implementation completion and workspace validation is in
scope, run the exact workspace baseline from the
[validation matrix](../../ai-kit-validation-matrix/references/validation.md):
formatting, non-Storage/non-App workspace Clippy, every Storage lane selected
by `feature_policy.py --all-backends`, and the docs-site gate. Preserve the
matrix's active-workflow boundaries, serial Cargo execution and native
`--locked`. App retains its separate owning gate; never use Storage
`--all-features`.

Use `bash scripts/verify_workspace_strict.sh` as the workspace strict
definition-of-done gate when the task is workspace-wide or changes shared
contracts.

## Crate Validation

Do not default to workspace-wide
`cargo test --locked --workspace --all-targets --all-features`. Read the relevant crate
`AGENTS.md` and run its documented command block.

Use crate-local strict-live scripts when behavior depends on live services,
external providers, backend behavior, protocol semantics, or runtime facts.

## Phase Done Checks

### `INLINE_PLANNING`

- Diff contains only inline planning comments.
- Every identified future task has a scoped TODO marker.
- Out-of-scope dependencies are reported instead of edited.

### `RESEARCH_REFINEMENT`

- Diff contains only TODO comment changes.
- Research facts are sourced or clearly marked as inference.
- Comments include enough implementation detail without becoming source docs.

### `IMPLEMENTATION`

- Scoped TODO markers are gone.
- Tests, docs, source contracts, and validation match the implemented behavior.
- Strict-live gates ran when required, or blockers are concrete and external.

## Final Response

Report:

- phase completed
- changed files
- validation commands and results
- remaining TODO markers, if any
- out-of-scope dependent issues
- blockers that require user action
