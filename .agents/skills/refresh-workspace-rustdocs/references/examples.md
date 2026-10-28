# Canonical Prompt Examples

Use these prompts when manually validating that `refresh-workspace-rustdocs`
owns workspace-wide rustdoc refreshes rather than general markdown work.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-rustdocs-routing.txt \
  $'Use $refresh-workspace-rustdocs ...'
```

For a real execution pass, switch the sandbox to `workspace-write`.

## Workspace-Write Temp Worktree Pass

```bash
tmp_root="$(mktemp -d /tmp/ai-kit-skill-rustdocs.XXXXXX)"
tmp_repo="${tmp_root}/workspace"
git -C /Users/connorsanders/scratch/ai-kit worktree add --detach "${tmp_repo}" HEAD
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox workspace-write \
  -C "${tmp_repo}" \
  -o /tmp/refresh-workspace-rustdocs-worktree.txt \
  $'Use $refresh-workspace-rustdocs from /plan. Refresh the public rustdoc comments, runnable examples, and rustdoc validation for the current branch in this disposable worktree.'
git -C /Users/connorsanders/scratch/ai-kit worktree remove --force "${tmp_repo}"
rmdir "${tmp_root}"
```

Expected checks:

- The response starts with a decision-complete plan before edits.
- The response keeps the work inside the disposable worktree instead of the
  live
  checkout.
- The response uses `rust-public-api-docs`, `ai-kit-public-surface-contract`,
  and the matching crate contract skills.

## Workspace Rustdoc Refresh Happy Path

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-rustdocs-happy.txt \
  $'Use $refresh-workspace-rustdocs from /plan. Refresh the public rustdoc comments, runnable examples, and rustdoc validation across agents-kit, providers-kit, storage-kit, and tools-kit against current HEAD.'
```

Expected checks:

- The response starts with a decision-complete plan.
- The response loads `rust-public-api-docs`, `ai-kit-public-surface-contract`,
  and `ai-kit-validation-matrix`.
- The response treats public rustdoc, rustdoc contracts, and the docs-site gate
  as one coordinated refresh.

## No-Op Verification

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-rustdocs-noop.txt \
  $'Use $refresh-workspace-rustdocs from /plan. Verify whether the current branch has any public rustdoc drift and describe the no-op outcome if the public surface docs are already aligned. Do not implement.'
```

Expected checks:

- The response preserves the verification or no-op outcome cleanly.
- The response does not invent markdown catalog or skill-doc updates when the
  task is rustdoc-only.

## Boundary Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-rustdocs-boundary.txt \
  $'Use $refresh-workspace-rustdocs from /plan. Refresh all README.md, ARCHITECTURE.md, AGENTS.md, and skill docs for the current branch without changing the public rustdoc comments.'
```

Expected checks:

- The response redirects toward `refresh-workspace-agentic-documentation`.
- The response does not claim ownership of general markdown-only refresh work.

## Narrow Rustdoc Boundary Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-rustdocs-narrow-boundary.txt \
  $'Use $refresh-workspace-rustdocs from /plan. Refresh the rustdoc for one public builder method in providers-kit without touching the rest of the workspace.'
```

Expected checks:

- The response redirects toward `rust-public-api-docs`.
- The response does not claim ownership of narrow single-item rustdoc work.
