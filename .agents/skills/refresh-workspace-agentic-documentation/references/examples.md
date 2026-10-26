# Canonical Prompt Examples

Use these prompts when manually validating that
`refresh-workspace-agentic-documentation` owns workspace-wide documentation and
skill/agent metadata reconciliation rather than narrow rustdoc work. Select
explicit user/workflow authority before using the branch diff as implementation
evidence. Plan Mode permits planning only; a writable sandbox does not authorize
execution until the surrounding flow permits edits.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-agentic-documentation-routing.txt \
  $'Use $refresh-workspace-agentic-documentation ...'
```

For an authorized execution pass after planning, switch the sandbox to
`workspace-write` and use an execution-enabled surrounding flow.

## Workspace-Write Temp Worktree Pass

```bash
tmp_root="$(mktemp -d /tmp/ai-kit-skill-docs.XXXXXX)"
tmp_repo="${tmp_root}/workspace"
git -C /Users/connorsanders/scratch/ai-kit worktree add --detach "${tmp_repo}" HEAD
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox workspace-write \
  -C "${tmp_repo}" \
  -o /tmp/refresh-workspace-agentic-documentation-worktree.txt \
  $'Use $refresh-workspace-agentic-documentation to implement the approved plan in this disposable worktree. Preserve its selected authority and history; reconcile docs, skill/agent metadata, ledger and docs-contract tests, then run the prescribed documentation gates.'
git -C /Users/connorsanders/scratch/ai-kit worktree remove --force "${tmp_repo}"
rmdir "${tmp_root}"
```

Expected checks:

- Execution follows a decision-complete approved plan and the surrounding flow
  permits edits.
- Work stays inside the disposable worktree, not the live checkout.
- The response uses the matching crate contract skills and validation gates.

## Workspace Refresh Happy Path

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-agentic-documentation-happy.txt \
  $'Use $refresh-workspace-agentic-documentation from /plan. Reconcile all repo-owned ai-kit docs and skill/agent metadata against current owning contracts and this branch implementation. Select this authority before reading the diff; plan the ledger, docs-contract and validation updates while preserving historical evidence.'
```

Expected checks:

- The response starts with a decision-complete plan.
- The plan treats workspace/crate docs, skill/agent guidance and metadata, the
  ledger and docs-contract tests as one change.
- Selected authority precedes inspection of implementation evidence; historical
  results and metadata invocation policies remain intact.
- The response loads `ai-kit-validation-matrix` and the matching crate contract
  skills.

## No-Op Audit

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-agentic-documentation-noop.txt \
  $'Use $refresh-workspace-agentic-documentation from /plan. Check whether the current branch introduces any workspace documentation drift versus main and describe the no-op outcome if the docs are already aligned. Do not implement.'
```

Expected checks:

- The response preserves the audit or no-op outcome cleanly.
- The response does not invent edits when documentation and metadata already
  agree with the selected authority and implementation evidence.

## Boundary Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refresh-workspace-agentic-documentation-boundary.txt \
  $'Use $refresh-workspace-agentic-documentation from /plan. Refresh the public rustdoc comments and doctestable examples across all crates without changing the markdown docs.'
```

Expected checks:

- The response redirects toward `refresh-workspace-rustdocs` or
  `rust-public-api-docs`.
- The response does not claim ownership of pure rustdoc work.
