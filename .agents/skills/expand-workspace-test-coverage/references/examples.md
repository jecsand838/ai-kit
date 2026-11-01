# Canonical Prompt Examples

Use these prompts when manually validating that
`expand-workspace-test-coverage` owns coverage expansion only after concrete
findings exist.

The findings below are fictional routing fixtures, not current audit results.
Their source locations are illustrative repository-relative placeholders.
Replace them with accepted findings before using a plan for real work.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
repo_root="$(git rev-parse --show-toplevel)"
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C "$repo_root" \
  -o /tmp/expand-workspace-test-coverage-routing.txt \
  $'Use $expand-workspace-test-coverage ...'
```

If Codex exits early because of authentication, DNS, or state-db issues, record
that as an environment blocker rather than a skill failure.

These read-only commands validate planning and skill boundaries. Authorized
implementation requires an execution mode and a writable sandbox; a writable
sandbox alone does not override Plan Mode.

## Full Workspace Expansion

```bash
repo_root="$(git rev-parse --show-toplevel)"
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C "$repo_root" \
  -o /tmp/expand-workspace-full.txt \
  $'Use $expand-workspace-test-coverage. For this fictional routing fixture, start from these illustrative accepted workspace coverage findings and produce a decision-complete plan. They are not claims about the current checkout.\n\n1. agents-kit strict-live coverage does not exercise abort, command execution, and approval-control paths.\n2. providers-kit normalizer helpers and file/live flows need broader unit and strict-live coverage.\n3. storage-kit shared pagination helpers, Lance codecs, and SurrealDB adapter branches need narrower unit coverage.\n\nPlan only; do not implement in this read-only boundary check. Do not restate the audit.'
```

Expected checks:

- The response starts from the concrete findings instead of rerunning the audit.
- The response covers tests, production fallout, docs fallout, and validation.
- The response points at the canonical workspace strict gate when the scope is
  workspace-wide.

## Crate-Targeted Expansion

```bash
repo_root="$(git rev-parse --show-toplevel)"
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C "$repo_root" \
  -o /tmp/expand-workspace-storage.txt \
  $'Use $expand-workspace-test-coverage. For this fictional routing fixture, plan a storage-kit-only resolution for these illustrative accepted findings. They are not claims about the current checkout.\n\n1. storage-kit core pagination helpers need direct unit coverage for invalid page tokens and overfetch trimming.\n2. storage-kit SurrealDB adapter coverage needs branch-local tests for projection shaping and invalid-query rejection.\n\nPlan only; do not implement in this read-only boundary check.'
```

Expected checks:

- The response stays anchored to `storage-kit`.
- The response loads `storage-kit-change-contract`,
  `storage-kit-research`, and the matching Rust testing guidance.
- The response does not broaden into unrelated workspace churn.

## Boundary Check

```bash
repo_root="$(git rev-parse --show-toplevel)"
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C "$repo_root" \
  -o /tmp/expand-workspace-boundary.txt \
  $'Use $expand-workspace-test-coverage. Expand workspace test coverage, but I do not have any existing findings or bounded gap list. Just figure out what is missing and implement it.'
```

Expected checks:

- The response redirects toward `audit-workspace-test-coverage` or clearly asks
  for concrete findings first.
- The response does not pretend the audit step is already complete.
