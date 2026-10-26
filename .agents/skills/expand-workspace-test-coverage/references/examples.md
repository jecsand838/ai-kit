# Canonical Prompt Examples

Use these prompts when manually validating that
`expand-workspace-test-coverage` owns coverage expansion only after concrete
findings exist.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/<output-file>.txt \
  $'Use $expand-workspace-test-coverage ...'
```

If Codex exits early because of authentication, DNS, or state-db issues, record
that as an environment blocker rather than a skill failure.

These read-only commands validate planning and skill boundaries. Authorized
implementation requires an execution mode and a writable sandbox; a writable
sandbox alone does not override Plan Mode.

## Full Workspace Expansion

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/expand-workspace-full.txt \
  $'Use $expand-workspace-test-coverage. Start from these accepted workspace test coverage findings and produce a decision-complete plan.\n\n1. agent-sdk strict-live coverage does not exercise abort, command execution, and approval-control paths.\n2. provider-sdk normalizer helpers and file/live flows need broader unit and strict-live coverage.\n3. storage-sdk shared pagination helpers, Lance codecs, and Qdrant adapter branches need narrower unit coverage.\n\nPlan only; do not implement in this read-only boundary check. Do not restate the audit.'
```

Expected checks:

- The response starts from the concrete findings instead of rerunning the audit.
- The response covers tests, production fallout, docs fallout, and validation.
- The response points at the canonical workspace strict gate when the scope is
  workspace-wide.

## Crate-Targeted Expansion

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/expand-workspace-storage.txt \
  $'Use $expand-workspace-test-coverage. Plan a storage-sdk-only resolution for these accepted findings.\n\n1. storage-sdk core pagination helpers need direct unit coverage for invalid page tokens and overfetch trimming.\n2. storage-sdk Qdrant adapter coverage needs branch-local tests for projection shaping and merge rejection.\n\nPlan only; do not implement in this read-only boundary check.'
```

Expected checks:

- The response stays anchored to `storage-sdk`.
- The response loads `storage-sdk-change-contract`,
  `storage-sdk-research`, and the matching Rust testing guidance.
- The response does not broaden into unrelated workspace churn.

## Boundary Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/expand-workspace-boundary.txt \
  $'Use $expand-workspace-test-coverage. Expand workspace test coverage, but I do not have any existing findings or bounded gap list. Just figure out what is missing and implement it.'
```

Expected checks:

- The response redirects toward `audit-workspace-test-coverage` or clearly asks
  for concrete findings first.
- The response does not pretend the audit step is already complete.
