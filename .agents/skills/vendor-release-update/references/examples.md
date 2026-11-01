# Canonical Prompt Examples

Use these prompts when manually validating that `vendor-release-update` owns
single-vendor updater dossier work rather than top-level refresh campaigns.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
repo_root="$(git rev-parse --show-toplevel)"
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C "$repo_root" \
  -o /tmp/vendor-release-update-plan.txt \
  $'Use $vendor-release-update ...'
```

For an authorized execution pass after planning, switch the sandbox to
`workspace-write` and use an execution-enabled surrounding flow. A writable
sandbox does not override Plan Mode. Demo dossier paths are placeholders;
replace them with the actual updater-generated dossier and approved plan.

## Planning Mode

This recipe requires an existing updater-generated dossier and an unused plan
output path. Replace the demo paths before use. The CLI writes the returned
Markdown through `-o`; the planning agent does not modify files or overwrite
the input dossier.

```bash
repo_root="$(git rev-parse --show-toplevel)"
test -f /tmp/demo-dossier/dossier.md || exit 1
test ! -e /tmp/demo-dossier/plan.md || exit 1
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C "$repo_root" \
  -o /tmp/demo-dossier/plan.md \
  $'Use $vendor-release-update. Read `/tmp/demo-dossier/dossier.md` and produce a decision-complete update plan for one vendor refresh. Return the plan as final Markdown only. Do not modify files.'
```

Expected checks:

- The response stays in planning mode.
- The response reads like one-vendor dossier planning, not like a campaign
  plan.

## Execution Mode

```bash
repo_root="$(git rev-parse --show-toplevel)"
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox workspace-write \
  -C "$repo_root" \
  -o /tmp/vendor-release-update-execution.txt \
  $'Use $vendor-release-update. Read `/tmp/demo-dossier/dossier.md` and `/tmp/demo-dossier/plan.md`, then execute the plan for one vendor refresh. Do not create commits or push branches.'
```

Expected checks:

- Execution follows an approved plan and the surrounding flow permits edits.
- The response follows the dossier plus plan for one vendor refresh.
- The response does not take over cleanup, branch creation, commits, or pushes.

## Boundary Check

```bash
repo_root="$(git rev-parse --show-toplevel)"
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C "$repo_root" \
  -o /tmp/vendor-release-update-negative-boundary.txt \
  $'Use $vendor-release-update. Refresh all outdated vendors in `agents-kit` and `storage-kit`, leave the final result on my current branch, and rerun the workspace validation gates.'
```

Expected checks:

- The response redirects toward `vendor-refresh-campaign` or clearly states
  that top-level campaigns belong there.
