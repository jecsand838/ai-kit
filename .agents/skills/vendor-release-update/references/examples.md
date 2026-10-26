# Canonical Prompt Examples

Use these prompts when manually validating that `vendor-release-update` owns
single-vendor updater dossier work rather than top-level refresh campaigns.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-release-update-plan.txt \
  $'Use $vendor-release-update ...'
```

For execution-mode validation, switch the sandbox to `workspace-write`.

## Planning Mode

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-release-update-plan.txt \
  $'Use $vendor-release-update. Read `/tmp/demo-dossier/dossier.md` and produce a decision-complete markdown update plan for one vendor refresh. Save the final markdown response to `/tmp/demo-dossier/plan.md`. Do not modify files.'
```

Expected checks:

- The response stays in planning mode.
- The response reads like one-vendor dossier planning, not like a campaign
  plan.

## Execution Mode

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox workspace-write \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-release-update-execution.txt \
  $'Use $vendor-release-update. Read `/tmp/demo-dossier/dossier.md` and `/tmp/demo-dossier/plan.md`, then execute the plan for one vendor refresh. Do not create commits or push branches.'
```

Expected checks:

- The response follows the dossier plus plan for one vendor refresh.
- The response does not take over cleanup, branch creation, commits, or pushes.

## Boundary Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-release-update-negative-boundary.txt \
  $'Use $vendor-release-update. Refresh all outdated vendors in `agent-sdk` and `storage-sdk`, leave the final result on my current branch, and rerun the workspace validation gates.'
```

Expected checks:

- The response redirects toward `vendor-refresh-campaign` or clearly states
  that top-level campaigns belong there.
