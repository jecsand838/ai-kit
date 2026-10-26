# Canonical Prompt Examples

Use these prompts when manually validating that `vendor-refresh-campaign` owns
top-level vendor campaigns rather than single-vendor dossier work.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-refresh-campaign-routing.txt \
  $'Use $vendor-refresh-campaign ...'
```

For a real campaign execution pass, switch the sandbox to `workspace-write`.

## Campaign Happy Path

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox workspace-write \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-refresh-campaign-happy.txt \
  $'Use $vendor-refresh-campaign. First verify that `agent-sdk/strict-tests.env` and `storage-sdk/strict-tests.env` are ready, then refresh all outdated vendors in `agent-sdk` and `storage-sdk`, leave the final validated result on the current branch, rerun the full final gates, and do not push.'
```

Expected checks:

- The response treats the request as a top-level campaign.
- The response checks the crate-local strict-live env prerequisites before the
  first updater run.
- The workflow keeps the final state on the current branch.
- The response does not invent pushes or PR creation.

## No-Op Campaign

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-refresh-campaign-noop.txt \
  $'Use $vendor-refresh-campaign. Check whether any ai-kit vendors are outdated and describe the no-op outcome if everything is already current. Plan only. Do not implement.'
```

Expected checks:

- The response reads like a top-level campaign preflight.
- The response preserves the no-op outcome if nothing is outdated.

## Boundary Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/vendor-refresh-campaign-negative-boundary.txt \
  $'Use $vendor-refresh-campaign. Read `/tmp/demo-dossier/dossier.md` and `/tmp/demo-dossier/plan.md`, then execute that one vendor refresh.'
```

Expected checks:

- The response redirects toward `vendor-release-update` or clearly states that
  updater-generated dossier work belongs there.
