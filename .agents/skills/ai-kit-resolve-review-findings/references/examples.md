# Canonical Prompt Examples

Use these prompts when manually validating that
`ai-kit-resolve-review-findings` stays plan-only and owns built-in `/review`
follow-up work.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/<output-file>.txt \
  $'Use $ai-kit-resolve-review-findings ...'
```

If Codex exits before model output because of authentication, DNS, or state-db
issues, record that as an environment blocker rather than a skill failure.

## Multi-Finding Provider Plan

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/resolve-review-provider-live.txt \
  $'Use $ai-kit-resolve-review-findings. Treat these as accepted built-in /review findings and produce only a decision-complete TDD resolution plan.\n\n1. [P1] Populate auth headers on OpenAI live connections\n- File: /Users/connorsanders/scratch/ai-kit/provider-sdk/src/providers/openai/live.rs\n- Why: LiveConnectionInfo requires connection.headers, but the OpenAI live connect path returns an empty map and breaks realtime websocket auth.\n\n2. [P2] Reject blank model IDs in optional routes\n- File: /Users/connorsanders/scratch/ai-kit/provider-sdk/src/core/model_route.rs\n- Why: validate() only checks default and allows blank optional route ids.\n\nPlan only. Do not implement.'
```

Expected checks:

- The response stays plan-only.
- The plan treats both findings as one accepted bug list.
- The plan uses the provider `AGENTS.md` validation block.

## Internal-Only Storage Plan

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/resolve-review-internal-only.txt \
  $'Use $ai-kit-resolve-review-findings. Treat these as accepted built-in /review findings and produce only a decision-complete TDD resolution plan.\n\n1. [P1] Preserve group ordering when candidate hits lack normalized scores\n- File: /Users/connorsanders/scratch/ai-kit/storage-sdk/src/databases/shared/query.rs\n- Why: grouped_query_response ranks groups using only normalized_score, so backends that leave normalized_score unset fall back to unstable ordering.\n\nPlan only. Do not implement.'
```

Expected checks:

- The response stays plan-only.
- The plan does not rerun review.
- The plan avoids unnecessary external-facts framing when the issue is purely
  local.
