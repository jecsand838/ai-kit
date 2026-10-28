# Canonical Prompt Examples

Use these prompts when manually validating that `ai-kit-refactor-plan` owns
optimization and cleanup planning rather than built-in `/review` follow-up work.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/<output-file>.txt \
  $'Use $ai-kit-refactor-plan ...'
```

If Codex exits before model output because of authentication, DNS, or state-db
issues, record that as an environment blocker rather than a skill failure.

## Optimization Planning

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refactor-plan-optimization.txt \
  $'Use $ai-kit-refactor-plan. Plan a decision-complete, tests-first cleanup for these accepted optimization audit findings.\n\n1. The provider live stack duplicates provider-specific live request shaping across multiple files and should consolidate shared sequencing without adding compatibility shims.\n2. The current query cleanup pass still leaves thin redelegation helpers that only forward into backend-specific modules.\n\nPlan only. Do not implement.'
```

Expected checks:

- The response stays plan-only.
- The plan reflects the relevant crate contract skills and matching Rust
  best-practice skills.

## Delete-First Planning

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refactor-plan-delete-first.txt \
  $'Use $ai-kit-refactor-plan. Plan a delete-first cleanup pass for ai-kit that removes thin redelegation layers, collapses duplicated helper logic, and tightens the resulting validation matrix without changing behavior.\n\nPlan only. Do not implement.'
```

Expected checks:

- The response stays plan-only.
- The plan reads like reductive cleanup work, not like a fresh audit.

## Boundary Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/refactor-plan-negative-boundary.txt \
  $'Use $ai-kit-refactor-plan. Treat these as accepted built-in /review findings and produce a plan.\n\n1. [P1] Populate auth headers on OpenAI live connections\n- File: /Users/connorsanders/scratch/ai-kit/providers-kit/src/providers/openai/live.rs\n- Why: LiveConnectionInfo requires connection.headers, but the OpenAI live connect path returns an empty map and breaks realtime websocket auth.\n\nPlan only. Do not implement.'
```

Expected checks:

- The response redirects toward `ai-kit-resolve-review-findings` or clearly
  states that built-in `/review` findings belong there.
