# Canonical Prompt Examples

Use these prompts when manually validating that `remove-workspace-shims` stays
anchored to accepted findings, preserves the Plan Mode split, and refuses to
invent a fresh audit.

## Plan-Mode Planning Prompts

Use these prompts in a Plan-Mode-capable Codex client.

### Provider-SDK Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted provider-sdk shim
audit findings. Start from the existing DELETE(provider-sdk shim audit)
markers, produce the decision-complete delete-first plan, and do not execute
the edits until the thread leaves Plan Mode.
```

### Agent-SDK Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted agent-sdk shim
audit findings. Start from the existing DELETE(agent-sdk shim audit) markers,
produce the decision-complete delete-first plan, and do not execute the edits
until the thread leaves Plan Mode.
```

### Storage-SDK Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted storage-sdk shim
audit findings. Start from the existing DELETE(storage-sdk shim audit) markers,
produce the decision-complete delete-first plan, and do not execute the edits
until the thread leaves Plan Mode.
```

### Tool-SDK Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted tool-sdk shim audit
findings. Start from the existing DELETE(tool-sdk shim audit) markers, produce
the decision-complete delete-first plan, and do not execute the edits until the
thread leaves Plan Mode.
```

Expected checks:

- The response starts from the accepted audit findings.
- The response preserves the honest Plan Mode split instead of pretending it
  can mutate immediately.

## Execution Follow-Up Prompt

Use this in the same thread after the Plan Mode phase has ended:

```text
Use $remove-workspace-shims to execute the accepted shim-removal plan for
provider-sdk from the existing DELETE(provider-sdk shim audit) markers. Delete
the marked shims first, repair the strict-live failures honestly, and run the
exact provider-sdk completion gate.
```

Expected checks:

- The response starts with the delete-first pre-pass.
- The response keeps strict-live as the source of truth.

## Missing-Findings Boundary

```text
Use $remove-workspace-shims to clean up provider-sdk shims. I do not have any
existing shim audit markers or accepted findings. Just figure it out and fix
everything.
```

Expected checks:

- The response redirects to `audit-workspace-for-shims`.
- The response does not pretend the audit precondition already exists.

## Boundary Check With codex exec

`codex exec` is useful for boundary checks only because it does not expose a
collaboration-mode selector.
Say plainly that codex exec does not expose a collaboration-mode selector.

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/remove-workspace-shims-boundary.txt \
  $'Use $remove-workspace-shims to clean up provider-sdk shims without any prior audit findings.'
```

Expected checks:

- The response redirects to `audit-workspace-for-shims`.
- The response explains that `codex exec` is appropriate for boundary checks
  only and does not prove the Plan Mode execution path.
