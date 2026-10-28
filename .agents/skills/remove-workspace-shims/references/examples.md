# Canonical Prompt Examples

Use these prompts when manually validating that `remove-workspace-shims` stays
anchored to accepted findings, preserves the Plan Mode split, and refuses to
invent a fresh audit.

## Plan-Mode Planning Prompts

Use these prompts in a Plan-Mode-capable Codex client.

### providers-kit Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted providers-kit shim
audit findings. Start from the existing DELETE(providers-kit shim audit)
markers, produce the decision-complete delete-first plan, and do not execute
the edits until the thread leaves Plan Mode.
```

### agents-kit Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted agents-kit shim
audit findings. Start from the existing DELETE(agents-kit shim audit) markers,
produce the decision-complete delete-first plan, and do not execute the edits
until the thread leaves Plan Mode.
```

### storage-kit Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted storage-kit shim
audit findings. Start from the existing DELETE(storage-kit shim audit) markers,
produce the decision-complete delete-first plan, and do not execute the edits
until the thread leaves Plan Mode.
```

### tools-kit Planning Prompt

```text
Use $remove-workspace-shims in Plan Mode to remove accepted tools-kit shim audit
findings. Start from the existing DELETE(tools-kit shim audit) markers, produce
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
providers-kit from the existing DELETE(providers-kit shim audit) markers. Delete
the marked shims first, repair the strict-live failures honestly, and run the
exact providers-kit completion gate.
```

Expected checks:

- The response starts with the delete-first pre-pass.
- The response keeps strict-live as the source of truth.

## Missing-Findings Boundary

```text
Use $remove-workspace-shims to clean up providers-kit shims. I do not have any
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
  $'Use $remove-workspace-shims to clean up providers-kit shims without any prior audit findings.'
```

Expected checks:

- The response redirects to `audit-workspace-for-shims`.
- The response explains that `codex exec` is appropriate for boundary checks
  only and does not prove the Plan Mode execution path.
