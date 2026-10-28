# Examples

Use these prompts as canonical examples for `ai-kit-audit`.

## Workspace Crate-Boundary Audit

```markdown
Use $ai-kit-audit.

Audit the `ai-kit` workspace for crate-boundary architectural improvements
between `agents-kit`, `tools-kit`, `providers-kit`, and `storage-kit`.

Scope mode: `workspace`
Audit topic: crate-boundary architecture

Do not edit files, create branches, create worktrees, spawn sub-agents, or
implement fixes. Return a structured findings backlog or "No findings
identified."
```

## Single-Crate Optimization Audit

```markdown
Use $ai-kit-audit.

Audit `tools-kit` for optimization and cleanup opportunities that preserve
public portability and source contracts.

Scope mode: `crate`
Target crate: `tools-kit`

Return findings with affected files/modules, overlap risk, parallelization
status, and recommended validation commands.
```

## No-Op Outcome

```markdown
Use $ai-kit-audit.

Audit `storage-kit/docs/testing.md` for stale strict-live guidance.

Scope mode: `paths`
Target paths:
- `storage-kit/docs/testing.md`

If there are no actionable findings, respond exactly:
`No findings identified.`
```

## Read-Only Forward Test

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/ai-kit-audit-forwardtest.txt \
  $'Use $ai-kit-audit. Audit the ai-kit workspace skill catalog for stale workflow entries. Do not edit files.'
```
