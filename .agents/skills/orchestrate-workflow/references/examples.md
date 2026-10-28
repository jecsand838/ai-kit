# Examples

Use these prompts as canonical examples for `orchestrate-workflow`.

## Inline Planning Workers

```markdown
Use $orchestrate-workflow.

Selected findings:
<paste selected findings from ai-kit-audit>

Workflow skill: `ai-kit-change-workflow`
Worker phase: `INLINE_PLANNING`
Task type: `refactor`
TODO marker: `AI-KIT-CHANGE-TODO(<scope>)`

Base branch: `custom-agents`
Worktree root: `/Users/connorsanders/scratch/ai-kit-worktrees`

Create one worktree branch per independent issue, spawn scoped workers, review
worker diffs, merge only clean validated work, and clean up successful
worktrees.
```

## Serial Queue For Overlap

```markdown
Use $orchestrate-workflow.

Selected findings:
<paste findings that may overlap in `providers-kit/src/core/tools/**`>

Workflow skill: `ai-kit-change-workflow`
Worker phase: `IMPLEMENTATION`

Only parallelize findings whose file ownership does not overlap. Put
overlapping findings in a serial queue and explain the queue order.
```

## Review-Only Failure Outcome

```markdown
Use $orchestrate-workflow.

Review these completed worker worktrees and merge only clean results:
<paste worktree paths and branches>

Stop instead of merging if validation failed, conflicts exist, or a worker
touched forbidden scope.
```

## Read-Only Routing Forward Test

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/orchestrate-workflow-routing.txt \
  $'Use $orchestrate-workflow. Given these selected findings, describe the worktree and worker split without creating branches or editing files: <findings>'
```
