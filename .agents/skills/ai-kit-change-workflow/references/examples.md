# Examples

Use these prompts as canonical examples for `ai-kit-change-workflow`.

## Inline Planning For One Crate

```markdown
Use $ai-kit-change-workflow.

Task type: `optimization`
Scope mode: `crate`
Target crate: `tools-kit`
Phase: `INLINE_PLANNING`
TODO marker: `AI-KIT-CHANGE-TODO(tools-kit)`

Identify the code changes needed for the selected finding. Add inline planning
comments only. Do not change behavior.
```

## Research Refinement

```markdown
Use $ai-kit-change-workflow.

Task type: `refactor`
Scope mode: `crates`
Target crates:
- `agents-kit`
- `providers-kit`

Phase: `RESEARCH_REFINEMENT`
TODO marker: `AI-KIT-CHANGE-TODO(agent-provider-boundary)`

Refine existing TODO comments with citations, implementation guidance,
deletion notes, docs impact, and validation impact. Do not implement.
```

## Implementation

```markdown
Use $ai-kit-change-workflow.

Task type: `bug-fix`
Scope mode: `paths`
Allowed paths:
- `storage-kit/src/core/query/**`
- `storage-kit/tests/**`

Phase: `IMPLEMENTATION`
TODO marker: `AI-KIT-CHANGE-TODO(storage-query)`

Resolve scoped TODO comments one by one. Do not remove a TODO until the work it
describes is implemented, tested, documented if needed, and validated.
```

## Read-Only Routing Forward Test

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/ai-kit-change-workflow-routing.txt \
  $'Use $ai-kit-change-workflow. Task type: docs. Scope mode: crate. Target crate: agents-kit. Phase: INLINE_PLANNING. Describe the required routing and phase gates without editing files.'
```
