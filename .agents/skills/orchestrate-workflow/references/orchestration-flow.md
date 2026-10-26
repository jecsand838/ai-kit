# Orchestration Flow

Use this flow after the user selects findings and names the workflow skill that
workers must use.

## Required Inputs

- Selected findings or issues.
- Base branch or base commit. Default to the current branch when clean.
- Worktree root. Default to a sibling directory such as
  `../ai-kit-worktrees` when the user does not specify one.
- Workflow skill and phase, for example
  `ai-kit-change-workflow` with `INLINE_PLANNING`.
- Merge policy. Default: auto-merge clean, reviewed, validated work only.

## Preflight

1. Run read-only git inspection:
   - `git status --short --branch`
   - `git branch --show-current`
   - `git worktree list`
2. Stop before creating worktrees if the orchestrator worktree has dirty
   tracked changes that would make the base ambiguous.
3. Confirm selected findings have enough scope to assign ownership. If not,
   return the missing detail needed from the user.

## Split Work

1. Group findings by owning crate, module, and files.
2. Mark issues independent only when their writable paths and core concepts do
   not significantly overlap.
3. Keep overlapping issues in a serial queue and explain the dependency.
4. Prefer small branch names:
   - branch: `codex/<short-issue-name>`
   - path: `<worktree-root>/<short-issue-name>`

## Create Worktrees

Use one branch per issue. Do not check out the same branch in multiple writable
worktrees.

```bash
git worktree add -b codex/<short-issue-name> <worktree-root>/<short-issue-name> <base-branch-or-commit>
```

If a branch or path already exists, stop and report it instead of force
overwriting.

## Spawn Workers

Spawn one worker per independent issue using the template in
`worker-contract.md`. Give each worker:

- worktree path
- branch
- issue details
- allowed scope
- forbidden scope
- workflow skill
- workflow phase
- TODO marker when relevant
- validation commands

Do not duplicate the same issue across workers.
