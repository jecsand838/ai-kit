# Review, Merge, And Cleanup

Use this after workers finish.

## Review Each Worktree

For each completed worker:

1. Inspect status and diff from the worker worktree.
2. Check that edits stay inside the assigned scope.
3. Check that the requested workflow phase was honored.
4. Check that validation ran or that blockers are concrete.
5. Reject or send back work that:
   - changed behavior during a comment-only phase
   - left scoped TODO markers unresolved during implementation
   - introduced macros outside `tools-kit::tool!`
   - added shims, hacks, synthetic behavior, or compatibility bloat
   - leaked backend- or provider-specific behavior through public APIs
   - skipped required validation without a concrete blocker

## Merge Policy

Default to clean auto-merge only:

1. Switch to the base branch in the orchestrator worktree.
2. Merge one reviewed branch at a time with `git merge --no-ff`.
3. Run the relevant validation after each merge or after a low-risk batch when
   the selected issues are truly independent.
4. Stop on conflicts, failed validation, unclear diffs, or scope drift.

If review finds a small integration or validation issue that stays within the
worker's assigned scope, fix it in that worker branch and rerun validation
before merging. Treat conflicts, broad redesign, or cross-scope changes as
blockers instead of patching through them in the orchestrator worktree.

Do not use destructive commands to resolve conflicts. If a conflict appears,
report the files and recommended next action.

## Cleanup

After a branch is merged and validation is acceptable:

```bash
git worktree remove <worktree-path>
git branch -d <branch-name>
git worktree prune
```

Do not delete worktrees or branches with unresolved worker changes, failed
validation, or unmerged commits.

## Final Report

Report:

- merged branches
- cleaned worktrees
- queued serial issues
- blocked issues
- validation results
- follow-up actions needed before the next orchestration pass
