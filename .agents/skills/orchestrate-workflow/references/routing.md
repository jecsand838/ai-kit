# Routing

Use this file after the user selects findings and asks for worktree or
sub-agent orchestration.

## Required Inputs

- Selected findings or issues.
- Workflow skill to use, for example `ai-kit-change-workflow`.
- Workflow phase, for example `INLINE_PLANNING`, `RESEARCH_REFINEMENT`, or
  `IMPLEMENTATION`.
- Base branch or commit. Default to the current branch when clean.
- Worktree root. Default to a sibling `../ai-kit-worktrees` directory when the
  user does not specify one.

## Always Load

- Root `AGENTS.md`
- [orchestration-flow.md](orchestration-flow.md)
- [worker-contract.md](worker-contract.md)
- [review-merge-cleanup.md](review-merge-cleanup.md)
- The workflow skill named by the user

## Use This Skill When

- The user selected findings from `ai-kit-audit` or another backlog.
- The user asks to create worktrees, fork selected issues, spawn workers, run
  parallel sub-agents, or merge worker outputs.

## Do Not Use This Skill When

- The user wants a fresh read-only audit. Use `ai-kit-audit`.
- The user wants a single in-place workflow pass. Use the named workflow skill
  directly.
- The selected issues cannot be scoped to independent ownership.

## Merge Policy

Default to clean auto-merge only. Stop for conflicts, failed validation,
scope drift, unresolved TODO markers, destructive cleanup needs, or unclear
worker output.
