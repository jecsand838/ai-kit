# Worker Contract

Customize this contract for each selected issue before spawning a worker.

```markdown
You are working in this worktree only:

`<absolute worktree path>`

Branch:

`<branch name>`

Issue:

`<issue title, severity, and full selected finding>`

Workflow skill:

`<skill name and path if useful>`

Workflow phase:

`<INLINE_PLANNING | RESEARCH_REFINEMENT | IMPLEMENTATION | other named phase>`

Task type:

`<optimization | code-review-fix | feature | bug-fix | refactor | docs | test-hardening | user-provided type>`

TODO marker:

`<task-specific marker or AI-KIT-CHANGE-TODO(<scope>)>`

Allowed scope:

`<crates/files/modules this worker may edit>`

Forbidden scope:

`<paths, crates, modules, and other selected issues this worker must not touch>`

Required instructions:

- Follow root `AGENTS.md`.
- Follow the nearest crate `AGENTS.md` files for the assigned scope.
- Use the specified workflow skill exactly.
- Load relevant ai-kit contract, research, Rust practice, validation, docs, or
  strict-live skills required by the workflow phase.

Hard constraints:

- Do not touch unrelated selected issues.
- Do not revert user changes or other workers' changes.
- Do not use macro-based reductive refactors.
- The only macro exception is `tools-kit::tool!`.
- Do not add shims, hacks, escape hatches, synthetic behavior, deterministic
  output shortcuts, or backwards-compatibility bloat.
- Preserve private exhaustive enum dispatch. Use the public RPITIT trait as the
  sole behavioral contract only at a seam enumerated by root `AGENTS.md`.
  The two open Tool dispatchers retain one private `Extension` arm; the nine
  Provider, Storage, and Agent dispatchers retain concrete built-ins only and
  callers remain statically typed. Use a private `pub(crate)` trait only for
  another real closed family.
- At the approved open Tool seam, every concrete built-in, the private enum,
  and the exported handle implement the public trait. Toolset's `from_backend`
  creates only `Extension`; built-in construction creates concrete arms
  directly and child implementations enter through associated types.
- Do not leak provider-, backend-, database-, agent-, runtime-, or
  source-specific behavior through public portable APIs.
- If required work escapes allowed scope, stop and report a dependent issue
  instead of editing outside scope.

Completion report:

- Summarize changed files.
- Summarize completed phase work.
- Report validation commands and results.
- Report unresolved blockers, conflicts, out-of-scope dependencies, and any
  remaining TODO markers.
```

For code-edit workers, explicitly tell them they are not alone in the codebase
and must not revert edits made by others.
