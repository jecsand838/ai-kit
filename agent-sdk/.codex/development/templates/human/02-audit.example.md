# Stage 2: Source And Specification Audit

From the workspace root, perform Stage 2 from
`agent-sdk/.codex/development/PROCESS.md`. Resolve workflow paths relative to
`agent-sdk/.codex/development/` and complete the common preload there. The cwd
does not grant workspace validation scope. Record agent-sdk-only scope unless
an explicitly approved coordinated plan names additional paths and gates.

Use one UTC run timestamp. Before analysis, record baseline HEAD and exact
initial staged, unstaged, and untracked path sets. Audit every non-vendored
agent-sdk owner against selected specs under root, core, codex, opencode, and
custom, plus only additional work named by that approved plan. Record any
change or proof outside that scope as a blocker for a separately authorized
workflow. Ask every architectural or behavioral question and obey the process's
specification limits.

Create `execution/{timestamp}-audit-findings.md`, dispatch and reconcile all
five section reviewers, and record observable worker completion. Finalize the
spec paths, accounting, and fingerprint. Preserve approved spec edits unstaged;
do not run code validation, plan, implement, stage, commit, or push.
