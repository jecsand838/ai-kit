# Stage 1: Development Execution Setup

From the workspace root, perform Stage 1 from
`agents-kit/.codex/development/PROCESS.md`. Resolve workflow paths relative to
`agents-kit/.codex/development/` and complete the common preload there. The
workspace-root cwd is path context only. Preserve the process's recorded scope:
agents-kit-only by default; additional paths/gates require an explicitly approved
coordinated plan. This does not waive Stage 1's action restrictions.

Inspect `execution/` first. If it is nonempty, stop and request explicit
discard authorization; do not delete prior state merely because setup was
requested. Otherwise copy `templates/execution/`, remove copied
`*.example.md`, and create every fixed runtime parent from the process.

Verify the generated tree contains only the required empty directory skeleton,
no generated path is tracked, and tracked templates are unchanged. Do not
audit, run any code validation gate, stage, commit, or push. Report the
skeleton and stop.
