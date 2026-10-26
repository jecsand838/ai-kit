# Stage 3: Vertical Planning

From the workspace root, perform Stage 3 from
`agent-sdk/.codex/development/PROCESS.md` for the latest audit. Resolve workflow
paths relative to `agent-sdk/.codex/development/` and complete the common
preload there. The workspace-root cwd is path context only; enforce the
recorded scope: agent-sdk-only by default, or the exact additional paths and
gates in an explicitly approved coordinated plan.

Dispatch one vertical-analysis worker and validate its artifact. Then dispatch
one vertical-researcher per accepted vertical and validate every artifact.
Create `execution/{timestamp}-plan.md` with the complete tree, file-specific
tasks, exact citations, test linkage, non-overlapping phases, coverage proofs,
and the last-phase gates selected by the process. Ordinary runtime work has the
sole agent-sdk verifier. Additional paths/packages/gates require the approved
coordinated plan; dependencies do not authorize them. Stop for a separately
authorized workflow for any required change or proof outside recorded scope.

Finalize the result as the immutable plan. Do not edit source, tests, specs,
markers, Git state, or remote state.
