# Update Workflow

## Planning Mode

Use planning mode when the prompt says to produce `plan.md` without modifying
files.

1. Read the dossier and the relevant workspace or crate `AGENTS.md` files.
2. Identify likely breaking changes, required tests-first coverage, public API
   or behavior adjustments, and doc updates.
   When the dossier uses no-regression completion, scope this analysis to
   sourced release drift and touched-path cleanup. Baseline failures do not
   authorize a general spec audit or backlog implementation.
3. Write a decision-complete plan that is specific enough to execute without
   re-discovery.
4. Keep the response as markdown only because the updater saves it directly as
   `plan.md`.

## Execution Mode

Use execution mode when the prompt points at both `dossier.md` and `plan.md`.

1. Read `plan.md` first, then confirm the details against the dossier inputs.
2. Update or add tests before production changes when the plan requires new
   coverage.
3. Implement the crate changes needed for the new vendor release.
   Do not implement unrelated planned behavior, expose every new upstream
   operation, or clean other backends/crates solely because a baseline gate is
   already failing.
4. Update owning crate or workspace docs if behavior, workflow, or validation
   guidance changed.
5. Do not delete old vendor archives or extracted directories.
6. Do not update the vendor lockfile unless the prompt explicitly delegates that
   mechanical step. In the updater flow, it does not: the updater installs its
   candidate lock before implementation so protocol-authority tests and target
   preflight inspect the selected target archive. Leave the candidate bytes
   unchanged. Target preflight runs after adaptation and before completion gates.
7. Do not create commits, branches, or pushes. Leave that to the updater.

## Strict Review Pause

A parent may invoke a strict single-vendor update with `--implementation-only`
to obtain an independent review before acceptance. This is also available on
`resume` for an existing version-1 strict dossier. Follow the implementation
prompt's focused-check sequencing: adapt the selected vendor and record its
regressions, but leave full preflight and completion gates to the parent-owned
validation phase. The leaf still must not delegate, commit, or change frozen
inputs.

The updater validates the branch, HEAD, scope, frozen inputs, and unchanged
recovery metadata after adaptation. Each invocation snapshots its fixed dossier
inputs, including the saved plan, release metadata, and upstream diff files;
they must remain regular files with unchanged contents through adaptation and
validation. Execution logs and other evidence outputs remain writable. It then
restores the exact original lock and retains implementation edits and both
source archives and extracts. The
parent reviews the actual diff together with `candidate-vendor-lock.toml`;
the checkout's restored original lock does not select the adapted target.

After review, `resume --dossier <dossier-directory> --validation-only` installs
that same candidate without discovery, planning, or implementation, runs every
configured strict gate, and finalizes the local vendor commit on success.
**Validation-only commits on success; it is not a read-only check.** A failed
phase retains edits and evidence and restores the original lock before commit.
Redirected updater outputs are rejected before writes. Restoration refuses
redirected lock, archive, and extraction paths. An absent original archive can
be restored only from an owned regular backup matching its recorded hash.
Restoration failures remain supplementary to the primary failure, and cleanup
attempts continue for remaining owned archives.
Use `resume --implementation-only` for further adaptation of the frozen target.
Split execution is unavailable for `no-regression`; ordinary update and resume
retain their existing complete execution behavior.

## Failure Review

When validation or completion gates fail:

1. Read the failing command output closely and map it back to the dossier scope.
2. Fix the underlying crate issue when it is newly introduced or directly
   owned by the vendor update. Under no-regression completion, preserve and
   report unchanged baseline failures outside the dossier scope.
3. Preserve the new vendor archive and extracted review directory so the updater
   can continue once the crate is fixed.
4. Leave the worktree in a state where `scripts/vendor_update.py resume --dossier
   <dossier-directory>` can continue implementation with the saved plan and rerun
   target preflight and completion gates. The updater restores the exact old
   lock on failure before commit and validates branch, HEAD, hashes, and permitted dirty scope
   before resuming. If interruption follows a successful local commit, keep that
   committed target; inspect unexpected HEAD changes without automatic rollback.
   Do not rerun ordinary `update` over retained edits or replace
   the target with a newly discovered release.
5. If an authorized campaign prerequisite outside the frozen vendor scope must
   land first, return that blocker to the campaign parent. Do not change HEAD,
   widen scope, or edit recovery metadata from the leaf. The campaign may use
   its narrow `import-prerequisite` preparation command after one reviewed
   prerequisite commit. Execute only the resulting new dossier; keep all
   original and copied frozen inputs and original evidence unchanged. This
   does not waive strict target preflight or completion gates.
