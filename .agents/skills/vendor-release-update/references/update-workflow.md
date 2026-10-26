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
