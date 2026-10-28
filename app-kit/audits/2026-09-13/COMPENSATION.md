# Independent compensation fence finding

**Classification:** Repository observation with executable confirmation. **Priority:** P1, overwriting another caller's lower workspace changes.

`app-kit/specs/WORKSPACE.md:86-89` requires compensation in reverse order using the **exact returned revision** and prior complete value. Current source violates that fence:

1. `src/workspace/mod.rs:817-820` loads the recorded materialization but uses only `workspace_id`.
2. Lines 821-829 fetch the newest lower `WorkspaceInfo`. If its config differs from `prior`, compensation passes this current observation to `apply_update`.
3. `apply_update` at lines 1211-1217 creates its UpdateWorkspaceRequest from `MaterializationRecord::from(current)`, replacing the recorded fence with the latest observed revision.
4. No revision comparison is made before that lower update in `reconcile_compensation` or the surrounding `reconcile` loop (`572-599`).

Concrete scenario: agent a begins with prior config at r1; App replacement succeeds on a and persists returned r2; agent b's update remains pending; another legitimate lower caller changes a to foreign config at r3; b rejects its update. Compensation then submits prior config to a with expected_revision **3**, succeeds at r4, and erases the foreign change. Correct compensation should use the exact returned r2 and receive/retain a conflict against r3. Returning `AppError::Conflict` from the overall replacement does not undo the lost foreign update.

The [public-only bounded probe](compensation-probe.rs.txt), test `audit_compensation_does_not_overwrite_a_foreign_revision`, supplies two direct non-Clone caller Agent hierarchies, uses a separate public lower Agent handle for the foreign r2→r3 mutation, and validates all lower expected revisions. It was temporarily placed in `app-kit/tests/audit_compensation.rs` and run with `cargo test -p app-kit --test audit_compensation -- --nocapture --test-threads=1`. It compiled successfully and failed its invariant in 0.03 seconds: actual `("prior", 4)`, required `("foreign", 3)`, observed lower update fences `[1, 2, 3]`. The temporary Cargo target was removed. No private app records or test hooks were used.

Recommended fix: carry and enforce the operation-owned lower revision from durable materialization through compensation. Treat an unrelated observed lower revision/configuration as conflict without rebasing the write. Define exact receipt-loss reconciliation for a compensation that may already have succeeded, preserving its own effect evidence; do not replace it with a broad latest-config match. Add a regression test through public lower APIs, including crash/reopen during compensation and a foreign same-config revision bump where the desired exact-fence semantics matter.
