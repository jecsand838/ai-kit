# Production defects discovered by direct mock tests

Original baseline: `ed507711410e4f75aa98f44819ab80fedd8e806e`.
The original mock-suite task froze production. The separately authorized
cancellation follow-up started at `1de968b3c98015f7fc48255d6ac40ef4593ca44d`.
Regressions remain enabled; fixture and validation-setup errors are distinguished
from production defects. Manifests, lockfiles, features, and vendor sources are unchanged.

## Repair status — 2026-10-25

M001 remains resolved. The separately authorized follow-up from `2da7fcaf`
repairs M002 and the independently reproduced `MOCK-APP-001`, with enabled
regressions in required primary targets. Focused evidence below distinguishes
the original failures from repaired behavior. The historical primary CI failure
at 132/133 and the separate restricted pass at 133/133 remain in
[validation history](pr22-validation.md); they do not qualify the repaired source.
Consult the PR's validation results for source-specific primary and restricted
qualification; focused repair passes alone do not establish either. Full workspace
release qualification remains unproven.

## MOCK-AGENT-001 — an unpolled unary prompt observer blocks completed cancellation

**Affected crate/status:** Custom `agents-kit`; resolved by the supplied patch, with its required regression enabled.

**Contract and expected behavior.** [Runtime Abort](../agents-kit/specs/RUNTIME.md#abort)
requires success after the captured generation reaches a valid durable terminal
and cancellation dispatch settles. The prompt waiter alone receives its result.
[Foreground prompt](../agents-kit/specs/CONVERSATION.md#foreground-prompt),
[ConversationBackend](../agents-kit/src/conversation.rs), and the
[Custom architecture](../agents-kit/ARCHITECTURE.md#custom-prompt-loop) preserve
runtime ownership independently of observation. Abort must complete while a
unary reply remains unread; later reading returns `Cancelled`, without duplicate effects.

**Original behavior and impact.** Keeping an accepted unary prompt alive but
unpolled after its Tool interaction made abort return `Timeout`, operation
`Abort`, phase `Cancellation`, message `prompt settlement did not converge`.
The Tool had already cancelled once. This produced false cancellation timeouts
and implicated cleanup paths waiting for the same settlement.

**Required tests.** `abort_pending_interaction_cancels_exactly_one_handler` in
[custom_mock_composition.rs](../agents-kit/tests/custom_mock_composition.rs)
retains the unpolled observer, repeats abort, consumes `Cancelled`, verifies one
cancellation and no interaction submission, and cleans up before asserting success.
`polling_unary_observation_concurrently_allows_abort_to_join_one_cancelled_handler`
is the concurrent-observation control; streaming reuse is covered by
`explicit_abort_owns_cancellation_and_conversation_remains_reusable`.
The added unread-reply-drop case requires owned cleanup after abort.

```sh
cargo test --locked -p agents-kit --test custom_mock_composition abort_pending_interaction_cancels_exactly_one_handler -- --exact --nocapture
cargo test --locked -p agents-kit --test custom_mock_composition
cargo test --locked -p agents-kit --test custom_prompt_runtime --all-features
```

**Fixture mistakes ruled out.** The scripted handler cancels immediately with no
await or gate; cancellation is exactly once and no storage fault is armed.
The same Provider/Tool/process-local StateStore passes concurrent-polling and
streaming controls. No service, credential, socket, process, or extension ingress
is involved. Independent review confirmed the contract violation.

**Diagnosis and fix.** The original `CustomPrompt::settle` awaited actual unary
acknowledgement before recording settlement; abort awaited that settlement.
The fix retains acknowledgement and full close/shutdown quiescence, but abort
observes durable terminal publication independently. Captured waiter settlement
is generation-specific. Unchanged continuation mutations roll back, preserving
rollback errors; changed mutations retain commit/reconciliation authority.
Production changes match the supplied patch exactly and remove 56 net lines.
Existing close fixtures now inject rollback where the redundant write was removed;
all fault, commit-authority, caller-loss, cleanup, no-replay, and recovery cases remain.

**Retained evidence.** Original 48-case runs had 47 passes and this sole failure.
Historical primary/restricted receipts are `target/mock-contracts/20261025T165348Z-817157b1/report.json`
and `target/mock-contracts/20261025T165457Z-34708207/report.json` (131/132 Rust cases;
18 runner tests passed). Fresh strengthened red evidence is
`target/mock-agent-001-resolution/red-custom-mock.log` (47 passed, two failed)
and `target/mock-contracts/20261025T220705Z-32b5f27b/report.json` (gated Tool case failed).
The exact-diff manifest is `target/mock-agent-001-resolution/applied-diff.json`.
Fresh focused evidence is `target/mock-contracts/20261025T220805Z-76e4858e/report.json`:
49 mock cases, 57 runtime cases, and all-target/all-feature Agent Clippy passed.
Close fixtures: `target/mock-contracts/20261025T223105Z-45905da5/report.json` (22 public cases and targeted Clippy passed); source contracts: `20261025T223527Z-060e8502` (15 passed after verified archive extraction).
Primary CI at the M001 follow-up: `20261025T223224Z-332b9212/report.json` (132/133 Rust cases; the App shutdown case below failed; 18 runner cases passed).
Separate restricted runtime: `20261025T223300Z-d464579e/report.json` (all 133 Rust cases passed). These receipts live beneath `target/mock-contracts/`.
The isolated rollback-error probe also passed in `20261025T221012Z-651c3165`.
Initial CI/restricted runs sharing the supplemental build directory are invalid
root-source evidence; `target/mock-agent-001-resolution/artifact-isolation-correction.json`
records their identities and correction. No failing test was weakened to produce a pass.

**Independent work and limits.** Other trait/consumer tests remained independent.
The original regression no longer blocks primary CI. Strict-live/workspace release
completion remains unproven: after the three model-role exports were corrected,
authenticated OpenAI/Anthropic generation succeeded. Refreshing only the isolated
Codex auth cache from the existing login then made native account validation and
the full precheck pass. At the user's request, the full verifier was interrupted;
the focused Codex generation/effort/reset/reopen case passed (1 passed, 28 filtered).
See `target/mock-agent-001-resolution/auth-resolution-result.json`; owned services were joined.

## MOCK-AGENT-002 — delayed abort cancels a successor interaction

**Affected crate/status:** Custom Agent; repaired in the authorized follow-up
from `2da7fcaf`. This is separate from M001's unread-reply settlement.
[Runtime Abort](../agents-kit/specs/RUNTIME.md#abort) and the
[Custom cancellation contract](../agents-kit/specs/custom/CONVERSATION.md#cancellation-and-durability)
exclude later prompts from a captured target and require owned handler cleanup.

**Expected and observed behavior.** Hold an abort observer after its admitted
cancellation-intent commit, complete its prompt, start a successor Tool
interaction, then resume the old abort. Abort should succeed for its completed
target and leave the successor independently resolvable. Instead it returned
`Conflict` (`Abort` / `Cancellation`, `prompt identity changed before cancellation`)
and cancelled the successor's interaction channel. No Tool `Cancel` was sent,
leaving an acquired handler unsettled. Applications could lose unrelated new
work and leak pending handler cleanup.

**Diagnosis and repair.** The abort observer invoked conversation-wide
interaction cancellation even after generation-specific cancellation rejected its
stale target. It now checks the captured generation under the existing fence and
synchronously dispatches only while that exact generation remains active. The
prompt owner settles its own interactions before durable terminal publication;
an old abort joins only its captured notification. A cancellation-intent
`Conflict` is reconciled only with positive captured-terminal proof. A closed
notification source, unexplained conflict, or non-conflict error is not success.

Separately, the Tool adapter's interaction-wait error escape dropped an acquired
handler. It now cancels that handler exactly once and returns the original
error, retaining cancellation failure as supplementary `Cleanup`. Generation
state, durable schemas, unary acknowledgement, and close/shutdown quiescence are
unchanged.

**Required regressions.** The former supplemental probe is now committed in
[custom_mock_composition.rs](../agents-kit/tests/custom_mock_composition.rs):

- `delayed_abort_does_not_cancel_a_successor_interaction` requires successful old
  abort, normal successor completion, two resolved interactions, zero
  cancellations, and no outstanding handlers.
- `delayed_abort_intent_read_does_not_reject_a_completed_target_or_cancel_its_successor`
  covers a target completing before intent reads it. It settles the admitted
  control-plane mutation before admitting the successor; matching terminal state
  is a successful no-op, not evidence of the Conflict branch.
- `abort_reconciles_intent_conflict_only_after_captured_durable_terminal` gates
  the intent's post-commit read until the real prompt publishes its terminal,
  proving reconciliation of the resulting state mismatch.
- `abort_retains_nonconflict_intent_failure_after_captured_terminal` preserves
  `Integration` / `ServiceUnavailable` even with terminal proof;
  `abort_retains_unexplained_intent_conflict_while_target_is_active` preserves
  `Conflict` / `TransactionConflict` without that proof. Both retain the primary
  diagnostic and allow natural prompt completion without handler cancellation.
- `unpresentable_tool_interaction_cancels_its_acquired_handler_once` and
  `unpresentable_tool_interaction_retains_primary_error_when_handler_cleanup_fails`
  explicitly supply a malformed interaction. Both retain the exact primary
  `Integration` message and require one Cancel; the latter also requires
  supplementary cleanup failure.

```sh
cargo test --locked -p agents-kit --test custom_mock_composition delayed_abort_does_not_cancel_a_successor_interaction -- --exact --nocapture
cargo test --locked -p agents-kit --test custom_mock_composition
```

**Fixture mistakes ruled out.** The admitted commit gate is settled independently
of its waiter. The successor is demonstrably active when the old abort resumes.
Typed observations count all cancellation attempts, including rejected terminal
reuse. One-shot transaction/load gates have integrity controls for current-state
observation, independent transactions, consumption, and error fidelity.
Malformed interactions are explicit invalid-source cases; conforming handler
behavior is unchanged. All M001 unread-reply, repeated-abort, stream,
observer-drop, and cleanup controls remain enabled.

**Retained evidence and source.** The original supplemental reproduction remains
in `target/mock-contracts/20261025T222216Z-57477342/report.json`, comparing
`1de968b3` and the M001 patch. The required-target red run against unchanged
production at `2da7fcaf25f0b6b5f06080780ef8a1444063de52` is
`target/mock-contracts/20261025T233707Z-fcaa1465/report.json`: 51 Agent cases
passed and the stale-abort plus two acquired-handler regressions failed.
The initial run `20261025T233330Z-106f8a4d` additionally exposed a test scheduling
mistake; its diagnosis remains in `target/repair-m002-app/initial-red-diagnosis.json`.
It is retained separately from corrected red evidence. First repaired full-target
validation passed 54/54 Agent cases in
`target/mock-contracts/20261025T234102Z-19e03926/report.json`. After independent
review added the three intent-error controls and their fixture integrity test,
all 58 Agent and 34 App cases passed on unchanged source in
`target/mock-contracts/20261025T234617Z-fdfce13e/report.json`, with zero ignored
or filtered cases. Independent review approved the production, regression,
fixture, and documentation diffs.

**Independent work and limits.** App repair and the remaining trait suites were
independent of this defect. This repair promotes required regression coverage
and does not claim native Codex/OpenCode or full strict-live qualification.

## MOCK-APP-001 — a conflicting read observation permanently fails shutdown

**Affected crate/status:** App; independently reproduced and repaired in the
follow-up from `2da7fcaf`. [App shutdown](../app-kit/specs/APP.md#shutdown) owns
bounded drain/cancellation and recoverable read observations; the
[storage architecture](../app-kit/ARCHITECTURE.md) retains serializable snapshot
validation and fresh deletion/generation fences.

**Expected and observed behavior.** Shutdown captures a nonempty read-only run
transaction while a real action is pending. The action then completes and
publishes a newer revision. Committing the old read observation must discard the
stale snapshot and observe again. Instead, both `Drain` and `CancelAndDrain`
returned permanent `Conflict("app state transaction conflicted")`, despite one
successful action execution. Users could receive a retained shutdown failure
from ordinary progress by their own accepted work.

**Diagnosis and repair.** `AppStore::load_run_for_recovery` converted its consuming
read-only commit conflict through the global permanent error mapping. That one
site now discards only `StateStoreError::TransactionConflict` and repeats the
existing bounded observation loop using a fresh transaction. Exhaustion remains
recoverable `Busy`, allowing the finalizer's existing bounded backoff. Global
conversion, mutation retries, uncertain receipts, rollback vetoes, semantic
conflicts, and deletion/generation fences are unchanged.

**Required regressions.** In
[workflow_mock_composition.rs](../app-kit/tests/workflow_mock_composition.rs),
`drain_shutdown_reobserves_a_conflicting_read_only_transaction` and
`cancelling_shutdown_reobserves_a_conflicting_read_only_transaction` hold the
next nonempty read-only commit, complete the real gated action, then release the
read. They require exactly one actual revision conflict, successful shutdown,
one action execution, and a settled action source. The one-shot fixture integrity
test proves empty/write transactions cannot consume the gate, a concurrent
writer causes the conflict, retained observations remain immutable, and a fresh
read succeeds. `shutdown_retains_a_permanent_read_commit_failure_without_retry`
requires a non-conflict serialization failure to remain `Integration` on both
initial and repeated shutdown observations.

```sh
cargo test --locked -p app-kit --test workflow_mock_composition drain_shutdown_reobserves_a_conflicting_read_only_transaction -- --exact --nocapture
cargo test --locked -p app-kit --test workflow_mock_composition cancelling_shutdown_reobserves_a_conflicting_read_only_transaction -- --exact --nocapture
cargo test --locked -p app-kit --test workflow_mock_composition
```

**Fixture mistakes ruled out.** No conflict is injected into the two regressions:
the fixture's ordinary serializable revision check detects real action
publication. The gate applies only to nonempty read-only commits, is consumed
once, and holds no ledger lock across its bounded wait. Gates and owned futures
settle before outcome assertions. The permanent-error control intentionally uses
a serialization failure rather than assuming the finalizer preserves every
input error category.

**Retained evidence and source.** The unchanged-production red run at
`2da7fcaf25f0b6b5f06080780ef8a1444063de52`,
`target/mock-contracts/20261025T233707Z-fcaa1465/report.json`, has 32 App passes
and the two shutdown regressions failing with the exact conflict above. The
initial run `20261025T233330Z-106f8a4d` also exposed an incorrect error-category
expectation in the negative control; the corrected run supersedes that fixture
expectation, not the production failures. First repaired full-target validation
passed all 34 App cases in `20261025T234102Z-19e03926/report.json`.

The historical primary log
`target/mock-contracts/20261025T223224Z-332b9212/32-app-kit-workflow_mock_composition.log`
failed `state_workflow_retains_checkpoints_restores_independently_and_deletes_explicitly`
at shutdown. Its assertion remains unchanged. That log alone does not establish
which internal read produced its conflict; MOCK-APP-001 is classified from the
new deterministic reproduction. A previous 30/30 pass did not resolve the
historical intermittent result.

**Existing contract alignment.** The broader unchanged-source run
`target/mock-contracts/20261025T234708Z-f9b0aefa/report.json` exposed one older
receipt-fault assertion in `read_only_run_observation_preserves_finalization_failures`
that required a single read-only conflict to remain fatal. That expectation
conflicted with the authorized retry behavior. Non-conflict load/commit failures
remain enabled in that test. The new enabled
`read_only_run_observation_retries_conflicts_with_a_bounded_budget` requires a
fresh observation after one conflict, bounded `Busy` under persistent conflicts,
recovery after disarming, and unchanged native records and revisions. It does
not fix the public contract to a private retry count. These injected receipt
faults are distinct from the genuine revision conflicts in the primary tests.
Independent review approved the test-only correction; production was unchanged.
The broad run's other gates passed, including primary CI (146 cases), Agent
contracts (113), routine App workloads (218, with 11 documented exclusions), both
all-target Clippy checks, docs/doctests, and restricted execution (146). The failed
contract result remains retained; subsequent affected-check evidence belongs to
its own fresh receipt.

**Independent work and limits.** The Agent and other primary targets could proceed
while this defect was isolated. The scoped repair removes the demonstrated
read-observation failure. It does not qualify App maximum capacity, native
persistence recovery, strict-live services, or the full workspace release.

## APP-002 — cancellation can be missed while execution reloads state

**Scope.** This defect was discovered by the required native-backed
`child_executor_contract` integration target, outside the primary direct-trait
mock lane. The user separately authorized its repair on
`documentation-improvements`; the existing cancellation deadlines and assertions
remain required.

**Observed failure.** On unchanged `main` at `8184f485d8ac517976409e2953355753c155ee0a`,
an isolated `shutdown_waits_for_the_original_cancelled_child_commit_receipt` run
failed its four-second cancellation-receipt observation. Isolated runs on the
documentation branch also exposed intermittent behavior. Phase diagnostics
showed parent cancellation returning by approximately 43 milliseconds into the
observation, followed by no child cancellation receipt before the deadline. The
two-second shutdown observation was never reached. Retained logs are
`target/cancellation-timeout/main-isolated-01.log` and
`target/cancellation-timeout/branch-diagnostic-04.log`.

**Diagnosis and repair.** Execution consumed a run notification before reloading
state but did not register its next listener until after that asynchronous read.
A cancellation broadcast during the read could therefore be lost. Returning an
older, valid snapshot left the original execution waiting for another change.
Notification registration now belongs to publication admission for both initial
loads and reloads. Either the read observes the cancellation or its retained
listener prompts another observation. Native commit receipts, cancellation
ownership, retry policy, public APIs and deadlines are unchanged.

**Required regression.**
`parent_cancellation_survives_a_stale_read_after_child_completion` in
[child_executor_contract.rs](../app-kit/tests/child_executor_contract.rs) completes
one original child and retains another unpolled. Its caller-supplied store holds
one real parent-read response after the native read has released ownership.
Public cancellation must commit while that response remains held. After releasing
the stale response, the test requires the original child cancellation receipt
within four seconds, before any shutdown can supply another wake. It also checks
exact child identity, no second-child effect or premature parent reduction, and
retained receipt ownership through the existing two-second shutdown observation.
The pre-fix run failed the receipt assertion in
`target/cancellation-timeout/stale-read-red.log`; its secondary cleanup failure
is retained rather than reclassified. The original recovered-case test remains
enabled alongside this deterministic regression.

The final fixture repeats the same pre-fix assertion failure in
`target/cancellation-timeout/stale-read-final-fixture-red.log` without the
secondary runtime panic. On failure only, it wakes its original retained
executor with an explicit error before shutdown; the failed cancellation
observation still determines the test result.

Current source-specific passes, failures and outstanding qualification belong
to the PR validation evidence. Reproducing this race and passing a focused
regression do not establish full App or workspace qualification.
