# Production defects discovered by direct mock tests

Original baseline: `ed507711410e4f75aa98f44819ab80fedd8e806e`.
The original mock-suite task froze production. The separately authorized
cancellation follow-up started at `1de968b3c98015f7fc48255d6ac40ef4593ca44d`.
Regressions remain enabled; fixture and validation-setup errors are distinguished
from production defects. Manifests, lockfiles, features, and vendor sources are unchanged.

## Codex 0.167.0 protocol adaptation — vendor update

The separately authorized Codex-only update starts at
`24a33f54401f502fdcdea776ec64fcf854bca18d`. These regressions exercise the public
Agent through a WebSocket protocol fixture and real Lance state, outside the
primary direct-trait mock lane. They establish adapter behavior, not native
Codex qualification. The updater owns the candidate vendor lock and archive.

Before each production area changed, enabled `codex_prompt_stream` tests
reproduced the following failures. Receipts, complete logs and source hashes
are retained under
`target/vendor-updates/agents-kit/codex/rust-v0.167.0/implementation/`.

| Area | Red receipt | Required regression |
| --- | --- | --- |
| Selected runtime identity | `01-identity-red`, `02-previous-identity-red` | Accept exactly 0.167.0; reject the previous 0.163.0 before publication or thread creation |
| Human-only approval kind | `04-human-command-red` | Recognize valid `humanCommand` as unsupported, preserve malformed-request rejection, and create no interaction or acceptance |
| Disappeared callback | `06-callback-retirement-red` | `disappeared_callback_cancels_only_exact_pending_identity` requires atomic cancellation of the exact still-pending journal and continuation, with no stale response |
| Typed errors and metadata | `10-metadata-errors-red`, `11-section-validation-red` | Preserve model-not-found and capacity details, reject malformed known errors and missing section metadata, and route foreign sections privately |
| Optional item timestamps | `14-item-times-red` | `item_times_reject_wrong_types_and_overflow_in_live_and_history` rejects invalid signed-millisecond values at both decode boundaries |
| Malformed private payloads | `25-parent-payload-red-network` | `codex_release::payloads` rejects malformed image references, hook summaries and process drain status while valid observed media remains private |
| Structured provider rejection | `26-parent-provider-error-red` | `structured_rejections_preserve_reader_and_explicit_next_prompt` requires PreconditionFailed/PolicyRejected, safe unknown fallback, malformed-known Protocol and continued reader ownership |
| Callback inventory deadline | `29-parent-callback-read-red-fixture` | `disappeared_callback_bounds_inventory_observation` requires a held read to fail within the original lifecycle deadline, with no approval response |
| Media identity consistency | `31-media-identity-red` | The two `selected_media_*identity*` cases reject missing/malformed media IDs in history and non-string live user client IDs, preserving exact existing generation extraction |

The full target passed 147 cases in `15-items-compatibility-green`, including
callback intent/commit races, definite conflict replay, ambiguity preservation,
flat item ordering and private media compatibility. Further affected-target
results belong to the implementation receipt. The failed `05-human-command-green`
invocation was a new fixture expectation error, and `09-callback-races` failed
to compile a new test wrapper; both failures remain preserved, not relabeled
as production evidence or passes.

All regressions remain enabled. The required independent diff review and native
strict gates are deferred to the parent's validation-only phase. A stale
upstream-source assertion outside this update's write scope remains a blocking
failure in `08-authority-coordinates`; repaired source-coordinate checks pass
separately in `13-authority-coordinates-expanded`. No focused result waives that
failure or establishes workspace or vendor release qualification.

Parent review identified the omitted typed payload work above. Receipt
`27-parent-payload-green` passes those five focused cases, including identical
live/history media validation. The initial parent invocation `24-parent-payload-red`
could not bind loopback sockets; it is environment evidence only. The structured
error case in receipt 25 timed out at the existing start-reconciliation boundary
before its intended assertion. The corrected fixture in receipt 26 reproduces the
actual wrong error detail through a direct received response; the earlier timeout
is retained as a fixture failure. These results do not waive the full target or
native qualification. The stale workspace authority anchor was independently
reviewed and repaired in prerequisite `4f264e8`; both source versions pass its
wire-fact assertion without changing either archive.
The callback inventory deadline regression exposed an unbounded read in the new
retirement path before its final repair. Definite conflict exhaustion, ambiguous
commit without replay, and primary staging failure with supplementary rollback
failure passed as compatibility controls in the same invocation. Receipt 28 was
a new fixture compile error and remains separate from behavioral red evidence.

## Surreal column-removal recovery — Storage vendor campaign

The separately authorized `upgrade-vendors` follow-up preserves a real
SurrealDB 4.0.0 WebSocket schema failure: the atomic field-definition and
stored-value removal received a typed transaction conflict at COMMIT. The
required Surreal strict lane reported 65 passes and one failure; the Lance
strict lane passed all 37 cases. The conflicting writer is unknown. No causal
link to the campaign's compatible Zstd backports was established.

Before production changes, the enabled `column_drop_*` cases in
`storage-kit/tests/surreal_partition_deadline.rs` reproduced five failures:
retry success, nonzero retry exhaustion, query/context deadlines and backoff.
The complete target reported 19 passes and five failures. The fixture uses the
public adapter and pinned native CBOR errors; it is wire/deadline evidence,
not direct-trait mock coverage or a real-service substitute. Original native
evidence remains under
`target/vendor-campaign-storage-20261108/security/zstd-remediation/failed-strict-live/`;
tests-first evidence remains under
`target/vendor-campaign-storage-20261108/surreal-column-recovery/`.

The regressions require bounded replay only after confirmed rollback, preserving
all uncertain or mixed-error failures without replay. Existing collection and
partition controls remain enabled. The native schema case additionally compares
exact three-record snapshots across removal, same-name re-addition with a changed
computed value, and removal again. Fresh native validation and final campaign
gates remain required; historical or fixture passes do not qualify the update.

## Release completion and version identities — 2026-11-05

The v0.0.4 preparation reproduces the release helper creating a public GitHub
release without hosted-doc readiness, before separating publication/finalization.
Enabled release tests now require five exact-version docsites and the reviewed
logo, immutable publication evidence and no upload during finalization retries.
Red/green receipts remain under `target/v0.0.4-preparation/`. The workflow
regression first failed because no separately retryable finalization job existed.
The three existing wire-version tests also each failed with observed `0.0.3`
against bumped `0.0.4` manifests, then passed after updating the identifiers.
These are scoped release regressions, not fresh native qualification.

## Rustdoc logo paths — 2026-11-05

At `fd92dfe`, the documentation-contract logo regression failed for all five
crates before repair: three used a workspace-relative URL and two lacked a logo.
Local nested Provider, Storage and Tool pages returned HTTP 404 for their image;
the crate-root image returned 200. Receipts remain in
`target/v0.0.3-preparation/logo-{regression,behavior}-red.*`. The enabled
`test_rustdoc_logos_work_outside_the_workspace_site` requires one shared,
public commit-pinned URL; its availability is checked separately before release.

## Provider session refactor evidence — 2026-11-01

The separately authorized session replacement starts at `2853a542`; its target
guidance was committed first as `eca58db`. The following defects were reproduced
through production wire paths before replacing the implementation. These are
wire regressions, distinct from the direct-trait mock lane. All remain enabled.

At PR #45 revision `1c537ae`, the following enabled session regressions failed before the fixes. Raw receipts are under `target/pr45-session-review-fixes/`.

- `red-managed.log` — unrenderable Ollama and mixed OpenAI tool results received receipts, and 1,024 dropped idle owners exhausted registration.
- The initial reaper panicked outside Tokio and retained failed cleanup slots. `red-reap-off-runtime.log` and `red-reap-cleanup.log` preserve those separate failures.
- `red-openai.log` — a later capture stole an earlier acknowledgement, a final result admitted a competing input, sealing lost an admitted continuation, and queued receipts lacked individual unstarted failures.
- `red-gemini.log` — a detached output transcript survived its generation boundary, and shutdown left queued commands unfinished.

| Defect | Required regression | Recorded red behavior |
| --- | --- | --- |
| Missing/unknown Realtime terminal status became success | `session_lifecycle::session_refactor_rejects_missing_or_invalid_terminal_status` | `Completed` without affirmative status |
| Realtime terminal usage was discarded | `session_lifecycle::session_refactor_preserves_authoritative_terminal_usage` | Completion arrived without final usage |
| Known-cancelled queued function calls remained executable | `session_lifecycle::openai_audio_transcription_functions_and_cancellation_are_normalized` | Cancelled call delivered instead of invalidation |
| CR-only SSE framing lost completion | `wire_behavior::sse_carriage_return_lines_and_fragmented_crlf_preserve_completion` | `InvalidResponse(UnexpectedEnd)` |

Raw local red logs are retained under `target/session-refactor-evidence/` as
`openai-terminal-red.log`, `openai-cancelled-tools-red.log`, and `framing-red.log`.
Missing replacement surface and ownership compilation failures are separately
recorded in `public-contract-red.log`, `ownership-contract-red.log`,
`new-session-profiles-red.log`, and `managed-contract-red.log`; those failures
alone are not behavioral proof.

Tests against the first replacement implementation also reproduced accepting an
unknown playback segment (`openai-behavior.log`), invalidating valid tools when
cancelling an already completed response, and omitting retained configuration
from the aggregate budget (`managed-behavior-rerun.log`). The earlier managed
run failed on an unsupported fixture model and is retained as fixture error,
not as cancellation evidence. `gemini-env-red.log` records the verifier missing
the new required interactive model role. Each repair requires passing
regressions, independent review, and final-source validation; these failure
records do not establish release qualification.

Independent review added enabled regressions for the following replacement
defects. Each log below records a behavioral failure before its associated fix;
the later full source gates remain authoritative for completion.

| Area | Behavioral red logs under `target/session-refactor-evidence/` |
| --- | --- |
| Lost/reassigned native command correlation and repeated control receipts | `openai-correlation-red-rerun.log` |
| Completion winning cancellation incorrectly invalidated executable tools | `cancellation-race-red.log` |
| Admitted automatic audio lost during provider sealing | `openai-audio-admission-red.log`, `gemini-audio-ordering-red.log` |
| Native tool declaration, frame bounds, duplicate terminals and late output | `gemini-reviewed-red-rerun.log` |
| Accepted command secretly retained after failed submission | `managed-capacity-red.log` |
| Control byte reserve, metadata accounting and fabricated cancellation identities | `managed-reviewed-bounds-red.log` |
| Admitted managed completion discarded and undeclared calls published | `managed-sealing-tool-red.log` |
| Terminal provider error misclassified as partial completion | `managed-terminal-error-red.log` |
| Premature managed transport EOF misclassified as a settled failure | `managed-unresolved-red.log` |
| Concurrent cancellation receipt orphaned and partial-tool continuity unsafe | `managed-cancel-handoff-red.log` |
| Executable tools published before reserving all retained completion copies | `completion-copy-budget-red.log` |
| Native control acknowledgement had no deadline | `openai-control-timeout-red.log` |
| Accepted-but-unstarted native work lacked an explicit failure | `gemini-unstarted-red.log` |
| Native malformed identity/argument errors leaked caller-input classification | `gemini-boundaries-behavior-red.log`, `openai-unstarted-errors-red.log` |
| Native interruption/turn boundaries disappeared or changed attribution | `gemini-boundaries-behavior-red.log` |
| New manual audio after tool interruption lost its generation ownership | `gemini-tool-interruption-red.log` |
| Supplied command error omitted the public operation context | `session-submit-context-red.log` |
| Session registration exhaustion omitted the open-session operation context | `session-registration-context-red.log` |
| Gemini setup omitted blocking function execution required by the portable continuation contract | `gemini-blocking-tools-red.log` |
| Live evidence accepted uncorrelated completion or old-response output | `session-evidence-red.log`, `session-evidence-correlation-red.log` |
| Cancellation evidence accepted a different command receipt or target | `cancellation-evidence-correlation-red.log` |
| Final transcription snapshots were omitted instead of replacing deltas | `transcription-snapshot-red.log` |
| Gemini's native resumption metadata terminated an otherwise valid same-socket interaction | `gemini-resumption-metadata-red.log` |
| Valid Gemini empty/VAD envelopes and transcription markers were rejected; malformed optional fields were silently ignored | `gemini-optional-envelopes-red.log` |
| Evidence rejected a new unattributed input transcript after the preceding transcript finalized | `input-transcription-evidence-red.log` |
| Final-only unattributed input snapshots overwrote previously completed transcripts | `input-final-only-transcript-red.log` |

The native-boundary API compile failure is separately retained in
`native-boundary-contract-red.log`. The initial Gemini close failures exposed a
shared socket defect: production close now drains the peer acknowledgement
within its original deadline, while retaining the strict fixture. The subsequent
OpenAI and shutdown fixture failures separately required flushing their queued
WebSocket close acknowledgements. Unexpected transport closure remains an error.
No suppressed cleanup failure,
skipped regression or narrowed native acceptance replaces these records.

The first cancellation-evidence invocation in `cancellation-evidence-red.log`
selected zero tests and is not red evidence; the corrected correlation log above
contains the actual failure. `release-target-inventory-red.log` separately records
the stale test inventory omitting the three new Provider targets already included
by the validation runner. Its additional
Tool assertion failure came from the same fixture's accumulated trace after the
Provider mismatch, not a separate Tool defect.

At candidate `597e5dd`, the focused eight-case live run passed all four
Conversation baselines and failed all four Gemini Live cases. Private native
diagnostics identified a `sessionResumptionUpdate` notification after setup,
which the adapter incorrectly treated as reconfiguration. The deterministic
`live_resumption_metadata_does_not_reconfigure_or_interrupt_generation`
regression reproduced termination of the original response before its fix;
the malformed-metadata and repeated-setup control remained passing. The
fixture uses a synthetic handle with the independently observed native shape.
Accepting this auxiliary metadata does not authorize retaining or replaying a
resume handle, sending another setup, or reconnecting. The original live failure
remains in `target/session-refactor-evidence/new-live-sessions.log`.

The subsequent four-case retry still failed; its private diagnostics supplied
the observed empty server envelope and uncorrelated input voice-activity shape.
The next deterministic batch recorded five failures against 19 passing controls,
including preserved original deadlines under auxiliary traffic and optional
transcription completion markers. The two evidence-helper failures separately
demonstrated conflation of consecutive input transcripts with unavailable command
identity; the attributed post-final-delta rejection control remained passing.
Those failures and the live retry remain retained in their own logs, including
`target/session-refactor-evidence/gemini-live-resumption-fix.log`.
The separate large-transcript fixture was added before reservation-transfer
implementation. Its red in `gemini-transcript-budget-red.log` reaches the
missing-marker protocol error, with a retained secondary close timeout; that
failure does not independently demonstrate double charging. The enabled case
requires two consecutive, consumed 17 MiB transcripts to finalize within the
existing session budget, exercising transfer and release on the repaired path.

The next focused native run passed manual audio, text/audio/transcription, and
the tool round trip. Its remaining cancellation/recovery failure was a scenario
constraint: the old request had been invalidated and its result rejected, while
a distinct new request belonged to the response started by `EndInput`. The
scenario rejected that valid new obligation. Its correction permits at most
one distinct, validated recovery call, fulfilled by the test caller, and keeps
the original invalidation, stale-result rejection, interruption, correlation,
fresh audio/transcription, completion, and turn-boundary assertions. This is
not classified as an adapter defect. The failed run remains in
`target/session-refactor-evidence/gemini-live-optional-fix.log`.

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

## MOCK-AGENT-003 — Model work precedes failed durable prompt admission

**Affected crate/status:** Custom Agent; confirmed during the authorized
Codex/Custom parity campaign. The repair passes the recorded Custom runtime
and mock targets; final campaign qualification remains in progress.

**Expected and observed behavior.** When the durable Kit execution-admission
write fails, the registered execution must settle without invoking the Model or
constructing a tool handler. The enabled
`custom_progress_acceptance_failure_keeps_the_execution_gate_closed` regression
in [custom_prompt_runtime.rs](../agents-kit/tests/custom_prompt_runtime.rs)
injects that exact write failure and observes one Model request where zero is
required. The original `Integration` / `ServiceUnavailable` admission error
remains observable; an error return alone therefore does not prove absence of
Model work. The fixture separately counts actual cancellation writes and faults
so an unexercised cleanup fault cannot masquerade as cleanup evidence.

**Ownership repair.** The existing run owner registers before its first poll
and owns one execution-release sender; the call owns its receiver. Synchronous
local validation can finish before admission, but publication, generation, and
tool work wait for durable Kit admission. Failed admission closes and joins
the unreleased execution rather than opening the gate during cleanup. Kit
admission does not establish remote Model acceptance. Immediate rejection by
the first Provider generation keeps its normalized terminal error, no successful
history turn, and no tool effects.

**Retained evidence.**
`target/vendor-campaign-agents-codex-20261109/parity-red-02-custom-runtime/`
records the exact invocation, source hashes, and failure before Custom
production edits. The candidate includes the reviewed Codex vendor adaptation;
Custom production is unchanged from `4f264e836985f486684b9f5445c95fae6c032d45`.
The other progress reds exercise newly authorized behavior and do not by
themselves establish pre-existing production defects. The first large-batch
probe had mismatched test call IDs and a cleanup assertion masking its prompt
error; that run remains retained. The corrected
`parity-red-03-custom-mocks-fixed/` run reaches the missing-milestone assertions,
including the 4,097-call batch, with exact scripted call identities.

Held-publication, cancellation, deadline, observer-loss, and shutdown cases must
also remain enabled. In particular, deadline bookkeeping must keep polling the
same admitted execution while waiting for an exclusive storage writer; merely
retaining its unpolled future cannot settle the original publication. Its
original deadline must fence the next effect before cleanup can supply another
cancellation. Current source-specific green evidence and independent review
belong to the campaign receipts; these reds do not establish qualification.

## MOCK-AGENT-004 — concurrent append makes a rejected acknowledgment contradictory

**Affected crate/status:** Shared Agent notification journal; reproduced during
Codex/Custom parity. The focused regression and recorded Custom runtime and mock
targets pass; final campaign qualification remains in progress.

**Expected and observed behavior.** An exact StateStore commit conflict proves
that a passive acknowledgment did not commit. The receiver must retain its
original delivered identity and retry against the current journal. A concurrent
append is valid progress, not evidence of a contradictory acknowledgment.
The enabled
`custom_notification_receipt_conflict_preserves_delivery_across_a_concurrent_append`
regression in [custom_mock_composition.rs](../agents-kit/tests/custom_mock_composition.rs)
holds a real acknowledgment snapshot while the accepted prompt appends its tool
milestone. The process-local direct-trait StateStore rejects the stale revision
with `TransactionConflict`; the fixture positively checks that native result.
Before repair, receiving fails with `InvalidState` and
`notification receipt commit outcome is contradictory`.

**Ownership repair.** The existing receipt owner handles definite commit
conflict before observing a changed whole-record view. It restores the retained
delivery cursor and retries only that acknowledgment. Successful and indeterminate
commits retain exact receipt reconciliation. No publication, prompt, handler,
interaction decision, or unknown commit is blindly repeated. The regression
requires two Model calls, one handler effect, distinct ordered milestone
identities, and an empty terminal drain after acknowledgment.

**Retained evidence.**
`target/vendor-campaign-agents-codex-20261109/parity-red-08-journal-conflict/`
contains the exact source and command receipt plus the genuine assertion failure.
The broader `parity-green-02-custom-runtime/` attempt retains three occurrences
of the same receipt failure; its name does not imply a passing run. Its other
failures are tracked separately. New Custom progress makes the shared journal
interleaving easy to exercise; the exact-conflict receipt classification being
repaired predates this parity work. Source-specific green evidence and independent
review remain required before this area is complete.

## MOCK-AGENT-005 — cancellation races with native approval publication

**Affected crate/status:** Codex Agent; reproduced during the authorized parity
campaign. Repair and qualification are in progress.

**Expected and observed behavior.** A callback checked before an awaited storage
operation must not publish actionable work after durable cancellation wins.
The enabled
`codex_progress_interaction_transaction_rechecks_cancellation_before_publication`
regression holds continuation observation before the Interaction transaction,
proves cancellation and native interrupt dispatch while the turn remains active, and
then releases the callback. Before repair, one new Interaction is published
where zero is permitted. A foreign-turn control retains its conflict error.

**Ownership repair.** The existing publication transaction checks the prompt's
tool admission after validating native turn ownership. Cancelled or terminal
same-turn work rolls back without publishing; a foreign turn remains a conflict.
Rollback failures and the original publication deadline retain their authority.
No callback decision, tool effect, or native turn is resent.

**Retained evidence.**
`target/vendor-campaign-agents-codex-20261109/parity-red-11-codex-atomic-admission-network/`
records the genuine one-versus-zero failure and unchanged source. The preceding
`parity-red-10-codex-atomic-admission/` attempt failed at the sandbox's local
listener restriction and is not behavioral evidence. The permissive transaction
check predates this parity change; the new progress tests expose its interleaving.
Focused green evidence, independent review, and complete qualification remain
required.

## MOCK-AGENT-006 — publication-failure cleanup stops on a cancellation conflict

**Affected crate/status:** Candidate Codex progress implementation; reproduced
before acceptance. This entry does not claim the new recovery path existed in
the previously selected implementation.

**Expected and observed behavior.** Cancellation claim and settlement can race
the independent reader's matching native terminal update. The causal regressions
`codex_progress_claim_conflict_reconciles_proven_native_terminal_without_interrupt`
and `codex_progress_settlement_conflict_reconciles_proven_native_terminal_without_resend`
hold each actual cancellation commit, positively observe the same generation's
terminal update, then release the stale transaction. Both verify a real
`TransactionConflict`. Before repair, cleanup propagates that conflict and
retains the old context despite the now-committed terminal evidence. The original
publication failure remains primary, but later admission and cleanup cannot
complete normally.

The repair belongs in the existing prompt recovery owner: reconcile only exact
local conflicts within the original cleanup deadline, preserve the primary
publication failure, and never resend an interruption or tool effect. Unknown
commit outcomes and other failures retain their uncertainty. The enabled tests
require exact interruption counts, bounded cleanup, context retirement, and
successful subsequent admission after proven settlement.

**Retained evidence.**
`target/vendor-campaign-agents-codex-20261109/parity-red-12-codex-recovery-conflicts/`
records both genuine failures and unchanged source. These causal tests establish
the conflict path; they do not by themselves establish the cause of the earlier
intermittent retention failures. Repair, independent implementation review, and
complete qualification remain required.

## MOCK-AGENT-007 — a late managed handler crosses the prompt deadline

**Affected crate/status:** Codex managed tools during the authorized progress
campaign. Behavioral failures precede the repair; complete qualification remains
required.

**Expected and observed behavior.** Handler acquisition and an already-admitted
Invoke may finish after the original prompt deadline. The two enabled
`codex_progress_deadline_cancels_a_late_*` regressions hold each boundary and the
exact timeout-intent write. They observe public Timeout while durable prompt
state is still Waiting/NotRequested and no native interrupt has been sent.
Before repair, releasing acquisition starts one forbidden Invoke; releasing an
in-flight Invoke publishes one forbidden actionable Interaction.

The existing prompt effect guard must remain with the exact non-Clone handler,
be checked before each new effect, and settle that handler through Cancel.
Completed and typed ToolError::Cancelled are normal cancellation outcomes;
another Interaction or a different error is unfinished or failed cleanup.
The original primary failure must be captured before cleanup waits, and real
cleanup failures must remain observable through the existing tracked worker.
A late native callback response must not be sent. No new producer, task, public
trait, record format, or deadline is authorized by this repair.

**Retained evidence.** `parity-red-13-codex-full-including-tool-deadlines` under
`target/vendor-campaign-agents-codex-20261109/` records both one-versus-zero
failures. Its subsequent cleanup deadlock required termination of only the
owned test process and is recorded separately below; that full target is
incomplete. `parity-red-15-codex-deadline-cancel-controls` records three further
Invoke failures. Two also have secondary cleanup assertions because the old
path never attempted the expected cancellation. Their response-channel EOF waits
additionally exposed a fixture sender retained after socket EOF; that sender
lifetime requires a separate test-only correction. These secondary failures are
not proofs of the repaired cancellation-result branches. All controls remain
enabled.

After the reviewed effect-guard repair and fixture EOF correction,
`parity-green-09-codex-late-tool-deadline` passes both late-handler cases, and
`parity-green-10-codex-deadline-cancel-controls` passes all three cancellation
outcome controls. Each receipt verifies unchanged source. The intervening
`parity-green-08-codex-late-tool-deadline` was a compile-only type-inference
failure, corrected with the exact existing worker result type; it establishes
no behavioral result. Full-target and integrated qualification remain required.

## MOCK-AGENT-008 — managed interaction cancellation reenters its own mutex

**Affected crate/status:** Codex managed interaction cleanup, exposed while
validating unread progress. The mutex repair requires the full enabled cleanup
regression and final qualification; removing the hang alone is insufficient.

**Expected and observed behavior.** A pending managed Interaction can follow
unread passive Progress facts. Public abort must still cancel and join its exact
handler. The fallback removed that Interaction's local owner inside an if-let
whose temporary pending-map guard remained alive. Generation reclamation then
locked the same map, blocking the current-thread runtime and its async deadlines.

`parity-red-13-codex-full-including-tool-deadlines/deadlock-sample.txt` records the
actual cancel_interaction -> reclaim_generation -> pending mutex stack. The
enabled `codex_progress_unacknowledged_facts_do_not_deadlock_tool_cancellation`
regression separately proves the exact unread ordering and abort path. It runs
one exact child test with a mandatory start receipt and bounded kill/wait;
fixture deletion requires proven child reaping. `parity-red-14-codex-unread-progress-cleanup`
reproduces the hang within that bound instead of freezing the whole target.

The repair releases the existing map guard before signalling the responder or
reclaiming the generation. `parity-green-07-codex-unread-progress-cleanup` no
longer hangs, but fails next admission; despite its receipt name, it is not a
passing regression. The diagnostic below establishes that remaining defect.

## MOCK-AGENT-009 — local tool cancellation leaves a durable Interaction pending

**Affected crate/status:** Codex managed interaction cleanup; confirmed after
the mutex repair. Repair and complete qualification remain required.

**Expected and observed behavior.** Cancelling an exact owned handler must also
settle its own durable Interaction without evicting or rewriting unread passive
facts. The ordinary public resolution path remains head-only. When that path
cannot reach an Interaction behind passive facts, the fallback cancelled the
local handler but left the journal Interaction Pending. Native cancellation
settled, yet the next prompt returned Busy at Prompt/Admission.

`parity-red-16-codex-interaction-retirement` retains the committed post-abort
journal: two passive Progress entries and the exact still-pending Interaction.
Its next-prompt assertion now reports the early public Busy directly, rather
than hiding it behind a native-acceptance timeout. The enabled regression keeps
its original admission, native acceptance, handler, callback, and cleanup claims.

The required repair belongs to the existing concrete managed-tool, durable
interaction, and notification owners. It must validate exact notification and
generation ownership, preserve raced resolution authority, and retain admitted
transaction ownership and uncertainty. It must not widen caller resolution,
resend a handler effect or callback decision, adopt another Interaction, or
change public notification/persistence formats. These fixture receipts establish
adapter defects, not native or workspace release qualification.

`parity-red-18-codex-retirement-owned` records nine further behavioral failures
before retirement production changes: no owned retirement is admitted, the first
unknown-commit variant observes zero retirement attempts, foreign Interaction
content is not rejected, and raced resolution uncertainty does not fence new admission or
retain its exact cleanup failure. Dropped-abort, reader-exit, awaited-shutdown,
and same-Agent independent-conversation controls remain enabled. Secondary
cleanup failures are retained separately from their primary assertions.
`parity-red-17-codex-retirement-owned` only exposed seven fixture assertions that
incorrectly required opaque PromptStream to implement Debug; their corrected
error-kind assertions produced the behavioral failures in red18.

`parity-red-19-codex-retirement-owned` retains ten failures after adding the late
shutdown-failure control and waiting for the exact commit receipt before native
completion. The first reviewed worker-owned repair passes eight of eleven
controls in `parity-red-20-codex-retirement-aggregation`. Its two durable validation
failures still lacked StateRead attribution, so later cleanup changed their
observed phase. This is a gap in the campaign repair, not a claim that the
unmodified implementation already performed retirement. The narrow follow-up
attributes only those persisted-state checks and leaves the assertions intact.
The third failure establishes the separate existing aggregation defect below.

## MOCK-AGENT-010 — tool shutdown discards an independent cleanup failure

**Affected crate/status:** Codex's existing managed Toolset shutdown aggregation,
exposed after the reviewed retirement repair. Complete qualification remains
required.

**Expected and observed behavior.** During consuming Agent shutdown, a retained
retirement failure remains primary when the Toolset's shared shutdown
independently fails. Both failures must
remain observable. The existing `cancellation_error.or(shutdown_error)` discards
the latter error whenever cancellation has already failed.

The enabled
`codex_progress_tool_retirement_retains_a_distinct_toolset_shutdown_failure`
regression first proves a real rollback and retained ServiceUnavailable at
StateCommit, then arms a distinct typed Toolset InvalidConfiguration failure,
which Agent normalizes to InvalidData.
It requires exactly one shutdown invocation and the original primary plus that
separate supplementary error. `parity-red-20-codex-retirement-aggregation`
records the lost secondary failure with the original aggregation unchanged.
This isolates the pre-existing aggregation statement after retirement support
became available; it does not claim the unrepaired adapter could already reach
that retirement outcome. The repair combines both errors in the existing
shutdown owner without changing one-error or success outcomes, effects, or API.

After the reviewed retirement, phase-attribution, and error-aggregation repairs,
`parity-green-11-codex-retirement-owned` passes all eleven controls with unchanged
source. This is scoped fixture evidence; the complete Codex target and final
integrated qualification remain required.

## MOCK-TOOL-001 — unbounded native HTTP response buffering

**Affected crate/status:** Tools MCP HTTP client through rmcp 1.5.0;
RUSTSEC-2026-0462 remediation and qualification are in progress.

The independent finite-body fixtures in
[http_response_limits/mod.rs](../tools-kit/tests/http_response_limits/mod.rs)
exercise declared oversized initialization, streamed initialization and tool
responses, and the smaller non-success response limit. Each withholds EOF,
positively observes the bounded payload delivery, counts native operations,
and joins the client and peer. On the old dependency,
`tools-red-01-response-limits/` records one passing control and four failures:
three directly wait for withheld EOF rather than rejecting the oversized body;
the streamed tool-response case also requires an abort fallback during bounded
cleanup, which masks its earlier rejection assertion and is retained separately.
It is not claimed as a clean assertion-only red. The corrected fixture now
reports early completion before cleanup assertions.

Select the published fixed SDK instead of adding an exception or a parallel
transport. Existing success, correlated JSON-RPC rejection, no-resend, shared
shutdown, and session-worker ownership controls remain required. Source and
command receipts live beneath
`target/vendor-campaign-agents-codex-20261109/`; fresh dependency security proof
alone does not establish behavioral completion. On the independently reviewed
repair, `tools-green-05-http-server-lifecycle/` passes all 15 HTTP cases,
including all four oversized-response cases. Complete crate and live gates
remain required.

## MOCK-TOOL-002 — unsupported selected protocol proceeds to inventory

**Affected crate/status:** Tools MCP client; reproduced before the SDK update.
`tools-baseline-02-protocol/` records that a peer selecting 2026-07-28 after the
client offered 2025-11-25 still receives `tools/list`, and construction succeeds.
The independent raw-peer regression
`client_protocol_revision_rejects_new_lifecycle_before_inventory` requires
rejection and joined native cleanup before inventory or tool execution.
The four retained revision negotiations and existing server's revision ceiling
pass in that same baseline and remain compatibility controls.

The selected update keeps private client/server revision boundaries through
2025-11-25. New stateless discovery, execution, and capability families are not
implicitly enabled by adopting the security fix. The selected-revision rejection
and all four retained-version controls pass in
`tools-green-05-http-server-lifecycle/`; complete validation remains required.

## MOCK-TOOL-003 — audio content metadata is discarded

**Affected crate/status:** Tools native content adaptation with rmcp 1.5.0;
selected-source repair and qualification are in progress.
`tools-baseline-03-content/` records that audio annotations survive but the
native audio `_meta` entry is dropped. The exact comparison in
`audio_content_metadata_is_preserved_with_annotations` fails; all five existing
content variants, their previously supported metadata, and output-schema
rejection controls pass. The new SDK represents this metadata explicitly;
retain it through the existing portable Audio metadata field without adding a
public content type. Inbound proof does not by itself establish outbound
roundtrip preservation; the served-audio regression is also required. Both
directions pass in `tools-green-05-http-server-lifecycle/` on the reviewed repair.

## MOCK-TOOL-004 — SDK upgrade admits an unowned stateless lifecycle

**Affected crate/status:** Candidate rmcp 3.9.0 adaptation; regression found
during independent review before the upgrade's acceptance. This is not claimed
as a defect in the previous selected SDK.

The selected SDK treats complete inline client metadata as stateless routing
even when it names retained protocol revision 2025-11-25. Its discovery route
also bypasses initialization. A version ceiling alone therefore fails to
preserve the supplied server's existing session and worker ownership.
`tools-red-04-sdk-lifecycle-and-compatibility/` records four genuine failures:
full and minimal inline metadata return inventory without initialization,
inline metadata overrides an initialized session, and sessionless discovery
returns success. The tests use raw HTTP, retain fresh-session usability and
joined cleanup, and preserve an ordinary request-metadata success control.

The repair belongs in the existing private HTTP admission policy, before native
routing can allocate stateless work. It must preserve bounded request bodies,
ordinary initialized requests, native session ownership, and shutdown. The
enabled regressions live in
[server_startup_regressions/mod.rs](../tools-kit/tests/server_startup_regressions/mod.rs).
All 58 other server cases and all 15 HTTP client cases passed in that red run.
The independently reviewed repair passes all 63 server cases, including an
additional partial-metadata control, and all 15 HTTP cases in
`tools-green-05-http-server-lifecycle/`. Complete crate and live gates remain
required; the original four failures remain retained.

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

**Separate fixture correction.** The first full App check with the notification
repair passed all 81 child-executor cases, then failed three notification
projection cases with revoked runtime ownership. Those three cases passed on
unchanged `8184f48`. Task-specific diagnostics showed their globally armed
synthetic corruption reaching both the intended snapshot observer and a
background execution reload. The latter correctly treats permanent read
corruption as fatal. The projection fixture now confines its fault to the
intended observer, as its existing orphan-target cases already did. A separate
task must still read the unchanged native checkpoint and ledger records through
both direct and transactional storage paths. Existing rejection, no-replay,
eventual resolution, and fatal-background-read assertions remain required.
The failed full invocation and causal trace are retained in
`target/cancellation-timeout/app-check-fixed.log` and
`target/cancellation-timeout/notification-projection-trace.log`; correcting the
fixture does not turn those earlier runs into passes.
The added separate-reader integrity assertion failed against the global fault
in `target/cancellation-timeout/notification-fixture-scope-red.log`, before the
observer restriction was applied.

Current source-specific passes, failures and outstanding qualification belong
to the PR validation evidence. Reproducing this race and passing a focused
regression do not establish full App or workspace qualification.

## Codex 0.167.1 compatibility refresh — pending qualification

This refresh is based on the reviewed 33-file Codex 0.167.0 reference.
The independently reviewed endpoint trees have identical App Server and protocol
sources. The 11 changed upstream paths concern the release version and native
incremental tools, including top-level `web_search` placement and foreground and
background compaction. The new `incremental_tools_legacy` flag is under
development and disabled by default; regular Responses selects it only for the
native Bedrock provider IDs, while Responses Lite retains its existing flag.
These native changes stay private. No feature, provider configuration or public
API is enabled or added by this adaptation.

The parent recorded tests-first compatibility evidence in
`target/vendor-campaign-agents-codex-20261109/codex-01671-identity-red-01/receipt.json`:
the exact initialization-rejection test exited 101 against the reviewed 0.167.0
reference because the added `codex/0.167.0` control was still accepted. The
original log, captured source and diff remain at that location. This establishes
the required selection change, not a retroactive defect in the former selection.
The new rejection tuple, older and malformed controls, and zero-thread-start
assertion remain intact.

Relative to the reviewed reference, the sole production change selects 0.167.1.
Positive fixture identities, source-authority expectations, strict setup and
research guidance follow the new frozen candidate lock. The parent completed
the protected guidance installation after the leaf returned; the exact
postimage and frozen-input verification are recorded in
`target/vendor-campaign-agents-codex-20261109/protected-guidance-01671-01/receipt.json`.
All original
0.167.0 findings, historical receipts and Tools remediation entries remain
unchanged. Independent implementation review, focused green proof, native
prerequisites and every fresh acceptance gate remain pending. This note claims
no new qualification; the retained native suite does not exhaustively qualify
upstream search, compaction or Bedrock behavior.
