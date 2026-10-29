# Strict-live repair follow-up — 2026-10-28

This record follows [the original campaign](strict-live-2026-10-28.md) on
`codex/prepare-initial-release`, based on HEAD
`ed504a4787c3254185cc9db193b32479a955e456` plus the recorded working-tree changes.
Private receipts retain source/diff, lockfile, build controls and log identity.
Executable hashes apply only to the named observations described below; they
are not claimed for every invocation. This is affected-crate repair evidence, not
workspace completion or release qualification. The original report, failures,
raw grades and cleanup uncertainties remain unchanged.

## Changes

- Gemini's required File Search role now selects exact
  `models/gemini-3.8-flash`. All six retrieval/deletion-consistency cases remain,
  with their existing assertions and cleanup. Explicit deterministic checks
  prevent the retained 2.5-only exception from applying to 3.8. Google documents
  [3.8 Flash File Search support](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash).
- The shared function-continuation instruction now requires literal copying of
  the complete opaque token. The Ollama model, sampling settings, fresh nonce,
  exact equality assertion and output handling are unchanged. Added wire tests
  preserve complete history, thinking, tools, JSON results and optional call
  IDs across unary/streaming paths, including an incorrect native token that
  must remain incorrect.
- App's obsolete docs.rs-header prohibition now checks only the two approved
  target settings and values. Runtime dependency, feature and build restrictions
  remain. These are supported [docs.rs build metadata](https://docs.rs/about/metadata).
- Four Agent source checks now use the existing checksum-verified ZIP test owner
  rather than ignored extraction directories. All protocol/schema facts remain,
  including the missing Codex facts and strict JSON type comparisons. The Rust
  case-count reduction is relocated coverage, not an exclusion. Active guidance
  distinguishes the current 29-case inventory from historical 28-case receipts.

No production adapters, public APIs, dependencies, archives or lockfiles changed.
No assertions were loosened and no provider output was repaired.

## Pre-fix evidence and limits

The original campaign supplies the recorded red evidence for the four Gemini
2.5 File Search HTTP-404 failures, App's docs.rs guard (6 passed / 1 failed),
and Agent's extraction-dependent source checks (11 passed / 4 missing-file
failures). No separate fresh pre-fix invocation is claimed for those checks in
this repair record. Their reports and private receipts remain unchanged.

The unchanged-code Ollama planning sample did reproduce the copying failure:
8 passes and 2 failures, with correct outbound tool-result tokens and incorrect
native responses. The separate post-change ten-trial confirmation also failed;
its complete outcomes appear below.

New deterministic replay and 3.8-exception-exclusion controls passed against
unchanged production adapter code. They add coverage; no failing pre-fix
invocation is claimed for those new cases. The transferred Agent archive checks
and repaired App guard likewise prove the corrected validation paths, not a
production-runtime defect. The clean-source snapshot is fresh post-change
evidence that the Agent checks need no ignored extraction directories.

## Recorded outcomes

| Validation | Result |
| --- | --- |
| Provider deterministic focused targets | PASS: environment 22/22; strict-live assertions 264/264; wire behavior 252/252 |
| App public-surface target | PASS: 7/7, including the corrected metadata guard |
| Agent clean-source snapshot | PASS: Python archive checks 10/10 and Rust source contracts 11/11, with committed ZIPs and no extracted vendor directories |
| Documentation contract | PASS: 13/13 |
| No-path and no-type-alias checks | PASS |
| Six exact Gemini File Search cases | Native PASS: 6/6; no 2.5-only qualification used |
| Fixed ten-trial Ollama confirmation | Native FAIL: 8 passed / 2 failed; trials 2 and 3 remain blocking |
| First canonical Provider attempt | FAIL/101 at Clippy `op_ref` in the new wire test; live stage unreached; redundant reference corrected before a separate canonical retry |
| Canonical Provider retry, long batches enabled | FAIL/101: 136 passed / 1 failed / 137 selected, zero ignored or filtered; all seven long batch cases and six Gemini File Search cases passed |
| Canonical Agent verifier | FAIL/101: 28 passed / 1 failed / 29 selected, zero ignored or filtered; Python archive authority 10/10, Rust source contracts 11/11 and OpenCode ownership 334/334 passed |
| Agent supplemental rustdoc, doctests, Clippy and formatting | PASS/0 separately: rustdoc, 270 ordinary + 1 compile-fail doctests, Clippy with warnings denied, and formatting; original canonical FAIL remains |
| Canonical App verifier | Command PASS/0: all gates and 18/18 live cases passed; original supervisor INCOMPLETE after service-cleanup PermissionError; later exact-owned absence verified separately |

The focused native invocations each selected exactly one test, ignored none,
and filtered the other 136. Independent postanalysis verified their log hashes,
exact selections, matching executable identity and stable source identity.
These diagnostics do not replace canonical verifiers.

The interim Provider deterministic receipt retains 538 passing executions, but
its whole-workspace tracked-diff hash changed while other assigned edits were
finishing. It is not frozen-workspace proof. The separate canonical Provider
retry freshly repeated all three targets successfully under the frozen source
binding. Its complete deterministic gate passed all 13 targets (638 tests),
formatting, Clippy, Provider docs/doctests and the workspace docs site with six
doctest invocations. Source, runner and all five environment digests remained
unchanged through the canonical run, October 28, 20:43:09–21:59:00 UTC.
App public-surface, Agent clean-source and focused-live receipts also have equal
start/end source identities. Original receipts remain unchanged.

The canonical live suite took 4,364.34 seconds and failed at the Anthropic case
described below. Realtime receipt validation still ran and passed. The final
post-live artifact-hygiene stage did not run because the verifier returned the
native Cargo failure first; the earlier deterministic hygiene check passed.
No final full-selection success marker was emitted.

The focused helper compared a regex tuple to a list and therefore marked all
16 individual receipts FAIL. It mislabeled 14 native passes; the other two are
real failures. Its original aggregate FAIL, individual flags, code, logs and
raw evidence remain intact. Separate `focused-postanalysis.json` and `.md`
reconcile the native outcomes without rerunning or rewriting receipts.

The Agent canonical invocation ran October 28, 21:59:01–22:27:27 UTC. Its fresh
precheck matched selected Codex 0.163.0 and OpenCode 1.19.3. The strict-live
suite took 647.73 seconds. Source/lock, all three execution helpers and the
Agent environment digest remained unchanged. The separate clean-source proof
independently matched ten named Agent/source/lock/vendor inputs to the compiled
snapshot, which contained no extracted vendor directories. Its Python 10/10
and Rust 11/11 results establish the repaired archive-reading path, not full
workspace qualification. The canonical compile-contract target passed 5/5
through its existing nested Cargo checks.

The Agent verifier stopped at the native strict-live failure before its
rustdoc, doctest, Clippy and formatting stages. They are unrun in that canonical
attempt. A separate supplemental invocation on October 28, 23:42:37–23:43:04 UTC
passed rustdoc, 270 ordinary and one compile-fail doctests, Clippy with warnings
denied, and formatting. Its source, helper and all five environment digests
remained unchanged; its command leader was reaped and process group absent.
These separate results do not turn the canonical FAIL into a pass.

## Remaining Ollama failure

Planning diagnostics on unchanged code produced 8 passes and 2 failures in a
fixed ten-trial sample. The byte-preserving local capture showed correctly
submitted tokens and incorrect native responses preserved by Kit. Those
failures remain evidence, separate from the later confirmation.

After the instruction clarification, another fixed ten-trial sample still
produced 8 passes and 2 failures on
`ministral-3:8b-instruct-2512-q4_K_M`. Trial 2 dropped four leading characters
from the token's suffix (39 expected bytes, 35 observed). Trial 3 invented a
verification/date prefix (39 expected bytes, 60 observed). Both returned normal
Stop responses after correctly correlated function calls/results. The fresh
receipts contain portable input/result observations, not independent raw
HTTP captures.

Every planned confirmation trial is listed below from
`focused-postanalysis.json`; the helper's separate reporting failure is
preserved as described above. Expected token length was 39 bytes in every trial.

| Trial | Native result | Observed bytes | Exact token |
| --- | --- | --- | --- |
| 1 | PASS | 39 | `true` |
| 2 | FAIL/101 | 35 | `false` |
| 3 | FAIL/101 | 60 | `false` |
| 4 | PASS | 39 | `true` |
| 5 | PASS | 39 | `true` |
| 6 | PASS | 39 | `true` |
| 7 | PASS | 39 | `true` |
| 8 | PASS | 39 | `true` |
| 9 | PASS | 39 | `true` |
| 10 | PASS | 39 | `true` |

The instruction change has **not resolved** this blocker. The subsequent
canonical continuation case passed once, but that does not erase either fixed
sample or establish reliability. There was no replacement model, output repair,
copying exception or retry-until-success.

## New Anthropic failure

`live::hosted::anthropic_unreachable_url_unary` failed with
`InvalidResponse(MissingField { field: "url" })` during Generate/Normalize:
expected String, actual Absent. Its configured model was exact
`claude-haiku-5-5`; the retained error does not echo a native response model.
Its preserved diagnostic contains a decoded
`server_tool_use` invocation named `web_fetch` with `input: {}`. This is the
invocation input, not an ordinary URL-less fetch-error result. The adapter
already handles error results without a URL when a valid correlated invocation
supplies the target; read-only tracing found no input rewrite.

The complete native response is absent from this sidecar, so any subsequent
correlated result is unknown. No adapter corruption defect or complete native
schema violation is established. The scenario verified a fresh HTTP-404
fixture and requires failed retrieval of that exact target with native
`url_not_accessible` and `ProviderRejected`. Inferring the absent target from
the prompt or accepting a generic error would weaken that proof. The streaming
control passed in this run; it does not erase the unary failure.

The bounded follow-up is an unchanged unary request/response capture including
all correlated native blocks and terminal metadata, with a streaming control
and every outcome retained. Add negative wire coverage for the empty invocation
while retaining existing valid-invocation/error-result controls; propose a
production fix only if complete evidence demonstrates adapter loss of a valid
native representation. The current failure remains blocking. See Anthropic's
[Web Fetch contract](https://platform.claude.com/docs/en/agents-and-tools/tool-use/web-fetch-tool).

## New Agent failure

`strict_live_suite::lance_conflict::opencode_lance_foreground_conflict_recomputes_after_native_event_commit`
failed at the exact terminal-output assertion in `lance_conflict.rs:259` on the
configured `gemma4:e4b-it-qat` model. The returned text wrapped the fresh expected
marker in `<execute>...</execute>` instead of returning the bare marker. This
case has no Question-copy exception; its exact-output failure remains blocking.

The retained observations show a fused stream, one native event commit, one
real foreground transaction conflict, one prepared attempt and one native user
input. The held transaction was released after failure. The first
post-generation exact-output assertion failed, so the subsequent
conflict/correlation assertions were not reached. The observed counters do not
establish those unexecuted assertions.

Native assistant text was not independently retained in this evidence. Model
versus adapter causality remains unknown, and the result does not establish a
Storage or concurrency defect. The original passing run does not erase this
failure. No output normalization, exception, model substitution or retry was
used to change its outcome.

A subsequent bounded read-only lookup used only the one recorded owned native
user-message ID. That row was unavailable in the queried SQLite view, so the
lookup stopped without inferring a session, reading any assistant row, or
searching other databases. The database, WAL and SHM hashes remained unchanged.
`agent-lance-native-provenance.json` records this inconclusive result. It proves
neither deletion nor the origin of the decoration; native causality remains
unknown.

## Gemini evidence and cleanup

All six cases selected exact `models/gemini-3.8-flash`. Eight generation
observations (four positive responses and two owned-store preflight/post-delete
pairs) stopped normally with FileSearch-only output. All four borrowed-store
responses contained the required exact Answer marker once. No unpaired code
declaration or 2.5-specific qualification was accepted.

Grounding receipts contain an identity-free, present empty FileSearch results
array under the existing Gemini retrievedContext contract. They do not prove
native file/citation identities, and generation HTTP was not independently
captured.

Both owned-store modes passed retrieval preflight, acknowledged deletion and
recorded a complete native HTTP 404 / `NOT_FOUND` response for the exact owned
store management lookup. Before/after cleanup receipts show source `deleted`,
store `lookup_absent` and no pending import operation. Both post-deletion
observations report **retrieval_persists**, with the marker returned and
retrieval revocation explicitly unproven. Management absence does not prove
retrieval erasure or guaranteed generation rejection. Borrowed stores were
preserved. These receipts clear none of the original campaign's unrelated
cleanup uncertainties or retained-output obligation.

## Canonical raw grades and resource ownership

Both Realtime Kit cases passed on exact `gpt-realtime-2.1`; both sessions closed
and both Providers shut down. Every raw copying grade is retained:

| Case | Raw grade | Value |
| --- | --- | --- |
| `live::resources::openai_interactive_session` | `function_turn` | `true` |
| `live::resources::openai_interactive_session` | `audio_turn` | `true` |
| `live::resources::openai_interactive_cancellation_and_subsequent_progress` | `post_cancellation_function_turn` | `true` |

These remain separate, nonblocking copying observations. Public-session
evidence does not independently prove native parity, semantic result use or
statistical reliability.

Nineteen final batch ownership receipts cover 18 unique known jobs. They show
zero uncertain publications, pending input/status/artifact discoveries or
uncertain deletions. Eighteen earlier accepted-submission snapshots remain
separate from the final cleanup receipts. One residual is the same one newly
retained Gemini provider-file batch output: Kit deletion received the exact
HTTP400/`INVALID_ARGUMENT` / `DeleteFileRequest.name` 40-character rejection.
The existing exception records awaiting provider expiration, the documented
42-day retention period and expiration unverified. This is a new retained
output; earlier campaign retention and uncertainty obligations remain.

The canonical owned-store cases again observed retrieval persistence after
verified management absence, with sources deleted and stores lookup-absent.
All three ordinary upload-lifecycle receipts record their owned files deleted
with no cleanup failure. The canonical runner leader was reaped and its process
group absent without cleanup signals. These are owner/process observations,
not an account-wide absence claim.

A separate read-only OpenAI inline-batch status diagnostic initially failed
because Python's default TLS trust store rejected the certificate chain. A
platform-curl retry kept certificate verification enabled and observed the job
in progress. This diagnostic issue did not fail a test or establish batch
completion; the canonical case later passed independently.

### Agent Question grades and cleanup

Both named Question cases produced valid version-2 Kit receipts after their
scenario cleanup on OpenCode 1.19.3 and `qwen3.5:9b-q4_K_M`. All six raw grades
remain visible:

| Case | Answer copy | Yes-description copy | No-description copy |
| --- | --- | --- | --- |
| `strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains` | `true` | `true` | `true` |
| `strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build` | `false` | `true` | `true` |

The native-idle waiter's failed answer-copy grade is nonblocking only under
its existing named exception. Both description grades passed in each case;
the six grades comprise five true values and one false value. This proves
neither model-quality acceptance nor general reliability and does not waive
the unrelated Lance-overlap failure.

The Agent canonical process and both owned Codex/OpenCode service process
groups were reaped. Recorded service ports were free after cleanup; independent
process inspection found no remaining members of those three groups. Receipt
and service-log hashes matched. These observations establish the named process
cleanup, not deletion of every persisted native resource. Three documented
runtime-loss/reopen child cases emitted repeated test headers; independent
analysis accounted for them without inflating the 29-case outer inventory.

## Canonical App results and cleanup

The canonical App command ran October 28, 22:27:28–23:40:39 UTC and exited 0.
Its fresh precheck matched OpenCode 1.19.3 and reported Ollama server 0.40.12,
with a separate client-version warning for 0.34.2. All 35 ordinary test targets
passed, totaling 673 cases with zero failures, ignored or filtered cases. This
includes the repaired seven-case public-surface target.

Fresh prepare inventory and execution passed exactly four cases in 5.70 seconds,
including `staged_action_and_atomic_barrier_survive_forced_process_loss` and its
four forced boundaries. The five nested one-test child banners are not extra
parent cases. Routine workload execution passed 165/165 `workflow_execution`
cases and 38/38 `worker_contract` cases, with zero ignored cases and the exact
ten-plus-one authored exclusions below. The verified test-profile artifacts
were unoptimized, with debug assertions and overflow checks enabled; the Lance
version-hint override remained unset. Routine source identities matched before
and after execution, all commands exited 0, and the receipt recorded no
interruptions or process-cleanup errors.

The routine exclusions remain:

- `workflow_execution::checkpoint_poll_capture_excludes_suspended_time_and_retains_open_spans`
- `workflow_execution::delivery_capacity_growth_measurement`
- `workflow_execution::delivery_capacity_prefix_measurement`
- `workflow_execution::late_foreign_prefix_is_observed_before_any_generation_claim`
- `workflow_execution::late_invalid_inventory_is_observed_before_any_generation_claim`
- `workflow_execution::reopen_accepts_valid_short_pages_beyond_the_old_scan_window`
- `workflow_execution::retained_revision_scan_accepts_short_pages_at_4095_4096_4097`
- `workflow_execution::storage_observation_cancelled_wait_does_not_remain_active`
- `workflow_execution::storage_observation_distinguishes_read_only_and_mutating_commits`
- `workflow_execution::storage_transaction_observation_separates_native_calls_and_owner_drop`
- `worker_contract::admission_cleanup_does_not_charge_lazy_execution_lifetimes`

These are five maximum-capacity cases, two delivery diagnostics and four
measurement calibrators, not new exclusions introduced for this repair.
Maximum-capacity qualification is not claimed. The ordinary 64-delivery
receipt/replay/cleanup regression remains selected.

App doctests passed 121 ordinary and seven compile-fail cases. All seven
examples, formatting, locked all-target checking, Clippy with warnings denied,
rustdoc with warnings and missing-docs denied, and no-alias/no-path checks
passed. macOS linking emitted large `__eh_frame` (>16MB) compact-unwind warnings
with a possible exception-handling performance impact. Raw warnings remain
unsuppressed; they are distinct from expected compile-fail doctest diagnostics
and terminal test failures.

All 18 App strict-live workflow cases passed with zero ignored or filtered
cases in 522.10 seconds. Four fault scenarios retained durable paths after their
recorded Integration cleanup results:

- `held_authority_after_real_recovery_error_expires_without_prompt_replay`
- `held_authority_read_expires_real_notification_recovery_without_replay`
- `permanent_authority_read_closes_real_notification_recovery_without_replay`
- `real_service_interruption_preserves_notification_truth`

Four new App temporary domains remained alongside four historical domains in
the pre-workflow baseline. The logs do not identify exact retained directories,
so individual case-to-directory mapping is not independently established.
These are retained evidence, not an all-artifacts-deleted claim.

The private supervisor nevertheless ended **INCOMPLETE**: stopping its owned
App OpenCode service raised `PermissionError`. Its canonical command receipt
records exit 0, a reaped command leader and an absent command process group;
the service cleanup attestation and service-log hash were not completed.
The original incomplete receipt is preserved.

At 23:42:34 UTC, separate exact-owned inspection found that service PID and
process group already absent and its port free; no supplemental signal was
needed, and the borrowed Ollama listener remained present. The later service-log
hash was recorded and matched. This proves later absence, not the cause of the
original cleanup error or whether the original supervisor reaped the service.
It does not retroactively turn the supervisor receipt into PASS.

App's recorded source/lock binding, all three execution helpers and its private
environment digest matched through the invocation and initial postanalysis.
Its tracked-diff binding is the same `69efcf59...` candidate recorded below.
Post-run binary hashes are additional file observations, not launch-time
attestations. `app-postanalysis.json` and `.md` preserve the complete case,
inventory, warning, provenance and cleanup analysis.

## Invocation and evidence

Canonical entrypoints are run sequentially from the workspace root:

```bash
(source providers-kit/strict-tests.env && export PROVIDERS_KIT_RUN_LONG_BATCHES=1 && bash providers-kit/scripts/verify_strict_live.sh)
(source agents-kit/strict-tests.env && bash agents-kit/scripts/verify_strict_live.sh)
bash app-kit/scripts/verify_strict_live.sh
```

App loads its own environment. One orchestrated Cargo command runs at a time;
Agent's compile-contract harness owns its nested checks. Final receipts must
identify their actual source and invocations; passing historical or focused
results do not replace a failed or incomplete gate.

The focused-live receipt binds the tracked diff to SHA-256
`035d5437486baee3c423f3bae1ae4fb0d42ce6934fdfee31dde0a603c8cfcd68`.
The canonical Provider retry and Agent receipts bind their frozen diff to
`69efcf59f86b570d1e332d914bc4896b2a47179135e3f0259179e2196b6ed3d1`.
Both use Cargo.lock SHA-256
`5f28a1a14d4ab6a4e5400d843dac2edb4aba4be4bdf6f5a61c840590a418f7d2`.
These identify working-tree contents relative to the HEAD above, not new commits.
The corresponding full identity fields are retained in
`provider-focused-live/status.json`,
`provider-canonical-after-lint/status.json`, and the Agent canonical receipt.

After the Cargo gates finished, this historical report and its ledger
classification were added. The 14 other changed implementation/guidance files
still match their hashes in `pre-report-binding.json`;
`final-source-binding.json` records that comparison, the unchanged original
report, and final report/ledger hashes. These documentation additions do not
create a new runtime-validation claim.

The focused executable SHA-256 is
`3e49d9f7e677b26697f088ce5d84f68a0aeb92ece5cb1307c3427bb8307934d6`;
the canonical post-run executable observation matches it. That later observation
is not an additional launch-time binary attestation. The recorded App precheck
reports Ollama server 0.40.12 and a separate client-version warning for 0.34.2;
the App command and supervisor outcomes are distinguished above. Agent precheck
proved the selected Codex 0.163.0 and OpenCode 1.19.3 identities. Model identities are recorded with
their cases above; these checks do not establish statistical model reliability.

Ignored private evidence is under
`target/strict-live-runs/2026-10-28-repairs/`, including focused gate receipts,
the extraction-free source snapshot, `focused-live-cases/`,
`focused-postanalysis.json`, `focused-postanalysis.md`, the first canonical
Provider failure, its separate retry, `provider-postanalysis.json` and `.md`,
`anthropic-url-triage.md`, and
`openai-inline-batch-status-observation.json`. Canonical Agent and App receipts
are under `owned-verifiers/agent-20261028T215901Z-0360697c/` and
`owned-verifiers/app-20261028T222728Z-325b1819/`. Additional evidence is in
`agent-trailing-static/`, `app-postanalysis.json` and `.md`,
`app-supplemental-cleanup.json`, and the routine workload receipt at
`target/app-workload-checks/20261028-180945-routine-91972/receipt.json`.

Separate `final-artifact-hygiene.json` reports no workspace-root build artifacts
or affected-crate `wip` entries, the unchanged original report hash, and all five
private environment files still ignored with mode 0600. This supplemental check
does not fill in Provider's natively skipped final hygiene stage or erase its
failure. `final-process-inspection.json` observes all eight recorded command and
owned-service process groups absent, while preserving App's original INCOMPLETE
receipt. It is not a global process, cloud-resource or durable-data absence claim.
Independent review of the final repair diff and report found no outstanding
findings. All 13 documentation-contract tests and the whitespace check passed.
Raw credentials, remote identifiers, nonce values and network addresses are
excluded from this report. Evidence remains local and ignored.
