# Strict-live execution record — 2026-10-28

This record covers source `ed504a4787c3254185cc9db193b32479a955e456`
on `codex/prepare-initial-release`. Execution ran October 28, 15:23:27–19:42:54 UTC
(10:23:27–14:42:54 America/Chicago). This is failure
collection, not workspace completion or release qualification. No production code,
assertions, features, archives, or lockfiles were changed to obtain these results.


## Recorded outcomes

| Crate | Original canonical invocation | Separate follow-up evidence |
| --- | --- | --- |
| Tools | PASS; 1/1 real MCP live case; shared docs site and six doctest invocations passed | No additional retry |
| Storage | FAIL/1: Docker daemon unavailable during startup and cleanup; live unreached | Diagnostic execution passed 294/294 across six lanes and 77 distinct cases; cleanup verified |
| Agents | FAIL/101: source-contract target 11 passed / 4 missing vendor-file failures; live unreached | Full canonical retry PASS: 29/29 live, all later gates and owned-service cleanup passed |
| App | FAIL/101: public-surface target 6 passed / 1 docs.rs metadata failure; prepare/live unreached | Diagnostic PASS: fresh precheck, all 4 prepare/crash cases, and all 18 workflow live cases; owned service settled |
| Providers | FAIL/101: 126 passed / 11 failed / 137 selected, zero ignored or filtered | Six-case Gemini batch diagnostic passed with 131 filtered; receipt accounting reconciled separately |

Storage's lane totals were Lance-only 35, redb 37, postgres 43, surrealdb 65,
sqlite 37, and combined alternatives 77, with zero failed, ignored, or filtered.
Six prerequisite lanes and sixteen feature checks completed before
Docker failed. Later binary execution used fresh services/paths,
exact inventories and owned cleanup. Independent observations found no owned
Compose resources or embedded lane directories remaining. The canonical failure
remains recorded.

Agent's retry passed all fifteen source-contract cases after checksum-verified
committed archives were safely extracted. It completed 29 live cases, rustdoc,
270 ordinary doctests plus one compile-fail doctest, Clippy, and formatting.
Both native-service groups were reaped and their ports freed. The extracted
archive trees served only as uncompiled protocol-source fixtures; Cargo dependencies
were unchanged. The guide
still incorrectly describes extraction-free source checks and a 28-case total;
follow-up must reconcile those clean-checkout and inventory discrepancies.

App's diagnostic ran all eighteen exact workflow cases, with zero failed, ignored
or filtered, after fresh four-case prepare proof. Its failed canonical metadata
assertion and the other canonical stages it blocked remain unsatisfied; a passing
workflow diagnostic does not replace them.

Provider's full selection included all seven long completion cases; all passed.
Its eleven failures comprise six original Gemini allowlist errors, one substantive
Ollama function-result token mismatch, and four pinned Gemini 2.5 File Search HTTP
404 failures. Successful current-model File Search cases do not substitute for
the pinned cases. Do not attribute the Ollama mismatch to model or adapter without
further evidence. Exact failures and owners appear below.

## Cleanup and raw grades

Nineteen canonical batch receipts retain four uncertain
Gemini publications and one pending input discovery. No sufficient remote IDs
were recorded for exact reconciliation. A 403 suggests rejection but proves no
absence; later success clears none of these obligations. Eleven known canonical
jobs have no pending cleanup flags; canonical residual/retained-file counts are
zero. Both failed owned-store fixtures record source/store deletion. These are
owner receipts, not account-wide absence queries.

The six-case Gemini retry passed after access recovery. Its frozen helper's exit-2
receipt-parser failure remains intact; separately reviewed wrapper-aware
postanalysis reconciles accounting without rerunning tests. One provider-file
output remains undeleted under the existing retention exception; expiration is
unverified. Retain that obligation independently of the five original uncertainties.

All campaign-owned reusable services settled; ports 4500, 4096 and 4097 were
free at the final observation, with no Cargo process remaining. App intentionally
retained four durable test domains after expected integration errors: three fatal
notification-authority fault cases and a native-service interruption case. Native
service cleanup passed; this is not a claim that every local artifact was deleted.
Borrowed File Search stores and the pre-existing Ollama daemon were preserved.

Both Agent Question Kit scenarios passed; their six raw copying grades are five
true and one false (delivery/replay answer copying). Both Provider Realtime Kit
scenarios and all three raw copying grades passed. Every grade appears below.
Only the explicitly approved copying observations are nonblocking; these results
do not establish general model quality or statistical reliability. Realtime's
public-session evidence does not independently prove native parity or semantic
result use.

## Invocation and execution notes

Canonical entrypoints from the workspace root:

```bash
(source tools-kit/strict-tests.env && bash tools-kit/scripts/verify_strict_live.sh)
(source storage-kit/strict-tests.env && bash storage-kit/scripts/verify_strict_live.sh --all-backends)
(source agents-kit/strict-tests.env && bash agents-kit/scripts/verify_strict_live.sh)
bash app-kit/scripts/verify_strict_live.sh
(source providers-kit/strict-tests.env && export PROVIDERS_KIT_RUN_LONG_BATCHES=1 && bash providers-kit/scripts/verify_strict_live.sh)
```

App loads its own env. One orchestrated Cargo command ran at a time; Agent's
canonical compile-contract harness owns five nested Cargo checks. Storage's first
compilation was intentionally interrupted for cache/disk consolidation, before
live execution; later observation proved its owned process group absent.
The second segment used absolute `CARGO_TARGET_DIR=$PWD/target/docs-site-build`,
`CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`, and
`AI_KIT_WORKSPACE_OWNS_DOCS_SITE=$PWD` after the earlier fresh shared-docs pass. Storage/Gemini diagnostics
executed reviewed hash-bound binaries; App's diagnostic explicitly preserves its
failed canonical gate. This was not one uninterrupted workspace-wrapper pass.

Orchestration issues remain separate: v1 Agent launcher-PID readiness
failed before any test, then owned cleanup completed; reviewed v2 verified the
actual listener. A Python metadata probe first rejected its own host-path assumption, then
returned `URLError`; a bounded curl probe confirmed restored HTTP 200 access.
Nonfatal Mach-O `__eh_frame` warnings, App's Ollama version warning, expected
compile-fail diagnostics and deterministic negative controls are not additional
product failures. Codex's untrusted-project notice and API-key plugin-catalog
warnings did not fail the native checks. No tracked source changed during
execution; only this report and its historical ledger entry were added afterward.

## Private evidence

Ignored evidence bases are `target/strict-live-runs/2026-10-28-152327Z/` and
`target/strict-live-runs/2026-10-28-163713Z/`. The latter contains
`storage-final-proof.json`, `agent-final-proof.json`, `provider-final-analysis.json`,
`app-final-analysis.json`, `campaign-final-source-and-cleanup.json`,
original verifier logs, preparation/review receipts, `provider-evidence/`,
`provider-gemini-diagnostic-retry/`, `provider-gemini-retry-evidence/`, and
`followup-supervisor-v2/`, and `app-live-diagnostic-20261028T193257Z-478aad03/`.
Raw logs, credentials, network addresses and remote resource identifiers are
excluded from this report. Evidence is local and ignored; it is not shipped with
a clean clone.

## Exact failed cases and owners

Original failures remain recorded after retries.

### Agent source-contract target

All four names start with `source_contract::`. The read failure is owned by
`agents-kit/tests/support/mod.rs:13`.

| Exact suffix | Owning assertion/input selection |
| --- | --- |
| `codex_vendor_release::selected_codex_wire_facts_are_sourced_without_becoming_public_api` | `agents-kit/tests/source_contract/codex_vendor_release.rs:50` |
| `opencode_vendor_release::selected_generated_opencode_schema_keeps_foreground_and_interaction_envelopes` | `agents-kit/tests/source_contract/opencode_vendor_release.rs:96` |
| `opencode_vendor_release::selected_opencode_configured_variants_and_discontinued_sharing_are_sourced` | `agents-kit/tests/source_contract/opencode_vendor_release.rs:214` |
| `opencode_vendor_release::selected_opencode_wire_facts_are_sourced_without_becoming_public_api` | `agents-kit/tests/source_contract/opencode_vendor_release.rs:52` |

The preparation/documentation owner is `agents-kit/docs/STRICT_TESTING.md:40`;
archive selections and checksums belong to `agents-kit/vendor/vendor-lock.toml`.

### App public-surface target

`production_and_manifest_keep_the_reduced_boundary` failed at
`app-kit/tests/public_surface.rs:1142`: `forbidden manifest entry:
[package.metadata.docs.rs]`. The corresponding configuration is in
`app-kit/Cargo.toml`.

### Provider strict-live target

All six names start with `live::batches::`; all owners below are in
`providers-kit/tests/live/batches.rs`.

| Exact suffix | Native operation / owner line | Original unresolved obligation |
| --- | --- | --- |
| `gemini_batch_cancellation_has_effect` | SubmitBatch / 1667 | Uncertain publication |
| `gemini_batch_missing_resources_fail` | SubmitBatch / 1679 | Uncertain publication |
| `gemini_batch_preserves_mixed_row_outcomes` | UploadFile / 1655 | Fixture preparation failed; no submission recorded |
| `gemini_batch_result_limit_is_enforced` | SubmitBatch / 1691 | Uncertain publication |
| `gemini_inline_batch_completes` | SubmitBatch / 1635 | Uncertain publication |
| `gemini_provider_file_batch_completes` | UploadFile / 1643 | Uncertain input discovery |

Fixture cleanup is owned by `providers-kit/tests/live/batches.rs:1343`;
unknown-input discovery at line 1263 is OpenAI-only. Gemini's submission adapter is
`providers-kit/src/providers/gemini/batches.rs`. No existing receipt-based Gemini
reconciliation owner proves the original missing identities. Storage's Docker
startup/cleanup failure belongs to `storage-kit/scripts/verify_strict_live.sh`;
its original errors are retained in `storage-verifier.log:5313`.

### Additional Provider failures observed during the canonical run

| Exact case | Owner | Observed blocker / follow-up |
| --- | --- | --- |
| `live::generation::ollama_function_continuation` | `providers-kit/tests/live/generation.rs:390`; shared scenario at line 114 | The returned verification token differs substantively from the submitted tool result. This is blocking, not a whitespace discrepancy or an approved raw-copy exception. Trace selected-model continuation replay and model output before attributing the cause; no adapter/model repair is established. |
| `live::hosted::gemini_deleted_store_consistency_streaming` | `providers-kit/tests/live/hosted.rs:669` | Retrieval preflight returned HTTP 404 `NOT_FOUND` for pinned `models/gemini-2.5-flash`, reported unavailable to new users. |
| `live::hosted::gemini_deleted_store_consistency_unary` | `providers-kit/tests/live/hosted.rs:658` | Same pinned-model availability failure; deleted-store behavior was not reached. |
| `live::hosted::gemini_file_search_streaming` | `providers-kit/tests/live/hosted.rs:445` | Same pinned-model availability failure. |
| `live::hosted::gemini_file_search_unary` | `providers-kit/tests/live/hosted.rs:434` | Same pinned-model availability failure. |

The legacy File Search selector is explicitly constrained at
`providers-kit/tests/live/hosted.rs:51`; setting an arbitrary replacement env model
would fail that contract. Current-model File Search cases targeting
`models/gemini-3.8-flash` recorded success, but do not substitute for the four
failed legacy-model cases. Follow-up must reconcile availability and the intended
qualification contract without silently skipping cases or changing test policy.

Both failed owned-store consistency cases have bounded, untruncated after-cleanup
receipts reporting `source_state=deleted` and `store_state=deleted`; retrieval
preflight remained false. These recorded owner states do not establish the missing
retrieval/deletion-consistency behavior or an independent cloud absence query.
The nineteen canonical batch ownership snapshots record eleven known jobs with
artifacts captured, zero pending status/artifact discovery/deletion flags, zero
residual files and zero retained outputs. No additional batch uncertainty was
found beyond the original four publications and one input-discovery obligation.
Per-submission snapshots are separate receipts and were not double-counted.

Exact private evidence: `provider-evidence/live__generation__ollama_function_continuation.json`,
the four matching `provider-evidence/live__hosted__gemini_*.json` failure receipts,
two `store-after-cleanup.*.observation.json` receipts, and nineteen case-level
`provider-evidence/live__batches__*.ownership.json` files. The final Provider summary and reviewed retry reconciliation are retained in
`provider-final-analysis.json`; canonical failure remains unchanged.

## Raw grade appendix

Agent schema-v2 receipts select OpenCode 1.19.3, provider `ollama`, and configured
model `qwen3.5:9b-q4_K_M`; both Kit contracts and scenario cleanup passed.

| Exact Agent case | Answer exact copying | Yes-description exact copying | No-description exact copying |
| --- | --- | --- | --- |
| `strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains` | **false** | true | true |
| `strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build` | true | true | true |

Provider's two schema-v1 Realtime receipts select `gpt-realtime-2.1` and record
successful Kit contracts. Preserve their three grades individually:

| Exact Provider case | Raw grade | Value |
| --- | --- | --- |
| `live::resources::openai_interactive_cancellation_and_subsequent_progress` | `post_cancellation_function_turn` | true |
| `live::resources::openai_interactive_session` | `audio_turn` | true |
| `live::resources::openai_interactive_session` | `function_turn` | true |

These permitted raw copying grades do not waive the blocking Ollama continuation
failure, prove independent native parity, or establish semantic result use beyond
the public-session evidence. Private raw grade receipts and parser summaries are
bound by `agent-final-proof.json` and `provider-final-analysis.json`.
