# Provider strict-live follow-up — 2026-10-29

This report preserves the [original campaign](strict-live-2026-10-28.md),
[first repairs](strict-live-followup-2026-10-28.md), and
[native investigation](strict-live-native-repairs-2026-10-28.md) unchanged.
Work continues on `codex/prepare-initial-release`, based on
`ed504a4787c3254185cc9db193b32479a955e456` plus the recorded working-tree changes.
No production adapter, public API, dependency, model ID, sampling setting,
fallback, output correction, or acceptance assertion changed in this pass.

## Findings and retained change

- **Gemini:** thirty fixed unchanged File Search checks passed with complete
  native captures. The earlier HTTP 400 remains unexplained; this is fresh
  passing evidence, not proof of a repaired Google or Kit defect.
- **Ollama:** an explicit JSON-field-extraction instruction failed two of five
  trials. The unchanged current instruction passed five of five in the same
  comparison. The candidate was rejected and removed; there was no confirmation
  run, candidate switch, or sample extension. The earlier 8/10 confirmation
  remains a blocker. Kit faithfully preserved both new incorrect native outputs.
  Fresh canonical continuation and ordinary unary generation also failed.
  Five subsequent unchanged unary diagnostics passed without reproducing that
  new unary failure; they establish no repair or qualification pass.
- **Diagnostic repair:** the canonical Provider verifier now supplies a fresh
  private evidence directory automatically. This fixes the demonstrated gap
  that lost the original Gemini error body. Explicit directories remain
  caller-owned and must be valid before live execution starts. Native failures,
  cleanup failures, successful response observations and batch ownership
  receipts retain their existing bounded, redacted export behavior.

The verifier creates its default directory with mode `0700`; evidence files use
`0600` and create-new semantics. Successful and failed runs retain their files.
Direct Cargo invocations still require an explicit evidence directory to enable
ordinary failure/generation exports; some mandatory owners have separate
fallback exports. Owning
instructions are in [the strict guide](../providers-kit/docs/STRICT_TESTING.md).

## Research

The research used the repository's Rust core/testing, async/networking,
Provider research/change-contract, strict-live audit and validation skills.
Pinned Rust remains `1.99.0`.

Google documents the exact selected
[Gemini 3.8 Flash model](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)
and [File Search request fields](https://ai.google.dev/api/generate-content#FileSearch).
The original bounded error was classified from a valid canonical status as
`INVALID_ARGUMENT` or `OUT_OF_RANGE`; the retained record cannot distinguish
the missing native details. Unchanged request construction and all thirty
captured diagnostic requests contain no service-tier, thinking, sampling, or
other-tool override. Accepted
[ProtoJSON field spelling](https://protobuf.dev/programming-guides/json/#format-description)
also supplies no basis for a speculative request rewrite. No HTTP 400 retry was
added. Google's [troubleshooting guidance](https://ai.google.dev/gemini-api/docs/troubleshooting)
does not establish the cause of this particular uncaptured response.

Ollama's [tool-call contract](https://docs.ollama.com/capabilities/tool-calling),
the selected model's [published template](https://ollama.com/library/ministral-3:8b-instruct-2512-q4_K_M/blobs/5b74c26b9e0b),
and the [model author's template](https://huggingface.co/mistralai/Ministral-3-8B-Instruct-2512/raw/main/chat_template.jinja)
agree with captured native framing. Explicit system instructions replace the
default persona/date text; failures with an explicit instruction therefore do
not support blaming those defaults. The correct complete assistant tool-call
message and JSON result reached the second request.

Pinned [Ollama 0.40.12 defaults](https://raw.githubusercontent.com/ollama/ollama/v0.40.12/api/types.go)
and its [option merge order](https://raw.githubusercontent.com/ollama/ollama/v0.40.12/server/routes.go),
combined with captured model metadata and requests, imply repetition penalty
`1.0` (disabled), presence/frequency penalties `0`, `top_k=40`, `top_p=0.9`,
and model temperature `0.15`. `repeat_last_n=64` has no effect with penalties
disabled. Requests supplied only `num_predict=4096`. These effective settings
are source-derived, not independently observed sampler internals. No sampler,
rendering, replay, or adapter-loss defect was demonstrated.

## Fixed Gemini diagnostics

The baseline plan declared ten unary and ten streaming executions, alternating
modes. A separate plan then declared five same-process streaming-then-unary
pairs, matching canonical ordering. Neither set adjusted requests after a
response. Every original grounding, exact-token, finish, streaming and cleanup
assertion remained enabled.

| Fixed set | Executed | Result | Native HTTP | Cleanup |
| --- | ---: | --- | --- | --- |
| Separate-process unary | 10 | 10 PASS | 10 × 200 | Settled |
| Separate-process streaming | 10 | 10 PASS | 10 × 200 | Settled |
| Same-process streaming → unary | 5 pairs / 10 cases | 10 PASS | 10 × 200 | Settled |

Every request was the same 594-byte JSON body, SHA-256
`0a6b7a4b1ae3cb832abb31163810fc948988b9ef165337b21514eec1723e2652`,
using `models/gemini-3.8-flash`, the configured borrowed store and
`maxOutputTokens=4096`. Each case made one request. All thirty native visible
responses exactly matched Kit; the stored marker and grounding/support indices
were valid. Native metadata omitted file IDs, so portable File Search results
were present but empty and public citation lists were empty. These observations
do not prove normalized citation identities.

Relay TLS verification, byte-preserving forwarding, bounded capture, exact
request bytes and helper/source/environment identities were checked. No
credential headers were persisted. All command, relay and curl owners settled
without cleanup signals or errors. The borrowed store was not deleted.

## Fixed Ollama comparison

The exact model stayed `ministral-3:8b-instruct-2512-q4_K_M` on Ollama `0.40.12`.
Five alternating baseline/candidate pairs were frozen before execution. The
candidate changed only the upfront system instruction to require extracting
the tool result's `verification_token` JSON string as arbitrary data. User
message, tool declaration, sampling, full assistant replay, JSON result,
fresh `strict-` plus 32-hex nonce, stop/equality assertions and cleanup were
unchanged. Nonces were generated only after validating the initial tool call.

| Pair | Order | Current instruction | JSON extraction candidate |
| --- | --- | --- | --- |
| 1 | Current → candidate | PASS | FAIL: fabricated token, 39/39 bytes |
| 2 | Candidate → current | PASS | PASS |
| 3 | Current → candidate | PASS | PASS |
| 4 | Candidate → current | PASS | FAIL: dropped first suffix character, 39/38 bytes |
| 5 | Current → candidate | PASS | PASS |

All ten native final responses reported `stop`; raw exact-copy grades were
true for all five current-instruction trials and candidate trials 2, 3 and 5,
false for candidate trials 1 and 4. All twenty HTTP captures were complete.
Both requests' model/options/history, full assistant replay, tool result and
correlation were verified. Kit/native output and finish parity held, including
both failures. Runtime/model/template identity stayed unchanged. The ten tokens
were unique and disjoint from the thirty tokens in the preceding fixed
twenty-trial pilot and ten-trial confirmation. Cleanup settled.

Selection required five complete candidate passes. The candidate's 3/5 result
therefore authorized no ten-trial confirmation. Its temporary Rust test and
redundant prompt-only wire arms were removed, restoring the unchanged 137-case
canonical inventory. The retained current scenario's bounded 5/5 result does
not erase its earlier failed confirmation or establish statistical reliability.

The private audit initially mishandled standalone assertion exports as malformed
observations. Copied historical failures reproduced that diagnostic defect;
the repaired audit passed nine offline tests, preserving the original failures
and rejecting altered replay, output corrections, fabricated stop, missing
observations and unknown schemas. Historical receipts were hash-checked and
unchanged. This helper repair is distinct from production behavior.

## Validation

The tracked regression commands were:

```bash
cargo test --locked -p providers-kit --test strict_env_contract --all-features strict_live_verifier_ -- --nocapture
cargo test --locked -p providers-kit --test strict_env_contract --all-features -- --nocapture
```

Diagnostics used a verified `cargo test --locked -p providers-kit --test
strict_live --all-features --no-run --message-format=json` build, then invoked
that exact binary with `--exact --nocapture --test-threads=1` for each individual
case. Gemini pairs used the unique `live::hosted::gemini_current_file_search_`
prefix with the same single-thread setting. The private plans retain each full
command and fixed ordering. The canonical invocation is
`bash providers-kit/scripts/verify_strict_live.sh`, with
`PROVIDERS_KIT_RUN_LONG_BATCHES=0` explicitly selected before launch and
`PROVIDERS_KIT_EVIDENCE_DIR` unset to exercise the new default. Build controls
use the existing `target/docs-site-build`, one Cargo build job and disabled
incremental compilation; no concurrent Cargo command runs.

Two meaningful process regressions failed before the verifier repair: missing
default evidence and acceptance of an empty configured directory. The focused
red run was **12 PASS / 2 FAIL**. The explicit-directory case already passed.
After repair the complete environment-contract target passed **26/26**,
including evidence retention on successful and failed child commands,
permissions, non-overwrite, invalid destinations and initialization failure.
The initialization-failure case is additional guard coverage, not a pre-fix
failure claim. Temporary expanded Ollama wire preservation coverage passed
against unchanged production code before the rejected scenario was removed.

The canonical Provider command finished **FAIL**, exit `101`: **128 passed,
2 failed, 0 ignored, 7 filtered**. The wrapper independently verified all 130
distinct expected fresh cases against the final 137-case build, retained the
raw failing receipt, and verified unchanged source, binary, environment and
accepted-history bindings. All six Gemini File Search cases passed. The two
failures were:

| Canonical case | Expected / observed bytes | Kit finish | Exact-copy assertion |
| --- | ---: | --- | --- |
| `live::generation::ollama_function_continuation` | 39 / 39 | Stop | FAIL: different token |
| `live::generation::ollama_generation` | 39 / 38 | Stop | FAIL |

Canonical artifacts contain complete normalized Kit observations, not independent
native response bytes. The continuation input sidecar preserves its exact
expected token and tool-result correlation. The ordinary unary case exported no
request or expected token, so its particular missing/changed character cannot
be identified. Prior native parity does not establish parity for these exact
canonical responses. Both failures remain preserved.

Before live execution, all **643 deterministic Provider tests**, **250 Provider
doctests**, formatting, Clippy, and the workspace rustdoc/doctest lanes passed.
The verifier validated both Realtime Kit receipts; all three raw copy grades
were true: function turn, audio turn and post-cancellation function turn.
The live failure prevented the trailing artifact-hygiene stage from running;
that exact file check was run separately and passed. Documentation-contract
tests passed **13/13**, as did whitespace, dependency-path and type-alias guards.
Independent review found no unresolved findings in the retained verifier,
process contracts, owning documentation, final report or ledger. All three
historical reports remain byte-for-byte unchanged; every pre-existing tracked
change outside the four retained owners also matches the starting diff.

The user's subsequent instruction limits qualification reruns after further
fixes to failing cases and reuses unchanged passing evidence. No second broad
verifier run was performed. Five fixed unchanged unary diagnostics are recorded
separately below; they are not post-fix qualification or an extension of the
failed continuation candidate's sample.

### Newly failed unary case: five fixed diagnostics

After the canonical command and cleanup finished, a separate plan declared
exactly five unchanged `live::generation::ollama_generation` executions using
the final 137-case binary. All five **passed**, each with one complete native
request/response, an exact native copy, native/Kit text and finish parity, and
settled cleanup. No prompt, model, sampling, expected-token format, output or
assertion changed. No additional sample was run after these five outcomes.

The expected nonce was extracted from the complete captured user instruction
using its exact anchored grammar, never inferred from a response. The five
tokens were unique and disjoint from 41 known preceding expected tokens. The
earlier canonical unary expected value was not exported, so freshness against
that particular unknown value cannot be independently checked. Source, binary,
environment, server/model/template and private helper bindings held.

These observations demonstrate faithful handling of these five responses. They
do not independently reconstruct the canonical native response, explain its
failure, or turn either canonical failure into a pass. No production repair is
supported by the captured evidence, and no assertion was weakened to obtain one.

### Canonical cleanup

The owned verifier process exited `101`, was reaped, and left no process-group
members or cleanup signals/errors. All twelve fresh batch cases produced final
ownership snapshots. Nine accepted-submission checkpoints correlate with nine
settled owned jobs: eight provider-retained terminal jobs with captured
artifacts and one deleted Gemini missing-resource prerequisite. There is no
pending status, artifact discovery, deletion uncertainty, publication
uncertainty or input-discovery uncertainty in those final records.

One generated file from `gemini_provider_file_batch_completes` remains
**undeleted**, under the existing exact generated-output name-length `DELETE`
HTTP 400 exception. Its owning succeeded job, terminal metadata and exact
native `INVALID_ARGUMENT` match that exception. The recorded 42-day expiration
policy has **not** been verified by observing expiration. Every other fresh
batch case's final inventory has zero residual files. Process cleanup and
accounted-for retention do not mean this file or the eight terminal jobs were
deleted. No new cleanup exception was introduced.

## Historical long-test acceptance

The user's rule accepts an unchanged historical long-test pass as satisfying
the same requirement as a fresh pass. The seven accepted cases are the four
OpenAI inline/file/mixed-row/result-limit cases and three Anthropic
inline/mixed-row/result-limit cases enumerated by the canonical verifier.
Their historical individual passes remain accepted despite the earlier
aggregate command's unrelated failure; that failed command is preserved.

The audited batch production/test/build inputs and private environment remain
unchanged. The historical command already enabled private evidence export,
including batch observations and ownership/cleanup receipts. The reviewed
verifier-directory setup change does not introduce an untested batch behavior;
its orchestration received fresh regression coverage. The fresh command's seven
filtered cases are separately accounted for by accepted historical coverage,
not silently waived or represented as newly executed tests.

## Evidence and limits

Private artifacts remain under
`target/strict-live-runs/2026-10-29-provider-followup/` (directory `0700`,
receipts and captures `0600`). `baseline.json` binds the branch, commit, three original report hashes
and copied helper hashes; `initial-source.patch` preserves the starting diff.
Each build manifest binds the source scope, lockfile, executable, inventory and
build receipt. Plans and individual receipts bind commands, credentials by hash
only, runtime identities, helpers, raw grades and cleanup. Principal records:

- `gemini-baseline-summary.json`, `gemini-pairs-summary.json` and
  `gemini-native-audit.json`: every Gemini trial, native artifact hash and result.
- `ollama-extraction-plan.json`, `ollama-extraction-summary.json`, per-trial
  audits and runtime snapshots: all ten Ollama trials and rejected selection.
- `ollama-extraction-analysis.json` and `rejected-candidate-removal.json`:
  independent audit of all ten trials/twenty requests and byte-for-byte
  restoration of the existing scenario, wire coverage and prior Anthropic fix.
- `ollama-extraction-audit-tests.json`: nine offline diagnostic regressions.
- `verifier-evidence-red/` and `verifier-evidence-green/`: original red/green
  process-test output and bound command receipts.
- `final-provider-build.json`: final 137-case canonical test inventory.
- `provider-validation-plan.json`, `provider-validation-summary.json`,
  `provider-canonical/` and `long-batch-reuse-final.json`: exact fresh inventory,
  the retained failing command, accepted historical cases and source bindings.
- `currentcanonical-observation-audit.json` and its separate corrected `v2`
  receipt: both canonical Ollama observations and explicit native/nonce limits.
  V2 compares each declared source scope separately; the original diagnostic
  incorrectly compared unequal scope objects. The original is preserved.
- `provider-trailing-artifact-hygiene/`: separately completed trailing check.
- `provider-canonical-cleanup-audit.json`: all final batch inventories,
  accepted checkpoints, residual file, raw Realtime grades and process cleanup.
- `ollama-unary-plan.json`, `ollama-unary-summary.json` and per-trial audits:
  the five unchanged probes of the newly failed ordinary unary case.
- `ollama-unary-analysis.json`: independent verification of all five captures,
  raw exact grades, native/Kit parity, source/runtime bindings and cleanup.

This pass establishes affected Provider evidence. It does not establish full
workspace release qualification. The unresolved Ollama failures and
unexplained historical Gemini 400 remain visible; neither is hidden by later
single passes or accepted historical batch coverage.
