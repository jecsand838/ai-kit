# Native strict-live investigation — 2026-10-28–29

This follow-up preserves the [original campaign](strict-live-2026-10-28.md)
and [earlier repair report](strict-live-followup-2026-10-28.md), their failures,
and their raw grades. It records work on `codex/prepare-initial-release`, based on
`ed504a4787c3254185cc9db193b32479a955e456` plus identified working-tree changes.
These results establish affected-crate evidence, not workspace release
qualification. No public APIs, dependencies, lockfiles, model IDs, fallbacks,
compatibility paths, model-text repairs, or assertion exceptions were added.

## Changes and conclusions

- **Anthropic production repair:** the existing private streaming usage owner
  now replaces supplied cumulative input/cache counts, including zero, and
  retains omitted values. A new required wire regression first failed against
  the original owner and then passed with the repair. This is distinct from
  the historical missing-URL failure, which did not reproduce.
- **Ollama scenario:** only its unary continuation scenario gets an upfront
  literal tool-result relay system instruction. Sampling and the fresh nonce
  remain unchanged; the other six shared-helper callers retain their inputs.
  Instruction-only won the fixed pilot; confirmation passed **8/10** and failed
  **2/10**, so its acceptance gate failed. Ollama remains an unresolved native-model blocker.
- **Agent scenario and evidence:** the canonical Lance-conflict prompt now
  explicitly requests opaque literal output without tags, markup, surrounding
  text, or tools. Sampling stays inherited. Full already-fetched history is
  analyzed through the shared evidence owner; bounded evidence is persisted
  before cleanup. Both the replacement pilot and ten confirmations passed.
  The unchanged baseline also passed, so no causal repair is established.
- **Additional coverage:** wire replay checks cover both requests, history,
  sampling, tools, complete assistant continuation, JSON results, optional
  correlation IDs, unary/streaming and incorrect native output preservation.
  Agent tests preserve tags/whitespace through the real public adapter and
  preserve original assertion failures when diagnostic capture fails.

The new preservation tests initially passed on unchanged production adapters;
they are additional coverage, not pre-fix failures. The Anthropic usage
regression is the production repair's red/green evidence.

## Research and native evidence

Anthropic's [delta usage schema](https://raw.githubusercontent.com/anthropics/anthropic-sdk-python/main/src/anthropic/types/message_delta_usage.py)
and [official accumulator](https://raw.githubusercontent.com/anthropics/anthropic-sdk-python/main/src/anthropic/lib/streaming/_messages.py)
specify cumulative counts and replacement when optional fields are supplied.
Five unchanged Web Fetch streams reported later input totals that Kit lost:

| Baseline streaming trial | Initial / old Kit input | Terminal native input |
| --- | ---: | ---: |
| 1 | 1299 | 2722 |
| 2 | 1304 | 2737 |
| 3 | 1303 | 2734 |
| 4 | 1300 | 2725 |
| 5 | 1297 | 2716 |

The missing-URL investigation retained five unchanged unary and five unchanged
streaming negative cases. All ten passed with complete captures. All eleven
native invocations had the requested URL and correlated `url_not_accessible`
results; none returned `invalid_tool_input`. Unary trial 3 contained two native
fetches inside one HTTP response, not a Kit or relay retry. Each streaming
block's initial empty input object was followed by valid argument fragments.
URL-less error-result bodies remain distinct from absent invocation arguments,
as described by [Anthropic Web Fetch](https://platform.claude.com/docs/en/agents-and-tools/tool-use/web-fetch-tool).
No URL was inferred from the prompt when grading results. This sample does not
repair, disprove, or explain the historical missing-argument failure.

All four unchanged negative/positive URL controls also passed before the usage
repair. After repair, fresh controls retained the same assertions and passed:

| Post-repair control | Native input / Kit input | Output | Thinking | Cached input |
| --- | ---: | ---: | ---: | ---: |
| Unreachable URL unary | 2728 / 2728 | 235 | 54 | 0 |
| Unreachable URL streaming | 2725 / 2725 | 264 | 0 | 0 |
| Positive URL context unary | 3873 / 3873 | 62 | 0 | 0 |
| Positive URL context streaming | 3873 / 3873 | 62 | 0 | 0 |

The streaming controls started at 1300 and 1330 input tokens, respectively;
their final values now agree. Native text, IDs, URL/error correlation and
positive retrieval/citations remained intact. `total_tokens` remained absent.
Live Kit artifacts retain completed responses, not individual usage events.
Intermediate usage-event, nonzero-cache, explicit-zero and omission coverage
comes from the deterministic regression, not a claimed live-event observation.

Ollama's [published model template](https://ollama.com/library/ministral-3:8b-instruct-2512-q4_K_M/blobs/5b74c26b9e0b)
contains default persona/date instructions when no system message is supplied.
Four separate render-only probes of the two request stages confirmed those
instructions in baseline renders and their replacement by the explicit relay
instruction in candidate renders. The server's
[render-only path](https://raw.githubusercontent.com/ollama/ollama/v0.40.12/server/routes.go)
returns the rendered prompt without generating a model response. These probes
were separate from every trial. They establish template behavior, not that
the date instructions caused a fabricated prefix.

## Fixed comparisons and every trial

The four arms were baseline/current sampling, clarified instruction/current
sampling, baseline/explicit zero, and clarified instruction/explicit zero.
Each received five trials; order rotated by one arm each round. Inputs were
fixed before startup/first request and never adjusted after a response.
Selection required five complete passes, preferring instruction-only, then
temperature-only, then combined. Confirmation was fixed at ten fresh trials;
no candidate switching or sample extension followed failures.

### Ollama pilot

| Round | Baseline | Instruction only | Temperature zero | Combined |
| --- | --- | --- | --- | --- |
| 1 | PASS | PASS | FAIL: dropped suffix character | PASS |
| 2 | PASS | PASS | PASS | PASS |
| 3 | FAIL: inserted characters | PASS | PASS | PASS |
| 4 | PASS | PASS | PASS | FAIL: dropped suffix character |
| 5 | FAIL: fabricated token | PASS | PASS | PASS |

All forty HTTP captures were complete. Both requests retained the same model
and sampling in each trial; the initial-versus-continuation tool-choice
distinction remained. Every wrong native string remained wrong after Kit
normalization. Twenty unique nonces were generated after validating the first
tool call. Instruction-only was selected at 5/5; baseline was 3/5 and both
temperature arms were 4/5.

### Ollama held-out confirmation

| Trial | Result | Native finish | Expected / observed bytes | Raw exact copy |
| --- | --- | --- | ---: | --- |
| 1 | PASS | stop | 39 / 39 | true |
| 2 | FAIL | length | 39 / 4101 | false, independently derived |
| 3 | FAIL | stop | 39 / 39 | false |
| 4 | PASS | stop | 39 / 39 | true |
| 5 | PASS | stop | 39 / 39 | true |
| 6 | PASS | stop | 39 / 39 | true |
| 7 | PASS | stop | 39 / 39 | true |
| 8 | PASS | stop | 39 / 39 | true |
| 9 | PASS | stop | 39 / 39 | true |
| 10 | PASS | stop | 39 / 39 | true |

Trial 2 exhausted 4096 output tokens; the successful-stop assertion failed
before exact equality was reached. Its raw copy grade was derived from the
complete native and portable texts. Trial 3 returned another 39-byte token.
All output and finish reasons were faithfully preserved. Ten unique nonces
had no overlap with the pilot. No temperature or seed override was introduced.
All command/relay cleanup settled. The failed confirmation remains blocking,
regardless of any later single canonical-case result.

### Agent pilots and confirmation

The first twenty planned attempts were all `INCOMPLETE`: five rounds of all
four arms. A diagnostic supervisor race rejected child registration before
any native model request. The original receipts, code and forced-cleanup
observations remain. These attempts supply no model or Lance assertion grades.

The race was reproduced deterministically: the old helper took its process
snapshot before discovering a newly published registration. V2 reads that
registration before obtaining a fresh ancestry snapshot. Actual parent/PGID
proof remains required. Ten synthetic checks passed. A separately declared
replacement fixed set used fresh labels, the same arms/rotation and sample
sizes; it did not overwrite or regrade the original attempts.

| Replacement round | Baseline | Instruction only | Temperature zero | Combined |
| --- | --- | --- | --- | --- |
| 1 | PASS | PASS | PASS | PASS |
| 2 | PASS | PASS | PASS | PASS |
| 3 | PASS | PASS | PASS | PASS |
| 4 | PASS | PASS | PASS | PASS |
| 5 | PASS | PASS | PASS | PASS |

Instruction-only was selected by the declared preference. The four temporary
pilot entrypoints and all child/config/temperature/process-group experiment
code were then removed from the Rust suite; its inventory returned to 29.

| Canonical confirmation trial | Result / raw exact copy |
| --- | --- |
| 1 | PASS / true |
| 2 | PASS / true |
| 3 | PASS / true |
| 4 | PASS / true |
| 5 | PASS / true |
| 6 | PASS / true |
| 7 | PASS / true |
| 8 | PASS / true |
| 9 | PASS / true |
| 10 | PASS / true |

All thirty completed executions had unique markers, matching native Ollama
text, typed OpenCode-history text and Kit output. Every original Lance overlap,
real transaction conflict, single committed generation, request-part ownership,
fused-stream and cleanup assertion passed in all ten confirmations. Canonical
requests omitted temperature and retained the selected model, 4096-token
bound and disabled reasoning. All observed processes/relays settled without
forced signals in these completed trials.

Persisted history was analyzed in full in memory; bounded derived evidence
and native text were retained, not the entire history response body.
`foreground_http_body_observed` remains `false`: captured Ollama provider
traffic and persisted OpenCode history do not prove the original OpenCode
foreground HTTP response bytes. The original wrapped-output failure was not
reproduced. Baseline 5/5 and confirmation 10/10 prove only these bounded runs,
not causal repair or statistical reliability.

## Deterministic failures, corrections, and validation

- First full wire run: 251 passed, one fixture assertion failed. A TCP connect
  reset was classified as a late accepted request even though no request
  stream existed and eventual listener refusal was observed. The narrow
  correction accepts reset as handshake rejection; zero response bytes,
  eventual `ConnectionRefused` and both two-second bounds remain required.
  The next full target passed 252/252. No fixture production change was made.
- Provider strict-live assertions passed 264/264; Agent evidence assertions
  passed 68/68. The new public Agent tag/whitespace test passed 1/1
  (334 filtered); it is additional preservation coverage.
- Anthropic's first usage regression failed 0/1, then the full wire target
  passed 253/253 after repair. Review corrected two fixture details: initial
  thinking tokens must not exceed output tokens, and omission of the optional
  details object replaces an invalid empty details object. All assertions
  remained. The final valid fixture freshly failed 0/1 with the original two
  production owners, which were temporarily substituted under a saved repair
  snapshot and restored in `finally`; the repaired final fixture passed 1/1.
- Private helper checks: campaign verdict/ownership 9/9, fixed-set runner
  7/7, relay 12/12, initial Agent supervisor 9/9, corrected V2 supervisor
  10/10. Earlier synthetic failures and the newly reproduced registration race
  remain recorded. Relay repairs separate headers/trailers from entity bytes
  and continue owned cleanup after inspection failures. Credentials are never
  persisted in headers; request bodies and responses are bounded, with hashes
  and completeness/redaction grades. Capture concerns HTTP entity bytes, not
  network packets or independent proof that a client consumed every byte.
- The initial render-only receipt summary looked under the wrong debug key;
  complete raw responses were retained. Separate analysis read the actual
  `_debug_info.rendered_template` field without another request. A supplemental
  read-only OpenAI batch-status probe later failed local Python TLS certificate
  verification before authenticated HTTP; verified system curl succeeded with
  no TLS bypass or batch mutation. That observation found the owned job
  `in_progress`, with zero completed/failed requests. These diagnostic issues
  are separate from canonical test outcomes and their receipts are retained.
  A later verified, read-only observation of the second mixed-row submission
  found it `in_progress`, with two rows, one failed and one unfinished. Neither
  observation changes the test verdict or substitutes for its final assertions.
- Independent reviewers inspected the actual Provider/Agent diffs and the
  diagnostic execution owners before final live validation. Identified
  findings were resolved; the recorded red/green distinctions remain.

## Canonical verification and accepted historical batch coverage

The first Provider attempt (`provider-canonical`) failed at formatting before
live validation: one shutdown assertion in the new regression needed rustfmt
layout. Its failure remains. The formatting-only correction is separately
snapshotted; `provider-canonical-formatted` is a fresh full invocation.

The interrupted fresh invocation passed formatting, Clippy with warnings denied, all
13 deterministic integration targets (639 tests), the empty lib target,
Provider doctests (249 ordinary plus one compile-fail), artifact hygiene and
the workspace docs site. The latter built supported rustdoc lanes and passed
six doctest invocations: Agent 271, App 128, Provider 250, Tools 66, Storage
235 default and 245 explicit alternatives. These are command-specific counts,
including the intentional repeated Provider doctest gate, not unique tests.

The user explicitly interrupted its OpenAI mixed-row completion wait and then
specified that a historical long-test pass satisfies the same qualification
requirement as a fresh pass unless the directly tested code changed. The seven
historical passes are therefore accepted coverage, rather than deferred tests.
Their original execution dates remain visible; they are not seven new executions.

| Accepted historical case | Result | Directly exercised code changed? |
| --- | --- | --- |
| `openai_inline_batch_completes` | PASS | No |
| `openai_provider_file_batch_completes` | PASS | No |
| `openai_batch_preserves_mixed_row_outcomes` | PASS | No |
| `openai_batch_result_limit_is_enforced` | PASS | No |
| `anthropic_inline_batch_completes` | PASS | No |
| `anthropic_batch_preserves_mixed_row_outcomes` | PASS | No |
| `anthropic_batch_result_limit_is_enforced` | PASS | No |

All names have the `live::batches::` prefix. Independent comparison checked 124
Provider production files and fourteen test/build inputs against the retained
historical snapshot. The batch test owner, assertions, shared harness, cleanup,
manifests, lock, toolchain and private Provider environment were unchanged.
Anthropic's new cumulative usage merge is reached only from streaming
`message_delta`; the batch JSONL decoder and mixed-row unary preflight use
unchanged complete-response paths. The seven historical log blocks and their
settled ownership receipts were independently verified.

Those cases ran on October 28, 20:43:09–21:59:00 UTC. Their enclosing command
remains **FAIL: 136 passed / 1 failed / 137 selected** because of the unrelated
Anthropic URL-context failure. The user's explicit acceptance rule supplies
the seven case requirements despite that aggregate failure; this is not the
normal complete-passing-command reuse contract. Source comparison and exact
receipt hashes are retained in `long-batch-source-comparison.json` and `.md`.

The interrupted current attempt remains `INCOMPLETE` with `InterruptedError`.
Its owned process group settled after SIGTERM, the leader was reaped, and no
Cargo process remained. Subsequent exact-resource inspection found both owned
OpenAI jobs already completed; no cancellation was sent. Five terminal-job
artifact files were deleted with acknowledged responses and verified HTTP 404;
the original source fixture was already 404. All twenty cleanup requests and
their child processes settled with no issues. Completion and cleanup do not
grade the interrupted test's unreached row/assertion checks.

A proposed reuse of that attempt's completed static stages was rejected before
any Cargo/live execution because its whole-process environment hash did not
match. That preflight failure is retained. The next invocation instead runs
the canonical static/documentation gates afresh and selects exactly 130 fresh
live cases, with the seven accepted historical cases filtered by the existing
`PROVIDERS_KIT_RUN_LONG_BATCHES=0` mechanism. No other exclusion is authorized.

The fresh scoped Provider verifier ran October 29, 04:31:54–05:04:38 UTC.
Its static and documentation gates passed again with the same counts above.
Live execution finished **129 passed / 1 failed / 0 ignored / 7 filtered**
in 1,791.81 seconds. Independent inventory checking confirmed exactly the
remaining 130 cases; the seven accepted historical cases satisfy their own
requirements under the user's rule. The enclosing fresh verifier remains
`FAIL/101`, with unchanged source, helpers, environment, runtime configuration
and executable bindings, and unsignaled process cleanup.

The only fresh failure was `live::hosted::gemini_current_file_search_unary`:
HTTP 400, `InvalidRequest`, at `ResponseHeaders`, before response normalization.
The canonical invocation had no diagnostic evidence directory, so its native
error body was not retained; only the redacted error remains. The ordinary
`gemini_file_search_unary` case, which constructs the same model/store/prompt
request, passed later in that same run. Both streaming variants passed.
All 22 Gemini production files and its live request owners match the earlier
passing snapshot; no specific cause or adapter defect has been established.
The five passing File Search/retrieval-consistency cases do not erase this
sixth case's failure.

Both deleted-store consistency probes passed their observation and cleanup
contract with exclusive File Search output, while retrieval persisted after
verified management absence. Neither used the 2.5-only declaration exception
or proved retrieval erasure. The logged `File Search-only exclusion=true`
means other hosted-tool families were excluded, not that an assertion was waived.

Both approved Realtime Kit contracts and their receipt validator passed. Every
raw model-copy grade is retained:

| Realtime copy grade | Result |
| --- | --- |
| Post-cancellation function turn | true |
| Interactive session function turn | false |
| Interactive session audio turn | true |

The one false grade is nonblocking under the existing two-case contract; it
is not model-quality acceptance or independent native-parity proof. The final
artifact-hygiene stage was skipped when the verifier returned the live failure;
it was run separately and passed. No full-selection success marker is claimed.

Agent's first fresh attempt stopped before any verifier tests: macOS
`lsof -F p` emitted both `p78310` and `f27`, and the private listener parser
rejected the file-descriptor line. The Codex server had reached readiness,
then its owned process was terminated, reaped and verified absent with its
port free. The attempt remains `INCOMPLETE`. A separate owned startup probe
reproduced the parser failure and also cleaned up successfully.

`canonical_v2.py` uses `lsof -t` and strict decimal PID parsing; exact listener
uniqueness, owned process-group membership, free-port checks and bounded cleanup
are unchanged. The actual native-output fixture fails against the original
helper, and all four revised parser contracts pass. Independent review found
no remaining blocker. The original helper, failed receipts and probe remain.

The full V2 Agent invocation ran October 29, 05:10:25–05:46:16 UTC. Its
precheck/reporting process contracts, archive authority, 750 top-level
deterministic Rust cases plus two isolated child executions, all **29 live
cases**, rustdoc and 271 doctests passed. This includes
335 OpenCode ownership tests and the original Lance-conflict case. Three
isolated child executions repeat case headers in the log; the authoritative
parent summary and independent unique-case inventory both confirm 29, with
zero ignored or filtered cases. Both owned services were terminated cleanly,
their leaders reaped, process groups absent and ports free.

All six Question copy grades are preserved; both Kit contracts and their
receipt validator passed:

| Question scenario | Answer copy | Yes-description copy | No-description copy |
| --- | --- | --- | --- |
| Delivery/replay/resolution/drain | true | true | true |
| Native idle terminal waiter/reopen | false | true | true |

These use exact `qwen3.5:9b-q4_K_M` on OpenCode 1.19.3. The false answer-copy
grade remains nonblocking only under the existing two-case exception; actual
native description fidelity and other typed Question requirements still passed.

The Agent verifier then failed Clippy on unused `MessageInfo`/`NativeMessage`
imports and an unnecessary reference in `fingerprint(&self.session)`. Its
original `FAIL/101` remains. Removing those unused imports and passing the
already-borrowed `self.session` directly changed no assertion, native input,
evidence comparison or cleanup behavior; independent source review confirmed
that these were the only two changes after live execution. The affected
evidence target freshly passed **68/68**, full Agent Clippy passed, and the
previously unreached formatting gate passed separately. This is a composed
record of completed gates, not a relabeled successful canonical command.

After Agent execution and cleanup, one predeclared diagnostic pair ran the
current Gemini unary and streaming File Search cases once each, without
changing model, store, prompt, sampling, deadlines or assertions. Each passed
1/1 with 136 other cases filtered. Each retained one complete native HTTP
capture plus Kit evidence through the verified-TLS byte-preserving relay;
both relays and command groups settled with unchanged bound inputs. The
canonical HTTP 400 did not reproduce. These successes do not explain the
original error, establish a repair or replace its failed receipt. The pair was
not extended or repeated.

Independent capture analysis verified one HTTP 200 per case, byte-identical
594-byte request bodies and exact native/Kit text parity. Native grounding
contained a retrieved context and support but no file identity, so Kit returned
no fabricated normalized file citations. The exact marker and STOP assertions
passed; this does not recover the missing body of the canonical failure.

Two generated Gemini batch outputs, one from the interrupted invocation and
one from the fresh scoped invocation, were retained under the existing
retention exception; their expiration is unverified. Both provider-file batch
tests passed but did not prove those resources' deletion or expiration. Their
private ownership checkpoints are retained. Agent's owned service cleanup and
the fixed Gemini diagnostic pair both settled without outstanding issues.

## Source, runtime, commands, and retained evidence

Private evidence is under `target/strict-live-runs/2026-10-28-native-repairs/`.
Files are private and ignored; this report does not claim they are available
from a clean checkout. Supervised Cargo and live command receipts record exact command, source/lock
identity, effective build/environment hashes, native result, logs, and cleanup.
They bind loaded environment-file hashes when an env file was loaded, and
helper/binary hashes where applicable. Supplemental and synthetic unittest
logs are separate evidence and do not all carry that full provenance set.
Comparisons freeze the plan, loaded environment, helpers and build manifest
across the whole set. Native and supervisor verdicts remain separate.

The build controls were `CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`, and
`CARGO_TARGET_DIR=<workspace>/target/docs-site-build`, with Rust/Cargo 1.99.0.
The canonical live-execution source snapshot has tracked-diff SHA-256
`c1df50e6c43fbe0985ee87bef7eb4dd01109cfb254b1e344f54fb67959822ce9`
and lockfile SHA-256
`5f28a1a14d4ab6a4e5400d843dac2edb4aba4be4bdf6f5a61c840590a418f7d2`.
The two later Agent test lint corrections have tracked-diff SHA-256
`0b96855fe68adbc9eb8da5b920c7dfd933a65fd7766d12c5574d759cf2f8a996`;
their fresh deterministic/lint/formatting receipts bind that source. The report
and ledger were added afterward and have a separate final documentation
identity. Earlier trials retain their own source and binary hashes; no later
snapshot retroactively identifies them.

The supervised canonical commands are:

```bash
bash providers-kit/scripts/verify_strict_live.sh # interrupted full attempt
PROVIDERS_KIT_RUN_LONG_BATCHES=0 bash providers-kit/scripts/verify_strict_live.sh
bash agents-kit/scripts/verify_strict_live.sh
```

The private supervisor loads only the relevant crate environment and supplies
the build controls above. Long batches were enabled in the interrupted attempt;
the subsequent seven exclusions were fixed before starting the fresh scoped
invocation under the user's accepted-history rule. The Agent
invocation owns its required local Codex/OpenCode services and verifies their
cleanup. The Agent command uses no skip or receipt-reuse option.

Installed native versions were Ollama 0.40.12, OpenCode 1.19.3, Codex 0.163.0.
Exact models remained `ministral-3:8b-instruct-2512-q4_K_M`,
`gemma4:e4b-it-qat` and `claude-haiku-5-5`. Local model/template snapshots were
retained before comparisons and before canonical verification; they are not
claimed as independent per-request attestations. The default Ollama model
temperature was 0.15; the Agent model stored temperature 1, top-k 64 and
top-p 0.95. Zero was used only in the predeclared pilot arms.

Important evidence owners:

- `baseline.json`: starting branch/source and both historical-report hashes.
- `fixed-plan.json`, `fixed-plan-v2.json`, and `*-confirmation-plan.json`:
  immutable comparison/confirmation declarations.
- `ollama-pilot-analysis.json`, `ollama-confirmation-analysis.json`, and
  `ollama-render-only/`: raw grades, byte preservation, sampling and rendering.
- `anthropic-native-analysis.json`, `usage-fixed-analysis.json`:
  native URL/result correlation, cumulative usage loss and post-fix parity.
- `agent-pilot-v2-postanalysis.json`, `agent-confirmation-analysis.json`:
  complete execution grades, native/history/Kit parity and cleanup.
- Per-trial `*-result.json`, command `status.json`, native `receipt.json`,
  `relay-final.json` and, for pilots, group ownership/cleanup receipts.
- `agent-pilot-source-snapshot/`, `final-source-runtime/`,
  `final-formatted-source.patch`: source snapshots and runtime identity.
- `user-authorized-batch-exception.json`, `historical-batch-acceptance-rule.json`,
  `long-batch-source-comparison.json`: explicit acceptance and unchanged-path proof.
- `openai-interrupted-batch-cleanup/`: exact terminal-job/file cleanup receipts.
- `provider-canonical-scoped/`, `provider-scoped-inventory.json`,
  `provider-scoped-analysis.json`: fresh 130-case results, raw grades and inventory.
- `agent-canonical/`, `agent-listener-probe/`, `agent-canonical-v2/`,
  `agent-canonical-analysis.json`: startup failure, corrected execution and grades.
- `post-agent-lint-source.json`, `agent-lint-cleanup-assertions/`,
  `agent-trailing-clippy-fixed/`, `agent-trailing-format/`: later lint-only source
  and fresh checks.
- `gemini-native-diagnostic-plan.json`, `gemini-native-diagnostic-summary.json`
  and the two labeled native/Kit captures: fixed follow-up diagnostics;
  `gemini-native-diagnostic-analysis.json` and `.md` independently audit them.

The later formatting-only change to one Provider regression assertion has a
separate source identity. It does not rewrite earlier confirmation receipts.
Both earlier historical reports remain byte-for-byte unchanged. The older
tuple/list verdict defect and App cleanup uncertainty remain in those reports;
new evidence does not silently convert their receipts into passes.

## Remaining limitations and final checks

Ollama's fixed confirmation remains blocking: eight passes and two failures,
with faithfully preserved native output. Gemini's canonical unary HTTP 400
also remains unresolved; later diagnostic passes do not supply its missing
native body or a causal repair. The historical Anthropic missing-URL failure
did not reproduce, while the separate demonstrated streaming-usage defect was
repaired with a failing regression and native parity checks.

The seven historical long-batch passes satisfy their requirements under the
user's explicit unchanged-code rule. They are not a reason to mark unrelated
failed confirmations or fresh cases passed. No assertions were weakened,
response text corrected, model fallback added or failed receipt overwritten.
These affected-crate results do not establish full workspace release
qualification; fresh Tools/Storage/App strict suites, the workspace wrapper and
its clean-checkout proof were not part of this pass.

All 13 documentation-contract tests, whitespace checks, no-path wiring and
no-type-alias guards passed. Independent review of the actual code diff and
final report resolved its findings, including the deterministic child-execution
count clarification. Both earlier reports retain their original SHA-256 hashes.
`final-validation/` and `final-documentation-source.json` bind the final report
and ledger separately from the earlier runtime source and command receipts.
