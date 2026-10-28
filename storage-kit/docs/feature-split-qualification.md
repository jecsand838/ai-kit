# Storage feature split qualification

## Candidate and scope

This active ledger tracks the feature migration from production StateStore
commit `24b8d286138d398ac105704e4e2c76583d943cae`. The baseline lockfile SHA-256 is
`7ef9b5f1511c54bf743df27c8af6e4084c84287e321b9f13ae3e54f38ecddc34`.
Execution receipts must identify the final candidate, including untracked files;
results below do not qualify later edits automatically.

Lance remains unconditional for both products. Five alternatives become opt-in;
SQLite/Qdrant cannot coexist. Downstream real persistence and knowledge fixtures
use Lance. Public traits, configuration shapes, errors and crate boundaries
remain unchanged. The subsequently approved settlement-normalization work
authorizes only the one-way private App format migration described below. The user subsequently authorized private
Agent/App production repairs and App normalization, tracked by exact reviewed
source ledgers. Provider/Tool executable production code is unchanged. Caller-defined trait
probes remain. The owning
[portable contract](PORTABLE_API.md) is unchanged; [testing](testing.md) owns
selection and invocation behavior.

This task authorizes Storage backend qualification and changed Agent/App live
cases only. It does not authorize Provider/Tool live suites, complete downstream
verifiers, or the five-crate live wrapper. No privileged instrumentation is used.
The subsequent native-vendor campaign explicitly authorizes Agent's complete
configured verifier and final App live/crash checks, as recorded below.

## Current-model compatibility campaign — scoped qualification complete

A subsequent explicit scope extends PR #21 to current Provider/Agent model
profiles, protocol corrections, session migration and strict-live qualification.
It starts at `af28cddebdced6a5e668deb736ddb7526e8ea823` and does not relabel
any earlier receipt. Public APIs, dependencies, persisted formats, assertions
and existing deadlines remain fixed. The historical scope below applies to
its own source; Provider production changes are now explicitly authorized.

### Final scoped qualification — October 23, 2026

The approved continuation completed on commit
`9fcd393261db9869662f4ad91fc752f1a3772a1b`, after the complete Codex callback
recapture recorded below. No further callback Kit defect was demonstrated.
The exact-case pass remains non-reproduction of the historical missing callback,
not an explanation or relabeling of that failure.

| Gate | Recorded outcome |
| --- | --- |
| Codex **0.163.0**, OpenCode held at **1.19.0** | [Full configured qualification](../../target/storage-feature-split-evidence/scoped-live-20261022-192642/agent-completion.json): **28 passed, 0 failed, 0 ignored, 0 filtered**, 612.59 seconds |
| OpenCode **1.19.3**, Codex held at **0.163.0** | [Full configured qualification](../../target/storage-feature-split-evidence/scoped-live-20261022-212823/agent-completion.json): **28 passed, 0 failed, 0 ignored, 0 filtered**, 611.21 seconds |
| App canonical verifier | [Final owner receipt](../../target/storage-feature-split-evidence/scoped-live-20261022-224209/service-ownership.json): **18 live workflow cases and four prepare cases passed**; live portion 504.06 seconds |
| Clean candidate and packages | [Package receipt](../../target/agent-app-repair-evidence/packaging-20261023-003141/receipts.json): **PASS** for fresh source compilation, five native package verifications and the extracted-package consumer |

Both native updates retained every configured formatting, Clippy, source-authority,
documentation, deterministic, strict-live and post-live gate. Each Agent run
included exactly the 28 inventory cases and three separately accounted supervised
crash children. Both Question Kit/scenario checks passed in each run. The raw
delivery/idle-reopen copy grades were **true/false** for the Codex update and
**false/false** for the final OpenCode update. These failed quality observations
remain visible under the existing two-case exception; no other assertion changed.
Independent reviews bind the [Codex result](../../target/model-compatibility-20261022/native-resume-after-callback/final-outcome-independent-review-v1.json)
and [OpenCode result](../../target/model-compatibility-20261022/native-resume-after-callback/opencode-final-outcome-independent-review-v1.json).

The updater finalized its previously authorized conditional local commits:
`5d32ac87ed75e2fd9d436901a1955686876c7153` for Codex and
`9fcd393261db9869662f4ad91fc752f1a3772a1b` for OpenCode. The successful selected
vendor locks were finalized, not restored to the old releases. Invocation-only
Agent environments were restored, owned children reaped and process groups
settled without cleanup errors. The OpenCode increment changed the exact native
version pin and release documentation/tests; independent review also corrected
joined cleanup in the new deterministic release control before its final green
run and the canonical full gate. No callback workaround or live retry was added.

App ran the canonical four-boundary crash case **once**, inside its four-case
prepare target; no standalone crash invocation followed. Its routine workload
checks passed **180 workflow-execution and 38 worker cases**, explicitly excluding
the **ten plus one capacity cases**. All other configured deterministic, example,
rustdoc and Clippy gates passed. The canonical verifier completed in 4,200.10
seconds within its unchanged 5,400-second bound. The [independent App review](../../target/model-compatibility-20261022/native-resume-after-callback/app-final-outcome-independent-review-v1.json)
confirms exact inventories, unchanged source/artifacts, restored environment,
successful scenario cleanup and settled supervisor/service ownership. Its local
Ollama prerequisite observed **0.40.8**; older server identities belong only to
their historical receipts.

Before package cache cleanup, all four retained Provider, Agent and App test
binaries were copied and independently hash-verified. The [preservation manifest](../../target/model-compatibility-20261022/native-resume-after-callback/pre-package-artifact-preservation-v1.json)
binds the original receipts and the copies. Provider and both App artifact hashes
match their qualification receipts. Agent's preserved binary hash is explicitly
a **post-qualification/pre-clean observation**: the Agent completion receipt
binds its executable path and log, not a contemporaneous executable hash. The
earlier Codex binary at that reused path cannot be reconstructed from this copy.
An initial preparation-only check mishandled an expected-absent Provider path;
its error and the successful absence-aware comparison remain separate evidence.

The [package details](../../target/agent-app-repair-evidence/packaging-20261023-003141/package-details.json)
prove that all five Kit libraries compiled from the frozen snapshot after local
Kit cache cleaning, all five archives matched candidate source, and packaged
sibling checksums and retained dependency identities matched. Dependencies were
reused; this is fresh candidate-source proof, not an empty dependency-cache build.
The initial consumer seed lock was stale and failed locked metadata preparation.
The existing owner's offline preparation and dependency audit then preceded
successful locked metadata and execution. This preparation result is not reported
as a passing qualification command. The extracted consumer resolved all five Kits
from those archives, used real Lance state and an App workflow, and completed
joined shutdown without constructing an Agent native runtime. Final targeted
cache cleanup succeeded; no return-to-root cleanup remains outstanding. The
[independent package review](../../target/model-compatibility-20261022/native-resume-after-callback/package-final-outcome-independent-review-v1.json)
passed all 35 checks, retaining the seed-preparation distinction. The
[post-package comparison](../../target/model-compatibility-20261022/native-resume-after-callback/post-package-source-settlement-v1.json)
confirms 1,565 unchanged candidate files and absent owned process groups.

Provider evidence remains **118 fresh nonbatch passes plus 19 reused historical
batch passes**, under its original identities and approved receipt-reuse policy.
It is not a fresh 137-case execution. All **244 receipt bindings and the exact
186-file Provider source set** matched before package cleanup. The preserved
Provider binary now carries the original artifact hash after cleanup removed its
build-cache path. No Provider batch, Tool live suite, workspace-wide live wrapper
or capacity workload was run in this continuation.

The [final Storage continuity review](../../target/model-compatibility-20261022/native-resume-after-callback/storage-source-continuity-final-v1.json)
retains the existing Storage receipts: **129 production/lock bindings, 34
test/script bindings and 653 App bindings** match the reviewed October 20
manifest and final package snapshot, with no added or removed source. The
historical October 13 platform evidence matches the 129 production/lock inputs;
two later verifier/test changes retain their separate recorded red/green
orchestration evidence. The older platform runs are not claimed to have executed
those newer script bytes. No Storage matrix was repeated.

All historical failures and incomplete captures remain unchanged. The Anthropic
capture limitation, five withdrawn stateless-audio/Gemini-session cases and
retained OpenAI Realtime support remain explicit. Gemini's previously retained
generated output is **undeleted; expiration unverified**. Successful downstream
cleanup does not change that remote-resource exception. This report update is a
subsequent documentation-only edit; the clean snapshot and package proof identify
the commit above, not an uncommitted report revision. No push or merge occurred.

### Codex callback recapture — complete targeted pass; qualification resuming

The separately authorized [repaired-capture invocation](../../target/storage-feature-split-evidence/scoped-live-20261022-191958/receipts.json)
ran the exact nonblocking-input case once on Codex **0.163.0** and
**gpt-6-luna**. It passed **1 case, 0 failed, 0 ignored, 27 filtered** against
the unchanged 28-case inventory. The prompt, public API, production code,
assertions, generation count, scenario/request/cleanup deadlines and
7,200-second supervisor were unchanged. This is non-reproduction of the missing
callback; it does not establish the cause of either historical failure.

The [complete evidence](../../target/storage-feature-split-evidence/scoped-live-20261022-191958/codex-nonblocking/codex-input-evidence.json)
records all 52 exchanged frames forwarded, one genuine nonblocking callback and
exactly one correlated `-32601` Unsupported response. The original no-actionable-
Interaction and exact `INPUT_SETTLED` assertions passed. The corrected diagnostic
binds the entire 400-byte caller instruction to its unchanged 439-byte rendered
native prompt. The single owned rollout snapshot contains 21 complete paginated
records, including the correlated `request_user_input` call/output and matching
terminal. Its 77,713 bytes remain within the existing bounds. Persisted tool
output does not expose the native internal success flag; this history snapshot
does not prove model-service byte fidelity or historical causality.

Kit hierarchy cleanup and native bridge/process cleanup both succeeded. Every
supervisor child was reaped and its owned process group settled without cleanup
errors. No remote batch or file resource was created. Source, environment,
configuration, lock and executable identities stayed unchanged during execution.
Independent review confirmed the exact outcome, evidence boundaries and cleanup.
The previous failed/incomplete capture and the earlier full **27/1** and **24/4**
Agent outcomes remain unchanged below.

The next admission renews the reviewed source comparison before the full Codex
qualification with OpenCode held at **1.19.0**, followed by OpenCode **1.19.3**,
App and package gates. Provider evidence remains **118 fresh nonbatch passes plus
19 reused historical batch passes**; no Provider batch is repeated. The
Anthropic capture limitation, five withdrawn audio/session cases and retained
Realtime support remain unchanged. Gemini's retained generated output remains
**undeleted; expiration unverified**. No Kit repair, extra targeted retry, push,
merge or capacity workload occurred in this recapture.

### Codex callback capture — scenario failed; diagnostic binding incomplete

The one authorized [direct invocation](../../target/storage-feature-split-evidence/scoped-live-20261022-183224/receipts.json)
used Codex **0.163.0**, exact model **gpt-6-luna**, the existing real stdio
bridge and Lance persistence. Its frozen inventory contained 28 Agent cases;
the exact nonblocking-input case finished **0 passed, 1 failed, 0 ignored,
27 filtered**, with test exit 101. The command settled in 5.77 seconds without
timing out. Prompts, models, generation counts, assertions and the existing
180/90/60/30-second scenario, transport, lifecycle and process-cleanup bounds
remained unchanged, as did the 7,200-second supervisor. No HTTP observer,
additional model request or native history RPC was introduced.

The exact `INPUT_SETTLED` answer assertion passed, but the required real native
`item/tool/requestUserInput` callback was absent. The bridge therefore observed
zero correlated callback responses; the original callback assertion remained
blocking. All **51 observed bridge frames were forwarded**, but the
[diagnostic export](../../target/storage-feature-split-evidence/scoped-live-20261022-183224/codex-nonblocking/codex-input-evidence.json)
reported `capture_complete=false`, `wire_complete=false` and
`rollout.complete=false`, with `rollout_wire_identity_mismatch`. Accordingly,
the invocation is **INCOMPLETE**, separately from the failed Kit scenario.

A concrete diagnostic binding error was identified: the observer compared the
raw **400-byte caller instruction** with the existing rendered native input,
which includes a **39-byte empty managed-context wrapper** and is **439 bytes**
in total. That rendered input matches the captured noncryptographic fingerprint
`116bb674842d77b5`. This explains the diagnostic identity rejection; it does not
explain the missing callback or establish a Kit production defect. The owned
rollout snapshot was rejected before its records were projected, and normal
cleanup subsequently deleted the native rollout. Its contents cannot now be
reconstructed from the retained hashes. A focused
[red regression](../../target/model-compatibility-20261022/codex-callback-capture/prompt-binding-red-v2.json)
reproduces this diagnostic mismatch. The earlier local attempt failed at an
incorrect new test projection-path assertion and is retained separately;
it is not mapping-defect red evidence. The diagnostic now binds the original caller
text to the complete expected native rendering and compares that exact rendering
at both the wire and persisted-history boundaries. Negative controls reject a
missing or altered wrapper, changed caller text, added whitespace and appended
text. It strips nothing and changes no Kit production code. This local repair
does not complete or regrade the retained capture. No later live execution has
been performed.

Cleanup succeeded independently: the export records successful Kit hierarchy
cleanup and native bridge/process cleanup. The existing supervisor reaped its
direct children and settled their process groups with no cleanup errors.
The [settled source check](../../target/model-compatibility-20261022/codex-callback-capture/settled-live-check.json)
confirms the candidate stayed unchanged during execution. Invocation overlays
left the authoritative environment and restored vendor lock unchanged. This
case created no remote batch or file resources.

The diagnostic and runner received independent actual-diff review before the
single live execution. The subsequent prompt-binding repair passed **52 assertion
tests**, Clippy, formatting, diff checks and five documentation contracts; its
[post-live validation](../../target/model-compatibility-20261022/codex-callback-capture/post-live-validation-local.json)
retains exact commands and source hashes. Initial test-authoring, compilation and
local-listener sandbox failures remain separate evidence, not Kit defects. The
repair's [independent review](../../target/model-compatibility-20261022/codex-callback-capture/independent-review-v3.json)
confirms exact prompt binding, unchanged live assertions and cleanup ownership,
and the continuing failed/incomplete classification. The
pre-execution **103 Codex protocol**, **19 cleanup** and **14 source-contract**
passes, documentation/doctests, 37 runner controls and their reviewed source
bindings remain recorded. Source-contract validation temporarily installed the
selected candidate lock and restored the original bytes and mode. No additional
native request was used to validate the diagnostic correction.

The original **27/1** and **24/4** full Agent failures remain unchanged below.
Provider's source-bound **118 fresh nonbatch plus 19 reused historical batch
passes** retain their original evidence; no Provider workload was repeated.
Codex finalization, OpenCode **1.19.3**, final Agent/App qualification, canonical
crash recovery and package checks remain blocked. No retry, vendor-manifest
refresh, commit, push, merge or capacity workload occurred in this capture pass.
The Anthropic capture limitation, five withdrawn audio/session cases and retained
Realtime support remain unchanged. The Gemini generated output remains
**undeleted; expiration unverified**.

### Resumed Codex qualification — 27 Agent passes, missing native callback blocks

After the two knowledge/recovery captures below passed and settled, the approved
Codex **0.163.0** workflow resumed with OpenCode held at **1.19.0**. The
[versioned resume manifest](../../target/model-compatibility-20261022/native-resume-after-captures/resume.json)
binds all 1,564 candidate files and 155 evidence inputs, retaining the original
reviewed dossier and all 47 prior evidence inputs. The only launcher changes
were the reviewed manifest path and hash. Twenty-three offline admission
controls and independent actual-diff review passed. Provider admission again
verified all 244 bindings and the exact 186-file Provider tree; its **118 fresh
nonbatch plus 19 reused historical batch passes** were reused without new
Provider workloads.

The [complete invocation](../../target/storage-feature-split-evidence/scoped-live-20261022-154135/service-ownership.json)
passed formatting, every configured Clippy lane, documentation/doctests,
prechecks, source authority and deterministic Agent gates, including **48
assertion controls, 57 Custom tests, 103 Codex tests and 310 OpenCode tests**.
Its full Agent live inventory then finished **27 passed, 1 failed, 0 ignored,
0 filtered**, in 658.04 seconds. Knowledge, recovered-interaction follow-up,
Anthropic continuation and OpenCode structured output passed in this full run;
their earlier failures and targeted passes remain separate historical evidence.

The sole failed case was
`codex_nonblocking_input_returns_unsupported_without_actionable_notification`.
Its prompt completed with the exact expected `INPUT_SETTLED` answer and no
actionable Interaction notification. The passive bridge then failed its
requirement to observe a real native `item/tool/requestUserInput` callback.
Consequently the correlated `Unsupported` response assertions were not reached.
This is missing required scenario evidence, not a copy-quality failure or a
demonstrated Kit normalization defect. The retained native stderr does not
establish why the callback was absent. No prompt, assertion, deadline, model,
generation retry or production correction was introduced to bypass it.

Both named OpenCode Question Kit checks passed and emitted their receipts after
successful scenario cleanup. Their whole-answer copy grades were both **false
(0/2)** and remain visible, nonblocking model-quality observations under the
existing two-case policy; that policy does not apply to the failed Codex case.

A separate, demonstrated invocation-accounting defect was found: the existing
wrapper counted all 31 libtest start lines as top-level cases, including the
three intentional crash-child reexecutions. The real verifier failed before
that post-gate check ran, so this defect did not cause the 27/1 result. After
settlement, the existing ignored completion owner was corrected to recognize
only those three source-named nested starts with their required framing, while
still requiring all 28 distinct top-level cases and the exact successful
terminal summary. Meaningful offline red evidence and
[23 green controls](../../target/model-compatibility-20261022/native-resume-after-captures/nested-inventory-repair/accounting-controls-84e94368a2d9-a29ef79260ea.json)
cover nested framing, missing/extra/duplicate cases, failed gates and rejection
of this actual failed log. No test, live assertion, completion gate, deadline or
cleanup policy changed. This offline repair does not qualify the failed run;
the old resume manifest remains historical and was not refreshed for a retry.

Cleanup and source restoration succeeded separately from the test result. The
existing case cleanup ran before the assertion was rethrown. Failed-fixture
directory deletion is not established; its existing diagnostic-retention policy
remains in force. The outer owner
reaped the updater and both native prerequisite children, settled their process
groups and recorded no cleanup errors. It restored the original environment
and vendor-lock bytes; HEAD remained unchanged and no updater commit finalized.
The [post-settlement identity check](../../target/model-compatibility-20261022/native-resume-after-captures/post-settlement-identity.json)
found no source or bound-evidence change during execution. Historical receipts,
archives and the failed dossier remain intact. No unchanged retry occurred.

The failed mandatory gate stops escalation. OpenCode **1.19.3**, final Agent
qualification, App's canonical 18 workflow/four prepare cases, crash recovery
and package gates remain pending. The Anthropic capture limitation, five
withdrawn audio/session cases and retained Realtime support are unchanged.
The Gemini generated output remains **undeleted; expiration unverified**.
No commit, push, merge, capacity workload or redundant batch execution occurred.

### Knowledge and recovery boundaries — targeted passes; qualification resuming

The authorized two-case investigation completed in
[invocation 143805](../../target/storage-feature-split-evidence/scoped-live-20261022-143805/receipts.json).
Both exact cases ran once against the frozen 28-case Agent inventory, each with
**1 passed, 0 failed, 0 ignored, 27 filtered**. Inputs, assertions, generation
limits, scenario/prompt/cleanup deadlines and the 7,200-second supervisor stayed
unchanged. These are targeted passes and non-reproduction, not a demonstrated
Kit repair or an explanation of either historical failure. The original
**24 passed / 4 failed** result and the earlier two targeted passes remain
separate evidence under their original criteria.

The managed-knowledge case passed all three phases in 17.15 seconds. Its
[complete comparison](../../target/storage-feature-split-evidence/scoped-live-20261022-143805/agent-knowledge/native-sdk-comparison.json)
checks the original ordered Provider messages against the native request,
including exact managed-reference source identities, titles, text, multiplicity
and scope exclusions. Reference membership is checked independently of retrieval
ranking; ordered Provider-to-wire content and the existing answer-order
assertions remain exact. Native, Provider and Agent outputs were byte-identical
and matched the expected answers in every phase (203, 210 and 96 bytes).
Existing fresh-handle reopen, wrong-database rejection and owner-integrity
checks passed. No prompt or output transformation was introduced.

The OpenCode recovery case passed in 87.91 seconds using **1.19.0**. Its original
exact-output assertion was evaluated before bounded diagnostics. The
[native terminal snapshot](../../target/storage-feature-split-evidence/scoped-live-20261022-143805/agent-recovery/native-sdk-comparison.json)
identified one exact follow-up User message and its unique successful terminal
Assistant, with validated session, parent, part and configured-model
relationships. Native and Kit text both equaled the unchanged 54-byte expected
answer. Parts were assembled in native PartID order without unescaping or
trimming. This is a same-execution history snapshot, not a capture of original
model-response bytes. The three read-only native requests (health, session
inventory and message history) remained inside the
earlier of the existing scenario deadline and ten seconds after readback start.
The Question quality-grade exception was not extended.

Only existing test/diagnostic owners changed: bounded `KnowledgeEvidence`
exports, the ignored local observer/comparator and invocation owner, and
`NativeQuestionReadback`. Failed phase evidence and primary assertion failures
survive supplementary diagnostic/export/cleanup failures. No production code,
public API, dependency, model, prompt or acceptance rule changed. Fresh local
identity evidence records Ollama **0.40.8** and the same selected model digests;
OS executable mapping and hashing do not attest the loaded process against an
official source tag. No Ollama upgrade occurred in this pass.

The [validation dossier](../../target/model-compatibility-20261022/knowledge-recovery/validation-before-live.json)
retains meaningful diagnostic red evidence and green controls: 48 assertion
tests, 14 source-contract tests, observer and runner self-checks, Clippy,
formatting, documentation contracts and diff checks passed. Infrastructure and
preparatory compile failures remain separately recorded. Source-contract checks
used the reviewed temporary Codex 0.163.0 candidate lock and restored the exact
original bytes and mode. Independent actual-diff reviews cover knowledge,
recovery, observer/runner, the guide and Provider continuity; separate outcome
reviews retain each boundary's limitations.

Kit cleanup succeeded independently of output comparisons. Knowledge's local
Lance state and hierarchy cleanup settled; recovery's original lifecycle and
fixture cleanup settled. The local observer forwarded three requests unchanged,
joined its worker and handlers, and exited successfully. All owned children
were reaped and their process groups settled without cleanup errors. These
cases created no remote batch/file resources. The
[post-settlement comparison](../../target/model-compatibility-20261022/knowledge-recovery/post-settlement-identity.json)
matches all 1,564 frozen source paths, additional inputs, invocation inputs,
environment/vendor-lock bytes and the executable.

Provider evidence remains **118 fresh nonbatch passes plus 19 reused historical
batch passes**; the reviewed continuity comparison matches all 244 admission
bindings and 186 Provider entries. No Provider workload was repeated. The
Anthropic capture limitation, five withdrawn audio/session cases and retained
Realtime support remain unchanged. The Gemini retained generated output remains
**undeleted; expiration unverified**. The successful targeted evidence admits
the approved sequential vendor and downstream qualification, whose full gates
remain pending until independently completed. No focused-repair commit, push,
merge or capacity workload occurred.

### Two Agent boundary captures — targeted passes; historical causes unresolved

The authorized capture-only pass completed in
[invocation 132619](../../target/storage-feature-split-evidence/scoped-live-20261022-132619/receipts.json).
It ran each selected case exactly once against one frozen artifact, with the
complete **28-case inventory** and **1 passed, 0 failed, 0 ignored, 27 filtered**
for each selection. These are two targeted passes, not a new Agent verifier or
vendor qualification. The earlier **24 passed / 4 failed** result below retains
its original criteria and failed outcomes. No production correction was
demonstrated or made; the knowledge and recovery failures remain blocking.

| Captured case | Observed boundary and conclusion |
| --- | --- |
| `custom_anthropic_continuation_stream_and_reopen_are_live` | Passed in 8.48 seconds. Three complete native streams: one real `strict_continue` call with fragmented arguments; correlated assistant replay and successful tool result followed by a 70-byte answer; then a 70-byte answer after reopen with a fresh supplied Provider and no tools. The test-local tap observed all three completions, matching text lengths, call counts and normalized terminal reasons, with no Provider error. Native call identities and parsed-argument hashes matched replay. This is non-reproduction; the old normalization rule and failing generation remain unknown. |
| `opencode_prompt_model_and_structured_output_are_live` | Passed in 92.51 seconds using OpenCode **1.19.0**. Two model-facing requests were observed: Gemma plain generation, then the exact Ministral structured prompt. The second request contained the expected `StructuredOutput` schema and `tool_choice: "required"`; native returned one schema-valid 24-byte argument object with `finish_reason: "tool_calls"`. The existing exact structured-output and subsequent public I/O assertions passed. No auxiliary summarization request was observed in this invocation; the observer and comparator permit and separately classify such traffic. |

Both [Anthropic](../../target/storage-feature-split-evidence/scoped-live-20261022-132619/agent-anthropic/native-sdk-comparison.json)
and [OpenCode](../../target/storage-feature-split-evidence/scoped-live-20261022-132619/agent-opencode/native-sdk-comparison.json)
captures have complete bounded framing and projections, with no forwarding
errors. This establishes the recorded request, replay and terminal facts; it
does not claim independent raw native-to-Kit answer equality from the tap's
length-only text observations. OpenCode's existing native-history diagnostic
runs on failure, so no such diagnostic was required by this passing execution.

Fresh read-only server evidence reports **Ollama 0.40.8**, not the previously
recorded 0.40.6. The exact model tags and digests are retained in
[local identity evidence](../../target/storage-feature-split-evidence/scoped-live-20261022-132619/local-ollama-identities.log).
The listening process's mapped executable was independently hashed before
capture; its hash remained frozen during execution. This is local provenance,
not attestation that its running image exactly implements an official source
tag. No server upgrade occurred in this pass. The server-version difference
limits comparison with the historical failure and cannot be called its fix.
The official [0.40.8 compatibility source](https://github.com/ollama/ollama/blob/v0.40.8/openai/openai.go)
still lacks a `ToolChoice` field in `ChatCompletionRequest`; the earlier
[tool-selection report](https://github.com/ollama/ollama/issues/17921) remains
corroboration of a native limitation, not the cause of this successful capture
or proof of the historical execution's cause. No Agent workaround is justified.

Diagnostic changes extend the existing ignored `KnowledgeRequest` observer,
self-checks and invocation owner. Ordinary Anthropic tool blocks, result/replay
identities, argument fragments and terminal events are projected without raw
text, credentials or thinking/signature contents. A concrete test-local
Provider/Model tap preserves the original event/error stream and records the
safe Provider rule, generation ordinal and initial/reopened phase before Agent
error projection. It exports after existing cleanup and preserves the primary
Kit failure if export fails. OpenCode projection now retains schema and parsed
argument hashes/validity, including invalid native arguments as evidence.
No public API, dependency, production code, prompt, model selection, assertion,
generation limit or deadline changed. Only invocation endpoint routing changed;
reversing the OpenCode base-URL overlay reproduced its original configuration.

Meaningful diagnostic red evidence and subsequent green controls are retained
under [the capture dossier](../../target/model-compatibility-20261022/agent-boundary-captures/validation-before-live.json):
8 focused tap controls, all 38 Agent assertion controls, 33 observer control
groups and 28 runner controls passed. The initial all-assertion sandbox attempt
could not bind local sockets; its 28/10 infrastructure result remains separate
from the successful local-network invocation. Agent Clippy, formatting,
documentation contracts, diff checks and 14 source-contract checks passed.
The source-contract run used the reviewed temporary Codex 0.163.0 candidate
lock, then restored the original bytes and mode; no updater finalization ran.
Independent actual-diff reviews cover the
[tap](../../target/model-compatibility-20261022/agent-boundary-captures/tap-independent-review.json),
[observer](../../target/model-compatibility-20261022/agent-boundary-captures/observer-independent-review.json)
and [runner](../../target/model-compatibility-20261022/agent-boundary-captures/runner-independent-review.json).

Cleanup is separate from the test outcomes. Both scenarios completed their
existing hierarchy/native cleanup and fixture removal. Anthropic's tap records
successful initial/reopened cleanup and both Provider shutdowns. Both observer
workers and forwarding handlers joined; both observer children were reaped and
their process groups settled without errors. The existing command owner reaped
each test child and cleared its owned process group. There were no remote
batch/file resources created by these cases. The existing 7,200-second Agent
supervisor, scenario/prompt bounds and all cleanup bounds were preserved.
[Post-settlement identity checks](../../target/model-compatibility-20261022/agent-boundary-captures/post-settlement-identity.json)
found no source, environment, vendor-lock, diagnostic or executable change
during execution.

Provider evidence remains **118 fresh nonbatch passes plus 19 reused historical
batch passes**, under its original identities. The independently reviewed
[versioned continuity comparison](../../target/model-compatibility-20261022/agent-boundary-captures/provider-continuity-review-v1.json)
matched all 244 admission bindings and all 186 frozen Provider entries,
including its tests/scripts/docs. No Provider run or batch was repeated.
The previous Anthropic capture limitation, five withdrawn audio/session cases,
retained Realtime support and Gemini generated-output exception remain explicit:
that output is **undeleted; expiration unverified**. Vendor finalization,
OpenCode 1.19.3, remaining Agent qualification, App, crash-recovery and package
gates remain pending. No commits, pushes, merges or capacity workloads occurred.

### Corrected native resume — Agent live gate failed; cleanup settled

The corrected invocation resumed the reviewed Codex 0.163.0 candidate with
OpenCode still at 1.19.0. It reused the existing dossier and implementation;
it did not run another planning or implementation stage. The
[frozen resume](../../target/model-compatibility-20261022/native-resume-corrected/resume.json)
binds 1,562 candidate files, the current branch/index, 47 reviewed inputs and the
exact 28-case inventory. The existing ignored wrappers preserve the original
updater's candidate-lock installation, all seven completion gates, restoration,
archive finalization and conditional local commit. Nineteen resume controls,
four original-updater controls and
[independent actual-diff review](../../target/model-compatibility-20261022/native-resume-corrected/resume-independent-review.json)
passed before execution. The prior environment failure below remains historical;
this attempt's actual eight-stage Agent precheck passed with the corrected wrapper.

The [canonical invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-233900/service-ownership.json)
passed formatting, configured workspace and three Storage Clippy lanes,
documentation/doctests, precheck/Question-receipt controls, the 14 source-contract
checks and the configured deterministic Agent lead-in. That lead-in includes
**103 Codex**, **310 OpenCode** and **57 Custom** tests. Rust 1.94.1, native
`--locked`, serial Cargo and one build job were retained. No live source,
assertion, model, generation count or deadline changed during this resume.

The complete Agent live invocation ended **24 passed, 4 failed, 0 ignored,
0 filtered** in 583.81 seconds. These four failures remain independently blocking:

| Case | Evidence and limit |
| --- | --- |
| `custom_anthropic_continuation_stream_and_reopen_are_live` | Stream reception returned `Protocol / ModelGeneration / InvalidSequence`, with nested `CompletionValidation / InvalidSequence`, before output-copy assertions. The public error does not retain the original Provider normalization rule. The log does not identify which generation failed; Kit, native and fixture causes remain unresolved. |
| `managed_knowledge_scopes_reopen_and_preserve_owner_integrity` | The secondary response copied the database suffix into the Agent B marker. The normalized Provider and Agent texts are exactly the same 96 bytes, and recorded request-marker counts match expectations. This excludes Agent output corruption for that text, but lacks independent native wire evidence. The exact-answer assertion failed before the subsequent phase assertions. |
| `opencode_prompt_model_and_structured_output_are_live` | The existing complete diagnostic records matching requested format/model, a completed native assistant with `structured_output` error and no structured result or valid structured-tool input. Kit rejection is `StructuredOutputMismatch`; the capture does not establish why native generation failed. |
| `opencode_reopen_cancels_recovered_pending_interaction_before_publication` | The reopened response escaped underscores in the requested token. Its exact-output assertion failed. This is not one of the two approved Question copy-grade exceptions. |

The bounded investigations preserve the
[Anthropic failure](../../target/model-compatibility-20261022/native-resume-corrected/custom-anthropic-failure-investigation.json)
and [knowledge boundary](../../target/model-compatibility-20261022/native-resume-corrected/knowledge-failure-investigation.json).
No cause was promoted from hypothesis to a demonstrated Kit repair, and no
assertion was weakened. All Codex live cases passed in this invocation, including
caller loss, structured output and reasoning selection; that does not make the
failed complete vendor gate a pass.

Both OpenCode Question cases emitted valid receipts after successful scenario
cleanup: **Kit contract 2/2; raw exact-copy quality 0/2**. The canonical receipt
validator accepted both and visibly reported both false copy grades. These
nonblocking grades neither erase the four failures nor establish model quality.

The final [continuity comparison](../../target/model-compatibility-20261022/native-resume-corrected/settled-continuity.json)
was taken before this report update: all 1,562 source files, 47 reviewed inputs,
301 invocation bindings and **244 Provider admission bindings** match. The Agent
executable SHA-256 is
`0a3407ebd80c8dc949a4951551fd35719901e4af73f843723c18188dd8b205c7`.
Provider qualification remains **118 fresh nonbatch passes plus 19 reused batch
passes** under its original receipts, not a fresh 137-case pass. No Provider case
or batch was rerun. The failed Custom Anthropic composition is additional evidence
outside the scope of those Provider receipts; it does not rewrite them.

Cleanup is separate from the failed gate. The owner reaped the updater and both
native children, settled all three process groups, and reported no cleanup errors.
A subsequent read-only process-table check found no remaining members. The
crate-local environment and original vendor lock were restored byte-for-byte;
the lock again selects Codex 0.162.1 and OpenCode 1.19.0. HEAD remains
`16f7fdef89d1c86b109185d57e5aba71003bc5b1`. The uncommitted Codex 0.163.0
candidate, dossier and old/new archives remain intact. No vendor finalization
or new commit occurred.

This mandatory failure stops escalation. OpenCode 1.19.3 qualification, a passing
final Agent gate, App's 18 cases, the canonical four-boundary crash case and package
proofs remain pending. No unchanged retry followed. Historical failures, the
Anthropic capture limitation, five withdrawn audio/session cases and retained
Realtime support remain explicit. The prior Gemini generated output remains
**undeleted, expiration unverified**. No push, merge, capacity workload,
redundant Provider batch, Tool live suite or workspace live wrapper ran.

### Native refresh resumed — Codex candidate retained; environment gate failed

The approved sequential refresh resumed from the qualified Provider candidate.
The [pre-edit freeze](../../target/model-compatibility-20261022/native-resume/freeze.json)
preserves the previous source and receipts. Local checkpoint
`16f7fdef89d1c86b109185d57e5aba71003bc5b1` records that compatibility baseline;
it is not a vendor qualification commit. Nothing was pushed or merged.

Provider evidence was reused without executing Provider cases or batches:
**118 fresh nonbatch passes plus 19 historical batch passes**, under the exact
scope and limitations recorded below. The final
[identity comparison](../../target/model-compatibility-20261022/native-resume/settled-continuity.json)
matches all 244 downstream admission bindings, including the original 237
qualification inputs and the intentional absence of the withdrawn Gemini session
implementation. This remains neither a fresh 137-case pass nor qualification of
the new Agent candidate.

The first preparation attempt, `scoped-live-20261021-221827`, generated an
out-of-scope proposal for a 29th live cold-resume case. It was stopped before
implementation edits or live execution. Its dossier/archive and
[incomplete receipt](../../target/model-compatibility-20261022/native-resume/stopped-preparation/receipt.json)
remain intact. The existing task-local updater wrapper was narrowly corrected
to carry the approved 28-case scope into fresh planning and implementation;
14 offline controls and independent review passed. No original completion
gate, assertion, model, generation count or deadline was removed.

The subsequent [Codex invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-223239/service-ownership.json)
prepared the source-backed 0.163.0 adaptation. Its archive SHA-256 is
`36fda4e61c251d805c2d68203c3a34b0f6881a88846e12776ca02fb13d6e67c7`.
The selected runtime was Codex 0.163.0 with OpenCode still at 1.19.0, preserving
the approved sequential update order. The production change updates exact version
validation and validates the new private `thread/readState/changed` metadata;
it does not publish that advisory state, persist it, or grant it prompt-terminal
authority. Public APIs, dependencies, durable formats, live inputs and deadlines
are unchanged. All 12 live-source files and the 28-case inventory remain exact.

Meaningful red/green controls cover version selection and malformed new metadata.
The final deterministic Codex target passed **103/103**, with independent
[actual-diff review](../../target/model-compatibility-20261022/native-resume/codex-review/final-review.json).
The earlier **102 passed / 1 failed** run remains preserved: a newly added
shutdown-rejection control timed out on its prompt path. That timeout's exact
cause is unresolved. The final control was narrowed to documented `thread/resume`
rejection, with unchanged finite bounds and no replay; its pass does not qualify
`turn/start` rejection reconciliation or establish a production timing fix.
The earlier source-contract run retained 13 passes and one expected old-lock
mismatch; it is not a passing canonical candidate-lock source gate. Independent
inspection verified all 57 source-authority rows against the candidate archive.

The updater installed its candidate lock and passed formatting, workspace
non-Storage Clippy, all three configured Storage Clippy lanes, workspace
documentation/doctests, 13 precheck process controls and nine Question-receipt
controls. The mandatory actual Agent precheck then failed at its environment
contract, before any Agent live case:

```text
retired strict env must be unset: TEST_OLLAMA_MODEL_GPT_OSS_20B
```

The demonstrated cause is invocation setup: the task-local environment wrapper
re-sourced historical Provider settings after Agent cleanup and reintroduced
three retired Ollama aliases. The existing wrapper now unsets only
`TEST_OLLAMA_MODEL_GPT_OSS_20B`, `TEST_OLLAMA_MODEL_QWEN3` and
`TEST_OLLAMA_MODEL_THINKING` after those sources. Its
[offline red/green evidence](../../target/model-compatibility-20261022/native-resume/environment-correction/green.json)
and [independent review](../../target/model-compatibility-20261022/native-resume/environment-correction/independent-review.json)
confirm both OpenCode phase selections retain all other 50 input hashes and all
11 selected model/effort/tier roles. Authoritative environment files, the
precheck, Kit code and selected models were not changed by this correction.
It has not been exercised by another qualification run. The failed invocation
remains **FAIL**, not a Kit/native failure or a qualified vendor update.

Cleanup completed independently of qualification: both owned native children
and the updater were reaped, all three process groups settled, and the owner
reported no cleanup errors. A subsequent process-table check found no remaining
members. The crate-local environment and prior vendor lock were restored
byte-for-byte. The lock again selects Codex 0.162.1 and OpenCode 1.19.0; the
uncommitted 0.163.0 source candidate, archive, dossier and all old archives remain
available. No updater finalization or vendor commit occurred. No current live
Question grades exist because no Agent live case ran.

The campaign stops at this mandatory gate failure. OpenCode 1.19.3, Agent's
28 live cases, App's 18 cases, canonical four-boundary crash recovery and package
proofs remain pending. App invocation preparation passed 18 offline controls and
independent review; that preparation is not App execution. The canonical App
verifier already owns the crash case, so no duplicate crash run is planned.
Package source comparison and invocation preparation likewise do not constitute
package qualification.

The earlier Anthropic capture limitation, five withdrawn audio/session cases,
retained Realtime support, and Gemini generated-output exception remain unchanged:
that output is **undeleted, expiration unverified**. Historical failures retain
their original criteria and outcomes. This report was updated after settlement;
no retry, push, merge, capacity workload, redundant batch, Tool live suite or
workspace live wrapper followed the failed gate.

### Realtime Kit/model-copy split — scoped Provider qualification passed

The user explicitly approved a narrow acceptance-policy change for the two
retained OpenAI Realtime cases. Production code, public APIs, dependencies,
models, prompts, audio inputs, generation counts and deadlines remain unchanged.
The [pre-edit freeze](../../target/model-compatibility-20261022/realtime-grade-split/freeze.json)
retains 1,562 source identities and 280 historical-file identities. The earlier
Realtime failures below remain failed under their original whole-transcript gate.

Kit/scenario qualification still requires real cancellation where requested,
the required function call, exact call/result correlation, successful result
submission, new audio and nonblank transcription after submission, authoritative
completion, evidence exports and cleanup. The original entire raw transcript is
separately compared with its expected phrase: two grades for the ordinary
function/speech case and one for post-cancellation progress. False copy grades
are visible and nonblocking; missing or invalid grades remain failures. The
initial cancelled turn has no copy grade. This public-boundary proof does not
independently establish native-to-Kit fidelity, semantic use of the result or
statistical reliability. No output trimming or retry is introduced.

The [two exact executions](../../target/storage-feature-split-evidence/scoped-live-20261021-215352/receipts.json)
passed their Kit/scenario gates, each with **1 passed / 0 failed / 0 ignored /
136 filtered**. The ordinary function and speech-input copy grades were true;
the post-cancellation function copy grade was **false**. Its full transcript was
retained: the successful result-send checkpoint followed 40 transcription bytes
and 115,200 audio bytes, with another 93 transcription bytes and 501,600 audio
bytes after submission. This proves the required public-observation progress,
not native response attribution or semantic use of the result. Initial
cancellation remains `not_applicable`, not a failed copy grade. All Session
close, Provider shutdown and required exports succeeded. Nine supervised
children exited successfully; their process groups settled without cleanup
errors and an independent process-table check found no remaining members.

The subsequent [canonical scoped Provider run](../../target/storage-feature-split-evidence/scoped-live-20261021-215653/receipts.json)
passed **118 fresh nonbatch cases / 0 failed / 0 ignored / 19 filtered**, with
the exact expected names and **zero fresh batch executions**. It combines these
results with the 19 verified historical batch passes from `scoped-live-20261021-165740`;
this is **not a fresh 137-case pass**. The verifier completed in 588.91 seconds,
inside its unchanged 669,600-second supervision bound. Both Realtime Kit cases
passed, and all three raw-copy grades were true in this run:

| Raw-copy grade | Exact-case run | Scoped Provider run |
| --- | --- | --- |
| Ordinary function turn | true | true |
| Ordinary speech-input turn | true | true |
| Post-cancellation function turn | **false** | true |

The later true grade neither erases the earlier false grade nor establishes
model reliability. Historical preamble failures retain their original failed
outcomes and acceptance criteria.

The qualified candidate was `83ac2c2886111c33304030023853df104d443c188752b99a3f0b668f12236f7c`,
with executable SHA-256 `59d4600a9673c92ad151572696cbe4a02bacd6a576bd2c34af1336446d0e8002`.
All 237 recorded scoped-run input identities matched after settlement, including
the environment, lockfile, artifact and reviewed receipt bindings; every command's
before/after candidate identity matched. Five supervised children exited zero,
with no cleanup errors or surviving process-group members. The existing read-only
downstream admission checks accepted this receipt; no downstream work was launched.

Fresh remote cleanup is separate evidence: all three upload lifecycle ledgers
record successful owned-file deletion. Both Gemini owned-store consistency journeys
record source deletion and management-plane store absence; this does not establish
retrieval-plane erasure. Existing Provider shutdown assertions passed for all fresh
cases, and both Realtime exports separately record successful Session close and
Provider shutdown. Reused batch cleanup remains attached to its original receipts.
The prior Gemini generated-output retention exception remains **undeleted,
expiration unverified**; no new deletion or expiry claim is made.

Meaningful red evidence covered combined copy/scenario rejection, pre-result-only
progress incorrectly accepted, missing public grade receipts, invalid completed
turns incorrectly receiving a private grade, and duplicate result attempts after
a failed send. The corrections passed 264 assertion controls, 250 wire tests,
19 session lifecycle tests, 22 environment/verifier controls and five source-contract
tests. The complete Provider non-live gate passed formatting, Clippy, deterministic
targets, doctests and the required workspace documentation gate. Forty receipt-reuse
controls and 47 downstream-admission controls passed. The initial sandbox-denied
loopback controls and an incidental test-syntax error remain recorded separately
from meaningful red evidence and the subsequent successful checks.

Independent reviews covered the actual Rust/policy changes, runner and verifier,
admission, source impact and live outcomes. The [versioned reuse comparison](../../target/model-compatibility-20261022/realtime-grade-split/batch-source-comparison.json)
reviews five exact test/reporting paths, including the shared finish/export owner
and verifier; it retains 143 unchanged batch-input guards and 60 original historical
bindings. Non-session batch behavior, assertions and cleanup are unchanged.
The [validation record](../../target/model-compatibility-20261022/realtime-grade-split/validation.json)
and private evidence directory preserve red/green and review details. Production
and dependencies are byte-identical to this pass's freeze, and all 280 frozen
historical files remain unchanged.

#### Provider and Agent assertion audit

This source audit identifies mixed proof obligations, not new Kit defects. Only
Realtime acceptance changes in this pass; every other rule remains unchanged.

| Area and current owner | Current proof and follow-up recommendation |
| --- | --- |
| Provider ordinary generation, reasoning and current-model cases (`tests/live/generation.rs`, `current_models.rs`, `ollama_settings.rs`) | Requested nonce equality measures model compliance; selected identity, valid completion, settings mapping and cleanup are separate. Consider a future explicit quality split without claiming that a copied nonce proves reasoning or sampling effectiveness. |
| Provider streaming (`tests/live_assertions/mod.rs`) | Exact deltas-to-authoritative-completion equality, event ordering and retained errors are fidelity obligations. Keep them blocking regardless of copy quality. |
| Provider function continuation (`tests/live/generation.rs`) | Genuine calls, valid arguments, exact correlation and unchanged assistant replay exercise continuation. Final copying includes model compliance. Any future split must retain result propagation evidence and state its limits. |
| Provider structured output (`tests/live/current_models.rs`, `ollama_settings.rs`) | Cloud schemas require a string while prompts request an exact nonce; classify schema and value failures separately. Ollama's `const` value is part of schema conformance and must not become a cosmetic grade. |
| Provider hosted tools and uploads (`tests/hosted_assertions/mod.rs`, `tests/live/resources.rs`) | `Answer:` presentation differs from actual tool execution, exact resources, citations and hidden-marker readability. Missing execution/citations/markers cannot be graded away as wording. |
| Provider batches (`tests/live_assertions/mod.rs`, `batches.rs`) | Row text includes model compliance; job/custom-ID ownership, inventory, mixed outcomes, bounds, cancellation and cleanup remain batch obligations. Preserve all 19 reused passes under their original stricter criteria. |
| Agent token/tool/recovery answers (`tests/strict_live_suite/prompt_and_io.rs`, `custom.rs`) | Natural-language copying is a candidate for a separately reviewed quality grade. Actual tool execution, exact selected model, durable reopen/recovery and authoritative output remain mandatory. |
| Agent streams and knowledge (`tests/strict_live_suite/custom.rs`, `knowledge_tools.rs`) | Exact native/Provider-to-Agent and delta/terminal fidelity, real retrieval, scope isolation and durable state remain blocking. Marker formatting cannot excuse a missing or leaking knowledge scope. |
| Agent structured output and failures (`tests/strict_live_suite/prompt_and_io.rs`) | Retain schema/value assertions, eligible nonempty output, native errors and lifecycle checks. Missing final output is not equivalent to a valid answer with a preamble. |
| Existing OpenCode Question grades (`tests/opencode_question_evidence/mod.rs`, `scripts/report_question_grades.py`) | The two named cases independently prove native ownership, exact Question answers and native-terminal/portable fidelity before grading copying. Retain their stronger evidence and narrow scope; do not claim Realtime has equivalent native capture. |

This pass ends with scoped Provider qualification and the audit above. The
Anthropic capture limitation and five withdrawn audio/session cases remain
explicit; Realtime remains supported. Other vendor, Agent/App, canonical crash
and package gates remain pending. No commit, push, merge, capacity workload,
redundant batch execution or unrelated live suite occurred. This report update
followed settlement and does not change the recorded tested-source identities.

### Realtime post-cancellation clarification — exact case remains failed

The [single authorized changed-input execution](../../target/storage-feature-split-evidence/scoped-live-20261021-211017/receipts.json)
finished **0 passed / 1 failed / 0 ignored / 136 filtered**, exit 101, after
verifying all 137 retained cases. Live execution took 7.46 seconds, within its
unchanged 1,800-second supervisor. The selected model remained `gpt-realtime-2.1`.
Only the existing ordinary function-turn no-preamble instruction was extended
to post-cancellation progress. The initial cancellation prompt, nonce generation,
tools, output equality, logical turns and all operation/cleanup deadlines stayed
unchanged. This was a fixture-input clarification, not a demonstrated Kit repair.
[OpenAI's voice guidance](https://developers.openai.com/api/docs/guides/voice-prompting#use-preambles-intentionally)
supports explicit preamble instructions; it does not establish this failure's cause.

Cancellation passed with 57,600 output PCM bytes. The subsequent turn emitted a
60-byte preamble before its one valid function call, then exactly copied the
103-byte expected phrase. Its full 163-byte transcript correctly failed the
unchanged equality assertion despite genuine audio and completion. The expected
answer was assigned only after the call; prior transcription was retained.
The public-session evidence has no native-frame capture, so the historical
cause and native-versus-Kit adaptation boundary remain unproven. No output was
trimmed, extracted or synthesized, and no unchanged retry followed.

The [outcome receipt](../../target/model-compatibility-20261021/realtime-preamble-clarification/exact-final-outcome.json)
binds candidate `4b53903295b469bf445914b1e0847a477933177960797769691b2b32f6e42a5a`
and executable `4391c3ef2be54e2c6bd9347ab35d86724da29f496f45651e91a35c4457f51504`.
Rust 1.94.1, Cargo.lock, authoritative environment, source and executable remained
fixed through settlement. Production and dependency source hashes match the
pre-edit freeze. Session close and Provider shutdown passed independently of
the failed assertion. All seven supervised children were reaped, their groups
settled without cleanup errors, and a separate process-table check found no
remaining members. No observer, batch job or remote file was created.

Focused validation passed 258 assertion tests, 19 session lifecycle tests, five
source-contract tests, formatting, Clippy and documentation-contract checks.
The new pre-call-transcription control passed before the prompt edit; this is
baseline-green coverage, not manufactured Kit red evidence. Runner controls have
separate meaningful red/green evidence, with independent Rust/docs and runner
reviews. The initial sandbox listener failures and an explicitly stopped docs
build lacking the required invocation environment remain recorded separately;
the latter is incomplete and does not supply a docs-site pass.

The 19 historical batch passes remain preserved. Their renewed source-impact
review admits only the prior generation-evidence change and this nonbatch
Realtime prompt delta; it does not waive the failed live case. **The fresh
118-case nonbatch gate was not started.** Vendor, Agent, App, crash-recovery and
package escalation remain blocked. Earlier 117/1/19 and other failures, the
Anthropic incomplete capture, five withdrawn cases and Realtime support remain
explicit. The historical Gemini generated output is still undeleted, with
expiration unverified. No commit, push, merge or capacity workload occurred.

### Anthropic missing-URL investigation — unchanged captured pass

The subsequent [single captured case](../../target/storage-feature-split-evidence/scoped-live-20261021-192717/receipts.json)
passed **1 / 0**, zero ignored and 136 filtered, in 2.84 live seconds after
verifying the complete 137-case inventory. Its source identity was
`78a0e8adb85abbe725c81ddf75601da9542b8501a1dfe99a3851e809f0c22eac`;
the executable remained `5f04dc236aaea38347cb5c8f73590006353ee59dcd773516b83292a9fae1fb64`.
Compiler, lockfile, authoritative environment, invocation inputs and source
remained fixed through settlement. The model, prompt template, per-run nonce
construction, URL preflight, tool version, assertions, generation count and
deadlines were unchanged. The generated target URL and resulting prompt bytes
are specific to this invocation, not identical to the historical failure.

The [complete native/Kit comparison](../../target/storage-feature-split-evidence/scoped-live-20261021-192717/anthropic-url/native-sdk-comparison.json)
records initial tool input `{}`, then 15 input fragments producing the exact
82-byte requested URL, one correlated `url_not_accessible` result and
`end_turn` / `message_stop`. Native and Kit completed text, URL/error/call
correlation and terminal projections match. The comparison does not claim
reasoning/usage equivalence or an independently exported caller prompt before
normalization. This is **non-reproduction**, not a demonstrated Kit repair or
an explanation of the earlier missing-URL failures. No production code or live
test input changed during this investigation.

The existing ignored observer now distinguishes JSON syntax, input shape and
canonical input identity, and records bounded output-budget/tool-choice facts.
Meaningful diagnostic red evidence preceded its corrections; all 25 existing
self-check groups pass. Independent actual-diff review corrected two comparison
defects before execution: portable `completed` status spelling and exact call-ID
pairing in first-appearance order rather than result arrival order. The latter
has a separate red/green ordering control. The 49 selected deterministic wire
controls, formatting and Clippy passed; an initial sandbox listener-permission
failure is preserved separately from the permitted local test pass.

The credential-free TLS preflight used the frozen system CA bundle and verified
hostname/certificates. Exactly one Messages request was forwarded. Kit shutdown,
observer worker/handler joins, direct-child reaping and process-group settlement
all succeeded without cleanup errors. No remote batch/file resources were
created by this case. The prior **117 / 1 / 19 filtered** outcome remains failed,
and all earlier receipts remain unchanged. The 19 source-bound batch passes may
be reused for the next 118-case nonbatch qualification; this targeted pass does
not replace that gate. The historical Gemini generated output remains undeleted
under the existing exception, with expiration unverified.

### Subsequent nonbatch gate — two mandatory failures

After the captured pass and independent review, the [next scoped invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-193007/receipts.json)
finished **116 passed / 2 failed / 0 ignored / 19 filtered**, exit 101, in
329.23 live seconds (578.52 supervised seconds). Exactly the expected 118
nonbatch cases ran; no batch case executed. All canonical prerequisite and
deterministic gates preceding live execution passed. Together with the 19
explicitly reused batch passes, this supplies **135 passing case results and
two mandatory failures**, not a fresh 137-case pass or completed qualification.

The missing-URL streaming case passed again. The failures were:

- `live::hosted::anthropic_web_search_unary`: the untruncated Kit export has
  one completed search operation and nine result URLs, but no operation or
  final-answer citations. The unchanged source-and-matching-citation assertion
  failed. This execution has no independent native-response capture, so it
  establishes neither native omission nor Kit citation loss.
- `live::resources::openai_upload_lifecycle`: generation returned HTTP 429,
  native code `credit_balance_exhausted` and type `insufficient_quota`, faithfully
  classified as `Billing`. The owned upload was deleted despite the generation
  failure. After settlement, the user reported adding credits. At that point no
  post-change request had qualified the case; the later targeted result below
  leaves this failed receipt unchanged. This billing response establishes no
  Kit defect.

Source identity `f0c3a1ecb5195a1298366df1bc2629eab6b830333b9617c3f781d08a8956a4c4`
remained fixed through settlement. All 224 frozen inputs matched, including the
executable `5f04dc236aaea38347cb5c8f73590006353ee59dcd773516b83292a9fae1fb64`,
Rust 1.94.1, lockfile, authoritative environment and source-bound reuse inputs.
No deadline expired or changed. The [final interpretation](../../target/model-compatibility-20261021/anthropic-missing-url-capture/nonbatch-final-outcome.json)
records exact case names and separate cleanup results.

All three ordinary uploads record successful deletion. Both newly owned Gemini
stores record management lookup absence and source deletion; retrieval erasure
is not established. Both OpenAI Realtime cases passed consuming close and
Provider shutdown. Failed-case exports contain no supplementary shutdown or
cleanup error. Owned children were reaped and process groups settled without
errors. No fresh batch resources were created. The prior Gemini generated
output remains undeleted under its existing retention exception, with expiration
unverified; the earlier interrupted Kit shutdown and follow-up absence-check
limitations remain unchanged.

The mandatory failures stop vendor, Agent/App, crash-recovery and packaging
escalation. No retry, focused-repair commit, push, merge or capacity workload
followed. The prepared native admission change is unexecuted and grants no
qualification claim. All historical failures and five withdrawn cases remain
explicit; OpenAI Realtime support remains retained.

### Restored-credit upload pass and incomplete citation capture

The [OpenAI upload execution](../../target/storage-feature-split-evidence/scoped-live-20261021-200006/receipts.json)
passed **1 / 0**, zero ignored and 136 filtered, in 5.23 live seconds after the
user restored credits. It used the direct official endpoint and unchanged
`gpt-6-luna` selection, assertions and deadlines. The test-owned upload records
successful deletion, no cleanup failure, and successful Provider shutdown.
The historical billing failure remains failed; this result qualifies only the
recorded upload journey, not account-wide credit availability.

The [Anthropic search execution](../../target/storage-feature-split-evidence/scoped-live-20261021-200054/receipts.json)
also passed **1 / 0**, zero ignored and 136 filtered, in 4.05 live seconds with
unchanged `claude-haiku-5-5`, fixed prompt, search tool,
4,096-token allowance and assertions. Its untruncated Kit export has four text
parts, two genuine citations, one completed search and nine results. This is
non-reproduction of the earlier missing-citation failure, not a demonstrated
Kit repair.

The diagnostic invocation remains **INCOMPLETE**. Exactly one Messages request
received HTTP 200; all 25,597 response bytes were forwarded, but the observer
stopped projecting the response after retaining search invocation and result
facts. All nine retained native result URL/title hashes match the Kit results.
Those result URLs cannot substitute for native answer citations. No complete
native text/citation comparison survives, and no raw response was retained.
The generic diagnostic error cannot establish which subsequent block or field
caused projection to stop. The [independent outcome review](../../target/model-compatibility-20261021/anthropic-citations-openai-upload/post-capture-review.json)
preserves these limits; successful live assertions do not repair missing
diagnostic evidence.

Both runs verified the full 137-case inventory before exact selection. Source
identity `650bad3ebfbd8c6449708cd65dbffb944188bcf4d4efab50502d81790efa15e8`
and executable `5f04dc236aaea38347cb5c8f73590006353ee59dcd773516b83292a9fae1fb64`
remained fixed during each invocation, as did Rust 1.94.1, the lockfile,
authoritative environment and frozen invocation inputs. Kit shutdown passed
separately in both cases. The Anthropic observer joined its worker and handlers;
its child was reaped and process group settled. Every supervised command
settled without cleanup errors or deadline expiry. No new batch resources were
created.

Offline investigation then established a diagnostic coverage defect: the
unary observer rejected documented `thinking` and `redacted_thinking` content
blocks. The [focused red control](../../target/model-compatibility-20261021/anthropic-citations-openai-upload/observer-thinking-revision/observer-red.json)
precedes the observer-only correction. The [29-group green controls](../../target/model-compatibility-20261021/anthropic-citations-openai-upload/observer-thinking-revision/observer-green.json)
cover these blocks, retaining only presence/type/length facts and bounded
failure phase/index metadata, plus unchanged forwarding, redaction and
settlement. [Independent incremental review](../../target/model-compatibility-20261021/anthropic-citations-openai-upload/observer-thinking-revision/independent-review.json)
passed on the final observer and controls; runner controls, documentation
contracts and diff checks also passed. This proves a parser limitation; it does
not establish that such a
block caused the captured failure. No Kit production or live-test code changed.
All files from the incomplete capture remain hash-identical.

The existing deterministic Rust receipts retain source applicability through an
explicit comparison showing no Rust, test or Cargo-input changes. The 19
verified batch passes remain available for scoped reuse. The next 118-case
nonbatch gate has not started; incomplete citation evidence stops escalation.
After the observer correction and review, the user explicitly selected **stop
with current evidence**. No additional Anthropic capture or qualification run
is authorized by that decision.
All historical failures, the five withdrawn cases and retained OpenAI Realtime
support remain explicit. The historical Gemini generated output remains
undeleted, with expiration unverified. No commit, push, merge or capacity
workload occurred.

### Resumption with targeted passes and reused batch evidence

The user subsequently authorized resuming qualification using the two current
targeted Kit passes and the 19 verified batch receipts, while retaining the
Anthropic citation-capture limitation. This supersedes the preceding pause;
it does not reclassify the incomplete capture or explain the historical
missing-citation failure. No capture is repeated.

The existing qualification owner will select exactly 118 fresh nonbatch cases
and reuse the 19 source-bound batch passes. All prerequisite, deterministic,
assertion, deadline and cleanup gates remain unchanged. The canonical script's
`FULL` banner is configuration text, not evidence of fresh batch execution.
Only a complete scoped result and its cleanup can admit downstream gates.
The earlier five withdrawn cases, Realtime support and undeleted Gemini
generated-output retention exception remain unchanged, with expiration
unverified. No push, merge or capacity workload is authorized.

### Resumed nonbatch gate — Realtime exact-transcript failure

The [authorized resumed invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-203054/receipts.json)
finished **117 passed / 1 failed / 0 ignored / 19 filtered**, exit 101, in
328.28 live seconds (587.90 supervised seconds). The exact 118-case nonbatch
inventory executed once; no batch case or diagnostic capture ran. All preceding
canonical prerequisite, deterministic, documentation and doctest gates passed.
Together with the 19 explicitly reused batch passes, this supplies **136 passing
case results and one mandatory failure**, not a fresh 137-case pass or completed
qualification.

Both previously failing Anthropic paths passed: unreachable-URL streaming and
web-search citations. OpenAI upload also passed after restored credits. The
earlier Anthropic capture remains incomplete and its historical missing-citation
cause remains unproven; accepting that limitation did not relax any live
assertion or reclassify a historical result.

The failure was
`live::resources::openai_interactive_cancellation_and_subsequent_progress`, using
`gpt-realtime-2.1`. Its first turn delivered audio, accepted explicit cancellation
and passed cancellation verification. The subsequent function turn delivered
one expected function call, output audio and completion, but its 177-byte output
transcript contained a 75-byte extra preamble before the exact 102-byte expected
phrase. The unchanged whole-transcript equality assertion failed. This was not
a timeout or failure to cancel. The retained public-session evidence contains
no native frames and does not establish a Kit adaptation defect or a provider
root cause. No prompt, output filtering, assertion, deadline or production
behavior changed, and no retry followed.

Source identity `424ff32123c195415f0df5ae90c8f1a45eb982411285df709b2a6e18f510e43c`
remained fixed through settlement. All 224 invocation input hashes matched,
including executable
`5f04dc236aaea38347cb5c8f73590006353ee59dcd773516b83292a9fae1fb64`,
Rust 1.94.1, lockfile, authoritative environment and source-bound reuse inputs.
The separate pre-run freeze of 1,568 source inputs and 138 historical files
also matched after settlement, before this report update. Independent
applicability review admitted the unchanged 19 batch receipts; their original
run and failure disposition remain intact.

The failed Realtime case explicitly records successful consuming Session close
and Provider shutdown. The other Realtime case passed as well. All three
ordinary owned uploads record successful deletion without cleanup errors; both
owned Gemini stores and their uploaded sources completed their cleanup checks.
Management absence does not establish retrieval erasure. All five supervised
children were reaped and their groups settled without cleanup errors; a separate
read-only process-table check found no remaining member of those groups. No
fresh batch resources were created. The historical Gemini generated output
remains undeleted under the existing retention exception, with expiration
unverified; interrupted-shutdown and independent-absence limitations remain
historical and unchanged.

[Independent final review](../../target/model-compatibility-20261021/targeted-pass-resumption/nonbatch-final-review.json)
confirmed the exact selection, retained receipt applicability, failure boundary
and cleanup. Documentation-contract and diff checks passed after reporting.

This mandatory failure stops vendor, Agent/App, crash-recovery and package
escalation. The five withdrawn cases and OpenAI Realtime support remain explicit.
No commit, push, merge, capacity workload or repeated capture occurred.

### User-authorized batch reuse and nonbatch resumption

The user explicitly revised this campaign to reuse verified batch receipts and
stop redundant batch execution. The prior `scoped-live-20261021-165740` result
remains **136 passed / 1 failed**; all 19 batch cases passed. Reconstructed frozen
source identity `e2759f9f12f7544e3b8310d08cf6e9e73fcad5235b5aefac813a1e79021c4f41`
matches that receipt. Batch production code, shared helpers, test cases, models,
environment and lockfile remain unchanged. The intervening Ollama-only required
input sidecar does not execute in batch cases. Its captured passing result is
still non-reproduction, not a demonstrated historical repair.

The repeated [137-case invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-182841/receipts.json)
was stopped through its existing supervisor at the user's request after eight
batch cases passed. The result is **INCOMPLETE**, interrupted by SIGINT with test
process exit -15, not a deadline failure or passing full suite. The child was
reaped and its process group settled without cleanup errors; source identity
`fa1f47ee6d6b435efbe7a0b5ec2113fe90c930b642521e3bf81df15bbdabcde5` remained fixed.
Normal Kit shutdown is not established for the interrupted process.

[Exact operational reconciliation](../../target/model-compatibility-20261021/batch-receipt-reuse/stopped-run-reconciliation.json)
found that its one owned Gemini readability-prerequisite batch had succeeded.
The actual mixed-row job had not been submitted. Terminal metadata contained
inline responses and no generated output file. The exact 89-byte source upload
was independently identified, deleted once with HTTP 200 and `{}`, and then
returned HTTP 403 on lookup. That lookup proves neither presence nor absence.
The [operational receipt](../../target/storage-feature-split-evidence/scoped-live-20261021-185120/operational-cleanup.json)
remains INCOMPLETE under its extra 404 check. Independent review confirmed that
the existing ordinary cleanup contract requires the successful deletion
acknowledgement, not a subsequent 404; the separate reconciliation records that
limited outcome without changing the interrupted test or its distinct Kit-list
assertion. No cancellation, repeated deletion or additional request followed.
All cleanup subprocesses were reaped and their groups settled. The earlier
Gemini generated output from the prior full run remains undeleted under its
42-day retention exception, with expiration unverified.

The resumed invocation will retain every canonical prerequisite and deterministic
gate and execute exactly **118 nonbatch cases**, reusing the **19 source-bound
batch passes**. Its private exact-name selection does not change assertions,
deadlines, the canonical verifier or repository release requirements. Completion
must be reported as combined evidence, never a new 137/137 execution. The five
withdrawn audio/session cases, OpenAI Realtime support and all historical failures
remain explicit. Vendor, Agent, App, crash-recovery and package gates remain
pending until this scoped Provider qualification and cleanup pass.

The [resumed nonbatch invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-185713/receipts.json)
finished **117 passed / 1 failed / 0 ignored / 19 filtered**, exit 101, after
341.43 live seconds (614.88 supervised seconds, without supervisor expiry).
Exactly the 118 expected nonbatch names executed; no batch case ran. Combined
with the 19 explicitly reused batch passes, this is **136 passing case results
and one mandatory failure**, not a fresh 137-case pass or completed qualification.
All canonical prerequisite and deterministic gates preceding live execution
passed. The [verified outcome](../../target/model-compatibility-20261021/batch-receipt-reuse/nonbatch-final-outcome.json)
binds source `8b76455c0cb3f88727e4d8bbbc77a32d4d32f914b2e6bac3968c74283719bf10`,
executable `5f04dc236aaea38347cb5c8f73590006353ee59dcd773516b83292a9fae1fb64`,
Rust 1.94.1, lockfile and unchanged frozen inputs through settlement.

Only `live::hosted::anthropic_unreachable_url_streaming` failed. The Kit reported
`InvalidResponse(MissingField { field: "url" })` during `GenerateStream` /
`Normalize`. Its failure export has no supplementary shutdown or export error.
This execution has no independent native stream capture, so the error alone
establishes neither native omission nor Kit loss. The earlier Anthropic passes
and failures remain separate; no retry or speculative correction follows.

All three ordinary uploads record deletion. Both newly owned Gemini stores
record management lookup absence and source deletion; this does not prove
retrieval erasure. Both OpenAI Realtime cases record successful consuming close
and Provider shutdown. The test child was reaped and its group settled without
errors. No fresh batch resources were created. The earlier generated-output
retention exception remains undeleted, expiration unverified. These are scoped
ownership results, not account-wide absence. The mandatory failure stops vendor,
Agent/App, crash and packaging escalation; the completed-run monitor is stopped.

### Stateless OpenAI audio withdrawn — retained qualification resumes

The subsequent [complete 137-case invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-165740/receipts.json)
finished **136 passed / 1 failed**, exit 101. Only
`live::generation::ollama_function_continuation` failed: its first exact tool
call passed, then its final answer contained 50 bytes rather than the required
39-byte nonce. The retained normalized exports do not independently retain the
original nonce or native request/response, so this does not establish a Kit
defect or native/Kit parity. The live body took 2,768.03 seconds; it did not time
out. All 19 batch cases passed on that recorded candidate. Source, environment,
lockfile and executable identities remained unchanged through settlement.

The invocation's final ownership records contain no pending or uncertain batch
ownership. The three ordinary upload receipts record deletion; both owned
Gemini stores record lookup absence and source deletion. One exact Gemini
generated output remains undeleted under the existing six-week retention
exception; expiration remains unverified. Both OpenAI Realtime cases record
successful close and Provider shutdown. The Ollama failure contains no
supplementary shutdown/export failure, and owned command cleanup and process
group settlement completed. These are separate evidence categories, not an
account-wide resource inventory. The failed result remains historical; the
next bounded Ollama capture must establish caller/native/Kit correlation before
any behavioral correction or full qualification rerun.

The [single unchanged Ollama capture](../../target/storage-feature-split-evidence/scoped-live-20261021-182726/receipts.json)
then passed **1 / 0**, zero ignored and 136 filtered, in 6.59 supervised seconds.
The full 137-case inventory was verified before execution. Both native requests
were completely observed: the caller's original nonce and `FunctionResult`
matched the second native request, assistant replay and call correlation matched,
and the native 39-byte final answer matched both the original nonce and Kit
output. The source was `2543f50f5bd1b6fa13a6ad9a4fc0a2e5319d7c10409637fbc8ddc09b2b1cceed`,
executable `5f04dc236aaea38347cb5c8f73590006353ee59dcd773516b83292a9fae1fb64`;
source/environment/lock/executable identities remained unchanged through
settlement. Kit shutdown succeeded, observer handlers and worker joined, and
the child was reaped with its owned process group absent. This is
**non-reproduction**, not a demonstrated Kit repair, prompt correction or
explanation of the earlier failure. No model inputs or production behavior
changed, and the conditional prompt clarification was not applied.

The [diagnostic TDD and review records](../../target/model-compatibility-20261021/ollama-continuation-capture/)
retain the meaningful observer red, green local forwarding/projection controls,
one failed synthetic timestamp-order setup control, its corrected passing
controls, and independent actual-diff review. Review corrected diagnostic-only
handling of missing Kit exports, thinking replay and native finish authority.
The required caller-input sidecar reuses the existing bounded private writer.
Validation passed: 257 strict-live assertion controls, five source-contract
controls, all-target Clippy, formatting, five documentation checks and diff
checks. These checks do not substitute for a new full Provider pass. The
complete 137-case verifier was then required on the frozen final candidate;
the later explicit batch-reuse authorization above revises this campaign scope.

The user explicitly withdrew stateless OpenAI audio from the supplied Kit scope.
This is a support-policy change, not a demonstrated repair of the historical
native empty/capitalization failures. The preceding receipts and private,
unsent unary evidence packet remain unchanged. OpenAI's native audio support
is not being described as unsupported by the service.

Unary/streamed generation on the five previously routed exact audio model IDs
now returns `Unsupported` before I/O, including text-only follow-ups. Audio-model
batches reject before uploads or borrowed-file access; ordinary Responses audio
input remains unsupported. The private Chat Completions route, stream accumulator
and continuation representation were removed. Public types, signatures,
dependencies, Realtime implementation/configuration, shared speech assets and
all retained assertions and deadlines are unchanged. The three stateless audio
live cases and their required environment role are withdrawn explicitly in the
[owning testing guide](../../providers-kit/docs/STRICT_TESTING.md). They are not
counted as passes. The retained full inventory is **137 cases**: OpenAI 38,
Anthropic 35, Gemini 38 and local Ollama 26. The earlier two Gemini session
withdrawals remain separate scope decisions.

The [preserved candidate and TDD evidence](../../target/model-compatibility-20261021/openai-audio-withdrawal/)
contains the original source/diagnostic/input hashes and historical receipts.
The first sandbox-bound run could not bind its local test server and is retained
as infrastructure failure. The authorized loopback baseline then recorded
**3 passed / 1 failed**: the new model-selection control reached transport for
all 20 audio/text-history and unary/stream combinations instead of rejecting.
The three unchanged Responses-input, batch and closed-provider controls passed
on that baseline. After the smallest owner correction, **4 passed / 0 failed**;
no network requests were observed and the supplied provider/test-server owners
settled. Rust 1.94.1, `--locked`, serial execution and one build job were used.
Independent actual-diff review passed. Its runner-artifact identity finding was
corrected before execution: the exact newly built inventory executable is hashed
and checked before the canonical verifier and after settlement. Formatting, shell
and Python syntax, five documentation-contract checks and `git diff --check`
passed. Full retained qualification is the next gate; this entry is not a live pass.

The [first resumed gate](../../target/storage-feature-split-evidence/scoped-live-20261021-165248/receipts.json)
failed at Clippy before live admission: withdrawing the audio environment role
left a single-element loop in the existing environment test. Its assertions
were preserved while replacing the loop with its one local binding. The receipt
remains FAIL/101; inventory 137 and source/environment/artifact checks passed,
and every owned command/group settled without cleanup errors. No remote batch
or generation ran in that attempt. The corrected candidate must pass the full
canonical gate afresh.

The [second resumed gate](../../target/storage-feature-split-evidence/scoped-live-20261021-165427/receipts.json)
passed Clippy and preceding deterministic targets, then recorded **249 passed /
1 failed** in wire tests. An older combined validation case still asserted
successful audio generation and Chat Completions rendering. That stale positive
assertion now requires the explicitly withdrawn operation to return
`Unsupported`; its other local validation assertions remain unchanged. Provider
and server cleanup complete before the resulting assertions. The affected exact
control then passed (1 passed, 249 filtered), without a native request. This is
a withdrawal-test correction, not another Kit defect. The gate remains FAIL/101
with unchanged source/environment/artifact identities and empty process-cleanup
errors. No live case or native request ran in this attempt.

The full canonical Provider verifier, including every retained long batch case,
will run before vendor, Agent/App, crash-recovery or package escalation. Its
existing **669,600-second** process envelope is unchanged. Seven completion
cases contain eight independent 24-hour waits, so the outer bound does not cover
their worst-case sum; expiry would leave qualification incomplete and would
not itself prove remote cleanup. Audio withdrawal removes these three cases
from the active gate, without relabeling their historical failures. No targeted
audio replay, capacity execution, commit, push or merge is part of this change.

### OpenAI streaming/function audio capture — failed, no Kit correction established

The next bounded pass ran exactly the two authorized audio cases, without
repeating unary. The [original receipt](../../target/storage-feature-split-evidence/scoped-live-20261021-163730/receipts.json)
retains invocation **INCOMPLETE**, exit 101: both cases failed, each with
**0 passed, 1 failed, 0 ignored and 139 filtered**. The full 140-case inventory
and both exact selections passed. Streaming took 1.73 seconds and function
calling took 1.14 seconds. Neither execution timed out.

| Boundary | Observed evidence | Conclusion and limit |
| --- | --- | --- |
| Streaming native response | HTTP 200; 1,177 forwarded bytes; three SSE events plus `[DONE]`; zero text bytes, no calls, `stop`, zero completion tokens | Complete native framing/content/usage capture contains no nonempty answer to lose. |
| Streaming Kit result | `GenerateStream / Normalize / InvalidResponse(message)`; no completion export; untruncated error body is only the six-byte `[DONE]` marker | Empty-result rejection agrees with the existing source contract. Exact Kit accumulator/body parity is unavailable; original `comparison_complete=false` and invocation `INCOMPLETE` remain unchanged. |
| Function response | HTTP 200; one native request; native/Kit call identity and parsed arguments match; native `stop` correctly normalizes to portable `ToolCall` | The 21-byte argument differs from the required sentence only by capitalization. The exact assertion failed before tool-result construction or a second generation. |

The [separate offline interpretation](../../target/model-compatibility-20261021/openai-audio-boundaries/post-capture-interpretation.json)
and independent read-only review establish native output failures at the observed
boundary, without identifying the internal model/service cause. They demonstrate
no new request-mapping or Kit output-loss defect. No production correction,
prompt change, output normalization or subsequent live execution was made.
The first-generation function failure leaves continuation replay and final
nonce delivery unqualified by this run. Unary remains blocked under its earlier
complete empty-response evidence; its private support packet was neither changed
nor sent externally.

Only existing ignored diagnostics and their runner/self-checks changed before
execution. Meaningful red evidence captured the old observer's rejection of
OpenAI streaming. Green local controls cover fragmented SSE/function arguments,
trailing usage and `[DONE]`, raw and parsed argument hashes, canonical assistant
replay, nonce correlation, malformed identities/terminals, redaction, bounds,
byte-identical forwarding, failed writes and joined cleanup. Usage failures
remain sticky across later stream frames; a single terminal newline does not
stand in for the SSE blank separator. Existing unary checks remain green.
Independent actual-diff review caught a runner error-precedence regression and
an overconfident missing-export label; both were corrected before live execution.
Focused controls exercise the actual runner cleanup clauses, preserving a primary
exception and an existing Kit exit 23 while recording supplementary settlement
failures and blocking the next case when cleanup is unresolved.

Kit shutdown, observer settlement and process cleanup succeeded separately.
The existing case owner aggregates shutdown/export failures: streaming retained
only its Kit error without supplementary failures, and function calling retained
only its exact-value assertion. Each observer joined its worker and handlers,
exited 0, and was reaped with its process group settled. Both test-process groups
settled without cleanup errors. Each Kit request caused exactly one upstream
request; two total generations ran. The credential-free TLS preflight succeeded
and closed its connection before live admission. Verified CA/hostname handling,
environment bytes and all existing 180-second operation/shutdown and 1,800-second
supervision bounds remained unchanged.

Execution used Rust 1.94.1, native `--locked`, one Cargo command/build job at a
time, serial tests and macOS/APFS. Candidate
`77392bd5a347971e7e63a7a9ae5280f366a81d8602418e2700935d7ebd0ab418`,
lockfile `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`,
and executable `cedc6a8db61eda13d0f321598ae00c04201f80ed7e6a32085816d1bc8cbcc11c`
matched through settlement. The model remained `gpt-audio-1.5`; the 115,428-byte
WAV, prompts, 4,096-token allowance, sampling omissions, function settings,
assertions and deadlines were unchanged. All 1,565 frozen source entries and
157 historical evidence files matched before this documentation update. The
source differs from the passed mixed-row batch candidate only in the intervening
qualification-report update; the batch receipt remains scoped to its recorded
candidate. The [freeze, red/green and review receipts](../../target/model-compatibility-20261021/openai-audio-boundaries/)
retain exact diagnostic/input identities. No Rust source or test changed in this
pass, so the preceding deterministic receipts are retained with that explicit
source comparison, rather than relabeled as new executions.

All three audio cases remain blocking. Full Provider qualification, vendor
finalization, Agent/App, crash recovery and packages remain pending. Historical
failures and withdrawn coverage remain intact. No unary/batch retry, unrelated
live suite, commit, push, merge or capacity workload occurred.

### OpenAI mixed-row batch with bounded status retries — targeted pass

The user authorized exactly one fresh mixed-row execution after the reviewed
status-read retry change. The [final receipt](../../target/storage-feature-split-evidence/scoped-live-20261021-152303/receipts.json)
records **PASS: 1 passed, 0 failed, 0 ignored, 139 filtered**, exit 0. Test-body
time was 2,237.12 seconds; supervised execution took 2,237.420 seconds. The
140-case inventory and exact one-case selection passed. Both independent
24-hour completion waits, polling cadence, assertions, cleanup bounds and the
669,600-second supervisor remained unchanged. No timeout or interruption occurred.

The readability prerequisite succeeded. The actual mixed-row result contained
one successful row and one failed row; the existing exact output, correlation
and deleted-file error assertions passed. This is current targeted evidence for
the retry-enabled candidate. There is no per-attempt evidence that this execution
encountered a retryable error or exercised a retry; separate deterministic
regressions establish retry behavior. This pass neither explains the
historical connection reset nor relabels either earlier failed batch attempt.

Remote cleanup, Kit shutdown and process settlement passed separately. The final
untruncated ownership record has both jobs at `ArtifactsCaptured`, all pending
discovery/status and publication/deletion uncertainty flags false, no residual
files and no retained outputs. This proves terminal reconciliation and the
owning cleanup's successful file-deletion acknowledgements, not independent
GET-404 verification of every artifact or deletion of native batch records.
The passing case includes the existing 180-second Provider shutdown; its error
aggregation would fail the case on cleanup or shutdown failure. The supervisor
recorded direct-child reaping and owned-process-group settlement with no cleanup
errors; the invocation itself exited 0.

Execution used Rust 1.94.1, native `--locked`, one build job, serial tests and
macOS/APFS. Candidate
`52641938b162b9016d7c86a57ef8f32e24484ee4c3c5780f6431412dd873cd6a`,
lockfile `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`,
and executable `cedc6a8db61eda13d0f321598ae00c04201f80ed7e6a32085816d1bc8cbcc11c`
remained unchanged through settlement. All 1,565 frozen source entries, including
the already-absent Gemini session source, matched before this report update;
runner, environment and executable hashes also matched. Independent read-only
review confirmed the result, source checks and separate cleanup conclusions.

Only this case is qualified by this receipt. OpenAI audio failures still block
complete Provider qualification; full Provider, vendor, Agent/App, crash and
package gates remain pending. Historical outcomes remain unchanged. Monitoring
ends after this final result; no repeat attempt, additional native request,
commit, push, merge or capacity workload is included.

### OpenAI batch-status retry policy — deterministic qualification

The user subsequently authorized bounded retries for OpenAI `batch_status`.
This is a resilience-policy improvement, not a diagnosis of the connection
reset recorded below. The existing transport owner now receives retryable
execution only from the status operation. Shared lookup is an
`OpenAiApiContext` receiver method with an explicit existing `RequestExecution`;
batch-results lookup remains fail-fast. Exact identity checks and normalization
are unchanged. There is no new owner, retry loop, task, dependency or public API.

Configured retry count and backoff, including zero retries, use the unchanged
transport implementation and one absolute request deadline. Successful-response
body-read, decoding and identity failures are not replayed. Retryable HTTP
rejections retain the existing diagnostic and error precedence. Listing, submission,
cancellation and file operations retain their prior policies. The fixture's
two separate 24-hour waits, one-minute polling, assertions, cleanup and shutdown
bounds are unchanged. Cleanup status reads use the same bounded status policy.

The [source freeze and red receipts](../../target/model-compatibility-20261021/openai-batch-status-retries/)
record 1,565 tracked/untracked source identities, the Rust 1.94.1 compiler,
lockfile, existing executable/environment hashes and all 20 historical run
artifacts. Before production edits, two new supplied-adapter regressions
failed after exactly one status GET: complete-request/pre-header disconnect
and HTTP 503, each followed by an available successful reply. Both tests
settled Provider shutdown and their existing local server owner without errors.
The disconnect is deterministic transport evidence, not an OS-reset or
pooled-connection reproduction.

All nine focused status tests then passed. They cover recovery, zero/exhausted
retry budgets, permanent and invalid responses, one absolute deadline, body-error
precedence, backoff that cannot fit, cancellation/shutdown, and fail-fast result
lookup/list/cancel behavior. Existing uncertain-submission coverage now requires
exactly one creation POST. All local server and Provider settlement checks passed.

The complete `providers-kit/scripts/check.sh` passed with Rust 1.94.1, native
`--locked`, one build job and serial tests. This includes 256 wire tests, ten
transport-diagnostic tests, 257 strict-live assertion checks, public and source
contracts, lifecycle/environment checks, Clippy, formatting, doctests and the
documentation-site gate. The strict-live assertion target is deterministic and
does not execute providers. Five documentation-contract checks also passed.
Independent actual-diff review found no production or test issue; its one
documentation precision correction distinguishes successful-response body failures
from diagnostics accompanying known retryable HTTP rejections.

Only the two OpenAI production files, two existing test files and three owning
documents changed in this pass. Existing transport, public API, configuration
and live fixtures (including inputs, assertions and deadlines) remain byte-identical to the frozen
candidate, as do all 20 historical run artifacts. No live request or batch
attempt was performed. The historical mixed-row result remains failed and
unqualified on this new candidate; audio failures still block complete Provider
qualification. No commit, push, merge or capacity workload occurred.

### Independent OpenAI mixed-row batch: connection reset, cleanup settled

The user authorized one independent execution despite the unresolved audio
blockers. The [final receipt](../../target/storage-feature-split-evidence/scoped-live-20261021-112011/receipts.json)
records **FAIL: 0 passed, 1 failed, 139 filtered**, exit 101, after 8,551.512
seconds. The 140-case inventory and exact one-case selection passed. Both
per-job 24-hour completion waits, all assertions, and the existing 669,600-second
outer supervision envelope remained unchanged; supervision did not time out.

The readability prerequisite completed and the actual mixed-row batch was
accepted. Its completion polling returned `Transport / BatchStatus /
RequestSend`, with a retained cause chain ending in `connection reset`. There
was no HTTP status, response body, or timeout classification. Mixed-row outcome
assertions were not reached. This is separate from the earlier HTTP 503 failure;
neither establishes which remote, intermediary, network, or local component
caused the interruption.

Cleanup succeeded separately. The final, untruncated ownership ledger records
both jobs as `ArtifactsCaptured`, no pending status or artifact/input discovery,
no publication/deletion uncertainty, no residual files, and no retained outputs.
This establishes terminal reconciliation and successful Kit deletion
acknowledgements, not successful completion of both jobs or independent GET-404
verification of every deleted file. The final failure export retains only the
primary polling error: under the existing aggregation path, batch cleanup,
Provider shutdown, and required exports added no failures. The supervisor
recorded successful direct-child reaping and process-group settlement; its final
disposition is `FAIL`, not `INCOMPLETE`.

The execution used Rust 1.94.1, native `--locked`, one build job and serial Cargo.
Candidate `e0b0592a147ed217284151d4e814030333549ce78e8419ab8c7cbfb8e90fd5a3`,
lockfile `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`,
and artifact `5d92c961c4b0949ecf8487dbfe2cbd61db569777895dc81edd0667bbb26daee4`
remained unchanged through settlement. The
[source comparison](../../target/model-compatibility-20261021/openai-independent-batch/historical-batch-source-comparison.json)
preserves unchanged named batch/transport inputs relative to the original full
Provider failure; it does not claim identical whole-candidate or execution context.

The subsequent read-only production audit found no demonstrated Kit defect.
`batch_status` is one request, not a Kit polling owner. OpenAI's `fetch_batch`
selects fail-fast execution; the test polls successful queued/running responses
every 60 seconds and propagates a failed read immediately. Its 180-second HTTP
request bound, 30-second connect bound, 90-second idle-pool retention, and the
24-hour job-observation bounds have different purposes. The retained evidence
does not identify stale pooled-connection reuse. Locked reqwest 0.13.2 retains
its native protocol-NACK retries, and hyper-util 0.1.20 already recovers
demonstrably unstarted requests on reused connections. The captured `SendRequest`
error does not establish that this safe-recovery condition held or that OpenAI
received the request. Disabling pooling, changing pool timing, or forcing an HTTP
version has no demonstrated basis. OpenAI's
[Python client](https://developers.openai.com/api/reference/python#retries)
uses configurable default retries for connection errors; matching that behavior
would be a deliberate resilience-policy change, not a proven cause-specific fix.
The [Batch guide](https://developers.openai.com/api/docs/guides/batch) separates
status retrieval from remote processing, and the
[connection-error guidance](https://developers.openai.com/api/docs/guides/error-codes)
does not attribute this particular reset.

The existing two retry-policy wire controls and ten transport-diagnostic tests
passed with no test edits. The first sandboxed control invocation failed to bind
localhost (`PermissionDenied`) before exercising Kit behavior; the same controls
passed with localhost access. Their [logs and source freeze](../../target/model-compatibility-20261021/openai-batch-reset-audit/)
are separate deterministic evidence, not a reproduced OS reset or live batch
qualification. Independent source and incremental-documentation reviews found no
actionable defect. Formatting, five documentation-contract checks, and diff
checks passed. The audit's source comparison confirms only the three owning
documents changed and all 20 run-evidence files remain byte-identical.
No production code, retry policy, assertion, deadline, or cleanup owner changed.
Audio and mixed-row qualification remain blocking. No further
batch attempt, live request, push, merge, or capacity workload was performed.

### Gemini interactive audio/session support withdrawn

The user explicitly withdrew Gemini audio support for now. Because the supplied
Live implementation always requested audio output, this withdraws all supplied
Gemini interactive sessions, including text-input Live journeys. `open_session`
now uses the existing sessionless `Unsupported` path before network I/O; Provider
admission still preserves `Closed` precedence after shutdown. Public types,
traits, signatures, errors, dependencies and crate boundaries remain unchanged.
Stateless Gemini audio generation and OpenAI sessions are unaffected. The private
Gemini Session owner and its dead dispatch/transport paths are removed.

This is a scope withdrawal, not a repair or a passing Gemini live qualification.
The two Gemini live cases are removed explicitly; the retained inventory is 140
(OpenAI 41, Anthropic 35, Gemini 38, Ollama 26). Exclusive Gemini session fixtures
are withdrawn; shared OpenAI assertions, prompts, deadlines and cleanup are
retained. The former interactive-model environment role is no longer required.
All previous results and source-bound receipts below remain historical evidence,
including the original 128/14 Provider result and every audio timeout.

A new public behavior regression requires immediate `Unsupported` for both former
Live IDs and a stateless Gemini ID, with default and formerly valid Manual and
Automatic settings, no connection, and subsequent `Closed` precedence. Against
the prior implementation it failed after the unchanged one-second test bound
because a valid configuration attempted connection. The
[starting snapshot](../../target/model-compatibility-20261021/gemini-audio-unsupported/before.json),
[red result](../../target/model-compatibility-20261021/gemini-audio-unsupported/red.log)
and assertion/source maps preserve the exact incremental withdrawal. The focused
regression now passes. The [Provider deterministic run](../../target/model-compatibility-20261021/gemini-audio-unsupported/provider-check.log)
passed all 609 non-live test cases and 240 doctests, including 19 session,
5 shutdown, 247 wire, 257 live-assertion and 17 environment-contract cases.
Its wrapper stopped at a historical link to the deleted Gemini source; the link
now points to the frozen source snapshot without changing the historical result.

Independent incremental-diff review confirmed API, stateless Gemini audio,
OpenAI behavior, assertion, deadline and cleanup preservation. It found one
fixture assertion to preserve: the exact session model moved from Gemini's setup
body to OpenAI's upgrade URI in three generic shutdown tests. That request check
is restored using the existing `HttpRequest` owner and upstream handshake
callback. The first closure form failed Clippy's large-error lint; the reviewed
owner implementation passes without suppressions, new types or runtime owners.
The final shutdown target passes 5/5. All intermediate outcomes are retained.

The [compiled live inventory](../../target/model-compatibility-20261021/gemini-audio-unsupported/inventory-result.json)
confirms exactly 140 cases, without executing them. The
[final check](../../target/model-compatibility-20261021/gemini-audio-unsupported/final-check-2.log)
passes formatting, Clippy with warnings denied, documentation contracts, and
the complete canonical documentation build/doctest gate (exit 0). Checks use Rust
1.94.1, native `--locked`, serial Cargo execution and one build job. Source
comparison confirms unchanged OpenAI session implementation, stateless Gemini
audio adapter, shared live cleanup owner, PCM assets and Cargo lockfile.

This removes Gemini sessions from the current support promise; it does not
complete broader qualification. OpenAI audio and mixed-row batch qualification
remain unresolved, followed by the complete retained Provider gate and downstream
vendor, Agent/App, crash and packaging obligations. No live, capacity, commit,
push or merge is included in this withdrawal.

### Gemini continuous-audio native control also timed out

One separately authorized native control used the continuous sender/receiver
flow from the [pinned OpenClaw probe](https://github.com/saariuslystoned/openclaw/blob/46c0b93118416f1119afb6d28bcc9571c38fc9db/proof/audio-stream-end-silence/probe/probe_input_tx.py).
This was an ignored, task-local raw-WebSocket adaptation, not an unchanged
execution of OpenClaw or the Google Python client, and not a Kit live test.
It retained the exact 819-byte Kit setup, model, instructions, `BLOCKING`
declaration, transcription configuration and original 108,924-byte PCM prefix.
After the existing paced prefix it continuously sent 1,024-byte zero chunks,
awaiting each send and 32 ms before the next. It sent no `audioStreamEnd`, tool
result, text request or second utterance. The stopping condition was the first
exactly valid `strict_live_session_probe` call or the unchanged 90-second
observation deadline. Public APIs, Kit code, live assertions and deadlines were
not changed.

**Result: native FAIL; diagnostic COMPLETE.** The single
[invocation](../../target/storage-feature-split-evidence/scoped-live-20261021-030013/receipts.json)
expired at 90,001.157 ms, with no function call. All 2,711 attempted audio sends
returned successfully and matched the passive capture: 2,775,420 PCM bytes,
comprising the original prefix and 2,604 additional zero chunks. The only native
JSON messages were `setupComplete` and `sessionResumptionUpdate`; no voice
activity, transcription, content or call was observed. A real native Close
completed framing. Transparent forwarding, native observation, task joining,
socket close, observer cleanup, child reaping and process-group settlement all
succeeded. Probe cleanup took 28.445 ms. The 1,800-second supervisor did not
expire. Kit cleanup is not applicable because no Kit session ran.

The [starting snapshot](../../target/model-compatibility-20261021/gemini-continuous-native/before.json)
and [pre-live record](../../target/model-compatibility-20261021/gemini-continuous-native/pre-live.json)
preserve source, compiler, lock, environment, audio, CA and diagnostic identities.
The invocation candidate was `295ff76c5f95f715965b77d6412aab66367e7b0c71adf51d1d3d2d1aa48fbbc3`;
inputs remained unchanged through settlement. Ten offline probe controls and
fourteen runner accounting controls passed without network requests.
[Independent review](../../target/model-compatibility-20261021/gemini-continuous-native/independent-review.json)
verified exact inputs, bounded ownership, redaction and failure precedence.
Review corrected disposable diagnostic accounting/cleanup issues before the
native execution; none was a Kit production defect. The isolated Python
environment changed no workspace dependency. The native control used
`websockets` 16.1.1; downloaded package identities and hashes are retained.

An additional offline comparison with Google Python client 2.32.0 found identical
audio base64 and MIME values, using its `realtime_input`/`mime_type` spellings
instead of our documented `realtimeInput`/`mimeType`. The initial strict object
equality check failed on those field spellings and is preserved in the
[serialization evidence](../../target/model-compatibility-20261021/gemini-continuous-native/official-client-serialization.json).
The [Live reference](https://ai.google.dev/api/live) documents the spellings used
by this control; this comparison establishes no request defect or service-side
processing claim.

Continuous silence alone did not restore progress with this setup. The Rust
Kit receive path is unnecessary for this observed symptom, but shared setup,
audio interpretation and provider behavior remain unresolved. This is neither
proof of a Google defect nor qualification of the Kit interruption journey.
No production correction is justified by this capture, and no further live
execution or broader qualification was admitted. All earlier failures remain.

### Gemini paced audio candidate implemented; exact case still blocked

The next approved candidate preserves Kit production code and the exact
108,924-byte automatic-interruption inputs, including their silence tails.
The existing test owner now sends each input as 106 full 1,024-byte chunks and
one 380-byte remainder, awaiting each send and then its PCM duration (32 ms or
11.875 ms) before the existing explicit `TurnEnd`. Both sequences remain inside
the same first 90-second future. The first genuine function call still precedes
the second utterance; all cancellation, correlation, output, subsequent-progress
and cleanup assertions and deadlines are unchanged. Two logical utterances now
permit at most 214 audio messages. No background sender or extra audio is added.

This is a changed-input compatibility experiment informed by Google's
[small-chunk guidance](https://ai.google.dev/gemini-api/docs/live-api/best-practices#streaming),
not a demonstrated Kit repair. OpenClaw's merged silence-cutoff correction has
no equivalent suppression path in this Kit. Finite paced inputs also differ
from the continuous silence in that external reproduction.

The [starting snapshot](../../target/model-compatibility-20261021/gemini-paced-audio/before.json)
preserves tracked/untracked source, diagnostics, compiler, lock, environment,
audio and historical receipt identities. Existing current/legacy session
controls pass with additional exact zero-valued full and remainder chunks;
this is passing baseline evidence, not manufactured red evidence. An initial
sandbox-denied loopback bind is retained separately from the successful controls.
The existing ignored runner checks indexed native chunk hashes and cumulative
Kit offsets against exported PCM, retaining attempted, successful and observed
prefixes separately. Partial input never qualifies the live case. The observer
is unchanged.

**Validation and review.** [Focused validation](../../target/model-compatibility-20261021/gemini-paced-audio/validation.json)
passed: 40 session lifecycle tests, 258 strict-live assertion tests, Clippy,
formatting and five documentation contracts. All 31
[offline checks of the actual runner validator](../../target/model-compatibility-20261021/gemini-paced-audio/runner-validation.json)
passed, including rejection of paced input by the old whole-buffer validator.
This is diagnostic red/green evidence, not a production defect.
[Independent actual-diff review](../../target/model-compatibility-20261021/gemini-paced-audio/independent-review.json)
found no actionable issue before live execution. Rust 1.94.1, native `--locked`,
one build job and serial Cargo execution were retained. The
[source comparison](../../target/model-compatibility-20261021/gemini-paced-audio/source-comparison.json)
retains prior production validation only because production and dependency inputs
are identical; it does not claim a new full qualification pass.

**One live attempt; failed.** The
[invocation receipt](../../target/storage-feature-split-evidence/scoped-live-20261021-023321/receipts.json)
records 142 available cases and exactly one selected: **0 passed / 1 failed /
0 ignored / 141 filtered**, exit 101. The command settled in 90.484 seconds;
the 1,800-second supervisor did not expire. The original 90-second interruption
future failed before the first function call, so the second utterance,
cancellation assertions and subsequent progress were not reached.

The [complete capture summary](../../target/model-compatibility-20261021/gemini-paced-audio/live-result.json)
binds all 107 observed first-utterance chunks to the exact 108,924-byte PCM hash
`78c1e406663cc628650eddacabc457e383082be01419bc1c85130f74502abbd9`.
All 108 audio/end sends succeeded. The end-marker attempt was at 3,644 ms;
millisecond evidence records 32–35 ms between full-chunk send returns and the
next attempt, and 14 ms after the remainder. Only setup completion and session
resumption metadata arrived before Close: no native JSON followed the input
and no portable event arrived. Forwarding proves socket transmission, not
provider processing. The paced finite-input candidate did not restore progress;
it establishes neither a Kit defect nor an upstream root cause.

Kit Session close and Provider shutdown succeeded. Transparent forwarding,
framing/projection, native observation, observer transport cleanup, child
reaping and group settlement completed independently; native-tail mode never
entered. The invocation source hash
`d3863e5fcb23bfc6f9b84488827d0906dc582dac03d1ea01f4490542d5a730d4`
and artifact hash
`bd294f641ea86eb8f2f495c3779db6ae4dba9ba36c728ad4a25d0876904a89a9`
are retained with unchanged environment, CA, observer and lock identities.
No retry, extra silence, prompt/tool-mode change or escalation occurred. Broader
Provider qualification, vendors, Agent/App, crash recovery and packages remain
pending. All historical failures below retain their original status.

### Gemini explicit automatic-audio boundary implemented; live progress still blocked

The approved `SessionInput::TurnEnd` extension is implemented within the existing
Gemini owner. It preserves public types, signatures, serialization, dependencies
and crate boundaries while explicitly broadening the formerly manual-only
behavior. Automatic `TurnEnd` after accepted audio sends only
`{"realtimeInput":{"audioStreamEnd":true}}`; one private paused state is entered
after successful sending. New audio resumes that state without clearing calls
or admitting text. Idle, duplicate and text-turn endings are rejected locally.
Manual/OpenAI behavior, native terminal authority, receive deadlines and cleanup
remain unchanged. Raw audio is never implicitly flushed.

**TDD and review.** The [frozen baseline](../../target/model-compatibility-20261021/gemini-automatic-turn-end/before.json)
preserves the previous candidate, untracked inputs and historical evidence.
All [three new regressions failed](../../target/model-compatibility-20261021/gemini-automatic-turn-end/red.json)
with `InvalidState` at automatic `TurnEnd` before the implementation changed;
their owned cleanup retained supplementary fixture errors without replacing that
primary failure. The [same three passed](../../target/model-compatibility-20261021/gemini-automatic-turn-end/green.json)
after the correction. Both supported Gemini profiles exercise exact encoding,
multiple chunks without implicit boundaries, paused admission, outstanding calls,
resumed audio, real cancellation identities, late-result rejection and progress.
The existing deadline owner also proves that metadata after an explicit boundary
cannot extend receive or release shutdown admission. Existing continuous-audio,
manual, rejected-send and cleanup controls remain.

[Validation](../../target/model-compatibility-20261021/gemini-automatic-turn-end/validation.json)
passed: **40 session tests, 247 wire tests, five source contracts, five
documentation contracts**, doctests, Clippy and formatting, with Rust **1.94.1**,
native `--locked`, one build job and serial Cargo execution.
[Independent review](../../target/model-compatibility-20261021/gemini-automatic-turn-end/independent-review.json)
found no actionable findings in the actual incremental diff before execution.
The observer is byte-identical to the previous capture. Only its existing runner
adds changed-input attribution and checks marker lengths/hashes against Kit send
evidence; no diagnostic framework or lifecycle owner was added.

**One changed-input live attempt.**
[scoped-live-20261021-015527](../../target/storage-feature-split-evidence/scoped-live-20261021-015527/receipts.json)
verified **142 total cases** and ran only
`live::resources::gemini_interactive_pending_call_interruption_and_subsequent_progress`:
**0 passed, 1 failed, 0 ignored, 141 filtered**, exit 101. Both planned original
108,924-byte inputs retain their exact speech and silence bytes. The only planned
protocol additions are `TurnEnd` immediately after each Audio, inside the same
first 90-second logical-turn budget. Model, prompts, assertions, subsequent-turn,
90-second close, 180-second shutdown and 1,800-second supervision remain fixed.

The [complete correlated capture](../../target/model-compatibility-20261021/gemini-automatic-turn-end/live-result.json)
contains setup completion and resumption metadata before the first audio, then
the exact audio and 41-byte stream-end message. Both Kit sends returned success
at 11 ms; native-bound hashes match. No native JSON followed the audio or marker
before the genuine Close. The Kit observed zero portable events and the original
90-second interruption observation expired. No first function call, second audio,
cancellation, late-result rejection or subsequent-progress assertion was reached.
The command took 90.557 seconds; its supervisor did not time out.

Kit close and Provider shutdown succeeded. Transparent forwarding, native
framing/projection, complete native observation and observer transport cleanup
succeeded independently. Tail mode was enabled but never entered; no tail or
overflow was recorded. The observer exited zero and its direct child and process
group settled; the test invocation also settled without cleanup errors.
Successful socket forwarding is not proof of provider processing or Kit delivery.

Execution source was `17dd7f3bfdf653e718e6c9e9d3f8113d4fc27530f16ec20ecf317aee2b7867cf`;
artifact SHA-256 was `b9c46e581111d6f74bbbf1cbe412f0c093248c36d895c0f9b278278c3a9e632c`.
Source and environment identities were unchanged through settlement. The lockfile
remained `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
The explicit boundary did not resolve this attempt. This establishes neither a
new Kit output-loss defect nor the provider-internal cause of either timeout.
Historical results remain failed under their original inputs. No retry, broader
qualification, capacity workload, commit, push or merge followed.

### Gemini automatic-audio capture complete; initial progress remains blocked

The single authorized automatic-interruption recheck produced a complete native
capture and a failed Kit journey. This distinguishes the diagnostic correction
from the unresolved service interaction; no Kit production change was justified.
The prior failures, fixed-silence experiment, ordinary-session pass and all
qualification receipts below retain their original status and inputs.

**Diagnostic TDD and review.** The existing observer incorrectly required a text
turn for projection completeness, although this exact journey sends only audio.
The [red control](../../target/model-compatibility-20261021/gemini-automatic-capture/red.json)
replays a fully framed audio-only connection and exposes that rejection. An
initial local-control invocation was blocked by sandbox loopback permissions;
its log is retained separately and is not behavioral red evidence. The correction
extends the existing observer: a nonempty validated audio blob can establish
input evidence alongside an existing text turn. It records only existing encoded
data/MIME hashes and lengths plus the actual audio count. Genuine setup and
setup completion, all framing/projection/error bounds, native-tail attribution,
original failure precedence and cleanup requirements remain. No second input,
tool call or successful answer is needed for complete observation of a failure.

The [green self-checks](../../target/model-compatibility-20261021/gemini-automatic-capture/green.json)
cover audio-only input, missing/malformed audio, unchanged text, real verified
local TLS late frames, forwarding, redaction, native-tail limits and settlement.
Five documentation contracts and diff checks pass.
[Independent actual-diff review](../../target/model-compatibility-20261021/gemini-automatic-capture/independent-review.json)
found no actionable findings before live execution. The existing invocation
runner restores the already-reviewed passive observer owner and checks exact
input identity against actual Kit submission progress. No owner, fixture
framework, production hook, API, dependency or live assertion was added.

**One exact execution.**
[scoped-live-20261021-013038](../../target/storage-feature-split-evidence/scoped-live-20261021-013038/receipts.json)
verified **142 total cases**, selected only
`live::resources::gemini_interactive_pending_call_interruption_and_subsequent_progress`,
and recorded **0 passed, 1 failed, 0 ignored, 141 filtered**, exit 101. It used
`models/gemini-3.8-live` and the unchanged prompts, automatic VAD, `BLOCKING`
function declaration and two planned audio inputs. Each input retains the
original 76,924-byte PCM plus exactly 32,000 zero bytes. Only the first input was
reached. The canonical encoded audio/MIME hashes match the observed native-bound
message and the Kit's actual 108,924-byte submission; sending returned success
after 11 ms. No text request, stream-end signal or additional generation was sent.

The [correlated result](../../target/model-compatibility-20261021/gemini-automatic-capture/live-result.json)
shows native `setupComplete` and `sessionResumptionUpdate`, both preceding the
audio submission. No native JSON message followed that audio before the genuine
native WebSocket Close: no activity metadata, transcription, tool call, output,
cancellation or native error was observed. The Kit recorded no portable event
and its existing 90-second interruption observation expired. The second audio,
held-call cancellation, late-result rejection and subsequent-progress assertions
were never reached. Command elapsed time was **90.906 seconds**; the unchanged
1,800-second supervisor did not time out. Logical-turn/close bounds remain
90 seconds and Provider shutdown remains 180 seconds.

**Independent outcomes.** Consuming Session close and Provider shutdown succeeded.
Native framing/projection, transparent socket forwarding and transport cleanup
all completed. Native-tail mode was enabled but never entered; no failed or
intentionally unforwarded bytes were recorded. The observer exited 0, its direct
child was reaped and its process group settled. The test process also settled
without cleanup errors. These successes do not change the Kit assertion failure,
and socket forwarding proves neither provider processing nor Kit application
delivery.

**Classification and remaining boundary.** There was no required native output
at the captured Kit boundary to lose or reject. This does not establish why
Gemini made no progress, nor does it prove an interruption defect: no pending
call existed. Google's [automatic-VAD guidance](https://ai.google.dev/gemini-api/docs/live-api/capabilities#automatic-vad)
recommends `audioStreamEnd` when continuous input pauses; its omission remains an
unproven cause. Portable `Audio` remains a raw chunk and `TurnEnd` remains a manual
boundary. No implicit flush, timer, input tuning, prompt change, deadline increase
or speculative Kit correction was applied. Provider-side investigation or an
explicit automatic-stream-boundary contract decision is the remaining next step;
no further live execution was authorized or performed.

Execution candidate SHA256 was
`92074d07f901aece8e8932cde8dc58cdc751452e625d07d092dd8683d571a925`,
unchanged through settlement. The executable remained
`d8babc903014e240eaa8ae166ce9da0e74e0f51a31c9eeec2b6c6ec8bbbbe06b`,
the same artifact as the preceding voice-activity qualification. Cargo.lock
remained `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Rust 1.94.1, native `--locked`, one build job and serial Cargo execution were used.
The [explicit source comparison](../../target/model-compatibility-20261021/gemini-automatic-capture/prelive-source-comparison.json)
retains prior Rust validation only for unchanged inputs, not as a new execution.
All 435 frozen historical files remained unchanged before the attempt. Final
comparison and review accompany this report update.

Five of the ten follow-up cases retain targeted passing evidence; five remain
unqualified: three OpenAI audio cases, Gemini automatic interruption and the
mixed-row OpenAI batch. Broader Provider, vendor, Agent/App, crash and package
gates remain pending. No retry, unrelated live suite, capacity workload, commit,
push or merge occurred.

### Gemini voice-activity metadata corrected; exact ordinary session passed

The retained standalone `voiceActivity` failure is a demonstrated Kit adaptation
defect: Google's [server-message schema](https://ai.google.dev/api/live#BidiGenerateContentServerMessage)
places that advisory field outside the message-type union. The narrowly approved
correction extends the existing `GeminiSession` receiver. After real setup it
accepts a zero-branch envelope containing an object-valued `voiceActivity` and
optionally `usageMetadata`; a normal branch may also carry this metadata. Real
usage/content/tool/cancellation/terminal processing continues unchanged. The
outer shape is checked, while inner VAD fields remain unconsumed. No event,
turn/call mutation, timer, owner, DTO, dependency or public API is added. Native
errors and conflicting branches retain precedence. This is distinct from the
prior exact-empty interoperability exception, which remains intact.

**TDD.** [Original red evidence](../../target/model-compatibility-20261021/gemini-voice-activity/red.json)
records baseline production rejecting the captured audio-turn envelope before
real progress and rejecting repeated metadata before its expected absolute
receive timeout. A third new test initially used noncanonical key order in an
exact diagnostic expectation; only that new input was corrected. The subsequent
[exact negative red](../../target/model-compatibility-20261021/gemini-voice-activity/red-negative.json)
proves the baseline also emitted content despite malformed nonobject voice
metadata. The [first post-fix run](../../target/model-compatibility-20261021/gemini-voice-activity/first-postfix.json)
passed progress and deadline controls, then exposed an incorrect newly authored
native-error expectation. It was aligned with the preexisting `ResourceExhausted`
classification, without changing production errors or any preexisting assertion.
All original logs, supplementary peer failures and source snapshots remain.
These two test-development mistakes are not additional Kit defects.

**Focused validation.** The corrected candidate passes
[37 session, 247 wire and five source-contract tests](../../target/model-compatibility-20261021/gemini-voice-activity/green-tests.json),
five documentation contracts, Provider all-target/all-feature Clippy and
formatting. Existing empty-object controls, exact body/context assertions,
function correlation, original receive deadlines and cleanup bounds remain.
Two existing test bodies moved to associated methods on `InteractiveServer`
for reuse; no fixture framework or production diagnostic hook was added.
Rust 1.94.1, native `--locked`, one build job and serial Cargo execution were
used. These scoped checks are not a complete Provider qualification.

**Review and one live execution.** [Independent actual-diff review](../../target/model-compatibility-20261021/gemini-voice-activity/independent-review.json)
found no actionable issues and confirmed existing assertions, ownership, error
precedence and deadlines. The runner changed only its receipt description.
[scoped-live-20261021-011420](../../target/storage-feature-split-evidence/scoped-live-20261021-011420/receipts.json)
verified all **142 cases**, selected exactly
`live::resources::gemini_interactive_session`, and ran it once: **1 passed,
0 failed, 0 ignored, 141 filtered**, exit 0. The unchanged direct journey used
the official endpoint and exact `models/gemini-3.8-live`. Its command completed
in **13.301 seconds** (test body 13.20 seconds), with no timeout inside the
unchanged 1,800-second supervisor. The 90-second turn/close bounds and 180-second
Provider shutdown bound were unchanged. No observer, extra native request,
model substitution, prompt change or retry was used.

The [Kit outcome and cleanup summary](../../target/model-compatibility-20261021/gemini-voice-activity/live-result.json)
records both original turns passing. The first had one correlated function call,
byte-exact returned-value transcription, 424,800 audio bytes and actual completion.
The audio-input turn had required input transcription, exact expected output
transcription, 87,842 audio bytes and actual completion. Consuming Session close
and Provider shutdown succeeded. Every invocation-owned child/group settled
without cleanup errors. Process settlement and Kit cleanup are independent
checks; neither substitutes for the behavioral assertions.

Candidate SHA256 `9697a60cb80851bea67ad21e978e5da3ce34f51fbc876f3ea3efed713559bd95`
was unchanged through settlement. Executable SHA256 was
`d8babc903014e240eaa8ae166ce9da0e74e0f51a31c9eeec2b6c6ec8bbbbe06b`;
Cargo.lock remains
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
The run used Rust 1.94.1 and APFS. The authoritative environment remained
byte-identical. This outcome/report update is documentation-only after the
recorded execution; production and test inputs remain identical. The direct
run captured no native wire frames, so it does not identify which advisory
frames occurred or explain why Google previously emitted empty objects.

Five of the ten follow-up cases now have targeted passing evidence; five remain
unqualified. This source-bound case pass is not a complete Provider verifier or
a claim of statistical reliability. All historical results below retain their
original meaning. Automatic Gemini
audio interruption, other Provider executions, batches, full qualification,
vendor finalization, Agent/App, crash and package gates remain outside this
repair. No commit, push, merge or capacity workload is authorized.

### Gemini post-setup empty-object exception implemented; next metadata boundary blocked

The explicitly approved correction accepts only an exact empty JSON object
after actual setup completion. It changes five lines inside the existing
`GeminiSession` owner, producing no event, mutating no turn/call state and
preserving the receive loop's original absolute deadline. The handshake still
requires `setupComplete`. This is a documented interoperability exception based
on the retained native capture and the pinned Google client receive paths, not a
claim that Google's API reference defines empty objects as keepalives. Only the
specific post-setup empty-rejection expectation was replaced; public APIs,
dependencies, all live inputs/assertions and existing deadlines remain unchanged.

**TDD and review.** The [red receipt](../../target/model-compatibility-20261021/gemini-empty-object/red.json)
records **1 passed / 2 failed** with production hash-identical to the frozen
baseline. The original branch guard prevented both real tool progress and the
expected timeout under continuous empty traffic. Test-peer cleanup assertions
also failed after that primary rejection; those supplementary failures remain
in the original log. The correction passed **34 session tests**, including exact
text/binary empties, real tool/result correlation, authoritative completion,
setup/invalid-envelope rejection, continuous-empty timeout and shutdown admission.
The new deterministic timeout case uses its own two-second request budget;
no existing timeout was changed. **247 wire tests, five source contracts, five
documentation contracts, formatting, Clippy and diff checks passed**. These
focused gates are not the full Provider verifier.
[Independent actual-diff review](../../target/model-compatibility-20261021/gemini-empty-object/independent-review.json)
found no actionable issues and verified that no owner, type, dependency or
public API was added. The reviewed runner reuses the existing invocation owner
and removes the diagnostic relay for the direct Kit recheck.

**One live execution.**
[scoped-live-20261021-005337](../../target/storage-feature-split-evidence/scoped-live-20261021-005337/receipts.json)
verified the full **142-case** inventory and exactly one selected
`live::resources::gemini_interactive_session`: **0 passed, 1 failed, 0 ignored,
141 filtered**, exit 101. It ran once against the official Google endpoint with
`models/gemini-3.8-live`, Rust 1.94.1, native `--locked`, one Cargo/build job and
APFS. The command took **13.101 seconds**, with no timeout inside the unchanged
1,800-second supervisor. Live source/settings/prompts/assertions were unchanged;
no observer, proxy, extra generation or retry was used.

Candidate SHA256 `fe052f1cc647ab9161c89c5ad767657f4aaa66d7cbf2f2e3cf68090e3afcc09c`
was unchanged through settlement. Executable SHA256 was
`2f3bdf3bde97f308b0eed5868dc1b2dec9fd2521e4b878953033fd69768db358`;
Cargo.lock remained
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
The source environment was only sourced and remained byte-identical. After live
settlement, final assertion review restored the prior exact
`outputAudioTranscription == {}` setup check inside the reused deterministic
journey. That exact local case, formatting and its Clippy target passed again;
independent review confirmed only the three assertion lines changed. Production,
live-case inputs and the runner remained identical to the live receipt. This
post-live test-only delta is recorded separately and did not trigger another
live execution.

**Distinct result and next blocker.** The first function turn passed its exact
assertions: one correlated call, exact returned-value transcription, native
audio and actual completion. During the subsequent audio turn, the Kit rejected
`{"voiceActivity":{"audioOffset":"0s","type":"ACTIVITY_START"}}`
with the existing exactly-one-branch error. The untruncated Kit diagnostic and
observation export establish this separate normalization boundary;
[Google documents `voiceActivity`](https://ai.google.dev/api/live#VoiceActivity)
outside the message union. The exact-empty exception intentionally does not
admit this nonempty envelope. Its validation/mapping needs a separately scoped
correction; no broad branch-count relaxation or second live execution occurred.
This direct run did not capture native wire frames and does not prove whether
empty objects occurred in this particular run.

[Kit observations and cleanup](../../target/model-compatibility-20261021/gemini-empty-object/live-result.json)
record successful consuming Session close and Provider shutdown. Invocation
process groups settled with no cleanup errors. These do not convert the failed
audio turn into a pass. The complete case remains unqualified, so four targeted
passes and six unqualified cases remain. Automatic Gemini interruption, OpenAI,
batches, full Provider qualification, vendor finalization, Agent/App, crash and
package gates were not run. All historical failures and native-tail limitations
below remain intact. No commit, push, merge or capacity workload occurred.

### Gemini native-tail observation completed; qualification remains blocked

The explicitly approved diagnostic was implemented in the existing ignored
observer, reviewed and executed **once**. It changes no Kit production code,
models, prompts, assertions, generation counts or deadlines. Unlike the earlier
transparent captures, it records native frames after a qualifying failed client
write without claiming their delivery. Historical captures remain incomplete
under their original rules. Four targeted passes and six unqualified cases
remain; this diagnostic does not admit broader qualification.

**TDD and independent review.** The preserved
[original red](../../target/model-compatibility-20261021/gemini-native-tail/native-tail-red.json)
demonstrates loss of the failed forwarding chunk and fragmented message. A
second [observer-only red](../../target/model-compatibility-20261021/gemini-native-tail/native-tail-reserve-red.json)
exposed a WebSocket FIN flag consuming the protected final-status allowance.
Both have hash-verified source snapshots. Two intervening mistakes in new local
test assertions remain separate development failures, not Kit or native defects.
The [final local suite](../../target/model-compatibility-20261021/gemini-native-tail/final-local-receipt.json)
passed all existing projection/forwarding controls, including verified loopback
TLS, failed-chunk preservation, three byte regions, fragmented-message data
totals, bounds, deadline/cancellation, redaction, error precedence and cleanup.
These are synthetic stream/local-service observations, not Google execution
evidence. [Independent actual-diff review](../../target/model-compatibility-20261021/gemini-native-tail/independent-review.json)
found no actionable issues before live admission.

**Frozen execution.**
[scoped-live-20261021-003404](../../target/storage-feature-split-evidence/scoped-live-20261021-003404/receipts.json)
verified the full 142-case inventory and exactly one selected
`live::resources::gemini_interactive_session`: **0 passed, 1 failed, 0 ignored,
141 filtered**, exit 101, no timeout. The command took 1.110 seconds inside the
unchanged 1,800-second supervisor bound. It used `models/gemini-3.8-live`, Rust
1.94.1, native `--locked`, serial Cargo/one build job and APFS. The credential-free
Google TLS preflight passed with certificate/hostname verification and no key
logging. Readiness used the observer's record rather than a probe connection.
The private loopback override left the authoritative environment byte-identical.

Candidate SHA256 `b1457f9578f350f0036200cc0633e9dc4dfc8173a048154f94be8d9f52435c68`
was unchanged through settlement. Executable SHA256 remained
`84ed2f1fd54d3e0a336c0e749ff18cf53ac67d11167c0cd8c3b46ba603e8a9a8`;
observer SHA256 was `ce2b30328ce5305a363857bc8b35666ffea701b0f16202df447e342ae6132514`.
The receipt retains the lockfile, trust bundle, Python, compiler, self-check and
runner identities. Subsequent report edits do not relabel the executed source.

**Separate results.** The native stream contained `setupComplete`, resumption
metadata, an empty top-level object, empty nested `serverContent`, another empty
top-level object, a tool call and a real Close. The Kit rejected the first `{}`
as `InvalidResponse(ProtocolViolation)` during `SessionReceive`, before accepting
a function call. Its session close and Provider shutdown both succeeded.
[Redacted accounting](../../target/model-compatibility-20261021/gemini-native-tail/capture-summary.json)
reconciles all seven native frames and 368 post-upgrade wire bytes:

| Native wire region | Bytes | Meaning |
| --- | ---: | --- |
| Successful write/drain prefix | 186 | Socket forwarding only; Kit consumption is not established |
| Failed drain chunk containing the tool call | 178 | Delivery uncertain; no write retry |
| Subsequent native Close | 4 | Intentionally not forwarded |

The tail therefore contained 182 bytes across two frames. Native framing and
projection reached the real Close without overflow or evidence loss:
`native_capture_complete=true`. The primary error remains `ConnectionResetError`
in native-to-client **drain**. Later client `wait_closed` reported
`BrokenPipeError`, errno 32. Transparent forwarding and observer transport cleanup
remain unsuccessful; observer exit was 1 and overall invocation `INCOMPLETE`.
The observer direct child was reaped and its process group settled, independently
of the transport error. The later tool call neither repairs the earlier invalid
message nor proves Kit delivery or a successful turn. No new Kit defect is
demonstrated, and no retry followed.

**Retained validation and exclusions.** The
[source comparison](../../target/model-compatibility-20261021/gemini-native-tail/prelive-source-comparison.json)
verified all 64 frozen Rust files, the lockfile, executable, environment and 232
historical artifacts unchanged. The earlier 247 wire tests, 32 session tests and
Clippy receipts remain source-bound evidence, not new executions or proof of the
changed Python diagnostic. Documentation contracts and diff checks passed.
Automatic Gemini audio, OpenAI generation, batches, the full Provider verifier,
vendor finalization, Agent/App checks and packages remain pending/outside this
pass. No commits, pushes, merges or capacity workloads occurred.

### Gemini settlement investigation: no additional repair established

The next investigation preserves the previous incomplete capture and finds no
additional demonstrated Kit or observer defect. **No live case was rerun, and
the complete passive capture remains blocked.** The native `{}` rejection,
later `ConnectionResetError`/`BrokenPipeError`, successful Kit cleanup attempts
and process reaping retain their original classifications. Four targeted passes
and six unqualified cases remain; no downstream gate is admitted.

**Ownership boundary.**
[SessionSocket::close](../../providers-kit/src/core/session/socket.rs) calls the
pinned `tokio-tungstenite` 0.29 sink close under the existing deadline. Its
`poll_close` queues/flushes the Close frame without reading the peer's Close.
[Gemini's then-current consuming close](../../target/model-compatibility-20261021/gemini-audio-unsupported/baseline/providers-kit/src/providers/gemini/session/mod.rs)
then drops the unique transport. The
[portable contract](../../providers-kit/docs/PORTABLE_API.md#configuration-and-lifecycle)
promises attempted native socket cleanup, not a peer-confirmed closing handshake
or delivery of late application messages. This explains how successful local
Kit cleanup can coexist with an incomplete passive capture; it does not identify
the exact read/write instruction responsible for either historical OS error.

[RFC 6455 section 5.5.1](https://www.rfc-editor.org/rfc/rfc6455#section-5.5.1)
does not guarantee continued data processing after sending Close. The RFC also
distinguishes connection closure from clean handshake completion. Its separate
failure algorithm does not, by itself, prove that the application's `{}`
rejection mandates that algorithm. The current source and contract do not
justify imposing a stronger production close obligation to obtain this capture.
[Source investigation](../../target/model-compatibility-20261020/gemini-settlement-investigation/source-investigation.json).

**Local evidence and limits.** The existing verified loopback TLS control still
forwarded all late frames when the caller retained its read half. One bounded
full-client-close variation was investigated without Google requests. It reached
the existing five-second relay/cleanup deadline, rather than reproducing the
live reset errors. Its staged native remainder waited for destination closure;
the timeout could itself release that wait. This is not causal evidence of a new
defect. The exploratory addition was removed from the self-check owner, while
its exact source, receipt and limitations remain retained. No tuning, repeated
traffic, new fixture owner or unchanged live retry followed.
[Local control receipt](../../target/model-compatibility-20261020/gemini-settlement-investigation/closed-client-control.json).

A byte-preserving relay cannot guarantee delivery to an already-closed receiving
socket. Continuing to collect native frames while discarding failed client writes
would be a different diagnostic, not successful transparent forwarding. Missing
peer Close and failed forwarding therefore remain incomplete, rather than being
suppressed. The observer, Kit, runner, executable, environment, assertions and
deadlines remain unchanged in this pass. OpenAI's private unsent packet and its
native-empty-output failure are unchanged. Provider-side investigation of the
retained empty Gemini message is still useful; another identical relay attempt
has no demonstrated repair to qualify. No commit, push, merge or capacity run
occurred.

### Gemini close ownership repaired; the one-shot capture remains incomplete

This bounded follow-up fixes the ignored observer's demonstrated premature TLS
closure. It changes no Rust production code, live inputs, assertions, models,
generation counts or Kit deadlines. **The single unchanged Gemini case failed,
and its capture remains `INCOMPLETE`; no retry followed.** OpenAI investigation
used retained evidence only, with no generation or other provider request.
All prior results, including the unspecified historical `SSLError`, remain
unchanged. Four targeted passes and six unqualified cases remain; broader
qualification is still blocked.

**TDD and review.** The new event-coordinated local TLS regression failed against
observer SHA256 `e51f3359658f3f321303faa347d4673cad43b6ed236e0b957a3f5c83ed9bcdcb`:
TLS closed before the peer WebSocket Close was forwarded. Its client retained
its read half after write-half EOF; late fragmented native frames exposed the
premature closure without sleeps or dependence on an OpenSSL error string.
The connection owner now closes transports only after directional settlement,
within the existing shared five-second bound, shortened by parent shutdown.
Complete capture requires both real Close boundaries, complete framing,
successful forwarding and cleanup. SSL error metadata remains bounded and
redacted. Codex mode, trust validation and byte forwarding are unchanged.
[Red receipt](../../target/model-compatibility-20261020/gemini-close-repair/local-tls-red.json),
[full local green receipt](../../target/model-compatibility-20261020/gemini-close-repair/full-selfchecks-green.json)
and [independent actual-diff review](../../target/model-compatibility-20261020/gemini-close-repair/independent-review.json)
cover close ordering, late frames, partial/trailing data, missing peer Close,
forwarding/SSL failures, cancellation, parent deadline and owned cleanup. The
full local self-check completed in 15.358 seconds. Temporary certificates were
removed; these controls establish orchestration behavior, not Google behavior.
The repair does not retrospectively identify the earlier SSL exception's reason.

**Frozen live invocation.**
[scoped-live-20261020-233317](../../target/storage-feature-split-evidence/scoped-live-20261020-233317/receipts.json)
ran only `live::resources::gemini_interactive_session` after review. Source
`6ba9458b89cfde9ebd9688edb75d19aa960dcb8a7dc14b06cde5bfd7dc71f2f7` was unchanged
through settlement; later report edits do not relabel that identity. The
executable SHA256 remained
`84ed2f1fd54d3e0a336c0e749ff18cf53ac67d11167c0cd8c3b46ba603e8a9a8`.
Rust 1.94.1, native `--locked`, serial Cargo, one build job, debug info zero,
assertions/overflow checks enabled, macOS/APFS and 91,590,299,648 available bytes
were recorded. Build preceded timing; the full inventory contained 142 cases,
with exactly one executed, zero ignored and 141 filtered. The exact case exited
101 in 0.706 seconds of process supervision without timeout. Google-only,
credential-free TLS preflight passed with the frozen 128-CA bundle and verified
hostname; the authoritative environment remained byte-identical.

**Observed failure and limits.** Native Binary messages contained
`setupComplete`, a resumption update, `{}`, then empty `serverContent`. The
complete outbound setup and text-turn hashes match the preceding attempt. The
Kit again rejected the top-level empty object, rather than treating it as a
keepalive. After the observer recorded the client's actual Close, it also
observed another `{}` and one native tool call. These late frames are additional
native evidence, not delivered portable calls or a successful completed turn.
Cross-direction observer timestamps do not establish native server causality.

This execution recorded no `SSLError`. It instead ended with a native-direction
`ConnectionResetError` during relay and a client-side `BrokenPipeError` during
`wait_closed`. The peer Close was not observed; framing and connection cleanup
were incomplete. These records do not pinpoint the exact failing read/write
instruction or prove exhaustive native output. The new bounded error evidence
is preserved without another observer change or execution.
[Redacted native/Kit comparison](../../target/storage-feature-split-evidence/scoped-live-20261020-233317/gemini-session/native-sdk-comparison.json).
Kit Session close and Provider shutdown succeeded separately. The observer
exited 1 and was reaped; its process group settled with no parent cleanup error.
Those facts do not turn failed observer cleanup into successful TLS settlement.

**OpenAI offline investigation.** The previous complete 848-byte native response
still exactly matches the Kit's untruncated error body: empty assistant text,
`stop`, zero completion tokens, no refusal and no function call. Request,
settings, ordered content and valid WAV hashes match the earlier attempt.
Official audio-chat documentation supports this endpoint, audio input and text
output selection; no request-rendering or output-loss defect is demonstrated.
Existing deterministic controls already reject empty `stop` while preserving
empty filtered outcomes. A private, unsent support packet references the retained
server request ID, completion identity/time, exact model, request/audio hashes,
WAV properties, native response and equality proof. The
[redacted audit](../../target/model-compatibility-20261020/gemini-close-repair/openai/audit.json)
contains no credentials or raw request identifiers. Provider-side investigation
of that existing request is the remaining useful boundary; no unchanged OpenAI
request, prompt change, synthetic answer or production correction was made.

**Validation and outstanding gates.** All 247 wire tests and 32 session tests,
Clippy, formatting, five documentation-contract checks and diff checks passed.
[Focused receipt](../../target/model-compatibility-20261020/gemini-close-repair/focused-validation.json).
The sandbox-denied loopback attempt is retained separately, not counted as
behavioral red evidence. Automatic Gemini audio remains blocked and unexecuted;
OpenAI audio streaming/function, mixed-row batches, the full Provider verifier,
vendor finalization, Agent/App live gates, crash recovery and packages remain
pending. No commit, push, merge or capacity workload occurred.

### Observer boundary capture: native empty output, Gemini capture incomplete

The next approved pass repaired OpenAI's ignored diagnostic usage parser and
extended the existing ignored WebSocket observer for one ordinary Gemini session.
**Both unchanged exact cases failed. No Kit production correction was justified.**
The invocation is `INCOMPLETE`: OpenAI's capture completed, while Gemini's later
observer TLS-close error prevents a complete capture claim. Four earlier targeted
passes and six unqualified cases remain; neither this receipt nor those targeted
passes replace the original full **128 passed / 14 failed** result.

[Invocation receipt](../../target/storage-feature-split-evidence/scoped-live-20261020-225451/receipts.json)
records source `3485d01e175fd67662b45dade0d8ee3c3868078b6acfd97a6b2c4a04514d6deb`
unchanged through final settlement, executable
`84ed2f1fd54d3e0a336c0e749ff18cf53ac67d11167c0cd8c3b46ba603e8a9a8`, and lock
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Build preceded execution using Rust 1.94.1, `--locked`, one Cargo/build job,
the existing unoptimized debug-zero profile with assertions and overflow checks,
and macOS/APFS. The full inventory contained 142 cases; each execution selected
one, ignored zero and filtered 141. Test inputs, models, prompts, generation
counts, assertions and deadlines were unchanged; the diagnostic transport routes
were explicitly changed to loopback observers. The
[freeze](../../target/model-compatibility-20261020/observer-boundary-capture/before.json)
preserves prior tracked/untracked inputs, observer sources, environment/CA hashes
and artifact identity. Later report edits do not change the execution receipt.

**Demonstrated diagnostic defect repaired through TDD.** The old OpenAI observer
rejected documented `text_tokens`, `image_tokens`, `cache_write_tokens` and null
detail counters. The repaired projection preserves missing/null/zero, ignores
additive unknown values without logging their names, and keeps validated content
facts when known usage is malformed. Content, usage and overall completeness are
distinct; malformed usage still fails capture qualification. Focused red/green
checks cover that boundary, byte forwarding, status, bounds, redaction and cleanup.
The exact usage field that broke the earlier `222146` capture remains unknown.

**OpenAI unary audio: complete native-output failure evidence.** One unchanged
request reached the official Chat Completions endpoint with the same prompt,
settings and 115,428-byte WAV identity as the prior attempt. HTTP 200 returned
an empty assistant content string, `finish_reason: stop`, 92 input tokens and
zero completion tokens. The 848-byte native response SHA256
`76ba7d29438043aa3ef72ee77e6609b1872a7784a55dc91426d175d70af220ab`
matches the Kit's untruncated error body. The Kit rejected the empty message with
`Generate / Normalize / InvalidResponse(InvalidField { field: "message" })`;
there was no nonempty native answer for it to lose. The test failed in 1.67 seconds,
before its exact-answer assertion. This establishes the current empty-output
boundary, not the cause of earlier nonempty formatting failures or which upstream
component produced the empty answer. No prompt, model, assertion or production
change followed. [Comparison](../../target/storage-feature-split-evidence/scoped-live-20261020-225451/audio-unary/native-sdk-comparison.json).

**Gemini: observed native empty object, incomplete connection evidence.** Verified
TLS and the HTTP 101 upgrade preceded an unchanged AUDIO/manual-activity setup,
BLOCKING function declaration and complete `clientContent` text turn. Native
Binary messages were observed in this order: `setupComplete`,
`sessionResumptionUpdate`, a three-byte empty JSON object, and `serverContent: {}`.
The empty object's wire payload hash
`ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356`
matches `{}\n`; the Kit retained the semantically identical `{}` and its existing
exactly-one-message-branch failure. No portable event or function call preceded
the failure in 0.78 seconds. Cross-direction observation times do not prove
upstream processing order or explain why the service sent that object.

The observer subsequently recorded primary, supplementary and cleanup `SSLError`
categories after forwarding client EOF. Its exact SSL reason was not retained;
it must not be inferred from the exception class. `projection_complete` was true
but `cleanup_complete` and final `complete` were false. The observed prefix is
useful evidence, **not a complete Gemini capture or successful settlement**.
No observer amendment or another live attempt followed this incomplete result.
[Capture comparison and limitation](../../target/storage-feature-split-evidence/scoped-live-20261020-225451/gemini-session/native-sdk-comparison.json).

**Validation and ownership remain separate.** Observer regressions and independent
actual-diff reviews passed before execution, including fragmented Binary frames,
strict UTF-8, Host-only handshake rewriting, unchanged frame forwarding, verified
TLS, redaction and bounded close/shutdown controls. Local controls did not predict
the real TLS-close failure. Provider's 247 wire tests, 32 session tests, Clippy,
formatting and five documentation-contract tests passed. An initial sandbox
loopback-denial run is retained separately and is not behavioral red evidence.
Kit cleanup reported no supplementary failures; Gemini explicitly completed
Session close and Provider shutdown. The HTTP observer joined its connections,
the WebSocket process was reaped and its group settled, and all test process
groups settled. These facts do not erase Gemini's observer cleanup error.
The original environment remained byte-identical, and no remote batch resources
were created. [Evidence and reviews](../../target/model-compatibility-20261020/observer-boundary-capture/).

Automatic Gemini audio remains blocked without another run or Audio/TurnEnd
change. Audio streaming/function cases, mixed-row batch, full Provider
qualification, vendor finalization, Agent/App live gates, crash recovery and
packaging remain pending. No commits, pushes, merges or capacity workloads ran.

### Verified-boundary repair: four targeted passes, six cases unqualified

The next approved bounded pass repaired the observer's TLS setup and one
source-proven Gemini Kit mapping defect. It executed exactly three cases once:
**Anthropic passed; Gemini ordinary session and OpenAI unary audio failed**.
The invocation remains `INCOMPLETE` because OpenAI's response projection failed.
Combined with the earlier three targeted passes, four of the ten cases now have
passing targeted evidence; **six remain unqualified**. These separate receipts
do not replace the original full **128 passed / 14 failed** Provider result.

[Invocation receipt](../../target/storage-feature-split-evidence/scoped-live-20261020-222146/receipts.json)
records source `b311cc25a15d54fda9420015e836adbabdad78708f48d054c740285bad7924e5`,
base `af28cdde`, unchanged lock SHA256
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`, and executable
`84ed2f1fd54d3e0a336c0e749ff18cf53ac67d11167c0cd8c3b46ba603e8a9a8`.
Rust 1.94.1, native `--locked`, one build job, the existing unoptimized low-memory
test profile, macOS/APFS and 92,094,820,352 available bytes were recorded.
Compilation preceded execution. The full inventory contained 142 cases; each
exact execution selected one, ignored zero and filtered 141. Source identities
remained unchanged through every command. The
[pre-edit freeze](../../target/model-compatibility-20261020/verified-boundary-repair/before.json)
includes tracked and untracked source, the prior diff, diagnostic inputs,
environment hash and CA identity. Later report edits do not alter that receipt.

**TLS prerequisite repaired, without a Kit change.** The observer parent used
invocation-only `SSL_CERT_FILE=/etc/ssl/cert.pem`; the system trust store and
dependencies were unchanged. Its hashed bundle loaded 128 CAs with certificate
and hostname verification enabled and key logging disabled. The frozen parent
Python 3.11.9/OpenSSL 3.0.13 interpreter performed credential-free TLS-only
connections to both official hosts, with 10-second connection and 30-second
process bounds. Both negotiated TLS 1.3, sent no HTTP request and closed.
Certificate hashes, bundle hash and separate process settlement are retained.
This fixes the previous diagnostic prerequisite failure; it does not explain
the original Anthropic missing-URL failure.

**Gemini complete Text mapping corrected through TDD.** Both supported exact
profiles now send one `clientContent` user turn with `turnComplete: true`, matching
the existing public complete-Text contract. Four new controls cover the two
profiles and Manual/Automatic modes, exact text, no extra native message,
active/pending-call rejection, function correlation, subsequent progress and
bounded consuming cleanup. All four failed against the old `realtimeInput.text`
envelope, then the complete **32-case session suite passed**. The initial sandbox
loopback denial is retained separately and is not behavioral red evidence.
Only the existing Text branch changed in production; state advances after the
successful send, and Audio, TurnEnd, idle fences and deadlines are unchanged.
[Red](../../target/model-compatibility-20261020/verified-boundary-repair/gemini-red-receipt.json)
and [green](../../target/model-compatibility-20261020/verified-boundary-repair/gemini-green-receipt.json)
receipts bind the production source and executable. The fix is independently
justified; the following live failure does not establish it as the cause of
the earlier malformed native message.

| Exact case | Result and evidence boundary |
| --- | --- |
| Anthropic unreachable-URL streaming | Passed unchanged in 7.43 seconds. One HTTP 200 stream had complete framing, 15 input-JSON fragments assembling the exact requested URL, matching tool/result identities, `url_not_accessible`, and `end_turn`/`message_stop`. Kit URL and typed failed hosted result matched. No Anthropic production correction was needed; the original missing-URL failure did not reproduce. |
| Gemini ordinary session | Failed again in 0.87 seconds on the exact untruncated two-byte `{}` message. The Text send returned successfully, but no portable event, function call, transcript or audio preceded the protocol rejection. The Kit correctly retained the invalid-message failure. The complete-Text correction did not resolve this native-output blocker. |
| OpenAI unary audio | Failed exact output again: 31 bytes instead of 21. Request capture confirms the official Chat Completions route, `gpt-audio-1.5`, text modality, no JSON-format request, unchanged prompt/settings and the exact valid WAV hash. One HTTP 200 response's 879 bytes were forwarded. However, diagnostic usage validation failed before projecting assistant text, so native-to-Kit text parity is **unproven**. No Kit audio correction or further attempt is justified. |

The Anthropic request/native/Kit URL SHA256 was
`70ccab092476e26c7068cc62fbb86be6545f1c8636a98371c50d47122f5533e6`.
[Correlated comparison](../../target/storage-feature-split-evidence/scoped-live-20261020-222146/anthropic-url/native-sdk-comparison.json).
OpenAI's decoded WAV SHA256 was
`d0fa059c4246c45900f63628eb40cf0b921d057e50200f44fa41bbbe1afb7db3`.
The observer's closed nested usage projection was stricter than the Kit's
portable counter extraction; the exact offending field/value was not retained.
Kit usage was 92 input / 5 output / 97 total, with zero cached and reasoning
tokens. The body hash cannot recover missing text evidence. The incomplete
capture is retained without another observer amendment or live execution.
[Capture limitation](../../target/storage-feature-split-evidence/scoped-live-20261020-222146/audio-unary/capture-limitation.json).

Validation passed: **247 wire tests, 32 session tests**, Provider Clippy with
warnings denied, formatting, five documentation-contract tests and diff checks.
Independent actual-diff reviews covered Gemini ownership/admission and the
observer's exact routes, redaction, forwarding, bounds, trust configuration and
settlement. Observer selfchecks passed after meaningful projection red evidence;
their local HTTPS stand-in did not predict the later real response-usage shape.
No new production owner/type, API, dependency, fixture framework or retry
mechanism was introduced. Existing live assertions, prompts, model selections,
generation counts and deadlines were unchanged.

All three case owners completed their Kit cleanup without a reported error;
Gemini explicitly records successful Session close and Provider shutdown.
Both observers joined their threads/connections, and every invocation-owned
process group settled. The original Provider environment remained byte-identical;
only per-invocation child wrappers selected loopback observation. No remote batch
resources were created. Cleanup success does not qualify the failed cases.

Remaining mandatory cases are all three OpenAI audio cases, both Gemini session
cases, and the unexecuted mixed-row batch. As explicitly selected, automatic
Gemini audio retains its contract and failed fixed-silence receipt, without
another run or stream-end extension. Full Provider qualification, vendor updates,
Agent/App live gates, crash recovery and packaging remain pending. No additional
prompt tuning, live retries, capacity workloads, commits, pushes or merge occurred.

### Bounded ten-failure follow-up: seven cases remained unqualified

The approved follow-up executed all nine short cases once on one frozen
candidate: **two passed and seven failed**. The invocation is `INCOMPLETE`
because the Anthropic diagnostic failed its TLS prerequisite. A subsequent
single, conditionally authorized Ollama prompt clarification passed its exact
case. Thus three of the ten cases have passing targeted evidence; **seven
remain unqualified**. The long OpenAI mixed-row batch was not admitted. No full
Provider rerun, vendor update, Agent/App live run, crash run or package gate was
admitted after these results. The original 128/14 full result and four separate
Anthropic batch passes below remain unchanged.

[Nine-case invocation](../../target/storage-feature-split-evidence/scoped-live-20261020-215215/receipts.json)
used source `8c34c759afe82dc9ca93848a5f64c804b7715efbf3e0a62c639c41eea7edee97`
and executable SHA256
`a1fe5727c9312552c5f6083f521f9f20fde09e73362491fc5453061df6fa2b7a`.
[Clarified Ollama invocation](../../target/storage-feature-split-evidence/scoped-live-20261020-215634/receipts.json)
used source `5da74f2d142e1c19668c8b929bc3687217c5e446a3acd8bfb513170d78d9b429`
and executable SHA256
`fa486b77d607d06c954bd55f67e5ce8ac197c5d4f53ed5437a94e235dbb18dd9`.
Both retain base `af28cdde`, the unchanged lock hash recorded below, Rust 1.94.1,
macOS/APFS, one build job, native `--locked`, and the existing low-memory test
profile. Builds preceded execution; inventories contained exactly 142 cases,
with one selected, zero ignored and 141 filtered per exact execution. Each
invocation records environment, observer and supervisor hashes, available space,
commands, artifact profile and stable before/after source identity. The
[pre-edit freeze](../../target/model-compatibility-20261020/remaining-failures/before.json)
includes tracked and untracked inputs and retains the prior source diff.

| Case | New result and classification |
| --- | --- |
| OpenAI audio unary | Failed the unchanged 21-byte exact-answer assertion; the returned answer was a 30-byte JSON object. The approved plain-text clarification did not qualify this case. |
| OpenAI audio streaming | Failed the same exact-answer assertion with 31 returned bytes. Stream-to-completion fidelity passed before the exact-answer check. No new Kit text-loss defect is established by this export. |
| OpenAI audio function continuation | Failed at the first response's audio-derived argument equality. Portable `ToolCall` and one `strict_lookup` call were present, but its value differed. The second generation, including the clarified final instruction, was not reached. This is not another replay HTTP400 or finish-reason failure. |
| OpenAI ordinary Realtime | Passed once with the approved instruction prohibiting pre-tool narration. Entire-transcript equality, exact function correlation, the subsequent audio-input turn, audio/transcription checks and consuming cleanup all passed. This qualifies changed inputs, not the historical prompt or statistical reliability. |
| Ollama ordinary streaming | The unchanged capture failed exact copy: native and Kit both returned the same 38-byte token instead of the requested 39-byte token. Complete NDJSON, including terminal content, proves no observed Kit text loss. The one approved literal-copy clarification then passed, with requested/native/Kit 39-byte hashes equal. |
| Ollama penalties/cache | Passed unchanged, both unary and streaming. Both native responses matched the requested token and Kit text; numeric penalty mapping and usage remained intact. This is non-reproduction of the earlier output failure, not a Kit correction. |
| Anthropic unreachable URL | Diagnostic prerequisite failure. Python's default trust store had zero CA certificates; `SSLCertVerificationError` occurred during TLS connect, before sending an upstream HTTP request. Kit transport failure, no HTTP status, no SSE and incomplete capture were preserved. The original missing-URL failure remains unexplained; no retry or certificate-verification bypass occurred. |
| Gemini ordinary session | Failed again on the exact empty top-level `{}` message, with the existing exactly-one-message-branch error. No portable events preceded it. This remains native protocol-output failure with correct Kit rejection under the documented contract. |
| Gemini pending-call interruption | The first 108,924-byte automatic-audio send succeeded, including exactly 32,000 appended zero bytes. The unchanged 90-second observation expired before any portable event or function call. Cancellation, late-result and second-audio assertions were not reached. The underlying native/VAD cause remains unknown; no further silence tuning or retry occurred. |
| OpenAI mixed-row batch | Not executed: the short-case gate failed. The earlier 503 and separately verified cleanup remain historical evidence. |

The initial Ollama native/Kit text hash was
`20cf5e7f015ec17d2edb6fb576dbba251e897d0312ef0f2d39e63849edefd95b`;
the request required hash
`aedebfa0a67fb1faa12f83ec8c5c0351160ddd35c04210ba96becbb233b42888`.
Thirty content frames and one terminal frame completed with `stop`; usage
637 input / 0 cached / 31 output matched the Kit, preserving zero rather than
absence. In the clarified run, requested/native/Kit hash was
`06320e08c63bd2140667aab0ed13be1a265afc256fddab604b20fda389ef32db`;
usage was 677 input / 595 cached / 34 output. Each streaming case made exactly
one upstream request. The penalties case made its existing two requests.
[Initial comparison](../../target/storage-feature-split-evidence/scoped-live-20261020-215215/ollama-stream/native-sdk-comparison.json).
The Anthropic observer's one recorded request counts the Kit-to-observer
attempt; it does not claim an HTTP request reached Anthropic.

Only the approved prompt/input changes were applied:

- Shared audio instruction appended: “Respond in plain text, not JSON. Output
  the requested sentence itself, without quotation marks, field names, braces,
  or Markdown.” The spoken answer was not supplied in the prompt.
- Audio continuation's final user instruction became: “The completed tool
  result is a JSON object. Reply with only the string value of verification_token,
  not the JSON object, field name, braces, quotation marks, or Markdown. Preserve
  every character of the value. Do not call another tool.”
- Ordinary OpenAI function-turn instruction appended: “Do not speak before
  calling the tool or while waiting for its result. After the result arrives,
  speak only its value field; do not narrate the tool call.”
- Ollama ordinary streaming, only after complete native evidence: “Copy the
  following token character for character, including its complete suffix:
  {nonce}. Treat it as literal text; do not infer, repair, shorten, or replace
  any character. Return only the token, without quotes, Markdown, or whitespace.”
- Gemini's existing interruption case retains every speech byte and appends
  exactly 32,000 zero bytes locally to the buffer used for both existing Audio
  sends. The shared asset, manual-audio cases and 90-second deadline are unchanged.

The unchanged production implementation passed **247 wire tests and 28 session
tests**, plus the two exact new controls, Clippy, formatting and documentation
contracts. The Anthropic control proves empty initial input plus fragmented URL
JSON, failed retrieval correlation, exact opaque replay and cleanup. The Gemini
control proves top-level `{}` rejection and valid empty nested control followed
by progress. Its first execution failed because the new test expected output
transcription while requesting it disabled; the local test setup was corrected
to enable and assert it. That failed fixture receipt is retained, not presented
as production red evidence. No new production change was made in this follow-up.

Independent reviews covered input scope, controls, passive forwarding, redaction,
terminal bounds, ownership and the conditional Ollama clarification. Disposable
observer selfchecks recorded red then green for the added projections and tested
one-byte chunking, terminal content, complete/incomplete framing, unchanged body
and status, bounds, failure propagation and joined local connections. Their TLS
stand-in was not external TLS qualification; the later real trust failure is
reported separately. Offline inspection found the existing system bundle
`/etc/ssl/cert.pem` parses 128 CA certificates (SHA256
`9dae8d76e55cb08991f2b672d58999ea15560d910759c16b544f843bdffbb994`). A future
approved Anthropic capture must supply and hash a verified CA bundle in the
observer parent environment and validate its trust-store prerequisite. No
additional Anthropic request was made in this bounded attempt.

All completed case owners attempted their unchanged Kit cleanup; no Kit cleanup
failure was reported. Session exports explicitly record successful consuming
close and Provider shutdown. All invocation-owned process groups settled, and
all observer threads/connections joined. Those facts do not turn case failures
or incomplete capture into passes. No remote batch resources were created by
this follow-up; the earlier explicitly authorized three-file deletion remains
separate evidence. No capacity work, commits, pushes or merge occurred.

### Initial full execution and four batch recoveries

The full Provider verifier completed on October 20, 2026 with **128 passed,
14 failed, zero ignored and zero filtered live cases**. All 142 declared cases
executed, including the long batch cases. The live target took 17,968.60 seconds;
the complete verifier took 18,223.38 seconds. Its source identity remained
`5393ae8e96ccd7c5507667009f9d03900ac57cd8c76c5e1a15e2411fd122f6b6`
before and after execution, at base `af28cdde`. Cargo.lock remained
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Rust 1.94.1, locked serial Cargo and the existing low-memory settings were used
on macOS/APFS. The supervisor reaped its direct child and settled its owned
process group without reported cleanup errors. That process receipt does not
establish remote batch cleanup or successful Kit behavior in failed cases.
[Full invocation receipt](../../target/storage-feature-split-evidence/scoped-live-20261020-152631/receipts.json),
[private verifier log](../../target/storage-feature-split-evidence/scoped-live-20261020-152631/provider-full-verifier.log).

All **20 current-model cases passed**, with 30 complete response exports:
17 unary and 13 streaming completions. Each of the ten frozen current
OpenAI/Claude/Gemini generation models passed its selected-model Minimal
journey; representative profiles also passed omitted/Maximum reasoning,
structured output and actual correlated function continuation. These results
qualify the recorded model/operation combinations, not every setting on every
model or internal reasoning execution. Gemini exports preserve selected model
identity, not an independent native `modelVersion` claim. All three current
Ollama thinking/schema cases and reduced-dimension embeddings passed. The
cache case observed real cached usage but failed its separate exact-answer
assertion. No Ollama Cloud lane ran.

| Failed area | Established evidence and remaining limitation |
| --- | --- |
| Three OpenAI audio cases | Mandatory output/function-call assertions failed. Their first exports also failed because the invocation supplied a nonexistent explicit evidence directory. A single bounded recovery capture using unchanged cases, requests, models, assertions and deadlines again failed all three; response exports and cleanup then succeeded. No successful audio qualification is claimed. |
| Four Anthropic batch cases | The parent's missing evidence directory caused export/checkpoint failures. With that precondition corrected, one predefined exact execution of each case passed: cancellation, missing resources, mixed-row outcomes and result limits. Cleanup and process settlement passed. These new receipts qualify the corrected invocation inputs; original failures remain failures. |
| OpenAI mixed-row batch | A deleted-file readability prerequisite received HTTP 503 with a non-JSON upstream connection/reset body. Cancellation and status cleanup also returned 503. The mixed-row assertions were not reached. Later read-only reconciliation confirmed the prerequisite job completed and identified its two input files plus one output file. All three were subsequently deleted under explicit user approval, with matching deletion acknowledgements and read-only absence checks. This separate cleanup does not replace the failed qualification receipt. |
| Ollama ordinary streaming and penalties/cache streaming | Correct model, Stop and exact delta-to-completion fidelity passed; returned synthetic tokens did not equal the requested tokens. Cache streaming reported 600 cached tokens out of 635 input tokens. No Kit text-loss defect was demonstrated; raw native request/NDJSON was not captured. |
| Anthropic unreachable-URL streaming | Terminal normalization found a `web_fetch` invocation with empty input and no required URL. The export lacks the complete SSE sequence and correlated tool result, so native missing arguments versus accumulation loss remains unresolved. |
| Gemini ordinary session | The retained native message was exactly `{}`. Rejection matches the documented requirement for a server-message branch; no Kit field loss was found. Session close and Provider shutdown passed. |
| Gemini pending-call interruption | A finite automatic-audio chunk produced no public event or function call before the existing 90-second deadline. Interruption was never exercised. Google's paused-audio guidance requires `audioStreamEnd`; the current portable chunk/manual-turn contract does not expose that automatic boundary. No implicit flush or reinterpretation was added. Close and shutdown passed. |
| OpenAI ordinary session | The model emitted an 87-byte spoken preamble before its function call, then the exact 103-byte requested phrase. The entire 190-byte transcript correctly failed the unchanged exact-output assertion. Close and shutdown passed; the second audio-input turn was not reached. |

The new OpenAI cancellation/subsequent-progress case passed. Pre-live gates
also passed: 245 wire, 27 session-lifecycle, 258 strict-live assertion, 17
environment-contract and five source-contract cases, plus the configured
public/lifecycle/error/transport, Clippy, formatting, documentation and doctest
gates. Deterministic passes cannot replace the failed native results.

The reviewed invocation setup correction changes neither production nor case
acceptance. The additional audio capture is evidence recovery after lost
exports, not a replacement full-verifier pass or a retry-until-green policy.
[Audio capture receipt](../../target/storage-feature-split-evidence/scoped-live-20261020-203305/receipts.json),
[failure classification](../../target/model-compatibility-20261020/review-late-live-failures.json).
No assertion, prompt, model or deadline was relaxed in response to these
failures. No production correction is inferred solely from a failed live case.

The four setup-corrected Anthropic cases took 26.17, 0.87, 357.95 and 209.95
seconds respectively, with one exact selected case, zero ignored and 141
intentionally filtered in each execution. Their existing long-completion and
cleanup deadlines were unchanged. All four executed on one unchanged candidate;
no batch case was retried within this correction invocation. This is a scoped
result, not a fresh passing full Provider verifier. **Ten of the original
fourteen failed cases remain unqualified**, including the audio function case
whose demonstrated defects were corrected but whose final answer still failed.
[Corrected Anthropic invocation](../../target/storage-feature-split-evidence/scoped-live-20261020-205425/receipts.json).

The recovered audio function response did contain one validated call with the
exact required arguments and opaque replay, but native `stop` remained portable
`Stop`. The existing portable contract and other adapters classify a successful
completed function request as `ToolCall`. A focused unary/SSE regression failed
with actual `Stop` versus expected `ToolCall`. The same audio response owner now
normalizes only successful `Stop` with validated nonempty calls; output limits,
filtering, malformed-call errors, text, replay and `[DONE]` requirements remain
unchanged. All ten focused audio wire cases passed, including terminal controls.
This correction neither removes the extra text in the two audio-answer failures
nor retroactively qualifies the earlier full run. Its affected exact live
function run then passed the original call/argument checks but received native
HTTP400 on the second request: the new fixture had dropped the original audio
history from a stateless text-output request. This is a separate fixture-input
defect. The existing case now retains its original audio before the unchanged
assistant, tool result and new text-only turn. Its two generations, exact
assertions and deadlines are unchanged. Both existing wire replay controls
check byte-identical audio history and exact assistant/result ordering; all ten
audio wire cases pass. The next live execution exposed a separate native HTTP400:
`ToolChoice::None` removed declarations but the audio renderer still emitted
`tool_choice`. The retained error explicitly rejects that field without tools.
The existing selection regression failed before the one-line correction and
passes afterward: the renderer now omits the selector when no effective tools
remain. Required/named choices still pass through existing declaration
validation; no public choice semantics or live assertion changed. All ten
audio wire cases pass with both empty-None and empty-Auto controls. The one
exact live journey after this correction passed the call/argument checks and
native acceptance of its streaming continuation, then failed its mandatory
final-answer assertion: 64 returned bytes instead of the exact 39-byte token.
Kit and process cleanup passed. No text was stripped, assertion relaxed or
unchanged audio run repeated. Protocol acceptance does not qualify the failed
exact-answer journey.
[Red evidence](../../target/model-compatibility-20261020/audio-finish-red.log),
[focused green evidence](../../target/model-compatibility-20261020/audio-finish-green.log),
[separate fixture failure](../../target/storage-feature-split-evidence/scoped-live-20261020-204225/receipts.json),
[history mapping controls](../../target/model-compatibility-20261020/audio-history-green.log),
[native selector failure](../../target/storage-feature-split-evidence/scoped-live-20261020-204732/receipts.json),
[selector red](../../target/model-compatibility-20261020/audio-empty-tools-red.log),
[selector green](../../target/model-compatibility-20261020/audio-empty-tools-green.log),
[final affected live result](../../target/storage-feature-split-evidence/scoped-live-20261020-205302/receipts.json).

Remote cleanup required separate explicit approval after automatic approval
review rejected deletion. The approved operational invocation made exactly
three deletes, no cancellation or generation, and settled every HTTP child and
its process group. Input/output artifacts and file metadata were retained;
OpenAI does not permit downloading the original `user_data` file, and that
HTTP400 receipt is preserved. Original qualification receipts were not changed.
[Operational cleanup](../../target/storage-feature-split-evidence/scoped-live-20261020-204042/operational-cleanup.json),
[process settlement](../../target/storage-feature-split-evidence/scoped-live-20261020-204042/operational-supervision.json).

The sequential Codex0.163.0/OpenCode1.19.3 finalization and final Agent/App,
crash-recovery and package checks remain **pending behind Provider
qualification**. Verified runtime downloads and updater/precheck regression
passes are preparation only. The existing finalized native versions and
historical qualification remain intact. No new commit, push, merge or capacity
execution occurred during this campaign. Later report-only changes do not
change the source identity of the recorded execution.

The preparatory native review compared every archive member across the frozen
release ranges: 1,308 changed Codex members and 87 changed OpenCode members.
Selected portable protocol paths received semantic review; this is not a
manual audit of every changed file or runtime qualification. Codex's new
`thread/readState/changed` requires classification by the existing notification
owner during its update. OpenCode's OpenAPI paths are unchanged; its added
`ProviderConfig.reasoning_options` does not authorize portable variant mapping.
Pinned archives, locks and selected versions remain unchanged.
[Source review and exact identities](../../target/model-compatibility-20261020/native-source-research/preparatory-review.json).

Independent final report review found no actionable discrepancy between the
reported counts, corrected scoped results, remaining failures, operational
cleanup and retained receipts. Final focused audio wire checks, Provider
Clippy, workspace formatting, documentation-contract checks and whitespace
checks passed. These checks do not close the outstanding live gates.

### Implementation and focused evidence

The requested research/Rust skills inform exact private profiles and existing
owners. The candidate adds OpenAI audio Chat Completions, current reasoning
validation, Gemini terminal/session handling and Ollama dimensions/options/usage.
All 245 Provider wire cases passed after meaningful red evidence for the changed
boundaries; session lifecycle passed 26/26. These are deterministic observations,
not native execution. Provider formatting, Clippy, public/compile contracts,
source/environment contracts and doctests passed; the strict-live assertion
target passed 252/252 before the final identity/interruption additions. The
first gate's Clippy finding and a later invalid test-response construction are
retained as failed receipts; both fixture corrections preserve acceptance.
The workspace documentation gate passed. Final gates must include subsequent
review corrections and remain pending.

New Agent regression evidence established an OpenCode admission defect:
unsupported reasoning/personality defaults could create a conversation or
workspace, report an update ready, and publish a workspace revision. The
existing configuration owners now reject them before those mutations. Four
focused public cases pass after recorded red evidence, including unchanged
revisions, subsequent progress and cleanup. Reopen semantics are unchanged.
Agent Clippy and independent reviews passed for this repair. The Codex and
Custom settings additions passed their focused baseline checks without a
production change.

Final Provider inventory preparation records **142 unique, nonignored cases**:
all 110 retained cases plus 32 focused additions. The compiled inventory and
artifact hash are retained separately from execution. The final assertion
target passed 258/258, including red/green checks for wrong current canonical
model identities and genuine interruption evidence. A new Gemini automatic
audio/pending-call protocol case passed against unchanged session production
behavior; this is deterministic baseline evidence, not a native interruption
receipt. Exact response identity checks apply only to the eight frozen current
OpenAI/Claude canonical IDs. Gemini's portable selected identity is preserved;
these tests do not independently prove its native `modelVersion`.

Evidence is retained under `target/model-compatibility-20261020/`, including
initial source/lock/vendor hashes, historical archive copies, local Ollama0.40.6
model digests, red/green logs and independent source-hashed reviews. The new
Agent complete deterministic and all final live/package gates remain pending.
Native refresh targets are frozen at Codex0.163.0 then OpenCode1.19.3;
the existing finalized versions remain selected until their sequential update.

The user explicitly narrowed Ollama qualification to **local only**. No cloud
credential or live cloud case is required or executed. Existing cloud API shape
is preserved; deterministic direct-origin rejection is not cloud qualification.
No capacity workload, Tool live suite, workspace live wrapper, push or merge
is part of this campaign. Local updater commits remain subject to successful
strict gates. Historical capacity/native failures and withdrawn coverage below
remain unchanged.

## Approved vendor refresh — scoped qualification completed

Codex **0.162.1** remains finalized at `49a6cb22`; OpenCode **1.19.0**
completed every configured updater gate and finalized locally at
`8d8ebf15e22c1f28059a18eca654fb0394ec25ec`. Both approved versions were
retained despite newer release discovery. The recovered-abort correction below
changes only test synchronization; no Kit production defect was established
for that timeout. The complete candidate includes the previously reviewed
vendor compatibility changes and tool-return description clarification.

| Current evidence | Result |
| --- | --- |
| Corrected abort-outcome cases | 9 passed, 297 intentionally unselected, 37.61 seconds |
| OpenCode protocol/ownership target | 306 passed, zero ignored or filtered, 858.37 seconds |
| Complete Agent live inventory | 24 passed, zero ignored or filtered, 661.37 seconds |
| App workflow-live inventory | 18 passed, zero ignored or filtered, 522.70 seconds |
| Canonical four-boundary crash case | 1 passed, 3 intentionally unselected, 4.37 seconds |
| Clean candidate and packages | Fresh five-crate source build, native verification of all five packages, sibling checksum checks, and extracted-consumer execution passed |

The Agent updater ran all seven configured completion gates, including the
complete verifier's prerequisite, source-authority, deterministic, live, and
post-live checks. Custom runtime's 56 cases and Codex protocol's 96 cases also
passed. Formatting, Clippy, documentation and doctest gates passed.
[Agent gate log](../../target/storage-feature-split-evidence/scoped-live-20261020-123530/vendor-update.log).
Both mandatory Question Kit receipts passed. Their separate raw-copy grades
were **delivery: pass; native-idle: fail (1/2)**. The latter remains a
nonblocking model-quality failure under the existing two-case exception;
knowledge and every other exact-output assertion retain their mandatory status.
A successful invocation does not explain all historical failures or establish
statistical reliability.

All 13 observed Codex connections settled. The selected passive model capture
is complete for four Question sessions and six knowledge calls. One unrelated
chat request has an incomplete `BrokenPipeError` receipt; this is not a claim
that every model request was captured completely. All five outer child
processes were reaped and their groups settled, with no cleanup errors.
The ignored Agent environment was restored byte-for-byte and Cargo.lock was
unchanged. Deliberate service termination is process settlement, not a claim
that every service exited zero.
[Agent ownership and observation receipt](../../target/storage-feature-split-evidence/scoped-live-20261020-123530/service-ownership.json).

App's exact inventory and the crash case executed at `8d8ebf15` against
OpenCode **1.19.0**. The canonical case covered staged action, staged input,
before barrier commit, and after barrier commit. All scoped App assertions
passed and invocation-owned processes settled. Four controlled failure
scenarios retained durable evidence with expected terminal cleanup errors;
their passing assertions prove bounded completion and error preservation,
not successful durable cleanup.
[App and crash receipts](../../target/storage-feature-split-evidence/scoped-live-20261020-131533/receipts.json),
[App process ownership](../../target/storage-feature-split-evidence/scoped-live-20261020-131533/service-ownership.json).

Packaging used Rust **1.94.1**, serial locked Cargo commands, and a fresh
**1,557-file** candidate snapshot on **macOS/APFS**, including eligible
untracked inputs. Native dependency build caches were reused; all five local
Kit libraries were freshly compiled. Native `cargo package --locked` verified
the five-package closure before the extracted-consumer check. Each archive's
source bytes and packaged sibling checksums matched. The consumer resolved
only those extracted Kit archives, retained a Lance-only normal Storage graph,
and completed a real App workflow with joined shutdown; it constructed no
Agent runtime. Its copied lock seed initially failed locked metadata, then
the existing offline preparation step produced an audited seed before locked
compilation and execution. That preparatory rejection remains recorded.
[Package receipts](../../target/agent-app-repair-evidence/packaging-20261020-132615/receipts.json),
[archive checksums and source audit](../../target/agent-app-repair-evidence/packaging-20261020-132615/package-details.json).

Independent reviews found no actionable issue in the actual correction or
combined vendor diff, and separately checked final inventories, grades, and
cleanup evidence. The source comparison retains earlier Storage evidence for
158 unchanged production/test/manifest/toolchain/lock inputs, without claiming
a new matrix or platform execution. This final report update follows execution
and is separately bound as a documentation-only change.
[Final source comparison and review record](../../target/vendor-refresh-20261020-closeout/qualification-summary.json).

The requested vendor/Agent/App/crash/package scope is complete. Earlier failed
invocations, native-output failures, capacity failures/screens, and withdrawn
coverage remain historical evidence below; none is relabeled as passing.
No capacity workload, Provider/Tool live suite, workspace live wrapper, push,
or merge ran during qualification. The authorized updater created the local
OpenCode commit; this final report update followed execution. Entries below
retain their status at the time of their recorded invocation.

## Recovered-abort investigation — fixture ownership handoff

The failed recovered-abort setup treated a streamed timeout followed by stream
fusion as proof that foreground ownership had ended. Source inspection shows
that `await_settlement` can publish that timeout and continue its admitted work
with the live registration. Switching the persistent Admission fault at that
point can let the foreground continue and perform cancellation, while the test
expects the recovered owner's error. Recovery correctly refuses a live
registration. This is a fixture precondition defect; no Kit production defect
has been demonstrated. The original failure lacks task timing, so its exact
interleaving remains unproven.

One exact diagnostic passed in **5.30 seconds**, returning the required
`Conflict / TransactionConflict` plus supplementary `Cleanup /
ServiceUnavailable` after **93.55 ms** of public abort. The selected write and
rollback failure occurred in the same task at approximately **78.4/78.7 ms**.
One subsequent complete diagnostic target passed **306/306**, zero ignored or
filtered, in **863.18 seconds**. Both retained the original assertions and
deadlines and settled their invocation-owned processes without cleanup errors.
Temporary synchronous logging can affect scheduling; these are
non-reproductions, not proof of a repaired production defect.
[Exact diagnostic](../../target/vendor-refresh-20261020-abort-investigation/receipts.json),
[full-context diagnostic](../../target/vendor-refresh-20261020-abort-investigation/full-context/receipts.json),
[source proof](../../target/vendor-refresh-20261020-abort-investigation/source-proof.json).

The correction reuses the existing concrete recovery fixtures' bounded
foreground-task drainage check before switching faults. It retains the
Admission fault during settlement and preserves the durable prompt for
recovery. All temporary diagnostics were removed. No production path, public
API, assertion, test count, native-call count, or existing deadline changed;
the three-second setup check remains inside the original twenty-second body
budget. Independent actual-diff review found no actionable issue.
All **nine** existing abort-outcome cases passed after correction in
**37.61 seconds**, with zero ignored and exactly 297 unselected cases.
Their original foreground/recovered/reopen, retry, receipt, uncertainty,
error-preservation, and cleanup assertions remain in place.
[Corrected focused receipt](../../target/vendor-refresh-20261020-abort-investigation/focused-green/receipts.json),
[independent review](../../target/vendor-refresh-20261020-abort-investigation/independent-review.json).
The subsequently completed vendor gates are recorded above.

The diagnostic runner's inherited descriptive plan reports the obsolete
299-case inventory; its selected libtest inventory and execution receipts are
authoritative: one selected case with 305 filtered, then all 306 with none
filtered. A preliminary diagnostic compilation error prevented execution and
is retained separately; it is not behavioral red evidence. The earlier
305/306 failure below remains unchanged historical evidence.

## OpenCode resume — historical deterministic abort-outcome gate failure

After the clarified direct test passed, the existing reviewed updater resumed
OpenCode **1.19.0**, with Codex **0.162.1**, without rerunning planning or
implementation. All configured formatting, Clippy, documentation and doctest
gates preceding the Agent verifier passed. Its precheck, archive-authority,
public/static contracts, Custom runtime (**56 cases**), and Codex protocol
coverage (**96 cases**) passed.

The OpenCode event/ownership target then reported **305 passed, one failed,
zero ignored or filtered**, in **867.03 seconds**. The failing
`opencode_abort_outcome_recovered_failed_rollback_vetoes_retry` expected
`Conflict / TransactionConflict` with supplementary cleanup evidence but
received `Timeout / Abort / Cancellation` without supplementary failures.
The failure occurred at the error-kind assertion after the case's one
persistence-attempt, one native-abort, and one outcome-record assertions.
This is a deterministic protocol/Storage failure, **not a new real-service
or model-output failure**. Its cause is not established by this receipt.
[Failure and source-bound outcome](../../target/storage-feature-split-evidence/scoped-live-20261020-114046/qualification-result.json).

Independent read-only review identified the existing five-second lifecycle
observer deadline as a path to this exact bare timeout, distinct from the
test's seven-second outer observation bound. The outcome record is collected
before the injected write failure, and the injected rollback failure follows
an awaited real rollback. No retained phase timing establishes that rollback
finished before the observer deadline. The cancellation owners and relevant
test/fault paths are unchanged from `49a6cb22`; this result establishes neither
a vendor regression nor a fixture defect. It does not authorize weakening
error-preservation assertions.

The strict updater stopped before the **24 Agent live cases**. No Question
grades were generated, and OpenCode finalization, App's 18 live cases, canonical
crash recovery, and final package/consumer checks remain pending. No unchanged
rerun, assertion relaxation, deadline change, or fixture expansion followed.
Earlier successful and failed receipts remain unchanged.

The original vendor lock and ignored environment were restored byte-for-byte;
the Cargo lock, HEAD, and frozen tracked/untracked source inputs remained
unchanged. Every invocation-owned child was reaped and its process group
settled, with no cleanup errors. The passive model observer correctly reported
incomplete evidence because the live target was never reached; its relay
threads joined. Process settlement is separate from test correctness.
[Process ownership](../../target/storage-feature-split-evidence/scoped-live-20261020-114046/service-ownership.json).

No vendor commit, push, merge, or capacity execution occurred in this resume.

## Clarified direct execution — exact answer delivery passed once

The subsequently authorized clarified direct case passed **one of one tests**
in **7.64 seconds**, with zero ignored or filtered cases, on Codex **0.162.1**
and configured **gpt-5.6-luna**. The existing tool description now states its
plain-string return shape. The prompt, assertions, model, one tool invocation,
90-second prompt deadline, and cleanup requirements are unchanged.
[Execution receipt](../../target/storage-feature-split-evidence/scoped-live-20261020-113522/service-ownership.json).

The native capture contains one tool call and two correlated inference
requests. This time the model-written cell emitted the returned string
directly. The **44-byte** answer has the same hash in the tool result, emitted
cell content, next inference input, and completed `final_answer`. Public exact
answer and streaming assertions passed. Conversation, notification, workspace,
and Agent cleanup all succeeded; the native child exited zero and its process
group settled. Source and invocation inputs remained unchanged during execution.
[Bounded native facts](../../target/storage-feature-split-evidence/scoped-live-20261020-113522/native-response-facts.json).

This qualifies the clarified input for this invocation; it does not relabel
earlier failures, establish their common cause, or establish statistical
reliability. The complete OpenCode 1.19.0 updater gates and subsequent App,
crash-recovery, and package qualification remain required. No capacity workload
is admitted by this result.

## Direct native capture — model-written code discarded the tool result

One authorized direct attempt ran unchanged on **Codex 0.162.1**,
**gpt-5.6-luna**, and fresh directory-backed Lance: **zero passed, one failed,
zero ignored or filtered**, in **8.50 seconds**, with the same
`Prompt / CompletionValidation / MissingData` failure. All four Kit cleanup
steps succeeded; the owned native process exited zero, was reaped, and its
process group settled. No retry or second live execution was performed.
[Execution and source identities](../../target/storage-feature-split-evidence/scoped-live-20261020-112136/service-ownership.json).

The existing native `CODEX_ROLLOUT_TRACE_ROOT` diagnostic captured one prompt
and two completed native inference requests. The model's first response
authored a code cell that awaited the interactive tool, then traversed the
returned value's optional `content` array, falling back to an empty array.
Native tool execution correctly returned the caller's **44-byte plain string**.
The model-written traversal therefore emitted nothing; the cell completed
successfully with zero content items. Its matching continuation request
contained no caller-answer token anywhere. The second decoded response already
contained an empty `final_answer`, before App Server filtering.
[Bounded correlated native facts](../../target/storage-feature-split-evidence/scoped-live-20261020-112136/native-response-facts.json).

This establishes a model-authored return-shape mistake for this invocation,
not an Agent Kit output-loss or Lance persistence defect. Native conversion
intentionally exposes text tool results as strings
(`tools/src/tool_output.rs`); the selected model's `tool_mode` takes precedence
over the process feature default (`core/src/tools/mod.rs`). The pinned Luna
metadata specifies `code_mode_only`, explaining why disabling the feature
default did not remove this native path. The tool had no result description;
no native declaration promised a `content` array.

The narrow correction adds the actual return shape and direct-emission
guidance to the existing tool description in the standalone and canonical
caller-loss cases. This is explicitly a **changed model input**. It supplies no
answer token and preserves the original prompt, model, generation counts,
assertions, deadlines, test inventory, and cleanup. No Kit production code or
fixture owner changed. The corrected description is **not yet live-qualified**:
the one-attempt limit permits no second execution in this invocation. Historical
empty-answer failures remain failed evidence; this capture does not prove that
all earlier occurrences had the same cause. OpenCode finalization and the
remaining vendor gates stay pending.

Focused Clippy compilation of both affected test targets, workspace formatting,
documentation-contract checks, whitespace checks, and independent actual-diff
review passed. These checks do not establish the clarified live outcome.
[Correction validation](../../target/storage-feature-split-evidence/scoped-live-20261020-112136/correction-validation.json).

Native tracing records typed completed response items, not every raw provider
frame. The observed transport was WebSocket; no HTTP diagnostic header was
needed. Native tracing adds synchronous diagnostic I/O, so this run is not a
latency comparison. Full native trace/log payloads are reduced to bounded
lengths, hashes, types, correlation and terminal facts, then removed after
independent review. Kit joining and native process reaping are recorded
separately. No capacity workload, commit, push, merge, or vendor finalization
was performed.

## Direct Codex live reproduction — same public failure without shared fixtures

The explicitly requested one-shot `codex_direct_live` diagnostic ran on
**Codex 0.162.1**, configured **gpt-5.6-luna**, and fresh directory-backed Lance:
**zero passed, one failed, zero ignored or filtered**, in **9.82 seconds**.
It returned the same `NotYetImplemented / Prompt / CompletionValidation /
MissingData` error. The test uses only public Kit calls and one caller-owned
interactive Local tool; it imports no shared test helpers, mocks no dependencies,
and uses no passive proxy. It retains the original interactive prompt, exact
answer and streaming assertions, and 90-second prompt deadline. This was one
execution, not a retry campaign.
[Execution log](../../target/storage-feature-split-evidence/scoped-live-20261016-152800/direct-live-execution.log).

The scenario deliberately starts a fresh conversation and omits the earlier
caller-loss/recovery turns and gate tool. Thus the same public failure does not
require those preceding turns or the shared fixture machinery. This attempt
does **not** establish the upstream cause or independently prove another empty
native final answer: it has no correlated wire capture. The failure occurs before
the final exact-answer and completed-stream assertions; those assertions are not
passing evidence. Its log also cannot establish whether native tool completion
occurred, because the output observer runs before and after answer submission.
The earlier captured native-empty-answer failure remains
separate, unresolved evidence and the complete vendor gate remains blocked.

Conversation close, notification drain, workspace close, and Agent shutdown all
completed successfully. The invocation-owned Codex process exited zero, was
reaped, and its group settled. Source and invocation inputs remained unchanged
through execution. The receipt records the candidate (including untracked
sources), lockfile, executable and native binary hashes, Rust **1.94.1**, exact
inventory, and unoptimized test profile. Compilation, focused Clippy, formatting,
and independent test/cleanup review passed before live execution. No production
code or canonical verifier inventory changed; no capacity work, commit, push,
merge, or vendor finalization occurred.
[Source, artifact and ownership receipt](../../target/storage-feature-split-evidence/scoped-live-20261016-152800/service-ownership.json).

## Full vendor resume — native empty final answer blocks completion

The resumed **Codex 0.162.1 / OpenCode 1.19.0** campaign ran the complete
**24-case Agent live inventory: 23 passed, one failed, zero ignored or filtered**,
in **676.33 seconds**. The mandatory Codex caller-loss/interactive-tool case again
returned `NotYetImplemented`, `CompletionValidation`, `MissingData`. OpenCode is
**not finalized**; the post-live verifier tail, final App 18 cases, canonical
four-boundary crash case, and final package/consumer checks remain pending.
[Exact inventory and result](../../target/storage-feature-split-evidence/scoped-live-20261016-003106/qualification-result.json).

This capture establishes the failure's live native boundary. On connection 10,
the matching tool callback and completed dynamic-tool item contain the same
**45-byte** text and exact hash. Codex subsequently emits an assistant item with
phase `final_answer` and **zero text bytes**, including its completed notification,
then a completed terminal. No eligible nonblank completed assistant answer was
delivered for that generation. This is not evidence that Agent discarded a
nonempty live answer. The terminal has `itemsView: notLoaded`; the only
`thread/read` used `includeTurns: false` before the generation. Exhaustive native
history absence and the upstream cause of the empty answer remain unproved.
The conditional prompt clarification therefore remains unapplied. No Kit,
prompt, model, assertion, generation-count, or deadline change was made.
[Independent correlated classification](../../target/storage-feature-split-evidence/scoped-live-20261016-003106/caller-loss-independent-review.json).

Both knowledge layouts passed all three original phases. All **six** captured
Ollama responses equal normalized Provider text exactly, and all six existing
Provider-to-Agent comparisons and exact marker assertions pass. The model-side
Question capture contains **four distinct native sessions**, each with a valid
Question tool call followed by the matching tool-result request and final
response. The two permitted Kit cases both passed; raw exact-copy grades were
**delivery: true; native-idle reopen: false (1/2)**. The two additional child/
recovery sessions are not assigned copy grades. These successes are unchanged
non-reproductions; they neither explain historical failures nor establish
statistical reliability. One unrelated model-capture limitation is retained;
completeness applies to the selected Question and knowledge boundaries.
[Model correlation and copy grades](../../target/storage-feature-split-evidence/scoped-live-20261016-003106/model-capture-correlation.json).

The preceding invocation stopped before any live cases because the passive
relay URL violated the canonical precheck's fixed direct Ollama URL. That
**failed invocation remains preserved**. The correction is confined to ignored
orchestration: prechecks, model warmup, and the shared OpenCode server retain
their original direct configuration. Cargo's documented host-runner mechanism
executes the exact recorded Agent live binary with identical arguments, changing
only its two model transport URLs. Existing Kit/fixture owners still launch and
settle native children. Canonical configuration inverse checks, binary hashes,
exclusive invocation receipts, and disposable exec-path tests protect the
boundary; independent actual-diff review approved it. No verifier assertion,
selection, prerequisite, or gate was removed.
[Failed setup](../../target/vendor-refresh-20261016-resume/20261016-000433/precheck-failure.json),
[routing review](../../target/vendor-refresh-20261016-resume/20261016-000433/routing-correction/prelaunch-review.json),
[actual executable identity](../../target/storage-feature-split-evidence/scoped-live-20261016-003106/model-runner-invocation.json).

Formatting, all configured pre-live Clippy/documentation lanes, prerequisite checks,
protocol authority, and deterministic lead-in gates passed, including **56
Custom, 96 Codex, and 306 OpenCode ownership cases**. Execution used Rust
**1.94.1**, `--locked`, one build job and serial Cargo on macOS/APFS. All **1,557**
tracked/untracked source inputs and Cargo lock identity stayed unchanged during
execution; the artifact matches the earlier failed live run. All **13** Codex
observer connections settled. Both model relays and their handlers joined, all
five invocation-owned direct children were reaped and process groups settled,
and cleanup recorded no error. The original environment and vendor lock were
restored byte-for-byte. Kit joining assertions and process reaping remain
separate evidence. The failed Codex case does not receive a passing Kit result
merely because the invocation settled.
[Ownership and restoration](../../target/storage-feature-split-evidence/scoped-live-20261016-003106/service-ownership.json).

No further live run, App/crash/package gate, vendor commit, push, merge, or
capacity workload followed this mandatory failure. Codex commit `49a6cb22` and
the reviewed uncommitted OpenCode candidate are preserved. All historical
failures, withdrawn coverage, and earlier receipt limitations remain below.

## Question model-boundary capture — unchanged non-reproduction

One exact Question-delivery diagnostic against **OpenCode 1.19.0 / Ollama
0.40.5** passed **1 case, zero failed or ignored, 23 filtered**, in **70.69
seconds**. This closes the observation gap for this successful invocation only;
the earlier mandatory empty-response failure remains unexplained and failed.
No corrective Kit, native, fixture, or prompt change is justified by this result.
The full Agent gate and OpenCode finalization remain incomplete.
[Correlated classification and identities](../../target/storage-feature-split-evidence/scoped-live-20261015-235357/classification.json).

The existing ignored HTTP pass-through gained a bounded OpenAI-compatible
projection, independently reviewed after disposable forwarding, redaction,
correlation, bounds, and settlement checks. Only the invocation-local provider
URL changed; the source environment stayed byte-identical. Requests were
forwarded once, without extra generation, replay, synthetic output, or added
native history requests. The record contains hashes, lengths, schema/tool facts,
finish reasons and usage; no raw messages, tool arguments, reasoning or credentials.
[Diagnostic review](../../target/vendor-refresh-20261015-fixes/question-model-capture/review.json).

The same original prompt appeared once in each of two correlated model requests.
Both selected the unchanged Ministral model, `tool_choice: auto`, 11 tools
including the correct Question declaration, and `max_tokens: 4096`. The first
HTTP 200/SSE response ended with `tool_calls`: one valid Question call, no text,
55 completion tokens. Its call-ID and argument hashes matched the following
request exactly; that request carried the matching 151-byte tool result. The
second HTTP 200/SSE response ended with `stop`, eight text bytes, two completion
tokens and no additional call. Both streams completed with `[DONE]`. The existing
native-to-public assertions independently verified exact question/answer delivery,
replay identity, invalid/duplicate resolution rejection, output fidelity and
streaming. The **raw exact-copy grade was false**, permitted only after those Kit
assertions passed. The other Question case was not run, so no combined grade is
claimed. [Bounded model wire evidence](../../target/storage-feature-split-evidence/scoped-live-20261015-235357/native-wire.json).

All **1,557** tracked/untracked source inputs stayed unchanged during execution.
The test executable matches the earlier failed full run exactly
(`9ed17176…280e`). Rust **1.94.1**, `--locked`, serial execution, one build job,
macOS/APFS, lock/runtime hashes, and exact inventory are recorded. Kit cleanup
assertions passed; relay/handlers joined, every owned command was reaped and its
group settled, and cleanup reported no errors. Process settlement is separate
from Kit joining. The inherited generic receipt scope label is not authoritative;
the exact commands and ownership scope identify this single case. The known
`/v1/models` prerequisite has an unsupported-projection marker; both actual chat
captures are complete. [Ownership receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-235357/wire-ownership.json).

Upstream empty-response/parser reports remain hypotheses. Local model metadata
declares the explicit Ministral parser, but does not prove which model image the
running server loaded. No parser/configuration workaround was applied. This pass
does not explain the historical failure or establish statistical reliability.
No unchanged rerun, vendor finalization, App/crash/package gate, commit, push,
merge, or capacity workload followed. Earlier entries retain their original
source-bound failures and limitations.

## Question and knowledge investigation — no corrective Kit change established

The two latest failures were audited against the selected source and retained
native evidence. No request-mapping, token-construction, or output-transformation
defect was demonstrated. Assertions, models, generations, deadlines, and cleanup
remain unchanged; the reviewed OpenCode compatibility corrections remain intact.
OpenCode finalization and the final App/crash/package gates are still blocked.

For **Question delivery**, the native capture records a matching request followed
by a completed assistant with no text, tool calls, or pending Question. The
failure occurs before replay, resolution, or copy grading. The Kit forwards the
original prompt and model, and plain native prompts leave tool selection
automatic. Pinned OpenCode permits a `stop` result with no calls; the Kit's empty
text result matches its documented normalization. The Question tool, schema, and
service are unchanged between the verified 1.17.18 and 1.19.0 archives. Retained
invocation logs also identify one in-window Question-delivery session dispatching
the configured Ollama model through the `build` agent. This is title/time
correlation, not a captured model request. Actual tool availability in that
request and the streamed model response remain unobserved, so model omission
versus native adapter behavior is unresolved. No Question generation was rerun.
[Source and retained-evidence audit](../../target/vendor-refresh-20261015-fixes/question-knowledge-audit-20261015-232358/question-audit.json).

For **managed knowledge**, the failing normalized request contained the complete
marker, and Agent returned Provider text unchanged. The fixture already explicitly
requires copying every character. Retained earlier Ollama evidence includes an
incorrect native copy with this same current instruction and unchanged relevant
normalization sources. That establishes earlier native copying variability, not
the uncaptured native cause of the latest omitted character.
[Historical native/source comparison](../../target/vendor-refresh-20261015-fixes/question-knowledge-audit-20261015-232358/knowledge-audit.json).

One bounded diagnostic reused the existing HTTP pass-through and the unchanged
separate-directory case: **1 passed, 0 failed, 0 ignored, 23 filtered**, in
**18.20 seconds**. All three original phases ran once. Each HTTP 200, completed
`stop` response matched Provider and Agent text exactly and passed the original
exact-output assertion. No sampling options were supplied. The executable hash
is identical to the preceding failed full run. Rust **1.94.1**, `--locked`, one
build job, macOS/APFS, tracked/untracked source identity, lock and artifact hashes,
and exact inventory are recorded. Every relay request completed, the relay and
handlers joined, the child/process group settled, and cleanup reported no errors.
This is **non-reproduction only**, not a fix, statistical reliability, or a
replacement for the failed complete gate.
[Diagnostic classification](../../target/storage-feature-split-evidence/scoped-live-20261015-232442/classification.json)
and [ownership receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-232442/wire-ownership.json).

Only the existing ignored runner gained a selector for this one case; forwarding,
fixture behavior, and cleanup were unchanged and independently reviewed. No
production or test acceptance change was justified. There was no further live
run, updater finalization, commit, push, merge, or capacity execution. Historical
failures and missing Question grade remain blocking and preserved below.

## Passive OpenCode resume — mandatory live failures remain blocking

The approved passive-only resume against **Codex 0.162.1 / OpenCode 1.19.0**
completed its single full Agent execution with **22 passed, 2 failed, zero
ignored or filtered**, in **574.41 seconds**. OpenCode qualification remains
**blocked**. Codex commit `49a6cb22e50c9b5e028e99ebc47b2855cd1a6755` and the
reviewed OpenCode candidate are unchanged. The updater restored the original
OpenCode **1.17.18** vendor lock; both source archives remain available. It did
not finalize or create another commit. [Source-bound result and exact inventory](../../target/storage-feature-split-evidence/scoped-live-20261015-220413/qualification-result.json).

The caller-loss case **passed unchanged**. Connection 10 contains three distinct
generations: interrupted gate, successful recovery, and interactive tool turn.
For the interactive generation, native sequence 50 accepted the turn, 54
completed the matching user item, 60 requested the tool, 62 completed it, 85
completed an eligible final assistant item, and 88 delivered the terminal
summary. The client tool response, completed tool, assistant item, and summary
agree on the same SHA-256 hash of **45 bytes of UTF-8 text**; assistant item identity also matches.
There was no overlapping history reconciliation. The sole earlier history read
excluded turns and establishes no exhaustive history claim. This is
**non-reproduction**, not a demonstrated repair or an explanation of earlier
failures. Independent review confirmed that classification.
[Correlated evidence](../../target/storage-feature-split-evidence/scoped-live-20261015-220413/caller-loss-classification.json).

| Mandatory failure | Observed boundary | Classification limit |
| --- | --- | --- |
| OpenCode Question delivery/replay | The stream ended with zero fragments and empty terminal output. Its existing failure capture found one matching native user/request and one completed assistant with `finish=stop`, no native error, no text parts, no tools, and no pending Question. | Requested native tool/output behavior was absent. This does not demonstrate Kit output loss or establish why native/model execution omitted it. |
| Managed knowledge, secondary scope | The correct marker was present once in the normalized request. The 95-byte Provider result omitted one `B` from the 96-byte expected answer; Agent returned the Provider text unchanged. | Excludes an Agent text transformation in this occurrence. No Ollama wire capture was added, so Provider versus underlying native/model cause remains unproven. The exact-output assertion remains mandatory. |

The Question delivery case failed its Kit assertions and emitted **no copy-grade
receipt**; this is not a nonblocking copy failure. The other permitted Question
case, native-idle reopen, passed Kit assertions and reported **raw exact copy:
false**. The grade reporter correctly failed for the missing required delivery
receipt. Neither result is described as `0/2` successful grade collection.

The existing ignored observer gained exact assistant text hashes and ordered,
separate tool-content hashes/types/lengths; it retains no raw text, arguments,
reasoning, media URLs, or credentials. Meaningful projection red checks,
in-memory forwarding/correlation/bounds/settlement green checks, and independent
actual-diff reviews passed. No native request or generation was added. The
existing invocation path reused the source-verified implementation instead of
running another planning or implementation leaf. Every original completion gate
remained intact. [Frozen inputs, self-checks, and reviews](../../target/vendor-refresh-20261015-fixes/opencode-correlated-resume-20261016-025556/invocation-inputs.json).

Configured formatting, Clippy lanes, documentation, prerequisites, authority,
public/compile contracts, and Custom **56**, Codex **96**, and OpenCode **306**
deterministic cases passed before the live failure. The verifier's post-live
tail was not reached. All **13** observer connections settled with complete
framing/correlation evidence and no error rows. Every invocation-owned direct
child was reaped and its process group settled; cleanup reported no errors and
the crate-local environment was restored byte-for-byte. Source comparison after
lock restoration matched all **1,557** reviewed inputs; `Cargo.lock` was unchanged.
[Invocation ownership](../../target/storage-feature-split-evidence/scoped-live-20261015-220413/service-ownership.json).

The next unchanged run was not attempted. No Kit or prompt correction was
justified by this capture. Final App **18**, canonical crash, and package/consumer
checks remain pending behind the failed Agent gate. Historical failures,
withdrawn coverage, and the earlier observer receipt limitation remain below.
No push, merge, capacity workload, or unrelated live suite was run.

## Historical approved native refresh — Codex finalized, OpenCode blocked on 2026-10-15

The campaign remains **incomplete**. Codex **0.162.1** completed every configured
updater gate and was finalized in local commit
`49a6cb22e50c9b5e028e99ebc47b2855cd1a6755`. The subsequent OpenCode **1.19.0**
candidate passed its focused release checks but its full Agent live execution
failed: **23 passed, 1 failed, zero ignored or filtered**, in **646.90 seconds**.
The recurring failure is Codex's caller-loss/interactive-tool case. OpenCode's
vendor lock remains **1.17.18**; both OpenCode archives and the uncommitted
candidate are retained. No push, merge or capacity workload occurred.

### Codex full-context diagnostic and completed gates

The [Codex invocation](../../target/storage-feature-split-evidence/scoped-live-20261015-200136/service-ownership.json)
ran the normal verifier lead-in and all **24** Agent cases against Codex
**0.162.1** and unchanged OpenCode **1.17.18**. All **24 passed** in **613.87
seconds**. Configured formatting, Clippy lanes, documentation/doctests,
prerequisites, source/public/compile contracts, Custom **56**, Codex **96** and
OpenCode **299** deterministic cases, and the verifier's post-live tail passed.
Both Question Kit assertions passed; raw exact-copy grades were **2/2**.
[Exact inventory and result](../../target/storage-feature-split-evidence/scoped-live-20261015-200136/qualification-result.json).

The passive observer forwarded the same native generation unchanged. Its
caller-loss connection recorded the accepted tool response, completed tool item,
an eligible nonblank `final_answer` item, and the same item in the terminal
summary. It recorded no overlapping history reconciliation on that connection.
All 13 observed connections settled with a complete terminal observation and no
parser errors. This establishes **non-reproduction** of the earlier missing-output
failure, not its cause or statistical reliability. OpenCode's structured-output
case also passed its original instruction; the failure-only history diagnostic
was therefore not invoked. Neither approved conditional prompt clarification
was applied. [Correlated classification](../../target/vendor-refresh-20261015-fixes/full-context-diagnostic/caller-loss-200136-wire-classification.json).

The updater returned success and created the authorized local vendor commit.
The outer invocation nevertheless remains **INCOMPLETE**: its final observer
signal raised `PermissionError` after the observer had already emitted its
complete terminal receipt. Direct-child waiting was not recorded for that
observer. The other children were reaped and their groups settled. A separate
[post-exit observation](../../target/storage-feature-split-evidence/scoped-live-20261015-200136/post-exit-process-observation.json)
found every exact invocation PID/group absent and every owned port closed.
This does not rewrite the original receipt or fabricate a successful wait.
The ignored owner now polls/reaps an exited leader before signaling its group;
ordinary disposable live/exited-child checks passed while preserving group
absence checks and error reporting. The test environment was restored
byte-for-byte. Process settlement remains distinct from Kit cleanup.

### OpenCode sourced corrections and failed qualification

The pinned **1.17.18 → 1.19.0** source/OpenAPI comparison established two private
wire corrections: provider `interleaved.field` accepts the upstream open string
vocabulary, and exact correlated message parents need not sort lexically before
their children. ID shape, non-self parent, session, role, exact parent/part/tool
correlation and completion validation remain enforced. Meaningful red tests
rejected valid `reasoning_text` and reversed-ID plain/structured replies; a
reversed-ID recovered-question test also failed before the correction.
The configuration rejection test initially exercised construction rather than
conversation creation; correcting that trigger is test setup repair, not a
third production defect. No public API, dependency, model, prompt, assertion,
generation count or deadline changed.

| OpenCode candidate evidence | Result |
| --- | --- |
| Focused release regressions | **7 passed**. |
| Complete event ownership target | **306 passed**, zero ignored/filtered, **853.98 seconds**. |
| Real release configuration/hierarchy | Passed with native **1.19.0**; generation-free additions preserve the original 24-case inventory. Startup records the existing process owner before awaiting launch. |
| Strict-live assertion and Question-grade contracts | Passed; workspace Clippy, including App, also passed. |
| Source authority | Pending: the implementation-stage check read the still-original **1.17.18** lock and failed the target-version assertion. It is not waived; candidate-lock validation must pass in updater completion. |
| Process prerequisite contract | A sandbox-denied process inspection prevented qualification; ordinary runtime readiness passed separately. These are not interchangeable receipts. |
| Full live execution | **23 passed, 1 failed**, **646.90 seconds**; all 24 expected names executed, with no ignored/filtered cases. |
| Question grades | Both Kit contracts passed; exact-copy grades **0/2**. |
| Remaining completion gates | Not completed; no OpenCode finalization or vendor commit. Final App **18**, canonical four-boundary crash, clean-candidate/package and extracted-consumer checks remain pending. |

The failure again reports `NotYetImplemented / Prompt / CompletionValidation /
MissingData` from the Codex caller-loss stream. Retained native SQLite row
**40893** at **2026-10-16 02:29:29 UTC** confirms that native code matched and
decoded a successful dynamic-tool response containing the test's unique answer;
the trace precedes callback-channel delivery. Subsequent rows record method-only
completion and thread deletion. They contain no final item type, phase, text or
turn identity. Native cleanup removed the matching history. No passive wire was
active for this OpenCode invocation. Thus **native omission versus Kit
reconciliation remains unresolved**; successful tool-response decoding cannot
establish eligible final output. No synthetic answer or prompt change was made.
OpenCode's structured-output case passed unchanged on **1.19.0**. Neither this
pass nor the earlier Codex pass changes the historical failures.

After the full suite returned failure and completed its case cleanup, the
coordinator interrupted only the invocation-owned updater group to prevent an
unchanged duplicate qualification attempt. The outer receipt records **FAIL/-2**;
the retained Cargo log records the complete failed suite. All three owned children were reaped
and groups settled without cleanup errors. This is a deliberate stop at a
mandatory failed gate, not a passing or completed updater run.
[Immutable failed-run inventory, classification and log hashes](../../target/storage-feature-split-evidence/scoped-live-20261015-205624/qualification-result.json),
[process settlement](../../target/storage-feature-split-evidence/scoped-live-20261015-205624/service-ownership.json),
and [1,557-file candidate identity including untracked inputs](../../target/storage-feature-split-evidence/scoped-live-20261015-205624/failed-candidate-source.json)
preserve this result independently of mutable dossier files.

Independent review found no remaining correctness issue in the two private
OpenCode changes or the generation-free live ownership changes. A one-line
formatting correction followed the frozen failure receipt; it changes no test
behavior. The source archive SHA-256 values are Codex
`cb647d70f6d36cb447a64d9de43add16b1d2e39b2eeaba58bb4dc146f33cf96e`
and candidate OpenCode
`002d855dec77ff18d8ea9cda4429bddc05e6275f9874db3eca92df2ea4402162`.
Cargo.lock remains
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Source-bound Storage results and withdrawn coverage below remain historical;
this vendor-only work supplies no new Storage or capacity claim.

## Native vendor repair — historical qualification failure on 2026-10-15

The reviewed foreign-path defect is corrected in the existing permission
validator. Forward, backward and mixed UNC separators now follow the pinned
Windows convention; malformed ordinary UNC roots remain `Protocol`, valid
foreign roots remain `Unsupported`, and native Windows/device-namespace handling
is preserved. The existing public protocol test covers these cases. Its protocol-fixture
red runs demonstrated the missing error response and incorrect malformed-path
classification before the corresponding corrections. The final focused case
passed; the final 96-case protocol target also passed during the full qualification
below. [Repair receipt](../../target/vendor-refresh-20261015-fixes/unc-repair.json)
keeps compilation/sandbox setup failures separate from behavioral red evidence.
Independent review found no remaining issue in this correction. Windows
execution is not claimed.

The prior live failure was investigated using retained native SQLite logs.
They prove the caller's tool answer reached native callback handling, but their
method-only events cannot establish final-answer eligibility. One unchanged
exact-case execution then used an invocation-owned passive byte-forwarding
observer, retaining only correlation hashes, item phases/text lengths, callback
success and terminal status. It **passed in 7.91 seconds**, one executed case and
23 intentional exclusions. Native nonempty `final_answer` items preceded the
successful follow-up and interactive-tool turn completions; original exact
output, single-handler and cleanup assertions passed. The observer completed
without truncation and all owned processes settled. No prompt, model, assertion,
deadline or live fixture changed. This is **non-reproduction**, not an established
cause or retroactive pass for the earlier failure. [Exact execution and ownership](../../target/storage-feature-split-evidence/scoped-live-20261015-175728/service-ownership.json),
[sanitized wire evidence](../../target/storage-feature-split-evidence/scoped-live-20261015-175728/wire-facts.jsonl),
and [retained historical evidence](../../target/vendor-refresh-20261015-fixes/live-native-evidence.json)
remain separate.

Every configured Codex updater gate remains required before finalization and
OpenCode refresh. The existing resume owner verified the repaired source and
saved plan without launching another implementation pass; original updater
candidate-lock installation/restoration, validation, archive finalization and
local commit remain authoritative. No push, merge or capacity execution is
authorized.

The first qualification-resume attempt stopped before gates because discovery
found newly released Codex `0.163.0`; its
[receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-175905/service-ownership.json)
records unchanged source and settled owned processes. The user then explicitly
selected **the approved `0.162.1` target**, preserving the saved plan. The
invocation now records newer discovery separately and validates the selected
release metadata/checksums before running unchanged completion gates. OpenCode's
approved target remains `1.19.0`; newly discovered `1.19.1` is not included in
this qualification. These selected versions are not described as today's latest.

The approved-target qualification then **failed** at the mandatory Agent live
gate: **22 passed, 2 failed, 0 ignored, 0 filtered**, in **727.79 seconds**. The
inventory contained the retained 22 cases plus two sourced Codex release cases.
Both additions passed. Workspace and configured Storage default, no-default and
Qdrant Clippy lanes, formatting,
documentation/doctests, prerequisites, public/recovery contracts, Custom's 56,
Codex's 96 and OpenCode's 299 deterministic cases passed before the live gate.
The complete Agent verifier did not finish its post-live tail and is not a pass.
[Invocation and cleanup receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-180100/service-ownership.json),
[exact gate log](../../target/storage-feature-split-evidence/scoped-live-20261015-180100/vendor-update.log),
and [24-case result inventory](../../target/vendor-refresh-20261015-fixes/qualification-result.json).

The two mandatory failures are:

- `codex_prompt_caller_loss_preserves_runtime_owned_cancellation` again reported
  `Prompt / CompletionValidation / MissingData` in the later interactive-tool
  stream. Native callback handling received the supplied answer successfully;
  the retained method-only log cannot establish final-item contents or exact
  turn correlation. Independent source review found no loss or overtaking in
  the ordinary serialized output/terminal path. It did not establish a cause or
  exclude every recovery path. The earlier isolated pass remains non-reproduction,
  and neither full-suite failure is relabeled.
  [Sanitized repeated-failure evidence](../../target/vendor-refresh-20261015-fixes/live-native-repeat-evidence.json).
- `opencode_prompt_model_and_structured_output_are_live` reported
  `Prompt / CompletionValidation / StructuredOutputMismatch` against unchanged
  OpenCode **1.17.18**. This is a failed mandatory result, not evidence that the
  proposed **1.19.0** update has been tested or would fix it. No model, assertion,
  prompt or deadline was changed to accommodate the failure.

The two permitted Question cases passed their Kit assertions; raw copy grades
were **0/1 for delivery** and **1/1 for recovered idle**, totaling **1/2**. Both
managed-knowledge cases passed their mandatory exact-output assertions. These
results do not waive either failure above.

The source and Cargo lock identities were unchanged across the invocation. The
updater restored the original vendor lock byte-for-byte; both archives and the
candidate sources remain available. Codex, OpenCode and updater process groups
were settled and direct children reaped, with no cleanup errors. This records
process settlement separately from the passing Kit cleanup assertions; it does
not turn failed tests into passes. Following the approved stop-on-failed-gate
policy, **OpenCode's refresh and final App/package qualification were not
started**. No vendor commit, push, merge or capacity workload occurred. The
foreign-path fix is retained in the uncommitted candidate; Codex **0.162.1**
remains the selected but unqualified target.

## Native vendor implementation resumption — historical failed live gate on 2026-10-15

The implementation budget was raised, for this invocation only, to **7,200
seconds**; planning remained **1,800 seconds**. The saved plan and partial
changes were resumed after an exact source/archive/dossier snapshot. No Kit,
test or cleanup deadline changed. Implementation completed, but the Codex
refresh remains **incomplete and unqualified**: its mandatory Agent live gate
failed. Under the approved stop-on-first-failed-gate policy, OpenCode's update
was not started. No vendor commit, push or merge occurred.

The [resume receipt](../../target/vendor-refresh-20261014/implementation-resume-20261015-150035/receipt.json)
preserves the prior dossier, partial diff and source hashes. The
[implementation handoff](../../target/vendor-updates/agents-kit/codex/rust-v0.162.1/resume-implementation-handoff.json)
records focused red/green results and the completed private adaptation for
Codex `0.162.1`: input blocking/approval semantics, typed notifications and
errors, asynchronous messages, function outputs, final-answer phases,
personality availability and permission locations. Public APIs, Cargo
dependencies and durable formats are unchanged. The selected runtime was
Codex `0.162.1`; OpenCode remained `1.17.18` throughout this invocation.

| Current execution | Result |
| --- | --- |
| Configured formatting and Clippy gates | Passed: workspace formatting, non-Storage workspace Clippy, and Storage default, no-default and Qdrant Clippy lanes. |
| Complete documentation gate | Passed: inventory/link checks, all five crates' rustdoc/doctests and both compatible maximal Storage documentation lanes. These were documentation checks, not Provider/Tool live suites. |
| Agent pre-live verifier | Passed: real prerequisites, candidate-lock protocol/source authority, static/public/compile contracts, lifecycle/cleanup tests, Custom runtime **56**, Codex protocol **96**, and OpenCode ownership **299** cases. |
| Agent live inventory | **23 passed, 1 failed**, zero ignored/measured/filtered, **653.84 seconds**. All 22 retained cases and two new Codex native-request cases executed. Both new cases passed. |
| Question grades | Kit assertions and scenario cleanup **2/2**; raw exact-copy grades **2/2**, using OpenCode `1.17.18`. These grades do not waive the other failed live case or establish general model quality. |
| Final campaign checks | Blocked: remaining Agent post-live verifier stages, Codex finalization, OpenCode refresh, final App **18** workflow cases/canonical crash, and applicable package/extracted-consumer checks. |

The failed case is
`strict_live_suite::prompt_and_io::codex_prompt_caller_loss_preserves_runtime_owned_cancellation`.
Its caller-loss abort and unary follow-up had returned before the later streamed
interactive-tool turn failed with `NotYetImplemented / Prompt /
CompletionValidation / MissingData` and `recorded prompt operation failed`.
Source inspection locates this outcome at completed-turn validation without a
stored eligible final answer. The retained log cannot distinguish failure
before interaction delivery from failure after resolution. It does not prove
a cancellation defect, fixture defect, or native/model cause. No correlated
native completion payload is retained. One native WebSocket-reset warning lacks
thread/turn correlation and cannot establish causation. Both mandatory knowledge
cases and the other Codex streaming/structured-output cases passed.

The [independent actual-diff review](../../target/storage-feature-split-evidence/scoped-live-20261015-150312/review-before-completion.json)
also retains one actionable **P2** issue: forward or mixed-separator UNC
locations can pass Unix absolute-path handling and become actionable permission
notifications, contrary to the pinned path convention. The correction and its
focused regression remain outstanding. No final review approval is claimed.

The [campaign receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-150312/service-ownership.json)
records updater exit **1** (failed live Cargo command **101**) and all
invocation-owned children reaped/process groups settled without cleanup errors.
This process settlement is separate from the failing scenario's uncompleted Kit
assertions. The updater restored the original vendor lock byte-for-byte:
Codex `rust-v0.144.1`, OpenCode `v1.17.18`. Original and candidate archives remain
intact; source edits still target the candidate and therefore are not a finalized
vendor update. The [failed-candidate receipt and patch](../../target/storage-feature-split-evidence/scoped-live-20261015-150312/failed-candidate.json)
preserve 1,556 tracked/nonignored untracked file hashes, unchanged Agent handoff
files, archive checksums and final results. Cargo lock identity remains
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.

Execution retained Rust **1.94.1**, `--locked`, serial Cargo commands and one
build job on macOS/APFS. Before the Agent verifier sourced its ignored
environment, `RUST_TEST_THREADS=1` was made explicit; the
[environment receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-150312/serial-test-environment.json)
records both hashes and the exact phase. No credentials, model selections,
native versions, deadlines or test selections changed. No capacity workload,
Provider/Tool live suite or workspace live wrapper ran. Historical timeout,
qualification-failure and withdrawn-coverage records remain unchanged below.

## Native vendor refresh resumption — historical timeout on 2026-10-15

The refresh remains **incomplete and unqualified**. The invocation-only planning
override was raised to **1,800 seconds**; implementation remained **900 seconds**.
Planning completed and produced the retained Codex
[plan](../../target/vendor-updates/agents-kit/codex/rust-v0.162.1/plan.md), but
implementation then exhausted its unchanged 900-second limit. The campaign
stopped before updater completion gates and before the OpenCode update. No
budget was increased automatically.

Before resumption, the original dossier and its file hashes were preserved in
the [resume snapshot](../../target/vendor-refresh-20261014/resume-20261015-113053/receipt.json).
The previous report alone was committed locally as
`cd236331f7de87203027d1c623a7cc87ce61e31f`, allowing the updater to start with a
clean tracked tree. The existing candidate archive, extracted sources, dossier,
verified runtimes, isolated environment and process owner were reused. Stable
discovery again selected Codex `rust-v0.162.1`; OpenCode's prepared target remains
`v1.19.0`. During this invocation, the native processes used Codex `0.162.1` and
the unchanged OpenCode `1.17.18`.

The implementation recorded meaningful failing protocol cases before adapting
the selected version, terminal-error decoding, blocking input policy and terminal
input approvals. An **intermediate** `codex_prompt_stream` execution passed all
78 then-present tests, with zero ignored or filtered cases; the target-version
strict-live prerequisite check also passed. These results do **not** qualify the
interrupted final source: later tests exposed remaining personality, asynchronous
question and notification drift, and the subsequent source edits were not built
or tested before timeout. The incomplete changes remain uncommitted for explicit
continuation. No successful vendor-update commit was created.

The [independent read-only review](../../target/storage-feature-split-evidence/scoped-live-20261015-113105/independent-review.json)
confirms unfinished adaptation: asynchronous agent-message fields still lack
live/history handling, `functionCallOutput` lacks its planned private item path,
and nonlocal permission URIs retain the old error classification. Interruption
recovery, shared-item callback correlation and malformed-error regression
coverage also remain incomplete. This is an incomplete-candidate review, not
approval; no final compilation result is claimed.

The [campaign receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-113105/service-ownership.json)
records the two timeout settings, exit 1, unchanged Cargo lock identity, and
successful reaping/settlement of the updater and both invocation-owned server
process groups, with no cleanup errors. The
[interrupted-candidate receipt](../../target/storage-feature-split-evidence/scoped-live-20261015-113105/interrupted-candidate.json)
retains final file hashes and an accompanying patch; raw red/green logs and the
complete updater log remain available. The vendor lock still pins Codex
`rust-v0.144.1` and OpenCode `v1.17.18`: timeout occurred before candidate-lock
installation, so restoration was unnecessary. Both original archives and the
candidate Codex archive remain intact.

Agent's complete retained live inventory, final App workflow-live/crash recovery,
package verification and completion lint/documentation gates remain outstanding
for these new sources. No capacity workloads, Provider/Tool live suites or
workspace live wrapper ran. Nothing was pushed or merged. The historical
300-second planning failure below remains failed evidence, and earlier
qualification does not qualify this partial vendor adaptation.

## Pre-merge native vendor refresh — historical stop on 2026-10-14

The subsequently approved Codex/OpenCode refresh is **incomplete**. The selected
targets are Codex `rust-v0.162.1` and OpenCode `v1.19.0`; the checked-in pins remain
`rust-v0.144.1` and `v1.17.18`. No Agent production code or public API changed.
Earlier qualification below continues to describe its recorded native versions,
not the proposed new releases.

Local commit `8b7d955f4a2abd221ef645f013acd21101926cc1` repairs only updater
prerequisites: Agent Cargo resolution gates use `--locked`; post-update gates
inspect the candidate vendor lock; failed validation or keyboard interruption
restores the original lock while retaining both archives. Restoration failures
are reported alongside the primary failure. Focused regressions recorded the old
behavior before correction; all **29 updater/protocol tests and five documentation
tests passed**. Independent actual-diff review found no remaining actionable
issues. [Prerequisite receipt](../../target/vendor-refresh-20261014/prerequisite-receipt.json)
and [test log](../../target/vendor-refresh-20261014/updater-final-green.log) retain
the identities and results.

The official macOS ARM runtime assets were downloaded and their published SHA256
digests verified; [asset identities](../../target/vendor-refresh-20261014/runtime-assets.json)
are retained. Codex `0.162.1` initially rejected the retired task-local
`approval_policy="untrusted"` startup override. Removing that override, as directed
by the target release's configuration source, allowed startup. The isolated
OpenCode XDG configuration also hid GitHub CLI's existing authentication; the
updater was explicitly pointed at its original GitHub configuration without
copying credentials. These prerequisite failures remain recorded.

The Codex updater then downloaded source archive SHA256
`cb647d70f6d36cb447a64d9de43add16b1d2e39b2eeaba58bb4dc146f33cf96e`
and created its [dossier](../../target/vendor-updates/agents-kit/codex/rust-v0.162.1/dossier.md).
Its planning execution **timed out after the existing 300-second limit**, before
producing a plan or running implementation/completion gates. Under the agreed
stop-on-first-failure rule, OpenCode's updater was not started. Both old source
archives, the candidate Codex archive and the incomplete dossier remain intact.

The [campaign receipt](../../target/storage-feature-split-evidence/scoped-live-20261014-223753/service-ownership.json)
records exit 1, source identities including the untracked candidate archive,
unchanged Cargo lock SHA256, and successful reaping/settlement of the updater and
both invocation-owned server process groups with no cleanup errors. Native TCP
readiness and binary-version checks are preparation evidence, not protocol or
strict-live qualification. No new Agent/App live, package, capacity, Provider/Tool
live or workspace-wrapper execution occurred. Nothing was pushed or merged.
The report update remains separate from the local prerequisite commit.

## Scoped feature-split closeout — 2026-10-14

**The Storage feature split and Lance-only downstream migration are qualified
within the revised task scope.** The current Agent inventory and final package
checks pass. This is not a whole-workspace release, maximum-capacity, or
normalization-performance qualification.

Execution used clean commit `f4b9778cd42154567ccbd458db95914b60c188ba`, candidate
SHA-256 `c458e93c1c6d9048e0d3296faa2914f0a6de749694d1ffe0a6114747fb0b5da2`,
and lock SHA-256
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Source identity includes nonignored untracked files and excludes historical
`tmp/**`. Rust 1.94.1, native `--locked`, one Cargo command/build job at a time,
the existing low-memory settings, and macOS/APFS were retained. Source and
invocation inputs stayed unchanged throughout execution.

| Final execution | Result and evidence |
| --- | --- |
| Agent strict-live | [All 22 cases passed](../../target/storage-feature-split-evidence/scoped-live-20261014-151713/receipts.json): 21 behavioral cases plus the environment contract, zero ignored or filtered cases, 527.26 seconds. The existing precheck and exact compiled inventory passed. No App or Provider/Tool live suite was invoked. |
| Native output and cleanup | [Service and knowledge receipts](../../target/storage-feature-split-evidence/scoped-live-20261014-151713/service-ownership.json) confirm all six knowledge generations match native, Provider and Agent output. Both invocation-owned services were reaped and settled; the existing relay joined; no cleanup errors were recorded. Kit cleanup assertions and supervisor process cleanup remain separate evidence. |
| Question grading | Kit assertions and cleanup passed **2/2**. Raw exact-copy grades were **1/2**: question-delivery passed; native-idle failed its existing nonblocking copy grade. [Grade output](../../target/storage-feature-split-evidence/scoped-live-20261014-151713/agent-strict-grades.log) remains visible. No other output assertion was relaxed; this is not model-quality acceptance. |
| Five native packages and clean candidate | [Verification passed](../../target/agent-app-repair-evidence/packaging-20261014-152710/receipts.json): fresh compilation of the five local Kit libraries from a 1,555-file candidate snapshot, native five-package verification before consumer patching, archive/source and sibling checksum checks, and the extracted Agent/App consumer's real Lance workflow and joined shutdown. Native dependency build caches were reused; this is not an empty-cache build. [Package details](../../target/agent-app-repair-evidence/packaging-20261014-152710/package-details.json) retain each archive and resolved graph identity. |
| Standalone Storage package | [Verification passed](../../target/storage-feature-split-evidence/packaging-20261014-153007-60e97132/receipt.json): default/no-default graph equivalence, Lance-only Agent/App production graphs, native package verification, extracted-consumer memory/directory persistence, conflicts, tombstones, reopen, knowledge coexistence and direct traits, plus five separate explicit backend opt-in builds. Opt-in compilation is not alternative-backend live execution. |

Both packaging runners preserve their initial stale disposable consumer-lock
probe failures as preparation evidence. Offline seed preparation was audited
before the subsequent locked metadata, builds and consumer executions passed.
The workspace lockfile was unchanged. All owned commands settled; the
five-package runner completed its local-package cache cleanup. Nothing was
published.

The [independent live review](../../target/storage-feature-split-evidence/scoped-live-20261014-151713/independent-review.json)
confirms the inventory, output fidelity, grades, source identities and cleanup.
The [final closeout review](../../target/storage-feature-split-evidence/final-closeout-20261014/independent-review.json)
audits packages, source continuity and these documentation changes.

Previously passing Storage default gates on
[macOS/APFS](../../target/storage-feature-split-evidence/storage-default-final-20261013-192659/receipt.json)
and [Linux/ext4](../../target/storage-feature-split-evidence/linux-final-20261013-193403-2196adb5/receipt.json)
remain applicable to unchanged Storage production/tests/scripts. The
[eight-lane, 24-legal-subset and eight-illegal-union receipt](../../target/storage-feature-split-evidence/storage-all-20261011-134317/receipt.json)
remains historical evidence for unchanged feature wiring and alternative owners;
it was not rerun after later Lance changes. Those changes have the separate
default-platform evidence above. App's
[18 live cases and four-boundary crash case](../../target/storage-feature-split-evidence/scoped-live-20261013-210125/receipts.json),
[static/public/example/doctest checks and stack regression](../../target/storage-feature-split-evidence/app-static-20261013-194655/receipts.json),
and [218 selected routine cases](../../target/app-workload-checks/20261013-203543-routine-457/receipt.json)
retain their source-compatible passes; they were not executed again here.
The older aggregate live receipt remains **FAIL**, despite its passing App rows.

Commit `f4b9778c` implements the user's withdrawal of the repaired-abort and
4,096-delivery test scenarios and their exclusive fixtures. Their historical
failures are preserved, including the 2,222-resolution timeout and latest
1,449.344-second failed screen. Neither scenario became a pass or a demonstrated
production repair through deletion. Maximum-capacity work is outside this
feature-split completion scope. No capacity workload or diagnostic prefix was
run; the other five dedicated maximum-capacity cases remain separately owned.

Only this report and two owning Storage status documents changed after execution.
The [source comparison](../../target/storage-feature-split-evidence/final-closeout-20261014/source-comparison.json)
binds all compiled inputs to the executed and packaged candidate. It also records
the exact differences from the retained earlier evidence. The final report bytes
were not repackaged and supply no new behavioral result. No production code,
tests, fixtures, public APIs, manifests or lockfile changed. No commit or push
was performed during this closeout.

## Maintenance directory-sync batching — recorded status on 2026-10-13

This section preserves the result before the October 14 test withdrawal and
closeout above. Its unresolved and mandatory-failure statements describe that
recorded inventory, not the current feature-split gate.

**The fixed comparison supports retaining this measured private improvement. A mandatory Agent live failure blocks release qualification, and the unchanged growth screen rejects a 4,096-delivery attempt.** No full-capacity execution followed; neither production correctness nor release completion is established.

The measured candidate is `266e8607a1f0f3ce5b96a973df81bb9713fa1e84e4785ace06a74578d263c081`; the fresh baseline is `3e9ac76e9c368433fee0a9928619481c7d14f93e3fa2b2abfead36fda3222148`. Both include untracked candidate files and retain lock SHA `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`. Execution used Rust 1.94.1, `--locked`, serial Cargo, one build job and ordinary macOS/APFS. Capacity artifacts retain assertions and overflow checks. Source, fixture, artifact, runtime and invocation-input identities remained stable within each recorded run.

Only `DurableIo`'s existing ledger and maintenance call sites changed: at most 64 same-parent retained descriptors share complete ancestor synchronization while every original file barrier remains. Publication guards, freshness, leases, maintenance policy, formats, APIs, dependencies, counts and deadlines remain unchanged. App and fixtures are byte-identical between measured variants. The [final independent production review](../../target/maintenance-directory-sync-evidence/final-production-review.json) passes with no blocking source findings: sticky failures, complete pending-file barrier attempts and final settlement under exclusive protection remain intact. Its fresh comparison of all 1,555 candidate files finds only two private Storage production changes and four Storage documents; all 28 App production files, 771 test-tree files, APIs, manifests and lockfile match the baseline. The sole new type is the private data-carrying reclamation state within the existing I/O ledger; no new worker, cache, fixture owner or production hook was added.

The baseline passed all 89 public StateStore cases. [Candidate focused qualification](../../target/maintenance-directory-sync-evidence/focused-20261013-184011-81007/qualification.json) passed, separately in test and capacity profiles: 16 native durability cases, 12 native transaction cases, 89 supplied-StateStore cases and seven exact App cases. One native phase diagnostic was explicitly filtered from semantic execution. All four existing diagnostic calibrators passed on each frozen variant outside timed samples.

The fixed ABBA comparison preserves all eight samples, including slow observations. Times below use A1/B1 and A2/B2 pairing:

| Measured interval | First pair: baseline → candidate | Second pair: baseline → candidate |
| --- | ---: | ---: |
| Storage commit through first observation, three profiles combined | 16.314 → 13.516 s (17.147% lower) | 19.045 → 17.539 s (7.905% lower) |
| App 256-delivery body plus original joined shutdown | 72.072 → 50.218 s (30.322% lower) | 62.391 → 49.723 s (20.304% lower) |

All six individual Storage profile comparisons and both Storage whole-child comparisons also improve. Every App sample resolves 256 targets and reaches checkpoint 770. [The retention review](../../target/maintenance-directory-sync-evidence/retention-decision.json) retains raw measurements and resource tradeoffs: Storage peak RSS rises **28.419% and 17.612%**; App peak RSS rises **0.689% and 4.782%**. These per-child peaks do not attribute memory to the descriptor vector. Inclusive timings overlap; they cannot isolate filesystem or CPU causes. Two samples per variant do not establish statistical reliability. Final file counts and retained versions match across variants; metadata bytes vary slightly.

Both subsequent 1,024-delivery diagnostics pass their assertions and original joined shutdowns, with complete costs **207.792 s** and **210.869 s**. However, slower 256-block costs of 49.001/50.403/52.651/58.283 s, growth increment 5.632 s, setup 0.314 s and the unchanged 100-second allowance produce **1,449.344 s**, exceeding the 1,200-second admission threshold. The [independently reviewed screen](../../target/maintenance-directory-sync-evidence/growth-screen-independent-review.json) therefore says **DO_NOT_RUN_4096**. This is a failed screening heuristic, not an observed new 4,096 timeout or a capacity pass.

The historical **2,222 public-resolution timeout**, **1,816.833-second screen**, standalone normalization's mixed ABBA failure and both earlier rejected maintenance policies remain unchanged evidence. This later, separately scoped comparison does not relabel them.

Native probes establish their exercised native primitives; supplied-adapter cases establish public Storage behavior. Private batching bounds and barrier order additionally rely on source proof. Deterministic App cases use real Lance with caller Agent fixtures, not real-Agent strict-live evidence. Kit joining comes from behavioral assertions; invocation-owned process reaping is separate. Neither establishes hardware power-loss durability.

| Release evidence | Recorded status |
| --- | --- |
| Storage platform gates | [macOS/APFS default gate](../../target/storage-feature-split-evidence/storage-default-final-20261013-192659/receipt.json) and [Linux/ext4 default gate](../../target/storage-feature-split-evidence/linux-final-20261013-193403-2196adb5/receipt.json) pass on unchanged measured source. Linux uses a fresh 1,555-file snapshot and build directory, default Docker security and uid/gid 65534 without supplementary groups. Owned processes/container settled. These are default Lance gates, not a repeated alternative-backend matrix. |
| App static and routine scope | [Static invocation](../../target/storage-feature-split-evidence/app-static-20261013-194655/receipts.json) passes 632 cases across 34 runtime targets, the separate compile contract, seven examples, check/Clippy, strict rustdoc, 127 doctests, formatting and boundary/path/alias guards. Only the canonical crash remains separately owned by scoped live. The [corrected routine invocation](../../target/app-workload-checks/20261013-203543-routine-457/receipt.json) also passes all 180 selected workflow and 38 selected worker cases on unchanged source, with settled owned processes. Its 11 workflow and one worker exclusions remain explicit; diagnostic/calibration and dedicated capacity evidence are separately owned. |
| Scoped Agent/App live and crash | The [settled scoped invocation](../../target/storage-feature-split-evidence/scoped-live-20261013-210125/receipts.json) is **FAIL**: Agent **22 passed / 1 mandatory repaired-abort failure**; App live **18/18 passed**; canonical four-boundary crash **1/1 passed** (three unrelated cases filtered). Agent/App live have no ignored or filtered cases. Both Question Kit contracts passed; raw exact-copy grades are **0/2**, nonblocking only for those two named cases. The [final independent audit](../../target/maintenance-directory-sync-evidence/final-live-review.json) confirms all six knowledge generations have correct request markers and identical native/Provider/Agent output; both knowledge cases pass this invocation without erasing prior failures. Source and inputs are stable; relay and all three service groups settled and reaped. Process cleanup does not erase the failed case's Kit cleanup errors. Provider/Tool live and the five-crate wrapper remain outside this authorization. |
| Native packaging and extracted consumers | [Clean candidate and five-package verification](../../target/agent-app-repair-evidence/packaging-20261013-212359/receipts.json) pass, including candidate sibling archive checksum checks and an extracted Agent/App consumer with real Lance persistence and joined shutdown. [Storage package qualification](../../target/storage-feature-split-evidence/packaging-20261013-212752-2aeb3c67/receipt.json) passes default/no-default graph equivalence, minimal downstream production graphs, the extracted default consumer, and five separate explicit backend opt-in builds. Disposable consumer lock preparation is recorded separately; native package verification and release checks use `--locked`. Nothing was published. |

The [bounded live triage](../../target/maintenance-directory-sync-evidence/repaired-abort-live-triage.json) preserves the repaired-abort failure: the first Kit abort result was a cancellation timeout where `InvalidState` was required. Notification drain, conversation/workspace closure and Agent shutdown also reported failures; later idle-abort and subsequent-generation assertions were not reached. The completed Agent test process group settled, which does not establish graceful Kit cleanup. Existing logs do not identify a Storage error or attribute the failure to maintenance, native restart, transport or Kit reconciliation. No production or fixture defect is established by this result, and the failure remains mandatory; it is outside the Question exact-copy exceptions. No retry, relaxed assertion or deadline change is credited.

The [first routine launch](../../target/app-workload-checks/20261013-203208-routine-94476/receipt.json) remains **INCOMPLETE**: it began a test-profile build without the required low-memory environment overrides, was interrupted during compilation, and ran no tests. The owned build process group was terminated and reaped with no cleanup errors; source remained unchanged. The corrected launch restores `CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_TEST_DEBUG=0` and `CARGO_PROFILE_DEV_DEBUG=0`. This operational correction is not a PASS for the interrupted invocation.

The [first scoped-live startup](../../target/storage-feature-split-evidence/scoped-live-20261013-210032/service-ownership.json) remains **INCOMPLETE**: the sandboxed Codex launcher returned `Operation not permitted` before readiness or any test execution. Its owned process was reaped and process group settled, with source and launcher inputs unchanged. This is a setup failure, not a failed Kit assertion or live PASS; the separately authorized run above supplies its own final FAIL receipt.

Authorized cleanup removed [**22.254 GiB** from five obsolete Linux build caches](../../target/project-linux-build-cleanup-20261013.json), then [**4.3826 GiB** from this completed Linux invocation's build cache](../../target/project-linux-completed-build-cleanup-20261013.json) after its bound PASS receipt and ownership checks. Separate [host cleanup](../../target/project-space-cleanup-20261013.json) reduced allocated target bytes by **9.207 GiB**. After routine settlement, [430 exact artifacts from the interrupted build](../../target/maintenance-directory-sync-evidence/wrong-profile-cleanup-20261014-020019-2140.json) were removed, reclaiming a further **1.016865 GiB** of allocated bytes; [independent verification](../../target/maintenance-directory-sync-evidence/wrong-profile-cleanup-20261014-020019-2140-verification.json) retained all 999 protected current-build artifacts. Source, datasets, Cargo homes, logs, unique historical archives and frozen measurement inputs were preserved. These are recorded allocated-byte changes, not a performance explanation or a promise of currently free space. No commit or push was performed.

Unchanged alternative-backend and feature-matrix evidence remains scoped to its original inputs. The [release-scope comparison](../../target/maintenance-directory-sync-evidence/release-scope-and-package-readiness-review.json) confirms the unchanged Storage manifest/workspace lock and all 120 non-Lance-StateStore production files; it does not claim that the historical eight-lane execution reran or qualifies changed Lance internals.

The final report/status edits occur after execution and change only three Storage documents. Their separate source comparison binds the unchanged compiled inputs to the executed candidate above. These documentation edits supply no new test result. The measured improvement remains retained, while the mandatory repaired-abort case and full capacity remain unresolved. No 4,096 run, fixture expansion, commit or push followed.

## App test simplification (2026-10-13)

**Scoped simplification validation passed.** This supplies no new maximum-capacity
qualification. The frozen starting candidate
is recorded in `target/app-test-simplification/baseline.json`, including 1,555
tracked and untracked candidate files (excluding immutable `tmp/**`). Its source
manifest digest is `5c64352b7e332fd0e0ae3f97f8fddd7dbd3009b30da6f26f5a635e003cdd2cd7`;
the lockfile remains `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
This scope changes no production code, APIs, workload counts or deadlines.

The preservation map and reviews live under `target/app-test-simplification/`.
Two duplicate 130-step memory deletion entrypoints are removed; their canonical
cases use the same helper, backend and faults, and distinct directory cases stay.
The successful tracing journey now applies both span and event/redaction
assertions once. The canonical 96-delivery history journey always checks v7 page
identity, immutable reuse and 1/63/64/65 boundaries, retaining all replay, history,
restore, deletion, export and cleanup assertions. Only the secondary snapshot
taken after a failed lower-entry observation is removed; normal observation,
25 ms whole-activation and 200 ms lower-entry cases remain.

Four instrumentation calibrators move from routine execution to mandatory exact
diagnostic preflight outside timed samples. Routine inventory changes from 223
to 218 (180 workflow plus 38 worker); deletion changes from 32 to 30 and
observability from 14 to 13. These counts describe selection, not new execution
results. Compile-only contracts stay in all-target checks and Clippy. The strict
wrapper credits one explicitly inventoried and successful current-invocation
crash journey; standalone scoped live execution remains available.

Fixture validation (process-owner, receipt-lifetime and real conflict-controller
checks), behavioral integration, diagnostics, real-service live evidence and
capacity remain distinct. The entire saved-order stack regression and all 18
App live cases are protected. No capacity workload is executed by this cleanup.
The 2,222-resolution timeout and latest 1,816.833-second screen remain failed
capacity evidence; 4,096 stays prohibited until screening passes.

Execution candidate: `5cb650ed0f0ef9c156376856f1dc55d21b92bf10dbaeafd6142b1f1ee5eb02e9`,
Rust 1.94.1, locked serial Cargo with one build job, ordinary macOS/APFS. All
399 crate production Rust source files, manifests/lockfile, full child-composition stack
scenario, live suite, crash fixture and worker fixture remain byte-identical to
the simplification baseline. No new Rust owner, fault control or runtime fixture
was introduced. Independent source review found no lost assertion.

| Fresh execution | Result | Receipt |
| --- | --- | --- |
| Deletion target | 30/30; canonical memory and distinct directory journeys preserved | `target/app-test-simplification/20261013-173259-affected/receipts.json` |
| Observability target | 13/13; both success assertions and every panic callback | Same affected receipt |
| Focused workflow behavior | 11/11; 96 history deliveries, routine 64, eight lower-entry/panic cases and separate 25 ms case | Same affected receipt |
| Diagnostic instrumentation | 4/4 exact checks; fixture validation, not App behavior | Same affected receipt |
| Stack regression | 1/1; unchanged shared scenario on default stack | Same affected receipt |
| Other preparation cases | 3/3; canonical crash explicitly excluded from this invocation | `target/app-test-simplification/20261013-175410-prepare/receipts.json` |
| App live | 18/18; native/service assertions and existing controlled-failure cases unchanged | `target/storage-feature-split-evidence/scoped-live-20261013-174347/receipts.json` |
| Canonical crash | 1/1 exact parent case, all four crash boundaries; executed once | Same scoped-live receipt |

Formatting, all-target compilation (including the zero-runtime compile contract),
Clippy with warnings denied and both architecture guards pass. Runner orchestration
passes 35 checks, retaining all 32 earlier checks. Shell orchestration passes three
checks with missing-inventory, command-failure, ignored/filter, empty-execution,
failed-count and missing-crash negative cases. These disposable command stubs
prove routing only; they are not App/live execution. Meaningful old-runner and
old-shell failures are preserved in `workload-runner-red.log` and
`shell-scope-red.log`; the new checks do not manufacture a product failure.

The current four-case preparation inventory is present. Its three ordinary cases
and one canonical crash case all ran once during this qualification. The shell's
same-invocation full-target credit and failure propagation were checked separately
by orchestration tests. No complete downstream verifier or workspace-wide live
wrapper was run. The 218-case routine selection is documented, but this task
executed only the affected cases listed above, not that entire selection.

All execution receipts retain stable source identities and clean supervisor
settlement. The live service owner additionally records clean invocation-owned
process reaping in `service-ownership.json`; Kit joining comes from the retained
behavioral cleanup assertions, not process reaping alone. Native App/Lance crash
recovery is distinct from real-Agent behavior. The final report-only edit is
recorded separately from the executed source; it changes no compiled input.

Preservation and independent reviews: `preservation-map.json`,
`workflow-independent-review.json`, `runner-independent-review.json`,
`docs-independent-review.json` and `final-combined-independent-review.json` under
`target/app-test-simplification/`. No performance diagnostic, maximum workload,
Storage matrix, Provider/Tool live suite, commit or push was performed. The
existing capacity block and unrelated historical qualification limitations remain.

## Settlement normalization and maintenance — active implementation

**Incomplete.** This is a new implementation and qualification effort, not a
replacement receipt for the failed capacity case. The original full run ending
at **2,222 public resolutions** and the **1,803.621-second** screening result
remain failed admission evidence. No new full-capacity attempt is authorized
until the unchanged screen passes.

Normalization result: **25 focused semantic cases pass**, but normalization's fixed
256-delivery ABBA **fails the paired complete-cost gate**. One pair improves
8.646%; the other regresses 11.090%. That standalone comparison remains failed
and is not retroactively qualified. The later combined normalization plus
preparation-write candidate passes a separate fixed comparison below; it is
retained as a measured candidate. The subsequent App failure repair below passes
its scoped deterministic, live, packaging and Clippy checks. Overall qualification
remains incomplete because full capacity is still unproved. Its two 1,024-delivery diagnostics pass,
but the **1,816.833-second** screen rejects a full 4,096 attempt. Both maintenance
policies remain rejected as recorded below.

The subsequent **single bounded commit/observation diagnostic passed**, but
established no safely removable production operation. Capacity remained
unresolved at that stop. A later authorization opened the separate preparation-file
durability experiment below. The bounded diagnostic is not performance evidence
for that candidate. The subsequently authorized comparisons and growth runs have
separate receipts below; no full-capacity attempt followed. See the bounded
diagnostic receipt for its original scope.

The owning [App persistence specification](../../app-kit/specs/STATE_STORE.md)
now authorizes cold, one-way `app/v6` → `app/v7` migration and immutable
occurrence-owned settlement pages. Public APIs, logical limits, workload counts,
operation deadlines and Storage's persisted format remain unchanged.

Code growth is constrained to existing domain owners: `RecordCodec` owns the
complete run codec, `AppStore` owns migration and coherent hydration,
`AppMutation` owns atomic publication, the existing deletion owner removes
occurrence-owned pages, and `LanceMaintenance`/`LanceBatches` own reclamation and
selected-row preparation in the rejected maintenance pilots. Those pilot paths
are not retained in the current candidate. The only new persisted entity is a settlement page;
the App root has one closed migration state. No versioned copies of the full
run schema, secondary cache, worker, queue, retry framework or strategy selector
are approved. Independent review must account for each added type and the old
path it replaces.

### Preparation-file durability — measured, release qualification incomplete

**Timeout qualification correction (2026-10-13):** the approved gate now
accepts either successful observation or an actual commit observation timeout,
while requiring at least one settled, independently verified commit. Fixed
8/16/32/64 MiB attempts, the 100 ms timeout, early stop after an actual timeout,
atomic publication and joined cleanup remain unchanged. All setup attempts
timing out still fails. The lifecycle test records bounded outcome counts;
it adds no delay, production hook or substitute response.

Separate actual timeout evidence remains the original test-profile 8 MiB
receipt below, credited only while its production and dependency hashes match.
It does not claim optimized-profile timeout occurrence. The historical failed
aggregate remains unchanged. Fresh exact tests and focused qualification now
pass under this corrected gate. The fixed preparation and combined-candidate
comparisons pass; growth screening still prohibits the full workload. Results
below distinguish the historical failure from current evidence.

**Measured improvement retained; capacity qualification remains incomplete.**
The App repair section below resolves the recorded assertion failures, production
stack overflow and Clippy findings with fresh scoped checks. Storage platform
receipts retain their unchanged-source scope. Only the private directory-write
policy and shared native-file classifier changed in Storage production during
this experiment; the later App stack correction is separately reviewed below. Existing App normalization/migration and
earlier uncommitted work remain intact. Public APIs, errors, dependencies,
formats, feature policy, deadlines, workload counts, native publication guards,
maintenance thresholds and ownership are unchanged.

Recognized native data, deletion and transaction files use non-replacing
hard-link installation followed by staging unlink, the complete ancestor
synchronization and the retained-file barrier. Manifest/hint/copy writes retain
their existing barriers. Preparation collisions are sticky failures in the
existing ledger. The [ownership and recovery proof](lancedb-state-store-prototype.md#preparation-file-durability-candidate--2026-10-13)
states its valid-history/cooperating-owner assumptions and power-loss limitations.
No worker, journal, cache, runtime framework or production hook was added.

Before production edits, the optimized baseline and exact source including
untracked files were frozen in
`target/preparation-durability-evidence/baseline-20261012-235600/`.
The meaningful native red prepared two real transactions from the same version
with the same UUID. The losing commit removed the winner's transaction file.
`red-confirmed.json` preserves that failed result; it is native extension-point
evidence, not direct collision injection into the supplied adapter.

The first focused execution, before the timeout-policy correction, used Rust **1.94.1**, `--locked`, one build
job and one Cargo command at a time, on macOS/APFS. Effective capacity
optimization, assertions and overflow settings were checked from Cargo artifact
messages. Exact executable inventories were checked; no selected case was
ignored. These historical results remain unchanged:

| Scope | Test profile | Capacity profile |
|---|---:|---:|
| Native durability | 16 passed | 16 passed |
| Native transaction semantics | 12 passed | 12 passed |
| Supplied StateStore | 89 passed | 88 passed, 1 failed |
| Focused App lifecycle/receipt/history cases | 7 passed | 7 passed |

The native phase-measurement case was explicitly excluded from these semantic
runs. The App selections include the unchanged 64-delivery routine, lost
receipts, Unknown reconciliation, page corruption, cancellation, retained
shutdown failure and directory fencing/reconstruction. They use real Lance and
deterministic caller Agent fixtures; they are not real-Agent strict-live results.

The optimized failure is
`actual_commit_observation_timeout_keeps_atomic_publication_authority`.
Its fixed four 8/16/32/64 MiB attempts produced **no actual consuming-commit
timeout witness**. Each attempt reaching finalization verified its atomic
receipt after joining and reopening. The log does not distinguish every setup
outcome or record per-attempt durations; it cannot establish that every commit
took less than 100 ms. Tokio also polls the response before its timer. This
failure demonstrates missing required coverage, not a new production correctness
defect. The unchanged test-profile case did observe the real timeout on its
first 8 MiB attempt, then verified the receipt and joined cleanup. That passing
result does not relabel the optimized failure.

The complete focused receipt is
`target/preparation-durability-evidence/focused-20261013-002052/qualification.json`:
**FAIL**, unchanged source during execution, no incomplete process cleanup.
It binds candidate digest
`3717248c840caca4308aec2553e8bcdf0945edf10aa62f06b14536375e24b476`
and lock digest
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Per-command logs preserve primary exits, exact counts, process cleanup and Kit
assertions separately. Subsequent documentation edits do not change those
executable sources. Earlier compile corrections and a new native test's encoded
path-separator mistake remain in the failed focused receipts; the corrected
test passes without changing the production candidate or weakening assertions.

Independent actual-diff review is recorded in
`target/preparation-durability-evidence/review.json`; it checks collision-ledger
handling, copy exclusion, multipart routing, error precedence and protection
lifetime. Focused Storage Clippy with denied warnings, changed-file formatting,
diff whitespace, and no-path/no-alias guards pass.

At that historical failed gate, no performance execution was admitted. The
approved outcome-neutral test correction removed only the requirement that a
particular timed branch occur. It introduced no workload, deadline, production
or dependency change. The original failure and actual-timeout witness remain
bound to their original source and artifact.

#### Corrected semantic gate

`timeout-correction-20261013-101902/summary.json` records one exact
`commit_observation_preserves_atomic_publication_authority` pass in each profile
(one executed, 88 filtered, none ignored). The preceding `101751` invocation
stopped after inventory and remains **INCOMPLETE**, not a test pass. The fresh
full focused receipt `focused-20261013-102051/qualification.json` is **PASS**:
**89 supplied StateStore**, **16 native durability**, **12 native transaction**
and **7 focused App** cases pass in each profile. The native measurement case is
excluded from this semantic inventory. Source stayed unchanged and process
cleanup completed. The actual-timeout witness is separately checked against
unchanged production/dependency inputs by `measurement-scope-update-review.json`;
these fresh passes do not claim that an optimized commit timed out.

Independent reviews in `timeout-diff-review.json`,
`measurement-neutral-independent-review.json`, `storage-benefit-review.json`,
`combined-independent-review.json` and `combined-identity-independent-review.json`
cover the corrected assertion, exact runner allowances, measured cost tradeoffs
and combined-run admission. All paths in this subsection are under
`target/preparation-durability-evidence/` unless stated otherwise.

#### Preparation-only and combined measurements

All samples below passed assertions and joined Kit cleanup, used fresh owned
APFS directories on the same macOS arm64 host, and kept the hint override unset.
Builds completed before timing. Rust **1.94.1**, `--locked`, one Cargo command/job,
and the effective capacity profile (optimization 3, debug 0, assertions and
overflow checks enabled) were verified. Existing fixtures, instrumentation,
operation counts and deadlines stayed identical within each comparison.

`measurement-20261013-102651/measurement.json` binds every command, raw log,
artifact, source, runner input, resource snapshot and cleanup result. Its fixed
order was Storage A1–B1–B2–A2, followed by App A1–B1–B2–A2. Only the preparation
protocol differs in production between its frozen A and B binaries.

Storage ran the existing exact `capacity_commit_followed_by_observation` case
once per sample (one executed, 23 filtered). Each sample contains 70 sequential
observations in each of the small, cardinality and medium-payload profiles;
each profile observed one reclamation. Totals below sum sequential operations
within that Storage process, in seconds. Combined is the directly measured
commit-through-first-observation interval, not a sum of overlapping timers.

| Sample | Preparation | Consuming commit | First observation | Warm observation | Combined | Whole child |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 0.099450 | 14.650056 | 5.720238 | 0.107237 | 20.370543 | 24.571790 |
| B1 | 0.090656 | 11.569800 | 5.224878 | 0.094538 | 16.794927 | 20.109209 |
| B2 | 0.090169 | 11.058632 | 5.707964 | 0.096784 | 16.766806 | 20.149434 |
| A2 | 0.098230 | 14.683261 | 9.492571 | 0.102840 | 24.176091 | 28.245540 |

Paired combined improvement is **17.553% / 30.647%**. Whole-child elapsed time,
which also includes setup, reopen and settled completion, improves
**18.161% / 28.663%**. Reopen/shutdown have no separate timer in this existing
target. Maintenance occurs inside the first observation and is not an additional
cost to add. A2's cardinality reclamation observation took 5.195591 seconds and
amplifies its paired benefit; it remains in the result. B1's cardinality and
medium-payload first-observation totals regress **0.766% / 7.827%**, despite
improved combined totals. Every warm-observation total improves.

Retained versions end at 9/48/12 across the three profiles, with 44/239/59 files
respectively in every sample. Directory sizes remain about 235 KB/11.32 MB/17.84 MB;
exact bytes and reclamation positions remain in the raw receipt. Whole-process
user/system CPU seconds are A1 3.71/4.60, B1 3.43/4.92, B2 3.50/4.95 and
A2 3.67/4.71: total CPU increases slightly in both pairs. Raw macOS peak RSS bytes
are 358,268,928 / 352,239,616 / 326,926,336 / 351,813,632 in execution order.
These are process metrics, not filesystem or crate-specific attribution.

Each App sample preserves 256 public resolutions, checkpoint 770, 257 received
notifications, 256 lower decisions/returns and original successful joined
shutdown. One exact test executes, 190 are filtered, none ignored. Timings are
seconds; body includes initial workflow execution. Setup is reported separately.

| Sample | Setup | Body | Joined shutdown | Body + shutdown | Peak RSS bytes |
|---|---:|---:|---:|---:|---:|
| A1 | 0.132 | 76.497 | 0.603 | 77.100 | 183,943,168 |
| B1 | 0.092 | 61.754 | 0.467 | 62.221 | 180,289,536 |
| B2 | 0.090 | 63.352 | 1.124 | 64.476 | 177,946,624 |
| A2 | 0.104 | 72.697 | 0.574 | 73.271 | 171,687,936 |

Both complete pairs improve: **19.298% / 12.003%**. The second candidate's
shutdown and peak RSS regress and remain included. All four directories retain
150 files, 32 manifests and approximately 5.765 MB. Raw OS user/system CPU seconds
are A1 14.14/17.71, B1 13.36/18.87, B2 12.90/18.39 and A2 14.11/17.53. The
normalized child-CPU field is unavailable because the OS timing wrapper owns the
child; raw readings are not attributed to App or Storage. Available space was
approximately 20.7 GB; filesystem and `vm_stat` snapshots are bound to each run.
Inclusive Storage/checkpoint timings are not added to public elapsed times.

The separately admitted combined comparison uses fresh A1–B1–B2–A2 samples,
not the earlier failed normalization samples. Its frozen A is the compatible
v6 executable; B includes normalization and the measured preparation protocol.
`combined-20261013-103940-11090/combined.json` records:

| Sample | Body seconds | Joined shutdown seconds | Complete seconds | Directory bytes | Peak RSS bytes |
|---|---:|---:|---:|---:|---:|
| A1 (v6) | 78.591 | 0.590 | 79.181 | 15,713,588 | 376,979,456 |
| B1 | 60.411 | 0.463 | 60.874 | 5,764,993 | 174,899,200 |
| B2 | 60.356 | 0.454 | 60.810 | 5,764,912 | 171,982,848 |
| A2 (v6) | 76.927 | 0.594 | 77.521 | 15,713,315 | 372,736,000 |

Complete cost improves **23.120% / 21.557%**. All four samples have the same
256-delivery semantic counts, 150 files and 32 manifests. This qualifies the
combined comparison on this machine; it neither erases normalization's earlier
failed standalone comparison nor proves release or maximum-capacity completion.

The measured candidate source digest is
`2e1cf8f1b2d0a0e70ea6b352bb0e2cca9fdc22d6bae63bf00e5dbd0bd6da24c6`;
its Storage/App executable hashes are respectively
`2ca19ede7a3e9d9c39d07ac4fb5540e900488b3188bcb492ee94697ceb03bbee` /
`bd594b0cea82a1437c4687eacc446aef7b9070fbd617d875006376777f5393d9`.
Preparation baseline source is
`09833382a2a3a245ddeb3bddf93327b961295e7b95b6c288054f106d14508c4c`,
with Storage/App hashes
`3dcbbd574a3f39c951f2e0af69aa68916f68e1c520b8b45aca3c2ef74641b433` /
`9fe67f6ce21c79d1c653727ae6be1d2a6391822cc1d59e304a31430b5f5cdada`.
The compatible v6 source/artifact are
`9999c637d831af0ce81df15fca347022e8d1c27960514e09cf02ade6993c261c` /
`d3adca9229992e06753372054f107c70a03f099e026ef10039b7e65ba13f5829`,
bound by `target/app-workload-checks/20261012-223417-diagnostic-87154/receipt.json`.
All use lock digest
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Source identities include untracked candidate files. Frozen binaries are labeled
with their own source, never the later checkout. This report update changes the
checkout digest without changing those measured executable inputs.

#### Growth screening stops further performance execution

Both admitted optimized 1,024-delivery diagnostics pass semantics and Kit joining:

| Sample | Four successive 256-delivery blocks (seconds) | Complete setup seconds | Body + shutdown seconds | Peak RSS bytes |
|---|---|---:|---:|---:|
| G1 | 58.844 / 61.206 / 66.537 / 68.040 | 0.328 | 255.321 | 516,358,144 |
| G2 | 60.527 / 64.528 / 64.828 / 73.789 | 0.291 | 264.321 | 540,459,008 |

Receipts are
`target/app-workload-checks/20261013-104555-diagnostic-11487/receipt.json` and
`target/app-workload-checks/20261013-105034-diagnostic-11630/receipt.json`.
`growth-screen.json` uses slower blocks 60.527/64.528/66.537/73.789 seconds,
maximum positive adjacent increase **7.252 seconds**, maximum complete setup
**0.328 seconds**, projected remaining blocks **1,451.124 seconds**, and the
unchanged **100-second** allowance. The estimate is **1,816.833 seconds**, above
**1,200**. Complete setup includes initial workflow/snapshot work outside the
first delivery block. The earlier accounting result remains in
`growth-screen.before-setup-correction.json`; adding that omitted setup changes
neither the rejection nor the workload policy.

**No 4,096-delivery execution is admitted, and performance execution stops.**
The original 2,222-resolution timeout, 1,803.621-second screen, normalization's
failed standalone comparison and both rejected maintenance policies remain
visible. Faster prefixes and process reaping do not establish full completion.
This is real App/Lance deterministic capacity evidence, not real-Agent
strict-live or hardware power-loss evidence. Native protocol fixtures and public
adapter fixtures establish separate boundaries, connected by source review.

#### Historical pre-repair release results — 2026-10-13

**This earlier candidate remained unqualified.** Passing measurements did not waive
behavioral failures. Execution binds source
`be32be621764db3c6e36baf36bb91552e3840c1a23fb67a2cb74a61d705fea37`.
The [release ledger review](../../target/preparation-durability-evidence/release-ledger-review.json)
compares all 1,555 source files with measured candidate `2e1cf8f1…`: only three
Storage documents and the reviewed lint-only, unmeasured maintenance-pilot test
branch differ. All production, lockfile and App/Agent inputs match. The first
macOS invocation's Clippy failure is preserved separately from its corrected
passing gate. This final documentation update does not relabel any source-bound
receipt. Earlier feature-matrix and alternative-backend receipts retain only
their original scope; no current full-matrix execution is claimed.

| Completed release scope | Recorded result |
|---|---|
| [Storage macOS/APFS](../../target/storage-feature-split-evidence/storage-default-final-20261013-110759/receipt.json) and [Linux/ext4](../../target/storage-feature-split-evidence/linux-final-20261013-111134-43f7fc2f/receipt.json), Lance-only | Each passes 163 deterministic tests, 24 strict-live tests, 36 Python tests, 240 doctests and static checks. |
| [App routine workflow](../../target/app-workload-checks/20261013-112542-routine-18287/receipt.json) | 181 passed, 3 failed, 7 explicitly filtered. |
| [Separate routine worker](../../target/storage-feature-split-evidence/remaining-worker-20261013-114712/receipts.json) | 38 passed; one capacity case filtered. |
| [App static/runtime invocation](../../target/storage-feature-split-evidence/app-static-20261013-115113/receipts.json) | 34 valid runtime inventories; 33 final summaries total 583 passed and 6 failed. Child composition separately logs 26 passes, then one stack-overflow SIGABRT; 19 cases never start. One crash case is deferred to live. |
| App examples/documentation/guards | Seven examples, 127 doctests, compilation, formatting and boundary guards pass. Clippy fails with four diagnostics. |

The [App release ledger](../../target/preparation-durability-evidence/app-release-ledger.json) preserves all nine assertion failures. Three routine cases never entered their selected lower boundary before the unchanged activation timeout. Existing diagnoses also identify a one-poll/list-entry assumption, physical-versus-logical checkpoint sizing, restore gating that misses transactional reads, and incomplete passive startup-read accounting. These explanations do not supply passing behavioral evidence. At this earlier receipt the stack overflow remained unclassified; process reaping does not prove Kit joining after SIGABRT. The repair below supplies separate production attribution and passing evidence. No maximum workloads or 256/1,024 diagnostics ran in these routine checks.

The [original scoped live run](../../target/storage-feature-split-evidence/scoped-live-20261013-123553/receipts.json) remains **FAIL**: Agent 18 passed/5 failed, App 18 passed, and exact crash recovery 1 passed. Four Agent failures explicitly report authentication rejection; the caller-loss timeout’s original cause remains unproven. After credential repair, the [separate exact five-case recovery](../../target/storage-feature-split-evidence/scoped-live-20261013-130518/receipts.json) passes on the same code. Together they cover all 23 Agent cases; there was no single all-pass full-suite invocation. Both mandatory knowledge cases pass with six matching native/Provider/Agent observations; Question Kit and raw-copy grades are each 2/2. Original failures remain preserved. Owned services and relay settled without reported process cleanup errors; this is distinct from universal successful Kit shutdown, particularly in deliberately failing live scenarios.

[Native five-crate packaging, clean Agent/App libraries and extracted-consumer execution](../../target/agent-app-repair-evidence/packaging-20261013-130726/receipts.json) pass. [Archive and lock details](../../target/agent-app-repair-evidence/packaging-20261013-130726/package-details.json) bind all sibling checksums and the 1,555-file candidate, including untracked source. Native Cargo package verification preceded the separately patched extracted consumer. That consumer commits expected App state over Lance and joins shutdown without constructing an Agent runtime. The seed-metadata exit 101 remains preparation: offline lock pruning was audited before successful locked proof. The [standalone Storage package consumer](../../target/storage-feature-split-evidence/packaging-20261013-131046-889ae1b3/receipt.json) also passes: memory/directory behavior, conflict, tombstones, reopen, sibling knowledge isolation, direct trait implementation and five separate explicit backend opt-in builds. Default/no-default and downstream normal graphs retain Lance-only implementation selection. Neither packaging nor live recovery waives the outstanding App failures and Clippy diagnostics.

The [final authentication recovery review](../../target/preparation-durability-evidence/codex-auth-recovery-final-review.json)
verifies exact inventory union, unchanged pinned configuration and settled owned
services. Recovery copied the reviewed newer local authentication into the
isolated test home with private permissions, preserving the previous file
privately. No credentials entered source, reports or test output. The original
full-run failures are not overwritten; the later five exact results follow a
specific setup correction, not unconditioned repetition.

Independent reviews also cover the
[App release ledger](../../target/preparation-durability-evidence/app-release-ledger.json),
[platform gates](../../target/preparation-durability-evidence/platform-review.json) and
[five-package proof](../../target/preparation-durability-evidence/five-package-independent-review.json).
No further performance execution, production correction or acceptance relaxation
was made after the failed growth screen. Public APIs, counts and deadlines are
unchanged. Nothing is committed or pushed.

#### App failure repair — 2026-10-13

The stack overflow was a production stack-composability defect under valid
public API composition. The original public fixture amplifies stack pressure,
but moving only the existing private restore future into a concrete pinned box
makes the exact original case pass on the same default stack. The restoration
body, public signatures, admission order, deadlines and native cleanup are
unchanged. There is no new task, erased future, trait, dependency or stack-size
setting. Static generated public-restore poll-frame reservation changed from
39,824 to 1,920 bytes; this is not a guarantee for arbitrary caller stacks.

The nine original assertion failures had different, source-established fixture
causes: one-poll versus actual selected-read admission, missing transactional-read
accounting, logical versus physical v7 byte-size expectation, stale restore-gate
placement, and mandatory lower entry inside a 25 ms whole-activation budget.
They did not demonstrate production correctness defects. Existing assertions
were preserved at the responsible public boundary. Explicitly approved policy
separates actual 25 ms timeout/no-lower-dispatch proof from eight lower-entry and
panic cases with 200 ms finite activation budgets. Existing 1,000 ms storage
preparation cases and all capacity deadlines/counts remain unchanged.

Assertion failures now retain their original outcome while the same App/runtime
observes cleanup. Terminal failure is accepted as joined only under the source-
proven, unchanged shutdown protocol; it never becomes successful test outcome.
Original errors and historical failures remain recorded. No broad AppError
classifier, runtime framework, production hook or replacement fixture was added.

Clippy corrections name the existing bounded memo entry (including all page
identity checks), simplify three migration expressions, and flatten an existing
successful-owner cleanup iterator without changing its body. Final all-target
Clippy and exact stack regression pass at 20261013-141903. Earlier candidate
return-type lint failures remain separate, failed receipts.

The complete five affected domain targets passed 215/215 before the final
private async-return spelling. That historical source differs in private future
construction spelling; final exact stack execution and final compilation must
not be conflated with the earlier full-target run. Six original fixture failures
also have individual source-bound green receipts. The final results below bind candidate
`4cec2b9cf53a89d1ced4b27de78b34a94ffdc71b97bf1db929b84c9151ca3f3b`,
including untracked source. Rust 1.94.1, `--locked`, one Cargo command/build job
and ordinary macOS/APFS were used. The lockfile remains
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.

| Final repair scope | Result |
|---|---|
| [Stack regression and Clippy](../../target/app-failure-repair/20261013-141903/receipts.json) | Original exact case passes on the default stack; all App targets pass Clippy. |
| [Routine App workload gate](../../target/app-workload-checks/20261013-142052-routine-56773/receipt.json) | 185 workflow and 38 worker cases pass; seven workflow diagnostics/capacity cases and one worker capacity case explicitly filtered; no ignored cases. |
| [Focused/static/docs/examples](../../target/app-failure-repair/20261013-142007/receipts.json) | Five exact deadline cases, 7 public-surface, 1 core and 4 preparation tests pass. Compile contracts, 127 doctests, seven examples, rustdoc, formatting, no-alias/no-path and diff checks pass. |
| [Optimized focused correctness](../../target/app-failure-repair/20261013-150001/receipts.json) | Seven exact cases pass: stack, the five deadline cases, and 64 deliveries with joined cleanup. This is not a diagnostic prefix or capacity-admission measurement. |
| [App live and crash recovery](../../target/storage-feature-split-evidence/scoped-live-20261013-145018/receipts.json) | 18/18 workflow-live and the one exact canonical four-boundary crash case pass. Its three unselected process-helper cases are explicitly filtered. [Owned service cleanup](../../target/storage-feature-split-evidence/scoped-live-20261013-145018/service-ownership.json) passes with stable source and runner inputs. |
| [Native packaging and clean consumer](../../target/agent-app-repair-evidence/packaging-20261013-150758/receipts.json) | Five-crate native package verification, clean Agent/App libraries, archive-byte and dependency-lock audits, Lance-only consumer graph and extracted-consumer execution pass. [Details](../../target/agent-app-repair-evidence/packaging-20261013-150758/package-details.json) bind all 1,555 candidate files and sibling archive checksums. |

Native package verification precedes the separately patched extracted consumer.
The seed-metadata exit 101 is the retained preparation receipt; audited offline
lock pruning precedes successful locked metadata/build/execution. It does not
change the workspace lock or substitute a patched graph for native verification.

These rows have overlapping tests and must not be summed as unique coverage.
The 64-delivery routine run reports 63 original resolution receipts, 63 replays,
one cancelled target, checkpoint 196 and one joined lower shutdown. Its controls
are deterministic App-contract evidence over real Lance. App workflow-live uses
real configured services, including its four deliberate-failure cases; the
canonical crash case proves App/Lance process recovery, not model behavior.

[Final incremental source review](../../target/app-failure-repair/final-incremental-source-review.json),
[stack review and artifact proof](../../target/app-failure-repair/stack-composability-independent-review.json),
[approved timing/cleanup review](../../target/app-failure-repair/preparation-policy-independent-review.json)
and [iterator-equivalence review](../../target/app-failure-repair/manual-flatten-independent-review.json)
pass. The final stack artifact is independently recorded; earlier explicit-return
and marked-fixture artifacts remain historical. No runtime debugger or privileged
instrumentation was used.

[Source comparison](../../target/app-failure-repair/final-source-comparison.json)
against the previously packaged candidate confirms unchanged Storage and Agent
production and lock inputs. Their earlier platform, feature-matrix and Agent-live
receipts retain their original scopes; none were rerun for this App-only repair.
Provider/Tool live suites and workspace-wide live wrappers were not run.
The final qualification-ledger update changes documentation only after execution;
it does not replace the receipts' recorded candidate identities.

Capacity remains blocked by the 1,816.833-second screen. The original 2,222-
resolution timeout, 1,803.621-second earlier screen and failed normalization
comparison remain historical failures. No performance workload or 4,096-delivery
attempt was rerun for these repairs. No commit or push occurred.

### Baseline and TDD receipts

- The source-bound optimized v6 binary generated a real 96-delivery history
  containing **295 checkpoints**, performed restore and deletion through public
  App APIs, and joined cleanup: **27.75 seconds**, one selected test passed.
- A second public workflow retained historical Unknown evidence and its original
  lower outcome: **1.16 seconds**, one selected test passed.
- A 130-action workflow created **1,436 history keys**, deleted exactly the first
  **256** through the existing bounded deletion owner, and retained the sealed
  deletion with **1,180 surviving keys**. Its deliberate second-page failure is
  preserved separately from joined ownership: **41.52 seconds**, one selected
  test passed.
- The new normalized-page test failed against v6 at the first required v7
  checkpoint assertion (`app/v6` versus `app/v7`): **1.24 seconds**, one test
  failed. No production encoder had changed.
- Two pinned native selected-fragment feasibility cases passed. The supplied
  adapter's GC-only regression failed as intended: expected head **64**, observed
  **65**. This records the old full-rewrite behavior before replacement.

App artifact, compiler, profile, source, lock, exact inventories and owned-process
receipts are retained in
`target/app-workload-checks/20261012-193412-diagnostic-27715/receipt.json` and
`target/app-workload-checks/20261012-193616-diagnostic-27819/receipt.json`.
Storage receipts are in
`target/settlement-normalization-evidence/maintenance-baseline.json`.
The earlier golden-generation failure was a test input error (a trailing slash
in the supplied temporary workspace root), not a Kit defect; it remains in
`target/app-workload-checks/20261012-192520-diagnostic-10095/receipt.json`.

These golden generators use real App execution and directory-backed Lance with
caller-defined deterministic Agent implementations. They establish neither
real-Agent strict-live behavior nor migrated-store correctness. Cold fixture
creation precedes the new encoder; migration and interruption qualification are
still required.

The first normalized ready-v7 public test now passes: **96 actual deliveries**,
the 1/63/64/65-entry page boundaries, exact historical receipt tuples, unchanged
full-page reuse, restore/deletion and joined cleanup, **27.26 seconds**. The
three cold-golden migration tests each fail at the expected unsupported-v6
boundary before the migration execution hook is installed. These are behavioral
red receipts, distinct from the earlier compile-only call-site corrections.
Their exact inventories and unchanged source identity are recorded in
`target/app-workload-checks/20261012-200222-diagnostic-29247/receipt.json`.

The first fixed maintenance A sample passes selected-fragment and GC semantics
but does not establish a benefit. Aggregate measured operation/reopen/shutdown
cost was **144.585 seconds**, versus **133.986 seconds** for the corresponding
full-image baseline. The mixed-size fixture preserved its original cold data
fragment; the retained-snapshot fixture removed the original deferred fragment
IDs but ended with 260 total fragments after continued writes. That is not a
claim that the total backlog shrank. Baseline and A receipts are respectively
`target/app-workload-checks/20261012-193913-diagnostic-28040/receipt.json` and
`target/app-workload-checks/20261012-194637-diagnostic-28488/receipt.json`.

**Both fixed maintenance policies were rejected at the first benefit pair.**
Candidate B passed all three semantic workloads and the GC-only regression,
but aggregate complete cost was **140.217 seconds**. Its cold-file case improved
from 46.147 to 44.137 seconds; homogeneous growth regressed from 42.250 to
44.623 seconds and the retained-snapshot backlog from 45.590 to 51.457 seconds.
The B receipt is
`target/app-workload-checks/20261012-200619-diagnostic-29463/receipt.json`.
Every sample and its source/artifact identity is retained in
`target/settlement-normalization-evidence/storage-maintenance/fixed-pilot-decision.json`.
Independent source review found no correctness blocker, but that cannot replace
the failed benefit gate. The exact production experiment was reverted; no
policy is selected and no informal tuning or repeat-until-green run is allowed.
The requested maintenance redesign is therefore **not completed**. App
normalization and migration continue as a separate, still-unqualified change.

The first applied-migration execution is recorded in
`target/app-workload-checks/20261012-202316-diagnostic-74143/receipt.json`.
The source remained unchanged and all invocation-owned processes settled.
Interrupted conversion/resume, corrupt/overflow preflight without writes,
fresh leaf corruption behind an unchanged checkpoint, and the routine
64-delivery lifecycle case passed. Three new fixture assertions failed:
checkpoint counting included projection links, and the deletion progress probe
attempted to delete a paused run. Independent source review established these
causes; later assertions in those tests remain unexecuted until their corrected
run. They are not recorded as migration passes.

The existing initial-attempt byte-reservation fixture also failed against v7
because its padding calibration measured compact physical JSON rather than the
unchanged complete logical row. Its original failure is retained in
`target/app-workload-checks/20261012-202558-diagnostic-74420/receipt.json`.
The correction must hydrate real pages for fixture calibration only, preserving
the 4 MiB logical limit and every lower-admission assertion. Source review also
identified a resumed-v6 validation gap; the prepared corruption regression must
fail before the narrow production correction is applied.

The corrected fixtures and expanded migration boundaries ran against one frozen
candidate in
`target/app-workload-checks/20261012-203705-diagnostic-77208/receipt.json`:
**nine exact cases passed and one deliberately expected regression failed**.
Passing cases cover complete logical history/revisions, historical Unknown,
sealed deletion and interruption after a deletion page, each migration root
phase, committed-root/checkpoint lost replies, simultaneous real Lance claims,
preflight corruption/overflow, and the unchanged logical byte reservation.
The failing case demonstrates a real implementation defect: resumed conversion
accepted a forbidden field introduced after preflight. The test joined the
unexpectedly successful App before failing. A reviewed fresh-row validation
correction is now applied. The supervisor's
PASS denotes the planned red/green experiment, not a passing corruption test or
complete migration qualification.

The follow-up receipt,
`target/app-workload-checks/20261012-205258-diagnostic-97347/receipt.json`,
records four passing exact cases: resumed-row corruption rejection, an older-page
update at the 127/128/129 boundaries with both later pages unchanged, the routine
64-delivery case, and the unchanged logical byte reservation. A fifth case
deliberately establishes a cleanup defect: an unavailable settlement-page read
followed by a failed rollback reply caused **eight** mutation attempts instead
of one. The original primary error survived, but its cleanup disposition was
lost before retry admission. Only the unavailable-read branch executed in this
red run; conflict coverage must be reported separately. All invocation-owned
processes settled and source identity was unchanged. A private error-disposition
correction is under independent review; this receipt does not qualify it.

The independently reviewed cleanup-disposition correction compiled and ran in
`target/app-workload-checks/20261012-213416-diagnostic-22745/receipt.json`:
**14 exact cases passed; two new outer-owner assertions failed**. Both the
unavailable and conflict page-read cases now stop after one failed rollback;
public Input resolution, ordinary recoverable retry, lost receipts, ordered
predecessors and cleanup reobservation passed. Migration of a fourth cold v6
fixture with current Pending/Unknown state and historical definitions passed,
as did the root-phase and lost-publication-reply cases. The fourth fixture was
captured by a frozen v6 executable after deliberate process loss; process
reaping is not a graceful-shutdown receipt for that interrupted owner.

The failing new workspace-deletion and acknowledged-lower-publication assertions
did not record their actual returned error, so they do not yet identify a
production defect. Passive bounded outcome/counter diagnostics are required
before changing behavior. Source identity remained unchanged and every
invocation-owned process was reaped. The candidate performance sample was
skipped. The earlier v6 256-delivery sample (77.111-second body plus 0.591-second
joined shutdown) remains an isolated historical baseline, not a completed ABBA
comparison; fixture corrections require a new frozen comparison sequence.

### Completed cleanup correction and final-fixture semantic checks

Passive outcome evidence in
`target/app-workload-checks/20261012-215333-diagnostic-28487/receipt.json`
established the two outer-owner failures: fatal cleanup publication raced the
return of the original public error, replacing it with `Closed`. The reviewed
correction keeps retry-vetoing cleanup disposition on the existing returning
owner and lets background/execution consumers apply their existing fatal
policy. It adds no acknowledgment protocol, task, error variant or public API.
The original workspace-deletion and acknowledged-lower-publication regressions,
plus Input resolution, now preserve their primary error and stop replay.

Two migration tests also passed after a real leaf-bearing publication followed
by an unreadable or revision-mismatched readback. They preserve the original
consuming-commit error, consume rollback once, keep the root non-runnable, and
permit exact later reconstruction without rewriting the already-published leaf.
They use controlled observations over actual Lance data; they do not claim
native disk corruption or preflight detection of hidden absent tombstone revisions.

The first 25-case run, `20261012-221833-diagnostic-85893`, passed 24 and failed
the stalled-resolution-observation fixture. Its predicate still treated the v7
settlement descriptor as a v6 inline array, so it never reached the intended
held boundary. The correction reads immutable pages through the same native
owner, including the same transaction snapshot, and keeps the physical
checkpoint unchanged for the original immutability assertion. It changes no
deadline, workload count, production behavior or acceptance assertion.

The corrected final fixture passed **all 25 exact cases** in
`target/app-workload-checks/20261012-223118-diagnostic-87027/receipt.json`.
This includes the unchanged 64-delivery routine, lost receipts, held observation,
observer cancellation, atomic older-page plus new-page changes, failed rollback
retry veto, migration readback failures and joined cleanup. Source stayed
unchanged and process cleanup had no errors. Independent reviews are retained in
`target/settlement-normalization-evidence/app-normalization/cleanup-public-return/`,
`migration-readback-negative/`, `stalled-observation-fixture/`, and
`final-app-review/`. Formatting and no-path/no-alias guards pass. These selected
results do not qualify every App target or any real-Agent live case.

### Normalization-only ABBA — fixed gate failed

All four samples use Rust **1.94.1**, the verified `capacity` profile, the same
final diagnostic fixture, default native hint policy, fresh APFS datasets and
one serial test process. They each complete **256 genuine public resolutions**,
reach checkpoint **770**, preserve the three transitions per ordinary delivery,
and pass original shutdown with joined Kit ownership. The exact inventory is
one passed, zero failed/ignored, and 190 intentionally filtered tests.
Compilation, metadata inspection and dataset disposal are outside the selected
body-plus-shutdown metric and remain separately recorded.

| Sample | Format | Body (s) | Joined shutdown (s) | Complete (s) |
| --- | --- | ---: | ---: | ---: |
| A1 | Frozen v6 | 79.517 | 0.609 | 80.126 |
| B1 | Candidate v7 | 72.651 | 0.547 | 73.198 |
| B2 | Same candidate v7 binary | 79.889 | 0.599 | 80.488 |
| A2 | Same frozen v6 binary | 71.863 | 0.590 | 72.453 |

The A1/B1 pair improves **8.646%**; A2/B2 regresses **11.090%**. This fails the
agreed requirement that both complete-cost pairs improve. **No growth or full
capacity run is admitted, and no repeat-until-green sample is planned.** The
normalization candidate remains available uncommitted for further diagnosis;
it has not passed retention or release qualification. The earlier isolated
77.702-second v6 sample is historical and was not substituted into this sequence.

After shutdown, v6 datasets contain **15,713,268 / 15,713,578 bytes** and v7
contains **5,764,843 / 5,764,856 bytes**, about **63.31% less**. Every sample has
150 files and 32 retained manifests. These are equal-workload physical
footprints, not a logical-row byte audit, a growth-rate estimate or proof of
reduced future maintenance time. Whole execution-child CPU seconds are
30.666 / 30.290 / 30.727 / 30.560 in ABBA order. Peak RSS remains unavailable:
the optional ordinary resource probe was denied; no privileged instrumentation
or privilege escalation was used. System memory snapshots and free space are retained.

Each sample observes 787 successful mutating Storage commits. Their inclusive
finalization times are 53.269 / 49.033 / 55.714 / 48.801 seconds. These include
waiting and cleanup and do not identify CPU, synchronization or maintenance
cost. Concurrent read durations cannot be added as independent elapsed costs.
Reduced bytes alone do not override the failed timing gate.

Exact receipts, including source/untracked-file digest, lock/compiler/profile,
original executable identities, filesystem, inventories and cleanup, are:

- A1: `target/app-workload-checks/20261012-223417-diagnostic-87154/receipt.json`
- B1: `target/app-workload-checks/20261012-224028-diagnostic-87467/receipt.json`
- B2: `target/app-workload-checks/20261012-224244-diagnostic-87538/receipt.json`
- A2: `target/app-workload-checks/20261012-224430-diagnostic-87608/receipt.json`

The independently reviewed analyzer and every sample are bound by
`target/settlement-normalization-evidence/abba-final-fixture/result-analysis.json`.
The original candidate was restored by exact hashes after A1; A2 explicitly
identifies the original v6 executable rather than relabeling it with current
source. Two B2 command-line mistakes rejected before creating any test process
and are preserved in launcher logs; the table contains exactly four actual
executions. The subsequent changes to this ledger are documentation-only.

Outstanding work remains explicit: consistent measured benefit, logical-byte
and migrated-store growth measurements, accepted maintenance policy, current
full App/static/live/process/package checks, and ultimately the original full
capacity case plus Kit settlement. No prior live result resolves those gaps.
The original 2,222-resolution failure and 1,803.621-second screen are unchanged.

### Bounded commit/observation diagnostic — stopped without a production change

The narrowed follow-up authorized reuse of a compatible receipt, or one execution
of the existing `capacity_commit_followed_by_observation` case. Historical
exact-case receipts used an unoptimized profile and different Lance hot-path
sources. Newer optimized receipts matched current Storage production sources
but executed other cases; a full-inventory listing was not execution evidence.

The existing case therefore ran **once**, unchanged, with Rust **1.94.1**,
`--locked`, the verified `capacity` profile (optimization level 3, debug
assertions and overflow checks enabled), one build job, and default hint policy
on macOS/Apple Silicon/APFS. Compilation preceded measurement. The existing
supervisor reported one passed test, zero failed/ignored/measured, 23 deliberately
filtered tests, and **23.586 seconds** for the execution command. All three
profiles completed 70 measured updates, exact value/revision assertions, an
actual retained-version drop, reopen, verification rollback and owner disposal.
Process-group settlement completed separately with no cleanup errors.

| Existing profile | Commit p50 (ms) | First observation p50 (ms) | Warm observation p50 (ms) | Maintenance-bearing first observation (s) |
| --- | ---: | ---: | ---: | ---: |
| 100 rows, 1 KiB payload | 66.325 | 0.726 | 0.489 | 1.688 |
| 10,000 rows, 1 KiB payload | 69.307 | 0.711 | 0.444 | 1.545 |
| 1,000 rows, 16 KiB payload | 68.140 | 0.633 | 0.417 | 1.698 |

These are quiescent supplied-adapter observations. The first-read samples
alternate ordinary loads and read-only transactions; the three maintenance
observations coincide with retained-version reductions at update 62, 23 and 59,
respectively. Consuming-commit time includes native work and owned cleanup; the
measurement does not isolate filesystem barriers, native merge work, queueing
or CPU. It cannot assign critical-path savings to the concurrent App workload.
No cumulative device-write or process-RSS claim is made.

The original source-bound receipt and raw log are retained at
`target/settlement-normalization-evidence/bounded-commit-observation-20261012-231943/`.
`receipt.json` contains source/untracked-file, lock, compiler, effective profile,
artifact, filesystem and command identities. Its initial `INCOMPLETE` status
records a one-off extraction error: the postprocessor expected a different
marker from the unchanged fixture's `LANCE_STATE_CAPACITY` output. The separately
checked `analysis.json` parses that same original log and records passing
execution. The original receipt was preserved; neither build nor workload was
repeated to correct extraction. The raw execution log SHA-256 is
`b5a86052c8a43514482bd540061b74b666170d4b6d3e82b095ad7ec0687e1ef2`.

**Decision: stop; capacity unresolved.** The data identify costly public
operations but do not establish avoidable work inside them. The proposed
background driver/session consolidation remains a hypothesis; its relevance and
post-admission freshness equivalence are unproven. No tracing, fixture,
production, cache, runtime, maintenance-policy or durability-barrier expansion
was made. Independent review checked historical reuse, raw execution,
extraction and the stop decision. The 256-delivery paired-benefit prerequisite
remains in force, so no 1,024 or 4,096 execution is admitted. The original
2,222-resolution timeout, 1,803.621-second screen, rejected maintenance policies
and failed normalization timing gate remain unchanged evidence.

## Version-hint diagnostic — 12 October 2026

**The diagnostic benefit gate failed; no production candidate was implemented.**
All four App executions and all sixteen Storage executions passed their selected
behavioral assertions and cleanup. The second App pair nevertheless regressed,
and maintenance measurements were mixed. Under the agreed two-pair rule this
ends the experiment. No 1,024-delivery growth sample or full 4,096-delivery case
was run. The earlier **1,803.621-second** admission screen and failed full run
ending at **2,222 public resolutions** remain unchanged. The retained private
App predecessor optimization is preserved.

### Policy comparison and App result

The experiment used fresh processes in fixed A–B–B–A order. A left
`LANCE_USE_VERSION_HINT` unset; B set it to exactly `0`. This native, process-cached
setting changes both hint reads and writes. It does not isolate synchronization
cost, and it is not a proposed production environment setting. Lance describes
the hint as optional in its [storage layout](https://lance.org/format/table/layout/);
the [pinned implementation](https://raw.githubusercontent.com/lance-format/lance/v7.0.0/rust/lance-table/src/io/commit.rs)
governs the experiment's actual behavior.

| App sample | Native policy | Body (s) | Joined shutdown (s) | Complete (s) |
| --- | --- | ---: | ---: | ---: |
| A1 | Default | 73.447 | 0.613 | 74.060 |
| B1 | Hints disabled | 70.770 | 0.592 | 71.362 |
| B2 | Hints disabled | 74.275 | 0.584 | 74.859 |
| A2 | Default | 72.631 | 0.593 | 73.224 |

A1/B1 improves complete cost by **3.643%**; A2/B2 regresses by **2.233%**.
Every sample completes 256 genuine public resolutions, reaches checkpoint 770,
passes the original semantic assertions and joins Kit cleanup. Each exact
inventory reports one passed, zero failed or ignored, and 166 deliberately
filtered cases. These are real App executions with directory-backed Lance and
a controlled caller Agent; they are not real-Agent strict-live evidence.

The App binary SHA-256 is
`f495fbb7dcf8f042ba99557eb1f1d1e2d2f0cce41ef473aef47f2fdf13bb3f18`
for all four legs. Source, fixture, loaded-library and non-hint environment
identities match, including each sample's before/after runtime audit. Frozen
executable compatibility checks still include the hint setting; the experiment
does not relax those checks to reuse a receipt under a different policy.

Whole-child user/system CPU seconds are respectively 12.143/16.085,
13.212/22.576, 13.236/22.661 and 13.260/17.385. They include all test-child threads
and provide no crate-specific CPU attribution. Each dataset ends with 32
retained manifests and about 15.713 MB; default samples have 150 files and
disabled-hint samples 149. Random native identities preclude treating every byte
difference as the hint's size. Peak RSS is unavailable; no memory benefit is
claimed.

### Supplied-Storage measurements

One unchanged performance executable runs four exact cases in each ABBA leg:
small deltas over 100/1,000/10,000 records with zero or current peer snapshots;
equal-byte scalar and structured maintenance boundaries; and an original
snapshot retained across 80 mutations. All sixteen executions pass, with one
selected test and twenty deliberately filtered cases each. The six small-delta
shapes each include five warmups and twenty measured observations, remain below
maintenance eligibility, and preserve exact values and revisions.

Both pairs improve mean complete small-delta cost across all six shapes,
approximately **14–24%**, while preparation and first/warm observations become
slower. The consuming-commit savings therefore trade against read costs. This
does not establish that skipping one hint write caused the net benefit.

The retained-original-snapshot case ends mutation at 82 retained versions,
verifies the original value/revision, then releases protection and reopens.
Maintenance leaves one retained version at head 83, with all rows validated.
Mean complete mutation/observation cost improves from 45.833 to 38.633 ms and
48.290 to 39.876 ms. Reopen with maintenance instead increases from approximately
1,312 to 1,317 ms and 1,321 to 1,358 ms. Scalar maintenance moves from 1,571 to
1,703 ms in the first pair and 1,107 to 1,046 ms in the second; structured
maintenance moves from 1,125 to 1,086 ms and 1,076 to 1,117 ms. Structured mean
serial commits move from 46.033 to 86.041 ms (**86.913% regression**) in the first
pair and 55.928 to 45.958 ms (**17.827% improvement**) in the second. All samples
are retained; the result is mixed, not a general maintenance improvement.

Hint presence is checked outside measured operation intervals. Default samples
observe hints during mutations; existing orphan cleanup may remove them on
reopen. Final absence alone therefore does not identify the policy. Native
hint-failure tests and every production synchronization barrier are unchanged.
The optional ordinary resource-counter prerequisite was unavailable
(`time -l` reported `sysctl kern.clockrate: Operation not permitted`). Storage
wall-time phases, files, versions, filesystem and free space remain recorded;
unavailable process CPU/RSS is neither zero nor a reason to repeat workloads.

### Changes, provenance and remaining scope

Only diagnostic/test guidance, runner preflight and measurement coverage changed
in this experiment. The runner accepts exactly `LANCE_USE_VERSION_HINT=0` for
the selected 256-delivery diagnostic; routine, growth and capacity execution
require it unset, and other values fail before execution. Four selected
pre-guard tests recorded red evidence, including actual acceptance of forbidden
policies. The remaining red results included missing receipt fields and a test
double serialization error; those alone are not behavioral evidence. All 32
Python runner tests subsequently passed, including failure propagation, frozen
identity and invocation-owned cleanup. The first green attempt corrected one
new expectation to the existing `INCOMPLETE` preflight status; production status
semantics did not change.
The Storage fixture adds a concrete retained-snapshot case and untimed hint
observations within its existing owner. There is no new production owner,
provider implementation, sorted listing, Session cache, dependency or public API.

Both comparisons use Rust 1.94.1, `--locked`, one build job, the verified capacity
profile (optimization 3, debug assertions and overflow checks enabled), and
macOS/APFS. Builds precede measured execution. Source including untracked files
remains `5de266844ed676f674599d0b9d76e2589152d3cfaf2987a53f0ef623c299378b`
throughout both comparisons; lock identity remains `bf799d0e…`. The Storage
artifact is `e90c0c70ed17c8e7174cb666d7b203b203495c4d39ea0fe868e807089fbd0267`.
This report is updated after measurement; it does not relabel earlier binaries.

Receipts under `target/capacity-version-hint-evidence/`:

- `app-policy-abba-20261012-181323/receipt.json`: commands, exact inventories,
  four retained App executable receipts and runtime identities.
- `app-policy-analysis.json` and `app-policy-independent-review.json`: raw-log
  hashes, complete-cost pairs, resource limits and independent gate verification.
- `storage-hint-abba-20261012-181900/receipt.json`: sixteen exact executions,
  all 36 measurement records, source/artifact identity and cleanup outcomes.
- `storage-hint-abba-analysis.json` and `storage-hint-abba-review.json`: every
  paired Storage statistic and independent execution/semantic review.

Runner red/green receipts remain under
`target/capacity-mutation-evidence/version-hint-diagnostic/`. Independent source
reviews cover the runner guard, measurement diff and execution drivers.
Workspace formatting, no-path/no-alias guards and focused capacity-profile
Clippy for `lancedb_state_performance` pass; the Clippy log is
`target/capacity-version-hint-evidence/storage-performance-clippy.log`. No
unaffected Storage matrix, Linux gate, packaging or live suite was repeated for
these diagnostic-only changes. Prior receipts retain their original source and
scope. The mandatory native knowledge-output failure and earlier routine
timeout recorded below remain unresolved. Capacity remains open; successful
diagnostics do not qualify the full boundary or justify another full run.

## Mutation-cost follow-up — 12 October 2026

**Capacity qualification remains incomplete.** A measured private App change
passes the agreed paired retention gate. Both subsequent 1,024-delivery
diagnostics pass, but their combined unchanged full-run admission screen rejects
the full attempt. Its new value is
**1,803.621 seconds**, above 1,200 seconds. The original 4,096 case was **not run**
again. Historical full-count failures, including the timeout after 2,222 returned
public resolutions, remain failed evidence. The separately accepted feature-split
scope is unchanged.

### Measured boundaries and candidate decisions

New passive observations separate transaction lifetime, forwarded native calls,
pre-finalization gaps, consuming finalization, and read-only versus mutating
transactions. Checkpoint spans count active polling separately from their elapsed
lifetime. The original baseline records about **0.54 seconds** of total checkpoint
active-poll time within roughly 78 seconds of complete 256-delivery execution.
Predecessor construction occurs inside those spans. These observations bound its
direct synchronous cost at this prefix; they do not isolate CPU, queueing, merge
preparation, durability or maintenance. Concurrent ordinary-load durations
overlap and cannot be added to elapsed time.

The Lance copy-on-write image-advancement experiment was **rejected and restored**.
Its complete App cost improved in the first pair (77.795 → 73.115 seconds), but
regressed in the second (78.597 → 79.375 seconds). It therefore failed the
preselected requirement that both pairs improve. The mixed result does not prove
a causal regression, and it was not rerun to seek a favorable outcome.

The retained App candidate removes an intermediate JSON value used only as
expected predecessor evidence. Existing mutation owners retain the typed
`RunRecord`; complete encoded-size/reserve checks, error ordering, fresh native
predecessor validation, ordered-JSON comparison, publication and cleanup remain.
No checkpoint cache, new public API, schema, dependency or lower-layer behavior
was introduced. Independent source review found no contract change. Seven focused
semantic cases passed on both the original and candidate executables. The final
17-case selection also passes in both test and capacity profiles, including byte
reservations, ordered/numeric evidence, missing predecessors, child admission,
readmission, lost receipts and joined cleanup. Receipts are
`20261012-155656-diagnostic-44747` and `20261012-160306-diagnostic-46036`; broader
affected-target qualification has the mixed results recorded below.

| Selected sample | Body + joined shutdown (s) | Checkpoint active-poll total (ms) |
| --- | ---: | ---: |
| A1, original | 78.597 | 536.199 |
| B1, typed predecessor | 73.585 | 423.091 |
| B2, typed predecessor | 73.367 | 431.151 |
| A2, original | 81.857 | 548.781 |

Both pairs improve complete cost (**6.377%**, **10.372%**) and active-poll cost per
closed checkpoint span (**21.094%**, **21.435%**). Each sample completes 256 public
resolutions, checkpoint 770, 256 original lower calls/returns, 775 closed spans,
and the original successful shutdown. This satisfies the agreed retention gate.
The observed 113–118 ms active-poll reduction does **not** explain or attribute
the whole 5.012–8.490 second elapsed difference. Machine variation and unmeasured
indirect effects remain possible; no statistical reliability claim is made.

A1 reuses the previous experiment's final original-baseline observation
(`152419`), whose frozen executable was retained at `150329`; focused tests and
compilation intervene before B1. The selected observations have ABBA order, but
are not an isolated contiguous four-run experiment. Original and candidate
executables retain distinct source identities. Both share fixture digest
`36be4871…`, Rust 1.94.1, the checked capacity profile and lockfile
`bf799d0e…`. Original source is `de2876de…`; candidate source is `1d5d39c1…`, with
App storage source `401f0b19…`. Full identities and runtime audits are in the
receipts. These are real App executions with directory-backed Lance and a
controlled caller Agent, not real-Agent strict-live tests.

Direct test-child CPU also declined in these samples, but covers every child
thread and does not attribute cost to App or Lance. Peak process RSS is
unavailable; host memory snapshots are not a substitute. Replacing a JSON DOM
with a typed predecessor increases the inline mutation enum size. Net memory
benefit is therefore unproven. Dataset storage is APFS; both prefix variants end
with 150 files and 32 retained manifests. Available space is recorded per run.

### Growth and full-run admission

Both candidate growth samples pass all 1,024 actual public resolutions, reach
checkpoint 3,074, observe 1,025 deliveries and 1,024 original lower calls/returns,
and finish successful joined cleanup. Each ends with 63 files, 14 retained
manifests and approximately 227.933 MB of data. Exact inventories report one
passed test, zero failed or ignored, and 166 deliberately filtered tests.

| Sample | 1–256 (s) | 257–512 (s) | 513–768 (s) | 769–1,024 (s) | Body + shutdown (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| G1 | 73.373 | 80.351 | 75.785 | 84.287 | 314.745 |
| G2 | 75.626 | 77.506 | 82.927 | 80.529 | 317.492 |

The slower block envelope is 75.626, 80.351, 82.927 and 84.287 seconds; its largest
positive adjacent increase is 4.725 seconds. With the slower combined setup of
0.436 seconds, the unchanged admission formula is:

```text
0.436 + 323.191 + sum(84.287 + j * 4.725, j = 1..12) + 100
= 1,803.621 seconds >= 1,200 seconds
```

This rejects another full attempt. It is a conservative admission heuristic,
not a measured 4,096 runtime or proof that the deadline cannot be met. Actual
blocks are not monotonically slower. All original full-count assertions and the
1,200-second body, 1,260-second activation and 2,400-second supervisor budgets
remain unchanged. No partial diagnostic qualifies the maximum settlement boundary.

### Native attribution and remaining qualification

The supported native-API probe passed once at `native-phase-20261012-155416`:
one exact test, ten intentionally filtered tests, three independent datasets,
and all 78 seed/warmup/measured observations retained. Each size has five warmups
and 20 measured two-key deltas plus the sentinel, finishing at version 27 with
27 retained versions. Fresh reopen validates all seven fields, exact values,
revisions, UUID/version identity, format 2.0 and disabled stable row IDs.

| Public rows | Mean preparation (ms) | Mean publication (ms) | Mean complete native job (ms) |
| --- | ---: | ---: | ---: |
| 100 | 2.81805 | 1.02485 | 4.25180 |
| 1,000 | 2.86870 | 1.01575 | 4.25375 |
| 10,000 | 3.49750 | 1.03660 | 4.93245 |

Preparation is the largest measured native phase in this small-delta fixture;
the result does not establish tens of milliseconds of fixed native planning
cost. Each measured delta records one 2,210-byte data file and one preparation
attempt. Those counters omit manifest, transaction, deletion and synchronization
I/O. No maintenance runs, and these small rows do not model growing App settlement
records. Only one ordered run was measured; CPU and process RSS were not captured.

This route omits the Kit durable ObjectStore wrapper, queue, cache, leases and
cleanup ledger. Do not subtract its timings from the different supplied-adapter
route and label the difference Kit or filesystem-barrier overhead. Successful
native child draining is recorded separately from process reaping; it does not
qualify failure-path Kit cleanup. The native result identifies further measurement
boundaries but supplies no basis to weaken durability or publication guards.

The two longer App samples contain 3,100 mutating consuming-commit observations
apiece, totaling 203.852 and 204.859 seconds. Their checkpoint active-poll totals
are 4.118 and 3.942 seconds. These are different boundaries: ordinary-load totals
overlap the 314.745/317.492-second complete runs, and consuming commits include
queueing and owned finalization. They are not isolated publication or CPU counts.
The remaining cost therefore needs controlled attribution at the supplied
adapter's operation and maintenance boundaries before another source change.
`next-investigation.json` records that bounded proposal; it was not executed and
does not admit another 4,096 attempt.

**Final affected qualification remains incomplete.** The retained private App
predecessor change passed all 17 exact regressions in each of the `test` and
`capacity` profiles. App static checks passed, including Clippy, public compile
contracts, examples, rustdoc, 127 doctests, formatting and architecture guards.
Seven affected App targets passed all 251 cases. The current macOS/APFS default
Storage verifier passed; this is not a fresh execution of all alternative lanes.

| Additional App execution | Result |
| --- | --- |
| Routine `workflow_execution` | 159 passed, 1 failed; 7 explicitly excluded capacity/diagnostic cases |
| Exact preparation-entry diagnostic | 1 passed; original entry failure did not recur |
| Separate routine `worker_contract` continuation | 38 passed; 1 explicitly excluded capacity case |

The routine failure remains recorded:
`panic_during_conversation_creation_does_not_orphan_run` did not observe the
required lower conversation entry within its original two-second observation.
The later exact diagnostic added only failure-path, payload-free observations;
it changed no deadline, assertion, notification, retry or cleanup behavior. Its
pass neither diagnoses nor erases the earlier failure. No further rerun was
used to obtain a green routine result.

Source identities remain separate. The seven-target, failed routine and macOS
Storage receipts bind `432109770fb2…`; the later passive diagnostic and worker
continuation bind `edfd347f5c88…`. Both retain the same private App production
candidate. The static and paired-measurement receipts retain their own source
identities. The diagnostic changes only the preparation-test helper; the 64,
256/1,024 and 4,096 delivery method bodies remain unchanged. Exact hashes,
commands, inventories, exclusions and process cleanup are linked by
`target/capacity-mutation-evidence/predecessor-only/qualification-draft/evidence.json`;
older receipts are not relabeled as current-checkout passes.

The scoped live invocation `scoped-live-20261012-170444` has settled with a
failure result: Agent strict-live passed 22 cases and failed one mandatory
managed-knowledge exact-output assertion; App workflow-live passed all 18 cases,
and the exact four-boundary crash case passed. Six passive comparisons preserved
identical native, Provider and Agent output. The failing marker includes one
extra character already in the native response; no Agent output transformation
was observed. This remains a blocking result outside the two Question copy-grade
exceptions. Both permitted Question cases passed Kit assertions; their raw exact-
copy grade is **0/2**. The relay and handlers joined, all three invocation-owned
process groups settled and were reaped, source and runner inputs stayed stable,
and no cleanup errors were recorded.

The current Linux default Storage gate **passed on ext4** at
`linux-final-20261012-173205-d4a4cab7`, using Rust 1.94.1 and guest uid/gid 65534
without supplementary groups. Source identity remained unchanged; the owned
container was removed with no cleanup errors. This establishes default Storage
coverage, not Linux App capacity. The preceding invocation
`linux-final-20261012-172913-7bfaebd9` passed 36 Python checks before rustfmt failed
on inherited `/root/.rustfmt.toml` permissions. Its failure remains recorded. The
reviewed correction supplies a fresh child-owned home; source, container security
and test requirements did not change.

Clean-candidate compilation, native Cargo package verification for all five
crates, exact sibling archive/graph audits and extracted-consumer execution
**passed** at `packaging-20261012-174543`. The consumer uses only extracted Kit
sources and the default Lance dependency graph; it constructs no Agent runtime
and is not strict-live evidence. Its separately prepared lockfile is audited,
then execution uses `--locked`. Native package verification precedes that
consumer preparation and uses no patched substitute graph.

The prior `packaging-20261012-172645` result remains **INCOMPLETE**: native
verification succeeded, but the stricter archive audit found eight stale bytes
at the end of reused Agent archives in both package output locations. The valid
prefix matched Cargo's current temporary archive, while the suffix matched the
previous longer archive's footer. The runner now preserves only the five exact
Kit archive names in both locations before native recreation. It neither rewrites
locks nor relaxes checksum checks. Artifact proof and independent review are
`package-stale-suffix-proof.json` and `package-output-fix-independent-review.json`
under the mutation evidence directory.

The predecessor ABBA comparison and both 1,024-delivery diagnostics remain
source-bound measurement evidence. The unchanged screen estimates 1,803.621
seconds against the 1,200-second body budget, so no new full 4,096-delivery
attempt was admitted. Dedicated maximum-capacity qualification remains
incomplete, separate from routine feature-split qualification under the approved
scope amendment. The historical full-workload timeout remains failed evidence.
Independent source reviews and the 27 Python orchestration checks do not replace
these outstanding behavioral results.

Evidence is under `target/capacity-mutation-evidence`: baseline phase analysis,
`cow-abba-app-analysis.json`, `predecessor-only/abba-analysis.json`,
`predecessor-only/growth-analysis.json`, `native-phase-analysis.json`, and their independent reviews link the
raw logs, compiler artifacts, exact inventories, source identities and cleanup.
No privileged instrumentation, count/deadline adjustment, commit or push occurs
in this follow-up.

## Optimized capacity follow-up — 12 October 2026

**Capacity qualification remains incomplete.** The approved optimized profile
improves both paired 256-delivery measurements, and both real 1,024-delivery
diagnostics pass. The predefined full-run admission screen is **2,231.093 seconds**,
above its required 1,200-second threshold. Consequently the original 4,096 case
was **not rerun**. This is a failed screening decision, not an observed optimized
4,096 timeout. The historical unoptimized failure at **2,222 completed public
resolutions** remains failed evidence permanently. The earlier feature-split
completion scope is unchanged.

### Candidate, implementation and scope

This follow-up starts at `b0c5401a8136e802217bf01760552a4aea9aefd6`.
Every paired/growth execution used unchanged candidate digest
`3fba00b2cceda8ea5684ac8667eed347bb502f1a18626974895ce43a34d491b0`,
including untracked test source, and lockfile digest
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
The toolchain was Rust 1.94.1 on macOS/Apple Silicon. Actual dataset storage was
APFS on `/dev/disk3s1`, mounted at `/System/Volumes/Data`. Cargo commands ran
serially with one build job, `--locked`, incremental compilation disabled and
the existing low-memory debug settings. Compilation finished before measurement.

The workspace `capacity` profile inherits release optimization, with level 3,
debug assertions and overflow checks enabled, no debug information, incremental
compilation disabled, LTO off and 16 codegen units. The runner checks effective
optimization/assertion/overflow settings in Cargo artifact messages for the
selected test and its seven named runtime libraries, and rejects conflicting
compiler/configuration/profile overrides. This follows Cargo's
[profile controls](https://doc.rust-lang.org/cargo/reference/profiles.html) and
[artifact reporting](https://doc.rust-lang.org/cargo/reference/external-tools.html#artifact-messages).
The runner builds first and uses that same verified executable for inventory and
execution. Routine scope remains on the test profile; diagnostic scope selects
test or capacity; exact capacity selection uses capacity. Unknown, empty,
ignored or unexpectedly filtered execution cannot pass.

The existing diagnostic owner now separates successful staged mutations from
read-only consuming commits, returned errors, dropped observers and currently
active observed futures. A dropped observer does not establish native cancellation
or completed ownership. Extra JSON encoding solely for byte counting was removed
from both measured profiles. The concrete fixture supports 256 and 1,024 genuine
deliveries with sparse block observations. No production Rust, public API,
dependency, feature policy or persisted format changed. The original routine-64
and full-4,096 method bodies remain byte-identical to the committed baseline;
all original counts, final-boundary assertions and deadlines remain intact.

### Tests-first and focused validation

Orchestration tests established meaningful failures before runner changes;
all 16 now pass, including exact selection, artifact/override rejection,
interruption, primary-error preservation and invocation-owned process cleanup.
The actual optimized observer-cancellation regression failed with an outstanding
count of one before the accounting guard was applied, then passed. This red is
recorded in `20261012-125033-diagnostic-25670`; a subsequent fixture compilation
failure is separately retained, not counted as behavioral evidence.

Both profiles passed the same focused selection: **23 workflow cases and seven
deletion cases**, zero failed or ignored, with 142 and 22 deliberately filtered
cases respectively. These include the 64-delivery routine regression, exact
receipt replay, lost observers, Unknown reconciliation, count/byte reservation,
corrupt and reordered evidence, read-only finalization/rollback failures,
reconstruction and joined shutdown. Test-profile receipt:
`20261012-125330-diagnostic-25895`; capacity-profile receipt:
`20261012-125659-diagnostic-25895`. The workflow runtimes were 155.12 and
61.81 seconds respectively. These are deterministic real-App/Lance executions
with a controlled caller Agent, not real-Agent strict-live results.

Focused App test-target Clippy with warnings denied, Rust formatting, the
16 orchestration tests, no-path/no-alias guards and `git diff --check` passed in
`target/capacity-profile-evidence/final-checks.json`. Unaffected Storage feature
matrices, live suites and the other five maximum workloads were not repeated.

### Fixed ABBA comparison

Each sample executes exactly 256 deliveries, reaches checkpoint 770, observes
256 original lower calls/returns, and completes successful App shutdown. Each
exact inventory reports one passed, zero failed/ignored and 164 intentionally
filtered tests. No sample was discarded or retried.

| Sample | Profile | Body (s) | Shutdown (s) | Complete measured cost (s) |
| --- | --- | ---: | ---: | ---: |
| A1 | test | 111.449 | 0.933 | 112.382 |
| B1 | capacity | 75.969 | 0.591 | 76.560 |
| B2 | capacity | 73.683 | 1.760 | 75.443 |
| A2 | test | 110.456 | 0.925 | 111.381 |

B1 improves against A1 by **31.875%**; B2 improves against A2 by **32.266%**.
B2's shutdown is slower than A2's, but complete cost still improves. Every sample
ends with 150 files and 32 retained manifests; physical bytes range from
15,713,399 to 15,713,529. Per-process peak RSS is unavailable: the ordinary macOS
resource probe cannot read `kern.clockrate` in this environment. Its failure is
recorded before admission, then the original workload executes once without the
optional resource wrapper. Host `vm_stat` snapshots do not establish process
memory cost. Available APFS space varied during measurement and is recorded;
no causal conclusion is drawn from that variation.

### Growth screen and stop decision

Both capacity-profile samples complete all 1,024 genuine public resolutions,
reach checkpoint 3,074, retain exact identities, return all 1,024 lower receipts,
and join cleanup successfully. They use separate directory datasets. Each exact
inventory again reports one passed and 164 deliberately filtered tests.

| Sample | 1–256 (s) | 257–512 (s) | 513–768 (s) | 769–1,024 (s) | Body + shutdown (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| G1 | 75.184 | 76.960 | 79.916 | 89.363 | 322.333 |
| G2 | 75.095 | 76.569 | 76.250 | 82.662 | 312.773 |

Each ends with 63 files, 14 retained manifests and approximately 227.933 MB of
physical files (227,932,916 and 227,932,853 bytes). Available space at admission
was 12,613,836,800 and 12,395,008,000 bytes. These are observed file sizes and host
space; neither is a process-memory measurement.

The slower blocks are 75.184, 76.960, 79.916 and 89.363 seconds. The largest
positive adjacent increase is `g = 9.447` seconds. The slower combined pre-body
setup plus initial workflow/snapshot phase is 0.448 seconds. Applying the agreed
formula without adjustment:

```text
0.448 + 321.423 + sum(89.363 + j * 9.447, j = 1..12) + 100
= 2,231.093 seconds >= 1,200 seconds
```

This heuristic rejects admission of another full attempt; it is not a prediction
of actual 4,096 completion time or a bound on finalization. No optimized full-count
pass, unoptimized pass, Linux capacity result or universal throughput claim is
made. The full test and its 1,200/1,260/2,400-second budgets remain unchanged.

### Remaining bottleneck and separate source proposal

The longer samples record 3,100 successful observed mutating commits each, with
inclusive observed durations of 207.046 and 199.755 seconds. Their 10,283
read-only commits total only 0.417 and 0.421 seconds. Ordinary load observations
sum to 504.629 and 490.631 seconds, exceeding wall time because concurrent
observations overlap. These establish expensive mutation/read observation
boundaries; they do not separate queueing, App processing, merge preparation,
maintenance, encoding or durability, and do not count all native publications.
Growing retained settlement histories and full-row maintenance remain plausible
source-supported costs, not demonstrated causal attribution.

The next source investigation should retain these measurements and separate
caller preparation from consuming Storage work through the existing concrete
measurement owners. A transaction-local interval from successful native begin
to consuming commit/rollback, with its own sequential native-call durations,
can identify preparation/scheduling gaps; it is not CPU time and excludes work
before begin. If consuming mutation or maintenance remains dominant, investigate
those phases through the existing Storage measurement target before changing App.

One conditional App candidate is sharing an immutable `ConversationNotification`
inside the private settlement target. `Arc::make_mut` currently copies the retained
settlement vector, including owned notification payloads. Sharing that immutable
payload could reduce copying while retaining the same positional serialization,
constructor validation, ordered JSON, predecessor comparisons, contextual authority
and public owned values. It adds reference counting/allocation and has no measured
benefit yet. **It is not implemented or justified as the fix by these results.**
Any later experiment needs exact serialization/replay/reconstruction/Unknown
regressions and paired complete-cost plus memory evidence before retention. Do
not reintroduce rejected checkpoint caches, native Session reuse, synchronization
shortcuts or altered maintenance/deadline policies.

### Evidence and review

All new receipts are under `target/capacity-profile-evidence`: `abba-receipts.json`,
`abba-analysis.json`, `growth-receipts.json`, `growth-analysis.json`,
`focused-receipts.json`, `final-checks.json`, and the source/independent-review
receipts. They point to exact commands, compiled artifacts, inventories, logs,
source/lock identities and cleanup. Reviews distinguish observed-future
accounting from native ownership, and verify unchanged full-test assertions.
The source-only contingency is `app-contingency-source-research.json`.
This report is a documentation-only delta after the frozen executions; it does
not retrospectively change their source identities. Changes remain uncommitted
and unpushed. Capacity completion remains blocked pending a relevant measured
correction and a successful full original case.

## Feature-split completion snapshot — 12 October 2026

**Scoped feature-split qualification is complete under the explicit capacity
scope amendment below.** The two exact real managed-knowledge cases pass with
native-output evidence. App's private read-only recovery
correction is retained after focused tests and paired measurements. The complete
4,096-delivery workload failed its unchanged body deadline after 2,222 completed
public resolutions; no partial progress qualifies that case. The user then
explicitly moved all six maximum-capacity workloads out of this feature-split
completion gate: the 1,024-owner case, the 4,096-delivery case, both 4,097-head
inventory cases, and both 1,450-action history cases now belong to a dedicated
manual or scheduled capacity gate. All original counts, authored deadlines,
assertions and per-case outer budgets remain intact. Sixty-four deliveries are
routine regression coverage; 256 remain a separately selected diagnostic prefix.
The recorded 4,096-delivery failure remains outstanding in capacity qualification.
Three additional performance experiments were rejected and restored. Public APIs,
errors, persisted formats, package versions, dependency resolution, workload
counts and operation deadlines remain unchanged. These qualification receipts
were collected before publication of the candidate.

This section supersedes the status of the historical snapshot below, without
changing any earlier receipt or failure. Every result applies to the source
identity recorded by its invocation. Final native package, clean-candidate and
extracted-consumer checks passed, as did the corrected exact source-boundary
audit. App compilation, documentation, examples and static checks passed; the
initial combined static invocation remains failed because its review manifest
omitted one already reviewed private file. The affected deterministic repairs and
actual routine-selector inventories have passing scoped receipts, including the
64-delivery regression. Maximum-capacity execution is excluded only by the explicit
scope amendment above; no timeout is reclassified as a pass. Live results remain
separate, source-bound executions. Both permitted Question Kit cases passed while
their raw exact-copy grades remain **0/2**. Ignored diagnostic tools and receipts
are not product APIs. The final report update is a documentation-only delta from
the packaged candidate; it does not claim the revised report bytes were packaged.

### Routine scope and continued deterministic evidence

The revised routine policy retains real App execution and Lance persistence.
It does not change the production 4,096 retained target-generation limit, the
maximum-capacity test's 1,200-second body budget or 1,260-second activation deadline,
or its reserved-Unknown and final-admission assertions. The diagnostic prefix
and full-capacity methods remain separate from routine execution. Smaller
lost-receipt and Unknown reconciliation cases remain mandatory routine coverage.
These deterministic App cases use a controlled caller Agent and are not
real-Agent strict-live evidence.

`deterministic-20261012-023227` passed the explicitly inventoried worker remainder
(38 passed, one capacity case filtered) and workflow remainder (155 passed, six
capacity/diagnostic cases filtered). Source and invocation cleanup remained stable.
The combined 193 passing cases retain their separate exact target receipts and
source digest `077bb715a460019825e4cd4cab3fe5c5b2e3f266854c72cefcd929bcd0f63716`.
These receipts do not claim full-capacity target qualification.

The next invocation, `deterministic-20261012-024731`, completed twelve further
App targets successfully, including all 29 deletion cases. Its child-executor
target returned 78 passed and two failed. The invocation was interrupted during
`hook_execution_contract`; continuation confirmed no owned qualification process
remained. This is incomplete execution, not proof of graceful Kit shutdown for
the interrupted case. Completed target receipts remain source-bound.

Both child failures were test controls attached only to ordinary reads, while
coherent recovery now reads the checkpoint through a native transaction. The
minimal correction applies the same controls to that transaction's actual
responses. Supporting checkpoint reads stay inside the same native snapshot;
all scenario bodies, assertions, deadlines and cleanup remain unchanged.
The two exact regressions passed once after the correction:
`delivery-child-budget-20261012-103754` and
`delivery-child-ordered-20261012-103910`. Independent source and applied-diff
reviews are in `read-only-recovery/child-executor-controls/`. The new routine case passed once in
`delivery-routine-20261012-104358`: 64 distinct deliveries, 63 original lower
receipts, 63 exact receipt replays, one public cancellation, terminal checkpoint
196, and one joined lower shutdown. Runtime was 24.33 seconds; the exact
inventory was one executed test and 161 intentionally filtered cases. Original
256/4,096 methods remain byte-identical. The routine receipt binds source
`ba78bd5a6e3042601a17d76d2e0c3cf9efe773b76e53af3778ea337db88cc70f`;
command observation took 24.72 seconds, separately from libtest's 24.33-second
reported execution. No production limit or throughput claim follows from this
routine pass. The new routine inventory is 156 workflow cases plus 38 worker
cases: the earlier 193 passes and this new exact pass are separate executions,
not a claim of one 194-case invocation.

The continued `deterministic-20261012-104558` invocation settled with **FAIL**
on stable source `cef082fb934bd993c5a8c835d8dd31ad327c98e0bb30e02e03d55734d08323a0`:
14 of its 16 target selections passed, including all 80 corrected child-executor
cases (503.67 seconds) and the then-current 102 hook cases (665.20 seconds).
Outcome recovery returned 15 passed/11 failed; workspace returned 20 passed/2
failed. All selected parent inventories completed with zero ignored cases, and
owned process cleanup reported no errors. The original failures remain in that
receipt. Across the earlier remainder, twelve preserved target passes and this
invocation, the latest completed selections covered 30 distinct targets; that
aggregation is not one whole-suite execution.

The outcome failures were fixture boundaries attached only to ordinary reads.
The reviewed concrete transaction owner now observes unchanged real native
routing snapshots. Ten routing cases continue to persist actual malformed rows
and require public projection/reopen rejection. The predecessor case additionally
supplies one explicitly contradictory response after real durable corruption;
completed rollback and zero selected native commit attempts prove App rejects it
before native publication. This is negative App validation evidence, not a claim
that a later row appeared in Lance's original snapshot. All eleven exact cases
passed in `outcome-affected-20261012-112037`; the complete 26-case target passed
in `outcome-full-20261012-112159` (11.99 seconds).

The two workspace race assertions had already passed; their single subsequent
progress attempt raced terminal capacity retirement and returned
`Busy("active run inventory changed")`. The fixture now observes its sole run's
real durable App count move from one to zero before that unchanged single
replacement attempt. Atomic retirement publishes the count, active-marker
removal and retained head together; the original native conflict/fence assertions,
deadlines and final joining remain. Both exact cases passed in
`workspace-progress-20261012-112119`, followed by the complete 22-case target
in `deterministic-20261012-112220` (11.60 seconds). Both full-target green receipts
bind source `4ae156279dbd11377fa5f0febfbee98dbc3fb2fa8d6d2f1e75aabe2b953e6c9b`,
with unchanged source during execution and no cleanup errors. Source and runner
reviews are in `read-only-recovery/outcome-controls/` and `workspace-progress/`
under `target/agent-app-repair-evidence`.

The earlier 102-case hook pass did not establish the fidelity of a fault intended
for mutation preparation after introducing read-only recovery. A new control test
failed meaningfully in `hook-control-20261012-111434`: the old fixture consumed
that fault during a read-only transaction. The small receipt-qualified fixture
correction then passed the new control plus all thirteen affected existing cases
in `hook-affected-20261012-111504`: 14 passed, 89 intentionally filtered, zero
ignored, in 83.52 seconds. The new full inventory is 103. These are separate
source-bound receipts, not one 103-case execution. All original scenario bodies,
assertions, deadlines and cleanup remain unchanged; review is in
`read-only-recovery/hook-optional-read/`. No App production change was required
for these fixture corrections.

Case-by-case accounting now identifies 768 unique passing ordinary test names
across 30 targets and 32 source-bound executed commands. The
hook accounting reuses only 89 unaffected names from the earlier 102-case pass;
its thirteen affected originals and one new control use the 14-case green
receipt. Workflow accounting adds only the new routine case to the prior 155;
worker remains 38. Outcome and workspace use their new full-target passes.
The canonical crash retains its separate live receipt, and all maximum-capacity
cases plus the diagnostic remain separately scoped. This is a reconciled evidence
union, not one 768-case execution or a full workspace gate. The exact name/source
mapping is `delivery-tier-split/completed-deterministic-inventory-final.json`.

The concrete two-target workload selector is now applied and matches its
independently reviewed source. Routine execution excludes exactly five workflow
capacity cases, the 256-delivery diagnostic and one worker capacity case. The
capacity scope retains all six original exact cases and outer budgets
(9,300; 2,400; 9,000; 3,600; 9,000; 4,800 seconds). Ten disposable-command checks
cover selection, failed exit, timeout, interruption and owned cleanup; unknown
CLI scope also rejects before execution. These are runner checks, not Kit tests.
The real `app-workload-inventories-20261012-111821` invocation passed all eleven
Cargo inventory commands: 162 total/156 routine workflow cases, 39 total/38 routine
worker cases, six exact capacity cases and the exact diagnostic. This proves
selection of 194 routine cases, not their reexecution or a capacity pass. Source
and cleanup stayed stable. Applied-file audit and exact receipt accounting are
in `target/agent-app-repair-evidence/delivery-tier-split/`.

The changed Storage measurement target separately passed locked, no-default
Clippy with warnings denied in `storage-measurement-clippy-20261012-111857`
(3.46 seconds). This is one static target check, not new Storage behavioral or
maximum-capacity execution.

The complete App static invocation `app-static-20261012-112255` ran on unchanged
source `4ae156279dbd11377fa5f0febfbee98dbc3fb2fa8d6d2f1e75aabe2b953e6c9b`.
All-target compilation, the compile-only caller-trait contract, all nine example
compilations, five deterministic example executions, warnings-denied Clippy,
strict rustdoc, all 127 inventoried doctests, formatting and no-path/no-alias
checks passed. No process cleanup error was recorded. Its aggregate result
remains **FAIL**: the source-boundary ledger still contained eleven approved
files and omitted the independently reviewed coherent App recovery change in
`app-kit/src/storage/mod.rs` (exact hash `aa487f76c5d3c4500653321042db6ddb399a51e8dba70e26f92f1869b6d91f0f`).
The reviewed correction adds only that twelfth exact file and hash; the scanner
and prior eleven authorizations are unchanged. The narrow recheck passed in
`downstream-boundary-20261012-113139`: exactly twelve reviewed private repairs,
with all other downstream executable bytes unchanged. This is a validation-ledger
correction, not another production edit or permission to accept arbitrary private
changes. The original failed invocation is retained. Native package and
clean-candidate execution subsequently passed as recorded below.

### Scoped live results and exact managed-knowledge correction

The complete scoped invocation
`target/storage-feature-split-evidence/scoped-live-20261012-021006` executed the
exact intended inventories on source digest
`9fa6a092175b5e789bda67fadbba1accf69a85c511a1eb36971cc68db7972b2e`:

| Coverage | Recorded result |
| --- | --- |
| Agent strict-live, including its environment contract | **21 passed, 2 failed**, 578.21 seconds; zero ignored or filtered cases |
| App workflow-live | **18 passed**, 540.50 seconds; zero ignored or filtered cases |
| Canonical four-boundary crash case | **Passed**, 5.66 seconds; three unrelated parent cases intentionally filtered |
| Both permitted OpenCode Question cases | Mandatory Kit assertions passed; raw exact-copy grades **0/2** |

Both mandatory failures were exact managed-knowledge output assertions. The
invocation-owned passive relay captured the same four real unary generations
that reached execution before those assertions stopped their cases. The
shared-directory secondary response omitted the first `B` from Agent-B's random
suffix; the separate-directory primary response omitted the initial `F350`
from its Conversation suffix. In both cases, complete correct marker tokens
were present in the original native request JSON. The native response already
contained the omission, and its text equaled both normalized Provider text and
the public Agent response. The native responses used the configured model,
returned HTTP 200 with terminal `stop`, and had no sampling override.

This directly excludes Provider/Agent output transformation as the cause of
these two observed omissions. It does not inspect Ollama internals, establish
statistical model reliability, or explain older failures without their own
same-generation records. The settled diagnosis is
`target/agent-app-repair-evidence/knowledge-copy-clarification/same-generation-settled.json`.
All four HTTP handlers joined with zero unfinished requests; all invocation-owned
native service groups were reaped, and source and launcher identities remained
stable. The full run retains exit 101 and Agent's two failures. Its six-phase
wire qualification remains **INCOMPLETE**, correctly: the separate-directory
reopened and secondary phases were never reached.

After that evidence satisfied the user's conditional authorization, the only
source change was the exact approved user instruction in the knowledge fixture's
`PromptRequest`. It tells the real model to copy each complete marker from the
managed-reference JSON entry's `text` independently, without shortening,
reconstructing or reusing another entry's suffix. No expected marker values
were supplied. The system instruction, configured model, fresh random marker
construction, exact output assertions, generation count, deadlines, original
requests' forwarding, and Kit production code were unchanged. The source diff
and independent review are in
`target/agent-app-repair-evidence/knowledge-copy-clarification/`.

The two affected real cases were then executed once each in
`target/storage-feature-split-evidence/scoped-live-20261012-023118`, on source
`077bb715a460019825e4cd4cab3fe5c5b2e3f266854c72cefcd929bcd0f63716`.
Both **passed**: one exact selected case per invocation, zero failures or ignored
cases, and 22 intentionally filtered unrelated Agent cases each. All six
original primary/reopened/secondary phases executed with one real generation
per phase. Complete marker sets reached the native requests and were copied
exactly; native response text, normalized Provider text and public Agent output
were identical. Native Ollama was **0.40.3**, using the unchanged
`ministral-3:8b-instruct-2512-q4_K_M`. Relay handlers and owned command groups
joined without errors; source identity stayed unchanged.

Independent inspection recomputed all six comparisons from raw diagnostic
records and the exact execution inventories. A read-only virtual reversion of
that one instruction reconstructs the preceding full-run source digest exactly,
including all other tracked and untracked candidate content. The proof is
`knowledge-copy-clarification/clarified-live-independent-review.json` under the
same evidence root. Thus the 21 unaffected Agent results and the two corrected
knowledge results are linked across **separate, source-bound invocations**;
there is no claim that one invocation passed all 23 Agent cases. App's 18-case
and crash receipts remain separately recorded. Question raw-copy grades stay
**0/2**, and their narrowly permitted exception is not applied to knowledge.

The earlier original-instruction diagnostic
`scoped-live-20261012-005106` also passed both layouts and all six generations.
That result remains historical; it did not erase earlier failures or establish
the cause of the later omissions. The new execution followed a specific,
authorized instruction correction supported by failing same-generation native
evidence; it was not an unchanged rerun until success.

These are real-provider/service tests with Lance persistence. Four App cases
still deliberately control lower-boundary failures while retaining real services;
the canonical crash case proves App/Lance recovery, not model behavior.
Artificial short-page capacity fixtures remain a separate evidence category.

### App read-only recovery and fixture evidence

The existing App storage owner now reads a run head, current checkpoint and
required predecessor through one real read-only StateStore transaction. It
retains every intrinsic and contextual validation, commits the read-only owner
before returning success or absence, and then applies the existing fresh head
fence. Read-only commits publish no rows. Failure rolls back the same owner;
a failed rollback preserves the primary public error and vetoes retry. Missing
row classification occurs after successful rollback. The existing eight-attempt
fence and original deadlines are unchanged. Exact-Arc intrinsic memoization
remains a bounded validation optimization, never an authority substitute.

`delivery-red-20261012-004542` records the meaningful public-behavior failure of
`read_only_run_observation_preserves_finalization_failures` before the owning
correction. All 28 then-current deletion cases passed in
`delivery-deletion-20261012-005357` (308.18 seconds), followed by the additional
retry-veto case in `delivery-veto-20261012-011036`. The latter distinguishes a
successful native rollback followed by recovery from a failed rollback receipt
that must not retry; original errors and joined cleanup remain asserted.
These directories are under `target/agent-app-repair-evidence`.
`read-only-recovery/independent-review.json` and
`retry-veto-independent-review.json` record actual-diff review and the narrow
source proof. This evidence is real-Lance App deterministic behavior, not
real-agent strict-live coverage or a complete final 29-case target receipt.

The existing ordered-receipt, attempt-read and notification-corruption controls
were moved within their concrete fixture owner so the same controls apply to
ordinary reads and transactional reads. Unarmed native calls and assertions are
unchanged; supporting reads inside a transaction use that same native snapshot.
The exact failed/green probes and review remain in the delivery-order,
notification and authority receipts and
`readonly-fixture-extraction/independent-review.json`.

A separate fixture correction prevents new read-only observations from consuming
faults intended for eight cancellation mutations. The held single run driver
and absence of Agent instances establish the observation scope. Failures now
occur at the mutation's receipt read after real Lance transaction admission,
and the original App rollback owner settles them. The passive rollback witness
first failed with `(0, 0, 0, 0)` instead of `(8, 8, 0, 0)` in
`delivery-driver-20261012-012550`, then passed in
`delivery-driver-20261012-012704`. The assertion occurs after normal App shutdown,
so the meaningful red does not strand the held driver. Existing cancellation,
single-driver, progress and deadline assertions remain. This is deliberate
lower-boundary error-handling evidence, not a native failure or live model claim.

The existing admission monitor also needed to distinguish read-only recovery from
mutation admission. Meaningful red `delivery-advance-20261012-014931` reached one
held native commit with zero receipt writes/deletes, no run advance and successful
read-only completion; its required real-conflict assertion failed after cleanup.
It therefore established a fixture coverage defect, not a production conflict
failure. The reviewed correction holds the actual mutation at its first receipt
read for the five existing-run cases while retaining the original pre-begin mode
for fresh admissions. Actual Lance commit errors are forwarded unchanged.
`delivery-advance-20261012-015024` passed after the correction, followed by the
admission, native-count, native-generation, native-cancellation, fresh-admission
and fresh-generation exact probes through `015140`. The 16 real competing
admissions, unchanged original head before release, real rejected stale mutation,
and subsequent progress remain asserted. The source review is
`read-only-recovery/mutation-monitor-applied-review.json`.

The predecessor evidence control previously altered both coherent read-only
recovery and the later mutation read. That allowed recovery to author the same
altered predecessor, removing the intended contradictory-read premise. The same
StateOrder failure was reproduced against the retained read-only production
baseline (`delivery-checkpoint-order-20261012-015754`) and the optional typed
candidate (`015534`). One transaction-local boolean now requires a successfully
read mutation receipt before the existing exact-key predecessor alteration.
Read-only recovery observes the original row; all original ordering, numeric,
revision/byte nonmutation, deadline and cleanup assertions remain unchanged.
The baseline exact probe passed in `delivery-checkpoint-order-20261012-020046`,
and the typed candidate probe passed separately in `020144`. This is deliberate
mutation-read contradiction evidence over real Lance, not native corruption or
live-agent behavior. Source proof and review are in
`read-only-recovery/predecessor-mutation-source-proof.json` and
`predecessor-mutation-independent-review.json`.

### Measured retained change and rejected experiments

Two identical 256-delivery diagnostic prefixes preceded and followed the
read-only change:

| Complete prefix | Baseline | Read-only candidate |
| --- | --- | --- |
| First execution | 109.432 s | 102.648 s |
| Second execution | 112.877 s | 107.127 s |
| Mean | 111.155 s | 104.888 s |

The mean improved 5.64%. All samples retained checkpoint 770, 256 returned
receipts, 6,184 staged puts and 15,088,963 diagnostic encoded bytes. Completed
ordinary load observations decreased from 18,521 to 9,287 while transaction loads
and begins increased. Summed consuming-commit observations remained roughly
69–73 seconds. Those sums may overlap and are not CPU time or independently
attributable worker/native phases. The measuring fixture's diagnostic encoding
is included equally in both candidates.

Directory footprint after joining changed from 142 files/30 retained manifests
to 150 files/32 manifests; this difference is disclosed rather than assigned a
cause without evidence. No per-process peak RSS or native allocation measurement
was available. Retained snapshots and decoded values still carry their existing
memory costs. `read-only-recovery/paired-prefix-analysis.json` records source
identities, exact counts and limitations. A prefix improvement cannot qualify
4,096 deliveries or extrapolate successful completion within its deadline.

Three further candidates were measured and **not retained**:

- Prevalidating authored App run rows into the existing four-entry memo produced
  complete prefixes of 103.728/109.145 seconds against 102.648/107.127 seconds.
  It also retained up to four additional parsed authored rows per concurrent
  commit through finalization; encoded-byte size would not bound their heap
  footprint. The production and documentation changes were restored.
- Bounding repeated directory synchronization at the canonical StateStore root
  produced prefixes of 108.811/102.814 seconds, a mean regression of about 0.88%.
  Separate shallow/deep measurements showed mixed costs: deep maintenance
  improved, but both shallow complete phase sums regressed. Available APFS space
  also differed between runs. The production, test and documentation experiment
  was restored; its source proof and passing semantic probes do not override
  the measured complete-cost result.
- Retaining typed authored checkpoints instead of reparsing their freshly encoded
  values preserved the seven focused checkpoint, capacity, notification and retry
  probes, but failed the complete-cost measurement gate. A fresh read-only
  baseline pair (`delivery-prefix-20261012-015151`) took 95.485/96.987 seconds for
  the body and 0.811/0.801 seconds for shutdown. The typed pair (`020411`) took
  96.897/97.613 seconds plus 0.819/0.815 seconds. Complete body-plus-shutdown cost
  therefore increased from 96.296/97.788 to 97.716/98.428 seconds: both pairs
  regressed, with a mean increase of 1.0614%. Both sources stayed unchanged during
  their invocations, all exact tests executed once per sample, and cleanup passed.
  All samples retained checkpoint 770, 256 returns, 6,184 staged puts,
  15,088,963 diagnostic encoded bytes, 3,350 completed transaction begins/commits,
  16,203 transaction loads and 9,287 ordinary loads. This is prefix evidence only.
  The unarmed predecessor-marker fixture correction occurred between candidates,
  and APFS free space changed; these comparison limits are disclosed. The typed
  candidate would also retain checkpoint/predecessor clones through staging and
  up to four pending rows per in-flight mutation, alongside the existing four-row
  memo. No process peak RSS was measured. The production source was restored to
  the read-only candidate `aa487f76`; source correctness and focused test success
  did not justify keeping an optimization without measured benefit. Exact data
  and source identities are in
  `typed-authored-checkpoint/paired-prefix-analysis.json`.

The first two paired receipts are `delivery-prefix-20261012-011137` and
`delivery-prefix-20261012-012112`; the full depth comparison is
`rooted-sync/paired-candidate-analysis.json`. The native-path review in
`rooted-sync/native-commit-cost-review.json` establishes that the pinned Lance
7.0.0 implementation already selects its optimized full-schema merge plan and
uses the original Dataset for the consuming commit. It does not establish that
native merge, synchronization, planning or any single component caused the App
timeout. No native retry, barrier, maintenance threshold, runtime ownership or
publication guard was weakened to improve timing.

Contemporaneous environmental measurements also require disclosure: `df` reported
100% capacity on the approximately 1.8 TiB APFS data volume while approximately
13 GiB remained available. The coordinator measured about 23 GiB in this checkout's
`target/debug`, 692 MiB in its package artifacts, and about 3 GiB in task evidence.
The paired run's ordinary `available-space.log` records the dataset's device and
free-space snapshot; these checkout directory sizes are separate coordinator
observations. Low free space is a comparison confound, not an established cause
of the delivery timeout or proof that the fixed workload is impossible. No
unrelated files were cleaned or proposed for removal to obtain a passing result.

### Current full workload and retained Storage evidence

The original exact 4,096-delivery case **failed** in
`target/storage-feature-split-evidence/deterministic-20261012-012808`. It retained
all original calls, returns, final boundary assertions, the 1,200-second body and
1,260-second authored node deadline. Its exact nonempty selection executed once:
0 passed, 1 failed, 0 ignored and 160 unrelated tests intentionally filtered.
The body reached its deadline at 1,200.115 seconds during resolve index 2,222,
after 2,222 completed public resolutions. The independent lower counters were
2,224 notifications received, 2,223 resolution calls and 2,223 returned lower
results. These are distinct observations, not interchangeable completion counts.
A single diagnostic public snapshot then observed checkpoint 6,671, running
status, no current interactions, no last action and no outcomes. The required
4,095/4,096 capacity and reserved-Unknown boundary assertions were not reached.

The initial `CancelAndDrain` observation returned `Timeout`. The same App/runtime
remained owned and driven; its first subsequent cancellation-only observation
returned retained `Integration("workflow execution authority observation failed")`
at total elapsed 1,253,909 ms. For this fixture's established cancellation-only
history, App installs Closed after draining owned work and consumes its supervisor
join before returning that retained finalizer result. This establishes joined
ownership with a failed operation outcome; it does not establish successful durable
shutdown publication. The original body timeout remained the primary failure.
The harness did not loop waiting for a retained error to become success.

Libtest finished with exit 101 in 1,258.97 seconds. The 2,400-second process
supervisor budget did not expire; its execution status was FAIL, no command
remained active, and invocation-owned process cleanup recorded no errors.
Candidate digest
`d37a73ef9e508f9f661c6d193894ae5f1f54da8893ba8032600e542dc18181f4`
and lock/runner identities were unchanged throughout that invocation.
`target/agent-app-repair-evidence/final-qualification-plan/delivery-4096-terminal-review.json`
records independent receipt inspection and the narrow joining source proof.
Public wait/resolve timing sums identify where observation time was spent but do
not attribute the timeout to a particular native component or establish a new
production correctness defect. The earlier 1,064-return timeout and all prior
failed executions remain failed receipts. No live or capacity PASS follows from
partial progress, prefix improvements or joined cleanup.

After restoring the rejected experiments, all 133 checked Storage production and
workspace/crate manifest/lock files exactly match the successful
`linux-final-20261011-233020-4f7290cd` snapshot. All nine script inputs also match
the separate 36-case `linux-script-only-20261011-234919-3c4bb8a8` PASS receipt.
`target/agent-app-repair-evidence/final-qualification-plan/storage-production-continuity.json`
records the comparison. Those historical commands retain their exact original
identities and scopes; the comparison is not a new execution. Unaffected Storage
feature-matrix/backend and default platform evidence is retained rather than
repeated solely because App recovery and diagnostic measurements changed.

The original 24-legal/8-illegal feature matrix and eight macOS lanes predate
four later Lance payload-reuse source changes, their two test-file changes,
verifier cleanup fixes and the exact Agent sibling requirement. They are not
byte-identical full-current-candidate receipts. The unchanged feature graph and
alternative backend owners support reuse of their recorded compatibility and
alternative coverage; the later macOS/Linux default Lance and 36-case script
receipts supply the changed implementation/orchestration evidence. These are
complementary, explicitly scoped source-bound results, not a claim that all eight
lanes ran again against the final candidate.

The original capacity inventories remain preserved: 1,024 retained execution
owners, 4,096 deliveries, both 4,097-head inventories, and both 1,450-action
cases. Their older completed receipts remain source-bound history after the App
recovery change. The explicit scope amendment removes all six maximum-capacity
executions from this feature-split completion gate; the delivery case remains
failed in the dedicated capacity ledger. No further performance correction is established
by the current paired measurements. This is a bounded finding about current
evidence, not a claim that improvement is impossible.

Native five-package verification and clean-candidate/extracted-consumer execution
passed in the final package invocation below. The corrected exact source-boundary
recheck also passed. App formatting, all-target compilation, compile contracts,
examples, Clippy, rustdoc/doctests and no-path/no-alias checks passed in the
source-bound static invocation above. The two failed deterministic targets passed
their complete repaired inventories, and the reviewed routine selector's actual
inventories passed. The original static aggregate failure remains retained.

The full scoped live invocation and exact corrected knowledge invocations are
settled as reported above: 21 unaffected Agent cases plus two corrected exact
knowledge cases across separate source-bound invocations, 18 App workflow cases,
and the canonical four-boundary crash case. Both Question Kit assertions passed;
the raw copy grades remain **0/2**. Subsequent changes are confined to deterministic
fixture controls, the new 64-delivery regression, script selection and reporting;
they introduce no changed live prompt, shared live fixture or production behavior
requiring another live invocation. This preserves the existing live receipts'
source/coverage limits, not a new live execution claim.

The completed App remainder and new routine probe account for 194 ordinary cases
across separate receipts. All six maximum cases and the diagnostic prefix retain
their separately declared scopes; neither full Cargo target is claimed qualified
at maximum capacity. No complete scoped PASS is inferred from an interrupted
process or from historical receipts covering changed code.

### Final native packaging, consumer and source continuity

`target/agent-app-repair-evidence/packaging-20261012-113511` passed on Rust 1.94.1,
aarch64 macOS and the independently observed APFS snapshot filesystem. It binds
candidate `d172b4201a40e197f59b08a518f1e2955b5ed86c40395c65f242163ed9f6b147`
and repository lock `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
The complete source snapshot contains 985 tracked and untracked candidate files;
ignored credentials, target artifacts and historical `tmp/**` are excluded.

The locked clean-source Agent/App library check freshly compiled all five local
Kit libraries from that snapshot in 16.72 seconds. Existing native dependency
artifacts were reused; this is fresh candidate-source evidence, not an empty
build-cache claim. Native `cargo package --locked --allow-dirty` then packaged and
verified all five crates in 72.14 seconds, without `--no-verify` or dependency
patching. All packaged original file bytes match the snapshot and every production
source is present. Storage contains zero vendor bytes. Independent raw archive
SHA-256 values are:

| Package, all version 0.1.0 | Archive SHA-256 |
| --- | --- |
| providers-kit | `2c19302df129685bf8c9ad1f9e660accdd2d795506ea5399fbe687d8f151e1e6` |
| storage-kit | `5ad29e8bd126b274f0baa5259edf3ad963375b008820cca7d68b7ea506c06595` |
| tools-kit | `a48fe55e7c348ee8acbefa826477c5573478f846f55826f30e50d95d15378a88` |
| agents-kit | `73eba2a649036543342db0172032e9814614a51903ef83283a0db9dc74731559` |
| app-kit | `fda69faee80403f5dccb1aa610e5204c365ca744fee39af43e220def987d5e01` |

The Agent and App packaged lockfiles identify their exact candidate sibling
archives by these checksums. App's normalized Agent requirement is `=0.1.0`.
No package version or repository lockfile changed. The separate extracted-consumer
composition deliberately maps all five siblings to those extracted archives after
native package verification. Its first seeded-lock `--locked` metadata attempt
returned 101; that is explicitly recorded as preparation, not a release-gate pass.
Audited offline lock pruning preserved allowed registry identities and dependency
edges, followed by successful **locked** metadata, graph and execution checks.
The consumer normal/build graph contains Lance and none of the five alternative
backend packages. Its real Lance-backed workflow and joined shutdown completed
in 44.36 seconds. It constructs no native Agent runtime and provides no additional
live-model evidence.

Every final invocation command retained its source identity, with no timeout or
process-cleanup error. The final local Kit artifact cleanup returned zero.
Independent review recomputed all archive/member checksums, checked all five
resolved consumer source paths, native sibling identities and the unchanged
985-file snapshot. The earlier `packaging-20261012-113324` failure is retained:
sandbox DNS prevented registry access, the owned Cargo group was stopped and
settled, and local artifact cleanup succeeded. The separate retry used authorized
ordinary network access; no privileged instrumentation was involved.

`target/agent-app-repair-evidence/final-qualification-plan/package-final-independent-review.json`
records that audit. The final change after successful packaging is this report's
status and evidence update only; source/manifest continuity must name that one
document difference rather than claim identical final archive bytes. All six
maximum-capacity cases retain their dedicated scope and original budgets, with
the 4,096-delivery timeout still outstanding. Scoped completion does not mean
maximum-capacity qualification, whole-workspace live qualification or model-copy
reliability. The user authorized committing and pushing this qualified scope on
12 October 2026; the outstanding capacity failure remains separate follow-up work.

## Historical qualification snapshot — 12 October 2026

**Scoped qualification remains incomplete.** The feature split, authorized
Agent/App corrections, exact packaged Agent requirement, and measured Lance
JSON reuse are implemented. Public APIs, persisted formats, package versions,
crate boundaries and Storage feature policy are unchanged. Nothing is committed
or pushed. Two mandatory managed-knowledge live cases and the original
4,096-delivery capacity workload remain failed; no additional Agent production
correction is justified by the new output-boundary evidence alone.

The final executable candidate has digest
`3128c2a4bbdeca74e6453d226bff2669d40f239a961ecdad37ebafd0ad77a025`.
The exact digest in each execution receipt is authoritative; the reporting-only
edit adding this section follows execution. The workspace lock remains
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
All commands below use Rust 1.94.1 and serial, locked Cargo execution where
applicable. The evidence directories are under `target/` and are not packaged
product APIs.

### Real live behavior

`storage-feature-split-evidence/scoped-live-20261011-235154` completed the exact
inventories without missing, ignored or unintended filtered parent cases:

| Coverage | Result |
| --- | --- |
| Agent strict-live, including environment contract | **21 passed, 2 failed**, 698.68 seconds |
| App workflow-live | **18 passed**, 547.06 seconds |
| Exact canonical four-boundary crash case | **Passed**, 5.32 seconds; three unrelated parent cases intentionally filtered |
| Both approved OpenCode Question cases | Mandatory Kit assertions passed; raw exact-copy grades **0/2** |

Both failures are mandatory managed-knowledge cases, outside the Question copy
exceptions. A passive caller-defined Provider/Model observer forwards all six
required methods to the real supplied implementation, reusing its Session and
continuation types. It introduces no generation, task, retry, changed request,
response, deadline, streaming behavior or shutdown behavior.

For each failing secondary phase, the observer recorded one unary generation,
the correct complete Database and Agent-B markers exactly once in the original
normalized request, and exact equality between normalized Provider text and the
public Agent response. In the shared-directory case, Provider text used the
Database marker's UUID for Agent-B. In the separate-directory case, it omitted
Agent-B's leading `D`. The original exact-output assertion failed first; the
later secondary evidence assertion was consequently not reached. Printed
observations are distinguished from passing assertions.

This localizes the discrepancy at or below the normalized Provider response
boundary. It does not distinguish Provider wire normalization from native
server/model behavior, and proves no general absence of Kit defects. The earlier
exact diagnostic run `scoped-live-20261011-223927` remains **FAIL** (one layout
passed, one failed). The final full run was justified by the intervening Lance
production change, not an isolated retry for a favorable output. No assertion,
model, output or acceptance exception was changed.

Four App cases deliberately control lower-boundary failures while retaining
real services. Their expected retained `Integration` outcomes and retained
fixture paths remain visible; successful test assertions do not mean every Kit
shutdown succeeded. The canonical crash case proves App/Lance process recovery,
not model behavior. Separately, all three invocation-owned native server groups
were reaped, test groups settled, and source/launcher identities stayed unchanged.
`service-ownership.json` and the independent final live review retain those
separate facts.

### Capacity and measured implementation change

The existing private JSON owner now memoizes exact validated payload bytes.
Authored encoding is lazy and shared by immutable clones. Maintenance still
scans and validates every persisted row; reuse requires the same identity,
revision, presence and exact bytes. Changed bytes are decoded completely.
Initialization and external changes retain full decoding, and uncertainty still
invalidates the image cache. Publication guards, tombstones, revision arithmetic,
leases, cleanup, native format and maintenance scheduling are unchanged.

The two new public-behavior cases verify changed valid JSON at the same revision
and rejection of changed malformed JSON before reclamation. Both already passed
before optimization; the meaningful performance baseline was measured rather
than manufacturing a behavioral failure. All **88 StateStore cases** passed
normally (`scoped-live-20261011-225024`) and with explicit `preserve_order`
(`storage-preserve-order-20261011-231843`), including deep JSON, copy-on-write,
cancellation, process conflicts, revisions and maintenance.

Two identical baseline/candidate pairs each used 128 records and 32 MiB of
encoded payload across the actual version-64 maintenance boundary:

| Shape and measured cost | Baseline pair | Candidate pair |
| --- | --- | --- |
| Structured JSON: commit plus boundary read | 4.973 / 5.018 s | 1.318 / 1.302 s |
| Structured JSON: sum of measured public phases and joins | 12.509 / 12.844 s | 7.172 / 7.127 s |
| String payload: commit plus boundary read | 2.001 / 2.047 s | 3.823 / 1.301 s |
| String payload: sum of measured public phases and joins | 6.228 / 6.282 s | 8.445 / 5.367 s |

The first string regression is retained, with no inferred cause. Summed phases
exclude assertion/native-inspection overhead and are not total test wall time.
Encoded bytes add resident memory beside decoded values (32 MiB of payload in
this probe); ordinary process-memory metrics were unavailable. The repeated
payload shares one input `Arc`, so this is a controlled mechanism measurement,
not a representative App capacity benchmark. Receipts are
`scoped-live-20261011-224103`, `scoped-live-20261011-225240`, and
`agent-app-repair-evidence/maintenance-byte-reuse-comparison.json`.

The original delivery workload then ran once after that measured correction:
`deterministic-20261011-225416` **failed at the unchanged 1,200-second body
budget**. It completed **2,000 public resolutions**; 2,001 lower calls returned,
with public resolution index 2,000 still under observation. The 4,096-delivery
limit/Unknown assertions were not reached. At the matched 1,024 milestone,
elapsed time improved from 928.872 to 519.732 seconds, but successive block
costs continued growing. This does not attribute the remaining cost to a native
phase or establish the required operating envelope. No count, node deadline,
observer budget or final-boundary assertion was reduced or enlarged.

The log and fixture control flow support successful initial App shutdown;
ordinary test exit 101, no outer supervisor timeout and no process-cleanup error
are separate evidence. Earlier failures remain recorded. The independent
`settlement-byte-reuse-final-comparison.json` distinguishes lower receipts from
completed public observations. Full inventory tests with artificial short pages
remain capacity/pagination evidence, not real-agent strict-live evidence.

### Packaging, platforms and execution ownership

The single approved manifest correction pins the workspace Agent requirement to
`=0.1.0`. Native verification of all five packages now passes, without patching
its dependency graph. `agent-app-repair-evidence/packaging-20261011-232410`
also passes exact sibling archive checksum checks, all 398 production-file byte
comparisons against its 984-file snapshot, and locked extracted-consumer
execution with joined cleanup and a Lance-only normal graph. Consumer lock
preparation is separately recorded; extracted-path patches are used only after
native package verification. Package versions and the workspace lock did not
change. The path-wiring guard's exact Agent allowlist literal was updated to
match this requirement; its red/green receipts and independent review are saved.

The preceding `packaging-20261011-231604` passed native verification but remains
**INCOMPLETE** because its audit compared Cargo's dependency shorthand literally.
The corrected audit resolves each edge to exact name/version/source/checksum,
rejecting drift and ambiguous references. Nine focused Python checks went from
six passes/one failure/two errors to nine passes. This correction changes only
ignored qualification tooling, not Kit dependencies or behavior.

The default Storage gate passes on macOS/APFS in
`storage-default-final-20261011-234709`: **157 parent integration cases, 240
doctests, 24 embedded strict-live cases and 36 Python cases**, plus formatting,
compilation, Clippy and rustdoc. The default Linux gate passes on actual ext4 in
`linux-final-20261011-233020-4f7290cd`, with the same Rust totals and its preceding
34-case Python inventory. Linux used Docker defaults and a verified fresh source
and build directory. The later script-only
`linux-script-only-20261011-234919-3c4bb8a8` passes all **36** updated Python cases
with exact input hashes, no Cargo/backend execution and settled container
ownership. Nested child executions are not counted as additional parent cases.

Two macOS attempts (`232050`, `232836`) remain failed. The latter captured an
`EPERM` during a process-group existence probe after forced child termination;
the verifier correctly retained resources when it could not establish
settlement. A small correction now reobserves that error within the existing
deadline, requiring both a reaped direct child and observed group absence before
resource deletion. Persistent denial retains ownership and error context.
[Apple's process-group signal implementation](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/kern/kern_sig.c)
supports an exit-between-poll-and-probe explanation; the failed scheduling was
not traced and the upstream source is not claimed identical to the running
kernel. Six controlled owner checks went from five passes/one failure to six
passes; the actual stubborn-child case and full macOS gate then passed. No
privileged instrumentation, new permission rule or relaxed settlement assertion
was introduced.

Final Agent all-target/all-feature Clippy, no-path/no-alias guards and diff
checks pass in `scoped-live-20261012-001631`; it is static validation, not a live
run. Independent actual-diff and receipt reviews cover each change. Relevant
source comparisons retain the earlier package/88-case evidence across later
script-only edits. Earlier eight-lane/24-legal/eight-illegal receipts retain
their recorded feature-graph coverage; they are not represented as fresh
whole-candidate executions. No Provider/Tool live suites or workspace-wide live
wrapper ran. The outstanding work is the mandatory knowledge-output discrepancy
below the Agent boundary and completion of the unchanged delivery capacity
workload; neither is waived by these passing checks.

## Earlier source-bound qualification entries (historical)

The entries below retain earlier results and contemporaneous status statements.
They do not override the current section or qualify subsequent source changes.

**Scoped qualification remains incomplete.** The feature split is implemented:
Lance is always available for both products, the five alternatives require
explicit features, incompatible SQLite/Qdrant unions are rejected, and real
downstream persistence and knowledge fixtures use Lance. Public contracts remain
unchanged. Agent/App private production repairs are now authorized; the original
prohibition recorded in historical sections no longer blocks those corrections.

Earlier Storage candidates have passing receipts for all eight macOS backend
lanes, 24 legal feature compilations, eight expected illegal-union rejections,
macOS/APFS and Linux/ext4 default gates, packaging, an extracted downstream
consumer, default dependency isolation and five separate opt-in builds. These
receipts retain their source identities. They do not automatically qualify the
later Lance image-cache optimization or subsequent Agent/App repairs.

The refreshed macOS/APFS verifier, `storage-all-20261011-134317`, now passes
all eight check/behavior lanes, all 24 legal feature compilations, all eight
expected incompatible-union rejections, and every selected live backend
invocation. It ran with Rust 1.94.1 and the unchanged lock hash
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Its source digest was
`3f8e8f569869e8889c70cffb0e468df5616640761f32b7e2ae65e98d39795ebd`
before and after execution; the complete 984-file candidate manifest separately
records digest `66f12f03bb47e19787534efb477927e6a3aec924585cdc964b69aabdf2bc87c1`.
These are explicitly different digest algorithms. The invocation took
6,828.883 seconds; owned services, volumes, network and launcher process group
settled without cleanup errors. Seventeen separate selection/target logs were
retained with hashes alongside the aggregate receipt. A later scope audit
compared all 168 relevant Storage source/test/script, workspace/dependency and
toolchain inputs against the saved matrix manifest and found no changes. The
recorded eight-lane, 24-legal and eight-illegal results therefore retain their
exact relevant-input coverage; they are not pending feature-matrix work or a new
whole-candidate execution.

Explicit `serde_json/preserve_order` coverage also has an 86/86 pass after the
cache changes (`storage-preserve-order-20261010-225422`, source digest
`ebf8ecdb2d14707076ec0bb9e53734dae22b3617241ea401a52e33b0a610162e`).
Saved manifests before and after that execution match current relevant Storage
inputs. This supports scoped continuity, with a disclosed limit: no complete
file manifest matching that execution's exact digest was found, and the
inherited runner later changed. The receipt remains bound to its recorded
source; it is neither a fresh current-candidate pass nor a waived gate. Exact
current-candidate execution, if required for final acceptance, remains absent.
These distinctions are recorded in
`target/agent-app-repair-evidence/remaining-qualification-source-scope-audit.json`.

Fresh default Linux Storage validation now passes in
`linux-final-20261011-204057-306521bd/receipt.json`. Rust 1.94.1 on
`aarch64-unknown-linux-gnu` ran the default Lance verifier against actual ext4
storage on `/workspace`; the gate took 699.425 seconds. The 984-file candidate
was verified inside the container, and before/after source identity remained
`bad3638d567042216d69020cdbf74379e4b03822e16d7f85fd213952e771d193`,
with unchanged lock hash
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
The run used ordinary Docker defaults, a fresh candidate/build directory and
fresh Cargo configuration; its owned container settled without cleanup errors.
Independent archive/log review confirmed 155 deterministic integration cases,
240 doctests, 24 embedded strict-live cases and 34 Python script cases, plus the
selected compilation, formatting, Clippy and rustdoc checks. The nested exact
child repeat is not counted as additional parent coverage. This qualifies the
selected default Storage gate and locked clean-candidate build at that source,
not Linux alternatives or Agent/App live tests. Its detailed review is
`linux-current-default-independent-review.json`.

Storage packaging now also passes at the same source and workspace lock in
`packaging-20261011-210142-515fd87f/receipt.json`. The Rust 1.94.1 macOS
invocation verified the 176-file Storage archive with `vendor/**` excluded,
then executed the extracted-package consumer for memory and directory storage,
clone/independent-connection conflicts, tombstone revision continuity, reopen,
same-directory knowledge coexistence, reserved-table protection and direct
caller trait implementation. Five separate locked compilation checks passed
for Redb, PostgreSQL, SurrealDB, SQLite and Qdrant opt-ins. These are compilation
checks, not additional alternative-backend live runs.

Four separately captured dependency queries confirmed Lance-only default
Storage and Agent/App normal/build graphs. Default and no-default Storage
outputs matched after removing only the already-declared empty root `default`
marker. The packaged consumer's separately recorded 516-registry-package lock
remained stable through execution and all opt-ins; every registry package
identity matched the workspace resolution. An initial locked seed probe failed
because that new consumer needed lock reconciliation; the recorded offline
reconciliation was preparation, followed by successful locked qualification.
The archive and consumer source/lock hashes remain in the receipt. Every
qualification command exited zero, and owned groups settled without cleanup
errors. The changed Agent/App clean-library check subsequently passed, but the
package closure failed as detailed below; this Storage receipt does not qualify
those packages or the unresolved live and capacity results.

Two earlier package attempts remain **INCOMPLETE**, both at unchanged source
`bad3638d567042216d69020cdbf74379e4b03822e16d7f85fd213952e771d193`.
`packaging-20261011-205303-4964c99d` encountered crates.io DNS failures in the
sandboxed package command; supervised interruption recorded exit -15 and no
cleanup errors. `packaging-20261011-205621-3a3dfff9` stopped before packaging
because its harness compared combined stdout/stderr as dependency graph data.
Both Cargo graph commands exited zero; the raw difference contained two
package-cache lock-wait diagnostics plus the already-accounted empty `default`
marker, not a dependency change. The ignored runner now separates and hashes
stderr for only its four graph queries. Meaningful ordinary Python-child tests
recorded three passes/one error before the correction and four passes afterward;
actual-diff review confirmed graph comparisons, exit handling and owned cleanup
were unchanged. These are harness receipts, not Cargo/package verification.
The subsequent real Storage package invocation above passed with that corrected
runner. Both earlier attempts retain their incomplete result and source identity.

The first fresh Agent/App package attempt, `packaging-20261011-211934`, remains
**INCOMPLETE**. Its locked snapshot library check exited zero, and Cargo reported
all five local Kit artifacts as freshly compiled. The harness then rejected the
Tool artifact because its source check compared the snapshot's `/var` spelling
with Cargo's canonical `/private/var` spelling. It did not reach package archive
verification or consumer execution. The candidate and workspace lock stayed
unchanged, and exact local-package cleanup completed without errors; the runner
recorded `root_requires_local_package_clean = false`. This remains a failed
source-verification attempt, not a completed Agent/App package qualification.

The one-line snapshot-path correction was independently reviewed after three
passing/one failing script-level checks became four passes. It canonicalizes the
owned extraction root without weakening artifact membership, fresh-compilation
requirements, cleanup or supervision. Those script checks do not themselves
qualify the Kit packages.

The corrected Agent/App attempt, `packaging-20261011-212606`, is **FAIL**.
The 984-file clean-candidate library check passed in 17.406 seconds on macOS/APFS,
with all five local Kit libraries freshly compiled from the verified snapshot.
Native Cargo package verification then completed Provider, Storage, Tool and
Agent 0.1.0 before App verification compiled registry `agents-kit` 0.1.1 instead
of this candidate's 0.1.0. App failed with 56 compilation errors involving missing Agent traits and modules;
the package command exited 101 after 307.943 seconds. The current source and
workspace lock stayed unchanged, and exact local-package cleanup succeeded with
`root_requires_local_package_clean = false`. This is an actual package
verification failure, not merely another source-check harness failure. The
extracted Agent/App consumer was not reached. Exact-candidate package closure
and consumer qualification remain outstanding; no requirements or package
versions have been changed to obtain a pass.

The repair continuation has focused green evidence for Codex creation
reconciliation (10 cases), OpenCode cancellation persistence (9), explicit
caller reconstruction (1), App workspace fencing (22), both App cancellation
fixtures, Lance StateStore (86), explicit deep-JSON `preserve_order` coverage
(86), and the OpenCode binding robustness correction (6). The newly demonstrated
OpenCode completed-transport re-poll defect has a meaningful red receipt and a
focused green receipt (1 case) after immediate future retirement. Full affected
target execution and independent final-candidate qualification remain required.

The current scoped live receipt, `scoped-live-20261011-004853`, executed Agent's
complete inventory with **22 passed / 1 failed**, App's complete inventory with
**18 passed / 0 failed**, and the exact canonical four-boundary crash case with
**1 passed / 0 failed**. Inventories contained no unexpected filtered or ignored
cases; invocation-owned process groups settled. The overall live result is
**FAIL** because the required Agent question-delivery case did not establish its
native prerequisite. Exact readback showed one matching native request followed
by one completed Assistant with no text, no tools and no pending question.
The Kit preserved that empty output. No lost Kit Question is demonstrated;
model versus native-runtime attribution remains unresolved. This is not a copy
quality exemption: the delivery case has no grade receipt. The other designated
OpenCode case met its Kit assertions and recorded a false exact-copy grade.
The two-receipt grade reporter therefore correctly failed.

A subsequent zero-generation diagnostic used a fresh owned OpenCode 1.17.18
server with the same approved isolated configuration and model. Supported
read-only discovery found Question registered, exposed to that model, and
allowed by the default build agent, with the expected 4,096/65,536 limits.
The server was reaped without cleanup errors. Source review also found that
both Question cases share the same session options; Kit session creation
explicitly allows tools, and prompt serialization cannot override agent,
tools or permissions. Neither source reasoning nor this current discovery
establishes the original provider request or historical session state. No new
conversation or generation was created, and the original live failure remains
blocking rather than being replaced by another stochastic attempt.

The delayed-terminal Codex correction passed the full 74-case deterministic
target and its formerly failing live case. Custom passed 56 deterministic cases
and all current live knowledge cases; the earlier mandatory output mismatch
remains historical evidence, not a waived assertion. The subsequent deterministic
run `deterministic-20261011-012429` passed all **533 Agent cases**, including all
299 OpenCode cases. Its completed App commands passed 725 cases and failed three
fixture assertions; the later 4,096-delivery invocation was interrupted before a
terminal result. The overall run is **INCOMPLETE**, not a full qualification.

The three App failures do not establish new production defects. The capacity
fixture expected `Running` for a prepared Input whose contract status is
`Paused`; its reviewed correction preserves all 1,024 actual owners. The receipt
probe counted JSON allocations retained by Lance and by the test itself, rather
than only returned pages. A new calibration reproduced that failure before the
test-only ownership isolation was applied; the original two-page bound remains.
Finally, sixteen concurrent preparations can legitimately exhaust the existing
eight-attempt admission budget. Two new deterministic cases passed against
unchanged production code: one real Lance conflict retries with the original
intent, and eight real conflicts leave the complete App inventory unchanged
before a later caller request succeeds. The concurrency test now accounts
for every initial outcome and proves nonpublication before resubmitting
only definitely rejected logical inputs. Focused execution
`fixture-green-20261011-130522` passed all five selected cases: ownership-probe
calibration, actual receipt cleanup, one-conflict retry, eight-conflict exhaustion,
and all sixteen concurrent inputs with quiescent progress. All selected cases had
stable source identities, exact inventories and settled process groups. That
concurrent run recorded 54 actual native conflicts and sixteen successful public
calls; the separate forced-exhaustion case proves rejection and fresh progress.

The 1,024-owner correction subsequently passed in
`deterministic-20261011-130641`: all actual Input owners remained active before
and after independent progress, then shutdown joined. The workload body took
94.273 seconds and the complete case 669.56 seconds. The subsequent
`deterministic-tail-20261011-131820` passed all 227 selected integration cases:
38 ordinary worker cases, 153 ordinary workflow cases, 22 workspace cases, seven
public-surface cases, and seven deterministic wrapper cases. Combined with the
separate capacity receipt, all 39 worker cases pass. Five large workflow cases
remain explicitly excluded from that tail, not credited as successful execution.
Agent/App compilation, doctests, Clippy and rustdoc passed, as did all five
selected deterministic examples, the 1,000-row directory history seed and two
fresh-process reopen checks, source-boundary checks and architecture guards.
The tail's overall result was **FAIL** solely because Agent rustfmt requested
layout changes in its test diagnostic helper; the correction and focused
formatting recheck are recorded separately. Prior Agent533/App539 receipts keep
their original source identity; continuation changes affect these two App test
targets and documentation, plus formatting in the Agent strict-live diagnostic
helper. That helper's bytes changed, but independent review confirmed unchanged
behavior. No executable production correction was added in this continuation.

The capacity continuation `deterministic-20261011-154202` retained its frozen
984-file candidate digest
`a6e73e6df6a3cde5ed48300a4f289a00e3451c0f077771883369417355773859`.
The 4,097-run-head case passed in 4,151.473 seconds, including all valid
4,095/4,096/4,097-page boundaries, history, deletion, subsequent execution and
shutdown. The original 1,450-action history case passed in 631.134 seconds;
its post-deletion progress check executes another complete 1,450-action workflow.
Initial generation took 3,979.587 and 222.500 seconds respectively. The earlier
logs do not attribute the remaining time among individual phases.

The 4,096-delivery case failed at target index 1,064 after 1,065 received targets
and 1,064 returned resolutions. Its public wait returned
`Timeout("app state-store operation timed out")` after 30.166 seconds; a diagnostic
snapshot returned Closed. Secondary shutdown retained
`Integration("workflow execution authority observation failed")` after joining.
The complete case took 1,008.863 seconds, below its body and outer budgets.
Source tracing narrows the outward timeout to App's `wait` read/observation path:
its recovery reads call the supplied StateStore's `load`. This is not direct
evidence of an App consuming-commit failure. A Lance load may first perform
owned maintenance, including native writes, so this distinction does not
identify or exclude any internal native I/O phase. Growing retained history,
full materialization and maintenance remain source-supported cost hypotheses,
not measured causes of this timeout. The separately retained authority-read
failure is consistent with an expired observation, but its redacted error alone
does not prove that cause. No complete 4,096-delivery result is credited.

Pinned-source review establishes a further diagnostic limit: Lance's load
admission and reply-observation deadlines, and directory shared-lease contention,
can all return
`StateStoreError::TimedOut { operation: "load", timeout_ms: 30000 }` in this
fixture. The shared lease has its own configured deadline after command admission;
the outward observer may expire before that worker result is returned. Even
recording the raw error before App's redaction would not identify which deadline
expired or establish any native maintenance phase. Later progress or reclamation
cannot assign maintenance to the original timed-out call. This additional
shared-lease source is documented in
`load-timeout-shared-lease-source-supplement.json`; none of these sources is
established as the failed call's cause. No additional raw-error capture has been
implemented or executed.

The delivery workload's directory storage is preexisting fixture behavior,
introduced with Redb file storage in ancestor commit `36765ff640bd5f952df32277f63ee08a735825d0`
and preserved by the Lance directory migration. The App contract is backend
neutral, but substituting memory would change this durable capacity scenario;
no equivalence is established. The old several-GiB comment is historical
motivation, not a current Lance size measurement. These limits and provenance
are independently reviewed in
`settlement-directory-origin-and-diagnostic-scope.json` and its accompanying
independent-review receipt. They do not excuse the unfinished 4,096-delivery
assertions, change deadlines/counts, or establish a native timeout cause.

On user-directed harness correction, the invocation-owned supervisor received
TERM, reaped its active foreign-prefix test group, and recorded **INCOMPLETE**.
That partial generation is not a capacity pass; the late-invalid case had not
started. Completed receipts and unchanged source identities were retained, with
no cleanup errors or unrelated process interruption.

These four inventory fixtures use actual App executions over real process-local
Lance memory. Their App reconstruction retains the same store connection;
it is not disk reopen or process recovery. A test-local wrapper deliberately
requests short pages and, for negative cases, alters returned pages. None of
these inventory fixtures constructs an Agent. They supply deterministic capacity,
pagination and rejection evidence only; strict-live and disk/process recovery
receipts remain separate. Artificially short-page timings are not application
performance benchmarks.

The harness defect conflated ownership completion with operation success.
App’s retained terminal error is valid behavior. A follow-up source review found
that success-only cleanup reobservation could loop forever on that error. In the specific valid
CancelAndDrain-only fixtures, this result is returned after task joining and is
replayed unchanged. The correction recognizes that narrow terminal outcome
without turning an operation error into success. Other returned outcomes and
caught panics leave joining unconfirmed and reobserve the same owner under the
unchanged bounded supervisor. Initial shutdown panics are caught in the
inventory and delivery fixtures; the worker and package fixtures retain their
existing uncaught initial-panic behavior. No general initial-panic recovery is
claimed. Original body failures and first shutdown receipts remain unchanged. Earlier extracted control-flow checks omitted sticky failures and
are historical narrow evidence only. The expanded exact-loop model recorded
15/34 red and 34/34 green; it is not actual App execution. Focused real App barrier tests now record meaningful red (one pass / one
failure from repeated joined Integration), then two passes in 0.14 seconds in
`cleanup-green-20261011-173855`. An intermediate run exposed an incorrect fixture
NotFound message; both failed receipts remain retained. The tests prove actual
observation timeout followed by successful joining or retained joined failure,
one lower shutdown, and preservation of the first timeout and original body
failure. The existing supervisor’s twelve focused Python probes also pass,
including disposable-child interruption and deadline cleanup. The completed
phase-observed capacity receipts and the still-missing evidence are separated
below.

The harness continuation adds sparse phase entry/completion/failure observations
for seeding, shutdown, enumeration, reopen, snapshot/history, deletion, subsequent
execution and final settlement. It initially retains every original count and
the second full workflow, so any later reduction requires measured cost and
specific equivalence reasoning. Storage implementation bytes are unchanged by
this diagnostic work; it does not require another Storage matrix or live run.
The completed Storage feature matrix retains its unchanged-input scope.
Refreshed default Linux Storage execution and Storage package verification now
have the source-bound passes above. Changed Agent/App clean-candidate libraries
also compile, but their native package closure failed and extracted-consumer
qualification remains outstanding as detailed above.

The first instrumented full history execution, `deterministic-20261011-174102`,
passed exactly one selected test (159 filtered, none ignored) in 647.569 seconds,
with stable candidate digest
`8e0cbc9c7acf006a05ad1e047a5b95734b404d95e7020e8b01f80fdc1c26c24d`.
All three reconstructed snapshots and 4,352-row histories, deletion, the second
full workflow, final assertions and Store-owner destruction completed. Kit
shutdown observers returned success; process-group cleanup separately reported
no errors. The earlier interrupted foreign-prefix run has only process-reaping
evidence, not graceful Kit shutdown evidence.

| Measured history phase | Seconds |
| --- | ---: |
| Original 1,450 actions | 231.431 |
| 4,095 / 4,096 / 4,097-page reconstruction | 8.186 / 7.692 / 7.872 |
| Each complete 4,352-row traversal | 0.712 / 0.711 / 0.725 |
| History deletion | 5.589 |
| Subsequent full 1,450-action workflow | 380.511 |
| Final App shutdown / final Store destruction | 2.374 / 0.049 |

Each complete inventory enumeration took 0.181–0.188 seconds. These are single
execution samples, not latency distributions or an application benchmark. The
second execution is retained deliberately: the same workflow starts from its
default zero state after deletion and must reach exactly 1,450. A partial run or
preloaded counter would change that assertion. Full traversal after each
reconstruction proves that reconstruction remains usable; its measured cost is
small. No underlying native performance cause is inferred from these phases.

The measured harness review retains these checks with distinct purposes. All
three history traversals together cost 2.148 seconds in the history case and
0.031 seconds in the head case, proving each reconstructed owner can read its
selected run through EOF. Fresh pre-reopen inventories cost 0.545 and 11.304
seconds respectively; those totals already contain their nested enumeration
intervals. They configure current history page packing and retain complete
before-images for failed-construction nonmutation assertions. Passing head
opens do not compare those before-images, but their failure branch does.
Reusing one image across opens is invalid: actual inventory sizes changed.
The second complete 1,450-action execution cost 380.511 seconds (58.76% of its
command wall time); it proves same-definition progress from zero after deletion.
A shorter workflow or admission-only check would remove that assertion. The
first history enumeration may duplicate its first baseline, but no exact
identity-preserving simplification has been demonstrated, and that baseline
call occupied only 0.182 seconds. No reduction is claimed. The source/measurement
review and independent confirmation are
`inventory-harness-cost-equivalence-review.json` and
`inventory-harness-cost-equivalence-independent-review.json`.

After this invocation settled, measurement-only changes selected
`std::time::Instant` and added explicit operation-outcome versus settlement
fields to shutdown markers. Pinned Tokio 1.51's nonpaused clock uses the same
standard monotonic clock, so the earlier timing receipt remains valid under its
original source identity. Independent review confirmed unchanged assertions,
control flow and budgets; this diagnostic refinement does not require repeating
that completed full history workload.

After all four inventory executions settled, four existing diagnostic print
expressions were refined to distinguish `ok`, `timeout`, `panic` and `error`;
settlement fields, control flow, counts, budgets and assertions stayed unchanged.
Independent review confirmed that reverting only those four expressions in the
applied file, SHA-256
`772224108f7e0dfe628e2226aeacfcc150320d93dff0dd9ae6d5eb54d20ff4b9`,
reproduces the entire preimage exactly.
At stable candidate digest
`5d8d24e1fae5e3ebb96d9d37c4a9885aec4e10c5a29db144a9327dd727287568`,
the two focused cleanup tests passed in `cleanup-green-20261011-203626`;
`capacity-harness-checks-20261011-203719` passed two existing shutdown cases,
12 documentation tests, Clippy, formatting and architecture guards. No cleanup
errors were reported. This diagnostics-only change preserves the completed
capacity assertions under their original receipt identities; it does not
relabel those runs as current-candidate executions. The actual-diff proof is
`inventory-shutdown-outcome-detail-applied-independent-review.json`.

The complete malformed-inventory execution, `deterministic-20261011-175421`,
passed one selected test (159 filtered, none ignored) in 311.403 seconds, with
stable candidate digest
`66ed6d81939f960e204903fe3e6b46b5bfd52bc79b79ccad491d6dff692c7877`.
Its real 1,450-action seed reached checkpoint 4,351 in 217.055 seconds. All ten
expected rejections passed: Epoch, Namespace, Oversized, EmptyWithCursor and
Nonprogressing page faults, each reached at read 2 and read 4,097. Public
before/after inventory equality held for every rejected construction, including
no generation claim. The seed App shutdown returned success in 0.180 seconds;
final Store-owner destruction completed in 0.042 seconds. Process-group cleanup
separately reported no errors. These ten expected error outcomes are assertions
within one passing test. No usable App was returned from those rejected opens,
so successful-reopen history/deletion/subsequent-execution branches do not apply
to this negative case. This is App page-validation evidence over real Lance
memory rows, not native database corruption, disk recovery or Agent live
evidence.

Independent reviews verified the raw phase markers, exact inventories and source
identities in `history-capacity-independent-measurement-review.json` and
`completed-inventory-capacity-report-review.json`. The contextual cleanup tests
and clock/escalation changes have separate actual-diff reviews in
`continuation-harness-final-green-supplement.json` and
`clock-escalation-applied-independent-review.json`. These receipts remain under
`target/agent-app-repair-evidence`; no review substitutes for unexecuted cases.

The existing escalation test now injects a Storage failure while lower shutdown
is gated. It observes Integration before joining, then settles the same owner
without another lower shutdown. This directly demonstrates why the terminal
classification cannot apply to arbitrary shutdown histories. That test and the
precompletion RejectActive, late Closed and dropped-observer cases pass in
`capacity-harness-supplement-20261011-175238`; affected documentation, Clippy,
formatting and architecture checks also pass.

The phase-observed 4,097-head case in `deterministic-20261011-180043` passed one
selected test (159 filtered, none ignored) in 4,184.152 seconds, with stable
candidate digest
`66ed6d81939f960e204903fe3e6b46b5bfd52bc79b79ccad491d6dff692c7877`.
All 4,097 genuine one-action workflows completed in 3,988.814 seconds, and the
public run inventory independently contained 4,097 heads. The first complete
inventory contained 106,536 rows; later pre-reopen inventories contained 106,528.
Reconstruction at configured page boundaries 4,095/4,096/4,097 took
57.112/57.859/57.613 seconds. The corresponding cumulative matching list-read
counters were 8,190/8,192/8,194, distinct from the configured boundary counts.
Each reconstruction preserved the selected run's checkpoint/state and all five
history rows. Deletion took 0.550 seconds; a newly admitted complete one-action
workflow then reached success and count 1 in 7.121 seconds after admission.
Initial and all three reconstructed App shutdowns returned success; final
Store-owner destruction completed in 0.219 seconds. Process-group cleanup
separately reported no errors. The exact raw-marker and source-bound review is
`head-capacity-independent-measurement-review.json`; these single execution
phase costs do not establish a native performance cause.

The foreign-prefix case in the same invocation also passed one selected test
(159 filtered, none ignored) in 4,114.745 seconds with the same stable source
identity. It generated all 4,097 genuine one-action heads in 4,050.400 seconds;
public enumeration independently confirmed that count. Both expected rejected
constructions reached their foreign-prefix fault at reads 2 and 4,097, returning
`Integration("storage page has invalid scope or order")` after 12.195 and 36.272
seconds. Complete before/after public inventories matched for each attempt,
with 106,536 rows, proving no record or generation mutation. These are two
expected error assertions inside one passing test. The seed App shutdown
returned success in 0.799 seconds; final Store-owner destruction completed in
0.183 seconds. No App handle was returned from the rejected constructions, so
no successful-reopen snapshot/history/deletion branch is credited to this case.
Process-group cleanup separately reported no errors. Raw hashes and the exact
receipt are retained in `foreign-capacity-phase-results.json`.

The supervisor settled with PASS for its exact two-case head/foreign selection,
no interruption and no cleanup errors. This completes the four phase-observed
inventory cases across their separate recorded source identities; it does not
qualify the entire 160-case target or the separate delivery/live requirements.

The separate 4,096-delivery timeout and failed mandatory Agent live case remain
unresolved; neither is waived by this harness correction or the fresh default
Linux Storage and Storage package passes. Changed Agent/App clean-candidate
libraries compile, but their package closure failed; exact-candidate package and
consumer checks remain outstanding as scoped above. The completed feature matrix
retains its recorded source and unchanged relevant-input comparison.
No prompt, model, output-fidelity assertion or public contract has been relaxed.
Changes remain uncommitted and unpushed.

The sections before **Authorized Agent/App repair continuation** record the
preceding feature-split candidate chronologically. Their authorization limits,
unchanged-production statements and failures apply to those exact historical
receipts, not the current repair scope. Later continuation receipts supersede
status claims only for the source and selected checks they actually execute.

## Historical tests-first receipts

| Change | Red evidence | Focused result |
| --- | --- | --- |
| Default feature policy | Initial Python run: 3 failures / 4 tests; default still selected SQLite and selector was absent | Policy and orchestration suite subsequently passed 19 / 19 |
| Semantic dependency selection | Added tests failed on manifest metadata, owner-specific requirements, target-scoped dependencies, ordered lock edges, and root patch configuration: 2 failures + 3 errors | All five corrected scenarios pass in the 19-case suite |
| Explicit empty comparison base | `empty-base-red.log`: an explicitly empty `--changed-since` passed and selected Lance alone, failing the expanded invalid-base regression | The parser now distinguishes absence from an empty argument; `empty-base-green.log`: 19 / 19 pass, including empty, whitespace and nonexistent bases. Independent review confirmed failed selection emits no backend list |
| Exact locked dependency roots | `closure-identity-red.log`: 23 tests / 6 failures, including the pinned Qdrant closure incorrectly including Lance's other reqwest version, a Provider-only duplicate-version update selecting every service, and missed direct-edge retarget/removal | Closure roots now come from Storage's exact locked direct dependency identities; base/candidate reachability detects removed edges even when another crate retains the package. `closure-identity-green.log`: 23 / 23 pass, including source ambiguity rejection. Independent actual-diff review found no actionable issue. Python orchestration evidence only |
| Lance-owned direct dependencies | `lance-dependency-owners-red.log`: 25 tests / 6 failures; direct Arrow/rustix requirement changes and isolated locked changes incorrectly selected all alternatives | `arrow-array`, `arrow-schema`, and `rustix` direct edges are classified as Lance-owned, matching their production uses. `lance-dependency-owners-green.log`: 25 / 25 pass. Shared transitive reachability remains authoritative: the actual pinned rustix also reaches shared tempfile tests, so its lockfile changes still broaden coverage. Independent actual-diff review found no actionable issue; Python orchestration evidence only |
| Verifier cancellation ownership | Actual disposable command fixture remained running beyond the 8-second witness after verifier TERM | Owned process group settles before verifier exits 143 |
| Storage public feature contract | Earlier Rust red attempt was interrupted; this is not behavioral red evidence | Root-run `feature_contract`: 4 cases passed; the later complete Storage receipt below covers this contract in all eight supported check lanes |
| Failed live command diagnostics | Root-run `strict_env_contract` reported 16 pass / 2 failures because captured native failures were replayed on stdout; a focused Python case reproduced missing primary stderr with cleanup also failing | Failed target logs now replay on stderr and preserve exit 29 and secondary cleanup context; `storage-strict-env-green.log`: 18 / 18 passed |
| Documentation inventory | Existing Lance capacity/prototype reports were missing from the active document inventory | Added ownership entries; 5 / 5 documentation tests passed |
| App deletion timeout fixture | `app-deletion-red.log`: the existing deletion test expected writer-admission timeout; concurrent Lance transaction admission instead completed, yielding 0 passed / 1 failed (exit 101) | A one-shot test-local lower-boundary timeout preserves the App fencing, timeout, retry, deletion and shutdown assertions. That case passes in `app-fixtures-green.log`; this is App error-handling evidence, not native Lance admission-timeout evidence |
| App native conflict control | New concrete control delegates staging and outcomes to independently opened real Lance stores; no synthetic conflict | `app-fixtures-green.log`: `lance_conflict_control` passed 1 / 1, proving actual conflicts, absent losing writes/receipts and later progress |
| App workspace/run fence | `app-fixtures-green.log`: existing concurrent test observed both start and replacement succeed; workspace target passed 18 / 19 (exit 101 overall) | Deterministic public-preflight regression confirms the same contract defect in `app-fence-deterministic-red-2.log`: 0 passed / 1 failed, 19 intentionally filtered, exit 101; no production correction is authorized |
| Lance Database memory namespaces | Migrated Agent public tests passed 15 / 22; seven managed-knowledge cases failed. `lance-memory-namespace-red.log` reproduces the native `memory:` path error through Storage's supplied Database | A hierarchical URI alone still failed (2 / 3 passed); that earlier filename is not a green receipt. The approved connection-owned memory provider and separate native sessions now pass all 3 `database_backend` cases, including root/namespaced scalar-index reads and independent-connection isolation. `lance-indexed-session-green.log` also passes all 4 native ownership cases; Agent public tests pass 22 / 22 in `agent-public-memory-green.log` |

Additional read-only evidence: the real candidate manifests and lockfile were
copied into a disposable Git repository. Changing only Storage package version
and a comment selected Lance alone, with exit 0 and the unchanged baseline lock
hash. No Cargo command or service was started by this dry run.

Independent App-fixture review also identified development/build dependency
feature edges, combined-owner paths, cleanup-before-PASS ordering, and owned
service process deadlines. Those corrections are included in the current
selector/verifier tests. Rustdoc-only detection is conservative around raw or
multiline strings and block comments. Empty live target execution is explicitly
rejected even when the command exits successfully.

Shell syntax checks and `git diff --check` passed at this intermediate source.
Rust orchestration files were formatted with Rust 1.94.1. Independent root review
identified the semantic dependency-selection corrections recorded above. A final
independent review is still required after all implementation edits settle.

Command recorders and loopback peers prove orchestration only. The ledger never
credits their success as native backend evidence.

## Intermediate Storage and prerequisite execution

The default verifier retry executed the following suites successfully before its
two subsequently corrected orchestration failures: feature contracts 4, public
API 8, Database backend 2, StateStore backend 3, native Lance feasibility 10,
durability feasibility 14, and Lance StateStore 86. The Database count predates
the new failing memory-namespace regression. The verifier as a whole returned
failure; these counts do not constitute a current complete default gate.

`storage-default-clippy-retry.log` records a successful default/no-default-feature
Storage all-target Clippy check with warnings denied. The first check identified
one nested test-fixture `if`; the correction changes no test behavior.
The serial 24-legal/8-illegal compilation matrix is recorded separately under
`target/storage-feature-split-evidence/feature-matrix/` and completed all 24 legal compilations and eight intended incompatibility rejections. Those receipts predate the approved memory dependency edge and do not replace the final-candidate matrix.

The downstream source boundary check compared 272 production files against the
baseline and found only 11 files with documentation changes. Actual downstream
Rust configuration scans find no alternative backend constructors. At this intermediate source,
Python policy and orchestration tests passed 25 / 25 and documentation inventory
tests passed 5 / 5. Later supervision coverage is recorded below. These are deterministic policy receipts, not live backend coverage.

The explicitly authorized isolated native authentication setup completed with
private file permissions. Agent's eight prerequisite stages passed with pinned
Codex 0.144.1 and OpenCode 1.17.18. Those prerequisite receipts are distinct from the scoped live execution recorded below. No credentials are included in this ledger.
Agent's deterministic prerequisite-script contracts also passed 12 / 12 in
`agent-precheck-contract-retry.log` (76.903 seconds), including the removed
alternative-storage prerequisites. The first invocation could not bind its
loopback fixtures inside the filesystem sandbox; the ordinary loopback-enabled
retry used no live credentials or privileged instrumentation.

The memory-namespace defect is within Storage's permitted correction scope.
Pinned native source shows that namespace creation supplies a table-specific
storage-options accessor, while metadata/reopen operations use other parameters;
the default native memory provider allocates different stores for those cache
keys. The correction retains one backing memory store per supplied connection and uses distinct namespace/table native Sessions. It preserves independent-connection isolation without a native Session ownership cycle. No upstream or public API change was made. The user explicitly approved the already-locked `lance-io =7.0.0` direct edge with default features disabled; all 1,022 package identities, sources and checksums remain unchanged. The new lock hash is `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.

Independent final source review found no additional feature/public-surface issue:
Redb/PostgreSQL/SurrealDB configuration visibility and serialization remain
unconditional, SQLite/Qdrant retain their existing visibility gates, validation
precedes disabled-backend rejection, and the Lance put future adopts JSON before
its first poll. The selector/verifier review also corrected the deterministic
Provider wrapper expectation so that only Storage omits a per-crate service
environment; all other environment, order and full-coverage assertions remain.
Its seven selected Rust process contracts subsequently passed in the deterministic
receipt below. Source review does not replace behavioral, packaging or platform
execution.

## Historical scoped live execution and subsequent corrections

The authorized scoped runner completed on the local Apple Silicon macOS host
with Rust 1.94.1. Its platform log records Darwin 25.6.0 / arm64; it does not
independently identify the dataset filesystem. Its source digest was
`f50f3fde13bd0ca5ddfac5b91753f174a94dec520d063e0d1374295437a9a1ad`, with the original
lock hash above. Every command retained that identity and settled its owned
process group. Its overall result was **FAIL**, with complete inventories:

| Target | Passed | Failed | Scope |
| --- | ---: | ---: | --- |
| Agent `strict_live` | 16 | 7 | All 23 entries, no ignored/filtered cases |
| App `workflow_live` | 15 | 3 | All 18 entries, no ignored/filtered cases |
| App canonical crash | 1 | 0 | Exact case exercises all four crash boundaries; three other tests intentionally filtered |

Receipts are under
`target/storage-feature-split-evidence/scoped-live-20261009-191856/`.
Both approved OpenCode Question Kit receipts passed. Their raw exact-copy
quality grades both failed (2 / 2); these remain the two explicitly nonblocking
quality results and do not establish model-quality acceptance.

Six Agent failures returned `NewConversation` / `StateCommit` /
`TransactionConflict`. The unchanged production code uses the same message in
multiple creation phases, so those live logs alone do not identify each phase.
A new deterministic public regression coordinates one native `thread/start`
response and its matching notification through two real Lance snapshots. It
observes one genuine foreground commit conflict and one native creation, then
fails the required successful reconciliation in 0.21 seconds:
`codex-creation-conflict-red.log` (0 passed, 1 failed, 46 intentionally filtered).
`observe_thread_creation` reconciles a conflict returned by load, but forwards
its commit conflict without the same reconciliation. This is a demonstrated
Agent production gap; no downstream production correction is authorized.

The seventh Agent failure is the Lance overlap test's exact-output assertion.
The native model returned an explanatory refusal instead of the literal marker.
The real event commit and foreground conflict were both observed once, and the
stream fused. This case is outside the two quality exceptions and was blocking
for that invocation; the later rerun below passes. Its prompt and assertions
have not been relaxed.

App's three live failures were an absent second expected native question,
abort settlement exceeding its existing bound, and a transaction conflict while
Agent recovered cancellation during reopen. The first two causes remain
unresolved. Source and error context identify the recovered-abort conflict path
in Agent; this is not evidence that Lance violated its transaction contract.
The crash/escalation, held-authority, compensation and unknown-workspace cases
remain represented by their actual passing live receipts.

Failure cleanup exposed test ownership problems. Seven App guarded paths now
retain directories unless App shutdown positively succeeds, using the existing
native-service retention method. Agent's existing directory owner also retains
paths while unwinding before confirmed cleanup. Original primary failures and
assertions remain intact. These subsequent fixture edits were independently
reviewed. Their later deterministic results are recorded below; the earlier live
receipt does not qualify changed live cleanup paths.

The native ownership probe first recorded 3 passing controls and one failure:
shared-session namespaced operations retained three references after every
explicit owner/runtime dropped. The separate-session control already released
both sessions. The shipped correction uses that supported native construction.
The retained native shared-session test is now an explicit counterexample,
measured before diagnostic cleanup; it does not claim that the upstream cycle
was fixed. The stronger separate-session case executes a populated scalar-index
query and requires both weak owners to have zero references before cleanup.
All four native cases and all three public Database cases pass.

The new target executes in every Storage check lane. Its gate selection and the
`lance-io` dependency owner first failed three Python assertions; after the
minimal selector/target-list correction, all 25 policy tests passed
(`memory-edge-policy-red.log`, `memory-edge-policy-green.log`). Independent source
review found no remaining production issue in the memory/session correction.

## Historical App contract failure and authorization blocker

The Lance fixture migration exposed a downstream production race. The existing
`workspace_mutations_and_run_admission_share_one_durable_fence` test reports
`started=true, replaced=true`. Its unscheduled race does not establish publication
order by itself. A new narrowly coordinated regression,
`workspace_replacement_rejects_run_admitted_during_advisory_preflight`, establishes
the unsafe order through ordinary public interfaces:

1. The existing caller-defined Agent pauses replacement in
   `check_workspace_update`, before any lower update.
2. The captured Workflow starts through real Lance storage and `run.wait()`
   returns `Running` with exactly one input interaction. The test verifies that
   no lower workspace update has occurred.
3. The test releases advisory preflight. Replacement succeeds when the contract
   requires rejection. The test attempts bounded `CancelAndDrain` and drops App
   before asserting the failing result.

The corrected exact run failed in 0.43 seconds with
`replacement_succeeded=true`. Its receipt is
[app-fence-deterministic-red-2.log](../../target/storage-feature-split-evidence/app-fence-deterministic-red-2.log).
The earlier `app-fence-deterministic-red.log` stopped at an incorrect fixture
assertion expecting `Paused`; active input workflows project `Running`. That
earlier run is **not** product-defect evidence.

The owning [Workspace contract](../../app-kit/specs/WORKSPACE.md#full-replacement)
requires replacement to reject a nonterminal run pinned to the current revision;
[public replacement documentation](../../app-kit/src/lib.rs) retains the same
obligation. Independent source review identifies this sequence:

- [`WorkspaceRuntime::replace`](../../app-kit/src/workspace/mod.rs) reads the
  workspace and scans for active runs before lower advisory preflight.
- Its later `persist_admission` samples a fresh App revision after preflight.
  That revision can include the intervening run admission.
- [`AppMutationKind::AdmitRun`](../../app-kit/src/storage/mod.rs) advances App
  capacity and inserts the run, leaving the unchanged workspace row untouched.
  `AdvanceWorkspace` then checks the newly sampled App revision and the original,
  still-current workspace revision. Both checks pass despite the stale
  no-active-run observation.
- Reversing the serial order cannot justify both successes: the Workflow handle
  captured its logical workspace revision before the race, and
  [`WorkflowRuntime::admit_owned`](../../app-kit/src/workflow/run/mod.rs) rejects
  a changed logical revision. Starting first leaves the demonstrated
  nonterminal input, which forbids replacement.

This is not evidence that Lance violated a native transaction guarantee: the
invalidated App observation occurs before its eventual storage transaction.
Coordinating both transactions onto one snapshot or accepting both outcomes
would hide the demonstrated App defect. The original assertion and new failing
regression remain intact; downstream executable production code is unchanged.
The approved task forbids the necessary downstream production correction, so
full migration qualification remains blocked even if Storage's own gates pass.

Reproduce using Rust 1.94.1 and the task's existing low-memory build settings:

```bash
cargo test --locked -p app-kit --test workspace_contract workspace_replacement_rejects_run_admitted_during_advisory_preflight -- --exact --nocapture --test-threads=1
```

The [combined App fixture receipt](../../target/storage-feature-split-evidence/app-fixtures-green.log)
predates the added deterministic case: `lance_conflict_control` passed 1 / 1;
`workspace_contract` passed 18 / 19 in 18.02 seconds, including the corrected
deletion-timeout fixture, with the original race as its sole failure. This is
not a current 20-case workspace-suite pass. The scoped live run above separately
records all 18 `workflow_live` cases and the canonical four-boundary crash test.
The later deterministic receipt records affected targets and identifies its
remaining failures; changes after each run require their own receipts. Independent reviews found the native
conflict and deterministic preflight controls preserve the intended assertions;
source review does not substitute for the pending execution.

The subsequent App fixture review found retained handles at four live cleanup
boundaries. The fixtures now explicitly drop all App/Workflow/Run or raw
StateStore owners before removing their durable directory. The canonical crash
fixture still reaps each child before reopening; its fallback destructor now
retains the directory if reaping fails. Scoped live and crash outcomes are
recorded above; later guarded-cleanup changes still require revalidation.
The bounded-deletion workload retains its pre-existing
120-second Lance supervision budget; no App operation deadline or behavioral
assertion was relaxed. Compensation still requires revisions 3 and 1, and
unknown-workspace creation retains its original 120-second Agent lifecycle
deadline with repeated actual native conflicts.

## Qualification commands

Run Cargo serially with Rust 1.94.1, existing low-memory settings, and `--locked`.
No Storage command uses `--all-features`.

```bash
python3 -m unittest discover -s storage-kit/scripts/tests -v
python3 -m unittest scripts.tests.test_documentation_contract
cargo test --locked -p storage-kit --test feature_contract --test strict_env_contract --no-default-features -- --nocapture --test-threads=1
bash storage-kit/scripts/verify_strict_live.sh
# With selected alternative test credentials/paths/unused ports configured:
bash storage-kit/scripts/verify_strict_live.sh --all-backends
```

The full verifier runs eight behavioral/check lanes: Lance-only; each of Redb,
PostgreSQL, SurrealDB, SQLite and Qdrant individually; then
`redb,postgres,surrealdb,sqlite` and `redb,postgres,surrealdb,qdrant`. It also compiles
all 24 legal subsets and verifies that eight incompatible unions fail for the
intended dependency conflict. Selection tests do not substitute for these builds.

Changed downstream live targets, after their native/provider prerequisite checks:

```bash
cargo test --locked -p agents-kit --test strict_live -- --nocapture --test-threads=1
cargo test --locked -p app-kit --test workflow_live -- --nocapture --test-threads=1
cargo test --locked -p app-kit --test prepare_contract staged_action_and_atomic_barrier_survive_forced_process_loss -- --exact --nocapture --test-threads=1
```

Capture `-- --list` inventories for each selected target and compare terminal
counts against those inventories. The current migration inventory expects Agent's
21 behavioral cases plus its environment contract (22 total), App's 18 workflow cases, and the one
canonical App test exercising four crash boundaries. Unexpected missing,
ignored, filtered or failed cases are incomplete/failed evidence, not success.
Preserve exact native/provider output and raw quality grade reporting.

Also run affected deterministic targets/examples, docs/doctests, Clippy,
architecture guards, ordinary macOS/Linux coverage, default/no-default graph
comparison, package verification, a disposable downstream consumer, and a locked
clean-candidate build. The receipts below distinguish executed gates from outstanding work. Do not infer completion from dependency compilation or historical
prototype/platform receipts.

## Recorded execution chronology

### Complete macOS Storage execution

`storage-all-20261009-204739/receipt.json` records exit 0 / **PASS** for
`verify_strict_live.sh --all-backends`, in 3,970.83 seconds on the local
Apple Silicon macOS host with Rust 1.94.1. This receipt does not independently
record the dataset filesystem. Source identity remained
`b98b370c5260fe3e16d375028ff0c672e21f3179ca0dfde5925928f9330209cb`; the lock identity
remained `bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.

All eight check lanes passed compilation, applicable integration tests, Clippy,
rustdoc and doctests. All 24 legal subsets compiled; all eight illegal unions
were rejected for the expected incompatibility. The subsequent strict-live
inventories executed completely, with no ignored or filtered tests:

| Feature lane | Live cases passed |
| --- | ---: |
| Lance only | 24 |
| Redb | 24 |
| PostgreSQL | 32 |
| SurrealDB | 47 |
| SQLite | 24 |
| Qdrant | 37 |
| Redb/PostgreSQL/SurrealDB/SQLite | 55 |
| Redb/PostgreSQL/SurrealDB/Qdrant | 68 |

These are 311 executions across overlapping lane inventories, not 311 unique
cases. The invocation's three containers, network and volumes settled and were
removed before PASS. Per-target inventories and logs are preserved beside the
receipt in `target-logs/`, with `live-inventory-summary.json`.

### Subsequent verifier correction

Independent review identified interruption/cleanup gaps in the verifier and a
missing script-contract step in `check.sh`. The actual candidate regression run
`supervision-current-red.log` executed 34 tests with seven failures and one error.
After the reviewed corrections, `supervision-current-green.log` passes 34 / 34
in 36.19 seconds. `storage-supervision-rust-green.log` passes all 19 Storage
orchestration cases in 38.56 seconds. Formatting, shell syntax and whitespace
checks also pass. These ordinary process/model tests prove script ownership and
failure propagation; they are not privileged instrumentation or native-storage
fault evidence.

The verifier now retains resources if command settlement cannot be established,
preserves primary failures, and waits for process-group disappearance after
termination. The workspace wrapper uses all eight explicit Storage Clippy lanes
while its default Storage live selection remains Lance-only. The seven selected downstream wrapper process contracts subsequently passed, as
recorded below. The subsequent current default verifier receipt is also recorded
below.
The full macOS receipt above predates these script/test-only corrections; it
remains evidence for the unchanged Storage implementation and feature matrix.

### Downstream deterministic baseline

`deterministic-20261009-215640/receipts.json` reports **INCOMPLETE** at candidate
`bacab5e279e37d922df56f93fd06b0ec26b9dde84de17317a1871fb7d3e3ac6d`, with lock
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a` unchanged before
and after every command. It used Rust 1.94.1 (`e408947bf`),
`aarch64-apple-darwin`. No command-group cleanup error was reported.

| Executed scope | Result | Limits |
| --- | --- | --- |
| Ten Agent deterministic targets | 453 passed, 13 failed | All inventories completed; `public_api` was separately 22 / 22 and was not repeated here |
| Twenty-nine App targets with terminal results | 599 passed, 6 failed | The canonical crash case was deliberately filtered from `prepare_contract` because it has its own scoped execution |
| App `workflow_execution` | Incomplete | Inventory contained 153 tests; supervision terminated and reaped the target after 1,800 seconds, without terminal aggregate counts |
| Provider workspace-wrapper contracts | 7 / 7 passed | Only `workspace_completion_` cases executed; nine unrelated cases were intentionally filtered; no Provider live execution |
| Agent / App doctests | 266 / 266 and 127 / 127 passed | Candidate-specific documentation execution |
| Agent / App checks | Passed | All-target compilation, Clippy, rustdoc and crate formatting |
| App examples and compile contracts | Passed at the recorded scope | All examples checked, `compile_contract` built without execution, five workflow examples executed, and the directory-backed performance example seeded 1,000 rows then reopened both without and with history paging |
| Repository boundaries | Passed | Downstream production-source boundary, no-path and no-alias guards |

The thirteen Agent failures were the existing Codex creation-conflict regression
and twelve OpenCode fixture cases. App's six terminal failures were checkpoint
history (one), deletion (two), lazy-worker capacity (one), and workspace fencing
(two). The separately interrupted workflow target also emitted failing cases,
but its missing summary must not be replaced by inferred aggregate counts.

The known Codex creation and App workspace-fence production failures remain
unchanged and blocking. A fixture failure alone does not establish a production
defect. Later focused runs distinguish coordination or workload issues from
those demonstrated contract gaps.

### Focused diagnostics and fixture corrections

The following frozen-source runs all retained the current lock hash above.
Each exact invocation records its complete inventory and intentional filter
count; a passing exact case does not qualify an entire changed target.

| Receipt directory | Source SHA-256 | Execution result |
| --- | --- | --- |
| `fixture-diagnostics-20261009-235040` | `6b128b35e88d49853986cb3d9663188c951e578fd11480f6851d58f00d5bb133` | FAIL: corrected checkpoint case passed; targeted failures remained; 23 previously uncompleted workflow cases executed as exact selections, with 20 passed and 3 failed |
| `fixture-corrections-20261010-002413` | `3d66ce97d6d34b7210be7503599c94981b28e9efb34c87b3ef78ec0ab592c902` | FAIL: two deletion cases and the catalog case passed; worker, idle receipt and three App workflow fixtures failed |
| `app-fixture-final-20261010-003334` | `3b21127b95f08b1d5b2cae03ae3740560c6b628899c43c0cf732ecdb2c0aa9a7` | PASS for its three exact App cases only |
| `idle-ordering-20261010-003607` | `4c27d04aafc6d7219c6ac3b22e1bfaed8fa90470da4bc98d65e8cce2e61e1473` | PASS for the exact Agent idle-receipt case only |
| `agent-fixture-final-20261010-003701` | `074546e5f2dd8bac6ce584cb70abfc733dff323736e2c44cc6a3eb13df2bf812` | PASS: full 282-case OpenCode target and Agent/App Clippy |

The checkpoint case originally exhausted a 20-second aggregate observer after
120 real actions. Its 180-second observer correction retained all 270 actions,
page-boundary, expiry, reopen and no-replay assertions; the exact case passed in
67.36 seconds, with 42 unrelated cases filtered.

Both copied-child deletion scenarios preserve two 130-action children,
parallelism, values, revisions, page/receipt assertions and all recovery/cleanup
bounds. A single 130-action Lance memory control already took 22.586–23.134
seconds, exceeding the original 20-second compose deadline. The fixture now
authors a 180-second successful-copy budget and matching observer. The exact
lost-receipt case passed in 69.95 seconds and held-original-seal/reopen case in
70.79 seconds, each filtering 23 unrelated cases. This is an actual authored
workload adjustment, not solely a test-harness timer. The original 20-second
`Closed` observations remain historical failing receipts; these greens do not
identify their precise initiating branch.

The final three App cases passed individually:

| Exact case | Test duration | Preserved behavior |
| --- | ---: | --- |
| `burr_characterization_first_action_input_ordered_transition_and_checkpoint_snapshot` | 4.94 s | Native state/ordering, admitted Sleep/Input timeout outcomes, checkpoint snapshots, cancellation and cleanup |
| `sleep_truth_load_retries_recoverable_errors_and_closes_on_permanent_failure` | 6.31 s | Selected recoverable reads, recovery after twelve recoverable read failures, permanent-failure disposition and owned cleanup |
| `unknown_notification_accept_preserves_the_other_agent_and_input` | 1.19 s | Actual accepted decision/lost receipt, peer and Input independence, exact identity, no replay and cleanup |

The first two earlier failures retained
`Integration("workflow execution authority observation failed")` during shutdown.
The [App execution-authority contract](../../app-kit/specs/APP.md) explicitly
requires fatal cleanup on permanent authority unreadability or read expiry.
Their case-local one-second Input/Sleep budgets allow the intended admitted
control-expiry scenario; shared `LINEAR`, every required Timeout outcome and
permanent-failure assertion remain unchanged. This evidence does not establish
a production defect in the earlier ten-millisecond workload. The exact initiating
subread remains source inference.

The third case exposed a fixture ordering race. Its diagnostic recorded one
accepted ID, one native resolution call, no return, one abort, cancellation,
one emitted/received notification and no terminal prompt return. The fixture
had returned its lost-receipt error before proving that the original prompt was
held before the next target. `GateLostReceipt` now uses existing receipt and
next-target controls to establish that order, while dropping the original
observer and returning the same error. No production behavior or assertion was
relaxed.

OpenCode's abort-intent selection passed 11 / 11 and reserved cancellation passed
1 / 1 in the diagnostic run. The actual transaction-owned catalog receipt and
precise peer schedule then passed its exact case in 3.32 seconds. The idle-receipt
case initially failed before its intended ambiguous-outcome injection. A separate
`idle-ordering-20261010-003156` diagnostic established a real foreground commit
conflict with the event writer, rather than a lost applied receipt. Its reviewed
correction publishes the same real `session.idle` event durably before the
selected abort HTTP reply. The exact case then passed in 0.65 seconds, observing
an actual successful commit, a lost receipt, zero repeated attempts and four
readbacks before dispatch. The earlier fixture conflict is not recorded as a
newly demonstrated Agent production defect.

The subsequent complete OpenCode target passed **282 / 282**, with no ignored,
measured or filtered cases, in 793.91 seconds. Its parent-owned
`agent-fixture-final-20261010-003701/receipts.json` also records successful Agent
and App all-target Clippy checks. Source
`074546e5f2dd8bac6ce584cb70abfc733dff323736e2c44cc6a3eb13df2bf812` and the current
lock hash were unchanged throughout; no timeout, interruption or command-group
cleanup error occurred. Independent actual-diff review is retained beside the
receipt. These reviewed test-only ownership/ordering corrections close all
twelve original OpenCode fixture failures. They do not close the separate Codex
creation conflict or replace the scoped Agent/App live receipts.

### Interrupted scoped live revalidation

`scoped-live-20261010-005044` reached **INCOMPLETE** after interruption. Its
completed Agent commands retained candidate
`074546e5f2dd8bac6ce584cb70abfc733dff323736e2c44cc6a3eb13df2bf812` and lock
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`. The last
completed command was App's 18-case inventory. The interrupted App execution has
no terminal source-after snapshot, so unchanged source for that unfinished
command is not independently established by its receipts.

| Target | Recorded result | Limit |
| --- | --- | --- |
| Agent `strict_live` | **16 passed, 7 failed**; all 23 entries, no ignored/measured/filtered cases | Terminal libtest summary in 798.60 seconds; supervised command exited 101 in 799.19 seconds without timeout or process-group cleanup error |
| App `workflow_live` | **8 passed, 3 failed** individually before interruption | The twelfth case started without a terminal result; six did not start. No 18-case aggregate, exit status or command cleanup receipt exists |
| App canonical crash | Not invoked | Its earlier passing receipt remains historical and is not current-run coverage |

Six Agent failures reproduce the known Codex `NewConversation` / `StateCommit` /
`TransactionConflict`: delivery, hierarchy, incremental output, caller-loss
cancellation, prompt/model/files and tool-resolution attempts. The real Lance
foreground/event-conflict case now passes. Both approved OpenCode Question Kit
scenarios and their cleanup pass; raw exact-copy grades fail **2 / 2** with valid
grade reporting. These two quality results remain nonblocking only under the
existing named exception; no model-quality acceptance is claimed.

The seventh Agent failure is
`opencode_repaired_abort_releases_generation_and_later_abort_is_idle`. It passes
the original repaired cancellation, reuse admission, idle-event and native-count
assertions, then a later generation's abort exceeds the existing 60-second
operation deadline. Terminal-notification drain, conversation/workspace closure
and Agent shutdown also report cancellation/quiescence failures; the initial
abort timeout remains primary. Independent source review found no established
fixture sequencing error and no evidence identifying whether native abort,
event settlement or persistence consumed the bound. This is an unresolved
blocking live failure, not a demonstrated new Storage or Agent production
defect. Successful process-supervisor cleanup does not prove scenario cleanup.

Three App cases failed before interruption:

- `caller_input_and_exact_real_agent_notification_resolve_publicly`: the
  cancellation race did not expose the expected real notification.
- `cancellation_during_durable_failure_preserves_original_timeout`: the gated
  abort did not settle after release; App shutdown observation timed out, then
  the execution thread panicked while its Tokio runtime shut down. Body timeout,
  cleanup observation loss and subsequent panic are separate evidence.
- `durable_reopen_cancellation_child_map_and_shutdown_converge`: lower Agent
  reopen surfaced `StateCommit` / `TransactionConflict`, with message
  `prompt cancellation could not commit`.

The partial log establishes these failed assertions and typed outcomes without
identifying every initiating phase. No fixture correction, new Storage defect
or completed App gate is inferred. Eight individual passes include exact-shared-
directory knowledge isolation and the separately named shutdown-escalation
reopen scenario; that scenario is not the unexecuted canonical four-boundary
crash case. `recoverable_read_keeps_cancelled_lower_notification_projectable_until_app_cancellation`
started but has no terminal result.

`interruption-reconciliation.json` preserves exact cases and artifact hashes.
The original incremental `receipts.json` remains unchanged; its final
`interrupted: null` field is not a terminal no-interruption or success receipt.
The subsequent ordinary process snapshot found no former test/native-service
processes. That absence does not establish graceful cleanup, terminal joining,
or prior directory-owner settlement. Apparent host restart is continuation
context, not a demonstrated cause from the persisted logs.

### Current macOS default gate

`storage-default-final-20261010-201459/receipt.json` records **PASS** for the
updated default `bash storage-kit/scripts/verify_strict_live.sh`, in 186.72
seconds. It used Rust 1.94.1 on `aarch64-apple-darwin` and retained candidate
`074546e5f2dd8bac6ce584cb70abfc733dff323736e2c44cc6a3eb13df2bf812` and lock
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a` throughout.
Ordinary `df` and the matching mounted-device record identify the actual owned
dataset directory's filesystem as **APFS**. No alternative service
configuration was sourced.

The exact default strict-live inventory passed **24 / 24**, with no ignored,
measured or filtered cases. Required portable, native and adapter targets also
passed, including the 86-case Lance StateStore suite, 19 Rust script contracts,
34 Python selection/supervision contracts, compilation, Clippy, rustdoc and
doctests. Its deliberately supervised single deep-JSON child selection retains
its own intentional filter count; it is not an additional complete 86-case run.
The final verifier emitted its selected-backend PASS marker only after cleanup.
The parent supervisor records no timeout or process-group cleanup errors.

This current receipt closes the default-verifier evidence gap after supervision
changes. It qualifies **Lance-only default execution**, not a new all-backend
matrix run; the complete alternative lanes and 24-legal/8-illegal matrix retain
their separately identified earlier source receipt.

### Packaged downstream construction and feature isolation

`packaging-20261010-201816-c34f3824/receipt.json` records **PASS** at the same
candidate and workspace lock identity. Rust 1.94.1 ran all commands serially;
`dataset-filesystem.json` identifies the consumer's real directory as **APFS**.
No command timed out or reported process-group cleanup errors.

The locked Storage normal/build graphs contain Lance and exclude alternative
native backend packages with both default and no-default features. Their only
raw difference is the inert root `default` marker for the declared empty default
feature; normalizing that marker makes the full graph output identical. Agent
and App normal/build dependency graphs also contain Lance without alternative
Storage implementations. The raw difference and normalized graph hashes are
retained; no dependency or other feature differences were normalized away.

`cargo package --locked -p storage-kit --allow-dirty` completed native package
verification. The actual archive contains **176 files**, excludes `vendor/**`,
and preserves every packaged registry identity against the workspace lock.
Its archive SHA-256 is
`afdc5fd87ddb7071f737f8ec64fc9a762c07a8043405d3b9fd5a4e3eb1871299`.
An extracted-package consumer then passed actual memory/directory construction,
clone and independent-connection histories, conflict, tombstones, reopen,
knowledge coexistence, reserved-state-table protection and direct caller trait
implementation. Five separate locked compilation invocations passed for
`redb`, `postgres`, `surrealdb`, `sqlite` and `qdrant`; they do not claim remote
service behavior or combine SQLite with Qdrant.

The disposable consumer's initial locked seed probe exited 101 because its
workspace-derived seed needed reconciliation for the standalone graph. That
probe is explicitly preparation, not a passing release gate. An offline
preparation step pruned the seed from 1,017 to 516 registry packages, followed
by mandatory successful `--locked` metadata and exact package-version/source/
checksum auditing before compilation. Every remaining identity matched the
workspace; the resulting consumer lock SHA-256
`6107d71161e63914807861040a22699e2fcb44c22d7fec9a96a0570f4cf201f0`
remained unchanged through default execution and all five opt-in checks. The
workspace lockfile was never rewritten. These package checks do not replace a
fresh Linux build of the complete candidate.

### Completed App-only live revalidation

`scoped-live-20261010-203618/receipts.json` completes the previously interrupted
App execution scope with **FAIL** overall. Its prerequisite check passed, and
all command source-before/source-after identities remain candidate
`074546e5f2dd8bac6ce584cb70abfc733dff323736e2c44cc6a3eb13df2bf812` and lock
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.

| Target | Result | Executed scope |
| --- | --- | --- |
| App `workflow_live` | **15 passed, 3 failed**, exit 101 | All 18 cases; zero ignored/measured/filtered; 559.45-second libtest duration, 560.05-second supervised command |
| App canonical crash | **1 passed**, exit 0 | Exact selected case, three other target cases intentionally filtered; 5.91-second libtest duration, 6.37-second supervised command |

The three workflow failures are the same cases observed before interruption:
missing real notification in the cancellation race, the gated durable-failure
abort failing to settle after release, and lower Agent reopen surfacing
`StateCommit` / `TransactionConflict` for prompt cancellation. The durable-failure
case also retains its App shutdown timeout and subsequent execution-thread
Tokio-runtime-shutdown panic. Complete target execution does not make these
outcomes nonblocking or establish their initiating phase.

The previously unfinished recoverable-read case and all six previously unstarted
cases now pass. This includes replacement compensation, unknown-workspace claim
recovery, two-agent process handoff and both separate/exact-shared-directory
knowledge workflows. These real results support the reviewed Lance fixture
adaptations without weakening their assertions.

The exact `staged_action_and_atomic_barrier_survive_forced_process_loss` case
executes all four existing boundaries (`Staged`, `InputStaged`, `BeforeBarrier`,
`AfterBarrier`) using the same directory through supervised child loss, reap,
reopen and shutdown. Its passing current receipt replaces the missing current
canonical-crash execution; it does not remove the separate workflow failures.

The runner records no timeout, interruption or process-group cleanup error.
Afterward, `app-live-resume-readiness/settlement.json` records ordinary SIGTERM
and reap of the one invocation-owned App OpenCode service (exit 143), followed
by process/listener absence. Unrelated user Ollama was untouched. These process
settlement receipts do not imply successful internal cleanup for scenarios that
explicitly returned shutdown errors or retained their directories.

### Linux default gate and locked clean candidate

`linux-final-20261010-204741-2edeb6b0/receipt.json` records **PASS** for the ordinary
Linux default Storage verifier. The container used Rust **1.94.1**,
`aarch64-unknown-linux-gnu`; actual dataset filesystem inspection records
**ext4** on `/dev/vda1` mounted at `/workspace`. The default verifier finished in
702.44 seconds; the outer attached container execution finished in 705.07
seconds. No command timed out or reported cleanup errors.

The clean candidate snapshot includes **984 verified files**, including
untracked candidate additions and excluding deleted tracked files, ignored
credentials/build artifacts and immutable `tmp/**`. Its manifest/archive are
retained with the receipt. Extracted files matched their recorded hashes before
execution; source identity remained
`074546e5f2dd8bac6ce584cb70abfc733dff323736e2c44cc6a3eb13df2bf812`, with lock
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
A fresh build directory and per-run Cargo home used no inherited Cargo
configuration. The locked verifier therefore supplies a fresh-candidate build
and execution receipt for **Storage**, not a workspace-wide build claim.

The collected target inventory selected Lance alone and executed **24 / 24**
strict-live cases, with zero ignored/measured/filtered cases. Required public,
feature, native/durability, ownership and adapter targets passed, including the
86-case Lance StateStore target, 19 Rust script contracts and 34 Python
selection/supervision tests, together with compilation, Clippy, rustdoc and
doctests. The separate intentionally selected one-case deep-JSON child retains
its explicit 85-case filter count; no skipped inventory is credited as a pass.
This is default embedded Linux coverage, not Linux alternative-backend or
Agent/App live qualification.

Docker used default security settings with its built-in `--init` process
reaper, no extra capabilities, tracing, privileged mode, Docker socket or host
credentials. Before removal the runner verified its container label and stopped
state, collected logs, then removed that owned container successfully. The owned
validation volume remains available for evidence/build reuse; no removal of that
volume is claimed. All command exits and final source checks are recorded.

### Historical remaining capacity and execution evidence

This subsection records the outstanding work at this earlier continuation point.
The current status above credits later receipts only for their recorded source
and selected checks; it does not erase or reclassify the failures below. These
workloads retain their original required work and assertions. Missing results at
this point were qualification gaps, not passes inferred from source review or
smaller workloads.

- **Lazy execution capacity:** the first observer expired during preparation
  (767 / 1,024 prepared; zero parked). Separate bounded preparation subsequently
  completed all 1,024 runs in approximately 96.38 seconds. The active 60-second
  observer then expired with only 164 Inputs parked. The caller-authored
  30-second shutdown observation returned Timeout, followed by an App execution
  thread's Tokio-runtime-shutdown panic while the test unwound. The body timeout,
  cleanup observation loss and later panic are separate evidence. Earlier
  30-second Input timers may already have expired, so no simultaneous 1,024-Input
  or final-ownership claim is established. No further timing guesses or reduced
  workload qualify this case.
- **Settlement count:** the real 4,096-delivery workload reached 438 accepted and
  returned receipts in its 1,200-second observer; all received/call/return counts
  were 438. Cleanup reported no error; the exact case failed after 1,213.32
  seconds. The first 256 receipts took 332.44 seconds and the next 182 took about
  867.67 seconds, so constant-throughput extrapolation is unjustified. The
  required capacity boundary remains unexecuted; no semantic defect is inferred.
- **Settlement bytes:** all actual byte-boundary probes passed in 290.95 seconds.
  Twenty expected injected abort-panic markers were observed and handled. This
  does not qualify the separate count workload.
- **Large inventory:** the 4,097 genuinely executed run-head and 1,450-action
  history fixtures remain unqualified. Original 300-second setup attempts either
  timed out or had their primary outcome masked by a cleanup fence conflict.
  The revised setup guards (7,200 / 1,800 seconds), terminal progress counters
  and primary-error preservation retain every genuine action/head and all later
  page-validation/cleanup assertions. They have not yet been executed. The
  cleanup conflict must not be reclassified as a proven timeout effect.

Full affected-target reruns after fixture corrections and the high-count
workloads above still require their applicable passing receipts. Current
macOS/Linux default Storage execution, the locked Storage clean-candidate build,
package/downstream construction, production dependency graphs and canonical
crash case now have passing receipts above. Agent's and App's latest complete
live targets are failing receipts, not execution gaps. Earlier platform/prototype
results do not qualify these outstanding downstream obligations.

The demonstrated Codex creation-conflict and App workspace-fence failures remain
blocking because their downstream production corrections are outside this task's
authorization. The unresolved later-generation OpenCode abort failure and the
three App failures reproduced by the complete rerun also remain blocking until diagnosed
and, where permitted, corrected with actual execution evidence. Historical live
receipts do not qualify changed or interrupted scopes. No passing Storage or
focused fixture result qualifies those failing contracts.

## Authorized Agent/App repair continuation — 2026-10-10

The user subsequently authorized the Agent/App production corrections. The
earlier authorization blocker above describes the preceding scope; it no longer
prevents these fixes. Execution remains **INCOMPLETE** until this continuation's
own scoped receipts pass. The existing feature split, accepted Storage
feasibility, and all historical outcomes remain intact.

The untouched continuation baseline contains 984 candidate files, source digest
`67c36f50b9e25e63af11f938e35ad9cef7ac98babeb27834ea481c99134e85af`, and lock digest
`bf799d0ebf7455456eb4ea84a17bb43fe360fa44e389b0cc8693ef7832ef7d8a`.
Its archive and per-file manifest are retained in
`target/agent-app-repair-evidence/baseline.tar` and
`baseline-identity.json`. Ignored credentials/build outputs and `tmp/**` are
excluded. Tests first establish consuming-commit creation reconciliation,
generation-checked abort outcome persistence, and replacement/run admission
fencing before production changes. Separate fixture corrections respect the
existing one-second abort grace, explicit caller reconstruction, and the finite
default Input deadline without reducing required capacity counts.

The private Lance image advancement is conditional on two fresh baseline and
two candidate measurements. The ordinary public-API harness records commit,
first load or transactional observation, warm load, combined cost, growing
history, and witnessed reclamation boundaries. Performance evidence does not
replace semantic tests. Final qualification still requires the scoped Agent/App
live inventories, complete capacity workloads, all Storage feature lanes, both
platforms, package checks, and independent actual-diff review. No public API,
persisted format, dependency, privileged instrumentation, commit, or push is
part of this continuation.

### Repair implementation and measured image advancement

The continuation extends the existing owners, without new public surface:

- Codex reconciles both definite creation-observation load and commit conflicts
  against the exact immutable obligation and native thread identity. It retains
  the original binding failure beneath any required hierarchy invalidation and
  cleanup context, without issuing another native creation.
- OpenCode cancellation persistence reuses generation mutation with the original
  deadline and native outcome. Proven nonpublication may authorize a fresh
  mutation; an uncertain consuming commit permits only exact receipt observation.
  Failed rollback vetoes retry. Reopen retains its single-attempt conflict rule.
- App replacement captures its revision before repeating the active-run scan,
  then uses that same revision for admission. Fixture corrections distinguish
  the existing Busy error from the supplied Lance transaction conflict below it,
  and preserve the one-second abort grace and exact Unknown reconciliation.

The initial behavior failures and later corrected assertion expectations are
separated in `target/agent-app-repair-evidence/tdd-baseline-results.json`.
Independent source reviews cover these changes and the incremental capacity
cleanup correction. Focused green execution is recorded below; full affected
targets and final-candidate qualification remain required.

Two baseline and two candidate public-API measurements used identical test
source and inputs on macOS/aarch64. Every profile authored 70 real commits,
observed at least one reclamation, checked retained history after reopen, and
alternated first ordinary and transactional observations. The complete raw
observations are in `measurement-{baseline,candidate}-native-receipts.json`
under the same evidence directory. Median combined commit-plus-first-observation
latencies, in milliseconds, were:

| Dataset | Baseline runs | Candidate runs | Paired reduction |
| --- | --- | --- | --- |
| 100 records, 1 KiB | 95.922 / 95.104 | 82.696 / 85.796 | 13.79% / 9.79% |
| 10,000 records, 1 KiB | 201.644 / 197.380 | 92.165 / 93.288 | 54.29% / 52.74% |
| 1,000 records, 16 KiB | 155.176 / 152.391 | 88.015 / 88.848 | 43.28% / 41.70% |

Commit medians alone increased 2.53–7.32% for the small dataset and 0.72–3.49%
for the larger payload dataset; the cardinality runs ranged from a 3.65%
improvement to a 3.33% regression. Thus the claimed improvement is combined
work, not a claim that every phase became faster. Raw distributions include
maintenance outliers; these bounded runs do not establish an SLO or statistical
reliability. macOS optional process-memory counters were unavailable. The
candidate shares JSON payload Arcs but clones O(N) image metadata and O(W)
overlay keys, so memory neutrality is not established.

One earlier baseline body passed but its optional `/usr/bin/time -l` wrapper
failed to read a sandboxed counter; that receipt remains failed and is not one
of the two credited baselines. The credited pair uses ordinary self-process
metrics only. Baseline and candidate source identities accompany each receipt.
Candidate Storage artifacts were explicitly rebuilt after removing only the
Storage package artifacts to avoid stale baseline artifacts in the shared build
directory. No full native dependency rebuild, privileged instrumentation, or
record-count reduction was used to obtain the measurements.

### Scoped repair execution and remaining live findings

The focused repair receipts establish ten Codex creation cases, nine OpenCode
cancellation-persistence cases, one exact caller-reconstruction case, all 22
App workspace cases, both cancellation-outcome fixtures, and all 86 supplied
Lance StateStore cases. The two App fixture assertion corrections retain their
failed receipts: a transient raw Unknown is distinct from an actionable Unknown,
and an abort future dropped at the grace deadline does not later acknowledge
success. The corrected fixtures preserve the original Timeout and prove joined
cleanup on a driven runtime. These results are recorded in
`target/agent-app-repair-evidence/tdd-focused-green-results.json`.

The full scoped live run in
`target/storage-feature-split-evidence/scoped-live-20261010-221700` completed every
selected inventory: Agent 22/23, App 17/18, and the canonical four-boundary crash
case 1/1. Both permitted OpenCode raw-copy grades were true with their mandatory
Kit assertions passing. They provide two observed outcomes, not a reliability
claim or an exemption for another case. The run remains **FAIL**.

- Agent's remaining structured-output case received native OpenCode
  `StructuredOutputError`. The checksum-verified 1.17.18 source emits this when
  terminal completion has no successfully captured structured result; the Kit
  correctly maps it to Protocol / CompletionValidation /
  StructuredOutputMismatch. Native message cleanup prevents distinguishing an
  omitted tool call from other unsuccessful structured-output behavior. No Kit
  defect, output normalization, model substitution, or automatic prompt retry is
  inferred. The classification receipt is
  `target/agent-app-repair-evidence/opencode-structured-output-failure.json`.
- App's remaining notification case passed its first real question and answer.
  Its second conversation failed before prompt polling and became Unknown at
  checkpoint two. One subsequent diagnostic execution, with unchanged
  assertions, recorded the exact public NewConversation / Admission /
  TransactionConflict result. This matches the definite binding-commit conflict
  branch after native session creation. The existing OpenCode contract permits
  retaining Creating for reopen; App correctly does not replay this unknown
  preparation. The diagnostic receipt is
  `target/agent-app-repair-evidence/app-preparation-20261010-223951/receipts.json`.
  A bounded retry of only the proven-unpublished binding transaction is being
  tested as a robustness improvement, not asserted as an already-required
  creation-success guarantee. Native creation, prompt dispatch, uncertain-commit
  observation, and Reopen's single-attempt conflict policy must remain intact.

The first live attempt was interrupted and remains INCOMPLETE after the isolated
Codex login was rejected as revoked. The user authorized repair; the isolated
test copy was refreshed from the current local login without modifying that
source login. The subsequent real Codex cases passed. No credentials are part of
the receipts or candidate. One restart initially reused an old native-log path;
the overwrite and corrected log ownership are disclosed in the auth-repair
receipt rather than claiming that old raw server log was preserved.

Separately, 12 prerequisite-script tests, nine grade-reporter tests, and the exact
pinned protocol-authority test passed with owned cleanup. Their fixtures are
deterministic orchestration evidence, not live-service evidence. Full affected
deterministic inventories, original capacity counts, current feature/platform
and package checks remain required. Historical passes do not qualify them.

The fresh OpenCode binding correction now passes all six focused cases after
three meaningful baseline failures. The real-Lance cases prove one-conflict
progress, original-deadline exhaustion, changed-obligation rejection, uncertain
commit observation without replay, rollback-failure veto, and single-attempt
Reopen. Native creation remains exactly once; no prompt is dispatched. Both the
actual implementation diff and test cleanup ownership received independent
review. Red and green receipts are `binding-tdd-20261010-225324` and
`binding-tdd-20261010-230212` under `target/agent-app-repair-evidence`.

The explicit `serde_json/preserve_order` StateStore target passed all 86 cases
on macOS/APFS (`storage-preserve-order-20261010-225422`). This executes Storage's
deep-value cases directly rather than inferring coverage from App feature
unification. Seven private Agent/App production files now comprise the reviewed
repair boundary; all other changed downstream production files contain only
documentation changes. A fresh scoped run and the remaining qualification are
still required for this candidate.

A further scoped execution (`scoped-live-20261010-230358`) completed Agent's
23-case inventory with 20 passes and three failures. App did not execute: its
previous isolated OpenCode endpoint was absent when prerequisites ran. The prior
server PIDs were confirmed absent, their raw logs retained, and only those two
task-owned services were restarted in root-owned sessions. The precise reason
for their earlier exit is not established. This run is **INCOMPLETE**, with
blocking Agent failures; it cannot qualify App or the canonical crash case.

The three failures are distinct:

- Custom/Ollama shared-directory knowledge output omitted one hexadecimal
  marker character. Existing logs have no direct native-output or retrieved-
  context fidelity witness, so model versus retrieval/Kit attribution remains
  unclassified. This mandatory case is outside the two OpenCode copy exceptions.
- Codex incremental cancellation cleanup reported a missing prompt record. A
  new deterministic delayed-terminal regression reproduces the same error after
  receipt-reconciled cancellation and exhausted public output. The red receipt
  is `codex-late-terminal-tdd-20261010-232715`; this is an established Kit gap,
  not permission to accept arbitrary missing prompt records.
- OpenCode's post-restart question did not arrive before its deadline. Cleanup
  then demonstrated a separate async-future re-poll panic. Source review found
  that a completed ambiguous HTTP result remains in the cancellation owner and
  can be polled again after accepted native abort. A deterministic regression
  must precede the correction. The earlier missing private-part evidence and
  question timeout still need separate attribution.

The structured-output case passed in this later full run; the earlier failure
is retained. One OpenCode copy case passed its Kit assertions and exact-copy
grade, but the failed recovery case supplied no valid grade receipt. The grade
report therefore failed as required. No extra exception, output normalization,
model substitution, or prompt replay was introduced.

The completed-transport regression subsequently produced a meaningful red result
in `opencode-response-tdd-20261010-234240`: the real HTTP connection was closed
while a successfully committed Lance cancellation claim retained its observer;
the configured native request deadline guaranteed a completed HTTP future when
the claim receipt was released. The same async-future re-poll panic occurred.
The first weaker attempt passed and is not credited as red evidence.

The private correction retires that future immediately at each of four manual
Ready observations while retaining the original result for the existing
settlement owner. Pending futures, native dispatch counts, original deadlines,
terminal authority and cancellation policy are unchanged. The focused case
passes in `opencode-response-tdd-20261010-234522`, with one native prompt, one
native abort, one committed claim, Cancelled terminal output and joined cleanup.
Independent actual-diff review passed. This is deterministic adapter evidence;
it neither qualifies the full 299-case target nor attributes the missing native
question. The full target subsequently executed 299 cases in 829.05 seconds: 298 passed
and one Abort-joiner case failed, with no ignored or filtered cases. Its receipt
is `deterministic-20261010-235338`. The initial sandbox invocation failed at
loopback binding before behavior; that environmental failure is preserved in
`agent-fixture-final-20261010-234820`, where Agent and App Clippy both passed.
The focused diagnostic `opencode-joiners-diagnostic-20261011-000832`
reproduced the failure and preserved the originating `Conflict` from continuation
reconciliation through joined shutdown. The fixture had started a separate
reconciliation writer immediately before the Abort owners. That conflict is an
allowed outcome under the existing pending-interaction contract; an observable
fixture-ordering correction is now applied and independently reviewed. The exact
case passes in `opencode-joiners-diagnostic-20261011-001524`: the Abort owner
settles the still-pending durable continuation before its required reconciliation
scans, preserving one native dispatch, original ambiguity counters, and later
progress. No blanket retry was added. The Codex delayed-terminal regression
remains red pending its separately reviewed production correction. Two new-test
compile attempts (`codex-historical-terminal-tdd-20261011-001835` and
`codex-historical-terminal-tdd-20261011-002006`) failed in fixture cleanup
ownership before execution; they are incomplete evidence, not behavioral red
results.

The terminal-only Codex correction is now applied in five existing private owners,
with owning behavior documentation and no public API or persisted-format change.
Native interruption acknowledgement/history can precede terminal event delivery;
full exact native history and a fresh current-owner check authenticate a delayed
terminal after Kit record compaction. Current-generation mutation stays on its
existing path, descendant routing remains intact, and historical proof requires
the exact bound root. No prompt is replayed or second result published.

The tests-first receipt `codex-historical-terminal-tdd-20261011-002217` contains
nine passing safety baselines, eight failing new cases, and the independently
selected original delayed-terminal failure. The same 18 selected cases all pass
in `codex-historical-terminal-tdd-20261011-002508`, with the full 74-case inventory
checked and exact filtered counts recorded. Both root and independent actual-diff
review passed. Read failure, incomplete/ambiguous/foreign history, changed outcome,
missing active state, actual orphan continuation, rollback failure, concurrent
new ownership, and held/uncertain consuming cleanup are covered. Full-target
execution and live qualification remain separate requirements.

The pinned native history reconstructs legacy message-item identities; this fix
therefore does not claim to reconcile late output items, which remain fail-closed.
Identical matching terminal evidence is idempotent under the existing outcome
contract; the adapter does not claim to count identical events after compaction.
All eleven private production files are constrained by the reviewed executable
hashes in the downstream-boundary receipt. Provider/Tool production bytes and
public contract files remain unchanged apart from permitted documentation.

The full corrected Agent targets passed on Rust 1.94.1 / macOS / APFS in
`agent-final-repairs-20261011-003246`: Codex 74/74, Custom 56/56, no ignored or
filtered cases, followed by all-target/all-feature Agent Clippy with warnings
denied. The previous run's sole lint failure was the private reconciliation
enum's layout; an independently reviewed optional boxed record corrected it.
The new Custom case uses its existing recording Model and actual independently
opened Lance StateStore/Database owners in one directory: exact whole knowledge
markers, scope isolation, terminal bytes, one generation and joined cleanup
passed. This deterministic result does not attribute or excuse the earlier real
Custom/Ollama output mismatch.

The original 4,096-delivery capacity case executed in
`app-delivery-capacity-20261011-003620` and failed before its aggregate deadline.
Milestones were 256 receipts at 107.806 seconds, 512 at 266.885 seconds and 768
at 513.942 seconds. At 614.743 seconds the next 20-second public `run.wait()`
observer expired: 835 notifications received, 834 original lower calls, and
834 completed lower receipts. Cleanup settled; the target ended at 616.66
seconds. No target count or deadline was changed. This is failed capacity
evidence, not a completed workload or a demonstrated serialization defect.
Ordinary filesystem samples are retained separately; the dominant cause of the
long observation remains unclassified. Pinned-source review establishes that
ordinary merge scans target keys/row identities, while full maintenance rewrites
all current logical payloads; it does not by itself attribute this timeout.

The diagnostic rerun `app-delivery-capacity-20261011-010927` failed at the same
835 received / 834 calls / 834 returns boundary after 615.937 seconds. Its
20-second wait observer expired, but the immediate bounded public snapshot
succeeded: checkpoint 2505, Running, one Notification with sequence/generation
2505, no action ordinal and no outcomes. Cleanup settled at 617.62 seconds.
This is eventual actionable publication, not proof that the original observer
succeeded or that the full capacity workload completed. Slow phases grew from
roughly 2 seconds early to 19.579 seconds at target 813; ordinary metadata
samples show version reclamation, but do not isolate its internal duration.
The initial `diskutil` metadata command was sandbox-blocked; a separate ordinary
`df`/`mount` observation recorded the actual APFS dataset filesystem. The original
failed metadata receipt remains intact.

The first 4,094 ordinary wait/resolve pairs no longer impose an undocumented
20-second per-operation latency requirement inherited from the Redb fixture.
They directly observe the same original operations inside the unchanged
1,200-second body and 1,260-second authored node deadline. Exact notification
identity/order, all original call/return counts, failure propagation, later
capacity-boundary observers, and joined cleanup remain unchanged. Independent
review checks this as a fixture correction only. Both earlier failures remain
failed evidence; complete capacity execution is still required.
