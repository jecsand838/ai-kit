# agents-kit Strict Testing

## Proof Lanes

### Primary direct-trait mock CI

Run `bash scripts/check_mock_contracts.sh --crate agents-kit` from the workspace
root for the required `mock_contracts` and `custom_mock_composition` targets.
The workspace primary entrypoint is `bash scripts/check_ci.sh`. These targets
use test-local structs and caller-owned exhaustive enums implementing the public
traits directly, with exact associated children and no extension ingress.

The composition target runs the real Custom Agent over caller Provider, Toolset,
StateStore, and optional Database implementations. It covers prompt ownership,
continuations, interactions, conflicts, cleanup, and reconstruction over retained
process-local state. Same-process reconstruction does not prove persistence
across process loss. The [mock testing contract](../../docs/mock-testing.md) owns
the complete inventory, fixture-integrity and runtime-independence requirements;
the [bug ledger](../../docs/mock-testing-production-bugs.md) separates required
regressions, resolved defects, supplemental probes, and remaining validation
failures. These targets do not establish native backend or real-service behavior
and do not replace the full verifier below.

### Static source contract

Static tests protect:

- the exact root/module public allowlist;
- associated Agent -> Workspace -> Conversation trait shape;
- exported-handle trait implementations;
- closed supplied-handle behavior and absence of Agent erasure;
- absence of factories, conformance owners, KnowledgeBase, LoopDriver,
  provider hosted declarations, normalization ingress, aliases,
  compatibility, backend trait objects, and protocol leakage;
- selected vendor metadata and sourced protocol anchors.

They do not lock private helper names, exact files, field graphs, actor names,
match spelling, or call choreography.

The verifier runs the complete `AgentKitProtocolAuthorityTests` Python class
against the committed vendor ZIP archives, verifying each archive's checksum
against `vendor-lock.toml` before reading uniquely matched members. These cases
check the protocol source coordinates, wire facts, native deletion ordering,
OpenAPI shapes and routes, and inventory/terminal facts. They require no ignored
vendor extraction. Rust source contracts retain selected-release metadata,
protocol privacy, and public-surface checks.

### Downstream compile contract

Compile tests implement a custom Agent with associated Workspace and
Conversation types from public imports. They prove Send/Sync and non-Clone
ownership, exact receivers/results, public result constructibility, direct use
without supplied-handle conversion, and associated child types. They also prove
direct non-Clone StateStore, Database, Provider, Model, Toolset, and Handler implementations;
associated transaction/model/continuation/session/handler ownership; built-in defaults; typed native
backend selection; and exact `Agent<S,D,P,T> -> Workspace<S,D,P,T> ->
Conversation<S,D,P,T>` preservation. Supplied-handle behavior requires the
corresponding public trait in scope; compile-negative cases guard against
duplicate inherent forwarding contracts.

The disposable downstream Cargo roots select the same approved Geo 0.32.0
source as the workspace root: Cargo does not inherit patches from dependency
manifests. These test projects copy the workspace lockfile and resolve offline.
Compile-negative checks must reach rustc diagnostics; dependency-resolution
failures cannot prove a rejected public API. Module placement and invariant-field
privacy have separate cases with the corresponding compiler error codes.

### Deterministic runtime contract

Ordinary private test helpers cover agent-owned validation, direct static
Agent/provider/storage/tool composition, hierarchy admission, stale revisions,
StateStore conflict mapping, notification exclusivity/replay,
interaction resolution, cancellation, caller loss, terminal publication,
redaction, and cleanup. Custom prompt tests execute direct foreign handlers
without Toolset normalization and a caller-owned Toolset/Handler enum. They
prove accepted tools survive provider selection, completed tool errors reach
the model, child teardown retains tools, and Agent shutdown settles tools once.
Direct caller Model implementations also prove Custom's complete-response
classification in unary and streaming calls: failed, truncated, filtered,
paused, absent, and contradictory finish reasons cannot execute tools or
commit successful history. These cases cover partial streaming text followed
by failure, recovery with the next prompt, and provider-neutral history after
reopen. Direct caller handlers prove that empty successful and failed tool
results are rejected, while real structured-only content, including JSON
`null`, and nonempty model-visible failures remain usable. This is evidence
about the agent-owned loop, not an assertion that a built-in provider emitted
any of those results.
Model-selection coverage separately verifies Codex's advertised effort choices,
sticky override reset after omission, model changes, and reopen. A missing
sourced reset default and an unadvertised effort must fail before `turn/start`.
Custom coverage verifies the five portable effort names and omission at its
direct Provider boundary. OpenCode coverage requires empty portable effort and
personality inventories and rejection before native session or prompt mutation;
native variants and agent names do not become portable selectors. These are
deterministic adapter contracts, not native model qualification.

The current-model compatibility addition retains the existing 24 live cases and
adds exactly four cases through the existing scenario owners:

| Case | Fixed calls and assertions | Existing finite bounds |
| --- | --- | --- |
| `custom_openai_continuation_stream_and_reopen_are_live` | Two public prompts: one actual `strict_continue` call and its model continuation, then exact prior-answer recall after Agent shutdown and reopen with fresh Provider and Toolset handles. Both prompts require nonempty deltas equal to the authoritative terminal, exact text, and one total tool execution. | 1,200-second body; 240-second prompt; 180-second lifecycle/cleanup. |
| `custom_anthropic_continuation_stream_and_reopen_are_live` | The same two-prompt journey on the exact configured Anthropic model; allowed-tool automatic choice must actually call the tool once. No forced-choice emulation. | The same Custom bounds. |
| `codex_reasoning_selection_reset_and_reopen_are_live` | Three public prompts on the exact configured model: advertised explicit effort, omitted effort, then omission after shutdown/reopen. Every terminal must exactly copy its fresh token. Deterministic request tests separately prove the native reset fields. | 1,200-second body; 300-second prompt; 90-second lifecycle/cleanup. |
| `opencode_cloud_generation_and_streaming_are_live` | Two public prompts on the exact configured OpenAI model through one invocation-owned OpenCode process: unary exact text, then nonempty incremental text exactly equal to the terminal. The isolated provider overlay does not alter the existing Ollama roles. | 900-second body; 300-second prompt; 90-second Kit cleanup; existing 30-second native process bounds. |

Each Custom tool prompt requires one tool-bearing Provider response and one
terminal Provider response; the reopened prompt requires one terminal response.
Unexpected extra tool execution fails the fixed count. These journeys never
retry a generation to obtain a matching answer. OpenCode cloud selection retains
an empty portable reasoning/personality inventory; native variants remain outside
the portable contract. All four cases retain Kit and native owner settlement on
failure. Model credentials and exact roles are mandatory prerequisites, not skip
switches. Each passing result qualifies only its recorded model and settings.
Selection regressions return installed but excluded tool names from direct caller
Models, through unary and streamed completions. They require whole-batch rejection
before handler creation, execution, or approval publication for `None`, `Allowed`,
and `RequiredOneOf`, including mixed eligible/excluded batches and clean prompt
reuse. Passing selected-tool controls retain exact continuation and tool results.
Unknown names under `Auto` and `Required` require the same protocol rejection.
A later excluded turn preserves the earlier accepted tool effect exactly once
without retaining failed-prompt history. Both unary and streaming failures carry
`Protocol` / `Prompt` / `CompletionValidation` / `InvalidData`.

The `native_cleanup_contract` target tests the test-owned cleanup utility itself:
observation panic and deadline expiry remain primary while later cleanup stages
run, cleanup panic/error/expiry does not skip parent cleanup, and a successful
observation reports its first cleanup failure only after all stages are attempted.
Every secondary cleanup diagnostic is retained and printed even when the original
observation panic or deadline remains the test failure.
It makes no claim about a real Codex or OpenCode service.

After requiring a real first fragment, native stream-abort scenarios keep draining
output while the same conversation's abort settles. Both observations remain owned
through failure or panic before assertions are restored. The helper contracts
exercise receiver progress, branch settlement, terminal errors and fused EOF;
they retain the live cancellation/completion assertions and existing deadlines.
Observer overflow remains a failure and never becomes a cancellation result.

Large inline observation and cleanup futures run in owned subprocess controls.
They must preserve the primary panic, drop the timed-out future before returning,
and still run later cleanup. Each child is reaped before its result is asserted;
the controls do not increase the test-thread stack limit.

The same target starts real subprocesses to prove that failed observations reap
a running child and preserve an already observed exit status. These controls use
`sh` and `sleep`, consistent with the canonical Bash verifier's Unix runtime
prerequisite. Process cleanup retains failed status/termination observations and
still makes a separately bounded wait attempt.

Builder tests prove reverse cleanup for direct non-Clone values, including
unsupported native installation before network connection. Shell tests use
caller-built Local registration with explicit cwd and bounded process/output
policy; prompt cwd never becomes implicit tool input.

The OpenCode inventory cases in `opencode_event_ownership` exercise the complete
selected HTTP provider payload through public model discovery. They accept
boolean and arbitrary string-field interleaving on connected and disconnected
providers, reject malformed shapes and inconsistent indexes before projection,
and verify authoritative connected membership, stable portable ordering, exact
configured selection, and absence of private metadata/default leakage. These
loopback cases prove decoding and selection only; selected-runtime behavior
still requires the strict-live verifier.

The `prompt_stream` target proves direct caller implementation, non-Unpin
source adaptation, exact terminal/error fusion, and honest Unsupported default
behavior. Custom prompt coverage proves real provider-source deltas, exact
function continuation, bounded overflow, observer loss, cooperative draining,
abort, and waiter-failure recovery including Agent shutdown followed by fresh
reopen. The `codex_prompt_stream` target drives the real WebSocket adapter with
selected pinned protocol fixtures: UTF8 fragments precede the authoritative
structured terminal, ignored reasoning and stale generations cannot leak into
output, and drop, overflow, deadline, abort, and native runtime task ownership
remain independent of observation. The same target covers a held start receipt,
one uncorrelated early fragment, a pre-response output flood, exact root-thread
ownership, a held caller StateStore transaction, and definite versus retrying
terminal cleanup failures. Early output remains unreadable until the original
receipt proves ownership; mismatched text is discarded, and overflow or runtime
revocation cannot block native progress. Held cleanup and timeout-intent commits
prove retained lower-operation ownership beyond observer deadlines. These are deterministic contract gates;
they do not establish a live provider or Codex service result.

OpenCode deterministic coverage also proves recovery state transitions without
claiming a live protocol result: startup/reconnect synchronize a sole new
stream; gap and structured-parent recovery capture the current stream; a
failed stream is dropped before replacement; retries consume one absolute
Agent-construction deadline even when a per-request timeout is longer; one-shot
stop interrupts connect, history, and backoff before joining the owner task; a
structured command timeout preserves its healthy stream; bounded command
capacity applies FIFO backpressure through the same absolute deadline and
never fabricates `Busy`; generation-mismatched
recovered prompt commits conflict; and abort progress cannot release before
its exact continuation settlement.
Recovered-abort outcome cases retain their setup fault until the original
foreground task drains under the existing bounded ownership check. A streamed
timeout and a fused stream establish observation completion, not foreground
release; changing faults earlier can exercise foreground cancellation instead
of the intended recovered owner.
Interaction settlement also retries definite local read, write, and commit
conflicts without replaying an accepted native response, bounds persistent
conflicts by the existing lifecycle deadline, and preserves non-conflict
storage failures.
Workspace creation similarly retains one identity across definite transaction
conflicts, commits one workspace, and bounds persistent conflicts by its
original lifecycle deadline. Non-conflict and uncertain commit failures do not
cause another mutation attempt.
Foreground prompt fault cases require definite local conflict recovery for
dispatch, admission, caller loss, response loss, delivery claim, and delivery
settlement. Finite conflicts must preserve one native prompt; persistent
conflicts must exhaust the original state-stage deadline. Separate admission
cases distinguish a rolled-back commit conflict, an unchanged ambiguous
commit, and a committed outcome whose receipt was lost. The last two may use
exact reads, never another mutation or native dispatch. These cases exercise
public caller-owned StateStore implementations and do not establish a native
built-in conflict; the controlled live Lance case below establishes that separately.
Terminal persistence and cancellation reservation separately exercise put and
consuming commit conflicts, failed rollback, unchanged uncertain commit, and
an applied commit with a lost receipt. Exact write and native prompt/abort
counts distinguish recomputation from unsafe replay. Further controls target
the cancellation dispatch authorization boundary, recovered reservation, and
primary Cleanup failure from an unchanged transaction. Owner-specific checks
reject repetition by the failed owner while allowing the same explicit abort
to recover under a different owner in the same generation. The protocol peer
checks that native abort arrives after that owner's successful Dispatching
commit. Expected idle repair after intentionally losing terminal authority
must return typed InvalidState and still remove the durable record before
independent hierarchy shutdown.
A separate no-storage-fault response-loss case requires one accepted native
abort to settle its exact generation while preserving the observer's original
Protocol error. The durable-state diagnostic distinguishes an unresolved
response from an accepted cancellation that failed to terminalize.
A separate conflict-plus-rollback-failure case requires preserved cleanup
causality and no later native dispatch. Receiver receipt cases cancel a pending
read-only observation before close, and interrupt a real passive acknowledgment
after native commit or during its exact read. Later process publication must
survive with its original identity, and cleanup must drain without a stale
whole-record contradiction. Negative receipt observations restore an already
acknowledged identity or invent an unaccounted sequence increment; both must
terminate with InvalidState. A receipt conflict followed by failed rollback
must retain Conflict plus cleanup evidence without another receipt write.
Interaction settlement timeouts assert typed
Shutdown/Cleanup/DeadlineElapsed and retained state, single writes, no handler
replay, and successful reopen rather than one scheduling-dependent message.

These helpers are not production conformance APIs and do not prove built-in
protocol/provider/tool/storage behavior.

The `error_contract` target proves safe caller construction, closed classification,
normalized sources, dependency timeout/cancellation distinctions, and bounded
retention. Native fixture targets assert structured failures, startup phases,
nonterminal retries, and Codex version-4 durable failure recovery. These tests
are deterministic evidence, not live-service evidence.

### Strict-live

Strict-live uses real selected Codex and OpenCode servers, real provider
models, real Toolsets/MCP dependencies, real StateStore/Database services, and
real processes. Mocks, scripted services, recordings, snapshots, fakes, and
synthetic output do not satisfy this lane. Missing required environment fails.

The Codex interactive-answer tool describes its actual return shape: a plain
string when invoked through native Code Mode. The selected model's native
metadata can require Code Mode even when the process feature default is off.
This description supplies no expected answer and changes no exact-output,
one-call, streaming, deadline, or cleanup assertion. A description correction
is a changed model input and requires fresh live qualification.

Kit correctness assertions run for every configured model. Prompts that request
an exact token or interaction answer require exact raw terminal equality, with
one explicitly approved acceptance-policy exception for these two cases:

- `strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains`
- `strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build`

Those cases keep the same real Question trajectory, independently generated
answer, configured model, original deadlines and cleanup. Their Kit gate requires
exact typed interaction/call/result correlation, ordered submitted answers,
existing replay/resolution/reopen/streaming behavior, and exact fidelity between
the complete native terminal text and untouched portable terminal text. Native
observations are test-local and read-only; they do not expose a public backend
envelope. Each scenario emits its receipt only after successful cleanup.

The shared Question prompt explicitly requests `Yes`/`Continue` first and
`No`/`Stop` second. Header, question, labels, flags, option count and order
remain exact Kit/scenario checks. Description copying is graded separately for
each option: the complete native description is compared byte-for-byte with
`Continue` or `Stop`, without trimming. The pending Question, running tool and
completed tool input must preserve the same actual descriptions exactly. Missing
or malformed descriptions, changed replay, option sorting and unordered
comparison still fail the Kit/scenario checks.

The pending-interaction process-loss reopen case retains its exact fresh-token
assertion. On a mismatch it captures bounded native history before cleanup,
within the original observation deadline. The diagnostic links the unique fresh
prompt to assistant parent identities and compares native text with both the
portable output and expected token; only counts, fixed tags, and equality flags
are printed. It rejects ambiguous roots, duplicate part identities and foreign
ownership. Recent assistants are reported by creation time, then identity,
including stale parents. The report is a post-response history observation,
not a capture of the preceding HTTP body or raw provider response. Capture
failure cannot replace the original assertion or create a passing grade.

When Question delivery fails before receiving its interaction, the existing
Kit-spawned case attempts two read-only native snapshots before cleanup: its
exact session messages and currently pending Questions. Fixed, bounded fields
report prompt-root correlation, Assistant completion/error/finish, text counts
and Question-tool state without native identities or arbitrary content. These
are current-state observations, not retained HTTP/SSE bytes or a grade. The
message summary is emitted before the pending-Question read; each of the two
fixed summaries is bounded to 4 KiB. Reads consume the original scenario deadline; interrupted capture preserves the
original failure and does not extend cleanup or authorize another invocation.

For native-idle and pending-interaction reopen, an early terminal child result
also emits a failure-only tuple of its case and public Agent/Workspace/Conversation
IDs using fallible writes. If the child fails, its parent retains the original
panic and attempts existing bounded native readback before fixture cleanup,
requiring exactly one session in its fresh, exclusively owned native process and
exact prompt-parent correlation. Health, session inventory and failure snapshots
are read only on that child-failure path, within the original parent deadline.
These later snapshots do not establish original HTTP/SSE parity or model authorship;
incomplete capture preserves the original failure. These child-failure snapshots
leave successful-path I/O, cleanup and acceptance unchanged and add no retry authority.

The separately authorized empty-completion investigation passively captures only
pending-interaction reopen and repaired-abort traffic between OpenCode and the
configured local Ollama endpoint. Its invocation changes only the Ollama base
URL; prompts, models, tools, generation limits and scenario deadlines remain
unchanged. Complete prompt comparisons, model/schema/selection facts and ordered
response-fragment hashes distinguish missing native tool output from later
normalization. Valid empty `stop` output can be complete diagnostic evidence
while the required-interaction assertion still fails. Foreground requests and
native background traffic are accounted for separately. Socket forwarding,
native capture completeness, Kit outcome, cleanup and process settlement remain
independent; a complete failed prefix never qualifies recovery or abort. The
existing 7,200-second supervisor owns each exact execution. This authorization
does not repeat passing Question cases or broaden their quality-grade exception.

The pending-interaction recovery investigation additionally reads native terminal
history after the existing follow-up prompt, before fixture cleanup. It retains
the original exact-output assertion failure before awaiting diagnostics. At most
three read-only requests identify the healthy owned server, its unique session,
and complete message history within the earlier of the original scenario deadline
and a ten-second readback bound. The exact original prompt, selected model,
message parent and part ownership identify one successful terminal Assistant;
its nonsynthetic text is concatenated in native PartID order without rewriting.
Bounded fingerprints and equality facts compare native text, portable output and
the expected token. This is a later native snapshot, not original HTTP-byte
evidence. Ambiguous or incomplete history cannot establish parity. The original
exact-output assertion remains blocking; this case has no copy-grade exception.

The separate-directory managed-knowledge investigation extends its existing
Provider evidence owner with bounded private observations of each reached phase's
ordered messages and expected synthetic references. Comparisons validate each
reference's source id, title and exact text, then compare Provider messages with
the local Ollama wire request and native text with Provider and Agent output.
Public summaries contain only hashes, lengths, structural facts and equality
results. The private sidecar survives a failed phase and exports through existing
cleanup without replacing a primary failure. A fully observed failed prefix is
diagnostic evidence, not a passing scenario; all three phases and every existing
scope, reopen, wrong-database and exact-output assertion remain required. The
capture uses an invocation-only endpoint override and preserves authoritative
environment bytes. Neither investigation authorizes an unchanged retry.

Typed-question mismatch errors retain only bounded, escaped previews of the
header, question, first two option labels/descriptions, flags and option count,
with explicit truncation. They omit native identities and unrelated message or
provider bodies. Validation still compares the original complete values;
diagnostic truncation never changes the acceptance rule or creates a grade.
Tool-correlation mismatch diagnostics report only identity/name equality flags,
state kind and fixed input-field shapes, counts and the same bounded string
previews. Unknown key names/values and native identities remain absent. The
complete original tool input still must equal the expected typed JSON, using
the unchanged descriptions observed in the pending Question. This preserves
native-input fidelity independently of the requested description wording.

Three separate model-copy grades compare the complete raw terminal string with
the independently generated answer, and each original option description with
its requested literal. A false grade is visibly **FAILED** but
nonblocking for Kit completion. It is never normalized, trimmed, case-folded,
parsed into an answer, accepted by substring or relabeled successful. One trial
per case is retained; there is no retry-until-pass or inferred reliability
threshold. Even two exact copies establish only these observations, not general
model-quality acceptance. All other exact-answer assertions remain blocking;
there is no weaker conformance profile or silent model substitution.

The canonical verifier retains complete strict-live output in a private 0700
`target/agents-kit-strict-live/question-grades-*` directory with a 0600 log. It
requires exactly two anchored `AGENTS_KIT_QUESTION_GRADE=<JSON>` receipts, one per
case, with six quality outcomes across the two cases. Version 2 has exactly
these fields: integer `version` equal to 2, the full `case` name, `sdk_contract`
equal to true, booleans `model_exact_copy`, `model_yes_description_exact_copy`
and `model_no_description_exact_copy`, and nonempty
`runtime_version`, `provider_id`, `model_id`, `configured_model` strings. Native
runtime version must equal the owning specification; configured model must equal
the unchanged `TEST_OLLAMA_MODEL_OPENCODE_SHELL` role. Native identity matching is
proved by the scenario, not by parsing the public opaque selector in the script.
Receipts contain no raw answer, description, output or session/call identifier.
Historical version-1 receipts retain their original scope and outcomes; they
do not establish the new description grades and cannot satisfy this validator.

Missing, malformed, unknown, duplicate, Kit-failed or wrong-provenance receipts
fail the gate. Reporting runs even if Cargo or log capture fails; the Cargo exit
remains primary, followed by log failure and receipt failure. Passing receipts
cannot override a failed test process. Ordinary `question_grade_contract.py`
process fixtures prove these orchestration rules with command stubs, including
successful Kit / failed copy and real `tee` log-open failure. The ordinary
`strict_live_assertions` Rust target independently covers the native evidence
and output-fidelity grader; the canonical verifier runs it before strict-live.
These fixtures provide no live-service or model-quality evidence.

The bounded Custom Anthropic investigation observes the supplied Provider and
Model through test-local composition. It preserves the original events and
errors while recording scenario phase, generation ordinal, terminal disposition
and the safe Provider normalization rule before Agent's public error projection.
The original cloud continuation, fresh-handle reopen, exact-output and cleanup
assertions remain blocking. Diagnostic export failure is supplementary to an
existing scenario failure; no successful diagnostic disposition precedes cleanup.
This observation adds no generation, retry, forwarding task or production hook.

The recorded PR #21 two-case boundary capture used the existing ignored HTTP
observer and invocation owners, with its original 28 Agent cases inventoried and
each selected case executed separately (one selected, zero ignored, 27 filtered).
A new capture on the integrated 29-case candidate must inventory that candidate
and report one selected, zero ignored, and 28 filtered. It retains the
7,200-second Agent supervisor and each scenario's original operation and cleanup
deadlines. Anthropic client-tool blocks and OpenCode's model-facing
`StructuredOutput` declaration/selection are diagnostic facts, not synthetic
answers. Missing or malformed native output can be complete observed evidence;
incomplete forwarding or projection cannot establish its absence. The observer
preserves application-body bytes, not TCP/header framing. Local Ollama remains a
plain-loopback boundary; Anthropic uses verified upstream TLS. A failed case
remains failed even when its capture is complete. Such focused captures do not
finalize a vendor update or replace the complete Agent gate.

Knowledge prompts require one line of managed marker values in Database,
Agent, Workspace, Conversation order, separated by single spaces, with absent
scopes omitted and no other text. Expected values appear only in ingested
reference data and test assertions. Exact full output proves both inclusion
and exclusion. The reopen case uses a second conversation created before
shutdown, with its own Conversation-scoped marker and no prior prompts, so an
old assistant answer cannot satisfy retrieval after reopen. The initial case
still proves the original conversation's four scopes; the cross-Agent case
requires only shared Database and that Agent's own marker.

Both directory layouts passively observe these three unary calls through
test-local owners of real Provider/Model values. Discovery, exact model selection,
generation, stream opening, and shutdown forward unchanged; no call is
added or retried. Per-phase evidence records exact synthetic-marker occurrence
counts in the original request and compares the unchanged normalized Provider
Text with the public Agent output. Only bounded metadata and synthetic markers
are logged; a 64 KiB evidence bound never truncates the returned response.
The original exact-output assertions remain mandatory and primary. This
evidence locates a mismatch at the normalized Provider/Agent boundary; it does
not establish raw HTTP response fidelity or attribute a failure to the model.

Native prompt/stream/question scenarios catch the entire observation, including
model selection, prompt admission, structured-output checks, interaction handling,
and the original absolute observation deadline. That deadline is inside the
owner-retaining scope; expiry cannot drop the cleanup sequence. The observation
budgets remain 300 seconds for the Codex plain scenario, 900 for OpenCode plain,
1,200 for either streaming/reopen scenario, and 1,260 for OpenCode question delivery.
Cleanup is separately bounded once per phase (60 seconds for plain/question,
90 for streaming; setup follows its configured lifecycle timeout within the
original case deadline), and every later parent cleanup is attempted after an earlier
error, panic, or timeout. Conversation close precedes receiver drain, workspace
close, and Agent shutdown. Primary observation failures are reported afterward;
cleanup failures cannot replace them. Successful first-phase stream records remain
available for the deliberate reopen; failed observation closes those children
instead. Partial hierarchy creation also retains cleanup of already-created owners.
These are bounded cleanup attempts: a timed-out native operation still follows the
production API's retained-work rules and is not claimed to have joined successfully.

Custom live cases also retain partial hierarchy construction and every helper's
Conversation outside the protected observation. Each consuming cleanup task is
joined with the existing 180-second lifecycle budget; later parents and temporary
paths are attempted even after an earlier failure. Streaming keeps one original
1,200-second observation deadline across both stages, with cleanup outside that
deadline. Only a successful first stage preserves quiescent children for reopen.
The process-loss child still exits deliberately after its durable handoff; a
failure before handoff instead cleans its acquired hierarchy and temporary root.

Managed-knowledge cases retain their Agent, descendants, pending consuming
cleanup task, and Lance paths outside the original 1,200-second scope or
300-second integration observation. Flow errors and exact marker assertions
remain primary over cleanup failures. The existing 180-second build/prompt,
210-second prompt wait, and 150-second shutdown budgets are unchanged. A
successful Agent shutdown preserves the unprompted descendant for reopen;
failed observations explicitly attempt child, parent, and path cleanup.

Hierarchy observations retain their original 240-second Codex and 300-second
OpenCode deadlines, and caller-loss retains its 390-second deadline inside the
owner-retaining scope. Intermediate Agent shutdown and caller tasks remain owned
until their joins complete, including when observation expires. The spawned
cutoff case records listener/process observations before fallback cleanup and
retains its creation, shutdown, and utility-command tasks. Restart/reopen
cases retain partial hierarchy owners and independently attempt
conversation close, notification drain, workspace close, Agent shutdown, and
process shutdown. Their existing stage-specific model and lifecycle limits remain
unchanged; outer observation watchdogs are 2,400 seconds for native-idle reopen,
1,290 for pending-interaction reopen. Crash-child and
stock OpenCode startup failures preserve their original observation while
attempting child termination and awaited reaping; directories remain when process
settlement cannot be confirmed. Intentional crash-child exits still establish
process loss, not graceful Agent shutdown.

### Direct one-prompt Codex diagnostic

`codex_direct_live` is an explicitly selected live diagnostic outside the canonical
verifier inventory. It imports no shared test helpers: public Agent calls use a
real caller-owned Codex server, directory-backed Lance, and one Local interactive
tool that returns the caller's actual notification answer. It performs one prompt
in a fresh conversation, using the original interactive instruction and configured
model, with exact output, real streaming, single tool invocation, and cleanup
assertions. It preserves the 90-second prompt deadline. It does not exercise the
preceding caller-loss and recovery turns and cannot replace that mandatory case.

The launcher must own the selected Codex process and verify its release, provide
`TEST_CODEX_WS_URL` and `TEST_CODEX_MODEL_DEFAULT`, and set
`TEST_CODEX_DIRECT_STATE_DIR` to a nonexistent directory under an existing private
evidence directory. The test retains that Lance dataset after Kit shutdown for
inspection. Run exactly once, with a nonempty inventory and outer process
supervision; do not retry an unchanged failure:

```bash
cargo +1.94.1 test --locked -p agents-kit --test codex_direct_live -- \
  codex_direct_interactive_answer_is_exact --exact --nocapture --test-threads=1
```

## Required Scenarios

The complete live suite retains:

- Agent shutdown and Workspace close with idle/active descendants;
- durable reopen, stale revisions, and process-loss reconciliation;
- prompt caller loss, abort races, timeout, cancellation fencing, and later
  reuse;
- ordered notification replay, receiver exclusivity, interactions, and
  terminal drain;
- Codex selected protocol lifecycle and cleanup;
- actual incremental text before the authoritative terminal for Custom, Codex,
  and OpenCode: the three `incremental_output_abort_and_reopen_are_live` cases
  observe real fragments, abort after partial output, shut down the Agent,
  reopen its persisted workspace/conversation with fresh runtime owners, and
  stream again before complete cleanup. A remote natural completion racing
  abort is valid terminal authority; deterministic gated cases separately prove
  cancellation of work that is still active. Terminal-only output does not
  satisfy these live cases;
- simultaneous streamed output and durable notification handling in the
  existing Custom approval, OpenCode question-delivery, and Codex caller-loss
  scenarios. They read real fragments through the same stream observer while
  resolving actual Agent notifications, require an authoritative terminal and
  exact single tool execution, and keep notification lease replay/drain checks.
  Codex process delivery also streams while checking ordered process events;
- exact `Some(7)` for the normal command deliberately exiting with status 7
  in Codex process delivery. The public exit-code field remains optional;
  that particular live scenario requires its available exit evidence;
- OpenCode conversation construction for an owned canonical backend-host cwd
  and a real directory-symlink alias, preserving the caller's exact public
  context. Each row owns an isolated native process and caller Lance store;
  native `/path` corroborates the shared directory identity. No prompt or model
  generation is used, and public observations are asserted after bounded
  Agent/process cleanup; missing finalizer receipts retain backing files;
- OpenCode selected event/history recovery and native tool interactions,
  including process restart/reopen followed by actionable delivery and
  terminal prompt completion;
- OpenCode process-loss reopen with a pending native interaction, build-owned
  cancellation before publication, and successful later prompt reuse;
- Custom Provider -> Model generation, exact prompt-local associated
  continuation across function turns, provider-neutral reopen history,
  Toolset execution, approvals, shell-as-tool, bounded parallelism, and
  cancellation;
- private managed-knowledge ingestion/retrieval/reopen/cleanup;
- exact conflict versus non-conflict StateStore error propagation.
- one controlled Lance/OpenCode event-versus-foreground overlap: retain a real
  event transaction after its prompt-row update and wait until a foreground
  transaction from the earlier snapshot reaches commit. Publish the event
  first, then release the foreground commit and require its actual supplied
  StateStore `TransactionConflict`. The original native prompt must finish
  once after fresh local recomputation, with exact request-part correlation.
  Both witnesses share the original 120-second prompt deadline with a
  15-second reserve for commit and recomputation. Native generation latency
  does not establish overlap. Cleanup releases both gates on every outcome,
  settles the hierarchy, joins the store, and removes its owned directory.
  The existing 240-second body, 120-second lifecycle/prompt/native request,
  30-second startup/idle/event-entry, 15-second StateStore operation, and
  150-second cleanup-observation bounds remain unchanged.

The Codex release request cases live in `strict_live_suite::delivery_and_events`:

- `codex_stdin_approval_reaches_the_original_process` starts a harmless bounded
  process in an owned workspace, settles its launch approval in the first turn,
  then requires the actual terminal-input approval in the next turn. It checks
  exact terminal text and the bytes written by the original process. A command
  approval cannot substitute for the stdin approval.
- `codex_nonblocking_input_returns_unsupported_without_actionable_notification`
  requires one real Default-mode input request, its exact correlated unsupported
  response, no actionable portable Interaction, and successful terminal cleanup.
  Failure to elicit the request is blocking, without retry or skip.

Both cases verify exact selected runtime identity through normal Kit startup,
use the configured model, require an empty personality inventory, and reject
explicit personality selection. Each owns a real stdio App Server with a passive
WebSocket framing bridge. The bridge forwards payloads unchanged and captures
ordered initialization, thread/turn identity, selected model and exact prompt
comparison, callback correlation, received/forwarded milestones, terminal state,
and existing history facts. In this no-knowledge fixture, prompt binding retains
separate fingerprints for the original caller text and its exact existing
empty-managed-context rendering. It compares the entire native prompt against
that rendering; it does not accept arbitrary prefixes or normalize the text.
It provides no scripted native responses or extra RPCs. Effective feature
responses do not prove the model-facing tool inventory.

The nonblocking case may export private `AGENTS_KIT_CODEX_INPUT_EVIDENCE` after
cleanup. Before conversation cleanup, it reads the native-emitted rollout path
once, within the earlier of the remaining scenario deadline and ten seconds.
The absolute regular non-symlink file must remain beneath the canonical configured
Codex home's sessions directory and retain stable identity and metadata. The
snapshot binds the exact thread, fixture directory, accepted turn, selected turn
model and the validated rendered native prompt, with its original-caller binding;
complete UTF-8 JSONL and a matching persisted terminal are required. Legacy and
paginated root histories retain their native record order;
paginated ordinals must be complete and consecutive. Native world-state and
correlated token-usage records contribute only structural counts, never contents. Limits are 8 MiB, 8,192 records, 1 MiB per record and 1 MiB exported
evidence. Missing terminals, compaction gaps, persistence warnings, redirected or
changing files, malformed records and exhausted bounds make evidence incomplete.
Projection retains ordered tool names, argument shape/validity, correlation,
content fingerprints and recognized error categories, excluding reasoning,
credentials, system instructions and raw native prose. Missing error metadata
cannot prove native tool success: persisted output omits the internal success flag.

Passive forwarding, ordinary App Server history and the rollout snapshot are
separate evidence boundaries. Complete native history is not a raw model-service
capture; no recorded tool call does not establish why native omitted it. A failed
scenario remains failed even with complete diagnostics. Diagnostic/export errors
remain supplementary to the original assertion, and cleanup always runs. The
stdin-approval journey and every blocking callback assertion remain unchanged. Processes use the existing isolated
`TEST_CODEX_HOME` and process-local overrides:
`features.write_stdin_approval=true`, `features.default_mode_request_user_input=true`,
`approval_policy="on-request"`, `approvals_reviewer="user"`, `mcp_servers={}`,
`features.apps=false`, `features.code_mode=false`, and `features.code_mode_only=false`.
They do not change the caller-managed server's configuration or credentials.
The caller-managed launch must omit the retired public
`approval_policy="untrusted"` override; selected Codex rejects that setting.
Native project-trust behavior remains internal to Codex.
The existing 180-second Codex scenario, 90-second request, 60-second hierarchy
cleanup, and 30-second process cleanup bounds remain unchanged.

Deterministic `codex_prompt_stream` cases cover the selected error/status matrix,
malformed known and unknown error classifications, redacted interruption causes,
full-history/reopen parity, callback identity and ambiguity, new notification
scope, async item rejection, and private function output. This does not claim
live generation of rare policy errors or model-dependent async message variants.

Selected-release deterministic coverage additionally verifies exact runtime
identity, typed `thread/readState/changed` validation (including omitted/null and
historical unread positions), foreign-thread rejection, and repeated receipt
isolation from portable notifications and prompt completion. Reopen controls
check explicit permission/cwd/root/config preservation and effective-policy
rejection. A `thread/resume` control checks private shutdown reason metadata and
one rejected resume without replay; it does not qualify `turn/start` rejection
reconciliation. Optional read/list receipt fields do not become Kit state. These fixtures
do not prove native read-receipt emission or cold-resume permission restoration.

The integrated selected-release inventory contains exactly 29 Agent strict-live cases,
with the recorded models, prompts, generation counts, settings roles, assertions,
deadlines, and three mandatory copy grades for each of the two Question cases. The earlier 28-case PR #21 candidate completed sequential
Codex 0.163.0 and OpenCode 1.19.3 qualification; the
[final qualification report](../../storage-kit/docs/feature-split-qualification.md#final-scoped-qualification--october-23-2026)
retains their source identities, outcomes and limitations. Those receipts do not
qualify later source changes automatically. For subsequent authorized updates,
the updater retains ownership of its candidate lock, all configured prerequisites
and completion gates, canonical live attempt, post-live checks, lock restoration
on failure, and archive finalization. Deterministic coverage does not add a
cold-resume live case or authorize extra focused attempts.

## Environment And Commands

The ignored `agents-kit/strict-tests.env` owns credentials and machine-specific
paths. The tracked example documents required names without secrets. Precheck
validates required environment values and authentication-state presence,
selected server versions, isolated state, service availability, and configured
Ollama roles. Credential usability is established only by successful real
operations in the live scenarios; presence of a key or auth file is not proof
of successful authentication.
Precheck HTTP reads have a two-second connection bound and ten-second total
bound, with retries disabled. Empty-prompt Ollama cold loads have a separate
360-second total bound. Each unload has one 30-second budget shared by its
single POST and read-only `/ps` polling until the exact model is absent,
including failure cleanup. No load begins before that absence is observed. The load budget is this gate's finite operational allowance,
not a claim that Ollama's five-minute native stalled-load detector is a total
load deadline. A local timeout does not establish native cancellation. Failed
cleanup is reported separately and never replaces the primary exit status.
An unload response alone acknowledges expiration scheduling; the additional
absence observation establishes that the prior residency is no longer listed,
not that every native worker has been joined. Every residency row must have a
nonblank string name; malformed rows cannot authorize a fresh load. Names are
compared exactly. The configured primary remains warm after successful precheck.

The native `/ps` row must retain the exact configured model identity, integer
context of at least 65,536, and positive equal `size_vram` and `size` byte
counts. Counters must be exactly representable for the JSON comparison. This
is the official Ollama 0.34.4 CLI's full-GPU branch; mixed residency rounding
is not accepted. The redundant `ollama ps` subprocess is unnecessary. TCP
readiness probes use the existing Node prerequisite with a two-second bound.
The ordinary crate-local `tests/precheck_contract.py` cases run real curl
against disposable orchestration fixtures; these are not strict-live evidence.

All real Agent persistence and managed-knowledge fixtures use embedded Lance.
No external storage service, SQL monitor, or storage credentials are required.
Memory fixtures remain process-local; durable and supervised-child cases own
one exact directory that the parent retains until children are reaped and
store owners settle. Separate-catalog and shared-catalog knowledge cases both
remain covered. Caller-defined trait probes retain their existing role.
The test entrypoint also fails immediately for missing Codex model/effort
selection, Gemini API key/base URL/service tier/model, OpenAI and Anthropic
keys/base URLs, or the three Custom/cloud model roles already required by
precheck. `TEST_MODEL_CONFORMANCE_PROFILE` is retired and rejected even when
set to an empty value; remove it from the env file and unset it in an existing
shell before sourcing the current configuration.

`TEST_CODEX_MODEL_DEFAULT` names the exact native selectable slug, with
`gpt-6-luna` as the example value. Each Codex generation scenario requires
the exact public inventory selector and sends it explicitly. An unavailable
configured model fails; advertised defaults or inventory order never replace
it. Authentication belongs to the isolated `TEST_CODEX_HOME/auth.json`, not
`TEST_OPENAI_*` values, which belong only to the separate Provider-backed and
OpenCode cloud journeys. `TEST_CODEX_REASONING_EFFORT` must be advertised by
the selected native model; omission/reset qualification cannot substitute a
different effort when it is unavailable.

OpenCode plain generation uses `TEST_OLLAMA_MODEL_OPENCODE_DEFAULT`; native
tool/interaction scenarios use `TEST_OLLAMA_MODEL_OPENCODE_SHELL`. The isolated
single-provider overlay gives every role its exact tag as its display name.
Tests require a unique matching inventory entry and round-trip its opaque
selector without decoding or reconstructing it. The alternate role is an
inventory/isolation prerequisite, not a generation-coverage claim. OpenCode
roles remain pairwise distinct and must match the overlay, with unchanged
privacy, tool, context, output-budget, and GPU requirements. Role tags are
configurable; model changes never change the assertions.

Existing Custom generation and streaming use the configured Gemini model.
The two added continuation/reopen journeys select `TEST_CUSTOM_OPENAI_MODEL`
and `TEST_CUSTOM_ANTHROPIC_MODEL` exactly. Custom managed knowledge uses
`TEST_OLLAMA_MODEL_CUSTOM_DEFAULT`; durable build cases also select that Ollama
model. The isolated OpenCode cloud journey requires `TEST_OPENCODE_CLOUD_MODEL`
as a bare model ID: the test adds the `openai/` prefix. It uses the same
caller-supplied OpenAI key/base URL and enables only that exact
OpenAI model in its own process, disables sharing/plugins/MCP/background
subagents, and uses the existing FullAccess execution policy. Its session-level
wildcard allow takes precedence over configured native-tool denial, so this
case does not qualify native-tool denial. Existing Ollama process configuration
and roles remain unchanged. The unused Ollama reasoning role is still not an
Agent prerequisite. providers-kit's separate env file owns its broader Provider
coverage.

Run from the workspace root:

```bash
source agents-kit/strict-tests.env
bash agents-kit/scripts/precheck_strict_live.sh
bash agents-kit/scripts/verify_strict_live.sh
```

For an explicitly requested local pass, `bash agents-kit/scripts/verify_strict_live.sh
--skip-strict-live` retains the process contracts, all local Rust targets,
documentation and lint gates while omitting the real precheck, `strict_live`
execution and its live grade reporting. It does not require loading the local
strict environment. The result is qualified local evidence, not Agent strict-live
completion. Workspace continuation and source-bound reuse are documented in the
[release qualification guide](../../docs/release-qualification.md).

`strict-tests.env.example` is the setup source for new environments. When
updating an existing local file, preserve its model, credentials, and unrelated
settings and explicitly export `TEST_CODEX_REASONING_EFFORT`. The example selects
`high`; use an effort advertised by the exact `TEST_CODEX_MODEL_DEFAULT` model.
For example, `gpt-5.6-luna` also supports `high`, as documented in the
[official model reference](https://developers.openai.com/api/docs/models/gpt-5.6-luna).
The native model inventory remains authoritative for the selected Codex runtime.
The precheck rejects a missing setting, and strict-live verifies model inventory
membership. Sourcing the file exports the selection to the verifier and Cargo
test processes; the workspace wrapper sources this same file before the Agent
gate. Neither path supplies a fallback.

The template also prepends the selected Codex `0.163.0` and OpenCode `1.19.3`
package directories under `target/`. Install those exact packages using its
commands, and update stale PATH entries in existing local env files. Verify
`codex --version` and `opencode --version` after sourcing the file: earlier PATH
entries can otherwise select a different release than the default shell.
Restart test services with the selected executables; changing PATH does not
replace an already-running server. Precheck retains exact executable and
server version checks against `specs/SPECIFICATIONS.md`.

The verifier owns the complete agent crate matrix: source/compile contracts,
deterministic targets, strict-live, rustdoc/doctests, clippy, and formatting.
Do not replace its terminal result with partial prior evidence.

When the workspace-wide integration is active, final workspace validation may
also invoke the five-crate strict wrapper. Ordinary agent-only changes remain
scoped to this crate-owned gate.

## Resource And Cleanup Rules

- Serialize Cargo commands. Select an artifact-only `CARGO_TARGET_DIR` for each
  Agent validation cycle, such as `target/agent-validation/<cycle>`, while
  preserving any target directory explicitly selected by the caller. Reuse
  that directory for focused commands and the canonical Agent gate throughout
  the cycle; no workspace-wide target default or additional runner is required.
  Check available disk space before a fresh build and retain the existing
  `CARGO_INCREMENTAL=0` requirement.
  Retire a cycle's artifacts only after its builds, tests, and owned processes
  finish, using `cargo clean --locked --target-dir "$CARGO_TARGET_DIR"` only for the
  selected artifact-only directory. Do not automatically delete existing
  targets or unrelated caches, or run an unqualified workspace `cargo clean`.
  The workspace `target` also contains live-test authentication and durable
  state; selecting a build directory does not authorize removing those data.
- Run the OpenCode protocol target with
  `cargo test --locked -p agents-kit --test opencode_event_ownership --all-features -- --nocapture --test-threads=1`.
  Each case owns loopback HTTP/SSE listeners, connections and runtime tasks;
  serial scheduling avoids competing fixture startup and port resources across
  cases. Concurrent operations inside each case still run concurrently under
  the same deadlines, including queue, abort, commit and shutdown races. This
  changes test-case scheduling, not operation limits or concurrency assertions.
- Bound operation admission and observation waits. An admitted finite blocking
  HTTP client constructor remains owned through its original join, including
  after caller loss; its settlement may exceed the construction deadline.
  Expiry still prevents subsequent process startup, HTTP execution, or Agent
  publication.
- Retain owner tasks/threads until joined or durably recovery-owned.
- Never treat a dropped `JoinHandle` or caller timeout as cleanup.
- Keep test artifacts in Cargo target, crate-owned temporary paths, or OS temp.
- Remove disposable docs/test caches after their gate while preserving
  reusable shared outputs.

## Selected OpenCode Release Regressions

The selected release's generated `packages/sdk/openapi.json` remains primary
HTTP-shape evidence. Source contracts protect its open interleaved field
vocabulary and unchanged session, foreground and interaction envelopes.
Deterministic `opencode_event_ownership` cases exercise arbitrary and empty
interleaved strings, malformed unions and provider indexes, exact connected
projection, configuration-error redaction, and nonmonotonic Assistant identities
in plain, structured and cancelled-origin recovery. Retained-origin negatives
still reject changed session/message/call identities and missing request-part
proof. An intermediate `unknown` finish cannot replace the foreground HTTP
result in unary or streaming operation. These are protocol fixtures, not live
model or service evidence.

Release admission additionally requires the exact selected health version;
scripted old, other, local, malformed, and unhealthy responses remain rejected,
with joined cleanup. Empty and populated native variant maps, including nested
overrides, leave portable effort/personality inventories empty and explicit
selectors unsupported before native mutation. Additive provider reasoning
configuration preserves session creation and its unrestricted permission rule.
Root and nested V2 configuration-error fixtures prove redaction, one read, and
no session creation or repair; they do not execute native configuration lowering.
Source contracts cover the generated optional effort/toggle/token-budget forms,
string-valued HTTP efforts versus nullable native configuration values,
configured variant precedence, and discontinued sharing. No new live workload
is introduced; the updater owns the canonical live attempt and all completion
gates.

The existing OpenCode hierarchy live case also owns generation-free release
checks on an isolated selected process: configured-role inventory with
`reasoning_text` and a custom interleaved field, restrictive V1 defaults with
unrestricted portable session-policy readback, and HTTP 400 rejection of root
and agent-scoped V2 `permissions`, including empty values. It verifies no
session creation or configuration rewrite for those rejected directories.
Inventory proves no generation behavior for arbitrary native fields; policy
readback proves no shell execution. This integration retains the original 28 cases and adds the repaired-abort case,
original generations, configured models, assertions and deadlines. Existing
plain/structured, stream/abort/reopen and Question scenarios remain mandatory;
nonmonotonic identities are not claimed as naturally reproduced live.
### Failure Diagnostics

The Custom live fixture observes the actual supplied Provider and its associated
Model before Agent error normalization. Failure records include the scenario
stage, generation ordinal, elapsed time, provider kind, selected model, shell
execution counters, configured request/connect limits, and bounded typed error
facts. Each cause reports its own typed timeout separately from those configured
limits. Native message inspection emits only
fixed transport indicators; those substring matches are hints, not proof of a
network cause. Raw native messages, bodies, URLs, credentials, headers, and
continuation contents are never printed or stored in Agent state. The observer
preserves the selected Model's associated continuation and delegates requests,
streams, cancellation, and shutdown without retries.

For the owned OpenCode repaired-abort process,
`AGENTS_KIT_OPENCODE_BOUNDARY_DIAGNOSTICS` can name an existing absolute directory
under the workspace's `target/`. The fixture appends a test-only native plugin
to that child's configuration. It observes serialized provider requests and
fetched response bytes before native provider parsing, retaining only bounded
model-match, Question schema, structured-call, finish, and completeness facts.
It does not change the model, tool choice, prompt, transport deadline, or
response bytes. Missing or incomplete records cannot establish absence of a
tool call. Complete text-only provider output still fails the required Question
interaction prerequisite; printed JSON never becomes a Kit interaction.

The pending-reopen observer correlates the unchanged fresh prompt with the
configured model, existing native session header, and user/assistant parent
identities. It reconstructs provider text in order and compares it with the
expected answer and a read-only `experimental.text.complete` checkpoint. The
checkpoint describes this plugin's position in native hook execution, not final
persisted text. Parent-correlated history is read before cleanup on observed
success as well as failure. Neither checkpoint observes the preceding foreground
HTTP response body; receipts explicitly retain that limitation.

The observer retains prompt, answer, native identities and text only in bounded
child memory. Limits are 4 KiB for the prompt, 256 bytes for the expected answer
and 64 KiB for reconstructed text, within the existing request, response, flow
and receipt bounds. Reports contain only comparison, correlation, completeness
and count facts; no raw text, native identities, headers or output hashes.
Ambiguous ownership, unsupported shapes, missing checkpoints, cancellation and
overflow yield incomplete evidence. Diagnostic work stays within the original
deadline and preserves the exact assertion result and bounded cleanup. A
matching native-history snapshot does not prove provider authorship or the body
of the preceding HTTP response. No output is trimmed or otherwise normalized.

These observers diagnose a failed invocation; they do not authorize repeated
model trials or relax any acceptance assertion. Verify changes to the OpenCode
observer with `node --test
agents-kit/tests/strict_live_suite/diagnostics/opencode-boundary.test.mjs`.
Custom observer tests are included in the existing `custom_prompt_runtime`
deterministic target. Preserve private failure receipts separately from the
two mandatory Question receipts and all six model-copy grades.

### Selected Codex Release Evidence

Use the runtime selected by SPECIFICATIONS.md and the isolated startup command
in strict-tests.env.example. Project trust is passed as an inline TOML map with
the dynamic repository root and approvals_reviewer="user". The explicit TOML
approval_policy="untrusted" value and quoted dotted path keys are invalid setup
for this release. Effective thread policy is derived; a null config/read policy
is not failure. Reuse an already-owned correctly configured service.

Codex catalog coverage requires the exact configured model and an empty
personality inventory; an explicit personality must fail before native work.
The dedicated terminal-input case uses its own WorkspaceWrite workspace with
the repository cwd, the same read-write root and enabled network. This ambient
sandbox makes the explicitly elevated launch retain a real sandbox bypass;
FullAccess alone does not establish that precondition. The case launches one
owned elevated native process, completes its launch turn, then sends nonempty
stdin in the next turn. It requires
an actual Terminal input approval, exact callback resolution through the public
API, durable replay, no duplicate Process Running, exact output, terminal
completion and cleanup. Missing approval, incorrect tool choice or timeout fails
without model substitution, skips or repeated model trials.

Existing fresh-thread stream completion, abort and reopen cases exercise the
retained full-read reconciliation path under target native history defaults.
Rare native errors, malformed payloads and deliberately unadvertised capabilities
have pinned source and deterministic-fixture evidence only. Full completion
still requires every configured gate and the complete strict-live verifier.

## Mainline integration boundaries

The integrated inventory adds
`opencode_repaired_abort_releases_generation_and_later_abort_is_idle` to the
28-case PR #21 inventory. It retains one stdin-approval journey and the Lance
foreground-conflict composition, rather than duplicate native executions. The
new case retains the original scenario, prompt, reconciliation, and cleanup
bounds. It exercises public Abort on a retained conversation, not hierarchy
reopen. Historical 28-case receipts do not qualify this integrated candidate.

The selected Codex foreground contract supersedes mainline's passive async-text
expectation: asynchronous delivery or non-null questions remain Unsupported in
live and recovered items. The selected OpenCode reopen contract supersedes
mainline's extra definite-conflict outcome attempt: reopen remains single-attempt,
while public recovered Abort retains bounded generation-fenced recomputation.
The integration retains mainline's exact scope, idempotency, rollback, deadline,
and last-error evidence safeguards without relaxing either acceptance boundary.
