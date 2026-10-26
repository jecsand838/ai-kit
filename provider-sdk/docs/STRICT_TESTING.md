# Strict Testing

Provider validation separates portable contract proof from native provider
proof.

## Primary Direct-Trait Lane

From the workspace root, run:

```bash
bash scripts/check_mock_contracts.sh --crate provider-sdk
bash scripts/check_ci.sh
```

The scoped command runs `mock_contracts` with the crate's default features;
`check_ci.sh` owns the complete workspace primary lane. Structs and exhaustive
caller-owned enums implement Provider, Model, and Session traits directly,
preserving their exact associated children and continuation types. The target
constructs no supplied backend, opens no sockets or backend processes, and
requires no credentials. Dependencies still compile. Scoped results are not
full-suite evidence, and these fixtures do not prove supplied wire behavior or
service compatibility. Follow the [shared mock contract](../../docs/mock-testing.md)
for inventory and runtime qualification. The broader all-feature deterministic
and strict-live gates below remain required.

## Deterministic Gate

Run:

```bash
bash provider-sdk/scripts/check.sh
```

The wrapper owns formatting, clippy with warnings denied, explicit non-live
provider test targets and all features, compile contracts, public behavior,
source contracts, rustdoc/doctests, and the workspace docs-site gate. Its
`strict_live_assertions` target checks the live suite's acceptance rules using
public DTOs. Those deterministic checks prove the assertions, not native
provider behavior. Clippy compiles all targets without executing live tests.

The live target has no ignored tests. Ordinary validation therefore selects
non-live targets explicitly; an unrestricted `cargo test --locked -p provider-sdk`
also selects the live target and requires its real services and configuration.

Deterministic coverage must prove:

- external `ProviderBackend -> ModelBackend -> SessionBackend` associated-child
  implementations used directly without SDK conversion;
- Provider/Model/Session handle trait implementations and ownership bounds;
- non-Clone caller Provider, Model, and Session implementations and exact
  associated children;
- public caller-constructed streams that yield two incremental deltas and an
  authoritative completed function call, preserving a continuation that has
  no Debug, equality, or serialization implementation; exact unchanged
  assistant replay followed by its normalized correlated function result;
- caller stream demand and source drop before polling and after partial
  consumption, without treating these fixtures as native admission proof;
- exact ModelId validation and lookup behavior;
- every associated-continuation mapping path, including success and mapper
  failure without invoking a mapper for opaque provider-file batches;
- function-call/result validation and correlation;
- the portable content matrix, including exact Gemini 3 inline
  function-result media and pre-I/O rejection of Gemini audio and remote
  function-result references;
- preservation of private continuation metadata across request replay;
- OpenAI URL-source projection with exact native API-feed continuation replay,
  and Gemini file-search activity with or without document identifiers in
  generation and streams, projecting only identified file evidence while
  malformed present fields remain errors;
- OpenAI explicit turn cancellation, plus Gemini session opening returning
  `Unsupported` before network I/O and retaining Provider `Closed` precedence;
- built-in Model selection racing shutdown, sealing across existing and cloned
  Models, and Model use after the original Provider handle is dropped;
- concurrent and cancelled shutdown waiters, draining admitted work, and
  terminal-result replay;
- bounded demand-driven streams releasing admission on completion, error, or
  drop, including terminal streams retained without further polling;
- OpenAI Session setup completing across Provider sealing, cancellation
  of pending setup and receive work, and consuming close after sealing or after
  parent handles are dropped;
- peer-initiated setup closure preserving its primary rejection and native
  code/reason while acknowledging the peer's close frame without a spurious
  supplementary cleanup error;
- opaque errors with provider-neutral classifications and operation/phase context;
- native HTTP/SSE error mappings, missing response fields, batch row location,
  request correlation, and local-versus-service deadlines;
- absent versus present-invalid stream fields, valid empty incremental
  content, and local request construction versus malformed remote handshakes;
- preservation of a primary HTTP rejection when collecting its diagnostic body
  fails, with bounded partial detail and a supplementary read failure;
- redaction of every native metadata field in default formatting, bounded UTF-8
  diagnostics, and honest unsupported errors;
- GA Anthropic file pagination without legacy beta headers, bounded upload URL
  validation, upload lifecycle identity, and ProviderFile submission;
- complete render-or-reject coverage for every closed HostedTool variant;
- the absence of factories, conformance owners, LoopDriver, hosted dispatch,
  model slot/capability matrices, implementation identities, erased backend
  adapters, Extension variants, normalization constructors, and compatibility
  APIs.

Minimal downstream implementations may prove the public contract, but they do
not prove built-in provider behavior.

## Public-Surface Review Gate

The error-contract migration deliberately adds opaque remote outcomes and
typed failure evidence, and replaces lossy failed-result payloads directly.
Historical reduction counts do not describe this approved surface. Every
added public signature must be reviewed as validated construction or
modification, borrowed inspection, ownership-preserving conversion, direct
handle behavior, or associated-continuation mapping. Compile contracts must
continue proving exact signatures, privacy, associated children, and rejected
ingress. Do not add a prose method inventory, API hash, rustdoc parser, or
private-topology scanner to enforce the gate.

## Strict-Live Gate

Run:

```bash
source provider-sdk/strict-tests.env && bash provider-sdk/scripts/verify_strict_live.sh
```

Strict-live uses real OpenAI, Anthropic, Gemini, and Ollama services and exact
configured model ids. It must exercise complete Provider-to-Model selection,
generation/streaming, supported embeddings/files/batches/hosted tools,
continuation replay, provider-supported OpenAI interactive behavior,
provider-wide shutdown, and honest unsupported paths. Supplied Gemini interactive
sessions are temporarily unsupported; their former live cases are withdrawn
explicitly, not skipped or counted as passing.

The verifier runs the independent cases for all four providers in one target:

```bash
cargo test --locked -p provider-sdk --test strict_live --all-features -- --nocapture --test-threads=1
```

Setting `PROVIDER_SDK_EVIDENCE_DIR` to an existing private directory also records
usable public generation and batch results before assertion checks can discard
them. These diagnostic sidecars retain text, hosted outcomes and row correlation
without serializing opaque continuation contents. They redact configured API
keys, use new files with private permissions, and cap each observation at 1 MiB
with an explicit truncation marker. Export failures are reported alongside the
original case and cleanup failures; observations never replace SDK operations
or count as passing assertions.

With that evidence directory configured, the ordinary Ollama unary function
continuation also exports its synthetic expected nonce and actual caller-created
`FunctionResult` before submitting the continuation. This required private
observation uses the same credential redaction and 1 MiB bound; export failure
stops continuation and returns through the existing Provider shutdown path.
Other providers and reasoning-streaming continuations are unchanged. A passive
diagnostic may compare this input with native request/response hashes and SDK
exports, but socket forwarding alone does not prove native processing. Missing
correlation or incomplete capture cannot establish an SDK or native defect, and
no diagnostic comparison substitutes for the exact live assertion.

There is no `#[ignore]` or `--ignored` selection. The default full verifier omits
no cases. Only the explicit long-batch toggle described below may exclude its
seven named completion cases below, with a mandatory partial-coverage label.
Each case owns its Provider and cleanup so a failed case does not prevent the harness from
attempting the remaining cases. Generation, streaming, discovery/runtime,
embeddings, function continuation, each supported hosted-tool family, file
lifecycle, batches, and interactive sessions retain their separate evidence.
The canonical verifier first checks required configuration, runs the
deterministic gate, and checks Ollama availability and exact installed models.
Each Ollama prerequisite request has a two-second connection limit and a
ten-second total transfer limit, including response headers and body. These
bounds do not change the SDK live cases' operation deadlines. A timed-out
prerequisite is failed, unexecuted live coverage.
A prerequisite failure prevents the live invocation and must be reported as
unexecuted coverage.

### Generation and hosted execution

Successful text generation uses the exact configured model selection, terminal
`FinishReason::Stop`, nonempty assistant text, and no unexpected function
calls. Error, filtering, truncation, paused generation, or missing terminal
reason cannot satisfy that proof. The function-call turn requires the exact
requested call and arguments; its continuation requires successful terminal
text equal to the supplied result. Streaming checks event ordering, exact
native model identity between Started and Completed, actual incremental text, and one successful authoritative
completion. Thinking, usage, or a `Completed` wrapper alone is insufficient.
Successful completion also applies to hosted output and file-reading responses;
exact nonce equality remains required for ordinary generation, continuations,
and successful text-control batch rows. File-reading preflights use the bounded
answer-field grammar and a content-only unpredictable secret.
Returned batch model versions remain provider-authored identities; tests never
rewrite them to match a selected alias or add a resource prefix.

Missing-model probes use text-only requests with default generation controls,
so advanced-option validation cannot reject an unknown model before the native
request. Missing-model and OpenAI missing-store assertions require `NotFound`, the
exact requested resource, and the scenario's native-code evidence. HTTP
rejections require actual status 404 and `ResponseHeaders`; in-stream failures
require no HTTP error status, `GenerateStream`/`Stream`, and an explicit
recognized nonempty code. An absent status alone is not streaming evidence.
Authentication, quota, transport, timeout, unrelated resources, and inconsistent
error context cannot satisfy these negative cases. Native code, type,
canonical status, reported HTTP status, and RPC code are assessed independently; a recognized
code cannot hide contradictory metadata. An exact structured resource identity
takes precedence over diagnostic token matching. Requested targets and echoed
custom IDs cannot supply that evidence. A conflicting structured
identity fails even when the diagnostic repeats the requested resource.

Negative streams may fail before Started or after incremental text, thinking,
usage, or hosted activity. Progress must follow one Started; any Completed,
duplicate Started, missing rejection, or item after the terminal error fails.
The captured error must still satisfy the intended resource rejection, and
the entire opening, consumption, and EOF wait shares the operation deadline.
The deleted-store observation path permits either a valid completed stream or
one resource error followed by EOF. It retains that error while draining;
an error after completion, any item after an error, or a timeout before EOF
fails structurally and cannot count as native rejection.

Deterministic public-error fixtures test these acceptance rules separately
from wire fixtures that test supplied adapter normalization. Neither proves
which native error a real missing File Search store returns; no native-code
mapping is inferred from a constructed assertion fixture.

Web-search cases across OpenAI, Anthropic, and Gemini, in both unary and
streaming modes, ask hosted research to find the authoritative reserved-domain
documentation for example.com and example.org, then extract the abbreviation
immediately before `-managed Reserved Domains` in its main page title. The
source is [the reserved-domain document](https://www.iana.org/domains/reserved);
this asks for an explicit source field rather than interpretation of the
relationship between a public-service function and its operator. The expected
complete answer remains `IANA`; it is never supplied in the request. The
shared prompt explicitly requests a plain-text answer line without Markdown;
emphasis such as `Answer: **IANA**` remains a failure, even when the explanation
identifies the correct source and abbreviation. Related
institutional answers such as `PTI` or `ICANN` still fail, including when an
explanation mentions `IANA`. Successful generation, native search evidence,
citations, and rejection of mixed failed or incomplete output remain
independently required. The scenario proves live search execution, portable
output and citation handling, and the requested source-field answer; it does
not prove that the answer could not have been recalled without searching.

Hosted checks require provider-authored evidence for the requested operation,
not merely a matching `HostedToolOutput` variant. Web Search and URL Context
need source evidence; File Search needs the provisioned marker and actual
search evidence; Code Execution needs a real successful execution result for
the requested computation. In positive cases, explicit failures and nonzero
execution exit codes fail the case. Optional status, citation, and result fields remain optional:
their absence cannot be rewritten into success, and a provider's separate
in-progress code record must not be confused with its terminal execution
result. File Search preserves omitted versus included-empty results and never
invents document identities. Web search distinguishes consulted source evidence
from the answer's native citation. OpenAI File Search independently reports
omitted results, included-empty results, blank identities, absent citations,
and citation/result identity mismatch. Gemini identity-free grounding remains
valid evidence when its exact undisclosed marker is returned and no status is
invented. File Search requires exclusion of other hosted families except for
the explicit [qualified Gemini streaming coverage](#qualified-gemini-file-search-streaming-coverage)
below. That exception preserves and reports the extra declaration; it does not
prove family exclusion or code execution. Unary calls, other selected models,
and other providers retain the exclusive-family requirement.

Hosted positive prompts request one case-sensitive `Answer: <value>` line,
with citations and optional prose on other lines. The assertion trims ASCII
boundary whitespace and recognizes the field at a physical line start or an
explicit text-part start. It counts all such fields, deduplicating coincident
anchors, and reads each value through the next real newline in the original
concatenated text. Part boundaries do not truncate values or hide extra text;
prefixes and values may span parts. It permits one sentence-final period for the exact
organization or stored marker. Computation values contain ASCII digits only,
parse without overflow, and equal the independently calculated sum of squares
through 100. Expected answers are not supplied as answer instructions.
Missing, duplicate, decorated, or incorrect values fail. This is an explicit
prompt contract, not a provider guarantee about citation placement. The SDK
response is never rewritten, and streamed bytes must still equal completed
text exactly. Completion, answer, tool activity, and source evidence are
assessed independently on the same response.

OpenAI exception fixtures run a child Python process containing the uncaught
`raise RuntimeError` sentinel, capture its actual stderr, print those captured
bytes, then call `check_returncode()`. The child has a ten-second timeout.
Only the exact requested four-statement program and outer ASCII whitespace are
admitted; no fabricated traceback or caught exception qualifies. The native
experiment exposed real logs in unary and streaming, whereas bare top-level
exceptions and same-process stderr routing returned empty outputs. This does
not create a guarantee that OpenAI exposes every top-level exception or a
native Python exit code. Gemini retains its bare Python raise, and Anthropic
retains its native `python3 -c` command with the two admitted quote-equivalent
forms. Their execution requirements remain unchanged.

Failure diagnostics retain the independently requested source, including its
fresh sentinel, alongside the existing observed-response evidence. Agreement
between returned source and logs cannot replace the requested sentinel. This
diagnostic context does not relax source, exception, or native-outcome checks.
When `PROVIDER_SDK_EVIDENCE_DIR` is configured, each code-failure case also
captures its exact fixture prompt, source, sentinel, selected model and mode
before generation through the existing bounded, private evidence owner. This
records intended SDK input, not native request admission or raw wire parity.
Export errors remain failures reported after Provider shutdown.

Native logs must contain a complete exception-summary line with the unique
sentinel. LF/CRLF and complete ANSI SGR colors are supported in a comparison
view; arbitrary controls, extra statements, printed literal tracebacks, and
assistant prose never prove failure. OpenAI code and logs must belong to the
same interpreter item, and the OpenAI negative permits exactly one interpreter
item. Additional execution fails even when one item contains valid evidence;
multiple genuine log entries within that one item remain valid. Anthropic and Gemini normalization pairs native
invocation/result IDs privately when supplied; Gemini permits unambiguous
ordering when IDs are absent. Pending declarations and continued result-only
activity cannot establish a paired live witness. Assertions never reconstruct
associations across records. A completed OpenAI tool-call lifecycle alone is
not exception evidence. See [Python subprocess semantics](https://docs.python.org/3/library/subprocess.html#subprocess.CompletedProcess.check_returncode).
Anthropic requires failed execution with a nonzero native exit and stderr;
Gemini requires `OUTCOME_FAILED`, not a deadline outcome. Their genuine exception
remains observed through later well-formed recovery. Positive computation
uses the same association rules and independently requires successful native
execution output, not just a correct final answer.

Completed search activity and its sources/results must belong to the same
operation. A separate in-progress or failed call cannot lend evidence to an
empty completed call. Native citations remain distinct from consulted sources.
Web actions are assessed as search, open-page, or find-in-page; optional query
fields are not invented. Gemini grounding preserves and correlates native
redirect URIs; this proves grounded research, not a redirect's ultimate origin.
URL cases separately prove the exact IANA target and extract the abbreviation
from its Further Reading link, avoiding the footer's separate PTI/ICANN names.

Private native normalization associates current-response invocation/results.
Pending Anthropic work remains pending; a result from earlier continuation can
remain unattributed and cannot satisfy paired evidence. Gemini execution uses
matching IDs when supplied and unambiguous ordering otherwise. OpenAI logs and
Gemini failure output use combined logs, not fabricated stdout/stderr. Anthropic
success requires its reported zero exit code; OpenAI/Gemini require absent exit
codes. Missing interpreter outputs remain unproven execution.

URL cases use provider-authored requested/retrieved URLs and per-attempt status.
The negative requires an attributed retrieval failure; quota, unsafe/paywall,
invalid-input, service failures, and another URL's rejection do not count.
Status text and retrieval timestamps never become citation excerpts. The negative
uses a unique `https://httpbin.org/status/404` URL. One unauthenticated preflight
requires HTTP 404 without redirects or retries within 60 seconds; it consumes
no body and supplies no provider execution proof. The SDK hosted response must
independently demonstrate failed retrieval of that exact URL. Gemini's negative
prompt requests a concise `RETRIEVAL_FAILED` final answer without rendered
citations. Native research found that explanatory responses could contain
support index 0 with zero grounding chunks, while this concise form retained
real URL failure metadata without malformed grounding. The text token is not
execution proof: exact native URL and retrieval-error assertions remain required.
Wire regressions cover both valid failure metadata and rejection of dangling
indices in unary and streaming. No SDK grounding validation is relaxed.
Reserved `.invalid` names are unsuitable because provider URL policy may reject them before retrieval.

An Anthropic missing-URL normalization error does not by itself establish native
omission or SDK fragment loss. A task-local capture must retain ordered initial
input, JSON-fragment, completed-input, tool/result identity and terminal facts,
then compare them with the successful SDK export or its untruncated failure
diagnostic. Keep capture completeness separate from test success. An exactly
correlated native `invalid_tool_input` still does not prove retrieval of the
requested target. Preserve original failures when an unchanged captured run
passes; that is non-reproduction, not a demonstrated repair. Diagnostic evidence
must remain bounded and redacted, with unchanged forwarding, operation deadlines
and owned cleanup.

Missing-citation diagnosis compares ordered native text-block citations with the
SDK export; search-result URLs cannot substitute for citations. A unary capture
must accommodate documented empty or opaque thinking blocks without retaining
reasoning text or treating them as output. Unprojected terminal metadata leaves
comparison incomplete. A passing live assertion does not repair an incomplete
capture or establish the cause of an earlier failure.

#### Qualified Gemini File Search streaming coverage

The user-approved qualification applies only to `HostedProvider::Gemini`,
`Operation::GenerateStream`, and the exact selected model
`models/gemini-2.5-flash`, in `gemini_file_search_streaming` and
`gemini_deleted_store_consistency_streaming`. The latter includes its owned-store
preflight and post-deletion observation. Eligibility uses the selected model
identity; response aliases, prefix rewriting, other models, unary generation,
and other providers cannot enable the exception.

Alongside genuine FileSearch evidence, at most one extra CodeExecution output
may contain exactly one PYTHON declaration of the complete form
`print(file_search.query("one nonempty literal"))`, with one quoted string
literal argument, optionally named `query`. It must have no execution results, no
lifecycle status, and no response artifacts. Additional code, statements,
arguments, declarations, outputs, actual execution results, status claims, or
other hosted families fail. A matching declaration alone proves neither
retrieval nor execution: successful completion, the correct marker, honest
grounding, and the full stream/EOF contract remain required for qualified
retrieval acceptance.

Preserve the entire supplied SDK response and declaration unchanged. Do not
drop, execute, rewrite, or relabel code as FileSearch, invent an execution
result, or suppress another provider error. This is a private test-coverage
contract change, not an SDK normalization change, provider bug fix, or claim
that the original exclusive-family expectation now succeeds.

Every qualified case retains a mandatory private, credential-redacted,
create-new report containing the full portable response/code and the selected
scope. When the declaration is present, its visible and persisted qualification
distinguishes the accepted retrieval or observation contract from strict
hosted-family exclusion, which remains unmet. The observation retains `contract`,
`strict_file_search_only_requirement_met`,
`unpaired_file_search_declaration_observed`, `code_execution_claimed: false`,
and the full portable `response`. Declaration-bearing qualification records
exclusion as false and declaration observation as true. Owned-store reports
retain these observations in `fixture.file_search_observations`, with
`preflight` or `post_delete` stages; the ordinary positive case also requires
its private report. Required exports use complete bounded evidence, not a truncated prefix;
write/size failures remain case failures after cleanup and shutdown. No raw
HTTP/SSE capture is implied by a portable response report. All native data
already preserved by the SDK remains available through its unchanged returned
response and opaque continuation handling.

In this qualified scope, each encountered `Completed` response also receives a
mandatory private, credential-redacted, complete capture before stream
validation, labeled `retrieval`, `preflight`, or `post_delete`. These captures
remain available when stream assertions fail; they do not establish acceptance,
retrieval, code execution, or erasure. Existing summary reports remain required.
Required capture export errors fail the case after bounded cleanup and shutdown.
This adds evidence preservation without changing acceptance or model eligibility.

Positive retrieval and owned-store preflight still require the correct marker
in the strict `Answer:` field. Post-deletion observation separately records
whether the complete secret appears exactly once as a whole token in assistant
text. With the allowed declaration, genuine grounding, Stop and a valid stream,
this can qualify even when the Answer format fails. Such a response is
conservatively `completed_without_rejection`, with `marker_returned: false`,
`marker_observed: true`, and the original `answer_contract_error` string. Its
original response and unmet-exclusion qualification remain intact. An absent,
wrong, embedded, or duplicate secret cannot qualify a declaration-bearing
response. A correctly formatted marker plus grounding still records
`retrieval_persists`; the existing no-declaration `completed_without_rejection`
outcome remains valid without that combined retrieval proof.

The post-deletion rejection/erasure limitations remain unchanged. An accepted
declaration does not establish code execution,
native resource rejection, or retrieval revocation. All unrelated SDK errors,
malformed/incomplete streams, fixture failures, cleanup failures and shutdown
failures remain fatal. Historical canonical command 73 remains 108 passed and
2 failed under the original exclusion rule; a later qualified pass must not
retroactively replace those failures or claim that original coverage. Focused
command 79 also remains failed under its then-current requirement for an Answer
field after deletion; recording a later whole-token observation does not rewrite
that run.

### Batch proof and resource ownership

OpenAI, Anthropic, and Gemini each run an inline batch with two successful
rows, a mixed batch with one valid row and one deleted-file reference, and an
independent cancellation case. OpenAI and Gemini also run two successful rows
from a freshly created provider-file input. Completed jobs alone do not prove
successful processing: result rows must have the exact expected correlation
ids without omissions, duplicates, or extras. Successful rows must pass the
generation checks; the mixed case requires the expected successful and failed
rows. Submission rejection or whole-batch failure does not prove mixed-row
behavior and must be reported without reclassifying it as success or assuming
an SDK defect.

Gemini batch-fixture uploads require an explicit `ACTIVE` SDK metadata lookup
within 90 seconds; omitted state and `PROCESSING` keep polling that exact
resource. `FAILED` retains the native processing error. Upload and lookup
metadata must match the actual `FileUploadRequest` filename, MIME type, and byte
length, including for JSONL inputs. The resource ID and returned usable HTTPS
input reference must remain unchanged. Matching but incorrect metadata from
both responses cannot replace the request facts. Other providers retain their
legitimate absent-state behavior. This follows the documented
[Gemini file state contract](https://ai.google.dev/api/files#State).

Batch fixtures retain the complete upload/readiness metadata sequence and
expected request metadata in their private ownership export after cleanup.
Export is mandatory when that history exists, uses `PROVIDER_SDK_EVIDENCE_DIR`
or a private workspace directory under `target/provider-sdk-retained-outputs/`,
and fails if the full redacted report exceeds its byte limit or cannot be
written. File contents are not added to this metadata history. Export errors
are combined with primary failures after cleanup; they never suppress cleanup.
An independent native `sha256Hash` comparison is diagnostic research only:
the public `FileObject` does not expose that native field, and such a comparison
cannot replace SDK readiness or generation proof.

Mixed-row setup uploads a small delivery-confirmation record whose confirmation
code is an unpredictable marker stored only in file contents, independent of
filename, display name, prompt, row IDs, and control answers. The prompt asks
for that code in the existing `Answer:` field rather than reproducing the file.
It reads through the same selected SDK model in unary and in one additional
one-row inline batch, retaining the identical generation request and input
reference. Both preflights must succeed before observing the exact file in SDK
listing. It retains both lifecycle ID and original input reference.
After successful SDK deletion, fully paginated SDK listing must establish
absence within 60 seconds. Empty pages with continuation, repeated cursors,
incomplete traversal, errors, and timeout cannot establish absence; a 403
metadata lookup alone proves neither existence nor absence. Gemini generation
retains the uploaded input URI rather than substituting a lifecycle ID.

A bounded independent-client/SDK diagnostic on 2026-09-23 compared both upload
paths with both generation paths using one bare-token fixture and one record
fixture. All four bare-token answers were incorrect; all four record answers
matched their hidden code. Every file passed ACTIVE, metadata, and native-hash
checks, and cleanup succeeded. This supports the record fixture, but does not
establish SDK batch readability, deletion attribution, or mixed-row outcomes.
Those remain requirements of the complete SDK lifecycle case.

Failed-row attribution requires that full setup history, the successful text
control, exact row inventory, a recognized native category, and the exact
owned file ID or original input URI in its diagnostic, with one narrow Gemini
exception: the observed unstructured RPC 7 message `The caller does not have
permission` can establish rejection after that entire controlled fixture history.
It requires `PermissionDenied` classified from RPC 7, complete diagnostics, no
structured details or alternate classification signals, and the exact correlated
failed row. Generic permission failures outside this history cannot qualify.
The SDK does not infer file identities or classify production errors from prose.
Eligible test categories
are OpenAI `file_not_found`, `invalid_file`, `invalid_file_id`, or
`invalid_request_error`; Anthropic `not_found_error` or `invalid_request_error`;
and Gemini `NOT_FOUND`/RPC 5, `INVALID_ARGUMENT`/RPC 3, or resource-specific
`PERMISSION_DENIED`/RPC 7. These are conservative test evidence sets, not an
exhaustive native taxonomy. HTTP 400/403/404 are not RPC row codes. Unknown or
unattributable errors remain row-error coverage unproven, and native responses
must never supply a row or subtype missing from the SDK result.

OpenAI batch completion waits check status immediately and then once per minute,
returning as soon as the job completes or fails, with a maximum of 24 hours per
job. This matches OpenAI's supported [Batch completion window](https://developers.openai.com/api/reference/resources/batches/methods/create);
it is not a fixed sleep or a guarantee of successful row results. OpenAI status
reads use the supplied SDK's bounded retry policy inside the original request
deadline. The fixture adds no retry loop and does not restart either per-job
completion deadline. An error returned after that policy ends the completion
observation and remains primary through bounded cleanup; it is neither a
pending status nor proof of remote job failure. Cleanup status reads use the
same policy within their existing outer bounds; cancellation and deletion
requests retain their existing policy.
Successful cleanup cannot qualify unobserved mixed-row results. Each Anthropic
inline, mixed-row and bounded-results case has one shared 24-hour completion
deadline, informed by Anthropic's [batch processing window](https://platform.claude.com/docs/en/build-with-claude/batch-processing#how-long-does-it-take-for-a-batch-to-process).
Each deadline is captured before fixture setup; the mixed-row case shares its
deadline across the readability-prerequisite and mixed-outcome jobs. Each checks status immediately then every
five seconds and returns on a terminal response. Only read-only status polling
uses this deadline. Effectful setup/submission, result retrieval and cleanup
retain their existing independent bounds and ownership; total case wall time
may therefore exceed 24 hours by those bounded operations. This case-level
completion budget does not guarantee a job finishes. OpenAI mixed-row proof
retains its two separate per-job 24-hour bounds. Gemini completion retains its
30-minute deadlines and five-second polling. Anthropic cancellation, missing
resources and unsupported-input cases keep their existing bounds. No production
request timeout or batch semantics change.

The verifier defaults to `PROVIDER_SDK_RUN_LONG_BATCHES=1`, selecting the full
strict-live suite. Set `PROVIDER_SDK_RUN_LONG_BATCHES=0` to omit exactly the four
OpenAI completion cases (inline, provider-file, mixed-row, and bounded-results)
and the three Anthropic completion cases listed below. All other cases remain
selected, including Anthropic missing-batch handling, unsupported file input and
cancellation, as well as OpenAI cancellation. This mode must visibly
report a **partial** run and name the omissions. Its success never establishes
a full strict-live pass. Only `0` and `1` are accepted. The toggle changes verifier
selection; direct Cargo invocations still execute selected cases normally.

The workspace wrapper requires full mode by default. An explicitly authorized
`bash scripts/verify_workspace_strict.sh --skip-long-provider-batches` run
validates the effective toggle, then sets `0` only in the Provider verifier's
child environment after sourcing its env file. It runs the other four verifiers
unchanged and fails on every remaining gate failure. Its final success receipt
is **QUALIFIED** and names exactly these exclusions:

- `live::batches::openai_inline_batch_completes`
- `live::batches::openai_provider_file_batch_completes`
- `live::batches::openai_batch_preserves_mixed_row_outcomes`
- `live::batches::openai_batch_result_limit_is_enforced`
- `live::batches::anthropic_batch_preserves_mixed_row_outcomes`
- `live::batches::anthropic_batch_result_limit_is_enforced`
- `live::batches::anthropic_inline_batch_completes`

This explicit scope exception does not establish a full Provider strict-live
pass. Invalid or duplicate wrapper arguments fail before env files are sourced;
invalid effective toggle values fail before gates run and are never echoed.

A failed preflight prevents negative submission and does not authorize a
replacement. Cancellation and cleanup use
separate drain bounds: 11 minutes for OpenAI and 3 minutes for Anthropic and Gemini.
Aggregate cleanup is limited to that drain allowance plus three minutes for
artifact discovery and deletion; unresolved resource IDs are reported.
Every status response must retain the submitted job identity. Failed or
expired jobs are not successful completion or cancellation evidence. A job
that completes before cancellation takes effect leaves cancellation unproven;
the result is reported without a replacement submission. Anthropic cancellation
can end in a portable Completed job, so its cancelled row results supply the
cancellation evidence. Polling and cleanup remain bounded and do not hide a
primary operation or assertion failure.

The suite creates fresh batch inputs and ordinary uploaded-file fixtures.
It does not require `TEST_OPENAI_BATCH_FILE_ID` or `TEST_GEMINI_BATCH_FILE_ID`.
OpenAI's native Files API may be used only to create a fresh input with
`purpose=batch`, which the portable upload API does not expose. Gemini batch
inputs and ordinary upload/delete operations use the supplied SDK. Submission,
status, cancellation, result retrieval, and validation exercise the public
supplied Provider and Model implementations.

The Gemini missing-batch case has one additional native setup exception. It
submits a dedicated one-row inline job through the SDK, waits up to 30 minutes
for a terminal state, captures exact owned artifact IDs, then issues one
native `batches.delete` request using the exact server-issued name. The request
uses the exact official origin, no redirects or retries, a 60-second timeout,
and bounded response bytes. Only successful empty/empty-object completion
establishes this prerequisite; failure or UNIMPLEMENTED leaves it unproven.
The three SDK status, bounded-results, and cancellation probes then run
independently and require missing-resource errors. Invented ID grammar and
400/403 are not substitutes. A terminal fixture does not assert successful
row generation, and cancellation alone does not delete a batch.

The exact-ID cleanup ledger distinguishes tracked, terminal, artifacts-captured,
deletion-uncertain, and deleted jobs. State changes before awaiting native
deletion. Deleted or uncertain-deletion jobs must not be cancelled, polled, or
rediscovered during cleanup. Previously captured owned files still use SDK
deletion. Artifact discovery failure prevents deleting the metadata needed
for cleanup, and uncertain deletion remains a reported failure.

Each Gemini deleted-store consistency mode has one further native prerequisite exception:
create a fresh File Search store, capture its exact server-issued name, and
import one tiny file uploaded through the public SDK. Its content-only secret
is independent of the filename, store name, and prompt. Poll only the exact
returned same-store import operation for at most three minutes; native readiness
requires explicit `done: true` without an error. Import acknowledgements can
contain a provisional response object while `done` is absent or false; this
does not admit preflight or replace polling. Invalid identities, malformed
result values, competing result/error objects, and pending errors still fail.
The SDK-uploaded source requires affirmative `Active` readiness, exact original
identity, expected name/MIME/byte length, and its unchanged usable input URI.
Absent state remains pending within the existing 90-second deadline. Actual
upload and lookup metadata are retained in the mandatory private consistency
report, including on validation failure; a mismatched lookup never redirects
cleanup away from the original owned source.
Native readiness is a prerequisite, not retrieval evidence. The same-mode SDK preflight must
return the secret marker with genuine File Search grounding. Empty assistant
answers, ordinary prose, and unexecuted code declarations cannot prove preflight.
Unrelated hosted outputs fail preflight and the post-deletion observation,
except for the exact declared and reported
[Gemini streaming qualification](#qualified-gemini-file-search-streaming-coverage).
Qualifying a declaration never lets it replace the marker or grounding proof.

The two `gemini_deleted_store_consistency_*` cases are explicitly provider
consistency probes, not guaranteed missing-store rejection tests. This is a
user-approved coverage-contract change. Native confirmation on 2026-09-23
returned DELETE 200 and GET 404/NOT_FOUND at 0, 60 and 180 seconds, yet both
modes still retrieved the hidden marker, including after source-file deletion.
The [store API](https://ai.google.dev/api/file-search/file-search-stores#method:-filesearchstores.delete)
does not establish a GenerateContent retrieval-invalidation deadline.

Delete the exact owned populated store with `force=true`, then perform one
bounded GET of that exact name. Require HTTP 404 plus complete native NOT_FOUND
and exact resource attribution; an acknowledgement, generic 403, wrong target,
or malformed response does not establish absence. Delete the separate uploaded
source through the SDK, then make the unchanged SDK request without replaying
its previous assistant response. Do not retry generation or wait arbitrarily
for a preferred outcome. Native calls use the official origin, no redirects or
retries, 60-second bounds and 64 KiB bodies; uncertain mutations are not repeated.
Cleanup still attempts each not-yet-attempted owned deletion after a failure.

Record one of `retrieval_persists`, `completed_without_rejection`, or
`native_rejection_observed`. The first requires the exact marker answer plus
File Search grounding; the second proves neither rejection nor erasure. For
the narrowly qualified streaming declaration, one observed whole secret token
without a valid Answer field remains in the second outcome, with the format
error retained separately from `marker_observed`.
Native rejection still requires complete exact-resource NotFound/NOT_FOUND or
InvalidRequest/INVALID_ARGUMENT evidence. Auth, quota, transport, timeout,
normalization, stream-structure, prerequisite, cleanup and shutdown failures
remain case failures. No result establishes global retrieval erasure.

Every probe writes a private, credential-redacted observation report, including
lookup evidence, timestamps, the public SDK response or error, and cleanup
ownership state. `PROVIDER_SDK_EVIDENCE_DIR` selects the directory; when unset,
a fresh private directory under `target/provider-sdk-consistency-probes/` is
used. The complete redacted report must fit within 1 MiB; an oversized report
fails before creating a file rather than dropping required fields into a
truncated diagnostic prefix. Export failures fail the case. The visible outcome explicitly states that
guaranteed rejection and retrieval erasure remain unproven. A Cargo pass means
the observation/lifecycle contract passed, not that the removed immediate
rejection expectation succeeded. Independent successful File Search coverage
and SDK normalization regressions remain mandatory. Native responses cannot
supply an SDK answer, manufacture a generation error, or repair normalization.

Native batch metadata may identify exact owned hidden input/output/error
artifacts for cleanup only. It must never supply result rows, repair SDK
responses, replace a failed SDK operation, or weaken a failed assertion. If
OpenAI inline submission fails after creating an input but before returning a
job id, bounded paginated file reads may identify the input only through its
exact submission-specific row inventory and model/request structure in the
downloaded JSONL. Finding a preflight input cannot account for another
submission's hidden file. Filename, time, and account
inventory differences are not ownership proof. Ambiguous or undiscoverable
artifacts remain undeleted and are reported as residual resources.
If submission may have been accepted before its job id was lost, cleanup
reports uncertain publication per submission and retains potentially referenced inputs;
an empty local job inventory never proves that no remote job is running.
Submission-error classification releases file dependencies only for local
configuration/input validation, closed admission, request construction, or an
affirmative HTTP400/422 rejection at response headers. Other HTTP4xx responses,
transport failures and malformed successful responses remain uncertain; response
status alone is not proof that no job was created. The original SDK error and
exact source/input ownership survive classification.
This classification follows the supplied adapters' verified phase paths. It is
not an admission guarantee for arbitrary caller implementations of ModelBackend.
Terminal state does not clear unresolved artifact discovery. Each discovery
failure retains its exact originating job/submission and known owned files.

The ordinary source used by the readability prerequisite is bound to that
submission before admission, separately from its JSONL input. Uncertain or
unsettled publication retains both files; deleting the source during a drain
timeout would invalidate an outstanding request and contaminate later evidence.
Exact terminal evidence or definitive submission rejection releases this
dependency. Later settlement and cleanup never retroactively pass a timed-out
case.

Owned file ids and jobs are retained through operation failures, and cleanup
is attempted before reporting the case failure. The original failure remains
primary; cleanup errors are reported alongside it. Provider shutdown does not
replace remote job cancellation or file deletion. The OpenAI batch-input
fixture also proves bounded downloaded bytes and valid JSONL. OpenAI ordinary
upload/read/list/delete is separate from batch-input download proof; native
download rejections retain their HTTP classification. Arbitrary HTTP 400 cannot
prove a purpose restriction. Anthropic input and Gemini media retain their
documented unsupported download behavior.

#### Temporary Gemini generated-output retention exception

The test harness may defer cleanup of an exact owned Gemini batch output after
an SDK deletion attempt returns the known name-length rejection. Eligibility
requires matching native completed-job metadata, its nonempty `endTime`, and
the exact `response.responsesFile` with a valid-character ID longer than 40
characters. An uploaded input, an unknown output, or an unsettled job cannot
qualify. The rejection must be `InvalidRequest` / `DeleteFile` /
`ResponseHeaders`, HTTP 400, native `INVALID_ARGUMENT`, reported HTTP 400, and
the exact recorded `DeleteFileRequest.name` 40-character diagnostic. Truncated
diagnostics, omitted or structured details, alternate native error signals,
causes, supplementary errors, or any other failure remain fatal.

This user-approved cleanup policy preserves every batch behavior assertion and
the production SDK's deletion error. It does not prove immediate deletion.
The output remains in `residual_files` as `awaiting_provider_expiration` and in
`retained_outputs` with its exact identity, owning batch, native completion time,
and original SDK error. Google's [batch results documentation](https://ai.google.dev/gemini-api/docs/batch-api#retrieving-results)
specifies six-week retention before permanent deletion. The evidence records
that expected retention separately from `deleted: false` and
`expiration_verified: false`; no automatic expiration check is implied.

Each deferral emits a visible qualification. Evidence persistence is mandatory:
use `PROVIDER_SDK_EVIDENCE_DIR`, or the harness creates a private directory under
`target/provider-sdk-retained-outputs/` when it is unset. Credentials are redacted,
files are created without overwriting existing evidence, and export failure
fails the case. A green case means its assertions and remaining cleanup passed
under this explicit exception, not that all artifacts were deleted. Removing
the exception requires a supported deletion path or newly verified behavior.

Ordinary upload lifecycle cases use independent nonces for the filename and
the confirmation code in a small delivery-confirmation record. Only file bytes
contain the expected answer; the prompt asks for the code without revealing it.
Generation requires exactly that code, successful completion and no unexpected
tool activity. The original upload identity remains the cleanup target even
when metadata validation fails. Gemini requires affirmative `ACTIVE` readiness
within 90 seconds, matching name, MIME type and byte count, and a usable,
unchanged input URI; omitted state never proves readiness. Other providers may
legitimately expose no processing state.

Upload acknowledgments, complete readiness lookups, generation input and output,
listing/download checkpoints, and deletion outcomes are mandatory private,
credential-redacted evidence. Export failures remain failures after cleanup and
shutdown. With no `PROVIDER_SDK_EVIDENCE_DIR`, ordinary upload and positive File
Search observations use a fresh private directory under workspace
`target/provider-sdk-required-observations/`; files are create-new and bounded
to 1 MiB without truncating required evidence. The fixture correction does not
establish the cause of historical attachment refusals or replace SDK proof.

### Interactive and provisioned evidence

Positive hosted-tool checks inspect every returned hosted family for failed or
incomplete terminal statuses, nested execution failures, and nonzero exit codes.
A successful requested operation cannot hide an unrelated failed operation.
Deliberate code-failure and unreachable-URL cases retain their separate negative
contracts. Stream checks preserve observed incompleteness in the final response
just as they preserve observed failures; ordinary progress may still settle to
successful completion.

OpenAI interactive proof sends one text/function turn followed by one speech turn.
Each completed turn independently requires output audio bytes, nonblank output
transcription and `Completed`. Its model-copy grade separately compares the entire
raw concatenated output transcription with its requested full phrase exactly.
There is no trimming, case folding, substring check, or punctuation tolerance.
The text turn requests exactly one function call with an empty object. Only its
function result contains a fresh pronounceable nonce phrase. That phrase grades
model copying; it does not independently prove native output fidelity. The speech turn requests
`Audio input received.` and requires no function calls. It also requires nonblank
input transcription, which may arrive before or after `Completed` within the same
90-second turn deadline; output transcription is not input evidence. Evidence
from the first turn cannot satisfy the second. A function turn records one
transcript-byte/audio-byte checkpoint only after its correlated result send
succeeds. New audio and nonblank transcription after that checkpoint are required;
pre-tool narration cannot satisfy continuation progress. Missing or repeated
result submission remains fatal. Session close is attempted before reporting a
turn failure. These are public Session observations, not native response identity
or proof that the model semantically used the result.

The nonce phrase is generated only after accepting the expected function call.
Each turn retains a bounded public Session event chronology, exact input/result
correlation, raw transcripts, and separate private PCM sidecars. `send_attempt`
and `send_return` distinguish attempted input from a returned SDK result; neither
proves native acknowledgement. Evidence survives the observation timeout and is
exported only after Session close and shared Provider shutdown have been attempted,
with their outcomes recorded. An export failure fails the case without erasing
its primary failure. The existing `PROVIDER_SDK_EVIDENCE_DIR` or fresh private
`target/provider-sdk-required-observations/` directory owns these create-new files.
JSON remains capped at 1 MiB; each turn permits 8 MiB of PCM per direction,
32 KiB per transcription, and 8,192 chronology entries/384 KiB of encoded entries.
Audio entries contain format, offset, and length; PCM bytes are never expanded
into JSON. Exceeding any bound fails explicitly instead of silently truncating.
The PCM allowance does not promise a full 90 seconds at arbitrary event
fragmentation: the independent chronology bound can fail sooner. Ordinary
controls cover complete 90-second-sized PCM and representative 10-second output
split into 20-ms chunks; neither establishes the live service's chunk cadence.
This is public-boundary evidence, not a native-frame capture or proof of
native-to-SDK parity. Ordinary `session_lifecycle` wire controls separately check
complete function results, exact split transcripts/audio, output before completion
(including a combined final frame), and independently ordered input transcription.

The current-model campaign retains a separate OpenAI interruption case; its
original positive function/audio journey remains independent. OpenAI requests the
finite spoken sequence one through one hundred and sends `CancelTurn` once,
immediately after its first nonempty output audio event. It requires actual
`TurnCancelled`, then one correlated function/result turn with new post-result
audio/transcription, authoritative completion and successful close.
An ordinary completion before cancellation is missing required evidence, not a
pass, and does not by itself identify an SDK defect. There is no sleep, retry, or
extra generation to force the desired outcome.

Gemini interactive audio/session support is temporarily withdrawn by explicit
scope decision. The supplied implementation returns `Unsupported` from
`open_session` before network I/O, including for text-input Live sessions, whose
former implementation requested audio output. Stateless Gemini audio input is
unchanged. The two former Gemini session live cases and their exclusive protocol
fixtures are removed; an ordinary public behavior test verifies rejection and
shutdown precedence. Shared OpenAI assertions, inputs, deadlines and cleanup
remain intact.

The [qualification report](../../storage-sdk/docs/feature-split-qualification.md)
preserves all previous Gemini session results, source identities, diagnostic
limits, packetization experiments and failed native control. Removing support
does not resolve their causes, convert failures into passes, or qualify the
remaining Provider campaign. Restoring this feature requires a new reviewed
implementation and fresh native qualification; historical passing subsets do
not establish current support.

Both the ordinary OpenAI function turn and post-cancellation progress explicitly
prohibit speech before the tool call and while waiting for its result. The entire
output transcription still participates in the separate exact-copy grade, including
any unwanted preamble collected before the expected answer is assigned. The prompts
and all deadlines remain unchanged from the recorded clarification candidate.
This acceptance-policy change does not establish the historical preamble's cause
or statistical reliability, and does not relabel earlier failed receipts.

Only `live::resources::openai_interactive_session` and
`live::resources::openai_interactive_cancellation_and_subsequent_progress` receive
this separation. The ordinary case reports `function_turn` and `audio_turn` copy
grades; the cancellation case reports `post_cancellation_function_turn`. The
interrupted turn's quality is `not_applicable`; incomplete turns are `unavailable`
and cannot qualify. All three completed-turn grades must be booleans. False grades
are visibly **FAILED (nonblocking)**; they do not erase any SDK, evidence or cleanup
failure. Every other Provider assertion remains blocking under its existing policy.

After Session close, Provider shutdown and required private exports succeed, each
case emits one anchored `PROVIDER_SDK_REALTIME_GRADE=<JSON>` receipt. Version 1 has
exactly `version`, `case`, `model_id`, `configured_model`, `sdk_contract` and
`model_exact_copy`. The latter holds exactly the named boolean grades for that
case; `sdk_contract` is true and both model strings equal the configured exact
`TEST_OPENAI_MODEL_INTERACTIVE`. Receipts contain no transcript, expected answer,
audio or native identity. Private exports retain the original full transcripts,
PCM, send chronology, checkpoint and separate outcomes within existing bounds.

The canonical verifier retains its log privately and requires exactly two valid
case receipts and three grades. Missing, malformed, unknown, duplicate,
wrong-model and SDK-failed receipts fail validation. Reporting also runs after
Cargo/log failure; original Cargo status takes precedence, followed by log-capture
and receipt-validation failures. A false copy grade cannot override a failed
process. Existing assertion and process-level orchestration tests validate these
rules without claiming real-service proof. No retry or reliability threshold is
inferred from an observed grade.

`tests/fixtures/input-speech-{16000,24000}.pcm` contain the original utterance
“Please say audio input received.”, synthesized once with
[eSpeak NG 1.52.0](https://github.com/espeak-ng/espeak-ng/releases/tag/1.52.0),
its algorithmic `en-us` voice at 150 words/minute, then converted with
`afconvert` to headerless signed 16-bit little-endian mono PCM. The retained historical 16 kHz
Gemini asset and active 24 kHz OpenAI input total 192,308 bytes (about 2.4 seconds
each). The Gemini asset remains available to interpret historical receipts,
without implying active session coverage.
No third-party recording or voice model is included; the original text and
generated speech are redistributable. eSpeak NG's
[GPLv3 section 2](https://github.com/espeak-ng/espeak-ng/blob/1.52.0/COPYING)
does not extend to output that is not itself covered content. Tests embed the
files directly and require no synthesis or conversion tools at runtime.

`strict_live_assertions` contains deterministic adversarial cases for these
predicates. `wire_behavior` cases prefixed `reaudit_` exercise blocked Gemini
responses, exact-model OpenAI provider-file submission, and OpenAI inline-input
ownership during rejection, cleanup failure, and caller cancellation. They use
public SDK operations against bounded test HTTP fixtures, so they are adapter
regression evidence only. Matching provider files must remain supported without
reupload, and an uncertain creation response never authorizes deletion of a file
that an accepted batch might still need. The dropped-caller fixture separately
releases a definitive rejection or a successful creation after cancellation; it
does not equate dropping a caller with remote rejection. Fixture and provider
cleanup are observed before reporting assertions.

The provider semantics behind these cases are documented in the official
[Gemini generation response](https://ai.google.dev/api/generate-content),
[Gemini safety](https://ai.google.dev/gemini-api/docs/safety-settings),
and [OpenAI Batch](https://developers.openai.com/api/docs/guides/batch) references.
In particular, a blocked prompt may have no candidates, blocked output may omit
content, and batch row bodies select their model.
Filtered-response fixtures distinguish omitted, null, and empty content in
unary and streaming calls. The native Content role is optional, and
[ProtoJSON null semantics](https://protobuf.dev/programming-guides/json/#null-values)
treat a null message field as unset. These exceptions do not accept empty
successful responses, scalar content, malformed parts, or contradictory prompt
and candidate outcomes.

OpenAI and Gemini strict-live require pre-provisioned File Search stores from
`strict-tests.env`. The stores must contain
`ai-kit-provider-strict-file-search-v1`; these two borrowed stores are never
mutated or deleted by the suite. Fresh owned Gemini consistency-probe stores do not
provision or replace this borrowed positive evidence. Missing configuration fails the precheck rather than
weakening or skipping proof.

The exact `TEST_OLLAMA_MODEL_DEFAULT` value must document and support native
tool calling plus replay. Continuation evidence runs only on that configured
model and never guesses support from its name.
The Ollama suite requires an automatic function call with exact arguments and
a continuation reporting the caller's result; forced selection must separately
return `Unsupported` because native chat has no forced-choice control.

The ordinary Ollama streaming case uses the approved literal-token clarification
after complete native capture established exact SDK fidelity for an incorrect
native answer. Its token grammar, selected model, omitted sampling overrides,
one generation, exact equality, stream fidelity and cleanup checks are unchanged.
This is a changed prompt input, not evidence of an SDK text-loss correction or
a waiver for incorrect native output. The penalties/cache case keeps its prompt.

Mocks, recordings, scripted provider servers, payload rewriting, model-id
normalization, and synthetic capability success never satisfy strict-live.

For the error-contract resolution baseline, record semantic red in the agreed
order: error contract, opaque remote outcomes, provider decoding, hosted evidence,
then fixture ownership. Distinguish missing-API compile failures from behavioral
red. Finish the deterministic targets, crate gate, downstream compilation, and
independent diff review before one canonical
verifier invocation. Preserve earlier run artifacts. Report every failure,
timeout, cleanup error, residual artifact, and unproven scenario separately.
Retain opaque errors through assessment and cleanup, including causes,
supplementary failures, classification basis, observed response details, and
exact owned-resource obligations. Ordinary logs retain safe context and trusted
check labels. Explicit bounded sensitive exports use private local artifacts;
arbitrary native strings and bodies remain redacted in ordinary logs. Missing responses
leave dependent checks not evaluated; independent failures on a usable response
are collected before cleanup and shutdown. Operation failures do not by
themselves establish SDK defects.
Set `PROVIDER_SDK_EVIDENCE_DIR` to an existing private run directory to explicitly
export failed-case evidence after cleanup and shutdown. Batch cases also export
a mandatory immutable ownership checkpoint after each accepted submission and
before polling, plus a final inventory after cleanup. These retain exact job/input
identities, source bindings, successful terminal jobs retained by the provider,
submission uncertainty, and remaining files. Without an evidence directory, an
accepted-submission checkpoint uses a fresh private workspace target directory.
Checkpoint export failure ends the operation and still runs bounded cleanup; it
never discards the accepted job from the in-memory ownership inventory. Each export creates
a new file with owner-only permissions; existing files are never overwritten.
Exports retain opaque SDK details, accepted remote failures, exact ownership,
and batch-row correlation. Configured API-key values are redacted before writing.
These artifacts can contain other sensitive native data and are not ordinary logs.
Preserve every failed run. Further remediation requires evidence of the cause,
meaningful failing regressions before production changes, and independent
review. Revalidate affected cases after justified changes; do not retry unchanged
cases until one passes, substitute models, or weaken assertions. Completion
requires a full canonical pass on the final unchanged code. Partial selections
and diagnostic captures cannot replace that proof.

## Public declaration ownership

The source contract checks that unrestricted public declarations occur only in
root `src/*.rs` files or the configuration subtree. Shared `core` helpers and
provider adapters stay crate-private. Compile-fail coverage rejects direct
`core` imports; public API and caller hierarchy tests exercise the supported
functional modules, associated children, and opaque handles. This source rule
protects the public boundary without fixing shared helpers to particular names
or implementation files.

## Withdrawn stateless OpenAI audio coverage

The user withdrew supplied stateless OpenAI audio support while retaining
OpenAI Realtime. The three positive Chat Completions cases are removed from the
current strict-live inventory:

- `live::audio::openai_audio_generation`;
- `live::audio::openai_audio_streaming`;
- `live::audio::openai_audio_function_continuation`.

Their failed qualification, original prompts, speech bytes, assertions, native
captures and cleanup receipts remain historical evidence in the
[qualification report](../../storage-sdk/docs/feature-split-qualification.md).
Withdrawal changes the supported scope; it is not an audio fix or a passing
audio qualification. The required `TEST_OPENAI_MODEL_AUDIO` role is retired.
The shared PCM speech asset remains in use by the unchanged Realtime cases.

Deterministic public-behavior tests now require `Unsupported` before network I/O
for each withdrawn exact audio model's unary/streamed generation, including
text-only history, and for audio-model inline/borrowed-file batches before
resource access. Ordinary Responses audio rejection and provider `Closed`
precedence remain controls. All retained live assertions, operation/shutdown
bounds and the complete long-batch gate remain mandatory and unchanged.

Historical diagnostics remain bounded, private evidence. Complete native
observation does not prove SDK application delivery or turn a failed assertion
into a pass; an absent SDK completion export does not erase native evidence.
The latest streaming capture's exact accumulator parity remains unavailable,
and its original incomplete comparison is preserved. No withdrawn OpenAI audio
or Gemini session case runs as part of the current qualification.

## October 2026 compatibility qualification

Current-model cases extend the existing concrete live owners. The campaign
started with 110 cases and added distinct protocol coverage; the two Gemini
session cases and the three stateless OpenAI audio cases are now explicitly
withdrawn as described above. Retained cases
keep their assertions and deadlines, without multiplying every operation by
every model. The exact executable
inventory must be captured before live execution; renamed/split coverage must
be accounted for explicitly. A current-model smoke result proves that request,
not every setting exposed by that model.

Every generation through the existing owner checks the returned identity for
the eight frozen current OpenAI and Claude targets against their exact selected
canonical ID, including tool-call responses. Response evidence is captured
before this check, and streaming must first pass its existing terminal and
ordering predicates. The current OpenAI catalog lists those same IDs without a
dated alternative; [Claude's versioning contract](https://platform.claude.com/docs/en/about-claude/models/model-ids-and-versions)
defines its 4.6-and-later dateless IDs as fixed snapshots. This rule does not
generalize to older aliases. Gemini's portable response retains the selected
`models/...` identity; that observation does not establish its separate native
`modelVersion`, and the campaign makes no such claim.

The current-model campaign requires `TEST_GEMINI_MODEL_FILE_SEARCH`. The Gemini File Search role remains exactly
`models/gemini-2.5-flash` for its existing restricted exception. Current Gemini
3.8 retrieval has separate blocking evidence; no exception transfers to it.
The Ollama verifier also checks both exact local boolean-thinking tags before
execution. Ollama Cloud execution is excluded by explicit user scope; only
local Ollama is qualified. Deterministic origin validation is not cloud evidence.

The new function-call streaming path requires Started, model consistency,
an authoritative ToolCall terminal and exact calls/arguments. It allows a
native tool-only turn without text deltas; the following final-answer turn
retains all existing incremental-text and exact-output assertions. Deterministic
acceptance tests distinguish this legitimate terminal from ordinary text,
missing Started, model mismatch and native errors.

Full completion includes long batches and does not use the seven-case partial
mode. Slow native completion remains failed/incomplete observation within the
original bounds, not proof by itself of a production defect. Historical failures,
model-specific exceptions and previously withdrawn coverage remain unchanged.

The existing `gpt-6-luna` Minimal generation also explicitly supplies temperature
`0.2` and top-p `0.9`, retaining its original 4,096-token budget, one generation,
exact answer, and deadline. Its Minimal profile maps to native `none`, where
these sampling controls are supported. This proves native acceptance alongside
focused wire mapping; output style does not prove parameter effectiveness.
The current scoped local-Ollama-only inventory is 137 cases: OpenAI 38,
Anthropic 35, Gemini 38, and Ollama 26. In addition to the three stateless OpenAI audio cases above,
the removed Gemini cases are
`live::resources::gemini_interactive_session` and
`live::resources::gemini_interactive_pending_call_interruption_and_subsequent_progress`.
Freeze and verify the exact executable inventory before the full live run. All
retained cases remain mandatory; the seven explicit long-batch exclusions and
their partial-coverage reporting rule are unchanged.


### PR #21 campaign: explicitly authorized batch-receipt reuse

The initial October 21, 2026 authorization stopped a repeated 137-case
invocation and reused 19 verified batch results from
`scoped-live-20261021-165740`. At that point the ordinary Ollama unary
function-continuation evidence sidecar was the only intervening executable test
change. Later reviewed changes included the Realtime prompt and grade split,
shared finish/export path and verifier; that initial comparison does not establish
applicability for those or future changes.

For this explicitly authorized campaign, each resumed candidate requires a
versioned source-impact comparison and reuse manifest binding the original
results and cleanup ledgers to source, lockfile, model selections, environment
and independent review. Preserve previous manifests verbatim. Review every
changed production, shared-helper, test and reporting path; establish that the
recorded batch behavior, assertions, inputs, deadlines and cleanup remain
applicable. Do not merely replace expected hashes. Missing reviews, stale
bindings or unresolved impact stop receipt reuse without authorizing redundant
batch execution. The
[final qualification report](../../storage-sdk/docs/feature-split-qualification.md#final-scoped-qualification--october-23-2026)
records the completed scoped result and its limitations. Preserve the prior
136/1 overall failure and later unchanged Ollama non-reproduction separately.

The task-local invocation retains the canonical verifier's environment,
deterministic, service/model prerequisite and hygiene gates. Its private Cargo
override excludes only the 19 exact `live::batches::` names from fresh execution;
the executable must list 137 cases and the fresh selection must contain exactly
118 nonbatch cases. Each run retains the reviewed candidate's assertions, inputs
and deadlines.
A successful result is **19 reused batch passes plus 118 fresh nonbatch passes**,
never a fresh 137/137 pass. The unchanged verifier's `FULL` configuration banner
must not override this explicit receipt scope. This campaign decision does not
change the canonical full verifier, the seven-case partial mode, or repository
release requirements.

Stop and settle the previous process owner before resumption. Reconcile only its
exact published resources through bounded native cleanup, preserving the
interrupted test result and distinguishing operational cleanup from SDK shutdown.
A successful Gemini file-deletion acknowledgement does not establish independent
lookup absence. Preserve any follow-up lookup failure and the existing exact
Gemini generated-output retention exception without claiming expiration.
