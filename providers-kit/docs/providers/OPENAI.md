# OpenAI Provider Notes

OpenAI construction uses provider-owned config and credential types. Model
selection uses the exact OpenAI model id through `Provider::model(ModelId)`.

## Model And Continuation

The concrete OpenAI Model owns selected-protocol generation, streaming, model-bound
batch submission, and supported session behavior. Provider-level resources
own file and batch lifecycle operations.

Opening OpenAI Realtime is authoritative only after the server acknowledges
the submitted configuration with `session.updated`; an optional preceding
`session.created` is accepted. The OpenAI Realtime path supports text,
configured raw audio, input and output transcription, function calls/results,
response cancellation, manual or automatic turn detection, and consuming
close.

Realtime function declarations render their own native `type`, `name`,
`description`, and `parameters` fields. The Responses-only `strict` parameter
is not accepted by Realtime. The [Realtime tool guide](https://developers.openai.com/api/docs/guides/realtime-mcp)
documents this function shape and the function-result event flow.

Text input is appended with `conversation.item.create` before `response.create`,
so the user turn and subsequent function results share the session's conversation.
The [Realtime conversation guide](https://developers.openai.com/api/docs/guides/realtime-conversations)
describes this stateful sequence; inline response `input` supplies a custom context.

The two retained Realtime live cases separate blocking Kit/scenario checks from
three raw exact-copy grades. [Realtime instructions](https://developers.openai.com/api/reference/resources/realtime/client-events)
guide model output but are not guaranteed to be followed. Function continuation
must still produce new audio and nonblank transcription after successful result
submission, with exact correlation, authoritative completion and cleanup. Entire
raw transcripts remain unchanged for copying grades, including preambles. A false
grade does not prove a Kit defect or grant semantic-answer quality; public Session
observations do not independently prove native parity. See the exact receipt and
acceptance policy in [strict testing](../STRICT_TESTING.md).

When history is caller-managed, preserve the complete model-authored assistant
Message and its private response/output-item metadata. Function results retain
the exact call id. Do not reconstruct continuation from visible text or expose
raw response items through portable maps.

Provider uploads use the Files API's `user_data` purpose and can be referenced
in Responses inputs. Content-download rejections retain their native HTTP
classification and opaque error details; message wording never changes a
rejection into `Unsupported`. Files that the service permits downloading,
including batch inputs, support downloads bounded by the caller's byte limit.
The [upload reference](https://developers.openai.com/api/reference/typescript/resources/files/methods/create)
and [content reference](https://developers.openai.com/api/reference/typescript/resources/files/methods/content)
document the API but do not list a stable purpose-specific download restriction.
Strict-live separates ordinary upload/read/list/delete from exact-byte and
byte-limit download proof using owned batch inputs. It does not accept an
arbitrary HTTP 400 as purpose-restriction evidence.

## Stateless audio is temporarily unsupported

The supplied OpenAI Model returns `Unsupported` before network I/O for
stateless audio input and the previously routed exact audio models:
`gpt-audio-1.5`, `gpt-audio-mini`, `gpt-audio-mini-2025-12-15`,
`gpt-4o-audio-preview`, and `gpt-4o-audio-preview-2025-06-03`. The model
restriction applies to both unary and streamed generation, including text-only
follow-ups. Audio-model inline and borrowed-file batch submissions also reject
before upload or borrowed-file reads. Existing provider closure takes precedence.
There is no fallback to Responses or another selected model.

This is a temporary supplied-implementation boundary, not a claim that OpenAI
lacks audio support. The official [audio guide](https://developers.openai.com/api/docs/guides/audio-chat-completions)
and [model reference](https://developers.openai.com/api/docs/models/gpt-audio-1.5)
describe native Chat Completions audio, streaming and function calling. The
withdrawn Kit path did not pass its exact live qualification. Its historical
empty-output and argument-formatting failures remain recorded in the
[qualification report](../../../storage-kit/docs/feature-split-qualification.md);
withdrawal neither fixes nor relabels them.

Public `ContentPart::Audio`, Model methods and opaque continuation APIs remain
available. Ordinary OpenAI generation continues through Responses, whose audio
input remains explicitly unsupported. OpenAI Realtime Session audio, tools,
transcription, interruption and consuming close remain supported and unchanged.

## Hosted Tools

OpenAI supports selected closed `HostedTool` families, including web search,
file search, and code execution. The concrete Model validates
model/tool combinations and renders provider-native requests. Provider-native
outputs normalize into the common hosted-output enum without leaking raw
provider payloads.

Web-search action sources expose provider-returned URLs. OpenAI also returns
[named API feeds](https://developers.openai.com/api/docs/guides/tools-web-search#sources).
Native sources with `type: "api"` and a nonblank `name` remain unchanged in the
assistant message's private continuation but do not become portable URL sources.
A feed-only action can therefore have an empty portable sources list. Malformed
URL sources and malformed API feed names still fail normalization.

Client functions are separate: OpenAI returns normalized `FunctionCall`
values, and the caller supplies `FunctionResult` values after external
execution.

Strict-live treats [web citations](https://developers.openai.com/api/docs/guides/tools-web-search)
as normal presentation and checks one explicit answer field independently.
It requires actual source and citation evidence; a correct answer alone is
insufficient. [File Search](https://developers.openai.com/api/docs/guides/tools-file-search)
requires included results and a real citation identifying a returned result,
with separate diagnostics for omitted/empty results and missing/mismatched
citations. [Code Interpreter](https://developers.openai.com/api/docs/guides/tools-code-interpreter)
exception proof pairs admitted source and matching native logs in the same
item. Exactly one interpreter item is admitted, with multiple genuine log
entries allowed inside it; extra execution fails the negative fixture.
It does not require a particular traceback header or invent process exit codes.
The fixture now runs the uncaught sentinel raise in a child Python process,
forwards its actual captured stderr, and propagates failure with
`check_returncode()`. The child timeout is ten seconds. Exact source and
same-item exception logs remain mandatory; the existing ANSI SGR matcher
handles native colored tracebacks. The prompt permits only that explicit
program, not arbitrary imports, recovery or fabricated output. Native unary
and streaming experiments exposed real child exception logs while bare raises
and same-process stderr routing returned empty output. The Kit still preserves
empty/absent outputs honestly and does not infer Python exit codes from a
completed tool-call lifecycle. [Python subprocess contract](https://docs.python.org/3/library/subprocess.html#subprocess.CompletedProcess.check_returncode).


For a missing File Search store, the service can send an in-band Responses
`error` event with type `invalid_request_error`, a null code, and no HTTP
status. The Kit preserves that `InvalidRequest` classification. Strict-live
recognizes the observed complete diagnostic `Vector store with id ['<id>']
not found.` only for the exact fixture ID, GenerateStream/Stream context,
native error-type classification, and untruncated native evidence with no
structured detail records, matching the observed envelope. Generic invalid
requests, caller-supplied identity, additional structured details, auth/quota
errors, and successful completion cannot pass. This narrow
test rule does not make message text a production classification mechanism.
The [streaming error schema](https://developers.openai.com/api/reference/resources/responses/streaming-events#error)
permits a nullable code; it does not guarantee this diagnostic wording.

## Selected-model batch inputs

A borrowed provider-file batch keeps the exact original file ID. Before job
creation, the selected Model downloads at most 200 MiB of actual UTF-8 JSONL and
validates all rows: at most 50,000, unique nonblank `custom_id`, POST to
`/v1/responses`, and `body.model` equal to the selected `ModelId`. Missing,
malformed, duplicate known binding fields, and mismatching rows return
`InvalidInput` before creation; a byte-limit or transport failure remains its
own typed error. Unknown native body options and CRLF are retained because the
file is neither rewritten nor re-uploaded. Borrowed inputs are never deleted by
submission. The Kit byte ceiling is a memory bound; native service limits and
file-purpose eligibility remain authoritative.

Inline JSONL has the same 50,000-row and 200 MiB Kit bounds. A concrete native
submission owner retains the upload through creation settlement, with one
original request deadline across those phases and a separate configured budget
for an authorized cleanup request. It never retries creation. Caller observer
loss before creation causes deletion of the exact input returned by the upload;
observer loss during creation keeps the native request owned until settlement.
Request-construction failure and definitive HTTP 400/422 rejection permit
owned-input cleanup. Transport loss, 408/409/429/5xx, and malformed successful
responses do not establish nonacceptance and retain the input with actionable
diagnostics. Accepted jobs retain their input, including when the caller never
claims the job. Native IDs learned before a later failure remain in bounded
resource records, without invented native resource type or owner fields.

Provider shutdown joins these owners and reports unclaimed outcomes. The
existing error diagnostic bounds still apply: ordinary IDs from all 64 retained
receipts fit the structured-record limit, while unusually long native fields
can be explicitly truncated under the general error contract. Kit payload
budgets measure admitted encoded input, not all temporary renderer/HTTP memory
or caller-owned requests. The [Batch guide](https://developers.openai.com/api/docs/guides/batch)
documents the service's separate 200 MB file limit and 50,000-request limit.

## Strict-Live Batch Fixtures

The supplied OpenAI `batch_status` performs a bounded GET of the exact job,
using the configured `RetryPolicy` within one absolute request deadline.
The existing shared transport retries eligible connection/request failures and
HTTP 408, 409, 429, 500, 502, 503, 504 and 529 responses. Its default is three
retries; zero disables Kit retries. Backoff, `Retry-After` handling and error
precedence follow the [portable transport contract](../PORTABLE_API.md).
Successful-response body-read, decoding and identity-validation failures are
not replayed. Retryable HTTP rejections retain the shared transport's existing
diagnostic and error precedence. Batch result lookup, listing, submission,
cancellation and file operations retain
their existing fail-fast policy. OpenAI's
[official Python client also offers configurable retries](https://developers.openai.com/api/reference/python#retries);
this does not imply identical retry policies or diagnose an earlier reset.
A status-read error does not establish that the accepted remote batch failed
or stopped. Callers retain ownership and must settle their job and files.

Strict-live creates fresh inline and provider-file batches and checks their
Kit result rows separately from cancellation. OpenAI completion checks status
immediately and then once per minute, returning on a terminal response, with
a maximum of 24 hours per job. A status read that still fails after its bounded
policy ends the observation early;
the fixture preserves that failure through owned cleanup. The completion window
does not require suppressing transport errors or retrying until it expires.
Cancellation cleanup retains its separate
11-minute allowance. The existing File Search store remains borrowed and is
never changed or deleted.

Portable uploads use `user_data`, so fresh provider-file batch input creation
has one test-only native exception: uploading JSONL with `purpose=batch`.
Batch submission, status, cancellation, bounded input download, results, and
file deletion still use the supplied Kit implementations. Native job metadata
may identify exact owned hidden input/output/error files for cleanup, but
must never replace Kit result retrieval or hide a failed Kit assertion.

Inline submission creates an input file internally, and the portable BatchJob
does not expose its id. If submission fails before returning a job, cleanup
may identify that input only through the exact submission-specific row IDs,
selected model, and request structure in bounded downloaded JSONL discovered by
paginated file reads. A case nonce alone, a matching filename, or
account inventory difference is insufficient. Unproven ownership is reported
as a residual artifact instead of authorizing deletion.

The mixed-row case retains unary file readability and adds one one-row batch
preflight using the same request and original file reference. Its independent
content-only secret does not appear in filenames or other request metadata.
Hidden input ownership is tracked per submission: a preflight input cannot
account for the later mixed batch's input.

Batch result retrieval reads both `output_file_id` and `error_file_id` under
one aggregate caller byte bound. Failed rows retain opaque `RemoteFailure`
values with separate native code/type, embedded HTTP status, and request ID.
An error-only job is a valid result collection. Partial retrieval or malformed
rows remain outer Kit failures, never a silently incomplete success.

Web actions preserve search, open-page, and find-in-page fields. A consulted
result is not promoted to an answer citation. Interpreter output preserves
combined logs without inventing stdout, stderr, result status, or exit codes;
`incomplete` remains distinct from successful completion. Missing native logs
cannot establish intentional-exception coverage.

## Current exact generation profiles

The October 2026 profiles cover `gpt-6-astra`, `gpt-6.1-sol`, `gpt-6-luna`,
and retained `gpt-6-sol` for existing generation controls, ordinary functions,
structured output and retained hosted tools. Selection never infers capability
from a model-name prefix. Unrecognized model overrides still fail honestly.

Portable Minimal means the lightest available tier: `low` for Astra/Sol6.1,
`none` for Luna/Sol6. Low/Medium/High map directly; Maximum maps to `max`.
Omitted reasoning stays omitted and does not imply `none`. For the current
Luna/Sol6 profiles, explicit sampling is supported only with explicit Minimal
(`none`); Astra/Sol6.1 sampling remains unsupported. Explicit reasoning token
budgets remain unsupported. Earlier valid exact profiles retain their mappings.
The existing five-value public enum cannot select native `xhigh` independently.
See the [current model settings](https://developers.openai.com/api/docs/guides/latest-model).

Realtime qualification selects `gpt-realtime-2.1`. Committed input audio uses
`gpt-transcribe`, the documented [WebSocket transcription path](https://developers.openai.com/api/docs/guides/realtime-transcription),
while preserving manual/VAD boundaries and existing transcription events.
No advanced voice/VAD or additional public session settings are introduced.

New defaults avoid deprecated/retiring GPT-4.1 Nano, GPT-5.4 Nano and old
Realtime selections. Exact older IDs are not silently replaced; service errors
remain authoritative. Historical receipts keep their original model identities.
Retirement dates and access restrictions come from the
[official lifecycle record](https://developers.openai.com/api/docs/deprecations),
not production wall-clock checks.
