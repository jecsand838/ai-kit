# Gemini Provider Notes

Gemini construction uses provider-owned config. Model selection uses the exact
Gemini resource/model id through `Provider::model(ModelId)`; the SDK does not
insert or remove resource prefixes.

## Model And Continuation

The concrete Gemini Model owns generation, streaming, embeddings and model-bound
batch submission. Interactive sessions are currently unsupported.

Current exact generation profiles include `models/gemini-4-argon` and
`models/gemini-3.8-flash`. Both expose `low`, `medium`, and `high` thinking
levels: portable Minimal/Low map to `low`, Medium to `medium`, and
High/Maximum to `high`. Gemini 3.1 Pro uses the same effort mapping; the
retained Gemini 3 Flash preview preserves its native `minimal` tier.
Omitted reasoning stays omitted. Current models reject token-budget and
temperature/top-p/top-k overrides before I/O because these controls cannot
honor the requested behavior. See [thinking](https://ai.google.dev/gemini-api/docs/generate-content/thinking)
and [sampling deprecation](https://ai.google.dev/gemini-api/docs/generate-content/whats-new-gemini-3.6).
The existing `responseMimeType`/`responseJsonSchema` encoding remains listed
in the REST contract; `responseFormat` is a newer alternative, not evidence
that the existing encoding has been removed.

Gemini interactive audio/session support is temporarily withdrawn by explicit
project scope decision. `ModelBackend::open_session` returns the existing
`Unsupported` error before network I/O for every Gemini model and valid
`SessionConfig`, including the former `models/gemini-3.8-live` and
`models/gemini-3.1-flash-live-preview` selections. Calls after Provider shutdown
retain the existing `Closed` admission error. The private Gemini model uses the
same `Infallible` session child as the other sessionless implementations.

The withdrawn implementation always requested audio output, so this restriction
also covers text input through Gemini Live. It does not change stateless
`GenerateContent` audio input, generation, streaming, embeddings or resources.
Public Session types and caller-owned implementations remain available. This
is an SDK support limitation, not a claim that Google's native Live API lacks
audio. The previous manual/automatic audio, transcription, function, metadata
and cancellation tests are withdrawn coverage; their receipts and unresolved
failures remain in the [qualification report](../../../storage-sdk/docs/feature-split-qualification.md).
Reintroducing Gemini sessions requires a separately reviewed implementation and
fresh live qualification; local `Unsupported` coverage is not native success.

Gemini thought signatures are private continuation metadata. Preserve the
complete model-authored assistant Message and replay every signed part exactly
where it was returned. Function results retain exact call ids and ordering.
Never merge, synthesize, expose, or drop signature-bearing parts.

Native [`Content.role`](https://ai.google.dev/api/generate-content#Content) may
be omitted, null, or empty for generated candidate content; a present nonempty
role must be `model`. This applies to actual text, function calls, and signed
continuation parts, not only filtered output. A candidate with a documented
filter finish may omit content or return null/empty content. A successful
candidate still requires actual content, and malformed content remains an error.
Documented account-policy termination `PUP_LIMITED_DISABLED` yields `Error`
and may omit content; `ESCALATION` yields `ContentFilter`. Long-decode
`CONTINUATION` instead returns `Unsupported` during normalization, including
stream and batch normalization. It never becomes successful truncated output
or issues a hidden continuation request. Its token resumes the original
request, not an ordinary assistant replay; see the
[long-decode contract](https://ai.google.dev/gemini-api/docs/generate-content/long-decode).
Prompt blocking is a distinct terminal outcome: it yields `ContentFilter` at
clean EOF, retains a candidate-free usage tail, and rejects candidate output
before or after the block. No answer, role, or signed part is fabricated.

A terminal streaming frame may contain a model-role content object without
new `parts`. Completion retains the earlier parts in order and uses the final
finish reason and usage without duplicating text. The Content `parts` field
follows [ProtoJSON presence and null semantics](https://protobuf.dev/programming-guides/json/):
omission, null, and an empty array mean no parts in that message, consistently
for unary and streamed responses. Malformed part values and inconsistent
stream identities remain errors.

Native candidate `STOP` can occur before further hosted-tool and answer frames
in observed Gemini streams. Portable `Completed` is emitted only after clean
transport EOF, retaining all parts, hosted activity, grounding, and latest
usage. A later candidate requires its own finish; unfinished trailing content
fails with `UnexpectedEnd`. Any candidate after an unsuccessful native finish
is a protocol error, while a candidate-free usage tail preserves that finish.
Every native finish discriminator is validated when received. Late native
errors and malformed data cannot be hidden by an earlier `STOP`. Deferred
normalization failures retain the original finished candidate's SSE data and
event ID, rather than labeling assembled response JSON as native evidence.
Function calls preserve native `Length` and other unsuccessful finishes; only
native `STOP` with calls becomes portable `ToolCall`, consistently with unary
generation. Code declarations remain visible regardless of thought markers.

Unary `generateContent` function results accept text and JSON. Exact supported
Gemini 3 models may also receive inline PNG, JPEG, or WebP images and inline PDF
or plain-text documents. The current exact mappings include
`models/gemini-4-argon`, `models/gemini-3.8-flash`,
`models/gemini-3.1-pro-preview` and `models/gemini-3-flash-preview`; do not infer
support from a model-name prefix or version substring. Audio and remote
`ImageUrl` or `File` references return `Unsupported` before I/O because Gemini
function-response media uses inline data rather than ordinary `fileData`.

## Current-Model Live Qualification Scope

The current-model slice adds five cases and eight generations: one Minimal
unary generation on each of Argon 4 and Flash 3.8; default unary and Maximum
streaming on Flash 3.8; unary and streaming schema output on Flash 3.8; and a
two-generation High-effort streaming function continuation on Flash 3.8.
Every function result reuses the returned call identity and complete assistant
message. Schema cases require the exact one-property JSON object and nonce.
These cases preserve the existing 4,096-token output bound, 180-second
per-generation observation deadline, transport bounds and shutdown owner.
They neither retry model output nor qualify every feature on both models.

Four existing File Search cases (retrieval and deleted-store consistency,
each unary and streaming) explicitly select `TEST_GEMINI_MODEL_FILE_SEARCH`,
which must be `models/gemini-2.5-flash`. Their source-bound compatibility
exception remains attached only to that model. Two additional retrieval
cases select exact `models/gemini-3.8-flash`, one unary and one streaming,
with the ordinary strict File Search evidence and no inherited exception.
All stored-marker, native citation, deletion observation and cleanup
assertions are retained. A current default model cannot silently move the
legacy exception to a different model.

## Files

Gemini file lifecycle ids remain distinct from generation URIs. Resumable
upload locations must be HTTP(S), have a host, and contain no URL user-info;
signed query parameters and cross-origin upload locations remain valid. File
upload, listing, metadata, deletion, and returned generation references are
supported. Uploaded-media byte download remains honestly `Unsupported`.

## Hosted Tools

Gemini supports selected `HostedTool` families including Google Search, URL
Context, File Search, and code execution. The Model owns
combination checks, rendering, and normalization. Provider-authored grounding
and citation data may be normalized; validated SearchEntryPoint presentation
metadata remains private. The SDK must not invent statuses, ids, or failure
text.

Web grounding chunks identify sources, not an executed search. The SDK reports
a `Search` action only for a nonempty native `webSearchQueries` list; otherwise
the action remains absent while results, citations, and optional status remain
unchanged. SearchEntryPoint presentation alone does not establish execution.
This distinction also applies when URL Context metadata accompanies web
grounding; it does not reassign or filter either output family. See the
[grounding response fields](https://ai.google.dev/gemini-api/docs/generate-content/google-search#understanding-the-grounding-response).

File Search [`retrievedContext.uri`](https://ai.google.dev/api/generate-content#RetrievedContext)
is optional. Portable file results and citations include only contexts with an
exact native URI; contexts without one cannot be represented by those
identity-bearing DTOs. The response text and File Search activity remain, with
queries and an included-empty result list when no context has a URI. A present
URI must be a nonblank string. Store names and titles do not replace document
identity, and
[`documentName`](https://googleapis.github.io/js-genai/release_docs/interfaces/types.GroundingChunkRetrievedContext.html#documentname)
belongs to Vertex AI Search rather than the Gemini API.

Successful [`files.delete`](https://ai.google.dev/api/files#method:-files.delete)
returns an empty JSON object. The SDK validates that object before reporting
deletion success; malformed JSON and nonempty objects remain invalid responses.

## Batch Operation Reads

Batch status and result retrieval read the exact native operation name with
GET. These reads, including the lookup after an acknowledged cancellation,
use the configured `RetryPolicy` within the original transport request deadline.
A transient rejection such as HTTP 503 can retry the same operation; malformed
success bodies and mismatched operation identities remain terminal errors.
Model-bound submission and the cancellation POST remain fail-fast. No retry
creates another job or repeats a cancellation write. See Google's
[batch polling contract](https://ai.google.dev/gemini-api/docs/batch-api)
and [transient-error guidance](https://ai.google.dev/gemini-api/docs/troubleshooting#retry-strategy).

Retry counts, delays, and error evidence belong to the existing shared HTTP
transport. The batch adapter owns only operation selection and normalization;
it adds no polling loop, separate deadline, or batch-specific retry owner.

## Strict-Live Evidence

Fresh provider-file batch inputs and ordinary files are uploaded and deleted
through the supplied SDK. Strict-live requires no pre-provisioned batch file
id. Its inline and provider-file completion cases validate two exact successful
rows; a separate mixed case requests one successful row and one deleted-file
failure. Each completion case allows 30 minutes, with a separate 3-minute
cancellation drain allowance and three additional minutes for artifact cleanup. Whole-batch rejection does not prove the intended
mixed-row result. The configured File Search store remains borrowed.

Batch and ordinary upload prerequisites require an explicit SDK `ACTIVE` lookup within
90 seconds. An omitted state is not readiness; native processing failures
remain failures. Both the upload acknowledgment and lookup must match the
requested filename, MIME type, and byte length, retain the exact resource ID,
and preserve the usable returned HTTPS input URI. This applies to plain-text
mixed-row fixtures, ordinary delivery-confirmation records, and `application/jsonl` batch inputs. The complete public
metadata history is mandatory private evidence retained through cleanup; see
the [batch proof policy](../STRICT_TESTING.md#batch-proof-and-resource-ownership).
The native [File schema](https://ai.google.dev/api/files#File) also defines
`sha256Hash`, but the portable `FileObject` does not expose it. Independent
native hash comparisons remain diagnostic research, not strict-live SDK proof
or a new public API guarantee.

Generated batch output IDs are assigned by Google and can exceed the Files API's
40-character ID limit; neither a shorter input filename nor `displayName`
controls the output ID. The harness still attempts SDK deletion, but the exact
known name-length rejection may invoke the temporary
[retention exception](../STRICT_TESTING.md#temporary-gemini-generated-output-retention-exception).
It preserves the completed batch's output identity, native completion time, and
deletion error in mandatory private evidence, with deletion and expiration both
unverified. [Batch results](https://ai.google.dev/gemini-api/docs/batch-api#retrieving-results)
have documented six-week retention. All generation, result, input cleanup, and
shutdown requirements remain in force; the exception does not return synthetic
SDK deletion success or replace provider-file tests with inline batches.

Hosted evidence checks preserve optional fields and separate execution phases.
An executable-code record and a separate completed execution-result record
may describe one computation. Native executableCode has no lifecycle status;
its portable status must remain absent rather than inventing InProgress. A matching code-execution enum alone is
insufficient; a Code Execution live case needs successful result evidence. Missing document
URIs in File Search must not be replaced with store names or guessed ids to
satisfy assertions.

Two File Search streaming cases have a user-approved test-coverage
qualification only for the exact selected `models/gemini-2.5-flash`. Genuine
retrieval with the correct marker, successful finish and valid complete stream
may coexist with one CodeExecution output containing one PYTHON
`print(file_search.query(...))` declaration whose sole argument is a nonempty
quoted string, optionally named `query`. Results, status, artifacts, additional
code or other hosted activity remain fatal. No code is executed, hidden or
relabeled, and production normalization is unchanged. Required private reports
retain the complete portable code/response and visibly distinguish qualified
retrieval from unmet strict family exclusion. They do not imply raw wire
capture, execution, or retrieval erasure. Unary and other selected models keep
the original exclusive-family rule. See the exact
[qualification contract](../STRICT_TESTING.md#qualified-gemini-file-search-streaming-coverage).

Strict-live deleted-store fixtures retain server-issued names. Each unary or
streaming case creates one fresh owned store and natively imports one file
uploaded through the SDK. Only its exact same-store operation is polled, for
at most three minutes. The SDK preflight must retrieve the independent secret
marker and expose genuine File Search grounding in that same mode. Native
code declarations and other unrelated hosted activity fail both preflight and
the post-deletion observation except for that narrowly scoped, fully reported
streaming qualification. It never replaces the marker or grounding requirement.
Native
readiness alone cannot pass preflight. Observed import acknowledgements carry
a provisional response object before `done` is set, despite the generic
Operation schema's result-presence wording. Following the official
[import polling example](https://ai.google.dev/gemini-api/docs/file-search#importing-files),
the fixture keeps polling that operation until explicit `done: true`; a pending
response never establishes readiness, and native errors remain failures.
The exact owned store is deleted with `force=true`, including its documents.
One exact resource lookup must then return HTTP 404 and native NOT_FOUND with
resource attribution. The separate uploaded file is deleted through the SDK
before a second same-mode SDK request. Official-origin native requests have
no redirects/retries, a 60-second deadline and a 64 KiB cap; uncertain mutations
are not repeated. The configured positive store remains borrowed and untouched.

The user-approved `gemini_deleted_store_consistency_*` cases observe provider
consistency rather than require immediate generation rejection. Native probes
on 2026-09-23 still retrieved an undisclosed marker in both modes after confirmed
store absence at 0, 60 and 180 seconds, even after source deletion. The
[delete/get contract](https://ai.google.dev/api/file-search/file-search-stores)
does not specify when GenerateContent must stop retrieving indexed content.
No SDK replay or dropped provider error was present in those native captures.

Probe reports distinguish persistent retrieval, completion without rejection,
and exactly attributed native rejection. For the qualified streaming
declaration only, post-deletion text containing the exact secret once as a
whole token can record `completed_without_rejection` even when its strict
Answer field is invalid. The report keeps `marker_returned: false`,
`marker_observed: true`, the Answer-contract error, and the untouched response.
Missing, wrong, embedded or repeated secret tokens cannot qualify that code;
grounding, Stop, valid streaming and every other qualification restriction
remain required. Ordinary positive and owned-store preflight still require the
strict Answer field. Reports never call a successful answer
rejection evidence or claim retrieval erasure. Lookup/preflight/SDK/cleanup
failures remain fatal, and durable private evidence is mandatory even without
an explicitly configured evidence directory. See the
[strict testing policy](../STRICT_TESTING.md) for this changed proof boundary.

URL-negative prompts request a concise final answer without rendered citations.
Native confirmation retained URL_RETRIEVAL_STATUS_ERROR for the exact target
while avoiding dangling support indices emitted with explanatory prose. The
[GenerateContent grounding contract](https://ai.google.dev/api/generate-content#GroundingSupport)
still governs SDK validation: malformed indices fail, and assistant prose
cannot replace native retrieval evidence. Unary/SSE regressions preserve both
paths. These schemas, rather than the separate Interactions format, govern
this implementation.

Gemini's intentional exception still requires the admitted bare Python raise,
OUTCOME_FAILED and the exact sentinel in native output. A deadline outcome
cannot substitute for the intended exception.

The mixed-row fixture uploads a delivery-confirmation record with an independent
secret confirmation code present only in file contents. It keeps the exact
original file input URI and proves that the selected SDK model can read the code in unary
and in a one-row batch using the same request before deletion, then
requires successful SDK deletion and complete SDK-list absence. A metadata
403 cannot prove absence. [Google's error model](https://google.aip.dev/193)
permits permission checks before existence checks. A failed row may use RPC
3, 5, or 7 (or the corresponding canonical status), but qualifies only with
the complete fixture history and the correct successful control row. Exact
owned-file identity in the diagnostic is sufficient but not universally promised
by the [batch row schema](https://ai.google.dev/api/batch-api#InlinedResponse).
The observed bare RPC 7 message `The caller does not have permission` also
qualifies after that complete proof chain, with RPC-based `PermissionDenied`,
untruncated diagnostics, no omitted or structured details, and no alternate
classification signals. This narrow test observation neither makes generic
permission errors sufficient nor changes SDK classification. HTTP 400/404/403
are not RPC row codes.

In a bounded 2026-09-23 diagnostic crossing SDK and independent-client upload
and generation, all four record reads returned the hidden code while all four
bare-token reads returned incorrect answers. The files had verified ACTIVE
state, matching metadata and native hashes; cleanup succeeded. That evidence
supports the record fixture and its specific question, without proving the
full SDK batch/deletion/mixed-row lifecycle or attributing the earlier wrong
answers to a confirmed SDK or provider defect. Native diagnostics do not
replace those live assertions.

The ordinary upload lifecycle uses the same meaningful record shape, asks for
only its hidden confirmation code, and retains exact-answer equality. It records
the full upload/readiness metadata, supplied-SDK response and deletion outcome
in mandatory private evidence. An earlier attachment-access refusal from the
bare-token fixture remains a failed historical observation; the record-based
fixture is not evidence that its precise native cause has been identified.

The missing-batch fixture uses a server-issued name rather than assuming an
identifier grammar. One narrowly permitted native
[`batches.delete`](https://ai.google.dev/api/batch-api#method:-batches.delete)
request deletes a freshly SDK-created terminal job after exact owned artifacts
have been captured. Status, bounded result retrieval, and cancellation are
then probed independently through the SDK. Native setup cannot supply result
rows or repair SDK errors. Deleted/uncertain-deletion jobs do not trigger
cleanup polling or cancellation; captured files still use SDK deletion.

REST body HTTP codes and batch RPC codes have separate typed namespaces.
HTTP 400 with canonical `FAILED_PRECONDITION` retains `FailedPrecondition` and
`ClassificationBasis::CanonicalStatus`, following Google's documented
[HTTP mapping](https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto#L99-L117).
The actual HTTP status and native details remain available; conflicting canonical
evidence does not override HTTP 401 or 403.
Repeated reason/domain, resource, field, retry, quota, and precondition details
remain explicitly inspectable in opaque failures. Provider-file batch rows
decode their native response/error wrappers; no missing row is supplied by
native fixture inspection.

Grounding preserves native redirect URIs and correlates citations to those
URIs without guessing their destination. URL outcomes remain per returned URL;
status strings are not excerpts. Executable code/result IDs are paired when
present, otherwise ordering must be unambiguous. Declaration status and process
exit codes remain absent. Failure output uses combined logs; a deadline outcome
does not establish the intentional exception.

Generated output deletion failures retain the exact returned file ID and SDK
error. No prefix removal, truncation, alias, or synthetic deletion success is
permitted; unresolved ownership remains available for explicit follow-up.
