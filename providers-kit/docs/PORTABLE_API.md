# Portable API

`providers-kit` exposes one direct-static resource hierarchy:

```text
Provider -> Model -> Session
```

The supplied handles and caller implementations use the same three backend
traits and two subordinate borrowed Session-half traits.
Caller types remain concrete and return their own associated children; the Kit
has no erased ingress, extension arm, or normalization constructor.

This document owns the current Session contract. Qualification is established
separately by recorded validation of the exact source and invocation.

## Contracts

`ProviderBackend` requires `model(ModelId)` and `shutdown(&self)`. Its
associated `Model` implements `ModelBackend`. Provider-level optional methods
are `list_models`, `model_info`, `upload_file`, `list_files`, `file_info`,
`delete_file`, `download_file`, `batch_status`, `cancel_batch`,
`batch_results`, `list_batches`, and `health`; each has an honest
`Unsupported` default. Small requests and resource IDs are owned. Downloads
and batch results require a caller-supplied `NonZeroUsize` byte limit, and
batch results carry the associated Model's exact continuation type.

`ModelBackend` declares both associated children:

```rust
trait ModelBackend {
    type Continuation: Clone + Send + Sync + 'static;
    type Session: SessionBackend;
}
```

It requires `model_id`, `generate`, and `generate_stream`. Optional `embed`,
`submit_batch`, and `open_session` operations default to `Unsupported`.
Generation and batch values use the associated `Continuation`; opening returns
the associated `Session`. A genuinely sessionless caller implementation may use
`std::convert::Infallible` as its `Session`.

`SessionBackend` is the unique conversational-continuity owner. The exact
borrowed-half contract is:

```rust
trait SessionBackend: Send + 'static {
    type Commands<'a>: SessionCommands + 'a
    where
        Self: 'a;
    type Events<'a>: SessionEvents + 'a
    where
        Self: 'a;

    fn id(&self) -> &str;
    fn model_id(&self) -> &ModelId;
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>);
    fn close(self) -> impl Future<Output = Result<(), KitError>> + Send;
}

trait SessionCommands: Send {
    fn submit(
        &mut self,
        command: SessionCommand,
    ) -> impl Future<Output = Result<CommandId, KitError>> + Send + '_;
}

trait SessionEvents: Send {
    fn recv(&mut self) -> impl Future<Output = Option<SessionEvent>> + Send + '_;
}
```

The half traits live in `session`; they are narrow behavioral contracts, not
erased extension entrypoints. Supplied owners and halves are opaque, non-Clone
values over private exhaustive concrete dispatch. A half cannot outlive its
owner or close independently. External implementations supply their own
associated halves and can construct all required tokens, inputs and outcomes
through public validated construction.

### Profiles and configuration

`SessionConfig::new(SessionProfile::Conversation)` selects managed conversation
through the existing generation/continuation path for OpenAI, Anthropic, Gemini
and Ollama. `SessionConfig::new(SessionProfile::Realtime)` selects native OpenAI
Realtime or Gemini Live. Unsupported provider/model/profile combinations fail
explicitly. Selection never follows a model-name substring or changes the exact
selected model, endpoint, transport or profile after rejection.

`SessionConfig` is owned, opaque and validated, with no default profile. Reuse
`GenerationConfig`, `ProviderToolSpec` and existing generation/content values
where their semantics apply. `with_generation(GenerationConfig)` and
`with_tools(Vec<ProviderToolSpec>)` are the canonical configuration modifiers;
borrowed `generation()` and `tools()` expose those existing values, including
`generation().tool_choice()`. There is no separate Session function/tool-choice
schema. Validated instruction, audio and turn-detection modifiers remain.
Every accepted setting is rendered or explicitly
rejected by the owning provider. Configuration is fixed for the Session lifetime;
there is no reconfiguration, export/import, resume or rewind API. A caller Model
may retain the complete value; no aggregate parts wrapper, type alias or
public-field decomposition is added.

### Commands, acceptance and correlation

The command half has one canonical `submit` path for validated user input,
correlated function-result batches, targeted cancellation and supported realtime
controls. It rejects arbitrary assistant-history injection. Input activity
start/end, audio-stream completion, input-buffer discard and playback
reconciliation remain distinct from response cancellation. Raw audio is one
chunk, not an implicit commit or end marker. Unsupported controls fail rather
than invoking a broader native operation under a narrower promise.

Successful submission transfers the command into bounded local ownership and
returns its `CommandId` in the same poll. This receipt is not provider acceptance.
Before acceptance, dropping submission leaves no accepted operation or remote
side effect. After acceptance, the owner settles it independently of the waiter,
without automatic resend. In-flight state precedes the first external write;
partially applied writes require reconciliation or uncertain continuity.

The command variants are `Input(Message<()>)`,
`ToolResults(Vec<SessionToolResult>)`, `Cancel(CancelTarget)`, `StartInput`,
`Audio { media_type, data }`, `EndInput`, `EndAudioStream`, `DiscardInput`, and
`ReconcilePlayback { segment, played }`. `CancelTarget` identifies an accepted
`CommandId` or a `ResponseId`; `played` is a `Duration`. `SessionToolResult::new`
associates a `ToolRequestId` with an already validated `FunctionResult`.
Submission validates the association against the live obligation registry.

Command, response, tool-request and output-segment tokens are distinct opaque
session-scoped values. Their public constructors validate the owner-local
session identity and nonzero sequence; supplied owners check exhaustion,
ownership, stale references and duplicates. Native response/item identities stay
private. Genuine optional function-call native IDs remain optional; local tokens
do not fabricate them.

One managed generation may be active at a time. Unrelated new generation is
rejected, but valid tool results, cancellation and control commands remain
possible. Validate each result batch completely before mutation. Retain accepted
results once, collect the required set and initiate exactly one continuation
generation when complete. This submits caller-supplied results; it never
executes tools, chooses an agent action or runs an autonomous loop.

### Events and continuity

`recv` yields correlated output, validated executable tool requests and their
invalidation, response outcomes, supported audio/transcription/accounting
observations and permanent termination. Post-acceptance command failures remain
observable even if no response was created. `None` means the event stream is
permanently drained, never merely idle. Fatal session failure uses one
authoritative terminal event carrying reason, continuity and unfinished work;
there is no conflicting second receive-error channel.

A response has at most one authoritative final outcome. Success requires
affirmative native terminal evidence. Partial completion, cancellation or
interruption, failure and unresolved work remain distinct; missing/unknown
status, malformed terminal data or premature EOF never become success. A
response ending does not finish input capture, playback, late transcription or
unrelated tool work. Preserve original auxiliary correlation; explicitly absent
native attribution is preferable to assigning the currently active response.
`Interruption { response: Option<ResponseId> }` reports a native interaction or
playback interruption independently of generation completion. It can follow an
already completed response without rewriting its outcome. An interrupted active
generation still receives its one `ResponseFinished` outcome.
`TurnEnded { response: Option<ResponseId> }` reports a native interaction/activity
boundary and never supplies evidence of successful generation. Each retains
the original response when the native mapping supports attribution; otherwise
the absence is explicit. Neither event alone cancels unrelated tool obligations.
Usage identifies its actual scope, retains unknown counts, and replaces
compatible interim snapshots with authoritative totals without summing them.
Transcription observations with `finalized = false` are deltas. A finalized
transcription is the authoritative full value for its original source/segment;
it replaces those interim deltas rather than being appended to them.

Cancellation reports already-settled, acknowledged-stop, locally-stopped or
unconfirmed/failed-stop resolution independently from continuity: reusable,
uncertain or closed. Reusable continuity does not prove remote work stopped or
that no other work remains. Uncertain continuity blocks further generation
mutations while observation and cleanup remain available. Cancellation does not
rewind accepted history, discard capture input or reconcile unplayed audio.
Native acknowledgements must match the target; unrelated errors, EOF and timeout
are not acknowledgements. A completed response wins a later cancellation race.
`ControlApplied` means the supported local/native control send path completed;
it does not independently prove remote application. Only the explicit
`CancellationResolution::Stopped` makes the matching native stop claim.

### Exact managed history and progress

Retain returned authoritative assistant `Message<C>` values unchanged, including
opaque continuation, ordered mixed content and tool-only messages. Accepted
caller inputs/results survive cancellation. Commit completed assistant state
before exposing completion or executable tool calls. Incomplete deltas remain
observations unless the generation path supplies valid authoritative replay.
Managed completion may expose a `GenerateResponse<()>` observation preserving
portable metadata while retaining the original message privately; the projection
is not the replay source and does not make opaque continuation serializable.
Native realtime does not fabricate a `GenerateResponse`.

Managed tools become executable only after authoritative replay state is ready;
native Live tools may require a result before response completion. Maintain a
bounded outstanding-call registry. Validate declaration, arguments, name,
genuine native ID, result content and token state. Retract known-cancelled queued
calls, invalidate delivered calls and reject cancelled results. Invalidation
does not undo a tool side effect the application already performed.

The event half drives bounded managed read demand. A pending receive must coexist
with commands, and dropping it loses neither parser state nor accepted work.
Control submission wakes the pending receiver/driver. Managed Sessions track
work only while a generation is active; native Sessions may track one protocol
driver. Every task has retained cancellation/join ownership. Cancellation,
sealing, deadlines and consuming close work without normal output draining.
Aggregate history/replay, command/event, argument and tool-registry bounds apply
in addition to native frame limits; reserved bounded terminal/control capacity
prevents backpressure from disabling cleanup. No silent truncation or eviction
is permitted.

## Generation and continuation

`Message<C>` has validated role-specific constructors and private fields.
`GenerateRequest<C>` requires a leading text-only instruction prefix followed
by at least one ordinary message. Function calls belong to Assistant messages,
function results belong to Tool messages, and provider thinking replay requires
an Assistant continuation.

The default `C` is opaque `GenerationContinuation`. It retains exact
provider-authored replay state and originating `ModelId`; its native contents
are neither inspectable nor serializable. Callers append the returned assistant
message unchanged. Generic callers can translate continuation ownership with
the public `try_map_continuation` methods on requests, responses, events, batch
submissions, and batch results. Serialization rejects continuation-bearing
messages, and deserialization always creates messages without continuation.
A returned assistant containing only provider-hosted activity can be replayed
through its retained continuation even when it has no visible text or function
calls. An empty caller message without continuation remains invalid.

`GenerationConfig::default()` means no overrides. Its validated modifiers and
borrowed getters cover temperature, top-p, top-k, maximum output tokens, stop
sequences, penalties, seed, and tool choice. Every concrete model either
renders each configured field or rejects it before I/O.

Portable content is limited to text, thinking, image URL, inline image/audio,
inline document, file reference, and function-result JSON. Provider renderers
enforce this input matrix before transport:

| Context | OpenAI | Anthropic | Gemini | Ollama |
|---|---|---|---|---|
| Leading instructions | Text | Text | Text | Text |
| Ordinary user input | Text, ImageUrl, Image, Document, File | Text, ImageUrl, Image, Document, File | Text, ImageUrl, Image, Audio, Document, File | Text, Image |
| Assistant replay | Text, calls, continued Thinking | Same | Same | Same |
| FunctionResult | Text, Json, ImageUrl, Image, Document, File | Text, Json, ImageUrl, Image, Document, File | Text and Json; exact supported Gemini 3 models may additionally accept inline Image or Document | Text, Json |

Gemini multimodal function results are restricted to exact supported model
ids, including `models/gemini-3.1-pro-preview` and
`models/gemini-3-flash-preview`. Inline images are limited to PNG, JPEG, and
WebP; inline documents are limited to PDF and plain text. Audio, `ImageUrl`,
and `File` function-result content are rejected with `Unsupported` before I/O.
Unknown or newly discovered model ids do not inherit Gemini 3 behavior through
prefix or version heuristics. Conversation Sessions reuse this selected-model
validation; native Realtime support is independently validated.

Stateless OpenAI audio is temporarily unsupported by the supplied implementation,
including unary/streamed generation and batch submission on its withdrawn exact
audio-model selections. The public audio content DTO and Model methods remain;
OpenAI Realtime Session audio remains required through the Session ownership
contract. See the [OpenAI boundary](providers/OPENAI.md).

Every unlisted combination returns `Unsupported` or `InvalidInput` before
network I/O. Ordinary JSON message content remains invalid; JSON is reserved
for structured function results.

## Resources

- `models`: opaque `ModelListRequest`, passive `ModelPage` and `ModelSummary`.
- `embedding`: text-only `EmbedRequest` and ordered vector `EmbedResponse`.
- `files`: opaque upload/list requests, lifecycle `FileObject`, and distinct
  generation `input_reference` for providers such as Gemini. Provider-managed
  references retain an optional media type when native rendering needs to
  distinguish images from documents.
- `batch`: inline rows or an opaque provider file, direct `BatchJob`, and
  exhaustive terminal `BatchResult` variants.
- `runtime`: successful health probes contain latency only; probe failure is an
  outer `KitError`.

`delete_file` returns `()`. Providers with deletion response bodies validate
the returned identity and confirmation; Gemini's successful empty response is
authoritative.

## Hosted tools

The closed request vocabulary is:

```rust
enum HostedTool {
    WebSearch,
    UrlContext,
    FileSearch { store_ids: Vec<String> },
    CodeExecution,
}
```

The selected concrete model validates exact support and renders the declaration.
There is no shared capability matrix or provider-specific public option object.
Omitted file-search results remain distinguishable from included-empty results.
File-search results and file citations contain evidence with native document
references. Identity-free grounding cannot enter those identity-bearing values;
it still establishes file-search activity, with any queries and response text.
A collection identifier or display title never substitutes for a document
identifier. `HostedWebSearchAction` distinguishes `Search`, `OpenPage`, and
`FindInPage`; optional native query and URL fields remain optional. Consulted
sources are distinct from native answer citations. API-feed metadata remains
private in the returned message's continuation.

URL activity retains native `requested_url` and `retrieved_url` independently,
one outcome per attempt. Neither comes from the caller's prompt. Citation
snippets contain excerpts, never retrieval status or timestamps. Execution
results use `logs` when native output does not distinguish stdout and stderr;
native process exit codes are present only when actually reported. `Incomplete`
hosted execution is distinct from success and failure.

Native invocation/result links are joined privately within a response.
Pending work remains pending, and result-only continuation activity retains
absent source/action attribution rather than inventing a prior invocation.
Returned native continuation remains unchanged.

## Configuration and lifecycle

`ProviderKind` is a construction choice only. `ProviderConfig::new` takes one
exact kind and one `TransportConfig`; live handles, results, and errors expose no
implementation identity. Transport uses one reused HTTP client, validated
`Duration` timeouts, and replay-safe retries within the original deadline.

The supplied `Provider` seals admission during shutdown, drains admitted work,
runs built-in shutdown once, joins the tracked owner, and replays the exact
terminal result. Cancelling an individual waiter does not detach cleanup.
Returned streams retain admission until completion, error, or drop. Session
resource tracking is separate from generation admission: idle Sessions hold no
generation admission. Sealing rejects new work and wakes registered Sessions;
accepted-but-unstarted commands settle explicitly. Admitted work remains
observable and cancellable. Consuming close remains available after sealing and
performs bounded local cleanup and joins; a previously sealed parent alone is
not a close failure. Close does not wait for ordinary output consumption or
application tool execution, and local cleanup does not claim remote-stop proof.

Supplied OpenAI inline batch submissions transfer the existing admission to a
tracked private owner. Dropping the submission observer does not cancel an
upload or a creation request already in progress. Before creation starts, an
abandoned submission deletes its known internally uploaded input; after
creation starts, only definitive rejection permits deletion. Uncertain or
accepted creation retains the input. Cleanup errors remain supplementary to
the original failure. Borrowed provider files are never deleted by submission.

Claiming a submitted result and reporting it during shutdown share one atomic
receipt. If shutdown reports an unclaimed outcome first, a surviving observer
receives that reported failure, not a later conflicting success. Unclaimed
accepted or uncertain submissions make shutdown fail with the exact known
native file/job identities in bounded resource diagnostics; repeated shutdown
returns the same terminal result. A successful notification is not receipt
consumption. Destroying the task's Tokio runtime publishes a local worker
failure and wakes its observer, but cannot guarantee remote cleanup after
the runtime disappears.

Inline admission is bounded per supplied Provider: 16 unsettled owners,
400 MiB of admitted encoded input, and 64 unclaimed receipts. Capacity exhaustion
returns `InvalidState` before upload. Completed work releases its payload
budget; claimed receipts can be reclaimed for later admission. Each input is
limited to 50,000 rows and 200 MiB before upload. These Kit memory/admission
bounds do not replace the native service's documented limits.

## Public modules

Primary handles, traits, `ModelId`, `ProviderConfig`, and `KitError` are at the
crate root. Supporting types live in `config`, `generation`, `embedding`,
`hosted`, `files`, `batch`, `models`, `session`, `runtime`, and `error`.

## Errors

`KitError` is an opaque, cloneable, nonserializable runtime error owner.
Supplied providers and caller implementations return the same contract through
the backend traits, command submission and structured event outcomes. `ErrorKind` is the sole portable classification
vocabulary. `RemoteFailure` is an opaque serializable remote outcome, with
`kind()`, `classification_basis()`, and explicit `native_details()` inspection.
The supporting error types remain in the `error` module.

`BatchResult::Failed { custom_id, error }` and `HostedToolStatus::Failed { error }`
contain `RemoteFailure`. `BatchStatus::Failed { errors }` preserves every native
job error; an empty collection means failed status without supplied diagnostic
records. These remain successful Kit calls returning failed remote work.
Transport, rejection, decoding, or normalization failure remains outer
`Err(KitError)`. An incorrect answer is not a production error classification.

`RemoteFailure::new(kind, details)` records `ClassificationBasis::Explicit` and
rejects local-only configuration, input, transport, timeout, lifecycle, and
response-violation categories. Native `Unsupported` remains valid. Private
adapters record HTTP status, native code/type/reason/outcome, canonical status,
or RPC code as the classification basis. Unknown unclassified remote failures
remain `ProviderRejected`. No basis promises retry safety or authenticates a
deserialized error's origin.

Use `kind()` for portable branching and `context()` for the enclosing public
operation, failure phase, and optional zero-based logical batch row (blank
JSONL lines do not count). `InvalidResponse` carries an explicit reason such as
`MissingField { field: "modelVersion" }`, `InvalidField`, `UnknownVariant`,
`IdentityMismatch`, `UnexpectedEnd`, or a byte limit. The detector assigns the
reason; consumers do not derive it from messages. Static field and rule labels
must contain only trusted schema descriptions.

Builtin local validation attaches `ValidationViolation` through
`validation_violation()`: `MissingField`, `Empty`, `InvalidFormat`, `InvalidValue`,
`OutOfRange`, `Duplicate`, `LimitExceeded`, `Incompatible`, or `Unsupported`.
Each variant carries a trusted static `field`; format/value/range/unsupported
variants also carry a static `rule`, limits carry a numeric `limit`, and
incompatibility carries a static `other` field. Labels identify the rejected
input and constraint, never caller values. For example, invalid temperature and
`top_p` remain separately identifiable without exposing their actual values.
Caller implementations may attach the same contract with
`with_validation_violation`. Configuration construction or transport failures
that do not reject an input invariant do not invent a validation violation.

Explicit required-field checks in stream decoding distinguish an absent key
(`MissingField`) from a present null, wrong type, or invalid value
(`InvalidField`). A malformed parent
object identifies the parent field; an unknown string discriminator remains
`UnknownVariant`. Valid empty text and partial-JSON fragments remain valid.

`Display` and `Debug` include only this typed context and numeric metadata.
Native messages, bodies, codes, types, parameters, request and event IDs, URLs,
and retry headers are potentially sensitive and require explicit
`native_details()` access. Native service code, error type, envelope/event type,
outcome, and canonical status remain separate. `NativeDetail` preserves repeated
reason/domain, resource, field, quota, precondition, and retry information.
Unknown information remains in a bounded error fragment rather than a public
JSON map. Requested targets and caller row correlation have separate explicit
accessors; neither is provider-authored resource attribution.

`ResponseDetails` retains observed JSON category/location, field types,
discriminator, identities, referenced index/count, or size. It does not put
observed values into trusted static `ResponseViolation` labels.

Capture bounds are 64 KiB per string/body, 256 repeated detail records, and
1 MiB retained strings across the entire runtime error tree. Trees retain at
most 32 errors including the root and eight levels including the root at depth
one. Retention preserves the primary root, then its cause subtree, then
supplementary failures in insertion order. Each retained node keeps classification
and context; UTF-8 strings are truncated only at character boundaries.
`diagnostics_truncated()` reports any string, record, depth, or node truncation
in the retained tree, including prior truncation. A child subtree is omitted if
its remaining budget cannot preserve valid remote classification evidence;
retained `RemoteFailure` values continue to satisfy their serialization contract.
Classifying evidence has
priority over other strings within each failure. Capture marks truncation and omitted records;
public remote construction and deserialization reject over-limit values.
Remote serialization preserves every retained field and truncation marker and
is an intentional sensitive export. Default formatting, including enclosing
result DTOs and error chains, remains redacted. No provider identity is exposed.

`http_status()` retains an actual unsuccessful enclosing HTTP response status.
Native `reported_http_status`, `rpc_code`, and `execution_exit_code` are distinct
numeric namespaces. The HTTP value used for classification is separately
retained in `ClassificationBasis::HttpStatus`. An embedded row status never
becomes an outer download status. A native stream or session error does not
acquire a synthetic HTTP status. A failed
diagnostic-body read cannot replace a known service rejection: partial native
details remain attached and `supplementary()` retains the secondary failure.
Cleanup can be retained the same way, with `ErrorPhase::Cleanup`.
`source()` exposes an explicit redacted cause, otherwise an attached remote
failure. Diagnostic-read and cleanup errors are supplementary, not causes.
Each child's operation and phase remain intact.

The current backoff policy is deterministic and has no jitter. `Retry-After`
is honored only as integer delta-seconds on HTTP 429; HTTP-date values and
`Retry-After` on other statuses currently use the configured backoff instead.
These shared-transport limitations are independent of which operations opt
into replay-safe execution.

Only already retryable operations use retry policy; generation remains
fail-fast. A rejected response is classified and its bounded diagnostics are
captured before retry backoff. If the backoff cannot fit while the logical
deadline still has time, the current rejection or transport failure is returned.
A connection timeout remains an actual `Timeout` even when the logical request
budget has time left. When policy exhaustion or the remaining backoff budget
prevents another attempt, that timeout keeps its genuine transport cause and the
latest authoritative rejection as supplementary evidence; declining a retry does
not replace it with a synthetic logical-deadline error.
An actually expired deadline remains `Timeout`, preserves its genuine cause,
and retains the most recent authoritative rejection as supplementary evidence.
Retries retain at most one prior rejection, not an attempt history. A body-read
timeout after rejection headers keeps that rejection primary and the read
timeout supplementary. For operations that consume a successful response body,
the latest rejection remains available until body consumption finishes: a body
timeout retains it as supplementary evidence alongside the actual read failure.
Successful completion, decoding failures, and other non-timeout body failures
discard that earlier rejection. This does not restart the request deadline or
authorize replay of a body read. Operations whose existing success condition
is receipt of headers, such as Anthropic health, continue to complete without
consuming the body.

Local WebSocket request construction failures are
`Configuration`/`RequestBuild`. Malformed HTTP received during the WebSocket
handshake is `InvalidResponse(ProtocolViolation)`/`SessionHandshake`, without
an invented HTTP status. Valid HTTP rejections retain their actual status and
native classification. Peer closure during supplied OpenAI Session receive has
`SessionReceive` phase; shutdown during setup has `SessionHandshake` phase.
Local already-closed admission remains `Lifecycle`. JSON decoding failures
retain their actual parser category and location independently of diagnostic
prose, including unary normalization and text/binary session messages.

The supplied adapters apply these provider contracts:

| Provider | Mapping and retained detail | Authority |
| --- | --- | --- |
| OpenAI | Specific native codes distinguish quota, rate limits, authentication and resource errors. Generic `invalid_request_error` does not override a more precise HTTP status. Type, code, parameter and request ID remain separate. Responses and Realtime error events retain native event correlation without inventing HTTP status. | [Error codes](https://developers.openai.com/api/docs/guides/error-codes), [Realtime errors](https://platform.openai.com/docs/api-reference/realtime-server-events) |
| Anthropic | Native error types map to authentication, permission, missing resource, rate limit, billing, conflict, overload and internal failure categories. Request IDs and SSE error bodies are retained. | [API errors](https://platform.claude.com/docs/en/api/errors) |
| Gemini | Google REST canonical status names are classified separately from HTTP numeric codes and Google RPC numeric codes. Unknown native detail remains in the bounded body; no numeric RPC code is treated as an HTTP status. | [Google error model](https://google.aip.dev/193), [machine error reasons](https://github.com/googleapis/googleapis/blob/master/google/api/error_reason.proto), [Batch API](https://ai.google.dev/api/batch-api) |
| Ollama | Opening HTTP errors use their status. NDJSON native error strings have no documented machine code and remain `ProviderRejected` rather than receiving a classification guessed from prose. | [API errors](https://docs.ollama.com/api/errors) |

HTTP 429 without more specific native evidence is `ResourceExhausted`.
Unrecognized errors without an authoritative HTTP category remain
`ProviderRejected`. A generic native wrapper does not override precise HTTP
status. A documented finer machine code can refine broad HTTP classification.
For example, HTTP 400 with canonical `FAILED_PRECONDITION` retains
`FailedPrecondition` with `ClassificationBasis::CanonicalStatus`.
Contradictory specific evidence cannot turn authentication into missing-resource
proof. All native facts remain retained. Local request validation,
transport failure, local `Timeout`, service `DeadlineExceeded`, closure, and invalid response remain distinct from
service rejection. Batch row failures and hosted execution outcomes returned by
a successful Kit operation remain ordinary result data.

Migration from enum-pattern matching uses `error.kind()` and the metadata
getters. Caller backend implementations construct errors with `KitError::new`
and attach explicit context or native details. The previous error variants are
not retained as aliases or compatibility constructors.
