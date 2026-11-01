# providers-kit Architecture

`providers-kit` models a provider as a resource that selects a model, and a
model as the owner of model-bound operations:

```text
Provider -> Model -> Session
```

The Session portions of this document specify the approved target under
implementation. They establish ownership before the tests-first migration and
do not assert a completed implementation or live qualification.

The crate intentionally does not contain a reusable agent loop. Tool execution,
approvals, shell behavior, generation-retry policy, durable recovery, and
final-response policy belong to the caller. A Conversation Session retains and
advances exact protocol history in response to caller commands; callers may
also continue using stateless messages and continuation directly. Provider owns bounded transport retries for operations explicitly
selected as replay-safe, through its shared client and original request deadline.
OpenAI batch status reads opt in; the lookup used by batch results, batch
mutations, and generation remain fail-fast.

## Public Trait And Private Dispatch Topology

| Public trait | Opaque handle | Private dispatcher |
|---|---|---|
| `ProviderBackend` | `Provider` | `ProviderInner` |
| `ModelBackend` | `Model` | `ModelInner` |
| `SessionBackend` | `Session` | `SessionInner` |
| `session::SessionCommands` | Borrowed command half | Closed command-half dispatcher |
| `session::SessionEvents` | Borrowed event half | Closed event-half dispatcher |

Provider, Model and Session dispatchers contain concrete OpenAI, Anthropic,
Gemini and Ollama variants. Every supplied provider owns its Conversation
implementation; OpenAI and Gemini additionally own independent native Realtime
implementations. Concrete owners, dispatchers and opaque handles implement the
same owning public trait. The borrowed halves obey the same closed dispatch and
opacity rules. A genuinely sessionless caller Model may still use `Infallible`.

`ProviderInner` returns its associated `ModelInner`, which returns its associated
`SessionInner`. The public parent assembles each opaque child once from its
dispatcher and the required shared `ProviderRuntime`. Concrete children retain
their provider-owned protocol behavior.

Caller implementations preserve their concrete types. A caller implements
`ProviderBackend`, returns its own associated `ModelBackend`, and that model
returns its own associated `SessionBackend`. No Kit constructor erases or
normalizes those values into `Provider`, `Model`, or `Session`. An application
that needs heterogeneous runtime selection owns the corresponding exhaustive
three-level enum hierarchy and its trait implementations.

The [primary mock lane](../docs/mock-testing.md) exercises test-local structs
and enums at these direct seams. Supplied wire/model tests and real-service
verification retain separate proof ownership.

## Resource Ownership

`Provider` owns:

- exact model discovery and selection;
- provider file operations;
- batch status, listing, cancellation, and result retrieval;
- provider runtime and health operations;
- admission and shared idempotent shutdown.

`Model` owns:

- exact provider-local `ModelId`;
- one exact associated continuation type;
- generation and streaming;
- embeddings;
- batch submission;
- session opening.

`Session` owns conversational continuity for one exact selected Model. Explicit
Conversation and Realtime profiles select distinct behavior without fallback.
The unique, non-Clone owner lends concrete command/event halves; a pending
receive does not hold the command half hostage, and only the owner can consume
itself for close. `Provider` and `Model` are cloneable handles to shared runtime.

These lifecycle guarantees belong to the supplied handles. Direct caller
implementations own their trait-method lifecycle and are not wrapped by this
runtime.

Provider shutdown seals all existing Model handles. Admitted work drains
before the one backend shutdown result is published to current and late
waiters. No Model-specific cleanup path competes with Provider shutdown.
Session resource registration is separate from active network-work admission.
Idle Sessions hold no generation admission. The existing atomic runtime seal
rejects new openings and generation work, wakes registered Sessions, and settles
accepted commands that never reached network admission. Observation and targeted
cancellation of admitted work remain available, as does consuming close. A
previously sealed parent alone does not make close fail.

One `ProviderRuntime` owns the backend, lifecycle mutex, notifications, and
authoritative phase enum: `Open` owns the admission count, `Closing` owns that
count plus the tracked cleanup task, and `Closed` owns the replayable terminal
result. A shutdown waiter temporarily holds the task's join handle in a lease;
cancellation returns the handle to the runtime for another waiter. The waiter
joins the task before publishing `Closed`.

An admitted operation holds a drop guard through its work, so return, error,
and future cancellation release admission. Returned streams retain their guard
until completion, error, or drop.

OpenAI inline batch submission is the resource-creating exception to cancelling
ordinary request futures. It transfers the existing admission to a concrete
native submission owner tracked by the same ProviderRuntime. The registry
bounds active encoded payloads, task handles, and unclaimed receipts; it is not
a public scheduler or backend abstraction. Registration activates the owner
only after releasing its bookkeeping lock. The publication guard exists before
spawn, so an unpolled or unwinding worker still publishes a local failure and
wakes the observer. Known service resource identities are checkpointed before
any subsequent await. The worker never interprets notification delivery as a
caller claiming its result.

Shutdown first seals new admission and drains all previously admitted work,
including submissions still preparing/registering their native owner. It then
naturally joins every registered task before reporting unclaimed receipts and
running backend shutdown. It does not abort batch tasks to manufacture a drained
state. Receipt claim and shutdown reporting use one private state transition;
shutdown may win against an observer that has not claimed its result. Task
completion drops its admission and payload reservation, so completed owners do
not retain a runtime reference cycle. Remote cleanup still requires a live
Tokio runtime; runtime destruction can only guarantee local terminal publication.

Demand-driven generation streams own their response body, bounded framing,
normalizer, and current event queue directly. They spawn no forwarding task.
OpenAI stateless generation uses Responses. Its selected Model rejects the
withdrawn exact audio-model selections before transport; Responses also rejects
inline audio input. Audio-model batch submission rejects before uploads or
borrowed-file reads. The private Chat Completions route, stream accumulator and
continuation payload are removed. That earlier audio withdrawal did not change
public generation types or the independent Realtime path; the approved Session
refactor now replaces Session ownership while preserving its supported audio
behavior.

### Session ownership and progress

Each concrete provider Session retains its exact Model and immutable validated
configuration. Shared managed mechanics call that concrete Model's existing
generation path and retain authoritative `Message<GenerationContinuation>`
history unchanged. The history owner keeps accepted input/results independently
of abortable worker locals. It commits an authoritative assistant before making
completion or executable tools observable; provisional deltas are observations,
not a reconstructed assistant. `GenerateResponse<()>` may project completed
portable output for observers while the original replay authority stays private.

Managed work has one tracked task per active generation and none while idle.
That task owns request opening, the generation stream and its parser, and a
generation admission through cleanup. Event-half demand grants bounded read
credit; retrying a dropped receive does not grant duplicate credit or lose an
already parsed event. Cancellation, sealing and the overall operation deadline
remain actionable while waiting for credit or output capacity. Completion
updates retained state before event delivery, so a later cancellation cannot
roll it back.

Native OpenAI Realtime and Gemini Live own their protocol mappings beneath their
provider subtrees and reuse the bounded authenticated socket infrastructure.
One tracked native driver may own the socket, partial writes/frames and native
correlation maps so unsolicited activity and commands progress concurrently.
The owner retains bounded command/output queues, outstanding tool obligations,
response outcomes and output segments. No lock spans a network await. Command
acceptance publishes a correlated receipt without awaiting remote writes, and
in-flight state exists before any externally visible write.

The existing ProviderRuntime owns bounded session registration and task/join
tracking, without becoming a provider router or general actor framework. Close
signals work, performs bounded cleanup and joins it; a cancelled close returns
join authority to runtime-owned cleanup. Owner drop signals/aborts local work
without detaching its join handle. Provider shutdown settles and joins registered
session work before backend cleanup, without waiting for an idle conversation,
application tool execution, or normal event draining. Only observed evidence
justifies remote-stop acknowledgement; local release is a distinct guarantee.

Normal queues and aggregate retained history/replay have byte and item bounds.
Reserved bounded control and terminal capacity keeps cancellation and close
actionable under backpressure. Limit failure terminates explicitly; no history
truncation, tool eviction or unbounded priority channel is used. Native response
completion does not imply capture completion, final transcription, playback
completion or resolved tool work. Late events preserve known correlation;
unavailable native attribution remains explicitly absent.

Cancellation, unsubmitted capture discard and playback reconciliation are
separate operations. Stop resolution is independent of reusable, uncertain or
closed continuity. Uncertain continuity forbids further generation mutations;
there is no reconnect/replay recovery mechanism. One fatal terminal observation
identifies unfinished work, and event receive ends only when permanently drained.

## Function Calling Boundary

providers-kit normalizes model-requested client functions to `FunctionCall` and
caller-supplied outputs to `FunctionResult`. It does not execute functions.

The assistant `Message<C>` returned by a model is the continuation authority.
Its `C` is the Model's exact associated continuation type; supplied Models use
the opaque `GenerationContinuation` carrier while its native contents remain
private. The continuation is replayed when the caller appends the message to
history. This preserves provider requirements such as OpenAI output-item
linkage and Gemini thought signatures without exposing native envelopes.

Provider-hosted tools use a separate closed `HostedTool` value. They are
validated and rendered by the selected concrete model and never enter
`tools-kit`.

## Module Ownership

- `src/provider.rs`: Provider trait, opaque handle, lifecycle, private root
  dispatch.
- `src/models.rs`: Model trait/handle, validated `ModelId`, and model discovery.
- `src/session.rs`: Session and borrowed-half traits, opaque handles, thin closed
  dispatch, and portable validated configuration, commands, tokens and events;
  no native protocol state machine.
- `src/generation.rs`: public messages, content, function calls/results,
  requests, responses, continuation, and streams.
- `src/hosted.rs`: closed portable hosted declarations and normalized output.
- `src/{batch,embedding,files,runtime,error}.rs`: the corresponding public
  resource values and portable error.
- `src/config/**`: configuration owners, credentials, transport settings, and
  retry policy; configuration is the sole folder-based public API exception.
- `src/core/**`: crate-private shared validation, request views, normalization,
  tool selection, stream framing, and socket behavior used by concrete providers.
  `core/session` contains only demonstrated shared managed/lifecycle mechanics,
  never another provider-switching router or generation client.
- `src/transport/**`: private HTTP clients, authentication, execution, and framing.
- `src/providers/{openai,anthropic,gemini,ollama}/**`: clients, wire DTOs,
  concrete model/session implementations, rendering, parsing, and native
  behavior.

Public declarations and their public methods are defined in the owning root
source file, except configuration. Functional modules are the downstream import
paths, and `lib.rs` reexports only the primary workflow surface. `core` has no
public declarations or public forwarding surface; its reusable implementation
uses restricted visibility. Public DTOs retain one authoritative definition.

Shared modules contain portable contracts and true shared behavior only.
Concrete provider or model dispatch never moves into shared helpers.

Provider family names remain construction or wire-protocol facts inside their
own implementation subtrees. They are not public runtime identity,
provenance, response, error, or routing fields.

## Deliberately Absent

There is no LoopDriver, hosted-tools dispatcher, durable factory, production
conformance owner, capability matrix, model slot set, runtime inventory,
fabricated conversation continuity, provider shell runtime, registered Toolset
runtime, public transport wrapper, implementation identity, erased backend
adapter, extension variant, normalization constructor, or compatibility facade.

## Error ownership

`error.rs` owns the opaque `KitError` and portable classification/context values.
Concrete provider adapters interpret documented native codes and error envelopes.
The shared HTTP layer supplies status-based classification only when the native
adapter cannot provide a more precise category. Public handle boundaries attach
operation context, including errors emitted while consuming a stream.

Private HTTP response owners carry response correlation through native decoding
and portable normalization. Diagnostic collection has its own outcome: failure
to read or decode error details is supplementary to the original HTTP rejection.
Session cleanup failures are supplementary to the failed opening operation.
Native diagnostics are bounded and never interpolated into default error output.
