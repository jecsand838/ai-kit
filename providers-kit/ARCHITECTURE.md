# providers-kit Architecture

`providers-kit` models a provider as a resource that selects a model, and a
model as the owner of model-bound operations:

```text
Provider -> Model -> Session
```

It intentionally does not contain a reusable agent loop. Transcript
advancement, tool execution, approvals, shell behavior, generation-retry policy,
durable recovery, and final-response policy belong to the caller that owns those
concerns. Provider owns bounded transport retries for operations explicitly
selected as replay-safe, through its shared client and original request deadline.
OpenAI batch status reads opt in; the lookup used by batch results, batch
mutations, and generation remain fail-fast.

## Public Trait And Private Dispatch Topology

| Public trait | Opaque handle | Private dispatcher |
|---|---|---|
| `ProviderBackend` | `Provider` | `ProviderInner` |
| `ModelBackend` | `Model` | `ModelInner` |
| `SessionBackend` | `Session` | `SessionInner` |

Provider and Model dispatchers contain concrete OpenAI, Anthropic, Gemini, and
Ollama variants. The Session dispatcher contains only OpenAI; sessionless
concrete Models use `Infallible` and retain the default pre-I/O `Unsupported`
path. Concrete owners, dispatchers, and opaque handles implement the same public
trait.

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

`Session` owns only provider-native interactive protocol operations. It is
unique and mutable; ordinary multi-turn generation reuses returned messages
and their opaque continuation. `Provider` and `Model` are cloneable handles to
shared provider runtime.

These lifecycle guarantees belong to the supplied handles. Direct caller
implementations own their trait-method lifecycle and are not wrapped by this
runtime.

Provider shutdown seals all existing Model handles. Admitted work drains
before the one backend shutdown result is published to current and late
waiters. No Model-specific cleanup path competes with Provider shutdown.
Session work is admitted through the same provider runtime, while consuming,
cleanup-capable `Session::close` remains available after the admission seal so
a unique session can always release its own transport state.

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
continuation payload are removed. Public generation types and the independent
Realtime Session owner remain unchanged.

Interactive OpenAI sessions model manual-audio capture and submitted
turns explicitly, bound native frames and outbound payloads, and consume
themselves during close after attempting socket cleanup.
Gemini's supplied Session implementation is withdrawn; its concrete Model uses
the existing `Infallible` session child and default pre-I/O `Unsupported` path.
Stateless Gemini audio rendering remains owned by its generation adapter.

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
- `src/session.rs`: Session trait, opaque handle, private session dispatch, and
  portable interactive configuration and events.
- `src/generation.rs`: public messages, content, function calls/results,
  requests, responses, continuation, and streams.
- `src/hosted.rs`: closed portable hosted declarations and normalized output.
- `src/{batch,embedding,files,runtime,error}.rs`: the corresponding public
  resource values and portable error.
- `src/config/**`: configuration owners, credentials, transport settings, and
  retry policy; configuration is the sole folder-based public API exception.
- `src/core/**`: crate-private shared validation, request views, normalization,
  tool selection, stream framing, and socket behavior used by concrete providers.
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
synthetic conversation session, provider shell runtime, registered Toolset
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
