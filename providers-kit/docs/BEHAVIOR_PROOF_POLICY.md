# Behavior Proof Policy

providers-kit exposes only behavior supported by a provider's authoritative
documentation and verified by the appropriate test lane.

## Proof Levels

1. Official provider documentation establishes native semantics.
2. Deterministic wire/model tests prove parsing, rendering, validation,
   continuation preservation, lifecycle, and error mapping.
3. Downstream compile/runtime fixtures, including the
   [primary direct-trait mocks](../../docs/mock-testing.md), prove the public
   trait contract without proving supplied wire behavior or service compatibility.
4. Strict-live against real configured services proves built-in behavior.

If a behavior is not documented or proven, return
an `KitError` classified as `ErrorKind::Unsupported`; do not invent a fallback, capability,
status, identifier, or output.

## Model Ownership

Model-bound behavior executes only after exact `Provider::model(ModelId)`
selection. Discovery output is descriptive and never silently selects a
model. There are no default operation slots or capability matrices.

Concrete provider model owners perform rendering and parsing. Shared portable
modules must not branch on private provider/model dispatch families.

## Function Continuation

Provider function calls are client-executed. providers-kit returns normalized
`FunctionCall` values and accepts normalized `FunctionResult` values, but it
does not run tools or own an agent loop.

The full model-authored assistant Message remains in history exactly as
returned. Private continuation metadata is retained and replayed without
public raw-map escape hatches. Call correlation must remain exact; failures
must never be patched with guessed ids or reordered provider parts.

Streaming opens and validates status before returning a demand-driven stream.
Body and frame limits are enforced incrementally, no forwarding task is
spawned, and native data after portable completion is discarded.

## Hosted Tools

Hosted tools are closed request values rendered by the selected model.
Provider-specific option checks, combination rules, and output normalization
remain in the concrete provider/model subtree. Ollama function calling is
client-executed and must not be presented as model-managed hosted execution.

Strict-live separates a hosted answer's explicit `Answer:` field from native
tool evidence and citation presentation. Parsing that field evaluates the
case's prompt contract; it does not rewrite Kit responses.
An answer field may begin at a physical line or explicit text-part start;
its value still extends through the next real newline, across part boundaries.
All fields count, including duplicates and incorrect fields. Ordinary generation,
continuations, and successful batch nonce answers remain exact. Streaming
still requires byte-for-byte agreement between deltas and completed text.
Missing answer formatting or execution evidence is an unproven scenario, not
automatic proof of a Kit defect.

The two OpenAI Realtime cases named in [strict testing](STRICT_TESTING.md)
separate Kit/scenario checks from raw exact-copy quality. Their mandatory checks
exercise real cancellation where requested, exact function correlation,
successful result submission, new audio and nonblank transcription after that
submission, authoritative completion and cleanup. The entire transcript is still
compared with the original expected phrase, without normalization; false copy
grades remain visible but nonblocking. All three completed-turn grades and both
successful Kit receipts are required. This public-boundary evidence does not
independently prove native-to-Kit fidelity or semantic use of the result.
No other generation, continuation, schema, retrieval or batch acceptance rule
changes. Earlier failures are not retrospectively regraded.

Deterministic tests of these predicates use the same private test support as
strict-live. They prove acceptance and rejection rules, never real service
execution. Independent checks report their own failures before owned-resource
cleanup and provider shutdown finish the case.

The user-approved Gemini File Search streaming qualification changes test
coverage only. It is limited to the exact selected `models/gemini-2.5-flash`,
Gemini `GenerateStream`, and the ordinary positive/owned-store consistency
cases. One narrowly matched Python `print(file_search.query(...))` declaration
may accompany proven retrieval, with no results, status, artifacts or other
hosted activity. The complete code and response remain unchanged in required
private evidence, and the case visibly reports that strict family exclusion
is unmet when the declaration is present. It is not execution evidence, a
production fix, or proof of the original exclusion contract. Unary calls,
other models/providers and unmatched activity remain strict failures. Positive
retrieval and preflight still require their strict Answer field. Only the
post-deletion observation separates one exact whole secret token from that
format: qualified code with grounding and the observed secret can be recorded
as `completed_without_rejection`, preserving `marker_returned: false`,
`marker_observed: true` and the Answer-contract error. Missing, wrong, embedded
or duplicate secret tokens do not qualify code. The
exact grammar, eligibility, reporting and retained checks belong to
[strict testing](STRICT_TESTING.md#qualified-gemini-file-search-streaming-coverage).
The current `models/gemini-3.8-flash` File Search role and all six live cases
retain the ordinary exclusive-family assertions; none exercises or inherits
this 2.5-only qualification.

Native fixture exceptions are limited to OpenAI batch-purpose input upload,
metadata reads identifying exact owned hidden cleanup artifacts, a single
Gemini deletion of a freshly Kit-created terminal batch, and create/import/delete
of one owned Gemini File Search store per consistency-probe mode, including
one exact post-deletion GET. The import uses one
Kit-uploaded file, polls only its exact native operation under a three-minute
deadline, and never supplies retrieval proof. Same-mode Kit preflight requires
the undisclosed stored marker, genuine File Search evidence, and no unrelated
hosted activity apart from that explicitly qualified streaming declaration.
Store deletion
uses its exact owned name with the documented document cascade; the separate
uploaded file is deleted through the Kit. These establish external prerequisites
using exact owned identities. All Kit behavior, including the post-deletion observation, executes through
supplied public Kit traits. Native
mutations are bounded and never retried after an uncertain outcome.
Native rows, messages, or metadata must not repair a failed Kit response.

The user-approved Gemini deleted-store cases report provider consistency rather
than guarantee immediate generation rejection. An owned-store GET 404 proves
management absence only. A successful post-deletion answer is explicitly
reported without rejection or erasure credit; only correlated native rejection
can establish that narrower observation. Mandatory private reports and visible
qualifications prevent a Cargo pass from claiming the removed guarantee.
The streaming qualification above does not widen these outcomes or establish
retrieval erasure; it additionally records unmet family exclusion when used.
Prerequisite, Kit protocol, cleanup and shutdown failures remain fatal.
Successful retrieval coverage is retained independently. See
[strict testing](STRICT_TESTING.md) for the exact outcomes and evidence policy.


The temporary Gemini generated-output retention exception changes only test
cleanup acceptance. After a Kit deletion attempt, the exact owned output of a
completed batch may remain awaiting the documented six-week expiration only for
the fully matched 40-character `DeleteFileRequest.name` rejection. All behavioral
assertions and production errors remain unchanged. Persistent private ownership
evidence retains the original error and explicitly unverified deletion; an
export failure or any unrelated cleanup failure still fails the case. See
[strict testing](STRICT_TESTING.md#temporary-gemini-generated-output-retention-exception).

Failed remote execution inside a valid response remains accepted result data
with an opaque `RemoteFailure`. Outer `KitError` preserves request, transport,
schema, and normalization failures. Classification is not resource attribution.
Causes and independent cleanup failures survive until final assessment;
ordinary formatting remains redacted. Serialization of bounded error details
is an explicit sensitive export, not the logging contract.

## Prohibitions

Do not add a reusable loop driver, shell/tool execution, public provider
envelopes, transport-error wrappers, model aliases, capability matrices,
opaque `Any` hosted payloads, public factories/conformance owners, compatibility
facades, hidden retries, or synthetic portable behavior.
Do not add implementation identity, provider provenance, erased backend
adapters, Extension variants, or normalization constructors. Caller backend
types and their associated children remain direct static implementations.
