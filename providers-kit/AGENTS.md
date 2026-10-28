# providers-kit AGENTS

This file adds provider-specific rules to the workspace root
[`AGENTS.md`](../AGENTS.md).

## Scope

`providers-kit` owns portable provider resources and selected-model execution
for OpenAI, Anthropic, Gemini, and Ollama:

```text
Provider -> Model -> Session
```

The public behavioral contracts are `ProviderBackend`, `ModelBackend`, and
`SessionBackend`. The exact caller surface is owned by
[`docs/PORTABLE_API.md`](docs/PORTABLE_API.md).

providers-kit does not own an agent loop, executable tool runtime, shell
runtime, approval system, durable factory, public conformance suite, or
higher-level orchestration registry.

## Dispatch And Caller Implementation Rules

- `Provider`, `Model`, and `Session` are opaque public handles over private
  exhaustive dispatchers containing only concrete built-in variants.
- Concrete built-ins, each private dispatcher, and each public handle
  implement the corresponding public RPITIT trait directly.
- `ProviderBackend::Model` is constrained by `ModelBackend`. Each Model owns
  one exact `Continuation` type plus one associated `SessionBackend`.
- Caller implementations retain their concrete types and return their own
  associated Model and Session implementations. There is no Kit normalization
  ingress at any level.
- Applications needing runtime heterogeneity define their own exhaustive
  Provider, Model, and Session enums and implement the public traits on them.
- Do not add erased adapters, `Extension` variants, backend trait objects,
  boxed backend futures, normalization constructors, one-field generic
  wrappers, aliases, macros, or duplicate behavioral traits.
- Dispatcher matches remain explicit and exhaustive and use receiver syntax.

## Provider And Model Ownership

- `Provider` owns exact model lookup and discovery, provider files, batch
  lifecycle, runtime health, and shared shutdown.
- `Model` owns one validated exact `ModelId` and all work that requires a
  selected model: generation, streaming, embeddings, batch submission, and
  session opening.
- `Session` owns only provider-native interactive behavior and is unique and
  mutable. Ordinary multi-turn generation uses messages plus continuation.
- `Provider` and `Model` are cloneable shared handles. `Session` is non-Clone.
- Provider shutdown seals existing Model handles, drains admitted work, runs
  backend shutdown once, and replays the same data-owned terminal result.
  Cancelling a shutdown waiter must not cancel cleanup. Model has no separate
  shutdown.
- Model lookup is exact. Do not normalize, alias, fall back, or select a
  configured default slot. `ModelId` is provider-local descriptive identity.
- Do not add implementation identity, provider provenance, reusable provider
  names, or equivalent fields to traits, handles, responses, errors, runtime
  records, or Kit-generated session ids. `ProviderKind` remains construction
  configuration only.
- Delete and reject capability matrices, operation-slot selectors,
  provider-specific public model-selection enums, and shared model slot sets.
  An operation itself returns `KitError` classified as `ErrorKind::Unsupported` when the
  selected model cannot perform it.

## Generation And Function Continuation

- `GenerateRequest` is one validated invariant-bearing request owner.
  Model operations borrow it; provider render views remain private.
- Generation, embedding, batch-submit, and session-open requests do not carry
  model selectors. Selection already occurred at `Provider::model`.
- `FunctionCall` and `FunctionResult` are provider-neutral, validated values.
  They retain call correlation without exposing provider envelopes.
- Returned assistant `Message<C>` values preserve model-authored continuation
  through their exact associated `C`; supplied Models use the opaque public
  `GenerationContinuation` carrier whose native contents remain private.
  Returned assistant messages must be appended unchanged. This includes
  OpenAI response linkage, Anthropic thinking blocks, Gemini thought
  signatures, and Ollama thinking replay. Callers append a validated
  function-result message after the assistant message.
- Generation streaming is demand-driven and bounded. It owns no forwarding
  task or channel, emits no event after `Completed`, and releases provider
  admission on completion, error, or drop.
- Do not expose raw provider metadata, reasoning payloads, wire DTOs, response
  ids, or continuation envelopes as general public maps.

## Hosted Tools

- Provider-hosted request vocabulary is the closed `HostedTool` enum carried
  by `ProviderToolSpec::Hosted`.
- Provider-specific validation and rendering occur on the selected Model.
  Unsupported model/provider combinations fail honestly before request
  submission when known.
- There is no `ProviderHostedTools`, `ProviderHostedToolsBackend`,
  `ProviderHostedToolDeclaration`, opaque `Any` payload, provider-hosted
  dispatcher, or hosted extension seam.
- Provider-hosted tools are request declarations. They do not execute through
  `tools-kit`, and providers-kit does not execute caller function tools.

## Module And Public-Surface Rules

Public API declarations, including public inherent methods and reexports, belong
in the crate's root `src/*.rs` files. `src/config/**` is the sole exception for
configuration owners. Public modules must own their declarations directly;
do not expose a public API through a facade over `core` or `transport`.

`src/core/**` contains only generic implementation shared by concrete providers.
Its cross-module items use restricted visibility such as `pub(crate)`, never
unrestricted `pub`. Provider-specific behavior remains in `src/providers/**`.

Supporting API lives in functional public modules:

- `config`
- `generation`
- `embedding`
- `hosted`
- `files`
- `batch`
- `models`
- `session`
- `runtime`
- `error`

The crate root reexports only the primary handles, their backend traits,
`KitError`, and the small set of construction types needed for a normal
workflow. Advanced DTO families remain in their owning modules.

Provider-private clients, transport DTOs, concrete model owners, renderers,
dispatchers, continuation metadata, and lifecycle state remain private.
Passive serde DTO fields may be public; invariant-bearing or runtime owners
keep fields private.

## Documentation Ownership

- `README.md`: downstream workflows
- `ARCHITECTURE.md`: ownership and private boundaries
- `AGENTS.md`: crate rules and canonical validation
- `docs/PORTABLE_API.md`: exact public hierarchy and signatures
- `docs/STRICT_TESTING.md`: proof lanes and command sequencing
- `docs/BEHAVIOR_PROOF_POLICY.md`: native-behavior evidence policy
- `docs/providers/*.md`: provider-specific facts and caveats

Update the owning docs and source contracts whenever a public type, module,
provider boundary, or validation command changes.

## Required Validation

Run these completion gates from the workspace root after the change is
assembled. During implementation, preserve the tests-first sequence: focused
red evidence before production edits, passing controls, and independent review.

```bash
bash providers-kit/scripts/check.sh
source providers-kit/strict-tests.env && bash providers-kit/scripts/verify_strict_live.sh
```

The crate wrapper must compile all targets and features and explicitly select
non-live test targets, including `strict_live_assertions`, compile contracts,
deterministic public behavior, source contracts, rustdoc/doctests, and the
docs-site gate. Clippy denies warnings. Live tests have no `#[ignore]` and the
verifier never uses `--ignored`. Its default full mode selects every live case;
the explicit `PROVIDERS_KIT_RUN_LONG_BATCHES=0` mode may omit only the four OpenAI
batch-completion cases and the three Anthropic completion cases: inline,
mixed-row, and bounded-results. It must report partial coverage with all seven
exact names. Cancellation, missing-resource and unsupported-input cases remain
required. No other omissions are permitted. Unrestricted Cargo test commands also select live tests. Strict-live alone proves built-in provider behavior
against real configured services.

Strict-live cases remain independent and use the supplied public Provider,
Model, and Session implementations. Shared test-local positive/negative
assertion checks cannot serve as live evidence. Successful completion must
validate exact model identity, generation terminal reasons, substantive hosted
execution evidence, and exact batch row outcomes. Cancellation is separate
from successful batch processing; completion races leave cancellation
unproven. OpenAI batch completion checks status immediately and then once per
minute for up to 24 hours per job. Each of the three Anthropic completion cases
has one shared 24-hour completion deadline with five-second polling. The mixed-row
case shares its deadline across readability-prerequisite and mixed-outcome jobs.
Setup, result reads and cleanup retain their independent bounds and ownership.
All other Anthropic and Gemini cases retain their existing bounds. Return immediately on a terminal response. Cancellation cleanup bounds remain
11 minutes for OpenAI and 3 minutes for other cloud providers.

Only the two OpenAI Realtime cases named in `docs/STRICT_TESTING.md` separate
blocking Kit/scenario results from three nonblocking raw exact-copy grades.
Actual cancellation, required calls and exact correlation, new post-result audio
and nonblank transcription, authoritative completion, evidence, close and shutdown
remain blocking. Missing, malformed or duplicate grades fail qualification.
Whole transcripts remain untouched. These public Session observations establish
neither independent native parity nor semantic use of the function result.
Every other Provider acceptance rule remains unchanged; historical failures keep
their original criteria and outcomes.

The user-approved File Search streaming qualification is test-only and limited
to Gemini `GenerateStream` with the exact selected `models/gemini-2.5-flash` in
the ordinary positive and owned-store consistency cases. Only the complete
single PYTHON `print(file_search.query(...))` declaration defined in
[`docs/STRICT_TESTING.md`](docs/STRICT_TESTING.md#qualified-gemini-file-search-streaming-coverage)
may coexist with proven retrieval, without results, status, artifacts or other
hosted activity. Preserve every returned declaration and response. Mandatory
private credential-redacted evidence and visible output must state that strict
family exclusion remains unmet when the declaration is present. Positive and
preflight Answer checks remain strict. Only the post-deletion observation may
record one exact whole secret token with grounding as completion without
rejection when the Answer format fails; preserve the format error, separate
marker-observed flag, original response and no-erasure qualification. Absent,
wrong, embedded or duplicate secret tokens never qualify code. This changes
coverage acceptance, not production behavior or historical failed outcomes.
Unary calls, other models/providers, unmatched activity, malformed streams,
cleanup/shutdown failures and export failures remain fatal.

Fresh batch and ordinary file fixtures are owned by their cases; configured
positive File Search stores remain borrowed. The owned Gemini consistency
stores are the separate narrow exception described below. Native API use is
limited to OpenAI
`purpose=batch` input creation, metadata needed to identify exact owned
hidden artifacts for cleanup, and one Gemini `batches.delete` request per
missing-batch case for a freshly Kit-created, terminal owned job. The latter
prepares an absent resource using its exact server-issued name; artifact
ownership must be captured before deletion. A further narrow exception creates one fresh owned Gemini File Search store
per consistency-probe mode, imports one file uploaded through the Kit, polls only the
returned import operation, and deletes that exact store with `force=true`, followed by one exact GET
requiring complete native HTTP 404/NOT_FOUND absence evidence.
The same-mode Kit preflight must retrieve an independently generated secret
marker with native File Search evidence before deletion; native import readiness
never supplies retrieval proof. The separate uploaded file is deleted through
the Kit. Import polling is bounded to three minutes. These native mutations use the
official origin, no redirects/retries, 60-second deadlines, and bounded bodies;
uncertain mutations are never repeated. The user-approved deleted-store probe reports persistent retrieval,
completion without rejection, or exactly attributed native rejection in mandatory
private evidence. Its completion never claims guaranteed rejection or retrieval
erasure. Kit errors unrelated to the attributed resource, malformed streams,
cleanup/shutdown errors and export failures remain fatal. No arbitrary propagation
sleep or synthetic lookup-derived generation error is permitted.
Native responses must never supply result proof,
repair Kit errors, or replace public Kit operations. Never delete by filename,
time, or account inventory difference. Preserve primary failures through
bounded cleanup and report residual artifacts that cannot be proven owned.

The user-approved temporary Gemini generated-output retention exception is
limited to test cleanup: attempt Kit deletion, then defer only the exact owned
completed batch output whose overlength ID receives the fully matched HTTP 400
`INVALID_ARGUMENT` / `DeleteFileRequest.name` 40-character rejection. Keep the
original Kit error and undeleted resource in mandatory private ownership
evidence, label expiration unverified, and visibly qualify the case result.
Every behavioral assertion, other cleanup failure, and shutdown requirement
remains authoritative. Evidence export failure is fatal. The exact eligibility
and documented six-week retention policy belong to
[`docs/STRICT_TESTING.md`](docs/STRICT_TESTING.md#temporary-gemini-generated-output-retention-exception).

For the error-contract resolution baseline, preserve the sequence of error
contract, opaque outcomes, provider decoding, hosted evidence, and fixture
ownership. Complete deterministic regressions and independent review before
strict-live validation. Preserve each prior run and report failures and unproven
cases separately. Subsequent repairs require causal evidence, TDD and review;
never retry unchanged cases until one passes, weaken assertions or substitute
models. Completion requires a full canonical pass on final unchanged code.
Mixed-file cases retain
unary readability and add batch readability before deletion; hidden input
ownership is per submission. Keep opaque errors through cleanup and shutdown,
and retain unresolved ownership independently of successful job terminality.

## Primary mock CI

The direct-trait targets are required by `bash scripts/check_ci.sh` and can be
scoped with `bash scripts/check_mock_contracts.sh --crate providers-kit`. Follow
[the mock test contract](../docs/mock-testing.md). Keep these credential-free
targets separate from existing integration and strict-live verification; the
existing gates above remain required. Production regressions stay enabled and
are recorded in [the bug ledger](../docs/mock-testing-production-bugs.md).
