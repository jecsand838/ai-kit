# providers-kit

This crate is in early development. Backwards compatibility is not guaranteed
until `1.0.0`; pin exact versions and review the
[release notes](https://github.com/jecsand838/ai-kit/releases) before upgrading.

Repository guides and examples linked below track `main`.

`providers-kit` is a portable provider and model API for OpenAI, Anthropic,
Gemini, and Ollama.

Its public hierarchy is:

```text
Provider -> Model -> Session
```

`Provider` selects an exact `ModelId` and owns provider-wide resources.
`Model` performs generation, streaming, embeddings, batch submission, and
session opening. All four supplied providers expose a Conversation profile
retaining exact conversational continuity. OpenAI Realtime and Gemini Live
provide explicit native Realtime profiles. Qualification is established by
recorded validation of the exact source and invocation. Stateless OpenAI
audio remains separately unsupported. Direct stateless multi-turn generation
continues to reuse returned messages.

```rust,ignore
use providers_kit::{ModelBackend, ModelId, Provider, ProviderBackend};

let provider = Provider::from_config(config)?;
let work = async {
    let model = provider.model(ModelId::new("exact-provider-model-id")?)?;
    model.generate(&request).await
}.await;
let shutdown = provider.shutdown().await;
let response = match (work, shutdown) {
    (Ok(response), Ok(())) => Ok(response),
    (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
    (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
}?;
```

The canonical [generation Rustdoc source](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/src/lib.rs) includes complete setup
and service prerequisites. Settle shutdown before propagating an operation
failure, preserving any cleanup failure alongside the primary error.

The three public backend traits are also the caller implementation system.
`ProviderBackend` returns its own associated `Model: ModelBackend`. That model
owns its exact `Continuation` type and returns its associated
`Session: SessionBackend`. Callers use those concrete values directly; they do
not convert them into the supplied handles.
Applications needing heterogeneous runtime selection can define their own
exhaustive Provider, Model, and Session enums. A Session returns concrete borrowed
halves implementing `session::SessionCommands` and `session::SessionEvents`;
their owner alone has consuming close authority.

`Provider`, `Model`, and `Session` are the Kit's built-in implementations of
those traits. Their private dispatchers and borrowed-half dispatchers contain
only concrete built-ins. All four supplied Models own Conversation Sessions;
unsupported profile/model combinations return `Unsupported` without fallback.
External Models that are genuinely sessionless may use `Infallible`.

### Breaking Session migration

Choose the profile explicitly, borrow the two halves, submit typed commands and
observe correlated events, then release both halves before consuming close:

```rust,ignore
use providers_kit::{KitError, ModelBackend, SessionBackend};
use providers_kit::session::{
    SessionCommands, SessionConfig, SessionEvents, SessionProfile,
};

let config = SessionConfig::new(SessionProfile::Conversation);
let mut session = model.open_session(config).await?;
let work = async {
    let (mut commands, mut events) = session.split();
    let accepted = commands.submit(command).await?;
    // Drive events while submitting subsequent commands through the other half.
    // Match correlated response/control outcomes; None means permanent drain.
    let event = events.recv().await;
    Ok::<_, KitError>((accepted, event))
}.await;
let close = session.close().await;
let (accepted, event) = match (work, close) {
    (Ok(observed), Ok(())) => Ok(observed),
    (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
    (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
}?;
```

This excerpt shows the borrowed-half and consuming-close transitions. The
canonical [Session Rustdoc source](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/src/session.rs) observes a correlated response
under a deadline and settles both Session close and the caller-owned Provider
shutdown, including failure paths.

Submission acceptance is local ownership, not remote acknowledgement. Realtime
uses the same command/event boundary with `SessionProfile::Realtime`; unsupported
controls fail explicitly. A response outcome does not close the conversation,
finish playback or settle unrelated tools. Cancellation leaves accepted input
intact and reports stop resolution separately from continuity. The old default
profile and owner-level `send`/`recv` are replaced, without compatibility facades.

Function calling is normalized without moving agent-loop policy into this
crate. A model response exposes validated `FunctionCall` values while its
assistant `Message<C>` retains its Model's exact continuation. Supplied Models
use opaque `GenerationContinuation` state whose native contents remain private.
The caller appends that message unchanged, executes functions elsewhere, and
adds validated `FunctionResult` content.

Provider-hosted request declarations are closed `HostedTool` values rendered
by the selected model. Executable local or MCP tools remain `tools-kit`
behavior.

`KitError` is opaque and exposes provider-neutral `error::ErrorKind` values,
operation/phase context, and typed response-validation reasons. Default formatting
is safe for ordinary logs; bounded native diagnostics require explicit access.
See [the error contract and provider mappings](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/docs/PORTABLE_API.md#errors).

See:

- [Portable API](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/docs/PORTABLE_API.md)
- [Architecture](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/ARCHITECTURE.md)
- [Strict Testing](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/docs/STRICT_TESTING.md)
- [Behavior Proof Policy](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/docs/BEHAVIOR_PROOF_POLICY.md)
- [Provider notes](https://github.com/jecsand838/ai-kit/tree/main/providers-kit/docs/providers)

Validation:

```bash
bash providers-kit/scripts/check.sh
source providers-kit/strict-tests.env && bash providers-kit/scripts/verify_strict_live.sh
```

`check.sh` selects deterministic test targets explicitly, including the live
assertions' positive and negative contract checks. Live tests are not ignored:
an unrestricted `cargo test --locked -p providers-kit` also runs them and requires
the configured services. The verifier runs independent cases for all four
providers so one failed operation does not hide the remaining coverage.

Batch tests create fresh owned inputs and verify successful rows, mixed
successful/failed rows, and cancellation separately. OpenAI completion checks
status immediately and then once per minute, stopping at completion or failure,
with a maximum of 24 hours per job. Anthropic inline, mixed-row and bounded-result
completion each have one shared 24-hour deadline with five-second polling. The
mixed-row case shares its deadline across readability-prerequisite and outcome
jobs. Setup, result reads and cleanup retain separate bounds and ownership.
All other Anthropic and Gemini cases retain their existing limits. To run the
other cases while omitting the four long OpenAI and three Anthropic completion
cases:

```bash
source providers-kit/strict-tests.env && \
  PROVIDERS_KIT_RUN_LONG_BATCHES=0 bash providers-kit/scripts/verify_strict_live.sh
```

This explicitly reports partial coverage. Leave the toggle unset or set it to
`1` for the full suite. Configured positive File Search stores remain
pre-provisioned and borrowed; the suite never mutates or deletes them. The
separate Gemini consistency probes create and delete fresh owned stores under
their narrow fixture exception. Those probes do not replace the borrowed
positive cases, and management absence does not prove retrieval erasure.
See [Strict Testing](https://github.com/jecsand838/ai-kit/blob/main/providers-kit/docs/STRICT_TESTING.md) for exact evidence requirements,
the narrow native fixture/cleanup exceptions, and validation policy.

## Direct public-trait tests

Run `bash scripts/check_mock_contracts.sh --crate providers-kit` from the workspace
root for the credential-free mock lane. The workspace primary CI entrypoint is
`bash scripts/check_ci.sh`. See [mock testing](https://github.com/jecsand838/ai-kit/blob/main/docs/mock-testing.md) for
targets, evidence, runtime qualification, and the separate release/live gates.
