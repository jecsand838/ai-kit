# Provider prerequisite audit for app-sdk completion — 2026-09-13

Scope: direct static Provider→Model→Session composition, normalized output construction, streaming/continuation ownership, and the Custom Agent integration dependency. Read-only source/API audit; no cargo, production edits, or fresh strict-live execution. Findings are repository observations unless explicitly marked external fact. Current source anchors must be re-resolved after earlier execution phases move lines.

## Decision

**No provider-sdk production prerequisite is demonstrated.** Keep the existing three backend traits and normalized DTOs. Agent can implement partial-output streaming using the already-public Model stream operation; App must not gain a provider-sdk dependency. Provider implementation changes are conditional on a newly failing owning behavioral test, not a mandatory empty refactor phase.

## Evidence

| Area | Exact source | Observation / consequence |
|---|---|---|
| Static root | `provider-sdk/src/provider.rs:28` | `ProviderBackend: Send + Sync + 'static` with associated `Model: ModelBackend`; no Clone, Default, serde, built-in identity, normalization, or root handle requirement. |
| Static children | `provider-sdk/src/models.rs:158` | `ModelBackend` associates both `Continuation` and `Session`; only the continuation requires Clone. `generate` and `generate_stream` retain its exact continuation parameter. Do not introduce independent Session or Continuation generics on Agent/App. |
| Unique session | `provider-sdk/src/session.rs:281` | Public mutable Session trait requires Send, has input/event DTOs, consuming close; caller Session implementation is independent of supplied Session. |
| Caller results | `provider-sdk/src/generation.rs:1612` | GenerateResponse is a passive DTO with public model/message/finish/citation/artifact/hosted-output/usage fields. No private-constructor block prevents foreign models returning results. |
| Caller continuation | `provider-sdk/src/generation.rs:167`, `:254`, `:259`, `:295` | `Message<C>` can be constructed as assistant, supplied a caller C, and inspected via borrowed continuation; message serde/native internals are not an integration requirement. |
| Caller streams | `provider-sdk/src/generation.rs:1654`, `:1698`, `:1704` | Public GenerateEvent enum and GenerateEventStream::new accept an arbitrary caller-owned Send+'static Stream of normalized events. The box erases a value stream, not provider/model behavior; no new backend-erasure seam is needed. |
| Existing proof | `provider-sdk/tests/provider_backend.rs:21`, `:23`, `:30`, `:166`, `:184`, `:375`, `:395`, `:450` | Existing fixtures are non-Clone Provider/Model/Session with custom continuation, public stream construction, function/generation/resource/session behavior, and sessionless Infallible. They provide source-visible test coverage; they were not re-run in this audit. |
| Existing stream contract | `provider-sdk/AGENTS.md:69`, `provider-sdk/docs/PORTABLE_API.md:60` | Demand-driven bounded generation, exact returned assistant replay, native continuation opaque to upper crates, no events after Completed, admission released on completion/error/drop. |

The stream wrapper at `generation.rs:1717` delegates polling; it does not validate an arbitrary backend’s full lifecycle. A foreign backend must honor the public contract. Agent should consume until one authoritative Completed result, reject premature EOF/error as an integration failure, preserve Completed.message unchanged within that prompt loop, and never infer an answer/continuation by concatenating deltas. No provider-specific native envelopes need to become public.

## Tests-first integration work (owned by Agent/App phases)

1. Extend Agent’s existing external provider fixtures with a non-Clone Model and non-serializable Continuation carrying an assertion marker. Make `generate_stream` emit at least two deltas and an authoritative Completed message with a function call + continuation; the next invocation must receive that exact returned message plus normalized FunctionResult. Fail before implementing Agent streaming. Verify partial text is observable before completion rather than synthesized afterward.
2. Prove callback/stream error and premature EOF do not publish synthetic completion, stream drop cancels owned work according to the amended Agent prompt contract, and whole-prompt deadline includes stream opening. Test a pending stream without sleeps by explicit readiness signals/paused time where available.
3. Prove durable reopen restores provider-neutral committed messages without continuation and accepts a freshly supplied provider implementation. No Model, Session, stream, native identifier, credential, or opaque continuation may enter app/state storage.
4. App end-to-end deterministic tests compose caller `AgentBackend` hierarchies, not Provider directly. The two-agent workflow’s real built-in lane is a separate strict-live proof.
5. Reuse `provider-sdk/tests/provider_backend.rs` if a missing provider contract assertion is found; do not create another public conformance harness or duplicate every existing test solely for this plan.

Minimal Agent-side consumer sketch (illustrative, not a new provider API):

```rust
impl<P: ProviderBackend> CustomProviderOperation<P> {
    // Existing operation owns cancellation and the whole prompt deadline.
    // Selected model remains P::Model; the completed Message retains
    // <P::Model as ModelBackend>::Continuation unchanged within this loop.
    // Stream events are normalized data. Stream transport stays in Provider.
}
```

## Reductions / nonchanges required

- Reuse ModelBackend::generate_stream, GenerateEvent, GenerateEventStream, Message, FunctionCall and FunctionResult. Do not add ProviderStreamBackend, provider identity, adapter constructor, public dyn backend, event forwarding task/channel in Provider, or second continuation carrier.
- Agent owns one shared prompt driver for terminal and streamed entrypoints; do not duplicate provider replay/rendering in Agent or App.
- Default generic convenience types elsewhere are not evidence that the trait requires supplied handles. Prove foreign concrete values work before deleting convenience defaults for aesthetic reasons.
- Keep native sessions optional with honest Unsupported; workflow orchestration does not justify synthetic provider sessions or a new agent loop below Agent.

## Exact execution gates if provider is changed

`bash provider-sdk/scripts/check.sh` is the authoritative wrapper: formatting; clippy `-p provider-sdk --all-targets --all-features -- -D warnings`; lib, compile_fail, public_api, provider_backend, shutdown_lifecycle, session_lifecycle, provider_behavior, wire_behavior, strict_env_contract, source_contract; doctests; docs-site/hygiene. Run targeted regression first, then wrapper once after changes settle. Strict-live: `source provider-sdk/strict-tests.env && bash provider-sdk/scripts/verify_strict_live.sh`. Do not print credentials. A deterministic foreign provider never counts as real-service proof. Even when provider production remains unchanged, the final five-crate release pass covers it.

DoD: relevant new Agent/provider boundary tests pass, unchanged provider regression gates stay green, and independent subagent review confirms no continuation leakage, built-in dependence, boxed backend ingress, duplicated driver, or premature terminal publication.

## Research / authority record

Loaded provider-sdk-research, provider-sdk-change-contract, tool-sdk-research, tool-sdk-change-contract, ai-kit-public-surface-contract; user-supplied Rust core/async/testing/networking/abstractions/architecture/structuring/dependencies guidance, root/crate AGENTS, both PORTABLE_API documents, and enum-dispatch reference. Workspace edition 2024 / MSRV 1.94; no dependencies required.

Current official provider docs were opened on 2026-09-13 for the narrowly relevant tool-call/replay boundary: [OpenAI function calling](https://developers.openai.com/api/docs/guides/function-calling), [Anthropic tool use](https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview), [Gemini function calling](https://ai.google.dev/gemini-api/docs/function-calling), [Ollama tool calling](https://docs.ollama.com/capabilities/tool-calling). Their client-executed tool-call flows support preserving the current split: Provider normalizes function requests/results; Agent executes and loops. This is not a renewed audit of every provider capability/model or strict-live correctness.
