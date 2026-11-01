---
name: providers-kit-change-contract
description: "Apply providers-kit Provider→Model→Session ownership, private enum dispatch, exact model selection, normalized function continuation, closed HostedTool, documentation, and validation rules."
---

# providers-kit Change Contract

## Purpose

Keep provider changes inside the reduced public hierarchy, provider-owned
conversation continuity and native protocol boundaries.

## Prerequisites

- Read [`providers-kit/AGENTS.md`](../../../providers-kit/AGENTS.md).
- Load `providers-kit-research` for provider behavior, model support,
  continuation, hosted tools, or strict-live assumptions.
- Load `ai-kit-public-surface-contract` for public imports, reexports, or
  visibility.
- Load the relevant Rust core, async, testing, abstraction, architecture, and
  structuring skills.

## Workflow

1. Preserve the three backend contracts `ProviderBackend`, `ModelBackend`, and
   `SessionBackend`, plus the two approved borrowed-half contracts
   `session::SessionCommands` and `session::SessionEvents`. No other public
   behavioral trait is authorized.
2. Preserve `Provider -> Model -> Session`. Provider owns its associated Model;
   Model owns its associated `Continuation` and Session. Caller implementations
   retain those concrete types and use them directly. There is no normalization
   ingress or independent continuation/session parameter. Session associates
   concrete borrowed command/event halves; only its unique owner closes.
3. Keep concrete built-ins, private exhaustive dispatchers, and public handles
   implementing the same trait. Each supplied dispatcher has explicit built-in
   variants only. Runtime composition enums are optional and caller-owned.
4. Put exact model lookup/discovery, files, batch lifecycle, health, and
   shutdown on Provider. Do not expose implementation identity or provider
   provenance through traits, handles, DTOs, errors, or generated resource ids.
5. Put generation, streaming, embeddings, batch submission, and session opening
   on Model. Require explicit Conversation or Realtime profile selection.
   Provider-owned Conversation sessions for all four supplied providers reuse
   their existing generation paths and retain exact authoritative continuation.
   OpenAI Realtime and Gemini Live own independent native mappings. Only
   genuinely sessionless caller Models may use `Infallible`; supplied providers
   must implement the Conversation baseline. Sessions own no tool execution,
   approvals, autonomous loop, persistence, fallback or automatic replay.
6. Keep ModelId exact. Delete/reject capability matrices, model selectors,
   configured operation slots, public provider-specific model enums, aliases,
   normalization, and fallback.
7. Use one validated GenerateRequest borrowed by Model methods. Keep render
   views private and remove model selectors from model-bound requests.
8. Normalize client functions through validated FunctionCall and
   FunctionResult. Preserve call correlation and the complete model-authored
   assistant Message. Keep provider replay state in opaque generic
   continuation ownership without requiring continuation serde.
9. Represent provider-hosted request tools as the closed HostedTool enum.
   The selected Model owns validation and rendering. Keep hosted output as one
   data-carrying enum without repeated provider/family metadata.
10. Keep provider clients, wire DTOs, renderers, concrete model owners, native
    continuation variants, and transport state private. Bound body, frame,
    diagnostic, download, batch-result, and WebSocket payload sizes.
11. Organize supporting API into `config`, `generation`, `embedding`,
    `hosted`, `files`, `batch`, `models`, `session`, and `runtime`. Keep only
    primary handles, traits, errors, and normal construction at the root.
12. For the approved Session refactor, update and independently review active
    target-state guidance first. Add meaningful failing tests before each
    production area, implement against the failures, and independently review
    the actual diff before closing the area. Update source contracts deliberately
    without weakening opacity. Run exact crate and full workspace gates; target
    guidance and compile-only red evidence do not prove behavior or completion.

## Removed Boundaries

Do not restore LoopDriver or its policies/hooks/errors, provider shell/tool
execution, ProviderHostedTools or opaque declarations, ProviderFactory,
production conformance owners, CapabilityMatrix, ModelSelection/model slot
sets, public transport wrappers, request routers, runtime inventory, fabricated
native continuity, attachments, child ingress, compatibility APIs, or a
tools-kit dependency.

## Output Rules

- No aliases, macros, unsafe, shims, generic frameworks, public dyn backends,
  provider kind/provenance routers, or duplicate behavior contracts.
- No erased backend adapters, Extension variants, boxed backend futures,
  normalization constructors, or trait-object replacement ingress.
- Use explicit receiver-method dispatch and owning domain methods.
- Preserve cloneable Provider/Model shared lifecycle, unique consuming Session
  close, demand-driven streaming, cancellation-safe shutdown, terminal-result
  replay, and redaction.
- Keep acceptance, responses, input capture, tool obligations and continuity
  distinct. Return a correlated receipt in the acceptance poll; accepted work
  survives dropped submission/receive futures. Idle receive is not permanent
  drain. Report fatal termination once, preserve late-event attribution and
  reject generation mutations when continuity is uncertain.
- Preserve accepted inputs/results and exact completed assistant messages;
  never rebuild replay from deltas. Validate tool-result batches atomically,
  release only executable calls with sufficient authoritative state, retract
  known-cancelled queued calls, and invalidate already-delivered calls.
- Reuse existing admission/sealing and transport machinery. Separate Session
  resource registration from active generation admission; idle sessions retain
  no normal permit. Observation, cancellation and close remain available after
  sealing. Track and join tasks across owner/close-future drop, and retain
  bounded control/terminal capacity under output backpressure.
- Return exact existing errors or honest Unsupported; never synthesize native
  behavior.
