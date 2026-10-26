---
name: agent-sdk-change-contract
description: "Apply agent-sdk hierarchy, tight public-surface, backend isolation, state/lifecycle, docs, and validation rules."
---

# agent-sdk Change Contract

## Trigger

Use this skill for agent-sdk specs, public API, production, tests, docs,
backend behavior, protocol, persistence, or validation.

## Prerequisites

- Read [`agent-sdk/AGENTS.md`](../../../agent-sdk/AGENTS.md).
- Read [`agent-sdk/specs/SPECIFICATIONS.md`](../../../agent-sdk/specs/SPECIFICATIONS.md).
- Read [`agent-sdk/specs/API.md`](../../../agent-sdk/specs/API.md) for every
  public change.
- Read the owning behavior/backend specs.
- Load `agent-sdk-research` for protocol or strict-live facts.
- Load `ai-kit-public-surface-contract` plus the Rust core, async, testing,
  abstraction, architecture, and structuring skills.

## Public Boundary

1. Keep exactly `AgentBackend`, `WorkspaceBackend`, and
   `ConversationBackend`.
2. `AgentBackend` owns associated `Workspace`; `WorkspaceBackend` owns
   associated `Conversation`.
3. Caller implementations stay concrete or use caller-owned exhaustive enums;
   never normalize them into supplied handles.
4. Supplied handles, closed dispatchers, and built-ins implement their trait.
5. Each supplied dispatcher contains only Codex, OpenCode, and Custom; reject
   erased adapters, `Extension` arms, and normalization constructors.
6. Keep primary handles, traits, builder, storage, and error at the crate
   root; group supporting values under the functional public modules in
   `specs/API.md`.
7. Reject factories, conformance suites, public KnowledgeBase, hierarchy
   ingress, wrappers, trait objects, registries, aliases, generated dispatch,
   compatibility surfaces, and duplicate behavioral traits.

## Construction

- Use `AgentBuilder::create(config, storage)` and
  `AgentBuilder::reopen(config, storage, id)`.
- Native Codex/OpenCode construction may use the matching typed-provider
  create/reopen methods to select `P` for upper composition without supplying
  or owning a provider value. Custom still receives a concrete `P` only
  through `with_provider`.
- `AgentStorage<S>::new` owns a direct `S: StateStoreBackend` implementation.
- Builder integration uses a direct `P: ProviderBackend`, already-built
  values of one `T: ToolsetBackend`, and an optional direct
  `D: DatabaseBackend` implementation. Agent, Workspace, and Conversation
  preserve `S`, `D`, `P`, and `T` statically with built-in defaults.
  `T::Handler` stays associated; neither `T` nor its Handler requires Clone.
  Mixed tool inventories use caller-owned exhaustive Toolset/Handler enums.
  Provider or Database replacement preserves accepted Toolset values. Provider
  children remain `P::Model`, its associated
  `Continuation`, and the Model-associated Session; never add independent
  child parameters.
- Caller-defined Provider/Model/Session implementations remain direct concrete
  values. Optional runtime composition uses caller-owned exhaustive enum
  hierarchies; do not normalize provider behavior into supplied handles or
  restore Provider erasure or implementation identity.
- Reopen callers resupply integrations. Do not erase Agent or storage
  implementations, add backend trait objects, or persist lower credentials,
  configs, factories, or declaration payloads.
- Keep build cleanup reverse-ordered, tracked, bounded, and first-error
  preserving; retain subsequent failures as normalized supplementary errors.
- Keep `SdkError` opaque, cloneable, and nonserializable. Its closed inspection
  enums live in `error`; preserve operation, phase, normalized causes, and
  bounded supplementary failures according to `specs/ERRORS.md`. Never expose
  native prose or lower-crate error values through formatting or `source()`.

## Custom

- Agent-sdk owns the multi-turn Custom prompt loop over direct
  `P: ProviderBackend` ownership. One accepted prompt owner uses
  `P::Model::{generate, generate_stream}` for unary and streaming execution.
  Validate complete finish reasons and call shape before handler construction
  or transcript advancement according to `specs/custom/CONVERSATION.md`.
  Session stays the Model-associated provider child and is not used or
  parameterized by Agent.
- Preserve `P::Model::Continuation` only through one active Custom prompt's
  model/tool turns. Do not add an Agent continuation generic, inspect or
  serialize the value, or persist it; reopen rebuilds provider-neutral history.
- Provider-sdk owns selected-model calls and normalized function
  calls/results, not an agent loop.
- Tool-sdk owns execution through `T: ToolsetBackend` and `T::Handler`.
- Custom owns transcript advancement, tool limits, bounded parallelism,
  durable approvals, shell as a local function tool, final-response detection,
  cancellation, and Toolset cleanup.
- Do not recreate LoopDriver, mounts, hook/recovery families, provider shell
  policies, generic retry frameworks, or provider-specific driver copies.

## Knowledge

- Managed knowledge is private over `AgentStorage<S>` plus optional
  `D: DatabaseBackend`.
- Public knowledge consists only of source, scope, policy, and ingestion
  request input.
- Owner lineage, retrieval request/budget, context/snippets/provenance,
  verification, Database layout, and formatting stay private.
- Knowledge is not StateStore state, a custom implementation seam, or an
  automatic model-visible tool.

## Lifecycle And State

- Prompt is foreground async work and remains runtime-owned after caller loss.
  PromptStream is bounded observation of that same accepted owner. Observer
  drop or overflow does not cancel work or release admission; its authoritative
  terminal response follows durable commit.
- Custom abort completes after its captured generation reaches a valid durable
  terminal and cancellation dispatch settles or is proven unnecessary; it does
  not wait for consumption of an unread unary reply. Reply acknowledgement and
  full close/shutdown quiescence remain separate requirements. Preserve target
  generation isolation and the owning
  [Custom conversation contract](../../../agent-sdk/specs/custom/CONVERSATION.md);
  do not infer that a targeted fix resolves every cancellation scenario.
- Agent shutdown and Workspace close cascade runtime invalidation while
  preserving quiescent records. Conversation close is terminal.
- Notification delivery is one exclusive durable at-least-once receiver with
  one interaction resolution path.
- Workspace updates are atomic and revision-checked; durable commit precedes
  stale-handle invalidation.
- Preserve exact storage conflicts. Retry/reconcile only safe operations within
  the existing lifecycle deadline; surface non-conflict errors unchanged.
- No lock or unrelated storage transaction crosses provider, tool, Database,
  notification, process, transport, or network waits.

## Protocol

- Ground Codex/OpenCode facts in selected vendored sources, official docs, or
  strict-live proof.
- Decode known events to typed closed enums and classify exhaustively.
- Keep provider envelopes, native ids, routes, clients, attachments, and
  storage keys private.
- Use `Unsupported` for impossible selected behavior,
  `NotYetImplemented` for a finite adapter gap, and `Protocol` for malformed
  selected wire input.

## Tests And Docs

- Production source remains test-free.
- Downstream tests prove associated children and absent child ingress.
- Ordinary private test helpers replace public conformance owners.
- Source contracts protect stable public/private architecture and removed
  symbols, not helper spelling or exact topology.
- Strict-live alone proves real built-ins and external dependencies.
- Update API/specs, AGENTS, architecture, README, strict-testing, scripts,
  source contracts, and env templates together.

## Output

- Name the governing specs and sourced facts.
- Prefer deletion and receiver-owned domain modeling.
- Report only validation actually run.
- Never invent reduced validation or synthetic backend success.

## References

- [`agent-sdk/AGENTS.md`](../../../agent-sdk/AGENTS.md)
- [`agent-sdk/specs/API.md`](../../../agent-sdk/specs/API.md)
- [`agent-sdk/ARCHITECTURE.md`](../../../agent-sdk/ARCHITECTURE.md)
- [`agent-sdk/docs/STRICT_TESTING.md`](../../../agent-sdk/docs/STRICT_TESTING.md)
