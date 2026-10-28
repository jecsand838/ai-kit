# Portable API Guide

[`specs/API.md`](../specs/API.md) is the sole exact public inventory.

## Caller Model

```text
AgentStorage -> AgentBuilder -> Agent -> Workspace -> Conversation
                                                    -> notification receiver
```

- Import primary handles, backend traits, builder, and error from the crate
  root.
- Import supporting values from `config`, `identity`, `workspace`,
  `conversation`, `prompt`, `notification`, `model`, `tool`, `knowledge`, or `error`.
- Build `AgentStorage<S>` directly from `S: storage_kit::StateStoreBackend`.
- Create or reopen with all required integrations up front.
- Pass a direct `P: providers_kit::ProviderBackend` implementation, already-built
  `T: tools_kit::ToolsetBackend` implementations, and an optional direct
  `D: storage_kit::DatabaseBackend` implementation.
- Reopen Workspace and Conversation values through typed ids after a stale
  revision or parent runtime restart.

Concrete Provider, Toolset, and storage implementations are sufficient and
need not implement Clone. Tool handlers remain `T::Handler`. Caller-owned
exhaustive Provider/Model/Session, Toolset/Handler, or storage enums are optional
runtime-composition tools and remain the caller's `P`, `T`, `S`, or `D`;
agents-kit adds no extension arm or normalization route.

## Custom Hierarchy

A downstream custom runtime implements:

```text
AgentBackend::Workspace: WorkspaceBackend
WorkspaceBackend::Conversation: ConversationBackend
```

The caller uses this hierarchy directly or defines matching exhaustive
Agent/Workspace/Conversation enums for runtime composition. Associated child
types preserve ownership without independent injection or Kit normalization.

All three supplied handles implement their own traits as closed private
Codex/OpenCode/Custom dispatchers. They contain no erased adapter or
`Extension` arm for caller implementations.

## Managed Construction

```rust,no_run
use agents_kit::{config::AgentConfig, Agent, AgentBuilder, AgentStorage, KitError};
use storage_kit::StateStoreBackend;

async fn build<S: StateStoreBackend>(
    config: AgentConfig,
    storage: AgentStorage<S>,
) -> Result<Agent<S>, KitError> {
    AgentBuilder::create(config, storage).build().await
}
```

Custom composition adds a direct `P: ProviderBackend` and zero or more
Toolsets. The resulting `Agent<S, D, P, T>` calls `P::Model::generate` or
`generate_stream` within its one prompt loop; Session
remains the Model-associated provider child and is not used or parameterized
by Agent. The Model-associated `Continuation` is also not an Agent parameter:
Custom preserves it only through one active prompt's model/tool turns and
stores only provider-neutral caller/final-assistant text. Model, Continuation,
and Session are not independent builder parameters.
Native Codex/OpenCode construction may instead use
`create_for_backends::<P, T>` or `reopen_for_backends::<P, T>` to preserve the same
static `P` in an upper inventory without supplying a provider value. These
methods do not create a second provider ingress or provider lifecycle owner;
Custom still receives the concrete value only through `with_provider`.
An optional direct DatabaseBackend implementation enables private managed
knowledge and changes the builder's `D` parameter. No public factory,
declaration registry, KnowledgeBase implementation, provider normalization
route, Toolset normalization route, or storage normalization route exists.

Select T once with `create_for_backends` or `reopen_for_backends`; each
`with_toolset(T)` appends without replacing existing values. Provider and
Database changes preserve T and its accepted inventory. Callers build ordinary
shell tools themselves with explicit cwd and process policy; Agent no longer
constructs a shell or binds tool input through task-local state.

## Runtime Semantics

- Prompt is an async foreground call. Accepted work remains runtime-owned if
  its caller future is dropped.
- `ConversationBackend::prompt_stream` returns transient real text and one
  authoritative terminal response through `prompt::PromptStream`. Observer
  drop and cancelled reads leave accepted work owned by its conversation.
  The default trait method returns `Unsupported`; terminal-only caller
  implementations remain valid for ordinary prompts. Custom streaming requires
  its selected Model's real `generate_stream` support.
- Abort joins only the current generation and preserves cancellation fencing.
- Workspace update is revision-checked; successful commit makes older handles
  stale.
- Agent shutdown and Workspace close preserve quiescent durable descendants
  for reopen.
- Conversation close is terminal.
- Notifications use one exclusive ordered at-least-once receiver and one
  interaction resolution path.
- Downstream Conversation implementations may construct that concrete receiver
  with `ConversationNotificationReceiver::from_stream`; this adapts values and
  is not an independent backend or lifecycle ingress.
- File reads validate portable relative paths and preserve exact bytes.
- Managed knowledge exposes only source/scope/policy/ingestion inputs; retrieved
  context and Database layout remain private.

## Ownership Boundaries

providers-kit owns one selected model call and normalized function
calls/results plus the associated continuation contract. agents-kit owns the
Custom multi-turn prompt/tool loop and never inspects or serializes that
continuation. tools-kit
owns Toolset/ToolHandler execution. A caller-supplied StateStoreBackend owns
control-plane transactions; a caller-supplied DatabaseBackend backs private
managed knowledge. `Agent<S, D, P, T>`, `Workspace<S, D, P, T>`, and
`Conversation<S, D, P, T>` preserve those concrete integration types.

No provider wire envelope, Toolset source variant, StateStore key, Database
payload, protocol route, native id, or secret crosses the agent public
surface.

## Failure inspection

Use `KitError::kind()` rather than matching the error owner. Supporting
`ErrorKind`, `ErrorContext`, `Operation`, `ErrorPhase`, `ErrorDetail`, and
`DeadlineKind` live in `agents_kit::error`; `KitError` remains available at the
crate root. The owner is cloneable and intentionally nonserializable.

Typed context identifies the requested operation and the detection phase.
Normalized causes represent causal failures; supplementary errors represent
later cleanup or recovery evidence. Neither remote cancellation nor a remote
conflict grants Agent cancellation or transaction authority. Bounds and safe
formatting are specified in [ERRORS](../specs/ERRORS.md).
