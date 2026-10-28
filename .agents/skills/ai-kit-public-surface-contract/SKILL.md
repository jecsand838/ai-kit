---
name: ai-kit-public-surface-contract
description: "Enforce ai-kit's reduced public surface: functional public modules, one erased Tool ingress, direct static Provider/Storage/Agent traits, associated child backends, opaque supplied handles, honest portable DTOs, and strict cross-crate ownership."
---

# ai-kit Public Surface Contract

## Purpose

Keep the five compiled Kit crates small, portable, and independently usable
without leaking private implementation topology or recreating deleted
frameworks.

## Trigger Boundary

Use this skill whenever public Rust types, traits, methods, reexports, module
visibility, docs, examples, source contracts, or cross-crate composition
change.

## Prerequisites

- Read root and nearest crate `AGENTS.md`.
- Load the owning crate research/change-contract skill.
- Read the owning portable API/spec document.
- Load Rust core, abstractions, architecture, testing, and structuring skills
  as applicable.

## Frozen Public Hierarchies

The eleven lower-crate backend traits and five App workflow traits are the
only public behavioral traits:

- provider: `ProviderBackend`, `ModelBackend`, `SessionBackend`;
- storage: `DatabaseBackend`, `StateStoreBackend`,
  `StateTransactionBackend`;
- tools: `ToolsetBackend`, `ToolHandlerBackend`;
- agents: `AgentBackend`, `WorkspaceBackend`, `ConversationBackend`;
- applications, under `workflow`: `ActionBackend`, `ConditionBackend`,
  `StateTyping`, `LifecycleHooks`, `ChildExecutor`.

Async methods use RPITIT. App's `ChildExecutor` alone uses a generic associated
future for concrete local, non-Send child futures. `ChildBatch` may pin those
concrete futures in `Box<F>` without erasing their types. The App specification
owns the exact signature; this exception does not widen lower-crate traits.

The hierarchy is:

```text
Provider -> Model -> Session
Database
StateStore -> StateTransaction
Toolset -> ToolHandler
Agent -> Workspace -> Conversation
```

Parent traits expose concrete associated children:

```rust
trait ProviderBackend {
    type Model: ModelBackend;
}

trait ModelBackend {
    type Continuation: Clone + Send + Sync + 'static;
    type Session: SessionBackend;
}

trait StateStoreBackend {
    type Transaction: StateTransactionBackend;
}

trait ToolsetBackend {
    type Handler: ToolHandlerBackend;
}

trait AgentBackend {
    type Workspace: WorkspaceBackend;
}

trait WorkspaceBackend {
    type Conversation: ConversationBackend;
}
```

Public erased extension construction exists only on Toolset. Its ToolHandler
enters through the associated type and is normalized privately. Provider,
Storage, and Agent callers implement their public traits directly with their
own associated children. Never add a Provider/Storage/Agent normalization
constructor or public child `from_backend`.

## Workflow

1. Identify the caller value and owning functional domain for every public
   item.
2. Keep primary handles, backend traits, and errors at crate roots. Place
   supporting DTOs in the approved functional modules:
   - provider: `config`, `generation`, `embedding`, `hosted`, `files`,
     `batch`, `models`, `session`, `runtime`, `error`;
   - storage: `database::{config,catalog,data,query,index}` and
     `state::{config,record,transaction}`;
   - tools: `declaration`, `execution`, `interaction`, `local`, `mcp`,
     `schema`;
   - agents: `config`, `identity`, `workspace`, `conversation`, `prompt`,
     `notification`, `model`, `tool`, `knowledge`, `error`.
   - applications: `workflow`.
3. Keep every supplied lower-crate handle opaque over a private exhaustive
   enum. Every concrete lower-crate built-in, enum, and handle implements the
   same public
   RPITIT contract. App preserves its five concrete workflow seams and the
   exact `ChildExecutor` exception above; it adds no backend dispatcher ingress.
4. Retain exactly one private `Extension` arm in each of the two Tool
   dispatchers. The sole root `from_backend` constructor creates only that
   arm. Keep all nine Provider, Storage, and Agent dispatchers closed over
   concrete built-ins; callers remain statically typed or use caller-owned
   exhaustive enums.
5. Normalize the Tool associated child privately. Do not introduce a
   one-field generic extension wrapper, public erased handle, bare trait
   object, duplicate private behavioral trait, dispatch macro, generated
   route, or wildcard implementation-family match.
6. Keep runtime owners and invariant-bearing values private. Public fields are
   acceptable only on intentional passive request, response, config,
   protocol, serialization, or persistence DTOs.
7. Reject native provider envelopes, transport objects, storage keys, database
   native commands, source runtimes, protocol clients, channels, tasks, and
   backend records from public exports and taught examples.
8. Update the owning contract, reexports, caller docs, rustdoc, compile tests,
   and forbidden-symbol guards together.

## Deleted Surface Guard

Never restore:

- lower-crate factory traits or factory request DTOs;
- product-level conformance owners, inputs, failures, or production fixtures;
- `LoopDriver`, provider-owned tool execution, shell, approval, mount, retry,
  or agent-loop families;
- `ProviderHostedToolsBackend`, `ProviderHostedTools`, or opaque hosted
  payload downcasting;
- public `KnowledgeBase`, `KnowledgeBaseBackend`, knowledge factory, or
  knowledge conformance;
- storage-to-tool adapters or the storage `toolsets` feature;
- portable database realtime or backend-native operation families;
- public direct child ingress;
- compatibility aliases, deprecated twins, tombstone modules, or forwarding
  facades.

## Cross-Crate Ownership

- `providers-kit` owns provider credentials, provider/model resources,
  generation/streaming/embedding/batch/session behavior, normalized
  function-call/result values, portable hosted-tool configuration, wire
  adaptation, and provider lifecycle.
- `tools-kit` owns executable Local/MCP client Toolsets, ToolHandlers,
  interaction state, and the independent McpServer product.
- `storage-kit` owns Database behavior and separate transactional StateStore
  behavior. It does not produce tools.
- `agents-kit` owns durable agent/workspace/conversation state, notifications,
  conflict reconciliation, private managed knowledge, and the Custom
  multi-turn loop. It composes direct `P: ProviderBackend` and already-built
  `T: ToolsetBackend` values with direct StateStoreBackend and DatabaseBackend
  implementations. Children remain `P::Model` and `T::Handler`; neither `T`
  nor its Handler requires Clone.
- `app-kit` owns application workspaces, workflow execution, checkpoints, and
  app control-plane records. It preserves a direct `A: AgentBackend` and
  statically retains its app `CS: StateStoreBackend`. It uses associated
  Workspace and Conversation types and has no providers-kit or tools-kit
  dependency. Action, condition, typing, lifecycle, and child-executor values
  remain concrete under `workflow`. Only its process-global field/tag
  serialization registry may privately erase captured codec functions; it
  exposes no `Any` payload or lower-backend normalization ingress.

Custom Agent construction accepts a direct ProviderBackend implementation and
already-built values of one direct `T: ToolsetBackend` type. Mixed inventories
use caller-owned exhaustive Toolset/Handler enums. Native Agent construction
may select the same
provider type only as static upper-composition context and owns no provider
value. All Agent construction accepts static storage implementations and never
duplicates lower credentials/configs,
reconstructs them from declarations, inspects private enums, or asks callers
to branch on implementation identity.

Provider models return normalized function calls and own an associated
continuation type. agents-kit executes those calls through Toolset/ToolHandler,
returns normalized function results, and preserves the exact continuation only
inside the active prompt loop.
providers-kit has no dependency on tools-kit.

## Testing Contract

- Compile tests prove public module paths, associated child signatures,
  exported-handle trait implementations, ownership, auto traits, privacy, and
  rejected child ingress.
- Deterministic integration tests prove validation, Tool erasure, direct
  Provider/Storage/Agent composition, lifecycle, cancellation, cleanup,
  redaction, and conflict behavior.
- Protocol/model tests prove normalized provider continuation.
- Storage actor, MCP client/server, and app workflow tests prove their owning
  behavior.
- Strict-live alone proves built-ins and external services.

Tests must not freeze exact private helper names, file topology, match-arm
spelling, or duplicated prose. Do not create a public conformance framework,
production fixture, callback alias, macro, or shared conformance crate.

## Shared Guardrails

- No public `dyn ...Backend`, serialized trait object, generalized runtime
  registry, or extension map. App's field/tag codec registry is the sole
  serialization-only exception; no action, hook, condition, typing, child
  executor, or lower-backend value enters it.
- No aliases, backwards-compatibility API, macro/generated dispatch, unsafe,
  `async_trait`, synthetic portable success, or hidden fallback.
- Credentials remain in owning validated wrappers.
- Honest unsupported and not-yet-implemented errors are preferable to
  emulation.
- Keep `tmp/**` immutable.

## Output Rules

- Explain public additions by caller value and owning domain.
- Prefer deletion or a functional module over another root export.
- Treat imports, visibility, docs, rustdoc, compile contracts, and
  forbidden-symbol searches as one surface.
- Report only validation that actually ran.

## References

- [`AGENTS.md`](../../../AGENTS.md)
- [`ARCHITECTURE.md`](../../../ARCHITECTURE.md)
- owning crate `AGENTS.md` and portable API/spec authority
- `rust-public-api-docs`
