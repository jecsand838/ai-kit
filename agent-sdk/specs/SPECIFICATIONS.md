# agent-sdk Specifications

This directory is the behavioral authority for agent-sdk. [`API.md`](API.md)
is the sole exact public inventory. Other documents own behavior and selected
backend deltas without adding signatures.

## Selected Sources

- Codex App Server: `rust-v0.163.0`
- OpenCode Server: `v1.19.3`

[`../vendor/vendor-lock.toml`](../vendor/vendor-lock.toml) mirrors the selected
archives and hashes. Vendored sources establish pinned protocol facts; official
documentation and strict-live supply behavior not guaranteed by declarations.

## Public Architecture

The portable runtime is:

```text
Agent -> Workspace -> Conversation
```

`AgentBackend`, `WorkspaceBackend`, and `ConversationBackend` are public RPITIT
contracts whose parents own associated child types. Caller implementations
remain concrete or use caller-owned exhaustive enums. Supplied handles are
closed Codex/OpenCode/Custom dispatchers without erasure or normalization.

Supporting values live in functional public modules. Backend protocols and
runtime topology remain private. There is no public factory, conformance,
KnowledgeBase, LoopDriver, hosted declaration, child-ingress, public trait
object, compatibility, or generated dispatch surface.

## Cross-Crate Ownership

- provider-sdk owns `Provider -> Model -> Session`, provider credentials,
  selected-model calls, normalized function calls/results, and continuation
  metadata;
- tool-sdk owns Toolset/ToolHandler execution and MCP products;
- storage-sdk StateStore owns control-plane state;
- storage-sdk Database backs private managed knowledge;
- agent-sdk owns the multi-turn Custom prompt loop, hierarchy, durable
  notifications, approvals, conflict reconciliation, cancellation, and
  lifecycle.

Custom construction accepts a direct `P: ProviderBackend`; native construction
may select that `P` only as static upper-composition context. Construction also
accepts already-built ToolsetBackend implementations and direct StateStoreBackend and optional
DatabaseBackend implementations. Reopen callers resupply owned values. The hierarchy preserves
provider, tool, and storage types statically; agent-sdk does not normalize
their behavior, persist lower credentials/configs, or route durable
factories. `P::Model`, its associated `Continuation`, and its associated
Session remain provider-owned child types. Custom preserves the exact
continuation only within one active prompt loop; Agent durable state remains
provider-neutral. `T::Handler` is the associated executable child; neither T
nor Handler requires Clone. Callers construct shell tools with explicit cwd,
process cancellation and output policy; Agent has no implicit shell factory.

## Specification Owners

| File | Responsibility |
|---|---|
| [`API.md`](API.md) | exact public modules, types, traits, and operations |
| [`RUNTIME.md`](RUNTIME.md) | hierarchy, prompt, models, files, update, lifecycle |
| [`BUILD_CONFIG.md`](BUILD_CONFIG.md) | configs and concrete lower-handle ingress |
| [`NOTIFICATIONS.md`](NOTIFICATIONS.md) | ordered delivery and interaction lifecycle |
| [`STATE.md`](STATE.md) | durable control-plane state, recovery, and reopen |
| [`KNOWLEDGE.md`](KNOWLEDGE.md) | private managed knowledge over Database |
| [`ERRORS.md`](ERRORS.md) | portable error categories |
| backend documents | selected native behavior and protocol deltas |

Backend documents cannot add a public operation or recreate deleted lower
crate models.

## Universal Invariants

- Public DTOs are passive. Runtime resources and lifecycle transitions belong
  to opaque owners.
- Accepted work is runtime-owned, not caller-future-owned.
- No synchronization guard or unrelated storage transaction survives provider,
  tool, Database, notification, process, or network I/O.
- Known protocol events are typed and exhaustively classified.
- Unsupported behavior fails honestly; adapter gaps are
  `NotYetImplemented`; malformed selected wire input is `Protocol`.
- Required memory, queues, retries, deadlines, and cleanup are bounded.
- SDK-owned processes use executable paths and argument vectors, drain output,
  and have tracked bounded shutdown.
- Public errors and tracing reveal no secret or private identity.

## Validation Ownership

Static source contracts protect public/private architecture and removed
symbols. Downstream compile tests prove associated child types, trait
implementations, ownership, and child-ingress privacy. Deterministic tests
prove agent-owned validation, notification, admission, cancellation,
redaction, conflict, and cleanup semantics. Strict-live alone proves real
Codex, OpenCode, provider, tool, storage, transport, and process behavior.

Public conformance runners are not product API; ordinary crate test helpers own
repeated cases.
