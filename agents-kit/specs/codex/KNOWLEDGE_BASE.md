# Codex Knowledge-Base Domain

This document owns the Codex `knowledge_base/` domain. It composes the shared
managed-knowledge implementation with Codex prompt formatting and lifecycle
ownership. [`../KNOWLEDGE.md`](../KNOWLEDGE.md) owns portable sources, scopes,
policy, ingestion, atomic publication, and Database semantics.

The domain is required even when managed storage is disabled. It provides one
consistent backend owner for validation, workspace/conversation association,
prompt retrieval, rendering, reopen verification, and cleanup; it is not a
public Codex knowledge API or a duplicate storage engine.

## Storage And Ownership Boundary

Managed knowledge uses only the caller-supplied
`D: storage_kit::DatabaseBackend`. The Codex knowledge domain never:

- stores chunks or embeddings in StateStore;
- constructs or defaults a Database, pool, credential, or backend selector;
- inspects Database identity, mode, capability, native locator, or persistence
  label;
- shuts down the caller-owned Database;
- turns knowledge into an automatic model-visible tool; or
- persists retrieved context as transcript or conversation memory.

Shared agent code owns source validation, planning, atomic replacement,
scoped querying, integrity references, and typed private retrieval results.
Codex owns only genuinely native source adaptation when one is required,
Codex-specific bounded selection policy, and final prompt-context rendering.
It must not introduce a shared `Codex` render mode, backend discriminator, or
public formatter setting.

## Enabled And Disabled State

Supplying a Database enables managed knowledge even when the initial source
set is empty, so compatible existing records remain retrievable. Omitting it
disables storage. Nonempty initial or later sources without a Database fail
according to [`../KNOWLEDGE.md`](../KNOWLEDGE.md); Codex does not silently
discard them or create a fallback store.

Disabled or explicitly disabled retrieval produces honest empty private
context without touching storage. A storage execution failure propagates as
`Integration`; unsupported content or operation semantics use the portable
categories rather than disabling knowledge globally.

## Ingestion And Scope

Database, Agent, Workspace, and Conversation source ownership follows the
portable receiver and explicit-scope rules. Sources contain no backend key,
namespace, owner id, or native locator. The Codex domain validates its owning
runtime and revision, then delegates one source-atomic replacement to shared
managed knowledge.

Success means the complete validated replacement is published. Failed or
incomplete candidate writes do not replace the prior authoritative set and
become bounded cleanup obligations. The implementation never scans or rewrites
unrelated caller data to imitate unsupported atomicity.

Workspace update changes future defaults but does not move source ownership.
Safe workspace delete and terminal conversation close remove, or durably
record cleanup for, the affected namespaces without touching Agent- or
Database-scoped sources.

## Conversation Materialization And Reopen

Conversation creation validates and materializes initial conversation sources
before the handle becomes usable. The durable control-plane conversation
record retains only private source identities and integrity references needed
to prove the committed Database content; it does not copy chunks, embeddings,
or backend storage configuration.

Reopen verifies every referenced source through ordinary public Database
operations using the supplied opaque handle. Exact compatible records may live
in any Database implementation. Missing or mismatched well-formed records are
`Conflict`; malformed or internally contradictory references are
`InvalidState`; execution failure is `Integration`. None becomes silent empty
retrieval.

## Prompt Retrieval And Rendering

After prompt and workspace-revision admission, the conversation resolves the
effective policy defined by [`../RUNTIME.md`](../RUNTIME.md). When retrieval is
required, it derives the query from the caller's prompt text, asks shared
knowledge for bounded typed results in portable precedence order, and passes
those results to the Codex renderer.

The renderer produces clearly delimited private context with source and chunk
traceability under fixed internal bounds. It preserves caller prompt text and
does not trim, normalize, or mutate the public request. Codex prompt formatting
is backend-owned; shared knowledge never constructs a Codex turn payload.

Retrieval happens before native prompt acceptance. A retrieval error returns
to the caller without starting a native turn. No Database call, rendering
operation, or protocol request occurs while a hierarchy guard or StateStore
transaction is held.

## Lifecycle

Knowledge work participates in runtime admission, so update, delete, close,
and shutdown observe it as active. Cancellation before publication releases
admission exactly once and publishes no partial result. Parent close preserves
durable knowledge for reopen; terminal conversation close and safe delete own
their scoped cleanup obligations.

The Database remains caller-owned throughout build, runtime, cascade,
shutdown, and reopen. The agents-kit never adds backend-specific sleeps,
shutdown calls, reopen probes, capability declarations, or lifecycle wrappers.

## Required Boundaries

- No Codex knowledge type, storage key, formatter, or retrieval policy is
  exported publicly.
- No root runtime dispatcher exists solely for knowledge backends.
- No StateStore import enters managed content storage, and no Database handle
  enters control-plane record DTOs.
- No generic normalizer, free ingestion helper, macro, item alias, or parallel
  source owner bypasses the shared validated source contract.
- No per-prompt client, pool, task family, or unbounded retrieval fan-out is
  created.

## Validation Obligations

Strict-live tests use real persistent Database implementations and the real
Codex prompt path to prove initial ingestion, all owning scopes, atomic
replacement, policy precedence, disabled behavior, conversation
materialization, integrity-checked drop/rebuild/reopen, prompt rendering,
cleanup obligations, and exact error propagation. Static tests protect the
StateStore/Database/public boundaries without asserting private schema or
helper spelling.
