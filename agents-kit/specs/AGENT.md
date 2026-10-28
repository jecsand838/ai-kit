# Portable Agent Domain

This document owns caller-visible Agent behavior and root runtime ownership.
[`API.md`](API.md) is the exact operation/type ledger;
[`BUILD_CONFIG.md`](BUILD_CONFIG.md) owns construction; and
[`RUNTIME.md`](RUNTIME.md) owns shared hierarchy, revision, and lifecycle
semantics.

## Runtime Root

`Agent` is the opaque portable runtime root. It owns the selected backend
runtime, durable Agent identity, workspace lifecycle, model inventory,
agent/database knowledge ingestion, and one-way shutdown. It does not expose
conversation operations, protocol clients, provider handles, backend ids,
StateStore keys, tool source identity, runtime generations, or capability
objects.

The root Agent dispatch is a private exhaustive family over Codex, OpenCode,
and Custom. Each concrete agent, dispatcher, and exported Agent implements the
public `AgentBackend` contract in [`API.md`](API.md). Caller implementations
remain concrete or use caller-owned exhaustive hierarchy enums. `BackendConfig`
selects built-ins only; it is not public identity or diagnostic provenance.

## Durable Identity And Inventory

Every Agent is committed through required `AgentStorage` before publication.
`id` is stable; `info` and `AgentStorage` inventory return only committed
portable facts. New and reopened agents both own full backend runtime and use
the same operations. There is no degraded reopen handle, ephemeral
capability-only Agent, or Custom build shell.

Workspace create/list/info/get/update/delete operate through durable records
and the concrete backend's attachment owner. Runtime maps may claim open
handles but cannot become inventory authority. A missing or foreign typed id
never exposes the actual owner.

## Models And Selection

`list_models` returns the complete selectable inventory the concrete backend
can source. Selectors, supported reasoning values, supported personalities,
display names, and any sourced single Agent-wide default round-trip through
portable types. Backend/provider coordinates and raw inventory remain private.

An unsupported inventory route is `Unsupported`; a finite adapter gap is
`NotYetImplemented`; malformed selected data is `Protocol`. No backend may
return an empty vector merely because its current adapter has not implemented
the available source.

Model, reasoning, and personality selections are revalidated in the eventual
workspace/conversation context before native/provider work. Listing alone does
not reserve future availability.

## Knowledge

Agent ingestion owns Agent scope by default and explicit Database scope when
the request selects it. It delegates source validation, atomic publication,
and private managed-knowledge composition to [`KNOWLEDGE.md`](KNOWLEDGE.md). It is not
prompt execution, StateStore mutation outside required integrity references,
or automatic tool creation.

## Workspace Mutation

Workspace preflight is read-only and advisory. Update and delete recheck
durable revision and admitted activity, commit atomically, and leave
postcommit backend cleanup as private recovery. The Agent cannot publish a
workspace whose complete context, native/provider attachment, state,
knowledge, tool catalog, and recovery ownership do not agree.

## Shutdown

Shutdown seals new Agent and descendant work, dispatches generation-fenced
cancellation through each concrete backend, settles interactions and waiters,
records recovery/cleanup obligations, releases backend/tool/provider/process
resources, and consumes the public Agent. Quiescent workspaces and
conversations remain durable and reopenable.

Independent cleanup failures preserve the first public error and attach later
normalized failures in observed cleanup order, without exposing native data. No lock or StateStore transaction crosses backend, network,
provider, tool, Database, notification, process, cancellation, or timer awaits.
