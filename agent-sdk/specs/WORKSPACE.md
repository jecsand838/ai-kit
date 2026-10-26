# Portable Workspace Domain

This document owns caller-visible Workspace context, defaults, revision,
conversation inventory, tool inventory, managed knowledge, and parent close.
Exact signatures come only from [`API.md`](API.md); shared lifecycle and update
rules remain in [`RUNTIME.md`](RUNTIME.md).

## Context

A Workspace is one durable working context under an Agent. Its portable state
is the complete name, backend-host cwd, typed roots and access, explicit
sandbox/network policy, model/personality/reasoning/knowledge defaults, and
nonzero revision.

The concrete backend must represent and enforce the whole context. It may not
drop roots, reinterpret paths through the SDK host for an external service,
silently enable network, broaden filesystem access, or treat a field as
advisory. A source-proven unsupported policy fails before durable commit or
native/provider work.

Passive validation checks shape, normalization, root coherence, and explicit
policy combinations. It does not contact a backend or assert filesystem
existence. Dynamic representability and native attachment validation happen
at the owning async boundary.

## Conversation Ownership

`new_conversation`, list/info, and typed-id reopen delegate to the concrete
workspace domain. Creation resolves complete defaults and initial
conversation knowledge, prepares the backend attachment/runtime, commits
discoverable state or recovery evidence, and publishes one current-revision
handle only when all owners agree.

Inventory comes from durable ownership-valid records. Reopen verifies the
current Workspace revision, complete defaults, backend attachment, tool
compatibility, managed-knowledge references, notification/request state, and
prior work reconciliation. It never invents a replacement native session,
thread, or provider driver for a missing portable id.

## Tool And Knowledge Operations

`list_tools` returns the complete source-neutral catalog the Agent owns and can
expose in this Workspace, as defined in [`CATALOG.md`](CATALOG.md). It is not
limited to one source family and never reports a known partial catalog as
complete.

Workspace knowledge ingestion atomically replaces Workspace scope by default
and may explicitly select Database or Agent scope under the shared validation
rules. It cannot select Conversation scope or move sources when defaults or
context update.

## Revisioned Update

Preflight validates the full replacement and uses transient read admission.
It compares the durable expected revision before dynamic `Busy`, excludes only
its own admission from readiness, and reserves no later mutation right.

Update rechecks revision and activity atomically, replaces the complete
portable context/defaults, preserves the backend attachment, increments once,
and commits before invalidating prior handles. The concrete backend may rebuild
or rebind quiescent private runtime after commit but cannot change committed
success into an error.

Delete rejects active/ambiguous descendants, removes portable inventory and
quiescent children atomically, then performs backend/knowledge/tool cleanup
through recorded obligations.

## Parent Close

Workspace close seals new admission, cancels/reconciles accepted descendant
work, settles interactions, releases live native/provider/tool runtimes, and
cascade-invalidates handles. Quiescent durable conversation records,
notifications, and knowledge remain for reopen. Parent close does not perform
terminal `Conversation::close` or acknowledge caller events.
