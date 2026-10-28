# Custom Agent

This document owns Custom construction, live root ownership, model discovery,
reopen, and shutdown. Exact public operations remain in [`../API.md`](../API.md).

## Ownership

A live Custom Agent owns:

- one caller-supplied `P: providers_kit::ProviderBackend` retained behind
  shared ownership without a `Clone` bound;
- process-local workspace and conversation route claims;
- its backend-private StateStore attachment and recovery owner;
- caller-supplied `T: ToolsetBackend` values and agent-owned loop and approval policy;
- optional private managed knowledge over a supplied Database;
- prompt admission, lifecycle sealing, and bounded shutdown.

Durable records are inventory authority. Runtime maps route only currently open
records and never substitute for StateStore discovery.

## Build And Reopen

`AgentBuilder::create` establishes a new identity from `AgentConfig` and
required `AgentStorage`; `AgentBuilder::reopen` requires the typed `AgentId`.
Both require callers to resupply the direct Provider implementation, Toolsets,
and optional knowledge Database needed by current runtime policy.

Custom validates passive configuration, provider model selectors, complete
tool registrations, agent-owned approval/loop policy, and knowledge
inputs before publication. It then creates or verifies the durable attachment
and private managed-knowledge facts. There is no default Provider, default
StateStore, implicit Database, durable lower-crate credential record, generic
Provider or Toolset normalization, storage
normalization, or prepublication provider loop.

Reopen compares only safe behavior-affecting facts and revalidates every
current lower handle through its owning public operations. It never
fingerprints credentials or debug output, serializes opaque handles, inspects
private lower-crate dispatchers, or branches on provider implementation ids.

## Provider And Model Boundary

Model inventory comes from the supplied Provider implementation. Custom
resolves each selected `ModelSelector` to the exact `P::Model` and performs
turns through `ModelBackend`. The Model-associated Session remains its child;
Custom adds no independent Model or Session parameter. Provider-native
discovery, credentials, wire metadata, and continuation envelopes remain
inside providers-kit.

The provider owns one model operation; Custom owns the multi-turn agent loop,
transcript advancement, tool execution, approvals, limits, cancellation,
notifications, shell-as-tool behavior, final-response detection, and durable
reconciliation. No provider capability summary is execution authority: the
actual operation either succeeds or returns its owning error.

Model listing exhausts bounded pagination, validates stable ids, rejects
duplicates and repeated cursors, maps source-neutral fields, and sorts by
selector. It never fabricates empty inventory or provider defaults.

## Shutdown

Shutdown seals admission, generation-fenced cancels or reconciles active
prompts, settles interaction waiters, releases live routes, and shuts down
owned lower handles only where the Agent contract assigns that ownership.
No lock or StateStore transaction crosses provider, tool, notification, or
timer waits. Quiescent hierarchy records remain reopenable; unfinished cleanup
is recorded as backend-private recovery state.
