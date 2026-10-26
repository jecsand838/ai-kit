# Custom Tool Catalog

Custom builds one complete source-neutral catalog from caller-supplied
`T: tool_sdk::ToolsetBackend` implementations. The same T is retained through
Agent, Workspace, and Conversation. Handler creation returns `T::Handler`;
neither implementation needs Clone or supplied-handle normalization.

Executable Toolsets contribute `tool_sdk::declaration::ToolInfo`. Names are
validated and unique across all executable routes. A caller-supplied shell
is an ordinary tool with caller-owned cwd/process policy; it is not a provider
executor, special loop hook, or hidden capability.

The catalog published through `Workspace::list_tools` is deterministic and
source-neutral. It contains no MCP connection details, handler values,
provider identities, credentials, or backend protocol envelopes.

## Reopen and compatibility

Durable compatibility state records the safe executable name, description,
input schema, default model and selection, approval policy, and loop limits.
Shell presence is represented by its ordinary catalog entry; no parallel
shell-shape carrier exists.

Reopen requires callers to resupply the direct Provider implementation,
Toolsets, and optional Database. Custom recomputes the safe catalog floor and
rejects a mismatch with `Conflict`. It never persists credentials, callbacks,
process environment values, or lower-crate private state.

## Lifecycle

Each accepted function call obtains a unique handler from its routed Toolset,
after the complete model response passes the classification in
[`CONVERSATION.md`](CONVERSATION.md). A completed handler result with neither
content nor structured content is `Integration`; the loop does not invent
replacement text. Valid structured-only content and model-visible tool errors
remain supported.

Agent shutdown settles live conversations, then shuts every retained Toolset
once and finally the caller's `P`. Conversation and Workspace teardown do not prematurely shut
the shared catalog. Build failure performs the same reverse-order cleanup
through tracked owners; cancellation of one waiter cannot detach accepted
cleanup.

Typed Tool failures normalize into Agent-owned errors. Tool timeouts remain
Timeout with their known duration; dependency closure and cancellation remain
Integration details rather than Agent lifecycle terminals. Reverse-order cleanup
retains the first failure and appends later failures, including Provider shutdown.
