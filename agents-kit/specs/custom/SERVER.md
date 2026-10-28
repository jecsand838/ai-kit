# Custom Runtime

The private Custom runtime owns live conversation routes over one retained
`Arc<P>` where `P: providers_kit::ProviderBackend`, the validated Toolset
catalog, durable server requests, and lifecycle cancellation.

Each route owns one foreground prompt fence and constructs model calls through
`P::model`, receiving `P::Model` directly. It does not own a provider driver,
mount registry, callback framework, or provider-specific loop state. The agent-owned
`CustomProviderCall` performs bounded model turns and tool execution over the
model's exact associated `Continuation`. Continuation-bearing assistant
messages remain local to that admitted call; committed route history is
provider-neutral.

## Route lifecycle

A route is published only after its durable conversation reaches `Ready`.
Creation cancellation after successful publication releases only the
process-local claim; the durable conversation remains reopenable. Reopen
creates a fresh route from committed state and resupplied lower-crate handles.

One route admits one prompt generation. Abort, deadline, Conversation close,
Workspace close, and Agent shutdown signal that generation once. A generation
retains admission until its model/tool work and durable terminal publication
settle. No accepted owner is detached.

## Resource lifecycle

The Agent owns `P` and Toolsets. Routes borrow their shared behavior and
own only unique ToolHandlers. Conversation close settles its route but does
not shut shared lower products. Agent shutdown seals routes, settles
notifications and reconciliation, shuts Toolsets in reverse order, then shuts
`P`.

Locks and StateStore transactions never cross provider, tool, database,
process, notification, or timer awaits. Terminal results preserve the first
error and settle every tracked task.
