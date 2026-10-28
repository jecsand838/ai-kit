# Custom Backend

Custom is the provider-backed implementation of the public
`Agent -> Workspace -> Conversation` hierarchy. Public signatures remain in
[`../API.md`](../API.md).

## Ownership

Custom owns durable hierarchy identity, prompt fencing, the private multi-turn
loop, notification delivery and resolution, recovery, managed-knowledge
coordination, tool execution, and deterministic cleanup. providers-kit owns
provider credentials, selected-model operations, wire adaptation, and the
Model-associated continuation contract. tools-kit owns Toolset and ToolHandler
behavior.

The private Custom runtime resolves `P: ProviderBackend -> P::Model`, sends one
normalized model turn at a time, executes requested functions through supplied
Toolsets, and advances the transcript until a terminal response or bounded
limit. Each returned assistant message retains the exact
`P::Model::Continuation` for that prompt only; Custom never inspects,
serializes, or normalizes it. It is not a public service, transport, driver framework, mount registry,
or second conversation authority.

## Domain Contracts

- [`AGENT.md`](AGENT.md): root construction, model inventory, reopen, shutdown.
- [`WORKSPACE.md`](WORKSPACE.md): workspace revision and conversation creation.
- [`CONVERSATION.md`](CONVERSATION.md): prompt, abort, reopen, notification, close.
- [`SERVER.md`](SERVER.md): private model-turn loop and route ownership.
- [`CATALOG.md`](CATALOG.md): Toolset catalog and execution.
- [`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md): private managed knowledge.
- [`NOTIFICATIONS.md`](NOTIFICATIONS.md): typed fact mapping.
- [`SERVER_REQUESTS.md`](SERVER_REQUESTS.md): actionable continuations.
- [`STATE_STORE.md`](STATE_STORE.md): durable records and recovery.

Custom supports the portable hierarchy, provider-backed model selection,
foreground prompt and abort, complete catalog rules, structured output,
agent-owned approvals, caller-owned shell tools, optional managed knowledge,
durable notification resolution, reopen, and lifecycle closure. A selected
model may reject an unsupported operation honestly; Custom does not fabricate
fallback behavior.

Provider implementation identity is absent. Model-native coordinates, wire
messages, tool source identities, handlers, runtime routes, and persistence
records remain private.
