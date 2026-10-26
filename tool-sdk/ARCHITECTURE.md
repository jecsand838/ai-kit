# tool-sdk Architecture

`tool-sdk` is the executable-tool boundary:

```text
Toolset -> ToolHandler
```

Built-in Toolsets are Local or MCP clients. An MCP server is a separate
caller-owned product that can expose any Toolset.

## Public Trait And Private Dispatch Topology

| Public trait | Opaque handle | Private dispatcher |
|---|---|---|
| `ToolsetBackend` | `Toolset` | `InnerToolset` |
| `ToolHandlerBackend` | `ToolHandler` | `InnerToolHandler` |

Concrete Local/MCP owners, the dispatchers, and the handles implement the same
public traits. Each dispatcher has explicit built-in variants and one erased
`Extension` arm.

Only Toolset has public custom ingress. `ToolsetBackend::Handler` lets a custom
toolset return a custom handler. Callers needing a supplied `Toolset` may use
`Toolset::from_backend`; its erased adapter privately normalizes that product
into ToolHandler. Direct consumers retain concrete toolsets and associated
handlers, including caller-owned exhaustive enums and the
[primary mock fixtures](../docs/mock-testing.md). They need no normalization.
There is no independent child extension constructor.

## Runtime Ownership

Toolset is a cloneable handle to one live source runtime. It owns inventory,
handler selection, and exactly-once shared shutdown. ToolHandler is unique and
owns one selected invocation state machine without borrowing Toolset.

The shared Toolset lifecycle is one private data-carrying enum: `Open` owns
the admission count, `Closing` owns that count plus the tracked cleanup task,
and `Closed` owns the replayable terminal result. The completion receiver and
join handle live only inside `Closing`, so no parallel field carries competing
lifecycle truth. A waiter joins the finished owner before publishing `Closed`;
cancelling one shutdown waiter cannot detach cleanup.

The public execution path is:

```text
ToolsetBackend::tool_handler(name).await
  -> ToolHandlerBackend::step(Invoke)
  -> Completed(ToolResult) | Interaction(ToolInteractionRequest)
  -> ToolHandlerBackend::step(Interaction)
```

A tool-authored failure is `Completed` with `ToolResult::is_error == true`.
Transport or state-machine failure remains an outer `ToolError`.

## Construction Ownership

`Toolset::local` accepts a vector of source-neutral ToolRegistration values.
Each registration owns complete ToolInfo plus a private handler implementation.
Function and interactive behavior are variants of one registration owner.

`Toolset::mcp` accepts one validated McpClientConfig. The MCP Toolset owns
connection initialization, inventory, native-name mapping, form interaction
correlation, remote call cancellation, and client cleanup.

No builder/config/source wrapper layer sits above these two constructors.

## Independent MCP Server

McpServer is a non-Clone tracked owner, not an InnerToolset variant and not a
Toolset lifecycle state.

```text
McpServer::start<T: ToolsetBackend>(Arc<T>, McpServerConfig)
  -> eager loopback Streamable HTTP serving
  -> public ToolsetBackend inventory/associated-handler behavior
McpServer::shutdown(self)
  -> signal + join tracked owner
```

The existing private protocol handler retains `Arc<T>` and clones only that
reference and its protocol configuration for each session. It does not require
`T: Clone` or normalize associated handlers. McpServer remains a non-generic
task owner and never shuts down the toolset. The caller retains separate
source cleanup authority. Consuming shutdown makes server terminal authority
unambiguous. Its private Serving owner seals admission under the same state
lock used to register callbacks, retains at most 64 callback tasks in a JoinSet,
and reaps them throughout serving. Protocol responses are observers of this
work. Drop and cancelled shutdown signal cancellation without aborting the
cleanup supervisor. The supervisor retains accepted lookup/construction and
Cancel futures, preserves their first cleanup failure or task panic, and joins
the HTTP graceful-close future after cancelling SSE observation. A deadline
limits the shutdown observation, not ownership of unfinished cleanup.

The private native session adapter reserves a bounded settlement slot before
creating a worker. Its separate 64-session capacity does not consume callback
slots. A unique transport transfers into that retained slot on close or Drop;
the slot removes and closes the native session handle before awaiting the exact
WorkerTransport close/join. Initialization releases the native map guard before
awaiting its response. Losing a request, initialization or shutdown observer
cannot drop an admitted native join future.

Serving drops the completed HTTP endpoint future, joins native settlement and
business tasks, then observes final protocol-reference retirement. A private
shared lifetime token is the last field in the native manager and concrete
handler, covering callback references after the native core's bounded drain.
The native outer session task's final close/logging path ends before its manager
reference retires. This is a domain-effect lifetime handoff, not possession of
rmcp's inaccessible outer JoinHandle or recovery of discarded native quit
payloads. An accepted transport dropped without its normal close records an
honest Transport failure; available close/join failures remain visible. Server
completion does not rely on task metrics, sleeps or detached cleanup.

The existing `mcp-server` feature alone enables the optional exact
`futures-core =0.3.32` dependency, without default features. It names the native
SessionManager stream bounds; the package version was already locked.

## Module Ownership

- `src/toolset.rs`: Toolset trait/handle, shared lifecycle, private root
  dispatch.
- `src/handler.rs`: Handler trait/handle and private handler dispatch.
- `src/declaration.rs`: ToolName, ToolInfo, annotations, and icons.
- `src/execution.rs`: invocation, result, content, and handler input/output.
- `src/interaction.rs`: source-neutral interaction requests and responses.
- `src/local.rs` and `src/sources/local/**`: ToolRegistration, typed-tool adapter, validation, and Local
  concrete owners.
- `src/mcp.rs` and `src/sources/mcp/**`: client config/runtime, protocol mapping, native names,
  interaction correlation, independent server/config, and tracked cleanup.
- `src/schema.rs`: schema validation and caller-facing schema values.

Provider-hosted declarations, provider request rendering, agent approvals,
storage tools, factories, and production conformance owners remain outside
this crate.

Construction is direct, inventory uses `Vec<ToolInfo>`, and serving is an
independent product. The crate has no compatibility configuration layer,
child ingress, product-level test framework, or duplicated execution result.
