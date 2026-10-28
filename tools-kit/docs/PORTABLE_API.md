# Portable API

Tool API is organized by functional modules rather than flattened wholesale at
the crate root.

## Public Hierarchy

```rust
pub trait ToolHandlerBackend: Send + 'static {
    fn step(&mut self, input: execution::ToolHandlerInput)
        -> impl Future<
            Output = Result<execution::ToolHandlerOutput, ToolError>,
        > + Send + '_;
}

pub trait ToolsetBackend: Send + Sync + 'static {
    type Handler: ToolHandlerBackend;

    fn list_tools(&self)
        -> impl Future<
            Output = Result<Vec<declaration::ToolInfo>, ToolError>,
        > + Send + '_;
    fn tool_handler(&self, name: declaration::ToolName)
        -> impl Future<Output = Result<Self::Handler, ToolError>> + Send + '_;
    fn shutdown(&self)
        -> impl Future<Output = Result<(), ToolError>> + Send + '_;
}
```

Toolset and ToolHandler implement their corresponding traits. Toolset is
cloneable; ToolHandler is unique and non-Clone. Import `ToolsetBackend` and
`ToolHandlerBackend` to call their behavioral methods; supplied handles have
no duplicate inherent behavioral methods.

## Custom Ingress

```rust
impl Toolset {
    pub fn from_backend<B: ToolsetBackend>(backend: B) -> Self;
}
```

The associated Handler is normalized privately by the Toolset extension
adapter. `ToolHandler::from_backend` does not exist. Built-in Local/MCP
selection creates concrete private Handler variants.

## Construction

```rust
impl Toolset {
    pub fn local(
        registrations: Vec<local::ToolRegistration>,
    ) -> Result<Self, ToolError>;

    pub async fn mcp(config: mcp::McpClientConfig) -> Result<Self, ToolError>;
}
```

Local construction accepts an empty inventory and rejects duplicate names.
`ToolRegistration` owns one complete ToolInfo and one private executable
handler variant.

`McpClientConfig` owns:

- stdio or Streamable HTTP connection;
- optional public-name namespace;
- request timeout;
- form-validation policy.

It has no source id, nested source config, serving config, or Toolset config
wrapper.

## Declaration

`ToolInfo` is the one complete model-visible declaration. It contains ToolName,
title/description, input and optional output schemas, annotations, icons, and
interaction capabilities. Inventory is returned directly as `Vec<ToolInfo>`.

MCP native names remain private and are used for protocol calls. ToolName is
the stable validated caller/provider-visible identity.

A successful result for a declaration with an output schema must contain
structured content conforming to that schema. Missing content and explicit JSON
null are distinct: missing content fails, while null is validated normally.
Model-visible error results retain their output-schema exemption. The supplied
MCP server resolves exactly one current declaration, validates arguments before
handler construction, and validates the completed result before serving it,
including when the caller supplies its own Toolset implementation.

## Execution

```text
step(Invoke)
  -> Completed(ToolResult)
  -> Interaction(ToolInteractionRequest)

step(Interaction(response))
  -> Completed(ToolResult)
  -> Interaction(next request)
```

`ToolHandlerOutput` has only `Completed` and `Interaction`. Accepted
invocation failure is:

```rust
ToolHandlerOutput::Completed(
    ToolResult::new(content)?.with_error(true)
)
```

ToolResult owns content, optional structured content, and `is_error`. It does
not repeat call id or tool name. The caller retains correlation.

Outer ToolError represents rejected input/state transitions, protocol or
transport failure, cancellation, timeout, unsupported behavior, or closed
runtime. A completed model-visible error is not duplicated inside ToolError.
When serving over MCP, only `Completed(ToolResult)` becomes a successful
`tools/call` response (including `isError = true`); outer ToolError values map
to MCP errors, with caller/request-shape failures using invalid params.

## Shared Toolset Lifecycle

The first Toolset shutdown seals listing and handler admission, starts tracked
backend cleanup once, drains admitted work, and publishes one cloneable
terminal result. Cancelling a waiter cannot cancel owner cleanup. Concurrent
and late callers observe the same result. Clone drop does not initiate async
cleanup.

These lifecycle requirements also apply when callers implement
`ToolsetBackend` directly and pass that concrete value to another crate. The
implementation owns accepted cleanup independently of its borrowed shutdown
future and of the last public handle. Consumer timeouts may end observation;
they must not abandon accepted cleanup. The implementation tracks its cleanup
task and replays one terminal result to later callers. `Toolset::from_backend`
is optional normalization, not a prerequisite for these trait guarantees.

MCP client session cleanup is bounded. A cleanup deadline is published as the
shared terminal ToolError rather than synthetic shutdown success.

The two retained `Arc::try_unwrap` paths transfer immutable registration data
during copy-on-write setup; they do not own lifecycle cleanup.

## Independent MCP Server

`McpServer` and `McpServerConfig` are available with the non-default
`mcp-server` feature.

```rust
impl mcp::McpServer {
    pub async fn start<T: ToolsetBackend>(
        toolset: std::sync::Arc<T>,
        config: mcp::McpServerConfig,
    ) -> Result<Self, ToolError>;

    pub async fn shutdown(self) -> Result<(), ToolError>;
}
```

McpServer is non-Clone and starts eagerly. Its config owns loopback address,
absolute endpoint path, pagination cursor prefix, interaction policy, and
shutdown timeout. The server routes `tools/list` and `tools/call` through
public `ToolsetBackend` and associated `ToolHandlerBackend` behavior. Its
private protocol handler retains `Arc<T>`; the public server itself is
non-generic. Neither `T` nor `T::Handler` requires `Clone` or normalization
through supplied Toolset/ToolHandler handles.

Protocol cancellation reaches the unique handler through `Cancel`. An already
started handler construction remains owned until it returns; cancellation is
checked before calling `Invoke`, including synchronous work in a caller's trait
implementation. Pending steps release their mutable borrow before cancellation
cleanup is awaited. Client elicitation admission and response observation share
one thirty-second deadline. Outstanding requests attempt protocol cancellation
before handler cleanup, with a separate one-second notification-delivery budget.
Delivery failure or timeout preserves the primary failure in the returned error.
Successful delivery acknowledges transport submission, not remote handler
settlement; a late protocol error response remains possible under the native
MCP transport's cancellation semantics.

Accepted form responses are validated before advancing the pending handler.
Missing or non-object content is rejected even when schema validation is disabled;
that option relaxes schema constraints only. A rejected response triggers awaited
handler cancellation, and cancellation failure retains the original input error.

The server admits at most 64 concurrent tool callbacks and rejects excess work
before consulting the caller's Toolset. Finished callbacks release capacity.
Native sessions have a separate limit of 64 concurrent admitted sessions.
An excess initialization is rejected before creating its native worker; closing
one selected session releases its capacity after native settlement and preserves
other sessions. Closing the server stops new sessions and tool callbacks.
Consuming shutdown seals admission and joins its tracked owner. Dropping the
server or cancelling shutdown signals that same owner; accepted inventory,
declaration, handler construction and cancellation futures remain owned through
settlement. Successful shutdown also requires settled native session work and
retirement of protocol references that could retain caller work. Losing an HTTP
initialization or shutdown observer does not discard that cleanup authority.
A shutdown deadline reports an error while cleanup remains retained.
Caller code must yield and eventually settle for asynchronous cleanup to finish;
the Tokio runtime must remain alive. Handler panic and cancellation-cleanup
failures remain visible in the server's terminal result.
Server shutdown never shuts down the toolset. Keep a caller-owned `Arc<T>` to
control that separate cleanup. Failed startup, server drop, and cancelled
server shutdown likewise leave source shutdown authority with the caller.

## Modules

- `declaration`: ToolName, ToolInfo, annotations, and icons
- `execution`: handler input/output, invocation, result, and content
- `interaction`: source-neutral request/response vocabulary
- `local`: ToolRegistration and typed local-tool construction
- `mcp`: MCP client/server configs and server handle
- `schema`: schema values and validation

The crate root contains Toolset, ToolHandler, their backend traits, and
ToolError. ToolName and every other supporting type stay in the functional
module that owns them. Construction is direct, child normalization is private,
and MCP serving has one separate tracked owner.
