# MCP Client And Server Research

## Sources

- <https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle>
- <https://modelcontextprotocol.io/specification/2025-11-25/basic/transports>
- <https://modelcontextprotocol.io/specification/2025-11-25/server/tools>
- <https://github.com/modelcontextprotocol/rust-sdk>
- <https://github.com/modelcontextprotocol/rust-sdk/tree/main/crates/rmcp>

## MCP Client Toolset

Verify initialization, capability negotiation, stdio/Streamable HTTP
differences, tools/list pagination and listChanged behavior, tools/call result
fidelity, empty inventories, native-name routing, interaction correlation,
request cancellation, and cleanup.

`McpClientConfig` directly owns connection, optional namespace, request
timeout, and form-validation policy. It has no source id, serving config, or
nested Toolset config.

## Independent MCP Server

McpServer serves any Toolset through public list/handler behavior. Verify:

- eager loopback Streamable HTTP startup;
- caller-known endpoint and opaque cursor-prefix validation;
- tools/list pagination;
- tools/call mapping through ToolHandler;
- model-visible error results rather than protocol failure;
- explicit interaction policy;
- one tracked task owner;
- consuming shutdown that signals and joins;
- drop or cancelled shutdown sealing admission and signalling the retained
  cleanup owner, never aborting its supervisor or detaching accepted work;
- accepted inventory, declaration, handler-construction, cancellation and
  native-session work remaining owned through settlement;
- shutdown deadlines reporting an error without discarding unfinished cleanup;
- no Toolset shutdown or source inspection.

The [portable server lifecycle](../../../../tools-kit/docs/PORTABLE_API.md#independent-mcp-server)
owns the exact cleanup contract. Asynchronous cleanup requires caller work to
yield and eventually settle and the Tokio runtime to remain alive.

Reject Toolset serve/stop, restartable server state, endpoint discovery,
agent/provider registration, or a server disguised as an MCP-client source
option.

Record the exact specification or rmcp source for every behavior decision and
separate protocol requirements from implementation choices.
