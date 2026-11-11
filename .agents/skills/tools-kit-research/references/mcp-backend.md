# MCP Client And Server Research

## Sources

- <https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle>
- <https://modelcontextprotocol.io/specification/2025-11-25/basic/transports>
- <https://modelcontextprotocol.io/specification/2025-11-25/server/tools>
- <https://github.com/modelcontextprotocol/rust-sdk>
- <https://github.com/modelcontextprotocol/rust-sdk/tree/main/crates/rmcp>
- Selected security remediation source: crates.io `rmcp 3.9.0`, archive SHA-256
  `d24a7e92f4bcaa36b97fa93c4fa7ca8f3e669eced949345b3462dba333c7f909`,
  upstream commit `5596a8a0bba2a1ec5519cc855cdadf23e4dcecdc`.

For this selected source, inspect `src/transport/common/client_side_http.rs`
for JSON/error response bounds; `src/service/client.rs`, `src/handler/server.rs`
and `src/model.rs` for offered, selected and supported protocol revisions;
`src/model/content.rs` for metadata conversion; and
`src/transport/streamable_http_server/session/local.rs` for native worker and
map-guard ownership. The default protocol revision is newer than Kit's retained
initialize ceiling. Do not equate using initialize with offering only the
supported revisions, or adopt newer stateless behavior during remediation.
In the selected HTTP transport, complete per-request metadata selects stateless
handling even for a retained revision and an existing session. Its required set
contains protocolVersion and clientCapabilities; clientInfo is optional.
Discovery has a separate direct path. Verify the pre-routing HTTP policy as well
as revision negotiation; a handler-only rejection occurs after native direct
service tasks can already have been created.

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
