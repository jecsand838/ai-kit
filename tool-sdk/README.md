# tool-sdk

`tool-sdk` executes Local and MCP tools through:

```text
Toolset -> ToolHandler
```

Construct a built-in Toolset directly:

```rust,ignore
use tool_sdk::Toolset;

let local = Toolset::local(registrations)?;
let mcp = Toolset::mcp(client_config).await?;
```

Import `ToolsetBackend` and `ToolHandlerBackend` to list `Vec<ToolInfo>`, select
one handler by `ToolName`, and drive its `step` state machine.
Tool-authored errors are completed `ToolResult` values with
`is_error = true`; outer `ToolError` values mean the invocation could not be
executed or the handler was used incorrectly.

Custom implementations implement `ToolsetBackend` and return their associated
`Handler: ToolHandlerBackend` directly. Consumers such as Agent and MCP serving
accept these concrete types. `Toolset::from_backend` optionally normalizes a
custom implementation into a supplied Toolset; children need no public
`ToolHandler::from_backend`.

With the non-default `mcp-server` feature, an MCP server is independent from
an MCP client Toolset:

```rust,ignore
use std::sync::Arc;
use tool_sdk::ToolsetBackend;

let tools = Arc::new(toolset);
let server = tool_sdk::mcp::McpServer::start(Arc::clone(&tools), server_config).await?;
server.shutdown().await?;
tools.shutdown().await?;
```

The server can expose any concrete `ToolsetBackend`, including a non-Clone
caller implementation with its own handler. It owns and joins its serving
task; the caller separately shuts down the underlying shared toolset.

See:

- [Portable API](docs/PORTABLE_API.md)
- [Architecture](ARCHITECTURE.md)
- [Strict Testing](docs/STRICT_TESTING.md)
- [Behavior Proof Policy](docs/BEHAVIOR_PROOF_POLICY.md)

Validation:

```bash
bash tool-sdk/scripts/check.sh
source tool-sdk/strict-tests.env && bash tool-sdk/scripts/verify_strict_live.sh
```

The live MCP case requires `TEST_TOOL_SDK_MCP_EXPECTED_RESULT_JSON`, a complete
independently specified successful `ToolResult`. The pinned filesystem example
reads a prepared marker file and compares exact text, structured content, and
metadata after shutdown. See [Strict Testing](docs/STRICT_TESTING.md).

## Direct public-trait tests

Run `bash scripts/check_mock_contracts.sh --crate tool-sdk` from the workspace
root for the credential-free mock lane. The workspace primary CI entrypoint is
`bash scripts/check_ci.sh`. See [mock testing](../docs/mock-testing.md) for
targets, evidence, runtime qualification, and the separate release/live gates.
