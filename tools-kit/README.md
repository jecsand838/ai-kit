# tools-kit

This crate is in early development. Backwards compatibility is not guaranteed
until `1.0.0`; pin exact versions and review the
[release notes](https://github.com/jecsand838/ai-kit/releases) before upgrading.

Repository guides and examples linked below track `main`.

`tools-kit` executes Local and MCP tools through:

```text
Toolset -> ToolHandler
```

Construct a built-in Toolset with `Toolset::local(registrations)` or
`Toolset::mcp(client_config).await`. The canonical Rustdoc sources for
[Local registration and invocation](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/src/lib.rs)
and [MCP clients](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/src/mcp.rs)
show complete setup, execution, and shared shutdown, including failure paths.

Import `ToolsetBackend` and `ToolHandlerBackend` to list `Vec<ToolInfo>`, select
one handler by `ToolName`, and drive its `step` state machine.
Tool-authored errors are completed `ToolResult` values with
`is_error = true`. Outer `ToolError` values cover input/output validation, state,
protocol, transport, cancellation, timeout, and lifecycle failures. They can
follow execution, including validation of a completed result, and do not prove
that no side effects occurred.

Custom implementations implement `ToolsetBackend` and return their associated
`Handler: ToolHandlerBackend` directly. Consumers such as Agent and MCP serving
accept these concrete types. `Toolset::from_backend` optionally normalizes a
custom implementation into a supplied Toolset; children need no public
`ToolHandler::from_backend`.

With the non-default `mcp-server` feature, an MCP server is independent from
an MCP client Toolset:

```rust,ignore
use std::sync::Arc;
use tools_kit::ToolsetBackend;

let tools = Arc::new(toolset);
let serving = async {
    let server = tools_kit::mcp::McpServer::start(Arc::clone(&tools), server_config).await?;
    server.shutdown().await
}.await;
let source_cleanup = tools.shutdown().await;
if let Err(error) = &source_cleanup {
    eprintln!("Source cleanup failed: {error}");
}
serving?;
source_cleanup?;
```

The server can expose any concrete `ToolsetBackend`, including a non-Clone
caller implementation with its own handler. It owns and joins its serving
task; the caller separately shuts down the underlying shared toolset.
Settle both cleanup calls before propagating their results, reporting a source
cleanup failure even when serving already failed. Keep the Tokio runtime driven
until retained cleanup settles. The canonical
[`mcp::McpServer` Rustdoc source](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/src/sources/mcp/server/mod.rs) includes a complete
client/server/source invocation and cleanup sequence.

See:

- [Portable API](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/docs/PORTABLE_API.md)
- [Architecture](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/ARCHITECTURE.md)
- [Strict Testing](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/docs/STRICT_TESTING.md)
- [Behavior Proof Policy](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/docs/BEHAVIOR_PROOF_POLICY.md)

Validation:

```bash
bash tools-kit/scripts/check.sh
source tools-kit/strict-tests.env && bash tools-kit/scripts/verify_strict_live.sh
```

The live MCP case requires `TEST_TOOLS_KIT_MCP_EXPECTED_RESULT_JSON`, a complete
independently specified successful `ToolResult`. The pinned filesystem example
reads a prepared marker file and compares exact text, structured content, and
metadata after shutdown. See [Strict Testing](https://github.com/jecsand838/ai-kit/blob/main/tools-kit/docs/STRICT_TESTING.md).

## Direct public-trait tests

Run `bash scripts/check_mock_contracts.sh --crate tools-kit` from the workspace
root for the credential-free mock lane. The workspace primary CI entrypoint is
`bash scripts/check_ci.sh`. See [mock testing](https://github.com/jecsand838/ai-kit/blob/main/docs/mock-testing.md) for
targets, evidence, runtime qualification, and the separate release/live gates.
