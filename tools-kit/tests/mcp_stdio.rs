pub mod support;

use std::collections::BTreeMap;

use serde_json::json;
use support::mcp_stdio_harness;
use tools_kit::{
    ToolError, Toolset, ToolsetBackend,
    mcp::{McpClientConfig, McpConnectionConfig},
};

#[tokio::test]
async fn stdio_mcp_lists_and_executes_remote_tools() {
    let toolset = mcp_stdio_harness::runtime(Some("repo")).await;
    let tools = toolset.list_tools().await.expect("inventory should load");
    assert!(tools.iter().any(|tool| tool.name.as_str() == "repo__echo"));

    let result = support::invoke_tool(
        &toolset,
        "repo__echo",
        Some("call-1"),
        json!({ "message": "hello" }),
    )
    .await
    .expect("stdio MCP echo should execute");

    assert_eq!(
        result.structured_content,
        Some(json!({ "echoed": "hello" }))
    );
}

#[tokio::test]
async fn stdio_mcp_validates_arguments_and_preserves_model_visible_errors() {
    let toolset = mcp_stdio_harness::runtime(Some("repo")).await;
    let error = support::invoke_tool(
        &toolset,
        "repo__echo",
        Some("call-invalid"),
        json!({ "message": 42 }),
    )
    .await
    .expect_err("invalid MCP arguments should fail");
    assert!(matches!(
        error,
        ToolError::InvalidArguments { .. } | ToolError::Protocol { .. }
    ));

    let result = support::invoke_tool(
        &toolset,
        "repo__error_metadata",
        Some("call-error"),
        json!({}),
    )
    .await
    .expect("model-visible failure remains a completed ToolResult");
    assert!(result.is_error);
    assert_eq!(
        result.metadata.get("trace_id"),
        Some(&json!("mcp-error-result"))
    );
}

#[tokio::test]
async fn stdio_mcp_preserves_content_metadata() {
    let toolset = mcp_stdio_harness::runtime(Some("repo")).await;
    let result = support::invoke_tool(
        &toolset,
        "repo__content_metadata",
        Some("call-content"),
        json!({}),
    )
    .await
    .expect("content metadata tool should execute");

    assert_eq!(
        result.content[0],
        tools_kit::execution::ToolContent::Text {
            text: "content metadata".to_owned(),
            metadata: Some(json!({
                "content_trace": "mcp-content",
                "priority": 0.75
            })),
        }
    );
}

#[tokio::test]
async fn stdio_mcp_shutdown_closes_the_owned_server_session() {
    let marker = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let config = mcp_stdio_harness::stdio_source(Some("repo"), Some(&marker), 2);
    let toolset = Toolset::mcp(config)
        .await
        .expect("stdio MCP toolset should build");

    toolset.shutdown().await.expect("shutdown should close MCP");
    assert_eq!(
        std::fs::read(&marker).expect("exit marker should be written"),
        b"closed"
    );
}

#[test]
fn mcp_client_rejects_an_empty_namespace() {
    let error = McpClientConfig::new(McpConnectionConfig::Stdio {
        program: "unused".to_owned(),
        args: Vec::new(),
        cwd: None,
        env: BTreeMap::new(),
    })
    .with_namespace("")
    .expect_err("empty namespace should fail");

    assert!(matches!(error, ToolError::InvalidConfiguration { .. }));
}
