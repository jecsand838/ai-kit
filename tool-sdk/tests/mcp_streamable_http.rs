#![cfg(feature = "mcp-server")]

pub mod support;

use serde_json::json;
use support::mcp_http_harness;
use tool_sdk::{
    ToolError, ToolHandlerBackend, ToolsetBackend,
    declaration::ToolName,
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation},
};

#[tokio::test]
async fn streamable_http_mcp_lists_and_executes_remote_tools() {
    let http_server = mcp_http_harness::HttpServerHandle::spawn().await;
    let toolset = http_server.runtime(Some("repo")).await;
    let tools = toolset.list_tools().await.expect("inventory should load");
    assert!(tools.iter().any(|tool| tool.name.as_str() == "repo__echo"));

    let result = support::invoke_tool(
        &toolset,
        "repo__echo",
        Some("call-echo"),
        json!({ "message": "hello" }),
    )
    .await
    .expect("Streamable HTTP MCP echo should execute");
    assert_eq!(
        result.structured_content,
        Some(json!({ "echoed": "hello" }))
    );
}

#[tokio::test]
async fn streamable_http_validates_arguments_and_preserves_model_visible_errors() {
    let http_server = mcp_http_harness::HttpServerHandle::spawn().await;
    let toolset = http_server.runtime(Some("repo")).await;

    let error = support::invoke_tool(
        &toolset,
        "repo__echo",
        Some("call-invalid"),
        json!({ "message": 42 }),
    )
    .await
    .expect_err("invalid arguments should fail");
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
async fn streamable_http_mcp_shutdown_closes_owned_client_session() {
    let http_server = mcp_http_harness::HttpServerHandle::spawn().await;
    let toolset = http_server.runtime(Some("repo")).await;
    toolset
        .shutdown()
        .await
        .expect("HTTP MCP shutdown should close");
}

#[tokio::test]
async fn streamable_http_shutdown_is_bounded_when_session_deletion_stalls() {
    let http_server = mcp_http_harness::HttpServerHandle::spawn_with_options(
        mcp_http_harness::HttpServerOptions {
            stall_delete_session: true,
            ..Default::default()
        },
    )
    .await;
    let config = http_server.source(Some("repo")).with_form_validation(true);
    let toolset = tool_sdk::Toolset::mcp(config)
        .await
        .expect("form-enabled HTTP MCP toolset should build");
    let mut handler = toolset
        .tool_handler(ToolName::new("repo__form_elicitation").unwrap())
        .await
        .expect("form handler should build");
    let output = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("pending-at-shutdown".to_owned()),
            arguments: json!({}),
        }))
        .await
        .expect("form call should pause for interaction");
    assert!(matches!(output, ToolHandlerOutput::Interaction(_)));

    let error = tokio::time::timeout(std::time::Duration::from_secs(6), toolset.shutdown())
        .await
        .expect("MCP client shutdown must honor its five-second cleanup bound")
        .expect_err("timed-out MCP session cleanup must not report success");
    assert!(matches!(
        &error,
        ToolError::Transport { tool: None, message }
            if message.contains("timed out") && message.contains("5s")
    ));
    assert_eq!(toolset.shutdown().await, Err(error));
}

#[tokio::test]
async fn output_schema_mcp_client_rejects_missing_structured_content_from_native_server() {
    // The independent native fixture deliberately returns malformed MCP success;
    // shipped McpServer validation cannot mask the MCP client's own obligation.
    let server = mcp_http_harness::HttpServerHandle::spawn().await;
    let connection = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tool_sdk::Toolset::mcp(server.source(Some("repo"))),
    )
    .await;
    let tools = match connection {
        Ok(Ok(tools)) => tools,
        failure => {
            let cleanup = server.shutdown().await;
            panic!("native MCP setup failed: {failure:?}; fixture cleanup={cleanup:?}");
        }
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        support::invoke_tool(
            &tools,
            "repo__invalid_output",
            Some("missing-structured"),
            json!({"omit_structured":true}),
        ),
    )
    .await;
    let shutdown = tokio::time::timeout(std::time::Duration::from_secs(6), tools.shutdown()).await;
    let server_cleanup = server.shutdown().await;
    let result = result.expect("native MCP call deadline after cleanup");
    assert!(
        matches!(result, Err(ToolError::InvalidOutput { .. })),
        "MCP client must enforce the declared output schema: {result:?}; client cleanup={shutdown:?}; server cleanup={server_cleanup:?}"
    );
    assert!(
        matches!(shutdown, Ok(Ok(()))) && server_cleanup.is_ok(),
        "client and native fixture must settle after malformed output"
    );
}
