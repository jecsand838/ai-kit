pub mod support;

use std::{path::Path, time::Duration};

use serde_json::json;
use support::mcp_stdio_harness;
use tool_sdk::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::ToolName,
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{
        ToolInteractionId, ToolInteractionKind, ToolInteractionRequest, ToolInteractionResponse,
    },
};

#[tokio::test]
async fn mcp_form_submit_resumes_and_completes() {
    let toolset = form_toolset().await;
    let mut handler = form_handler(&toolset).await;
    let request = invoke_until_form(&mut handler, Some("form-submit")).await;

    assert_eq!(request.tool_name.as_str(), "repo__form_elicitation");
    assert_eq!(request.call_id.as_deref(), Some("form-submit"));
    assert_eq!(request.prompt, "Choose a form option");
    let ToolInteractionKind::Form {
        schema,
        schema_validation,
    } = &request.kind
    else {
        panic!("expected MCP form interaction");
    };
    assert!(*schema_validation);
    assert_eq!(schema["type"], "object");

    let completed = handler
        .step(ToolHandlerInput::Interaction(
            ToolInteractionResponse::submit(
                request.id,
                json!({ "choice": "yes", "note": "from test" }),
            ),
        ))
        .await
        .expect("submitted MCP form should resume the call");
    let ToolHandlerOutput::Completed(result) = completed else {
        panic!("expected completed MCP form result");
    };
    assert_eq!(
        result.structured_content,
        Some(json!({
            "action": "accept",
            "content": {
                "choice": "yes",
                "note": "from test"
            }
        }))
    );
}

#[tokio::test]
async fn mcp_form_decline_and_cancel_map_to_protocol_actions() {
    let declined = complete_form_with(ToolInteractionResponse::decline).await;
    assert_eq!(
        declined.structured_content,
        Some(json!({ "action": "decline", "content": null }))
    );

    let cancelled = complete_form_with(ToolInteractionResponse::cancel).await;
    assert_eq!(
        cancelled.structured_content,
        Some(json!({ "action": "cancel", "content": null }))
    );
}

#[tokio::test]
async fn mcp_form_id_mismatch_preserves_pending_interaction() {
    let toolset = form_toolset().await;
    let mut handler = form_handler(&toolset).await;
    let request = invoke_until_form(&mut handler, Some("form-mismatch")).await;

    let error = handler
        .step(ToolHandlerInput::Interaction(
            ToolInteractionResponse::submit(
                ToolInteractionId::new(),
                json!({ "choice": "wrong-id" }),
            ),
        ))
        .await
        .expect_err("mismatched interaction id should be rejected");
    assert!(matches!(error, ToolError::InvalidInteraction { .. }));

    let completed = handler
        .step(ToolHandlerInput::Interaction(
            ToolInteractionResponse::submit(request.id, json!({ "choice": "correct-id" })),
        ))
        .await
        .expect("pending interaction should remain resumable");
    assert!(matches!(completed, ToolHandlerOutput::Completed(_)));
}

#[tokio::test]
async fn dropping_or_cancelling_pending_handler_releases_the_serial_session() {
    let toolset = form_toolset().await;
    let mut dropped = form_handler(&toolset).await;
    let _request = invoke_until_form(&mut dropped, Some("form-drop")).await;
    drop(dropped);

    let mut after_drop = form_handler(&toolset).await;
    tokio::time::timeout(
        Duration::from_secs(1),
        invoke_until_form(&mut after_drop, Some("after-drop")),
    )
    .await
    .expect("dropped handler must release the session");
    drop(after_drop);

    let mut cancelled = form_handler(&toolset).await;
    let _request = invoke_until_form(&mut cancelled, Some("form-cancel")).await;
    let error = cancelled
        .step(ToolHandlerInput::Cancel {
            reason: Some("release session".to_owned()),
        })
        .await
        .expect_err("cancellation is an outer lifecycle failure");
    assert!(matches!(error, ToolError::Cancelled { .. }));

    let mut after_cancel = form_handler(&toolset).await;
    tokio::time::timeout(
        Duration::from_secs(1),
        invoke_until_form(&mut after_cancel, Some("after-cancel")),
    )
    .await
    .expect("cancelled handler must release the session");
}

#[tokio::test]
async fn cancellation_sends_the_remote_cancel_notification() {
    let marker = tempfile::NamedTempFile::new()
        .expect("cancel marker should be created")
        .into_temp_path();
    let marker_path = marker.to_path_buf();
    let config = mcp_stdio_harness::stdio_source_with_cancel_marker(Some("repo"), &marker_path, 2)
        .with_form_validation(true);
    let toolset = Toolset::mcp(config)
        .await
        .expect("form-enabled stdio MCP toolset should build");
    let mut handler = toolset
        .tool_handler(ToolName::new("repo__cancel_observer").unwrap())
        .await
        .expect("cancel observer handler should build");

    let request = invoke_until_form(&mut handler, Some("cancel-observer")).await;
    assert_eq!(request.prompt, "Hold until cancellation");
    let error = handler
        .step(ToolHandlerInput::Cancel {
            reason: Some("caller requested remote cancel".to_owned()),
        })
        .await
        .expect_err("cancelling a pending call is an outer failure");
    assert!(matches!(
        error,
        ToolError::Cancelled {
            reason: Some(reason),
            ..
        } if reason == "caller requested remote cancel"
    ));

    let marker = wait_for_cancel_marker(&marker_path).await;
    assert!(marker.contains("request_id="));
    assert!(marker.contains("reason=caller requested remote cancel"));
}

#[tokio::test]
async fn cancellation_after_an_interrupted_invoke_reaches_the_remote_request() {
    let marker = tempfile::NamedTempFile::new()
        .expect("cancel marker should be created")
        .into_temp_path();
    let marker_path = marker.to_path_buf();
    let config = mcp_stdio_harness::stdio_source_with_cancel_marker(Some("repo"), &marker_path, 2);
    let toolset = Toolset::mcp(config)
        .await
        .expect("stdio MCP toolset should build");
    let mut handler = toolset
        .tool_handler(ToolName::new("repo__blocking_cancel_observer").unwrap())
        .await
        .expect("blocking observer handler should build");

    let mut invoke = Box::pin(handler.step(ToolHandlerInput::Invoke(ToolInvocation {
        call_id: Some("interrupted-mcp".to_owned()),
        arguments: json!({}),
    })));
    tokio::select! {
        result = &mut invoke => panic!("blocking MCP call completed unexpectedly: {result:?}"),
        marker = wait_for_marker_text(&marker_path, "started_request_id=") => {
            assert!(marker.contains("started_request_id="));
        }
    }
    drop(invoke);

    let error = handler
        .step(ToolHandlerInput::Cancel {
            reason: Some("cancel interrupted MCP call".to_owned()),
        })
        .await
        .expect_err("the interrupted MCP invocation should remain cancellable");
    assert!(matches!(
        error,
        ToolError::Cancelled {
            reason: Some(reason),
            ..
        } if reason == "cancel interrupted MCP call"
    ));
    let marker = wait_for_marker_text(&marker_path, "reason=").await;
    assert!(marker.contains("request_id="));
    assert!(marker.contains("reason=cancel interrupted MCP call"));
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Cancel { reason: None })
            .await,
        Err(ToolError::InvalidConfiguration { .. })
    ));
}

async fn complete_form_with(
    make_response: fn(ToolInteractionId, Option<String>) -> ToolInteractionResponse,
) -> ToolResult {
    let toolset = form_toolset().await;
    let mut handler = form_handler(&toolset).await;
    let request = invoke_until_form(&mut handler, None).await;
    let completed = handler
        .step(ToolHandlerInput::Interaction(make_response(
            request.id, None,
        )))
        .await
        .expect("MCP form decision should return to the server");
    let ToolHandlerOutput::Completed(result) = completed else {
        panic!("expected completed MCP decision result");
    };
    result
}

async fn form_toolset() -> Toolset {
    let config = mcp_stdio_harness::stdio_source(Some("repo"), None, 2).with_form_validation(true);
    Toolset::mcp(config)
        .await
        .expect("form-enabled stdio MCP toolset should build")
}

async fn form_handler(toolset: &Toolset) -> tool_sdk::ToolHandler {
    toolset
        .tool_handler(ToolName::new("repo__form_elicitation").unwrap())
        .await
        .expect("form elicitation handler should build")
}

async fn invoke_until_form(
    handler: &mut tool_sdk::ToolHandler,
    call_id: Option<&str>,
) -> ToolInteractionRequest {
    let output = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: call_id.map(str::to_owned),
            arguments: json!({}),
        }))
        .await
        .expect("MCP form tool should pause for interaction");
    let ToolHandlerOutput::Interaction(request) = output else {
        panic!("expected MCP form interaction");
    };
    request
}

async fn wait_for_cancel_marker(path: &Path) -> String {
    wait_for_marker_text(path, "reason=").await
}

async fn wait_for_marker_text(path: &Path, expected: &str) -> String {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(contents) = tokio::fs::read_to_string(path).await
                && contents.contains(expected)
            {
                return contents;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("MCP cancelled notification marker should be written")
}
