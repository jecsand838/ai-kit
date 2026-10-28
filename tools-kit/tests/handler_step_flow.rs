use serde_json::json;
use tools_kit::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{ToolInteractionCapabilities, ToolInteractionKind, ToolInteractionResponse},
    local::ToolRegistration,
    schema::JsonSchema,
};

fn interactive_toolset() -> Toolset {
    let mut info = ToolInfo::new(
        ToolName::new("ask").unwrap(),
        JsonSchema::strict(json!({"type":"object"})),
    );
    info.interactions = ToolInteractionCapabilities {
        form: false,
        form_schema_validation: false,
        text: true,
        multiline_text: false,
    };
    Toolset::local(vec![ToolRegistration::interactive(
        info,
        |_invocation, context| async move {
            let answer = context.request_text("answer?", false).await?;
            Ok(ToolResult::text(answer))
        },
    )])
    .unwrap()
}

#[tokio::test]
async fn local_handler_resumes_exact_pending_interaction() {
    let toolset = interactive_toolset();
    let mut handler = toolset
        .tool_handler(ToolName::new("ask").unwrap())
        .await
        .unwrap();
    let output = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("call".to_owned()),
            arguments: json!({}),
        }))
        .await
        .unwrap();
    let ToolHandlerOutput::Interaction(request) = output else {
        panic!("handler should request text");
    };
    assert!(matches!(request.kind, ToolInteractionKind::Text { .. }));
    let output = handler
        .step(ToolHandlerInput::Interaction(
            ToolInteractionResponse::submit(request.id, json!("accepted")),
        ))
        .await
        .unwrap();
    let ToolHandlerOutput::Completed(result) = output else {
        panic!("handler should complete");
    };
    assert_eq!(result, ToolResult::text("accepted"));
}

#[tokio::test]
async fn mismatched_interaction_id_is_rejected_without_losing_pending_state() {
    let toolset = interactive_toolset();
    let mut handler = toolset
        .tool_handler(ToolName::new("ask").unwrap())
        .await
        .unwrap();
    let ToolHandlerOutput::Interaction(request) = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: None,
            arguments: json!({}),
        }))
        .await
        .unwrap()
    else {
        panic!("handler should request text");
    };
    let error = handler
        .step(ToolHandlerInput::Interaction(
            ToolInteractionResponse::submit(Default::default(), json!("wrong")),
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, ToolError::InvalidInteraction { .. }));
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Interaction(
                ToolInteractionResponse::submit(request.id, json!("right")),
            ))
            .await
            .unwrap(),
        ToolHandlerOutput::Completed(_)
    ));
}
