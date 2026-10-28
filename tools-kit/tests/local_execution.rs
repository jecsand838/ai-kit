pub mod support;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tools_kit::{
    ToolError, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolContent, ToolResult},
    local::ToolRegistration,
    schema::JsonSchema,
};

#[derive(Deserialize, schemars::JsonSchema)]
struct EchoArgs {
    message: String,
}

#[derive(Serialize, schemars::JsonSchema)]
struct EchoOutput {
    echoed: String,
}

#[tokio::test]
async fn typed_tool_executes_and_exposes_complete_info() {
    let registration = ToolRegistration::typed("echo", |args: EchoArgs| async move {
        Ok(EchoOutput {
            echoed: args.message,
        })
    })
    .unwrap();
    let toolset = Toolset::local(vec![registration]).unwrap();

    let tools = toolset.list_tools().await.unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name.as_str(), "echo");
    assert!(tools[0].output_schema.is_some());

    let result = support::invoke_tool(&toolset, "echo", Some("call-1"), json!({"message":"hello"}))
        .await
        .unwrap();
    assert_eq!(result.structured_content, Some(json!({"echoed":"hello"})));
}

#[tokio::test]
async fn empty_local_toolsets_are_valid() {
    let toolset = Toolset::local(Vec::new()).expect("empty local inventory should be valid");
    assert!(toolset.list_tools().await.unwrap().is_empty());
}

#[tokio::test]
async fn local_toolsets_create_independent_handlers() {
    let calls = Arc::new(AtomicUsize::new(0));
    let info = ToolInfo::new(
        ToolName::new("count").unwrap(),
        JsonSchema::strict(json!({"type":"object"})),
    );
    let toolset = Toolset::local(vec![ToolRegistration::function(info, {
        let calls = Arc::clone(&calls);
        move |_| {
            let calls = Arc::clone(&calls);
            async move {
                let count = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(ToolResult::text(count.to_string()))
            }
        }
    })])
    .unwrap();

    support::invoke_tool(&toolset, "count", None, json!({}))
        .await
        .unwrap();
    support::invoke_tool(&toolset.clone(), "count", None, json!({}))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn duplicate_local_names_always_fail() {
    let name = ToolName::new("same").unwrap();
    let info = ToolInfo::new(name, JsonSchema::strict(json!({"type":"object"})));
    let first =
        ToolRegistration::function(info.clone(), |_| async { Ok(ToolResult::text("first")) });
    let second = ToolRegistration::function(info, |_| async { Ok(ToolResult::text("second")) });
    assert!(matches!(
        Toolset::local(vec![first, second]),
        Err(ToolError::DuplicateName { .. })
    ));
}

#[tokio::test]
async fn model_visible_failure_is_a_completed_result() {
    let info = ToolInfo::new(
        ToolName::new("failure").unwrap(),
        JsonSchema::strict(json!({"type":"object"})),
    );
    let toolset = Toolset::local(vec![ToolRegistration::function(info, |_| async {
        Ok(ToolResult::text("failed").with_error(true))
    })])
    .unwrap();

    let result = support::invoke_tool(&toolset, "failure", None, json!({}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(
        result.content,
        vec![ToolContent::Text {
            text: "failed".to_owned(),
            metadata: None,
        }]
    );
}

#[tokio::test]
async fn shutdown_seals_all_clones() {
    let info = ToolInfo::new(
        ToolName::new("ping").unwrap(),
        JsonSchema::strict(json!({"type":"object"})),
    );
    let toolset = Toolset::local(vec![ToolRegistration::function(info, |_| async {
        Ok(ToolResult::text("pong"))
    })])
    .unwrap();
    let clone = toolset.clone();
    clone.shutdown().await.unwrap();
    assert!(matches!(
        toolset.list_tools().await,
        Err(ToolError::Closed { .. })
    ));
    toolset.shutdown().await.unwrap();
}
