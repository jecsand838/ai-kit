use std::{
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use serde_json::json;
use tokio::sync::Notify;
use tool_sdk::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::ToolInteractionCapabilities,
    local::ToolRegistration,
    schema::JsonSchema,
};

fn tool_info(name: &str) -> ToolInfo {
    ToolInfo::new(
        ToolName::new(name).unwrap(),
        JsonSchema::strict(json!({"type":"object"})),
    )
}

struct TaskDropMarker(Arc<AtomicBool>);

impl Drop for TaskDropMarker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelling_a_ready_handler_is_an_outer_cancellation() {
    let toolset = Toolset::local(vec![ToolRegistration::function(
        tool_info("status"),
        |_| async { Ok(ToolResult::text("ok")) },
    )])
    .unwrap();
    let mut handler = toolset
        .tool_handler(ToolName::new("status").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Cancel {
                reason: Some("not needed".to_owned())
            })
            .await,
        Err(ToolError::Cancelled { .. })
    ));
}

#[tokio::test]
async fn cancelling_a_pending_interaction_aborts_the_owned_task() {
    let mut info = tool_info("ask");
    info.interactions = ToolInteractionCapabilities {
        form: false,
        form_schema_validation: false,
        text: true,
        multiline_text: false,
    };
    let toolset = Toolset::local(vec![ToolRegistration::interactive(
        info,
        |_invocation, context| async move {
            Ok(ToolResult::text(
                context.request_text("continue?", false).await?,
            ))
        },
    )])
    .unwrap();
    let mut handler = toolset
        .tool_handler(ToolName::new("ask").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Invoke(ToolInvocation {
                call_id: None,
                arguments: json!({})
            }))
            .await
            .unwrap(),
        ToolHandlerOutput::Interaction(_)
    ));
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Cancel {
                reason: Some("cancelled".to_owned())
            })
            .await,
        Err(ToolError::Cancelled { .. })
    ));
}

#[tokio::test]
async fn cancelling_after_an_interrupted_invoke_aborts_and_joins_the_owned_task() {
    let started = Arc::new(Notify::new());
    let task_dropped = Arc::new(AtomicBool::new(false));
    let toolset = Toolset::local(vec![ToolRegistration::function(tool_info("blocked"), {
        let started = Arc::clone(&started);
        let task_dropped = Arc::clone(&task_dropped);
        move |_| {
            let started = Arc::clone(&started);
            let task_dropped = Arc::clone(&task_dropped);
            async move {
                let _drop_marker = TaskDropMarker(task_dropped);
                started.notify_one();
                future::pending::<Result<ToolResult, ToolError>>().await
            }
        }
    })])
    .unwrap();
    let mut handler = toolset
        .tool_handler(ToolName::new("blocked").unwrap())
        .await
        .unwrap();

    let mut invoke = Box::pin(handler.step(ToolHandlerInput::Invoke(ToolInvocation {
        call_id: Some("interrupted-local".to_owned()),
        arguments: json!({}),
    })));
    tokio::select! {
        result = &mut invoke => panic!("blocked invocation completed unexpectedly: {result:?}"),
        () = started.notified() => {}
    }
    drop(invoke);

    assert!(
        !task_dropped.load(Ordering::SeqCst),
        "dropping the step future must leave the worker owned by the handler"
    );
    let error = handler
        .step(ToolHandlerInput::Cancel {
            reason: Some("cancel interrupted local call".to_owned()),
        })
        .await
        .expect_err("the interrupted invocation should remain cancellable");
    assert!(matches!(
        error,
        ToolError::Cancelled {
            reason: Some(reason),
            ..
        } if reason == "cancel interrupted local call"
    ));
    assert!(
        task_dropped.load(Ordering::SeqCst),
        "Cancel must abort and join the owned worker before returning"
    );
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Cancel { reason: None })
            .await,
        Err(ToolError::InvalidConfiguration { .. })
    ));
}
