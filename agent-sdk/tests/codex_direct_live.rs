//! Explicit one-prompt live diagnostic; no shared fixture or simulated dependency.
//! The caller owns the real Codex server and supplies a fresh Lance directory.

use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_sdk::config::{AgentConfig, CodexConfig};
use agent_sdk::conversation::NewConversation;
use agent_sdk::notification::{ConversationNotificationKind, NotificationResolution};
use agent_sdk::prompt::{PromptEvent, PromptRequest};
use agent_sdk::tool::ToolSelection;
use agent_sdk::workspace::{
    NewWorkspace, WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
    WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
};
use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, ConversationBackend, WorkspaceBackend};
use futures_util::FutureExt;
use storage_sdk::StateStore;
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use tool_sdk::Toolset;
use tool_sdk::declaration::{ToolInfo, ToolName};
use tool_sdk::execution::ToolResult;
use tool_sdk::interaction::ToolInteractionCapabilities;
use tool_sdk::local::ToolRegistration;
use tool_sdk::schema::JsonSchema;

#[tokio::test]
async fn codex_direct_interactive_answer_is_exact() {
    let endpoint = std::env::var("TEST_CODEX_WS_URL").expect("real Codex endpoint required");
    let address = endpoint
        .strip_prefix("ws://")
        .expect("direct WebSocket endpoint required")
        .parse()
        .expect("Codex socket address");
    let model_name = std::env::var("TEST_CODEX_MODEL_DEFAULT").expect("exact Codex model required");
    let directory = PathBuf::from(
        std::env::var_os("TEST_CODEX_DIRECT_STATE_DIR").expect("fresh Lance directory required"),
    );
    std::fs::create_dir(&directory).expect("diagnostic must not reuse an existing dataset");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut info = ToolInfo::new(
        ToolName::new("strict_stream_question").unwrap(),
        JsonSchema::strict(serde_json::json!({
            "type": "object",
            "additionalProperties": false
        })),
    );
    info.description = Some(
        "Ask the caller for an answer and return that answer as plain text. In Code Mode, the return value is a string, not an object with a content array. Emit the returned string directly."
            .to_owned(),
    );
    info.interactions = ToolInteractionCapabilities {
        text: true,
        multiline_text: false,
        form: false,
        form_schema_validation: false,
    };
    let question = ToolRegistration::interactive(info, {
        let calls = Arc::clone(&calls);
        move |_invocation, context| {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let answer = context
                    .request_text("Answer the strict Codex stream question", false)
                    .await?;
                Ok(ToolResult::text(answer))
            }
        }
    });
    let toolset = Toolset::local(vec![question]).expect("real caller-owned interactive tool");
    let config = AgentConfig::new(
        CodexConfig::tcp(address, Duration::from_secs(10), Duration::from_secs(90))
            .unwrap()
            .into(),
        Duration::from_secs(60),
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap()
    .with_display_name("direct-codex-live")
    .unwrap();
    let mut owned_agent = None;
    let mut owned_workspace = None;
    let mut owned_conversation = None;
    let mut owned_notifications = None;
    // Construction has SDK deadlines; the process supervisor bounds the whole
    // attempt. Never cancel a construction future before retaining its handle.
    let observed = AssertUnwindSafe(Box::pin(async {
            let state = StateStore::connect(StateStoreConfig::Lance(
                LanceStateStoreConfig::directory(&directory),
            ))
            .await
            .expect("real directory-backed Lance");
            let agent = owned_agent.insert(
                AgentBuilder::create(config, AgentStorage::new(state))
                    .with_toolset(toolset)
                    .build()
                    .await
                    .expect("real Codex Agent construction and handshake"),
            );
            let models = agent.list_models().await.expect("real native model inventory");
            let mut matching = models.iter().filter(|model| model.selector().as_str() == model_name);
            let model = matching.next().expect("configured model advertised").selector().clone();
            assert!(matching.next().is_none(), "configured model must be unique");
            let cwd = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let context = WorkspaceContext::new(
                cwd.clone(),
                vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadWrite).unwrap()],
                WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::FullAccess, WorkspaceNetworkAccess::Enabled),
            ).unwrap();
            let workspace = owned_workspace.insert(agent.new_workspace(
                NewWorkspace::new("direct-codex-workspace", context, WorkspaceDefaults::empty()).unwrap(),
            ).await.expect("real workspace"));
            let conversation = owned_conversation.insert(workspace.new_conversation(
                NewConversation::new(Some("direct interactive answer".to_owned()), None, Vec::new()).unwrap(),
            ).await.expect("fresh real conversation"));
            let notifications = owned_notifications.insert(conversation.notification_stream().unwrap());
            // Keep the failing phase's original instruction, including its tool exclusion.
            let mut stream = conversation.prompt_stream(
                PromptRequest::new("Call strict_stream_question exactly once with empty arguments and wait for its real caller-supplied answer. Then reply with exactly that answer. Do not call strict_prompt_gate or any other tool.")
                    .unwrap()
                    .with_model(model)
                    .with_tool_selection(ToolSelection::Auto).unwrap()
                    .with_timeout(Duration::from_secs(90)).unwrap(),
            ).await.expect("one real prompt admitted");
            let mut observation = Box::pin(async {
                let mut fragments = 0;
                loop {
                    match stream.recv().await.expect("authoritative terminal required").expect("real prompt output") {
                        PromptEvent::OutputDelta { text } => {
                            assert!(!text.is_empty(), "real fragments must contain text");
                            fragments += 1;
                        }
                        PromptEvent::Completed(response) => {
                            eprintln!("direct-live terminal: fragments={fragments} output_bytes={}", response.output().len());
                            assert!(fragments > 0, "real incremental output required");
                            assert!(!response.output().is_empty(), "nonempty final answer required");
                            assert!(stream.recv().await.is_none(), "terminal must fuse stream");
                            break response;
                        }
                    }
                }
            });
            let notification = tokio::time::timeout(Duration::from_secs(90), async {
                loop {
                    tokio::select! {
                        notification = notifications.recv() => {
                            let notification = notification.expect("notification channel open").expect("real notification");
                            if matches!(notification.kind(), ConversationNotificationKind::Interaction { response_schema: Some(_), .. }) {
                                break notification;
                            }
                        }
                        response = &mut observation => panic!("prompt ended before the caller interaction: {response:?}"),
                    }
                }
            }).await.expect("real interactive request within deadline");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let answer = format!("CODEX_STREAM_ANSWER-{}-{nonce}", std::process::id());
            conversation.resolve_notification(notification.id().clone(), NotificationResolution::Submit {
                content: serde_json::Value::String(answer.clone()),
            }).await.expect("caller-supplied answer accepted");
            let response = tokio::time::timeout(Duration::from_secs(90), &mut observation)
                .await.expect("authoritative completion within deadline");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "resolution must not replay the tool");
            assert_eq!(response.output(), answer, "exact caller answer, without normalization");
            eprintln!("direct-live assertions: prompts=1 tool_calls=1 exact_answer=true");
        }))
    .catch_unwind()
    .await;

    // Keep the runtime driven and attempt every owned close, even after a body
    // panic or timeout. Each public close keeps its original lifecycle deadline.
    let mut cleanup_failed = false;
    if let Some(conversation) = owned_conversation {
        let mut closing = Box::pin(AssertUnwindSafe(conversation.close()).catch_unwind());
        let result = match tokio::time::timeout(Duration::from_secs(90), &mut closing).await {
            Ok(result) => result,
            Err(timeout) => {
                cleanup_failed = true;
                eprintln!(
                    "direct-live conversation observation failed: {timeout}; retaining close"
                );
                closing.await
            }
        };
        eprintln!("direct-live conversation cleanup: {result:?}");
        cleanup_failed |= !matches!(result, Ok(Ok(())));
    }
    if let Some(mut notifications) = owned_notifications {
        let result = tokio::time::timeout(
            Duration::from_secs(90),
            AssertUnwindSafe(async {
                while let Some(notification) = notifications.recv().await {
                    notification.expect("remaining notification delivery");
                }
            })
            .catch_unwind(),
        )
        .await;
        eprintln!("direct-live notification settlement: {result:?}");
        cleanup_failed |= !matches!(result, Ok(Ok(())));
    }
    if let Some(workspace) = owned_workspace {
        let mut closing = Box::pin(AssertUnwindSafe(workspace.close()).catch_unwind());
        let result = match tokio::time::timeout(Duration::from_secs(90), &mut closing).await {
            Ok(result) => result,
            Err(timeout) => {
                cleanup_failed = true;
                eprintln!("direct-live workspace observation failed: {timeout}; retaining close");
                closing.await
            }
        };
        eprintln!("direct-live workspace cleanup: {result:?}");
        cleanup_failed |= !matches!(result, Ok(Ok(())));
    }
    if let Some(agent) = owned_agent {
        let mut closing = Box::pin(AssertUnwindSafe(agent.shutdown()).catch_unwind());
        let result = match tokio::time::timeout(Duration::from_secs(90), &mut closing).await {
            Ok(result) => result,
            Err(timeout) => {
                cleanup_failed = true;
                eprintln!("direct-live Agent observation failed: {timeout}; retaining shutdown");
                closing.await
            }
        };
        eprintln!("direct-live Agent cleanup: {result:?}");
        cleanup_failed |= !matches!(result, Ok(Ok(())));
    }
    match observed {
        Ok(()) => assert!(!cleanup_failed, "SDK cleanup must succeed"),
        Err(primary) => std::panic::resume_unwind(primary),
    }
}
