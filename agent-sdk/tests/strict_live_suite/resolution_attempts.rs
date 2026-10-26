use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::FutureExt;

use agent_sdk::notification::{
    ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver,
    NotificationResolution,
};
use agent_sdk::prompt::PromptRequest;
use agent_sdk::tool::ToolSelection;
use agent_sdk::{AgentBackend, ConversationBackend, WorkspaceBackend};
use agent_sdk::{AgentBuilder, SdkError};
use tool_sdk::Toolset;
use tool_sdk::declaration::{ToolInfo, ToolName};
use tool_sdk::execution::ToolResult;
use tool_sdk::interaction::ToolInteractionCapabilities;
use tool_sdk::local::ToolRegistration;
use tool_sdk::schema::JsonSchema;

use super::fixture::{StrictBackend, StrictFixture};

struct ResolutionScenario;

impl ResolutionScenario {
    const TERMINAL_TIMEOUT: Duration = Duration::from_secs(180);
    const REPLAY_WINDOW: Duration = Duration::from_secs(3);
    const DRAIN_TIMEOUT: Duration = Duration::from_secs(45);
    const TOOL_NAME: &'static str = "strict_resolution_attempt";

    fn toolset(completions: Arc<AtomicUsize>) -> Toolset {
        let name = ToolName::new(Self::TOOL_NAME).expect("strict-live tool name must be valid");
        let info = ToolInfo::new(
            name,
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "additionalProperties": false
            })),
        );
        let registration = ToolRegistration::interactive(info, move |_invocation, context| {
            let completions = Arc::clone(&completions);
            async move {
                let answer = context
                    .request_text("Provide the resolution acknowledgement", false)
                    .await?;
                completions.fetch_add(1, Ordering::SeqCst);
                Ok(ToolResult::text(answer))
            }
        })
        .description("Call exactly once to prove durable one-shot resolution.")
        .interactions(ToolInteractionCapabilities {
            text: true,
            ..ToolInteractionCapabilities::default()
        });
        Toolset::local(vec![registration]).expect("strict-live Toolset must build")
    }

    fn builder(
        fixture: &StrictFixture,
        config: agent_sdk::config::AgentConfig,
        id: Option<agent_sdk::identity::AgentId>,
        completions: Arc<AtomicUsize>,
    ) -> AgentBuilder {
        let builder = match id {
            Some(id) => AgentBuilder::reopen(config, fixture.storage(), id),
            None => AgentBuilder::create(config, fixture.storage()),
        };
        builder.with_toolset(Self::toolset(completions))
    }

    async fn interaction<F>(
        receiver: &mut ConversationNotificationReceiver,
        prompt: &mut Pin<Box<F>>,
    ) -> ConversationNotification
    where
        F: Future<Output = Result<agent_sdk::prompt::PromptResponse, SdkError>>,
    {
        tokio::time::timeout(Self::TERMINAL_TIMEOUT, async {
            loop {
                tokio::select! {
                    biased;
                    notification = receiver.recv() => {
                        let notification = notification
                            .expect("resolution delivery must remain open")
                            .expect("resolution notification must succeed");
                        if matches!(
                            notification.kind(),
                            ConversationNotificationKind::Interaction { .. }
                        ) {
                            break notification;
                        }
                    }
                    result = &mut *prompt => {
                        panic!("Codex prompt completed before its tool interaction: {result:?}")
                    }
                }
            }
        })
        .await
        .expect("real Codex tool interaction must arrive")
    }

    async fn prove_no_replay(
        receiver: &mut ConversationNotificationReceiver,
        resolved: &agent_sdk::identity::NotificationId,
    ) {
        let deadline = tokio::time::Instant::now() + Self::REPLAY_WINDOW;
        loop {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Err(_) => break,
                Ok(Some(Ok(notification))) => assert_ne!(
                    notification.id(),
                    resolved,
                    "resolved interaction must never replay"
                ),
                Ok(Some(Err(error))) => {
                    panic!("resolution delivery failed during replay proof: {error:?}")
                }
                Ok(None) => panic!("resolution delivery closed while runtime remained open"),
            }
        }
    }

    async fn drain(receiver: &mut ConversationNotificationReceiver) {
        tokio::time::timeout(Self::DRAIN_TIMEOUT, async {
            while let Some(notification) = receiver.recv().await {
                notification.expect("terminal drain must contain valid committed notifications");
            }
        })
        .await
        .expect("terminal notification drain must end");
    }
}

#[tokio::test]
async fn tool_interaction_resolution_terminalizes_once_and_never_replays() {
    let fixture = StrictFixture::new().await;
    let config = StrictFixture::config(
        StrictBackend::Codex,
        &StrictFixture::unique("strict-resolution-attempt"),
        Duration::from_secs(60),
    );
    let completions = Arc::new(AtomicUsize::new(0));
    let agent = ResolutionScenario::builder(&fixture, config.clone(), None, completions.clone())
        .build()
        .await
        .expect("resolution agent must build");
    let model = match std::panic::AssertUnwindSafe(async {
        StrictFixture::model(&agent, StrictBackend::Codex, "TEST_CODEX_MODEL_DEFAULT").await
    })
    .catch_unwind()
    .await
    {
        Ok(model) => model,
        Err(panic) => {
            let _ = agent.shutdown().await;
            std::panic::resume_unwind(panic);
        }
    };
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(StrictFixture::workspace(&StrictFixture::unique(
            "strict-resolution-workspace",
        )))
        .await
        .expect("resolution workspace must be created");
    let workspace_id = workspace.id();
    let conversation = workspace
        .new_conversation(StrictFixture::conversation("Resolution attempt"))
        .await
        .expect("resolution conversation must be created");
    let conversation_id = conversation.id();
    let mut receiver = conversation
        .notification_stream()
        .expect("resolution receiver must be acquired");

    let request = PromptRequest::new(format!(
        "Call the registered dynamic tool `{}` exactly once. Wait for caller input. After it returns, reply with exactly RESOLUTION_COMPLETE and no other text. Do not use a shell command or another tool.",
        ResolutionScenario::TOOL_NAME
    ))
    .expect("resolution prompt must be valid")
    .with_model(model)
    .with_tool_selection(ToolSelection::Auto)
    .expect("resolution tool selection must be valid")
    .with_timeout(ResolutionScenario::TERMINAL_TIMEOUT)
    .expect("resolution prompt timeout must be valid");
    let mut prompt = Box::pin(conversation.prompt(request));
    let interaction = ResolutionScenario::interaction(&mut receiver, &mut prompt).await;
    let interaction_id = interaction.id().clone();
    let ConversationNotificationKind::Interaction {
        response_schema,
        dismissible,
        ..
    } = interaction.kind()
    else {
        unreachable!("the selected notification is an interaction")
    };
    assert_eq!(
        response_schema,
        &Some(serde_json::json!({ "type": "string" }))
    );
    assert!(*dismissible);

    conversation
        .resolve_notification(
            interaction_id.clone(),
            NotificationResolution::Submit {
                content: serde_json::Value::String("accepted".to_owned()),
            },
        )
        .await
        .expect("matching tool interaction must resolve");
    let duplicate = conversation
        .resolve_notification(
            interaction_id.clone(),
            NotificationResolution::Submit {
                content: serde_json::Value::String("duplicate".to_owned()),
            },
        )
        .await;
    assert!(
        matches!(duplicate, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Conflict | agent_sdk::error::ErrorKind::NotFound))
    );

    let response = tokio::time::timeout(ResolutionScenario::TERMINAL_TIMEOUT, &mut prompt)
        .await
        .expect("resolved Codex prompt must reach terminal")
        .expect("resolved Codex prompt must succeed");
    assert_eq!(completions.load(Ordering::SeqCst), 1);
    ResolutionScenario::prove_no_replay(&mut receiver, &interaction_id).await;
    drop(prompt);

    drop(receiver);
    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("resolution agent must shut down before reopen");

    let reopened_completions = Arc::new(AtomicUsize::new(0));
    let reopened = ResolutionScenario::builder(
        &fixture,
        config,
        Some(agent_id),
        Arc::clone(&reopened_completions),
    )
    .build()
    .await
    .expect("resolution agent must reopen");
    let reopened_workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("resolution workspace must reopen");
    let reopened_conversation = reopened_workspace
        .get_conversation(conversation_id)
        .await
        .expect("resolution conversation must reopen");
    let mut reopened_receiver = reopened_conversation
        .notification_stream()
        .expect("reopened resolution receiver must be acquired");
    ResolutionScenario::prove_no_replay(&mut reopened_receiver, &interaction_id).await;
    assert_eq!(
        reopened_completions.load(Ordering::SeqCst),
        0,
        "reopen must not reconstruct or replay the resolved continuation"
    );

    reopened_conversation
        .close()
        .await
        .expect("reopened resolution conversation must close");
    ResolutionScenario::drain(&mut reopened_receiver).await;
    reopened_workspace
        .close()
        .await
        .expect("reopened resolution workspace must close");
    reopened
        .shutdown()
        .await
        .expect("reopened resolution agent must shut down");
    assert_eq!(response.output(), "RESOLUTION_COMPLETE");
}
