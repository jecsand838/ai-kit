//! Real Custom Agent composition with process-local direct public-trait implementations.
//! No lower Kit product, extension ingress, process, socket, or credential is used.
// Nested direct-trait async composition exceeds the default compiler depth on Linux.
#![recursion_limit = "256"]

mod mock_database;
mod mock_memory;
mod mock_runtime;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
use agents_kit::conversation::NewConversation;
use agents_kit::error::{ErrorDetail, ErrorKind, ErrorPhase};
use agents_kit::model::ModelSelector;
use agents_kit::notification::{ConversationNotificationKind, NotificationResolution};
use agents_kit::prompt::{PromptEvent, PromptRequest, StructuredOutputRequest, UsageInfo};
use agents_kit::tool::ToolSelection;
use agents_kit::workspace::{
    NewWorkspace, UpdateWorkspaceRequest, WorkspaceContext, WorkspaceDefaults,
    WorkspaceExecutionPolicy, WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess,
    WorkspaceSandboxMode,
};
use agents_kit::{
    Agent, AgentBackend, AgentBuilder, AgentStorage, Conversation, ConversationBackend, Workspace,
    WorkspaceBackend,
};
use providers_kit::generation::{ContentPart, FinishReason, GenerateEvent, Role};
use providers_kit::{ModelBackend, ProviderBackend};
use tools_kit::declaration::ToolName;
use tools_kit::execution::{ToolHandlerInput, ToolInvocation};
use tools_kit::{ToolHandlerBackend, ToolsetBackend};

use mock_memory::{CommitOutcome, Control, MemoryStore};
use mock_runtime::{
    CallerTools, Continuation, Gate, MODEL, MockProvider, MockTools, ProviderControl, ProviderStep,
    TOOL, ToolCancellation, ToolControl, ToolOutcome, ToolScript,
};

struct Directory {
    path: PathBuf,
}

impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("agent-direct-mocks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temporary workspace");
        Self {
            path: path.canonicalize().expect("canonical workspace"),
        }
    }

    fn workspace_request(&self, name: &str) -> NewWorkspace {
        // Custom requires this declared policy for caller-built executable tools.
        // These fixtures have no transport; CI independently restricts OS networking.
        let context = WorkspaceContext::new(
            self.path.clone(),
            vec![
                WorkspaceRoot::new(self.path.clone(), WorkspaceRootAccess::ReadWrite)
                    .expect("root"),
            ],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .expect("context");
        NewWorkspace::new(name, context, WorkspaceDefaults::empty()).expect("workspace request")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).expect("remove temporary workspace");
    }
}

struct Fixture<T: ToolsetBackend> {
    agent: Agent<MemoryStore, storage_kit::Database, MockProvider, T>,
    workspace: Workspace<MemoryStore, storage_kit::Database, MockProvider, T>,
    conversation: Conversation<MemoryStore, storage_kit::Database, MockProvider, T>,
    provider: Arc<ProviderControl>,
    tools: Arc<ToolControl>,
    state: Control,
    directory: Directory,
}

enum AbortPause {
    Committed(Arc<mock_memory::CommitGate>),
    BeforeRead(Arc<mock_memory::StateGate>),
}

enum AbortIntentObservation {
    ChangedAfterTerminal,
    FailureAfterTerminal,
    ConflictWhileActive,
}

impl Fixture<MockTools> {
    async fn abort_reconciliation_observes(observation: AbortIntentObservation) {
        let fixture = Self::new(
            vec![
                ProviderStep::unary(ProviderControl::function(&Continuation::new())),
                ProviderStep::unary(ProviderControl::response("completed naturally")),
            ],
            vec![ToolOutcome::Interaction],
        )
        .await;
        let mut receiver = fixture
            .conversation
            .notification_stream()
            .expect("receiver");
        let mut prompt = Box::pin(
            fixture
                .conversation
                .prompt(PromptRequest::new("complete during abort observation").expect("request")),
        );
        let notification = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    result = &mut prompt => panic!("prompt settled before interaction: {result:?}"),
                    item = receiver.recv() => {
                        let item = item.expect("notification source").expect("notification");
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break item;
                        }
                    }
                }
            }
        })
        .await
        .expect("bounded interaction");
        let commit = mock_memory::CommitGate::new();
        fixture
            .state
            .commit_as(CommitOutcome::Admitted(Arc::clone(&commit)));
        let mut abort = Box::pin(fixture.conversation.abort());
        tokio::select! {
            () = commit.entered() => {},
            result = &mut abort => panic!("abort settled before intent commit: {result:?}"),
        }
        let read = mock_memory::StateGate::new();
        let step = match observation {
            AbortIntentObservation::ChangedAfterTerminal => {
                mock_memory::LoadStep::Current(Arc::clone(&read))
            }
            AbortIntentObservation::FailureAfterTerminal => mock_memory::LoadStep::Failure {
                gate: Arc::clone(&read),
                error: storage_kit::StateStoreError::unavailable(
                    "load",
                    "scripted observation failure",
                ),
            },
            AbortIntentObservation::ConflictWhileActive => mock_memory::LoadStep::Failure {
                gate: Arc::clone(&read),
                error: storage_kit::StateStoreError::transaction_conflict(
                    "load",
                    "scripted observation conflict",
                ),
            },
        };
        fixture.state.load_as(step);
        fixture.state.settle_admitted();
        read.entered().await;
        // The intent is published, but its reconciliation observation has not
        // happened. Let the actual prompt owner publish a different terminal
        // record, without fabricating or inspecting any private stored value.
        let early_completion = if matches!(observation, AbortIntentObservation::ConflictWhileActive)
        {
            None
        } else {
            fixture
                .conversation
                .resolve_notification(
                    notification.id().clone(),
                    NotificationResolution::Submit {
                        content: serde_json::json!("42"),
                    },
                )
                .await
                .expect("resolve before reconciliation");
            Some(
                tokio::time::timeout(Duration::from_secs(3), &mut prompt)
                    .await
                    .expect("bounded natural terminal"),
            )
        };
        read.release();
        let aborted = tokio::time::timeout(Duration::from_secs(3), &mut abort)
            .await
            .expect("bounded abort observation");
        drop(abort);
        let completed = match early_completion {
            Some(completed) => completed,
            None => {
                fixture
                    .conversation
                    .resolve_notification(
                        notification.id().clone(),
                        NotificationResolution::Submit {
                            content: serde_json::json!("42"),
                        },
                    )
                    .await
                    .expect("unexplained conflict leaves interaction resolvable");
                tokio::time::timeout(Duration::from_secs(3), &mut prompt)
                    .await
                    .expect("bounded completion after abort failure")
            }
        };
        drop(prompt);
        drop(receiver);
        fixture
            .conversation
            .close()
            .await
            .expect("conversation cleanup");
        fixture.workspace.close().await.expect("workspace cleanup");
        fixture.agent.shutdown().await.expect("agent cleanup");
        match observation {
            AbortIntentObservation::ChangedAfterTerminal => {
                aborted.expect("captured terminal proves intent conflict is obsolete")
            }
            AbortIntentObservation::FailureAfterTerminal => {
                let error = aborted.expect_err("terminal proof cannot erase non-conflict failure");
                assert_eq!(error.kind(), ErrorKind::Integration);
                assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                assert_eq!(error.message(), "conversation state could not be loaded");
            }
            AbortIntentObservation::ConflictWhileActive => {
                let error = aborted.expect_err("unexplained intent conflict remains an error");
                assert_eq!(error.kind(), ErrorKind::Conflict);
                assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                assert_eq!(error.message(), "conversation state could not be loaded");
            }
        }
        assert_eq!(
            completed
                .expect("natural completion survives failed observation")
                .output(),
            "completed naturally"
        );
        assert_eq!(fixture.tools.cancellations(), 0);
        assert_eq!(fixture.tools.interactions().len(), 1);
        assert_eq!(fixture.tools.invocations().len(), 1);
        assert_eq!(fixture.provider.requests().len(), 2);
        fixture.tools.assert_consumed();
        fixture.provider.assert_consumed();
        fixture.state.assert_consumed();
        assert_eq!(fixture.tools.shutdowns(), 1);
        assert_eq!(fixture.provider.shutdowns(), 1);
    }

    async fn delayed_abort_preserves_successor(pause: AbortPause) {
        let fixture = Self::new(
            vec![
                ProviderStep::unary(ProviderControl::function(&Continuation::new())),
                ProviderStep::unary(ProviderControl::response("first complete")),
                ProviderStep::unary(ProviderControl::function(&Continuation::new())),
                ProviderStep::unary(ProviderControl::response("successor complete")),
            ],
            vec![ToolOutcome::Interaction, ToolOutcome::Interaction],
        )
        .await;
        let mut receiver = fixture
            .conversation
            .notification_stream()
            .expect("receiver");
        let mut first = Box::pin(
            fixture
                .conversation
                .prompt(PromptRequest::new("first").expect("request")),
        );
        let notification = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    result = &mut first => panic!("first settled before interaction: {result:?}"),
                    item = receiver.recv() => {
                        let item = item.expect("notification source").expect("notification");
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break item;
                        }
                    }
                }
            }
        })
        .await
        .expect("bounded first interaction");
        match &pause {
            AbortPause::Committed(gate) => fixture
                .state
                .commit_as(CommitOutcome::Admitted(Arc::clone(gate))),
            AbortPause::BeforeRead(gate) => fixture.state.gate_next_begin(Arc::clone(gate)),
        }
        let mut delayed_abort = Box::pin(fixture.conversation.abort());
        tokio::select! {
            () = async {
                match &pause {
                    AbortPause::Committed(gate) => gate.entered().await,
                    AbortPause::BeforeRead(gate) => gate.entered().await,
                }
            } => {},
            result = &mut delayed_abort => panic!("abort settled before gated intent: {result:?}"),
        }
        if matches!(pause, AbortPause::Committed(_)) {
            fixture.state.settle_admitted();
        }
        // Keep the old abort observer unpolled while its captured generation
        // finishes independently of cancellation intent observation.
        fixture
            .conversation
            .resolve_notification(
                notification.id().clone(),
                NotificationResolution::Submit {
                    content: serde_json::json!("42"),
                },
            )
            .await
            .expect("first interaction remains independently resolvable");
        let first_result = tokio::time::timeout(Duration::from_secs(3), &mut first)
            .await
            .expect("bounded first completion");
        drop(first);
        let early_stale_result = match &pause {
            AbortPause::BeforeRead(gate) => {
                // The captured generation is terminal before intent reads it.
                // Settle this admitted mutation before admitting a successor:
                // the runtime intentionally serializes control-plane mutations.
                gate.release();
                Some(
                    tokio::time::timeout(Duration::from_secs(6), &mut delayed_abort)
                        .await
                        .expect("bounded captured abort result"),
                )
            }
            AbortPause::Committed(_) => None,
        };
        let mut successor = Box::pin(
            fixture
                .conversation
                .prompt(PromptRequest::new("successor").expect("request")),
        );
        let notification = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    result = &mut successor => panic!("successor settled before interaction: {result:?}"),
                    item = receiver.recv() => {
                        let item = item.expect("notification source").expect("notification");
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break item;
                        }
                    }
                }
            }
        }).await.expect("bounded successor interaction");
        let stale_result = match early_stale_result {
            Some(result) => result,
            None => tokio::time::timeout(Duration::from_secs(6), &mut delayed_abort)
                .await
                .expect("bounded captured abort result"),
        };
        drop(delayed_abort);
        let resolved = fixture
            .conversation
            .resolve_notification(
                notification.id().clone(),
                NotificationResolution::Submit {
                    content: serde_json::json!("42"),
                },
            )
            .await;
        let successor_result = tokio::time::timeout(Duration::from_secs(3), &mut successor)
            .await
            .expect("bounded successor completion");
        drop(successor);
        drop(receiver);
        fixture
            .conversation
            .close()
            .await
            .expect("conversation cleanup");
        fixture.workspace.close().await.expect("workspace cleanup");
        fixture.agent.shutdown().await.expect("agent cleanup");
        assert_eq!(
            first_result.expect("natural success may win").output(),
            "first complete"
        );
        stale_result.expect("the captured generation already reached its durable terminal");
        resolved.expect("the successor interaction remains resolvable after the old abort");
        assert_eq!(
            successor_result
                .expect("successor remains independent")
                .output(),
            "successor complete"
        );
        assert_eq!(fixture.tools.interactions().len(), 2);
        assert_eq!(fixture.tools.cancellations(), 0);
        assert_eq!(fixture.tools.invocations().len(), 2);
        assert_eq!(fixture.provider.requests().len(), 4);
        fixture.provider.assert_consumed();
        fixture.tools.assert_consumed();
        fixture.state.assert_consumed();
        assert_eq!(fixture.provider.shutdowns(), 1);
        assert_eq!(fixture.tools.shutdowns(), 1);
    }

    async fn unpresentable_interaction_settles_handler(cancellation: ToolCancellation) {
        let fixture = Self::new(
            vec![ProviderStep::unary(ProviderControl::function(
                &Continuation::new(),
            ))],
            vec![ToolOutcome::UnpresentableInteraction(cancellation)],
        )
        .await;
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            fixture
                .conversation
                .prompt(PromptRequest::new("invalid interaction").expect("request")),
        )
        .await
        .expect("bounded rejected interaction");
        fixture
            .conversation
            .close()
            .await
            .expect("conversation cleanup");
        fixture.workspace.close().await.expect("workspace cleanup");
        fixture.agent.shutdown().await.expect("agent cleanup");
        let error = result.expect_err("unpresentable interaction must fail");
        assert_eq!(error.kind(), ErrorKind::Integration);
        assert_eq!(
            error.message(),
            "tool interaction prompt cannot be presented safely"
        );
        assert_eq!(
            fixture.tools.cancellations(),
            1,
            "every acquired nonterminal handler must settle"
        );
        assert_eq!(fixture.tools.interactions().len(), 0);
        assert_eq!(fixture.tools.invocations().len(), 1);
        match cancellation {
            ToolCancellation::Completed => assert!(error.supplementary().is_empty()),
            ToolCancellation::TransportFailure => {
                assert_eq!(error.supplementary().len(), 1);
                assert_eq!(
                    error.supplementary()[0].context().phase,
                    ErrorPhase::Cleanup
                );
                assert_eq!(error.supplementary()[0].kind(), ErrorKind::Integration);
            }
            ToolCancellation::Cancelled => panic!("this case requires an explicit cleanup outcome"),
        }
        fixture.provider.assert_consumed();
        fixture.tools.assert_consumed();
        fixture.state.assert_consumed();
        assert_eq!(fixture.provider.shutdowns(), 1);
        assert_eq!(fixture.tools.shutdowns(), 1);
    }

    async fn new(steps: Vec<ProviderStep>, outcomes: Vec<ToolOutcome>) -> Self {
        Self::configured(
            steps,
            outcomes,
            CustomToolApproval::Allow,
            CustomLoopLimits::new(4, 4, 1).expect("limits"),
        )
        .await
    }

    async fn configured(
        steps: Vec<ProviderStep>,
        outcomes: Vec<ToolOutcome>,
        approval: CustomToolApproval,
        limits: CustomLoopLimits,
    ) -> Self {
        let tools = ToolControl::scripted(outcomes.into_iter().map(ToolScript::new).collect());
        Self::build(steps, tools.tools(), tools, approval, limits).await
    }

    fn config(approval: CustomToolApproval, limits: CustomLoopLimits) -> AgentConfig {
        let custom = CustomAgentConfig::new(
            ModelSelector::new(MODEL).expect("selector"),
            ToolSelection::Allowed(vec![TOOL.into()]),
            limits,
            approval,
        )
        .expect("custom configuration");
        AgentConfig::new(custom.into(), Duration::from_secs(3), NonZeroUsize::MIN)
            .expect("configuration")
    }
}

impl<T: ToolsetBackend> Fixture<T> {
    async fn build(
        steps: Vec<ProviderStep>,
        supplied_tools: T,
        tools: Arc<ToolControl>,
        approval: CustomToolApproval,
        limits: CustomLoopLimits,
    ) -> Self {
        let state = Control::default();
        let provider = ProviderControl::scripted(steps);
        let config = Fixture::<MockTools>::config(approval, limits);
        let agent = AgentBuilder::create_for_backends::<MockProvider, T>(
            config,
            AgentStorage::new(state.store()),
        )
        .with_provider(provider.provider())
        .with_toolset(supplied_tools)
        .build()
        .await
        .expect("actual Custom Agent");
        let directory = Directory::new();
        let workspace = agent
            .new_workspace(directory.workspace_request("mock workspace"))
            .await
            .expect("workspace");
        let conversation = workspace
            .new_conversation(
                NewConversation::new(Some("mock conversation".into()), None, Vec::new())
                    .expect("conversation request"),
            )
            .await
            .expect("conversation");
        Self {
            agent,
            workspace,
            conversation,
            provider,
            tools,
            state,
            directory,
        }
    }

    async fn finish_after_revision_change(self) {
        drop(self.conversation);
        drop(self.workspace);
        self.agent
            .shutdown()
            .await
            .expect("agent owns stale descendant cleanup");
        self.provider.assert_consumed();
        self.tools.assert_consumed();
        self.state.assert_consumed();
        assert_eq!(self.provider.shutdowns(), 1);
        assert_eq!(self.tools.shutdowns(), 1);
    }

    async fn finish(self) {
        self.conversation
            .close()
            .await
            .expect("conversation cleanup");
        self.workspace.close().await.expect("workspace cleanup");
        self.agent.shutdown().await.expect("agent cleanup");
        self.provider.assert_consumed();
        self.tools.assert_consumed();
        self.state.assert_consumed();
        assert_eq!(self.provider.shutdowns(), 1);
        assert_eq!(self.tools.shutdowns(), 1);
    }
}

#[tokio::test]
async fn actual_custom_retains_non_clone_integrations_and_exact_selected_model() {
    let fixture = Fixture::new(
        vec![ProviderStep::unary(ProviderControl::response("answer"))],
        Vec::new(),
    )
    .await;
    assert_eq!(
        fixture.agent.list_models().await.expect("models")[0]
            .selector()
            .as_str(),
        MODEL
    );
    assert_eq!(
        fixture.workspace.list_tools().await.expect("tools")[0].name(),
        TOOL
    );
    let response = fixture
        .conversation
        .prompt(PromptRequest::new("question").expect("prompt"))
        .await
        .expect("completion");
    assert_eq!(response.output(), "answer");
    assert_eq!(response.usage(), Some(&UsageInfo::new(2, 3)));
    let selected = fixture.provider.selections();
    assert!(!selected.is_empty());
    assert!(selected.iter().all(|id| id.as_str() == MODEL));
    let requests = fixture.provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].messages()[0].content(),
        &[ContentPart::Text {
            text: "question".into()
        }]
    );
    assert_eq!(fixture.tools.handlers(), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn abort_reconciles_intent_conflict_only_after_captured_durable_terminal() {
    Fixture::abort_reconciliation_observes(AbortIntentObservation::ChangedAfterTerminal).await;
}

#[tokio::test]
async fn abort_retains_nonconflict_intent_failure_after_captured_terminal() {
    Fixture::abort_reconciliation_observes(AbortIntentObservation::FailureAfterTerminal).await;
}

#[tokio::test]
async fn abort_retains_unexplained_intent_conflict_while_target_is_active() {
    Fixture::abort_reconciliation_observes(AbortIntentObservation::ConflictWhileActive).await;
}

#[tokio::test]
async fn delayed_abort_does_not_cancel_a_successor_interaction() {
    Fixture::delayed_abort_preserves_successor(AbortPause::Committed(
        mock_memory::CommitGate::new(),
    ))
    .await;
}

#[tokio::test]
async fn delayed_abort_intent_read_does_not_reject_a_completed_target_or_cancel_its_successor() {
    Fixture::delayed_abort_preserves_successor(AbortPause::BeforeRead(
        mock_memory::StateGate::new(),
    ))
    .await;
}

#[tokio::test]
async fn unpresentable_tool_interaction_cancels_its_acquired_handler_once() {
    Fixture::unpresentable_interaction_settles_handler(ToolCancellation::Completed).await;
}

#[tokio::test]
async fn unpresentable_tool_interaction_retains_primary_error_when_handler_cleanup_fails() {
    Fixture::unpresentable_interaction_settles_handler(ToolCancellation::TransportFailure).await;
}

#[tokio::test]
async fn actual_custom_tool_loop_retains_opaque_continuation_and_correlation() {
    for outcome in [ToolOutcome::Completed, ToolOutcome::ModelFailure] {
        let token = Continuation::new();
        let fixture = Fixture::new(
            vec![
                ProviderStep::unary(ProviderControl::function(&token)),
                ProviderStep::unary(ProviderControl::response("done")),
            ],
            vec![outcome],
        )
        .await;
        let response = fixture
            .conversation
            .prompt(PromptRequest::new("lookup").expect("prompt"))
            .await
            .expect("completion");
        assert_eq!(response.output(), "done");
        assert_eq!(response.usage(), Some(&UsageInfo::new(4, 6)));
        let requests = fixture.provider.requests();
        assert_eq!(requests.len(), 2);
        let assistant = requests[1]
            .messages()
            .iter()
            .find(|message| message.role() == Role::Assistant)
            .expect("assistant replay");
        assert!(assistant.continuation().expect("exact token").same(&token));
        let results = requests[1]
            .messages()
            .iter()
            .find(|message| message.role() == Role::Tool)
            .expect("tool result")
            .function_results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].call_id(), Some("call-1"));
        assert_eq!(results[0].name(), TOOL);
        assert_eq!(
            results[0].is_error(),
            matches!(outcome, ToolOutcome::ModelFailure)
        );
        assert_eq!(
            results[0].content(),
            &[ContentPart::Text {
                text: "mock result".into()
            }]
        );
        assert_eq!(
            fixture.tools.invocations()[0].arguments,
            serde_json::json!({"key":"answer"})
        );
        assert_eq!(fixture.tools.handlers(), 1);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn actual_custom_accepts_both_caller_tool_enum_branches() {
    for second in [false, true] {
        let tools = ToolControl::scripted(vec![ToolScript::new(ToolOutcome::Completed)]);
        let supplied = if second {
            CallerTools::Second(tools.tools())
        } else {
            CallerTools::First(tools.tools())
        };
        let token = Continuation::new();
        let fixture = Fixture::build(
            vec![
                ProviderStep::unary(ProviderControl::function(&token)),
                ProviderStep::unary(ProviderControl::response("enum composed")),
            ],
            supplied,
            tools,
            CustomToolApproval::Allow,
            CustomLoopLimits::new(4, 4, 1).expect("limits"),
        )
        .await;
        assert_eq!(
            fixture
                .conversation
                .prompt(PromptRequest::new("enum").expect("prompt"))
                .await
                .expect("completion")
                .output(),
            "enum composed"
        );
        assert_eq!(fixture.tools.handlers(), 1);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn actual_custom_stream_forwards_source_deltas_and_one_durable_terminal() {
    let fixture = Fixture::new(
        vec![ProviderStep::Stream(vec![
            Ok(GenerateEvent::ContentDelta {
                delta: "real ".into(),
            }),
            Ok(GenerateEvent::ContentDelta {
                delta: "deltas".into(),
            }),
            Ok(GenerateEvent::Completed(Box::new(
                ProviderControl::response("real deltas"),
            ))),
        ])],
        Vec::new(),
    )
    .await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("stream").expect("prompt"))
        .await
        .expect("admission");
    let mut deltas = String::new();
    let mut terminals = 0;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(3), stream.recv())
        .await
        .expect("bounded stream")
    {
        match event.expect("event") {
            PromptEvent::OutputDelta { text } => deltas.push_str(&text),
            PromptEvent::Completed(response) => {
                terminals += 1;
                assert_eq!(response.output(), "real deltas");
            }
        }
    }
    assert_eq!(deltas, "real deltas");
    assert_eq!(terminals, 1);
    assert!(stream.recv().await.is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn actual_custom_stream_replays_exact_tool_continuation() {
    let token = Continuation::new();
    let fixture = Fixture::new(
        vec![
            ProviderStep::Stream(vec![Ok(GenerateEvent::Completed(Box::new(
                ProviderControl::function(&token),
            )))]),
            ProviderStep::Stream(vec![
                Ok(GenerateEvent::ContentDelta {
                    delta: "final".into(),
                }),
                Ok(GenerateEvent::Completed(Box::new(
                    ProviderControl::response("final"),
                ))),
            ]),
        ],
        vec![ToolOutcome::Completed],
    )
    .await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("stream tool").expect("prompt"))
        .await
        .expect("admission");
    let mut complete = false;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(3), stream.recv())
        .await
        .expect("bounded stream")
    {
        if let PromptEvent::Completed(response) = event.expect("event") {
            complete = true;
            assert_eq!(response.output(), "final");
        }
    }
    assert!(complete);
    assert!(
        fixture.provider.requests()[1]
            .messages()
            .iter()
            .filter_map(|message| message.continuation())
            .any(|continuation| continuation.same(&token))
    );
    fixture.finish().await;
}

#[tokio::test]
async fn deliberately_malformed_source_eof_is_protocol_failure_and_does_not_poison_next_prompt() {
    let fixture = Fixture::new(
        vec![
            ProviderStep::MalformedStream(vec![Ok(GenerateEvent::ContentDelta {
                delta: "partial".into(),
            })]),
            ProviderStep::unary(ProviderControl::response("reused")),
        ],
        Vec::new(),
    )
    .await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("malformed source").expect("prompt"))
        .await
        .expect("admission");
    let mut failure = None;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(3), stream.recv())
        .await
        .expect("bounded stream")
    {
        if let Err(error) = event {
            failure = Some(error);
        }
    }
    assert!(matches!(
        failure.expect("premature EOF must fail").kind(),
        ErrorKind::Protocol
    ));
    assert_eq!(
        fixture
            .conversation
            .prompt(PromptRequest::new("next").expect("prompt"))
            .await
            .expect("next prompt")
            .output(),
        "reused"
    );
    fixture.finish().await;
}

#[tokio::test]
async fn dropping_prompt_observation_does_not_cancel_accepted_work() {
    let gate = Gate::new();
    let fixture = Fixture::new(
        vec![
            ProviderStep::GatedStream {
                gate: Arc::clone(&gate),
                events: vec![Ok(GenerateEvent::Completed(Box::new(
                    ProviderControl::response("retained answer"),
                )))],
            },
            ProviderStep::unary(ProviderControl::response("next answer")),
        ],
        Vec::new(),
    )
    .await;
    let stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("retained question").expect("prompt"))
        .await
        .expect("admission");
    gate.entered().await;
    drop(stream);
    gate.release();
    let response = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match fixture
                .conversation
                .prompt(PromptRequest::new("next question").expect("prompt"))
                .await
            {
                Ok(response) => break response,
                Err(error) if matches!(error.kind(), ErrorKind::Busy) => {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("next prompt failed: {error:?}"),
            }
        }
    })
    .await
    .expect("accepted work settles");
    assert_eq!(response.output(), "next answer");
    let requests = fixture.provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .messages()
            .iter()
            .any(|message| message.content()
                == [ContentPart::Text {
                    text: "retained answer".into()
                }])
    );
    fixture.finish().await;
}

#[tokio::test]
async fn explicit_abort_owns_cancellation_and_conversation_remains_reusable() {
    let gate = Gate::new();
    let fixture = Fixture::new(
        vec![
            ProviderStep::GatedStream {
                gate: Arc::clone(&gate),
                events: vec![Ok(GenerateEvent::Completed(Box::new(
                    ProviderControl::response("must not publish"),
                )))],
            },
            ProviderStep::unary(ProviderControl::response("after abort")),
        ],
        Vec::new(),
    )
    .await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("cancel this").expect("prompt"))
        .await
        .expect("admission");
    gate.entered().await;
    tokio::time::timeout(Duration::from_secs(3), fixture.conversation.abort())
        .await
        .expect("bounded cancellation")
        .expect("abort");
    let error = stream
        .recv()
        .await
        .expect("terminal")
        .expect_err("cancelled terminal");
    assert!(matches!(error.kind(), ErrorKind::Cancelled));
    assert!(stream.recv().await.is_none());
    assert_eq!(
        fixture
            .conversation
            .prompt(PromptRequest::new("next").expect("prompt"))
            .await
            .expect("completion")
            .output(),
        "after abort"
    );
    assert!(
        fixture.provider.requests()[1]
            .messages()
            .iter()
            .all(|message| message.role() != Role::Assistant)
    );
    fixture.finish().await;
}

#[tokio::test]
async fn actual_custom_interaction_correlates_resolution_and_executes_effect_once() {
    let token = Continuation::new();
    let fixture = Fixture::new(
        vec![
            ProviderStep::unary(ProviderControl::function(&token)),
            ProviderStep::unary(ProviderControl::response("resolved")),
        ],
        vec![ToolOutcome::Interaction],
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("notification stream");
    let mut prompt = Box::pin(
        fixture
            .conversation
            .prompt(PromptRequest::new("interactive").expect("prompt")),
    );
    let notification = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::select! {
                result = &mut prompt => panic!("prompt settled before interaction: {result:?}"),
                item = receiver.recv() => {
                    let item = item.expect("notification").expect("notification result");
                    if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break item; }
                }
            }
        }
    }).await.expect("bounded interaction");
    fixture
        .conversation
        .resolve_notification(
            notification.id().clone(),
            NotificationResolution::Submit {
                content: serde_json::json!("42"),
            },
        )
        .await
        .expect("resolve");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), &mut prompt)
            .await
            .expect("bounded completion")
            .expect("response")
            .output(),
        "resolved"
    );
    let duplicate = fixture
        .conversation
        .resolve_notification(
            notification.id().clone(),
            NotificationResolution::Submit {
                content: serde_json::json!("42"),
            },
        )
        .await;
    assert!(
        duplicate.is_err(),
        "settled notification cannot execute again"
    );
    assert_eq!(fixture.tools.invocations().len(), 1);
    assert_eq!(fixture.tools.interactions().len(), 1);
    drop(prompt);
    drop(receiver);
    fixture.finish().await;
}

#[tokio::test]
async fn actual_custom_structured_output_is_validated_before_terminal_acceptance() {
    for (text, valid) in [("{\"count\":2}", true), ("{\"count\":\"bad\"}", false)] {
        let fixture = Fixture::new(
            vec![ProviderStep::unary(ProviderControl::response(text))],
            Vec::new(),
        )
        .await;
        let request = PromptRequest::new("json").expect("prompt").with_structured_output(StructuredOutputRequest::new(serde_json::json!({"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false})).expect("schema"));
        let result = fixture.conversation.prompt(request).await;
        assert!(
            fixture.provider.requests()[0].response_format().is_some(),
            "structured schema forwarded to Provider"
        );
        if valid {
            assert_eq!(
                result.expect("valid response").structured_output(),
                Some(&serde_json::json!({"count":2}))
            );
        } else {
            assert!(result.is_err(), "invalid structured output must fail");
        }
        fixture.finish().await;
    }
}

#[tokio::test]
async fn rejected_completion_never_enters_provider_neutral_history() {
    let mut rejected = ProviderControl::response("unaccepted answer");
    rejected.finish_reason = Some(FinishReason::Length);
    let fixture = Fixture::new(
        vec![
            ProviderStep::unary(rejected),
            ProviderStep::unary(ProviderControl::response("accepted")),
        ],
        Vec::new(),
    )
    .await;
    let error = fixture
        .conversation
        .prompt(PromptRequest::new("unaccepted question").expect("prompt"))
        .await
        .expect_err("length completion fails");
    assert!(matches!(error.kind(), ErrorKind::Integration));
    fixture
        .conversation
        .prompt(PromptRequest::new("accepted question").expect("prompt"))
        .await
        .expect("response");
    assert_eq!(fixture.provider.requests()[1].messages().len(), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn model_turn_limit_stops_before_another_provider_call() {
    let token = Continuation::new();
    let fixture = Fixture::configured(
        vec![ProviderStep::unary(ProviderControl::function(&token))],
        vec![ToolOutcome::Completed],
        CustomToolApproval::Allow,
        CustomLoopLimits::new(1, 4, 1).expect("limits"),
    )
    .await;
    assert!(
        fixture
            .conversation
            .prompt(PromptRequest::new("bounded").expect("prompt"))
            .await
            .is_err()
    );
    assert_eq!(fixture.provider.requests().len(), 1);
    assert_eq!(fixture.tools.invocations().len(), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn denied_tools_never_create_or_invoke_a_handler() {
    let token = Continuation::new();
    let fixture = Fixture::configured(
        vec![ProviderStep::unary(ProviderControl::function(&token))],
        Vec::new(),
        CustomToolApproval::Deny,
        CustomLoopLimits::new(4, 4, 1).expect("limits"),
    )
    .await;
    assert!(
        fixture
            .conversation
            .prompt(PromptRequest::new("denied").expect("prompt"))
            .await
            .is_err()
    );
    assert_eq!(fixture.tools.handlers(), 0);
    assert_eq!(fixture.tools.invocations().len(), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn workspace_revision_stales_old_conversation_before_provider_effects() {
    let fixture = Fixture::new(Vec::new(), Vec::new()).await;
    let info = fixture.workspace.info().await.expect("workspace info");
    let update = UpdateWorkspaceRequest::new(
        info.id().clone(),
        info.revision(),
        "updated",
        info.context().clone(),
        info.defaults().clone(),
    )
    .expect("update");
    let updated = fixture
        .agent
        .update_workspace(update)
        .await
        .expect("revision update");
    assert_eq!(updated.revision(), info.revision() + 1);
    let error = fixture
        .conversation
        .prompt(PromptRequest::new("stale").expect("prompt"))
        .await
        .expect_err("stale handle");
    assert!(
        matches!(error.kind(), ErrorKind::Conflict | ErrorKind::Closed),
        "invalidated old handle must reject work: {error:?}"
    );
    assert!(fixture.provider.requests().is_empty());
    fixture.finish_after_revision_change().await;
}

#[tokio::test]
async fn same_process_reconstruction_uses_history_without_opaque_continuations() {
    let token = Continuation::new();
    let fixture = Fixture::new(
        vec![
            ProviderStep::unary(ProviderControl::function(&token)),
            ProviderStep::unary(ProviderControl::response("first answer")),
        ],
        vec![ToolOutcome::Completed],
    )
    .await;
    fixture
        .conversation
        .prompt(PromptRequest::new("first question").expect("prompt"))
        .await
        .expect("first completion");
    let agent_id = fixture.agent.id();
    let workspace_id = fixture.workspace.id();
    let conversation_id = fixture.conversation.id();
    drop(fixture.conversation);
    fixture.workspace.close().await.expect("close workspace");
    fixture.agent.shutdown().await.expect("shutdown");
    let provider = ProviderControl::scripted(vec![ProviderStep::unary(ProviderControl::response(
        "second answer",
    ))]);
    let tools = ToolControl::scripted(Vec::new());
    let reopened = AgentBuilder::reopen_for_backends::<MockProvider, MockTools>(
        Fixture::<MockTools>::config(
            CustomToolApproval::Allow,
            CustomLoopLimits::new(4, 4, 1).expect("limits"),
        ),
        AgentStorage::new(fixture.state.store()),
        agent_id,
    )
    .with_provider(provider.provider())
    .with_toolset(tools.tools())
    .build()
    .await
    .expect("same-process reopen");
    let workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("workspace reopen");
    let conversation = workspace
        .get_conversation(conversation_id)
        .await
        .expect("conversation reopen");
    assert_eq!(
        conversation
            .prompt(PromptRequest::new("second question").expect("prompt"))
            .await
            .expect("reopened completion")
            .output(),
        "second answer"
    );
    let requests = provider.requests();
    assert!(
        requests[0]
            .messages()
            .iter()
            .all(|message| message.continuation().is_none())
    );
    assert!(
        requests[0]
            .messages()
            .iter()
            .any(|message| message.content()
                == [ContentPart::Text {
                    text: "first answer".into()
                }])
    );
    assert_eq!(
        fixture.provider.requests().len(),
        2,
        "reopen must not replay old effects"
    );
    conversation.close().await.expect("close conversation");
    workspace.close().await.expect("close workspace");
    reopened.shutdown().await.expect("shutdown");
    provider.assert_consumed();
    tools.assert_consumed();
    fixture.provider.assert_consumed();
    fixture.tools.assert_consumed();
}

#[tokio::test]
async fn workspace_creation_failure_before_publication_leaves_no_inventory_entry() {
    for outcome in [CommitOutcome::BeforePublication, CommitOutcome::Conflict] {
        let fixture = Fixture::new(Vec::new(), Vec::new()).await;
        let before = fixture.agent.list_workspaces().await.expect("inventory");
        fixture.state.commit_as(outcome);
        let result = fixture
            .agent
            .new_workspace(fixture.directory.workspace_request("failed workspace"))
            .await;
        assert!(result.is_err());
        assert_eq!(
            fixture.agent.list_workspaces().await.expect("inventory"),
            before
        );
        fixture.finish().await;
    }
}

#[tokio::test]
async fn memory_fixture_distinguishes_lost_commit_reply_from_failed_publication() {
    use storage_kit::state::record::{StateKey, StateRecord};
    use storage_kit::{StateStoreBackend, StateTransactionBackend};
    let control = Control::default();
    let store = control.store();
    let key = StateKey::new("fixture", "ambiguous").expect("key");
    for (outcome, published) in [
        (CommitOutcome::BeforePublication, false),
        (CommitOutcome::AfterPublication, true),
    ] {
        let mut transaction = store.begin_transaction().await.expect("begin");
        transaction
            .put(StateRecord::new(
                key.clone(),
                serde_json::json!("published"),
            ))
            .await
            .expect("put");
        control.commit_as(outcome);
        assert!(transaction.commit().await.is_err());
        assert_eq!(
            store.load(key.clone()).await.expect("reconcile").is_some(),
            published
        );
    }
    control.assert_consumed();
}

#[tokio::test]
async fn fixture_scripts_reject_unexpected_calls_and_wrong_arguments() {
    let provider = ProviderControl::scripted(Vec::new());
    assert!(
        provider
            .provider()
            .model(providers_kit::models::ModelId::new("other-model").expect("id"))
            .is_err()
    );
    let model = provider
        .provider()
        .model(providers_kit::models::ModelId::new(MODEL).expect("id"))
        .expect("model");
    let request = providers_kit::generation::GenerateRequest::new(vec![
        providers_kit::generation::Message::user_text("unexpected").expect("message"),
    ])
    .expect("request");
    assert!(model.generate(&request).await.is_err());
    let tools = ToolControl::scripted(vec![ToolScript::new(ToolOutcome::Completed)]);
    let mut handler = tools
        .tools()
        .tool_handler(ToolName::new(TOOL).expect("name"))
        .await
        .expect("handler");
    assert!(
        handler
            .step(ToolHandlerInput::Invoke(ToolInvocation {
                call_id: Some("wrong".into()),
                arguments: serde_json::json!({})
            }))
            .await
            .is_err()
    );
    assert!(tools.invocations().is_empty());
    handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("call-1".into()),
            arguments: serde_json::json!({"key":"answer"}),
        }))
        .await
        .expect("matching invocation");
    assert!(
        handler
            .step(ToolHandlerInput::Invoke(ToolInvocation {
                call_id: Some("call-1".into()),
                arguments: serde_json::json!({"key":"answer"})
            }))
            .await
            .is_err()
    );
    assert!(
        tools
            .tools()
            .tool_handler(ToolName::new(TOOL).expect("name"))
            .await
            .is_err()
    );
    assert_eq!(tools.cancellations(), 0);
    for _ in 0..2 {
        assert!(
            handler
                .step(ToolHandlerInput::Cancel { reason: None })
                .await
                .is_err()
        );
    }
    assert_eq!(
        tools.cancellations(),
        2,
        "attempt observations include rejected terminal cancellation reuse"
    );
    tools.assert_consumed();
    provider.assert_consumed();
}

#[tokio::test]
async fn provider_failure_preserves_error_category_and_allows_next_prompt() {
    let failure = providers_kit::KitError::new(
        providers_kit::error::ErrorKind::Unsupported,
        providers_kit::error::Operation::Generate,
        providers_kit::error::ErrorPhase::InputValidation,
    );
    let fixture = Fixture::new(
        vec![
            ProviderStep::Failure(failure),
            ProviderStep::unary(ProviderControl::response("after failure")),
        ],
        Vec::new(),
    )
    .await;
    let error = fixture
        .conversation
        .prompt(PromptRequest::new("unsupported").expect("prompt"))
        .await
        .expect_err("provider failure");
    assert!(matches!(error.kind(), ErrorKind::Unsupported));
    assert_eq!(
        fixture
            .conversation
            .prompt(PromptRequest::new("next").expect("prompt"))
            .await
            .expect("response")
            .output(),
        "after failure"
    );
    fixture.finish().await;
}

#[tokio::test]
async fn outer_tool_failure_never_becomes_a_completed_model_result() {
    let fixture = Fixture::new(
        vec![ProviderStep::unary(ProviderControl::function(
            &Continuation::new(),
        ))],
        vec![ToolOutcome::OuterFailure],
    )
    .await;
    assert!(
        fixture
            .conversation
            .prompt(PromptRequest::new("operation failure").expect("prompt"))
            .await
            .is_err()
    );
    assert_eq!(fixture.provider.requests().len(), 1);
    assert_eq!(fixture.tools.invocations().len(), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn abort_pending_interaction_cancels_exactly_one_handler() {
    let fixture = Fixture::new(
        vec![ProviderStep::unary(ProviderControl::function(
            &Continuation::new(),
        ))],
        vec![ToolOutcome::Interaction],
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("receiver");
    let mut prompt = Box::pin(
        fixture
            .conversation
            .prompt(PromptRequest::new("pending").expect("prompt")),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::select! {
                result = &mut prompt => panic!("prompt settled before interaction: {result:?}"),
                item = receiver.recv() => if matches!(item.expect("notification").expect("item").kind(), ConversationNotificationKind::Interaction { .. }) { break; },
            }
        }
    }).await.expect("bounded interaction");
    // Keep the accepted unary observation alive but unpolled while cancellation
    // joins its owned work. Observation is not cancellation authority.
    let abort_result = fixture.conversation.abort().await;
    let repeated_abort = fixture.conversation.abort().await;
    let error = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
        .await
        .expect("bounded settlement")
        .expect_err("cancelled");
    assert!(matches!(error.kind(), ErrorKind::Cancelled));
    assert_eq!(fixture.tools.cancellations(), 1);
    assert_eq!(fixture.tools.interactions().len(), 0);
    drop(prompt);
    drop(receiver);
    fixture
        .conversation
        .abort()
        .await
        .expect("settlement after observing cancelled reply");
    fixture.finish().await;
    assert!(
        abort_result.is_ok(),
        "MOCK-AGENT-001: an unpolled prompt observer blocked cancellation after the handler settled: {abort_result:?}"
    );
    repeated_abort.expect("repeated abort must not wait for response acknowledgement");
}

#[tokio::test]
async fn dropping_unread_unary_reply_after_abort_releases_owned_cleanup() {
    let fixture = Fixture::new(
        vec![ProviderStep::unary(ProviderControl::function(
            &Continuation::new(),
        ))],
        vec![ToolOutcome::Interaction],
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("receiver");
    let mut prompt = Box::pin(
        fixture
            .conversation
            .prompt(PromptRequest::new("pending").expect("prompt")),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::select! {
                result = &mut prompt => panic!("prompt settled before interaction: {result:?}"),
                item = receiver.recv() => if matches!(item.expect("notification").expect("item").kind(), ConversationNotificationKind::Interaction { .. }) { break; },
            }
        }
    }).await.expect("bounded interaction");
    // Keep the accepted unary observation alive but unpolled while cancellation
    // joins its owned work. Observation is not cancellation authority.
    let abort_result = fixture.conversation.abort().await;
    assert_eq!(fixture.tools.cancellations(), 1);
    assert_eq!(fixture.tools.interactions().len(), 0);
    drop(prompt);
    drop(receiver);
    fixture.finish().await;
    assert!(
        abort_result.is_ok(),
        "abort must complete before the unread reply is dropped: {abort_result:?}"
    );
}

#[tokio::test]
async fn build_validation_cleans_accepted_integrations_without_replacing_primary_error() {
    let provider = ProviderControl::scripted(Vec::new());
    let first = ToolControl::scripted(Vec::new());
    let second = ToolControl::scripted(Vec::new());
    first.fail_shutdown();
    let result = AgentBuilder::create_for_backends::<MockProvider, MockTools>(
        Fixture::<MockTools>::config(
            CustomToolApproval::Allow,
            CustomLoopLimits::new(4, 4, 1).expect("limits"),
        ),
        AgentStorage::new(Control::default().store()),
    )
    .with_provider(provider.provider())
    .with_toolset(first.tools())
    .with_toolset(second.tools())
    .build()
    .await;
    let error = match result {
        Ok(agent) => {
            agent.shutdown().await.expect("cleanup unexpected build");
            panic!("duplicate tool names must fail");
        }
        Err(error) => error,
    };
    assert!(
        matches!(error.kind(), ErrorKind::InvalidConfig),
        "primary validation category retained: {error:?}"
    );
    assert!(
        !error.supplementary().is_empty(),
        "cleanup error remains attached to primary validation error"
    );
    assert_eq!(first.shutdowns(), 1);
    assert_eq!(second.shutdowns(), 1);
    assert_eq!(provider.shutdowns(), 1);
    first.assert_consumed();
    second.assert_consumed();
}

#[tokio::test]
async fn custom_builder_requires_provider_value_before_any_effect() {
    let state = Control::default();
    let result = AgentBuilder::create_for_backends::<MockProvider, MockTools>(
        Fixture::<MockTools>::config(
            CustomToolApproval::Allow,
            CustomLoopLimits::new(4, 4, 1).expect("limits"),
        ),
        AgentStorage::new(state.store()),
    )
    .build()
    .await;
    assert!(result.is_err());
    assert_eq!(state.commits(), 0);
}

#[tokio::test]
async fn actual_custom_routes_knowledge_through_caller_database_atomic_operation() {
    use agents_kit::knowledge::{IngestKnowledgeRequest, KnowledgeScope, KnowledgeSource};
    use storage_kit::database::catalog::{CatalogRequest, CatalogResponse};
    use storage_kit::database::data::{DataRequest, DataResponse, MutationResult};
    let database = mock_database::Control::new(vec![
        mock_database::Step::Catalog(Ok(CatalogResponse::Completed)),
        mock_database::Step::Data(Ok(DataResponse::Mutation(MutationResult {
            inserted: Some(1),
            counts_exact: true,
            ..MutationResult::default()
        }))),
        mock_database::Step::Catalog(Err(storage_kit::StorageError::backend_op(
            "catalog",
            "scripted database failure",
        ))),
    ]);
    let provider = ProviderControl::scripted(Vec::new());
    let tools = ToolControl::scripted(Vec::new());
    let agent = AgentBuilder::create_for_backends::<MockProvider, MockTools>(
        Fixture::<MockTools>::config(
            CustomToolApproval::Allow,
            CustomLoopLimits::new(4, 4, 1).expect("limits"),
        ),
        AgentStorage::new(Control::default().store()),
    )
    .with_provider(provider.provider())
    .with_toolset(tools.tools())
    .with_knowledge_database(database.database())
    .build()
    .await
    .expect("Custom with caller database");
    agent
        .ingest_knowledge(
            IngestKnowledgeRequest::new(Vec::new())
                .expect("empty")
                .with_scope(KnowledgeScope::Agent),
        )
        .await
        .expect("empty no-op");
    assert!(
        agent
            .ingest_knowledge(
                IngestKnowledgeRequest::new(Vec::new())
                    .expect("empty")
                    .with_scope(KnowledgeScope::Conversation)
            )
            .await
            .is_err()
    );
    assert!(
        database.catalog().is_empty(),
        "scope validation precedes effects"
    );
    let source = KnowledgeSource::text(
        "caller-source",
        "Caller title",
        "Exact caller-authored knowledge sentence.",
    )
    .expect("source");
    agent
        .ingest_knowledge(
            IngestKnowledgeRequest::new(vec![source.clone()])
                .expect("request")
                .with_scope(KnowledgeScope::Database),
        )
        .await
        .expect("atomic database ingestion");
    let catalog = database.catalog();
    let CatalogRequest::CreateCollection(collection) = &catalog[0] else {
        panic!("collection admission");
    };
    let requests = database.data();
    let DataRequest::ReplacePartition(replacement) = &requests[0] else {
        panic!("database-scoped ingestion requires atomic replacement");
    };
    assert_eq!(replacement.locator, collection.locator);
    assert_eq!(replacement.records.len(), 1);
    assert!(
        replacement.records[0]
            .fields
            .values()
            .any(|value| value == &serde_json::json!("Exact caller-authored knowledge sentence."))
    );
    let error = agent
        .ingest_knowledge(
            IngestKnowledgeRequest::new(vec![source])
                .expect("request")
                .with_scope(KnowledgeScope::Database),
        )
        .await
        .expect_err("database failure");
    assert!(matches!(error.kind(), ErrorKind::Integration));
    assert_eq!(database.data().len(), 1, "catalog failure prevents write");
    agent.shutdown().await.expect("shutdown");
    database.assert_consumed();
    provider.assert_consumed();
    tools.assert_consumed();
}

#[tokio::test]
async fn workspace_update_reconciles_publication_after_commit_reply_failure() {
    let fixture = Fixture::new(Vec::new(), Vec::new()).await;
    let original = fixture.workspace.info().await.expect("info");
    fixture.state.commit_as(CommitOutcome::AfterPublication);
    let update = UpdateWorkspaceRequest::new(
        original.id().clone(),
        original.revision(),
        "confirmed despite lost reply",
        original.context().clone(),
        original.defaults().clone(),
    )
    .expect("request");
    let updated = fixture
        .agent
        .update_workspace(update)
        .await
        .expect("committed public state reconciles lost reply");
    assert_eq!(updated.revision(), original.revision() + 1);
    assert_eq!(
        fixture
            .agent
            .workspace_info(original.id().clone())
            .await
            .expect("committed state"),
        updated
    );
    assert!(fixture.provider.requests().is_empty());
    fixture.finish_after_revision_change().await;
}

#[tokio::test]
async fn workspace_update_conflict_keeps_old_revision_and_retry_is_one_effect() {
    let fixture = Fixture::new(Vec::new(), Vec::new()).await;
    let original = fixture.workspace.info().await.expect("info");
    let update = UpdateWorkspaceRequest::new(
        original.id().clone(),
        original.revision(),
        "single update",
        original.context().clone(),
        original.defaults().clone(),
    )
    .expect("request");
    fixture.state.commit_as(CommitOutcome::Conflict);
    let error = fixture
        .agent
        .update_workspace(update.clone())
        .await
        .expect_err("conflict");
    assert!(matches!(error.kind(), ErrorKind::Conflict));
    assert_eq!(
        fixture
            .agent
            .workspace_info(original.id().clone())
            .await
            .expect("unchanged"),
        original
    );
    let updated = fixture.agent.update_workspace(update).await.expect("retry");
    assert_eq!(updated.revision(), original.revision() + 1);
    fixture.finish_after_revision_change().await;
}

#[tokio::test]
async fn admitted_workspace_commit_survives_loss_of_its_waiter() {
    let fixture = Fixture::new(Vec::new(), Vec::new()).await;
    let original = fixture.workspace.info().await.expect("info");
    let gate = mock_memory::CommitGate::new();
    fixture
        .state
        .commit_as(CommitOutcome::Admitted(Arc::clone(&gate)));
    let update = UpdateWorkspaceRequest::new(
        original.id().clone(),
        original.revision(),
        "admitted update",
        original.context().clone(),
        original.defaults().clone(),
    )
    .expect("request");
    let mut waiter = Box::pin(fixture.agent.update_workspace(update));
    tokio::select! {
        () = gate.entered() => {},
        result = &mut waiter => panic!("waiter settled before admitted commit release: {result:?}"),
    }
    drop(waiter);
    fixture.state.settle_admitted();
    let updated = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let current = fixture
                .agent
                .workspace_info(original.id().clone())
                .await
                .expect("read committed state");
            if current.revision() > original.revision() {
                break current;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("admitted commit remains authoritative");
    assert_eq!(updated.name(), "admitted update");
    assert_eq!(updated.revision(), original.revision() + 1);
    fixture.finish_after_revision_change().await;
}

#[tokio::test]
async fn fixture_admitted_commit_is_owned_after_future_drop() {
    use storage_kit::state::record::{StateKey, StateRecord};
    use storage_kit::{StateStoreBackend, StateTransactionBackend};
    let state = Control::default();
    let store = state.store();
    let key = StateKey::new("fixture", "admitted").expect("key");
    let mut transaction = store.begin_transaction().await.expect("begin");
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!("retained")))
        .await
        .expect("write");
    let gate = mock_memory::CommitGate::new();
    state.commit_as(CommitOutcome::Admitted(Arc::clone(&gate)));
    let mut commit = Box::pin(transaction.commit());
    tokio::select! {
        () = gate.entered() => {},
        result = &mut commit => panic!("commit settled before release: {result:?}"),
    }
    drop(commit);
    assert!(
        store
            .load(key.clone())
            .await
            .expect("before release")
            .is_none()
    );
    state.settle_admitted();
    assert_eq!(
        store
            .load(key)
            .await
            .expect("read")
            .expect("committed")
            .record
            .value
            .as_ref(),
        &serde_json::json!("retained")
    );
    state.assert_consumed();
}

#[test]
fn native_builder_type_selection_needs_no_provider_value() {
    let native =
        agents_kit::config::CodexConfig::spawn(Duration::from_secs(1), Duration::from_secs(1))
            .expect("passive config");
    let config =
        AgentConfig::new(native.into(), Duration::from_secs(1), NonZeroUsize::MIN).expect("config");
    let builder = AgentBuilder::create_for_backends::<MockProvider, MockTools>(
        config,
        AgentStorage::new(Control::default().store()),
    );
    // Construction of the passive builder proves static composition. Native
    // initialization/shutdown remain in process and strict-live targets.
    drop(builder);
}

#[test]
#[should_panic(expected = "unconsumed required Provider calls")]
fn fixture_detects_unconsumed_provider_expectations() {
    ProviderControl::scripted(vec![ProviderStep::unary(ProviderControl::response(
        "required",
    ))])
    .assert_consumed();
}

#[test]
#[should_panic(expected = "unconsumed required Tool handler")]
fn fixture_detects_unconsumed_tool_expectations() {
    ToolControl::scripted(vec![ToolScript::new(ToolOutcome::Completed)]).assert_consumed();
}

#[test]
#[should_panic(expected = "unconsumed required database expectation")]
fn fixture_detects_unconsumed_database_expectations() {
    mock_database::Control::new(vec![mock_database::Step::Catalog(Ok(
        storage_kit::database::catalog::CatalogResponse::Completed,
    ))])
    .assert_consumed();
}

#[tokio::test]
async fn actual_custom_composes_provider_tool_and_storage_enum_hierarchies_together() {
    for second in [false, true] {
        let token = Continuation::new();
        let provider = ProviderControl::scripted(vec![
            ProviderStep::unary(ProviderControl::function(&token)),
            ProviderStep::unary(ProviderControl::response("all caller enums")),
        ]);
        let tools = ToolControl::scripted(vec![ToolScript::new(ToolOutcome::Completed)]);
        let state = Control::default();
        let supplied_provider = if second {
            mock_runtime::CallerProvider::Second(provider.provider())
        } else {
            mock_runtime::CallerProvider::First(provider.provider())
        };
        let supplied_tools = if second {
            CallerTools::Second(tools.tools())
        } else {
            CallerTools::First(tools.tools())
        };
        let supplied_store = if second {
            mock_memory::CallerStore::Second(state.store())
        } else {
            mock_memory::CallerStore::First(state.store())
        };
        let agent = AgentBuilder::create_for_backends::<mock_runtime::CallerProvider, CallerTools>(
            Fixture::<MockTools>::config(
                CustomToolApproval::Allow,
                CustomLoopLimits::new(4, 4, 1).expect("limits"),
            ),
            AgentStorage::new(supplied_store),
        )
        .with_provider(supplied_provider)
        .with_toolset(supplied_tools)
        .build()
        .await
        .expect("enum composition");
        let directory = Directory::new();
        let workspace = agent
            .new_workspace(directory.workspace_request("enums"))
            .await
            .expect("workspace");
        let conversation = workspace
            .new_conversation(NewConversation::new(None, None, Vec::new()).expect("request"))
            .await
            .expect("conversation");
        assert_eq!(
            conversation
                .prompt(PromptRequest::new("all enum request").expect("request"))
                .await
                .expect("response")
                .output(),
            "all caller enums"
        );
        assert!(
            provider.requests()[1]
                .messages()
                .iter()
                .filter_map(|message| message.continuation())
                .any(|continuation| continuation.same(&token))
        );
        conversation.close().await.expect("close");
        workspace.close().await.expect("close");
        agent.shutdown().await.expect("shutdown");
        provider.assert_consumed();
        tools.assert_consumed();
        state.assert_consumed();
        assert_eq!(provider.shutdowns(), 1);
        assert_eq!(tools.shutdowns(), 1);
    }
}

#[tokio::test]
async fn prompt_overrides_forward_exact_reasoning_and_tool_policy() {
    use agents_kit::model::ReasoningEffort;
    use providers_kit::generation::{
        ReasoningConfig, ReasoningEffort as ProviderEffort, ToolChoice,
    };
    let mappings = [
        ("minimal", ProviderEffort::Minimal),
        ("low", ProviderEffort::Low),
        ("medium", ProviderEffort::Medium),
        ("high", ProviderEffort::High),
        ("maximum", ProviderEffort::Maximum),
    ];
    let fixture = Fixture::new(
        (0..mappings.len() + 1)
            .map(|_| ProviderStep::unary(ProviderControl::response("answer")))
            .collect(),
        Vec::new(),
    )
    .await;
    for (selection, effort) in mappings {
        fixture
            .conversation
            .prompt(
                PromptRequest::new("reason")
                    .expect("request")
                    .with_reasoning_effort(ReasoningEffort::new(selection).expect("effort"))
                    .with_tool_selection(ToolSelection::None)
                    .expect("selection"),
            )
            .await
            .expect("prompt");
        let requests = fixture.provider.requests();
        let request = requests.last().expect("request observation");
        assert_eq!(
            request.reasoning(),
            Some(&ReasoningConfig {
                effort: Some(effort),
                ..ReasoningConfig::default()
            })
        );
        assert!(request.tools().is_empty());
        assert_eq!(request.generation().tool_choice(), Some(&ToolChoice::None));
    }
    for unsupported in ["none", "max", "High"] {
        let error = fixture
            .conversation
            .prompt(
                PromptRequest::new("invalid effort")
                    .expect("request")
                    .with_reasoning_effort(
                        ReasoningEffort::new(unsupported).expect("portable effort"),
                    ),
            )
            .await
            .expect_err("unsupported Custom effort");
        assert_eq!(error.kind(), ErrorKind::Unsupported);
    }
    assert_eq!(fixture.provider.requests().len(), mappings.len());
    fixture
        .conversation
        .prompt(PromptRequest::new("omitted effort").expect("request"))
        .await
        .expect("prompt");
    assert!(
        fixture
            .provider
            .requests()
            .last()
            .expect("request")
            .reasoning()
            .is_none()
    );
    fixture.finish().await;
}

#[tokio::test]
async fn required_tool_policy_is_satisfied_once_then_allows_terminal_completion() {
    use providers_kit::generation::ToolChoice;
    let fixture = Fixture::new(
        vec![
            ProviderStep::unary(ProviderControl::function(&Continuation::new())),
            ProviderStep::unary(ProviderControl::response("satisfied")),
        ],
        vec![ToolOutcome::Completed],
    )
    .await;
    let response = fixture
        .conversation
        .prompt(
            PromptRequest::new("required")
                .expect("request")
                .with_tool_selection(ToolSelection::Required)
                .expect("selection"),
        )
        .await
        .expect("completion");
    assert_eq!(response.output(), "satisfied");
    let requests = fixture.provider.requests();
    assert_eq!(
        requests[0].generation().tool_choice(),
        Some(&ToolChoice::Required)
    );
    assert_eq!(
        requests[1].generation().tool_choice(),
        Some(&ToolChoice::Auto)
    );
    assert_eq!(fixture.tools.invocations().len(), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn actual_custom_sibling_conversations_keep_independent_histories_and_lifecycles() {
    let fixture = Fixture::new(
        vec![
            ProviderStep::unary(ProviderControl::response("first answer")),
            ProviderStep::unary(ProviderControl::response("second answer")),
        ],
        Vec::new(),
    )
    .await;
    let sibling = fixture
        .workspace
        .new_conversation(
            NewConversation::new(Some("independent".into()), None, Vec::new()).expect("request"),
        )
        .await
        .expect("sibling");
    assert_ne!(sibling.id(), fixture.conversation.id());
    fixture
        .conversation
        .prompt(PromptRequest::new("first question").expect("request"))
        .await
        .expect("first completion");
    fixture
        .conversation
        .close()
        .await
        .expect("close first only");
    assert_eq!(
        sibling
            .prompt(PromptRequest::new("second question").expect("request"))
            .await
            .expect("sibling remains open")
            .output(),
        "second answer"
    );
    assert_eq!(fixture.provider.requests()[1].messages().len(), 1);
    assert_eq!(
        fixture.provider.requests()[1].messages()[0].content(),
        &[ContentPart::Text {
            text: "second question".into()
        }]
    );
    sibling.close().await.expect("sibling close");
    fixture.workspace.close().await.expect("workspace close");
    fixture.agent.shutdown().await.expect("agent shutdown");
    fixture.provider.assert_consumed();
    fixture.tools.assert_consumed();
}

#[tokio::test]
async fn fixture_shutdown_seals_retained_roots_models_and_handlers_exactly_once() {
    let provider = ProviderControl::scripted(Vec::new());
    let root = provider.provider();
    let model = root
        .model(providers_kit::models::ModelId::new(MODEL).expect("model"))
        .expect("model child");
    root.shutdown().await.expect("shutdown");
    root.shutdown().await.expect("idempotent shutdown");
    assert_eq!(provider.shutdowns(), 1);
    assert!(
        root.list_models(providers_kit::models::ModelListRequest::default())
            .await
            .is_err()
    );
    let request = providers_kit::generation::GenerateRequest::new(vec![
        providers_kit::generation::Message::user_text("after shutdown").expect("message"),
    ])
    .expect("request");
    assert!(model.generate(&request).await.is_err());
    assert!(provider.requests().is_empty());
    let tools = ToolControl::scripted(vec![ToolScript::new(ToolOutcome::Completed)]);
    let root = tools.tools();
    let mut handler = root
        .tool_handler(ToolName::new(TOOL).expect("name"))
        .await
        .expect("handler");
    handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("call-1".into()),
            arguments: serde_json::json!({"key":"answer"}),
        }))
        .await
        .expect("required invocation completes");
    root.shutdown().await.expect("shutdown");
    root.shutdown().await.expect("idempotent shutdown");
    assert_eq!(tools.shutdowns(), 1);
    assert!(matches!(
        root.list_tools().await,
        Err(tools_kit::ToolError::Closed { .. })
    ));
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Invoke(ToolInvocation {
                call_id: Some("call-1".into()),
                arguments: serde_json::json!({"key":"answer"})
            }))
            .await,
        Err(tools_kit::ToolError::Closed { .. })
    ));
    assert_eq!(tools.invocations().len(), 1);
    provider.assert_consumed();
    tools.assert_consumed();
}

#[tokio::test]
#[should_panic(expected = "unconsumed acquired Tool invocation")]
async fn fixture_detects_acquired_handler_dropped_without_required_invocation() {
    let tools = ToolControl::scripted(vec![ToolScript::new(ToolOutcome::Completed)]);
    let handler = tools
        .tools()
        .tool_handler(ToolName::new(TOOL).expect("name"))
        .await
        .expect("required handler");
    drop(handler);
    tools.assert_consumed();
}

#[tokio::test]
async fn tool_approval_reconciles_lost_commit_reply_without_duplicate_effect() {
    let fixture = Fixture::configured(
        vec![
            ProviderStep::unary(ProviderControl::function(&Continuation::new())),
            ProviderStep::unary(ProviderControl::response("approved once")),
        ],
        vec![ToolOutcome::Completed],
        CustomToolApproval::Interaction,
        CustomLoopLimits::new(4, 4, 1).expect("limits"),
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("receiver");
    let mut prompt = Box::pin(
        fixture
            .conversation
            .prompt(PromptRequest::new("approval").expect("request")),
    );
    let notification = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::select! {
                result = &mut prompt => panic!("prompt settled before approval: {result:?}"),
                item = receiver.recv() => {
                    let item = item.expect("notification").expect("result");
                    if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break item; }
                }
            }
        }
    }).await.expect("bounded approval");
    assert_eq!(
        fixture.tools.handlers(),
        0,
        "approval precedes executable handler acquisition"
    );
    fixture.state.commit_as(CommitOutcome::AfterPublication);
    fixture
        .conversation
        .resolve_notification(notification.id().clone(), NotificationResolution::Allow)
        .await
        .expect("durable resolution intent reconciles lost reply");
    let response = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
        .await
        .expect("bounded prompt")
        .expect("response");
    assert_eq!(response.output(), "approved once");
    assert!(
        fixture
            .conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow)
            .await
            .is_err()
    );
    assert_eq!(fixture.tools.handlers(), 1);
    assert_eq!(fixture.tools.invocations().len(), 1);
    assert_eq!(fixture.provider.requests().len(), 2);
    drop(prompt);
    drop(receiver);
    fixture.finish().await;
}

#[tokio::test]
async fn polling_unary_observation_concurrently_allows_abort_to_join_one_cancelled_handler() {
    let fixture = Fixture::new(
        vec![ProviderStep::unary(ProviderControl::function(
            &Continuation::new(),
        ))],
        vec![ToolOutcome::Interaction],
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("receiver");
    let mut prompt = Box::pin(
        fixture
            .conversation
            .prompt(PromptRequest::new("pending").expect("request")),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::select! {
                result = &mut prompt => panic!("prompt settled before interaction: {result:?}"),
                item = receiver.recv() => if matches!(item.expect("notification").expect("item").kind(), ConversationNotificationKind::Interaction { .. }) { break; },
            }
        }
    }).await.expect("bounded interaction");
    let (aborted, concurrent, response) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            fixture.conversation.abort(),
            fixture.conversation.abort(),
            &mut prompt
        )
    })
    .await
    .expect("bounded concurrent cancellation");
    aborted.expect("abort");
    concurrent.expect("concurrent abort");
    assert_eq!(
        response.expect_err("cancelled prompt").kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(fixture.tools.cancellations(), 1);
    assert_eq!(fixture.tools.interactions().len(), 0);
    drop(prompt);
    drop(receiver);
    fixture.finish().await;
}

#[tokio::test]
async fn aggregate_tool_limit_rejects_before_executable_handler_creation() {
    let fixture = Fixture::configured(
        vec![ProviderStep::unary(ProviderControl::function(
            &Continuation::new(),
        ))],
        Vec::new(),
        CustomToolApproval::Allow,
        CustomLoopLimits::new(4, 0, 1).expect("zero tool budget is valid"),
    )
    .await;
    assert!(
        fixture
            .conversation
            .prompt(PromptRequest::new("no tool budget").expect("request"))
            .await
            .is_err()
    );
    assert_eq!(fixture.provider.requests().len(), 1);
    assert_eq!(fixture.tools.handlers(), 0);
    assert!(fixture.tools.invocations().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn excluded_tool_call_is_rejected_before_executable_handler_creation() {
    let fixture = Fixture::new(
        vec![ProviderStep::unary(ProviderControl::function(
            &Continuation::new(),
        ))],
        Vec::new(),
    )
    .await;
    let request = PromptRequest::new("tools excluded")
        .expect("request")
        .with_tool_selection(ToolSelection::None)
        .expect("selection");
    assert!(fixture.conversation.prompt(request).await.is_err());
    assert!(fixture.provider.requests()[0].tools().is_empty());
    assert_eq!(fixture.tools.handlers(), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn custom_validation_rejects_non_object_structured_schema_before_provider_effects() {
    let fixture = Fixture::new(Vec::new(), Vec::new()).await;
    let request = PromptRequest::new("invalid Custom schema")
        .expect("request")
        .with_structured_output(
            StructuredOutputRequest::new(serde_json::json!({"type":"string"}))
                .expect("valid portable schema"),
        );
    let error = fixture
        .conversation
        .prompt(request)
        .await
        .expect_err("Custom object schema required");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(fixture.provider.requests().is_empty());
    assert_eq!(fixture.tools.handlers(), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn actual_custom_file_reads_preserve_exact_bytes_and_reject_escaping_paths() {
    let fixture = Fixture::new(Vec::new(), Vec::new()).await;
    let contents = b"exact\r\nbytes\x00\xff";
    std::fs::write(fixture.directory.path.join("input.bin"), contents).expect("temporary file");
    let file = fixture
        .conversation
        .read_file(PathBuf::from("input.bin"))
        .await
        .expect("read file");
    assert_eq!(file.path(), PathBuf::from("input.bin"));
    assert_eq!(file.bytes(), Some(contents.as_slice()));
    assert!(file.text().is_none());
    let error = fixture
        .conversation
        .read_file(PathBuf::from("../escape"))
        .await
        .expect_err("relative path escape");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(fixture.provider.requests().is_empty());
    fixture.finish().await;
}
