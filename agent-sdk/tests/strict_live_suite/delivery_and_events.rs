use crate::native_cleanup::NativeCleanup;

use std::time::Duration;

use futures_util::FutureExt;

use agent_sdk::notification::{
    ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver,
    NotificationResolution, ProcessState,
};
use agent_sdk::prompt::PromptRequest;
use agent_sdk::{Agent, Conversation, ConversationBackend};
use agent_sdk::{AgentBackend, WorkspaceBackend};

use super::fixture::{StrictBackend, StrictCodexProcess, StrictFixture};
use crate::opencode_question_evidence::readback::NativeQuestionReadback;

pub(super) struct DeliveryScenario;

enum CodexRequestScenario {
    Stdin,
    NonblockingInput,
}

impl CodexRequestScenario {
    async fn prompt(
        conversation: &Conversation,
        request: PromptRequest,
        expected_approval: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> agent_sdk::prompt::PromptResponse {
        let mut notifications = conversation.notification_stream().unwrap();
        let mut prompt = Box::pin(conversation.prompt(request));
        let mut approvals = 0;
        let response = tokio::time::timeout_at(deadline, async {
            loop {
                tokio::select! {
                    biased;
                    notification = notifications.recv() => {
                        let notification = notification.expect("native delivery remains open").unwrap();
                        if let ConversationNotificationKind::Interaction { title, response_schema, .. } = notification.kind() {
                            assert_eq!(Some(title.as_str()), expected_approval, "the exact native approval kind is required");
                            assert!(response_schema.is_none());
                            approvals += 1;
                            assert_eq!(approvals, 1, "no substitute or repeated approval");
                            let resolved = conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await;
                            assert!(matches!(resolved, Err(error) if error.kind() == agent_sdk::error::ErrorKind::Conflict), "native enqueue retains ambiguity");
                            // Delivery remains fenced until this turn settles;
                            // its identity-only callback notification is not acceptance.
                            break (&mut prompt).await.expect("approved native prompt must complete");
                        }
                    }
                    result = &mut prompt => break result.expect("native prompt must complete"),
                }
            }
        }).await.expect("native request journey remains within its original deadline");
        assert_eq!(
            approvals,
            usize::from(expected_approval.is_some()),
            "failure to elicit the native request is blocking"
        );
        response
    }

    async fn run(self) {
        use agent_sdk::workspace::{
            NewWorkspace, WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
            WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
        };
        let fixture = StrictFixture::new().await;
        let process = StrictCodexProcess::start(fixture.directory()).await;
        let capture_input = matches!(self, Self::NonblockingInput);
        let input_evidence = process.input_evidence();
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let deadline = tokio::time::Instant::now() + DeliveryScenario::CODEX_TIMEOUT;
        let observed = NativeCleanup::observe(deadline, async {
            let agent = owned_agent.insert(agent_sdk::AgentBuilder::create(process.config(), fixture.storage()).build().await.unwrap());
            let model = StrictFixture::model(agent, StrictBackend::Codex, "TEST_CODEX_MODEL_DEFAULT").await;
            let models = agent.list_models().await.unwrap();
            assert!(models.iter().all(|model| model.personalities().is_empty()), "retired Codex personalities must never be advertised");
            let directory = std::fs::canonicalize(fixture.directory()).unwrap();
            let context = WorkspaceContext::new(directory.clone(), vec![WorkspaceRoot::new(directory.clone(), WorkspaceRootAccess::ReadWrite).unwrap()],
                WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::WorkspaceWrite, WorkspaceNetworkAccess::Enabled)).unwrap();
            let workspace = owned_workspace.insert(agent.new_workspace(NewWorkspace::new(StrictFixture::unique("codex-requests"), context, WorkspaceDefaults::empty()).unwrap()).await.unwrap());
            let conversation = owned_conversation.insert(workspace.new_conversation(StrictFixture::conversation("Native request release coverage")).await.unwrap());
            let personality = agent_sdk::model::PersonalitySelector::new("none").unwrap();
            let rejected = conversation.prompt(PromptRequest::new("must not start").unwrap().with_model(model.clone()).with_personality(personality)).await.unwrap_err();
            assert_eq!(rejected.kind(), agent_sdk::error::ErrorKind::Unsupported);
            match self {
                Self::Stdin => {
                    std::fs::write(directory.join("stdin_check.py"), "import signal,sys\nfrom pathlib import Path\nsignal.alarm(120)\ntext=sys.stdin.readline()\nPath('stdin-result.txt').write_text(text)\nprint(text,end='',flush=True)\n").unwrap();
                    let launch = PromptRequest::new("Use exec_command exactly once to run `python3 -u stdin_check.py` in the current workspace with tty=true, yield_time_ms=1000, sandbox_permissions=\"require_escalated\", and justification=\"Start the owned stdin approval test\". Keep the returned session_id for the next turn. Do not write stdin, poll, or invoke another tool. Once the running session is returned, reply exactly STDIN_READY.").unwrap()
                        .with_model(model.clone()).with_timeout(DeliveryScenario::CODEX_TIMEOUT).unwrap();
                    let ready = Self::prompt(conversation, launch, Some("Command approval"), deadline).await;
                    assert_eq!(ready.output(), "STDIN_READY");
                    let token = StrictFixture::unique("STDIN_ACCEPTED");
                    let input = PromptRequest::new(format!("Call write_stdin exactly once for the session_id returned in the previous turn, with chars equal to `{token}\\n` (one trailing newline), yield_time_ms=1000. Await approval and process completion. Do not start a new command. Reply exactly {token}." )).unwrap()
                        .with_model(model).with_timeout(DeliveryScenario::CODEX_TIMEOUT).unwrap();
                    let response = Self::prompt(conversation, input, Some("Terminal input approval"), deadline).await;
                    assert_eq!(response.output(), token);
                    assert_eq!(std::fs::read_to_string(directory.join("stdin-result.txt")).unwrap(), format!("{token}\n"), "the real reviewed process must receive the exact input");
                }
                Self::NonblockingInput => {
                    let request = PromptRequest::new("Stay in Default mode. Call the native request_user_input tool exactly once with one question: id=choice, header=Choice, question=Choose a value?, options=[{label:First,description:First choice},{label:Second,description:Second choice}]. Do not imitate the tool call in text. After the tool returns, including cancellation or unsupported response, reply exactly INPUT_SETTLED. Do not call other tools.").unwrap()
                        .with_model(model).with_timeout(DeliveryScenario::CODEX_TIMEOUT).unwrap();
                    process.prepare_input_evidence(&directory, request.model().unwrap().as_str(), request.text());
                    let response = Self::prompt(conversation, request, None, deadline).await;
                    assert_eq!(response.output(), "INPUT_SETTLED");
                    process.assert_nonblocking_response();
                }
            }
        }).await;
        if capture_input {
            process.capture_input_rollout(deadline).await;
        }
        let mut cleanup = NativeCleanup::new(Duration::from_secs(60));
        if let Some(conversation) = owned_conversation.take() {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = owned_workspace.take() {
            cleanup.settle(workspace.close()).await;
        }
        if let Some(agent) = owned_agent.take() {
            cleanup.settle(agent.shutdown()).await;
        }
        let hierarchy_cleaned = cleanup.diagnostics().is_empty();
        let before_process_cleanup = cleanup.diagnostics().len();
        cleanup
            .settle::<std::io::Error>(async {
                process.shutdown().await;
                Ok(())
            })
            .await;
        if capture_input {
            let cleaned = cleanup.diagnostics().is_empty();
            input_evidence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .record_cleanup(
                    hierarchy_cleaned,
                    cleanup.diagnostics().len() == before_process_cleanup,
                );
            if let Some(path) = std::env::var_os("AGENT_SDK_CODEX_INPUT_EVIDENCE") {
                cleanup
                    .settle(async {
                        input_evidence
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .export_after_cleanup(
                                &std::path::PathBuf::from(path),
                                matches!(&observed, Ok(Ok(()))),
                                cleaned,
                            )
                    })
                    .await;
            }
        }
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_stdin_approval_reaches_the_original_process() {
    CodexRequestScenario::Stdin.run().await;
}

#[tokio::test]
async fn codex_nonblocking_input_returns_unsupported_without_actionable_notification() {
    CodexRequestScenario::NonblockingInput.run().await;
}

impl DeliveryScenario {
    const CODEX_TIMEOUT: Duration = Duration::from_secs(180);
    const OPENCODE_INTERACTION_TIMEOUT: Duration = Duration::from_secs(600);
    const OPENCODE_TERMINAL_TIMEOUT: Duration = Duration::from_secs(660);
    const DRAIN_TIMEOUT: Duration = Duration::from_secs(45);
    const QUIET_TIMEOUT: Duration = Duration::from_secs(1);

    async fn receive(receiver: &mut ConversationNotificationReceiver) -> ConversationNotification {
        tokio::time::timeout(Self::DRAIN_TIMEOUT, receiver.recv())
            .await
            .expect("notification delivery must remain bounded")
            .expect("notification delivery must remain open")
            .expect("notification delivery must succeed")
    }

    pub(super) async fn drain(receiver: &mut ConversationNotificationReceiver) {
        tokio::time::timeout(Self::DRAIN_TIMEOUT, async {
            while let Some(notification) = receiver.recv().await {
                notification.expect("terminal drain must contain valid committed notifications");
            }
        })
        .await
        .expect("terminal notification drain must end");
    }

    pub(super) async fn assert_no_interaction(receiver: &mut ConversationNotificationReceiver) {
        let deadline = tokio::time::Instant::now() + Self::QUIET_TIMEOUT;
        loop {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Err(_) => return,
                Ok(Some(Ok(notification))) => assert!(
                    !matches!(
                        notification.kind(),
                        ConversationNotificationKind::Interaction { .. }
                    ),
                    "interaction must not be duplicated"
                ),
                Ok(Some(Err(error))) => panic!("notification observation failed: {error}"),
                Ok(None) => panic!("notification delivery closed before conversation close"),
            }
        }
    }

    pub(super) async fn resolve_opencode_question(
        conversation: &Conversation,
        notification: &ConversationNotification,
    ) -> String {
        let answer = StrictFixture::unique("strict-opencode-answer");
        conversation
            .resolve_notification(
                notification.id().clone(),
                NotificationResolution::Submit {
                    content: serde_json::json!({ "answers": [[answer]] }),
                },
            )
            .await
            .expect("OpenCode question must accept its canonical answer");
        answer
    }

    async fn resolve_codex_approval(
        conversation: &Conversation,
        notification: &ConversationNotification,
    ) -> bool {
        let ConversationNotificationKind::Interaction {
            response_schema, ..
        } = notification.kind()
        else {
            panic!("native command approval must be an Interaction");
        };
        assert!(
            response_schema.is_none(),
            "native command approval must remain approval-shaped"
        );
        let result = conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow)
            .await;
        match result {
            Ok(()) => false,
            Err(error) if error.kind() == agent_sdk::error::ErrorKind::Conflict => true,
            Err(error) => panic!(
                "Codex approval must confirm acceptance or report fenced ambiguity: {error:?}"
            ),
        }
    }

    pub(super) async fn opencode_model(agent: &Agent) -> agent_sdk::model::ModelSelector {
        StrictFixture::model(
            agent,
            StrictBackend::OpenCode,
            "TEST_OLLAMA_MODEL_OPENCODE_SHELL",
        )
        .await
    }

    pub(super) fn opencode_question_prompt() -> &'static str {
        "Use OpenCode's built-in `question` tool exactly once. Issue a real question tool call before producing any response text. Do not print, quote, describe, or imitate the tool call in response text. The `questions` argument must contain exactly one entry with `header` set to `Continue`, `question` set to `Continue?`, and `multiple` set to `false`. Its `options` must contain exactly two entries in this order: first, one with `label` `Yes` and `description` `Continue`; second, one with `label` `No` and `description` `Stop`. Use the specified header, question, labels, and descriptions exactly as written, without leading or trailing whitespace. Wait for the tool reply. Then output exactly the first answer value returned by the tool and stop without invoking another tool."
    }

    fn assert_question_schema(schema: &serde_json::Value) {
        assert_eq!(schema.pointer("/type"), Some(&serde_json::json!("object")));
        assert_eq!(
            schema.pointer("/additionalProperties"),
            Some(&serde_json::Value::Bool(false))
        );
        assert_eq!(
            schema.pointer("/properties/answers/minItems"),
            Some(&serde_json::json!(1))
        );
        assert_eq!(
            schema.pointer("/properties/answers/maxItems"),
            Some(&serde_json::json!(1))
        );
        assert_eq!(
            schema
                .pointer("/properties/answers/prefixItems")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            schema.pointer("/properties/answers/items"),
            Some(&serde_json::Value::Bool(false))
        );
        assert!(
            schema.pointer("/properties/answer").is_none(),
            "the obsolete singular answer field must remain absent"
        );
    }
}

#[tokio::test]
async fn codex_process_delivery_is_ordered_durable_exclusive_and_drains() {
    let fixture = StrictFixture::new().await;
    let (_config, agent) = fixture
        .create(
            StrictBackend::Codex,
            &StrictFixture::unique("strict-codex-process-delivery"),
        )
        .await;
    let workspace = agent
        .new_workspace(StrictFixture::workspace(&StrictFixture::unique(
            "strict-codex-process-workspace",
        )))
        .await
        .expect("Codex process workspace must be created");
    let conversation = workspace
        .new_conversation(StrictFixture::conversation("Codex process delivery"))
        .await
        .expect("Codex process conversation must be created");
    let deadline = tokio::time::Instant::now() + DeliveryScenario::CODEX_TIMEOUT;
    let mut observed = std::panic::AssertUnwindSafe(tokio::time::timeout_at(deadline, async {
        let mut receiver = conversation
            .notification_stream()
            .expect("first Codex receiver acquisition must succeed");
        assert!(matches!(conversation.notification_stream(), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Busy)));

        let token = StrictFixture::unique("CODEX_PROCESS");
        let request = PromptRequest::new(format!(
            "Use the native shell command-execution capability exactly once. Run one host-appropriate command that writes the exact token {token} to standard output, waits at least two seconds, and exits with status 7. Do not call a registered tool. After the command finishes, reply with exactly {token}_DONE and no other text."
        ))
        .expect("Codex process prompt must be valid")
        .with_model(StrictFixture::model(&agent, StrictBackend::Codex, "TEST_CODEX_MODEL_DEFAULT").await)
        .with_timeout(DeliveryScenario::CODEX_TIMEOUT)
        .expect("Codex process prompt timeout must be valid");
        let stream = conversation
            .prompt_stream(request)
            .await
            .expect("Codex process stream must be admitted");
        let mut prompt = Box::pin(NativeCleanup::observe_stream(stream));
        let mut response = None;

        let running = loop {
                tokio::select! {
                    biased;
                    notification = receiver.recv() => {
                        let notification = notification
                            .expect("Codex delivery must remain open")
                            .expect("Codex notification must succeed");
                        match notification.kind() {
                            ConversationNotificationKind::Interaction { .. } => {
                                if DeliveryScenario::resolve_codex_approval(&conversation, &notification).await {
                                    // Native enqueue is ambiguous. The same prompt's terminal
                                    // lifecycle reconciles it before later delivery may advance.
                                    response = Some(tokio::time::timeout_at(deadline, &mut prompt)
                                        .await.expect("ambiguous Codex approval must reconcile at prompt terminal"));
                                    drop(receiver);
                                    receiver = conversation.notification_stream()
                                        .expect("terminal reconciliation must release Codex delivery");
                                }
                            }
                            ConversationNotificationKind::Process {
                                state: ProcessState::Running,
                                ..
                            } => break notification,
                            ConversationNotificationKind::Process {
                                state: ProcessState::Exited { .. },
                                ..
                            } => panic!("Codex Process Exited arrived before Process Running"),
                            _ => {}
                        }
                    }
                    result = &mut prompt, if response.is_none() => {
                        panic!("Codex prompt completed before Process Running: {result:?}")
                    }
                }
        };
        let running_id = running.id().clone();
        let running_label = match running.kind() {
            ConversationNotificationKind::Process {
                label,
                state: ProcessState::Running,
            } => label.clone(),
            other => panic!("expected Codex Process Running, received {other:?}"),
        };
        assert_eq!(running_label.trim(), running_label);
        assert!(!running_label.is_empty());
        assert!(
            !running_label.contains(&token),
            "portable process label must not leak command output"
        );

        drop(receiver);
        receiver = conversation
            .notification_stream()
            .expect("receiver drop must release the Codex delivery lease");
        let replay = DeliveryScenario::receive(&mut receiver).await;
        assert_eq!(replay.id(), &running_id);
        assert_eq!(replay.kind(), running.kind());

        let exited = loop {
                tokio::select! {
                    biased;
                    notification = receiver.recv() => {
                        let notification = notification
                            .expect("Codex delivery must remain open")
                            .expect("Codex notification must succeed");
                        match notification.kind() {
                            ConversationNotificationKind::Interaction { .. } => {
                                if DeliveryScenario::resolve_codex_approval(&conversation, &notification).await {
                                    // Native enqueue is ambiguous. The same prompt's terminal
                                    // lifecycle reconciles it before later delivery may advance.
                                    response = Some(tokio::time::timeout_at(deadline, &mut prompt)
                                        .await.expect("ambiguous Codex approval must reconcile at prompt terminal"));
                                    drop(receiver);
                                    receiver = conversation.notification_stream()
                                        .expect("terminal reconciliation must release Codex delivery");
                                }
                            }
                            ConversationNotificationKind::Process {
                                state: ProcessState::Running,
                                ..
                            } => panic!("Codex emitted duplicate Process Running"),
                            ConversationNotificationKind::Process {
                                state: ProcessState::Exited { .. },
                                ..
                            } => break notification,
                            _ => {}
                        }
                    }
                    result = &mut prompt, if response.is_none() => {
                        panic!("Codex prompt completed before Process Exited: {result:?}")
                    }
                }
        };
        match exited.kind() {
            ConversationNotificationKind::Process {
                label,
                state: ProcessState::Exited { exit_code },
            } => {
                assert_eq!(label, &running_label);
                assert_eq!(*exit_code, Some(7));
            }
            other => panic!("expected Codex Process Exited, received {other:?}"),
        }

        let response = match response {
            Some(response) => response,
            None => tokio::time::timeout_at(deadline, &mut prompt)
                .await
                .expect("Codex process prompt must reach terminal"),
        };
        assert_eq!(response.output(), format!("{token}_DONE"));
        drop(prompt);

        receiver
    }))
    .catch_unwind()
    .await;

    // Preserve the original assertion failure until all owned resources settle.
    let conversation_closed = conversation.close().await;
    let drained = if let Ok(Ok(receiver)) = &mut observed {
        Some(
            std::panic::AssertUnwindSafe(DeliveryScenario::drain(receiver))
                .catch_unwind()
                .await,
        )
    } else {
        None
    };
    let workspace_closed = workspace.close().await;
    let agent_stopped = agent.shutdown().await;
    match observed {
        Ok(result) => {
            drop(result.expect("Codex process delivery must finish within its original deadline"))
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Some(Err(panic)) = drained {
        std::panic::resume_unwind(panic);
    }
    conversation_closed.expect("Codex conversation must close");
    workspace_closed.expect("Codex workspace must close");
    agent_stopped.expect("Codex agent must shut down");
}

#[tokio::test]
async fn opencode_question_delivery_replays_resolves_once_and_drains() {
    let deadline = tokio::time::Instant::now()
        + DeliveryScenario::OPENCODE_INTERACTION_TIMEOUT
        + DeliveryScenario::OPENCODE_TERMINAL_TIMEOUT;
    let fixture = tokio::time::timeout_at(deadline, StrictFixture::new())
        .await
        .expect("strict-live question storage setup must stay bounded");
    let (config, address) = StrictFixture::opencode_config(
        &StrictFixture::unique("strict-opencode-question"),
        Duration::from_secs(60),
    );
    let (agent, workspace, conversation) = fixture
        .hierarchy_before(
            config,
            &StrictFixture::unique("strict-opencode-question-workspace"),
            "OpenCode question delivery",
            deadline,
        )
        .await;
    let mut primary_failure = None;
    let mut observed = NativeCleanup::observe(deadline, async {
    let model = DeliveryScenario::opencode_model(&agent).await;
    let native = NativeQuestionReadback::new(&format!("http://{address}"), deadline).await;
    let session = native.created_session(&std::collections::BTreeSet::new()).await;
    let mut receiver = conversation
        .notification_stream()
        .expect("first OpenCode receiver acquisition must succeed");
    assert!(
        matches!(conversation.notification_stream(), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Busy))
    );

    let request = PromptRequest::new(DeliveryScenario::opencode_question_prompt())
        .expect("OpenCode question prompt must be valid")
        .with_model(model)
        .with_timeout(DeliveryScenario::OPENCODE_INTERACTION_TIMEOUT)
        .expect("OpenCode question timeout must be valid");
    let stream = conversation
        .prompt_stream(request)
        .await
        .expect("OpenCode question stream must be admitted");
    let mut prompt = Box::pin(NativeCleanup::observe_stream(stream));
    eprintln!("question stream phase=await_interaction");
    let interaction = std::panic::AssertUnwindSafe(async {
    tokio::time::timeout(DeliveryScenario::OPENCODE_INTERACTION_TIMEOUT, async {
        loop {
            tokio::select! {
                biased;
                notification = receiver.recv() => {
                    let notification = notification
                        .expect("OpenCode delivery must remain open")
                        .expect("OpenCode notification must succeed");
                    if matches!(
                        notification.kind(),
                        ConversationNotificationKind::Interaction { .. }
                    ) {
                        break notification;
                    }
                }
                result = &mut prompt => {
                    panic!("OpenCode prompt completed before its question: {result:?}")
                }
            }
        }
    })
    .await
    .expect("OpenCode question must arrive")
    }).catch_unwind().await;
    let interaction = match interaction {
        Ok(interaction) => interaction,
        Err(failure) => {
            eprintln!("question native failure capture=started");
            let context = native.capture_failure(&session, DeliveryScenario::opencode_question_prompt(),
                &mut primary_failure, failure).await;
            eprintln!("question native failure evidence={context}");
            std::panic::resume_unwind(primary_failure.take().expect("original question failure remains owned"));
        }
    };
    eprintln!("question stream phase=interaction_received");
    let interaction_id = interaction.id().clone();

    drop(receiver);
    receiver = conversation
        .notification_stream()
        .expect("receiver drop must release the OpenCode delivery lease");
    let replay = DeliveryScenario::receive(&mut receiver).await;
    assert_eq!(replay.id(), &interaction_id);
    assert_eq!(replay.kind(), interaction.kind());

    let ConversationNotificationKind::Interaction {
        response_schema,
        dismissible,
        ..
    } = replay.kind()
    else {
        unreachable!("the selected notification is an interaction")
    };
    assert!(*dismissible);
    DeliveryScenario::assert_question_schema(
        response_schema
            .as_ref()
            .expect("OpenCode question must publish a response schema"),
    );

    let invalid = conversation
        .resolve_notification(interaction_id.clone(), NotificationResolution::Allow)
        .await;
    assert!(
        matches!(invalid, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput))
    );
    let evidence = std::panic::AssertUnwindSafe(native.pending(&session, DeliveryScenario::opencode_question_prompt(),
        &StrictFixture::required("TEST_OLLAMA_MODEL_OPENCODE_SHELL"))).catch_unwind().await;
    let evidence = match evidence {
        Ok(evidence) => evidence,
        Err(failure) => {
            let context = native.capture_failure(&session, DeliveryScenario::opencode_question_prompt(),
                &mut primary_failure, failure).await;
            eprintln!("question native failure evidence={context}");
            std::panic::resume_unwind(primary_failure.take().expect("original question failure remains owned"));
        }
    };
    eprintln!("question stream phase=pending_verified");
    let answer = DeliveryScenario::resolve_opencode_question(&conversation, &replay).await;
    eprintln!("question stream phase=answer_resolved");
    let duplicate = conversation
        .resolve_notification(
            interaction_id,
            NotificationResolution::Deny {
                reason: "duplicate".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(duplicate, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Conflict | agent_sdk::error::ErrorKind::NotFound))
    );

    eprintln!("question stream phase=await_terminal");
    let response = tokio::time::timeout(DeliveryScenario::OPENCODE_TERMINAL_TIMEOUT, &mut prompt)
        .await
        .expect("resolved OpenCode prompt must reach terminal");
    drop(prompt);

    let grade = native.completed(evidence, &session, &answer, response.output()).await;
    (receiver, grade, native.version().to_owned())
    }).await;
    if let Some(failure) = primary_failure {
        eprintln!("question native failure capture=incomplete; original_failure_retained=true");
        observed = Err(failure);
    }
    let mut cleanup = NativeCleanup::new(Duration::from_secs(60));
    cleanup.settle(conversation.close()).await;
    if let Ok(Ok((receiver, _, _))) = &mut observed {
        cleanup
            .settle(async {
                DeliveryScenario::drain(receiver).await;
                Ok::<(), agent_sdk::SdkError>(())
            })
            .await;
    }
    cleanup.settle(workspace.close()).await;
    cleanup.settle(agent.shutdown()).await;
    let (receiver, grade, version) = cleanup.finish(observed);
    drop(receiver);
    grade.emit("strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains", &version);
}
