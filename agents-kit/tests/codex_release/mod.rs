use super::{CodexFixture, CodexPeer, FirstStart, NativeCleanup};
use agents_kit::error::{ErrorDetail, ErrorKind};
use agents_kit::notification::{ConversationNotificationKind, NotificationResolution};
use agents_kit::prompt::{PromptEvent, PromptRequest};
use agents_kit::{AgentBackend, ConversationBackend, WorkspaceBackend};
use futures_util::FutureExt;
use serde_json::{Value, json};
use std::time::Duration;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{StateKey, StateRecord};
use storage_kit::{StateStore, StateStoreBackend, StateTransactionBackend};

#[tokio::test]
async fn materialization_rejects_invalid_history_and_cleans_native_creation() {
    use agents_kit::config::{AgentConfig, CodexConfig};
    use agents_kit::conversation::NewConversation;
    use agents_kit::workspace::{
        NewWorkspace, WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
        WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
    };
    use agents_kit::{AgentBuilder, AgentStorage};
    use storage_kit::state::record::StateListRequest;
    for (reply, expected) in [
        (
            json!({"threadSource":"foreign-source"}),
            ErrorKind::InvalidState,
        ),
        (json!({"threadSource":null}), ErrorKind::InvalidState),
        (json!({"id":"foreign-thread"}), ErrorKind::InvalidState),
        (
            json!({"cwd":std::env::temp_dir().join("foreign-cwd")}),
            ErrorKind::InvalidState,
        ),
        (json!({"turns":null}), ErrorKind::Protocol),
        (json!({"turns":42}), ErrorKind::Protocol),
        (
            json!({"turns":[{"id":"unexpected-turn","status":"completed","error":null,"items":[],"itemsView":"full"}]}),
            ErrorKind::InvalidState,
        ),
        (
            json!({"error":{"code":-32600,"message":"secret-native-read-rejection"}}),
            ErrorKind::Integration,
        ),
    ] {
        let peer = CodexPeer::start_with(FirstStart::Materialization(reply.clone())).await;
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
                .unwrap()
                .into(),
            Duration::from_secs(3),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let agent = AgentBuilder::create(config, AgentStorage::new(raw.clone()))
            .build()
            .await
            .unwrap();
        let root = std::env::temp_dir().join(format!(
            "agents-kit-codex-materialize-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let context = WorkspaceContext::new(
            root.clone(),
            vec![WorkspaceRoot::new(root.clone(), WorkspaceRootAccess::ReadWrite).unwrap()],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .unwrap();
        let workspace = agent
            .new_workspace(
                NewWorkspace::new("materialization", context, WorkspaceDefaults::empty()).unwrap(),
            )
            .await
            .unwrap();
        let mut unexpected = None;
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                let error = match workspace
                    .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                    .await
                {
                    Err(error) => error,
                    Ok(conversation) => {
                        unexpected = Some(conversation);
                        panic!("invalid materialization published a conversation: {reply}");
                    }
                };
                assert_eq!(error.kind(), expected, "{reply}: {error:?}");
                assert!(!format!("{error} {error:?}").contains("secret-native"));
                assert!(
                    workspace.list_conversations().await.unwrap().is_empty(),
                    "failed creation cannot publish portable inventory"
                );
                assert!(
                    raw.list(StateListRequest::new("agents_kit_codex_threads_v1").unwrap())
                        .await
                        .unwrap()
                        .records
                        .is_empty()
                );
                assert!(
                    raw.list(StateListRequest::new("agents_kit_codex_cleanup_v1").unwrap())
                        .await
                        .unwrap()
                        .records
                        .is_empty(),
                    "successful native deletion must settle its durable cleanup obligation"
                );
                let requests = peer.requests.lock().unwrap();
                assert_eq!(
                    requests
                        .iter()
                        .filter(|request| request["method"] == "thread/start")
                        .count(),
                    1
                );
                assert_eq!(
                    requests
                        .iter()
                        .filter(|request| request["method"] == "thread/delete")
                        .count(),
                    1
                );
            }))
            .catch_unwind()
            .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        if let Some(conversation) = unexpected {
            cleanup.settle(conversation.close()).await;
        }
        cleanup.settle(workspace.close()).await;
        cleanup.settle(agent.shutdown()).await;
        cleanup
            .settle(async {
                peer.shutdown().await;
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn empty_paginated_conversation_materializes_before_cold_reopen() {
    let CodexFixture {
        peer,
        agent,
        workspace,
        conversation,
        root,
    } = CodexFixture::build_with_peer(CodexPeer::start_with(FirstStart::PaginatedEmpty).await)
        .await;
    let workspace_id = workspace.id();
    let conversation_id = conversation.id();
    let mut owned_workspace = Some(workspace);
    let mut owned_conversation = Some(conversation);
    let observed = std::panic::AssertUnwindSafe(async {
        owned_workspace.take().unwrap().close().await.unwrap();
        drop(owned_conversation.take());
        let workspace = owned_workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
        let conversation = workspace
            .get_conversation(conversation_id.clone())
            .await
            .expect("empty paginated conversation must survive cold resume");
        assert_eq!(conversation.id(), conversation_id);
        owned_conversation = Some(conversation);
        let requests = peer.requests.lock().unwrap();
        assert!(
            !requests
                .iter()
                .any(|request| request["method"] == "turn/start"),
            "empty conversation must be durable without a prompt"
        );
        let detach = requests
            .iter()
            .position(|request| request["method"] == "thread/unsubscribe")
            .unwrap();
        assert!(
            requests[..detach]
                .iter()
                .any(|request| request["method"] == "thread/read"
                    && request["params"]["threadId"] == "stream-thread"
                    && request["params"]["includeTurns"] == true),
            "full-history materialization must precede native detach"
        );
    })
    .catch_unwind()
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    if let Some(conversation) = owned_conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(workspace) = owned_workspace {
        cleanup.settle(workspace.close()).await;
    }
    cleanup.settle(agent.shutdown()).await;
    cleanup
        .settle(async {
            peer.shutdown().await;
            Ok::<(), agents_kit::KitError>(())
        })
        .await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed.map(Ok));
}

impl CodexFixture {
    async fn release_request(
        method: &str,
        mut params: Value,
        title: Option<&str>,
        code: Option<i64>,
    ) {
        let mut fixture = Self::build().await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("request").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            params["threadId"] = json!("stream-thread");
            params["turnId"] = json!(turn);
            let id = json!("callback-distinct-from-item");
            fixture.peer.events.send(json!({"id":id,"method":method,"params":params})).await.unwrap();
            let (notification, response) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    response = fixture.peer.responses.recv() => (None, response.unwrap()),
                    notification = notifications.recv() => {
                        let notification = notification.unwrap().unwrap();
                        let resolution = match notification.kind() {
                            ConversationNotificationKind::Interaction { response_schema: None, .. } => NotificationResolution::Allow,
                            ConversationNotificationKind::Interaction { .. } => NotificationResolution::Submit { content: json!({"answer":"answer"}) },
                            kind => panic!("unexpected request event {kind:?}"),
                        };
                        let result = fixture.conversation.resolve_notification(notification.id().clone(), resolution).await;
                        assert!(matches!(result, Err(ref error) if error.kind() == ErrorKind::Conflict), "{result:?}");
                        (Some(notification), fixture.peer.responses.recv().await.unwrap())
                    }
                }
            }).await.unwrap();
            fixture.peer.complete(&turn, "ordinary answer").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(_)))));
            assert_eq!(response["id"], id);
            assert_eq!(response["error"]["code"].as_i64(), code, "{method}: {params}");
            if let Some(title) = title {
                let notification = notification.unwrap();
                assert!(!format!("{notification:?}").contains("secret-process-id"));
                assert!(matches!(notification.kind(), ConversationNotificationKind::Interaction { title: actual, .. } if actual == title));
                if title == "Terminal input approval" && params.get("reason").is_none_or(|reason| reason == "") {
                    assert!(matches!(notification.kind(), ConversationNotificationKind::Interaction { body, .. } if body == "Approve terminal input?"));
                }
            } else {
                assert!(notification.is_none(), "unsupported/malformed request published an interaction");
            }
            assert!(fixture.peer.responses.try_recv().is_err(), "one response per request");
        }).catch_unwind().await;
        fixture.finish_release(observed, None).await;
    }
}

#[tokio::test]
async fn approval_kind_is_typed_and_keeps_callback_identity() {
    for (kind, title, code) in [
        (None, Some("Command approval"), None),
        (Some(json!("command")), Some("Command approval"), None),
        (
            Some(json!("writeStdin")),
            Some("Terminal input approval"),
            None,
        ),
        (Some(Value::Null), None, Some(-32602)),
        (Some(json!("unknown")), None, Some(-32602)),
        (Some(json!(3)), None, Some(-32602)),
    ] {
        let mut params = json!({"itemId":"same-command","approvalId":"separate-approval","startedAtMs":0,
            "command":"safe command", "availableDecisions":["accept","cancel"]});
        if let Some(kind) = kind {
            params["kind"] = kind;
        }
        CodexFixture::release_request("item/commandExecution/requestApproval", params, title, code)
            .await;
    }
}

#[tokio::test]
async fn blocking_input_uses_selected_default_not_deprecated_timer() {
    for (blocking, timer, supported, code) in [
        (None, None, true, None),
        (Some(Value::Null), None, true, None),
        (Some(json!(true)), Some(json!(1)), true, None),
        (Some(json!(false)), None, false, Some(-32601)),
        (Some(json!("true")), None, false, Some(-32602)),
        (Some(json!(true)), Some(json!("bad")), false, Some(-32602)),
    ] {
        let mut params = json!({"itemId":"question","questions":[{"id":"q","header":"Question","question":"Answer?"}]});
        if let Some(blocking) = blocking {
            params["isBlocking"] = blocking;
        }
        if let Some(timer) = timer {
            params["autoResolutionMs"] = timer;
        }
        CodexFixture::release_request(
            "item/tool/requestUserInput",
            params,
            supported.then_some("Question"),
            code,
        )
        .await;
    }
}

#[tokio::test]
async fn extended_elicitations_are_unsupported_and_validate_native_fields() {
    for (params, code) in [
        (
            json!({"mode":"openaiForm","message":"form","requestedSchema":{}}),
            -32601,
        ),
        (
            json!({"mode":"openaiForm","message":5,"requestedSchema":{}}),
            -32602,
        ),
        (
            json!({"mode":"openai/userVerification","title":"verify","description":"private","challenge":"secret-token"}),
            -32601,
        ),
        (
            json!({"mode":"openai/userVerification","title":"verify","description":"private","challenge":false}),
            -32602,
        ),
    ] {
        let mut params = params;
        params["serverName"] = json!("native-server");
        CodexFixture::release_request("mcpServer/elicitation/request", params, None, Some(code))
            .await;
    }
}

#[tokio::test]
async fn permission_path_context_is_unsupported_not_malformed() {
    for (cwd, code) in [
        (json!("relative/path"), -32601),
        (
            json!(if cfg!(windows) {
                "/remote/path"
            } else {
                "C:\\remote\\path"
            }),
            -32601,
        ),
        (json!(42), -32602),
    ] {
        CodexFixture::release_request(
            "item/permissions/requestApproval",
            json!({"cwd":cwd,"itemId":"permissions",
            "startedAtMs":0,"permissions":{}}),
            None,
            Some(code),
        )
        .await;
    }
}

// Main's passive-async expectation is superseded by the selected foreground-only
// contract. Keep both live and recovered rejection; no async item becomes output.
#[tokio::test]
async fn async_final_answer_is_unsupported_in_live_and_recovered_output() {
    for recovered in [false, true] {
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let asynchronous = json!({"id":"async","type":"agentMessage","text":" complete async \n","phase":"final_answer","delivery":"async","questions":null});
        let peer = CodexPeer::start_with(if recovered {
            FirstStart::RecoveredTurn {
                status: "completed".into(),
                error: Value::Null,
                items: vec![asynchronous.clone()],
            }
        } else {
            FirstStart::Ordinary
        })
        .await;
        let mut fixture = CodexFixture::build_with_peer(peer).await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("answer").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            if !recovered {
                fixture.peer.notify("item/completed", json!({"threadId":"stream-thread","turnId":turn,"completedAtMs":1,"item":asynchronous})).await;
            }
            let error = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap().unwrap().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert!(stream.recv().await.is_none());
        }).catch_unwind().await;
        fixture.finish_release(observed, Some(baseline + 1)).await;
    }
}

impl CodexFixture {
    pub(super) async fn release_event(
        method: &str,
        mut params: Value,
        expected: Option<ErrorKind>,
    ) {
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut fixture = Self::build_with_store(CodexPeer::start().await, raw.clone()).await;
        let pending_cleanup = (method == "thread/reverted" && expected == Some(ErrorKind::InvalidState)).then(|| {
            let owner = "conversation_00000000000000000000000000000001".parse::<agents_kit::identity::ConversationId>().unwrap();
            let key = StateKey::new("agents_kit_codex_cleanup_v1", format!("{}:delete:conversation:{}", fixture.agent.id().as_str(), owner.as_str())).unwrap();
            let value = json!({"version":1,"agent_id":fixture.agent.id(),"owner":{"owner":"conversation","id":owner},"action":"delete","thread_ids":["previous-thread"]});
            (key, value)
        });
        let prompt_key = StateKey::new(
            "agents_kit_codex_prompts_v1",
            fixture.conversation.id().as_str(),
        )
        .unwrap();
        let mut accepted_prompt = None;
        let foreign_metadata = params["threadId"] == "foreign-thread" && expected.is_none();
        let observed = std::panic::AssertUnwindSafe(async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("event").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"accepted","delta":"accepted"})).await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "accepted"));
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let record = raw.load(prompt_key.clone()).await.unwrap().unwrap();
                    if record.record.value["state"]["turn_id"] == turn {
                        accepted_prompt = Some(record.record.value);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("native receipt must commit before scoped metadata");
            if let Some((key, value)) = &pending_cleanup {
                let mut transaction = raw.begin_transaction().await.unwrap();
                transaction.put(StateRecord::new(key.clone(), value.clone())).await.unwrap();
                transaction.commit().await.unwrap();
            }
            if params["turnId"] == "turn" { params["turnId"] = json!(turn); }
            fixture.peer.notify(method, params.clone()).await;
            if foreign_metadata {
                fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"foreign-thread","turnId":"foreign-turn","itemId":"foreign-answer","delta":"foreign output"})).await;
            }
            // Wait for either an owning error or a barrier in the same inbound queue.
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"barrier","delta":"barrier"})).await;
            let first = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap().unwrap();
            match expected {
                Some(kind) => {
                    let error = first.expect_err(&format!("{method}: {params}: expected {kind:?}"));
                    assert_eq!(error.kind(), kind, "{method}: {params}");
                    if method == "thread/reverted" && kind == ErrorKind::InvalidState {
                        assert_eq!(error.detail(), Some(&ErrorDetail::AttachmentMismatch));
                        let retained = raw.load(prompt_key.clone()).await.unwrap().expect("reverted history must preserve recovery and cleanup ownership");
                        assert_eq!(&retained.record.value, accepted_prompt.as_ref().unwrap(), "invalidation must preserve the entire accepted prompt snapshot");
                        let (cleanup_key, cleanup_value) = pending_cleanup.as_ref().unwrap();
                        assert_eq!(*raw.load(cleanup_key.clone()).await.unwrap().expect("history invalidation must preserve native cleanup obligation").record.value, *cleanup_value);
                    }
                    assert!(!format!("{error:?}").contains("secret-token"));
                    assert!(stream.recv().await.is_none());
                }
                None => {
                    assert!(matches!(&first, Ok(PromptEvent::OutputDelta { text }) if text == "barrier"), "{method}: {first:?}");
                    if foreign_metadata {
                        let retained = raw.load(prompt_key.clone()).await.unwrap().expect("foreign metadata cannot discard the accepted prompt");
                        assert_eq!(&retained.record.value, accepted_prompt.as_ref().unwrap(), "foreign metadata must preserve the original turn's complete durable state");
                        let conversations = fixture.workspace.list_conversations().await.unwrap();
                        assert_eq!(conversations.len(), 1, "foreign metadata must not create a conversation");
                        assert_eq!(conversations[0].id(), &fixture.conversation.id());
                        fixture.peer.notify(method, params.clone()).await;
                    }
                    assert!(tokio::time::timeout(Duration::from_millis(20), notifications.recv()).await.is_err(), "private metadata published a notification");
                    drop(notifications);
                    let mut replay = fixture.conversation.notification_stream().unwrap();
                    assert!(tokio::time::timeout(Duration::from_millis(20), replay.recv()).await.is_err(), "private metadata left a durable notification");
                    fixture.peer.complete(&turn, "ordinary final").await;
                    assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "ordinary final"));
                    assert!(stream.recv().await.is_none());
                }
            }
        }).catch_unwind().await;
        fixture
            .finish_release(observed, expected.map(|_| baseline + 1))
            .await;
        if let Some((key, value)) = pending_cleanup {
            assert_eq!(
                &raw.load(prompt_key)
                    .await
                    .unwrap()
                    .expect("runtime revocation must retain prompt recovery")
                    .record
                    .value,
                accepted_prompt.as_ref().unwrap(),
                "revocation must preserve the entire accepted prompt snapshot"
            );
            assert_eq!(
                *raw.load(key)
                    .await
                    .unwrap()
                    .expect("runtime revocation cannot discard pending native cleanup")
                    .record
                    .value,
                value
            );
        }
    }
}

#[tokio::test]
async fn metadata_objects_reject_positional_arrays_and_preserve_optional_nulls() {
    for read_state in [
        json!({"revision":""}),
        json!({"firstUnread":null,"revision":"opaque-revision"}),
    ] {
        CodexFixture::release_event(
            "thread/readState/changed",
            json!({"threadId":"stream-thread","readState":read_state}),
            None,
        )
        .await;
    }
    for params in [
        json!({"threadId":"stream-thread","readState":[null,"opaque-revision"]}),
        json!(["stream-thread",{"firstUnread":null,"revision":"opaque-revision"}]),
    ] {
        CodexFixture::release_event(
            "thread/readState/changed",
            params,
            Some(ErrorKind::Protocol),
        )
        .await;
    }
}

#[tokio::test]
async fn selected_notifications_validate_fields_and_classify_without_output() {
    let cases = [
        (
            "thread/reverted",
            json!({"threadId":"stream-thread"}),
            "threadId",
            Some(ErrorKind::InvalidState),
        ),
        (
            "thread/attachment/updated",
            json!({"threadId":"stream-thread","attachmentType":"custom","identityKey":"key","attachmentId":"attachment","operation":"created"}),
            "operation",
            None,
        ),
        (
            "thread/queue/changed",
            json!({"threadId":"stream-thread"}),
            "threadId",
            Some(ErrorKind::Unsupported),
        ),
        (
            "project/changed",
            json!({"projectId":"project","changeType":"created"}),
            "changeType",
            None,
        ),
        (
            "thread/project/updated",
            json!({"threadId":"stream-thread","projectId":null}),
            "projectId",
            None,
        ),
        (
            "thread/environment/connected",
            json!({"threadId":"stream-thread","environmentId":"environment"}),
            "environmentId",
            None,
        ),
        (
            "thread/environment/disconnected",
            json!({"threadId":"stream-thread","environmentId":"environment"}),
            "environmentId",
            None,
        ),
        (
            "autoApprovalReview/strictReviewRequired",
            json!({"threadId":"stream-thread","turnId":"turn","startedAtMs":-1}),
            "startedAtMs",
            None,
        ),
        (
            "rawResponse/completed",
            json!({"threadId":"stream-thread","turnId":"turn","responseId":"response","usage":null,"usageMetadata":null}),
            "usage",
            None,
        ),
        (
            "mcpServer/event/stream/notification",
            json!({"subscriptionId":"subscription","notification":{"method":"event","params":null}}),
            "notification",
            Some(ErrorKind::Unsupported),
        ),
        (
            "account/gatewayOAuth/changed",
            json!({"authUrl":null,"providerId":"provider","status":"notReady","error":null}),
            "status",
            None,
        ),
        (
            "modelProvider/authRecoveryStarted",
            json!({"threadId":"stream-thread","turnId":"turn","provider":"provider","message":"secret-token"}),
            "message",
            None,
        ),
        (
            "modelProvider/authRecoveryCompleted",
            json!({"threadId":"stream-thread","turnId":"turn","provider":"provider","message":"secret-token"}),
            "provider",
            None,
        ),
        (
            "thread/realtime/item/started",
            json!({"threadId":"stream-thread","item":{"id":"item","realtimeSessionId":"session","type":"realtimeSessionStarted"}}),
            "item",
            Some(ErrorKind::Unsupported),
        ),
        (
            "thread/realtime/item/completed",
            json!({"threadId":"stream-thread","item":{"id":"item","realtimeSessionId":"session","type":"realtimeSessionClosed","outcome":"ended"}}),
            "item",
            Some(ErrorKind::Unsupported),
        ),
        (
            "thread/realtime/item/transcript/delta",
            json!({"threadId":"stream-thread","itemId":"item","delta":""}),
            "delta",
            Some(ErrorKind::Unsupported),
        ),
    ];
    for (method, params, field, outcome) in cases {
        CodexFixture::release_event(method, params.clone(), outcome).await;
        let mut malformed = params;
        malformed[field] = json!(false);
        CodexFixture::release_event(method, malformed, Some(ErrorKind::Protocol)).await;
    }
    CodexFixture::release_event(
        "thread/project/updated",
        json!({"threadId":"stream-thread"}),
        None,
    )
    .await;
}

#[tokio::test]
async fn selected_items_validate_changed_metadata_and_exclude_nonanswers() {
    let cases = [
        (
            json!({"type":"agentMessage","text":"async question","delivery":"async","questions":[{"title":"Choose","options":["a"]}]}),
            "questions",
            ErrorKind::Unsupported,
        ),
        (
            json!({"type":"collabAgentToolCall","tool":"wait","status":"interrupted","senderThreadId":"stream-thread","receiverThreadIds":[],"agentsStates":{"worker":{"status":"interrupted","message":null}}}),
            "status",
            ErrorKind::Unsupported,
        ),
        (
            json!({"type":"subAgentActivity","kind":"completed","agentThreadId":"worker","agentPath":"worker"}),
            "kind",
            ErrorKind::Unsupported,
        ),
        (
            json!({"type":"dynamicToolCall","tool":"native","arguments":{},"status":"completed","contentItems":[{"type":"inputAudio","audioUrl":"secret-token"}],"success":true}),
            "contentItems",
            ErrorKind::Unsupported,
        ),
    ];
    for (mut item, field, outcome) in cases {
        item["id"] = json!("item");
        let params =
            json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":1,"item":item});
        CodexFixture::release_event("item/completed", params.clone(), Some(outcome)).await;
        let mut malformed = params;
        malformed["item"][field] = json!(false);
        CodexFixture::release_event("item/completed", malformed, Some(ErrorKind::Protocol)).await;
    }
    for output in [
        json!("native output"),
        json!([{"type":"input_text","text":"native output"}]),
    ] {
        CodexFixture::release_event("item/completed", json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":1,
            "item":{"id":"result","type":"functionCallOutput","name":"native","namespace":null,"output":output}}), None).await;
    }
    CodexFixture::release_event(
        "item/completed",
        json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":1,
        "item":{"id":"result","type":"functionCallOutput","name":"native","output":false}}),
        Some(ErrorKind::Protocol),
    )
    .await;
}

impl CodexFixture {
    pub(super) async fn finish_release(
        self,
        observed: Result<(), Box<dyn std::any::Any + Send>>,
        revoked: Option<usize>,
    ) {
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        if let Some(baseline) = revoked {
            cleanup
                .settle(async {
                    self.revoke(baseline).await;
                    Ok::<(), agents_kit::KitError>(())
                })
                .await;
        } else {
            cleanup.settle(self.conversation.close()).await;
            cleanup.settle(self.workspace.close()).await;
            cleanup.settle(self.agent.shutdown()).await;
            cleanup
                .settle(async {
                    self.peer.shutdown().await;
                    Ok::<(), agents_kit::KitError>(())
                })
                .await;
            cleanup
                .settle(async { std::fs::remove_dir_all(self.root) })
                .await;
        }
        cleanup.finish(observed.map(Ok));
    }
}

#[tokio::test]
async fn review_stdin_approval_never_exposes_process_identity() {
    for reason in [None, Some(""), Some("Allow input to the approved process")] {
        let mut params = json!({"itemId":"same-command","kind":"writeStdin","startedAtMs":0,
            "command":"write_stdin --session-id secret-process-id input", "availableDecisions":["accept","cancel"]});
        if let Some(reason) = reason {
            params["reason"] = json!(reason);
        }
        CodexFixture::release_request(
            "item/commandExecution/requestApproval",
            params,
            Some("Terminal input approval"),
            None,
        )
        .await;
    }
}

#[tokio::test]
async fn review_permissions_validate_wire_before_context_support() {
    for (field, value) in [
        ("permissions", json!(42)),
        ("environmentId", json!(42)),
        ("reason", json!(42)),
    ] {
        let mut params =
            json!({"cwd":"relative/path","itemId":"permissions","startedAtMs":0,"permissions":{}});
        params[field] = value;
        CodexFixture::release_request(
            "item/permissions/requestApproval",
            params,
            None,
            Some(-32602),
        )
        .await;
    }
}

#[tokio::test]
async fn review_notification_scope_preserves_native_thread_and_turn() {
    for thread in ["stream-thread", "child-thread", "foreign-thread"] {
        CodexFixture::release_event(
            "thread/reverted",
            json!({"threadId":thread}),
            (thread != "foreign-thread").then_some(ErrorKind::InvalidState),
        )
        .await;
    }
    for method in [
        "rawResponse/completed",
        "modelProvider/authRecoveryStarted",
        "modelProvider/authRecoveryCompleted",
        "autoApprovalReview/strictReviewRequired",
    ] {
        for (thread, turn, outcome) in [
            ("stream-thread", "turn", None),
            ("child-thread", "child-turn", None),
            ("foreign-thread", "foreign-turn", None),
            ("stream-thread", "wrong-turn", Some(ErrorKind::Protocol)),
            ("child-thread", "unobserved-child-turn", None),
            ("stream-thread ", "turn", Some(ErrorKind::Protocol)),
            ("stream-thread", " turn-1", Some(ErrorKind::Protocol)),
        ] {
            CodexFixture::release_event(
                method,
                json!({"threadId":thread,"turnId":turn,"responseId":"response",
                "provider":"provider","message":"secret-token","startedAtMs":0}),
                outcome,
            )
            .await;
        }
    }
    CodexFixture::release_event("project/changed", json!({"projectId":"project","changeType":"updated","threadId":"foreign-thread","turnId":"foreign-turn"}), None).await;
}

#[tokio::test]
async fn review_auxiliary_items_require_ids_in_history_and_live() {
    let items = [
        json!({"type":"functionCallOutput","name":"native","output":"private"}),
        json!({"type":"dynamicToolCall","tool":"native","arguments":{},"status":"completed"}),
        json!({"type":"collabAgentToolCall","tool":"wait","status":"interrupted","senderThreadId":"stream-thread","receiverThreadIds":[],"agentsStates":{}}),
        json!({"type":"subAgentActivity","kind":"completed","agentThreadId":"child-thread","agentPath":"worker"}),
    ];
    for item in items {
        for id in [None, Some(json!(42))] {
            let mut item = item.clone();
            if let Some(id) = id {
                item["id"] = id;
            }
            for recovered in [true, false] {
                if recovered {
                    let baseline = tokio::runtime::Handle::current()
                        .metrics()
                        .num_alive_tasks();
                    let peer = CodexPeer::start_with(FirstStart::RecoveredTurn { status:"completed".into(), error:Value::Null,
                        items:vec![item.clone(), json!({"id":"answer","type":"agentMessage","text":"ordinary","phase":"final_answer"})] }).await;
                    let fixture = CodexFixture::build_with_peer(peer).await;
                    let observed = std::panic::AssertUnwindSafe(async {
                        let error = fixture
                            .conversation
                            .prompt(PromptRequest::new("recover").unwrap())
                            .await
                            .unwrap_err();
                        assert_eq!(error.kind(), ErrorKind::Protocol, "{item}");
                    })
                    .catch_unwind()
                    .await;
                    fixture.finish_release(observed, Some(baseline + 1)).await;
                } else {
                    CodexFixture::release_event("item/completed", json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":1,"item":item}), Some(ErrorKind::Protocol)).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn review_collaboration_effort_rejects_empty_and_accepts_custom() {
    for (effort, outcome) in [
        ("", ErrorKind::Protocol),
        ("custom-effort", ErrorKind::Unsupported),
    ] {
        CodexFixture::release_event("item/completed", json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":1,
            "item":{"id":"item","type":"collabAgentToolCall","tool":"wait","status":"interrupted","senderThreadId":"stream-thread",
            "receiverThreadIds":[],"agentsStates":{},"reasoningEffort":effort}}), Some(outcome)).await;
    }
}

#[tokio::test]
async fn review_metadata_descendant_turn_is_not_root_turn() {
    CodexFixture::release_event(
        "rawResponse/completed",
        json!({"threadId":"child-thread","turnId":"child-turn","responseId":"response"}),
        None,
    )
    .await;
}

#[tokio::test]
async fn review_metadata_ids_are_not_trimmed_or_reinterpreted() {
    CodexFixture::release_event(
        "rawResponse/completed",
        json!({"threadId":"stream-thread","turnId":" turn-1","responseId":"response"}),
        Some(ErrorKind::Protocol),
    )
    .await;
}

#[tokio::test]
async fn config_removed_fanout_is_not_required_and_active_features_are() {
    use agents_kit::conversation::NewConversation;
    let fixture = CodexFixture::build().await;
    let CodexFixture {
        peer,
        agent,
        workspace,
        conversation,
        root,
    } = fixture;
    conversation.close().await.unwrap();
    let observed = std::panic::AssertUnwindSafe(async {
        *peer.features.lock().unwrap() = json!({"data":[{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":false}],"nextCursor":null});
        let created = workspace.new_conversation(NewConversation::new(None,None,Vec::new()).unwrap()).await;
        let conversation = match created { Ok(value) => value, Err(error) => panic!("removed feature cannot block construction: {error:?}") };
        conversation.close().await.unwrap();
        for features in [json!([{ "name":"multi_agent","enabled":false }]),
            json!([{"name":"multi_agent","enabled":true},{"name":"multi_agent_v2","enabled":false}]),
            json!([{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":"false"}])] {
            *peer.features.lock().unwrap() = json!({"data":features,"nextCursor":null});
            assert!(workspace.new_conversation(NewConversation::new(None,None,Vec::new()).unwrap()).await.is_err());
        }
        let requests = peer.requests.lock().unwrap();
        for request in requests.iter().filter(|request| request["method"] == "thread/start") {
            assert!(request["params"]["config"].get("features.enable_fanout").is_none());
            assert_eq!(request["params"]["config"]["features.multi_agent"], false);
            assert_eq!(request["params"]["config"]["features.multi_agent_v2"], false);
        }
    }).catch_unwind().await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    cleanup.settle(workspace.close()).await;
    cleanup.settle(agent.shutdown()).await;
    cleanup
        .settle(async {
            peer.shutdown().await;
            Ok::<(), agents_kit::KitError>(())
        })
        .await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed.map(Ok));
}

#[tokio::test]
async fn permission_grant_paths_require_supported_context_after_wire_validation() {
    for (permissions, code) in [
        (json!({"fileSystem":{"read":["relative/path"]}}), -32601),
        (
            json!({"fileSystem":{"entries":[{"path":{"type":"path","path":"relative/path"},"access":"read"}]}}),
            -32601,
        ),
        (json!({"fileSystem":{"read":[42]}}), -32602),
    ] {
        CodexFixture::release_request(
            "item/permissions/requestApproval",
            json!({"cwd":std::env::temp_dir(),
            "itemId":"permissions","startedAtMs":0,"permissions":permissions}),
            None,
            Some(code),
        )
        .await;
    }
}

#[tokio::test]
async fn selected_runtime_rejects_previous_version_before_handle_publication() {
    use agents_kit::config::{AgentConfig, CodexConfig};
    use agents_kit::{AgentBuilder, AgentStorage};
    let peer = CodexPeer::start().await;
    *peer.identity.lock().unwrap() = json!("codex/0.144.1");
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let config = AgentConfig::new(
        CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
            .unwrap()
            .into(),
        Duration::from_secs(3),
        std::num::NonZeroUsize::new(1).unwrap(),
    )
    .unwrap();
    let observed = std::panic::AssertUnwindSafe(async {
        match AgentBuilder::create(config, AgentStorage::new(store))
            .build()
            .await
        {
            Err(error) => assert_eq!(error.kind(), ErrorKind::InvalidConfig),
            Ok(agent) => {
                agent.shutdown().await.unwrap();
                panic!("previous native version accepted");
            }
        }
    })
    .catch_unwind()
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    cleanup
        .settle(async {
            peer.shutdown().await;
            Ok::<(), agents_kit::KitError>(())
        })
        .await;
    cleanup.finish(observed.map(Ok));
}

#[tokio::test]
async fn persisted_personality_is_rejected_on_reopen_without_native_mutation() {
    use agents_kit::config::{AgentConfig, CodexConfig};
    use agents_kit::{AgentBuilder, AgentStorage};
    use storage_kit::StateTransactionBackend;
    use storage_kit::state::record::StateRecord;
    for selection in [None, Some("previous_selection"), Some("accepted_selection")] {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let fixture = CodexFixture::build_with_store(CodexPeer::start().await, raw.clone()).await;
        let CodexFixture {
            peer,
            agent,
            workspace,
            conversation,
            root,
        } = fixture;
        let agent_id = agent.id();
        let conversation_id = conversation.id();
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
                .unwrap()
                .into(),
            Duration::from_secs(3),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        drop(conversation);
        workspace.close().await.unwrap();
        agent.shutdown().await.unwrap();
        let observed = std::panic::AssertUnwindSafe(async {
        let (key, value) = match selection {
            None => (StateKey::new("agents_kit_codex_native_selection_v1", conversation_id.as_str()).unwrap(),
                json!({"version":1,"conversation_id":conversation_id,"selection":{"model":null,"reasoning_effort":null,"personality":"friendly"}})),
            Some(field) => {
                let mut value = json!({"version":4,"conversation_id":conversation_id,"client_message_id":"pending-message",
                    "output_schema":null,"previous_selection":{"model":null,"reasoning_effort":null,"personality":null},
                    "accepted_selection":{"model":null,"reasoning_effort":null,"personality":null},
                    "tool_selection":"automatic","timeout":null,"state":{"state":"may_be_accepted"},
                    "cancellation":"not_requested","waiter":"waiting"});
                value[field]["personality"] = json!("friendly");
                (StateKey::new("agents_kit_codex_prompts_v1", conversation_id.as_str()).unwrap(),value)
            }
        };
        let mut transaction = raw.begin_transaction().await.unwrap();
        transaction.put(StateRecord::new(key.clone(), value.clone())).await.unwrap();
        transaction.commit().await.unwrap();
        match AgentBuilder::reopen(config,AgentStorage::new(raw.clone()),agent_id).build().await {
            Err(error) => assert_eq!(error.kind(), ErrorKind::Unsupported),
            Ok(agent) => { agent.shutdown().await.unwrap(); panic!("persisted personality silently accepted"); }
        }
        assert_eq!(*raw.load(key).await.unwrap().unwrap().record.value, value, "rejection cannot migrate durable selection");
        assert_eq!(peer.initializations.load(std::sync::atomic::Ordering::SeqCst), 1, "unsupported reopen must not reconnect");
    }).catch_unwind().await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        cleanup
            .settle(async {
                peer.shutdown().await;
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
        cleanup.finish(observed.map(Ok));
    }
}

#[tokio::test]
async fn native_deletion_rejection_retains_cleanup_without_automatic_replay() {
    use agents_kit::workspace::DeleteWorkspaceRequest;
    let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let CodexFixture {
        peer,
        agent,
        workspace,
        conversation,
        root,
    } = CodexFixture::build_with_store(CodexPeer::start().await, raw.clone()).await;
    let observed = std::panic::AssertUnwindSafe(async {
        let info = workspace.info().await.unwrap();
        peer.reject(
            "thread/delete",
            -32600,
            "cannot delete internal worker thread: secret-native-id",
        );
        agent
            .delete_workspace(
                DeleteWorkspaceRequest::new(info.id().clone(), info.revision()).unwrap(),
            )
            .await
            .expect("committed portable deletion retains separate native cleanup ownership");
        let key = StateKey::new(
            "agents_kit_codex_cleanup_v1",
            format!(
                "{}:delete:workspace:{}",
                agent.id().as_str(),
                info.id().as_str()
            ),
        )
        .unwrap();
        let retained = raw
            .load(key.clone())
            .await
            .unwrap()
            .expect("native rejection cannot complete deletion obligation");
        assert_eq!(retained.record.value["action"], "delete");
        assert_eq!(
            retained.record.value["thread_ids"],
            json!(["stream-thread"])
        );
        assert_eq!(
            agent
                .workspace_info(info.id().clone())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
        assert!(agent.list_workspaces().await.unwrap().is_empty());
        assert_eq!(
            peer.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request["method"] == "thread/delete")
                .count(),
            1
        );
        assert_eq!(
            raw.load(key.clone()).await.unwrap().unwrap().record.value,
            retained.record.value
        );
        (key, retained.record.value)
    })
    .catch_unwind()
    .await;
    drop(conversation);
    drop(workspace);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    cleanup.settle(agent.shutdown()).await;
    if let Ok((key, value)) = &observed {
        cleanup
            .settle(async {
                assert_eq!(
                    raw.load(key.clone()).await.unwrap().unwrap().record.value,
                    *value
                );
                assert_eq!(
                    peer.requests
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|request| request["method"] == "thread/delete")
                        .count(),
                    1,
                    "shutdown must not replay a rejected deletion mutation"
                );
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
    }
    cleanup
        .settle(async {
            peer.shutdown().await;
            Ok::<(), agents_kit::KitError>(())
        })
        .await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed.map(|_| Ok(())));
}
