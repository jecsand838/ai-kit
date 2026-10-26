//! Selected OpenCode wire regressions. These scripted peers are not live proof.

use std::sync::atomic::Ordering;
use std::time::Duration;

use agent_sdk::conversation::NewConversation;
use agent_sdk::error::{ErrorDetail, ErrorKind};
use agent_sdk::prompt::{PromptEvent, PromptRequest};
use agent_sdk::{AgentBackend, AgentBuilder, ConversationBackend, SdkError, WorkspaceBackend};
use serde_json::{Value, json};
use storage_sdk::StateStoreBackend;
use storage_sdk::state::record::StateListRequest;
use tokio::net::TcpStream;

use super::native_cleanup::NativeCleanup;
use super::{OpenCodeEventProbe, OpenCodeEventStats, OpenCodeStreamCase};

#[derive(Default)]
pub(super) struct ReleaseFixture {
    pub(super) health: Option<Value>,
    pub(super) config: Option<Value>,
    pub(super) config_error: Option<Value>,
    pub(super) config_reads: usize,
    pub(super) session_requests: Vec<Value>,
    pub(super) correlation: Option<Correlation>,
    pub(super) inventory: Option<Value>,
}

#[tokio::test]
async fn release_selected_server_version_is_exact() {
    for (healthy, version, expected) in [
        (true, "1.19.3", None),
        (true, "1.19.0", Some(ErrorKind::InvalidConfig)),
        (true, "1.19.4", Some(ErrorKind::InvalidConfig)),
        (true, "local", Some(ErrorKind::InvalidConfig)),
        (true, "1.19", Some(ErrorKind::Protocol)),
        (true, "1.19.3+", Some(ErrorKind::Protocol)),
        (false, "1.19.3", Some(ErrorKind::Protocol)),
    ] {
        let mut probe = OpenCodeEventProbe::start(0).await;
        probe.stats.release.lock().unwrap().health =
            Some(json!({"healthy": healthy, "version": version}));
        let stats = std::sync::Arc::clone(&probe.stats);
        let mut agent = None;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                let result = AgentBuilder::create(
                    probe.config(Duration::from_secs(2), Duration::from_secs(3)),
                    OpenCodeEventProbe::storage().await,
                )
                .build()
                .await;
                let actual = match result {
                    Ok(owner) => {
                        agent = Some(owner);
                        None
                    }
                    Err(error) => {
                        eprintln!("health {healthy}/{version}: {error:?}");
                        Some(error.kind())
                    }
                };
                assert_eq!(
                    actual, expected,
                    "exact release admission: {healthy}/{version}"
                );
                assert_eq!(stats.next_session.load(Ordering::SeqCst), 0);
                assert!(stats.sessions.lock().unwrap().is_empty());
            },
        )
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        if let Some(agent) = agent {
            cleanup.settle(agent.shutdown()).await;
        }
        // Retain ownership across the graceful-settlement deadline so expiry
        // cannot detach the server task.
        let mut server = probe
            .task
            .take()
            .expect("the probe retains its server task");
        let mut joined = false;
        cleanup
            .settle::<SdkError>(async {
                probe.shutdown().await;
                let result = (&mut server).await;
                joined = true;
                result.expect("the probe server must join");
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok(())
            })
            .await;
        if !joined {
            server.abort();
            match server.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    cleanup.settle(async { Err::<(), _>(error) }).await;
                }
            }
        }
        eprintln!(
            "health {healthy}/{version}: joined cleanup; diagnostics={:?}",
            cleanup.diagnostics()
        );
        cleanup.finish(observed);
    }
}

impl ReleaseFixture {
    pub(super) fn inventory(&self) -> Value {
        self.inventory
            .clone()
            .unwrap_or_else(|| serde_json::from_str(Self::INVENTORY).unwrap())
    }

    const INVENTORY: &str = r#"{"all":[{"id":"provider","name":"Probe Provider","source":"custom","env":[],"key":null,"options":{},"models":{"model":{"id":"model","providerID":"provider","api":{"id":"probe","url":"https://example.invalid","npm":"probe"},"name":"Probe Model","family":null,"capabilities":{"temperature":true,"reasoning":false,"attachment":false,"toolcall":true,"input":{"text":true,"audio":false,"image":false,"video":false,"pdf":false},"output":{"text":true,"audio":false,"image":false,"video":false,"pdf":false},"interleaved":false},"cost":{"input":0,"output":0,"cache":{"read":0,"write":0},"tiers":null,"experimentalOver200K":null},"limit":{"context":1024,"input":1024,"output":1024},"status":"active","options":{},"headers":{},"release_date":"2026-01-01","variants":null}}}],"default":{"provider":"model"},"connected":["provider"]}"#;
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Correlation {
    Nonmonotonic,
    WrongSession,
    UnrelatedParent,
    MissingRequestPart,
    WrongRole,
    SelfParent,
    Incomplete,
}

impl Correlation {
    pub(super) async fn respond(
        self,
        stream: &mut TcpStream,
        stats: &OpenCodeEventStats,
        request: Value,
    ) {
        let parent = OpenCodeStreamCase::PARENT;
        let assistant = "msg_000000000002abcdefghijklmn";
        let user = json!({"id": parent, "sessionID": "ses_probe", "role": "user",
            "time": {"created": 1}, "agent": "agent",
            "model": {"modelID": "model", "providerID": "provider"}});
        let mut part = json!({"id": request["parts"][0]["id"], "sessionID": "ses_probe",
            "messageID": parent, "type": "text", "text": request["parts"][0]["text"]});
        if matches!(self, Self::MissingRequestPart) {
            part["id"] = json!("prt_unrelated");
        }
        *stats.prompt_parent.lock().unwrap() = Some(json!({"info": user, "parts": [part]}));
        let properties = [
            json!({"sessionID": "ses_probe", "info": user}),
            json!({"sessionID": "ses_probe", "part": part, "time": 1}),
        ];
        let mut rows = Vec::new();
        for (seq, (kind, data)) in ["message.updated", "message.part.updated"]
            .into_iter()
            .zip(properties)
            .enumerate()
        {
            let id = format!("evt_release_parent_{seq}");
            rows.push(json!({"id": id, "aggregate_id": "ses_probe", "seq": seq,
                "type": format!("{kind}.1"), "data": data}));
            if request.get("format").is_none() {
                let _ = stats.events.send(
                    json!({"directory": env!("CARGO_MANIFEST_DIR"),
                    "payload": {"id": id, "type": kind, "properties": data}})
                    .to_string(),
                );
            }
        }
        *stats.history_rows.lock().unwrap() = rows;
        let mut info = OpenCodeStreamCase::assistant(assistant, parent);
        match self {
            Self::Nonmonotonic | Self::MissingRequestPart => {}
            Self::WrongSession => info["sessionID"] = json!("ses_foreign"),
            Self::UnrelatedParent => info["parentID"] = json!("msg_000000000001abcdefghijklmn"),
            Self::WrongRole => info = user,
            Self::SelfParent => info["parentID"] = json!(assistant),
            Self::Incomplete => {
                info["time"].as_object_mut().unwrap().remove("completed");
            }
        }
        if request.get("format").is_some() {
            info["structured"] = json!({"ack": "STRUCTURED_ACK"});
        }
        let body = json!({"info": info, "parts": [{"id": "prt_release_output",
            "sessionID": "ses_probe", "messageID": assistant, "type": "text", "text": "release answer"}]});
        stats.messages.fetch_add(1, Ordering::SeqCst);
        OpenCodeEventProbe::json(stream, &body.to_string()).await;
        stats.prompt_responses.fetch_add(1, Ordering::SeqCst);
    }

    async fn verify(self, structured: bool) {
        let case = OpenCodeStreamCase::start().await;
        case.probe.stats.release.lock().unwrap().correlation = Some(self);
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                let request = if structured {
                    OpenCodeEventProbe::structured_request(Duration::from_secs(2))
                } else {
                    PromptRequest::new("release correlation").unwrap()
                };
                let result = case.conversation.prompt(request).await;
                if matches!(self, Self::Nonmonotonic) {
                    let response = result.expect(
                        "exact request ownership must accept nonmonotonic native identities",
                    );
                    assert_eq!(response.output(), "release answer");
                    if structured {
                        assert_eq!(
                            response.structured_output(),
                            Some(&json!({"ack": "STRUCTURED_ACK"}))
                        );
                    }
                } else {
                    let error =
                        result.expect_err("unowned or incomplete Assistant must never complete");
                    assert!(
                        matches!(error.kind(), ErrorKind::InvalidState | ErrorKind::Timeout),
                        "{self:?}: {error:?}"
                    );
                }
                assert_eq!(
                    case.probe.stats.messages.load(Ordering::SeqCst),
                    1,
                    "no prompt replay"
                );
            },
        )
        .await;
        case.finish_observed(observed).await;
    }
}

#[tokio::test]
async fn release_interleaved_response_accepts_open_string_vocabulary() {
    let case = OpenCodeStreamCase::start().await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(20),
        async {
            for interleaved in [
                json!(false),
                json!(true),
                json!({"field":"reasoning"}),
                json!({"field":"reasoning_content"}),
                json!({"field":"reasoning_details"}),
                json!({"field":"reasoning_text"}),
                json!({"field":"custom_reasoning"}),
                json!({"field":""}),
            ] {
                let mut inventory = ReleaseFixture::default().inventory();
                inventory["all"][0]["models"]["model"]["capabilities"]["interleaved"] =
                    interleaved.clone();
                case.probe.stats.release.lock().unwrap().inventory = Some(inventory);
                let models = case.agent.list_models().await.unwrap_or_else(|error| {
                    panic!("source-valid interleaved {interleaved} rejected: {error:?}")
                });
                assert_eq!(models.len(), 1);
                assert!(models[0].reasoning_efforts().is_empty());
                assert!(models[0].personalities().is_empty());
            }
        },
    )
    .await;
    case.finish_observed(observed).await;
}

#[tokio::test]
async fn release_inventory_keeps_response_shape_and_index_validation() {
    let case = OpenCodeStreamCase::start().await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(20),
        async {
            for interleaved in [
                json!("reasoning_text"),
                json!(42),
                Value::Null,
                json!({}),
                json!({"field":false}),
                json!({"field":"reasoning", "extra":true}),
            ] {
                let mut inventory = ReleaseFixture::default().inventory();
                inventory["all"][0]["models"]["model"]["capabilities"]["interleaved"] = interleaved;
                case.probe.stats.release.lock().unwrap().inventory = Some(inventory);
                assert_eq!(
                    case.agent.list_models().await.unwrap_err().kind(),
                    ErrorKind::Protocol
                );
            }
            for change in 0..5 {
                let mut inventory = ReleaseFixture::default().inventory();
                match change {
                    0 => inventory["connected"] = json!(["missing"]),
                    1 => inventory["connected"] = json!(["provider", "provider"]),
                    2 => inventory["default"]["provider"] = json!("missing"),
                    3 => inventory["all"][0]["models"]["model"]["id"] = json!("wrong"),
                    4 => {
                        let duplicate = inventory["all"][0].clone();
                        inventory["all"].as_array_mut().unwrap().push(duplicate);
                    }
                    _ => unreachable!(),
                }
                case.probe.stats.release.lock().unwrap().inventory = Some(inventory);
                assert_eq!(
                    case.agent.list_models().await.unwrap_err().kind(),
                    ErrorKind::Protocol
                );
            }
            for connected in [false, true] {
                let mut inventory = ReleaseFixture::default().inventory();
                inventory["all"][0]["source"] = json!("api");
                inventory["connected"] = if connected {
                    json!(["provider"])
                } else {
                    json!([])
                };
                case.probe.stats.release.lock().unwrap().inventory = Some(inventory);
                assert_eq!(
                    case.agent.list_models().await.unwrap().len(),
                    usize::from(connected)
                );
            }
        },
    )
    .await;
    case.finish_observed(observed).await;
}

#[tokio::test]
async fn release_plain_prompt_accepts_nonmonotonic_correlated_identity() {
    Correlation::Nonmonotonic.verify(false).await;
}

#[tokio::test]
async fn release_structured_prompt_accepts_nonmonotonic_correlated_identity() {
    Correlation::Nonmonotonic.verify(true).await;
}

#[tokio::test]
async fn release_foreground_retains_exact_ownership_controls() {
    for correlation in [
        Correlation::WrongSession,
        Correlation::UnrelatedParent,
        Correlation::MissingRequestPart,
        Correlation::WrongRole,
        Correlation::SelfParent,
        Correlation::Incomplete,
    ] {
        correlation.verify(false).await;
        correlation.verify(true).await;
    }
}

#[tokio::test]
async fn release_unknown_finish_event_cannot_complete_stream_or_unary_prompt() {
    for streaming in [false, true] {
        let case = OpenCodeStreamCase::start().await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                let unary = case.conversation.prompt(OpenCodeStreamCase::request());
                tokio::pin!(unary);
                let mut stream = if streaming {
                    Some(case.stream().await)
                } else {
                    tokio::select! {
                        result = &mut unary => panic!("HTTP must remain held: {result:?}"),
                        () = case.probe.wait_for_messages(1) => {}
                    }
                    None
                };
                case.wait_for_admission().await;
                let mut info = OpenCodeStreamCase::assistant(
                    OpenCodeStreamCase::ASSISTANT,
                    OpenCodeStreamCase::PARENT,
                );
                info["finish"] = json!("unknown");
                case.emit(
                    "evt_release_unknown",
                    "message.updated",
                    json!({"sessionID":"ses_probe", "info":info}),
                );
                loop {
                    let records = case
                        .faults
                        .inner
                        .list(
                            StateListRequest::new(super::OPENCODE_EVENT_PROGRESS_NAMESPACE)
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    if records.records.iter().any(|row| {
                        row.record.value["recent"].as_array().is_some_and(|recent| {
                            recent
                                .iter()
                                .any(|event| event["id"] == "evt_release_unknown")
                        })
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                if let Some(stream) = &mut stream {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(150), stream.recv())
                            .await
                            .is_err(),
                        "intermediate completion cannot publish a terminal"
                    );
                } else {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(150), &mut unary)
                            .await
                            .is_err(),
                        "intermediate completion cannot finish unary work"
                    );
                }
                case.release();
                if let Some(stream) = &mut stream {
                    let PromptEvent::Completed(response) =
                        OpenCodeStreamCase::next(stream).await.unwrap()
                    else {
                        panic!("HTTP must supply terminal response")
                    };
                    assert_eq!(response.output(), "authoritative HTTP answer");
                    assert!(stream.recv().await.is_none());
                } else {
                    assert_eq!(unary.await.unwrap().output(), "authoritative HTTP answer");
                }
            },
        )
        .await;
        case.finish_observed(observed).await;
    }
}

#[tokio::test]
async fn release_native_variants_remain_private() {
    use agent_sdk::model::{PersonalitySelector, ReasoningEffort};
    use agent_sdk::workspace::WorkspaceDefaults;

    let case = OpenCodeStreamCase::start().await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(20),
        async {
            for variants in [
                json!({}),
                json!({"high": {"reasoningEffort": "high", "textVerbosity": "low",
                    "providerOptions": {"nested": {"budget": 4096, "enabled": true}}},
                    "custom": {"thinking": {"type": "enabled", "budgetTokens": 1024}}}),
            ] {
                let mut inventory = ReleaseFixture::default().inventory();
                inventory["all"][0]["models"]["model"]["variants"] = variants;
                case.probe.stats.release.lock().unwrap().inventory = Some(inventory);
                let models = case.agent.list_models().await.unwrap();
                assert_eq!(models.len(), 1);
                assert!(models[0].reasoning_efforts().is_empty());
                assert!(models[0].personalities().is_empty());
                let sessions = case.probe.stats.next_session.load(Ordering::SeqCst);
                let messages = case.probe.stats.messages.load(Ordering::SeqCst);
                for defaults in [
                    WorkspaceDefaults::new(None, Some(ReasoningEffort::new("high").unwrap()), None)
                        .unwrap(),
                    WorkspaceDefaults::empty()
                        .with_personality(PersonalitySelector::new("build").unwrap()),
                ] {
                    let request = if let Some(effort) = defaults.reasoning_effort() {
                        OpenCodeStreamCase::request().with_reasoning_effort(effort.clone())
                    } else {
                        OpenCodeStreamCase::request()
                            .with_personality(defaults.personality().unwrap().clone())
                    };
                    assert_eq!(
                        case.conversation.prompt(request).await.unwrap_err().kind(),
                        ErrorKind::Unsupported
                    );
                    let result = case
                        .workspace
                        .new_conversation(
                            NewConversation::new(None, Some(defaults), Vec::new()).unwrap(),
                        )
                        .await;
                    let error = match result {
                        Err(error) => error,
                        Ok(conversation) => {
                            conversation.close().await.unwrap();
                            panic!("native variant data must not authorize portable selectors");
                        }
                    };
                    assert_eq!(error.kind(), ErrorKind::Unsupported);
                    assert_eq!(
                        case.probe.stats.next_session.load(Ordering::SeqCst),
                        sessions
                    );
                    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), messages);
                }
            }
        },
    )
    .await;
    case.finish_observed(observed).await;
}

#[tokio::test]
async fn release_additive_reasoning_config_preserves_session_permission() {
    let case = OpenCodeStreamCase::start().await;
    let reads = case.probe.stats.release.lock().unwrap().config_reads;
    case.probe.stats.release.lock().unwrap().config = Some(json!({
        "share": "disabled",
        "provider": {"provider": {"models": {"model": {"reasoning_options": [
            {"type": "effort", "values": ["low", "high"]},
            {"type": "toggle"},
            {"type": "budget_tokens", "min": 1024, "max": 4096}
        ]}}}}
    }));
    let mut conversation = None;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(10),
        async {
            conversation = Some(
                case.workspace
                    .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                    .await
                    .expect("additive provider configuration must remain compatible"),
            );
            let release = case.probe.stats.release.lock().unwrap();
            assert_eq!(release.config_reads, reads + 1);
            assert_eq!(release.session_requests.len(), 2);
            assert_eq!(
                release.session_requests[1]["permission"],
                json!([{"permission": "*", "pattern": "*", "action": "allow"}])
            );
        },
    )
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    if let Some(conversation) = conversation {
        cleanup.settle(conversation.close()).await;
    }
    case.finish_observed(
        NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async { cleanup.finish(observed) },
        )
        .await,
    )
    .await;
}

#[tokio::test]
async fn release_config_rejection_is_redacted_and_creates_no_session() {
    for (name, path) in [
        ("ConfigInvalidError", json!(["permissions"])),
        (
            "ConfigInvalidError",
            json!(["agents", "build", "permissions"]),
        ),
        (
            "ConfigInvalidError",
            json!(["agent", "build", "permissions"]),
        ),
        (
            "ConfigInvalidError",
            json!(["mode", "build", "permissions"]),
        ),
        ("ConfigRemoteAuthError", Value::Null),
    ] {
        let probe = OpenCodeEventProbe::start(0).await;
        probe.stats.release.lock().unwrap().config_error = Some(json!({"name":name,
            "data": if name == "ConfigInvalidError" {
                json!({"path":"PRIVATE_CONFIG", "issues":[{"path":path, "message":"WIRE_SECRET"}]})
            } else { json!({"url":"https://private.invalid", "remote":"WIRE_SECRET"}) }}));
        let mut agent = None;
        let mut workspace = None;
        let mut conversation = None;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                let owner = agent.insert(
                    AgentBuilder::create(
                        probe.config(Duration::from_secs(2), Duration::from_secs(3)),
                        OpenCodeEventProbe::storage().await,
                    )
                    .build()
                    .await
                    .unwrap(),
                );
                let workspace = workspace.insert(
                    owner
                        .new_workspace(OpenCodeEventProbe::workspace_request())
                        .await
                        .unwrap(),
                );
                let result = workspace
                    .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                    .await;
                let error = match result {
                    Ok(unexpected) => {
                        conversation = Some(unexpected);
                        panic!("invalid native configuration must fail before session creation")
                    }
                    Err(error) => error,
                };
                assert_eq!(error.http_status(), Some(400));
                assert_eq!(error.kind(), ErrorKind::InvalidInput);
                assert_eq!(error.detail(), Some(&ErrorDetail::InvalidRequest));
                for private in ["PRIVATE_", "WIRE_SECRET", "private.invalid", name] {
                    assert!(!format!("{error} {error:?}").contains(private));
                }
                assert!(std::error::Error::source(&error).is_none());
                assert!(probe.stats.sessions.lock().unwrap().is_empty());
                assert_eq!(probe.stats.next_session.load(Ordering::SeqCst), 0);
                assert_eq!(
                    probe.stats.release.lock().unwrap().config_reads,
                    1,
                    "configuration is not repaired or retried"
                );
            },
        )
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        if let Some(conversation) = conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = workspace {
            cleanup.settle(workspace.close()).await;
        }
        if let Some(agent) = agent {
            cleanup.settle(agent.shutdown()).await;
        }
        cleanup
            .settle::<SdkError>(async {
                probe.shutdown().await;
                Ok(())
            })
            .await;
        cleanup.finish(observed);
    }
}
