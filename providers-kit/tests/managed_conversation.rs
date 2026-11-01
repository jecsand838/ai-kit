// Fixture provenance: these are synthetic native protocol envelopes based on
// this crate's committed wire_behavior generation fixtures: OpenAI Responses
// response.completed, Anthropic message/content-block SSE, Gemini signed
// generateContent parts, and Ollama final chat NDJSON. Each provider keeps its
// own envelope. Request/close barriers exercise the actual HTTP transport; this
// deterministic evidence does not claim captured live traffic or model quality.
mod support;

use providers_kit::config::{
    AnthropicConfig, ApiKey, GeminiConfig, OllamaConfig, OpenAiConfig, ProviderKind,
    TransportConfig,
};
use providers_kit::generation::{
    ContentPart, FunctionCall, FunctionResult, JsonSchema, Message, ProviderFunctionTool,
    ProviderToolSpec,
};
use providers_kit::session::{
    ResponseOutcome, SessionCommand, SessionCommands, SessionConfig, SessionEvent, SessionEvents,
    SessionProfile, SessionToolResult, ToolRequestId,
};
use providers_kit::{
    ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig, SessionBackend,
};
use serde_json::{Value, json};
use std::time::Duration;
use support::scripted_http::{ScriptedResponse, spawn_scripted_http_server};

#[derive(Clone, Copy, Debug)]
enum FixtureProvider {
    OpenAi,
    Anthropic,
    Gemini,
    Ollama,
}

impl FixtureProvider {
    fn build(self, mut url: url::Url) -> Provider {
        let key = || ApiKey::new("session-fixture-key").unwrap();
        let kind = match self {
            Self::OpenAi => {
                url.set_path("/v1");
                ProviderKind::OpenAi(OpenAiConfig::new(key()).with_base_url(url))
            }
            Self::Anthropic => {
                ProviderKind::Anthropic(AnthropicConfig::new(key()).with_base_url(url))
            }
            Self::Gemini => ProviderKind::Gemini(GeminiConfig::new(key()).with_base_url(url)),
            Self::Ollama => {
                url.set_path("/api");
                ProviderKind::Ollama(OllamaConfig::local().with_base_url(url).unwrap())
            }
        };
        Provider::from_config(ProviderConfig::new(kind, TransportConfig::default())).unwrap()
    }

    fn model(self) -> &'static str {
        match self {
            Self::OpenAi => "gpt-6-luna",
            Self::Anthropic => "claude-session",
            Self::Gemini => "models/gemini-session",
            Self::Ollama => "ollama-session",
        }
    }

    fn response(self, text: Option<&str>) -> ScriptedResponse {
        // Independently shaped native envelopes follow existing provider generation
        // fixtures. The transport is scripted; the production encoders/parsers run.
        match self {
            Self::OpenAi => {
                let output = match text {
                    Some(text) => {
                        json!([{"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}])
                    }
                    None => {
                        json!([{"type":"function_call","id":"item-preserved","call_id":"call-1","name":"lookup","arguments":"{\"key\":\"a\"}"}])
                    }
                };
                ScriptedResponse::sse(&format!(
                    "data: {}\n\n",
                    json!({"type":"response.completed","response":{"model":self.model(),"status":"completed","output":output}})
                ))
            }
            Self::Anthropic => {
                let block = match text {
                    Some(text) => json!({"type":"text","text":text}),
                    None => {
                        json!({"type":"tool_use","id":"call-1","name":"lookup","input":{"key":"a"}})
                    }
                };
                let mut wire = String::new();
                for event in [
                    json!({"type":"message_start","message":{"id":"msg-fixture","type":"message","role":"assistant","content":[],"model":self.model(),"stop_reason":null,"usage":{"input_tokens":2,"output_tokens":0}}}),
                    json!({"type":"content_block_start","index":0,"content_block":block}),
                    json!({"type":"content_block_stop","index":0}),
                    json!({"type":"message_delta","delta":{"stop_reason":if text.is_some(){"end_turn"}else{"tool_use"}},"usage":{"output_tokens":3}}),
                    json!({"type":"message_stop"}),
                ] {
                    wire.push_str(&format!(
                        "event: {}\ndata: {event}\n\n",
                        event["type"].as_str().unwrap()
                    ));
                }
                ScriptedResponse::sse(&wire)
            }
            Self::Gemini => {
                let part = match text {
                    Some(text) => json!({"text":text}),
                    None => {
                        json!({"functionCall":{"id":"call-1","name":"lookup","args":{"key":"a"}},"thoughtSignature":"exact-signed-state"})
                    }
                };
                ScriptedResponse::sse(&format!(
                    "data: {}\n\n",
                    json!({"candidates":[{"content":{"role":"model","parts":[part]},"finishReason":"STOP"}],"modelVersion":"gemini-session"})
                ))
            }
            Self::Ollama => {
                let message = match text {
                    Some(text) => json!({"role":"assistant","content":text}),
                    None => {
                        json!({"role":"assistant","content":"","thinking":"exact-thinking-state","tool_calls":[{"function":{"name":"lookup","arguments":{"key":"a"}}}]})
                    }
                };
                ScriptedResponse::chunks(200,"application/x-ndjson",vec![format!("{}\n",json!({"model":self.model(),"message":message,"done":true,"done_reason":"stop","prompt_eval_count":2,"eval_count":3})).into_bytes()])
            }
        }
    }

    fn assert_replay(self, request: &Value) {
        let rendered = request.to_string();
        for text in [
            "first input",
            "first answer",
            "second input",
            "result-value",
        ] {
            assert!(rendered.contains(text), "{self:?} lost {text}: {request}");
        }
        match self {
            Self::OpenAi => assert!(rendered.contains("item-preserved")),
            Self::Anthropic => assert!(rendered.contains("tool_use_id")),
            Self::Gemini => assert!(rendered.contains("exact-signed-state")),
            Self::Ollama => {
                assert!(rendered.contains("exact-thinking-state"));
                assert!(
                    !rendered.contains("call-1"),
                    "Ollama must not fabricate native IDs"
                );
            }
        }
    }
}

struct Observation {
    tool: Option<(ToolRequestId, FunctionCall)>,
    response: providers_kit::session::ResponseId,
}

impl Observation {
    async fn completed(events: &mut impl SessionEvents, expected: &str) -> Self {
        let mut tool = None;
        loop {
            match tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .expect("response progresses")
                .expect("session remains open")
            {
                SessionEvent::ToolRequested { request, call, .. } => tool = Some((request, call)),
                SessionEvent::ResponseFinished {
                    response,
                    outcome,
                    output,
                    ..
                } => {
                    assert!(matches!(outcome, ResponseOutcome::Completed), "{outcome:?}");
                    let output =
                        output.expect("managed completion projects existing generation result");
                    if !expected.is_empty() {
                        assert!(
                            output.message.content().iter().any(
                                |part| matches!(part,ContentPart::Text{text} if text==expected)
                            )
                        );
                    }
                    return Self { tool, response };
                }
                SessionEvent::CommandFailed { error, .. }
                | SessionEvent::Terminated { reason: error, .. } => {
                    panic!("unexpected session failure: {error:?}")
                }
                _ => {}
            }
        }
    }
}

#[tokio::test]
async fn all_conversation_providers_retain_two_exchanges_and_exact_tool_replay() {
    for fixture in [
        FixtureProvider::OpenAi,
        FixtureProvider::Anthropic,
        FixtureProvider::Gemini,
        FixtureProvider::Ollama,
    ] {
        let mut server = spawn_scripted_http_server(vec![
            fixture.response(Some("first answer")),
            fixture.response(None),
            fixture.response(Some("final answer")),
        ])
        .await;
        let provider = fixture.build(server.base_url());
        let model = provider
            .model(ModelId::new(fixture.model()).unwrap())
            .unwrap();
        let config=SessionConfig::new(SessionProfile::Conversation).with_tools(vec![ProviderToolSpec::Function(ProviderFunctionTool::new("lookup",None,JsonSchema::new(json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"]})).unwrap()).unwrap())]).unwrap();
        let mut session = model
            .open_session(config)
            .await
            .expect("Conversation is the four-provider baseline");
        {
            let (mut commands, mut events) = session.split();
            let mut pending = std::pin::pin!(events.recv());
            assert!(
                futures_util::poll!(&mut pending).is_pending(),
                "idle recv stays pending"
            );
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("first input").unwrap(),
                ))
                .await
                .unwrap();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), pending)
                    .await
                    .unwrap(),
                Some(SessionEvent::ResponseStarted { .. })
            ));
        }
        {
            let (mut commands, mut events) = session.split();
            Observation::completed(&mut events, "first answer").await;
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("second input").unwrap(),
                ))
                .await
                .unwrap();
            let observed = Observation::completed(&mut events, "").await;
            let (request, call) = observed.tool.expect("tool-only response is executable");
            let invalid = FunctionResult::new(
                call.id().map(str::to_owned),
                "wrong_name",
                vec![ContentPart::Text { text: "bad".into() }],
                false,
            )
            .unwrap();
            assert!(
                commands
                    .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                        request.clone(),
                        invalid
                    )]))
                    .await
                    .is_err(),
                "mismatch rejected before any mutation"
            );
            let result = FunctionResult::new(
                call.id().map(str::to_owned),
                call.name(),
                vec![ContentPart::Text {
                    text: "result-value".into(),
                }],
                false,
            )
            .unwrap();
            let valid = SessionToolResult::new(request, result);
            commands
                .submit(SessionCommand::ToolResults(vec![valid.clone()]))
                .await
                .unwrap();
            assert!(
                commands
                    .submit(SessionCommand::ToolResults(vec![valid]))
                    .await
                    .is_err(),
                "duplicate accepted result cannot restart generation"
            );
            Observation::completed(&mut events, "final answer").await;
            let mut idle = std::pin::pin!(events.recv());
            assert!(
                futures_util::poll!(&mut idle).is_pending(),
                "settled session is idle, not drained"
            );
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
        let requests = server.requests();
        assert_eq!(
            requests.len(),
            3,
            "one continuation for one complete result set"
        );
        fixture.assert_replay(&serde_json::from_slice::<Value>(&requests[2].body).unwrap());
    }
}

#[tokio::test]
async fn cancelled_opening_keeps_accepted_user_input_once_and_permits_next_exchange() {
    use providers_kit::session::{CancelTarget, CancellationResolution};
    use std::sync::Arc;
    use support::scripted_http::RecordedRequest;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio::task::JoinSet;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let opening = Arc::new(Notify::new());
    let reached = Arc::clone(&opening);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (mut first,_)=listener.accept().await.unwrap();
        let first_request=RecordedRequest::read(&mut first).await.unwrap();
        reached.notify_one();
        // No response headers: cancellation must own and drop the opening future.
        // The second request is an independent accept, not a timer-based guess.
        let (mut second,_)=listener.accept().await.unwrap();
        let second_request=RecordedRequest::read(&mut second).await.unwrap();
        let body=format!("data: {}\n\n",json!({"type":"response.completed","response":{"model":"gpt-session","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"after cancellation"}]}]}}));
        second.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        (first_request,second_request)
    });
    let provider = FixtureProvider::OpenAi.build(url);
    let model = provider
        .model(ModelId::new("gpt-session").unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    {
        let (mut commands, mut events) = session.split();
        let original = commands
            .submit(SessionCommand::Input(
                Message::user_text("accepted before cancellation").unwrap(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), opening.notified())
            .await
            .unwrap();
        commands
            .submit(SessionCommand::Cancel(CancelTarget::Command(original)))
            .await
            .unwrap();
        let mut cancelled = false;
        loop {
            match tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                SessionEvent::ResponseFinished { outcome, .. } => {
                    assert!(matches!(outcome, ResponseOutcome::Cancelled));
                    cancelled = true;
                }
                SessionEvent::Cancellation { resolution, .. } => {
                    assert!(matches!(resolution, CancellationResolution::LocallyStopped));
                    break;
                }
                _ => {}
            }
        }
        assert!(cancelled);
        commands
            .submit(SessionCommand::Input(
                Message::user_text("followup after cancellation").unwrap(),
            ))
            .await
            .unwrap();
        Observation::completed(&mut events, "after cancellation").await;
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    let (_, second) = tokio::time::timeout(Duration::from_secs(3), tasks.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let request = String::from_utf8(second.body).unwrap();
    assert_eq!(request.matches("accepted before cancellation").count(), 1);
    assert_eq!(request.matches("followup after cancellation").count(), 1);
}

#[tokio::test]
async fn idle_conversation_has_no_generation_admission_and_seal_drains_permanently() {
    let mut server = spawn_scripted_http_server(Vec::new()).await;
    let provider = FixtureProvider::Anthropic.build(server.base_url());
    let model = provider
        .model(ModelId::new("claude-session").unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), provider.shutdown())
        .await
        .expect("idle session must not retain generation admission")
        .unwrap();
    {
        let (mut commands, mut events) = session.split();
        assert!(
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("after seal").unwrap()
                ))
                .await
                .is_err()
        );
        assert!(matches!(
            events.recv().await,
            Some(SessionEvent::Terminated { .. })
        ));
        assert!(events.recv().await.is_none());
        assert!(events.recv().await.is_none());
    }
    session.close().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn cancelling_a_settled_response_preserves_its_valid_tool_obligation() {
    use providers_kit::session::{CancelTarget, CancellationResolution};
    let fixture = FixtureProvider::OpenAi;
    let mut server = spawn_scripted_http_server(vec![
        fixture.response(None),
        fixture.response(Some("final answer")),
    ])
    .await;
    let provider = fixture.build(server.base_url());
    let model = provider
        .model(ModelId::new(fixture.model()).unwrap())
        .unwrap();
    let config = SessionConfig::new(SessionProfile::Conversation)
        .with_tools(vec![ProviderToolSpec::Function(
            ProviderFunctionTool::new(
                "lookup",
                None,
                JsonSchema::new(json!({"type":"object"})).unwrap(),
            )
            .unwrap(),
        )])
        .unwrap();
    let mut session = model.open_session(config).await.unwrap();
    {
        let (mut commands, mut events) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("request lookup").unwrap(),
            ))
            .await
            .unwrap();
        let observation = Observation::completed(&mut events, "").await;
        commands
            .submit(SessionCommand::Cancel(CancelTarget::Response(
                observation.response,
            )))
            .await
            .unwrap();
        assert!(
            matches!(
                events.recv().await,
                Some(SessionEvent::Cancellation {
                    resolution: CancellationResolution::AlreadySettled,
                    ..
                })
            ),
            "settled response is not a cancelled tool request"
        );
        let (request, call) = observation.tool.unwrap();
        let result = FunctionResult::new(
            call.id().map(str::to_owned),
            call.name(),
            vec![ContentPart::Text {
                text: "result-value".into(),
            }],
            false,
        )
        .unwrap();
        commands
            .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                request, result,
            )]))
            .await
            .expect("settled cancellation preserves required valid results");
        Observation::completed(&mut events, "final answer").await;
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
}

#[tokio::test]
async fn oversized_retained_configuration_is_rejected_before_session_acceptance() {
    let mut server = spawn_scripted_http_server(Vec::new()).await;
    let provider = FixtureProvider::Anthropic.build(server.base_url());
    let model = provider
        .model(ModelId::new("claude-session").unwrap())
        .unwrap();
    let config = SessionConfig::new(SessionProfile::Conversation)
        .with_instructions("x".repeat(33 * 1024 * 1024))
        .unwrap();
    assert!(
        model.open_session(config).await.is_err(),
        "retained instructions must consume the aggregate session budget"
    );
    provider.shutdown().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn sealing_immediately_after_acceptance_reports_unstarted_command() {
    let mut server = spawn_scripted_http_server(Vec::new()).await;
    let provider = FixtureProvider::Anthropic.build(server.base_url());
    let model = provider
        .model(ModelId::new("claude-session").unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    let accepted = {
        let (mut commands, _) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("accepted before sealing").unwrap(),
            ))
            .await
            .unwrap()
    };
    // Current-thread scheduling gives shutdown the next poll before the newly
    // scheduled generation task. Its atomic admission sees the sealed parent.
    provider.shutdown().await.unwrap();
    {
        let (_, mut events) = session.split();
        assert!(
            matches!(events.recv().await,Some(SessionEvent::CommandFailed{command,not_started:true,..}) if command==accepted)
        );
        assert!(matches!(
            events.recv().await,
            Some(SessionEvent::Terminated { .. })
        ));
        assert!(events.recv().await.is_none());
    }
    session.close().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn managed_protocol_failures_and_truncated_bodies_never_fabricate_completion() {
    for (index, reply) in [
        ScriptedResponse::json(r#"{"missing":"terminal envelope"}"#),
        ScriptedResponse::error(
            400,
            r#"{"error":{"message":"fixture rejection","type":"invalid_request_error"}}"#,
        )
        .with_header("x-request-id", "session-rejected-request"),
        ScriptedResponse::sse(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
        )
        .advertised_content_length(4096),
    ]
    .into_iter()
    .enumerate()
    {
        let mut server = spawn_scripted_http_server(vec![reply]).await;
        let provider = FixtureProvider::OpenAi.build(server.base_url());
        let model = provider
            .model(ModelId::new("gpt-session").unwrap())
            .unwrap();
        let mut session = model
            .open_session(SessionConfig::new(SessionProfile::Conversation))
            .await
            .unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("failure probe").unwrap(),
                ))
                .await
                .unwrap();
            loop {
                match tokio::time::timeout(Duration::from_secs(3), events.recv())
                    .await
                    .unwrap()
                    .unwrap()
                {
                    SessionEvent::ResponseFinished {
                        outcome,
                        output,
                        continuity,
                        ..
                    } => {
                        if index == 1 {
                            assert!(
                                matches!(outcome, ResponseOutcome::Failed(_)),
                                "explicit remote rejection is authoritative"
                            );
                        } else {
                            assert!(
                                matches!(outcome, ResponseOutcome::Unresolved(_)),
                                "malformed or truncated stream has no affirmative remote outcome: {outcome:?}"
                            );
                        }
                        assert!(output.is_none());
                        assert_eq!(
                            continuity,
                            providers_kit::session::SessionContinuity::Reusable,
                            "managed local history remains exact independently of unknown remote outcome"
                        );
                        break;
                    }
                    SessionEvent::Terminated { .. } => {
                        panic!("managed generation failure should settle its response explicitly")
                    }
                    _ => {}
                }
            }
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
    }
}

#[tokio::test]
async fn full_observation_queue_never_accepts_an_unreported_command() {
    use providers_kit::session::CancelTarget;
    let fixture = FixtureProvider::OpenAi;
    let mut server = spawn_scripted_http_server(vec![fixture.response(Some("first answer"))]).await;
    let provider = fixture.build(server.base_url());
    let model = provider
        .model(ModelId::new(fixture.model()).unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    let mut accepted = std::collections::BTreeSet::new();
    {
        let (mut commands, mut events) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("first input").unwrap(),
            ))
            .await
            .unwrap();
        let response = Observation::completed(&mut events, "first answer")
            .await
            .response;
        let mut rejected = false;
        for _ in 0..512 {
            match commands
                .submit(SessionCommand::Cancel(CancelTarget::Response(
                    response.clone(),
                )))
                .await
            {
                Ok(id) => {
                    accepted.insert(id);
                }
                Err(_) => {
                    rejected = true;
                    break;
                }
            }
        }
        assert!(
            rejected,
            "idle control observations are bounded without a consumer"
        );
    }
    tokio::time::timeout(Duration::from_secs(3), provider.shutdown())
        .await
        .unwrap()
        .unwrap();
    {
        let (_, mut events) = session.split();
        let mut observed = std::collections::BTreeSet::new();
        while let Some(event) = events.recv().await {
            match event {
                SessionEvent::Cancellation { command, .. }
                | SessionEvent::CommandFailed { command, .. } => {
                    observed.insert(command);
                }
                SessionEvent::Terminated {
                    unfinished_commands,
                    ..
                } => {
                    for command in unfinished_commands {
                        assert!(
                            accepted.contains(&command),
                            "rejected future secretly transferred command ownership"
                        );
                        observed.insert(command);
                    }
                }
                _ => {}
            }
        }
        assert_eq!(
            accepted, observed,
            "every accepted command retains an observation"
        );
    }
    session.close().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
}

#[tokio::test]
async fn active_owner_drop_without_event_consumption_releases_transport_before_shutdown() {
    use std::sync::Arc;
    use support::scripted_http::RecordedRequest;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio::task::JoinSet;
    for cancel_close in [false, true] {
        for drop_parents in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url =
                url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
            let reached = Arc::new(Notify::new());
            let opening = Arc::clone(&reached);
            let mut tasks = JoinSet::new();
            tasks.spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                RecordedRequest::read(&mut socket).await.unwrap();
                opening.notify_one();
                let mut byte = [0_u8; 1];
                socket.read(&mut byte).await.unwrap()
            });
            let provider = FixtureProvider::OpenAi.build(url);
            let model = provider
                .model(ModelId::new("gpt-session").unwrap())
                .unwrap();
            let mut session = model
                .open_session(SessionConfig::new(SessionProfile::Conversation))
                .await
                .unwrap();
            {
                let (mut commands, _) = session.split();
                commands
                    .submit(SessionCommand::Input(
                        Message::user_text("abandon observation").unwrap(),
                    ))
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(3), reached.notified())
                .await
                .unwrap();
            if cancel_close {
                let mut close = std::pin::pin!(session.close());
                assert!(
                    futures_util::poll!(&mut close).is_pending(),
                    "active cleanup transfers a join lease before it waits"
                );
                // Dropping the polled consuming close future must return its join lease.
            } else {
                drop(session);
            }
            let remaining_provider = if drop_parents {
                drop(model);
                drop(provider);
                None
            } else {
                Some(provider)
            };
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), tasks.join_next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                0,
                "drop cancels the actual opening transport"
            );
            if let Some(provider) = remaining_provider {
                tokio::time::timeout(Duration::from_secs(3), provider.shutdown())
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn multi_tool_batches_are_atomic_and_continue_only_after_the_required_set() {
    use std::num::NonZeroU64;
    let first = format!(
        "data: {}\n\n",
        json!({"type":"response.completed","response":{"model":"gpt-6-luna","status":"completed","output":[{"type":"function_call","id":"item-a","call_id":"call-a","name":"lookup","arguments":"{\"key\":\"a\"}"},{"type":"function_call","id":"item-b","call_id":"call-b","name":"lookup","arguments":"{\"key\":\"b\"}"}]}})
    );
    let mut server = spawn_scripted_http_server(vec![
        ScriptedResponse::sse(&first),
        FixtureProvider::OpenAi.response(Some("final answer")),
    ])
    .await;
    let provider = FixtureProvider::OpenAi.build(server.base_url());
    let model = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    let config = SessionConfig::new(SessionProfile::Conversation)
        .with_tools(vec![ProviderToolSpec::Function(
            ProviderFunctionTool::new(
                "lookup",
                None,
                JsonSchema::new(json!({"type":"object"})).unwrap(),
            )
            .unwrap(),
        )])
        .unwrap();
    let mut session = model.open_session(config).await.unwrap();
    {
        let (mut commands, mut events) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("two lookups").unwrap(),
            ))
            .await
            .unwrap();
        let mut calls = Vec::new();
        loop {
            match events.recv().await.unwrap() {
                SessionEvent::ToolRequested { request, call, .. } => calls.push((request, call)),
                SessionEvent::ResponseFinished { outcome, .. } => {
                    assert!(matches!(outcome, ResponseOutcome::Completed));
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(calls.len(), 2);
        let result_a = FunctionResult::new(
            calls[0].1.id().map(str::to_owned),
            "lookup",
            vec![ContentPart::Text {
                text: "result-a".into(),
            }],
            false,
        )
        .unwrap();
        let result_b = FunctionResult::new(
            calls[1].1.id().map(str::to_owned),
            "lookup",
            vec![ContentPart::Text {
                text: "result-b".into(),
            }],
            false,
        )
        .unwrap();
        let a = SessionToolResult::new(calls[0].0.clone(), result_a.clone());
        let foreign = SessionToolResult::new(
            ToolRequestId::new("other-session", NonZeroU64::new(1).unwrap()).unwrap(),
            result_b.clone(),
        );
        assert!(
            commands
                .submit(SessionCommand::ToolResults(vec![a.clone(), foreign]))
                .await
                .is_err()
        );
        commands
            .submit(SessionCommand::ToolResults(vec![a.clone()]))
            .await
            .unwrap();
        assert!(
            commands
                .submit(SessionCommand::ToolResults(vec![a]))
                .await
                .is_err()
        );
        assert_eq!(
            server.requests().len(),
            1,
            "partial required results do not start generation"
        );
        let b = SessionToolResult::new(calls[1].0.clone(), result_b);
        commands
            .submit(SessionCommand::ToolResults(vec![b]))
            .await
            .unwrap();
        Observation::completed(&mut events, "final answer").await;
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    let request = String::from_utf8(requests[1].body.clone()).unwrap();
    assert_eq!(request.matches("result-a").count(), 1);
    assert_eq!(request.matches("result-b").count(), 1);
}

#[tokio::test]
async fn cancelling_a_tool_continuation_keeps_accepted_results_in_exact_history() {
    use providers_kit::session::CancelTarget;
    use std::sync::Arc;
    use support::scripted_http::RecordedRequest;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio::task::JoinSet;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let reached = Arc::new(Notify::new());
    let opening = Arc::clone(&reached);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        RecordedRequest::read(&mut first).await.unwrap();
        FixtureProvider::OpenAi
            .response(None)
            .write(&mut first)
            .await
            .unwrap();
        let (mut second, _) = listener.accept().await.unwrap();
        RecordedRequest::read(&mut second).await.unwrap();
        opening.notify_one();
        let (mut third, _) = listener.accept().await.unwrap();
        let request = RecordedRequest::read(&mut third).await.unwrap();
        FixtureProvider::OpenAi
            .response(Some("final answer"))
            .write(&mut third)
            .await
            .unwrap();
        request
    });
    let provider = FixtureProvider::OpenAi.build(url);
    let model = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    let config = SessionConfig::new(SessionProfile::Conversation)
        .with_tools(vec![ProviderToolSpec::Function(
            ProviderFunctionTool::new(
                "lookup",
                None,
                JsonSchema::new(json!({"type":"object"})).unwrap(),
            )
            .unwrap(),
        )])
        .unwrap();
    let mut session = model.open_session(config).await.unwrap();
    {
        let (mut commands, mut events) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("original input").unwrap(),
            ))
            .await
            .unwrap();
        let (request, call) = Observation::completed(&mut events, "").await.tool.unwrap();
        let result = FunctionResult::new(
            call.id().map(str::to_owned),
            call.name(),
            vec![ContentPart::Text {
                text: "accepted-result-value".into(),
            }],
            false,
        )
        .unwrap();
        let accepted = commands
            .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                request, result,
            )]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .unwrap();
        commands
            .submit(SessionCommand::Cancel(CancelTarget::Command(accepted)))
            .await
            .unwrap();
        loop {
            if let Some(SessionEvent::Cancellation { .. }) = events.recv().await {
                break;
            }
        }
        commands
            .submit(SessionCommand::Input(
                Message::user_text("continue after stopped result generation").unwrap(),
            ))
            .await
            .unwrap();
        Observation::completed(&mut events, "final answer").await;
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    let request = tokio::time::timeout(Duration::from_secs(3), tasks.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let body = String::from_utf8(request.body).unwrap();
    assert_eq!(body.matches("accepted-result-value").count(), 1);
    assert_eq!(body.matches("original input").count(), 1);
    assert!(
        body.contains("item-preserved"),
        "original tool-only assistant native replay survives"
    );
}

#[tokio::test]
async fn nearly_full_history_keeps_reserved_cancellation_capacity() {
    use providers_kit::session::CancelTarget;
    let mut accepted_inputs = 0;
    // Probe a small neighborhood of the public 32 MiB retention budget. The
    // test does not depend on the private layout of owners or queue records.
    for spare in (512..=2048).step_by(64) {
        let mut server = spawn_scripted_http_server(Vec::new()).await;
        let provider = FixtureProvider::OpenAi.build(server.base_url());
        let model = provider
            .model(ModelId::new("gpt-session").unwrap())
            .unwrap();
        let mut session = model
            .open_session(SessionConfig::new(SessionProfile::Conversation))
            .await
            .unwrap();
        let cancel = {
            let (mut commands, _) = session.split();
            match commands
                .submit(SessionCommand::Input(
                    Message::user_text("x".repeat(32 * 1024 * 1024 - spare)).unwrap(),
                ))
                .await
            {
                Ok(command) => {
                    accepted_inputs += 1;
                    Some(
                        commands
                            .submit(SessionCommand::Cancel(CancelTarget::Command(command)))
                            .await,
                    )
                }
                Err(_) => None,
            }
        };
        provider.shutdown().await.unwrap();
        session.close().await.unwrap();
        assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
        if let Some(cancel) = cancel {
            assert!(
                cancel.is_ok(),
                "accepted history exhausted the reserved stop budget at spare={spare}: {cancel:?}"
            );
        }
    }
    assert!(
        accepted_inputs > 0,
        "fixture must reach admitted near-limit history"
    );
}

#[tokio::test]
async fn observational_citations_consume_the_aggregate_retention_budget() {
    let fixture = FixtureProvider::OpenAi;
    let citation_title = "c".repeat(4 * 1024 * 1024);
    let final_body = format!(
        "data: {}\n\n",
        json!({"type":"response.completed","response":{"model":fixture.model(),"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"cited output","annotations":[{"type":"url_citation","url":"https://example.test/source","title":citation_title,"start_index":0,"end_index":5}]}]}]}})
    );
    let mut server = spawn_scripted_http_server(vec![
        fixture.response(Some("first answer")),
        fixture.response(Some("first answer")),
        fixture.response(Some("first answer")),
        ScriptedResponse::sse(&final_body),
    ])
    .await;
    let provider = fixture.build(server.base_url());
    let model = provider
        .model(ModelId::new(fixture.model()).unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    let mut bounded = false;
    {
        let (mut commands, mut events) = session.split();
        for turn in 0..4 {
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("u".repeat(13 * 1024 * 1024 / 2)).unwrap(),
                ))
                .await
                .unwrap();
            loop {
                match tokio::time::timeout(Duration::from_secs(30), events.recv())
                    .await
                    .unwrap()
                    .unwrap()
                {
                    SessionEvent::ResponseFinished { outcome, .. } => {
                        assert!(matches!(outcome, ResponseOutcome::Completed));
                        assert_ne!(
                            turn, 3,
                            "large observation bypassed retained-byte admission"
                        );
                        break;
                    }
                    SessionEvent::Terminated { reason, .. } => {
                        assert_eq!(reason.kind(), providers_kit::error::ErrorKind::InvalidInput);
                        bounded = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    assert!(bounded);
    assert!(
        session.close().await.is_err(),
        "cleanup retains the primary resource failure"
    );
    provider.shutdown().await.err();
    assert_eq!(server.finish(Duration::from_secs(3)).await, Ok(()));
}

#[tokio::test]
async fn fabricated_same_session_cancellation_tokens_fail_before_acceptance() {
    use providers_kit::session::{CancelTarget, CommandId, ResponseId};
    use std::num::NonZeroU64;
    let mut server = spawn_scripted_http_server(Vec::new()).await;
    let provider = FixtureProvider::Anthropic.build(server.base_url());
    let model = provider
        .model(ModelId::new("claude-session").unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    let scope = session.id().to_owned();
    {
        let (mut commands, mut events) = session.split();
        for target in [
            CancelTarget::Command(
                CommandId::new(scope.clone(), NonZeroU64::new(9).unwrap()).unwrap(),
            ),
            CancelTarget::Response(ResponseId::new(scope, NonZeroU64::new(10).unwrap()).unwrap()),
        ] {
            assert!(
                commands
                    .submit(SessionCommand::Cancel(target))
                    .await
                    .is_err(),
                "session scope alone does not establish an issued identity"
            );
        }
        let mut pending = std::pin::pin!(events.recv());
        assert!(futures_util::poll!(&mut pending).is_pending());
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
}

#[tokio::test]
async fn provider_sealing_preserves_an_admitted_managed_completion() {
    use std::sync::Arc;
    use support::scripted_http::RecordedRequest;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio::task::JoinSet;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let reached = Arc::new(Notify::new());
    let opening = Arc::clone(&reached);
    let release = Arc::new(Notify::new());
    let released = Arc::clone(&release);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        RecordedRequest::read(&mut socket).await.unwrap();
        opening.notify_one();
        released.notified().await;
        FixtureProvider::OpenAi
            .response(Some("admitted answer"))
            .write(&mut socket)
            .await
            .unwrap();
    });
    let provider = FixtureProvider::OpenAi.build(url);
    let model = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    {
        let (mut commands, _) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("accepted request").unwrap(),
            ))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), reached.notified())
        .await
        .unwrap();
    let mut shutdown = std::pin::pin!(provider.shutdown());
    assert!(
        futures_util::poll!(&mut shutdown).is_pending(),
        "shutdown is waiting on admitted native work"
    );
    release.notify_one();
    {
        let (_, mut events) = session.split();
        Observation::completed(&mut events, "admitted answer").await;
    }
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap();
    {
        let (_, mut events) = session.split();
        assert!(matches!(
            events.recv().await,
            Some(SessionEvent::Terminated { .. })
        ));
        assert!(events.recv().await.is_none());
    }
    session.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), tasks.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn managed_undeclared_function_calls_never_become_executable() {
    let fixture = FixtureProvider::OpenAi;
    let mut server = spawn_scripted_http_server(vec![fixture.response(None)]).await;
    let provider = fixture.build(server.base_url());
    let model = provider
        .model(ModelId::new(fixture.model()).unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    {
        let (mut commands, mut events) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("no declared functions").unwrap(),
            ))
            .await
            .unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                SessionEvent::ToolRequested { .. } => {
                    panic!("undeclared native function became executable")
                }
                SessionEvent::ResponseFinished { outcome, .. } => {
                    assert!(matches!(
                        outcome,
                        ResponseOutcome::Failed(_) | ResponseOutcome::Unresolved(_)
                    ));
                    break;
                }
                SessionEvent::Terminated { reason, .. } => {
                    assert!(matches!(
                        reason.kind(),
                        providers_kit::error::ErrorKind::InvalidResponse(_)
                    ));
                    break;
                }
                _ => {}
            }
        }
    }
    session.close().await.err();
    provider.shutdown().await.err();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
}

#[tokio::test]
async fn managed_error_finish_reasons_are_failures_with_observational_output() {
    for (fixture,reply) in [
        (FixtureProvider::Gemini,ScriptedResponse::sse(&format!("data: {}\n\n",json!({"candidates":[{"content":{"role":"model","parts":[{"text":"partial output"}]},"finishReason":"MALFORMED_FUNCTION_CALL"}],"modelVersion":"gemini-session"})))),
        (FixtureProvider::Ollama,ScriptedResponse::chunks(200,"application/x-ndjson",vec![format!("{}\n",json!({"model":"ollama-session","message":{"role":"assistant","content":"partial output"},"done":true,"done_reason":"error"})).into_bytes()])),
    ] {
        let mut server=spawn_scripted_http_server(vec![reply]).await;
        let provider=fixture.build(server.base_url());let model=provider.model(ModelId::new(fixture.model()).unwrap()).unwrap();
        let mut session=model.open_session(SessionConfig::new(SessionProfile::Conversation)).await.unwrap();
        {
            let (mut commands,mut events)=session.split();commands.submit(SessionCommand::Input(Message::user_text("error finish").unwrap())).await.unwrap();
            loop {
                if let SessionEvent::ResponseFinished{outcome,output,..}=tokio::time::timeout(Duration::from_secs(3),events.recv()).await.unwrap().unwrap(){
                    let ResponseOutcome::Failed(error)=outcome else{panic!("provider error finish must be failure, got {outcome:?}");};
                    assert_eq!(error.kind(),providers_kit::error::ErrorKind::ProviderRejected);
                    assert!(error.native_details().code.is_none(),"normalization cannot invent a native error code");
                    let output=output.expect("returned failure output remains observational");
                    assert_eq!(output.finish_reason,Some(providers_kit::generation::FinishReason::Error));
                    assert!(output.message.content().iter().any(|content|matches!(content,ContentPart::Text{text} if text=="partial output")));
                    break;
                }
            }
        }
        session.close().await.unwrap();provider.shutdown().await.unwrap();assert_eq!(server.finish(Duration::from_secs(2)).await,Ok(()));
    }
}

#[tokio::test]
async fn concurrent_accepted_cancellations_each_keep_their_own_terminal_receipt() {
    use providers_kit::session::{CancelTarget, CancellationResolution};
    use std::sync::Arc;
    use support::scripted_http::RecordedRequest;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio::task::JoinSet;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let reached = Arc::new(Notify::new());
    let opening = Arc::clone(&reached);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        RecordedRequest::read(&mut socket).await.unwrap();
        opening.notify_one();
        let mut byte = [0];
        socket.read(&mut byte).await.unwrap()
    });
    let provider = FixtureProvider::OpenAi.build(url);
    let model = provider
        .model(ModelId::new("gpt-session").unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    {
        let (mut commands, mut events) = session.split();
        let input = commands
            .submit(SessionCommand::Input(
                Message::user_text("cancel twice").unwrap(),
            ))
            .await
            .unwrap();
        reached.notified().await;
        let Some(SessionEvent::ResponseStarted { response, .. }) = events.recv().await else {
            panic!("response identity must be observable");
        };
        let first = commands
            .submit(SessionCommand::Cancel(CancelTarget::Command(input.clone())))
            .await
            .unwrap();
        let second = commands
            .submit(SessionCommand::Cancel(CancelTarget::Response(
                response.clone(),
            )))
            .await
            .unwrap();
        let mut receipts = std::collections::BTreeSet::new();
        while receipts.len() < 2 {
            if let SessionEvent::Cancellation {
                command,
                target,
                resolution,
                ..
            } = tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .expect("all accepted cancels settle without another input")
                .unwrap()
            {
                if command == first {
                    assert!(matches!(target,CancelTarget::Command(id) if id==input));
                    assert!(matches!(resolution, CancellationResolution::LocallyStopped));
                } else {
                    assert_eq!(command, second);
                    assert!(matches!(target,CancelTarget::Response(id) if id==response));
                    assert!(matches!(resolution, CancellationResolution::AlreadySettled));
                }
                assert!(receipts.insert(command), "one receipt per accepted cancel");
            }
        }
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), tasks.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn partial_function_calls_leave_continuity_uncertain_and_are_not_executable() {
    let body = format!(
        "data: {}\n\n",
        json!({"type":"response.incomplete","response":{"model":"gpt-6-luna","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"function_call","id":"item-partial","call_id":"call-partial","name":"lookup","arguments":"{\"key\":\"a\"}"}]}})
    );
    let mut server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&body)]).await;
    let provider = FixtureProvider::OpenAi.build(server.base_url());
    let model = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    let config = SessionConfig::new(SessionProfile::Conversation)
        .with_tools(vec![ProviderToolSpec::Function(
            ProviderFunctionTool::new(
                "lookup",
                None,
                JsonSchema::new(json!({"type":"object"})).unwrap(),
            )
            .unwrap(),
        )])
        .unwrap();
    let mut session = model.open_session(config).await.unwrap();
    {
        let (mut commands, mut events) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("incomplete function output").unwrap(),
            ))
            .await
            .unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                SessionEvent::ToolRequested { .. } => {
                    panic!("partial calls are observational only")
                }
                SessionEvent::ResponseFinished {
                    outcome,
                    continuity,
                    output,
                    ..
                } => {
                    assert!(matches!(outcome, ResponseOutcome::Partial { .. }));
                    assert_eq!(
                        continuity,
                        providers_kit::session::SessionContinuity::Uncertain
                    );
                    assert_eq!(output.unwrap().message.function_calls().len(), 1);
                    break;
                }
                _ => {}
            }
        }
        assert!(
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("cannot replay unanswered incomplete calls").unwrap()
                ))
                .await
                .is_err()
        );
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(server.finish(Duration::from_secs(2)).await, Ok(()));
}

#[tokio::test]
async fn executable_tool_batch_reserves_all_retained_copies_before_publication() {
    let arguments = json!({"key":"a".repeat(6*1024*1024)}).to_string();
    let body = format!(
        "data: {}\n\n",
        json!({"type":"response.completed","response":{"model":"gpt-6-luna","status":"completed","output":[{"type":"function_call","id":"item-large","call_id":"call-large","name":"lookup","arguments":arguments}]}})
    );
    let mut server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&body)]).await;
    let provider = FixtureProvider::OpenAi.build(server.base_url());
    let model = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    let config = SessionConfig::new(SessionProfile::Conversation)
        .with_tools(vec![ProviderToolSpec::Function(
            ProviderFunctionTool::new(
                "lookup",
                None,
                JsonSchema::new(json!({"type":"object"})).unwrap(),
            )
            .unwrap(),
        )])
        .unwrap();
    let mut session = model.open_session(config).await.unwrap();
    {
        let (mut commands, mut events) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("u".repeat(6 * 1024 * 1024)).unwrap(),
            ))
            .await
            .unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(30), events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                SessionEvent::ToolRequested { .. } => panic!(
                    "tool exposed before its complete observation and obligation budget was reserved"
                ),
                SessionEvent::ResponseFinished { .. } => {
                    panic!("oversized completed tool retention must fail explicitly")
                }
                SessionEvent::Terminated { reason, .. } => {
                    assert_eq!(reason.kind(), providers_kit::error::ErrorKind::InvalidInput);
                    break;
                }
                _ => {}
            }
        }
    }
    assert!(session.close().await.is_err());
    provider.shutdown().await.err();
    assert_eq!(server.finish(Duration::from_secs(3)).await, Ok(()));
}

#[tokio::test]
async fn generation_deadline_remains_actionable_without_an_event_consumer() {
    use support::scripted_http::RecordedRequest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinSet;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut url = url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    url.set_path("/v1");
    let mut tasks = JoinSet::new();
    tasks.spawn(async move{
        let(mut socket,_)=listener.accept().await.unwrap();RecordedRequest::read(&mut socket).await.unwrap();
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let mut byte=[0];socket.read(&mut byte).await.unwrap()
    });
    let transport = TransportConfig::new(
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(1),
        providers_kit::config::RetryPolicy::default(),
    )
    .unwrap();
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::OpenAi(
            OpenAiConfig::new(ApiKey::new("session-fixture-key").unwrap()).with_base_url(url),
        ),
        transport,
    ))
    .unwrap();
    let model = provider
        .model(ModelId::new("gpt-session").unwrap())
        .unwrap();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Conversation))
        .await
        .unwrap();
    {
        let (mut commands, _) = session.split();
        commands
            .submit(SessionCommand::Input(
                Message::user_text("no observation demand").unwrap(),
            ))
            .await
            .unwrap();
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), tasks.join_next())
            .await
            .expect("deadline closes native stream without consumer polling")
            .unwrap()
            .unwrap(),
        0
    );
    {
        let (_, mut events) = session.split();
        loop {
            if let SessionEvent::ResponseFinished {
                outcome,
                continuity,
                ..
            } = events.recv().await.unwrap()
            {
                let ResponseOutcome::Unresolved(error) = outcome else {
                    panic!("deadline has no affirmative remote outcome");
                };
                assert_eq!(error.kind(), providers_kit::error::ErrorKind::Timeout);
                assert_eq!(
                    continuity,
                    providers_kit::session::SessionContinuity::Reusable
                );
                break;
            }
        }
    }
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
}
