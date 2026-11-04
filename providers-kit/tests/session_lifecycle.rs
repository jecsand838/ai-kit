use providers_kit::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use providers_kit::config::{ApiKey, GeminiConfig, OpenAiConfig, ProviderKind, TransportConfig};
use providers_kit::generation::{
    ContentPart, FinishReason, FunctionCall, FunctionResult, GenerationConfig, JsonSchema, Message,
    ProviderFunctionTool, ProviderToolSpec, ToolChoice,
};
use providers_kit::models::ModelId;
use providers_kit::session::{
    CancelTarget, CancellationResolution, ResponseOutcome, SessionBackend, SessionCommand,
    SessionCommands, SessionConfig, SessionContinuity, SessionEvent, SessionEvents, SessionProfile,
    SessionToolResult, SessionTurnDetection, ToolRequestId,
};
use providers_kit::{Model, ModelBackend, Provider, ProviderBackend, ProviderConfig, Session};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use url::Url;

#[tokio::test]
async fn diagnostic_resolution_peer_closure_keeps_receive_phase() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::start(move |mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            socket.close(None).await.expect("peer close writes");
            let _ = socket.next().await;
        })
        .await;
        let (provider, model) = server.openai_model();
        let mut session = model
            .open_session(SessionConfig::new(SessionProfile::Realtime))
            .await
            .expect("session opens");
        let error = InteractiveServer::terminal_error(&mut session).await;
        let close = session.close().await;
        let shutdown = provider.shutdown().await;
        server.finish().await;
        if let Err(close) = close {
            assert_eq!(
                close.context().operation,
                Operation::SessionClose,
                "peer closure may already have ended native close admission"
            );
        }
        shutdown.expect("provider cleanup completes");
        assert_eq!(error.kind(), ErrorKind::Closed);
        assert_eq!(error.context().operation, Operation::SessionReceive);
        assert_eq!(error.context().phase, ErrorPhase::SessionReceive);
    })
    .await
    .expect("peer closure proof is bounded");
}

#[tokio::test]
async fn diagnostic_resolution_socket_json_keeps_parser_observations() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for binary in [false, true] {
            let server = InteractiveServer::start(move |mut socket| async move {
                InteractiveServer::receive_setup(&mut socket).await;
                InteractiveServer::acknowledge_setup(&mut socket).await;
                let malformed = "{\"private\": ";
                let frame = if binary {
                    WebSocketMessage::Binary(malformed.as_bytes().to_vec().into())
                } else {
                    WebSocketMessage::Text(malformed.into())
                };
                socket.send(frame).await.expect("malformed frame writes");
                InteractiveServer::expect_close(&mut socket).await;
            })
            .await;
            let (provider, model) = server.openai_model();
            let mut session = model
                .open_session(SessionConfig::new(SessionProfile::Realtime))
                .await
                .expect("session opens");
            let error = InteractiveServer::terminal_error(&mut session).await;
            session.close().await.expect("session closes");
            provider.shutdown().await.expect("provider shuts down");
            server.finish().await;
            assert_eq!(error.context().operation, Operation::SessionReceive);
            assert_eq!(error.context().phase, ErrorPhase::Decode);
            assert!(matches!(
                error.response_details(),
                Some(providers_kit::error::ResponseDetails::Json {
                    category: providers_kit::error::JsonErrorCategory::Eof,
                    line: 1,
                    column: 12
                })
            ));
            assert!(!error.to_string().contains("private"));
        }
    })
    .await
    .expect("socket decode proof is bounded");
}

#[tokio::test]
async fn diagnostic_resolution_session_payload_decode_keeps_observations() {
    use providers_kit::error::{JsonErrorCategory, ResponseDetails};
    tokio::time::timeout(Duration::from_secs(10), async {
        for (payload, category) in [
            (serde_json::json!({"type": "response.function_call_arguments.done", "call_id": "fixture-call", "name": "lookup", "arguments": "{"}), JsonErrorCategory::Eof),
            (serde_json::json!({"type": "response.usage.updated", "usage": {"input_tokens": "wrong-shape"}}), JsonErrorCategory::Data),
            (serde_json::json!({"type": "error", "error": {"code": []}}), JsonErrorCategory::Data),
        ] {
            let server = InteractiveServer::start(move |mut socket| async move {
                InteractiveServer::receive_setup(&mut socket).await;
                InteractiveServer::acknowledge_setup(&mut socket).await;
                InteractiveServer::send_json(&mut socket, payload).await;
                InteractiveServer::expect_close(&mut socket).await;
            }).await;
            let (provider, model) = server.openai_model();
            let mut session = model.open_session(SessionConfig::new(SessionProfile::Realtime)).await.unwrap();
            let error = InteractiveServer::terminal_error(&mut session).await;
            session.close().await.unwrap();
            provider.shutdown().await.unwrap();
            server.finish().await;
            assert_eq!(error.context().operation, Operation::SessionReceive);
            assert_eq!(error.context().phase, ErrorPhase::Decode);
            assert!(matches!(error.response_details(), Some(ResponseDetails::Json { category: actual, .. }) if *actual == category));
        }
    }).await.expect("session payload decoding proof is bounded");
}

#[tokio::test]
async fn session_refactor_rejects_missing_or_invalid_terminal_status() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for payload in [
            serde_json::json!({"type": "response.done", "event_id": "terminal-missing-response"}),
            serde_json::json!({"type": "response.done", "response": null}),
            serde_json::json!({"type": "response.done", "response": {"id": "response-fixture"}}),
            serde_json::json!({"type": "response.done", "response": {"id": "response-fixture", "status": null}}),
            serde_json::json!({"type": "response.done", "response": {"id": "response-fixture", "status": 7}}),
        ] {
            let server = InteractiveServer::terminal_response(payload).await;
            let (provider, model) = server.openai_model();
            let mut session = model.open_session(SessionConfig::new(SessionProfile::Realtime)).await.unwrap();
            session.split().0.submit(InteractiveServer::text("terminal evidence")).await.unwrap();
            let observed = InteractiveServer::terminal_error(&mut session).await;
            session.close().await.expect("malformed terminal still releases session");
            provider.shutdown().await.expect("malformed terminal cleanup releases admission");
            server.finish().await;
            let error = observed;
            assert!(matches!(
                error.kind(),
                ErrorKind::InvalidResponse(ResponseViolation::MissingField { .. }
                    | ResponseViolation::InvalidField { .. })
            ));
            assert_eq!(error.context().operation, Operation::SessionReceive);
        }
    }).await.expect("terminal validation proof is bounded");
}

#[tokio::test]
async fn session_refactor_preserves_authoritative_terminal_usage() {
    tokio::time::timeout(Duration::from_secs(10), async {
        // Realtime reports authoritative usage on response.done.response, not a
        // required separate usage frame. Missing input count stays unknown.
        let server = InteractiveServer::terminal_response(serde_json::json!({
            "type": "response.done",
            "event_id": "terminal-usage",
            "response": {
                "id": "response-fixture",
                "status": "completed",
                "output": [],
                "usage": {"output_tokens": 7, "total_tokens": 9}
            }
        })).await;
        let (provider, model) = server.openai_model();
        let mut session = model.open_session(SessionConfig::new(SessionProfile::Realtime)).await.unwrap();
        session.split().0.submit(InteractiveServer::text("usage evidence")).await.unwrap();
        let observed = InteractiveServer::next_event(&mut session).await;
        session.close().await.expect("usage case session closes");
        provider.shutdown().await.expect("usage case provider closes");
        server.finish().await;
        assert!(matches!(observed, SessionEvent::Usage { ref usage, finalized: true, .. }
            if usage.input_tokens.is_none()
                && usage.output_tokens == Some(7)
                && usage.total_tokens == Some(9)),
            "authoritative terminal usage must be delivered without inventing unknown counts: {observed:?}");
    }).await.expect("terminal usage proof is bounded");
}

#[tokio::test]
async fn session_refactor_incomplete_response_preserves_partial_outcome() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::terminal_response(serde_json::json!({
            "type": "response.done",
            "event_id": "terminal-incomplete",
            "response": {
                "id": "response-fixture",
                "status": "incomplete",
                "status_details": {"type": "incomplete", "reason": "max_output_tokens"},
                "output": []
            }
        })).await;
        let (provider, model) = server.openai_model();
        let mut session = model.open_session(SessionConfig::new(SessionProfile::Realtime)).await.unwrap();
        let observed = {
            let (mut commands, mut events) = session.split();
            let command = commands.submit(SessionCommand::Input(Message::user_text("partial evidence").unwrap())).await.unwrap();
            let response = match events.recv().await.expect("response starts") {
                SessionEvent::ResponseStarted { command: Some(actual), response } => {
                    assert_eq!(actual, command);
                    response
                }
                other => panic!("expected correlated response start, got {other:?}"),
            };
            (response, events.recv().await)
        };
        session.close().await.expect("partial response still closes");
        provider.shutdown().await.expect("partial response admission releases");
        server.finish().await;
        assert!(matches!(observed.1,
            Some(SessionEvent::ResponseFinished {
                ref response,
                outcome: ResponseOutcome::Partial { reason: Some(FinishReason::Length) },
                continuity: SessionContinuity::Reusable,
                output: None,
            }) if response == &observed.0),
            "native incomplete is a partial outcome, not malformed data or successful completion: {:?}", observed.1);
    }).await.expect("partial response proof is bounded");
}

#[tokio::test]
async fn session_refactor_pending_receive_allows_submission_and_keeps_correlation() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::start(|mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_type(&mut socket, "conversation.item.create").await;
            let create = InteractiveServer::receive_json(&mut socket).await;
            assert_eq!(create["type"], "response.create");
            let metadata = create.pointer("/response/metadata").cloned();
            let mut started = serde_json::json!({
                "type": "response.created",
                "event_id": "start-duplex",
                "response": {"id": "response-duplex", "status": "in_progress", "output": []}
            });
            if let Some(metadata) = metadata {
                started["response"]["metadata"] = metadata;
            }
            InteractiveServer::send_json(&mut socket, started).await;
            InteractiveServer::send_json(&mut socket, serde_json::json!({
                "type": "response.output_text.delta", "event_id": "delta-duplex",
                "response_id": "response-duplex", "item_id": "item-duplex",
                "output_index": 0, "content_index": 0, "delta": "hello"
            })).await;
            InteractiveServer::send_json(&mut socket, serde_json::json!({
                "type": "response.done", "event_id": "done-duplex",
                "response": {"id": "response-duplex", "status": "completed", "output": []}
            })).await;
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.openai_model();
        let mut session = model.open_session(SessionConfig::new(SessionProfile::Realtime)).await.unwrap();
        {
            let (mut commands, mut events) = session.split();
            let mut receiving = Box::pin(events.recv());
            assert!(poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await.is_pending(),
                "the receive is genuinely pending before command submission");
            let command = commands.submit(SessionCommand::Input(Message::user_text("duplex input").unwrap())).await.unwrap();
            let response = match receiving.await.expect("response starts") {
                SessionEvent::ResponseStarted { command: Some(actual), response } => {
                    assert_eq!(actual, command);
                    response
                }
                other => panic!("expected correlated response start, got {other:?}"),
            };
            assert!(matches!(events.recv().await,
                Some(SessionEvent::Output { response: actual, segment: Some(_), content: ContentPart::Text { text } })
                    if actual == response && text == "hello"));
            assert!(matches!(events.recv().await,
                Some(SessionEvent::ResponseFinished { response: actual, outcome: ResponseOutcome::Completed,
                    continuity: SessionContinuity::Reusable, output: None }) if actual == response));
            let mut idle = Box::pin(events.recv());
            assert!(poll_fn(|cx| Poll::Ready(idle.as_mut().poll(cx))).await.is_pending(),
                "idle receive is pending, not permanently drained");
        }
        session.close().await.expect("duplex session closes");
        provider.shutdown().await.expect("duplex provider closes");
        server.finish().await;
    }).await.expect("duplex progress proof is bounded");
}

struct InteractiveServer {
    base_url: Url,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl InteractiveServer {
    async fn terminal_response(payload: serde_json::Value) -> Self {
        // GA envelope provenance:
        // https://developers.openai.com/api/reference/resources/realtime/server-events#response.done
        Self::start(move |mut socket| async move {
            Self::receive_setup(&mut socket).await;
            Self::acknowledge_setup(&mut socket).await;
            Self::expect_type(&mut socket, "conversation.item.create").await;
            Self::start_response(&mut socket, "response-fixture").await;
            Self::send_json(&mut socket, payload).await;
            Self::expect_close(&mut socket).await;
        })
        .await
    }

    async fn peer_closed_setup() {
        let reason = "The service is currently unavailable.";
        let server = Self::start(move |mut socket| async move {
            Self::receive_setup(&mut socket).await;
            socket
                .close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: reason.into(),
                }))
                .await
                .expect("peer setup rejection is sent");
            let reply = socket
                .next()
                .await
                .expect("peer close must receive a reply")
                .expect("peer close reply must decode");
            let WebSocketMessage::Close(Some(reply)) = reply else {
                panic!("peer close must receive its native acknowledgement");
            };
            assert_eq!(reply.code, CloseCode::Error);
            assert_eq!(reply.reason.as_str(), reason);
        })
        .await;
        let (provider, model) = server.openai_model();
        let mut owned_session = None;
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                match model
                    .open_session(SessionConfig::new(SessionProfile::Realtime))
                    .await
                {
                    Err(error) => Some(error),
                    Ok(session) => {
                        owned_session = Some(session);
                        None
                    }
                }
            }))
            .catch_unwind()
            .await;
        let mut failures = Vec::new();
        if let Some(session) = owned_session {
            match tokio::time::timeout(Duration::from_secs(3), session.close()).await {
                Ok(Ok(())) => {}
                result => failures.push(format!("unexpected session close: {result:?}")),
            }
        }
        match tokio::time::timeout(Duration::from_secs(3), provider.shutdown()).await {
            Ok(Ok(())) => {}
            result => failures.push(format!("provider shutdown: {result:?}")),
        }
        if let Err(error) = server.finish_checked().await {
            failures.push(error);
        }
        for failure in &failures {
            eprintln!("peer-close setup fixture cleanup failure: {failure}");
        }
        let error = match observed {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(Err(error)) => panic!("peer-close setup observation timed out: {error}"),
            Ok(Ok(error)) => error.expect("peer close must prevent session publication"),
        };
        assert_eq!(
            error.kind(),
            ErrorKind::InvalidResponse(ResponseViolation::UnexpectedEnd)
        );
        assert_eq!(error.context().operation, Operation::OpenSession);
        assert_eq!(error.context().phase, ErrorPhase::SessionHandshake);
        assert_eq!(error.http_status(), None);
        let message = format!("OpenAI websocket ended during interactive setup: 1011 {reason}");
        assert_eq!(
            error.native_details().message.as_deref(),
            Some(message.as_str())
        );
        assert!(!error.to_string().contains(reason));
        assert!(!format!("{error:?}").contains(reason));
        assert!(
            error.supplementary().is_empty(),
            "acknowledging peer closure must not invent a cleanup error: {error:?}"
        );
        assert!(
            failures.is_empty(),
            "peer-close cleanup failures: {failures:?}"
        );
    }

    async fn reject_handshake(status: u16, body: serde_json::Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("rejection fixture binds");
        let address = listener.local_addr().expect("rejection fixture address");
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("handshake connects");
            let mut request = Vec::new();
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let mut bytes = [0; 1024];
                let count = stream
                    .read(&mut bytes)
                    .await
                    .expect("handshake request reads");
                assert_ne!(count, 0, "handshake request cannot end before its headers");
                request.extend_from_slice(&bytes[..count]);
                assert!(request.len() <= 16_384, "fixture request header bound");
            }
            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 {status:03} Fixture rejection\r\nContent-Type: application/json\r\nContent-Length: {}\r\nx-request-id: sensitive-handshake-request\r\nRetry-After: 9\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("complete rejection writes");
            stream
                .shutdown()
                .await
                .expect("rejection connection closes");
        });
        Self {
            base_url: Url::parse(&format!("http://{address}")).expect("rejection URL"),
            task: Some(task),
        }
    }

    async fn start<F, Fut>(handler: F) -> Self
    where
        F: FnOnce(WebSocketStream<TcpStream>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("interactive session server should bind");
        let address = listener
            .local_addr()
            .expect("interactive session server should have an address");
        let task = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("interactive websocket should connect");
            handler(
                tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("interactive websocket should upgrade"),
            )
            .await;
        });
        Self {
            base_url: Url::parse(&format!("http://{address}"))
                .expect("interactive session server URL should parse"),
            task: Some(task),
        }
    }

    async fn automatic_audio() -> Self {
        Self::start(move |mut socket| async move {
            let setup = Self::receive_json(&mut socket).await;
            assert_eq!(setup["type"], "session.update");
            Self::acknowledge_setup(&mut socket).await;
            let audio = Self::receive_json(&mut socket).await;
            assert_eq!(audio["type"], "input_audio_buffer.append");
            assert_eq!(audio["audio"], "AQID");
            while let Some(message) = socket.next().await {
                if message
                    .expect("automatic-audio close frame should decode")
                    .is_close()
                {
                    socket
                        .flush()
                        .await
                        .expect("automatic-audio close acknowledgement flushes");
                    break;
                }
            }
        })
        .await
    }

    async fn openai() -> Self {
        Self::start(|mut socket| async move {
            let update=Self::receive_json(&mut socket).await;
            assert_eq!(update["session"]["output_modalities"],serde_json::json!(["audio"]));
            assert_eq!(update["session"]["audio"]["input"]["transcription"]["model"],"gpt-transcribe");
            assert!(update["session"]["audio"]["input"]["turn_detection"].is_null());
            assert_eq!(update["session"]["tools"][0]["name"],"lookup");
            assert!(update["session"]["tools"][0].get("strict").is_none());
            Self::send_json(&mut socket,serde_json::json!({"type":"session.created"})).await;
            Self::send_json(&mut socket,serde_json::json!({"type":"session.updated","session":{"type":"realtime","model":"gpt-realtime-mini","output_modalities":["audio"]}})).await;
            Self::expect_type(&mut socket,"conversation.item.create").await;
            Self::start_response(&mut socket,"r1").await;
            for payload in [
                serde_json::json!({"type":"response.output_audio.delta","response_id":"r1","item_id":"a1","content_index":0,"delta":"AQID"}),
                serde_json::json!({"type":"response.output_audio_transcript.delta","response_id":"r1","item_id":"a1","content_index":0,"delta":"hello"}),
                serde_json::json!({"type":"response.done","response":{"id":"r1","status":"completed","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}),
            ] {Self::send_json(&mut socket,payload).await;}
            assert_eq!(Self::receive_json(&mut socket).await,serde_json::json!({"type":"input_audio_buffer.append","audio":"BAUG"}));
            Self::expect_type(&mut socket,"input_audio_buffer.commit").await;
            Self::start_response(&mut socket,"r2").await;
            Self::expect_type(&mut socket,"input_audio_buffer.append").await; // B has started before A's commit acknowledgement.
            Self::send_json(&mut socket,serde_json::json!({"type":"input_audio_buffer.committed","item_id":"input1"})).await;
            Self::send_json(&mut socket,serde_json::json!({"type":"response.function_call_arguments.done","response_id":"r2","item_id":"tool1","call_id":"call_1","name":"lookup","arguments":"{\"key\":\"value\"}"})).await;
            Self::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"r2","status":"completed"}})).await;
            let result=Self::receive_json(&mut socket).await;
            assert_eq!(result["item"]["type"],"function_call_output");
            assert_eq!(result["item"]["call_id"],"call_1");
            assert_eq!(result["item"]["output"],"42");
            Self::start_response(&mut socket,"r3").await;
            Self::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"r3","status":"completed"}})).await;
            // Late input transcript retains capture identity, independently of current response.
            Self::send_json(&mut socket,serde_json::json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"input1","delta":"caller"})).await;
            Self::expect_type(&mut socket,"conversation.item.create").await;
            Self::start_response(&mut socket,"r4").await;
            Self::send_json(&mut socket,serde_json::json!({"type":"response.function_call_arguments.done","response_id":"r4","item_id":"tool2","call_id":"call_2","name":"lookup","arguments":"{\"key\":\"cancelled\"}"})).await;
            Self::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"r4","status":"cancelled"}})).await;
            Self::expect_type(&mut socket,"conversation.item.create").await;
            Self::start_response(&mut socket,"r5").await;
            assert_eq!(Self::receive_json(&mut socket).await,serde_json::json!({"type":"response.cancel","response_id":"r5"}));
            Self::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"r5","status":"cancelled"}})).await;
            Self::expect_close(&mut socket).await;
        }).await
    }

    async fn start_response(socket: &mut WebSocketStream<TcpStream>, id: &str) {
        let create = Self::receive_json(socket).await;
        assert_eq!(create["type"], "response.create");
        Self::send_json(socket,serde_json::json!({"type":"response.created","response":{"id":id,"status":"in_progress","metadata":create["response"]["metadata"]}})).await;
    }

    async fn send_json(socket: &mut WebSocketStream<TcpStream>, payload: serde_json::Value) {
        socket
            .send(WebSocketMessage::Text(payload.to_string().into()))
            .await
            .expect("interactive session event should send");
    }

    async fn expect_type(socket: &mut WebSocketStream<TcpStream>, expected: &str) {
        assert_eq!(Self::receive_json(socket).await["type"], expected);
    }

    async fn receive_json(socket: &mut WebSocketStream<TcpStream>) -> serde_json::Value {
        let message = socket
            .next()
            .await
            .expect("session websocket message should arrive")
            .expect("session websocket message should decode");
        serde_json::from_str(
            message
                .to_text()
                .expect("session websocket message should be text"),
        )
        .expect("session websocket message should be JSON")
    }

    async fn receive_setup(socket: &mut WebSocketStream<TcpStream>) {
        let setup = Self::receive_json(socket).await;
        assert_eq!(setup["type"], "session.update");
    }

    async fn acknowledge_setup(socket: &mut WebSocketStream<TcpStream>) {
        Self::send_json(
            socket,
            serde_json::json!({
                "type": "session.updated",
                "session": {
                    "type": "realtime",
                    "model": "gpt-realtime-mini",
                    "output_modalities": ["text"],
                }
            }),
        )
        .await;
    }

    async fn expect_close(socket: &mut WebSocketStream<TcpStream>) {
        let message = socket
            .next()
            .await
            .expect("session close frame should arrive")
            .expect("session close frame should decode");
        assert!(
            message.is_close(),
            "session should send native socket close"
        );
        socket
            .flush()
            .await
            .expect("peer Close acknowledgement flushes");
    }

    async fn expect_cancelled_setup(socket: &mut WebSocketStream<TcpStream>) {
        match socket.next().await {
            None
            | Some(Err(WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => {}
            Some(Err(WebSocketError::Io(error))) => assert!(matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
            )),
            Some(Ok(message)) => assert!(message.is_close()),
            other => panic!("cancelled setup should close or drop its socket: {other:?}"),
        }
    }

    fn openai_model(&self) -> (Provider, Model) {
        let provider = Provider::from_config(ProviderConfig::new(
            ProviderKind::OpenAi(
                OpenAiConfig::new(
                    ApiKey::new("openai-session-key").expect("fixture API key should validate"),
                )
                .with_base_url(self.base_url.clone()),
            ),
            TransportConfig::default(),
        ))
        .expect("interactive provider should construct");
        let model = provider
            .model(ModelId::new("gpt-realtime-mini").expect("fixture model id should validate"))
            .expect("interactive model should construct");
        (provider, model)
    }

    fn lookup_function() -> ProviderFunctionTool {
        ProviderFunctionTool::new(
            "lookup",
            Some("Looks up one value".to_string()),
            JsonSchema::new(serde_json::json!({
                "type": "object",
                "properties": { "key": { "type": "string" } },
                "required": ["key"],
            }))
            .expect("function schema should validate"),
        )
        .expect("function declaration should validate")
    }

    fn manual_config(input_audio: &str) -> SessionConfig {
        SessionConfig::new(SessionProfile::Realtime)
            .with_input_audio(input_audio, true)
            .unwrap()
            .with_output_audio("audio/pcm;rate=24000", true)
            .unwrap()
            .with_turn_detection(SessionTurnDetection::Manual)
            .with_tools(vec![ProviderToolSpec::Function(Self::lookup_function())])
            .unwrap()
    }

    fn function_config(
        functions: Vec<ProviderFunctionTool>,
        choice: Option<ToolChoice>,
    ) -> Result<SessionConfig, providers_kit::KitError> {
        let config = SessionConfig::new(SessionProfile::Realtime).with_tools(
            functions
                .into_iter()
                .map(ProviderToolSpec::Function)
                .collect(),
        )?;
        match choice {
            Some(choice) => {
                config.with_generation(GenerationConfig::default().with_tool_choice(choice))
            }
            None => Ok(config),
        }
    }

    fn audio(media_type: &str, data: Vec<u8>) -> SessionCommand {
        SessionCommand::Audio {
            media_type: media_type.to_string(),
            data,
        }
    }
    fn text(text: &str) -> SessionCommand {
        SessionCommand::Input(Message::user_text(text).unwrap())
    }

    async fn answer_function(
        session: &mut Session,
        request: ToolRequestId,
        call: &FunctionCall,
    ) -> providers_kit::session::CommandId {
        let wrong = FunctionResult::new(
            call.id().map(str::to_owned),
            "other",
            vec![ContentPart::Text {
                text: "wrong".into(),
            }],
            false,
        )
        .unwrap();
        assert!(
            session
                .split()
                .0
                .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                    request.clone(),
                    wrong
                )]))
                .await
                .is_err()
        );
        let result = FunctionResult::new(
            call.id().map(str::to_owned),
            call.name(),
            vec![ContentPart::Text { text: "42".into() }],
            false,
        )
        .unwrap();
        session
            .split()
            .0
            .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                request, result,
            )]))
            .await
            .unwrap()
    }

    async fn send_input(session: &mut Session, input: SessionCommand) {
        session
            .split()
            .0
            .submit(input)
            .await
            .expect("valid command accepted");
    }

    async fn next_event(session: &mut Session) -> SessionEvent {
        loop {
            let event = session
                .split()
                .1
                .recv()
                .await
                .expect("session not terminal");
            if !matches!(
                event,
                SessionEvent::ResponseStarted { .. } | SessionEvent::ControlApplied { .. }
            ) {
                return event;
            }
        }
    }

    async fn terminal_error(session: &mut Session) -> providers_kit::KitError {
        loop {
            match session
                .split()
                .1
                .recv()
                .await
                .expect("terminal event must precede None")
            {
                SessionEvent::Terminated { reason, .. } => return reason,
                SessionEvent::ResponseFinished {
                    outcome: ResponseOutcome::Completed,
                    ..
                } => panic!("invalid response fabricated completion"),
                _ => {}
            }
        }
    }

    async fn finish_session(self, session: Session, provider: Provider) {
        session.close().await.expect("session should close");
        provider.shutdown().await.expect("provider should close");
        self.finish().await;
    }

    async fn finish_checked(mut self) -> Result<(), String> {
        let mut task = self
            .task
            .take()
            .expect("interactive server task remains owned");
        match tokio::time::timeout(Duration::from_secs(3), &mut task).await {
            Ok(result) => {
                result.map_err(|error| format!("interactive fixture join failed: {error}"))
            }
            Err(_) => {
                task.abort();
                let settled = task.await;
                Err(format!(
                    "interactive fixture join timed out; aborted task settled: {settled:?}"
                ))
            }
        }
    }

    async fn finish(mut self) {
        self.task
            .take()
            .expect("interactive server task should be owned")
            .await
            .expect("interactive session server should join");
    }
}

#[tokio::test]
async fn session_handshake_http_rejections_preserve_native_classification_and_metadata() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for status in [401, 429] {
            let body = serde_json::json!({"error":{"type":if status == 401 { "invalid_request_error" } else { "insufficient_quota" },"code":if status == 401 { "invalid_api_key" } else { "insufficient_quota" },"param":"sensitive-parameter","message":"sensitive-handshake-body"}});
            let server = InteractiveServer::reject_handshake(status, body.clone()).await;
            let (provider, model) = server.openai_model();
            let error = match model.open_session(SessionConfig::new(SessionProfile::Realtime)).await {
                Err(error) => error,
                Ok(session) => { session.close().await.expect("unexpected session closes"); panic!("HTTP rejection must prevent session publication"); }
            };
            provider.shutdown().await.expect("rejected provider closes");
            server.finish().await;
            let expected = match status {
                401 => ErrorKind::Authentication,
                429 => ErrorKind::QuotaExceeded,
                _ => unreachable!("fixture statuses are exhaustive"),
            };
            assert_eq!(error.kind(), expected);
            assert_eq!(error.http_status(), Some(status));
            assert_eq!(error.context().operation, Operation::OpenSession);
            assert_eq!(error.context().phase, ErrorPhase::SessionHandshake);
            assert_eq!(error.native_details().request_id.as_deref(), Some("sensitive-handshake-request"));
            assert_eq!(error.native_details().retry_after.as_deref(), Some("9"));
            assert_eq!(error.native_details().body.as_deref(), Some(body.to_string().as_str()));
            assert!(!error.native_details().truncated);
            assert_eq!(error.native_details().code.as_deref(), Some(if status == 401 { "invalid_api_key" } else { "insufficient_quota" }));
            assert_eq!(error.native_details().parameter.as_deref(), Some("sensitive-parameter"));
            assert!(!format!("{error:?}").contains("sensitive-"));
            assert!(!error.to_string().contains("sensitive-"));
        }
    }).await.expect("handshake rejection proof is bounded");
}

#[tokio::test]
async fn malformed_remote_session_handshake_is_a_response_protocol_failure() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::reject_handshake(0, serde_json::json!({})).await;
        let (provider, model) = server.openai_model();
        let (error, close) = match model
            .open_session(SessionConfig::new(SessionProfile::Realtime))
            .await
        {
            Err(error) => (Some(error), Ok(())),
            Ok(session) => (None, session.close().await),
        };
        let shutdown = provider.shutdown().await;
        server.finish().await;
        close.expect("any unexpectedly published session closes");
        shutdown.expect("malformed handshake releases provider admission");
        let error = error.expect("invalid HTTP status must prevent session publication");
        assert!(
            matches!(
                error.kind(),
                ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation { .. })
            ),
            "remote HTTP syntax must be a response failure: {error}"
        );
        assert_eq!(error.context().operation, Operation::OpenSession);
        assert_eq!(error.context().phase, ErrorPhase::SessionHandshake);
        assert_eq!(error.http_status(), None);
    })
    .await
    .expect("malformed handshake proof and fixture joins are bounded");
}

#[tokio::test]
async fn oversized_session_request_uri_fails_locally_before_connect() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("local validation listener binds");
        let address = listener.local_addr().expect("local listener address");
        // The adapter joins a relative endpoint onto this directory. The
        // URL is valid, but exceeds the HTTP URI parser's size limit.
        let base_url = Url::parse(&format!("http://{address}/{}/", "x".repeat(70_000)))
            .expect("long directory URL parses");
        let key = ApiKey::new("session-fixture-key").expect("fixture key validates");
        let (kind, model_id) = (
            ProviderKind::OpenAi(OpenAiConfig::new(key).with_base_url(base_url)),
            "gpt-realtime-mini",
        );
        let provider = Provider::from_config(ProviderConfig::new(kind, TransportConfig::default()))
            .expect("valid base URL constructs a provider");
        let model = provider
            .model(ModelId::new(model_id).expect("model identity validates"))
            .expect("exact model selects");
        let (error, close) = match model
            .open_session(SessionConfig::new(SessionProfile::Realtime))
            .await
        {
            Err(error) => (Some(error), Ok(())),
            Ok(session) => (None, session.close().await),
        };
        let connected = tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_ok();
        let shutdown = provider.shutdown().await;
        drop(listener);
        close.expect("any unexpectedly published session closes");
        shutdown.expect("local construction failure releases provider admission");
        let error = error.expect("oversized request URI cannot publish a session");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.context().operation, Operation::OpenSession);
        assert_eq!(error.context().phase, ErrorPhase::RequestBuild);
        assert_eq!(error.http_status(), None);
        assert!(!connected, "invalid local URI cannot connect");
    })
    .await
    .expect("local URI construction proof is bounded");
}

#[tokio::test]
async fn openai_peer_closed_setup_preserves_rejection_and_acknowledges_close() {
    InteractiveServer::peer_closed_setup().await;
}

#[tokio::test]
async fn failed_native_session_setup_sends_close_and_preserves_the_primary_rejection() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let body = serde_json::json!({"type":"error","event_id":"sensitive-event","error":{"type":"invalid_request_error","code":"invalid_api_key","message":"sensitive-setup-rejection"}});
        let emitted = body.clone();
        let server = InteractiveServer::start(move |mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::send_json(&mut socket, emitted).await;
            // A dropped TCP stream is insufficient: the failed setup must emit a
            // native Close before ownership is released.
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.openai_model();
        let error = match model.open_session(SessionConfig::new(SessionProfile::Realtime)).await {
            Err(error) => error,
            Ok(session) => { session.close().await.expect("unexpected session closes"); panic!("native setup rejection must prevent session publication"); }
        };
        provider.shutdown().await.expect("failed setup releases provider admission");
        server.finish().await;
        assert_eq!(error.kind(), ErrorKind::Authentication);
        assert_eq!(error.context().operation, Operation::OpenSession);
        assert_eq!(error.context().phase, ErrorPhase::SessionHandshake);
        assert_eq!(error.http_status(), None);
        assert_eq!(error.native_details().body.as_deref(), Some(body.to_string().as_str()));
        assert!(error.supplementary().is_empty(), "successful close must not fabricate a cleanup failure");
        assert!(!format!("{error:?}").contains("sensitive-"));
    }).await.expect("failed setup cleanup proof is bounded");
}

impl Drop for InteractiveServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[test]
fn session_config_validates_and_preserves_every_declared_setting() {
    let base = SessionConfig::new(SessionProfile::Realtime);
    assert!(base.clone().with_instructions(" ").is_err());
    assert!(base.clone().with_input_audio("text/plain", false).is_err());
    assert!(
        base.clone()
            .with_input_audio("audio/not a type", false)
            .is_err()
    );
    assert!(
        base.clone()
            .with_generation(GenerationConfig::default().with_tool_choice(ToolChoice::Required))
            .is_err()
    );
    let function = InteractiveServer::lookup_function();
    assert!(
        base.clone()
            .with_tools(vec![
                ProviderToolSpec::Function(function.clone()),
                ProviderToolSpec::Function(function.clone())
            ])
            .is_err()
    );
    let config = base
        .with_instructions("Be concise")
        .unwrap()
        .with_input_audio("audio/pcm;rate=16000", true)
        .unwrap()
        .with_output_audio("audio/pcm;rate=24000", true)
        .unwrap()
        .with_turn_detection(SessionTurnDetection::Manual)
        .with_tools(vec![ProviderToolSpec::Function(function)])
        .unwrap()
        .with_generation(
            GenerationConfig::default().with_tool_choice(ToolChoice::Tool {
                name: "lookup".into(),
            }),
        )
        .unwrap();
    assert_eq!(config.instructions(), Some("Be concise"));
    assert_eq!(config.input_audio_format(), Some("audio/pcm;rate=16000"));
    assert_eq!(config.output_audio_format(), Some("audio/pcm;rate=24000"));
    assert!(config.input_transcription() && config.output_transcription());
    assert_eq!(config.turn_detection(), SessionTurnDetection::Manual);
    assert_eq!(config.tools().len(), 1);
    assert!(
        matches!(config.generation().tool_choice(),Some(ToolChoice::Tool{name}) if name=="lookup")
    );
}

#[tokio::test]
async fn cancelled_session_setup_releases_shutdown_admission() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (setup_sent, setup_received) = oneshot::channel();
        let server = InteractiveServer::start(move |mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            setup_sent.send(()).expect("setup waiter should remain");
            InteractiveServer::expect_cancelled_setup(&mut socket).await;
        })
        .await;
        let (provider, model) = server.openai_model();
        let mut opening = Box::pin(model.open_session(SessionConfig::new(SessionProfile::Realtime)));
        tokio::select! {
            setup = setup_received => setup.expect("native setup should reach the server"),
            result = &mut opening => panic!("setup must wait for native acknowledgment: {result:?}"),
        }

        let mut shutdown = Box::pin(provider.shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), shutdown.as_mut())
                .await
                .is_err(),
            "shutdown must drain the admitted session setup",
        );
        assert!(matches!(
            provider.model(model.model_id().clone()),
            Err(ref error) if error.kind() == ErrorKind::Closed
        ));
        drop(opening);
        shutdown
            .await
            .expect("cancelled setup must release shutdown admission");
        server.finish().await;
    })
    .await
    .expect("cancelled session setup proof should be bounded");
}

#[tokio::test]
async fn session_setup_finishes_after_sealing_and_observation_drains() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (setup_sent, setup_received) = oneshot::channel();
        let (resume_setup, resumed_setup) = oneshot::channel();
        let server = InteractiveServer::start(move |mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            setup_sent.send(()).expect("setup waiter should remain");
            resumed_setup.await.expect("setup should be resumed");
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.openai_model();
        let mut opening = Box::pin(model.open_session(SessionConfig::new(SessionProfile::Realtime)));
        tokio::select! {
            setup = setup_received => setup.expect("native setup should reach the server"),
            result = &mut opening => panic!("setup must wait for native acknowledgment: {result:?}"),
        }

        let mut shutdown = Box::pin(provider.shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), shutdown.as_mut())
                .await
                .is_err(),
            "shutdown must wait while native setup is held",
        );
        assert!(matches!(
            provider.model(model.model_id().clone()),
            Err(ref error) if error.kind() == ErrorKind::Closed
        ));
        resume_setup.send(()).expect("server should await setup release");
        let mut session = opening
            .await
            .expect("admitted session setup should finish after sealing");
        shutdown
            .await
            .expect("finished session setup must release shutdown admission");
        assert!(matches!(
            session.split().0.submit(InteractiveServer::text("sealed")).await,
            Err(ref error) if error.kind() == ErrorKind::Closed
        ));
        assert!(matches!(session.split().1.recv().await, Some(SessionEvent::Terminated { continuity: SessionContinuity::Closed, .. })));
        assert!(session.split().1.recv().await.is_none());
        session
            .close()
            .await
            .expect("session should retain consuming cleanup after sealing");
        server.finish().await;
    })
    .await
    .expect("session setup and sealing proof should be bounded");
}

#[tokio::test]
async fn pending_session_recv_does_not_hold_generation_admission() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::start(move |mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.openai_model();
        let mut session = model
            .open_session(SessionConfig::new(SessionProfile::Realtime))
            .await
            .expect("interactive session should open");
        let (_, mut events) = session.split();
        let mut receiving = Box::pin(events.recv());
        assert!(
            poll_fn(|context| Poll::Ready(receiving.as_mut().poll(context)))
                .await
                .is_pending(),
            "recv must be polled before its cancellation is tested",
        );
        provider
            .shutdown()
            .await
            .expect("idle observation does not hold network admission");
        assert!(matches!(
            provider.model(model.model_id().clone()),
            Err(ref error) if error.kind() == ErrorKind::Closed
        ));
        drop(receiving);

        session
            .close()
            .await
            .expect("session close should remain available after recv cancellation");
        server.finish().await;
    })
    .await
    .expect("pending session recv cancellation proof should be bounded");
}

#[tokio::test]
async fn session_close_survives_dropping_parent_handles() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::start(move |mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.openai_model();
        let session = model
            .open_session(SessionConfig::new(SessionProfile::Realtime))
            .await
            .expect("interactive session should open");
        drop(model);
        drop(provider);
        session
            .close()
            .await
            .expect("session must own native cleanup independently of parent variables");
        server.finish().await;
    })
    .await
    .expect("session cleanup after parent drop proof should be bounded");
}

#[tokio::test]
async fn rejected_automatic_audio_does_not_advance_session_state() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::automatic_audio().await;
        let (provider, model) = server.openai_model();
        let media_type = "audio/pcm;rate=24000";
        let config = SessionConfig::new(SessionProfile::Realtime)
            .with_input_audio(media_type, false)
            .expect("audio input should validate");
        let mut session = model
            .open_session(config)
            .await
            .expect("interactive session should open");

        assert!(matches!(
            session
                .split().0.submit(InteractiveServer::audio(
                    media_type,
                    vec![0; 6 * 1024 * 1024],
                ))
                .await,
            Err(ref error) if error.kind() == ErrorKind::InvalidInput
        ));
        InteractiveServer::send_input(
            &mut session,
            InteractiveServer::audio(media_type, vec![1, 2, 3]),
        )
        .await;
        assert!(
            matches!(
                session.split().1.recv().await,
                Some(SessionEvent::ControlApplied { .. })
            ),
            "valid accepted audio reaches transport before close"
        );
        server.finish_session(session, provider).await;
    })
    .await
    .expect("automatic-audio rejection proof should be bounded");
}

#[tokio::test]
async fn openai_opening_rejects_server_errors_close_and_unrelated_events() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for response in [
            Some(serde_json::json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "code": "fixture_error",
                    "message": "fixture rejection",
                }
            })),
            Some(serde_json::json!({
                "type": "response.created",
                "response": {"status": "in_progress"},
            })),
            None,
        ] {
            let server = InteractiveServer::start(move |mut socket| async move {
                assert_eq!(
                    InteractiveServer::receive_json(&mut socket).await["type"],
                    "session.update"
                );
                if let Some(response) = response {
                    InteractiveServer::send_json(&mut socket, response).await;
                } else {
                    socket
                        .close(None)
                        .await
                        .expect("fixture server close should send");
                }
            })
            .await;
            let (provider, model) = server.openai_model();
            assert!(
                model
                    .open_session(SessionConfig::new(SessionProfile::Realtime))
                    .await
                    .is_err(),
                "OpenAI must not publish a session before session.updated"
            );
            provider
                .shutdown()
                .await
                .expect("rejected-opening provider should shut down");
            server.finish().await;
        }
    })
    .await
    .expect("OpenAI opening rejection proof should be bounded");
}

#[tokio::test]
async fn openai_realtime_function_choices_preserve_native_declarations() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for (choice, expected_choice, names) in [
            (None, serde_json::Value::Null, vec!["lookup", "unused"]),
            (Some(ToolChoice::Auto), serde_json::json!("auto"), vec!["lookup", "unused"]),
            (Some(ToolChoice::None), serde_json::json!("none"), vec!["lookup", "unused"]),
            (Some(ToolChoice::Required), serde_json::json!("required"), vec!["lookup", "unused"]),
            (Some(ToolChoice::Any), serde_json::json!("required"), vec!["lookup", "unused"]),
            (Some(ToolChoice::Tool { name: "lookup".to_string() }), serde_json::json!({"type": "function", "name": "lookup"}), vec!["lookup", "unused"]),
        ] {
            let schema = serde_json::json!({"type": "object", "properties": {"optional": {"type": "string"}}});
            let functions = ["lookup", "unused"].map(|name| {
                ProviderFunctionTool::new(name, None, JsonSchema::new(schema.clone()).unwrap()).unwrap()
            }).to_vec();
            let server = InteractiveServer::start(move |mut socket| async move {
                let setup = InteractiveServer::receive_json(&mut socket).await;
                assert_eq!(setup["session"]["tool_choice"], expected_choice);
                assert_eq!(setup["session"]["tools"], serde_json::json!(names.into_iter().map(|name| {
                    serde_json::json!({"type": "function", "name": name, "description": null, "parameters": schema})
                }).collect::<Vec<_>>()));
                InteractiveServer::acknowledge_setup(&mut socket).await;
                InteractiveServer::expect_close(&mut socket).await;
            }).await;
            let (provider, model) = server.openai_model();
            let config = InteractiveServer::function_config(functions, choice).unwrap();
            let session = model.open_session(config).await.expect("Realtime function choice should open");
            server.finish_session(session, provider).await;
        }
    }).await.expect("Realtime function choices should be bounded");
}

#[tokio::test]
async fn openai_realtime_allowed_function_subsets_fail_before_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::OpenAi(
            OpenAiConfig::new(ApiKey::new("fixture-key").unwrap()).with_base_url(
                Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap(),
            ),
        ),
        TransportConfig::default(),
    ))
    .unwrap();
    let model = provider
        .model(ModelId::new("gpt-realtime-mini").unwrap())
        .unwrap();
    for mode in [
        providers_kit::generation::AllowedToolMode::Auto,
        providers_kit::generation::AllowedToolMode::Required,
    ] {
        let config = InteractiveServer::function_config(
            vec![InteractiveServer::lookup_function()],
            Some(ToolChoice::Allowed {
                mode,
                names: vec!["lookup".to_string()],
            }),
        )
        .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), model.open_session(config))
                .await
                .unwrap(),
            Err(ref error) if error.kind() == ErrorKind::Unsupported
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    provider.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_audio_transcription_functions_and_cancellation_are_normalized() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let server=InteractiveServer::openai().await;
        let (provider,model)=server.openai_model();
        let mut session=model.open_session(InteractiveServer::manual_config("audio/pcm;rate=24000")).await.unwrap();
        InteractiveServer::send_input(&mut session,InteractiveServer::text("first")).await;
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::Output{content:ContentPart::Audio{data,..},segment:Some(_),..} if data==vec![1,2,3]));
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::Transcription{text,finalized:false,..} if text=="hello"));
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::Usage{usage,finalized:true,..} if usage.input_tokens==Some(1)&&usage.output_tokens==Some(2)&&usage.total_tokens==Some(3)));
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::ResponseFinished{outcome:ResponseOutcome::Completed,..}));
        let capture=session.split().0.submit(SessionCommand::StartInput).await.unwrap();
        InteractiveServer::send_input(&mut session,InteractiveServer::audio("audio/pcm;rate=24000",vec![4,5,6])).await;
        InteractiveServer::send_input(&mut session,SessionCommand::EndInput).await;
        InteractiveServer::send_input(&mut session,SessionCommand::StartInput).await;
        InteractiveServer::send_input(&mut session,InteractiveServer::audio("audio/pcm;rate=24000",vec![1,2])).await;
        let (request,call)=match InteractiveServer::next_event(&mut session).await {SessionEvent::ToolRequested{request,call,..}=>(request,call),event=>panic!("expected tool: {event:?}")};
        InteractiveServer::answer_function(&mut session,request,&call).await;
        assert_eq!(session.split().0.submit(InteractiveServer::text("overlapping continuation")).await.unwrap_err().kind(), ErrorKind::InvalidState);
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::ResponseFinished{outcome:ResponseOutcome::Completed,..}));
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::ResponseFinished{outcome:ResponseOutcome::Completed,..}));
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::Transcription{source:providers_kit::session::TranscriptionSource::Input{command:Some(actual)},text,..} if actual==capture && text=="caller"));
        InteractiveServer::send_input(&mut session,InteractiveServer::text("cancel-call")).await;
        // Known cancelled calls must never become executable.
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::ResponseFinished{outcome:ResponseOutcome::Cancelled,..}));
        let command=session.split().0.submit(InteractiveServer::text("cancel")).await.unwrap();
        session.split().0.submit(SessionCommand::Cancel(CancelTarget::Command(command))).await.unwrap();
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::ResponseFinished{outcome:ResponseOutcome::Cancelled,..}));
        assert!(matches!(InteractiveServer::next_event(&mut session).await,SessionEvent::Cancellation{resolution:CancellationResolution::Stopped,..}));
        assert!(session.split().0.submit(InteractiveServer::audio("audio/pcm;rate=24000",vec![0;6*1024*1024])).await.is_err());
        server.finish_session(session,provider).await;
    }).await.expect("OpenAI lifecycle remains bounded");
}

#[tokio::test]
async fn gemini_non_live_model_realtime_is_unsupported_before_connect() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::Gemini(
            GeminiConfig::new(ApiKey::new("fixture-key").unwrap()).with_base_url(
                Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap(),
            ),
        ),
        TransportConfig::default(),
    ))
    .expect("provider should construct");
    let models = ["models/gemini-3.8-flash"]
        .into_iter()
        .map(|id| {
            provider
                .model(ModelId::new(id).unwrap())
                .expect("model should select")
        })
        .collect::<Vec<_>>();
    let checked = std::panic::AssertUnwindSafe(async {
        for model in &models {
            for config in [
                SessionConfig::new(SessionProfile::Realtime),
                InteractiveServer::manual_config("audio/pcm;rate=16000"),
                InteractiveServer::manual_config("audio/pcm;rate=16000")
                    .with_turn_detection(SessionTurnDetection::Automatic),
            ] {
                let error =
                    match tokio::time::timeout(Duration::from_secs(1), model.open_session(config))
                        .await
                        .expect("unsupported session should fail without waiting for a server")
                    {
                        Err(error) => error,
                        Ok(session) => {
                            session
                                .close()
                                .await
                                .expect("unexpected session should close");
                            panic!("Gemini non-Live model must reject Realtime");
                        }
                    };
                assert_eq!(error.kind(), ErrorKind::Unsupported);
                assert_eq!(error.context().operation, Operation::OpenSession);
                assert_eq!(error.context().phase, ErrorPhase::InputValidation);
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err(),
            "unsupported sessions must not connect or send HTTP"
        );
        provider.shutdown().await.expect("provider should close");
        for model in &models {
            let error = match tokio::time::timeout(
                Duration::from_secs(1),
                model.open_session(SessionConfig::new(SessionProfile::Realtime)),
            )
            .await
            .expect("closed admission should fail immediately")
            {
                Err(error) => error,
                Ok(session) => {
                    session
                        .close()
                        .await
                        .expect("unexpected sealed session should close");
                    panic!("sealed provider must reject session admission");
                }
            };
            assert_eq!(error.kind(), ErrorKind::Closed);
            assert_eq!(error.context().operation, Operation::OpenSession);
        }
    })
    .catch_unwind()
    .await;
    provider
        .shutdown()
        .await
        .expect("provider cleanup should settle");
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn openai_playback_requires_an_observed_segment_before_acceptance() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::start(|mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"session.updated","session":{"type":"realtime","model":"gpt-realtime-mini","output_modalities":["audio"]}})).await;
            // An invalid playback command must not reach this actual transport boundary.
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.openai_model();
        let mut session=model.open_session(SessionConfig::new(SessionProfile::Realtime).with_output_audio("audio/pcm;rate=24000",true).unwrap()).await.unwrap();
        let unknown=providers_kit::session::OutputSegmentId::new(session.id(),std::num::NonZeroU64::new(999).unwrap()).unwrap();
        let submitted=session.split().0.submit(SessionCommand::ReconcilePlayback{segment:unknown,played:Duration::from_millis(5)}).await;
        let cleanup=session.close().await;
        provider.shutdown().await.unwrap();
        server.finish().await;
        cleanup.unwrap();
        assert!(submitted.is_err(),"unknown segment cannot acquire local acceptance: {submitted:?}");
    }).await.expect("playback preflight is bounded");
}

#[tokio::test]
async fn openai_unattributed_response_does_not_steal_pending_command() {
    let server=InteractiveServer::start(|mut socket|async move {
        InteractiveServer::receive_setup(&mut socket).await;
        InteractiveServer::acknowledge_setup(&mut socket).await;
        InteractiveServer::expect_type(&mut socket,"conversation.item.create").await;
        InteractiveServer::expect_type(&mut socket,"response.create").await;
        InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.created","response":{"id":"vad-response","status":"in_progress"}})).await;
        InteractiveServer::expect_close(&mut socket).await;
    }).await;
    let (provider, model) = server.openai_model();
    let mut session = model
        .open_session(SessionConfig::new(SessionProfile::Realtime))
        .await
        .unwrap();
    session
        .split()
        .0
        .submit(InteractiveServer::text("pending explicit request"))
        .await
        .unwrap();
    let observed = session.split().1.recv().await;
    server.finish_session(session, provider).await;
    assert!(
        matches!(
            observed,
            Some(SessionEvent::ResponseStarted { command: None, .. })
        ),
        "missing native attribution cannot consume an unrelated pending command: {observed:?}"
    );
}

#[tokio::test]
async fn openai_repeated_cancellation_settles_every_accepted_receipt() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let server=InteractiveServer::start(|mut socket|async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_type(&mut socket,"conversation.item.create").await;
            InteractiveServer::start_response(&mut socket,"response-repeated-cancel").await;
            for _ in 0..2 {InteractiveServer::expect_type(&mut socket,"response.cancel").await;}
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"response-repeated-cancel","status":"cancelled"}})).await;
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.openai_model();
        let mut session=model.open_session(SessionConfig::new(SessionProfile::Realtime)).await.unwrap();
        session.split().0.submit(InteractiveServer::text("cancel twice")).await.unwrap();
        let response=match session.split().1.recv().await.unwrap(){SessionEvent::ResponseStarted{response,..}=>response,event=>panic!("{event:?}")};
        let first=session.split().0.submit(SessionCommand::Cancel(CancelTarget::Response(response.clone()))).await.unwrap();
        let second=session.split().0.submit(SessionCommand::Cancel(CancelTarget::Response(response))).await.unwrap();
        assert!(matches!(session.split().1.recv().await,Some(SessionEvent::ResponseFinished{outcome:ResponseOutcome::Cancelled,..})));
        let observed=session.split().1.recv().await.unwrap();
        let first_settled=matches!(observed,SessionEvent::Cancellation{command,resolution:CancellationResolution::Stopped,..} if command==first);
        if first_settled {assert!(matches!(session.split().1.recv().await,Some(SessionEvent::Cancellation{command,resolution:CancellationResolution::Stopped,..}) if command==second));}
        server.finish_session(session,provider).await;
        assert!(first_settled,"later cancellation must not overwrite the first accepted receipt");
    }).await.expect("repeat cancellation is bounded");
}

#[tokio::test]
async fn openai_repeated_input_discard_preserves_each_acknowledgement() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let server=InteractiveServer::start(|mut socket|async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            for _ in 0..2 {InteractiveServer::expect_type(&mut socket,"input_audio_buffer.clear").await;}
            for _ in 0..2 {InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"input_audio_buffer.cleared"})).await;}
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.openai_model();
        let mut session=model.open_session(SessionConfig::new(SessionProfile::Realtime).with_input_audio("audio/pcm;rate=24000",false).unwrap()).await.unwrap();
        let first=session.split().0.submit(SessionCommand::DiscardInput).await.unwrap();
        let second=session.split().0.submit(SessionCommand::DiscardInput).await.unwrap();
        let observed=session.split().1.recv().await.unwrap();
        let first_settled=matches!(observed,SessionEvent::ControlApplied{command} if command==first);
        if first_settled {assert!(matches!(session.split().1.recv().await,Some(SessionEvent::ControlApplied{command}) if command==second));}
        server.finish_session(session,provider).await;
        assert!(first_settled,"a later input discard must not overwrite earlier accepted work");
    }).await.expect("repeat discard is bounded");
}

#[tokio::test]
async fn openai_completed_response_winning_cancellation_keeps_tools_valid() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let (runtime, seal) = oneshot::channel::<Provider>();
        let server=InteractiveServer::start(|mut socket|async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_type(&mut socket,"conversation.item.create").await;
            InteractiveServer::start_response(&mut socket,"race-completed").await;
            InteractiveServer::expect_type(&mut socket,"response.cancel").await;
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.function_call_arguments.done","response_id":"race-completed","item_id":"race-tool","call_id":"race-call","name":"lookup","arguments":"{\"key\":\"race\"}"})).await;
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"race-completed","status":"completed"}})).await;
            InteractiveServer::expect_type(&mut socket,"input_audio_buffer.append").await;
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.created","response":{"id":"vad","status":"in_progress"}})).await;
            assert_eq!(InteractiveServer::receive_json(&mut socket).await["item"]["type"], "function_call_output");
            assert!(seal.await.unwrap().shutdown().now_or_never().is_none());
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"vad","status":"completed"}})).await;
            InteractiveServer::start_response(&mut socket,"continuation").await;
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"continuation","status":"completed"}})).await;
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.openai_model();
        assert!(runtime.send(provider.clone()).is_ok());
        let mut session=model.open_session(InteractiveServer::function_config(vec![InteractiveServer::lookup_function()],None).unwrap().with_input_audio("audio/pcm;rate=24000",false).unwrap()).await.unwrap();
        session.split().0.submit(InteractiveServer::text("cancel race")).await.unwrap();
        let response=match session.split().1.recv().await.unwrap(){SessionEvent::ResponseStarted{response,..}=>response,event=>panic!("{event:?}")};
        session.split().0.submit(SessionCommand::Cancel(CancelTarget::Response(response))).await.unwrap();
        let Some(SessionEvent::ToolRequested {request, call, ..}) = session.split().1.recv().await else { panic!("completed tool must survive cancellation") };
        InteractiveServer::send_input(&mut session, InteractiveServer::audio("audio/pcm;rate=24000", vec![1,2])).await;
        while !matches!(session.split().1.recv().await.unwrap(), SessionEvent::ResponseStarted { command: None, .. }) {}
        let result = InteractiveServer::answer_function(&mut session, request, &call).await;
        assert!(matches!(InteractiveServer::next_event(&mut session).await, SessionEvent::ResponseFinished {outcome:ResponseOutcome::Completed,..}));
        assert!(matches!(session.split().1.recv().await,Some(SessionEvent::ResponseStarted{command:Some(id),..}) if id==result));
        assert!(matches!(InteractiveServer::next_event(&mut session).await, SessionEvent::ResponseFinished {outcome:ResponseOutcome::Completed,..}));
        server.finish_session(session,provider).await;
    }).await.expect("cancel/completion race is bounded");
}

#[tokio::test]
async fn openai_automatic_audio_admission_survives_seal_before_response_created() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let (appended,received)=oneshot::channel();
        let (release,resume)=oneshot::channel();
        let server=InteractiveServer::start(|mut socket|async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_type(&mut socket,"input_audio_buffer.append").await;
            appended.send(()).unwrap();
            resume.await.unwrap();
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.created","response":{"id":"audio-after-seal","status":"in_progress"}})).await;
            InteractiveServer::send_json(&mut socket,serde_json::json!({"type":"response.done","response":{"id":"audio-after-seal","status":"completed"}})).await;
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.openai_model();
        let mut session=model.open_session(SessionConfig::new(SessionProfile::Realtime).with_input_audio("audio/pcm;rate=24000",false).unwrap()).await.unwrap();
        session.split().0.submit(InteractiveServer::audio("audio/pcm;rate=24000",vec![1,2,3,4])).await.unwrap();
        received.await.unwrap();
        assert!(matches!(session.split().1.recv().await,Some(SessionEvent::ControlApplied{..})));
        let mut shutdown=Box::pin(provider.shutdown());
        assert!(poll_fn(|context|Poll::Ready(shutdown.as_mut().poll(context))).await.is_pending());
        release.send(()).unwrap();
        let observation=async {
            let mut complete=false;
            while let Some(event)=session.split().1.recv().await {
                if matches!(event,SessionEvent::ResponseFinished{outcome:ResponseOutcome::Completed,..}){complete=true;}
            }
            complete
        };
        let (shutdown,complete)=tokio::join!(shutdown,observation);
        shutdown.unwrap();session.close().await.unwrap();server.finish().await;
        assert!(complete,"native automatic generation admitted by audio must survive seal before response.created");
    }).await.expect("automatic-audio seal race is bounded");
}

#[tokio::test]
async fn openai_unacknowledged_control_has_a_bounded_timeout() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let server=InteractiveServer::start(|mut socket|async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_type(&mut socket,"input_audio_buffer.clear").await;
            // Native control never acknowledges; the owner must bound this wait.
            assert!(socket.next().await.unwrap().unwrap().is_close());
            socket.get_mut().write_all(&[0xff, 0]).await.unwrap(); // Invalid peer close reply.
        }).await;
        let timeout=Duration::from_millis(200);
        let provider=Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("fixture-key").unwrap()).with_base_url(server.base_url.clone())),TransportConfig::new(timeout,timeout,timeout,providers_kit::config::RetryPolicy::default()).unwrap())).unwrap();
        let model=provider.model(ModelId::new("gpt-realtime-mini").unwrap()).unwrap();
        let mut session=model.open_session(SessionConfig::new(SessionProfile::Realtime).with_input_audio("audio/pcm;rate=24000",false).unwrap()).await.unwrap();
        let command=session.split().0.submit(SessionCommand::DiscardInput).await.unwrap();
        let observed=tokio::time::timeout(Duration::from_secs(1),session.split().1.recv()).await;
        let cleanup = session.close().await.unwrap_err();
        server.finish().await;
        assert_eq!(cleanup.context().operation, Operation::SessionClose);
        let mut held = Vec::new();
        for _ in 0..1024 {
            held.push(model.open_session(SessionConfig::new(SessionProfile::Conversation)).await.unwrap());
        }
        let shutdown = provider.shutdown().await.unwrap_err();
        assert_eq!((shutdown.context().operation, shutdown.context().phase, shutdown.kind()), (Operation::Shutdown, cleanup.context().phase, cleanup.kind()));
        assert!(matches!(observed,Ok(Some(SessionEvent::Terminated{reason,unfinished_commands,..})) if reason.kind()==ErrorKind::Timeout && unfinished_commands.contains(&command)),"unacknowledged accepted control must time out with its unfinished receipt");
    }).await.expect("control acknowledgement proof is bounded");
}

#[tokio::test]
async fn openai_sealing_settles_accepted_input_that_never_started() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let server=InteractiveServer::start(|mut socket|async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            // Current-thread caller seals immediately after local acceptance;
            // no conversation mutation may cross this transport boundary.
            InteractiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.openai_model();
        let mut session=model.open_session(SessionConfig::new(SessionProfile::Realtime)).await.unwrap();
        let accepted=session.split().0.submit(InteractiveServer::text("accepted before seal")).await.unwrap();
        let cancel=session.split().0.submit(SessionCommand::Cancel(CancelTarget::Command(accepted.clone()))).await.unwrap();
        provider.shutdown().await.unwrap();
        for expected in [accepted, cancel] {
            assert!(matches!(session.split().1.recv().await,Some(SessionEvent::CommandFailed{command,not_started:true,..}) if command==expected));
        }
        assert!(matches!(session.split().1.recv().await,Some(SessionEvent::Terminated{unfinished_commands,..}) if unfinished_commands.is_empty()));
        assert!(session.split().1.recv().await.is_none());
        session.close().await.unwrap();server.finish().await;
    }).await.expect("seal-before-start settlement is bounded");
}

#[tokio::test]
async fn openai_invalid_native_function_arguments_remain_response_errors() {
    let server=InteractiveServer::terminal_response(serde_json::json!({"type":"response.function_call_arguments.done","response_id":"response-fixture","call_id":"invalid-arguments","name":"lookup","arguments":"[]"})).await;
    let (provider, model) = server.openai_model();
    let mut session = model
        .open_session(
            InteractiveServer::function_config(vec![InteractiveServer::lookup_function()], None)
                .unwrap(),
        )
        .await
        .unwrap();
    session
        .split()
        .0
        .submit(InteractiveServer::text("invalid native call"))
        .await
        .unwrap();
    let error = InteractiveServer::terminal_error(&mut session).await;
    server.finish_session(session, provider).await;
    assert!(
        matches!(error.kind(), ErrorKind::InvalidResponse(_)),
        "native invalid argument shape is not caller InvalidInput: {error:?}"
    );
}
