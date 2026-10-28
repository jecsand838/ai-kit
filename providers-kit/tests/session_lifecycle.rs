use providers_kit::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use providers_kit::config::{ApiKey, GeminiConfig, OpenAiConfig, ProviderKind, TransportConfig};
use providers_kit::generation::{
    ContentPart, FunctionCall, FunctionResult, JsonSchema, ProviderFunctionTool, ToolChoice,
};
use providers_kit::models::ModelId;
use providers_kit::session::{
    SessionBackend, SessionConfig, SessionEvent, SessionInput, SessionTurnDetection,
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
            .open_session(SessionConfig::default())
            .await
            .expect("session opens");
        let error = session.recv().await.expect_err("peer ended session");
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
                .open_session(SessionConfig::default())
                .await
                .expect("session opens");
            let error = session.recv().await.expect_err("malformed frame fails");
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
            let mut session = model.open_session(SessionConfig::default()).await.unwrap();
            let error = session.recv().await.expect_err("malformed session payload fails");
            session.close().await.unwrap();
            provider.shutdown().await.unwrap();
            server.finish().await;
            assert_eq!(error.context().operation, Operation::SessionReceive);
            assert_eq!(error.context().phase, ErrorPhase::Decode);
            assert!(matches!(error.response_details(), Some(ResponseDetails::Json { category: actual, .. }) if *actual == category));
        }
    }).await.expect("session payload decoding proof is bounded");
}

struct InteractiveServer {
    base_url: Url,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl InteractiveServer {
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
                match model.open_session(SessionConfig::default()).await {
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
                    break;
                }
            }
        })
        .await
    }

    async fn openai() -> Self {
        Self::start(|mut socket| async move {
            let update = Self::receive_json(&mut socket).await;
            assert_eq!(update["type"], "session.update");
            assert_eq!(
                update["session"]["output_modalities"],
                serde_json::json!(["audio"])
            );
            assert!(
                update["session"]["audio"]["input"]
                    .get("transcription")
                    .is_some()
            );
            assert_eq!(
                update["session"]["audio"]["input"]["transcription"]["model"],
                "gpt-transcribe"
            );
            assert!(update["session"]["audio"]["input"]["turn_detection"].is_null());
            let function = Self::lookup_function();
            assert_eq!(
                update["session"]["tools"],
                serde_json::json!([{
                    "type": "function",
                    "name": function.name(),
                    "description": function.description(),
                    "parameters": function.input_schema().schema(),
                }]),
                "Realtime accepts its function fields without Responses-only strict",
            );
            Self::send_json(
                &mut socket,
                serde_json::json!({ "type": "session.created" }),
            )
            .await;
            Self::send_json(
                &mut socket,
                serde_json::json!({
                    "type": "session.updated",
                    "session": {
                        "type": "realtime",
                        "model": "gpt-realtime-mini",
                        "output_modalities": ["audio"],
                    }
                }),
            )
            .await;

            let first = Self::receive_json(&mut socket).await;
            assert_eq!(
                first,
                serde_json::json!({
                    "type": "conversation.item.create",
                    "item": {
                        "type": "message", "role": "user",
                        "content": [{"type": "input_text", "text": "first"}],
                    },
                })
            );
            assert_eq!(
                Self::receive_json(&mut socket).await,
                serde_json::json!({"type": "response.create"})
            );
            for payload in [
                serde_json::json!({
                    "type": "response.output_audio.delta",
                    "delta": "AQID",
                }),
                serde_json::json!({
                    "type": "response.output_audio_transcript.delta",
                    "delta": "hello",
                }),
                serde_json::json!({
                    "type": "response.usage.updated",
                    "usage": {
                        "input_tokens": 1,
                        "output_tokens": 2,
                        "total_tokens": 3,
                    },
                }),
                serde_json::json!({
                    "type": "response.done",
                    "response": { "status": "completed" },
                }),
            ] {
                Self::send_json(&mut socket, payload).await;
            }

            assert_eq!(
                Self::receive_json(&mut socket).await,
                serde_json::json!({
                    "type": "input_audio_buffer.append",
                    "audio": "BAUG",
                }),
            );
            assert_eq!(
                Self::receive_json(&mut socket).await,
                serde_json::json!({ "type": "input_audio_buffer.commit" }),
            );
            Self::expect_type(&mut socket, "response.create").await;
            for payload in [
                serde_json::json!({
                    "type": "response.function_call_arguments.done",
                    "call_id": "call_1",
                    "name": "lookup",
                    "arguments": serde_json::json!({ "key": "value" }).to_string(),
                }),
                serde_json::json!({
                    "type": "response.done",
                    "response": { "status": "completed" },
                }),
            ] {
                Self::send_json(&mut socket, payload).await;
            }
            let result = Self::receive_json(&mut socket).await;
            assert_eq!(result["type"], "conversation.item.create");
            assert_eq!(result["item"]["call_id"], "call_1");
            Self::expect_type(&mut socket, "response.create").await;
            Self::send_json(
                &mut socket,
                serde_json::json!({
                    "type": "response.done",
                    "response": { "status": "completed" },
                }),
            )
            .await;
            Self::send_json(
                &mut socket,
                serde_json::json!({
                    "type": "conversation.item.input_audio_transcription.delta",
                    "delta": "caller",
                }),
            )
            .await;

            Self::expect_type(&mut socket, "conversation.item.create").await;
            Self::expect_type(&mut socket, "response.create").await;
            for payload in [
                serde_json::json!({
                    "type": "response.function_call_arguments.done",
                    "call_id": "call_2",
                    "name": "lookup",
                    "arguments": "{}",
                }),
                serde_json::json!({
                    "type": "response.done",
                    "response": { "status": "cancelled" },
                }),
            ] {
                Self::send_json(&mut socket, payload).await;
            }

            Self::expect_type(&mut socket, "conversation.item.create").await;
            Self::expect_type(&mut socket, "response.create").await;
            assert_eq!(
                Self::receive_json(&mut socket).await,
                serde_json::json!({ "type": "response.cancel" }),
            );
            Self::send_json(
                &mut socket,
                serde_json::json!({
                    "type": "response.done",
                    "response": { "status": "cancelled" },
                }),
            )
            .await;

            while let Some(message) = socket.next().await {
                if message
                    .expect("OpenAI close frame should decode")
                    .is_close()
                {
                    break;
                }
            }
        })
        .await
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
        SessionConfig::default()
            .with_input_audio(input_audio, true)
            .expect("input audio config should validate")
            .with_output_audio("audio/pcm;rate=24000", true)
            .expect("output audio config should validate")
            .with_turn_detection(SessionTurnDetection::Manual)
            .with_functions(vec![Self::lookup_function()], None)
            .expect("function config should validate")
    }

    fn audio(media_type: &str, data: Vec<u8>) -> SessionInput {
        SessionInput::Audio {
            media_type: media_type.to_string(),
            data,
        }
    }

    fn text(text: &str) -> SessionInput {
        SessionInput::Text {
            text: text.to_string(),
        }
    }

    async fn answer_function(
        session: &mut Session,
        call: &FunctionCall,
        mismatched_content: ContentPart,
        result_content: ContentPart,
    ) {
        let mismatched = FunctionResult::new(
            call.id().map(str::to_string),
            "other",
            vec![mismatched_content],
            false,
        )
        .expect("mismatched function result should be structurally valid");
        assert!(
            session
                .send(SessionInput::FunctionResult(mismatched))
                .await
                .is_err(),
            "a function result must retain the originating function name",
        );
        Self::send_input(
            session,
            SessionInput::FunctionResult(
                FunctionResult::new(
                    call.id().map(str::to_owned),
                    call.name().to_owned(),
                    vec![result_content],
                    false,
                )
                .expect("function result should validate"),
            ),
        )
        .await;
    }

    async fn send_input(session: &mut Session, input: SessionInput) {
        session
            .send(input)
            .await
            .expect("valid interactive session input should send");
    }

    async fn expect_event(session: &mut Session, expected: SessionEvent) {
        assert_eq!(
            session
                .recv()
                .await
                .expect("interactive session event should arrive"),
            expected,
        );
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
            let error = match model.open_session(SessionConfig::default()).await {
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
        let (error, close) = match model.open_session(SessionConfig::default()).await {
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
        let (error, close) = match model.open_session(SessionConfig::default()).await {
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
        let error = match model.open_session(SessionConfig::default()).await {
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
    assert!(
        SessionConfig::default().with_instructions(" ").is_err(),
        "blank instructions must be rejected",
    );
    assert!(
        SessionConfig::default()
            .with_input_audio("text/plain", false)
            .is_err(),
        "non-audio input formats must be rejected",
    );
    assert!(
        SessionConfig::default()
            .with_input_audio("audio/not a type", false)
            .is_err(),
        "malformed audio media types must be rejected",
    );
    assert!(
        SessionConfig::default()
            .with_functions(Vec::new(), Some(ToolChoice::Required))
            .is_err(),
        "required function selection needs a declaration",
    );

    let function = InteractiveServer::lookup_function();
    assert!(
        SessionConfig::default()
            .with_functions(vec![function.clone(), function.clone()], None)
            .is_err(),
        "duplicate function declarations must be rejected",
    );
    let config = SessionConfig::default()
        .with_instructions("Be concise")
        .expect("instructions should validate")
        .with_input_audio("audio/pcm;rate=16000", true)
        .expect("input audio should validate")
        .with_output_audio("audio/pcm;rate=24000", true)
        .expect("output audio should validate")
        .with_turn_detection(SessionTurnDetection::Manual)
        .with_functions(
            vec![function],
            Some(ToolChoice::Tool {
                name: "lookup".to_string(),
            }),
        )
        .expect("function selection should validate");

    assert_eq!(config.instructions(), Some("Be concise"));
    assert_eq!(config.input_audio_format(), Some("audio/pcm;rate=16000"));
    assert_eq!(config.output_audio_format(), Some("audio/pcm;rate=24000"));
    assert!(config.input_transcription());
    assert!(config.output_transcription());
    assert_eq!(config.turn_detection(), SessionTurnDetection::Manual);
    assert_eq!(config.functions().len(), 1);
    assert!(matches!(config.tool_choice(), Some(ToolChoice::Tool { name }) if name == "lookup"));
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
        let mut opening = Box::pin(model.open_session(SessionConfig::default()));
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
async fn session_setup_finishes_after_sealing_but_only_close_remains_available() {
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
        let mut opening = Box::pin(model.open_session(SessionConfig::default()));
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
            session.send(InteractiveServer::text("sealed")).await,
            Err(ref error) if error.kind() == ErrorKind::Closed
        ));
        assert!(matches!(session.recv().await, Err(ref error) if error.kind() == ErrorKind::Closed));
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
async fn dropping_a_pending_session_recv_releases_shutdown_admission() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::start(move |mut socket| async move {
            InteractiveServer::receive_setup(&mut socket).await;
            InteractiveServer::acknowledge_setup(&mut socket).await;
            InteractiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.openai_model();
        let mut session = model
            .open_session(SessionConfig::default())
            .await
            .expect("interactive session should open");
        let mut receiving = Box::pin(session.recv());
        assert!(
            poll_fn(|context| Poll::Ready(receiving.as_mut().poll(context)))
                .await
                .is_pending(),
            "recv must be polled before its cancellation is tested",
        );
        let mut shutdown = Box::pin(provider.shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), shutdown.as_mut())
                .await
                .is_err(),
            "shutdown must drain the pending recv",
        );
        assert!(matches!(
            provider.model(model.model_id().clone()),
            Err(ref error) if error.kind() == ErrorKind::Closed
        ));
        drop(receiving);
        shutdown
            .await
            .expect("dropping pending recv must release shutdown admission");
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
            .open_session(SessionConfig::default())
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
        let config = SessionConfig::default()
            .with_input_audio(media_type, false)
            .expect("audio input should validate");
        let mut session = model
            .open_session(config)
            .await
            .expect("interactive session should open");

        assert!(matches!(
            session
                .send(InteractiveServer::audio(
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
                model.open_session(SessionConfig::default()).await.is_err(),
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
            (Some(ToolChoice::Tool { name: "lookup".to_string() }), serde_json::json!({"type": "function", "name": "lookup"}), vec!["lookup"]),
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
            let config = SessionConfig::default().with_functions(functions, choice).unwrap();
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
        let config = SessionConfig::default()
            .with_functions(
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
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = InteractiveServer::openai().await;
        let (provider, model) = server.openai_model();
        let mut session = model
            .open_session(InteractiveServer::manual_config("audio/pcm;rate=24000"))
            .await
            .expect("OpenAI session should open");

        InteractiveServer::send_input(&mut session, InteractiveServer::text("first")).await;
        InteractiveServer::expect_event(
            &mut session,
            SessionEvent::Audio {
                media_type: "audio/pcm;rate=24000".to_string(),
                data: vec![1, 2, 3],
            },
        )
        .await;
        InteractiveServer::expect_event(
            &mut session,
            SessionEvent::OutputTranscriptionDelta {
                delta: "hello".to_string(),
            },
        )
        .await;
        assert!(matches!(
            session.recv().await.expect("usage should arrive"),
            SessionEvent::Usage(usage)
                if usage.input_tokens == Some(1)
                    && usage.output_tokens == Some(2)
                    && usage.total_tokens == Some(3)
        ));
        InteractiveServer::expect_event(&mut session, SessionEvent::Completed).await;
        InteractiveServer::send_input(&mut session, SessionInput::TurnStart).await;
        InteractiveServer::send_input(
            &mut session,
            InteractiveServer::audio("audio/pcm;rate=24000", vec![4, 5, 6]),
        )
        .await;
        InteractiveServer::send_input(&mut session, SessionInput::TurnEnd).await;
        let call = match session.recv().await.expect("function call should arrive") {
            SessionEvent::FunctionCall(call) => call,
            other => panic!("expected function call, received {other:?}"),
        };
        InteractiveServer::answer_function(
            &mut session,
            &call,
            ContentPart::Text {
                text: "wrong".to_string(),
            },
            ContentPart::Text {
                text: "42".to_string(),
            },
        )
        .await;
        InteractiveServer::expect_event(&mut session, SessionEvent::Completed).await;
        InteractiveServer::expect_event(
            &mut session,
            SessionEvent::InputTranscriptionDelta {
                delta: "caller".to_string(),
            },
        )
        .await;

        InteractiveServer::send_input(&mut session, InteractiveServer::text("cancel-call")).await;
        assert!(matches!(
            session.recv().await.expect("function call should arrive"),
            SessionEvent::FunctionCall(call) if call.id() == Some("call_2")
        ));
        InteractiveServer::expect_event(
            &mut session,
            SessionEvent::FunctionCallsCancelled {
                call_ids: vec!["call_2".to_string()],
            },
        )
        .await;
        InteractiveServer::expect_event(&mut session, SessionEvent::TurnCancelled).await;

        InteractiveServer::send_input(&mut session, InteractiveServer::text("cancel")).await;
        InteractiveServer::send_input(&mut session, SessionInput::CancelTurn).await;
        InteractiveServer::expect_event(&mut session, SessionEvent::TurnCancelled).await;
        InteractiveServer::send_input(&mut session, SessionInput::TurnStart).await;
        assert!(matches!(
            session
                .send(InteractiveServer::audio(
                    "audio/pcm;rate=24000",
                    vec![0; 6 * 1_024 * 1_024],
                ))
                .await,
            Err(ref error) if error.kind() == ErrorKind::InvalidInput
        ));
        server.finish_session(session, provider).await;
    })
    .await
    .expect("OpenAI session lifecycle should be bounded");
}

#[tokio::test]
async fn gemini_sessions_are_unsupported_before_connect() {
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
    let models = [
        "models/gemini-3.8-live",
        "models/gemini-3.1-flash-live-preview",
        "models/gemini-3.8-flash",
    ]
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
                SessionConfig::default(),
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
                            panic!("Gemini sessions must remain unsupported");
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
                model.open_session(SessionConfig::default()),
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
