//! Gemini Live protocol fixtures are derived from the official v1beta reference:
//! https://ai.google.dev/api/live (read 2026-11-01), especially the server-message
//! union, tool call/result correlation, and independent transcription delivery.
//! These local wire tests prove mapping and lifecycle behavior, not live service parity.

use std::future::{Future, poll_fn};
use std::num::NonZeroU32;
use std::task::Poll;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use providers_kit::config::{ApiKey, GeminiConfig, ProviderKind, RetryPolicy, TransportConfig};
use providers_kit::error::ErrorKind;
use providers_kit::generation::{
    ContentPart, FunctionResult, GenerationConfig, JsonSchema, Message, ProviderFunctionTool,
    ProviderToolSpec,
};
use providers_kit::session::{
    CancelTarget, ResponseOutcome, SessionCommand, SessionCommands, SessionConfig,
    SessionContinuity, SessionEvent, SessionEvents, SessionProfile, SessionToolResult,
    SessionTurnDetection, TranscriptionSource,
};
use providers_kit::{
    Model, ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig, SessionBackend,
};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use url::Url;

struct GeminiLiveServer {
    base_url: Url,
    task: Option<JoinHandle<()>>,
}

struct GeminiUpgrade;

impl Callback for GeminiUpgrade {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        assert_eq!(
            request.uri().path(),
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
        );
        assert_eq!(request.headers()["x-goog-api-key"], "fixture-key");
        assert!(
            request.uri().query().is_none(),
            "credentials stay out of URL"
        );
        Ok(response)
    }
}

impl GeminiLiveServer {
    async fn start<F, Fut>(handler: F) -> Self
    where
        F: FnOnce(WebSocketStream<TcpStream>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fixture binds");
        let address = listener.local_addr().expect("fixture address");
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("Live connection arrives");
            let socket = tokio_tungstenite::accept_hdr_async(stream, GeminiUpgrade)
                .await
                .expect("native Live WebSocket upgrades");
            handler(socket).await;
        });
        Self {
            base_url: Url::parse(&format!("http://{address}/")).expect("fixture URL"),
            task: Some(task),
        }
    }

    fn model(&self) -> (Provider, Model) {
        self.model_with_timeout(Duration::from_secs(3))
    }

    fn model_with_timeout(&self, timeout: Duration) -> (Provider, Model) {
        let provider = Provider::from_config(ProviderConfig::new(
            ProviderKind::Gemini(
                GeminiConfig::new(ApiKey::new("fixture-key").expect("key"))
                    .with_base_url(self.base_url.clone()),
            ),
            TransportConfig::new(
                timeout,
                Duration::from_secs(2),
                timeout,
                RetryPolicy::default(),
            )
            .expect("bounded transport"),
        ))
        .expect("provider");
        let model = provider
            .model(ModelId::new("models/gemini-3.8-live").expect("exact model"))
            .expect("model selection");
        (provider, model)
    }

    fn config() -> SessionConfig {
        SessionConfig::new(SessionProfile::Realtime)
            .with_instructions("Use lookup for requested records")
            .expect("instructions")
            .with_generation(
                GenerationConfig::default()
                    .with_temperature(0.2)
                    .expect("temperature")
                    .with_max_output_tokens(NonZeroU32::new(512).expect("nonzero")),
            )
            .expect("generation")
            .with_tools(vec![ProviderToolSpec::Function(
                ProviderFunctionTool::new(
                    "lookup",
                    Some("Read a record".into()),
                    JsonSchema::new(json!({
                        "type": "object",
                        "properties": {"key": {"type": "string"}},
                        "required": ["key"]
                    }))
                    .expect("schema"),
                )
                .expect("function"),
            )])
            .expect("tools")
            .with_input_audio("audio/pcm;rate=16000", true)
            .expect("input format")
            .with_output_audio("audio/pcm;rate=24000", true)
            .expect("output format")
    }

    async fn setup(socket: &mut WebSocketStream<TcpStream>) {
        let message = Self::receive(socket).await;
        assert_eq!(message.as_object().expect("envelope").len(), 1);
        let setup = &message["setup"];
        assert_eq!(setup["model"], "models/gemini-3.8-live");
        assert_eq!(
            setup["generationConfig"]["responseModalities"],
            json!(["AUDIO"])
        );
        assert_eq!(setup["generationConfig"]["maxOutputTokens"], 512);
        assert!(
            (setup["generationConfig"]["temperature"]
                .as_f64()
                .expect("temperature")
                - 0.2)
                .abs()
                < 0.00001
        );
        assert_eq!(
            setup["systemInstruction"]["parts"][0]["text"],
            "Use lookup for requested records"
        );
        assert_eq!(
            setup["tools"][0]["functionDeclarations"][0]["name"],
            "lookup"
        );
        assert!(setup.get("inputAudioTranscription").is_some());
        assert!(setup.get("outputAudioTranscription").is_some());
        assert!(setup.get("sessionResumption").is_none());
        assert!(setup.get("contextWindowCompression").is_none());
        Self::send(socket, json!({"setupComplete": {}})).await;
    }

    async fn receive(socket: &mut WebSocketStream<TcpStream>) -> Value {
        let message = socket.next().await.expect("frame").expect("valid frame");
        match message {
            WebSocketMessage::Text(text) => serde_json::from_str(text.as_ref()).expect("JSON"),
            other => panic!("expected native JSON, got {other:?}"),
        }
    }

    async fn send(socket: &mut WebSocketStream<TcpStream>, value: Value) {
        socket
            .send(WebSocketMessage::Text(value.to_string().into()))
            .await
            .expect("native frame writes");
    }

    async fn input(socket: &mut WebSocketStream<TcpStream>, expected: &str) {
        let message = Self::receive(socket).await;
        assert_eq!(
            message,
            json!({"clientContent": {
                "turns": [{"role": "user", "parts": [{"text": expected}]}],
                "turnComplete": true
            }})
        );
    }

    async fn expect_close(socket: &mut WebSocketStream<TcpStream>) {
        loop {
            match socket.next().await {
                Some(Ok(WebSocketMessage::Close(_))) => {
                    socket.flush().await.expect("close acknowledgement");
                    return;
                }
                Some(Ok(WebSocketMessage::Pong(_))) => {}
                other => panic!("expected consuming close, got {other:?}"),
            }
        }
    }

    async fn finish(mut self) {
        self.task
            .take()
            .expect("one join owner")
            .await
            .expect("fixture joined");
    }

    async fn completion(events: &mut impl SessionEvents) -> providers_kit::session::ResponseId {
        loop {
            match events.recv().await.expect("session remains open") {
                SessionEvent::ResponseFinished {
                    response,
                    outcome: ResponseOutcome::Completed,
                    continuity: SessionContinuity::Reusable,
                    output: None,
                } => return response,
                SessionEvent::ResponseStarted { .. }
                | SessionEvent::ControlApplied { .. }
                | SessionEvent::Usage { .. } => {}
                other => panic!("expected authoritative native completion, got {other:?}"),
            }
        }
    }

    async fn invalid_native_call(call: Value, validation_cause: bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = Self::start(move |mut socket| async move {
                Self::setup(&mut socket).await;
                Self::input(&mut socket, "invalid native call").await;
                Self::send(&mut socket, json!({"toolCall":{"functionCalls":[call]}})).await;
                Self::expect_close(&mut socket).await;
            })
            .await;
            let (provider, model) = server.model();
            let mut session = model.open_session(Self::config()).await.unwrap();
            {
                let (mut commands, mut events) = session.split();
                commands
                    .submit(SessionCommand::Input(
                        Message::user_text("invalid native call").unwrap(),
                    ))
                    .await
                    .unwrap();
                loop {
                    match events.recv().await.expect("malformed native call terminal") {
                        SessionEvent::ResponseStarted { .. }
                        | SessionEvent::ControlApplied { .. } => {}
                        SessionEvent::Terminated { reason, .. } => {
                            assert!(
                                matches!(reason.kind(), ErrorKind::InvalidResponse(_)),
                                "provider payload cannot be classified as caller input: {reason:?}"
                            );
                            assert_eq!(
                                reason.context().operation,
                                providers_kit::error::Operation::SessionReceive
                            );
                            if validation_cause {
                                assert_eq!(
                                    reason
                                        .cause()
                                        .expect("original validation diagnostic")
                                        .kind(),
                                    ErrorKind::InvalidInput
                                );
                            }
                            break;
                        }
                        unexpected => panic!("malformed native call escaped: {unexpected:?}"),
                    }
                }
            }
            session.close().await.unwrap();
            provider.shutdown().await.unwrap();
            server.finish().await;
        })
        .await
        .expect("malformed native call bounded");
    }

    async fn auxiliary_frames(frames: Vec<Value>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = Self::start(move |mut socket| async move {
                Self::setup(&mut socket).await;
                for frame in frames {
                    Self::send(&mut socket, frame).await;
                }
                Self::send(&mut socket, json!({"usageMetadata":{"promptTokenCount":7}})).await;
                Self::input(&mut socket, "after auxiliary metadata").await;
                Self::send(
                    &mut socket,
                    json!({"serverContent":{"generationComplete":true}}),
                )
                .await;
                Self::expect_close(&mut socket).await;
            })
            .await;
            let (provider, model) = server.model();
            let mut session = model.open_session(Self::config()).await.unwrap();
            {
                let (mut commands, mut events) = session.split();
                loop {
                    match events
                        .recv()
                        .await
                        .expect("auxiliary metadata is not terminal")
                    {
                        SessionEvent::Usage { usage, .. } if usage.input_tokens == Some(7) => break,
                        SessionEvent::Usage { .. } => {}
                        unexpected => {
                            panic!("auxiliary metadata invented response/activity: {unexpected:?}")
                        }
                    }
                }
                let mut pending = Box::pin(events.recv());
                poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                let receipt = commands
                    .submit(SessionCommand::Input(
                        Message::user_text("after auxiliary metadata").unwrap(),
                    ))
                    .await
                    .unwrap();
                let Some(SessionEvent::ResponseStarted {
                    command: Some(command),
                    response,
                }) = pending.await
                else {
                    panic!("only accepted input starts a response")
                };
                assert_eq!(command, receipt);
                assert_eq!(Self::completion(&mut events).await, response);
            }
            session.close().await.unwrap();
            provider.shutdown().await.unwrap();
            server.finish().await;
        })
        .await
        .expect("auxiliary metadata bounded");
    }
}

#[tokio::test]
async fn live_array_function_arguments_are_invalid_native_response() {
    GeminiLiveServer::invalid_native_call(
        json!({"id":"native-invalid-args","name":"lookup","args":[]}),
        false,
    )
    .await;
}

#[tokio::test]
async fn live_interruption_after_generation_completion_preserves_both_native_boundaries() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "generation before playback interruption").await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent":{"generationComplete":true}}),
            )
            .await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"interrupted":true}}))
                .await;
            GeminiLiveServer::input(&mut socket, "next input").await;
            // The old turn boundary can arrive after the next command is accepted.
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"turnComplete":true}}))
                .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent":{"generationComplete":true}}),
            )
            .await;
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("generation before playback interruption").unwrap(),
                ))
                .await
                .unwrap();
            let completed = GeminiLiveServer::completion(&mut events).await;
            let Some(SessionEvent::Interruption {
                response: Some(interrupted),
            }) = events.recv().await
            else {
                panic!("native playback interruption must remain observable")
            };
            assert_eq!(interrupted, completed);
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("next input").unwrap(),
                ))
                .await
                .unwrap();
            loop {
                match events.recv().await.expect("late original turn boundary") {
                    SessionEvent::ResponseStarted { response, .. } => {
                        assert_ne!(response, completed)
                    }
                    SessionEvent::ControlApplied { .. } => {}
                    SessionEvent::TurnEnded {
                        response: Some(ended),
                    } => {
                        assert_eq!(ended, completed);
                        break;
                    }
                    unexpected => {
                        panic!("late boundary changed response ownership: {unexpected:?}")
                    }
                }
            }
            assert_ne!(GeminiLiveServer::completion(&mut events).await, completed);
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    })
    .await
    .expect("independent native boundaries bounded");
}

#[tokio::test]
async fn live_unattributed_native_activity_does_not_invent_a_response() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"interrupted":true}}))
                .await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"turnComplete":true}}))
                .await;
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .unwrap();
        {
            let (_commands, mut events) = session.split();
            assert!(matches!(
                events.recv().await,
                Some(SessionEvent::Interruption { response: None })
            ));
            assert!(matches!(
                events.recv().await,
                Some(SessionEvent::TurnEnded { response: None })
            ));
            {
                let mut idle = Box::pin(events.recv());
                poll_fn(|cx| {
                    assert!(idle.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    })
    .await
    .expect("unattributed activity bounded");
}

#[tokio::test]
async fn live_invalid_native_function_identity_preserves_validation_cause() {
    GeminiLiveServer::invalid_native_call(
        json!({"id":" padded-native-id","name":"lookup","args":{}}),
        true,
    )
    .await;
}

impl Drop for GeminiLiveServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test]
async fn live_undeclared_tool_is_never_published_as_executable() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "undeclared tool").await;
            GeminiLiveServer::send(&mut socket, json!({"toolCall":{"functionCalls":[{"id":"unknown-native","name":"not_declared","args":{}}]}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.model();
        let mut session = model.open_session(GeminiLiveServer::config()).await.unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands.submit(SessionCommand::Input(Message::user_text("undeclared tool").unwrap())).await.unwrap();
            loop {
                match events.recv().await.expect("protocol terminal") {
                    SessionEvent::ResponseStarted { .. } | SessionEvent::ControlApplied { .. } => {},
                    SessionEvent::Terminated { reason, .. } => { assert!(matches!(reason.kind(), ErrorKind::InvalidResponse(_))); break; },
                    other => panic!("undeclared tool escaped validation: {other:?}"),
                }
            }
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    }).await.expect("undeclared native tool bounded");
}

#[tokio::test]
async fn live_sealing_preserves_admitted_audio_before_native_response_starts() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (captured, capture_observed) = oneshot::channel();
        let (respond, response_allowed) = oneshot::channel();
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            let audio = GeminiLiveServer::receive(&mut socket).await;
            assert_eq!(audio["realtimeInput"]["audio"]["data"], "AQIDBA==");
            captured.send(()).unwrap();
            response_allowed.await.unwrap();
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AQIDBA=="}}]}}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"generationComplete":true}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"turnComplete":true}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.model();
        let mut session = model.open_session(GeminiLiveServer::config()).await.unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands.submit(SessionCommand::Audio {media_type:"audio/pcm;rate=16000".into(),data:vec![1,2,3,4]}).await.unwrap();
            capture_observed.await.unwrap();
            let shutdown = provider.shutdown();
            tokio::pin!(shutdown);
            poll_fn(|cx| { assert!(shutdown.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
            assert_eq!(provider.model(ModelId::new("models/gemini-3.8-live").unwrap()).err().unwrap().kind(), ErrorKind::Closed);
            respond.send(()).unwrap();
            let mut audio = false;
            loop {
                match events.recv().await.expect("admitted native output survives sealing") {
                    SessionEvent::Output { content:ContentPart::Audio { .. }, .. } => audio = true,
                    SessionEvent::ResponseFinished { outcome:ResponseOutcome::Completed, .. } => break,
                    SessionEvent::ResponseStarted { command:None, .. } | SessionEvent::ControlApplied { .. } => {},
                    unexpected => panic!("sealed provider lost admitted native outcome: {unexpected:?}"),
                }
            }
            assert!(audio);
            shutdown.await.unwrap();
        }
        session.close().await.unwrap();
        server.finish().await;
    }).await.expect("admitted audio sealing bounded");
}

#[tokio::test]
async fn live_sealing_after_acceptance_reports_unstarted_work_explicitly() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .unwrap();
        {
            let (mut commands, mut events) = session.split();
            let receipt = commands
                .submit(SessionCommand::Input(
                    Message::user_text("must not start").unwrap(),
                ))
                .await
                .unwrap();
            let end = commands.submit(SessionCommand::EndAudioStream).await.unwrap();
            let shutdown = provider.shutdown();
            tokio::pin!(shutdown);
            poll_fn(|cx| {
                assert!(shutdown.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            for expected in [receipt, end] {
                assert!(matches!(events.recv().await, Some(SessionEvent::CommandFailed { command, not_started: true, .. }) if command == expected));
            }
            assert!(matches!(events.recv().await, Some(SessionEvent::Terminated { unfinished_commands, .. }) if unfinished_commands.is_empty()));
            assert!(events.recv().await.is_none());
            shutdown.await.unwrap();
        }
        session.close().await.unwrap();
        server.finish().await;
    })
    .await
    .expect("unstarted settlement bounded");
}

#[tokio::test]
async fn live_automatic_output_transcription_can_precede_audio_parts() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            let _ = GeminiLiveServer::receive(&mut socket).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"outputTranscription":{"text":"first transcription"}}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AQIDBA=="}}]}}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"generationComplete":true}})).await;
            // Deliberately invalid: all output transcription must precede generationComplete.
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"outputTranscription":{"finished":true}}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.model();
        let mut session = model.open_session(GeminiLiveServer::config()).await.unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands.submit(SessionCommand::Audio {media_type:"audio/pcm;rate=16000".into(),data:vec![1,2,3,4]}).await.unwrap();
            let mut transcription = None;
            loop {
                match events.recv().await.expect("automatic observations") {
                    SessionEvent::Transcription {source:TranscriptionSource::Output {response,..},text,..} => { assert_eq!(text,"first transcription"); transcription=Some(response); },
                    SessionEvent::Output { response, .. } => assert_eq!(transcription.as_ref(),Some(&response)),
                    SessionEvent::ResponseFinished {response,outcome:ResponseOutcome::Completed,..} => { assert_eq!(transcription.as_ref(),Some(&response));break; },
                    SessionEvent::ResponseStarted {command:None,..} | SessionEvent::ControlApplied {..} => {},
                    unexpected => panic!("independent output transcription must start its generation: {unexpected:?}"),
                }
            }
            assert!(matches!(events.recv().await, Some(SessionEvent::Terminated { reason, .. }) if matches!(reason.kind(), ErrorKind::InvalidResponse(_))));
            assert!(events.recv().await.is_none());
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    }).await.expect("automatic transcription bounded");
}

#[tokio::test]
async fn live_oversized_audio_is_rejected_before_receipt_or_wire_write() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            // The next native frame must be close; no oversized accepted Audio is sent.
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .unwrap();
        {
            let (mut commands, _events) = session.split();
            let error = commands
                .submit(SessionCommand::Audio {
                    media_type: "audio/pcm;rate=16000".into(),
                    data: vec![0; 6 * 1024 * 1024],
                })
                .await
                .expect_err("frame size is known before acceptance");
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    })
    .await
    .expect("oversized audio bounded");
}

#[tokio::test]
async fn live_duplicate_generation_terminal_is_a_protocol_failure() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "one response").await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent":{"generationComplete":true}}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent":{"generationComplete":true}}),
            )
            .await;
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("one response").unwrap(),
                ))
                .await
                .unwrap();
            GeminiLiveServer::completion(&mut events).await;
            let Some(SessionEvent::Terminated { reason, .. }) = events.recv().await else {
                panic!("duplicate terminal must end protocol")
            };
            assert!(matches!(reason.kind(), ErrorKind::InvalidResponse(_)));
            assert!(events.recv().await.is_none());
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    })
    .await
    .expect("duplicate terminal bounded");
}

#[tokio::test]
async fn live_late_model_content_cannot_create_a_successful_unrequested_response() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "one response").await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent":{"generationComplete":true}}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent":{"modelTurn":{"parts":[{"text":"late output"}]}}}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent":{"generationComplete":true}}),
            )
            .await;
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("one response").unwrap(),
                ))
                .await
                .unwrap();
            GeminiLiveServer::completion(&mut events).await;
            let Some(SessionEvent::Terminated { reason, .. }) = events.recv().await else {
                panic!("late content must not invent another response")
            };
            assert!(matches!(reason.kind(), ErrorKind::InvalidResponse(_)));
            assert!(events.recv().await.is_none());
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    })
    .await
    .expect("late content bounded");
}

#[tokio::test]
async fn live_tool_result_progresses_before_completion_and_late_input_transcription_is_unattributed()
 {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "first").await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"toolCall": {"functionCalls": [{
                    "id": "native-call-1", "name": "lookup", "args": {"key": "first"}
                }]}}),
            )
            .await;
            let result = GeminiLiveServer::receive(&mut socket).await;
            assert_eq!(
                result,
                json!({"toolResponse": {"functionResponses": [{
                    "id": "native-call-1", "name": "lookup", "response": {"value": 42}
                }]}})
            );
            // Each server message has one union member; ordinary observations can coexist
            // within serverContent. This is not an invented OpenAI-style terminal envelope.
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {
                    "modelTurn": {"role": "model", "parts": [{"inlineData": {
                        "mimeType": "audio/pcm;rate=24000", "data": "AQIDBA=="
                    }}]}, "outputTranscription": {"text": "forty two"}
                }}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"generationComplete": true}}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"turnComplete": true, "interactionStatus": "IDLE"}}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"inputTranscription": {"text": "late first input"}}}),
            )
            .await;
            GeminiLiveServer::input(&mut socket, "second").await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"generationComplete": true}}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"turnComplete": true, "interactionStatus": "IDLE"}}),
            )
            .await;
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .expect("Live opens");
        {
            let (mut commands, mut events) = session.split();
            let first_event = {
                let pending = events.recv();
                tokio::pin!(pending);
                poll_fn(|cx| {
                    assert!(
                        pending.as_mut().poll(cx).is_pending(),
                        "idle is pending, not drained"
                    );
                    Poll::Ready(())
                })
                .await;
                commands
                    .submit(SessionCommand::Input(
                        Message::user_text("first").expect("input"),
                    ))
                    .await
                    .expect("accepted while receive pending");
                pending.await.expect("response starts")
            };
            let first_response = match first_event {
                SessionEvent::ResponseStarted { response, .. } => response,
                other => panic!("expected correlated start, got {other:?}"),
            };
            let (request, call) = loop {
                match events.recv().await.expect("tool event") {
                    SessionEvent::ToolRequested {
                        response,
                        request,
                        call,
                    } => {
                        assert_eq!(response, first_response);
                        break (request, call);
                    }
                    SessionEvent::ControlApplied { .. } => {}
                    other => panic!("tool request must precede terminal, got {other:?}"),
                }
            };
            assert_eq!(call.id(), Some("native-call-1"));
            let mut pending_result_event = Some({
                let mut pending = Box::pin(events.recv());
                poll_fn(|cx| {
                    assert!(
                        pending.as_mut().poll(cx).is_pending(),
                        "native tool request waits for caller result"
                    );
                    Poll::Ready(())
                })
                .await;
                commands
                    .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                        request,
                        FunctionResult::new(
                            call.id().map(str::to_owned),
                            call.name(),
                            vec![ContentPart::Json {
                                value: json!({"value": 42}),
                            }],
                            false,
                        )
                        .expect("result"),
                    )]))
                    .await
                    .expect(
                        "tool result accepted while receive is pending before response completion",
                    );
                pending.await.expect("result makes progress")
            });
            let mut audio = false;
            let mut transcription = false;
            loop {
                let event = match pending_result_event.take() {
                    Some(event) => event,
                    None => events.recv().await.expect("response observation"),
                };
                match event {
                    SessionEvent::Output {
                        response,
                        segment,
                        content: ContentPart::Audio { media_type, data },
                    } => {
                        assert_eq!(response, first_response);
                        assert!(segment.is_some());
                        assert_eq!(media_type, "audio/pcm;rate=24000");
                        assert_eq!(data, [1, 2, 3, 4]);
                        audio = true;
                    }
                    SessionEvent::Transcription {
                        source: TranscriptionSource::Output { response, .. },
                        text,
                        ..
                    } => {
                        assert_eq!(response, first_response);
                        assert_eq!(text, "forty two");
                        transcription = true;
                    }
                    SessionEvent::ResponseFinished {
                        response,
                        outcome: ResponseOutcome::Completed,
                        output: None,
                        ..
                    } => {
                        assert_eq!(response, first_response);
                        break;
                    }
                    SessionEvent::ControlApplied { .. } | SessionEvent::Usage { .. } => {}
                    other => panic!("unexpected response observation {other:?}"),
                }
            }
            assert!(audio && transcription);
            loop {
                match events.recv().await.expect("late transcription") {
                    SessionEvent::Transcription {
                        source: TranscriptionSource::Input { command: None },
                        text,
                        ..
                    } => {
                        assert_eq!(text, "late first input");
                        break;
                    }
                    SessionEvent::ControlApplied { .. } => {}
                    SessionEvent::TurnEnded {
                        response: Some(response),
                    } => assert_eq!(response, first_response),
                    other => {
                        panic!(
                            "late observation must not become a second response outcome: {other:?}"
                        )
                    }
                }
            }
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("second").expect("input"),
                ))
                .await
                .expect("next conversation input");
            let second_response = GeminiLiveServer::completion(&mut events).await;
            assert_ne!(first_response, second_response);
        }
        session.close().await.expect("close");
        provider.shutdown().await.expect("shutdown");
        server.finish().await;
    })
    .await
    .expect("Live round trip bounded");
}

#[tokio::test]
async fn live_native_tool_cancellation_invalidates_results_and_preserves_subsequent_activity() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (cancel, cancelled) = oneshot::channel();
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "cancelled tool").await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"toolCall": {"functionCalls": [{
                    "id": "cancelled-native-call", "name": "lookup", "args": {"key": "a"}
                }]}}),
            )
            .await;
            cancelled
                .await
                .expect("call delivered before native cancellation");
            GeminiLiveServer::send(
                &mut socket,
                json!({"toolCallCancellation": {"ids": ["cancelled-native-call"]}}),
            )
            .await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent": {"interrupted": true}}))
                .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"turnComplete": true, "interactionStatus": "IDLE"}}),
            )
            .await;
            // Invalidated results must not become a toolResponse on the wire.
            GeminiLiveServer::input(&mut socket, "after interruption").await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"generationComplete": true}}),
            )
            .await;
            GeminiLiveServer::send(
                &mut socket,
                json!({"serverContent": {"turnComplete": true, "interactionStatus": "IDLE"}}),
            )
            .await;
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .expect("Live opens");
        {
            let (mut commands, mut events) = session.split();
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("cancelled tool").expect("input"),
                ))
                .await
                .expect("accepted");
            let (request, call, response) = loop {
                match events.recv().await.expect("call") {
                    SessionEvent::ToolRequested {
                        request,
                        call,
                        response,
                    } => break (request, call, response),
                    SessionEvent::ResponseStarted { .. } | SessionEvent::ControlApplied { .. } => {}
                    other => panic!("unexpected before call: {other:?}"),
                }
            };
            cancel.send(()).expect("release cancellation");
            let mut invalidated = false;
            loop {
                match events.recv().await.expect("cancellation observations") {
                    SessionEvent::ToolsInvalidated { requests } => {
                        assert_eq!(requests, vec![request.clone()]);
                        invalidated = true;
                    }
                    SessionEvent::ResponseFinished {
                        outcome: ResponseOutcome::Interrupted,
                        ..
                    } => break,
                    SessionEvent::Interruption {
                        response: Some(interrupted),
                    } => assert_eq!(interrupted, response),
                    other => panic!("unexpected cancellation event: {other:?}"),
                }
            }
            assert!(invalidated);
            let Some(SessionEvent::TurnEnded {
                response: Some(ended),
            }) = events.recv().await
            else {
                panic!("native interruption ends its activity independently")
            };
            assert_eq!(ended, response);
            let error = commands
                .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                    request,
                    FunctionResult::new(
                        call.id().map(str::to_owned),
                        call.name(),
                        vec![ContentPart::Text {
                            text: "too late".into(),
                        }],
                        false,
                    )
                    .expect("result"),
                )]))
                .await
                .expect_err("cancelled result is rejected before acceptance");
            assert!(matches!(
                error.kind(),
                ErrorKind::InvalidInput | ErrorKind::InvalidState
            ));
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("after interruption").expect("input"),
                ))
                .await
                .expect("subsequent work");
            GeminiLiveServer::completion(&mut events).await;
        }
        session.close().await.expect("close");
        provider.shutdown().await.expect("shutdown");
        server.finish().await;
    })
    .await
    .expect("tool invalidation bounded");
}

#[tokio::test]
async fn live_manual_audio_keeps_activity_boundaries_distinct_and_rejects_narrow_cancel() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            let setup = GeminiLiveServer::receive(&mut socket).await;
            assert_eq!(setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"]["disabled"], true);
            GeminiLiveServer::send(&mut socket, json!({"setupComplete": {}})).await;
            assert_eq!(GeminiLiveServer::receive(&mut socket).await, json!({"realtimeInput": {"activityStart": {}}}));
            assert_eq!(GeminiLiveServer::receive(&mut socket).await, json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000", "data": "AQIDBA=="}}}));
            assert_eq!(GeminiLiveServer::receive(&mut socket).await, json!({"realtimeInput": {"activityEnd": {}}}));
            GeminiLiveServer::send(&mut socket, json!({"serverContent": {"generationComplete": true}})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent": {"turnComplete": true, "interactionStatus": "IDLE"}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.model();
        let mut session = model.open_session(GeminiLiveServer::config().with_turn_detection(SessionTurnDetection::Manual)).await.expect("manual Live opens");
        {
            let (mut commands, mut events) = session.split();
            let activity = commands.submit(SessionCommand::StartInput).await.expect("activity starts");
            let error = commands.submit(SessionCommand::Cancel(CancelTarget::Command(activity))).await.expect_err("native narrow cancellation unavailable");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            commands.submit(SessionCommand::Audio { media_type: "audio/pcm;rate=16000".into(), data: vec![1, 2, 3, 4] }).await.expect("audio");
            commands.submit(SessionCommand::EndInput).await.expect("activity ends");
            GeminiLiveServer::completion(&mut events).await;
        }
        session.close().await.expect("close");
        provider.shutdown().await.expect("shutdown");
        server.finish().await;
    }).await.expect("manual boundaries bounded");
}

#[tokio::test]
async fn live_manual_activity_interrupts_a_tool_response_and_starts_its_own_generation() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let server=GeminiLiveServer::start(|mut socket|async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket,"request a tool").await;
            GeminiLiveServer::send(&mut socket,json!({"toolCall":{"functionCalls":[{"id":"interrupted-call","name":"lookup","args":{"key":"a"}}]}})).await;
            assert_eq!(GeminiLiveServer::receive(&mut socket).await,json!({"realtimeInput":{"activityStart":{}}}));
            let audio=GeminiLiveServer::receive(&mut socket).await;
            assert_eq!(audio["realtimeInput"]["audio"]["data"],"AQIDBA==");
            assert_eq!(GeminiLiveServer::receive(&mut socket).await,json!({"realtimeInput":{"activityEnd":{}}}));
            GeminiLiveServer::send(&mut socket,json!({"toolCallCancellation":{"ids":["interrupted-call"]}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"interrupted":true}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"turnComplete":true}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AQIDBA=="}}]},"outputTranscription":{"text":"audio input received"}}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"generationComplete":true}})).await;
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"turnComplete":true}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.model();
        let mut session=model.open_session(GeminiLiveServer::config().with_turn_detection(SessionTurnDetection::Manual)).await.unwrap();
        {
            let (mut commands,mut events)=session.split();
            commands.submit(SessionCommand::Input(Message::user_text("request a tool").unwrap())).await.unwrap();
            let (original,request)=loop {match events.recv().await.unwrap(){SessionEvent::ToolRequested {response,request,..}=>break(response,request),SessionEvent::ResponseStarted{..}|SessionEvent::ControlApplied{..}=>{},other=>panic!("missing original call: {other:?}")}};
            commands.submit(SessionCommand::StartInput).await.unwrap();
            commands.submit(SessionCommand::Audio{media_type:"audio/pcm;rate=16000".into(),data:vec![1,2,3,4]}).await.unwrap();
            let capture=commands.submit(SessionCommand::EndInput).await.unwrap();
            let mut next=None;
            let mut invalidated=false;
            let mut interrupted=false;
            loop {match events.recv().await.unwrap(){
                SessionEvent::ToolsInvalidated {requests}=>{assert_eq!(requests,vec![request.clone()]);invalidated=true;},
                SessionEvent::Interruption {response:Some(response)}=>assert_eq!(response,original),
                SessionEvent::ResponseFinished {response,outcome:ResponseOutcome::Interrupted,..}=>{assert_eq!(response,original);interrupted=true;},
                SessionEvent::ResponseStarted {command:Some(command),response}=>{assert_eq!(command,capture);assert_ne!(response,original);next=Some(response);},
                SessionEvent::Output{response,..}|SessionEvent::Transcription{source:TranscriptionSource::Output{response,..},..}=>assert_eq!(next.as_ref(),Some(&response)),
                SessionEvent::ResponseFinished{response,outcome:ResponseOutcome::Completed,..}=>{assert_eq!(next.as_ref(),Some(&response));break;},
                SessionEvent::TurnEnded{response:Some(response)}=>assert_eq!(response,original),
                SessionEvent::ControlApplied{..}=>{},
                other=>panic!("new input lost generation ownership: {other:?}"),
            }}
            assert!(invalidated&&interrupted);
        }
        session.close().await.unwrap();provider.shutdown().await.unwrap();server.finish().await;
    }).await.expect("manual input interruption bounded");
}

#[tokio::test]
async fn live_cancelled_tool_can_be_reissued_as_a_distinct_recovery_obligation() {
    // Provenance: the 2026-11-01 real Gemini cancellation/recovery case issued
    // a new native function ID after cancelling the original request. This
    // passing control preserves that authentic ordering; it is not a bug fix.
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "request a tool").await;
            GeminiLiveServer::send(&mut socket, json!({"toolCall":{"functionCalls":[{
                "id":"cancelled-call","name":"lookup","args":{"key":"a"}
            }]}})).await;
            assert_eq!(GeminiLiveServer::receive(&mut socket).await, json!({"realtimeInput":{"activityStart":{}}}));
            assert_eq!(GeminiLiveServer::receive(&mut socket).await["realtimeInput"]["audio"]["data"], "AQIDBA==");
            assert_eq!(GeminiLiveServer::receive(&mut socket).await, json!({"realtimeInput":{"activityEnd":{}}}));
            GeminiLiveServer::send(&mut socket, json!({"toolCallCancellation":{"ids":["cancelled-call"]}})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"outputTranscription":{"text":"unfinished"}}})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"interrupted":true}})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"turnComplete":true}})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"inputTranscription":{"text":"Please say audio input received."}}})).await;
            GeminiLiveServer::send(&mut socket, json!({"toolCall":{"functionCalls":[{
                "id":"recovery-call","name":"lookup","args":{"key":"a"}
            }]}})).await;
            assert_eq!(GeminiLiveServer::receive(&mut socket).await, json!({"toolResponse":{"functionResponses":[{
                "id":"recovery-call","name":"lookup","response":{"answer":"audio input received"}
            }]}}), "only the fresh obligation may reach the native socket");
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{
                "modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AQIDBA=="}}]},
                "outputTranscription":{"text":"audio input received","finished":true}
            }})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"generationComplete":true}})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"turnComplete":true}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.model();
        let mut session = model.open_session(GeminiLiveServer::config().with_turn_detection(SessionTurnDetection::Manual)).await.unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands.submit(SessionCommand::Input(Message::user_text("request a tool").unwrap())).await.unwrap();
            let (original, request, call) = loop {
                match events.recv().await.unwrap() {
                    SessionEvent::ToolRequested { response, request, call } => break (response, request, call),
                    SessionEvent::ResponseStarted { .. } | SessionEvent::ControlApplied { .. } => {},
                    unexpected => panic!("original request missing: {unexpected:?}"),
                }
            };
            commands.submit(SessionCommand::StartInput).await.unwrap();
            commands.submit(SessionCommand::Audio { media_type:"audio/pcm;rate=16000".into(), data:vec![1,2,3,4] }).await.unwrap();
            let activity = commands.submit(SessionCommand::EndInput).await.unwrap();
            let mut recovery = None;
            let mut original_segment = None;
            let mut invalidated = false;
            let mut interrupted = false;
            let mut old_finished = false;
            let mut old_turn_ended = false;
            let mut input_observed = false;
            let mut submitted = false;
            let mut audio = false;
            let mut transcription = false;
            let mut completed = false;
            loop {
                match events.recv().await.unwrap() {
                    SessionEvent::ToolsInvalidated { requests } => {
                        assert!(!invalidated);
                        assert_eq!(requests, vec![request.clone()]);
                        invalidated = true;
                    }
                    SessionEvent::Interruption { response:Some(response) } => {
                        assert_eq!(response, original);
                        interrupted = true;
                    }
                    SessionEvent::ResponseFinished { response, outcome:ResponseOutcome::Interrupted, continuity:SessionContinuity::Reusable, output:None } => {
                        assert_eq!(response, original);
                        old_finished = true;
                    }
                    SessionEvent::ResponseStarted { command:Some(command), response } => {
                        assert_eq!(command, activity);
                        assert_ne!(response, original);
                        assert!(recovery.replace(response).is_none());
                    }
                    SessionEvent::ToolRequested { response, request:fresh, call:fresh_call } => {
                        assert!(invalidated && interrupted && old_finished && old_turn_ended);
                        assert_eq!(recovery.as_ref(), Some(&response));
                        assert_ne!(fresh, request);
                        assert_ne!(fresh_call.id(), call.id());
                        assert_eq!(fresh_call.name(), "lookup");
                        assert_eq!(fresh_call.arguments(), &json!({"key":"a"}));
                        assert!(!submitted);
                        let stale = FunctionResult::new(call.id().map(str::to_owned), call.name(), vec![ContentPart::Json { value:json!({"answer":"stale"}) }], false).unwrap();
                        let error = commands.submit(SessionCommand::ToolResults(vec![SessionToolResult::new(request.clone(), stale)])).await.expect_err("original obligation remains stale while its replacement is pending");
                        assert_eq!(error.kind(), ErrorKind::InvalidState);
                        let result = FunctionResult::new(fresh_call.id().map(str::to_owned), fresh_call.name(), vec![ContentPart::Json { value:json!({"answer":"audio input received"}) }], false).unwrap();
                        commands.submit(SessionCommand::ToolResults(vec![SessionToolResult::new(fresh, result)])).await.unwrap();
                        submitted = true;
                    }
                    SessionEvent::Output { response, content:ContentPart::Audio { data, .. }, .. } => {
                        assert!(submitted);
                        assert_eq!(recovery.as_ref(), Some(&response));
                        assert_eq!(data, [1,2,3,4]);
                        audio = true;
                    }
                    SessionEvent::Transcription { source:TranscriptionSource::Input { command:None }, text, .. } => {
                        assert_eq!(text, "Please say audio input received.");
                        input_observed = true;
                    }
                    SessionEvent::Transcription { source:TranscriptionSource::Output { response, segment }, text, finalized:false } if response == original => {
                        assert_eq!(text, "unfinished");
                        assert!(original_segment.replace(segment).is_none());
                    }
                    SessionEvent::Transcription { source:TranscriptionSource::Output { response, segment }, text, finalized:true } => {
                        assert_ne!(original_segment.as_ref().expect("original transcript segment"), &segment);
                        assert!(submitted);
                        assert_eq!(recovery.as_ref(), Some(&response));
                        assert_eq!(text, "audio input received");
                        transcription = true;
                    }
                    SessionEvent::ResponseFinished { response, outcome:ResponseOutcome::Completed, continuity:SessionContinuity::Reusable, output:None } => {
                        assert_eq!(recovery.as_ref(), Some(&response));
                        completed = true;
                    }
                    SessionEvent::TurnEnded { response:Some(response) } if response == original => old_turn_ended = true,
                    SessionEvent::TurnEnded { response:Some(response) } => {
                        assert_eq!(recovery.as_ref(), Some(&response));
                        assert!(completed && submitted && audio && transcription && input_observed);
                        break;
                    }
                    SessionEvent::ControlApplied { .. } => {},
                    unexpected => panic!("recovery lost original correlation: {unexpected:?}"),
                }
            }
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    }).await.expect("cancelled and fresh obligations remain bounded");
}

#[tokio::test]
async fn live_function_setup_selects_blocking_caller_owned_continuation() {
    // Gemini 3.8's NON_BLOCKING default and supported BLOCKING declaration:
    // https://ai.google.dev/gemini-api/docs/live-api/capabilities (2026-11-01).
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            let setup = GeminiLiveServer::receive(&mut socket).await;
            assert_eq!(
                setup["setup"]["tools"][0]["functionDeclarations"][0]["behavior"],
                "BLOCKING",
                "Gemini 3.8 defaults to NON_BLOCKING; caller-owned continuation must choose its mode explicitly"
            );
            GeminiLiveServer::send(&mut socket, json!({"setupComplete":{}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.model();
        let session = model.open_session(GeminiLiveServer::config()).await.expect("blocking Live setup");
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    }).await.expect("one blocking setup bounded");
}

#[tokio::test]
async fn live_resumption_metadata_does_not_reconfigure_or_interrupt_generation() {
    // Provenance: all four real Live cases at 597e5dd received this known
    // server-message member without sessionResumption in setup. Preserve the
    // envelope shape; the opaque live handle is replaced with a fixture value.
    // Raw failures remain in target/session-refactor-evidence/new-live-sessions/.
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::send(&mut socket, json!({"sessionResumptionUpdate":{"newHandle":"fixture-opaque-handle","resumable":true}})).await;
            GeminiLiveServer::input(&mut socket, "continue on the original connection").await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"modelTurn":{"parts":[{"text":"original connection answer"}]}}})).await;
            GeminiLiveServer::send(&mut socket, json!({"sessionResumptionUpdate":{"resumable":false}})).await;
            GeminiLiveServer::send(&mut socket, json!({"serverContent":{"generationComplete":true}})).await;
            // The next client message must be close, never setup or resumption.
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider, model) = server.model();
        let mut session = model.open_session(GeminiLiveServer::config()).await.unwrap();
        {
            let (mut commands, mut events) = session.split();
            let receipt = commands.submit(SessionCommand::Input(Message::user_text("continue on the original connection").unwrap())).await.unwrap();
            let mut response = None;
            let mut answer = false;
            loop {
                match events.recv().await.expect("response survives auxiliary metadata") {
                    SessionEvent::ResponseStarted { command:Some(command),response:started } => {assert_eq!(command,receipt);assert!(response.replace(started).is_none());},
                    SessionEvent::Output { response:observed,content:ContentPart::Text { text },.. } => {assert_eq!(response.as_ref(),Some(&observed));assert_eq!(text,"original connection answer");answer=true;},
                    SessionEvent::ResponseFinished { response:finished,outcome:ResponseOutcome::Completed,continuity:SessionContinuity::Reusable,output:None } => {assert_eq!(response.as_ref(),Some(&finished));break;},
                    SessionEvent::ControlApplied { .. } => {},
                    unexpected => panic!("known resumption metadata changed the active generation: {unexpected:?}"),
                }
            }
            assert!(answer);
            let mut idle = Box::pin(events.recv());
            poll_fn(|cx| {assert!(idle.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    }).await.expect("auxiliary resumption metadata bounded");
}

#[tokio::test]
async fn live_malformed_resumption_metadata_and_repeated_setup_remain_protocol_errors() {
    for payload in [
        json!({"sessionResumptionUpdate":null}),
        json!({"sessionResumptionUpdate":{"resumable":"true"}}),
        json!({"sessionResumptionUpdate":{"newHandle":7,"resumable":true}}),
        json!({"sessionResumptionUpdate":{},"serverContent":{"generationComplete":true}}),
        json!({"setupComplete":{}}),
    ] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = GeminiLiveServer::start(move |mut socket| async move {
                GeminiLiveServer::setup(&mut socket).await;
                GeminiLiveServer::input(&mut socket, "validate auxiliary metadata").await;
                GeminiLiveServer::send(&mut socket, payload).await;
                GeminiLiveServer::expect_close(&mut socket).await;
            })
            .await;
            let (provider, model) = server.model();
            let mut session = model
                .open_session(GeminiLiveServer::config())
                .await
                .unwrap();
            {
                let (mut commands, mut events) = session.split();
                commands
                    .submit(SessionCommand::Input(
                        Message::user_text("validate auxiliary metadata").unwrap(),
                    ))
                    .await
                    .unwrap();
                loop {
                    match events
                        .recv()
                        .await
                        .expect("malformed server metadata must terminate")
                    {
                        SessionEvent::Terminated {
                            reason,
                            unfinished_responses,
                            ..
                        } => {
                            assert!(matches!(reason.kind(), ErrorKind::InvalidResponse(_)));
                            assert_eq!(unfinished_responses.len(), 1);
                            break;
                        }
                        SessionEvent::ResponseStarted { .. }
                        | SessionEvent::ControlApplied { .. } => {}
                        unexpected => panic!(
                            "malformed auxiliary envelope fabricated progress: {unexpected:?}"
                        ),
                    }
                }
                assert!(events.recv().await.is_none());
            }
            session.close().await.unwrap();
            provider.shutdown().await.unwrap();
            server.finish().await;
        })
        .await
        .expect("invalid auxiliary metadata bounded");
    }
}

#[tokio::test]
async fn live_empty_server_envelopes_are_nonterminal_keepalives() {
    // Observed after setup in three real cases in gemini-live-resumption-fix/.
    GeminiLiveServer::auxiliary_frames(vec![json!({}), json!({})]).await;
}

#[tokio::test]
async fn live_input_voice_activity_is_uncorrelated_auxiliary_telemetry() {
    // voiceActivity is alongside usageMetadata, outside messageType in the
    // official Live server envelope. The first frame was observed in the real
    // manual-audio case in gemini-live-resumption-fix/ on 2026-11-01.
    GeminiLiveServer::auxiliary_frames(vec![
        json!({"voiceActivity":{"audioOffset":"0s","type":"ACTIVITY_START"}}),
        json!({"voiceActivity":{"audioOffset":"1.250s","type":"ACTIVITY_END"},"usageMetadata":{"promptTokenCount":2}}),
        json!({"voiceActivity":{}}),
    ]).await;
}

#[tokio::test]
async fn live_auxiliary_messages_do_not_extend_the_original_response_deadline() {
    tokio::time::timeout(Duration::from_secs(6),async {
        let sent=std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed=std::sync::Arc::clone(&sent);
        let server=GeminiLiveServer::start(move |mut socket|async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket,"keep the original deadline").await;
            let mut heartbeat=tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _=heartbeat.tick()=>{
                        let index=sent.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
                        let frame=if index.is_multiple_of(2) {json!({})} else {json!({"voiceActivity":{"type":"ACTIVITY_START","audioOffset":"0s"}})};
                        GeminiLiveServer::send(&mut socket,frame).await;
                    },
                    message=socket.next()=>{
                        assert!(matches!(message,Some(Ok(WebSocketMessage::Close(_)))));
                        socket.flush().await.expect("deadline close acknowledgement");break;
                    },
                }
            }
        }).await;
        let (provider,model)=server.model();
        let mut session=model.open_session(GeminiLiveServer::config()).await.unwrap();
        {
            let (mut commands,mut events)=session.split();
            commands.submit(SessionCommand::Input(Message::user_text("keep the original deadline").unwrap())).await.unwrap();
            loop {
                match events.recv().await.expect("original deadline remains authoritative") {
                    SessionEvent::Terminated { reason,unfinished_responses,.. }=>{assert_eq!(reason.kind(),ErrorKind::Timeout);assert_eq!(unfinished_responses.len(),1);break;},
                    SessionEvent::ResponseStarted{..}|SessionEvent::ControlApplied{..}=>{},
                    unexpected=>panic!("auxiliary traffic changed generation outcome: {unexpected:?}"),
                }
            }
            assert!(events.recv().await.is_none());
        }
        assert!(observed.load(std::sync::atomic::Ordering::SeqCst)>=2,"native heartbeat continued while the response waited");
        session.close().await.unwrap();provider.shutdown().await.unwrap();server.finish().await;
    }).await.expect("heartbeats cannot indefinitely postpone the three-second response deadline");
}

#[tokio::test]
async fn live_optional_transcription_markers_finalize_accumulated_input_and_output() {
    // Google SDK Transcription documents optional text/finished:
    // https://googleapis.github.io/js-genai/release_docs/interfaces/types.Transcription.html
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "transcription markers").await;
            for content in [
                json!({"inputTranscription":{"languageCode":"en"}}),
                json!({"inputTranscription":{"text":"first "}}),
                json!({"inputTranscription":{"text":"input"}}),
                json!({"inputTranscription":{"finished":true}}),
                json!({"inputTranscription":{"text":null,"finished":null}}),
                json!({"inputTranscription":{"text":"second input"}}),
                json!({"inputTranscription":{"finished":true}}),
                json!({"inputTranscription":{"finished":true}}),
                json!({"outputTranscription":{"languageCode":"en"}}),
                json!({"outputTranscription":{"text":"hello "}}),
                json!({"outputTranscription":{"text":"world","finished":true}}),
                json!({"generationComplete":true}),
            ] {
                GeminiLiveServer::send(&mut socket, json!({"serverContent":content})).await;
            }
            GeminiLiveServer::expect_close(&mut socket).await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .unwrap();
        {
            let (mut commands, mut events) = session.split();
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("transcription markers").unwrap(),
                ))
                .await
                .unwrap();
            let mut input = Vec::new();
            let mut output = Vec::new();
            let mut response = None;
            let mut segment = None;
            loop {
                match events
                    .recv()
                    .await
                    .expect("transcription markers remain supported")
                {
                    SessionEvent::ResponseStarted {
                        response: started, ..
                    } => response = Some(started),
                    SessionEvent::Transcription {
                        source: TranscriptionSource::Input { command: None },
                        text,
                        finalized,
                    } => input.push((text, finalized)),
                    SessionEvent::Transcription {
                        source:
                            TranscriptionSource::Output {
                                response: observed,
                                segment: observed_segment,
                            },
                        text,
                        finalized,
                    } => {
                        assert_eq!(response.as_ref(), Some(&observed));
                        if let Some(segment) = &segment {
                            assert_eq!(segment, &observed_segment);
                        } else {
                            segment = Some(observed_segment);
                        }
                        output.push((text, finalized));
                    }
                    SessionEvent::ResponseFinished {
                        outcome: ResponseOutcome::Completed,
                        ..
                    } => break,
                    SessionEvent::ControlApplied { .. } => {}
                    unexpected => {
                        panic!("optional transcription fields changed meaning: {unexpected:?}")
                    }
                }
            }
            assert_eq!(
                input,
                vec![
                    ("first ".into(), false),
                    ("input".into(), false),
                    ("first input".into(), true),
                    ("second input".into(), false),
                    ("second input".into(), true),
                    ("".into(), true)
                ]
            );
            assert_eq!(
                output,
                vec![("hello ".into(), false), ("hello world".into(), true)]
            );
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        server.finish().await;
    })
    .await
    .expect("optional transcript finalization bounded");
}

#[tokio::test]
async fn live_transcript_finalization_transfers_retained_bytes_without_duplicate_budget() {
    tokio::time::timeout(Duration::from_secs(20),async {
        let (consumed,mut acknowledgements)=tokio::sync::mpsc::channel(1);
        let server=GeminiLiveServer::start(move |mut socket|async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket,"large independent transcripts").await;
            for _ in 0..2 {
                for index in 0..=17 {
                    let transcript=if index<17 {json!({"text":"x".repeat(1024*1024)})} else {json!({"finished":true})};
                    GeminiLiveServer::send(&mut socket,json!({"serverContent":{"inputTranscription":transcript}})).await;
                    tokio::select! {
                        message=socket.next()=>{
                            assert!(matches!(message,Some(Ok(WebSocketMessage::Close(_)))));
                            socket.flush().await.expect("failed finalization still closes cleanly");return;
                        },
                        acknowledgement=acknowledgements.recv()=>{
                            if acknowledgement.is_none(){GeminiLiveServer::expect_close(&mut socket).await;return;}
                        },
                    }
                }
            }
            GeminiLiveServer::send(&mut socket,json!({"serverContent":{"generationComplete":true}})).await;
            GeminiLiveServer::expect_close(&mut socket).await;
        }).await;
        let (provider,model)=server.model_with_timeout(Duration::from_secs(15));
        let mut session=model.open_session(GeminiLiveServer::config()).await.unwrap();
        {
            let (mut commands,mut events)=session.split();
            commands.submit(SessionCommand::Input(Message::user_text("large independent transcripts").unwrap())).await.unwrap();
            let mut deltas=0;let mut snapshots=0;
            loop {match events.recv().await.expect("retained transcript remains within32MiB") {
                SessionEvent::Transcription { source:TranscriptionSource::Input {command:None},text,finalized:false }=>{assert_eq!(text.len(),1024*1024);deltas+=1;consumed.send(()).await.unwrap();},
                SessionEvent::Transcription { source:TranscriptionSource::Input {command:None},text,finalized:true }=>{assert_eq!(text.len(),17*1024*1024);assert_eq!(deltas,17);deltas=0;snapshots+=1;consumed.send(()).await.unwrap();},
                SessionEvent::ResponseFinished {outcome:ResponseOutcome::Completed,..}=>break,
                SessionEvent::ResponseStarted{..}|SessionEvent::ControlApplied{..}=>{},
                unexpected=>panic!("transcript terminated before required finalization: {unexpected:?}"),
            }}
            assert_eq!(snapshots,2);assert_eq!(deltas,0);
        }
        session.close().await.unwrap();provider.shutdown().await.unwrap();server.finish().await;
    }).await.expect("bounded transcript ownership transfer");
}

#[tokio::test]
async fn live_malformed_auxiliary_and_terminal_fields_are_not_silently_ignored() {
    for payload in [
        json!({"voiceActivity":null}),
        json!({"voiceActivity":{"type":true}}),
        json!({"voiceActivity":{"type":"NOT_ACTIVITY"}}),
        json!({"voiceActivity":{"audioOffset":3}}),
        json!({"serverContent":{"inputTranscription":{"text":3}}}),
        json!({"serverContent":{"inputTranscription":{"finished":"yes"}}}),
        json!({"serverContent":{"outputTranscription":{"finished":"yes"}}}),
        json!({"serverContent":{"generationComplete":"yes"}}),
        json!({"serverContent":{"turnComplete":"yes"}}),
        json!({"serverContent":{"interrupted":"yes"}}),
        json!({"serverContent":{"waitingForInput":"yes"}}),
    ] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = GeminiLiveServer::start(move |mut socket| async move {
                GeminiLiveServer::setup(&mut socket).await;
                GeminiLiveServer::input(&mut socket, "malformed native fields").await;
                GeminiLiveServer::send(&mut socket, payload).await;
                GeminiLiveServer::send(
                    &mut socket,
                    json!({"serverContent":{"generationComplete":true}}),
                )
                .await;
                GeminiLiveServer::expect_close(&mut socket).await;
            })
            .await;
            let (provider, model) = server.model();
            let mut session = model
                .open_session(GeminiLiveServer::config())
                .await
                .unwrap();
            {
                let (mut commands, mut events) = session.split();
                commands
                    .submit(SessionCommand::Input(
                        Message::user_text("malformed native fields").unwrap(),
                    ))
                    .await
                    .unwrap();
                loop {
                    match events
                        .recv()
                        .await
                        .expect("malformed fields must fail before later success")
                    {
                        SessionEvent::Terminated { reason, .. } => {
                            assert!(matches!(reason.kind(), ErrorKind::InvalidResponse(_)));
                            break;
                        }
                        SessionEvent::ResponseStarted { .. }
                        | SessionEvent::ControlApplied { .. } => {}
                        unexpected => {
                            panic!("malformed native field escaped validation: {unexpected:?}")
                        }
                    }
                }
                assert!(events.recv().await.is_none());
            }
            session.close().await.unwrap();
            provider.shutdown().await.unwrap();
            server.finish().await;
        })
        .await
        .expect("malformed native fields bounded");
    }
}

#[tokio::test]
async fn live_premature_eof_reports_one_terminal_without_success() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = GeminiLiveServer::start(|mut socket| async move {
            GeminiLiveServer::setup(&mut socket).await;
            GeminiLiveServer::input(&mut socket, "unfinished").await;
            socket.close(None).await.expect("native close");
            let _ = socket.next().await;
        })
        .await;
        let (provider, model) = server.model();
        let mut session = model
            .open_session(GeminiLiveServer::config())
            .await
            .expect("Live opens");
        {
            let (mut commands, mut events) = session.split();
            commands
                .submit(SessionCommand::Input(
                    Message::user_text("unfinished").expect("input"),
                ))
                .await
                .expect("accepted");
            let mut terminated = false;
            while let Some(event) = events.recv().await {
                match event {
                    SessionEvent::Terminated {
                        continuity,
                        unfinished_responses,
                        ..
                    } => {
                        assert!(!terminated, "terminal publication is unique");
                        assert!(matches!(
                            continuity,
                            SessionContinuity::Closed | SessionContinuity::Uncertain
                        ));
                        assert_eq!(unfinished_responses.len(), 1);
                        terminated = true;
                    }
                    SessionEvent::ResponseStarted { .. } | SessionEvent::ControlApplied { .. } => {}
                    other => panic!("premature EOF cannot complete unfinished work: {other:?}"),
                }
            }
            assert!(terminated);
        }
        session
            .close()
            .await
            .expect("local ownership closes after peer loss");
        provider.shutdown().await.expect("shutdown");
        server.finish().await;
    })
    .await
    .expect("EOF bounded");
}
