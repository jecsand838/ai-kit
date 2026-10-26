use provider_sdk::error::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use provider_sdk::batch::{BatchInput, BatchSubmitRequest};
use provider_sdk::config::{
    AnthropicConfig, ApiKey, GeminiConfig, OllamaConfig, OpenAiConfig, ProviderKind,
    TransportConfig,
};
use provider_sdk::embedding::EmbedRequest;
use provider_sdk::generation::{GenerateEvent, GenerateRequest, Message};
use provider_sdk::models::{ModelId, ModelListRequest};
use provider_sdk::session::{SessionBackend, SessionConfig, SessionInput};
use provider_sdk::{ModelBackend, Provider, ProviderBackend, ProviderConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Barrier, Notify, mpsc};
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use url::Url;

const DEADLINE: Duration = Duration::from_secs(10);
const MODEL_ID: &str = "models/gemini-3.1-flash-live-preview";
const OPENAI_GENERATION_MODEL_ID: &str = "gpt-4.1";
const OPENAI_SESSION_MODEL_ID: &str = "gpt-realtime-2.1";
const GENERATE_BODY: &str = concat!(
    "{\"model\":\"gpt-4.1\",\"status\":\"completed\",",
    "\"output\":[{\"id\":\"msg_lifecycle\",\"type\":\"message\",\"role\":\"assistant\",",
    "\"content\":[{\"type\":\"output_text\",\"text\":\"complete\"}]}],",
    "\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}",
);
const STREAM_BODY: &str = concat!(
    "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"complete\"}]},",
    "\"finishReason\":\"STOP\"}],",
    "\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1,\"totalTokenCount\":2},",
    "\"modelVersion\":\"models/gemini-3.1-flash-live-preview\"}\n\n",
);

struct BlockingGenerateServer {
    base_url: Url,
    started: Arc<Notify>,
    release: Arc<Notify>,
    task: Option<tokio::task::JoinHandle<()>>,
}

struct HeldStreamServer {
    base_url: Url,
    accepted: mpsc::UnboundedReceiver<()>,
    release_first: Arc<Notify>,
    stop_second: Arc<Notify>,
    task: Option<tokio::task::JoinHandle<()>>,
}

struct HttpRequest;

impl BlockingGenerateServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("blocking generation server should bind");
        let address = listener
            .local_addr()
            .expect("blocking generation server should have an address");
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let observed = Arc::clone(&started);
        let permitted = Arc::clone(&release);
        let task = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("interactive session should connect");
            let session = async move {
                let mut socket = tokio_tungstenite::accept_hdr_async(stream, HttpRequest)
                    .await
                    .expect("interactive session should upgrade");
                let setup = socket
                    .next()
                    .await
                    .expect("interactive setup should arrive")
                    .expect("interactive setup should decode");
                let setup: serde_json::Value = serde_json::from_str(
                    setup.to_text().expect("interactive setup should be text"),
                )
                .expect("interactive setup should be JSON");
                assert_eq!(setup["type"], "session.update");
                assert_eq!(setup["session"]["type"], "realtime");
                assert_eq!(
                    setup["session"]["output_modalities"],
                    serde_json::json!(["audio"])
                );
                socket
                    .send(WebSocketMessage::Text(
                        serde_json::json!({
                            "type": "session.updated",
                            "session": {
                                "type": "realtime",
                                "model": OPENAI_SESSION_MODEL_ID,
                                "output_modalities": ["audio"],
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("interactive setup should complete");
                while let Some(message) = socket.next().await {
                    if message
                        .expect("interactive close frame should decode")
                        .is_close()
                    {
                        break;
                    }
                }
            };
            let generation = async move {
                let (mut stream, _) = listener
                    .accept()
                    .await
                    .expect("generation request should connect");
                HttpRequest::read(&mut stream).await;
                observed.notify_one();
                permitted.notified().await;
                if let Err(error) =
                    HttpRequest::write_response(&mut stream, "application/json", GENERATE_BODY)
                        .await
                {
                    assert!(
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::NotConnected
                        ),
                        "generation response failed unexpectedly: {error}",
                    );
                }
            };
            tokio::join!(session, generation);
        });
        Self {
            base_url: Url::parse(&format!("http://{address}"))
                .expect("generation server URL should parse"),
            started,
            release,
            task: Some(task),
        }
    }

    async fn finish(mut self) {
        self.task
            .take()
            .expect("blocking generation server task should be owned")
            .await
            .expect("blocking generation server task should join");
    }
}

impl HeldStreamServer {
    async fn start(first_body: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("held stream server should bind");
        let address = listener
            .local_addr()
            .expect("held stream server should have an address");
        let (accepted, observer) = mpsc::unbounded_channel();
        let release_first = Arc::new(Notify::new());
        let stop_second = Arc::new(Notify::new());
        let first_release = Arc::clone(&release_first);
        let second_stop = Arc::clone(&stop_second);
        let task = tokio::spawn(async move {
            let (first, _) = listener
                .accept()
                .await
                .expect("first stream request should connect");
            let first_observer = accepted.clone();
            let first = async move {
                HeldStreamServer::serve_first(first, first_observer, first_release, first_body)
                    .await;
            };
            let second = async move {
                let (second, _) = listener
                    .accept()
                    .await
                    .expect("second stream request should connect");
                HeldStreamServer::serve_second(second, accepted, second_stop).await;
            };
            tokio::join!(first, second);
        });
        Self {
            base_url: Url::parse(&format!("http://{address}"))
                .expect("held stream server URL should parse"),
            accepted: observer,
            release_first,
            stop_second,
            task: Some(task),
        }
    }

    async fn serve_first(
        mut stream: TcpStream,
        accepted: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
        body: &'static str,
    ) {
        HttpRequest::read(&mut stream).await;
        HttpRequest::write_headers(&mut stream, "text/event-stream", body.len())
            .await
            .expect("first stream headers should write");
        accepted
            .send(())
            .expect("first stream observer should remain available");
        release.notified().await;
        stream
            .write_all(body.as_bytes())
            .await
            .expect("first stream body should write");
        stream
            .shutdown()
            .await
            .expect("first stream response should close");
    }

    async fn serve_second(
        mut stream: TcpStream,
        accepted: mpsc::UnboundedSender<()>,
        stop: Arc<Notify>,
    ) {
        HttpRequest::read(&mut stream).await;
        HttpRequest::write_headers(&mut stream, "text/event-stream", STREAM_BODY.len())
            .await
            .expect("second stream headers should write");
        accepted
            .send(())
            .expect("second stream observer should remain available");
        stop.notified().await;
    }

    async fn wait_for_both(&mut self) {
        self.accepted
            .recv()
            .await
            .expect("first stream should reach the server");
        self.accepted
            .recv()
            .await
            .expect("second stream should reach the server");
    }

    async fn finish(mut self) {
        self.stop_second.notify_one();
        self.task
            .take()
            .expect("held stream server task should be owned")
            .await
            .expect("held stream server task should join");
    }
}

impl Drop for BlockingGenerateServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Drop for HeldStreamServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl tokio_tungstenite::tungstenite::handshake::server::Callback for HttpRequest {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        let query = format!("model={OPENAI_SESSION_MODEL_ID}");
        assert_eq!(request.uri().query(), Some(query.as_str()));
        Ok(response)
    }
}

impl HttpRequest {
    async fn read(stream: &mut TcpStream) {
        let mut received = Vec::new();
        let (headers_end, content_length) = loop {
            let mut chunk = [0_u8; 1_024];
            let read = stream
                .read(&mut chunk)
                .await
                .expect("request should remain readable");
            assert_ne!(read, 0, "request should include complete headers");
            received.extend_from_slice(&chunk[..read]);
            if let Some(headers_end) = received.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&received[..headers_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                break (headers_end, content_length);
            }
        };
        let body_start = headers_end + 4;
        while received.len().saturating_sub(body_start) < content_length {
            let mut chunk = [0_u8; 1_024];
            let read = stream
                .read(&mut chunk)
                .await
                .expect("request body should remain readable");
            assert_ne!(read, 0, "request should include its declared body");
            received.extend_from_slice(&chunk[..read]);
        }
    }

    async fn write_headers(
        stream: &mut TcpStream,
        content_type: &str,
        content_length: usize,
    ) -> std::io::Result<()> {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(headers.as_bytes()).await?;
        stream.flush().await
    }

    async fn write_response(
        stream: &mut TcpStream,
        content_type: &str,
        body: &str,
    ) -> std::io::Result<()> {
        Self::write_headers(stream, content_type, body.len()).await?;
        stream.write_all(body.as_bytes()).await?;
        stream.shutdown().await
    }
}

impl BlockingGenerateServer {
    fn provider(&self) -> Provider {
        Provider::from_config(ProviderConfig::new(
            ProviderKind::OpenAi(
                OpenAiConfig::new(
                    ApiKey::new("openai-lifecycle-key").expect("fixture API key should validate"),
                )
                .with_base_url(self.base_url.clone()),
            ),
            TransportConfig::default(),
        ))
        .expect("OpenAI provider should construct")
    }
}

impl HeldStreamServer {
    fn provider(&self) -> Provider {
        Provider::from_config(ProviderConfig::new(
            ProviderKind::Gemini(
                GeminiConfig::new(
                    ApiKey::new("gemini-lifecycle-key").expect("fixture API key should validate"),
                )
                .with_base_url(self.base_url.clone()),
            ),
            TransportConfig::default(),
        ))
        .expect("Gemini provider should construct")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_provider_seals_existing_cloned_and_racing_models() {
    tokio::time::timeout(DEADLINE, async {
        let key = ApiKey::new("lifecycle-key").expect("fixture API key should validate");
        let base_url = Url::parse("http://127.0.0.1:9").expect("fixture URL should parse");
        let kinds = [
            (
                ProviderKind::OpenAi(
                    OpenAiConfig::new(key.clone()).with_base_url(base_url.clone()),
                ),
                "gpt-4.1",
            ),
            (
                ProviderKind::Anthropic(
                    AnthropicConfig::new(key.clone()).with_base_url(base_url.clone()),
                ),
                "claude-test",
            ),
            (
                ProviderKind::Gemini(GeminiConfig::new(key).with_base_url(base_url.clone())),
                MODEL_ID,
            ),
            (
                ProviderKind::Ollama(
                    OllamaConfig::local()
                        .with_base_url(base_url.join("api").expect("API URL should parse"))
                        .expect("Ollama API base should validate"),
                ),
                "chat-model",
            ),
        ];
        let request = GenerateRequest::new(vec![
            Message::user_text("sealed").expect("message should validate"),
        ])
        .expect("request should validate");
        let embedding =
            EmbedRequest::new(vec!["sealed".to_string()]).expect("embedding should validate");
        let batch = BatchSubmitRequest::Inline {
            rows: vec![BatchInput::new("row-1", request.clone()).expect("row should validate")],
        };

        for (kind, exact_id) in kinds {
            let provider =
                Provider::from_config(ProviderConfig::new(kind, TransportConfig::default()))
                    .expect("provider should construct without I/O");
            let id = ModelId::new(exact_id).expect("exact model id should validate");
            let model = provider.model(id.clone()).expect("model should select");
            let mut models = vec![model.clone(), model];
            let barrier = Arc::new(Barrier::new(2));
            let selecting_provider = provider.clone();
            let selecting_barrier = Arc::clone(&barrier);
            let selecting_id = id.clone();
            let selecting = tokio::spawn(async move {
                selecting_barrier.wait().await;
                selecting_provider.model(selecting_id)
            });
            let closing_provider = provider.clone();
            let closing = tokio::spawn(async move {
                barrier.wait().await;
                closing_provider.shutdown().await
            });
            let (selected, closed) = tokio::join!(selecting, closing);
            closed
                .expect("shutdown task should join")
                .expect("provider shutdown should succeed");
            match selected.expect("model selection task should join") {
                Ok(model) => {
                    models.push(model.clone());
                    models.push(model);
                }
                Err(ref error) if error.kind() == ErrorKind::Closed => {}
                Err(error) => panic!("racing model selection failed unexpectedly: {error}"),
            }

            for model in models {
                assert_eq!(model.model_id(), &id);
                for (operation, result) in [
                    ("generate", model.generate(&request).await.map(|_| ())),
                    (
                        "generate_stream",
                        model.generate_stream(&request).await.map(|_| ()),
                    ),
                    ("embed", model.embed(&embedding).await.map(|_| ())),
                    ("submit_batch", model.submit_batch(&batch).await.map(|_| ())),
                    (
                        "open_session",
                        model
                            .open_session(SessionConfig::default())
                            .await
                            .map(|_| ()),
                    ),
                ] {
                    assert!(
                        matches!(result, Err(ref error) if error.kind() == ErrorKind::Closed),
                        "sealed {exact_id} Model {operation} must return Closed: {result:?}"
                    );
                }
            }
            assert!(
                matches!(provider.model(id), Err(ref error) if error.kind() == ErrorKind::Closed)
            );
            assert!(matches!(
                provider.list_models(ModelListRequest::default()).await,
                Err(ref error) if error.kind() == ErrorKind::Closed
            ));
            provider.shutdown().await.expect("shutdown should replay");
        }
    })
    .await
    .expect("provider construction and sealing races should be bounded");
}

#[tokio::test(flavor = "current_thread")]
async fn a_model_keeps_working_after_its_provider_handle_is_dropped() {
    tokio::time::timeout(DEADLINE, async {
        let server = BlockingGenerateServer::start().await;
        let provider = server.provider();
        let model = provider
            .model(ModelId::new(OPENAI_GENERATION_MODEL_ID).expect("model id should validate"))
            .expect("model should select");
        let session = provider
            .model(ModelId::new(OPENAI_SESSION_MODEL_ID).expect("session model id should validate"))
            .expect("session model should select")
            .open_session(
                SessionConfig::default()
                    .with_output_audio("audio/pcm;rate=24000", false)
                    .expect("OpenAI output audio should validate"),
            )
            .await
            .expect("session should open");
        session.close().await.expect("session should close");
        drop(provider);

        let running = tokio::spawn(async move {
            let request = GenerateRequest::new(vec![
                Message::user_text("still open").expect("message should validate"),
            ])
            .expect("request should validate");
            model.generate(&request).await
        });
        server.started.notified().await;
        server.release.notify_one();
        let response = running
            .await
            .expect("generation task should join")
            .expect("Model should retain the provider runtime");
        assert_eq!(response.model.as_str(), OPENAI_GENERATION_MODEL_ID);
        server.finish().await;
    })
    .await
    .expect("Model ownership proof should be bounded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_and_replays_with_cancelled_and_concurrent_waiters() {
    tokio::time::timeout(DEADLINE, async {
        for cancel_all in [true, false] {
            let server = BlockingGenerateServer::start().await;
            let provider = server.provider();
            let model = provider
                .model(ModelId::new(OPENAI_GENERATION_MODEL_ID).expect("model id should validate"))
                .expect("model should select before shutdown");
            let mut session = provider
                .model(ModelId::new(OPENAI_SESSION_MODEL_ID).expect("session model id should validate"))
                .expect("session model should select")
                .open_session(
                    SessionConfig::default()
                        .with_output_audio("audio/pcm;rate=24000", false)
                        .expect("OpenAI output audio should validate"),
                )
                .await
                .expect("session should open before shutdown");

            let running_model = model.clone();
            let running = tokio::spawn(async move {
                let request = GenerateRequest::new(vec![
                    Message::user_text("wait").expect("message should validate"),
                ])
                .expect("request should validate");
                running_model.generate(&request).await
            });
            server.started.notified().await;

            let (entered, mut observed) = mpsc::channel(3);
            let mut waiters = Vec::new();
            for _ in 0..3 {
                let provider = provider.clone();
                let entered = entered.clone();
                waiters.push(tokio::spawn(async move {
                    let mut shutdown = Box::pin(provider.shutdown());
                    assert!(futures_util::poll!(shutdown.as_mut()).is_pending());
                    entered.send(()).await.expect("waiter should be observed");
                    shutdown.await
                }));
                observed.recv().await.expect("shutdown waiter should enter");
            }
            drop(entered);
            assert!(matches!(
                provider.model(ModelId::new(OPENAI_GENERATION_MODEL_ID).expect("model id should validate")),
                Err(ref error) if error.kind() == ErrorKind::Closed
            ));
            assert!(waiters.iter().all(|waiter| !waiter.is_finished()));
            let cancelled = if cancel_all { 3 } else { 1 };
            for waiter in waiters.drain(..cancelled) {
                waiter.abort();
                assert!(
                    waiter
                        .await
                        .expect_err("aborted shutdown waiter should be cancelled")
                        .is_cancelled()
                );
            }

            let request = GenerateRequest::new(vec![
                Message::user_text("later").expect("message should validate"),
            ])
            .expect("request should validate");
            assert!(matches!(
                model.generate(&request).await,
                Err(ref error) if error.kind() == ErrorKind::Closed
            ));

            server.release.notify_one();
            running
                .await
                .expect("generation task should join")
                .expect("admitted generation should complete");
            for waiter in waiters {
                waiter
                    .await
                    .expect("surviving shutdown waiter should join")
                    .expect("concurrent shutdown waiter should receive success");
            }
            provider
                .shutdown()
                .await
                .expect("late shutdown waiter should replay success");
            provider
                .shutdown()
                .await
                .expect("repeated shutdown should replay success");

            assert!(matches!(
                session
                    .send(SessionInput::Text {
                        text: "sealed".to_string(),
                    })
                    .await,
                Err(ref error) if error.kind() == ErrorKind::Closed
            ));
            session
                .close()
                .await
                .expect("session close must remain available");
            server.finish().await;
        }
    })
    .await
    .expect("provider shutdown waiter-loss scenario should be bounded");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_stream_opening_releases_provider_admission() {
    tokio::time::timeout(DEADLINE, async {
        let server = BlockingGenerateServer::start().await;
        let provider = server.provider();
        let model = provider
            .model(ModelId::new(OPENAI_GENERATION_MODEL_ID).expect("model id should validate"))
            .expect("model should select before shutdown");
        let session = provider
            .model(ModelId::new(OPENAI_SESSION_MODEL_ID).expect("session model id should validate"))
            .expect("session model should select")
            .open_session(
                SessionConfig::default()
                    .with_output_audio("audio/pcm;rate=24000", false)
                    .expect("OpenAI output audio should validate"),
            )
            .await
            .expect("session should open before shutdown");
        let opening = tokio::spawn(async move {
            let request = GenerateRequest::new(vec![
                Message::user_text("cancel opening").expect("message should validate"),
            ])
            .expect("request should validate");
            model.generate_stream(&request).await
        });
        server.started.notified().await;

        opening.abort();
        let error = match opening.await {
            Err(error) => error,
            Ok(_) => panic!("opening task should be cancelled"),
        };
        assert!(error.is_cancelled());
        provider
            .shutdown()
            .await
            .expect("cancelled opening must release provider admission");
        session
            .close()
            .await
            .expect("session close must remain available after sealing");
        server.release.notify_one();
        server.finish().await;
    })
    .await
    .expect("stream-opening cancellation proof should be bounded");
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_waits_for_returned_streams_until_completion_error_or_drop() {
    tokio::time::timeout(DEADLINE, async {
        for (body, expect_error) in [(STREAM_BODY, false), ("data: {malformed}\n\n", true)] {
            let mut server = HeldStreamServer::start(body).await;
            let provider = server.provider();
            let model = provider
                .model(ModelId::new(MODEL_ID).expect("model id should validate"))
                .expect("model should select before shutdown");
            let request = GenerateRequest::new(vec![
                Message::user_text("stream").expect("message should validate"),
            ])
            .expect("request should validate");
            let mut terminal_stream = model
                .generate_stream(&request)
                .await
                .expect("first stream should open");
            let dropped_stream = model
                .generate_stream(&request)
                .await
                .expect("second stream should open");
            server.wait_for_both().await;

            let shutdown_provider = provider.clone();
            let mut shutdown = tokio::spawn(async move { shutdown_provider.shutdown().await });
            let later_id = ModelId::new(MODEL_ID).expect("model id should validate");
            loop {
                match provider.model(later_id.clone()) {
                    Err(ref error) if error.kind() == ErrorKind::Closed => break,
                    Ok(_) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected model admission error: {error}"),
                }
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                    .await
                    .is_err(),
                "returned streams must retain admission"
            );

            server.release_first.notify_one();
            let terminal = loop {
                match terminal_stream.next().await {
                    Some(Ok(GenerateEvent::Completed(_))) => break Ok(()),
                    Some(Err(error)) => break Err(error),
                    Some(Ok(_)) => {}
                    None => panic!("first stream must publish a terminal event"),
                }
            };
            if expect_error {
                assert!(matches!(terminal, Err(ref error) if matches!(error.kind(), ErrorKind::InvalidResponse(_))));
            } else {
                terminal.expect("first stream should reach terminal completion");
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                    .await
                    .is_err(),
                "the second returned stream must continue blocking shutdown"
            );

            drop(dropped_stream);
            shutdown
                .await
                .expect("shutdown task should join")
                .expect("shutdown should complete after stream drop");
            drop(terminal_stream);
            provider
                .shutdown()
                .await
                .expect("terminal shutdown should replay");
            server.finish().await;
        }
    })
    .await
    .expect("provider returned-stream shutdown scenario should be bounded");
}
