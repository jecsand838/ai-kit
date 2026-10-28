use providers_kit::error::{ErrorKind, ErrorPhase, Operation};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use providers_kit::batch::{
    BatchInput, BatchJob, BatchListRequest, BatchPage, BatchResult, BatchStatus, BatchSubmitRequest,
};
use providers_kit::embedding::{EmbedRequest, EmbedResponse};
use providers_kit::files::{FileListRequest, FileObject, FilePage, FileState, FileUploadRequest};
use providers_kit::generation::{
    ContentPart, FinishReason, FunctionCall, FunctionResult, GenerateEvent, GenerateEventStream,
    GenerateRequest, GenerateResponse, Message, Role, Usage,
};
use providers_kit::models::{ModelBackend, ModelId, ModelListRequest, ModelPage, ModelSummary};
use providers_kit::runtime::HealthStatus;
use providers_kit::session::{SessionBackend, SessionConfig, SessionEvent, SessionInput};
use providers_kit::{KitError, ProviderBackend};

struct CustomProvider;

struct CustomModel {
    id: ModelId,
    continuation: CustomContinuation,
    stream_probe: Arc<StreamProbe>,
}

// The public contract requires Clone, not Debug, equality or serde. Identity
// here detects replacement of the exact returned continuation during replay.
#[derive(Clone)]
struct CustomContinuation(Arc<()>);

#[derive(Default)]
struct StreamProbe {
    polls: AtomicUsize,
    drops: AtomicUsize,
}

struct CustomStream {
    events: VecDeque<Result<GenerateEvent<CustomContinuation>, KitError>>,
    probe: Arc<StreamProbe>,
}

impl Stream for CustomStream {
    type Item = Result<GenerateEvent<CustomContinuation>, KitError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        stream.probe.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(stream.events.pop_front())
    }
}

impl Drop for CustomStream {
    fn drop(&mut self) {
        self.probe.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct CustomSession {
    id: String,
    model_id: ModelId,
    events: VecDeque<SessionEvent>,
}

impl CustomModel {
    fn response(
        &self,
        text: impl Into<String>,
    ) -> Result<GenerateResponse<CustomContinuation>, KitError> {
        Ok(GenerateResponse {
            model: self.id.clone(),
            message: Message::assistant_text(text)?.with_continuation(self.continuation.clone())?,
            finish_reason: Some(FinishReason::Stop),
            citations: Vec::new(),
            artifacts: Vec::new(),
            hosted_tool_outputs: Vec::new(),
            usage: Usage::default(),
        })
    }

    fn function_response(&self) -> Result<GenerateResponse<CustomContinuation>, KitError> {
        let message = Message::assistant(
            vec![
                ContentPart::Thinking {
                    text: Some("retained thinking".to_owned()),
                },
                ContentPart::Text {
                    text: "authoritative function request".to_owned(),
                },
            ],
            vec![FunctionCall::new(
                Some("caller-call".to_owned()),
                "resolve",
                serde_json::json!({"key": "answer"}),
            )?],
        )?
        .with_continuation(self.continuation.clone())?;
        Ok(GenerateResponse {
            model: self.id.clone(),
            message,
            finish_reason: Some(FinishReason::ToolCall),
            citations: Vec::new(),
            artifacts: Vec::new(),
            hosted_tool_outputs: Vec::new(),
            usage: Usage {
                input_tokens: Some(7),
                output_tokens: Some(3),
                total_tokens: Some(10),
                ..Usage::default()
            },
        })
    }

    fn assert_replay(&self, request: &GenerateRequest<CustomContinuation>) {
        let messages = request.messages();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role(), Role::User);
        let assistant = &messages[1];
        let expected = self
            .function_response()
            .expect("expected response is valid");
        assert_eq!(assistant.role(), Role::Assistant);
        assert_eq!(assistant.content(), expected.message.content());
        assert_eq!(
            assistant.function_calls(),
            expected.message.function_calls()
        );
        assert!(Arc::ptr_eq(
            &assistant
                .continuation()
                .expect("exact replay continuation")
                .0,
            &self.continuation.0,
        ));
        assert_eq!(messages[2].role(), Role::Tool);
        assert_eq!(messages[2].function_results().len(), 1);
        let result = &messages[2].function_results()[0];
        assert_eq!(result.call_id(), Some("caller-call"));
        assert_eq!(result.name(), "resolve");
        assert_eq!(
            result.content(),
            [ContentPart::Json {
                value: serde_json::json!({"answer": 42}),
            }]
        );
        assert!(!result.is_error());
    }
}

impl ProviderBackend for CustomProvider {
    type Model = CustomModel;

    fn model(&self, id: ModelId) -> Result<Self::Model, KitError> {
        Ok(CustomModel {
            id,
            continuation: CustomContinuation(Arc::new(())),
            stream_probe: Arc::new(StreamProbe::default()),
        })
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, KitError> {
        assert!(request.cursor().is_none());
        Ok(ModelPage {
            models: vec![ModelSummary {
                id: ModelId::new("custom-model")?,
                display_name: Some("Custom model".to_string()),
                description: None,
                context_window_tokens: Some(4_096),
            }],
            next_cursor: None,
        })
    }

    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, KitError> {
        Ok(ModelSummary {
            id,
            display_name: Some("Custom model".to_string()),
            description: None,
            context_window_tokens: Some(4_096),
        })
    }

    async fn upload_file(&self, request: FileUploadRequest) -> Result<FileObject, KitError> {
        Ok(FileObject {
            id: "file-1".to_string(),
            file_name: Some(request.file_name().to_string()),
            size_bytes: Some(request.data().len() as u64),
            media_type: Some(request.media_type().to_string()),
            created_at: None,
            state: Some(FileState::Active),
            input_reference: None,
        })
    }

    async fn list_files(&self, _request: FileListRequest) -> Result<FilePage, KitError> {
        Ok(FilePage {
            files: Vec::new(),
            next_cursor: None,
        })
    }

    async fn file_info(&self, id: String) -> Result<FileObject, KitError> {
        Ok(FileObject {
            id,
            file_name: None,
            size_bytes: None,
            media_type: None,
            created_at: None,
            state: Some(FileState::Active),
            input_reference: None,
        })
    }

    async fn delete_file(&self, id: String) -> Result<(), KitError> {
        assert_eq!(id, "file-1");
        Ok(())
    }

    async fn download_file(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<u8>, KitError> {
        assert_eq!(id, "file-1");
        assert!(max_bytes.get() >= 3);
        Ok(b"abc".to_vec())
    }

    async fn batch_status(&self, id: String) -> Result<BatchJob, KitError> {
        Ok(batch_job(id, BatchStatus::Running))
    }

    async fn cancel_batch(&self, id: String) -> Result<BatchJob, KitError> {
        Ok(batch_job(id, BatchStatus::Cancelled))
    }

    async fn batch_results(
        &self,
        id: String,
        _max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<CustomContinuation>>, KitError> {
        assert_eq!(id, "batch-1");
        Ok(vec![BatchResult::Cancelled {
            custom_id: "row-1".to_string(),
        }])
    }

    async fn list_batches(&self, _request: BatchListRequest) -> Result<BatchPage, KitError> {
        Ok(BatchPage {
            jobs: vec![batch_job("batch-1".to_string(), BatchStatus::Running)],
            next_cursor: None,
        })
    }

    async fn health(&self) -> Result<HealthStatus, KitError> {
        Ok(HealthStatus {
            latency: Duration::ZERO,
        })
    }

    async fn shutdown(&self) -> Result<(), KitError> {
        Ok(())
    }
}

impl ModelBackend for CustomModel {
    type Continuation = CustomContinuation;
    type Session = CustomSession;

    fn model_id(&self) -> &ModelId {
        &self.id
    }

    async fn generate(
        &self,
        request: &GenerateRequest<CustomContinuation>,
    ) -> Result<GenerateResponse<CustomContinuation>, KitError> {
        if request.messages().last().map(Message::role) == Some(Role::Tool) {
            self.assert_replay(request);
        }
        self.response(format!(
            "response to {} message(s)",
            request.messages().len()
        ))
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<CustomContinuation>,
    ) -> Result<GenerateEventStream<CustomContinuation>, KitError> {
        assert_eq!(request.messages().len(), 1);
        Ok(GenerateEventStream::new(CustomStream {
            events: VecDeque::from([
                Ok(GenerateEvent::Started {
                    model: self.id.clone(),
                }),
                Ok(GenerateEvent::ContentDelta {
                    delta: "first partial".to_owned(),
                }),
                Ok(GenerateEvent::ContentDelta {
                    delta: "second partial".to_owned(),
                }),
                Ok(GenerateEvent::Completed(Box::new(
                    self.function_response()?,
                ))),
            ]),
            probe: Arc::clone(&self.stream_probe),
        }))
    }

    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, KitError> {
        Ok(EmbedResponse {
            vectors: request
                .inputs()
                .iter()
                .enumerate()
                .map(|(index, _)| vec![index as f32])
                .collect(),
            usage: Usage::default(),
        })
    }

    async fn submit_batch(
        &self,
        _request: &BatchSubmitRequest<CustomContinuation>,
    ) -> Result<BatchJob, KitError> {
        Ok(batch_job("batch-1".to_string(), BatchStatus::Queued))
    }

    async fn open_session(&self, _config: SessionConfig) -> Result<Self::Session, KitError> {
        Ok(CustomSession {
            id: "session-1".to_string(),
            model_id: self.id.clone(),
            events: VecDeque::new(),
        })
    }
}

impl SessionBackend for CustomSession {
    fn id(&self) -> &str {
        &self.id
    }

    fn model_id(&self) -> &ModelId {
        &self.model_id
    }

    async fn send(&mut self, input: SessionInput) -> Result<(), KitError> {
        match input {
            SessionInput::Text { text } => {
                self.events
                    .push_back(SessionEvent::TextDelta { delta: text });
                self.events.push_back(SessionEvent::Completed);
                Ok(())
            }
            _ => Err(KitError::new(
                ErrorKind::Unsupported,
                Operation::SessionSend,
                ErrorPhase::InputValidation,
            )),
        }
    }

    async fn recv(&mut self) -> Result<SessionEvent, KitError> {
        self.events.pop_front().ok_or_else(|| {
            KitError::new(
                ErrorKind::InvalidState,
                Operation::SessionReceive,
                ErrorPhase::Lifecycle,
            )
        })
    }

    async fn close(self) -> Result<(), KitError> {
        Ok(())
    }
}

fn batch_job(id: String, status: BatchStatus) -> BatchJob {
    BatchJob {
        id,
        status,
        created_at: None,
        completed_at: None,
        usage: None,
    }
}

fn assert_hierarchy<P>()
where
    P: ProviderBackend<Model = CustomModel>,
    CustomModel: ModelBackend<Continuation = CustomContinuation, Session = CustomSession>,
    CustomSession: SessionBackend,
{
}

#[tokio::test]
async fn caller_hierarchy_is_used_directly_without_sdk_normalization() {
    assert_hierarchy::<CustomProvider>();
    let provider = CustomProvider;
    let models = provider
        .list_models(ModelListRequest::default())
        .await
        .expect("direct provider should list models");
    assert_eq!(models.models.len(), 1);
    let summary = provider
        .model_info(ModelId::new("custom-model").expect("model id should validate"))
        .await
        .expect("direct provider should fetch model metadata");
    assert_eq!(summary.id.as_str(), "custom-model");

    let uploaded = provider
        .upload_file(
            FileUploadRequest::new("data.txt", "text/plain", b"abc".to_vec())
                .expect("upload should validate"),
        )
        .await
        .expect("direct provider should upload");
    assert_eq!(uploaded.id, "file-1");
    assert!(
        provider
            .list_files(FileListRequest::default())
            .await
            .expect("file list should run")
            .files
            .is_empty()
    );
    assert_eq!(
        provider
            .file_info("file-1".to_string())
            .await
            .expect("file metadata should run")
            .id,
        "file-1"
    );
    assert_eq!(
        provider
            .download_file(
                "file-1".to_string(),
                NonZeroUsize::new(8).expect("nonzero limit")
            )
            .await
            .expect("file download should run"),
        b"abc"
    );
    provider
        .delete_file("file-1".to_string())
        .await
        .expect("file delete should run");

    assert!(matches!(
        provider
            .batch_status("batch-1".to_string())
            .await
            .expect("batch status should run")
            .status,
        BatchStatus::Running
    ));
    assert!(matches!(
        provider
            .cancel_batch("batch-1".to_string())
            .await
            .expect("batch cancel should run")
            .status,
        BatchStatus::Cancelled
    ));
    let results = provider
        .batch_results(
            "batch-1".to_string(),
            NonZeroUsize::new(64).expect("nonzero limit"),
        )
        .await
        .expect("batch results should run");
    assert!(matches!(
        &results[0],
        BatchResult::Cancelled { custom_id } if custom_id == "row-1"
    ));
    assert_eq!(
        provider
            .list_batches(BatchListRequest::default())
            .await
            .expect("batch list should run")
            .jobs
            .len(),
        1
    );
    assert_eq!(
        provider.health().await.expect("health should run").latency,
        Duration::ZERO
    );

    let model = provider
        .model(ModelId::new("custom-model").expect("model id should validate"))
        .expect("provider should return its exact model");
    let request = GenerateRequest::new(vec![
        Message::<CustomContinuation>::user_text("hello").expect("message should validate"),
    ])
    .expect("request should validate");
    let response = model
        .generate(&request)
        .await
        .expect("generation should run");
    assert_eq!(response.model, *model.model_id());
    assert!(Arc::ptr_eq(
        &response.message.continuation().expect("continuation").0,
        &model.continuation.0,
    ));
    let embedded = model
        .embed(&EmbedRequest::new(vec!["hello".to_string()]).expect("embed should validate"))
        .await
        .expect("embedding should run");
    assert_eq!(embedded.vectors, vec![vec![0.0]]);
    let submitted = model
        .submit_batch(&BatchSubmitRequest::Inline {
            rows: vec![BatchInput::new("row-1", request).expect("batch row should validate")],
        })
        .await
        .expect("batch submission should run");
    assert_eq!(submitted.id, "batch-1");

    let mut session = model
        .open_session(SessionConfig::default())
        .await
        .expect("model should return its exact session");
    assert_eq!(session.id(), "session-1");
    assert_eq!(session.model_id(), model.model_id());
    session
        .send(SessionInput::Text {
            text: "echo".to_string(),
        })
        .await
        .expect("session input should send");
    assert_eq!(
        session.recv().await.expect("text event should exist"),
        SessionEvent::TextDelta {
            delta: "echo".to_string()
        }
    );
    assert_eq!(
        session.recv().await.expect("completion should exist"),
        SessionEvent::Completed
    );
    session.close().await.expect("consuming close should run");
    provider
        .shutdown()
        .await
        .expect("direct provider shutdown should run");
}

#[tokio::test]
async fn caller_stream_preserves_exact_continuation_and_normalized_function_replay() {
    let provider = CustomProvider;
    let model = provider
        .model(ModelId::new("custom-model").expect("model id should validate"))
        .expect("caller model should construct");
    let request = GenerateRequest::new(vec![
        Message::user_text("begin").expect("user message should validate"),
    ])
    .expect("request should validate");
    let mut stream = model
        .generate_stream(&request)
        .await
        .expect("caller should construct a public stream");
    assert_eq!(model.stream_probe.polls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        stream.next().await,
        Some(Ok(GenerateEvent::Started { model: id })) if id == *model.model_id()
    ));
    assert_eq!(model.stream_probe.polls.load(Ordering::SeqCst), 1);
    for (index, expected) in ["first partial", "second partial"].into_iter().enumerate() {
        assert!(matches!(
            stream.next().await,
            Some(Ok(GenerateEvent::ContentDelta { delta })) if delta == expected
        ));
        assert_eq!(model.stream_probe.polls.load(Ordering::SeqCst), index + 2);
    }
    let response = match stream.next().await {
        Some(Ok(GenerateEvent::Completed(response))) => response,
        _ => panic!("the exact completed response must follow both partial deltas"),
    };
    assert_eq!(model.stream_probe.polls.load(Ordering::SeqCst), 4);
    assert_eq!(response.model, *model.model_id());
    assert_eq!(response.finish_reason, Some(FinishReason::ToolCall));
    assert_eq!(response.usage.input_tokens, Some(7));
    assert_eq!(response.usage.output_tokens, Some(3));
    assert_eq!(response.usage.total_tokens, Some(10));
    assert_eq!(
        response.message.content(),
        model
            .function_response()
            .expect("authoritative response should validate")
            .message
            .content()
    );
    assert!(Arc::ptr_eq(
        &response.message.continuation().expect("continuation").0,
        &model.continuation.0,
    ));
    assert!(serde_json::to_value(&response.message).is_err());
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(model.stream_probe.drops.load(Ordering::SeqCst), 1);

    let mut messages = request.messages().to_vec();
    messages.push(response.message);
    messages.push(
        Message::tool(vec![
            FunctionResult::new(
                Some("caller-call".to_owned()),
                "resolve",
                vec![ContentPart::Json {
                    value: serde_json::json!({"answer": 42}),
                }],
                false,
            )
            .expect("function result should validate"),
        ])
        .expect("tool message should validate"),
    );
    let continued = GenerateRequest::new(messages).expect("continued request should validate");
    let response = model
        .generate(&continued)
        .await
        .expect("caller Model should receive exact assistant replay and correlated result");
    assert_eq!(
        response.message.content(),
        [ContentPart::Text {
            text: "response to 3 message(s)".to_owned(),
        }]
    );
    provider
        .shutdown()
        .await
        .expect("provider should shut down");
}

#[tokio::test]
async fn dropping_caller_stream_releases_unpolled_or_partial_source() {
    let provider = CustomProvider;
    let model = provider
        .model(ModelId::new("custom-model").expect("model id should validate"))
        .expect("caller model should construct");
    let request = GenerateRequest::new(vec![
        Message::user_text("begin").expect("user message should validate"),
    ])
    .expect("request should validate");
    let stream = model
        .generate_stream(&request)
        .await
        .expect("unpolled stream should construct");
    drop(stream);
    assert_eq!(model.stream_probe.polls.load(Ordering::SeqCst), 0);
    assert_eq!(model.stream_probe.drops.load(Ordering::SeqCst), 1);

    let mut stream = model
        .generate_stream(&request)
        .await
        .expect("partially consumed stream should construct");
    assert!(matches!(
        stream.next().await,
        Some(Ok(GenerateEvent::Started { .. }))
    ));
    assert!(matches!(
        stream.next().await,
        Some(Ok(GenerateEvent::ContentDelta { .. }))
    ));
    drop(stream);
    assert_eq!(model.stream_probe.polls.load(Ordering::SeqCst), 2);
    assert_eq!(model.stream_probe.drops.load(Ordering::SeqCst), 2);
    provider
        .shutdown()
        .await
        .expect("provider should shut down");
}

#[tokio::test]
async fn optional_operations_default_to_unsupported_and_sessionless_models_use_infallible() {
    struct MinimalProvider;
    struct MinimalModel(ModelId);

    impl ProviderBackend for MinimalProvider {
        type Model = MinimalModel;

        fn model(&self, id: ModelId) -> Result<Self::Model, KitError> {
            Ok(MinimalModel(id))
        }

        async fn shutdown(&self) -> Result<(), KitError> {
            Ok(())
        }
    }

    impl ModelBackend for MinimalModel {
        type Continuation = ();
        type Session = Infallible;

        fn model_id(&self) -> &ModelId {
            &self.0
        }

        async fn generate(
            &self,
            _request: &GenerateRequest<()>,
        ) -> Result<GenerateResponse<()>, KitError> {
            Err(KitError::new(
                ErrorKind::Unsupported,
                Operation::Generate,
                ErrorPhase::InputValidation,
            ))
        }

        async fn generate_stream(
            &self,
            _request: &GenerateRequest<()>,
        ) -> Result<GenerateEventStream<()>, KitError> {
            Err(KitError::new(
                ErrorKind::Unsupported,
                Operation::GenerateStream,
                ErrorPhase::InputValidation,
            ))
        }

        async fn open_session(&self, _config: SessionConfig) -> Result<Infallible, KitError> {
            Err(KitError::new(
                ErrorKind::Unsupported,
                Operation::OpenSession,
                ErrorPhase::InputValidation,
            ))
        }
    }

    let provider = MinimalProvider;
    assert!(matches!(
        provider.list_models(ModelListRequest::default()).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    assert!(matches!(
        provider.list_files(FileListRequest::default()).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    let model = provider
        .model(ModelId::new("minimal").expect("model id should validate"))
        .expect("minimal model should construct");
    assert!(matches!(
        model
            .embed(&EmbedRequest::new(vec!["input".to_string()]).expect("embed should validate"))
            .await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
}
