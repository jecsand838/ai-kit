//! Caller-owned public-trait implementations, independent of provider services.
mod mock_support;

use std::collections::VecDeque;
use std::convert::Infallible;
use std::marker::PhantomPinned;
use std::num::{NonZeroU32, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use futures_util::{StreamExt, stream};
use mock_support::{
    Alpha, Beta, CallerProvider, Continuation, Probe, Reply, Request, ScriptedProvider, Step,
};
use providers_kit::batch::{
    BatchInput, BatchJob, BatchListRequest, BatchPage, BatchResult, BatchStatus, BatchSubmitRequest,
};
use providers_kit::embedding::{EmbedRequest, EmbedResponse};
use providers_kit::error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation, RemoteFailure};
use providers_kit::files::{FileListRequest, FileObject, FilePage, FileState, FileUploadRequest};
use providers_kit::generation::{
    Artifact, Citation, ContentPart, FileReference, FinishReason, FunctionCall, FunctionResult,
    GenerateEvent, GenerateEventStream, GenerateRequest, GenerateResponse, GenerationConfig,
    JsonSchema, Message, ProviderFunctionTool, ProviderToolSpec, ToolChoice, Usage,
};
use providers_kit::hosted::{
    HostedCodeExecutionResult, HostedExecutableCode, HostedFileSearchResult, HostedTool,
    HostedToolOutput, HostedToolStatus, HostedWebSearchAction, HostedWebSearchResult,
    HostedWebSearchSource,
};
use providers_kit::models::{ModelId, ModelListRequest, ModelPage, ModelSummary};
use providers_kit::runtime::HealthStatus;
use providers_kit::session::{SessionConfig, SessionEvent, SessionInput, SessionTurnDetection};
use providers_kit::{KitError, ModelBackend, ProviderBackend, SessionBackend};

struct Case;

enum SessionExchange {
    Input(SessionInput),
    Event(SessionEvent),
}

#[derive(Default)]
struct WakeProbe(AtomicUsize);

impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Case {
    fn id() -> ModelId {
        ModelId::new("caller-exact-model-v2").unwrap()
    }
    fn bound() -> NonZeroUsize {
        NonZeroUsize::new(4096).unwrap()
    }
    fn error(operation: Operation) -> KitError {
        KitError::new(ErrorKind::Transport, operation, ErrorPhase::RequestSend)
            .with_request_target("fixture-resource")
            .with_correlation_id("fixture-correlation")
    }
    fn summary() -> ModelSummary {
        ModelSummary {
            id: Self::id(),
            display_name: Some("Caller model".into()),
            description: Some("Exact selection".into()),
            context_window_tokens: Some(4096),
        }
    }
    fn file() -> FileObject {
        FileObject {
            id: "file-opaque".into(),
            file_name: Some("input.txt".into()),
            size_bytes: Some(3),
            media_type: Some("text/plain".into()),
            created_at: Some("2026-01-01".into()),
            state: Some(FileState::Active),
            input_reference: None,
        }
    }
    fn upload() -> FileUploadRequest {
        FileUploadRequest::new("input.txt", "text/plain", b"abc".to_vec()).unwrap()
    }
    fn batch(status: BatchStatus) -> BatchJob {
        BatchJob {
            id: "batch-opaque".into(),
            status,
            created_at: None,
            completed_at: None,
            usage: None,
        }
    }
    fn config() -> SessionConfig {
        SessionConfig::default()
            .with_instructions("Keep exact caller instructions")
            .unwrap()
            .with_input_audio("audio/pcm", true)
            .unwrap()
            .with_output_audio("audio/pcm", true)
            .unwrap()
            .with_turn_detection(SessionTurnDetection::Manual)
            .with_functions(vec![Self::function()], Some(ToolChoice::Auto))
            .unwrap()
    }
    fn function() -> ProviderFunctionTool {
        ProviderFunctionTool::new(
            "resolve",
            Some("Resolve a caller record".into()),
            JsonSchema::new(serde_json::json!({"type":"object","properties":{"key":{"type":"integer"}},"required":["key"]})).unwrap(),
        ).unwrap()
    }
    fn input() -> SessionInput {
        SessionInput::Text {
            text: "session turn".into(),
        }
    }
    fn request<C: Clone>(token: C) -> GenerateRequest<C> {
        GenerateRequest::new(vec![
            Message::user_text("Resolve record").unwrap(),
            Message::assistant(
                vec![ContentPart::Thinking {
                    text: Some("retained".into()),
                }],
                vec![
                    FunctionCall::new(
                        Some("call-7".into()),
                        "resolve",
                        serde_json::json!({"key":42}),
                    )
                    .unwrap(),
                ],
            )
            .unwrap()
            .with_continuation(token)
            .unwrap(),
            Message::tool(vec![
                FunctionResult::new(
                    Some("call-7".into()),
                    "resolve",
                    vec![ContentPart::Json {
                        value: serde_json::json!({"found":true}),
                    }],
                    false,
                )
                .unwrap(),
            ])
            .unwrap(),
        ])
        .unwrap()
        .with_tools(vec![
            ProviderToolSpec::Function(Self::function()),
            ProviderToolSpec::Hosted(HostedTool::WebSearch),
            ProviderToolSpec::Hosted(HostedTool::UrlContext),
            ProviderToolSpec::Hosted(HostedTool::FileSearch {
                store_ids: vec!["store-opaque".into()],
            }),
            ProviderToolSpec::Hosted(HostedTool::CodeExecution),
        ])
        .unwrap()
        .with_generation(
            GenerationConfig::default()
                .with_temperature(0.25)
                .unwrap()
                .with_top_p(0.9)
                .unwrap()
                .with_max_output_tokens(NonZeroU32::new(32).unwrap())
                .with_stop_sequences(vec!["STOP".into()])
                .unwrap()
                .with_seed(7),
        )
        .unwrap()
    }
    fn response<C>(token: C) -> GenerateResponse<C> {
        GenerateResponse {
            model: Self::id(),
            message: Message::assistant_text("record found")
                .unwrap()
                .with_continuation(token)
                .unwrap(),
            finish_reason: Some(FinishReason::Stop),
            citations: Self::citations(),
            artifacts: Self::artifacts(),
            hosted_tool_outputs: Self::hosted_outputs(),
            usage: Usage {
                input_tokens: Some(8),
                output_tokens: Some(3),
                total_tokens: Some(11),
                ..Usage::default()
            },
        }
    }
    fn citations() -> Vec<Citation> {
        vec![
            Citation::Url {
                url: "https://example.invalid/source".into(),
                title: Some("Record source".into()),
                snippet: Some("record found".into()),
                start_index: Some(0),
                end_index: Some(12),
            },
            Citation::File {
                reference: FileReference::Provider {
                    id: "file-cited".into(),
                    media_type: Some("text/plain".into()),
                },
                title: Some("Retained source".into()),
                snippet: Some("source excerpt".into()),
                start_index: Some(0),
                end_index: Some(12),
            },
        ]
    }
    fn artifacts() -> Vec<Artifact> {
        vec![
            Artifact::Inline {
                media_type: "application/octet-stream".into(),
                file_name: Some("result.bin".into()),
                description: Some("exact bytes".into()),
                data: vec![0, 1, 127, 128, 255],
            },
            Artifact::File {
                reference: FileReference::Provider {
                    id: "file-artifact".into(),
                    media_type: Some("text/plain".into()),
                },
                file_name: Some("result.txt".into()),
                description: Some("provider-owned result".into()),
            },
        ]
    }
    fn hosted_outputs() -> Vec<HostedToolOutput> {
        vec![
            HostedToolOutput::WebSearch {
                status: Some(HostedToolStatus::Completed),
                action: Some(HostedWebSearchAction::Search {
                    query: Some("record 42".into()),
                    queries: vec!["record 42".into()],
                    sources: vec![HostedWebSearchSource {
                        title: Some("Record source".into()),
                        url: "https://example.invalid/source".into(),
                    }],
                }),
                results: vec![HostedWebSearchResult {
                    title: Some("Record source".into()),
                    url: "https://example.invalid/source".into(),
                    snippet: Some("record found".into()),
                }],
                citations: Self::citations(),
            },
            HostedToolOutput::FileSearch {
                status: Some(HostedToolStatus::Completed),
                queries: vec!["record 42".into()],
                results: Some(vec![HostedFileSearchResult {
                    file_id: "file-cited".into(),
                    file_name: Some("input.txt".into()),
                    text: Some("source excerpt".into()),
                    score: Some(0.75),
                }]),
                citations: Self::citations(),
            },
            HostedToolOutput::CodeExecution {
                status: Some(HostedToolStatus::Completed),
                code: vec![HostedExecutableCode {
                    language: Some("python".into()),
                    code: "print(6 * 7)".into(),
                }],
                results: vec![HostedCodeExecutionResult {
                    status: Some(HostedToolStatus::Completed),
                    logs: Some("combined log".into()),
                    stdout: Some("42\n".into()),
                    stderr: Some(String::new()),
                    exit_code: Some(0),
                }],
                artifacts: Self::artifacts(),
            },
            HostedToolOutput::UrlContext {
                requested_url: Some("https://example.invalid/request".into()),
                retrieved_url: Some("https://example.invalid/source".into()),
                status: Some(HostedToolStatus::Completed),
                citations: Self::citations(),
            },
        ]
    }
    fn assert_response_payload<C>(response: &GenerateResponse<C>) {
        assert_eq!(response.model, Self::id());
        assert_eq!(
            response.message.content(),
            [ContentPart::Text {
                text: "record found".into()
            }]
        );
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        assert_eq!(response.citations, Self::citations());
        assert_eq!(response.artifacts, Self::artifacts());
        assert_eq!(response.hosted_tool_outputs, Self::hosted_outputs());
        assert_eq!(response.usage.total_tokens, Some(11));
    }
    fn events<C>(token: C) -> VecDeque<Result<GenerateEvent<C>, KitError>> {
        let mut events = VecDeque::from([
            Ok(GenerateEvent::Started { model: Self::id() }),
            Ok(GenerateEvent::ThinkingDelta {
                delta: "thinking".into(),
            }),
            Ok(GenerateEvent::ContentDelta {
                delta: "record found".into(),
            }),
        ]);
        events.extend(
            Self::hosted_outputs()
                .into_iter()
                .map(|output| Ok(GenerateEvent::HostedTool(Box::new(output)))),
        );
        events.push_back(Ok(GenerateEvent::Usage(Usage::default())));
        events.push_back(Ok(GenerateEvent::Completed(Box::new(Self::response(
            token,
        )))));
        events
    }
    fn session_dialogue() -> Vec<SessionExchange> {
        vec![
            SessionExchange::Input(SessionInput::TurnStart),
            SessionExchange::Input(SessionInput::Audio {
                media_type: "audio/pcm".into(),
                data: vec![0, 1, 127, 128, 255, 0],
            }),
            SessionExchange::Input(SessionInput::TurnEnd),
            SessionExchange::Event(SessionEvent::InputTranscriptionDelta {
                delta: "resolve forty-two".into(),
            }),
            SessionExchange::Event(SessionEvent::Audio {
                media_type: "audio/pcm".into(),
                data: vec![0, 128, 255, 127],
            }),
            SessionExchange::Event(SessionEvent::OutputTranscriptionDelta {
                delta: "checking the record".into(),
            }),
            SessionExchange::Event(SessionEvent::FunctionCall(
                FunctionCall::new(
                    Some("session-call".into()),
                    "resolve",
                    serde_json::json!({"key":42}),
                )
                .unwrap(),
            )),
            SessionExchange::Input(SessionInput::FunctionResult(
                FunctionResult::new(
                    Some("session-call".into()),
                    "resolve",
                    vec![ContentPart::Json {
                        value: serde_json::json!({"found":true}),
                    }],
                    false,
                )
                .unwrap(),
            )),
            SessionExchange::Event(SessionEvent::Usage(Usage {
                input_tokens: Some(5),
                output_tokens: Some(3),
                total_tokens: Some(8),
                ..Usage::default()
            })),
            SessionExchange::Event(SessionEvent::Completed),
            SessionExchange::Input(Self::input()),
            SessionExchange::Event(SessionEvent::TextDelta {
                delta: "response before cancellation".into(),
            }),
            SessionExchange::Event(SessionEvent::FunctionCall(
                FunctionCall::new(
                    Some("cancelled-call".into()),
                    "resolve",
                    serde_json::json!({"key":43}),
                )
                .unwrap(),
            )),
            SessionExchange::Input(SessionInput::CancelTurn),
            SessionExchange::Event(SessionEvent::FunctionCallsCancelled {
                call_ids: vec!["cancelled-call".into()],
            }),
            SessionExchange::Event(SessionEvent::TurnCancelled),
        ]
    }
    fn session_script<C>() -> Vec<Step<C>> {
        let mut steps = vec![
            Self::step(Request::Model(Self::id()), Reply::Unit),
            Self::step(
                Request::OpenSession(Self::config()),
                Reply::Session("session-media".into()),
            ),
        ];
        for exchange in Self::session_dialogue() {
            steps.push(match exchange {
                SessionExchange::Input(input) => {
                    Self::step(Request::Send("session-media".into(), input), Reply::Unit)
                }
                SessionExchange::Event(event) => {
                    Self::step(Request::Recv("session-media".into()), Reply::Event(event))
                }
            });
        }
        steps.push(Self::step(
            Request::Close("session-media".into()),
            Reply::Unit,
        ));
        steps.push(Self::step(Request::Shutdown, Reply::Unit));
        steps
    }
    async fn exercise_session<P: ProviderBackend>(provider: P) {
        let model = provider.model(Self::id()).unwrap();
        let mut session = model.open_session(Self::config()).await.unwrap();
        assert_eq!(session.id(), "session-media");
        assert_eq!(session.model_id(), &Self::id());
        for exchange in Self::session_dialogue() {
            match exchange {
                SessionExchange::Input(input) => session.send(input).await.unwrap(),
                SessionExchange::Event(expected) => {
                    assert_eq!(session.recv().await.unwrap(), expected)
                }
            }
        }
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
    }
    fn embed() -> EmbedRequest {
        EmbedRequest::new(vec!["first".into(), "second".into()])
            .unwrap()
            .with_dimensions(NonZeroU32::new(2).unwrap())
    }
    fn submission<C: Clone>(token: C) -> BatchSubmitRequest<C> {
        BatchSubmitRequest::Inline {
            rows: vec![BatchInput::new("row-success", Self::request(token)).unwrap()],
        }
    }
    fn step<C>(request: Request<C>, reply: Reply<C>) -> Step<C> {
        Step {
            request,
            reply: Ok(reply),
        }
    }
    fn script<C: Clone>(token: C) -> Vec<Step<C>> {
        vec![
            Self::step(
                Request::ListModels(ModelListRequest::default()),
                Reply::Models(ModelPage {
                    models: vec![Self::summary()],
                    next_cursor: Some("models-next".into()),
                }),
            ),
            Self::step(
                Request::ListModels(
                    ModelListRequest::default()
                        .with_cursor("models-next")
                        .unwrap(),
                ),
                Reply::Models(ModelPage {
                    models: vec![],
                    next_cursor: None,
                }),
            ),
            Self::step(
                Request::ModelInfo(Self::id()),
                Reply::ModelInfo(Self::summary()),
            ),
            Self::step(Request::Upload(Self::upload()), Reply::File(Self::file())),
            Self::step(
                Request::ListFiles(FileListRequest::default()),
                Reply::Files(FilePage {
                    files: vec![Self::file()],
                    next_cursor: Some("files-next".into()),
                }),
            ),
            Self::step(
                Request::ListFiles(
                    FileListRequest::default()
                        .with_cursor("files-next")
                        .unwrap(),
                ),
                Reply::Files(FilePage {
                    files: vec![],
                    next_cursor: None,
                }),
            ),
            Self::step(
                Request::FileInfo("file-opaque".into()),
                Reply::File(Self::file()),
            ),
            Self::step(
                Request::Download("file-opaque".into(), Self::bound()),
                Reply::Bytes(b"abc".to_vec()),
            ),
            Step {
                request: Request::Download("file-opaque".into(), NonZeroUsize::new(2).unwrap()),
                reply: Err(KitError::new(
                    ErrorKind::RequestTooLarge,
                    Operation::DownloadFile,
                    ErrorPhase::ReadBody,
                )),
            },
            Self::step(Request::Delete("file-opaque".into()), Reply::Unit),
            Self::step(
                Request::BatchStatus("batch-opaque".into()),
                Reply::Batch(Self::batch(BatchStatus::Running)),
            ),
            Self::step(
                Request::CancelBatch("batch-opaque".into()),
                Reply::Batch(Self::batch(BatchStatus::Cancelled)),
            ),
            Self::step(
                Request::BatchResults("batch-opaque".into(), Self::bound()),
                Reply::Results(vec![
                    BatchResult::Succeeded {
                        custom_id: "row-success".into(),
                        output: Box::new(Self::response(token.clone())),
                    },
                    BatchResult::Failed {
                        custom_id: "row-failed".into(),
                        error: RemoteFailure::new(
                            ErrorKind::NotFound,
                            NativeErrorDetails {
                                code: Some("missing".into()),
                                ..Default::default()
                            },
                        )
                        .unwrap(),
                    },
                    BatchResult::Cancelled {
                        custom_id: "row-cancelled".into(),
                    },
                    BatchResult::Expired {
                        custom_id: "row-expired".into(),
                    },
                ]),
            ),
            Self::step(
                Request::ListBatches(
                    BatchListRequest::default()
                        .with_cursor("batch-next")
                        .unwrap(),
                ),
                Reply::Batches(BatchPage {
                    jobs: vec![Self::batch(BatchStatus::Cancelled)],
                    next_cursor: None,
                }),
            ),
            Self::step(
                Request::Health,
                Reply::Health(HealthStatus {
                    latency: Duration::from_millis(2),
                }),
            ),
            Self::step(Request::Model(Self::id()), Reply::Unit),
            Self::step(
                Request::Generate(Self::request(token.clone())),
                Reply::Generate(Self::response(token.clone())),
            ),
            Self::step(
                Request::Stream(Self::request(token.clone())),
                Reply::Events(Self::events(token.clone())),
            ),
            Self::step(
                Request::Embed(Self::embed()),
                Reply::Embed(EmbedResponse {
                    vectors: vec![vec![0.1, 0.2], vec![0.3, 0.4]],
                    usage: Usage::default(),
                }),
            ),
            Self::step(
                Request::Submit(Self::submission(token)),
                Reply::Batch(Self::batch(BatchStatus::Queued)),
            ),
            Self::step(
                Request::Submit(BatchSubmitRequest::ProviderFile {
                    file_id: "file-opaque".into(),
                }),
                Reply::Batch(Self::batch(BatchStatus::Queued)),
            ),
            Self::step(
                Request::OpenSession(Self::config()),
                Reply::Session("session-opaque".into()),
            ),
            Self::step(
                Request::Send("session-opaque".into(), Self::input()),
                Reply::Unit,
            ),
            Self::step(
                Request::Recv("session-opaque".into()),
                Reply::Event(SessionEvent::TextDelta {
                    delta: "session answer".into(),
                }),
            ),
            Self::step(
                Request::Recv("session-opaque".into()),
                Reply::Event(SessionEvent::Completed),
            ),
            Self::step(Request::Close("session-opaque".into()), Reply::Unit),
            Self::step(Request::Shutdown, Reply::Unit),
        ]
    }
    async fn exercise<P, C>(provider: &P, token: C, inspect: impl Fn(&C))
    where
        P: ProviderBackend,
        P::Model: ModelBackend<Continuation = C>,
        C: Clone + Send + Sync + 'static,
    {
        let page = provider
            .list_models(ModelListRequest::default())
            .await
            .unwrap();
        assert_eq!(page.models, vec![Self::summary()]);
        assert!(
            provider
                .list_models(
                    ModelListRequest::default()
                        .with_cursor(page.next_cursor.unwrap())
                        .unwrap()
                )
                .await
                .unwrap()
                .models
                .is_empty()
        );
        assert_eq!(
            provider.model_info(Self::id()).await.unwrap(),
            Self::summary()
        );
        assert_eq!(
            provider.upload_file(Self::upload()).await.unwrap(),
            Self::file()
        );
        let page = provider
            .list_files(FileListRequest::default())
            .await
            .unwrap();
        assert_eq!(page.files, vec![Self::file()]);
        assert!(
            provider
                .list_files(
                    FileListRequest::default()
                        .with_cursor(page.next_cursor.unwrap())
                        .unwrap()
                )
                .await
                .unwrap()
                .files
                .is_empty()
        );
        assert_eq!(
            provider.file_info("file-opaque".into()).await.unwrap(),
            Self::file()
        );
        assert_eq!(
            provider
                .download_file("file-opaque".into(), Self::bound())
                .await
                .unwrap(),
            b"abc"
        );
        assert_eq!(
            provider
                .download_file("file-opaque".into(), NonZeroUsize::new(2).unwrap())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::RequestTooLarge
        );
        provider.delete_file("file-opaque".into()).await.unwrap();
        assert!(matches!(
            provider
                .batch_status("batch-opaque".into())
                .await
                .unwrap()
                .status,
            BatchStatus::Running
        ));
        assert!(matches!(
            provider
                .cancel_batch("batch-opaque".into())
                .await
                .unwrap()
                .status,
            BatchStatus::Cancelled
        ));
        let rows = provider
            .batch_results("batch-opaque".into(), Self::bound())
            .await
            .unwrap();
        assert_eq!(rows.len(), 4);
        for row in rows {
            match row {
                BatchResult::Succeeded { custom_id, output } => {
                    assert_eq!(custom_id, "row-success");
                    Self::assert_response_payload(&output);
                    inspect(output.message.continuation().unwrap());
                }
                BatchResult::Failed { custom_id, error } => {
                    assert_eq!(custom_id, "row-failed");
                    assert_eq!(error.kind(), ErrorKind::NotFound);
                    assert_eq!(error.native_details().code.as_deref(), Some("missing"));
                }
                BatchResult::Cancelled { custom_id } => assert_eq!(custom_id, "row-cancelled"),
                BatchResult::Expired { custom_id } => assert_eq!(custom_id, "row-expired"),
            }
        }
        let batches = provider
            .list_batches(
                BatchListRequest::default()
                    .with_cursor("batch-next")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(batches.jobs, vec![Self::batch(BatchStatus::Cancelled)]);
        assert!(batches.next_cursor.is_none());
        assert_eq!(
            provider.health().await.unwrap().latency,
            Duration::from_millis(2)
        );
        let model = provider.model(Self::id()).unwrap();
        assert_eq!(model.model_id(), &Self::id());
        let request = Self::request(token.clone());
        let response = model.generate(&request).await.unwrap();
        inspect(response.message.continuation().unwrap());
        Self::assert_response_payload(&response);
        let mut events = model.generate_stream(&request).await.unwrap();
        assert!(
            matches!(events.next().await.unwrap().unwrap(),GenerateEvent::Started{model} if model==Self::id())
        );
        assert!(
            matches!(events.next().await.unwrap().unwrap(),GenerateEvent::ThinkingDelta{delta} if delta=="thinking")
        );
        assert!(
            matches!(events.next().await.unwrap().unwrap(),GenerateEvent::ContentDelta{delta} if delta=="record found")
        );
        for expected in Self::hosted_outputs() {
            let GenerateEvent::HostedTool(actual) = events.next().await.unwrap().unwrap() else {
                panic!("hosted activity event")
            };
            assert_eq!(*actual, expected);
        }
        assert!(matches!(
            events.next().await.unwrap().unwrap(),
            GenerateEvent::Usage(_)
        ));
        let GenerateEvent::Completed(response) = events.next().await.unwrap().unwrap() else {
            panic!("terminal response")
        };
        inspect(response.message.continuation().unwrap());
        Self::assert_response_payload(&response);
        assert!(events.next().await.is_none());
        drop(events);
        assert_eq!(
            model.embed(&Self::embed()).await.unwrap().vectors,
            vec![vec![0.1, 0.2], vec![0.3, 0.4]]
        );
        assert!(matches!(
            model
                .submit_batch(&Self::submission(token))
                .await
                .unwrap()
                .status,
            BatchStatus::Queued
        ));
        assert_eq!(
            model
                .submit_batch(&BatchSubmitRequest::ProviderFile {
                    file_id: "file-opaque".into()
                })
                .await
                .unwrap()
                .id,
            "batch-opaque"
        );
        let mut session = model.open_session(Self::config()).await.unwrap();
        assert_eq!(session.id(), "session-opaque");
        assert_eq!(session.model_id(), &Self::id());
        session.send(Self::input()).await.unwrap();
        assert_eq!(
            session.recv().await.unwrap(),
            SessionEvent::TextDelta {
                delta: "session answer".into()
            }
        );
        assert_eq!(session.recv().await.unwrap(), SessionEvent::Completed);
        session.close().await.unwrap();
        provider.shutdown().await.unwrap();
        provider.shutdown().await.unwrap();
        assert_eq!(
            provider.health().await.unwrap_err().kind(),
            ErrorKind::Closed
        );
        assert_eq!(
            model.generate(&request).await.err().unwrap().kind(),
            ErrorKind::Closed
        );
    }
    fn inspect_observations<C>(probe: &Probe<C>, inspect: impl Fn(&C)) {
        probe.observed(|requests| {
            for request in requests {
                match request {
                    Request::Generate(r) | Request::Stream(r) => {
                        inspect(r.messages()[1].continuation().unwrap())
                    }
                    Request::Submit(BatchSubmitRequest::Inline { rows }) => {
                        inspect(rows[0].request().messages()[1].continuation().unwrap())
                    }
                    _ => {}
                }
            }
        });
        probe.assert_complete();
        assert_eq!(probe.shutdown_count(), 1);
        assert_eq!(probe.close_count(), 1);
        assert_eq!(probe.dropped_session_count(), 1);
        assert_eq!(probe.dropped_stream_count(), 1);
    }
    fn ready<F: Future>(future: F) -> F::Output {
        let mut pinned = std::pin::pin!(future);
        match pinned
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("immediate fixture suspended"),
        }
    }

    fn failure_script<C: Clone>(token: C) -> Vec<Step<C>> {
        let requests = vec![
            (
                Request::Model(ModelId::new("missing-model").unwrap()),
                Operation::SelectModel,
            ),
            (
                Request::ListModels(ModelListRequest::default()),
                Operation::ListModels,
            ),
            (Request::ModelInfo(Self::id()), Operation::ModelInfo),
            (Request::Upload(Self::upload()), Operation::UploadFile),
            (
                Request::ListFiles(FileListRequest::default()),
                Operation::ListFiles,
            ),
            (Request::FileInfo("missing".into()), Operation::FileInfo),
            (Request::Delete("missing".into()), Operation::DeleteFile),
            (
                Request::Download("missing".into(), Self::bound()),
                Operation::DownloadFile,
            ),
            (
                Request::BatchStatus("missing".into()),
                Operation::BatchStatus,
            ),
            (
                Request::CancelBatch("missing".into()),
                Operation::CancelBatch,
            ),
            (
                Request::BatchResults("missing".into(), Self::bound()),
                Operation::BatchResults,
            ),
            (
                Request::ListBatches(BatchListRequest::default()),
                Operation::ListBatches,
            ),
            (Request::Health, Operation::Health),
        ];
        let mut steps: Vec<Step<C>> = requests
            .into_iter()
            .map(|(request, operation)| Step {
                request,
                reply: Err(Self::error(operation)),
            })
            .collect();
        steps.push(Self::step(Request::Model(Self::id()), Reply::Unit));
        for (request, operation) in [
            (
                Request::Generate(Self::request(token.clone())),
                Operation::Generate,
            ),
            (
                Request::Stream(Self::request(token.clone())),
                Operation::GenerateStream,
            ),
            (Request::Embed(Self::embed()), Operation::Embed),
            (
                Request::Submit(Self::submission(token)),
                Operation::SubmitBatch,
            ),
            (Request::OpenSession(Self::config()), Operation::OpenSession),
        ] {
            steps.push(Step {
                request,
                reply: Err(Self::error(operation)),
            });
        }
        steps.push(Self::step(
            Request::OpenSession(Self::config()),
            Reply::Session("failed-session".into()),
        ));
        for (request, operation) in [
            (
                Request::Send("failed-session".into(), Self::input()),
                Operation::SessionSend,
            ),
            (
                Request::Recv("failed-session".into()),
                Operation::SessionReceive,
            ),
            (
                Request::Close("failed-session".into()),
                Operation::SessionClose,
            ),
            (Request::Shutdown, Operation::Shutdown),
        ] {
            steps.push(Step {
                request,
                reply: Err(Self::error(operation)),
            });
        }
        steps
    }

    async fn exercise_failures<P, C>(provider: &P, token: C)
    where
        P: ProviderBackend,
        P::Model: ModelBackend<Continuation = C>,
        C: Clone + Send + Sync + 'static,
    {
        let mut errors = vec![
            (
                provider
                    .model(ModelId::new("missing-model").unwrap())
                    .err()
                    .unwrap(),
                Operation::SelectModel,
            ),
            (
                provider
                    .list_models(ModelListRequest::default())
                    .await
                    .unwrap_err(),
                Operation::ListModels,
            ),
            (
                provider.model_info(Self::id()).await.unwrap_err(),
                Operation::ModelInfo,
            ),
            (
                provider.upload_file(Self::upload()).await.unwrap_err(),
                Operation::UploadFile,
            ),
            (
                provider
                    .list_files(FileListRequest::default())
                    .await
                    .unwrap_err(),
                Operation::ListFiles,
            ),
            (
                provider.file_info("missing".into()).await.unwrap_err(),
                Operation::FileInfo,
            ),
            (
                provider.delete_file("missing".into()).await.unwrap_err(),
                Operation::DeleteFile,
            ),
            (
                provider
                    .download_file("missing".into(), Self::bound())
                    .await
                    .unwrap_err(),
                Operation::DownloadFile,
            ),
            (
                provider.batch_status("missing".into()).await.unwrap_err(),
                Operation::BatchStatus,
            ),
            (
                provider.cancel_batch("missing".into()).await.unwrap_err(),
                Operation::CancelBatch,
            ),
            (
                provider
                    .batch_results("missing".into(), Self::bound())
                    .await
                    .err()
                    .unwrap(),
                Operation::BatchResults,
            ),
            (
                provider
                    .list_batches(BatchListRequest::default())
                    .await
                    .unwrap_err(),
                Operation::ListBatches,
            ),
            (provider.health().await.unwrap_err(), Operation::Health),
        ];
        let model = provider.model(Self::id()).unwrap();
        errors.extend([
            (
                model
                    .generate(&Self::request(token.clone()))
                    .await
                    .err()
                    .unwrap(),
                Operation::Generate,
            ),
            (
                model
                    .generate_stream(&Self::request(token.clone()))
                    .await
                    .err()
                    .unwrap(),
                Operation::GenerateStream,
            ),
            (
                model.embed(&Self::embed()).await.unwrap_err(),
                Operation::Embed,
            ),
            (
                model
                    .submit_batch(&Self::submission(token))
                    .await
                    .unwrap_err(),
                Operation::SubmitBatch,
            ),
            (
                model.open_session(Self::config()).await.err().unwrap(),
                Operation::OpenSession,
            ),
        ]);
        let mut session = model.open_session(Self::config()).await.unwrap();
        errors.extend([
            (
                session.send(Self::input()).await.unwrap_err(),
                Operation::SessionSend,
            ),
            (session.recv().await.unwrap_err(), Operation::SessionReceive),
            (session.close().await.unwrap_err(), Operation::SessionClose),
            (provider.shutdown().await.unwrap_err(), Operation::Shutdown),
            (provider.shutdown().await.unwrap_err(), Operation::Shutdown),
        ]);
        for (error, operation) in errors {
            assert_eq!(error.kind(), ErrorKind::Transport);
            assert_eq!(error.context().operation, operation);
            assert_eq!(error.context().phase, ErrorPhase::RequestSend);
            assert_eq!(error.request_target(), Some("fixture-resource"));
            assert_eq!(error.correlation_id(), Some("fixture-correlation"));
        }
    }
}

#[tokio::test]
async fn every_scripted_operation_preserves_typed_failure_context_for_struct_and_enum_families() {
    let token = Alpha(Arc::new(()));
    let (provider, probe) = ScriptedProvider::new(Case::failure_script(token.clone()));
    Case::exercise_failures(&provider, token.clone()).await;
    probe.assert_complete();
    assert_eq!(probe.shutdown_count(), 1);
    assert_eq!(probe.dropped_session_count(), 1);
    let (provider, probe) = ScriptedProvider::new(Case::failure_script(token.clone()));
    Case::exercise_failures(&CallerProvider::Alpha(provider), Continuation::Alpha(token)).await;
    probe.assert_complete();
    let token = Beta {
        identity: Arc::new(()),
        _pinned: PhantomPinned,
    };
    let (provider, probe) = ScriptedProvider::new(Case::failure_script(token.clone()));
    Case::exercise_failures(&CallerProvider::Beta(provider), Continuation::Beta(token)).await;
    probe.assert_complete();
}

#[tokio::test]
async fn struct_hierarchy_all_operations_preserve_requests_and_exact_continuations() {
    let token = Alpha(Arc::new(()));
    let (provider, probe) = ScriptedProvider::new(Case::script(token.clone()));
    Case::exercise(&provider, token.clone(), |c| {
        assert!(Arc::ptr_eq(&token.0, &c.0))
    })
    .await;
    Case::inspect_observations(&probe, |c| assert!(Arc::ptr_eq(&token.0, &c.0)));
}

#[tokio::test]
async fn both_caller_enum_families_preserve_associated_children_and_unpin_free_continuations() {
    let token = Alpha(Arc::new(()));
    let (provider, probe) = ScriptedProvider::new(Case::script(token.clone()));
    Case::exercise(
        &CallerProvider::Alpha(provider),
        Continuation::Alpha(token.clone()),
        |c| match c {
            Continuation::Alpha(c) => assert!(Arc::ptr_eq(&token.0, &c.0)),
            Continuation::Beta(_) => panic!("wrong continuation family"),
        },
    )
    .await;
    Case::inspect_observations(&probe, |c| assert!(Arc::ptr_eq(&token.0, &c.0)));
    let token = Beta {
        identity: Arc::new(()),
        _pinned: PhantomPinned,
    };
    let (provider, probe) = ScriptedProvider::new(Case::script(token.clone()));
    Case::exercise(
        &CallerProvider::Beta(provider),
        Continuation::Beta(token.clone()),
        |c| match c {
            Continuation::Beta(c) => assert!(Arc::ptr_eq(&token.identity, &c.identity)),
            Continuation::Alpha(_) => panic!("wrong continuation family"),
        },
    )
    .await;
    Case::inspect_observations(&probe, |c| {
        assert!(Arc::ptr_eq(&token.identity, &c.identity))
    });
}

#[tokio::test]
async fn mismatched_continuations_are_rejected_before_unary_stream_or_batch_delegation() {
    let (alpha, alpha_probe) =
        ScriptedProvider::new(vec![Case::step(Request::Model(Case::id()), Reply::Unit)]);
    let (beta, beta_probe) =
        ScriptedProvider::new(vec![Case::step(Request::Model(Case::id()), Reply::Unit)]);
    for (provider, wrong) in [
        (
            CallerProvider::Alpha(alpha),
            Continuation::Beta(Beta {
                identity: Arc::new(()),
                _pinned: PhantomPinned,
            }),
        ),
        (
            CallerProvider::Beta(beta),
            Continuation::Alpha(Alpha(Arc::new(()))),
        ),
    ] {
        let model = provider.model(Case::id()).unwrap();
        assert_eq!(
            model
                .generate(&Case::request(wrong.clone()))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            model
                .generate_stream(&Case::request(wrong.clone()))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            model
                .submit_batch(&Case::submission(wrong))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    alpha_probe.assert_complete();
    beta_probe.assert_complete();
    alpha_probe.observed(|calls| assert_eq!(calls.len(), 1));
    beta_probe.observed(|calls| assert_eq!(calls.len(), 1));
}

#[tokio::test]
async fn streams_have_no_prefetch_release_on_drop_and_terminate_after_exact_error() {
    let token = Alpha(Arc::new(()));
    let request = Case::request(token.clone());
    let (provider, probe) = ScriptedProvider::new(vec![
        Case::step(Request::Model(Case::id()), Reply::Unit),
        Case::step(
            Request::Stream(request.clone()),
            Reply::Events(VecDeque::from([
                Ok(GenerateEvent::Started { model: Case::id() }),
                Ok(GenerateEvent::Completed(Box::new(Case::response(token)))),
            ])),
        ),
        Case::step(
            Request::Stream(request.clone()),
            Reply::Events(VecDeque::from([Err(Case::error(
                Operation::GenerateStream,
            ))])),
        ),
    ]);
    let model = provider.model(Case::id()).unwrap();
    let mut events = model.generate_stream(&request).await.unwrap();
    assert_eq!(probe.poll_count(), 0);
    events.next().await.unwrap().unwrap();
    assert_eq!(probe.poll_count(), 1);
    drop(events);
    assert_eq!(probe.dropped_stream_count(), 1);
    let mut events = model.generate_stream(&request).await.unwrap();
    let error = events.next().await.unwrap().err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Transport);
    assert_eq!(error.correlation_id(), Some("fixture-correlation"));
    assert!(events.next().await.is_none());
    drop(events);
    probe.assert_complete();
    assert_eq!(probe.dropped_stream_count(), 2);
}

#[tokio::test]
async fn malformed_source_is_explicitly_separate_from_conforming_fixture() {
    // The public stream adapter promises value adaptation, not source validation.
    let mut malformed: GenerateEventStream<Alpha> =
        GenerateEventStream::new(stream::iter(vec![Ok(GenerateEvent::ContentDelta {
            delta: "unterminated".into(),
        })]));
    assert!(matches!(
        malformed.next().await.unwrap().unwrap(),
        GenerateEvent::ContentDelta { .. }
    ));
    assert!(malformed.next().await.is_none());
}

#[tokio::test]
async fn held_streams_drain_on_completion_error_or_drop_after_shutdown_waiter_loss() {
    for cleanup_fails in [false, true] {
        let token = Alpha(Arc::new(()));
        let request = Case::request(token.clone());
        let shutdown_reply = if cleanup_fails {
            Err(Case::error(Operation::Shutdown))
        } else {
            Ok(Reply::Unit)
        };
        let (provider, probe) = ScriptedProvider::new(vec![
            Case::step(Request::Model(Case::id()), Reply::Unit),
            Case::step(
                Request::Stream(request.clone()),
                Reply::Events(VecDeque::from([Ok(GenerateEvent::Completed(Box::new(
                    Case::response(token.clone()),
                )))])),
            ),
            Case::step(
                Request::Stream(request.clone()),
                Reply::Events(VecDeque::from([Err(Case::error(
                    Operation::GenerateStream,
                ))])),
            ),
            Case::step(
                Request::Stream(request.clone()),
                Reply::Events(VecDeque::from([Ok(GenerateEvent::Completed(Box::new(
                    Case::response(token),
                )))])),
            ),
            Step {
                request: Request::Shutdown,
                reply: shutdown_reply,
            },
        ]);
        let model = provider.model(Case::id()).unwrap();
        let mut completed = model.generate_stream(&request).await.unwrap();
        let mut failed = model.generate_stream(&request).await.unwrap();
        let dropped = model.generate_stream(&request).await.unwrap();
        {
            let mut abandoned = std::pin::pin!(provider.shutdown());
            assert!(
                abandoned
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert_eq!(probe.shutdown_count(), 1);
        assert_eq!(
            provider.health().await.unwrap_err().kind(),
            ErrorKind::Closed
        );
        assert_eq!(
            provider.model(Case::id()).err().unwrap().kind(),
            ErrorKind::Closed
        );
        assert_eq!(
            model.generate_stream(&request).await.err().unwrap().kind(),
            ErrorKind::Closed
        );
        let mut observer = std::pin::pin!(provider.shutdown());
        let wake = Arc::new(WakeProbe::default());
        let waker = Waker::from(Arc::clone(&wake));
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(matches!(
            completed.next().await.unwrap().unwrap(),
            GenerateEvent::Completed(_)
        ));
        // Completed streams release admission before their public value is dropped.
        assert_eq!(probe.dropped_stream_count(), 0);
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(
            failed.next().await.unwrap().err().unwrap().kind(),
            ErrorKind::Transport
        );
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(dropped);
        assert!(
            wake.0.load(Ordering::SeqCst) > 0,
            "shutdown waiter must be woken after the last release"
        );
        let result = Case::ready(observer);
        if cleanup_fails {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Transport);
            assert_eq!(error.context().operation, Operation::Shutdown);
            assert_eq!(error.correlation_id(), Some("fixture-correlation"));
            assert_eq!(
                Case::ready(provider.shutdown()).unwrap_err().kind(),
                ErrorKind::Transport
            );
        } else {
            result.unwrap();
            Case::ready(provider.shutdown()).unwrap();
        }
        assert!(completed.next().await.is_none());
        assert!(failed.next().await.is_none());
        drop(completed);
        drop(failed);
        assert_eq!(probe.shutdown_count(), 1);
        assert_eq!(probe.dropped_stream_count(), 3);
        probe.assert_complete();
    }
}

#[tokio::test]
async fn session_audio_functions_transcripts_and_cancellation_cross_struct_and_enum_seams() {
    let (provider, probe) = ScriptedProvider::<Alpha>::new(Case::session_script());
    Case::exercise_session(provider).await;
    probe.assert_complete();
    assert_eq!(probe.close_count(), 1);
    assert_eq!(probe.dropped_session_count(), 1);
    let (provider, probe) = ScriptedProvider::<Alpha>::new(Case::session_script());
    Case::exercise_session(CallerProvider::Alpha(provider)).await;
    probe.assert_complete();
    assert_eq!(probe.close_count(), 1);
    assert_eq!(probe.dropped_session_count(), 1);
    let (provider, probe) = ScriptedProvider::<Beta>::new(Case::session_script());
    Case::exercise_session(CallerProvider::Beta(provider)).await;
    probe.assert_complete();
    assert_eq!(probe.close_count(), 1);
    assert_eq!(probe.dropped_session_count(), 1);
}

#[tokio::test]
async fn sessions_retain_independent_identity_and_consuming_close_does_not_close_sibling() {
    let (provider, probe) = ScriptedProvider::<Alpha>::new(vec![
        Case::step(Request::Model(Case::id()), Reply::Unit),
        Case::step(
            Request::OpenSession(Case::config()),
            Reply::Session("first".into()),
        ),
        Case::step(
            Request::OpenSession(Case::config()),
            Reply::Session("second".into()),
        ),
        Case::step(Request::Send("second".into(), Case::input()), Reply::Unit),
        Case::step(Request::Close("first".into()), Reply::Unit),
        Case::step(
            Request::Recv("second".into()),
            Reply::Event(SessionEvent::Completed),
        ),
        Case::step(Request::Close("second".into()), Reply::Unit),
        Case::step(Request::Shutdown, Reply::Unit),
    ]);
    let model = provider.model(Case::id()).unwrap();
    let first = model.open_session(Case::config()).await.unwrap();
    let mut second = model.open_session(Case::config()).await.unwrap();
    assert_eq!(first.id(), "first");
    assert_eq!(second.id(), "second");
    second.send(Case::input()).await.unwrap();
    first.close().await.unwrap();
    assert_eq!(second.recv().await.unwrap(), SessionEvent::Completed);
    second.close().await.unwrap();
    provider.shutdown().await.unwrap();
    probe.assert_complete();
    assert_eq!(probe.close_count(), 2);
    assert_eq!(probe.dropped_session_count(), 2);
}

#[tokio::test]
async fn session_cleanup_remains_available_after_provider_shutdown_seals_work() {
    // PORTABLE_API.md retains consuming Session close after Provider sealing.
    let (provider, probe) = ScriptedProvider::<Alpha>::new(vec![
        Case::step(Request::Model(Case::id()), Reply::Unit),
        Case::step(
            Request::OpenSession(Case::config()),
            Reply::Session("cleanup-after-sealing".into()),
        ),
        Case::step(Request::Shutdown, Reply::Unit),
        Case::step(Request::Close("cleanup-after-sealing".into()), Reply::Unit),
    ]);
    let model = provider.model(Case::id()).unwrap();
    let mut session = model.open_session(Case::config()).await.unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(
        provider.model(Case::id()).err().unwrap().kind(),
        ErrorKind::Closed
    );
    assert_eq!(
        model
            .open_session(Case::config())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Closed
    );
    assert_eq!(
        session.send(Case::input()).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
    assert_eq!(session.recv().await.unwrap_err().kind(), ErrorKind::Closed);
    session.close().await.unwrap();
    provider.shutdown().await.unwrap();
    probe.assert_complete();
    assert_eq!(probe.shutdown_count(), 1);
    assert_eq!(probe.close_count(), 1);
    assert_eq!(probe.dropped_session_count(), 1);
}

#[test]
fn fixture_rejects_unexpected_wrong_and_unconsumed_expectations() {
    let (provider, probe) =
        ScriptedProvider::<Alpha>::new(vec![Case::step(Request::Model(Case::id()), Reply::Unit)]);
    assert!(catch_unwind(AssertUnwindSafe(|| probe.assert_complete())).is_err());
    assert!(
        catch_unwind(AssertUnwindSafe(
            || provider.model(ModelId::new("wrong").unwrap())
        ))
        .is_err()
    );
    let (provider, _) = ScriptedProvider::<Alpha>::new(vec![]);
    assert!(catch_unwind(AssertUnwindSafe(|| Case::ready(provider.health()))).is_err());
    let (provider, _) = ScriptedProvider::<Alpha>::new(vec![Case::step(
        Request::Health,
        Reply::Health(HealthStatus {
            latency: Duration::ZERO,
        }),
    )]);
    assert!(catch_unwind(AssertUnwindSafe(|| provider.model(Case::id()))).is_err());
}

struct UnsupportedProvider;
struct UnsupportedModel(ModelId);

impl ProviderBackend for UnsupportedProvider {
    type Model = UnsupportedModel;
    fn model(&self, id: ModelId) -> Result<Self::Model, KitError> {
        Ok(UnsupportedModel(id))
    }
    async fn shutdown(&self) -> Result<(), KitError> {
        Ok(())
    }
}

impl ModelBackend for UnsupportedModel {
    type Continuation = Alpha;
    type Session = Infallible;
    fn model_id(&self) -> &ModelId {
        &self.0
    }
    async fn generate(
        &self,
        _request: &GenerateRequest<Alpha>,
    ) -> Result<GenerateResponse<Alpha>, KitError> {
        Err(KitError::new(
            ErrorKind::Unsupported,
            Operation::Generate,
            ErrorPhase::InputValidation,
        ))
    }
    async fn generate_stream(
        &self,
        _request: &GenerateRequest<Alpha>,
    ) -> Result<GenerateEventStream<Alpha>, KitError> {
        Err(KitError::new(
            ErrorKind::Unsupported,
            Operation::GenerateStream,
            ErrorPhase::InputValidation,
        ))
    }
}

#[tokio::test]
async fn optional_trait_defaults_report_unsupported_instead_of_synthetic_success() {
    let provider = UnsupportedProvider;
    let model = provider.model(Case::id()).unwrap();
    let token = Alpha(Arc::new(()));
    let errors = vec![
        provider
            .list_models(ModelListRequest::default())
            .await
            .unwrap_err(),
        provider.model_info(Case::id()).await.unwrap_err(),
        provider.upload_file(Case::upload()).await.unwrap_err(),
        provider
            .list_files(FileListRequest::default())
            .await
            .unwrap_err(),
        provider.file_info("missing".into()).await.unwrap_err(),
        provider.delete_file("missing".into()).await.unwrap_err(),
        provider
            .download_file("missing".into(), Case::bound())
            .await
            .unwrap_err(),
        provider.batch_status("missing".into()).await.unwrap_err(),
        provider.cancel_batch("missing".into()).await.unwrap_err(),
        provider
            .batch_results("missing".into(), Case::bound())
            .await
            .err()
            .unwrap(),
        provider
            .list_batches(BatchListRequest::default())
            .await
            .unwrap_err(),
        provider.health().await.unwrap_err(),
        model.embed(&Case::embed()).await.unwrap_err(),
        model
            .submit_batch(&Case::submission(token))
            .await
            .unwrap_err(),
        model.open_session(Case::config()).await.unwrap_err(),
    ];
    assert_eq!(errors.len(), 15);
    for error in errors {
        assert_eq!(error.kind(), ErrorKind::Unsupported);
    }
    provider.shutdown().await.unwrap();
}
