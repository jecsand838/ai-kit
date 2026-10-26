use std::collections::VecDeque;
use std::marker::PhantomPinned;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::{Stream, StreamExt};
use provider_sdk::batch::{BatchJob, BatchListRequest, BatchPage, BatchResult, BatchSubmitRequest};
use provider_sdk::embedding::{EmbedRequest, EmbedResponse};
use provider_sdk::error::{ErrorKind, ErrorPhase, Operation};
use provider_sdk::files::{FileListRequest, FileObject, FilePage, FileUploadRequest};
use provider_sdk::generation::{
    GenerateEvent, GenerateEventStream, GenerateRequest, GenerateResponse,
};
use provider_sdk::models::{ModelId, ModelListRequest, ModelPage, ModelSummary};
use provider_sdk::runtime::HealthStatus;
use provider_sdk::session::{SessionConfig, SessionEvent, SessionInput};
use provider_sdk::{ModelBackend, ProviderBackend, SdkError, SessionBackend};

// Neither continuation implements Debug, equality or serde; Beta is also !Unpin.
#[derive(Clone)]
pub struct Alpha(pub Arc<()>);

#[derive(Clone)]
pub struct Beta {
    pub identity: Arc<()>,
    pub _pinned: PhantomPinned,
}

#[derive(Clone)]
pub enum Continuation {
    Alpha(Alpha),
    Beta(Beta),
}

pub enum Request<C> {
    Model(ModelId),
    ListModels(ModelListRequest),
    ModelInfo(ModelId),
    Upload(FileUploadRequest),
    ListFiles(FileListRequest),
    FileInfo(String),
    Delete(String),
    Download(String, NonZeroUsize),
    BatchStatus(String),
    CancelBatch(String),
    BatchResults(String, NonZeroUsize),
    ListBatches(BatchListRequest),
    Health,
    Shutdown,
    Generate(GenerateRequest<C>),
    Stream(GenerateRequest<C>),
    Embed(EmbedRequest),
    Submit(BatchSubmitRequest<C>),
    OpenSession(SessionConfig),
    Send(String, SessionInput),
    Recv(String),
    Close(String),
}

impl<C> Request<C> {
    fn assert_matches(&self, other: &Self) {
        match (self, other) {
            (Self::Model(a), Self::Model(b)) | (Self::ModelInfo(a), Self::ModelInfo(b)) => {
                assert_eq!(a, b)
            }
            (Self::ListModels(a), Self::ListModels(b)) => assert_eq!(a, b),
            (Self::Upload(a), Self::Upload(b)) => assert_eq!(a, b),
            (Self::ListFiles(a), Self::ListFiles(b)) => assert_eq!(a, b),
            (Self::FileInfo(a), Self::FileInfo(b))
            | (Self::Delete(a), Self::Delete(b))
            | (Self::BatchStatus(a), Self::BatchStatus(b))
            | (Self::CancelBatch(a), Self::CancelBatch(b)) => assert_eq!(a, b),
            (Self::Download(a, n), Self::Download(b, m))
            | (Self::BatchResults(a, n), Self::BatchResults(b, m)) => assert_eq!((a, n), (b, m)),
            (Self::ListBatches(a), Self::ListBatches(b)) => assert_eq!(a, b),
            (Self::Generate(a), Self::Generate(b)) | (Self::Stream(a), Self::Stream(b)) => {
                Self::assert_generation(a, b);
            }
            (Self::Embed(a), Self::Embed(b)) => assert_eq!(a, b),
            (Self::Submit(a), Self::Submit(b)) => match (a, b) {
                (
                    BatchSubmitRequest::Inline { rows: expected },
                    BatchSubmitRequest::Inline { rows: actual },
                ) => {
                    assert_eq!(expected.len(), actual.len());
                    for (expected, actual) in expected.iter().zip(actual) {
                        assert_eq!(expected.custom_id(), actual.custom_id());
                        Self::assert_generation(expected.request(), actual.request());
                    }
                }
                (
                    BatchSubmitRequest::ProviderFile { file_id: expected },
                    BatchSubmitRequest::ProviderFile { file_id: actual },
                ) => assert_eq!(expected, actual),
                _ => panic!("unexpected batch submission kind"),
            },
            (Self::OpenSession(a), Self::OpenSession(b)) => assert_eq!(a, b),
            (Self::Send(a, input), Self::Send(b, other)) => assert_eq!((a, input), (b, other)),
            (Self::Recv(a), Self::Recv(b)) | (Self::Close(a), Self::Close(b)) => assert_eq!(a, b),
            (Self::Health, Self::Health) | (Self::Shutdown, Self::Shutdown) => {}
            _ => panic!("unexpected operation; typed script expected a different request"),
        }
    }

    fn assert_generation(expected: &GenerateRequest<C>, actual: &GenerateRequest<C>) {
        assert_eq!(expected.tools(), actual.tools());
        assert_eq!(expected.response_format(), actual.response_format());
        assert_eq!(expected.reasoning(), actual.reasoning());
        assert_eq!(expected.generation(), actual.generation());
        assert_eq!(expected.messages().len(), actual.messages().len());
        for (expected, actual) in expected.messages().iter().zip(actual.messages()) {
            assert_eq!(expected.role(), actual.role());
            assert_eq!(expected.content(), actual.content());
            assert_eq!(expected.function_calls(), actual.function_calls());
            assert_eq!(expected.function_results(), actual.function_results());
            // Continuations deliberately require neither equality nor serde.
            // The scenario checks their exact Arc identity in recorded requests.
            assert_eq!(
                expected.continuation().is_some(),
                actual.continuation().is_some()
            );
        }
    }
}

pub enum Reply<C> {
    Unit,
    Models(ModelPage),
    ModelInfo(ModelSummary),
    File(FileObject),
    Files(FilePage),
    Bytes(Vec<u8>),
    Batch(BatchJob),
    Results(Vec<BatchResult<C>>),
    Batches(BatchPage),
    Health(HealthStatus),
    Generate(GenerateResponse<C>),
    Events(VecDeque<Result<GenerateEvent<C>, SdkError>>),
    Embed(EmbedResponse),
    Session(String),
    Event(SessionEvent),
}

pub struct Step<C> {
    pub request: Request<C>,
    pub reply: Result<Reply<C>, SdkError>,
}

enum Admission {
    Open,
    Draining(Result<(), SdkError>),
    Closed(Result<(), SdkError>),
}

struct State<C> {
    script: VecDeque<Step<C>>,
    observations: Vec<Request<C>>,
    admission: Admission,
    streams: usize,
}

pub struct Probe<C> {
    state: Mutex<State<C>>,
    polls: AtomicUsize,
    stream_drops: AtomicUsize,
    session_drops: AtomicUsize,
    closes: AtomicUsize,
    shutdowns: AtomicUsize,
    drained: tokio::sync::Notify,
}

impl<C> Probe<C> {
    pub fn poll_count(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }
    pub fn dropped_stream_count(&self) -> usize {
        self.stream_drops.load(Ordering::SeqCst)
    }
    pub fn dropped_session_count(&self) -> usize {
        self.session_drops.load(Ordering::SeqCst)
    }
    pub fn close_count(&self) -> usize {
        self.closes.load(Ordering::SeqCst)
    }
    pub fn shutdown_count(&self) -> usize {
        self.shutdowns.load(Ordering::SeqCst)
    }

    fn release_stream(&self) {
        let notify = {
            let mut state = self.state.lock().unwrap();
            state.streams = state
                .streams
                .checked_sub(1)
                .expect("one release per stream");
            if state.streams == 0 {
                if let Admission::Draining(result) = &state.admission {
                    state.admission = Admission::Closed(result.clone());
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if notify {
            self.drained.notify_waiters();
        }
    }

    pub fn assert_complete(&self) {
        let complete = self.state.lock().unwrap().script.is_empty();
        assert!(complete, "unconsumed provider expectations");
    }

    pub fn observed(&self, inspect: impl FnOnce(&[Request<C>])) {
        inspect(&self.state.lock().unwrap().observations);
    }

    fn call(&self, request: Request<C>, operation: Operation) -> Result<Reply<C>, SdkError> {
        let mut state = self.state.lock().unwrap();
        if matches!(
            state.admission,
            Admission::Draining(_) | Admission::Closed(_)
        ) && !matches!(&request, Request::Close(_))
        {
            return Err(SdkError::new(
                ErrorKind::Closed,
                operation,
                ErrorPhase::Lifecycle,
            ));
        }
        let expected = state.script.front().expect("exhausted provider script");
        expected.request.assert_matches(&request);
        state.observations.push(request);
        let reply = state.script.pop_front().unwrap().reply;
        if matches!(&reply, Ok(Reply::Events(_))) {
            state.streams += 1;
        }
        reply
    }
}

pub struct ScriptedProvider<C> {
    probe: Arc<Probe<C>>,
}

impl<C> ScriptedProvider<C> {
    pub fn new(script: Vec<Step<C>>) -> (Self, Arc<Probe<C>>) {
        let probe = Arc::new(Probe {
            state: Mutex::new(State {
                script: script.into(),
                observations: Vec::new(),
                admission: Admission::Open,
                streams: 0,
            }),
            polls: AtomicUsize::new(0),
            stream_drops: AtomicUsize::new(0),
            session_drops: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            shutdowns: AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
        });
        (
            Self {
                probe: Arc::clone(&probe),
            },
            probe,
        )
    }
}

pub struct ScriptedModel<C> {
    id: ModelId,
    probe: Arc<Probe<C>>,
}

pub struct ScriptedSession<C> {
    id: String,
    model: ModelId,
    probe: Arc<Probe<C>>,
    // SessionBackend requires Send, never Sync or Clone.
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl<C: Clone + Send + Sync + 'static> ProviderBackend for ScriptedProvider<C> {
    type Model = ScriptedModel<C>;
    fn model(&self, id: ModelId) -> Result<Self::Model, SdkError> {
        let Reply::Unit = self
            .probe
            .call(Request::Model(id.clone()), Operation::SelectModel)?
        else {
            panic!("model reply")
        };
        Ok(ScriptedModel {
            id,
            probe: Arc::clone(&self.probe),
        })
    }
    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, SdkError> {
        let Reply::Models(value) = self
            .probe
            .call(Request::ListModels(request), Operation::ListModels)?
        else {
            panic!("models reply")
        };
        Ok(value)
    }
    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, SdkError> {
        let Reply::ModelInfo(value) = self
            .probe
            .call(Request::ModelInfo(id), Operation::ModelInfo)?
        else {
            panic!("model info reply")
        };
        Ok(value)
    }
    async fn upload_file(&self, request: FileUploadRequest) -> Result<FileObject, SdkError> {
        let Reply::File(value) = self
            .probe
            .call(Request::Upload(request), Operation::UploadFile)?
        else {
            panic!("file reply")
        };
        Ok(value)
    }
    async fn list_files(&self, request: FileListRequest) -> Result<FilePage, SdkError> {
        let Reply::Files(value) = self
            .probe
            .call(Request::ListFiles(request), Operation::ListFiles)?
        else {
            panic!("files reply")
        };
        Ok(value)
    }
    async fn file_info(&self, id: String) -> Result<FileObject, SdkError> {
        let Reply::File(value) = self
            .probe
            .call(Request::FileInfo(id), Operation::FileInfo)?
        else {
            panic!("file reply")
        };
        Ok(value)
    }
    async fn delete_file(&self, id: String) -> Result<(), SdkError> {
        let Reply::Unit = self
            .probe
            .call(Request::Delete(id), Operation::DeleteFile)?
        else {
            panic!("delete reply")
        };
        Ok(())
    }
    async fn download_file(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<u8>, SdkError> {
        let Reply::Bytes(value) = self
            .probe
            .call(Request::Download(id, max_bytes), Operation::DownloadFile)?
        else {
            panic!("download reply")
        };
        assert!(value.len() <= max_bytes.get(), "script violates byte bound");
        Ok(value)
    }
    async fn batch_status(&self, id: String) -> Result<BatchJob, SdkError> {
        let Reply::Batch(value) = self
            .probe
            .call(Request::BatchStatus(id), Operation::BatchStatus)?
        else {
            panic!("batch reply")
        };
        Ok(value)
    }
    async fn cancel_batch(&self, id: String) -> Result<BatchJob, SdkError> {
        let Reply::Batch(value) = self
            .probe
            .call(Request::CancelBatch(id), Operation::CancelBatch)?
        else {
            panic!("cancel reply")
        };
        Ok(value)
    }
    async fn batch_results(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<C>>, SdkError> {
        let Reply::Results(value) = self.probe.call(
            Request::BatchResults(id, max_bytes),
            Operation::BatchResults,
        )?
        else {
            panic!("results reply")
        };
        Ok(value)
    }
    async fn list_batches(&self, request: BatchListRequest) -> Result<BatchPage, SdkError> {
        let Reply::Batches(value) = self
            .probe
            .call(Request::ListBatches(request), Operation::ListBatches)?
        else {
            panic!("batches reply")
        };
        Ok(value)
    }
    async fn health(&self) -> Result<HealthStatus, SdkError> {
        let Reply::Health(value) = self.probe.call(Request::Health, Operation::Health)? else {
            panic!("health reply")
        };
        Ok(value)
    }
    async fn shutdown(&self) -> Result<(), SdkError> {
        loop {
            let mut changed = std::pin::pin!(self.probe.drained.notified());
            changed.as_mut().enable();
            {
                let mut state = self.probe.state.lock().unwrap();
                match &state.admission {
                    Admission::Closed(result) => return result.clone(),
                    Admission::Draining(_) => {}
                    Admission::Open => {
                        state
                            .script
                            .front()
                            .expect("exhausted shutdown script")
                            .request
                            .assert_matches(&Request::Shutdown);
                        state.observations.push(Request::Shutdown);
                        let result = state
                            .script
                            .pop_front()
                            .unwrap()
                            .reply
                            .map(|reply| assert!(matches!(reply, Reply::Unit)));
                        self.probe.shutdowns.fetch_add(1, Ordering::SeqCst);
                        if state.streams == 0 {
                            state.admission = Admission::Closed(result.clone());
                            return result;
                        }
                        state.admission = Admission::Draining(result);
                    }
                }
            }
            changed.await;
        }
    }
}

impl<C: Clone + Send + Sync + 'static> ModelBackend for ScriptedModel<C> {
    type Continuation = C;
    type Session = ScriptedSession<C>;
    fn model_id(&self) -> &ModelId {
        &self.id
    }
    async fn generate(
        &self,
        request: &GenerateRequest<C>,
    ) -> Result<GenerateResponse<C>, SdkError> {
        let Reply::Generate(value) = self
            .probe
            .call(Request::Generate(request.clone()), Operation::Generate)?
        else {
            panic!("generation reply")
        };
        Ok(value)
    }
    async fn generate_stream(
        &self,
        request: &GenerateRequest<C>,
    ) -> Result<GenerateEventStream<C>, SdkError> {
        let Reply::Events(events) = self
            .probe
            .call(Request::Stream(request.clone()), Operation::GenerateStream)?
        else {
            panic!("stream reply")
        };
        Ok(GenerateEventStream::new(ScriptedStream {
            events,
            probe: Arc::clone(&self.probe),
            terminal: false,
            admitted: true,
        }))
    }
    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, SdkError> {
        let Reply::Embed(value) = self
            .probe
            .call(Request::Embed(request.clone()), Operation::Embed)?
        else {
            panic!("embed reply")
        };
        Ok(value)
    }
    async fn submit_batch(&self, request: &BatchSubmitRequest<C>) -> Result<BatchJob, SdkError> {
        let Reply::Batch(value) = self
            .probe
            .call(Request::Submit(request.clone()), Operation::SubmitBatch)?
        else {
            panic!("submit reply")
        };
        Ok(value)
    }
    async fn open_session(&self, config: SessionConfig) -> Result<Self::Session, SdkError> {
        let Reply::Session(id) = self
            .probe
            .call(Request::OpenSession(config), Operation::OpenSession)?
        else {
            panic!("session reply")
        };
        Ok(ScriptedSession {
            id,
            model: self.id.clone(),
            probe: Arc::clone(&self.probe),
            _not_sync: std::marker::PhantomData,
        })
    }
}

impl<C: Clone + Send + Sync + 'static> SessionBackend for ScriptedSession<C> {
    fn id(&self) -> &str {
        &self.id
    }
    fn model_id(&self) -> &ModelId {
        &self.model
    }
    async fn send(&mut self, input: SessionInput) -> Result<(), SdkError> {
        let Reply::Unit = self.probe.call(
            Request::Send(self.id.clone(), input),
            Operation::SessionSend,
        )?
        else {
            panic!("send reply")
        };
        Ok(())
    }
    async fn recv(&mut self) -> Result<SessionEvent, SdkError> {
        let Reply::Event(value) = self
            .probe
            .call(Request::Recv(self.id.clone()), Operation::SessionReceive)?
        else {
            panic!("receive reply")
        };
        Ok(value)
    }
    async fn close(self) -> Result<(), SdkError> {
        self.probe.closes.fetch_add(1, Ordering::SeqCst);
        let Reply::Unit = self
            .probe
            .call(Request::Close(self.id.clone()), Operation::SessionClose)?
        else {
            panic!("close reply")
        };
        Ok(())
    }
}

impl<C> Drop for ScriptedSession<C> {
    fn drop(&mut self) {
        self.probe.session_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct ScriptedStream<C> {
    events: VecDeque<Result<GenerateEvent<C>, SdkError>>,
    probe: Arc<Probe<C>>,
    terminal: bool,
    admitted: bool,
}

// The queue has no structurally pinned field. Moving a queued continuation is
// valid even when the continuation itself opts out of Unpin.
impl<C> Unpin for ScriptedStream<C> {}

impl<C> Stream for ScriptedStream<C> {
    type Item = Result<GenerateEvent<C>, SdkError>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        stream.probe.polls.fetch_add(1, Ordering::SeqCst);
        if stream.terminal {
            return Poll::Ready(None);
        }
        let event = stream.events.pop_front();
        if matches!(&event, Some(Ok(GenerateEvent::Completed(_))) | Some(Err(_))) {
            assert!(
                stream.events.is_empty(),
                "conforming script has data after terminal"
            );
            stream.terminal = true;
            stream.admitted = false;
            stream.probe.release_stream();
        }
        if event.is_none() {
            assert!(stream.terminal, "conforming stream ended without terminal");
        }
        Poll::Ready(event)
    }
}

impl<C> Drop for ScriptedStream<C> {
    fn drop(&mut self) {
        if self.admitted {
            self.probe.release_stream();
        }
        self.probe.stream_drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub enum CallerProvider {
    Alpha(ScriptedProvider<Alpha>),
    Beta(ScriptedProvider<Beta>),
}
pub enum CallerModel {
    Alpha(ScriptedModel<Alpha>),
    Beta(ScriptedModel<Beta>),
}
pub enum CallerSession {
    Alpha(ScriptedSession<Alpha>),
    Beta(ScriptedSession<Beta>),
}

impl Continuation {
    fn alpha(self) -> Result<Alpha, SdkError> {
        match self {
            Self::Alpha(value) => Ok(value),
            Self::Beta(_) => Err(Self::mismatch()),
        }
    }
    fn beta(self) -> Result<Beta, SdkError> {
        match self {
            Self::Beta(value) => Ok(value),
            Self::Alpha(_) => Err(Self::mismatch()),
        }
    }
    fn mismatch() -> SdkError {
        SdkError::new(
            ErrorKind::InvalidInput,
            Operation::ValidateInput,
            ErrorPhase::InputValidation,
        )
    }
}

impl ProviderBackend for CallerProvider {
    type Model = CallerModel;
    fn model(&self, id: ModelId) -> Result<Self::Model, SdkError> {
        match self {
            Self::Alpha(p) => p.model(id).map(CallerModel::Alpha),
            Self::Beta(p) => p.model(id).map(CallerModel::Beta),
        }
    }
    async fn list_models(&self, r: ModelListRequest) -> Result<ModelPage, SdkError> {
        match self {
            Self::Alpha(p) => p.list_models(r).await,
            Self::Beta(p) => p.list_models(r).await,
        }
    }
    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, SdkError> {
        match self {
            Self::Alpha(p) => p.model_info(id).await,
            Self::Beta(p) => p.model_info(id).await,
        }
    }
    async fn upload_file(&self, r: FileUploadRequest) -> Result<FileObject, SdkError> {
        match self {
            Self::Alpha(p) => p.upload_file(r).await,
            Self::Beta(p) => p.upload_file(r).await,
        }
    }
    async fn list_files(&self, r: FileListRequest) -> Result<FilePage, SdkError> {
        match self {
            Self::Alpha(p) => p.list_files(r).await,
            Self::Beta(p) => p.list_files(r).await,
        }
    }
    async fn file_info(&self, id: String) -> Result<FileObject, SdkError> {
        match self {
            Self::Alpha(p) => p.file_info(id).await,
            Self::Beta(p) => p.file_info(id).await,
        }
    }
    async fn delete_file(&self, id: String) -> Result<(), SdkError> {
        match self {
            Self::Alpha(p) => p.delete_file(id).await,
            Self::Beta(p) => p.delete_file(id).await,
        }
    }
    async fn download_file(&self, id: String, n: NonZeroUsize) -> Result<Vec<u8>, SdkError> {
        match self {
            Self::Alpha(p) => p.download_file(id, n).await,
            Self::Beta(p) => p.download_file(id, n).await,
        }
    }
    async fn batch_status(&self, id: String) -> Result<BatchJob, SdkError> {
        match self {
            Self::Alpha(p) => p.batch_status(id).await,
            Self::Beta(p) => p.batch_status(id).await,
        }
    }
    async fn cancel_batch(&self, id: String) -> Result<BatchJob, SdkError> {
        match self {
            Self::Alpha(p) => p.cancel_batch(id).await,
            Self::Beta(p) => p.cancel_batch(id).await,
        }
    }
    async fn batch_results(
        &self,
        id: String,
        n: NonZeroUsize,
    ) -> Result<Vec<BatchResult<Continuation>>, SdkError> {
        match self {
            Self::Alpha(p) => p
                .batch_results(id, n)
                .await?
                .into_iter()
                .map(|row| row.try_map_continuation(|c| Ok(Continuation::Alpha(c))))
                .collect(),
            Self::Beta(p) => p
                .batch_results(id, n)
                .await?
                .into_iter()
                .map(|row| row.try_map_continuation(|c| Ok(Continuation::Beta(c))))
                .collect(),
        }
    }
    async fn list_batches(&self, r: BatchListRequest) -> Result<BatchPage, SdkError> {
        match self {
            Self::Alpha(p) => p.list_batches(r).await,
            Self::Beta(p) => p.list_batches(r).await,
        }
    }
    async fn health(&self) -> Result<HealthStatus, SdkError> {
        match self {
            Self::Alpha(p) => p.health().await,
            Self::Beta(p) => p.health().await,
        }
    }
    async fn shutdown(&self) -> Result<(), SdkError> {
        match self {
            Self::Alpha(p) => p.shutdown().await,
            Self::Beta(p) => p.shutdown().await,
        }
    }
}

impl ModelBackend for CallerModel {
    type Continuation = Continuation;
    type Session = CallerSession;
    fn model_id(&self) -> &ModelId {
        match self {
            Self::Alpha(m) => m.model_id(),
            Self::Beta(m) => m.model_id(),
        }
    }
    async fn generate(
        &self,
        r: &GenerateRequest<Continuation>,
    ) -> Result<GenerateResponse<Continuation>, SdkError> {
        match self {
            Self::Alpha(m) => m
                .generate(&r.clone().try_map_continuation(Continuation::alpha)?)
                .await?
                .try_map_continuation(|c| Ok(Continuation::Alpha(c))),
            Self::Beta(m) => m
                .generate(&r.clone().try_map_continuation(Continuation::beta)?)
                .await?
                .try_map_continuation(|c| Ok(Continuation::Beta(c))),
        }
    }
    async fn generate_stream(
        &self,
        r: &GenerateRequest<Continuation>,
    ) -> Result<GenerateEventStream<Continuation>, SdkError> {
        match self {
            Self::Alpha(m) => Ok(GenerateEventStream::new(
                m.generate_stream(&r.clone().try_map_continuation(Continuation::alpha)?)
                    .await?
                    .map(|event| {
                        event.and_then(|e| e.try_map_continuation(|c| Ok(Continuation::Alpha(c))))
                    }),
            )),
            Self::Beta(m) => Ok(GenerateEventStream::new(
                m.generate_stream(&r.clone().try_map_continuation(Continuation::beta)?)
                    .await?
                    .map(|event| {
                        event.and_then(|e| e.try_map_continuation(|c| Ok(Continuation::Beta(c))))
                    }),
            )),
        }
    }
    async fn embed(&self, r: &EmbedRequest) -> Result<EmbedResponse, SdkError> {
        match self {
            Self::Alpha(m) => m.embed(r).await,
            Self::Beta(m) => m.embed(r).await,
        }
    }
    async fn submit_batch(
        &self,
        r: &BatchSubmitRequest<Continuation>,
    ) -> Result<BatchJob, SdkError> {
        match self {
            Self::Alpha(m) => {
                m.submit_batch(&r.clone().try_map_continuation(Continuation::alpha)?)
                    .await
            }
            Self::Beta(m) => {
                m.submit_batch(&r.clone().try_map_continuation(Continuation::beta)?)
                    .await
            }
        }
    }
    async fn open_session(&self, r: SessionConfig) -> Result<Self::Session, SdkError> {
        match self {
            Self::Alpha(m) => m.open_session(r).await.map(CallerSession::Alpha),
            Self::Beta(m) => m.open_session(r).await.map(CallerSession::Beta),
        }
    }
}

impl SessionBackend for CallerSession {
    fn id(&self) -> &str {
        match self {
            Self::Alpha(s) => s.id(),
            Self::Beta(s) => s.id(),
        }
    }
    fn model_id(&self) -> &ModelId {
        match self {
            Self::Alpha(s) => s.model_id(),
            Self::Beta(s) => s.model_id(),
        }
    }
    async fn send(&mut self, r: SessionInput) -> Result<(), SdkError> {
        match self {
            Self::Alpha(s) => s.send(r).await,
            Self::Beta(s) => s.send(r).await,
        }
    }
    async fn recv(&mut self) -> Result<SessionEvent, SdkError> {
        match self {
            Self::Alpha(s) => s.recv().await,
            Self::Beta(s) => s.recv().await,
        }
    }
    async fn close(self) -> Result<(), SdkError> {
        match self {
            Self::Alpha(s) => s.close().await,
            Self::Beta(s) => s.close().await,
        }
    }
}
