use std::cell::Cell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use provider_sdk::error::{ErrorKind, ErrorPhase, Operation};
use provider_sdk::generation::{
    FinishReason, FunctionCall, GenerateEvent, GenerateEventStream, GenerateRequest,
    GenerateResponse, Message, Usage,
};
use provider_sdk::models::{ModelId, ModelListRequest, ModelPage, ModelSummary};
use provider_sdk::{ModelBackend, ProviderBackend, SdkError};
use tool_sdk::declaration::{ToolInfo, ToolName};
use tool_sdk::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult};
use tool_sdk::interaction::{
    ToolInteractionCapabilities, ToolInteractionId, ToolInteractionKind, ToolInteractionRequest,
    ToolInteractionResponse,
};
use tool_sdk::schema::JsonSchema;
use tool_sdk::{ToolError, ToolHandlerBackend, ToolsetBackend};

pub(super) const MODEL: &str = "mock-selected-model";
pub(super) const TOOL: &str = "mock_lookup";

// No Debug, Eq, Default, or serialization bound is needed to compose Custom.
#[derive(Clone)]
pub(super) struct Continuation {
    identity: Arc<()>,
}

impl Continuation {
    pub(super) fn new() -> Self {
        Self {
            identity: Arc::new(()),
        }
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}

pub(super) struct Gate {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl Gate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        })
    }

    async fn wait(&self) {
        self.entered.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("release gate open")
            .forget();
    }

    pub(super) async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.acquire())
            .await
            .expect("bounded source admission")
            .expect("entered gate open")
            .forget();
    }

    pub(super) fn release(&self) {
        self.release.add_permits(1);
    }
}

pub(super) enum ProviderStep {
    Unary(Box<GenerateResponse<Continuation>>),
    Failure(SdkError),
    Stream(Vec<Result<GenerateEvent<Continuation>, SdkError>>),
    MalformedStream(Vec<Result<GenerateEvent<Continuation>, SdkError>>),
    GatedStream {
        gate: Arc<Gate>,
        events: Vec<Result<GenerateEvent<Continuation>, SdkError>>,
    },
}

impl ProviderStep {
    pub(super) fn unary(response: GenerateResponse<Continuation>) -> Self {
        Self::Unary(Box::new(response))
    }
}

#[derive(Default)]
pub(super) struct ProviderControl {
    steps: Mutex<VecDeque<ProviderStep>>,
    requests: Mutex<Vec<GenerateRequest<Continuation>>>,
    selected: Mutex<Vec<ModelId>>,
    shutdowns: AtomicUsize,
    closed: AtomicBool,
}

impl ProviderControl {
    pub(super) fn scripted(steps: Vec<ProviderStep>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into()),
            ..Self::default()
        })
    }

    pub(super) fn provider(self: &Arc<Self>) -> MockProvider {
        MockProvider {
            control: Arc::clone(self),
        }
    }
    pub(super) fn requests(&self) -> Vec<GenerateRequest<Continuation>> {
        self.requests.lock().expect("requests").clone()
    }
    pub(super) fn shutdowns(&self) -> usize {
        self.shutdowns.load(Ordering::SeqCst)
    }
    pub(super) fn selections(&self) -> Vec<ModelId> {
        self.selected.lock().expect("selections").clone()
    }
    pub(super) fn assert_consumed(&self) {
        assert!(
            self.steps.lock().expect("script").is_empty(),
            "unconsumed required Provider calls"
        );
    }

    pub(super) fn response(text: &str) -> GenerateResponse<Continuation> {
        Self::assistant(
            Message::assistant_text(text).expect("assistant"),
            FinishReason::Stop,
        )
    }

    pub(super) fn function(token: &Continuation) -> GenerateResponse<Continuation> {
        let call = FunctionCall::new(
            Some("call-1".into()),
            TOOL,
            serde_json::json!({"key":"answer"}),
        )
        .expect("call");
        Self::assistant(
            Message::assistant(Vec::new(), vec![call])
                .expect("assistant")
                .with_continuation(token.clone())
                .expect("continuation"),
            FinishReason::ToolCall,
        )
    }

    fn conforming_stream(
        events: Vec<Result<GenerateEvent<Continuation>, SdkError>>,
    ) -> Vec<Result<GenerateEvent<Continuation>, SdkError>> {
        assert!(
            matches!(
                events.last(),
                Some(Ok(GenerateEvent::Completed(_)) | Err(_))
            ),
            "conforming stream requires exactly one terminal"
        );
        assert!(
            events[..events.len() - 1]
                .iter()
                .all(|event| !matches!(event, Ok(GenerateEvent::Completed(_)) | Err(_))),
            "conforming stream forbids events after terminal"
        );
        events
    }

    fn assistant(
        message: Message<Continuation>,
        reason: FinishReason,
    ) -> GenerateResponse<Continuation> {
        GenerateResponse {
            model: ModelId::new(MODEL).expect("model"),
            message,
            finish_reason: Some(reason),
            citations: Vec::new(),
            artifacts: Vec::new(),
            hosted_tool_outputs: Vec::new(),
            usage: Usage {
                input_tokens: Some(2),
                output_tokens: Some(3),
                ..Usage::default()
            },
        }
    }

    fn take(
        &self,
        request: &GenerateRequest<Continuation>,
        operation: Operation,
    ) -> Result<ProviderStep, SdkError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SdkError::new(
                ErrorKind::InvalidState,
                operation,
                ErrorPhase::Lifecycle,
            ));
        }
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        self.steps
            .lock()
            .expect("script")
            .pop_front()
            .ok_or_else(|| SdkError::new(ErrorKind::InvalidState, operation, ErrorPhase::Lifecycle))
    }
}

pub(super) struct MockProvider {
    control: Arc<ProviderControl>,
}
pub(super) struct MockModel {
    control: Arc<ProviderControl>,
    id: ModelId,
}

impl ProviderBackend for MockProvider {
    type Model = MockModel;

    fn model(&self, id: ModelId) -> Result<Self::Model, SdkError> {
        if self.control.closed.load(Ordering::SeqCst) {
            return Err(SdkError::new(
                ErrorKind::InvalidState,
                Operation::SelectModel,
                ErrorPhase::Lifecycle,
            ));
        }
        self.control
            .selected
            .lock()
            .expect("selections")
            .push(id.clone());
        if id.as_str() != MODEL {
            return Err(SdkError::new(
                ErrorKind::Unsupported,
                Operation::SelectModel,
                ErrorPhase::InputValidation,
            ));
        }
        Ok(MockModel {
            control: Arc::clone(&self.control),
            id,
        })
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, SdkError> {
        if self.control.closed.load(Ordering::SeqCst) {
            return Err(SdkError::new(
                ErrorKind::InvalidState,
                Operation::ListModels,
                ErrorPhase::Lifecycle,
            ));
        }
        assert!(request.cursor().is_none(), "mock inventory has one page");
        Ok(ModelPage {
            models: vec![ModelSummary {
                id: ModelId::new(MODEL).expect("model"),
                display_name: Some("Mock model".into()),
                description: None,
                context_window_tokens: Some(4096),
            }],
            next_cursor: None,
        })
    }

    async fn shutdown(&self) -> Result<(), SdkError> {
        if !self.control.closed.swap(true, Ordering::SeqCst) {
            self.control.shutdowns.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

impl ModelBackend for MockModel {
    type Continuation = Continuation;
    type Session = std::convert::Infallible;

    fn model_id(&self) -> &ModelId {
        &self.id
    }

    async fn generate(
        &self,
        request: &GenerateRequest<Continuation>,
    ) -> Result<GenerateResponse<Continuation>, SdkError> {
        match self.control.take(request, Operation::Generate)? {
            ProviderStep::Unary(response) => Ok(*response),
            ProviderStep::Failure(error) => Err(error),
            ProviderStep::Stream(_)
            | ProviderStep::MalformedStream(_)
            | ProviderStep::GatedStream { .. } => Err(SdkError::new(
                ErrorKind::InvalidState,
                Operation::Generate,
                ErrorPhase::Lifecycle,
            )),
        }
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<Continuation>,
    ) -> Result<GenerateEventStream<Continuation>, SdkError> {
        let (events, gate) = match self.control.take(request, Operation::GenerateStream)? {
            ProviderStep::Stream(events) => (ProviderControl::conforming_stream(events), None),
            ProviderStep::MalformedStream(events) => (events, None),
            ProviderStep::GatedStream { gate, events } => {
                (ProviderControl::conforming_stream(events), Some(gate))
            }
            ProviderStep::Failure(error) => return Err(error),
            ProviderStep::Unary(_) => {
                return Err(SdkError::new(
                    ErrorKind::InvalidState,
                    Operation::GenerateStream,
                    ErrorPhase::Lifecycle,
                ));
            }
        };
        let source = futures_util::stream::unfold(
            (VecDeque::from(events), gate),
            |(mut events, gate)| async move {
                if let Some(gate) = gate {
                    gate.wait().await;
                }
                events.pop_front().map(|event| (event, (events, None)))
            },
        );
        Ok(GenerateEventStream::new(source))
    }
}

#[derive(Clone, Copy)]
pub(super) enum ToolOutcome {
    Completed,
    ModelFailure,
    Interaction,
    UnpresentableInteraction(ToolCancellation),
    OuterFailure,
}

#[derive(Clone, Copy)]
pub(super) enum ToolCancellation {
    Cancelled,
    Completed,
    TransportFailure,
}

pub(super) struct ToolScript {
    expected: ToolInvocation,
    outcome: ToolOutcome,
}

impl ToolScript {
    pub(super) fn new(outcome: ToolOutcome) -> Self {
        Self {
            expected: ToolInvocation {
                call_id: Some("call-1".into()),
                arguments: serde_json::json!({"key":"answer"}),
            },
            outcome,
        }
    }
}

#[derive(Default)]
pub(super) struct ToolControl {
    scripts: Mutex<VecDeque<ToolScript>>,
    invocations: Mutex<Vec<ToolInvocation>>,
    interactions: Mutex<Vec<ToolInteractionResponse>>,
    handlers: AtomicUsize,
    pending: AtomicUsize,
    cancellations: AtomicUsize,
    shutdowns: AtomicUsize,
    closed: AtomicBool,
    fail_shutdown: AtomicBool,
}

impl ToolControl {
    pub(super) fn scripted(scripts: Vec<ToolScript>) -> Arc<Self> {
        Arc::new(Self {
            scripts: Mutex::new(scripts.into()),
            ..Self::default()
        })
    }
    pub(super) fn tools(self: &Arc<Self>) -> MockTools {
        MockTools {
            control: Arc::clone(self),
        }
    }
    pub(super) fn assert_consumed(&self) {
        assert!(
            self.scripts.lock().expect("tool script").is_empty(),
            "unconsumed required Tool handler"
        );
        assert_eq!(
            self.pending.load(Ordering::SeqCst),
            0,
            "unconsumed acquired Tool invocation"
        );
    }
    pub(super) fn invocations(&self) -> Vec<ToolInvocation> {
        self.invocations.lock().expect("invocations").clone()
    }
    pub(super) fn interactions(&self) -> Vec<ToolInteractionResponse> {
        self.interactions.lock().expect("interactions").clone()
    }
    pub(super) fn handlers(&self) -> usize {
        self.handlers.load(Ordering::SeqCst)
    }
    pub(super) fn cancellations(&self) -> usize {
        self.cancellations.load(Ordering::SeqCst)
    }
    pub(super) fn shutdowns(&self) -> usize {
        self.shutdowns.load(Ordering::SeqCst)
    }
    pub(super) fn fail_shutdown(&self) {
        self.fail_shutdown.store(true, Ordering::SeqCst);
    }
}

pub(super) struct MockTools {
    control: Arc<ToolControl>,
}
pub(super) struct MockHandler {
    control: Arc<ToolControl>,
    phase: HandlerPhase,
    // A caller handler is Send but need not be Sync.
    local_state: PhantomData<Cell<()>>,
}

enum HandlerPhase {
    Ready(ToolScript),
    Waiting {
        id: ToolInteractionId,
        call_id: Option<String>,
        cancellation: ToolCancellation,
    },
    Finished,
}

impl MockTools {
    fn info(&self) -> ToolInfo {
        let mut info = ToolInfo::new(
            ToolName::new(TOOL).expect("tool name"),
            JsonSchema::strict(
                serde_json::json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"],"additionalProperties":false}),
            ),
        );
        info.interactions = ToolInteractionCapabilities {
            text: true,
            ..ToolInteractionCapabilities::default()
        };
        info
    }
}

impl ToolsetBackend for MockTools {
    type Handler = MockHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        if self.control.closed.load(Ordering::SeqCst) {
            return Err(ToolError::Closed {
                operation: "list_tools".into(),
            });
        }
        Ok(vec![self.info()])
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        if self.control.closed.load(Ordering::SeqCst) {
            return Err(ToolError::Closed {
                operation: "tool_handler".into(),
            });
        }
        if name.as_str() != TOOL {
            return Err(ToolError::UnknownTool { name });
        }
        let script = self
            .control
            .scripts
            .lock()
            .expect("tool script")
            .pop_front()
            .ok_or_else(|| ToolError::InvalidConfiguration {
                message: "exhausted handler script".into(),
            })?;
        self.control.handlers.fetch_add(1, Ordering::SeqCst);
        self.control.pending.fetch_add(1, Ordering::SeqCst);
        Ok(MockHandler {
            control: Arc::clone(&self.control),
            phase: HandlerPhase::Ready(script),
            local_state: PhantomData,
        })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        if !self.control.closed.swap(true, Ordering::SeqCst) {
            self.control.shutdowns.fetch_add(1, Ordering::SeqCst);
        }
        if self.control.fail_shutdown.load(Ordering::SeqCst) {
            return Err(ToolError::InvalidConfiguration {
                message: "scripted shutdown failure".into(),
            });
        }
        Ok(())
    }
}

impl MockHandler {
    fn finish(&mut self) {
        assert!(
            !matches!(self.phase, HandlerPhase::Finished),
            "handler has one terminal"
        );
        self.phase = HandlerPhase::Finished;
        assert!(
            self.control.pending.fetch_sub(1, Ordering::SeqCst) > 0,
            "pending invocation accounting"
        );
    }
}

impl ToolHandlerBackend for MockHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        let name = ToolName::new(TOOL).expect("tool name");
        if matches!(&input, ToolHandlerInput::Cancel { .. }) {
            self.control.cancellations.fetch_add(1, Ordering::SeqCst);
        }
        if self.control.closed.load(Ordering::SeqCst) {
            return Err(ToolError::Closed {
                operation: "step".into(),
            });
        }
        match (&self.phase, input) {
            (HandlerPhase::Ready(script), ToolHandlerInput::Invoke(input)) => {
                if input != script.expected {
                    return Err(ToolError::InvalidConfiguration {
                        message: "unexpected invocation arguments or call id".into(),
                    });
                }
                self.control
                    .invocations
                    .lock()
                    .expect("invocations")
                    .push(input.clone());
                match script.outcome {
                    ToolOutcome::Completed | ToolOutcome::ModelFailure => {
                        let is_error = matches!(script.outcome, ToolOutcome::ModelFailure);
                        self.finish();
                        Ok(ToolHandlerOutput::Completed(
                            ToolResult::text("mock result").with_error(is_error),
                        ))
                    }
                    ToolOutcome::Interaction | ToolOutcome::UnpresentableInteraction(_) => {
                        let (prompt, cancellation) = match script.outcome {
                            ToolOutcome::UnpresentableInteraction(cancellation) => {
                                (String::new(), cancellation)
                            }
                            _ => ("Supply the answer".into(), ToolCancellation::Cancelled),
                        };
                        let id = ToolInteractionId::new();
                        self.phase = HandlerPhase::Waiting {
                            id: id.clone(),
                            call_id: input.call_id.clone(),
                            cancellation,
                        };
                        Ok(ToolHandlerOutput::Interaction(ToolInteractionRequest {
                            id,
                            tool_name: name,
                            call_id: input.call_id,
                            prompt,
                            kind: ToolInteractionKind::Text { multiline: false },
                        }))
                    }
                    ToolOutcome::OuterFailure => {
                        self.finish();
                        Err(ToolError::Transport {
                            tool: Some(name),
                            message: "scripted operation failure".into(),
                        })
                    }
                }
            }
            (
                HandlerPhase::Waiting { id, call_id, .. },
                ToolHandlerInput::Interaction(response),
            ) => {
                let ToolInteractionResponse::Submit {
                    id: supplied,
                    content,
                } = &response
                else {
                    return Err(ToolError::InvalidConfiguration {
                        message: "expected submission".into(),
                    });
                };
                if supplied != id || content != "42" {
                    return Err(ToolError::InvalidConfiguration {
                        message: "wrong interaction id or content".into(),
                    });
                }
                assert_eq!(call_id.as_deref(), Some("call-1"));
                self.control
                    .interactions
                    .lock()
                    .expect("interactions")
                    .push(response);
                self.finish();
                Ok(ToolHandlerOutput::Completed(ToolResult::text(
                    "approved 42",
                )))
            }
            (
                HandlerPhase::Ready(_) | HandlerPhase::Waiting { .. },
                ToolHandlerInput::Cancel { reason },
            ) => {
                let cancellation = match self.phase {
                    HandlerPhase::Waiting { cancellation, .. } => cancellation,
                    _ => ToolCancellation::Cancelled,
                };
                self.finish();
                match cancellation {
                    ToolCancellation::Cancelled => Err(ToolError::Cancelled { tool: name, reason }),
                    ToolCancellation::Completed => {
                        Ok(ToolHandlerOutput::Completed(ToolResult::text("cancelled")))
                    }
                    ToolCancellation::TransportFailure => Err(ToolError::Transport {
                        tool: Some(name),
                        message: "scripted cancellation failure".into(),
                    }),
                }
            }
            (HandlerPhase::Finished, _)
            | (HandlerPhase::Ready(_), ToolHandlerInput::Interaction(_))
            | (HandlerPhase::Waiting { .. }, ToolHandlerInput::Invoke(_)) => {
                Err(ToolError::InvalidConfiguration {
                    message: "terminal or invalid handler transition".into(),
                })
            }
        }
    }
}

pub(super) enum CallerTools {
    First(MockTools),
    Second(MockTools),
}
pub(super) enum CallerHandler {
    First(MockHandler),
    Second(MockHandler),
}

impl ToolsetBackend for CallerTools {
    type Handler = CallerHandler;
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        match self {
            Self::First(tools) => tools.list_tools().await,
            Self::Second(tools) => tools.list_tools().await,
        }
    }
    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        match self {
            Self::First(tools) => tools.tool_handler(name).await.map(CallerHandler::First),
            Self::Second(tools) => tools.tool_handler(name).await.map(CallerHandler::Second),
        }
    }
    async fn shutdown(&self) -> Result<(), ToolError> {
        match self {
            Self::First(tools) => tools.shutdown().await,
            Self::Second(tools) => tools.shutdown().await,
        }
    }
}

impl ToolHandlerBackend for CallerHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match self {
            Self::First(handler) => handler.step(input).await,
            Self::Second(handler) => handler.step(input).await,
        }
    }
}

pub(super) enum CallerProvider {
    First(MockProvider),
    Second(MockProvider),
}
pub(super) enum CallerModel {
    First(MockModel),
    Second(MockModel),
}

impl ProviderBackend for CallerProvider {
    type Model = CallerModel;

    fn model(&self, id: ModelId) -> Result<Self::Model, SdkError> {
        match self {
            Self::First(provider) => provider.model(id).map(CallerModel::First),
            Self::Second(provider) => provider.model(id).map(CallerModel::Second),
        }
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, SdkError> {
        match self {
            Self::First(provider) => provider.list_models(request).await,
            Self::Second(provider) => provider.list_models(request).await,
        }
    }

    async fn shutdown(&self) -> Result<(), SdkError> {
        match self {
            Self::First(provider) => provider.shutdown().await,
            Self::Second(provider) => provider.shutdown().await,
        }
    }
}

impl ModelBackend for CallerModel {
    type Continuation = Continuation;
    type Session = std::convert::Infallible;

    fn model_id(&self) -> &ModelId {
        match self {
            Self::First(model) => model.model_id(),
            Self::Second(model) => model.model_id(),
        }
    }

    async fn generate(
        &self,
        request: &GenerateRequest<Continuation>,
    ) -> Result<GenerateResponse<Continuation>, SdkError> {
        match self {
            Self::First(model) => model.generate(request).await,
            Self::Second(model) => model.generate(request).await,
        }
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<Continuation>,
    ) -> Result<GenerateEventStream<Continuation>, SdkError> {
        match self {
            Self::First(model) => model.generate_stream(request).await,
            Self::Second(model) => model.generate_stream(request).await,
        }
    }
}
