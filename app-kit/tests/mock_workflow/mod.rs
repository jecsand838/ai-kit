//! Direct workflow trait owners. Action sources and typed data intentionally stay local.
use app_kit::AppError;
use app_kit::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, ChildBatch,
    ChildCompletion, ChildExecutor, ConditionBackend, ConditionDeclaration, LifecycleEvent,
    LifecycleHooks, StateChange, StateTyping, WorkflowState,
};
use futures_util::Stream;
use futures_util::future::Either;
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub enum ActionPlan {
    Complete(i64),
    PartialThenComplete(i64),
    Failure(&'static str),
    Gated(Arc<ActionGate>, i64),
    MalformedEof,
    MalformedDuplicateTerminal,
}
#[derive(Default)]
pub struct ActionGate {
    entered: tokio::sync::Notify,
    released: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
impl ActionGate {
    pub async fn entered(&self) {
        self.entered.notified().await;
    }
    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    fn poll(&self, cx: &Context<'_>) -> Poll<()> {
        let mut waker = self.waker.lock().unwrap();
        if self.released.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        *waker = Some(cx.waker().clone());
        self.entered.notify_one();
        Poll::Pending
    }
}
pub struct ExpectedAction {
    count: i64,
    inputs: Map<String, Value>,
    plan: ActionPlan,
}
impl ExpectedAction {
    pub fn new(count: i64, plan: ActionPlan) -> Self {
        Self {
            count,
            inputs: Map::new(),
            plan,
        }
    }
    pub fn with_inputs(mut self, inputs: Map<String, Value>) -> Self {
        self.inputs = inputs;
        self
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct ActionObservation {
    pub state: Map<String, Value>,
    pub inputs: Map<String, Value>,
    pub node: String,
    pub ordinal: u64,
    pub mode: ActionMode,
    pub workspace: String,
    pub workflow: String,
    pub run: String,
    pub occurrence: String,
    pub checkpoint: u64,
    pub attempt: u64,
    pub parent: Option<String>,
    pub parent_occurrence: Option<String>,
}
pub struct ActionScript {
    expected: Mutex<VecDeque<ExpectedAction>>,
    observed: Mutex<Vec<ActionObservation>>,
    drops: AtomicUsize,
}
pub struct CallerAction {
    declaration: ActionDeclaration,
    script: Arc<ActionScript>,
}
pub struct LocalActionStream {
    events: RefCell<VecDeque<Result<ActionEvent, AppError>>>,
    pinned: PhantomPinned,
    local: Rc<()>,
    gate: Option<Arc<ActionGate>>,
    script: Arc<ActionScript>,
}
pub enum ActionChoice {
    First(CallerAction),
    Second(CallerAction),
}
pub enum ActionStreamChoice {
    First(Pin<Box<LocalActionStream>>),
    Second(Pin<Box<LocalActionStream>>),
}
impl ActionScript {
    pub fn new(expected: Vec<ExpectedAction>) -> Arc<Self> {
        Arc::new(Self {
            expected: Mutex::new(expected.into()),
            observed: Mutex::new(Vec::new()),
            drops: AtomicUsize::new(0),
        })
    }
    pub fn action(self: &Arc<Self>, key: &str) -> CallerAction {
        CallerAction {
            declaration: ActionDeclaration {
                key: key.into(),
                revision: "r1".into(),
                reads: vec!["count".into()],
                writes: vec!["count".into()],
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: vec!["mock".into()],
                source: Some("caller-owned test".into()),
                mode: ActionMode::Both,
            },
            script: Arc::clone(self),
        }
    }
    pub fn observations(&self) -> Vec<ActionObservation> {
        self.observed.lock().unwrap().clone()
    }
    pub fn drops(&self) -> usize {
        self.drops.load(Ordering::SeqCst)
    }
    pub fn assert_drained(&self) {
        assert!(
            self.expected.lock().unwrap().is_empty(),
            "unconsumed required action"
        );
    }
}
impl CallerAction {
    pub fn requiring(mut self, key: &str) -> Self {
        self.declaration.required_inputs.push(key.into());
        self
    }
}
impl ActionBackend for CallerAction {
    type Stream = LocalActionStream;
    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }
    fn start(&self, r: ActionRequest) -> Self::Stream {
        self.script
            .observed
            .lock()
            .unwrap()
            .push(ActionObservation {
                state: r.state.values().clone(),
                inputs: r.inputs.clone(),
                node: r.context.node,
                ordinal: r.context.action_ordinal,
                mode: r.context.mode,
                workspace: r.context.workspace.to_string(),
                workflow: r.context.workflow.to_string(),
                run: r.context.run.to_string(),
                occurrence: r.context.occurrence,
                checkpoint: r.context.checkpoint,
                attempt: r.context.attempt,
                parent: r.context.parent.map(|id| id.to_string()),
                parent_occurrence: r.context.parent_occurrence,
            });
        let expected = self.script.expected.lock().unwrap().pop_front();
        let mut gate = None;
        let events = match expected {
            None => vec![Err(AppError::InvalidInput(
                "unexpected action: script exhausted".into(),
            ))],
            Some(e)
                if r.state.values().get("count") != Some(&Value::from(e.count))
                    || e.inputs != r.inputs =>
            {
                vec![Err(AppError::InvalidInput("wrong action arguments".into()))]
            }
            Some(e) => match e.plan {
                ActionPlan::Complete(value) => vec![Ok(ActionEvent::Completed {
                    result: Map::from_iter([("result".into(), value.into())]),
                    writes: vec![StateChange::Assign {
                        values: Map::from_iter([("count".into(), value.into())]),
                    }],
                })],
                ActionPlan::PartialThenComplete(value) => vec![
                    Ok(ActionEvent::Partial {
                        result: Map::from_iter([("progress".into(), true.into())]),
                    }),
                    Ok(ActionEvent::Completed {
                        result: Map::from_iter([("result".into(), value.into())]),
                        writes: vec![StateChange::Assign {
                            values: Map::from_iter([("count".into(), value.into())]),
                        }],
                    }),
                ],
                ActionPlan::Failure(message) => vec![Err(AppError::Conflict(message.into()))],
                ActionPlan::Gated(selected, value) => {
                    gate = Some(selected);
                    vec![Ok(ActionEvent::Completed {
                        result: Map::new(),
                        writes: vec![StateChange::Assign {
                            values: Map::from_iter([("count".into(), value.into())]),
                        }],
                    })]
                }
                ActionPlan::MalformedEof => Vec::new(),
                ActionPlan::MalformedDuplicateTerminal => vec![
                    Ok(ActionEvent::Completed {
                        result: Map::new(),
                        writes: Vec::new(),
                    }),
                    Ok(ActionEvent::Completed {
                        result: Map::new(),
                        writes: Vec::new(),
                    }),
                ],
            },
        };
        LocalActionStream {
            events: RefCell::new(events.into()),
            pinned: PhantomPinned,
            local: Rc::new(()),
            gate,
            script: Arc::clone(&self.script),
        }
    }
}
impl Stream for LocalActionStream {
    type Item = Result<ActionEvent, AppError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let source = self.as_ref().get_ref();
        let _ = &source.pinned;
        if source
            .gate
            .as_ref()
            .is_some_and(|gate| gate.poll(cx).is_pending())
        {
            return Poll::Pending;
        }
        assert_eq!(Rc::strong_count(&source.local), 1);
        Poll::Ready(source.events.borrow_mut().pop_front())
    }
}
impl Drop for LocalActionStream {
    fn drop(&mut self) {
        self.script.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl ActionBackend for ActionChoice {
    type Stream = ActionStreamChoice;
    fn declaration(&self) -> &ActionDeclaration {
        match self {
            Self::First(a) => a.declaration(),
            Self::Second(a) => a.declaration(),
        }
    }
    fn start(&self, r: ActionRequest) -> Self::Stream {
        match self {
            Self::First(a) => ActionStreamChoice::First(Box::pin(a.start(r))),
            Self::Second(a) => ActionStreamChoice::Second(Box::pin(a.start(r))),
        }
    }
}
impl Stream for ActionStreamChoice {
    type Item = Result<ActionEvent, AppError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::First(s) => s.as_mut().poll_next(cx),
            Self::Second(s) => s.as_mut().poll_next(cx),
        }
    }
}
pub struct CallerCondition {
    declaration: ConditionDeclaration,
    expected: i64,
    result: Result<bool, &'static str>,
    observed: Arc<Mutex<Vec<Map<String, Value>>>>,
}
pub enum ConditionChoice {
    First(CallerCondition),
    Second(CallerCondition),
}
impl CallerCondition {
    pub fn new(expected: i64, result: Result<bool, &'static str>) -> Self {
        Self {
            declaration: ConditionDeclaration {
                key: "choose".into(),
                revision: "r1".into(),
                reads: vec!["count".into()],
            },
            expected,
            result,
            observed: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub fn observations(&self) -> Vec<Map<String, Value>> {
        self.observed.lock().unwrap().clone()
    }
}
impl ConditionBackend for CallerCondition {
    fn declaration(&self) -> &ConditionDeclaration {
        &self.declaration
    }
    fn evaluate(&self, state: &WorkflowState) -> Result<bool, AppError> {
        self.observed.lock().unwrap().push(state.values().clone());
        if state.values().get("count") != Some(&Value::from(self.expected)) {
            return Err(AppError::InvalidInput("wrong condition state".into()));
        }
        self.result.map_err(|m| AppError::Conflict(m.into()))
    }
}
impl ConditionBackend for ConditionChoice {
    fn declaration(&self) -> &ConditionDeclaration {
        match self {
            Self::First(c) => c.declaration(),
            Self::Second(c) => c.declaration(),
        }
    }
    fn evaluate(&self, s: &WorkflowState) -> Result<bool, AppError> {
        match self {
            Self::First(c) => c.evaluate(s),
            Self::Second(c) => c.evaluate(s),
        }
    }
}
#[derive(Default)]
pub struct TypingTrace {
    calls: Mutex<Vec<&'static str>>,
}
impl TypingTrace {
    pub fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}
pub struct CallerTyping {
    trace: Arc<TypingTrace>,
    maximum: Option<i64>,
}
pub enum TypingChoice {
    First(CallerTyping),
    Second(CallerTyping),
}
pub struct LocalData {
    state: WorkflowState,
    local: Rc<()>,
    pinned: PhantomPinned,
}
pub enum TypedData {
    First(LocalData),
    Second(LocalData),
}
impl CallerTyping {
    pub fn new(trace: &Arc<TypingTrace>, maximum: Option<i64>) -> Self {
        Self {
            trace: Arc::clone(trace),
            maximum,
        }
    }
    fn validate(&self, state: &WorkflowState) -> Result<(), AppError> {
        if self.maximum.is_some_and(|max| {
            state
                .values()
                .get("count")
                .and_then(Value::as_i64)
                .is_some_and(|value| value > max)
        }) {
            Err(AppError::InvalidInput("typing count limit".into()))
        } else {
            Ok(())
        }
    }
}
impl StateTyping for CallerTyping {
    type Data = LocalData;
    fn construct_data(&self, s: &WorkflowState) -> Result<Self::Data, AppError> {
        self.trace.calls.lock().unwrap().push("construct_data");
        self.validate(s)?;
        Ok(LocalData {
            state: s.clone(),
            local: Rc::new(()),
            pinned: PhantomPinned,
        })
    }
    fn construct_state(&self, d: &Self::Data) -> Result<WorkflowState, AppError> {
        self.trace.calls.lock().unwrap().push("construct_state");
        assert_eq!(Rc::strong_count(&d.local), 1);
        let _ = &d.pinned;
        self.validate(&d.state)?;
        Ok(d.state.clone())
    }
    fn validate_pre(&self, s: &WorkflowState, _: &StateChange) -> Result<(), AppError> {
        self.trace.calls.lock().unwrap().push("pre");
        self.validate(s)
    }
    fn validate_post(
        &self,
        _: &WorkflowState,
        s: &WorkflowState,
        _: &StateChange,
    ) -> Result<(), AppError> {
        self.trace.calls.lock().unwrap().push("post");
        self.validate(s)
    }
    fn validate_state(&self, s: &WorkflowState) -> Result<(), AppError> {
        self.trace.calls.lock().unwrap().push("state");
        self.validate(s)
    }
}
impl StateTyping for TypingChoice {
    type Data = TypedData;
    fn construct_data(&self, s: &WorkflowState) -> Result<Self::Data, AppError> {
        match self {
            Self::First(t) => t.construct_data(s).map(TypedData::First),
            Self::Second(t) => t.construct_data(s).map(TypedData::Second),
        }
    }
    fn construct_state(&self, d: &Self::Data) -> Result<WorkflowState, AppError> {
        match (self, d) {
            (Self::First(t), TypedData::First(d)) => t.construct_state(d),
            (Self::Second(t), TypedData::Second(d)) => t.construct_state(d),
            (Self::First(_), TypedData::Second(_)) | (Self::Second(_), TypedData::First(_)) => {
                Err(AppError::InvalidInput("typing variant mismatch".into()))
            }
        }
    }
    fn validate_pre(&self, s: &WorkflowState, c: &StateChange) -> Result<(), AppError> {
        match self {
            Self::First(t) => t.validate_pre(s, c),
            Self::Second(t) => t.validate_pre(s, c),
        }
    }
    fn validate_post(
        &self,
        b: &WorkflowState,
        s: &WorkflowState,
        c: &StateChange,
    ) -> Result<(), AppError> {
        match self {
            Self::First(t) => t.validate_post(b, s, c),
            Self::Second(t) => t.validate_post(b, s, c),
        }
    }
    fn validate_state(&self, s: &WorkflowState) -> Result<(), AppError> {
        match self {
            Self::First(t) => t.validate_state(s),
            Self::Second(t) => t.validate_state(s),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookEvent {
    Registered,
    ChildFinished,
    ExecuteStarted,
    ExecuteFinished,
    StepStarted,
    StepFinished,
    StreamStarted,
    StreamItem,
    StreamFinished,
    SpanStarted,
    SpanAttribute,
    SpanFinished,
}
impl HookEvent {
    fn from_event(event: &LifecycleEvent<'_>) -> Self {
        match event {
            LifecycleEvent::Registered { .. } => Self::Registered,
            LifecycleEvent::ChildFinished { .. } => Self::ChildFinished,
            LifecycleEvent::ExecuteStarted { .. } => Self::ExecuteStarted,
            LifecycleEvent::ExecuteFinished { .. } => Self::ExecuteFinished,
            LifecycleEvent::StepStarted { .. } => Self::StepStarted,
            LifecycleEvent::StepFinished { .. } => Self::StepFinished,
            LifecycleEvent::StreamStarted { .. } => Self::StreamStarted,
            LifecycleEvent::StreamItem { .. } => Self::StreamItem,
            LifecycleEvent::StreamFinished { .. } => Self::StreamFinished,
            LifecycleEvent::SpanStarted { .. } => Self::SpanStarted,
            LifecycleEvent::SpanAttribute { .. } => Self::SpanAttribute,
            LifecycleEvent::SpanFinished { .. } => Self::SpanFinished,
        }
    }
}
#[derive(Default)]
pub struct HookTrace {
    events: Mutex<Vec<(bool, HookEvent)>>,
    completions: Mutex<Vec<Value>>,
}
impl HookTrace {
    pub fn completions(&self) -> Vec<Value> {
        self.completions.lock().unwrap().clone()
    }
    fn completion(&self, event: &LifecycleEvent<'_>) {
        if let LifecycleEvent::ExecuteFinished {
            failure: Some(failure),
            ..
        } = event
        {
            self.completions
                .lock()
                .unwrap()
                .push(Self::failure(failure));
        }
    }
    fn failure(failure: &app_kit::workflow::WorkflowFailure) -> Value {
        match failure {
            app_kit::workflow::WorkflowFailure::Action(_) => serde_json::json!({"kind":"action"}),
            app_kit::workflow::WorkflowFailure::Hook { cause, .. } => {
                serde_json::json!({"kind":"hook","cause":cause.as_deref().map(Self::failure)})
            }
            _ => serde_json::json!({"kind":"other"}),
        }
    }
    pub fn events(&self) -> Vec<(bool, HookEvent)> {
        self.events.lock().unwrap().clone()
    }
}
pub struct CallerHooks {
    trace: Arc<HookTrace>,
    fail: Option<HookEvent>,
}
pub enum HooksChoice {
    First(CallerHooks),
    Second(CallerHooks),
}
impl CallerHooks {
    pub fn new(trace: &Arc<HookTrace>, fail: Option<HookEvent>) -> Self {
        Self {
            trace: Arc::clone(trace),
            fail,
        }
    }
}
impl LifecycleHooks for CallerHooks {
    fn observe(&self, e: &LifecycleEvent<'_>) -> Result<(), AppError> {
        self.trace.completion(e);
        let kind = HookEvent::from_event(e);
        self.trace.events.lock().unwrap().push((false, kind));
        if self.fail == Some(kind) {
            Err(AppError::Conflict("mock synchronous hook failure".into()))
        } else {
            Ok(())
        }
    }
    async fn observe_async<'a>(&'a self, e: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        self.trace.completion(e);
        let kind = HookEvent::from_event(e);
        self.trace.events.lock().unwrap().push((true, kind));
        if self.fail == Some(kind) {
            Err(AppError::Conflict("mock asynchronous hook failure".into()))
        } else {
            Ok(())
        }
    }
}
impl LifecycleHooks for HooksChoice {
    fn observe(&self, e: &LifecycleEvent<'_>) -> Result<(), AppError> {
        match self {
            Self::First(h) => h.observe(e),
            Self::Second(h) => h.observe(e),
        }
    }
    async fn observe_async<'a>(&'a self, e: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        match self {
            Self::First(h) => h.observe_async(e).await,
            Self::Second(h) => h.observe_async(e).await,
        }
    }
}
#[derive(Default)]
pub struct ChildTrace {
    batches: Mutex<Vec<(ActionMode, usize)>>,
}
impl ChildTrace {
    pub fn batches(&self) -> Vec<(ActionMode, usize)> {
        self.batches.lock().unwrap().clone()
    }
}
pub struct ForwardExecutor {
    trace: Arc<ChildTrace>,
}
pub struct ReverseExecutor {
    trace: Arc<ChildTrace>,
    omit_completion: bool,
}
pub enum ExecutorChoice {
    Forward(ForwardExecutor),
    Reverse(ReverseExecutor),
}
pub struct ReverseFuture<'a, F> {
    executor: &'a ReverseExecutor,
    pending: Vec<Pin<Box<F>>>,
    completed: Vec<ChildCompletion>,
    error: Option<AppError>,
}
impl ForwardExecutor {
    pub fn new(trace: &Arc<ChildTrace>) -> Self {
        Self {
            trace: Arc::clone(trace),
        }
    }
}
impl ReverseExecutor {
    pub fn new(trace: &Arc<ChildTrace>) -> Self {
        Self {
            trace: Arc::clone(trace),
            omit_completion: false,
        }
    }
    pub fn malformed_missing_completion(trace: &Arc<ChildTrace>) -> Self {
        Self {
            trace: Arc::clone(trace),
            omit_completion: true,
        }
    }
}
impl ChildExecutor for ForwardExecutor {
    type Execution<'a, F>
        = ChildBatch<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        self.trace
            .batches
            .lock()
            .unwrap()
            .push((batch.mode(), batch.len()));
        batch
    }
}
impl ChildExecutor for ReverseExecutor {
    type Execution<'a, F>
        = ReverseFuture<'a, F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        self.trace
            .batches
            .lock()
            .unwrap()
            .push((batch.mode(), batch.len()));
        let (pending, error) = match batch.into_tasks() {
            Ok(tasks) => (tasks, None),
            Err(error) => (Vec::new(), Some(error)),
        };
        ReverseFuture {
            executor: self,
            pending,
            completed: Vec::new(),
            error,
        }
    }
}
impl<F: Future<Output = Result<ChildCompletion, AppError>>> Future for ReverseFuture<'_, F> {
    type Output = Result<Vec<ChildCompletion>, AppError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _ = self.executor;
        if let Some(error) = self.error.take() {
            return Poll::Ready(Err(error));
        }
        while let Some(child) = self.pending.last_mut() {
            match child.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(completed)) => {
                    self.pending.pop();
                    self.completed.push(completed);
                }
            }
        }
        if self.executor.omit_completion {
            self.completed.pop();
        }
        Poll::Ready(Ok(std::mem::take(&mut self.completed)))
    }
}
impl ChildExecutor for ExecutorChoice {
    type Execution<'a, F>
        = Either<ChildBatch<F>, ReverseFuture<'a, F>>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        match self {
            Self::Forward(e) => Either::Left(e.execute(batch)),
            Self::Reverse(e) => Either::Right(e.execute(batch)),
        }
    }
}
