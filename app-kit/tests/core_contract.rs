//! Downstream compile contracts are compiled by Cargo against its selected
//! dependency graph. No subprocess guesses artifacts from other feature lanes.
//! Missing target APIs deliberately fail compilation until the core slice lands.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use app_kit::AppError;
use app_kit::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionRequest, ChildBatch, ChildCompletion,
    ChildExecutor, ConditionBackend, ConditionDeclaration, LifecycleEvent, LifecycleHooks,
    StateChange, StateTyping, WorkflowState,
};
use futures_util::Stream;

pub struct CallerAction {
    declaration: ActionDeclaration,
}

pub struct CallerLocalStream {
    captured: Rc<usize>,
    done: bool,
}

impl Stream for CallerLocalStream {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        self.done = true;
        let result = serde_json::Map::from_iter([("count".into(), (*self.captured).into())]);
        Poll::Ready(Some(Ok(ActionEvent::Completed {
            result,
            writes: Vec::new(),
        })))
    }
}

impl ActionBackend for CallerAction {
    type Stream = CallerLocalStream;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, _: ActionRequest) -> Self::Stream {
        CallerLocalStream {
            captured: Rc::new(1),
            done: false,
        }
    }
}

pub struct CallerCondition {
    declaration: ConditionDeclaration,
}

impl ConditionBackend for CallerCondition {
    fn declaration(&self) -> &ConditionDeclaration {
        &self.declaration
    }
    fn evaluate(&self, _: &WorkflowState) -> Result<bool, AppError> {
        Ok(true)
    }
}

pub struct CallerTyping;

impl StateTyping for CallerTyping {
    type Data = Rc<usize>;
    fn construct_data(&self, _: &WorkflowState) -> Result<Self::Data, AppError> {
        Ok(Rc::new(1))
    }
    fn construct_state(&self, _: &Self::Data) -> Result<WorkflowState, AppError> {
        WorkflowState::new(serde_json::Map::new())
    }
    fn validate_pre(&self, _: &WorkflowState, _: &StateChange) -> Result<(), AppError> {
        Ok(())
    }
    fn validate_post(
        &self,
        _: &WorkflowState,
        _: &WorkflowState,
        _: &StateChange,
    ) -> Result<(), AppError> {
        Ok(())
    }
    fn validate_state(&self, _: &WorkflowState) -> Result<(), AppError> {
        Ok(())
    }
}

pub struct CallerHooks;

impl LifecycleHooks for CallerHooks {
    fn observe(&self, _: &LifecycleEvent<'_>) -> Result<(), AppError> {
        Ok(())
    }
    async fn observe_async<'a>(&'a self, _: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        Ok(())
    }
}

pub struct CallerExecutor;

pub struct Reversed<'a, F> {
    executor: &'a CallerExecutor,
    pending: Vec<Pin<Box<F>>>,
    completed: Vec<ChildCompletion>,
}

impl<F: Future<Output = Result<ChildCompletion, AppError>>> Future for Reversed<'_, F> {
    type Output = Result<Vec<ChildCompletion>, AppError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _ = self.executor;
        while let Some(child) = self.pending.last_mut() {
            match child.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(completion)) => {
                    self.pending.pop();
                    self.completed.push(completion);
                }
            }
        }
        Poll::Ready(Ok(std::mem::take(&mut self.completed)))
    }
}

impl ChildExecutor for CallerExecutor {
    type Execution<'a, F>
        = Reversed<'a, F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        Reversed {
            executor: self,
            pending: batch
                .into_tasks()
                .expect("the unpolled batch transfers its existing pins"),
            completed: Vec::new(),
        }
    }
}

pub fn configure<CS: storage_kit::StateStoreBackend, A: agents_kit::AgentBackend>(
    state: CS,
    agent: A,
    action: CallerAction,
    condition: CallerCondition,
) -> app_kit::AppConfig<
    CS,
    A,
    CallerAction,
    CallerCondition,
    CallerTyping,
    CallerHooks,
    CallerExecutor,
> {
    app_kit::AppConfig::new(
        state,
        vec![app_kit::AgentSpec::new("worker".into(), "r1".into(), agent)],
        Vec::new(),
    )
    .with_actions(vec![action])
    .with_conditions(vec![condition])
    .with_state_typing(CallerTyping)
    .with_hooks(CallerHooks)
    .with_child_executor(CallerExecutor)
}

#[test]
fn state_typing_preserves_caller_local_data() {
    let state = WorkflowState::new(serde_json::Map::new()).expect("empty ordered state");
    let typing = CallerTyping;
    let data = typing.construct_data(&state).expect("caller conversion");
    assert_eq!(*data, 1);
    typing
        .validate_roundtrip(&state)
        .expect("default roundtrip accepts reconstruction");
}
