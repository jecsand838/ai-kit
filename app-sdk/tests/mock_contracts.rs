//! Public workflow trait fixtures; no lower product, extension, process, or socket.
use app_sdk::AppError;
use app_sdk::workflow::{
    ActionBackend, ActionContext, ActionEvent, ActionMode, ActionRequest, ChildBatch,
    ChildCompletion, ChildExecutor, ConditionBackend, LifecycleEvent, LifecycleHooks, StateChange,
    StateTyping, WorkflowState,
};
use futures_util::StreamExt;
use serde_json::Map;
use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
pub mod mock_workflow;
use mock_workflow::{
    ActionChoice, ActionPlan, ActionScript, CallerCondition, CallerHooks, CallerTyping,
    ConditionChoice, ExecutorChoice, ExpectedAction, HookEvent, HookTrace, HooksChoice, TypedData,
    TypingChoice, TypingTrace,
};

struct Contract;
impl Contract {
    fn state(count: i64) -> WorkflowState {
        WorkflowState::new(Map::from_iter([("count".into(), count.into())])).unwrap()
    }
    fn request(count: i64) -> ActionRequest {
        ActionRequest {
            state: Self::state(count),
            inputs: Map::new(),
            context: ActionContext {
                workspace: "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc"
                    .parse()
                    .unwrap(),
                workflow: "mock".parse().unwrap(),
                run: "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse().unwrap(),
                occurrence: "00000000000040008000000000000001".into(),
                checkpoint: 0,
                action_ordinal: 0,
                node: "first".into(),
                attempt: 0,
                mode: ActionMode::Asynchronous,
                parent: None,
                parent_occurrence: None,
            },
        }
    }
    async fn action<A: ActionBackend>(action: A, script: &ActionScript) {
        assert_eq!(action.declaration().key, "count");
        assert_eq!(action.declaration().revision, "r1");
        assert_eq!(action.declaration().reads, ["count"]);
        assert_eq!(action.declaration().writes, ["count"]);
        let stream = action.start(Self::request(3));
        let mut stream = std::pin::pin!(stream);
        assert!(matches!(
            stream.next().await,
            Some(Ok(ActionEvent::Partial { .. }))
        ));
        let Some(Ok(ActionEvent::Completed { result, writes })) = stream.next().await else {
            panic!("completed terminal")
        };
        assert_eq!(result["result"], 4);
        assert_eq!(writes.len(), 1);
        assert!(stream.next().await.is_none());
        script.assert_drained();
        let seen = script.observations();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].state["count"], 3);
        assert_eq!(seen[0].mode, ActionMode::Asynchronous);
        assert_eq!(seen[0].inputs, Map::new());
        let expected = Self::request(3).context;
        assert_eq!(seen[0].node, expected.node);
        assert_eq!(seen[0].ordinal, expected.action_ordinal);
        assert_eq!(seen[0].workspace, expected.workspace.to_string());
        assert_eq!(seen[0].workflow, expected.workflow.to_string());
        assert_eq!(seen[0].run, expected.run.to_string());
        assert_eq!(seen[0].occurrence, expected.occurrence);
        assert_eq!(seen[0].checkpoint, expected.checkpoint);
        assert_eq!(seen[0].attempt, expected.attempt);
        assert_eq!(seen[0].parent, expected.parent.map(|id| id.to_string()));
        assert_eq!(seen[0].parent_occurrence, expected.parent_occurrence);
    }
    fn typing<T: StateTyping>(typing: &T) {
        let state = Self::state(1);
        let change = StateChange::Assign {
            values: Map::from_iter([("count".into(), 2.into())]),
        };
        let next = state.apply(&change, typing).unwrap();
        assert_eq!(next.values()["count"], 2);
        let data = typing.construct_data(&next).unwrap();
        assert_eq!(
            typing.construct_state(&data).unwrap().values(),
            next.values()
        );
        typing.validate_pre(&state, &change).unwrap();
        typing.validate_post(&state, &next, &change).unwrap();
        typing.validate_state(&next).unwrap();
        typing.validate_roundtrip(&next).unwrap();
        assert!(typing.validate_state(&Self::state(20)).is_err());
    }
}
#[tokio::test]
async fn action_struct_preserves_request_metadata_partial_terminal_and_local_stream() {
    let script = ActionScript::new(vec![ExpectedAction::new(
        3,
        ActionPlan::PartialThenComplete(4),
    )]);
    Contract::action(script.action("count"), &script).await;
    assert_eq!(script.drops(), 1);
}
#[tokio::test]
async fn exhaustive_action_enum_exercises_each_exact_stream_arm() {
    for second in [false, true] {
        let script = ActionScript::new(vec![ExpectedAction::new(
            3,
            ActionPlan::PartialThenComplete(4),
        )]);
        let action = if second {
            ActionChoice::Second(script.action("count"))
        } else {
            ActionChoice::First(script.action("count"))
        };
        Contract::action(action, &script).await;
        assert_eq!(script.drops(), 1);
    }
}
#[tokio::test]
async fn scripted_actions_reject_wrong_arguments_and_exhaustion_and_keep_errors() {
    let script = ActionScript::new(vec![
        ExpectedAction::new(1, ActionPlan::Complete(2)),
        ExpectedAction::new(1, ActionPlan::Failure("specific failure")),
    ]);
    let action = script.action("count");
    assert!(
        matches!(Box::pin(action.start(Contract::request(0))).next().await,Some(Err(AppError::InvalidInput(m))) if m=="wrong action arguments")
    );
    assert!(
        matches!(Box::pin(action.start(Contract::request(1))).next().await,Some(Err(AppError::Conflict(m))) if m=="specific failure")
    );
    assert!(
        matches!(Box::pin(action.start(Contract::request(1))).next().await,Some(Err(AppError::InvalidInput(m))) if m.contains("exhausted"))
    );
    script.assert_drained();
}
#[test]
fn required_script_expectations_cannot_silently_disappear() {
    let script = ActionScript::new(vec![ExpectedAction::new(0, ActionPlan::Complete(1))]);
    assert!(std::panic::catch_unwind(|| script.assert_drained()).is_err());
}
#[tokio::test]
async fn explicitly_malformed_sources_are_distinct_from_conforming_scripts() {
    let script = ActionScript::new(vec![
        ExpectedAction::new(0, ActionPlan::MalformedEof),
        ExpectedAction::new(0, ActionPlan::MalformedDuplicateTerminal),
    ]);
    let action = script.action("count");
    assert!(
        Box::pin(action.start(Contract::request(0)))
            .next()
            .await
            .is_none()
    );
    let mut twice = Box::pin(action.start(Contract::request(0)));
    assert!(matches!(
        twice.next().await,
        Some(Ok(ActionEvent::Completed { .. }))
    ));
    assert!(matches!(
        twice.next().await,
        Some(Ok(ActionEvent::Completed { .. }))
    ));
    script.assert_drained();
}
#[test]
fn condition_struct_and_both_enum_arms_forward_immutable_state_and_errors() {
    let concrete = CallerCondition::new(1, Ok(true));
    assert_eq!(concrete.declaration().key, "choose");
    assert!(concrete.evaluate(&Contract::state(1)).unwrap());
    assert_eq!(concrete.observations()[0]["count"], 1);
    assert!(matches!(
        concrete.evaluate(&Contract::state(2)),
        Err(AppError::InvalidInput(_))
    ));
    for condition in [
        ConditionChoice::First(CallerCondition::new(1, Ok(false))),
        ConditionChoice::Second(CallerCondition::new(1, Ok(false))),
    ] {
        assert_eq!(condition.declaration().revision, "r1");
        assert!(!condition.evaluate(&Contract::state(1)).unwrap());
    }
    let failed = CallerCondition::new(1, Err("condition error"));
    assert!(
        matches!(failed.evaluate(&Contract::state(1)),Err(AppError::Conflict(m)) if m=="condition error")
    );
}
#[test]
fn typing_struct_and_enums_preserve_local_data_and_all_validation_methods() {
    let trace = Arc::new(TypingTrace::default());
    Contract::typing(&CallerTyping::new(&trace, Some(5)));
    for typing in [
        TypingChoice::First(CallerTyping::new(&trace, Some(5))),
        TypingChoice::Second(CallerTyping::new(&trace, Some(5))),
    ] {
        Contract::typing(&typing);
    }
    for method in ["construct_data", "construct_state", "pre", "post", "state"] {
        assert!(trace.calls().contains(&method));
    }
}
#[test]
fn typing_rejects_mismatched_enum_data_before_delegation() {
    let trace = Arc::new(TypingTrace::default());
    let typing = TypingChoice::First(CallerTyping::new(&trace, None));
    let wrong = TypedData::Second(
        CallerTyping::new(&Arc::new(TypingTrace::default()), None)
            .construct_data(&Contract::state(0))
            .unwrap(),
    );
    assert!(matches!(
        typing.construct_state(&wrong),
        Err(AppError::InvalidInput(_))
    ));
    assert!(trace.calls().is_empty());
}
#[tokio::test]
async fn hooks_struct_and_enums_preserve_borrowed_events_and_typed_failures() {
    let trace = Arc::new(HookTrace::default());
    let run = "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse().unwrap();
    let event = LifecycleEvent::ExecuteStarted {
        run: &run,
        occurrence: "00000000000040008000000000000001",
    };
    let concrete = CallerHooks::new(&trace, None);
    concrete.observe(&event).unwrap();
    concrete.observe_async(&event).await.unwrap();
    for hooks in [
        HooksChoice::First(CallerHooks::new(&trace, None)),
        HooksChoice::Second(CallerHooks::new(&trace, None)),
    ] {
        hooks.observe(&event).unwrap();
        hooks.observe_async(&event).await.unwrap();
    }
    assert_eq!(
        trace.events(),
        vec![
            (false, HookEvent::ExecuteStarted),
            (true, HookEvent::ExecuteStarted),
            (false, HookEvent::ExecuteStarted),
            (true, HookEvent::ExecuteStarted),
            (false, HookEvent::ExecuteStarted),
            (true, HookEvent::ExecuteStarted)
        ]
    );
    let failed = CallerHooks::new(&trace, Some(HookEvent::ExecuteStarted));
    assert!(matches!(failed.observe(&event), Err(AppError::Conflict(_))));
    assert!(matches!(
        failed.observe_async(&event).await,
        Err(AppError::Conflict(_))
    ));
}
struct LocalChild<'a> {
    polls: &'a Cell<usize>,
    local: Rc<()>,
    pinned: PhantomPinned,
}
impl Future for LocalChild<'_> {
    type Output = Result<ChildCompletion, AppError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let source = self.as_ref().get_ref();
        source.polls.set(source.polls.get() + 1);
        assert_eq!(Rc::strong_count(&source.local), 1);
        let _ = &source.pinned;
        Poll::Ready(Err(AppError::Conflict("local child".into())))
    }
}
struct ChildSignature;
impl ChildSignature {
    fn preserve<'a>(
        executor: &'a ExecutorChoice,
        batch: ChildBatch<LocalChild<'a>>,
    ) -> impl Future<Output = Result<Vec<ChildCompletion>, AppError>> + 'a {
        executor.execute(batch)
    }
}
#[test]
fn child_executor_positive_contract_accepts_borrowed_local_non_unpin_future() {
    let _ = ChildSignature::preserve;
    let polls = Cell::new(0);
    let child = LocalChild {
        polls: &polls,
        local: Rc::new(()),
        pinned: PhantomPinned,
    };
    let mut child = std::pin::pin!(child);
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        child.as_mut().poll(&mut context),
        Poll::Ready(Err(AppError::Conflict(_)))
    ));
    assert_eq!(polls.get(), 1);
}
