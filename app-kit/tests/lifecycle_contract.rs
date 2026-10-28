//! Direct hook behavior and the exhaustive borrowed event vocabulary.
//! Runtime dispatch order and tracking wire projections are separate contracts.

use std::future::Future;
use std::sync::Mutex;

use app_kit::workflow::{
    ActionContext, ActionMode, ActionOutcome, Checkpoint, GraphDescription, LifecycleEvent,
    LifecycleHooks, NoHooks, RunId, RunLineage, RunSnapshot, WorkflowFailure, WorkflowState,
};
use app_kit::{AppError, WorkspaceId};
use serde_json::{Map, Value, json};

struct DefaultHooks;

impl LifecycleHooks for DefaultHooks {}

// No Clone is required on a caller hook. Each call borrows the original event
// and its data; the async future keeps those borrows across a suspension.
struct CallerHooks {
    calls: Mutex<Vec<String>>,
    reject: bool,
}

impl CallerHooks {
    fn context() -> ActionContext {
        ActionContext {
            workspace: "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc"
                .parse()
                .unwrap(),
            workflow: "hook-contract".parse().unwrap(),
            run: "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse().unwrap(),
            occurrence: "8a7c251eb5b84d9e8f0b123456789abc".into(),
            checkpoint: 3,
            action_ordinal: 1,
            node: "action".into(),
            attempt: 2,
            mode: ActionMode::Asynchronous,
            parent: None,
            parent_occurrence: None,
        }
    }

    fn inspect(event: &LifecycleEvent<'_>) -> String {
        match event {
            LifecycleEvent::Registered {
                workspace,
                run,
                occurrence,
                graph,
                parent,
                lineage,
            } => {
                let _: &WorkspaceId = workspace;
                let _: &RunId = run;
                let _: &str = occurrence;
                let _: &GraphDescription = graph;
                let _: &Option<&ActionContext> = parent;
                let _: &Option<&RunLineage> = lineage;
                "registered".into()
            }
            LifecycleEvent::ChildFinished { parent, snapshot } => {
                let _: &ActionContext = parent;
                let _: &RunSnapshot = snapshot;
                "child".into()
            }
            LifecycleEvent::ExecuteStarted { run, occurrence } => {
                let _: &RunId = run;
                let _: &str = occurrence;
                "execute-start".into()
            }
            LifecycleEvent::ExecuteFinished { snapshot, failure } => {
                let _: &RunSnapshot = snapshot;
                let _: &Option<&WorkflowFailure> = failure;
                "execute-finish".into()
            }
            LifecycleEvent::StepStarted {
                context,
                state,
                inputs,
            } => {
                assert_eq!(context.action_ordinal, 1);
                assert_eq!(state.values().get("value"), Some(&json!(4)));
                assert_eq!(inputs.get("transient"), Some(&json!("caller")));
                "step".into()
            }
            LifecycleEvent::StepFinished {
                context,
                outcome,
                checkpoint,
            } => {
                let _: &ActionContext = context;
                let _: &Option<Result<&ActionOutcome, &WorkflowFailure>> = outcome;
                let _: &Checkpoint = checkpoint;
                "step-finish".into()
            }
            LifecycleEvent::StreamStarted { context, span } => {
                let _: &ActionContext = context;
                let _: &Option<&str> = span;
                "stream-start".into()
            }
            LifecycleEvent::StreamItem {
                context,
                span,
                index,
                result,
            } => {
                let _: &ActionContext = context;
                let _: &Option<&str> = span;
                let _: &usize = index;
                let _: &Map<String, Value> = result;
                "stream-item".into()
            }
            LifecycleEvent::StreamFinished {
                context,
                span,
                items,
                failure,
            } => {
                let _: &ActionContext = context;
                let _: &Option<&str> = span;
                let _: &usize = items;
                let _: &Option<&WorkflowFailure> = failure;
                "stream-finish".into()
            }
            LifecycleEvent::SpanStarted {
                context,
                id,
                name,
                parent,
                dependencies,
            } => {
                let _: &ActionContext = context;
                let _: &&str = id;
                let _: &&str = name;
                let _: &Option<&str> = parent;
                let _: &&[String] = dependencies;
                "span-start".into()
            }
            LifecycleEvent::SpanAttribute {
                context,
                span,
                key,
                value,
                tags,
            } => {
                let _: &ActionContext = context;
                let _: &Option<&str> = span;
                let _: &&str = key;
                let _: &Value = value;
                let _: &Map<String, Value> = tags;
                "span-attribute".into()
            }
            LifecycleEvent::SpanFinished { context, id } => {
                let _: &ActionContext = context;
                let _: &&str = id;
                "span-finish".into()
            }
        }
    }

    async fn assert_send(future: impl Future<Output = Result<(), AppError>> + Send) {
        future.await.expect("borrowed asynchronous hook");
    }
}

impl LifecycleHooks for CallerHooks {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        self.calls.lock().unwrap().push(Self::inspect(event));
        if self.reject {
            Err(AppError::Conflict("caller synchronous hook".into()))
        } else {
            Ok(())
        }
    }

    async fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        tokio::task::yield_now().await;
        self.calls.lock().unwrap().push(Self::inspect(event));
        if self.reject {
            Err(AppError::InvalidInput("caller asynchronous hook".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn default_hooks_succeed_without_requiring_overrides() {
    let context = CallerHooks::context();
    let event = LifecycleEvent::ExecuteStarted {
        run: &context.run,
        occurrence: &context.occurrence,
    };
    let supplied = NoHooks;
    supplied
        .observe(&event)
        .expect("supplied synchronous default");
    CallerHooks::assert_send(supplied.observe_async(&event)).await;
    let caller = DefaultHooks;
    caller.observe(&event).expect("caller synchronous default");
    CallerHooks::assert_send(caller.observe_async(&event)).await;
}

#[tokio::test]
async fn hook_future_borrows_the_original_state_and_inputs_across_await() {
    let context = CallerHooks::context();
    let state = WorkflowState::new(Map::from_iter([("value".into(), json!(4))])).unwrap();
    let inputs = Map::from_iter([("transient".into(), json!("caller"))]);
    let event = LifecycleEvent::StepStarted {
        context: &context,
        state: &state,
        inputs: &inputs,
    };
    let hooks = CallerHooks {
        calls: Mutex::new(Vec::new()),
        reject: false,
    };
    hooks.observe(&event).expect("direct synchronous callback");
    CallerHooks::assert_send(hooks.observe_async(&event)).await;
    assert_eq!(*hooks.calls.lock().unwrap(), ["step", "step"]);
    assert_eq!(inputs.get("transient"), Some(&json!("caller")));
}

#[tokio::test]
async fn direct_hook_errors_keep_the_callers_category_and_message() {
    let context = CallerHooks::context();
    let event = LifecycleEvent::ExecuteStarted {
        run: &context.run,
        occurrence: &context.occurrence,
    };
    let hooks = CallerHooks {
        calls: Mutex::new(Vec::new()),
        reject: true,
    };
    assert!(
        matches!(hooks.observe(&event), Err(AppError::Conflict(message))
        if message == "caller synchronous hook")
    );
    assert!(
        matches!(hooks.observe_async(&event).await, Err(AppError::InvalidInput(message))
        if message == "caller asynchronous hook")
    );
    assert_eq!(
        *hooks.calls.lock().unwrap(),
        ["execute-start", "execute-start"]
    );
}
