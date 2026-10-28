//! Caller-owned action and condition inventories, without executing a workflow.
//! Input metadata acceptance follows the fixed rurr archive's
//! crates/rurr-core/src/action/function.rs:735–891; revision coexistence and
//! inventory replacement follow the App configuration contract.

use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, ChildBatch,
    ChildCompletion, ChildExecutor, ConditionBackend, ConditionDeclaration, InlineExecutor,
    JsonState, LifecycleEvent, LifecycleHooks, NoActions, NoConditions, NoHooks, StateTyping,
    WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown};
use futures_util::stream::{self, Iter};
use serde_json::{Map, json};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::StateListRequest;
use storage_kit::{StateStore, StateStoreBackend};

// None of the caller behavior owners is Clone. Configuration replacement and App
// opening must preserve the exact implementation types without normalization.
struct CallerAction {
    declaration: ActionDeclaration,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    panic_on_declaration: bool,
}

struct CallerCondition {
    declaration: ConditionDeclaration,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    panic_on_declaration: bool,
}

struct CallerTyping {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

struct CallerHooks {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

struct CallerExecutor {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

struct Bindings;

impl Bindings {
    async fn config() -> AppConfig<StateStore, Agent> {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real memory transactions");
        AppConfig::new(store, Vec::new(), Vec::new())
    }

    async fn close<
        X: ActionBackend,
        C: ConditionBackend,
        T: StateTyping,
        H: LifecycleHooks,
        E: ChildExecutor,
    >(
        app: &mut App<StateStore, Agent, X, C, T, H, E>,
    ) {
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(3),
        })
        .await
        .expect("unused bindings introduce no execution or cleanup work");
    }

    fn shared<T: Send + Sync>(_value: &T) {}

    async fn check(action: CallerAction, condition: CallerCondition, accepted: bool, case: &str) {
        let result = App::open(
            Self::config()
                .await
                .with_actions(vec![action])
                .with_conditions(vec![condition]),
        )
        .await;
        match result {
            Ok(mut app) => {
                Self::close(&mut app).await;
                assert!(accepted, "oversized declaration was accepted: {case}");
            }
            Err(error) => assert!(
                !accepted && matches!(error, AppError::InvalidInput(_)),
                "declaration {case}: {error}"
            ),
        }
    }
}

impl CallerAction {
    fn new(key: &str, revision: &str, calls: &Arc<AtomicUsize>) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: key.into(),
                revision: revision.into(),
                reads: Vec::new(),
                writes: Vec::new(),
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: ActionMode::Both,
            },
            calls: Arc::clone(calls),
            drops: Arc::new(AtomicUsize::new(0)),
            panic_on_declaration: false,
        }
    }
}

impl ActionBackend for CallerAction {
    type Stream = Iter<std::vec::IntoIter<Result<ActionEvent, AppError>>>;

    fn declaration(&self) -> &ActionDeclaration {
        assert!(
            !self.panic_on_declaration,
            "caller action declaration panic"
        );
        &self.declaration
    }

    fn start(&self, _request: ActionRequest) -> Self::Stream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        stream::iter(vec![Ok(ActionEvent::Completed {
            result: Map::new(),
            writes: Vec::new(),
        })])
    }
}

impl Drop for CallerAction {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl CallerCondition {
    fn new(key: &str, revision: &str, calls: &Arc<AtomicUsize>) -> Self {
        Self {
            declaration: ConditionDeclaration {
                key: key.into(),
                revision: revision.into(),
                reads: Vec::new(),
            },
            calls: Arc::clone(calls),
            drops: Arc::new(AtomicUsize::new(0)),
            panic_on_declaration: false,
        }
    }
}

impl ConditionBackend for CallerCondition {
    fn declaration(&self) -> &ConditionDeclaration {
        assert!(
            !self.panic_on_declaration,
            "caller condition declaration panic"
        );
        &self.declaration
    }

    fn evaluate(&self, _state: &WorkflowState) -> Result<bool, AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }
}

impl Drop for CallerCondition {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl CallerTyping {
    fn new(calls: &Arc<AtomicUsize>) -> Self {
        Self {
            calls: Arc::clone(calls),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl StateTyping for CallerTyping {
    type Data = Rc<WorkflowState>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Rc::new(state.clone()))
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(data.as_ref().clone())
    }
}

impl Drop for CallerTyping {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl CallerHooks {
    fn new(calls: &Arc<AtomicUsize>) -> Self {
        Self {
            calls: Arc::clone(calls),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl LifecycleHooks for CallerHooks {
    fn observe(&self, _event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn observe_async<'a>(&'a self, _event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for CallerHooks {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl CallerExecutor {
    fn new(calls: &Arc<AtomicUsize>) -> Self {
        Self {
            calls: Arc::clone(calls),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ChildExecutor for CallerExecutor {
    type Execution<'a, F>
        = ChildBatch<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        batch
    }
}

impl Drop for CallerExecutor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn consuming_configuration_replaces_each_exact_caller_inventory() {
    let action_calls = Arc::new(AtomicUsize::new(0));
    let condition_calls = Arc::new(AtomicUsize::new(0));
    let discarded_action = CallerAction::new("invalid key", "", &action_calls);
    let discarded_action_drops = Arc::clone(&discarded_action.drops);
    let discarded_condition = CallerCondition::new("invalid key", "", &condition_calls);
    let discarded_condition_drops = Arc::clone(&discarded_condition.drops);
    let action = CallerAction::new("action", "r1", &action_calls);
    let action_drops = Arc::clone(&action.drops);
    let condition = CallerCondition::new("predicate", "r1", &condition_calls);
    let condition_drops = Arc::clone(&condition.drops);
    let config = Bindings::config()
        .await
        .with_actions(vec![discarded_action])
        .with_conditions(vec![discarded_condition]);
    assert_eq!(discarded_action_drops.load(Ordering::SeqCst), 0);
    assert_eq!(discarded_condition_drops.load(Ordering::SeqCst), 0);
    let config = config.with_actions(vec![action]);
    assert_eq!(discarded_action_drops.load(Ordering::SeqCst), 1);
    assert_eq!(discarded_condition_drops.load(Ordering::SeqCst), 0);
    let config = config.with_conditions(vec![condition]);
    assert_eq!(discarded_condition_drops.load(Ordering::SeqCst), 1);
    assert_eq!(action_drops.load(Ordering::SeqCst), 0);
    assert_eq!(condition_drops.load(Ordering::SeqCst), 0);
    let mut app: App<StateStore, Agent, CallerAction, CallerCondition> = App::open(config)
        .await
        .expect("only the replacement inventories are transferred to App");
    assert_eq!(action_calls.load(Ordering::SeqCst), 0);
    assert_eq!(condition_calls.load(Ordering::SeqCst), 0);
    assert_eq!(action_drops.load(Ordering::SeqCst), 0);
    assert_eq!(condition_drops.load(Ordering::SeqCst), 0);
    Bindings::close(&mut app).await;
    drop(app);
    assert_eq!(action_drops.load(Ordering::SeqCst), 1);
    assert_eq!(condition_drops.load(Ordering::SeqCst), 1);
    assert_eq!(discarded_action_drops.load(Ordering::SeqCst), 1);
    assert_eq!(discarded_condition_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn consuming_configuration_preserves_all_seven_exact_caller_types() {
    let calls = Arc::new(AtomicUsize::new(0));
    let previous_typing = CallerTyping::new(&calls);
    let previous_hooks = CallerHooks::new(&calls);
    let previous_executor = CallerExecutor::new(&calls);
    let previous = [
        Arc::clone(&previous_typing.drops),
        Arc::clone(&previous_hooks.drops),
        Arc::clone(&previous_executor.drops),
    ];
    let typing = CallerTyping::new(&calls);
    let hooks = CallerHooks::new(&calls);
    let executor = CallerExecutor::new(&calls);
    let retained = [
        Arc::clone(&typing.drops),
        Arc::clone(&hooks.drops),
        Arc::clone(&executor.drops),
    ];
    let action = CallerAction::new("action", "r1", &calls);
    let condition = CallerCondition::new("predicate", "r1", &calls);
    let action_drops = Arc::clone(&action.drops);
    let condition_drops = Arc::clone(&condition.drops);
    let config = Bindings::config()
        .await
        .with_state_typing(previous_typing)
        .with_hooks(previous_hooks)
        .with_child_executor(previous_executor)
        .with_actions(vec![action])
        .with_conditions(vec![condition]);
    assert!(
        previous
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 0)
    );
    let config = config.with_state_typing(typing);
    assert_eq!(previous[0].load(Ordering::SeqCst), 1);
    assert_eq!(previous[1].load(Ordering::SeqCst), 0);
    assert_eq!(previous[2].load(Ordering::SeqCst), 0);
    let config = config.with_hooks(hooks);
    assert_eq!(previous[1].load(Ordering::SeqCst), 1);
    assert_eq!(previous[2].load(Ordering::SeqCst), 0);
    let config = config.with_child_executor(executor);
    assert!(
        previous
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 1)
    );
    assert!(
        retained
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 0)
    );
    assert_eq!(action_drops.load(Ordering::SeqCst), 0);
    assert_eq!(condition_drops.load(Ordering::SeqCst), 0);

    let mut app: App<
        StateStore,
        Agent,
        CallerAction,
        CallerCondition,
        CallerTyping,
        CallerHooks,
        CallerExecutor,
    > = App::open(config).await.expect("all owners remain concrete");
    Bindings::shared(&app);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "unused typing, hooks and child execution do not run during configuration"
    );
    assert!(
        retained
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 0)
    );
    Bindings::close(&mut app).await;
    drop(app);
    assert!(
        retained
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 1)
    );
    assert!(
        previous
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 1)
    );
    assert_eq!(action_drops.load(Ordering::SeqCst), 1);
    assert_eq!(condition_drops.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn replacing_custom_typing_hooks_and_executor_restores_exact_default_types() {
    let calls = Arc::new(AtomicUsize::new(0));
    let typing = CallerTyping::new(&calls);
    let hooks = CallerHooks::new(&calls);
    let executor = CallerExecutor::new(&calls);
    let drops = [
        Arc::clone(&typing.drops),
        Arc::clone(&hooks.drops),
        Arc::clone(&executor.drops),
    ];
    let config: AppConfig<StateStore, Agent> = Bindings::config()
        .await
        .with_state_typing(typing)
        .with_hooks(hooks)
        .with_child_executor(executor)
        .with_state_typing(JsonState)
        .with_hooks(NoHooks)
        .with_child_executor(InlineExecutor);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 1));
    let mut app: App<StateStore, Agent> = App::open(config)
        .await
        .expect("only App-owned defaults restore the two-required-parameter configuration");
    Bindings::close(&mut app).await;
    drop(app);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 1));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_unpolled_open_releases_all_configured_behavior_values_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let typing = CallerTyping::new(&calls);
    let hooks = CallerHooks::new(&calls);
    let executor = CallerExecutor::new(&calls);
    let drops = [
        Arc::clone(&typing.drops),
        Arc::clone(&hooks.drops),
        Arc::clone(&executor.drops),
    ];
    let config = Bindings::config()
        .await
        .with_state_typing(typing)
        .with_hooks(hooks)
        .with_child_executor(executor);
    let opening = App::open(config);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 0));
    drop(opening);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 1));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_or_panicking_declarations_drop_every_transferred_value_before_storage_effects() {
    for failure in [
        "invalid_action",
        "invalid_condition",
        "panic_action",
        "panic_condition",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut actions = ["a", "b", "c"].map(|key| CallerAction::new(key, "r1", &calls));
        let mut conditions = ["a", "b", "c"].map(|key| CallerCondition::new(key, "r1", &calls));
        let typing = CallerTyping::new(&calls);
        let hooks = CallerHooks::new(&calls);
        let executor = CallerExecutor::new(&calls);
        let drops = actions
            .iter()
            .map(|action| Arc::clone(&action.drops))
            .chain(
                conditions
                    .iter()
                    .map(|condition| Arc::clone(&condition.drops)),
            )
            .chain([
                Arc::clone(&typing.drops),
                Arc::clone(&hooks.drops),
                Arc::clone(&executor.drops),
            ])
            .collect::<Vec<_>>();
        match failure {
            "invalid_action" => actions[1].declaration.revision.clear(),
            "invalid_condition" => conditions[1].declaration.revision.clear(),
            "panic_action" => actions[1].panic_on_declaration = true,
            "panic_condition" => conditions[1].panic_on_declaration = true,
            _ => unreachable!("closed preflight failure cases"),
        }
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real memory transactions");
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store.clone(), Vec::new(), Vec::new());
        let result = App::open(
            config
                .with_actions(Vec::from(actions))
                .with_conditions(Vec::from(conditions))
                .with_state_typing(typing)
                .with_hooks(hooks)
                .with_child_executor(executor),
        )
        .await;
        if failure.starts_with("panic_") {
            assert!(
                matches!(result, Err(AppError::Integration(_))),
                "{failure} must become an owned integration failure"
            );
        } else {
            assert!(
                matches!(result, Err(AppError::InvalidInput(_))),
                "{failure} must fail validation"
            );
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "preflight cannot execute behavior"
        );
        assert!(
            drops.iter().all(|count| count.load(Ordering::SeqCst) == 1),
            "{failure} must drop each transferred value exactly once, including unvisited entries"
        );
        let remaining = store
            .list(StateListRequest::new("app-kit").expect("App state namespace"))
            .await
            .expect("committed state remains observable");
        assert!(
            remaining.records.is_empty() && remaining.next_start_after.is_none(),
            "{failure} must not publish App records before declaration preflight"
        );
    }
}

#[tokio::test]
async fn empty_uninhabited_inventories_restore_the_default_config_types() {
    let calls = Arc::new(AtomicUsize::new(0));
    let config: AppConfig<StateStore, Agent> = Bindings::config()
        .await
        .with_actions(vec![CallerAction::new("invalid key", "", &calls)])
        .with_conditions(vec![CallerCondition::new("invalid key", "", &calls)])
        .with_actions(Vec::<NoActions>::new())
        .with_conditions(Vec::<NoConditions>::new());
    let mut app: App<StateStore, Agent> = App::open(config)
        .await
        .expect("empty inventories need no placeholder behavior value");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Bindings::close(&mut app).await;
}

#[tokio::test]
async fn binding_metadata_accepts_pinned_data_keys_duplicates_and_input_overlap() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut action = CallerAction::new("action", "r1", &calls);
    let data_keys = ["", "two words", "雪", "line\nname", "a/b", "_private"]
        .map(str::to_owned)
        .to_vec();
    action.declaration.reads = data_keys.clone();
    action.declaration.reads.extend(data_keys.clone());
    action.declaration.writes = data_keys.clone();
    action.declaration.required_inputs = data_keys.clone();
    action.declaration.required_inputs.extend(data_keys.clone());
    action.declaration.optional_inputs = data_keys.clone();
    action
        .declaration
        .optional_inputs
        .extend(["optional".into(), "optional".into()]);
    action.declaration.bound_inputs = Map::from_iter(
        data_keys
            .iter()
            .map(|key| (key.clone(), json!(null)))
            .chain([
                (
                    "undeclared".into(),
                    json!("bound values need no input declaration"),
                ),
                (
                    "optional".into(),
                    json!("bound wins over the optional default"),
                ),
            ]),
    );
    action
        .declaration
        .default_inputs
        .insert("optional".into(), json!("default"));
    action.declaration.tags = vec!["a tag".into(), "a tag".into()];
    action.declaration.source = Some("caller source\nwith formatting".into());
    let mut condition = CallerCondition::new("predicate", "r1", &calls);
    condition.declaration.reads = data_keys.clone();
    condition.declaration.reads.extend(data_keys);
    let mut app = App::open(
        Bindings::config()
            .await
            .with_actions(vec![action])
            .with_conditions(vec![condition]),
    )
    .await
    .expect("input/read names are data, and duplicate declarations are not invalid bindings");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Bindings::close(&mut app).await;
}

#[tokio::test]
async fn distinct_revisions_coexist_but_duplicate_exact_condition_bindings_fail() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = App::open(
        Bindings::config()
            .await
            .with_actions(vec![
                CallerAction::new("action", "r2", &calls),
                CallerAction::new("action", "r1", &calls),
            ])
            .with_conditions(vec![
                CallerCondition::new("predicate", "r2", &calls),
                CallerCondition::new("predicate", "r1", &calls),
            ]),
    )
    .await
    .expect("opaque distinct revisions may coexist in caller order");
    Bindings::close(&mut app).await;

    let conditions = App::open(Bindings::config().await.with_conditions(vec![
        CallerCondition::new("predicate", "r1", &calls),
        CallerCondition::new("predicate", "r1", &calls),
    ]))
    .await;
    assert!(matches!(conditions, Err(AppError::InvalidInput(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_condition_identity_or_revision_fails_before_predicate_evaluation() {
    let calls = Arc::new(AtomicUsize::new(0));
    for (key, revision) in [
        ("", "r1"),
        ("Not Canonical", "r1"),
        ("predicate", ""),
        ("predicate", " r1"),
        ("predicate", "r1\n"),
    ] {
        let result = App::open(
            Bindings::config()
                .await
                .with_conditions(vec![CallerCondition::new(key, revision, &calls)]),
        )
        .await;
        assert!(
            matches!(result, Err(AppError::InvalidInput(_))),
            "{key:?}/{revision:?}"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn action_declaration_errors_are_rejected_before_any_action_runs() {
    for invalid in [
        "reserved_required",
        "reserved_optional",
        "reserved_bound",
        "reserved_default",
        "required_default",
        "undeclared_default",
        "bad_key",
        "empty_revision",
        "duplicate_binding",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut action = CallerAction::new("first", "action-r1", &calls);
        action.declaration.required_inputs = vec!["question".into()];
        action.declaration.optional_inputs = vec!["format".into(), "bound".into()];
        action
            .declaration
            .bound_inputs
            .insert("bound".into(), json!("fixed"));
        action
            .declaration
            .default_inputs
            .insert("format".into(), json!("plain"));
        match invalid {
            "reserved_required" => action.declaration.required_inputs.push("__context".into()),
            "reserved_optional" => action.declaration.optional_inputs.push("__context".into()),
            "reserved_bound" => {
                action
                    .declaration
                    .bound_inputs
                    .insert("__context".into(), json!(1));
            }
            "reserved_default" => {
                action
                    .declaration
                    .default_inputs
                    .insert("__context".into(), json!(1));
            }
            "required_default" => {
                action
                    .declaration
                    .default_inputs
                    .insert("question".into(), json!(1));
            }
            "undeclared_default" => {
                action
                    .declaration
                    .default_inputs
                    .insert("unknown".into(), json!(1));
            }
            "bad_key" => action.declaration.key = "Not Canonical".into(),
            "empty_revision" => action.declaration.revision.clear(),
            "duplicate_binding" => {}
            _ => unreachable!("closed test case list"),
        }
        let mut actions = vec![action];
        if invalid == "duplicate_binding" {
            actions.push(CallerAction::new("first", "action-r1", &calls));
        }
        let result = App::open(Bindings::config().await.with_actions(actions)).await;
        assert!(
            matches!(result, Err(AppError::InvalidInput(_))),
            "invalid declaration {invalid} must fail preflight"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no source was constructed or polled"
        );
    }
}

#[tokio::test]
async fn action_and_condition_inventories_reuse_the_4096_entry_bound() {
    let calls = Arc::new(AtomicUsize::new(0));
    for (count, accepted) in [(4095, true), (4096, true), (4097, false)] {
        for action_inventory in [true, false] {
            let actions = if action_inventory {
                (0..count)
                    .map(|index| CallerAction::new(&format!("action{index}"), "r1", &calls))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let conditions = if action_inventory {
                Vec::new()
            } else {
                (0..count)
                    .map(|index| CallerCondition::new(&format!("predicate{index}"), "r1", &calls))
                    .collect::<Vec<_>>()
            };
            let result = App::open(
                Bindings::config()
                    .await
                    .with_actions(actions)
                    .with_conditions(conditions),
            )
            .await;
            match result {
                Ok(mut app) => {
                    Bindings::close(&mut app).await;
                    assert!(accepted, "inventory of {count} values must reject");
                }
                Err(error) => assert!(
                    !accepted && matches!(error, AppError::InvalidInput(_)),
                    "action_inventory={action_inventory}, {count} values: {error}"
                ),
            }
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn action_and_condition_binding_identities_reuse_existing_key_and_revision_bounds() {
    let calls = Arc::new(AtomicUsize::new(0));
    for (length, revision, accepted) in [
        (128, false, true),
        (129, false, false),
        (256, true, true),
        (257, true, false),
    ] {
        for predicate in [false, true] {
            let mut action = CallerAction::new("action", "r1", &calls);
            let mut condition = CallerCondition::new("predicate", "r1", &calls);
            match (predicate, revision) {
                (false, false) => action.declaration.key = "a".repeat(length),
                (false, true) => action.declaration.revision = "r".repeat(length),
                (true, false) => condition.declaration.key = "a".repeat(length),
                (true, true) => condition.declaration.revision = "r".repeat(length),
            }
            Bindings::check(
                action,
                condition,
                accepted,
                &format!("predicate={predicate}, revision={revision}, {length} bytes"),
            )
            .await;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn declaration_data_keys_use_the_existing_128_decoded_utf8_byte_bound() {
    let calls = Arc::new(AtomicUsize::new(0));
    for (key, accepted) in [
        ("a".repeat(128), true),
        ("a".repeat(129), false),
        ("é".repeat(64), true),
        (format!("{}a", "é".repeat(64)), false),
        ("\n".repeat(128), true),
        ("\n".repeat(129), false),
    ] {
        for field in [
            "reads",
            "writes",
            "required",
            "optional",
            "bound",
            "default",
            "condition",
        ] {
            let mut action = CallerAction::new("action", "r1", &calls);
            let mut condition = CallerCondition::new("predicate", "r1", &calls);
            match field {
                "reads" => action.declaration.reads.push(key.clone()),
                "writes" => action.declaration.writes.push(key.clone()),
                "required" => action.declaration.required_inputs.push(key.clone()),
                "optional" => action.declaration.optional_inputs.push(key.clone()),
                "bound" => {
                    action
                        .declaration
                        .bound_inputs
                        .insert(key.clone(), json!(null));
                }
                "default" => {
                    action.declaration.optional_inputs.push(key.clone());
                    action
                        .declaration
                        .default_inputs
                        .insert(key.clone(), json!(null));
                }
                "condition" => condition.declaration.reads.push(key.clone()),
                _ => unreachable!("closed metadata fields"),
            }
            Bindings::check(
                action,
                condition,
                accepted,
                &format!("{field}: {} decoded bytes", key.len()),
            )
            .await;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn declaration_collections_reuse_the_4096_entry_bound_without_deduplicating_lists() {
    let calls = Arc::new(AtomicUsize::new(0));
    for (count, accepted) in [(4096, true), (4097, false)] {
        for field in [
            "reads",
            "writes",
            "required",
            "optional",
            "bound",
            "default",
            "tags",
            "condition",
        ] {
            let mut action = CallerAction::new("action", "r1", &calls);
            let mut condition = CallerCondition::new("predicate", "r1", &calls);
            match field {
                "reads" => action.declaration.reads = vec!["key".into(); count],
                "writes" => action.declaration.writes = vec!["key".into(); count],
                "required" => action.declaration.required_inputs = vec!["key".into(); count],
                "optional" => action.declaration.optional_inputs = vec!["key".into(); count],
                "bound" => {
                    action.declaration.bound_inputs = (0..count)
                        .map(|index| (format!("key{index}"), json!(null)))
                        .collect();
                }
                "default" => {
                    action.declaration.optional_inputs =
                        (0..count).map(|index| format!("key{index}")).collect();
                    action.declaration.default_inputs = action
                        .declaration
                        .optional_inputs
                        .iter()
                        .map(|key| (key.clone(), json!(null)))
                        .collect();
                }
                "tags" => action.declaration.tags = vec!["tag".into(); count],
                "condition" => condition.declaration.reads = vec!["key".into(); count],
                _ => unreachable!("closed metadata fields"),
            }
            Bindings::check(
                action,
                condition,
                accepted,
                &format!("{field}: {count} entries"),
            )
            .await;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn action_tag_and_source_text_reuse_the_65536_utf8_byte_bound() {
    let calls = Arc::new(AtomicUsize::new(0));
    for (text, accepted) in [
        ("x".repeat(65_536), true),
        ("x".repeat(65_537), false),
        ("雪".repeat(21_845) + "x", true),
        ("雪".repeat(21_845) + "xx", false),
        ("\n".repeat(65_536), true),
    ] {
        for field in ["tag", "source"] {
            let mut action = CallerAction::new("action", "r1", &calls);
            if field == "tag" {
                action.declaration.tags.push(text.clone());
            } else {
                action.declaration.source = Some(text.clone());
            }
            Bindings::check(
                action,
                CallerCondition::new("predicate", "r1", &calls),
                accepted,
                &format!("{field}: {} decoded bytes", text.len()),
            )
            .await;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn declaration_aggregate_json_bytes_are_bounded_even_when_every_field_fits() {
    let calls = Arc::new(AtomicUsize::new(0));
    // Each field fits its own limit. The aggregate includes JSON escaping and
    // structural bytes. Leave 256 bytes for structural overhead in the near-limit
    // accepted case without freezing the private declaration encoding shape.
    for (last_tag_bytes, accepted) in [(65_536 - 256, true), (65_536, false)] {
        let mut action = CallerAction::new("action", "r1", &calls);
        action.declaration.tags = vec!["x".repeat(65_536); 15];
        action.declaration.tags.push("x".repeat(last_tag_bytes));
        Bindings::check(
            action,
            CallerCondition::new("predicate", "r1", &calls),
            accepted,
            &format!("ASCII tag content: {} bytes", 15 * 65_536 + last_tag_bytes),
        )
        .await;
    }
    // Escaping can exceed the same 1 MiB budget despite smaller decoded text.
    for (tag_count, accepted) in [(2, true), (3, false)] {
        let mut action = CallerAction::new("action", "r1", &calls);
        action.declaration.tags = vec!["\0".repeat(65_536); tag_count];
        Bindings::check(
            action,
            CallerCondition::new("predicate", "r1", &calls),
            accepted,
            &format!("{tag_count} escaped tags"),
        )
        .await;
    }
    // Repeated 128-byte data keys stay valid individually and below the list
    // bound; their JSON escape expansion crosses 1 MiB between these counts.
    for (read_count, accepted) in [(1359, true), (1361, false)] {
        let mut condition = CallerCondition::new("predicate", "r1", &calls);
        condition.declaration.reads = vec!["\0".repeat(128); read_count];
        Bindings::check(
            CallerAction::new("action", "r1", &calls),
            condition,
            accepted,
            &format!("{read_count} escaped condition reads"),
        )
        .await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
