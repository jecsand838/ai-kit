use std::rc::Rc;
use std::sync::Mutex;

use app_kit::AppError;
use app_kit::workflow::{JsonState, StateChange, StateTyping, WorkflowState};
use serde_json::{Map, Value, json};

// Neither the typing owner nor Data implements Clone. Rc also deliberately
// makes Data non-Send/non-Sync, and Data has no serialization implementation.
struct CallerData {
    state: Rc<WorkflowState>,
}

struct CallerTyping {
    calls: Mutex<Vec<Phase>>,
    reject: Option<Phase>,
    reconstruct_differently: bool,
    expected_after: Option<WorkflowState>,
    expected_remove: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Pre,
    Post,
    State,
    ConstructData,
    ConstructState,
}

impl CallerTyping {
    fn new(reject: Option<Phase>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            reject,
            reconstruct_differently: false,
            expected_after: None,
            expected_remove: None,
        }
    }

    fn observe(&self, phase: Phase) -> Result<(), AppError> {
        self.calls.lock().expect("typing calls").push(phase);
        if self.reject == Some(phase) {
            Err(AppError::InvalidInput(format!("caller rejected {phase:?}")))
        } else {
            Ok(())
        }
    }

    fn calls(&self) -> Vec<Phase> {
        self.calls.lock().expect("typing calls").clone()
    }

    fn initial() -> WorkflowState {
        WorkflowState::new(Map::from_iter([
            ("count".to_owned(), json!(1)),
            ("items".to_owned(), json!([1])),
            ("optional".to_owned(), json!(true)),
        ]))
        .expect("initial state")
    }
}

impl StateTyping for CallerTyping {
    type Data = CallerData;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        self.observe(Phase::ConstructData)?;
        Ok(CallerData {
            state: Rc::new(state.clone()),
        })
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        self.observe(Phase::ConstructState)?;
        if self.reconstruct_differently {
            WorkflowState::new(Map::from_iter([("count".to_owned(), json!(99))]))
        } else {
            Ok(data.state.as_ref().clone())
        }
    }

    fn validate_pre(&self, state: &WorkflowState, change: &StateChange) -> Result<(), AppError> {
        assert_eq!(state.values().get("count"), Some(&json!(1)));
        if let Some(expected) = &self.expected_remove {
            let StateChange::Remove { keys } = change else {
                panic!("wipe callbacks must receive resolved removal");
            };
            assert_eq!(keys, expected);
        }
        self.observe(Phase::Pre)
    }

    fn validate_post(
        &self,
        before: &WorkflowState,
        after: &WorkflowState,
        change: &StateChange,
    ) -> Result<(), AppError> {
        assert_eq!(before.values().get("count"), Some(&json!(1)));
        if matches!(change, StateChange::Increment { values } if !values.is_empty()) {
            assert_eq!(after.values().get("count"), Some(&json!(2)));
        }
        if let Some(expected) = &self.expected_after {
            assert_eq!(
                after, expected,
                "post validation must see the complete atomic payload"
            );
        }
        if let Some(expected) = &self.expected_remove {
            let StateChange::Remove { keys } = change else {
                panic!("wipe post callback must receive resolved removal");
            };
            assert_eq!(keys, expected);
        }
        self.observe(Phase::Post)
    }

    fn validate_state(&self, state: &WorkflowState) -> Result<(), AppError> {
        self.observe(Phase::State)?;
        if state.values().contains_key("count") {
            Ok(())
        } else {
            Err(AppError::InvalidInput("count is required".to_owned()))
        }
    }

    // Exercise the trait's default validate_roundtrip implementation.
}

struct DefaultValidation;

impl StateTyping for DefaultValidation {
    type Data = CallerData;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        Ok(CallerData {
            state: Rc::new(state.clone()),
        })
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        Ok(data.state.as_ref().clone())
    }
}

struct SkipRoundtrip;

impl StateTyping for SkipRoundtrip {
    type Data = CallerData;

    fn construct_data(&self, _: &WorkflowState) -> Result<Self::Data, AppError> {
        panic!("overridden roundtrip must skip construct_data")
    }

    fn construct_state(&self, _: &Self::Data) -> Result<WorkflowState, AppError> {
        panic!("overridden roundtrip must skip construct_state")
    }

    fn validate_roundtrip(&self, _: &WorkflowState) -> Result<(), AppError> {
        Ok(())
    }
}

#[test]
fn primitive_operations_run_ordered_typing_and_accept_non_send_data() {
    let base = CallerTyping::initial();
    let retained = base.clone();
    let original_values = base.values().clone();
    let changes = [
        StateChange::Assign {
            values: Map::from_iter([("optional".to_owned(), json!(false))]),
        },
        StateChange::Append {
            values: Vec::from([("items".to_owned(), json!(2))]),
        },
        StateChange::Extend {
            values: vec![("items".to_owned(), vec![json!(2), json!(3)])],
        },
        StateChange::Increment {
            values: Vec::from([("count".to_owned(), json!(1))]),
        },
        StateChange::Remove {
            keys: vec!["optional".to_owned()],
        },
        StateChange::Wipe {
            keep: vec!["count".to_owned()],
        },
    ];
    for change in changes {
        let typing = CallerTyping::new(None);
        let candidate = base.apply(&change, &typing).expect("typed operation");
        assert_eq!(
            typing.calls(),
            [
                Phase::Pre,
                Phase::Post,
                Phase::State,
                Phase::ConstructData,
                Phase::ConstructState
            ]
        );
        assert_eq!(
            base.values(),
            &original_values,
            "operation mutated its borrowed input"
        );
        assert_eq!(
            retained.values(),
            &original_values,
            "operation mutated a retained clone"
        );
        assert_ne!(
            candidate, base,
            "fixture operation must actually change state"
        );
    }
}

#[test]
fn merge_and_subset_validate_snapshots_without_primitive_operation_callbacks() {
    // Pinned state.rs:286–315 uses validated_with_values ->
    // new_with_typing_system. It does not call apply_operation's pre/post lane.
    for reject in [None, Some(Phase::Pre), Some(Phase::Post)] {
        for change in [
            StateChange::Merge {
                state: WorkflowState::new(Map::from_iter([("optional".into(), json!(false))]))
                    .expect("incoming state"),
            },
            StateChange::Subset {
                keys: vec!["count".into()],
            },
        ] {
            let base = CallerTyping::initial();
            let retained = base.clone();
            let typing = CallerTyping::new(reject);
            let candidate = base
                .apply(&change, &typing)
                .expect("snapshot operations must not invoke primitive pre/post callbacks");
            assert_eq!(
                typing.calls(),
                [Phase::State, Phase::ConstructData, Phase::ConstructState]
            );
            assert_ne!(candidate, base);
            assert_eq!(base, retained);
            assert_eq!(base, CallerTyping::initial());
        }
    }
}

#[test]
fn snapshot_validation_failure_stops_later_phases_without_mutating_the_base() {
    let phases = [Phase::State, Phase::ConstructData, Phase::ConstructState];
    for (index, phase) in phases.into_iter().enumerate() {
        for change in [
            StateChange::Merge {
                state: WorkflowState::new(Map::from_iter([("optional".into(), json!(false))]))
                    .expect("incoming state"),
            },
            StateChange::Subset {
                keys: vec!["count".into()],
            },
        ] {
            let base = CallerTyping::initial();
            let typing = CallerTyping::new(Some(phase));
            let error = base
                .apply(&change, &typing)
                .expect_err("snapshot validation must fail");
            assert!(
                error
                    .to_string()
                    .contains(&format!("caller rejected {phase:?}"))
            );
            assert_eq!(typing.calls(), phases[..=index]);
            assert_eq!(base, CallerTyping::initial());
        }
    }
}

#[test]
fn each_validation_failure_stops_later_phases_and_preserves_original_state() {
    let phases = [
        Phase::Pre,
        Phase::Post,
        Phase::State,
        Phase::ConstructData,
        Phase::ConstructState,
    ];
    for (index, phase) in phases.iter().copied().enumerate() {
        let base = CallerTyping::initial();
        let original_values = base.values().clone();
        let typing = CallerTyping::new(Some(phase));
        let error = base
            .apply(
                &StateChange::Increment {
                    values: Vec::from([("count".to_owned(), json!(1))]),
                },
                &typing,
            )
            .expect_err("caller validation must reject the candidate");
        assert!(
            error
                .to_string()
                .contains(&format!("caller rejected {phase:?}"))
        );
        assert_eq!(typing.calls(), phases[..=index]);
        assert_eq!(base.values(), &original_values);
    }
}

#[test]
fn mutation_error_does_not_call_post_or_roundtrip_and_keeps_input() {
    let base = CallerTyping::initial();
    let typing = CallerTyping::new(None);
    let error = base.apply(
        &StateChange::Append {
            values: Vec::from([("count".to_owned(), json!(1))]),
        },
        &typing,
    );
    assert!(error.is_err());
    assert_eq!(typing.calls(), [Phase::Pre]);
    assert_eq!(base, CallerTyping::initial());
}

#[test]
fn full_state_validation_remains_required_for_operations_and_snapshots() {
    for change in [
        StateChange::Remove {
            keys: vec!["count".to_owned()],
        },
        StateChange::Wipe {
            keep: vec!["items".to_owned()],
        },
        StateChange::Subset {
            keys: vec!["items".to_owned()],
        },
    ] {
        let base = CallerTyping::initial();
        let typing = CallerTyping::new(None);
        let error = base
            .apply(&change, &typing)
            .expect_err("required count must remain present");
        assert!(error.to_string().contains("count is required"));
        if matches!(change, StateChange::Subset { .. }) {
            assert_eq!(typing.calls(), [Phase::State]);
        } else {
            assert_eq!(typing.calls(), [Phase::Pre, Phase::Post, Phase::State]);
        }
        assert_eq!(base, CallerTyping::initial());
    }
}

#[test]
fn default_roundtrip_requires_successful_reconstruction_without_raw_equality() {
    let base = CallerTyping::initial();
    let mut typing = CallerTyping::new(None);
    typing.reconstruct_differently = true;
    let candidate = base
        .apply(
            &StateChange::Increment {
                values: Vec::from([("count".to_owned(), json!(1))]),
            },
            &typing,
        )
        .expect("successful reconstruction need not equal raw state");
    assert_eq!(candidate.values().get("count"), Some(&Value::from(2)));
    assert_eq!(
        typing.calls(),
        [
            Phase::Pre,
            Phase::Post,
            Phase::State,
            Phase::ConstructData,
            Phase::ConstructState
        ]
    );
    assert_eq!(base, CallerTyping::initial());
}

#[test]
fn default_validation_hooks_and_roundtrip_override_remain_optional() {
    let base = CallerTyping::initial();
    let change = StateChange::Increment {
        values: Vec::from([("count".to_owned(), json!(1))]),
    };
    let default_validated = base
        .apply(&change, &DefaultValidation)
        .expect("default success validation hooks");
    let overridden = base
        .apply(&change, &SkipRoundtrip)
        .expect("roundtrip override");
    let raw_typing: JsonState = Default::default();
    let raw_json = base.apply(&change, &raw_typing).expect("raw JSON typing");
    assert_eq!(default_validated, overridden);
    assert_eq!(default_validated, raw_json);
    assert!(WorkflowState::default().values().is_empty());
}

#[test]
fn duplicate_snapshot_selectors_preserve_pinned_order() {
    let base = WorkflowState::new(Map::from_iter([
        ("z".into(), json!(1)),
        ("a".into(), json!(2)),
        ("b".into(), json!(3)),
    ]))
    .expect("ordered base");
    let kept = base
        .apply(
            &StateChange::Wipe {
                keep: vec!["b".into(), "a".into(), "b".into()],
            },
            &JsonState,
        )
        .expect("duplicate keep names are idempotent");
    assert_eq!(
        kept.values().keys().map(String::as_str).collect::<Vec<_>>(),
        ["a", "b"]
    );
    let selected = base
        .apply(
            &StateChange::Subset {
                keys: vec!["b".into(), "z".into(), "b".into(), "a".into()],
            },
            &JsonState,
        )
        .expect("duplicate selection keeps its first requested position");
    assert_eq!(
        selected
            .values()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["b", "z", "a"]
    );
    assert_eq!(
        base.values().keys().map(String::as_str).collect::<Vec<_>>(),
        ["z", "a", "b"]
    );
}

#[test]
fn json_object_keys_are_data_even_when_empty_or_containing_control_characters() {
    let keys = ["Display Name", "λ", "a/b", "tilde~key", "", "line\nbreak"];
    let nested = Value::Object(keys.iter().map(|key| ((*key).into(), json!(1))).collect());
    let values = keys
        .iter()
        .map(|key| ((*key).into(), nested.clone()))
        .collect::<Map<_, _>>();
    let base = WorkflowState::new(values.clone()).expect("JSON data keys are not graph identities");
    let replaced = base
        .apply(
            &StateChange::Assign {
                values: Map::from_iter([("".into(), json!({"\n": true}))]),
            },
            &JsonState,
        )
        .expect("exact empty and control-containing keys survive mutation");
    assert_eq!(base.values(), &values);
    assert_eq!(replaced.values()[""], json!({"\n": true}));
    assert_eq!(
        replaced
            .values()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        keys
    );
}

#[test]
fn multi_key_payloads_validate_once_after_all_entries_and_preserve_order() {
    // Pinned state.rs:202–252 and state/operations.rs:9–112 apply one full
    // payload before transition validation. Repeated array/integer keys remain.
    let cases = [
        (
            StateChange::Assign {
                values: Map::from_iter([
                    ("count".into(), json!(2)),
                    ("new".into(), json!(7)),
                    ("optional".into(), json!(false)),
                ]),
            },
            json!({"count":2,"items":[1],"optional":false,"new":7}),
        ),
        (
            StateChange::Append {
                values: vec![
                    ("items".into(), json!(2)),
                    ("new".into(), json!([3])),
                    ("items".into(), json!(4)),
                ],
            },
            json!({"count":1,"items":[1,2,4],"optional":true,"new":[[3]]}),
        ),
        (
            StateChange::Extend {
                values: vec![
                    ("items".into(), vec![json!(2), json!(3)]),
                    ("new".into(), vec![]),
                    ("items".into(), vec![json!(4)]),
                ],
            },
            json!({"count":1,"items":[1,2,3,4],"optional":true,"new":[]}),
        ),
        (
            StateChange::Increment {
                values: vec![
                    ("count".into(), json!(2)),
                    ("new".into(), json!(2)),
                    ("count".into(), json!(-1)),
                    ("new".into(), json!(3)),
                ],
            },
            json!({"count":2,"items":[1],"optional":true,"new":5}),
        ),
        (
            StateChange::Remove {
                keys: vec![
                    "optional".into(),
                    "missing".into(),
                    "items".into(),
                    "optional".into(),
                ],
            },
            json!({"count":1}),
        ),
    ];
    for (change, expected) in cases {
        let base = CallerTyping::initial();
        let retained = base.clone();
        let expected = WorkflowState::new(expected.as_object().expect("case state").clone())
            .expect("expected state");
        let mut typing = CallerTyping::new(None);
        typing.expected_after = Some(expected.clone());
        let after = base
            .apply(&change, &typing)
            .expect("one whole operation succeeds");
        assert_eq!(after, expected);
        assert_eq!(
            after.values().keys().collect::<Vec<_>>(),
            expected.values().keys().collect::<Vec<_>>()
        );
        assert_eq!(
            typing.calls(),
            [
                Phase::Pre,
                Phase::Post,
                Phase::State,
                Phase::ConstructData,
                Phase::ConstructState
            ]
        );
        assert_eq!(base, retained);
        assert_eq!(base, CallerTyping::initial());
    }
}

#[test]
fn failure_after_an_earlier_payload_entry_discards_the_entire_candidate() {
    for change in [
        StateChange::Append {
            values: vec![("items".into(), json!(2)), ("count".into(), json!(3))],
        },
        StateChange::Extend {
            values: vec![
                ("items".into(), vec![json!(2)]),
                ("count".into(), vec![json!(3)]),
            ],
        },
        StateChange::Increment {
            values: vec![("count".into(), json!(1)), ("optional".into(), json!(1))],
        },
        StateChange::Increment {
            values: vec![("count".into(), json!(1)), ("new".into(), json!("invalid"))],
        },
        StateChange::Increment {
            values: vec![
                ("count".into(), json!(1)),
                ("new".into(), json!(i64::MAX)),
                ("new".into(), json!(i64::MAX)),
                ("new".into(), json!(2)),
            ],
        },
    ] {
        let base = CallerTyping::initial();
        let typing = CallerTyping::new(None);
        assert!(base.apply(&change, &typing).is_err());
        assert_eq!(typing.calls(), [Phase::Pre]);
        assert_eq!(base, CallerTyping::initial());
    }
}

#[test]
fn empty_payloads_still_invoke_the_complete_primitive_validation_sequence() {
    for change in [
        StateChange::Assign { values: Map::new() },
        StateChange::Append { values: vec![] },
        StateChange::Extend { values: vec![] },
        StateChange::Remove { keys: vec![] },
        StateChange::Increment { values: vec![] },
    ] {
        let base = CallerTyping::initial();
        let typing = CallerTyping::new(None);
        assert_eq!(base.apply(&change, &typing).expect("empty operation"), base);
        assert_eq!(
            typing.calls(),
            [
                Phase::Pre,
                Phase::Post,
                Phase::State,
                Phase::ConstructData,
                Phase::ConstructState
            ]
        );
    }
}

#[test]
fn wipe_validators_observe_the_resolved_removal_in_original_key_order() {
    let base = CallerTyping::initial();
    let mut typing = CallerTyping::new(None);
    typing.expected_remove = Some(vec!["items".into(), "optional".into()]);
    let after = base
        .apply(
            &StateChange::Wipe {
                keep: vec!["count".into()],
            },
            &typing,
        )
        .expect("normalized wipe");
    assert_eq!(
        after.values(),
        &Map::from_iter([("count".into(), json!(1))])
    );
    assert_eq!(
        typing.calls(),
        [
            Phase::Pre,
            Phase::Post,
            Phase::State,
            Phase::ConstructData,
            Phase::ConstructState
        ]
    );
}

#[test]
fn payload_and_selector_counts_observe_the_explicit_app_resource_limit() {
    // App's finite collection bound is an explicit resource policy, not an
    // assertion that the pinned rurr implementation imposes this limit.
    for count in [4_095, 4_096, 4_097] {
        for change in [
            StateChange::Append {
                values: vec![("new".into(), json!(1)); count],
            },
            StateChange::Extend {
                values: vec![("new".into(), vec![]); count],
            },
            StateChange::Increment {
                values: vec![("count".into(), json!(0)); count],
            },
            StateChange::Remove {
                keys: vec!["missing".into(); count],
            },
            StateChange::Wipe {
                keep: vec!["count".into(); count],
            },
            StateChange::Subset {
                keys: vec!["count".into(); count],
            },
        ] {
            let before = CallerTyping::initial();
            let result = before.apply(&change, &JsonState);
            if count <= 4_096 {
                assert!(result.is_ok(), "within payload bound: {result:?}");
            } else {
                assert!(
                    result
                        .expect_err("over-bound payload")
                        .to_string()
                        .contains("state operation exceeds its item bound")
                );
            }
            assert_eq!(before, CallerTyping::initial());
        }
    }
}
