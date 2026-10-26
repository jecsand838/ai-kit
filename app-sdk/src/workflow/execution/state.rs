use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use serde_json::{Map, Number, Value};

use crate::AppError;
use crate::workflow::definition::{CompiledDefinition, JsonBoundary, Reducer, StateMutation};
use crate::workflow::execution::{StagedActivationRecord, StagedOutcomeRecord, StateChangeRecord};
use crate::workflow::run::{FailureCategory, FrontierRecord};
use crate::workflow::{ConditionBackend, StateChange, StateTyping, WorkflowState};

// One activation owns intermediate state and insertion history. Native
// expressions and caller-resolved writes use the same accumulator; only their
// input resolution differs. Persisted records retain its final delta only.
struct ActivationState {
    state: WorkflowState,
    writes: BTreeSet<String>,
    retained_positions: BTreeSet<String>,
}

impl StagedActivationRecord {
    pub(super) fn mutate<T: StateTyping, C: ConditionBackend>(
        node: String,
        lineage: String,
        mutations: &[StateMutation],
        base: &Map<String, Value>,
        typing: &T,
        conditions: &[C],
    ) -> Self {
        let resolved = catch_unwind(AssertUnwindSafe(|| {
            let mut state = ActivationState::new(base)?;
            for mutation in mutations {
                // Expressions observe this action's preceding mutations. Siblings
                // independently begin at the same committed barrier state.
                let change = mutation.evaluate(&state.state, conditions)?;
                if state.apply(&change, typing).is_err() {
                    return Ok(StagedOutcomeRecord::Failed {
                        category: FailureCategory::State,
                        message: "workflow state operation failed".into(),
                    });
                }
            }
            Ok::<_, AppError>(StagedOutcomeRecord::Complete {
                changes: state.finish(),
                result: Map::new(),
            })
        }));
        let outcome = match resolved {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => StagedOutcomeRecord::Failed {
                category: FailureCategory::Evaluation,
                message: match error {
                    AppError::InvalidInput(message) => message,
                    _ => "workflow evaluation failed".into(),
                },
            },
            Err(_) => StagedOutcomeRecord::Failed {
                category: FailureCategory::Execution,
                message: "workflow callback panicked".into(),
            },
        };
        Self {
            node,
            lineage,
            outcome,
            children: None,
        }
    }

    pub(crate) fn action<T: StateTyping>(
        activation: &FrontierRecord,
        before: &Map<String, Value>,
        writes: &[StateChange],
        result: Map<String, Value>,
        typing: &T,
    ) -> Result<Self, AppError> {
        JsonBoundary::validate_size(&result)?;
        StateChange::validate_count(writes.len())?;
        let mut state = ActivationState::new(before)?;
        for change in writes {
            if state.apply(change, typing).is_err() {
                return Ok(Self::failed(
                    activation.node.clone(),
                    activation.lineage.clone(),
                    FailureCategory::State,
                    "workflow state operation failed".to_owned(),
                ));
            }
        }
        Ok(Self::complete(
            activation.node.clone(),
            activation.lineage.clone(),
            state.finish(),
            result,
        ))
    }
}

impl ActivationState {
    fn new(base: &Map<String, Value>) -> Result<Self, AppError> {
        Ok(Self {
            state: WorkflowState::new(base.clone())?,
            writes: BTreeSet::new(),
            retained_positions: base.keys().cloned().collect(),
        })
    }

    fn apply<T: StateTyping>(&mut self, change: &StateChange, typing: &T) -> Result<(), AppError> {
        match change {
            StateChange::Assign { values } => self.writes.extend(values.keys().cloned()),
            StateChange::Append { values } | StateChange::Increment { values } => {
                self.writes
                    .extend(values.iter().map(|(key, _)| key.clone()));
            }
            StateChange::Extend { values } => {
                self.writes
                    .extend(values.iter().map(|(key, _)| key.clone()));
            }
            StateChange::Remove { keys } => self.writes.extend(keys.iter().cloned()),
            StateChange::Wipe { keep } => {
                self.writes.extend(
                    self.state
                        .values()
                        .keys()
                        .filter(|key| !keep.contains(key))
                        .cloned(),
                );
            }
            StateChange::Merge { state } => self.writes.extend(state.values().keys().cloned()),
            StateChange::Subset { .. } => {
                self.writes.extend(self.state.values().keys().cloned());
                self.retained_positions.clear();
            }
        }
        self.state = self.state.apply(change, typing)?;
        self.retained_positions
            .retain(|key| self.state.values().contains_key(key));
        Ok(())
    }

    fn finish(self) -> Vec<StateChangeRecord> {
        let mut changes = self
            .writes
            .iter()
            .filter(|key| !self.state.values().contains_key(*key))
            .map(|target| StateChangeRecord::Remove {
                target: target.clone(),
            })
            .collect::<Vec<_>>();
        for (target, value) in self.state.values {
            if self.writes.contains(&target) {
                changes.push(if self.retained_positions.contains(&target) {
                    StateChangeRecord::Assign { target, value }
                } else {
                    StateChangeRecord::Reinsert { target, value }
                });
            }
        }
        changes
    }
}

impl StateMutation {
    fn evaluate<C: ConditionBackend>(
        &self,
        state: &WorkflowState,
        conditions: &[C],
    ) -> Result<StateChange, AppError> {
        match self {
            Self::Assign { target, value } => Ok(StateChange::Assign {
                values: Map::from_iter([(target.clone(), value.evaluate(state, conditions)?)]),
            }),
            Self::Append { target, value } => Ok(StateChange::Append {
                values: vec![(target.clone(), value.evaluate(state, conditions)?)],
            }),
            Self::Extend { target, values } => {
                let Value::Array(values) = values.evaluate(state, conditions)? else {
                    return Err(AppError::InvalidInput(
                        "state extend expression did not produce an array".to_owned(),
                    ));
                };
                Ok(StateChange::Extend {
                    values: vec![(target.clone(), values)],
                })
            }
            Self::Increment { target, value } => Ok(StateChange::Increment {
                values: vec![(target.clone(), value.evaluate(state, conditions)?)],
            }),
            Self::Remove { target } => Ok(StateChange::Remove {
                keys: vec![target.clone()],
            }),
            Self::Wipe { keep } => Ok(StateChange::Wipe { keep: keep.clone() }),
            Self::Merge { state: value } => {
                let Value::Object(values) = value.evaluate(state, conditions)? else {
                    return Err(AppError::InvalidInput(
                        "state merge expression did not produce an object".to_owned(),
                    ));
                };
                Ok(StateChange::Merge {
                    state: WorkflowState::new(values)?,
                })
            }
            Self::Subset { keys } => Ok(StateChange::Subset { keys: keys.clone() }),
        }
    }
}

impl StateChangeRecord {
    pub(super) fn apply_all<'a>(
        activations: impl Iterator<Item = &'a StagedActivationRecord>,
        base: &Map<String, Value>,
        definition: &CompiledDefinition,
    ) -> Result<Map<String, Value>, AppError> {
        let mut activations = activations.collect::<Vec<_>>();
        activations
            .sort_by(|left, right| (&left.node, &left.lineage).cmp(&(&right.node, &right.lineage)));
        let mut state = base.clone();
        let mut assignments = BTreeMap::<&str, (Reducer, Vec<Value>, bool)>::new();
        let mut assignment_order = Vec::new();
        let mut removals = BTreeSet::new();
        for activation in activations {
            for change in activation.changes() {
                match change {
                    Self::Assign { target, value } | Self::Reinsert { target, value } => {
                        let reducer = activation
                            .reducer(target, definition)
                            .unwrap_or(Reducer::RejectConcurrent);
                        let entry = assignments.entry(target).or_insert_with(|| {
                            assignment_order.push(target.as_str());
                            (reducer, Vec::new(), false)
                        });
                        if entry.0 != reducer {
                            return Err(AppError::InvalidInput(
                                "state target has conflicting reducers in one barrier".to_owned(),
                            ));
                        }
                        entry.1.push(value.clone());
                        entry.2 |= matches!(change, Self::Reinsert { .. });
                    }
                    Self::Remove { target } => {
                        removals.insert(target.as_str());
                    }
                }
            }
        }
        if removals
            .iter()
            .any(|target| assignments.contains_key(target))
        {
            return Err(AppError::InvalidInput(
                "state key is both assigned and removed in one barrier".to_owned(),
            ));
        }
        for target in removals {
            state.shift_remove(target);
        }
        for target in assignment_order {
            let (reducer, values, reinsert) = assignments.remove(target).ok_or_else(|| {
                AppError::Integration("state contribution order lost its target".to_owned())
            })?;
            let prior = state.get(target).cloned().unwrap_or(Value::Null);
            let value = reducer.apply(prior, values)?;
            if reinsert {
                state.shift_remove(target);
            }
            state.insert(target.to_owned(), value);
        }
        Ok(state)
    }
}

impl Reducer {
    pub(crate) fn apply(&self, prior: Value, values: Vec<Value>) -> Result<Value, AppError> {
        match self {
            Self::RejectConcurrent => {
                if values.len() != 1 {
                    return Err(AppError::InvalidInput(
                        "reject-concurrent reducer received multiple values".to_owned(),
                    ));
                }
                values
                    .into_iter()
                    .next()
                    .ok_or_else(|| AppError::Integration("reducer lost its sole value".to_owned()))
            }
            Self::Collect => Ok(Value::Array(values)),
            Self::AppendArray => {
                let Value::Array(mut output) = prior else {
                    return Err(AppError::InvalidInput(
                        "append-array reducer requires an array state value".to_owned(),
                    ));
                };
                for value in values {
                    let Value::Array(mut value) = value else {
                        return Err(AppError::InvalidInput(
                            "append-array contribution must be an array".to_owned(),
                        ));
                    };
                    output.append(&mut value);
                }
                Ok(Value::Array(output))
            }
            Self::SumInteger => {
                let mut total = prior.as_i64().ok_or_else(|| {
                    AppError::InvalidInput(
                        "sum-integer reducer requires an integer state value".to_owned(),
                    )
                })?;
                for value in values {
                    total = total
                        .checked_add(value.as_i64().ok_or_else(|| {
                            AppError::InvalidInput(
                                "sum-integer contribution must be an integer".to_owned(),
                            )
                        })?)
                        .ok_or_else(|| {
                            AppError::InvalidInput("sum-integer reducer overflowed".to_owned())
                        })?;
                }
                Ok(Value::Number(Number::from(total)))
            }
            Self::MergeObject => {
                let Value::Object(mut output) = prior else {
                    return Err(AppError::InvalidInput(
                        "merge-object reducer requires an object state value".to_owned(),
                    ));
                };
                for value in values {
                    let Value::Object(value) = value else {
                        return Err(AppError::InvalidInput(
                            "merge-object contribution must be an object".to_owned(),
                        ));
                    };
                    for (key, value) in value {
                        output.insert(key, value);
                    }
                }
                Ok(Value::Object(output))
            }
        }
    }
}
