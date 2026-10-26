use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::AppError;
use crate::workflow::definition::{
    ChildInitializer, CompiledDefinition, JsonBoundary, MapDeclaration, PromptPart, Reducer,
    RouteMode, Step, ValueExpr,
};
use crate::workflow::run::{
    ActionPosition, AttemptIdentity, CheckpointPhaseRecord, CheckpointRecord,
    ChildMembershipRecord, ChildTask, FailureCategory, FrontierRecord, MAX_CHILD_DEPTH, RunRecord,
};
use crate::workflow::{
    ActionOutcome, ConditionBackend, ExecutionEvent, StateTyping, WorkflowId, WorkflowState,
};

mod expression;
mod state;

pub(crate) const MAX_FRONTIER: usize = 4_096;
pub(crate) const MAX_RUNTIME_BYTES: usize = 1_048_576;
const MAX_SUPERSTEPS: u64 = 1_000_000;
const MAX_MAP_ITEMS: usize = 4_096;

pub(crate) struct ExecutionEngine<'a, T: StateTyping, C: ConditionBackend> {
    definition: &'a CompiledDefinition,
    typing: &'a T,
    conditions: &'a [C],
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct StagedActivationRecord {
    pub(crate) node: String,
    pub(crate) lineage: String,
    pub(crate) outcome: StagedOutcomeRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) children: Option<(u64, Vec<ChildMembershipRecord>)>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum StagedOutcomeRecord {
    Complete {
        changes: Vec<StateChangeRecord>,
        result: Map<String, Value>,
    },
    Deferred,
    Failed {
        category: FailureCategory,
        message: String,
    },
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum StateChangeRecord {
    Assign { target: String, value: Value },
    Reinsert { target: String, value: Value },
    Remove { target: String },
}

pub(crate) enum BspDecision {
    Activate(Vec<FrontierRecord>),
    AwaitControls,
    AdmitChildren {
        sequence: u64,
        children: Vec<RunRecord>,
    },
    ExecuteChildren {
        sequence: u64,
        children: Vec<RunRecord>,
    },
    Stage(Vec<StagedActivationRecord>),
    Barrier {
        state: Map<String, Value>,
        frontier: Vec<FrontierRecord>,
        settled_joins: BTreeMap<String, usize>,
        superstep: u64,
        outcomes: Vec<ActionOutcome>,
        sources: Vec<FrontierRecord>,
    },
    Routed {
        frontier: Vec<FrontierRecord>,
    },
    Inputs {
        node: String,
        lineage: String,
        required: Vec<String>,
    },
    Interaction {
        node: String,
        lineage: String,
        message: String,
    },
    Agent(AgentRequest),
    Descendants {
        node: String,
        lineage: String,
        inputs: Vec<ChildTask>,
        runtime_inputs: Vec<Map<String, Value>>,
    },
    Sleep {
        node: String,
        lineage: String,
        delay_millis: u64,
    },
    Succeeded,
    Failed {
        category: FailureCategory,
        message: String,
        observation: Option<ActionPosition>,
    },
}

impl BspDecision {
    pub(crate) fn pending_node(&self, halt_before: &[String]) -> Option<&str> {
        match self {
            Self::Activate(selected) => selected
                .iter()
                .find(|activation| halt_before.iter().any(|node| node == &activation.node))
                .map(|activation| activation.node.as_str()),
            Self::AwaitControls
            | Self::AdmitChildren { .. }
            | Self::ExecuteChildren { .. }
            | Self::Stage(_)
            | Self::Barrier { .. }
            | Self::Routed { .. }
            | Self::Inputs { .. }
            | Self::Interaction { .. }
            | Self::Agent(_)
            | Self::Descendants { .. }
            | Self::Sleep { .. }
            | Self::Succeeded
            | Self::Failed { .. } => None,
        }
    }

    fn failed(category: FailureCategory, message: String) -> Self {
        Self::Failed {
            category,
            message,
            observation: None,
        }
    }

    fn limit(message: &str) -> Self {
        Self::failed(FailureCategory::Limit, message.to_owned())
    }

    fn descendants(
        activation: &FrontierRecord,
        child_depth: u8,
        resolved: impl ExactSizeIterator<Item = Result<(ChildTask, Map<String, Value>), AppError>>,
    ) -> Result<Self, AppError> {
        if resolved.len() > 0 && child_depth >= MAX_CHILD_DEPTH {
            return Ok(Self::limit("child workflow depth bound exhausted"));
        }
        let mut inputs = Vec::with_capacity(resolved.len());
        let mut runtime_inputs = Vec::with_capacity(resolved.len());
        let mut input_bytes = 2_usize;
        let mut runtime_bytes = 2_usize;
        let mut task_bytes = 2_usize;
        for input in resolved {
            let (input, runtime) = input?;
            JsonBoundary::validate_inputs(&runtime)?;
            let encoded = serde_json::to_vec(&runtime).map_err(|_| {
                AppError::InvalidInput("child runtime input encoding failed".into())
            })?;
            let Some(next_bytes) = runtime_bytes
                .checked_add(usize::from(!runtime_inputs.is_empty()))
                .and_then(|bytes| bytes.checked_add(encoded.len()))
                .filter(|bytes| *bytes <= MAX_RUNTIME_BYTES)
            else {
                return Ok(Self::limit("child runtime input byte bound exhausted"));
            };
            runtime_bytes = next_bytes;
            runtime_inputs.push(runtime);
            let encoded = serde_json::to_vec(&input.input)
                .map_err(|_| AppError::InvalidInput("child input encoding failed".into()))?;
            let Some(next_bytes) = input_bytes
                .checked_add(usize::from(!inputs.is_empty()))
                .and_then(|bytes| bytes.checked_add(encoded.len()))
                .filter(|bytes| *bytes <= MAX_RUNTIME_BYTES)
            else {
                return Ok(Self::limit("child input byte bound exhausted"));
            };
            input_bytes = next_bytes;
            let encoded = serde_json::to_vec(&input)
                .map_err(|_| AppError::InvalidInput("child task encoding failed".into()))?;
            let Some(next_bytes) = task_bytes
                .checked_add(usize::from(!inputs.is_empty()))
                .and_then(|bytes| bytes.checked_add(encoded.len()))
                .filter(|bytes| *bytes <= crate::storage::MAX_RECORD_BYTES)
            else {
                return Ok(Self::limit(
                    "child task metadata exceeds the durable record bound",
                ));
            };
            task_bytes = next_bytes;
            inputs.push(input);
        }
        Ok(Self::Descendants {
            node: activation.node.clone(),
            lineage: activation.lineage.clone(),
            inputs,
            runtime_inputs,
        })
    }
}

pub(crate) struct AgentRequest {
    node: String,
    lineage: String,
    attempt_identity: Option<AttemptIdentity>,
    agent: String,
    prompt: String,
    stream: bool,
    partials: Option<tokio::sync::mpsc::Sender<ExecutionEvent>>,
    structured_schema: Option<Value>,
    text_target: Option<String>,
    structured_target: Option<String>,
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum RouteIdentity {
    Lineage(String),
    Join(usize),
}

struct RoutedFrontier<'definition, C: ConditionBackend> {
    definition: &'definition CompiledDefinition,
    conditions: &'definition [C],
    entries: BTreeMap<(String, RouteIdentity), FrontierRecord>,
}

impl<'a, T: StateTyping, C: ConditionBackend> ExecutionEngine<'a, T, C> {
    pub(crate) fn new(
        definition: &'a CompiledDefinition,
        typing: &'a T,
        conditions: &'a [C],
    ) -> Self {
        Self {
            definition,
            typing,
            conditions,
        }
    }

    pub(crate) fn advance(&self, checkpoint: &CheckpointRecord) -> Result<BspDecision, AppError> {
        if let CheckpointPhaseRecord::Routing { sources } = &checkpoint.phase {
            return self.route_committed(checkpoint, sources);
        }
        if checkpoint.superstep >= MAX_SUPERSTEPS {
            return Ok(BspDecision::limit("workflow superstep bound exhausted"));
        }
        if checkpoint.frontier.is_empty()
            && checkpoint.staged.is_empty()
            && checkpoint.controls.is_empty()
        {
            return Ok(BspDecision::Succeeded);
        }
        if checkpoint.frontier.len() > MAX_FRONTIER {
            return Ok(BspDecision::limit("workflow frontier bound exhausted"));
        }

        let staged_keys = checkpoint
            .staged
            .iter()
            .map(|staged| (staged.node.as_str(), staged.lineage.as_str()))
            .collect::<BTreeSet<_>>();
        let mut ready = checkpoint
            .frontier
            .iter()
            .filter(|activation| {
                !staged_keys.contains(&(activation.node.as_str(), activation.lineage.as_str()))
                    && checkpoint
                        .control_for(&activation.node, &activation.lineage)
                        .is_none()
            })
            .collect::<Vec<_>>();
        ready
            .sort_by(|left, right| (&left.node, &left.lineage).cmp(&(&right.node, &right.lineage)));
        let mut selected = Vec::new();
        for activation in ready {
            if checkpoint.loses_join_first(self.definition, &activation.node, &activation.lineage) {
                selected.push(activation.clone());
                continue;
            }
            let Some(node) = self.definition.node(&activation.node) else {
                return Ok(BspDecision::failed(
                    FailureCategory::Execution,
                    "workflow frontier references an unknown node".to_owned(),
                ));
            };
            match node.step() {
                Step::JoinAll {} | Step::JoinFirst {} => {
                    if checkpoint.activation_ready(self.definition, activation) {
                        selected.push(activation.clone());
                    }
                }
                Step::Action { .. }
                | Step::State { .. }
                | Step::Input { .. }
                | Step::Agent { .. }
                | Step::Child { .. }
                | Step::Map(_)
                | Step::Pipeline { .. }
                | Step::Sleep { .. } => selected.push(activation.clone()),
            }
        }
        if !selected.is_empty() {
            return Ok(BspDecision::Activate(selected));
        }
        if !checkpoint.controls.is_empty() {
            return Ok(BspDecision::AwaitControls);
        }
        if !checkpoint.staged.is_empty() {
            return self.publish_staged(checkpoint);
        }
        Ok(BspDecision::failed(
            FailureCategory::Execution,
            "workflow frontier cannot make progress".to_owned(),
        ))
    }

    pub(crate) fn activate(
        &self,
        checkpoint: &CheckpointRecord,
        activation: &FrontierRecord,
        child_depth: u8,
        resolve: impl FnMut(WorkflowId, Option<String>) -> Result<(WorkflowId, String), AppError>,
    ) -> Result<BspDecision, AppError> {
        if checkpoint.loses_join_first(self.definition, &activation.node, &activation.lineage) {
            return Ok(BspDecision::Stage(vec![StagedActivationRecord {
                node: activation.node.clone(),
                lineage: activation.lineage.clone(),
                outcome: StagedOutcomeRecord::Deferred,
                children: None,
            }]));
        }
        let node = self
            .definition
            .node(&activation.node)
            .ok_or_else(|| AppError::Integration("selected workflow node is unavailable".into()))?;
        match node.step() {
            Step::Action { .. } => Err(AppError::Integration(
                "caller activation requires its local source owner".into(),
            )),
            Step::JoinAll {} | Step::JoinFirst {} => {
                Ok(BspDecision::Stage(vec![StagedActivationRecord::complete(
                    activation.node.clone(),
                    activation.lineage.clone(),
                    Vec::new(),
                    Map::new(),
                )]))
            }
            Step::State { mutations } => {
                Ok(BspDecision::Stage(vec![StagedActivationRecord::mutate(
                    activation.node.clone(),
                    activation.lineage.clone(),
                    mutations,
                    &checkpoint.state,
                    self.typing,
                    self.conditions,
                )]))
            }
            Step::Input { .. }
            | Step::Agent { .. }
            | Step::Child { .. }
            | Step::Map(_)
            | Step::Pipeline { .. }
            | Step::Sleep { .. } => {
                let mut decision = match catch_unwind(AssertUnwindSafe(|| {
                    self.prepare_control(checkpoint, activation, child_depth, resolve)
                })) {
                    Ok(Ok(decision)) => decision,
                    Ok(Err(error)) => BspDecision::failed(
                        FailureCategory::Evaluation,
                        match error {
                            AppError::InvalidInput(message) => message,
                            _ => "workflow evaluation failed".into(),
                        },
                    ),
                    Err(_) => BspDecision::failed(
                        FailureCategory::Execution,
                        "workflow callback panicked".into(),
                    ),
                };
                if let BspDecision::Failed { observation, .. } = &mut decision {
                    *observation = Some(checkpoint.proposed_position(
                        self.definition,
                        &activation.node,
                        &activation.lineage,
                        &[],
                    )?);
                }
                Ok(decision)
            }
        }
    }

    fn prepare_control(
        &self,
        checkpoint: &CheckpointRecord,
        activation: &FrontierRecord,
        child_depth: u8,
        mut resolve: impl FnMut(WorkflowId, Option<String>) -> Result<(WorkflowId, String), AppError>,
    ) -> Result<BspDecision, AppError> {
        let node = self
            .definition
            .node(&activation.node)
            .ok_or_else(|| AppError::Integration("workflow node is unavailable".into()))?;
        match node.step() {
            Step::Input { message, .. } => Ok(BspDecision::Interaction {
                node: activation.node.clone(),
                lineage: activation.lineage.clone(),
                message: PromptPart::render_all(message, &checkpoint.state)?,
            }),
            Step::Agent { .. } => Ok(BspDecision::Agent(AgentRequest::recover(
                self.definition,
                &checkpoint.state,
                node.id(),
                &activation.lineage,
                None,
            )?)),
            Step::Child {
                input,
                inputs,
                task_key,
                initializer,
                ..
            } => {
                let state = WorkflowState::new(checkpoint.state.clone())?;
                let (selection, history) = node.step().child_selection(0)?;
                let (workflow, revision) = selection.resolve(&state, self.conditions)?;
                let (workflow, revision) = resolve(workflow, revision)?;
                BspDecision::descendants(
                    activation,
                    child_depth,
                    std::iter::once(Ok((
                        ChildTask {
                            workflow,
                            revision,
                            history,
                            task_key: ChildTask::resolve_key(
                                task_key.as_deref(),
                                "0-0".into(),
                                &state,
                                self.conditions,
                            )?,
                            initializer: initializer
                                .as_ref()
                                .map(|value| value.resolve(&state, self.conditions))
                                .transpose()?,
                            input: ValueExpr::evaluate_map(input, &state, self.conditions)?,
                        },
                        ValueExpr::evaluate_map(inputs, &state, self.conditions)?,
                    ))),
                )
            }
            Step::Map(MapDeclaration::Items {
                items,
                input_key,
                index_key,
                input,
                inputs,
                task_key,
                initializer,
                ..
            }) => {
                let state = WorkflowState::new(checkpoint.state.clone())?;
                let Value::Array(values) = items.evaluate(&state, self.conditions)? else {
                    return Ok(BspDecision::failed(
                        FailureCategory::Evaluation,
                        "map expression did not produce an array".into(),
                    ));
                };
                if values.len() > MAX_MAP_ITEMS {
                    return Ok(BspDecision::limit("map item bound exhausted"));
                }
                let (selection, history) = node.step().child_selection(0)?;
                let (workflow, revision) = selection.resolve(&state, self.conditions)?;
                let (workflow, revision) = resolve(workflow, revision)?;
                let base_input = ValueExpr::evaluate_map(input, &state, self.conditions)?;
                let runtime = ValueExpr::evaluate_map(inputs, &state, self.conditions)?;
                let task_key = task_key
                    .as_deref()
                    .map(|value| {
                        ChildTask::resolve_key(Some(value), String::new(), &state, self.conditions)
                    })
                    .transpose()?;
                let initializer = initializer
                    .as_ref()
                    .map(|value| value.resolve(&state, self.conditions))
                    .transpose()?;
                let inputs = values.into_iter().enumerate().map(|(index, value)| {
                    let mut mapped = base_input.clone();
                    mapped.insert(input_key.clone(), value);
                    if let Some(index_key) = index_key {
                        mapped.insert(index_key.clone(), Value::from(index as u64));
                    }
                    Ok((
                        ChildTask {
                            workflow: workflow.clone(),
                            revision: revision.clone(),
                            input: mapped,
                            history,
                            task_key: task_key.clone().unwrap_or_else(|| format!("0-{index}")),
                            initializer: initializer.clone(),
                        },
                        runtime.clone(),
                    ))
                });
                BspDecision::descendants(activation, child_depth, inputs)
            }
            Step::Map(MapDeclaration::Tasks { tasks, .. }) => {
                let state = WorkflowState::new(checkpoint.state.clone())?;
                let Value::Array(values) = tasks.evaluate(&state, self.conditions)? else {
                    return Ok(BspDecision::failed(
                        FailureCategory::Evaluation,
                        "map tasks expression did not produce an array".into(),
                    ));
                };
                if values.len() > MAX_MAP_ITEMS {
                    return Ok(BspDecision::limit("map task bound exhausted"));
                }
                BspDecision::descendants(
                    activation,
                    child_depth,
                    values
                        .into_iter()
                        .map(|value| ChildTask::resolved(value, &mut resolve)),
                )
            }
            Step::Pipeline { children, .. } => {
                let state = WorkflowState::new(checkpoint.state.clone())?;
                BspDecision::descendants(
                    activation,
                    child_depth,
                    children.iter().enumerate().map(|(index, child)| {
                        let (selection, history) = node.step().child_selection(index)?;
                        let (workflow, revision) = selection.resolve(&state, self.conditions)?;
                        let (workflow, revision) = resolve(workflow, revision)?;
                        Ok((
                            ChildTask {
                                workflow,
                                revision,
                                history,
                                task_key: ChildTask::resolve_key(
                                    child.task_key.as_deref(),
                                    format!("{index}-0"),
                                    &state,
                                    self.conditions,
                                )?,
                                initializer: child
                                    .initializer
                                    .as_ref()
                                    .map(|value| value.resolve(&state, self.conditions))
                                    .transpose()?,
                                input: ValueExpr::evaluate_map(
                                    &child.input,
                                    &state,
                                    self.conditions,
                                )?,
                            },
                            ValueExpr::evaluate_map(&child.inputs, &state, self.conditions)?,
                        ))
                    }),
                )
            }
            Step::Sleep { delay_ms, .. } => Ok(BspDecision::Sleep {
                node: activation.node.clone(),
                lineage: activation.lineage.clone(),
                delay_millis: *delay_ms,
            }),
            Step::Action { .. } | Step::State { .. } | Step::JoinAll {} | Step::JoinFirst {} => {
                Err(AppError::Integration(
                    "workflow node has no control preparation".into(),
                ))
            }
        }
    }

    pub(crate) fn stage_after_control(
        &self,
        node: &str,
        lineage: &str,
        contributions: Vec<(String, Value)>,
        result: Map<String, Value>,
    ) -> Result<StagedActivationRecord, AppError> {
        for (_, value) in &contributions {
            crate::workflow::definition::JsonBoundary::validate(value)?;
        }
        crate::workflow::definition::JsonBoundary::validate_size(&result)?;
        let staged = StagedActivationRecord::complete(
            node.to_owned(),
            lineage.to_owned(),
            contributions
                .into_iter()
                .map(|(target, value)| StateChangeRecord::Assign { target, value })
                .collect(),
            result,
        );
        if serde_json::to_vec(&staged).is_ok_and(|encoded| encoded.len() <= MAX_RUNTIME_BYTES) {
            Ok(staged)
        } else {
            Err(AppError::InvalidInput(
                "workflow stage exceeds its private bound".to_owned(),
            ))
        }
    }

    fn settle_staged(
        &self,
        checkpoint: &CheckpointRecord,
    ) -> Result<Map<String, Value>, (FailureCategory, String)> {
        if let Some(failure) = checkpoint
            .selected_staged(self.definition)
            .find_map(|activation| {
                let StagedOutcomeRecord::Failed { category, message } = &activation.outcome else {
                    return None;
                };
                Some((*category, message.clone()))
            })
        {
            return Err(failure);
        }
        let failure = |category| (category, "workflow state reduction failed".to_owned());
        let state = StateChangeRecord::apply_all(
            checkpoint.selected_staged(self.definition),
            &checkpoint.state,
            self.definition,
        )
        .map_err(|error| {
            failure(if matches!(error, AppError::Integration(_)) {
                FailureCategory::Execution
            } else {
                FailureCategory::State
            })
        })?;
        self.definition
            .validate_state(&state)
            .map_err(|_| failure(FailureCategory::State))?;
        let state = WorkflowState::new(state).map_err(|_| failure(FailureCategory::State))?;
        state
            .validate_typing(self.typing)
            .map_err(|_| failure(FailureCategory::State))?;
        Ok(state.values)
    }

    fn publish_staged(&self, checkpoint: &CheckpointRecord) -> Result<BspDecision, AppError> {
        let completed_keys = checkpoint
            .staged
            .iter()
            .map(|entry| (entry.node.as_str(), entry.lineage.as_str()))
            .collect::<BTreeSet<_>>();
        let frontier = checkpoint
            .frontier
            .iter()
            .filter(|existing| {
                !completed_keys.contains(&(existing.node.as_str(), existing.lineage.as_str()))
            })
            .cloned()
            .collect();
        let state = match catch_unwind(AssertUnwindSafe(|| self.settle_staged(checkpoint)))
            .unwrap_or_else(|_| {
                Err((
                    FailureCategory::Execution,
                    "workflow callback panicked".into(),
                ))
            }) {
            Ok(state) => state,
            Err((category, message)) => {
                let failed = checkpoint
                    .selected_staged(self.definition)
                    .find(|activation| {
                        matches!(&activation.outcome, StagedOutcomeRecord::Failed { .. })
                    })
                    .or_else(|| {
                        checkpoint
                            .selected_staged(self.definition)
                            .find(|activation| {
                                matches!(&activation.outcome, StagedOutcomeRecord::Complete { .. })
                            })
                    });
                let observation = failed
                    .map(|activation| {
                        checkpoint.proposed_position(
                            self.definition,
                            &activation.node,
                            &activation.lineage,
                            &[],
                        )
                    })
                    .transpose()?;
                return Ok(BspDecision::Failed {
                    category,
                    message,
                    observation,
                });
            }
        };
        let mut settled_joins = checkpoint.settled_joins.clone();
        let mut outcomes = Vec::new();
        let mut sources = Vec::new();
        let first_ordinal = checkpoint.next_action_ordinal()?;
        for activation in checkpoint.selected_staged(self.definition) {
            let StagedOutcomeRecord::Complete { result, .. } = &activation.outcome else {
                continue;
            };
            if self.definition.is_join_first(&activation.node) {
                let node = &activation.node;
                let visit = self.definition.join_visit(node, &activation.lineage);
                settled_joins.insert(node.clone(), visit);
            }
            let source = checkpoint
                .frontier
                .iter()
                .find(|source| {
                    source.node == activation.node && source.lineage == activation.lineage
                })
                .ok_or_else(|| {
                    AppError::Integration("completed activation source is unavailable".to_owned())
                })?;
            sources.push(source.clone());
            let ordinal = first_ordinal
                .checked_add(outcomes.len() as u64)
                .ok_or_else(|| AppError::Conflict("run action ordinal is exhausted".to_owned()))?;
            outcomes.push(ActionOutcome {
                node: activation.node.clone(),
                ordinal,
                result: result.clone(),
            });
        }
        Ok(BspDecision::Barrier {
            state,
            frontier,
            settled_joins,
            superstep: self.next_superstep(checkpoint)?,
            outcomes,
            sources,
        })
    }

    fn route_committed(
        &self,
        checkpoint: &CheckpointRecord,
        sources: &[FrontierRecord],
    ) -> Result<BspDecision, AppError> {
        let mut routed = RoutedFrontier::new(self.definition, self.conditions);
        let state = WorkflowState::new(checkpoint.state.clone())?;
        for remaining in &checkpoint.frontier {
            routed.insert(remaining.clone())?;
        }
        for source in sources {
            if !routed.route(&source.node, &source.lineage, &state)? {
                return Ok(BspDecision::limit("workflow lineage byte bound exhausted"));
            }
        }
        Ok(BspDecision::Routed {
            frontier: routed.finish(),
        })
    }

    fn next_superstep(&self, checkpoint: &CheckpointRecord) -> Result<u64, AppError> {
        checkpoint
            .superstep
            .checked_add(1)
            .filter(|next| *next <= MAX_SUPERSTEPS)
            .ok_or_else(|| AppError::Conflict("workflow superstep bound exhausted".to_owned()))
    }
}

impl AgentRequest {
    pub(crate) fn recover(
        definition: &CompiledDefinition,
        state: &Map<String, Value>,
        node: &str,
        lineage: &str,
        attempt_identity: Option<AttemptIdentity>,
    ) -> Result<Self, AppError> {
        let Some(Step::Agent {
            agent,
            prompt,
            stream,
            structured_schema,
            text_target,
            structured_target,
            ..
        }) = definition.node(node).map(|node| node.step())
        else {
            return Err(AppError::Integration(
                "agent control definition is unavailable".to_owned(),
            ));
        };
        Ok(Self {
            node: node.to_owned(),
            lineage: lineage.to_owned(),
            attempt_identity,
            agent: agent.clone(),
            prompt: PromptPart::render_all(prompt, state)?,
            stream: *stream,
            partials: None,
            structured_schema: structured_schema.clone(),
            text_target: text_target.clone(),
            structured_target: structured_target.clone(),
        })
    }

    pub(crate) fn node(&self) -> &str {
        &self.node
    }

    pub(crate) fn lineage(&self) -> &str {
        &self.lineage
    }

    pub(crate) fn attempt_identity(&self) -> Option<&AttemptIdentity> {
        self.attempt_identity.as_ref()
    }

    pub(crate) fn agent(&self) -> &str {
        &self.agent
    }

    pub(crate) fn prompt(&self) -> &str {
        &self.prompt
    }

    pub(crate) fn is_streaming(&self) -> bool {
        self.stream
    }

    pub(crate) fn observe_partials(&mut self, sender: tokio::sync::mpsc::Sender<ExecutionEvent>) {
        self.partials = Some(sender);
    }

    pub(crate) fn take_partials(&mut self) -> Option<tokio::sync::mpsc::Sender<ExecutionEvent>> {
        self.partials.take()
    }

    pub(crate) fn structured_schema(&self) -> Option<&Value> {
        self.structured_schema.as_ref()
    }

    pub(crate) fn text_target(&self) -> Option<&str> {
        self.text_target.as_deref()
    }

    pub(crate) fn structured_target(&self) -> Option<&str> {
        self.structured_target.as_deref()
    }
}

impl StagedActivationRecord {
    pub(crate) fn complete(
        node: String,
        lineage: String,
        changes: Vec<StateChangeRecord>,
        result: Map<String, Value>,
    ) -> Self {
        Self {
            node,
            lineage,
            outcome: StagedOutcomeRecord::Complete { changes, result },
            children: None,
        }
    }

    pub(crate) fn failed(
        node: String,
        lineage: String,
        category: FailureCategory,
        message: String,
    ) -> Self {
        Self {
            node,
            lineage,
            outcome: StagedOutcomeRecord::Failed { category, message },
            children: None,
        }
    }

    fn changes(&self) -> &[StateChangeRecord] {
        match &self.outcome {
            StagedOutcomeRecord::Complete { changes, .. } => changes,
            StagedOutcomeRecord::Deferred | StagedOutcomeRecord::Failed { .. } => &[],
        }
    }

    fn reducer(&self, target: &str, definition: &CompiledDefinition) -> Option<Reducer> {
        match definition.node(&self.node).map(|node| node.step()) {
            Some(Step::Pipeline { reductions, .. }) if reductions.is_empty() => {
                definition.reducer(target)
            }
            Some(step) if step.child_plan().is_some() => step
                .child_plan()?
                .1
                .iter()
                .find(|reduction| reduction.target() == target)
                .map(|reduction| reduction.reducer()),
            _ => definition.reducer(target),
        }
    }
}

impl<'definition, C: ConditionBackend> RoutedFrontier<'definition, C> {
    fn new(definition: &'definition CompiledDefinition, conditions: &'definition [C]) -> Self {
        Self {
            definition,
            conditions,
            entries: BTreeMap::new(),
        }
    }

    fn route(
        &mut self,
        source: &str,
        lineage: &str,
        state: &WorkflowState,
    ) -> Result<bool, AppError> {
        let route = self.definition.route(source).ok_or_else(|| {
            AppError::Integration("compiled workflow route is missing".to_owned())
        })?;
        for branch in route.branches() {
            let selected = branch.condition().map_or(Ok(true), |condition| {
                condition.evaluate(state, self.conditions)
            })?;
            if !selected {
                continue;
            }
            if let Some(target) = branch.target().node() {
                if lineage
                    .len()
                    .checked_add(target.len())
                    .and_then(|bytes| bytes.checked_add(1))
                    .is_none_or(|bytes| bytes > MAX_RUNTIME_BYTES)
                {
                    return Ok(false);
                }
                self.insert(FrontierRecord {
                    node: target.to_owned(),
                    arrived: true,
                    lineage: format!("{lineage}/{target}"),
                })?;
            }
            if matches!(route.mode(), RouteMode::First) {
                break;
            }
        }
        Ok(true)
    }

    fn insert(&mut self, entry: FrontierRecord) -> Result<(), AppError> {
        let identity = match self.definition.node(&entry.node).map(|node| node.step()) {
            Some(Step::JoinAll {} | Step::JoinFirst {}) => {
                RouteIdentity::Join(self.definition.join_visit(&entry.node, &entry.lineage))
            }
            _ => RouteIdentity::Lineage(entry.lineage.clone()),
        };
        let key = (entry.node.clone(), identity);
        if let Some(existing) = self.entries.get_mut(&key) {
            existing.arrived |= entry.arrived;
            if entry.lineage < existing.lineage {
                existing.lineage = entry.lineage;
            }
        } else {
            self.entries.insert(key, entry);
        }
        if self.entries.len() > MAX_FRONTIER {
            return Err(AppError::InvalidInput(
                "workflow frontier exceeds the private bound".to_owned(),
            ));
        }
        Ok(())
    }

    fn finish(self) -> Vec<FrontierRecord> {
        self.entries.into_values().collect()
    }
}

impl ChildTask {
    fn resolve_key<C: ConditionBackend>(
        value: Option<&ValueExpr>,
        default: String,
        state: &WorkflowState,
        conditions: &[C],
    ) -> Result<String, AppError> {
        match value {
            Some(value) => match value.evaluate(state, conditions)? {
                Value::String(key) => Ok(key),
                _ => Err(AppError::InvalidInput(
                    "child task key must be a string".into(),
                )),
            },
            None => Ok(default),
        }
    }

    fn resolved(
        value: Value,
        resolve: &mut impl FnMut(WorkflowId, Option<String>) -> Result<(WorkflowId, String), AppError>,
    ) -> Result<(Self, Map<String, Value>), AppError> {
        let Value::Object(mut fields) = value else {
            return Err(AppError::InvalidInput(
                "resolved child task must be an object".into(),
            ));
        };
        let task_key = match fields.remove("task_key") {
            Some(Value::String(key)) => key,
            _ => {
                return Err(AppError::InvalidInput(
                    "resolved child task requires a string task_key".into(),
                ));
            }
        };
        let workflow = match fields.remove("workflow") {
            Some(Value::String(workflow)) => workflow.parse()?,
            _ => {
                return Err(AppError::InvalidInput(
                    "resolved child task requires a workflow string".into(),
                ));
            }
        };
        let revision = match fields.remove("revision") {
            Some(Value::String(revision)) => Some(revision),
            None => None,
            _ => {
                return Err(AppError::InvalidInput(
                    "resolved child task revision must be a string".into(),
                ));
            }
        };
        let (workflow, revision) = resolve(workflow, revision)?;
        let input = match fields.remove("input") {
            Some(Value::Object(input)) => input,
            _ => {
                return Err(AppError::InvalidInput(
                    "resolved child task requires object input".into(),
                ));
            }
        };
        let inputs = match fields.remove("inputs") {
            Some(Value::Object(inputs)) => inputs,
            None => Map::new(),
            _ => {
                return Err(AppError::InvalidInput(
                    "resolved child task inputs must be an object".into(),
                ));
            }
        };
        let history = match fields.remove("history") {
            Some(value) => serde_json::from_value(value).map_err(|_| {
                AppError::InvalidInput("resolved child task history is invalid".into())
            })?,
            None => Default::default(),
        };
        let initializer = fields
            .remove("initializer")
            .map(|value| {
                serde_json::from_value::<ChildInitializer<Value>>(value).map_err(|_| {
                    AppError::InvalidInput("resolved child initializer is invalid".into())
                })
            })
            .transpose()?;
        if let Some(initializer) = &initializer {
            initializer.validate()?;
        }
        if !fields.is_empty() {
            return Err(AppError::InvalidInput(
                "resolved child task contains unknown fields".into(),
            ));
        }
        Ok((
            Self {
                workflow,
                revision,
                task_key,
                input,
                history,
                initializer,
            },
            inputs,
        ))
    }
}
