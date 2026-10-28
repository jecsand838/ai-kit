use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use agents_kit::prompt::StructuredOutputRequest;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Number, Value};

use crate::AppError;
use crate::workflow::execution::MAX_RUNTIME_BYTES;
use crate::workflow::{
    ActionBackend, ActionMode, CheckpointSelector, ConditionBackend, ResumePosition, WorkflowId,
    WorkflowState,
};

mod graph;

const SCHEMA: &str = "app-kit.workflow/v2";
pub(super) const MAX_SOURCE_BYTES: usize = 1_048_576;
const MAX_NODES: usize = 1_024;
const MAX_STATE_KEYS: usize = 1_024;
pub(crate) const MAX_COLLECTION_ITEMS: usize = 4_096;
pub(super) const MAX_TEXT_BYTES: usize = 65_536;
pub(super) const MAX_DEPTH: usize = 64;
const MAX_ATTEMPTS: u32 = 64;
const MAX_DURATION_MS: u64 = 31_536_000_000;

pub(crate) struct CompiledDefinition {
    document: DefinitionDocument,
    fingerprint: String,
    node_index: BTreeMap<String, usize>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DefinitionDocument {
    schema: String,
    id: String,
    revision: String,
    state: Map<String, Value>,
    required: Vec<String>,
    reducers: BTreeMap<String, Reducer>,
    entry: String,
    nodes: Vec<Node>,
    routes: BTreeMap<String, Route>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Node {
    id: String,
    step: Step,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(
        default,
        deserialize_with = "SchemaText::present_tags",
        skip_serializing_if = "Option::is_none"
    )]
    tags: Option<Vec<String>>,
    #[serde(default)]
    retry: Option<RetryPolicy>,
    #[serde(default)]
    deadline_ms: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Step {
    Action {
        action: String,
        revision: String,
        #[serde(default)]
        project_result: bool,
    },
    Agent {
        agent: String,
        prompt: Vec<PromptPart>,
        #[serde(default)]
        stream: bool,
        #[serde(default)]
        structured_schema: Option<Value>,
        #[serde(default)]
        text_target: Option<String>,
        #[serde(default)]
        structured_target: Option<String>,
    },
    State {
        mutations: Vec<StateMutation>,
    },
    Input {
        message: Vec<PromptPart>,
        #[serde(default)]
        schema: Option<Value>,
        #[serde(default)]
        target: Option<String>,
    },
    Child {
        #[serde(
            default,
            deserialize_with = "SchemaText::present_string",
            skip_serializing_if = "Option::is_none"
        )]
        workflow: Option<String>,
        #[serde(
            default,
            deserialize_with = "ValueExpr::present",
            skip_serializing_if = "Option::is_none"
        )]
        select: Option<Box<ValueExpr>>,
        #[serde(
            default,
            deserialize_with = "SchemaText::present_string",
            skip_serializing_if = "Option::is_none"
        )]
        revision: Option<String>,
        input: BTreeMap<String, ValueExpr>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        inputs: BTreeMap<String, ValueExpr>,
        #[serde(
            default,
            deserialize_with = "ValueExpr::present",
            skip_serializing_if = "Option::is_none"
        )]
        task_key: Option<Box<ValueExpr>>,
        #[serde(
            default,
            deserialize_with = "ChildInitializer::present",
            skip_serializing_if = "Option::is_none"
        )]
        initializer: Option<Box<ChildInitializer<ValueExpr>>>,
        #[serde(default)]
        history: ChildHistory,
        reductions: Vec<ChildReduction>,
    },
    Map(MapDeclaration),
    Pipeline {
        children: Vec<PipelineChild>,
        #[serde(default)]
        reductions: Vec<ChildReduction>,
    },
    // Empty struct variants enforce deny_unknown_fields on tag-only payloads.
    JoinAll {},
    JoinFirst {},
    Sleep {
        message: String,
        delay_ms: u64,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
pub(crate) enum MapDeclaration {
    Items {
        #[serde(
            default,
            deserialize_with = "SchemaText::present_string",
            skip_serializing_if = "Option::is_none"
        )]
        workflow: Option<String>,
        #[serde(
            default,
            deserialize_with = "ValueExpr::present",
            skip_serializing_if = "Option::is_none"
        )]
        select: Option<Box<ValueExpr>>,
        #[serde(
            default,
            deserialize_with = "SchemaText::present_string",
            skip_serializing_if = "Option::is_none"
        )]
        revision: Option<String>,
        items: ValueExpr,
        input_key: String,
        #[serde(default)]
        index_key: Option<String>,
        #[serde(default)]
        input: BTreeMap<String, ValueExpr>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        inputs: BTreeMap<String, ValueExpr>,
        max_parallel: usize,
        #[serde(
            default,
            deserialize_with = "ValueExpr::present",
            skip_serializing_if = "Option::is_none"
        )]
        task_key: Option<Box<ValueExpr>>,
        #[serde(
            default,
            deserialize_with = "ChildInitializer::present",
            skip_serializing_if = "Option::is_none"
        )]
        initializer: Option<Box<ChildInitializer<ValueExpr>>>,
        #[serde(default)]
        history: ChildHistory,
        reductions: Vec<ChildReduction>,
    },
    Tasks {
        tasks: ValueExpr,
        max_parallel: usize,
        reductions: Vec<ChildReduction>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PipelineChild {
    #[serde(
        default,
        deserialize_with = "SchemaText::present_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) workflow: Option<String>,
    #[serde(
        default,
        deserialize_with = "ValueExpr::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) select: Option<Box<ValueExpr>>,
    #[serde(
        default,
        deserialize_with = "SchemaText::present_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) revision: Option<String>,
    pub(crate) input: BTreeMap<String, ValueExpr>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) inputs: BTreeMap<String, ValueExpr>,
    #[serde(
        default,
        deserialize_with = "ValueExpr::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) task_key: Option<Box<ValueExpr>>,
    #[serde(
        default,
        deserialize_with = "ChildInitializer::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) initializer: Option<Box<ChildInitializer<ValueExpr>>>,
    #[serde(default)]
    pub(crate) history: ChildHistory,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, bound(deserialize = "V: Deserialize<'de>"))]
pub(crate) struct ChildInitializer<V> {
    pub(crate) source: V,
    #[serde(
        default,
        deserialize_with = "ChildInitializer::<V>::present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) occurrence: Option<V>,
    #[serde(deserialize_with = "ChildInitializer::<V>::checkpoint")]
    pub(crate) checkpoint: CheckpointSelector,
    pub(crate) position: ResumePosition,
    #[serde(default)]
    pub(crate) overrides: BTreeMap<String, V>,
    #[serde(
        default,
        deserialize_with = "ChildInitializer::<V>::present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) fallback: Option<V>,
}

pub(crate) enum ChildSelection<'a> {
    Literal {
        workflow: &'a str,
        revision: Option<&'a str>,
    },
    Dynamic(&'a ValueExpr),
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChildHistory {
    #[default]
    Retain,
    DeleteAfterReduction,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PromptPart {
    Text { text: String },
    State { path: String },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RetryPolicy {
    Never {},
    Immediate {
        max_attempts: u32,
    },
    Fixed {
        max_attempts: u32,
        max_elapsed_ms: u64,
        delay_ms: u64,
    },
    Exponential {
        max_attempts: u32,
        max_elapsed_ms: u64,
        initial_ms: u64,
        maximum_ms: u64,
        multiplier: u32,
    },
}

#[derive(Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Reducer {
    RejectConcurrent,
    Collect,
    AppendArray,
    SumInteger,
    MergeObject,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StateMutation {
    Assign { target: String, value: ValueExpr },
    Append { target: String, value: ValueExpr },
    Extend { target: String, values: ValueExpr },
    Increment { target: String, value: ValueExpr },
    Remove { target: String },
    Wipe { keep: Vec<String> },
    Merge { state: ValueExpr },
    Subset { keys: Vec<String> },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ValueExpr {
    Literal {
        value: Value,
    },
    State {
        path: String,
    },
    Length {
        value: Box<ValueExpr>,
    },
    Positive {
        value: Box<ValueExpr>,
    },
    Negative {
        value: Box<ValueExpr>,
    },
    Not {
        value: Box<ValueExpr>,
    },
    And {
        values: Vec<ValueExpr>,
    },
    Or {
        values: Vec<ValueExpr>,
    },
    Conditional {
        when: Box<Condition>,
        then: Box<ValueExpr>,
        #[serde(rename = "else")]
        otherwise: Box<ValueExpr>,
    },
    Attribute {
        value: Box<ValueExpr>,
        name: String,
    },
    Index {
        value: Box<ValueExpr>,
        index: Box<ValueExpr>,
    },
    Slice {
        value: Box<ValueExpr>,
        start: Option<Box<ValueExpr>>,
        stop: Option<Box<ValueExpr>>,
        stride: Option<Box<ValueExpr>>,
    },
    Add {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Subtract {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Multiply {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Divide {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Remainder {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    FloorDivide {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Power {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChildReduction {
    source: String,
    target: String,
    reducer: Reducer,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Route {
    mode: RouteMode,
    branches: Vec<RouteBranch>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RouteMode {
    First,
    Broadcast,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RouteBranch {
    #[serde(default)]
    when: Option<Condition>,
    target: RouteTarget,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RouteTarget {
    Node { id: String },
    End {},
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Condition {
    Custom {
        condition: String,
        revision: String,
        #[serde(
            default,
            deserialize_with = "SchemaText::present_string",
            skip_serializing_if = "Option::is_none"
        )]
        name: Option<String>,
    },
    Constant {
        value: bool,
    },
    Exists {
        path: String,
    },
    Equal {
        left: ValueExpr,
        right: ValueExpr,
    },
    NotEqual {
        left: ValueExpr,
        right: ValueExpr,
    },
    LessThan {
        left: ValueExpr,
        right: ValueExpr,
    },
    LessThanOrEqual {
        left: ValueExpr,
        right: ValueExpr,
    },
    GreaterThan {
        left: ValueExpr,
        right: ValueExpr,
    },
    GreaterThanOrEqual {
        left: ValueExpr,
        right: ValueExpr,
    },
    In {
        left: ValueExpr,
        right: ValueExpr,
    },
    NotIn {
        left: ValueExpr,
        right: ValueExpr,
    },
    Contains {
        left: ValueExpr,
        right: ValueExpr,
    },
    Is {
        left: ValueExpr,
        right: ValueExpr,
    },
    IsNot {
        left: ValueExpr,
        right: ValueExpr,
    },
    Compare {
        values: Vec<ValueExpr>,
        operators: Vec<Comparison>,
    },
    Not {
        condition: Box<Condition>,
    },
    All {
        conditions: Vec<Condition>,
    },
    Any {
        conditions: Vec<Condition>,
    },
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Comparison {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    NotIn,
    Contains,
    Is,
    IsNot,
}

struct StrictValue {
    value: Value,
    depth: usize,
}

struct StrictValueVisitor;

pub(crate) struct SchemaText;

#[derive(Clone, Copy, Debug)]
pub(crate) struct JsonBoundary;

impl CompiledDefinition {
    pub(crate) fn parse(source: &str) -> Result<Self, AppError> {
        if source.is_empty() || source.len() > MAX_SOURCE_BYTES {
            return Err(AppError::InvalidInput(
                "workflow definition has an invalid encoded size".to_owned(),
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_str(source);
        let StrictValue { value, .. } =
            StrictValue::deserialize(&mut deserializer).map_err(|_| {
                AppError::InvalidInput("workflow definition is invalid JSON".to_owned())
            })?;
        deserializer.end().map_err(|_| {
            AppError::InvalidInput("workflow definition has trailing data".to_owned())
        })?;
        let mut document: DefinitionDocument = serde_json::from_value(value).map_err(|_| {
            AppError::InvalidInput("workflow definition does not match the v2 schema".to_owned())
        })?;
        // No outgoing transitions is ordinary graph exhaustion. Normalize the
        // omitted spelling once so execution retains its total route lookup.
        for node in &document.nodes {
            document
                .routes
                .entry(node.id.clone())
                .or_insert_with(|| Route {
                    mode: RouteMode::First,
                    branches: Vec::new(),
                });
        }
        document.validate()?;
        document.required.sort();
        let node_index = document
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.id.clone(), index))
            .collect();
        let canonical = serde_json::to_string(&document).map_err(|_| {
            AppError::Integration("workflow definition canonicalization failed".to_owned())
        })?;
        let fingerprint = canonical;
        Ok(Self {
            document,
            fingerprint,
            node_index,
        })
    }

    pub(crate) fn id(&self) -> &str {
        &self.document.id
    }

    pub(crate) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub(crate) fn revision(&self) -> &str {
        &self.document.revision
    }

    pub(crate) fn validate_bindings<X: ActionBackend, C: ConditionBackend>(
        &self,
        actions: &[X],
        conditions: &[C],
    ) -> Result<(), AppError> {
        let mut resolved = true;
        for node in &self.document.nodes {
            if matches!(&node.step, Step::Action { .. })
                && node.step.action_binding(actions).is_none()
            {
                return Err(AppError::InvalidInput(
                    "workflow action key and revision are not configured".into(),
                ));
            }
            resolved &= node
                .step
                .visit_reads(&self.document, conditions, &mut |_, _| {});
        }
        for route in self.document.routes.values() {
            for condition in route
                .branches
                .iter()
                .filter_map(|branch| branch.when.as_ref())
            {
                resolved &= condition.visit_reads(conditions, &mut |_, _| {});
            }
        }
        if resolved {
            Ok(())
        } else {
            Err(AppError::InvalidInput(
                "workflow condition key and revision are not configured".into(),
            ))
        }
    }

    pub(crate) fn entry(&self) -> &str {
        &self.document.entry
    }

    pub(crate) fn node(&self, id: &str) -> Option<&Node> {
        self.node_index
            .get(id)
            .map(|index| &self.document.nodes[*index])
    }

    pub(crate) fn route(&self, id: &str) -> Option<&Route> {
        self.document.routes.get(id)
    }

    pub(crate) fn reducer(&self, key: &str) -> Option<Reducer> {
        self.document.reducers.get(key).copied()
    }

    pub(crate) fn can_reach(&self, source: &str, target: &str) -> bool {
        let mut reached = BTreeSet::new();
        let mut pending = vec![source];
        while let Some(source) = pending.pop() {
            if source == target {
                return true;
            }
            if reached.len() == MAX_NODES || !reached.insert(source) {
                continue;
            }
            pending.extend(self.route(source).into_iter().flat_map(Route::targets));
        }
        false
    }

    pub(crate) fn join_visit(&self, node: &str, lineage: &str) -> usize {
        let visit = lineage.split('/').filter(|&part| part == node).count();
        match self.node(node).map(Node::step) {
            Some(Step::JoinAll {} | Step::JoinFirst {}) => visit,
            _ => 0,
        }
    }

    pub(crate) fn is_join_first(&self, node: &str) -> bool {
        matches!(self.node(node).map(Node::step), Some(Step::JoinFirst {}))
    }

    pub(crate) fn has_dynamic_children(&self) -> bool {
        self.document
            .nodes
            .iter()
            .any(|node| node.step.has_dynamic_children())
    }

    pub(crate) fn child_targets(&self) -> impl DoubleEndedIterator<Item = (&str, Option<&str>)> {
        self.document
            .nodes
            .iter()
            .flat_map(|node| node.step.child_targets())
    }

    pub(crate) fn validate_child_relationships<'a>(
        &'a self,
        definitions: &'a BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
        selected: &BTreeMap<WorkflowId, String>,
    ) -> Result<(), AppError> {
        let mut pending = vec![self];
        let mut incoming = BTreeMap::new();
        let mut edges = BTreeMap::new();
        while let Some(definition) = pending.pop() {
            let identity = (definition.id(), definition.revision());
            if edges.contains_key(&identity) {
                continue;
            }
            let mut children = Vec::new();
            for node in &definition.document.nodes {
                node.step.validate_child_relationship(
                    definition,
                    definitions,
                    selected,
                    &mut children,
                )?;
            }
            let targets = children
                .iter()
                .copied()
                .map(|child| (child.id(), child.revision()))
                .collect::<BTreeSet<_>>();
            incoming.entry(identity).or_insert(0usize);
            for target in &targets {
                *incoming.entry(*target).or_insert(0) += 1;
            }
            edges.insert(identity, targets);
            pending.extend(children);
        }
        let mut ready = incoming
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(identity, _)| *identity)
            .collect::<BTreeSet<_>>();
        let mut visited = 0;
        while let Some(identity) = ready.pop_first() {
            visited += 1;
            for child in &edges[&identity] {
                let degree = incoming.get_mut(child).ok_or_else(|| {
                    AppError::InvalidInput("workflow child definition is missing".into())
                })?;
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(*child);
                }
            }
        }
        if visited != incoming.len() {
            return Err(AppError::InvalidInput(
                "workflow child closure must be acyclic".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn agent_keys(&self) -> BTreeSet<&str> {
        self.document
            .nodes
            .iter()
            .filter_map(|node| match &node.step {
                Step::Agent { agent, .. } => Some(agent.as_str()),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn validate_input(
        &self,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, AppError> {
        if input.len() > MAX_STATE_KEYS {
            return Err(AppError::InvalidInput(
                "workflow input contains too many keys".to_owned(),
            ));
        }
        let mut state = self.document.state.clone();
        for (key, value) in input {
            SchemaText::state_key(key)?;
            if !self.document.reducers.contains_key(key) {
                return Err(AppError::InvalidInput(
                    "workflow input writes an undeclared state key".to_owned(),
                ));
            }
            JsonBoundary::validate(value)?;
            state.insert(key.clone(), value.clone());
        }
        self.validate_state(&state)?;
        Ok(state)
    }

    pub(crate) fn validate_state(&self, state: &Map<String, Value>) -> Result<(), AppError> {
        if state.len() > MAX_STATE_KEYS {
            return Err(AppError::InvalidInput(
                "workflow state contains too many keys".to_owned(),
            ));
        }
        for key in &self.document.required {
            if !state.contains_key(key) {
                return Err(AppError::InvalidInput(
                    "workflow state is missing a required key".to_owned(),
                ));
            }
        }
        JsonBoundary::validate_map(state)
    }
}

impl DefinitionDocument {
    fn validate(&self) -> Result<(), AppError> {
        if self.schema != SCHEMA {
            return Err(AppError::InvalidInput(
                "workflow definition schema must be app-kit.workflow/v2".to_owned(),
            ));
        }
        SchemaText::workflow_id(&self.id)?;
        SchemaText::revision(&self.revision)?;
        SchemaText::node_or_state("workflow entry", &self.entry)?;
        if self.nodes.is_empty() || self.nodes.len() > MAX_NODES {
            return Err(AppError::InvalidInput(
                "workflow definition has an invalid node count".to_owned(),
            ));
        }
        if self.state.len() > MAX_STATE_KEYS
            || self.required.len() > MAX_STATE_KEYS
            || self.reducers.len() > MAX_STATE_KEYS
        {
            return Err(AppError::InvalidInput(
                "workflow definition has too many state declarations".to_owned(),
            ));
        }
        JsonBoundary::validate_map(&self.state)?;

        let mut required = BTreeSet::new();
        for key in &self.required {
            SchemaText::state_key(key)?;
            if !required.insert(key) {
                return Err(AppError::InvalidInput(
                    "workflow required state keys must be unique".to_owned(),
                ));
            }
            if !self.state.contains_key(key) {
                return Err(AppError::InvalidInput(
                    "workflow initial state is missing a required key".to_owned(),
                ));
            }
        }
        for key in self.state.keys() {
            SchemaText::state_key(key)?;
            if !self.reducers.contains_key(key) {
                return Err(AppError::InvalidInput(
                    "every workflow state key requires a reducer".to_owned(),
                ));
            }
        }
        for key in self.reducers.keys() {
            SchemaText::state_key(key)?;
            if !self.state.contains_key(key) {
                return Err(AppError::InvalidInput(
                    "workflow reducer targets an undeclared state key".to_owned(),
                ));
            }
        }

        let mut node_ids = BTreeSet::new();
        for node in &self.nodes {
            node.validate(&self.state)?;
            if !node_ids.insert(node.id.as_str()) {
                return Err(AppError::InvalidInput(
                    "workflow node ids must be unique".to_owned(),
                ));
            }
        }
        if !node_ids.contains(self.entry.as_str()) {
            return Err(AppError::InvalidInput(
                "workflow entry does not name a node".to_owned(),
            ));
        }
        if self.routes.len() != self.nodes.len()
            || self.routes.keys().any(|id| !node_ids.contains(id.as_str()))
        {
            return Err(AppError::InvalidInput(
                "workflow routes must exactly cover its nodes".to_owned(),
            ));
        }

        for route in self.routes.values() {
            route.validate(&node_ids)?;
        }
        self.validate_reachability()?;
        Ok(())
    }

    fn validate_reachability(&self) -> Result<(), AppError> {
        let mut reached = BTreeSet::from([self.entry.as_str()]);
        let mut pending = vec![self.entry.as_str()];
        while let Some(source) = pending.pop() {
            if let Some(route) = self.routes.get(source) {
                pending.extend(route.targets().filter(|target| reached.insert(*target)));
            }
        }
        (reached.len() == self.nodes.len())
            .then_some(())
            .ok_or_else(|| {
                AppError::InvalidInput("workflow contains an unreachable node".to_owned())
            })
    }
}

impl Node {
    fn validate(&self, state: &Map<String, Value>) -> Result<(), AppError> {
        SchemaText::node_or_state("workflow node id", &self.id)?;
        if self
            .name
            .iter()
            .chain(self.description.iter())
            .any(|text| text.len() > MAX_TEXT_BYTES)
            || self.tags.as_ref().is_some_and(|tags| {
                tags.len() > MAX_COLLECTION_ITEMS
                    || tags.iter().any(|tag| tag.len() > MAX_TEXT_BYTES)
            })
        {
            return Err(AppError::InvalidInput(
                "workflow node metadata exceeds its bound".to_owned(),
            ));
        }
        if self
            .deadline_ms
            .is_some_and(|value| value == 0 || value > MAX_DURATION_MS)
        {
            return Err(AppError::InvalidInput(
                "workflow node deadline is outside the private bound".to_owned(),
            ));
        }
        if let Some(retry) = &self.retry {
            if !matches!(&self.step, Step::Agent { .. }) {
                return Err(AppError::InvalidInput(
                    "retry policy is only supported for agent nodes".to_owned(),
                ));
            }
            retry.validate()?;
        }
        self.step.validate(state)
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn step(&self) -> &Step {
        &self.step
    }

    pub(crate) fn deadline_millis(&self) -> u64 {
        self.deadline_ms.unwrap_or(MAX_DURATION_MS)
    }

    pub(crate) fn retry_delay_millis(&self, attempt: u32, elapsed_millis: u64) -> Option<u64> {
        self.retry
            .as_ref()
            .and_then(|retry| retry.delay_millis(attempt, elapsed_millis))
    }

    pub(crate) fn retry_permits_attempt(&self, attempt: u32, elapsed_millis: u64) -> bool {
        self.retry
            .as_ref()
            .is_some_and(|retry| retry.permits_attempt(attempt, elapsed_millis))
    }
}

impl Step {
    pub(crate) fn action_binding<'a, X: ActionBackend>(&self, actions: &'a [X]) -> Option<&'a X> {
        let Self::Action {
            action, revision, ..
        } = self
        else {
            return None;
        };
        actions.iter().find(|binding| {
            let declaration = binding.declaration();
            declaration.key == *action && declaration.revision == *revision
        })
    }

    fn validate(&self, state: &Map<String, Value>) -> Result<(), AppError> {
        match self {
            Self::Action {
                action, revision, ..
            } => {
                SchemaText::node_or_state("workflow action key", action)?;
                SchemaText::revision(revision)?;
            }
            Self::Agent {
                agent,
                prompt,
                structured_schema,
                text_target,
                structured_target,
                ..
            } => {
                SchemaText::workflow_id(agent)?;
                PromptPart::validate_all(prompt)?;
                if let Some(schema) = structured_schema {
                    JsonBoundary::validate(schema)?;
                    StructuredOutputRequest::new(schema.clone()).map_err(|_| {
                        AppError::InvalidInput(
                            "agent structured output schema is invalid".to_owned(),
                        )
                    })?;
                }
                for target in [text_target, structured_target].into_iter().flatten() {
                    SchemaText::state_target(target, state)?;
                }
                if structured_target.is_some() && structured_schema.is_none() {
                    return Err(AppError::InvalidInput("invalid agent output".to_owned()));
                }
            }
            Self::State { mutations } => {
                if mutations.len() > MAX_COLLECTION_ITEMS {
                    return Err(AppError::InvalidInput(
                        "state step contains too many mutations".to_owned(),
                    ));
                }
                for mutation in mutations {
                    mutation.validate()?;
                }
            }
            Self::Input {
                message,
                schema,
                target,
            } => {
                PromptPart::validate_all(message)?;
                if let Some(schema) = schema {
                    JsonBoundary::validate(schema)?;
                    JsonBoundary::validate_schema(schema)?;
                }
                if let Some(target) = target {
                    SchemaText::state_target(target, state)?;
                }
            }
            Self::Child {
                workflow,
                revision,
                select,
                input,
                inputs,
                reductions,
                task_key,
                initializer,
                ..
            } => {
                ChildSelection::new(workflow.as_deref(), revision.as_deref(), select.as_deref())?
                    .validate()?;
                if let Some(task_key) = task_key {
                    task_key.validate()?;
                }
                if let Some(initializer) = initializer {
                    initializer.validate()?;
                }
                Self::validate_child_input(input)?;
                Self::validate_child_input(inputs)?;
                ChildReduction::validate_all(reductions, state, false)?;
            }
            Self::Map(MapDeclaration::Items {
                workflow,
                revision,
                select,
                items,
                input_key,
                index_key,
                input,
                inputs,
                max_parallel,
                reductions,
                task_key,
                initializer,
                ..
            }) => {
                ChildSelection::new(workflow.as_deref(), revision.as_deref(), select.as_deref())?
                    .validate()?;
                SchemaText::state_key(input_key)?;
                if let Some(index_key) = index_key {
                    SchemaText::state_key(index_key)?;
                }
                if index_key.as_deref() == Some(input_key.as_str())
                    || input.contains_key(input_key)
                    || index_key
                        .as_ref()
                        .is_some_and(|index_key| input.contains_key(index_key))
                {
                    return Err(AppError::InvalidInput(
                        "map input bindings must use distinct keys".to_owned(),
                    ));
                }
                items.validate()?;
                if let Some(task_key) = task_key {
                    task_key.validate()?;
                }
                if let Some(initializer) = initializer {
                    initializer.validate()?;
                }
                Self::validate_child_input(input)?;
                Self::validate_child_input(inputs)?;
                if *max_parallel == 0 || *max_parallel > MAX_COLLECTION_ITEMS {
                    return Err(AppError::InvalidInput(
                        "map max_parallel is outside the private bound".to_owned(),
                    ));
                }
                ChildReduction::validate_all(reductions, state, true)?;
            }
            Self::Map(MapDeclaration::Tasks {
                tasks,
                max_parallel,
                reductions,
            }) => {
                tasks.validate()?;
                if *max_parallel == 0 || *max_parallel > MAX_COLLECTION_ITEMS {
                    return Err(AppError::InvalidInput(
                        "map max_parallel is outside the private bound".into(),
                    ));
                }
                ChildReduction::validate_all(reductions, state, true)?;
            }
            Self::Pipeline {
                children,
                reductions,
            } => {
                if children.is_empty() || children.len() > MAX_COLLECTION_ITEMS {
                    return Err(AppError::InvalidInput(
                        "pipeline child count is outside the private bound".to_owned(),
                    ));
                }
                for child in children {
                    child.selection()?.validate()?;
                    if let Some(task_key) = &child.task_key {
                        task_key.validate()?;
                    }
                    if let Some(initializer) = &child.initializer {
                        initializer.validate()?;
                    }
                    Self::validate_child_input(&child.input)?;
                    Self::validate_child_input(&child.inputs)?;
                }
                ChildReduction::validate_all(reductions, state, false)?;
            }
            Self::JoinAll {} | Self::JoinFirst {} => {}
            Self::Sleep { message, delay_ms } => {
                SchemaText::text("sleep message", message)?;
                if *delay_ms == 0 || *delay_ms > MAX_DURATION_MS {
                    return Err(AppError::InvalidInput(
                        "sleep delay is outside the private bound".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_child_relationship<'a>(
        &self,
        parent: &CompiledDefinition,
        definitions: &'a BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
        selected: &BTreeMap<WorkflowId, String>,
        children: &mut Vec<&'a CompiledDefinition>,
    ) -> Result<(), AppError> {
        let Some((_, reductions)) = self.child_plan() else {
            return Ok(());
        };
        let single = match self {
            Self::Child {
                workflow,
                revision,
                input,
                ..
            } => workflow
                .as_deref()
                .map(|workflow| (workflow, revision.as_deref(), input, [None, None])),
            Self::Map(MapDeclaration::Items {
                workflow,
                revision,
                input_key,
                index_key,
                input,
                ..
            }) => workflow.as_deref().map(|workflow| {
                (
                    workflow,
                    revision.as_deref(),
                    input,
                    [Some(input_key.as_str()), index_key.as_deref()],
                )
            }),
            _ => None,
        };
        let pipeline = match self {
            Self::Pipeline { children, .. } => children.as_slice(),
            _ => &[],
        };
        for (workflow, revision, input, generated_keys) in
            single
                .into_iter()
                .chain(pipeline.iter().filter_map(|child| {
                    child.workflow.as_deref().map(|workflow| {
                        (
                            workflow,
                            child.revision.as_deref(),
                            &child.input,
                            [None, None],
                        )
                    })
                }))
        {
            let child_id = WorkflowId::from_str(workflow)?;
            let revision = revision.or_else(|| selected.get(&child_id).map(String::as_str));
            let child = definitions
                .get(&child_id)
                .and_then(|definitions| {
                    definitions
                        .iter()
                        .find(|definition| Some(definition.revision()) == revision)
                })
                .ok_or_else(|| {
                    AppError::InvalidInput("workflow child definition is missing".to_owned())
                })?;
            for key in input
                .keys()
                .map(String::as_str)
                .chain(generated_keys.into_iter().flatten())
            {
                SchemaText::state_target(key, &child.document.state)?;
            }
            for reduction in reductions {
                reduction.validate_relationship(parent, child)?;
            }
            children.push(child.as_ref());
        }
        Ok(())
    }

    fn validate_child_input(input: &BTreeMap<String, ValueExpr>) -> Result<(), AppError> {
        if input.len() > MAX_STATE_KEYS {
            return Err(AppError::InvalidInput(
                "child input contains too many bindings".to_owned(),
            ));
        }
        for (key, expression) in input {
            SchemaText::state_key(key)?;
            expression.validate()?;
        }
        Ok(())
    }

    fn child_targets(&self) -> impl DoubleEndedIterator<Item = (&str, Option<&str>)> {
        let single = match self {
            Self::Child {
                workflow, revision, ..
            }
            | Self::Map(MapDeclaration::Items {
                workflow, revision, ..
            }) => workflow
                .as_deref()
                .map(|workflow| (workflow, revision.as_deref())),
            _ => None,
        };
        let children = match self {
            Self::Pipeline { children, .. } => children.as_slice(),
            _ => &[],
        };
        single
            .into_iter()
            .chain(children.iter().filter_map(|child| {
                child
                    .workflow
                    .as_deref()
                    .map(|workflow| (workflow, child.revision.as_deref()))
            }))
    }

    pub(crate) fn child_selection(
        &self,
        index: usize,
    ) -> Result<(ChildSelection<'_>, ChildHistory), AppError> {
        match self {
            Self::Child {
                workflow,
                revision,
                select,
                history,
                ..
            } if index == 0 => Ok((
                ChildSelection::new(workflow.as_deref(), revision.as_deref(), select.as_deref())?,
                *history,
            )),
            Self::Map(MapDeclaration::Items {
                workflow,
                revision,
                select,
                history,
                ..
            }) => Ok((
                ChildSelection::new(workflow.as_deref(), revision.as_deref(), select.as_deref())?,
                *history,
            )),
            Self::Pipeline { children, .. } => children
                .get(index)
                .ok_or_else(|| AppError::Integration("child declaration index is missing".into()))
                .and_then(|child| Ok((child.selection()?, child.history))),
            _ => Err(AppError::Integration(
                "child declaration is unavailable".into(),
            )),
        }
    }

    fn has_dynamic_children(&self) -> bool {
        match self {
            Self::Child { select, .. } | Self::Map(MapDeclaration::Items { select, .. }) => {
                select.is_some()
            }
            Self::Map(MapDeclaration::Tasks { .. }) => true,
            Self::Pipeline { children, .. } => children.iter().any(|child| child.select.is_some()),
            _ => false,
        }
    }

    pub(crate) fn validate_child_binding(
        &self,
        parent: &CompiledDefinition,
        child: &CompiledDefinition,
        input: &Map<String, Value>,
    ) -> Result<(), AppError> {
        for key in input.keys() {
            SchemaText::state_target(key, &child.document.state)?;
        }
        let (_, reductions) = self
            .child_plan()
            .ok_or_else(|| AppError::Integration("child declaration is unavailable".into()))?;
        for reduction in reductions {
            reduction.validate_relationship(parent, child)?;
        }
        Ok(())
    }

    pub(crate) fn child_plan(&self) -> Option<(usize, &[ChildReduction])> {
        match self {
            Self::Child { reductions, .. } | Self::Pipeline { reductions, .. } => {
                Some((1, reductions))
            }
            Self::Map(
                MapDeclaration::Items {
                    max_parallel,
                    reductions,
                    ..
                }
                | MapDeclaration::Tasks {
                    max_parallel,
                    reductions,
                    ..
                },
            ) => Some((*max_parallel, reductions)),
            _ => None,
        }
    }

    pub(crate) fn validate_mode(&self, mode: ActionMode) -> Result<(), AppError> {
        if matches!(self, Self::Pipeline { .. }) && mode != ActionMode::Synchronous {
            return Err(AppError::InvalidInput(
                "pipeline requires synchronous execution".into(),
            ));
        }
        Ok(())
    }
}

impl RetryPolicy {
    fn validate(&self) -> Result<(), AppError> {
        let (attempts, elapsed, first, maximum, multiplier) = match self {
            Self::Never {} => return Ok(()),
            Self::Immediate { max_attempts } => (*max_attempts, None, None, None, None),
            Self::Fixed {
                max_attempts,
                max_elapsed_ms,
                delay_ms,
            } => (
                *max_attempts,
                Some(*max_elapsed_ms),
                Some(*delay_ms),
                Some(*delay_ms),
                None,
            ),
            Self::Exponential {
                max_attempts,
                max_elapsed_ms,
                initial_ms,
                maximum_ms,
                multiplier,
            } => (
                *max_attempts,
                Some(*max_elapsed_ms),
                Some(*initial_ms),
                Some(*maximum_ms),
                Some(*multiplier),
            ),
        };
        if attempts == 0
            || attempts > MAX_ATTEMPTS
            || elapsed.is_some_and(|value| value == 0 || value > MAX_DURATION_MS)
            || first.is_some_and(|value| value == 0 || value > MAX_DURATION_MS)
            || maximum.is_some_and(|value| value == 0 || value > MAX_DURATION_MS)
            || first
                .zip(maximum)
                .is_some_and(|(first, maximum)| first > maximum)
            || multiplier.is_some_and(|value| value < 2)
        {
            return Err(AppError::InvalidInput(
                "workflow retry policy is outside the private bound".to_owned(),
            ));
        }
        Ok(())
    }

    fn permits_attempt(&self, attempt: u32, elapsed_millis: u64) -> bool {
        let (maximum_attempts, maximum_elapsed) = match self {
            Self::Never {} => return false,
            Self::Immediate { max_attempts } => (*max_attempts, None),
            Self::Fixed {
                max_attempts,
                max_elapsed_ms,
                ..
            }
            | Self::Exponential {
                max_attempts,
                max_elapsed_ms,
                ..
            } => (*max_attempts, Some(*max_elapsed_ms)),
        };
        attempt > 0
            && attempt <= maximum_attempts
            && maximum_elapsed.is_none_or(|maximum| elapsed_millis <= maximum)
    }

    fn delay_millis(&self, attempt: u32, elapsed_millis: u64) -> Option<u64> {
        let delay = match self {
            Self::Never {} => return None,
            Self::Immediate { .. } => 0,
            Self::Fixed { delay_ms, .. } => *delay_ms,
            Self::Exponential {
                initial_ms,
                maximum_ms,
                multiplier,
                ..
            } => {
                let exponent = attempt.saturating_sub(1);
                let factor = u64::from(*multiplier).saturating_pow(exponent);
                initial_ms.saturating_mul(factor).min(*maximum_ms)
            }
        };
        self.permits_attempt(attempt.checked_add(1)?, elapsed_millis.checked_add(delay)?)
            .then_some(delay)
    }
}

impl PromptPart {
    fn validate_all(parts: &[Self]) -> Result<(), AppError> {
        if parts.is_empty() || parts.len() > MAX_COLLECTION_ITEMS {
            return Err(AppError::InvalidInput(
                "workflow prompt has an invalid part count".to_owned(),
            ));
        }
        for part in parts {
            match part {
                Self::Text { text } => SchemaText::text("prompt text", text)?,
                Self::State { path } => {
                    SchemaText::state_path(path)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn render_all(
        parts: &[Self],
        state: &Map<String, Value>,
    ) -> Result<String, AppError> {
        let mut rendered = String::new();
        for part in parts {
            let fragment = match part {
                Self::Text { text } => Cow::Borrowed(text.as_str()),
                Self::State { path } => match JsonBoundary::at_path(state, path) {
                    Some(Value::String(value)) => Cow::Borrowed(value.as_str()),
                    Some(value) => Cow::Owned(value.to_string()),
                    None => {
                        return Err(AppError::InvalidInput(
                            "prompt references missing workflow state".to_owned(),
                        ));
                    }
                },
            };
            if rendered
                .len()
                .checked_add(fragment.len())
                .is_none_or(|length| length > MAX_RUNTIME_BYTES)
            {
                return Err(AppError::InvalidInput(
                    "rendered workflow prompt exceeds the private byte bound".to_owned(),
                ));
            }
            rendered.push_str(&fragment);
        }
        if rendered.is_empty() {
            return Err(AppError::InvalidInput(
                "rendered workflow prompt is empty".to_owned(),
            ));
        }
        Ok(rendered)
    }
}

impl StateMutation {
    fn validate(&self) -> Result<(), AppError> {
        match self {
            Self::Assign { target, value }
            | Self::Append { target, value }
            | Self::Increment { target, value }
            | Self::Extend {
                target,
                values: value,
            } => {
                SchemaText::state_key(target)?;
                value.validate()
            }
            Self::Remove { target } => SchemaText::state_key(target),
            Self::Merge { state: value } => value.validate(),
            Self::Wipe { keep: keys } | Self::Subset { keys } => {
                if keys.len() > MAX_COLLECTION_ITEMS {
                    return Err(AppError::InvalidInput(
                        "state key selection exceeds its item bound".to_owned(),
                    ));
                }
                for key in keys {
                    SchemaText::state_key(key)?;
                }
                Ok(())
            }
        }
    }
}

impl ValueExpr {
    fn validate(&self) -> Result<(), AppError> {
        match self {
            Self::Literal { value } => JsonBoundary::validate(value),
            Self::State { path } => SchemaText::state_path(path),
            Self::Length { value }
            | Self::Positive { value }
            | Self::Negative { value }
            | Self::Not { value }
            | Self::Attribute { value, .. } => value.validate(),
            Self::And { values } | Self::Or { values } => {
                if values.len() < 2 {
                    return Err(AppError::InvalidInput(
                        "boolean expression requires at least two operands".to_owned(),
                    ));
                }
                for value in values {
                    value.validate()?;
                }
                Ok(())
            }
            Self::Conditional {
                when,
                then,
                otherwise,
            } => {
                when.validate()?;
                then.validate()?;
                otherwise.validate()
            }
            Self::Index { value, index } => {
                value.validate()?;
                index.validate()
            }
            Self::Slice {
                value,
                start,
                stop,
                stride,
            } => {
                value.validate()?;
                for bound in [start, stop, stride].into_iter().flatten() {
                    bound.validate()?;
                }
                Ok(())
            }
            Self::Add { left, right }
            | Self::Subtract { left, right }
            | Self::Multiply { left, right }
            | Self::Divide { left, right }
            | Self::Remainder { left, right }
            | Self::FloorDivide { left, right }
            | Self::Power { left, right } => {
                left.validate()?;
                right.validate()
            }
        }
    }
}

impl ChildReduction {
    fn validate_all(
        reductions: &[Self],
        state: &Map<String, Value>,
        map: bool,
    ) -> Result<(), AppError> {
        if reductions.len() > MAX_STATE_KEYS {
            return Err(AppError::InvalidInput(
                "child reduction list exceeds the private bound".to_owned(),
            ));
        }
        let mut targets = BTreeMap::<&str, (Reducer, BTreeSet<&str>)>::new();
        for reduction in reductions {
            SchemaText::state_path(&reduction.source)?;
            SchemaText::state_target(&reduction.target, state)?;
            if map && reduction.reducer == Reducer::RejectConcurrent {
                return Err(AppError::InvalidInput(
                    "map reduction cannot reject concurrent contributions".to_owned(),
                ));
            }
            let entry = targets
                .entry(reduction.target.as_str())
                .or_insert_with(|| (reduction.reducer, BTreeSet::new()));
            if entry.0 != reduction.reducer || !entry.1.insert(reduction.source.as_str()) {
                return Err(AppError::InvalidInput(
                    "child reductions duplicate or conflict at one target".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_relationship(
        &self,
        parent: &CompiledDefinition,
        child: &CompiledDefinition,
    ) -> Result<(), AppError> {
        SchemaText::state_read(&self.source, &child.document.state)?;
        if parent.reducer(&self.target) != Some(self.reducer) {
            return Err(AppError::InvalidInput(
                "child reduction must match the parent state reducer".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn source(&self) -> &str {
        &self.source
    }

    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    pub(crate) fn reducer(&self) -> Reducer {
        self.reducer
    }
}

impl Route {
    fn validate(&self, node_ids: &BTreeSet<&str>) -> Result<(), AppError> {
        if self.branches.len() > MAX_COLLECTION_ITEMS {
            return Err(AppError::InvalidInput(
                "workflow route has an invalid branch count".to_owned(),
            ));
        }
        if matches!(self.mode, RouteMode::First)
            && self
                .branches
                .iter()
                .filter(|branch| branch.when.is_none())
                .count()
                > 1
        {
            return Err(AppError::InvalidInput(
                "first route permits at most one default branch".to_owned(),
            ));
        }
        for branch in &self.branches {
            if let Some(condition) = &branch.when {
                condition.validate()?;
            }
            if let RouteTarget::Node { id } = &branch.target
                && !node_ids.contains(id.as_str())
            {
                return Err(AppError::InvalidInput(
                    "workflow route targets an unknown node".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn targets(&self) -> impl Iterator<Item = &str> {
        self.branches
            .iter()
            .filter_map(|branch| match &branch.target {
                RouteTarget::Node { id } => Some(id.as_str()),
                RouteTarget::End {} => None,
            })
    }

    pub(crate) fn mode(&self) -> RouteMode {
        self.mode
    }

    pub(crate) fn branches(&self) -> &[RouteBranch] {
        &self.branches
    }
}

impl RouteBranch {
    pub(crate) fn condition(&self) -> Option<&Condition> {
        self.when.as_ref()
    }

    pub(crate) fn target(&self) -> &RouteTarget {
        &self.target
    }
}

impl RouteTarget {
    pub(crate) fn node(&self) -> Option<&str> {
        match self {
            Self::Node { id } => Some(id),
            Self::End {} => None,
        }
    }
}

impl Condition {
    fn validate(&self) -> Result<(), AppError> {
        match self {
            Self::Custom {
                condition,
                revision,
                name,
            } => {
                SchemaText::node_or_state("condition binding key", condition)?;
                SchemaText::revision(revision)?;
                if name
                    .as_ref()
                    .is_some_and(|name| name.len() > MAX_TEXT_BYTES)
                {
                    return Err(AppError::InvalidInput(
                        "condition display name exceeds its byte bound".into(),
                    ));
                }
                Ok(())
            }
            Self::Constant { .. } => Ok(()),
            Self::Exists { path } => SchemaText::state_path(path),
            Self::Equal { left, right }
            | Self::NotEqual { left, right }
            | Self::LessThan { left, right }
            | Self::LessThanOrEqual { left, right }
            | Self::GreaterThan { left, right }
            | Self::GreaterThanOrEqual { left, right }
            | Self::In { left, right }
            | Self::NotIn { left, right }
            | Self::Contains { left, right }
            | Self::Is { left, right }
            | Self::IsNot { left, right } => {
                left.validate()?;
                right.validate()
            }
            Self::Compare { values, operators } => {
                if values.len() < 2
                    || values.len() > MAX_COLLECTION_ITEMS
                    || operators.len() != values.len() - 1
                {
                    return Err(AppError::InvalidInput(
                        "comparison chain has an invalid size".to_owned(),
                    ));
                }
                values.iter().try_for_each(|value| value.validate())
            }
            Self::Not { condition } => condition.validate(),
            Self::All { conditions } | Self::Any { conditions } => {
                if conditions.len() > MAX_COLLECTION_ITEMS {
                    return Err(AppError::InvalidInput(
                        "condition list has an invalid size".to_owned(),
                    ));
                }
                conditions
                    .iter()
                    .try_for_each(|condition| condition.validate())
            }
        }
    }
}

impl Step {
    // One traversal owns binding completeness and canonical read keys. Missing
    // bindings never short-circuit traversal: authored graph inspection still
    // includes later built-in reads while omitting unresolved declarations.
    fn visit_reads<C: ConditionBackend>(
        &self,
        definition: &DefinitionDocument,
        bindings: &[C],
        visit: &mut impl FnMut(&str, bool),
    ) -> bool {
        let mut resolved = true;
        match self {
            Self::Agent { prompt, .. }
            | Self::Input {
                message: prompt, ..
            } => {
                for part in prompt {
                    if let PromptPart::State { path } = part {
                        visit(JsonBoundary::path_key(path).as_ref(), true);
                    }
                }
            }
            Self::State { mutations } => {
                for mutation in mutations {
                    resolved &= mutation.visit_reads(definition, bindings, visit);
                }
            }
            Self::Child {
                input,
                inputs,
                select,
                task_key,
                initializer,
                ..
            } => {
                if let Some(select) = select {
                    resolved &= select.visit_reads(bindings, visit);
                }
                if let Some(task_key) = task_key {
                    resolved &= task_key.visit_reads(bindings, visit);
                }
                if let Some(initializer) = initializer {
                    resolved &= initializer.visit_reads(bindings, visit);
                }
                for value in input.values().chain(inputs.values()) {
                    resolved &= value.visit_reads(bindings, visit);
                }
            }
            Self::Map(MapDeclaration::Items {
                items,
                input,
                inputs,
                select,
                task_key,
                initializer,
                ..
            }) => {
                resolved &= items.visit_reads(bindings, visit);
                if let Some(select) = select {
                    resolved &= select.visit_reads(bindings, visit);
                }
                if let Some(task_key) = task_key {
                    resolved &= task_key.visit_reads(bindings, visit);
                }
                if let Some(initializer) = initializer {
                    resolved &= initializer.visit_reads(bindings, visit);
                }
                for value in input.values().chain(inputs.values()) {
                    resolved &= value.visit_reads(bindings, visit);
                }
            }
            Self::Map(MapDeclaration::Tasks { tasks, .. }) => {
                resolved &= tasks.visit_reads(bindings, visit);
            }
            Self::Pipeline { children, .. } => {
                for child in children {
                    if let Some(task_key) = &child.task_key {
                        resolved &= task_key.visit_reads(bindings, visit);
                    }
                    if let Some(initializer) = &child.initializer {
                        resolved &= initializer.visit_reads(bindings, visit);
                    }
                    if let Some(select) = &child.select {
                        resolved &= select.visit_reads(bindings, visit);
                    }
                    for value in child.input.values().chain(child.inputs.values()) {
                        resolved &= value.visit_reads(bindings, visit);
                    }
                }
            }
            Self::Action { .. } | Self::JoinAll {} | Self::JoinFirst {} | Self::Sleep { .. } => {}
        }
        resolved
    }
}

impl StateMutation {
    fn visit_reads<C: ConditionBackend>(
        &self,
        definition: &DefinitionDocument,
        bindings: &[C],
        visit: &mut impl FnMut(&str, bool),
    ) -> bool {
        match self {
            Self::Assign { value, .. } | Self::Merge { state: value } => {
                value.visit_reads(bindings, visit)
            }
            Self::Append { target, value }
            | Self::Increment { target, value }
            | Self::Extend {
                target,
                values: value,
            } => {
                visit(target, true);
                value.visit_reads(bindings, visit)
            }
            Self::Remove { .. } => true,
            Self::Wipe { .. } | Self::Subset { .. } => {
                for key in definition.state.keys() {
                    visit(key, true);
                }
                true
            }
        }
    }
}

impl ValueExpr {
    pub(crate) fn visit_reads<C: ConditionBackend>(
        &self,
        bindings: &[C],
        visit: &mut impl FnMut(&str, bool),
    ) -> bool {
        match self {
            Self::Literal { .. } => true,
            Self::State { path } => {
                visit(JsonBoundary::path_key(path).as_ref(), true);
                true
            }
            Self::Length { value }
            | Self::Positive { value }
            | Self::Negative { value }
            | Self::Not { value }
            | Self::Attribute { value, .. } => value.visit_reads(bindings, visit),
            Self::And { values } | Self::Or { values } => {
                let mut resolved = true;
                for value in values {
                    resolved &= value.visit_reads(bindings, visit);
                }
                resolved
            }
            Self::Conditional {
                when,
                then,
                otherwise,
            } => {
                let mut resolved = when.visit_reads(bindings, visit);
                resolved &= then.visit_reads(bindings, visit);
                resolved &= otherwise.visit_reads(bindings, visit);
                resolved
            }
            Self::Index { value, index } => {
                let resolved = value.visit_reads(bindings, visit);
                index.visit_reads(bindings, visit) & resolved
            }
            Self::Slice {
                value,
                start,
                stop,
                stride,
            } => {
                let mut resolved = value.visit_reads(bindings, visit);
                for bound in [start, stop, stride].into_iter().flatten() {
                    resolved &= bound.visit_reads(bindings, visit);
                }
                resolved
            }
            Self::Add { left, right }
            | Self::Subtract { left, right }
            | Self::Multiply { left, right }
            | Self::Divide { left, right }
            | Self::Remainder { left, right }
            | Self::FloorDivide { left, right }
            | Self::Power { left, right } => {
                let resolved = left.visit_reads(bindings, visit);
                right.visit_reads(bindings, visit) & resolved
            }
        }
    }
}

impl Condition {
    pub(crate) fn binding<'a, C: ConditionBackend>(&self, bindings: &'a [C]) -> Option<&'a C> {
        let Self::Custom {
            condition,
            revision,
            ..
        } = self
        else {
            return None;
        };
        bindings.iter().find(|binding| {
            let declaration = binding.declaration();
            declaration.key == *condition && declaration.revision == *revision
        })
    }

    pub(crate) fn visit_reads<C: ConditionBackend>(
        &self,
        bindings: &[C],
        visit: &mut impl FnMut(&str, bool),
    ) -> bool {
        match self {
            Self::Custom { .. } => {
                let Some(binding) = self.binding(bindings) else {
                    return false;
                };
                for key in &binding.declaration().reads {
                    // Caller declarations name literal top-level keys, not
                    // paths, even when a key contains '/' or '~'.
                    visit(key, true);
                }
                true
            }
            Self::Constant { .. } => true,
            Self::Exists { path } => {
                visit(JsonBoundary::path_key(path).as_ref(), false);
                true
            }
            Self::Equal { left, right }
            | Self::NotEqual { left, right }
            | Self::LessThan { left, right }
            | Self::LessThanOrEqual { left, right }
            | Self::GreaterThan { left, right }
            | Self::GreaterThanOrEqual { left, right }
            | Self::In { left, right }
            | Self::NotIn { left, right }
            | Self::Contains { left, right }
            | Self::Is { left, right }
            | Self::IsNot { left, right } => {
                let resolved = left.visit_reads(bindings, visit);
                right.visit_reads(bindings, visit) & resolved
            }
            Self::Not { condition } => condition.visit_reads(bindings, visit),
            Self::All { conditions } | Self::Any { conditions } => {
                let mut resolved = true;
                for condition in conditions {
                    resolved &= condition.visit_reads(bindings, visit);
                }
                resolved
            }
            Self::Compare { values, .. } => {
                let mut resolved = true;
                for value in values {
                    resolved &= value.visit_reads(bindings, visit);
                }
                resolved
            }
        }
    }
}

impl JsonBoundary {
    pub(crate) fn validate_inputs(values: &Map<String, Value>) -> Result<(), AppError> {
        Self::validate_map(values)?;
        if values.keys().any(|key| key.starts_with("__")) {
            return Err(AppError::InvalidInput(
                "runtime input uses the reserved namespace".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(value: &Value) -> Result<(), AppError> {
        Self::validate_size(value)?;
        Self::validate_structure(vec![(value, 1)], 0)
    }

    pub(crate) fn validate_map(values: &Map<String, Value>) -> Result<(), AppError> {
        Self::validate_size(values)?;
        let mut pending = Vec::with_capacity(values.len());
        for (key, value) in values {
            Self::validate_key(key)?;
            pending.push((value, 2));
        }
        Self::validate_structure(pending, values.len())
    }

    pub(crate) fn validate_size<T: Serialize>(value: &T) -> Result<(), AppError> {
        let encoded = serde_json::to_vec(value)
            .map_err(|_| AppError::InvalidInput("workflow JSON value is invalid".to_owned()))?;
        if encoded.len() > MAX_SOURCE_BYTES {
            return Err(AppError::InvalidInput(
                "workflow JSON value exceeds the private byte bound".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_structure(
        mut pending: Vec<(&Value, usize)>,
        mut items: usize,
    ) -> Result<(), AppError> {
        while let Some((value, depth)) = pending.pop() {
            if depth > MAX_DEPTH {
                return Err(AppError::InvalidInput(
                    "workflow JSON value exceeds the private depth bound".to_owned(),
                ));
            }
            match value {
                Value::Array(values) => {
                    items = items.saturating_add(values.len());
                    for value in values {
                        pending.push((value, depth + 1));
                    }
                }
                Value::Object(values) => {
                    items = items.saturating_add(values.len());
                    for (key, value) in values {
                        Self::validate_key(key)?;
                        pending.push((value, depth + 1));
                    }
                }
                _ => {}
            }
            if items > MAX_COLLECTION_ITEMS * MAX_DEPTH {
                return Err(AppError::InvalidInput(
                    "workflow JSON value exceeds the private item bound".to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_key(key: &str) -> Result<(), AppError> {
        if key.len() > 128 {
            return Err(AppError::InvalidInput(
                "workflow JSON object key is invalid".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn path_key(path: &str) -> Cow<'_, str> {
        Self::decode_segment(path.split('/').next().unwrap_or(path))
    }

    fn decode_segment(encoded: &str) -> Cow<'_, str> {
        if encoded.contains('~') {
            Cow::Owned(encoded.replace("~1", "/").replace("~0", "~"))
        } else {
            Cow::Borrowed(encoded)
        }
    }

    pub(crate) fn at_path<'a>(state: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
        let mut segments = path.split('/');
        let key = Self::decode_segment(segments.next()?);
        let mut current = state.get(key.as_ref())?;
        for encoded in segments {
            let segment = Self::decode_segment(encoded);
            current = match current {
                Value::Object(object) => object.get(segment.as_ref())?,
                Value::Array(array) => {
                    if segment.is_empty()
                        || segment.starts_with('0') && segment.len() > 1
                        || !segment.bytes().all(|byte| byte.is_ascii_digit())
                    {
                        return None;
                    }
                    array.get(segment.parse::<usize>().ok()?)?
                }
                _ => return None,
            };
        }
        Some(current)
    }

    pub(crate) fn validate_schema(schema: &Value) -> Result<(), AppError> {
        jsonschema::draft202012::meta::validate(schema).map_err(|_| {
            AppError::InvalidInput("input schema is not valid Draft 2020-12".to_owned())
        })?;
        jsonschema::draft202012::options()
            .with_retriever(Self)
            .build(schema)
            .map(|_| ())
            .map_err(|_| {
                AppError::InvalidInput("input schema must be a self-contained document".to_owned())
            })
    }

    pub(crate) fn validate_instance(schema: &Value, value: &Value) -> Result<(), AppError> {
        let validator = jsonschema::draft202012::options()
            .with_retriever(Self)
            .build(schema)
            .map_err(|_| {
                AppError::Integration("compiled input schema is unavailable".to_owned())
            })?;
        if validator.is_valid(value) {
            Ok(())
        } else {
            Err(AppError::InvalidInput(
                "input does not satisfy its JSON schema".to_owned(),
            ))
        }
    }
}

impl fmt::Display for JsonBoundary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("workflow schemas cannot retrieve external documents")
    }
}

impl std::error::Error for JsonBoundary {}

impl jsonschema::Retrieve for JsonBoundary {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(*self))
    }
}

impl SchemaText {
    fn present_tags<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<String>>, D::Error> {
        // Omission inherits binding metadata; an explicit array replaces it,
        // including []. Present null must not silently become inheritance.
        Vec::<String>::deserialize(deserializer).map(Some)
    }

    fn present_string<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        // Missing fields use serde's default; present null is never omission.
        // Each owning field applies its own string validation after decoding.
        String::deserialize(deserializer).map(Some)
    }

    fn workflow_id(value: &str) -> Result<(), AppError> {
        Self::node_or_state("workflow identity", value)
    }

    pub(crate) fn node_or_state(label: &str, value: &str) -> Result<(), AppError> {
        let valid = !value.is_empty()
            && value.len() <= 128
            && value.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || (index > 0 && matches!(byte, b'_' | b'-'))
            });
        if valid {
            Ok(())
        } else {
            Err(AppError::InvalidInput(format!("{label} is not canonical")))
        }
    }

    fn state_key(value: &str) -> Result<(), AppError> {
        JsonBoundary::validate_key(value)
    }

    fn state_target(value: &str, state: &Map<String, Value>) -> Result<(), AppError> {
        Self::state_key(value)?;
        state
            .contains_key(value)
            .then_some(())
            .ok_or_else(|| AppError::InvalidInput("workflow state target is undeclared".to_owned()))
    }

    fn state_path(value: &str) -> Result<(), AppError> {
        if value.len() > MAX_TEXT_BYTES {
            return Err(AppError::InvalidInput(
                "workflow state path exceeds its byte bound".to_owned(),
            ));
        }
        for token in value.split('/') {
            let bytes = token.as_bytes();
            let mut index = 0usize;
            while index < bytes.len() {
                if bytes[index] == b'~'
                    && (index + 1 >= bytes.len() || !matches!(bytes[index + 1], b'0' | b'1'))
                {
                    return Err(AppError::InvalidInput(
                        "workflow state path is not a canonical JSON pointer".to_owned(),
                    ));
                }
                index += if bytes[index] == b'~' { 2 } else { 1 };
            }
            Self::state_key(&JsonBoundary::decode_segment(token))?;
        }
        Ok(())
    }

    fn state_read(value: &str, state: &Map<String, Value>) -> Result<(), AppError> {
        Self::state_path(value)?;
        Self::state_target(&JsonBoundary::path_key(value), state)
    }

    pub(crate) fn revision(value: &str) -> Result<(), AppError> {
        if value.is_empty()
            || value.len() > 256
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            Err(AppError::InvalidInput(
                "workflow revision is invalid".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn text(label: &str, value: &str) -> Result<(), AppError> {
        if value.is_empty()
            || value.len() > MAX_TEXT_BYTES
            || value.chars().any(|character| character == '\0')
        {
            Err(AppError::InvalidInput(format!("{label} is invalid")))
        } else {
            Ok(())
        }
    }
}

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

impl<'de> Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue {
            value: Value::Bool(value),
            depth: 1,
        })
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(StrictValue {
            value: Value::Number(Number::from(value)),
            depth: 1,
        })
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(StrictValue {
            value: Value::Number(Number::from(value)),
            depth: 1,
        })
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Number::from_f64(value)
            .map(|number| StrictValue {
                value: Value::Number(number),
                depth: 1,
            })
            .ok_or_else(|| E::custom("non-finite JSON numbers are not supported"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(StrictValue {
            value: Value::String(value.to_owned()),
            depth: 1,
        })
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(StrictValue {
            value: Value::String(value),
            depth: 1,
        })
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue {
            value: Value::Null,
            depth: 1,
        })
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue {
            value: Value::Null,
            depth: 1,
        })
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        let mut depth = 1usize;
        while let Some(value) = sequence.next_element::<StrictValue>()? {
            if values.len() >= MAX_COLLECTION_ITEMS {
                return Err(serde::de::Error::custom(
                    "JSON array exceeds the private item bound",
                ));
            }
            depth = depth.max(value.depth.saturating_add(1));
            if depth > MAX_DEPTH {
                return Err(serde::de::Error::custom(
                    "JSON array exceeds the private depth bound",
                ));
            }
            values.push(value.value);
        }
        Ok(StrictValue {
            value: Value::Array(values),
            depth,
        })
    }

    fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        let mut depth = 1usize;
        while let Some(key) = entries.next_key::<String>()? {
            if values.len() >= MAX_COLLECTION_ITEMS {
                return Err(serde::de::Error::custom(
                    "JSON object exceeds the private item bound",
                ));
            }
            if values.contains_key(&key) {
                return Err(serde::de::Error::custom("duplicate JSON object key"));
            }
            let value = entries.next_value::<StrictValue>()?;
            depth = depth.max(value.depth.saturating_add(1));
            if depth > MAX_DEPTH {
                return Err(serde::de::Error::custom(
                    "JSON object exceeds the private depth bound",
                ));
            }
            values.insert(key, value.value);
        }
        Ok(StrictValue {
            value: Value::Object(values),
            depth,
        })
    }
}

impl<'a> ChildSelection<'a> {
    fn new(
        workflow: Option<&'a str>,
        revision: Option<&'a str>,
        select: Option<&'a ValueExpr>,
    ) -> Result<Self, AppError> {
        match (workflow, revision, select) {
            (Some(workflow), revision, None) => Ok(Self::Literal { workflow, revision }),
            (None, None, Some(select)) => Ok(Self::Dynamic(select)),
            _ => Err(AppError::InvalidInput(
                "child requires exactly one literal or dynamic selector".into(),
            )),
        }
    }

    fn validate(&self) -> Result<(), AppError> {
        match self {
            Self::Literal { workflow, revision } => {
                SchemaText::workflow_id(workflow)?;
                if let Some(revision) = revision {
                    SchemaText::revision(revision)?;
                }
                Ok(())
            }
            Self::Dynamic(select) => select.validate(),
        }
    }

    pub(crate) fn resolve<C: ConditionBackend>(
        &self,
        state: &crate::workflow::WorkflowState,
        conditions: &[C],
    ) -> Result<(WorkflowId, Option<String>), AppError> {
        match self {
            Self::Literal { workflow, revision } => {
                Ok((WorkflowId::from_str(workflow)?, revision.map(str::to_owned)))
            }
            Self::Dynamic(select) => {
                let Value::Object(mut fields) = select.evaluate(state, conditions)? else {
                    return Err(AppError::InvalidInput(
                        "child selector must produce an object".into(),
                    ));
                };
                let Some(Value::String(workflow)) = fields.remove("workflow") else {
                    return Err(AppError::InvalidInput(
                        "child selector requires a workflow key".into(),
                    ));
                };
                let revision = match fields.remove("revision") {
                    None => None,
                    Some(Value::String(revision)) => {
                        SchemaText::revision(&revision)?;
                        Some(revision)
                    }
                    Some(_) => {
                        return Err(AppError::InvalidInput(
                            "child selector revision must be a string".into(),
                        ));
                    }
                };
                if !fields.is_empty() {
                    return Err(AppError::InvalidInput(
                        "child selector contains unknown fields".into(),
                    ));
                }
                Ok((WorkflowId::from_str(&workflow)?, revision))
            }
        }
    }
}

impl PipelineChild {
    fn selection(&self) -> Result<ChildSelection<'_>, AppError> {
        ChildSelection::new(
            self.workflow.as_deref(),
            self.revision.as_deref(),
            self.select.as_deref(),
        )
    }
}

impl ValueExpr {
    fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Box<Self>>, D::Error> {
        Box::<Self>::deserialize(deserializer).map(Some)
    }
}

impl<V> ChildInitializer<V> {
    fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Box<Self>>, D::Error>
    where
        V: Deserialize<'de>,
    {
        Box::<Self>::deserialize(deserializer).map(Some)
    }

    fn present_value<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<V>, D::Error>
    where
        V: Deserialize<'de>,
    {
        V::deserialize(deserializer).map(Some)
    }

    fn checkpoint<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<CheckpointSelector, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if value.get("kind").and_then(Value::as_str) == Some("latest")
            && value
                .as_object()
                .is_some_and(|fields| fields.contains_key("value"))
        {
            return Err(serde::de::Error::custom(
                "latest initializer checkpoint rejects value",
            ));
        }
        serde_json::from_value(value).map_err(serde::de::Error::custom)
    }
}

impl ChildInitializer<ValueExpr> {
    fn validate(&self) -> Result<(), AppError> {
        self.source.validate()?;
        for value in self.occurrence.iter().chain(self.fallback.iter()) {
            value.validate()?;
        }
        Step::validate_child_input(&self.overrides)
    }

    pub(crate) fn resolve<C: ConditionBackend>(
        &self,
        state: &WorkflowState,
        conditions: &[C],
    ) -> Result<ChildInitializer<Value>, AppError> {
        let resolved = ChildInitializer {
            source: self.source.evaluate(state, conditions)?,
            occurrence: self
                .occurrence
                .as_ref()
                .map(|value| value.evaluate(state, conditions))
                .transpose()?,
            checkpoint: self.checkpoint.clone(),
            position: self.position.clone(),
            overrides: self
                .overrides
                .iter()
                .map(|(key, value)| {
                    value
                        .evaluate(state, conditions)
                        .map(|value| (key.clone(), value))
                })
                .collect::<Result<_, _>>()?,
            fallback: self
                .fallback
                .as_ref()
                .map(|value| value.evaluate(state, conditions))
                .transpose()?,
        };
        resolved.validate()?;
        Ok(resolved)
    }

    fn visit_reads<C: ConditionBackend>(
        &self,
        bindings: &[C],
        visit: &mut impl FnMut(&str, bool),
    ) -> bool {
        let mut resolved = self.source.visit_reads(bindings, visit);
        for value in self
            .occurrence
            .iter()
            .chain(self.fallback.iter())
            .chain(self.overrides.values())
        {
            resolved &= value.visit_reads(bindings, visit);
        }
        resolved
    }
}

impl ChildInitializer<Value> {
    pub(crate) fn selector(&self) -> Result<crate::workflow::RunSelector, AppError> {
        let id = self
            .source
            .as_str()
            .ok_or_else(|| {
                AppError::InvalidInput("initializer source must be a RunId string".into())
            })?
            .parse()?;
        let selector = match &self.occurrence {
            Some(Value::String(occurrence)) => crate::workflow::RunSelector::Exact {
                id,
                occurrence: occurrence.clone(),
            },
            None => crate::workflow::RunSelector::Latest(id),
            Some(_) => {
                return Err(AppError::InvalidInput(
                    "initializer occurrence must be a string".into(),
                ));
            }
        };
        selector.validate()?;
        Ok(selector)
    }

    pub(crate) fn validate(&self) -> Result<(), AppError> {
        self.selector()?;
        for (key, value) in &self.overrides {
            SchemaText::state_key(key)?;
            JsonBoundary::validate(value)?;
        }
        if let Some(value) = &self.fallback {
            if !value.is_object() {
                return Err(AppError::InvalidInput(
                    "initializer fallback must be an object".into(),
                ));
            }
            JsonBoundary::validate(value)?;
        }
        Ok(())
    }
}
