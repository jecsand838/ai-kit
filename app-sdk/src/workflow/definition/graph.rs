use std::collections::BTreeMap;

use serde_json::Map;

use crate::AppError;
use crate::workflow::definition::{
    Comparison, CompiledDefinition, Condition, DefinitionDocument, JsonBoundary,
    MAX_COLLECTION_ITEMS, MAX_NODES, MapDeclaration, RouteBranch, RouteTarget, SCHEMA, SchemaText,
    StateMutation, Step, ValueExpr,
};
use crate::workflow::{
    ActionBackend, ConditionBackend, GraphComposition, GraphDescription, GraphEdge, GraphNode,
};

impl CompiledDefinition {
    pub(crate) fn graph<X: ActionBackend, C: ConditionBackend>(
        &self,
        actions: &[X],
        conditions: &[C],
    ) -> GraphDescription {
        let nodes = self
            .document
            .nodes
            .iter()
            .map(|node| {
                let mut description = GraphNode {
                    id: node.id.clone(),
                    name: node.name.clone(),
                    description: node.description.clone(),
                    tags: node.tags.clone().unwrap_or_default(),
                    kind: node.step.kind().to_owned(),
                    reads: Vec::new(),
                    writes: Vec::new(),
                    required_inputs: Vec::new(),
                    optional_inputs: Vec::new(),
                    source: if matches!(&node.step, Step::Action { .. }) {
                        None
                    } else {
                        serde_json::to_string(node).ok()
                    },
                };
                if let Some(binding) = node.step.action_binding(actions) {
                    let declaration = binding.declaration();
                    description.reads = declaration.reads.clone();
                    description.writes = declaration.writes.clone();
                    description.required_inputs = declaration.required_inputs.clone();
                    description.optional_inputs = declaration.optional_inputs.clone();
                    description.source = declaration.source.clone();
                    if node.tags.is_none() {
                        description.tags = declaration.tags.clone();
                    }
                }
                // Unbound inspection intentionally omits unresolved declaration
                // reads; App preflight checks completeness before registration.
                node.step
                    .visit_reads(&self.document, conditions, &mut |key, _| {
                        description.read(key);
                    });
                node.step.describe(&mut description, &self.document);
                description
            })
            .collect();
        let mut edges = Vec::new();
        for node in &self.document.nodes {
            if let Some(route) = self.document.routes.get(&node.id) {
                edges.extend(route.branches.iter().map(|branch| GraphEdge {
                    from: node.id.clone(),
                    to: branch.target.node().map(str::to_owned),
                    condition: branch.when.as_ref().map(Condition::display_text),
                }));
            }
        }
        GraphDescription {
            id: crate::workflow::WorkflowId(self.document.id.clone()),
            revision: self.document.revision.clone(),
            entry: self.document.entry.clone(),
            nodes,
            edges,
        }
    }

    pub(crate) fn compose(composition: GraphComposition) -> Result<Self, AppError> {
        if composition.imports.is_empty()
            || composition.imports.len() > MAX_NODES
            || composition.connections.len() > MAX_COLLECTION_ITEMS
        {
            return Err(AppError::InvalidInput(
                "graph composition exceeds its import or connection bounds".to_owned(),
            ));
        }
        SchemaText::revision(&composition.revision)?;
        SchemaText::node_or_state("graph composition entry", &composition.entry)?;
        let mut document = DefinitionDocument {
            schema: SCHEMA.to_owned(),
            id: composition.id.to_string(),
            revision: composition.revision,
            state: Map::new(),
            required: Vec::new(),
            reducers: BTreeMap::new(),
            entry: composition.entry,
            nodes: Vec::new(),
            routes: BTreeMap::new(),
        };
        let mut revisions = BTreeMap::new();
        for import in composition.imports {
            let definition = import.definition.compiled;
            let revision = (
                definition.document.id.clone(),
                definition.document.revision.clone(),
            );
            if let Some(previous) = revisions.get(&revision) {
                if previous != &definition.fingerprint {
                    return Err(AppError::Conflict(
                        "graph imports disagree on one definition revision".to_owned(),
                    ));
                }
            } else {
                revisions.insert(revision, definition.fingerprint);
            }
            document.import(definition.document, &import.prefix)?;
        }
        for connection in composition.connections {
            SchemaText::node_or_state("graph connection source", &connection.from)?;
            SchemaText::node_or_state("graph connection target", &connection.to)?;
            let route = document.routes.get_mut(&connection.from).ok_or_else(|| {
                AppError::InvalidInput("graph connection source is not an imported node".to_owned())
            })?;
            route.branches.push(RouteBranch {
                when: None,
                target: RouteTarget::Node { id: connection.to },
            });
        }
        let source = serde_json::to_string(&document)
            .map_err(|_| AppError::Integration("composed graph could not be encoded".to_owned()))?;
        Self::parse(&source)
    }
}

impl DefinitionDocument {
    fn import(&mut self, source: Self, prefix: &str) -> Result<(), AppError> {
        if !prefix.is_empty() {
            SchemaText::node_or_state("graph import prefix", prefix)?;
        }
        if self.nodes.len().saturating_add(source.nodes.len()) > MAX_NODES {
            return Err(AppError::InvalidInput(
                "graph composition exceeds its node bound".to_owned(),
            ));
        }
        // Validate before expansion: a large caller prefix must never be
        // multiplied across every node and transition before rejection.
        for node in &source.nodes {
            if prefix.len().saturating_add(node.id.len()) > 128 {
                return Err(AppError::InvalidInput(
                    "prefixed graph node identity is too long".to_owned(),
                ));
            }
        }
        for (key, value) in source.state {
            match self.state.get(&key) {
                Some(previous) if previous != &value => {
                    return Err(AppError::Conflict(
                        "graph imports disagree on initial state".to_owned(),
                    ));
                }
                Some(_) => {}
                None => {
                    self.state.insert(key, value);
                }
            }
        }
        for (key, reducer) in source.reducers {
            if let Some(previous) = self.reducers.insert(key, reducer)
                && previous != reducer
            {
                return Err(AppError::Conflict(
                    "graph imports disagree on a state reducer".to_owned(),
                ));
            }
        }
        for key in source.required {
            if !self.required.contains(&key) {
                self.required.push(key);
            }
        }
        for mut node in source.nodes {
            node.id = format!("{prefix}{}", node.id);
            self.nodes.push(node);
        }
        for (id, mut route) in source.routes {
            for branch in &mut route.branches {
                if let RouteTarget::Node { id } = &mut branch.target {
                    *id = format!("{prefix}{id}");
                }
            }
            if self.routes.insert(format!("{prefix}{id}"), route).is_some() {
                return Err(AppError::Conflict(
                    "graph import node names collide".to_owned(),
                ));
            }
        }
        // Imports only add nodes/edges and compatible state declarations.
        // Bound the growing candidate before admitting another expansion.
        JsonBoundary::validate_size(self)
    }
}

impl GraphNode {
    fn read(&mut self, key: &str) {
        if !self.reads.iter().any(|value| value == key) {
            self.reads.push(key.to_owned());
        }
    }

    fn write(&mut self, key: &str) {
        if !self.writes.iter().any(|value| value == key) {
            self.writes.push(key.to_owned());
        }
    }
}

impl Step {
    fn kind(&self) -> &'static str {
        match self {
            Self::Action { .. } => "action",
            Self::Agent { .. } => "agent",
            Self::State { .. } => "state",
            Self::Input { .. } => "input",
            Self::Child { .. } => "child",
            Self::Map(_) => "map",
            Self::Pipeline { .. } => "pipeline",
            Self::JoinAll {} => "join_all",
            Self::JoinFirst {} => "join_first",
            Self::Sleep { .. } => "sleep",
        }
    }

    fn describe(&self, node: &mut GraphNode, definition: &DefinitionDocument) {
        match self {
            Self::Agent {
                text_target,
                structured_target,
                ..
            } => {
                for target in text_target.iter().chain(structured_target.iter()) {
                    node.write(target);
                }
            }
            Self::State { mutations } => {
                for mutation in mutations {
                    mutation.describe(node, definition);
                }
            }
            Self::Input { target, .. } => {
                if let Some(target) = target {
                    node.write(target);
                }
            }
            Self::Pipeline { reductions, .. } if reductions.is_empty() => {
                for key in definition.state.keys() {
                    node.write(key);
                }
            }
            Self::Child { reductions, .. }
            | Self::Map(
                MapDeclaration::Items { reductions, .. } | MapDeclaration::Tasks { reductions, .. },
            )
            | Self::Pipeline { reductions, .. } => {
                for reduction in reductions {
                    node.write(&reduction.target);
                }
            }
            Self::Action { .. } | Self::JoinAll {} | Self::JoinFirst {} | Self::Sleep { .. } => {}
        }
    }
}

impl StateMutation {
    fn describe(&self, node: &mut GraphNode, definition: &DefinitionDocument) {
        match self {
            Self::Assign { target, .. }
            | Self::Append { target, .. }
            | Self::Increment { target, .. }
            | Self::Extend { target, .. }
            | Self::Remove { target } => node.write(target),
            Self::Wipe { .. } | Self::Subset { .. } | Self::Merge { .. } => {
                for key in definition.state.keys() {
                    node.write(key);
                }
            }
        }
    }
}

impl Condition {
    fn display_text(&self) -> String {
        match self {
            Self::Custom {
                condition, name, ..
            } => name.as_ref().unwrap_or(condition).clone(),
            Self::Constant { value } => if *value { "True" } else { "False" }.to_owned(),
            Self::Exists { path } => format!("exists({path:?})"),
            Self::Equal { left, right } => Comparison::Equal.display_pair(left, right),
            Self::NotEqual { left, right } => Comparison::NotEqual.display_pair(left, right),
            Self::LessThan { left, right } => Comparison::LessThan.display_pair(left, right),
            Self::LessThanOrEqual { left, right } => {
                Comparison::LessThanOrEqual.display_pair(left, right)
            }
            Self::GreaterThan { left, right } => Comparison::GreaterThan.display_pair(left, right),
            Self::GreaterThanOrEqual { left, right } => {
                Comparison::GreaterThanOrEqual.display_pair(left, right)
            }
            Self::In { left, right } => Comparison::In.display_pair(left, right),
            Self::NotIn { left, right } => Comparison::NotIn.display_pair(left, right),
            Self::Contains { left, right } => Comparison::Contains.display_pair(left, right),
            Self::Is { left, right } => Comparison::Is.display_pair(left, right),
            Self::IsNot { left, right } => Comparison::IsNot.display_pair(left, right),
            Self::Not { condition } => format!("~{}", condition.display_text()),
            Self::All { conditions } | Self::Any { conditions } => {
                let conjunction = matches!(self, Self::All { .. });
                if conditions.is_empty() {
                    return if conjunction { "True" } else { "False" }.to_owned();
                }
                conditions
                    .iter()
                    .map(Self::display_text)
                    .collect::<Vec<_>>()
                    .join(if conjunction { " & " } else { " | " })
            }
            Self::Compare { values, operators } => {
                let mut text = String::new();
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        text.push(' ');
                        text.push_str(operators[index - 1].display_operator());
                        text.push(' ');
                    }
                    text.push_str(&value.display_text());
                }
                text
            }
        }
    }
}

impl Comparison {
    fn display_operator(&self) -> &'static str {
        match self {
            Self::Equal => "==",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::In => "in",
            Self::NotIn => "not in",
            Self::Contains => "contains",
            Self::Is => "is",
            Self::IsNot => "is not",
        }
    }

    fn display_pair(&self, left: &ValueExpr, right: &ValueExpr) -> String {
        format!(
            "{} {} {}",
            left.display_text(),
            self.display_operator(),
            right.display_text()
        )
    }
}

impl ValueExpr {
    fn display_text(&self) -> String {
        match self {
            Self::Literal { value } => Self::display_literal(value),
            Self::State { path } => {
                if path.contains('/') {
                    format!("state[{path:?}]")
                } else {
                    path.clone()
                }
            }
            Self::Length { value } => format!("len({})", value.display_text()),
            Self::Positive { value } => format!("+({})", value.display_text()),
            Self::Negative { value } => format!("-({})", value.display_text()),
            Self::Not { value } => format!("not ({})", value.display_text()),
            Self::And { values } => values
                .iter()
                .map(|value| format!("({})", value.display_text()))
                .collect::<Vec<_>>()
                .join(" and "),
            Self::Or { values } => values
                .iter()
                .map(|value| format!("({})", value.display_text()))
                .collect::<Vec<_>>()
                .join(" or "),
            Self::Conditional {
                when,
                then,
                otherwise,
            } => format!(
                "({} if {} else {})",
                then.display_text(),
                when.display_text(),
                otherwise.display_text()
            ),
            Self::Attribute { value, name } => format!("({}).{name}", value.display_text()),
            Self::Index { value, index } => {
                format!("({})[{}]", value.display_text(), index.display_text())
            }
            Self::Slice {
                value,
                start,
                stop,
                stride,
            } => {
                let start = start
                    .as_ref()
                    .map(|value| value.display_text())
                    .unwrap_or_default();
                let stop = stop
                    .as_ref()
                    .map(|value| value.display_text())
                    .unwrap_or_default();
                let stride = stride
                    .as_ref()
                    .map(|value| format!(":{}", value.display_text()))
                    .unwrap_or_default();
                format!("({})[{start}:{stop}{stride}]", value.display_text())
            }
            Self::Add { left, right } => {
                format!("({} + {})", left.display_text(), right.display_text())
            }
            Self::Subtract { left, right } => {
                format!("({} - {})", left.display_text(), right.display_text())
            }
            Self::Multiply { left, right } => {
                format!("({} * {})", left.display_text(), right.display_text())
            }
            Self::Divide { left, right } => {
                format!("({} / {})", left.display_text(), right.display_text())
            }
            Self::Remainder { left, right } => {
                format!("({} % {})", left.display_text(), right.display_text())
            }
            Self::FloorDivide { left, right } => {
                format!("({} // {})", left.display_text(), right.display_text())
            }
            Self::Power { left, right } => {
                format!("({} ** {})", left.display_text(), right.display_text())
            }
        }
    }

    fn display_literal(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "None".to_owned(),
            serde_json::Value::Bool(value) => if *value { "True" } else { "False" }.to_owned(),
            serde_json::Value::Number(value) => value.to_string(),
            serde_json::Value::String(_) => value.to_string(),
            serde_json::Value::Array(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(Self::display_literal)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            serde_json::Value::Object(values) => format!(
                "{{{}}}",
                values
                    .iter()
                    .map(|(key, value)| format!("{key:?}: {}", Self::display_literal(value)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}
