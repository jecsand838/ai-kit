use serde_json::{Value, json};

use crate::AppError;
use crate::workflow::definition::{JsonBoundary, MAX_SOURCE_BYTES};
use crate::workflow::{
    GraphComposition, GraphDescription, GraphFormat, NoActions, NoConditions, WorkflowDefinition,
};

impl WorkflowDefinition {
    /// Describes authored nodes, ordered edges and declared state access.
    ///
    /// This projection owns metadata only; it never retains executable bindings
    /// or lower handles. Callable metadata is resolved when bindings are supplied.
    ///
    /// # Example
    ///
    /// ```
    /// use app_sdk::workflow::WorkflowDefinition;
    ///
    /// let definition = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#)?;
    /// let graph = definition.graph();
    /// assert_eq!(graph.nodes[0].id, "increment");
    /// assert_eq!(graph.nodes[0].writes, ["count"]);
    /// # Ok::<(), app_sdk::AppError>(())
    /// ```
    pub fn graph(&self) -> GraphDescription {
        self.compiled.graph::<NoActions, NoConditions>(&[], &[])
    }

    /// Renders deterministic inspection JSON, DOT or Mermaid without a subprocess.
    ///
    /// Caller text is escaped for the chosen format. This is an inspection
    /// document, distinct from [`GraphDescription::tracking_record`].
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` when the projection exceeds JSON or output-size limits,
    /// or `Integration` if graph metadata cannot be encoded.
    ///
    /// # Example
    ///
    /// ```
    /// use app_sdk::workflow::WorkflowDefinition;
    ///
    /// let definition = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#)?;
    /// use app_sdk::workflow::GraphFormat;
    /// let mermaid = definition.export(GraphFormat::Mermaid)?;
    /// assert!(mermaid.starts_with("flowchart TD"));
    /// # Ok::<(), app_sdk::AppError>(())
    /// ```
    pub fn export(&self, format: GraphFormat) -> Result<String, AppError> {
        format.render(&self.graph())
    }

    /// Imports graph pieces with explicit node prefixes and appended connections.
    ///
    /// The ordinary compiler checks the assembled definition. Conflicting state,
    /// reducers, reused revisions, node names, entrypoints and edges fail before
    /// any runtime or lower operation is admitted. Behavior binding keys retain
    /// their original meaning; only graph-local node references are prefixed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for conflicting state/reducers, incompatible revisions,
    /// colliding prefixed nodes, invalid connections/entrypoints, graph errors, or
    /// exceeded compiler bounds. No runtime or lower operation is admitted.
    ///
    /// # Example
    ///
    /// ```
    /// use app_sdk::workflow::WorkflowDefinition;
    /// const SOURCE: &str = r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#;
    /// let definition = WorkflowDefinition::parse(SOURCE)?;
    /// use app_sdk::workflow::{GraphComposition, GraphConnection, GraphImport};
    /// let composition = GraphComposition {
    ///     id: "composed".parse()?, revision: "r1".into(), entry: "a_increment".into(),
    ///     imports: vec![
    ///         GraphImport { definition, prefix: "a_".into() },
    ///         GraphImport { definition: WorkflowDefinition::parse(SOURCE)?, prefix: "b_".into() },
    ///     ],
    ///     connections: vec![GraphConnection { from: "a_increment".into(), to: "b_increment".into() }],
    /// };
    /// let composed = WorkflowDefinition::compose(composition)?;
    /// assert_eq!(composed.graph().nodes.len(), 2);
    /// assert_eq!(composed.graph().entry, "a_increment");
    /// # Ok::<(), app_sdk::AppError>(())
    /// ```
    pub fn compose(composition: GraphComposition) -> Result<Self, AppError> {
        Ok(Self {
            compiled: crate::workflow::definition::CompiledDefinition::compose(composition)?,
        })
    }
}

impl GraphDescription {
    /// Produces the pinned rurr application-graph tracking object.
    ///
    /// Every node must carry actual source metadata. Missing source is an error;
    /// no placeholder code is invented. Explicit end edges do not create a
    /// synthetic action or transition. The caller owns transport and JSONL framing.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` if a node lacks actual source metadata or the tracking
    /// record exceeds JSON bounds. Unbound caller actions may have no source metadata;
    /// the method does not invent callback source.
    ///
    /// # Example
    ///
    /// ```
    /// use app_sdk::workflow::WorkflowDefinition;
    ///
    /// let definition = WorkflowDefinition::parse(r#"{
    ///     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    ///     "state":{"count":0}, "required":["count"],
    ///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    ///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    ///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    ///     ]}}], "routes":{}
    /// }"#)?;
    /// let record = definition.graph().tracking_record()?;
    /// assert_eq!(record["type"], "application");
    /// assert_eq!(record["entrypoint"], "increment");
    /// # Ok::<(), app_sdk::AppError>(())
    /// ```
    pub fn tracking_record(&self) -> Result<Value, AppError> {
        let mut actions = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let source = node.source.as_ref().ok_or_else(|| {
                AppError::InvalidInput(format!("graph node '{}' has no source metadata", node.id))
            })?;
            actions.push(json!({
                "type": "action", "name": node.id, "reads": node.reads,
                "writes": node.writes, "code": source, "inputs": node.required_inputs,
                "optional_inputs": node.optional_inputs,
            }));
        }
        let transitions = self
            .edges
            .iter()
            .filter_map(|edge| {
                edge.to.as_ref().map(|target| {
                    json!({
                        "type": "transition", "from_": edge.from, "to": target,
                        "condition": edge.condition.as_deref().unwrap_or("default"),
                    })
                })
            })
            .collect::<Vec<_>>();
        let record = json!({
            "type": "application", "entrypoint": self.entry,
            "actions": actions, "transitions": transitions,
        });
        JsonBoundary::validate(&record)?;
        Ok(record)
    }
}

impl GraphFormat {
    fn render(&self, graph: &GraphDescription) -> Result<String, AppError> {
        let output = match self {
            Self::Json => {
                let value = serde_json::to_value(graph).map_err(|_| {
                    AppError::Integration("graph metadata could not be encoded".to_owned())
                })?;
                JsonBoundary::validate(&value)?;
                serde_json::to_string(&value).map_err(|_| {
                    AppError::Integration("graph metadata could not be encoded".to_owned())
                })?
            }
            Self::Dot => Self::dot(graph),
            Self::Mermaid => Self::mermaid(graph),
        };
        if output.len() > MAX_SOURCE_BYTES {
            return Err(AppError::InvalidInput(
                "graph document exceeds the private byte bound".to_owned(),
            ));
        }
        Ok(output)
    }

    fn dot(graph: &GraphDescription) -> String {
        let mut output = format!("digraph \"{}\" {{\n", graph.id);
        for node in &graph.nodes {
            let label = Self::Dot.escape(node.name.as_deref().unwrap_or(&node.id));
            output.push_str(&format!("  \"{}\" [label=\"{label}\"];\n", node.id));
        }
        for edge in &graph.edges {
            let Some(target) = &edge.to else { continue };
            output.push_str(&format!("  \"{}\" -> \"{target}\"", edge.from));
            if let Some(condition) = &edge.condition {
                let label = Self::Dot.escape(condition);
                output.push_str(&format!(" [label=\"{label}\"]"));
            }
            output.push_str(";\n");
        }
        output.push_str("}\n");
        output
    }

    fn mermaid(graph: &GraphDescription) -> String {
        let mut output = "flowchart TD\n".to_owned();
        for node in &graph.nodes {
            let label = Self::Mermaid.escape(node.name.as_deref().unwrap_or(&node.id));
            let id = Self::mermaid_id(&node.id);
            output.push_str(&format!("  {id}[\"{label}\"]\n"));
        }
        for edge in &graph.edges {
            let Some(target) = &edge.to else { continue };
            let from = Self::mermaid_id(&edge.from);
            let target = Self::mermaid_id(target);
            match &edge.condition {
                Some(condition) => {
                    let label = Self::Mermaid.escape(condition);
                    output.push_str(&format!("  {from} -->|\"{label}\"| {target}\n"));
                }
                None => output.push_str(&format!("  {from} --> {target}\n")),
            }
        }
        output
    }

    fn mermaid_id(id: &str) -> String {
        let mut output = String::with_capacity(id.len() + 2);
        output.push_str("n_");
        for character in id.chars() {
            match character {
                '-' => output.push_str("_2d"),
                '_' => output.push_str("_5f"),
                character => output.push(character),
            }
        }
        output
    }

    fn escape(&self, text: &str) -> String {
        let mut output = String::with_capacity(text.len());
        for character in text.chars() {
            match self {
                Self::Dot | Self::Json => match character {
                    '\\' => output.push_str("\\\\"),
                    '"' => output.push_str("\\\""),
                    '\n' => output.push_str("\\n"),
                    '\r' => output.push_str("\\r"),
                    '\t' => output.push_str("\\t"),
                    value if value.is_control() => {
                        output.push_str(&format!("\\u{:04x}", u32::from(value)))
                    }
                    value => output.push(value),
                },
                Self::Mermaid => match character {
                    '"' | '&' | '<' | '>' | '[' | ']' | '#' | '|' | '\\' => {
                        output.push_str(&format!("#{};", u32::from(character)));
                    }
                    value if value.is_control() => {
                        output.push_str(&format!("#{};", u32::from(value)))
                    }
                    value => output.push(value),
                },
            }
        }
        output
    }
}
