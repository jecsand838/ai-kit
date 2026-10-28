use std::path::PathBuf;
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ExecutionOptions, GraphComposition, GraphConnection, GraphDescription, GraphEdge, GraphFormat,
    GraphImport, GraphNode, RunStatus, WorkflowDefinition, WorkflowFailure,
};
use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
use serde::Deserialize;
use serde_json::{Map, Value};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphCorpus {
    provenance: Provenance,
    cases: Vec<GraphCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Provenance {
    kind: String,
    archive_sha256: String,
    archive_commit: String,
    members: Vec<String>,
    note: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphCase {
    id: String,
    definition: Value,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum Expected {
    DefinitionError,
    Succeeded { output: Value },
    SucceededState { state: Map<String, Value> },
    EvaluationError,
}

impl GraphCorpus {
    async fn verify(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        assert_eq!(self.provenance.kind, "pinned-source-derived");
        assert_eq!(
            self.provenance.archive_sha256,
            "5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e"
        );
        assert_eq!(
            self.provenance.archive_commit,
            "005bfe3ece631e64b06dac2d42ef84235f727a4f"
        );
        assert!(!self.provenance.members.is_empty());
        assert!(!self.provenance.note.is_empty());
        assert!(!self.cases.is_empty());
        let mut failures = Vec::new();
        for case in self.cases {
            let definition = match WorkflowDefinition::parse(&case.definition.to_string()) {
                Err(_) if matches!(case.expected, Expected::DefinitionError) => continue,
                Err(error) => {
                    failures.push(format!("{}: accepted graph rejected: {error}", case.id));
                    continue;
                }
                Ok(_) if matches!(case.expected, Expected::DefinitionError) => {
                    failures.push(format!("{}: invalid graph accepted", case.id));
                    continue;
                }
                Ok(definition) => definition,
            };
            let store =
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                    .await?;
            let config: AppConfig<StateStore, Agent> =
                AppConfig::new(store, Vec::new(), vec![definition]);
            let mut app = App::open(config).await?;
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some(case.id.clone()),
                    root: std::env::current_dir()?,
                })
                .await?;
            let run = workspace
                .workflow("route-parity".parse()?)
                .await?
                .start(ExecutionOptions::default())
                .await?;
            let observed = tokio::time::timeout(Duration::from_secs(2), run.wait()).await??;
            let matches = match case.expected {
                Expected::DefinitionError => false,
                Expected::Succeeded { output } => {
                    matches!(observed.status, RunStatus::Succeeded)
                        && observed.state.values().get("output") == Some(&output)
                }
                Expected::SucceededState { state } => {
                    matches!(observed.status, RunStatus::Succeeded)
                        && observed.state.values() == &state
                }
                Expected::EvaluationError => matches!(
                    observed.status,
                    RunStatus::Failed(WorkflowFailure::Evaluation(_))
                ),
            };
            if !matches {
                failures.push(format!("{}: unexpected graph result/state", case.id));
            }
            app.shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(2),
            })
            .await?;
        }
        assert!(
            failures.is_empty(),
            "source-derived graph failures:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
}

#[tokio::test]
async fn serial_routes_match_pinned_rurr() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rurr/routes.json");
    let corpus: GraphCorpus = serde_json::from_slice(&std::fs::read(path)?)?;
    corpus.verify().await
}

struct GraphFixture {
    document: Value,
}

#[test]
fn definition_schema_accepts_only_the_coherent_v2_language() {
    let mut fixture = GraphFixture::counter("schema_version");
    fixture.document["schema"] = Value::from("app-kit.workflow/v2");
    assert!(
        WorkflowDefinition::parse(&fixture.document.to_string()).is_ok(),
        "the adopted document version must be accepted"
    );
    for unsupported in ["app-kit.workflow/v1", "app-kit.workflow/v3", "", "v2"] {
        fixture.document["schema"] = Value::from(unsupported);
        assert!(
            WorkflowDefinition::parse(&fixture.document.to_string()).is_err(),
            "unsupported schema {unsupported} cannot enter a compatibility parser"
        );
    }
}

impl GraphFixture {
    fn counter(id: &str) -> Self {
        Self {
            document: serde_json::json!({
                "schema":"app-kit.workflow/v2", "id":id, "revision":"v1",
                "state":{"count":0}, "required":["count"],
                "reducers":{"count":"reject_concurrent"}, "entry":"step",
                "nodes":[{"id":"step", "step":{"kind":"state", "mutations":[
                    {"kind":"increment", "target":"count", "value":{"kind":"literal", "value":1}}
                ]}}], "routes":{}
            }),
        }
    }

    fn parse(&self) -> WorkflowDefinition {
        WorkflowDefinition::parse(&self.document.to_string()).expect("valid graph fixture")
    }

    fn chain(mut self, count: usize) -> Self {
        self.document["entry"] = Value::from("n0");
        let node = self.document["nodes"][0].clone();
        self.document["nodes"] = Value::Array(
            (0..count)
                .map(|index| {
                    let mut node = node.clone();
                    node["id"] = Value::from(format!("n{index}"));
                    node
                })
                .collect(),
        );
        self.document["routes"] = Value::Object(
            (0..count.saturating_sub(1))
                .map(|index| {
                    (
                        format!("n{index}"),
                        serde_json::json!({"mode":"first", "branches":[{
                            "target":{"kind":"node", "id":format!("n{}", index + 1)}
                        }]}),
                    )
                })
                .collect(),
        );
        self
    }

    fn import(&self, prefix: &str) -> GraphImport {
        GraphImport {
            definition: self.parse(),
            prefix: prefix.to_owned(),
        }
    }

    fn pair(&self) -> GraphComposition {
        GraphComposition {
            id: "composed".parse().expect("id"),
            revision: "v1".to_owned(),
            entry: "one_step".to_owned(),
            imports: vec![self.import("one_"), self.import("two_")],
            connections: vec![GraphConnection {
                from: "one_step".to_owned(),
                to: "two_step".to_owned(),
            }],
        }
    }

    async fn execute(definition: WorkflowDefinition) -> Value {
        let id = definition.graph().id;
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("store");
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store, Vec::new(), vec![definition]);
        let mut app = App::open(config).await.expect("App");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("graph-execution".to_owned()),
                root: std::env::current_dir().expect("cwd"),
            })
            .await
            .expect("workspace");
        let run = workspace
            .workflow(id)
            .await
            .expect("workflow")
            .start(ExecutionOptions::default())
            .await
            .expect("run");
        let snapshot = tokio::time::timeout(Duration::from_secs(2), run.wait())
            .await
            .expect("bounded run")
            .expect("snapshot");
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(2),
        })
        .await
        .expect("shutdown");
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        Value::Object(snapshot.state.values().clone())
    }

    fn tracking() -> GraphDescription {
        GraphDescription {
            id: "tracking".parse().expect("id"),
            revision: "v1".to_owned(),
            entry: "decide".to_owned(),
            nodes: vec![GraphNode {
                id: "decide".to_owned(),
                name: Some("Display label".to_owned()),
                description: None,
                tags: vec!["entry".to_owned()],
                kind: "action".to_owned(),
                reads: vec!["count".to_owned()],
                writes: vec!["count".to_owned()],
                required_inputs: vec!["request".to_owned()],
                optional_inputs: vec!["style".to_owned()],
                source: Some("caller supplied source".to_owned()),
            }],
            edges: vec![
                GraphEdge {
                    from: "decide".to_owned(),
                    to: Some("decide".to_owned()),
                    condition: Some("count > 0".to_owned()),
                },
                GraphEdge {
                    from: "decide".to_owned(),
                    to: None,
                    condition: None,
                },
            ],
        }
    }
}

#[test]
fn authored_tags_preserve_display_strings_order_duplicates_and_array_type() {
    // Pinned FunctionAction.with_tags retains arbitrary caller strings. App's
    // authored node tags use the same data vocabulary and finite text bound.
    for tags in [
        vec![
            String::new(),
            "  spaced tag  ".into(),
            "Unicode 🦀 / @ #\n\0".into(),
        ],
        vec!["duplicate".into(), "duplicate".into()],
        vec!["é".repeat(32_768)],
        vec!["tag".into(); 4_096],
        Vec::new(),
    ] {
        let mut fixture = GraphFixture::counter("tag-data");
        fixture.document["nodes"][0]["tags"] = serde_json::json!(tags);
        let graph = fixture.parse().graph();
        assert_eq!(graph.nodes[0].tags, tags);
    }
    for invalid in [
        Value::Null,
        Value::Bool(true),
        Value::from("tag"),
        serde_json::json!([1]),
        serde_json::json!(["x".repeat(65_537)]),
        serde_json::json!(vec!["tag"; 4_097]),
    ] {
        let mut fixture = GraphFixture::counter("tag-data");
        fixture.document["nodes"][0]["tags"] = invalid;
        assert!(matches!(
            WorkflowDefinition::parse(&fixture.document.to_string()),
            Err(app_kit::AppError::InvalidInput(_))
        ));
    }
}

#[test]
fn graph_inspection_preserves_authored_nodes_metadata_and_state_contracts() {
    let mut fixture = GraphFixture::counter("inspection");
    fixture.document["entry"] = Value::from("zeta");
    fixture.document["nodes"][0]["id"] = Value::from("zeta");
    fixture.document["nodes"][0]["name"] = Value::from("Increment counter");
    fixture.document["nodes"][0]["description"] = Value::from("One immutable operation");
    fixture.document["nodes"][0]["tags"] = serde_json::json!(["shared", "first"]);
    fixture.document["nodes"]
        .as_array_mut()
        .expect("nodes")
        .push(serde_json::json!({
            "id":"alpha", "tags":["shared", "last"], "step":{"kind":"state", "mutations":[]}
        }));
    fixture.document["routes"] = serde_json::json!({
        "zeta":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"alpha"}}]}
    });
    let graph = fixture.parse().graph();
    assert_eq!(graph.id.to_string(), "inspection");
    assert_eq!(graph.revision, "v1");
    assert_eq!(graph.entry, "zeta");
    assert_eq!(
        graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        ["zeta", "alpha"]
    );
    let node = &graph.nodes[0];
    assert_eq!(node.name.as_deref(), Some("Increment counter"));
    assert_eq!(node.description.as_deref(), Some("One immutable operation"));
    assert_eq!(node.tags, ["shared", "first"]);
    assert_eq!(node.kind, "state");
    assert_eq!(node.reads, ["count"]);
    assert_eq!(node.writes, ["count"]);
    assert!(node.required_inputs.is_empty() && node.optional_inputs.is_empty());
    let _: Value = serde_json::from_str(node.source.as_deref().expect("authored built-in source"))
        .expect("JSON source");
    assert_eq!(graph.edges.len(), 1);
    assert_eq!(graph.edges[0].from, "zeta");
    assert_eq!(graph.edges[0].to.as_deref(), Some("alpha"));
    assert!(graph.edges[0].condition.is_none());
}

#[test]
fn graph_exports_are_deterministic_and_escape_caller_labels() {
    let mut fixture = GraphFixture::counter("escaping");
    let label = "quote \" slash \\ newline\n] --> injected[<script>";
    fixture.document["nodes"][0]["name"] = Value::from(label);
    let definition = fixture.parse();
    let json = definition.export(GraphFormat::Json).expect("JSON graph");
    assert_eq!(
        json,
        definition.export(GraphFormat::Json).expect("repeat JSON")
    );
    let graph: Value = serde_json::from_str(&json).expect("JSON");
    assert_eq!(graph["id"], "escaping");
    assert_eq!(graph["entry"], "step");
    assert_eq!(graph["nodes"][0]["name"], label);
    assert_eq!(graph["nodes"][0]["writes"], serde_json::json!(["count"]));
    assert!(graph.get("checkpoint").is_none() && graph.get("runtime").is_none());
    let dot = definition.export(GraphFormat::Dot).expect("DOT");
    assert!(dot.contains("digraph") && dot.contains("step"));
    assert!(dot.contains("\\\"") && dot.contains("\\\\") && dot.contains("\\n"));
    assert!(!dot.contains(label));
    assert_eq!(
        dot,
        definition.export(GraphFormat::Dot).expect("repeat DOT")
    );
    let mermaid = definition.export(GraphFormat::Mermaid).expect("Mermaid");
    assert!(mermaid.contains("flowchart") && mermaid.contains("step"));
    assert!(!mermaid.contains(label) && !mermaid.contains("<script>"));
    assert_eq!(
        mermaid,
        definition
            .export(GraphFormat::Mermaid)
            .expect("repeat Mermaid")
    );
}

#[test]
fn small_graph_renderings_do_not_inherit_unrendered_metadata_limits() {
    let mut fixture = GraphFixture::counter("large-source");
    let mutation = serde_json::json!({
        "kind":"assign", "target":"count",
        "value":{"kind":"literal", "value":"\"".repeat(60)}
    });
    fixture.document["nodes"][0]["step"]["mutations"] = Value::Array(vec![mutation; 3_500]);
    let definition = fixture.parse();
    assert!(
        definition.graph().nodes[0]
            .source
            .as_ref()
            .expect("source")
            .len()
            > 600_000
    );
    assert!(
        definition.export(GraphFormat::Json).is_err(),
        "escaped metadata exceeds the JSON byte bound"
    );
    for format in [GraphFormat::Dot, GraphFormat::Mermaid] {
        let output = definition
            .export(format)
            .expect("bounded label-only rendering");
        assert!(output.len() < 100);
    }
}

#[test]
fn graph_rendering_limits_measure_the_chosen_format_bytes() {
    let mut fixture = GraphFixture::counter("format-limit").chain(8);
    for node in fixture.document["nodes"].as_array_mut().expect("nodes") {
        node["name"] = Value::from("\"".repeat(60_000));
    }
    let definition = fixture.parse();
    let dot = definition
        .export(GraphFormat::Dot)
        .expect("raw DOT remains below1MiB");
    assert!(dot.len() > 900_000 && dot.len() <= 1_048_576);
    assert!(serde_json::to_vec(&dot).expect("JSON encoded DOT").len() > 1_048_576);
    assert!(
        definition.export(GraphFormat::Mermaid).is_err(),
        "Mermaid quote entities make its actual output exceed1MiB"
    );
}

#[test]
fn graph_mermaid_identifiers_do_not_become_edge_syntax() {
    // Mermaid's official flowchart lexer recognizes repeated '-' as link
    // syntax; workflow identifiers allow it. Projection must encode identity
    // punctuation without altering the original graph or merging distinct IDs.
    // https://github.com/mermaid-js/mermaid/blob/develop/packages/mermaid/src/diagrams/flowchart/parser/flow.jison
    let mut fixture = GraphFixture::counter("mermaid-identities").chain(4);
    let names = ["a---b", "a-b", "a_2db", "end"];
    fixture.document["entry"] = Value::from(names[0]);
    for (node, name) in fixture.document["nodes"]
        .as_array_mut()
        .expect("nodes")
        .iter_mut()
        .zip(names)
    {
        node["id"] = Value::from(name);
    }
    fixture.document["routes"] = serde_json::json!({
        "a---b":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"a-b"}}]},
        "a-b":{"mode":"first", "branches":[{"when":{"kind":"constant", "value":true}, "target":{"kind":"node", "id":"a_2db"}}]},
        "a_2db":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"end"}}]}
    });
    let definition = fixture.parse();
    let mermaid = definition.export(GraphFormat::Mermaid).expect("Mermaid");
    let mut identifiers = Vec::new();
    for line in mermaid.lines().filter(|line| line.contains("[\"")) {
        let (identifier, _) = line.trim().split_once("[\"").expect("node declaration");
        assert!(
            identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "canonical workflow names may contain Mermaid edge punctuation: {identifier}"
        );
        assert!(
            !identifiers.iter().any(|value| value == identifier),
            "escaped identities must remain unique"
        );
        identifiers.push(identifier.to_owned());
    }
    assert_eq!(identifiers.len(), 4);
    let edges = mermaid
        .lines()
        .filter(|line| line.contains(" -->"))
        .map(|line| {
            let (from, tail) = line.trim().split_once(" -->").expect("edge");
            let to = tail
                .rsplit_once("| ")
                .map_or(tail.trim(), |(_, target)| target);
            (from, to)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        edges,
        identifiers
            .windows(2)
            .map(|pair| (pair[0].as_str(), pair[1].as_str()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        definition
            .graph()
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        names
    );
}

#[test]
fn graph_composition_checks_prefixed_identity_limits() {
    let fixture = GraphFixture::counter("identity-limit");
    let prefix = "a".repeat(124);
    let definition = WorkflowDefinition::compose(GraphComposition {
        id: "composed".parse().expect("id"),
        revision: "v1".to_owned(),
        entry: format!("{prefix}step"),
        imports: vec![fixture.import(&prefix)],
        connections: Vec::new(),
    })
    .expect("128-byte combined identity");
    assert_eq!(definition.graph().nodes[0].id.len(), 128);
    for prefix in ["a".repeat(125), "a".repeat(1_048_576)] {
        let composition = GraphComposition {
            id: "composed".parse().expect("id"),
            revision: "v1".to_owned(),
            entry: "step".to_owned(),
            imports: vec![fixture.import(&prefix)],
            connections: Vec::new(),
        };
        assert!(WorkflowDefinition::compose(composition).is_err());
    }
}

#[test]
fn graph_composition_checks_the_combined_node_limit() {
    let first = GraphFixture::counter("first").chain(512);
    for count in [512, 513] {
        let second = GraphFixture::counter("second").chain(count);
        let result = WorkflowDefinition::compose(GraphComposition {
            id: "composed".parse().expect("id"),
            revision: "v1".to_owned(),
            entry: "one_n0".to_owned(),
            imports: vec![first.import("one_"), second.import("two_")],
            connections: vec![GraphConnection {
                from: "one_n511".to_owned(),
                to: "two_n0".to_owned(),
            }],
        });
        if count == 512 {
            assert_eq!(result.expect("1,024 nodes").graph().nodes.len(), 1_024);
        } else {
            assert!(
                result.is_err(),
                "1,025 combined nodes must fail compilation"
            );
        }
    }
}

#[test]
fn graph_tracking_projection_matches_pinned_wire_fields_and_omits_end_edges() {
    // Source-derived field/order expectation from pinned tracking/02_branching_expr/graph.json.
    let graph = GraphFixture::tracking();
    assert_eq!(
        graph.tracking_record().expect("tracking"),
        serde_json::json!({
            "type":"application", "entrypoint":"decide", "actions":[
                {"type":"action", "name":"decide", "reads":["count"], "writes":["count"],
                 "code":"caller supplied source", "inputs":["request"], "optional_inputs":["style"]}
            ], "transitions":[
                {"type":"transition", "from_":"decide", "to":"decide", "condition":"count > 0"}
            ]
        })
    );
}

#[test]
fn graph_tracking_requires_source_and_names_default_conditions() {
    let mut graph = GraphFixture::tracking();
    graph.edges[0].condition = None;
    assert_eq!(
        graph.tracking_record().expect("tracking")["transitions"][0]["condition"],
        "default"
    );
    graph.nodes[0].source = None;
    assert!(
        graph.tracking_record().is_err(),
        "missing source cannot become invented code"
    );
}

#[tokio::test]
async fn graph_composition_reuses_a_definition_and_executes_connections() {
    let fixture = GraphFixture::counter("reusable");
    let definition = WorkflowDefinition::compose(fixture.pair()).expect("composition");
    assert_eq!(
        definition
            .graph()
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        ["one_step", "two_step"]
    );
    assert_eq!(GraphFixture::execute(definition).await["count"], 2);
    assert_eq!(GraphFixture::execute(fixture.parse()).await["count"], 1);
}

#[test]
fn graph_composition_rejects_name_reference_and_entry_errors() {
    let fixture = GraphFixture::counter("reusable");
    let mut duplicate = fixture.pair();
    duplicate.imports[1].prefix = "one_".to_owned();
    let mut unknown_from = fixture.pair();
    unknown_from.connections[0].from = "missing".to_owned();
    let mut unknown_to = fixture.pair();
    unknown_to.connections[0].to = "missing".to_owned();
    let mut unknown_entry = fixture.pair();
    unknown_entry.entry = "missing".to_owned();
    let mut invalid_prefix = fixture.pair();
    invalid_prefix.imports[0].prefix = "not valid/".to_owned();
    let mut empty = fixture.pair();
    empty.imports.clear();
    for composition in [
        duplicate,
        unknown_from,
        unknown_to,
        unknown_entry,
        invalid_prefix,
        empty,
    ] {
        assert!(WorkflowDefinition::compose(composition).is_err());
    }
}

#[test]
fn graph_composition_rejects_conflicting_state_and_reused_revisions() {
    let first = GraphFixture::counter("first");
    let mut different_state = GraphFixture::counter("second");
    different_state.document["state"]["count"] = Value::from(5);
    let mut composition = first.pair();
    composition.imports[1] = different_state.import("two_");
    assert!(WorkflowDefinition::compose(composition).is_err());
    let mut changed = GraphFixture::counter("first");
    changed.document["nodes"][0]["step"]["mutations"][0]["value"]["value"] = Value::from(2);
    let mut composition = first.pair();
    composition.imports[1] = changed.import("two_");
    assert!(
        WorkflowDefinition::compose(composition).is_err(),
        "one id/revision cannot name different content"
    );
}

#[tokio::test]
async fn graph_and_conditions_use_decoded_state_path_root_keys() {
    let mut fixture = GraphFixture::counter("data_paths");
    fixture.document["state"] =
        serde_json::json!({"a/b":3,"a~1b":8,"root":{"line\nkey":4},"count":0});
    fixture.document["required"] = serde_json::json!([]);
    fixture.document["reducers"] = serde_json::json!({"a/b":"reject_concurrent","a~1b":"reject_concurrent","root":"reject_concurrent","count":"reject_concurrent"});
    fixture.document["nodes"] = serde_json::json!([
        {"id":"step","step":{"kind":"state","mutations":[
            {"kind":"assign","target":"count","value":{"kind":"add","left":{"kind":"state","path":"a~1b"},"right":{"kind":"state","path":"a~01b"}}}
        ]}},
        {"id":"finish","step":{"kind":"state","mutations":[
            {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
        ]}}
    ]);
    fixture.document["routes"] = serde_json::json!({"step":{"mode":"first","branches":[{
        "when":{"kind":"all","conditions":[
            {"kind":"equal","left":{"kind":"state","path":"a~1b"},"right":{"kind":"literal","value":3}},
            {"kind":"equal","left":{"kind":"state","path":"root/line\nkey"},"right":{"kind":"literal","value":4}}
        ]},"target":{"kind":"node","id":"finish"}
    }]}});
    let definition = fixture.parse();
    assert_eq!(definition.graph().nodes[0].reads, ["a/b", "a~1b"]);
    assert_eq!(GraphFixture::execute(definition).await["count"], 12);
}
