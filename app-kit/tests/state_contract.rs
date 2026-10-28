use std::path::PathBuf;
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ExecutionOptions, RunSelector, RunSnapshot, RunStatus, WorkflowDefinition, WorkflowFailure,
};
use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
use serde::Deserialize;
use serde_json::{Map, Value};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

// These are source-derived expectations, not results of executing rurr. Pairs
// preserve authored state order even before serde_json/preserve_order is enabled.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateCorpus {
    provenance: Provenance,
    cases: Vec<StateCase>,
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
struct StateCase {
    id: String,
    family: String,
    source: String,
    initial: Vec<(String, Value)>,
    #[serde(default)]
    required: Vec<String>,
    #[serde(default)]
    reducers: Map<String, Value>,
    program: Program,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Program {
    Serial { mutations: Vec<Value> },
    Sequence { actions: Vec<Vec<Value>> },
    Siblings { writers: Vec<Writer> },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Writer {
    node: String,
    mutations: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum Expected {
    Value {
        state: Vec<(String, Value)>,
        #[serde(default)]
        object_order: Vec<(String, Vec<String>)>,
    },
    StateFailure,
    EvaluationFailure,
}

impl StateCorpus {
    fn load(name: &str) -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rurr")
            .join(format!("{name}.json"));
        let corpus: Self = serde_json::from_slice(&std::fs::read(path).expect("state fixture"))
            .expect("strict state corpus");
        assert_eq!(
            corpus.provenance.archive_sha256,
            "5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e"
        );
        assert_eq!(
            corpus.provenance.archive_commit,
            "005bfe3ece631e64b06dac2d42ef84235f727a4f"
        );
        assert!(matches!(
            corpus.provenance.kind.as_str(),
            "pinned-source-derived" | "app-extension-contract"
        ));
        assert!(!corpus.provenance.members.is_empty());
        assert!(!corpus.provenance.note.is_empty());
        corpus
    }

    async fn verify(&self, family: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cases = self
            .cases
            .iter()
            .filter(|case| case.family == family)
            .collect::<Vec<_>>();
        assert!(!cases.is_empty(), "test must select actual cases");
        let mut failures = Vec::new();
        for case in cases {
            assert!(!case.source.is_empty(), "case needs an exact source anchor");
            let definition = match WorkflowDefinition::parse(&case.definition()) {
                Ok(definition) => definition,
                Err(error) => {
                    failures.push(format!(
                        "{}: accepted state program rejected: {error}",
                        case.id
                    ));
                    continue;
                }
            };
            let state =
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                    .await?;
            let config: AppConfig<StateStore, Agent> =
                AppConfig::new(state, Vec::new(), vec![definition]);
            let mut app = App::open(config).await?;
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some("state-contract".to_owned()),
                    root: std::env::current_dir()?,
                })
                .await?;
            let workflow = workspace.workflow("state_contract".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let snapshot = tokio::time::timeout(Duration::from_secs(2), run.wait()).await??;
            case.check(&snapshot, "completed", &mut failures);
            // A fresh run handle in the same App proves checkpoint readback;
            // this is not a fresh App/process recovery test.
            let readback = workflow
                .open_run(RunSelector::Latest(run.id().clone()))
                .await?
                .snapshot()
                .await?;
            case.check(&readback, "checkpoint readback", &mut failures);
            if readback.checkpoint != snapshot.checkpoint || readback.state != snapshot.state {
                failures.push(format!(
                    "{}: checkpoint readback changed committed state or fence",
                    case.id
                ));
            }
            app.shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(2),
            })
            .await?;
        }
        assert!(
            failures.is_empty(),
            "state contract failures:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
}

impl StateCase {
    fn definition(&self) -> String {
        let mut reducers = self.reducers.clone();
        for (key, _) in &self.initial {
            reducers
                .entry(key.clone())
                .or_insert_with(|| Value::String("reject_concurrent".to_owned()));
        }
        let mut nodes = Vec::new();
        let mut routes = Map::new();
        match &self.program {
            Program::Serial { mutations } => {
                nodes.push(serde_json::json!({
                    "id": "entry", "step": {"kind": "state", "mutations": mutations}
                }));
                routes.insert(
                    "entry".to_owned(),
                    serde_json::json!({
                        "mode": "first", "branches": [{"target": {"kind": "end"}}]
                    }),
                );
            }
            Program::Sequence { actions } => {
                assert!(!actions.is_empty(), "sequence needs at least one action");
                let mut previous = None;
                for (index, mutations) in actions.iter().enumerate() {
                    let id = if index == 0 {
                        "entry".to_owned()
                    } else {
                        format!("action{index}")
                    };
                    nodes.push(serde_json::json!({
                        "id": id, "step": {"kind": "state", "mutations": mutations}
                    }));
                    if let Some(previous) = previous {
                        routes.insert(
                            previous,
                            serde_json::json!({
                                "mode":"first", "branches":[{"target":{"kind":"node","id":id}}]
                            }),
                        );
                    }
                    previous = Some(id);
                }
            }
            Program::Siblings { writers } => {
                assert!(
                    !writers.is_empty(),
                    "use a no-op writer for an empty contribution set"
                );
                nodes.push(serde_json::json!({
                    "id": "entry", "step": {"kind": "state", "mutations": []}
                }));
                let mut branches = Vec::new();
                for writer in writers {
                    nodes.push(serde_json::json!({
                        "id": writer.node,
                        "step": {"kind": "state", "mutations": writer.mutations}
                    }));
                    branches.push(serde_json::json!({
                        "target": {"kind": "node", "id": writer.node}
                    }));
                    routes.insert(
                        writer.node.clone(),
                        serde_json::json!({
                            "mode": "first", "branches": [{"target": {"kind": "end"}}]
                        }),
                    );
                }
                routes.insert(
                    "entry".to_owned(),
                    serde_json::json!({
                        "mode": "broadcast", "branches": branches
                    }),
                );
            }
        }
        // Write the initial object in fixture order, independently of the map
        // implementation under test. Never pre-sort it in the test harness.
        let state = self
            .initial
            .iter()
            .map(|(key, value)| {
                format!("{}:{value}", serde_json::to_string(key).expect("JSON key"))
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{"schema":"app-kit.workflow/v2","id":"state_contract","revision":"v1","state":{{{state}}},"required":{},"reducers":{},"entry":"entry","nodes":{},"routes":{}}}"#,
            serde_json::to_string(&self.required).expect("required keys"),
            serde_json::to_string(&reducers).expect("reducers"),
            serde_json::to_string(&nodes).expect("nodes"),
            serde_json::to_string(&routes).expect("routes"),
        )
    }

    fn check(&self, snapshot: &RunSnapshot, phase: &str, failures: &mut Vec<String>) {
        let (status_matches, expected) = match &self.expected {
            Expected::Value { state, .. } => {
                (matches!(snapshot.status, RunStatus::Succeeded), state)
            }
            Expected::StateFailure => (
                matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::State(_))
                ),
                &self.initial,
            ),
            Expected::EvaluationFailure => (
                matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::Evaluation(_))
                ),
                &self.initial,
            ),
        };
        if !status_matches {
            failures.push(format!("{} ({phase}): wrong terminal outcome", self.id));
        }
        let actual = snapshot
            .state
            .values()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        if &actual != expected {
            failures.push(format!(
                "{} ({phase}): expected ordered state {expected:?}, observed {actual:?}",
                self.id
            ));
        }
        if let Expected::Value { object_order, .. } = &self.expected {
            for (key, order) in object_order {
                let actual = snapshot
                    .state
                    .values()
                    .get(key)
                    .and_then(Value::as_object)
                    .map(|object| object.keys().cloned().collect::<Vec<_>>());
                if actual.as_ref() != Some(order) {
                    failures.push(format!(
                        "{} ({phase}): object {key} order {actual:?}, expected {order:?}",
                        self.id
                    ));
                }
            }
        }
    }
}

#[tokio::test]
async fn ordered_updates_and_removals_match_pinned_rurr()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    StateCorpus::load("state_mutations").verify("ordered").await
}

#[tokio::test]
async fn collection_mutations_match_pinned_rurr()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    StateCorpus::load("state_mutations")
        .verify("collections")
        .await
}

#[tokio::test]
async fn integer_mutations_match_pinned_rurr()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    StateCorpus::load("state_mutations").verify("integer").await
}

#[tokio::test]
async fn state_projections_match_pinned_rurr()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    StateCorpus::load("state_mutations")
        .verify("projections")
        .await
}

#[tokio::test]
async fn state_mutation_failures_are_atomic() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    StateCorpus::load("state_mutations").verify("atomic").await
}

#[tokio::test]
async fn sibling_reducers_apply_canonical_contributions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    StateCorpus::load("state_reducers").verify("reducers").await
}

#[tokio::test]
async fn sibling_reducer_failures_are_atomic()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    StateCorpus::load("state_reducers").verify("atomic").await
}

#[tokio::test]
async fn state_paths_address_exact_json_data_keys()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    StateCorpus::load("state_mutations").verify("paths").await
}
