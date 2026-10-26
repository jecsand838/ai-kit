use std::path::PathBuf;
use std::time::Duration;

use agent_sdk::Agent;
use app_sdk::workflow::{ExecutionOptions, RunStatus, WorkflowDefinition, WorkflowFailure};
use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
use serde::Deserialize;
use serde_json::Value;
use storage_sdk::StateStore;
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OracleCorpus {
    provenance: Provenance,
    cases: Vec<OracleCase>,
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
struct OracleCase {
    id: String,
    evaluation: Evaluation,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Evaluation {
    Expression { value: Value },
    Condition { value: Value },
}

impl Evaluation {
    fn workflow(&self) -> Value {
        let mut document = serde_json::json!({
            "schema": "app-sdk.workflow/v2", "id": "expression", "revision": "v1",
            "state": {"output": null}, "required": [],
            "reducers": {"output": "reject_concurrent"}, "entry": "evaluate",
            "nodes": [], "routes": {}
        });
        match self {
            Self::Expression { value } => {
                document["nodes"] = serde_json::json!([
                    {"id": "evaluate", "step": {"kind": "state", "mutations": [
                        {"kind": "assign", "target": "output", "value": value}
                    ]}}
                ]);
                document["routes"] = serde_json::json!({
                    "evaluate": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
                });
            }
            Self::Condition { value } => {
                document["nodes"] = serde_json::json!([
                    {"id": "evaluate", "step": {"kind": "state", "mutations": []}},
                    {"id": "true", "step": {"kind": "state", "mutations": [
                        {"kind": "assign", "target": "output", "value": {"kind": "literal", "value": true}}
                    ]}},
                    {"id": "false", "step": {"kind": "state", "mutations": [
                        {"kind": "assign", "target": "output", "value": {"kind": "literal", "value": false}}
                    ]}}
                ]);
                document["routes"] = serde_json::json!({
                    "evaluate": {"mode": "first", "branches": [
                        {"when": value, "target": {"kind": "node", "id": "true"}},
                        {"target": {"kind": "node", "id": "false"}}
                    ]},
                    "true": {"mode": "first", "branches": [{"target": {"kind": "end"}}]},
                    "false": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
                });
            }
        }
        document
    }
}

#[derive(Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum Expected {
    DefinitionError,
    Value { value: Value },
    EvaluationError { message_contains: Option<String> },
}

impl OracleCorpus {
    fn load(name: &str) -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rurr")
            .join(format!("{name}.json"));
        let corpus: Self = serde_json::from_slice(&std::fs::read(path).expect("oracle fixture"))
            .expect("strict source-derived corpus");
        assert_eq!(corpus.provenance.kind, "pinned-source-derived");
        assert_eq!(
            corpus.provenance.archive_sha256,
            "5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e"
        );
        assert_eq!(
            corpus.provenance.archive_commit,
            "005bfe3ece631e64b06dac2d42ef84235f727a4f"
        );
        assert!(!corpus.provenance.members.is_empty());
        assert!(!corpus.provenance.note.is_empty());
        assert!(!corpus.cases.is_empty());
        corpus
    }

    async fn verify(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut failures = Vec::new();
        // Each case opens its own App so a rejected graph or failed activation
        // cannot mask another case's independent result.
        for case in &self.cases {
            let source = case.evaluation.workflow();
            let definition = match WorkflowDefinition::parse(&source.to_string()) {
                Ok(definition) => definition,
                Err(_) if matches!(case.expected, Expected::DefinitionError) => continue,
                Err(error) => {
                    failures.push(format!(
                        "{}: accepted expression failed compilation: {error}",
                        case.id
                    ));
                    continue;
                }
            };
            if matches!(case.expected, Expected::DefinitionError) {
                failures.push(format!("{}: invalid expression compiled", case.id));
                continue;
            }
            let state =
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                    .await?;
            let config: AppConfig<StateStore, Agent> =
                AppConfig::new(state, Vec::new(), vec![definition]);
            let mut app = App::open(config).await?;
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some("source-derived-parity".to_owned()),
                    root: std::env::current_dir()?,
                })
                .await?;
            let run = workspace
                .workflow("expression".parse()?)
                .await?
                .start(ExecutionOptions::default())
                .await?;
            let snapshot = tokio::time::timeout(Duration::from_secs(2), run.wait()).await??;
            match &case.expected {
                Expected::DefinitionError => panic!("invalid source cannot enter execution"),
                Expected::Value { value } => {
                    let value_matches =
                        snapshot
                            .state
                            .values()
                            .get("output")
                            .is_some_and(|observed| {
                                observed == value
                                    && (!value.is_f64()
                                        || observed.as_f64().zip(value.as_f64()).is_some_and(
                                            |(actual, expected)| {
                                                actual.to_bits() == expected.to_bits()
                                            },
                                        ))
                            });
                    if !matches!(snapshot.status, RunStatus::Succeeded) || !value_matches {
                        failures.push(format!(
                            "{}: expected {value}, observed state {:?}",
                            case.id, snapshot.state
                        ));
                    }
                }
                Expected::EvaluationError { message_contains } => {
                    if !matches!(
                        snapshot.status,
                        RunStatus::Failed(WorkflowFailure::Evaluation(_))
                    ) || snapshot.state.values().get("output") != Some(&Value::Null)
                    {
                        failures.push(format!("{}: expected atomic evaluation failure", case.id));
                    }
                    if let Some(expected) = message_contains
                        && !matches!(&snapshot.status,
                            RunStatus::Failed(WorkflowFailure::Evaluation(message)) if message.contains(expected)
                        )
                    {
                        let actual = match &snapshot.status {
                            RunStatus::Failed(WorkflowFailure::Evaluation(message)) => {
                                message.as_str()
                            }
                            _ => "not an evaluation failure",
                        };
                        failures.push(format!(
                            "{}: expected error containing {expected:?}, observed {actual}",
                            case.id
                        ));
                    }
                }
            }
            app.shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(2),
            })
            .await?;
        }
        assert!(
            failures.is_empty(),
            "source-derived parity failures:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
}

#[tokio::test]
async fn numeric_arithmetic_matches_pinned_rurr()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    OracleCorpus::load("numeric").verify().await
}

#[tokio::test]
async fn comparisons_match_pinned_rurr() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    OracleCorpus::load("comparison").verify().await
}

#[tokio::test]
async fn expression_language_matches_pinned_rurr()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    OracleCorpus::load("language").verify().await
}
