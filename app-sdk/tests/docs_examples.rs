use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_sdk::Agent;
use app_sdk::workflow::{
    ExecutionOptions, RunStatus, SyncExecution, WorkflowDefinition, WorkflowState,
};
use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
use futures_util::FutureExt;
use serde_json::{Value, json};
use storage_sdk::StateStore;
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct Documentation {
    root: PathBuf,
}

impl Documentation {
    fn new() -> Self {
        Self {
            root: Path::new(env!("CARGO_MANIFEST_DIR")).join("docs"),
        }
    }

    fn files(&self, extension: &str) -> Vec<PathBuf> {
        let mut pending = vec![self.root.clone()];
        let mut files = Vec::new();
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).expect("documentation directory is readable")
            {
                let path = entry.expect("documentation entry is readable").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|value| value == extension) {
                    files.push(path);
                }
            }
        }
        files.sort();
        files
    }

    fn assert_guide_inventory(&self) {
        let index = std::fs::read_to_string(self.root.join("README.md"))
            .expect("the downstream guide index exists");
        let required = [
            "QUICKSTART.md",
            "TRAIT_INTEGRATION.md",
            "WORKFLOW_LANGUAGE.md",
            "ACTIONS_AND_INPUTS.md",
            "STATE_AND_PARALLELISM.md",
            "MULTI_AGENT_WORKFLOWS.md",
            "EXECUTION_CONTROLS.md",
            "STREAMING.md",
            "INTERACTIONS_AND_CONTROLS.md",
            "PERSISTENCE_AND_RECOVERY.md",
            "LIFECYCLE_AND_OPERATIONS.md",
            "OBSERVABILITY.md",
            "ERRORS_AND_TROUBLESHOOTING.md",
            "STRICT_TESTING.md",
            "RURR_PARITY.md",
        ];
        let missing = required
            .into_iter()
            .filter(|name| {
                !self.root.join(name).is_file() || !index.contains(&format!("]({name})"))
            })
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "missing or unlinked guides: {missing:?}"
        );
    }

    fn assert_local_links(&self) {
        let files = self.files("md");
        assert!(
            !files.is_empty(),
            "published guide inventory must be nonempty"
        );
        for file in files {
            let contents = std::fs::read_to_string(&file).expect("guide is readable UTF-8");
            // The published guides use ordinary inline Markdown links. This
            // checks local file targets; it does not claim anchor resolution,
            // external URL availability, or prose completeness.
            for suffix in contents.split("](").skip(1) {
                let target = suffix.split_once(')').expect("inline link is closed").0;
                if target.starts_with('#') || target.contains("://") {
                    continue;
                }
                let relative = target.split('#').next().unwrap().trim_matches(['<', '>']);
                assert!(
                    file.parent().unwrap().join(relative).is_file(),
                    "{} has a missing local target {target}",
                    file.display()
                );
            }
        }
    }

    fn assert_workflow_examples(&self) {
        let directory = self.root.join("examples");
        assert!(
            directory.is_dir(),
            "documented workflow JSON corpus is missing"
        );
        let examples = self
            .files("json")
            .into_iter()
            .filter(|path| path.starts_with(&directory))
            .collect::<Vec<_>>();
        assert!(!examples.is_empty(), "workflow corpus must be nonempty");
        for path in examples {
            let source = std::fs::read_to_string(&path).expect("workflow example is readable");
            WorkflowDefinition::parse(&source)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        }
    }

    fn assert_workflow_example_inventory(&self) {
        let required = [
            "linear_state",
            "expressions_loop",
            "multi_agent_sequential",
            "multi_agent_parallel",
            "input",
            "invoice_controls",
            "child",
            "child_counter",
            "map",
            "joins",
            "retry",
            "sleep",
            "pipeline",
        ];
        let missing = required
            .into_iter()
            .filter(|name| {
                !self
                    .root
                    .join("examples")
                    .join(format!("{name}.json"))
                    .is_file()
            })
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "missing downstream workflow examples: {missing:?}"
        );
    }

    fn assert_executable_examples(&self) {
        let index = std::fs::read_to_string(self.root.join("README.md")).unwrap();
        for name in [
            "pure_state",
            "multi_agent",
            "actions_and_inputs",
            "controls_and_streaming",
            "history_and_recovery",
            "state",
            "graph",
            "performance",
            "child_composition",
        ] {
            let relative = format!("../examples/{name}.rs");
            assert!(
                self.root.join(&relative).is_file() && index.contains(&format!("]({relative})")),
                "missing or unlinked executable example: {name}"
            );
        }
    }

    fn definitions(&self, name: &str, dependencies: &[&str]) -> Vec<WorkflowDefinition> {
        std::iter::once(name)
            .chain(dependencies.iter().copied())
            .map(|name| {
                let path = self.root.join("examples").join(format!("{name}.json"));
                let source = std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
                WorkflowDefinition::parse(&source).expect("documented workflow parses")
            })
            .collect()
    }

    async fn assert_native_workflow(
        &self,
        name: &str,
        dependencies: &[&str],
        options: ExecutionOptions,
        expected: Value,
    ) {
        let definitions = self.definitions(name, dependencies);
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        // No Agent is constructed or invoked for these native-only journeys.
        // Direct caller Agent integration has its own public contract tests.
        let mut app = App::open(AppConfig::<_, Agent>::new(store, Vec::new(), definitions))
            .await
            .unwrap();
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some(name.to_owned()),
                    root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                })
                .await
                .unwrap();
            let workflow = workspace.workflow(name.parse().unwrap()).await.unwrap();
            let run = workflow.start(options).await.unwrap();
            let terminal = run.wait().await.unwrap();
            assert!(matches!(terminal.status, RunStatus::Succeeded), "{name}");
            assert_eq!(
                terminal.state.values(),
                expected.as_object().unwrap(),
                "{name}"
            );
        }))
        .catch_unwind()
        .await;
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match observed {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(result) => result.expect("documented workflow finishes within its test budget"),
        }
        closed
            .expect("App cleanup is bounded")
            .expect("App cleanup succeeds");
    }
}

#[test]
fn required_downstream_guides_are_present_and_linked() {
    Documentation::new().assert_guide_inventory();
}

#[test]
fn published_guide_local_file_links_resolve() {
    Documentation::new().assert_local_links();
}

#[test]
fn every_documented_workflow_example_parses() {
    Documentation::new().assert_workflow_examples();
}

#[test]
fn required_workflow_example_categories_have_complete_documents() {
    Documentation::new().assert_workflow_example_inventory();
}

#[test]
fn required_executable_examples_are_present_and_linked() {
    Documentation::new().assert_executable_examples();
}

#[tokio::test]
async fn documented_linear_state_workflow_produces_its_explained_result() {
    Documentation::new()
        .assert_native_workflow(
            "linear_state",
            &[],
            ExecutionOptions::default(),
            json!({"count": 2, "label": "complete"}),
        )
        .await;
}

#[tokio::test]
async fn documented_loop_uses_pinned_numeric_semantics() {
    Documentation::new()
        .assert_native_workflow(
            "expressions_loop",
            &[],
            ExecutionOptions::default(),
            json!({"count": 3, "quotient": 2.5, "remainder": 1}),
        )
        .await;
}

#[tokio::test]
async fn documented_sleep_continues_to_its_next_node() {
    Documentation::new()
        .assert_native_workflow(
            "sleep",
            &[],
            ExecutionOptions::default(),
            json!({"awake": true}),
        )
        .await;
}

#[tokio::test]
async fn documented_child_reduces_its_final_state() {
    Documentation::new()
        .assert_native_workflow(
            "child",
            &["child_counter"],
            ExecutionOptions::default(),
            json!({"sum": 5}),
        )
        .await;
}

#[tokio::test]
async fn documented_map_handles_empty_and_bounded_inputs_with_one_definition() {
    for (items, sum) in [(json!([]), 0), (json!([1, 2, 3]), 9)] {
        Documentation::new()
            .assert_native_workflow(
                "map",
                &["child_counter"],
                ExecutionOptions {
                    state: WorkflowState::new(serde_json::Map::from_iter([(
                        "items".into(),
                        items.clone(),
                    )]))
                    .unwrap(),
                    ..ExecutionOptions::default()
                },
                json!({"items": items, "sum": sum}),
            )
            .await;
    }
}

#[tokio::test]
async fn documented_join_retains_both_canonical_sibling_contributions() {
    Documentation::new()
        .assert_native_workflow(
            "joins",
            &[],
            ExecutionOptions::default(),
            json!({"values": ["left", "right"]}),
        )
        .await;
}

#[test]
fn documented_pipeline_hands_off_state_through_synchronous_execution() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut app = runtime.block_on(async {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        App::open(AppConfig::<_, Agent>::new(
            store,
            Vec::new(),
            Documentation::new().definitions("pipeline", &["child_counter"]),
        ))
        .await
        .unwrap()
    });
    let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut run = runtime.block_on(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some("documented pipeline".into()),
                    root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("pipeline".parse().unwrap())
                .await
                .unwrap();
            workflow.prepare(WorkflowState::default()).await.unwrap()
        });
        // The multithread runtime remains alive while this caller is outside
        // its entered context. Pipeline executes only on this sync lane.
        let terminal = SyncExecution::open(&mut run, ExecutionOptions::default())
            .unwrap()
            .finish()
            .unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(
            terminal.state.values(),
            json!({"sum":11}).as_object().unwrap()
        );
        assert_eq!(terminal.last_action_ordinal, Some(0));
        assert_eq!(terminal.outcomes.len(), 1);
        assert_eq!(
            &terminal.outcomes[0].result,
            json!({
                "child_states":[{"value":5},{"value":6}], "final_state":{"value":6},
            })
            .as_object()
            .unwrap()
        );
    }));
    let closed = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await
    });
    drop(app);
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
    closed
        .expect("documented pipeline cleanup is bounded")
        .unwrap();
}
