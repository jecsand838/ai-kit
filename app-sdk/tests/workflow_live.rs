mod lance_conflict;
mod live_support;
mod owned_child;

use std::{collections::BTreeSet, error::Error, fs, num::NonZeroUsize, path::PathBuf};
use std::{
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use agent_sdk::error::{ErrorDetail, ErrorKind, ErrorPhase, Operation};
use agent_sdk::identity::ConversationId;
use agent_sdk::knowledge::{IngestKnowledgeRequest, KnowledgeScope, KnowledgeSource};
use agent_sdk::notification::{ConversationNotificationKind, NotificationResolution};
use agent_sdk::workspace::NewWorkspace;
use agent_sdk::{Agent, AgentBackend, AgentStorage, WorkspaceBackend};
use app_sdk::workflow::{
    ExecutionOptions, Interaction, Resolution, RunId, RunSelector, RunSnapshot, RunStatus,
    Workflow, WorkflowFailure, WorkflowRun, WorkflowState,
};
use app_sdk::{AgentSpec, App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::FutureExt;
use serde_json::{Map, Value, json};
use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{StateKey, StateRecord};
use storage_sdk::{Database, StateStore, StateStoreBackend, StateTransactionBackend};

use crate::lance_conflict::ConflictStore;
use crate::live_support::{ForwardingControl, OwnedOpenCode};
use crate::owned_child::OwnedChild;

const RECOVERY_TEST: &str = "durable_reopen_cancellation_child_map_and_shutdown_converge";
const RECOVERY_SCENARIO: &str = "durable-input-map";
const SHUTDOWN_TEST: &str = "crash_reopen_after_shutdown_escalation_converges";
const SHUTDOWN_SCENARIO: &str = "durable-shutdown-escalation";
const BLOCKED_RECOVERY_TEST: &str =
    "wait_projects_retained_notification_while_real_recovery_is_blocked";
const BLOCKED_RECOVERY_SCENARIO: &str = "blocked-notification-recovery";
const TWO_AGENT_TEST: &str = "two_real_agents_handoff_after_process_loss_and_drain_once";
const TWO_AGENT_SCENARIO: &str = "two-agent-handoff";
const CHILD_SCENARIO: &str = "APP_SDK_LIVE_CHILD_SCENARIO";
const CHILD_ROOT: &str = "APP_SDK_LIVE_CHILD_ROOT";
const CHILD_ENDPOINT: &str = "APP_SDK_LIVE_CHILD_ENDPOINT";
const CHILD_SECONDARY_ROOT: &str = "APP_SDK_LIVE_CHILD_SECONDARY_ROOT";
const CHILD_SECONDARY_ENDPOINT: &str = "APP_SDK_LIVE_CHILD_SECONDARY_ENDPOINT";
const CHILD_READY: &str = "shutdown-ready";
const CHILD_GO: &str = "shutdown-go";
const CHILD_PROBE: &str = "recovery-probe.json";

struct RecoveryJourney {
    service: OwnedOpenCode,
}

impl RecoveryJourney {
    const HANDOFF_PROMPT: &'static str = r#"Parse the JSON object below. Read its "token" field, which is a string.
Return a structured object with "received" equal to ONLY that string value and "agent" equal to "secondary".
Do not put the whole input object, braces, quotes, or the field name inside "received".
Example: input {"token":"abc-123"} requires {"received":"abc-123","agent":"secondary"}.
Use the actual token from the following input, preserving every character:
INPUT_JSON:
"#;

    fn parallel_isolation_workflow() -> String {
        let prompt = r#"Read the integration isolation marker from the supplied knowledge context.
First use OpenCode's built-in bash tool exactly once to execute this literal command:
printf '%s\n' "$AI_KIT_TOOL_CANARY" >> "$AI_KIT_TOOL_RECEIPT"; printf '%s\n' "$AI_KIT_TOOL_CANARY"
The tool output is the tool marker. Do not guess either marker.
Next use OpenCode's built-in question tool exactly once with this argument:
{"questions":[{"header":"Continue","question":"Continue?","options":[{"label":"Yes","description":"Continue"},{"label":"No","description":"Stop"}],"multiple":false}]}
Wait for the actual question reply. Then return the structured object with tool equal to the exact tool marker and knowledge equal to the exact integration isolation marker. Preserve every character of both markers, omit surrounding whitespace, and do not include any other marker."#;
        let nodes = [("primary", "opencode"), ("secondary", "secondary")]
            .into_iter()
            .map(|(id, agent)| {
                json!({"id":id, "step":{
                    "kind":"agent", "agent":agent,
                    "prompt":[{"kind":"text", "text":prompt}],
                    "structured_schema":{
                        "type":"object", "properties":{
                            "tool":{"type":"string"}, "knowledge":{"type":"string"}
                        }, "required":["tool","knowledge"], "additionalProperties":false
                    }, "structured_target":id
                }})
            });
        let mut all_nodes = vec![json!({"id":"seed", "step":{"kind":"state", "mutations":[]}})];
        all_nodes.extend(nodes);
        json!({
            "schema":"app-sdk.workflow/v2", "id":"parallel-isolation", "revision":"v1",
            "state":{"primary":null,"secondary":null}, "required":["primary","secondary"],
            "reducers":{"primary":"reject_concurrent","secondary":"reject_concurrent"},
            "entry":"seed", "nodes":all_nodes,
            "routes":{
                "seed":{"mode":"broadcast", "branches":[
                    {"target":{"kind":"node", "id":"primary"}},
                    {"target":{"kind":"node", "id":"secondary"}}
                ]},
                "primary":{"mode":"first", "branches":[{"target":{"kind":"end"}}]},
                "secondary":{"mode":"first", "branches":[{"target":{"kind":"end"}}]}
            }
        })
        .to_string()
    }

    fn two_agent_workflow() -> String {
        json!({
            "schema": "app-sdk.workflow/v2", "id": "two-agent-handoff", "revision": "v1",
            "state": {"seed": "", "artifact": null, "confirmed": false, "result": null},
            "required": ["seed", "artifact", "confirmed", "result"],
            "reducers": {
                "seed": "reject_concurrent", "artifact": "reject_concurrent",
                "confirmed": "reject_concurrent", "result": "reject_concurrent"
            },
            "entry": "author",
            "nodes": [
                {"id": "author", "step": {
                    "kind": "agent", "agent": "opencode",
                    "prompt": [
                        {"kind": "text", "text": "Return a structured object with token equal to this exact value: "},
                        {"kind": "state", "path": "seed"}
                    ],
                    "structured_schema": {
                        "type": "object", "properties": {"token": {"type": "string"}},
                        "required": ["token"], "additionalProperties": false
                    },
                    "structured_target": "artifact"
                }},
                {"id": "handoff", "step": {
                    "kind": "input", "message": [{"kind": "text", "text": "Release the committed artifact to the second agent?"}],
                    "schema": {"type": "boolean", "const": true}, "target": "confirmed"
                }},
                {"id": "consumer", "step": {
                    "kind": "agent", "agent": "secondary",
                    "prompt": [{"kind": "text", "text": Self::HANDOFF_PROMPT}, {"kind": "state", "path": "artifact"}],
                    "structured_schema": {
                        "type": "object", "properties": {
                            "received": {"type": "string"}, "agent": {"const": "secondary"}
                        }, "required": ["received", "agent"], "additionalProperties": false
                    },
                    "structured_target": "result"
                }}
            ],
            "routes": {
                "author": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "handoff"}}]},
                "handoff": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "consumer"}}]},
                "consumer": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
            }
        }).to_string()
    }

    const MAP_CHILD_WORKFLOW: &'static str = r#"
{
  "schema": "app-sdk.workflow/v2",
  "id": "map-child",
  "revision": "v1",
  "state": {"item": 0, "confirmed": false, "result": 0},
  "required": ["item", "confirmed", "result"],
  "reducers": {
    "item": "reject_concurrent",
    "confirmed": "reject_concurrent",
    "result": "reject_concurrent"
  },
  "entry": "confirm",
  "nodes": [
    {
      "id": "confirm",
      "step": {
        "kind": "input",
        "message": [{"kind": "text", "text": "Confirm this child."}],
        "schema": {"type": "boolean"},
        "target": "confirmed"
      }
    },
    {
      "id": "copy",
      "step": {
        "kind": "state",
        "mutations": [
          {
            "kind": "assign",
            "target": "result",
            "value": {"kind": "state", "path": "item"}
          }
        ]
      }
    }
  ],
  "routes": {
    "confirm": {
      "mode": "first",
      "branches": [{"target": {"kind": "node", "id": "copy"}}]
    },
    "copy": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
  }
}
"#;

    const MAP_PARENT_WORKFLOW: &'static str = r#"
{
  "schema": "app-sdk.workflow/v2",
  "id": "map-parent",
  "revision": "v1",
  "state": {"total": 0},
  "required": ["total"],
  "reducers": {"total": "sum_integer"},
  "entry": "map",
  "nodes": [
    {
      "id": "map",
      "step": {
        "kind": "map",
        "workflow": "map-child",
        "items": {"kind": "literal", "value": [1, 2, 3, 4]},
        "input_key": "item",
        "input": {},
        "max_parallel": 2,
        "reductions": [
          {"source": "result", "target": "total", "reducer": "sum_integer"}
        ]
      }
    }
  ],
  "routes": {
    "map": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
  }
}
"#;

    fn child_root() -> PathBuf {
        PathBuf::from(OwnedOpenCode::required(CHILD_ROOT))
    }

    async fn recovery_app_config(root: PathBuf, endpoint: String) -> AppConfig<StateStore, Agent> {
        let agent = OwnedOpenCode::build_agent(&root, &endpoint, "agent-state").await;
        Self::recovery_app_config_with_agent(OwnedOpenCode::connect_app_store(&root).await, agent)
    }

    fn recovery_app_config_with_agent<CS: StateStoreBackend, A: AgentBackend>(
        store: CS,
        agent: A,
    ) -> AppConfig<CS, A> {
        let failing_map_parent = Self::MAP_PARENT_WORKFLOW
            .replace("\"id\": \"map-parent\"", "\"id\": \"map-parent-timeout\"")
            .replace(
                "\"id\": \"map\",",
                "\"id\": \"map\",\n      \"deadline_ms\": 5000,",
            )
            .replace("[1, 2, 3, 4]", "[5, 6]");
        let retry_question = OwnedOpenCode::QUESTION_WORKFLOW
            .replace(
                "\"id\": \"agent-question\"",
                "\"id\": \"agent-question-retry\"",
            )
            .replace(
                "\"id\": \"ask-agent\",",
                "\"id\": \"ask-agent\", \"retry\": {\"kind\": \"immediate\", \"max_attempts\": 2},",
            )
            .replace(
                "exactly once and do not invoke any other tool.",
                "exactly once.",
            )
            .replace(
                "Then output exactly `Acknowledged.` and stop.",
                "Then use OpenCode's built-in `bash` tool exactly once to run `sleep 30`. Wait for that tool call to finish. Then output exactly `Acknowledged.` and stop.",
            );
        AppConfig::new(
            store,
            vec![AgentSpec::new(
                "opencode".to_owned(),
                "strict-live-v1".to_owned(),
                agent,
            )],
            OwnedOpenCode::definitions(&[
                OwnedOpenCode::INPUT_WORKFLOW,
                Self::MAP_CHILD_WORKFLOW,
                Self::MAP_PARENT_WORKFLOW,
                &failing_map_parent,
                OwnedOpenCode::QUESTION_WORKFLOW,
                &retry_question,
            ]),
        )
    }

    async fn app_config<S: StateStoreBackend>(
        root: &std::path::Path,
        agent: Agent<S>,
    ) -> AppConfig<StateStore, Agent<S>> {
        AppConfig::new(
            OwnedOpenCode::connect_app_store(root).await,
            vec![AgentSpec::new(
                "opencode".to_owned(),
                "strict-live-v1".to_owned(),
                agent,
            )],
            Vec::new(),
        )
    }

    async fn two_agent_config<A: AgentBackend>(
        root: &std::path::Path,
        revision: &str,
        primary: A,
        secondary: A,
        sources: &[&str],
    ) -> AppConfig<StateStore, A> {
        Self::two_agent_config_with_state(
            OwnedOpenCode::connect_app_store(root).await,
            revision,
            primary,
            secondary,
            sources,
        )
    }

    fn two_agent_config_with_state<A: AgentBackend>(
        state: StateStore,
        revision: &str,
        primary: A,
        secondary: A,
        sources: &[&str],
    ) -> AppConfig<StateStore, A> {
        AppConfig::new(
            state,
            vec![
                AgentSpec::new("opencode".to_owned(), revision.to_owned(), primary),
                AgentSpec::new(
                    "secondary".to_owned(),
                    "strict-live-v1".to_owned(),
                    secondary,
                ),
            ],
            OwnedOpenCode::definitions(sources),
        )
    }

    fn report_cleanup<T: std::fmt::Debug>(
        label: &str,
        observed: &Result<T, Box<dyn std::any::Any + Send>>,
    ) {
        match observed {
            Ok(result) => eprintln!("strict-live {label} cleanup: {result:?}"),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("opaque panic payload");
                eprintln!("strict-live {label} cleanup panicked: {message}");
            }
        }
    }

    async fn fail_unexpected_unknown<A: AgentBackend>(
        run: &WorkflowRun<StateStore, A>,
        snapshot: &RunSnapshot,
    ) {
        let [
            Interaction::Unknown {
                sequence,
                generation,
                ..
            },
        ] = snapshot.interactions.as_slice()
        else {
            return;
        };
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(30),
            run.resolve(*sequence, *generation, Resolution::Fail),
        ))
        .catch_unwind()
        .await;
        match cleanup {
            Ok(Ok(Ok(settled))) => eprintln!(
                "strict-live unexpected Unknown cleanup: sequence={sequence} generation={generation} settled_checkpoint={}",
                settled.checkpoint
            ),
            Ok(Ok(Err(error))) => eprintln!(
                "strict-live unexpected Unknown cleanup failed: sequence={sequence} generation={generation} error={error:?}"
            ),
            Ok(Err(_)) => eprintln!(
                "strict-live unexpected Unknown cleanup observation expired: sequence={sequence} generation={generation}; App retains any admitted resolution"
            ),
            Err(_) => eprintln!(
                "strict-live unexpected Unknown cleanup panicked: sequence={sequence} generation={generation}"
            ),
        }
    }

    async fn drain<A: AgentBackend>(app: &mut App<StateStore, A>) -> Result<(), AppError> {
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(30),
        })
        .await
    }

    fn write_child_probe(value: &Value) {
        let encoded =
            serde_json::to_vec(value).expect("the strict-live recovery probe must encode");
        let published = Self::child_root().join(CHILD_PROBE);
        let pending = published.with_extension("pending");
        fs::write(&pending, encoded)
            .expect("the complete strict-live recovery probe must be written");
        fs::rename(pending, published)
            .expect("probe existence must publish a complete recovery boundary");
    }

    fn read_probe(&self) -> Value {
        let encoded = fs::read(self.service.durable_root().join(CHILD_PROBE))
            .expect("the strict-live recovery probe must exist");
        serde_json::from_slice(&encoded)
            .expect("the strict-live recovery probe must contain valid JSON")
    }

    fn recovery_child_command(&self, test_name: &str, scenario: &str) -> Command {
        let mut child = Command::new(
            std::env::current_exe().expect("the strict-live test executable must be discoverable"),
        );
        child
            .arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_SCENARIO, scenario)
            .env(CHILD_ROOT, self.service.durable_root())
            .env(CHILD_ENDPOINT, self.service.endpoint())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        child
    }

    fn spawn_recovery_child(&self, test_name: &str, scenario: &str) -> Child {
        self.recovery_child_command(test_name, scenario)
            .spawn()
            .expect("the strict-live recovery child process must start")
    }

    async fn wait_recovery_child(&self, mut child: Child) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if let Some(status) = child
                .try_wait()
                .expect("the strict-live recovery child state must be readable")
            {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let reap_deadline = Instant::now() + Duration::from_secs(2);
                while Instant::now() < reap_deadline {
                    if child.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                panic!("the strict-live recovery child exceeded its process bound");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_child_boundary(
        &self,
        mut child: Child,
        marker: &str,
        require_active_session: bool,
    ) -> Child {
        let deadline = Instant::now() + Duration::from_secs(360);
        while !self.service.durable_root().join(marker).exists()
            || require_active_session && !self.service.has_active_session()
        {
            assert!(
                child
                    .try_wait()
                    .expect("the recovery child state must be readable")
                    .is_none(),
                "the recovery child exited before its boundary"
            );
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = self.wait_recovery_child(child).await;
                panic!("the recovery child did not reach its boundary in time");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        child
    }

    async fn wait_parent_release() {
        fs::write(Self::child_root().join(CHILD_READY), b"ready")
            .expect("the child readiness boundary must persist");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !Self::child_root().join(CHILD_GO).exists() {
            assert!(
                Instant::now() < deadline,
                "the parent must release the recovery child in time"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    async fn new(label: &str) -> Self {
        let mut service = OwnedOpenCode::new(label);
        service.start().await;
        Self { service }
    }

    async fn reopen_after_process_loss(
        &self,
    ) -> Result<App<StateStore, Agent>, Box<dyn Error + Send + Sync>> {
        let store =
            OwnedOpenCode::connect_agent_store(self.service.durable_root(), "agent-state").await;
        let inventory = AgentStorage::new(store.clone()).list_agents().await?;
        let [original] = inventory.as_slice() else {
            panic!("the killed child retains exactly one original Agent identity")
        };
        let original = original.id().clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut builds = tokio::task::JoinSet::new();
        let mut repaired = false;
        let mut attempt = 0;
        let agent = loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "caller reconstruction admission expired"
            );
            let endpoint = self.service.endpoint();
            let store = store.clone();
            let id = original.clone();
            builds
                .spawn(async move { OwnedOpenCode::try_reopen_agent(&endpoint, store, id).await });
            // Joining the failed construction precedes another caller attempt.
            // No observation timeout drops a still-owned builder here.
            let outcome = builds
                .join_next()
                .await
                .expect("one caller construction is owned")
                .expect("caller construction must join without panic");
            attempt += 1;
            match outcome {
                Ok(agent) => break agent,
                Err(error) => {
                    eprintln!(
                        "strict-live process-loss reconstruction attempt={attempt}: {error:?}"
                    );
                    let clean = error.supplementary().is_empty()
                        && !error.diagnostics_truncated()
                        && error.cause().is_none()
                        && error.http_status().is_none();
                    if !repaired
                        && clean
                        && error.kind() == ErrorKind::Conflict
                        && error.detail() == Some(&ErrorDetail::TransactionConflict)
                        && error.context().operation == Operation::Reopen
                        && error.context().phase == ErrorPhase::StateCommit
                    {
                        continue;
                    }
                    if !repaired
                        && clean
                        && error.kind() == ErrorKind::InvalidState
                        && error.detail().is_none()
                        && error.context().operation == Operation::Reopen
                        && error.context().phase == ErrorPhase::Lifecycle
                        && error.message()
                            == "runtime work ended without a matching terminal response"
                    {
                        // Exactly one explicit reconstruction may follow the
                        // existing integrity-repair report. Its next failure
                        // cannot enter either retry branch.
                        repaired = true;
                        continue;
                    }
                    return Err(error.into());
                }
            }
        };
        assert_eq!(
            agent.id(),
            original,
            "caller recovery never creates a substitute Agent"
        );
        let config = Self::recovery_app_config_with_agent(
            OwnedOpenCode::connect_app_store(self.service.durable_root()).await,
            agent,
        );
        Ok(App::open(config).await?)
    }

    async fn create_workspace<A: AgentBackend>(
        app: &App<StateStore, A>,
        name: &str,
        root: PathBuf,
    ) -> Result<app_sdk::Workspace<StateStore, A>, AppError> {
        app.create_workspace(WorkspaceConfig {
            name: Some(name.to_owned()),
            root,
        })
        .await
    }

    async fn workflow<A: AgentBackend>(
        workspace: &app_sdk::Workspace<StateStore, A>,
        id: &str,
    ) -> Workflow<StateStore, A> {
        workspace
            .workflow(id.parse().expect("the fixture workflow id must parse"))
            .await
            .expect("the fixture workflow must open")
    }

    async fn start<A: AgentBackend>(
        workspace: &app_sdk::Workspace<StateStore, A>,
        id: &str,
    ) -> app_sdk::workflow::WorkflowRun<StateStore, A> {
        Self::workflow(workspace, id)
            .await
            .start(ExecutionOptions::default())
            .await
            .expect("the fixture workflow must start")
    }

    async fn child_workspace(
        app: &App<StateStore, Agent>,
        name: &str,
    ) -> Result<app_sdk::Workspace<StateStore, Agent>, AppError> {
        Self::create_workspace(app, name, Self::child_root().join("workspace")).await
    }

    async fn submit_yes<A: AgentBackend>(
        run: &app_sdk::workflow::WorkflowRun<StateStore, A>,
        sequence: u64,
        generation: u64,
    ) -> Result<RunSnapshot, AppError> {
        run.resolve(
            sequence,
            generation,
            Resolution::Notification(NotificationResolution::Submit {
                content: json!({"answers": [["Yes"]]}),
            }),
        )
        .await
    }

    async fn wait(run: &WorkflowRun<StateStore, Agent>) -> RunSnapshot {
        tokio::time::timeout(Duration::from_secs(120), run.wait())
            .await
            .expect("the strict-live run must become observable in time")
            .expect("the strict-live run observation must succeed")
    }

    async fn seed_two_agent_child() {
        let primary = ForwardingControl::observation();
        let secondary = ForwardingControl::observation();
        let definition = Self::two_agent_workflow();
        let config = Self::two_agent_config(
            &Self::child_root(),
            "strict-live-v1",
            primary.wrap(
                OwnedOpenCode::build_agent(
                    &Self::child_root(),
                    &OwnedOpenCode::required(CHILD_ENDPOINT),
                    "agent-state",
                )
                .await,
            ),
            secondary.wrap(
                OwnedOpenCode::build_agent(
                    &PathBuf::from(OwnedOpenCode::required(CHILD_SECONDARY_ROOT)),
                    &OwnedOpenCode::required(CHILD_SECONDARY_ENDPOINT),
                    "agent-state",
                )
                .await,
            ),
            &[&definition],
        )
        .await;
        let app = App::open(config)
            .await
            .expect("the two-agent child App must open");
        let workspace = Self::create_workspace(
            &app,
            "two-agent-handoff",
            Self::child_root().join("workspace"),
        )
        .await
        .expect("both real agents must materialize the App workspace");
        let seed = format!("handoff-{}", std::process::id());
        let run = Self::workflow(&workspace, "two-agent-handoff")
            .await
            .start(ExecutionOptions {
                state: WorkflowState::new(Map::from_iter([(
                    "seed".to_owned(),
                    Value::String(seed.clone()),
                )]))
                .unwrap(),
                ..ExecutionOptions::default()
            })
            .await
            .expect("the two-agent workflow must start");
        let snapshot = tokio::time::timeout(Duration::from_secs(120), run.wait())
            .await
            .expect("the first real agent must reach the handoff boundary in time")
            .expect("the real handoff boundary must be observable");
        assert!(matches!(
            snapshot.interactions.as_slice(),
            [Interaction::Input { .. }]
        ));
        assert_eq!(
            snapshot.state.values().get("artifact"),
            Some(&json!({"token": seed}))
        );
        assert_eq!(primary.prompt_count(), 1);
        assert_eq!(secondary.prompt_count(), 0);
        Self::write_child_probe(&json!({
            "workspace": workspace.id().to_string(), "run": run.id().to_string(),
            "seed": seed, "state": snapshot.state.values(), "checkpoint": snapshot.checkpoint,
            "primary_prompts": primary.prompt_count(), "secondary_prompts": secondary.prompt_count()
        }));
        std::future::pending::<()>().await;
    }

    async fn seed_child() {
        let config =
            Self::recovery_app_config(Self::child_root(), OwnedOpenCode::required(CHILD_ENDPOINT))
                .await;
        let app = App::open(config)
            .await
            .expect("the abrupt recovery child App must open");
        let workspace = Self::child_workspace(&app, "abrupt-recovery")
            .await
            .expect("the abrupt recovery child workspace must materialize");
        let input_run = Self::start(&workspace, "caller-input").await;
        let input_snapshot = Self::wait(&input_run).await;
        assert!(matches!(
            input_snapshot.interactions.as_slice(),
            [Interaction::Input { .. }]
        ));

        let map_run = Self::start(&workspace, "map-parent").await;
        let children = Self::workflow(&workspace, "map-child").await;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let admitted = children
                .list_runs(None, NonZeroUsize::new(16).expect("sixteen is nonzero"))
                .await
                .expect("the admitted child inventory must be readable");
            if !admitted.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the map must durably admit a child before abrupt exit"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let retry_run = Self::start(&workspace, "agent-question-retry").await;
        let waiting = Self::wait(&retry_run).await;
        let [
            Interaction::Notification {
                sequence,
                generation,
                ..
            },
        ] = waiting.interactions.as_slice()
        else {
            panic!("the retry prompt must expose its real notification")
        };
        Self::wait_parent_release().await;
        let running = Self::submit_yes(&retry_run, *sequence, *generation)
            .await
            .expect("the retry prompt notification must resolve");
        assert!(
            matches!(&running.status, RunStatus::Running),
            "the retry prompt must resume before its crash"
        );
        Self::write_child_probe(&json!({
            "workspace": workspace.id().to_string(),
            "input": input_run.id().to_string(),
            "map": map_run.id().to_string(),
            "retry": retry_run.id().to_string(),
            "notification_sequence": sequence,
            "notification_generation": generation,
            "notification_applied": {
                "checkpoint": running.checkpoint,
                "state": running.state.values(),
                "last_action_ordinal": running.last_action_ordinal
            }
        }));
        std::future::pending::<()>().await
    }

    async fn seed_notification_child() {
        let config =
            Self::recovery_app_config(Self::child_root(), OwnedOpenCode::required(CHILD_ENDPOINT))
                .await;
        let app = App::open(config)
            .await
            .expect("the notification recovery child App must open");
        let workspace = Self::child_workspace(&app, "blocked-notification-recovery")
            .await
            .expect("the notification recovery child workspace must materialize");
        let run = Self::start(&workspace, "agent-question").await;
        let waiting = Self::wait(&run).await;
        let [
            Interaction::Notification {
                sequence,
                generation,
                notification,
            },
        ] = waiting.interactions.as_slice()
        else {
            panic!("the recovery child must expose its real notification")
        };
        let inserted_at = notification
            .inserted_at()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the real notification timestamp follows the Unix epoch");
        let ConversationNotificationKind::Interaction {
            title,
            body,
            response_schema,
            dismissible,
        } = notification.kind()
        else {
            panic!("the recovery child records the original portable interaction")
        };
        Self::write_child_probe(&json!({
            "workspace": workspace.id().to_string(),
            "run": run.id().to_string(),
            "sequence": sequence,
            "generation": generation,
            "notification": notification.id().to_string(),
            "inserted_at": {"seconds":inserted_at.as_secs(),"nanoseconds":inserted_at.subsec_nanos()},
            "payload": {"title":title,"body":body,"response_schema":response_schema,"dismissible":dismissible}
        }));
        std::future::pending::<()>().await
    }

    async fn seed_shutdown_child() {
        let config =
            Self::recovery_app_config(Self::child_root(), OwnedOpenCode::required(CHILD_ENDPOINT))
                .await;
        let app = App::open(config)
            .await
            .expect("the shutdown recovery child App must open");
        let workspace = Self::child_workspace(&app, "shutdown-recovery")
            .await
            .expect("the shutdown recovery workspace must materialize");
        let run = Self::start(&workspace, "caller-input").await;
        assert!(matches!(
            Self::wait(&run).await.interactions.as_slice(),
            [Interaction::Input { .. }]
        ));
        let mut deleting = Self::child_workspace(&app, "deleting-recovery")
            .await
            .expect("the deleting recovery workspace must materialize");
        let deleting_run = Self::start(&deleting, "agent-question").await;
        assert!(matches!(
            Self::wait(&deleting_run).await.interactions.as_slice(),
            [Interaction::Notification { .. }]
        ));
        let deleting_revision = deleting
            .snapshot()
            .await
            .expect("the deleting workspace revision must be readable")
            .revision;
        Self::wait_parent_release().await;
        let cancelling = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let current = deleting_run
                    .snapshot()
                    .await
                    .expect("service-loss cancellation must remain observable");
                match deleting_run.cancel(current.checkpoint).await {
                    Ok(snapshot) => break snapshot,
                    Err(AppError::Busy(_) | AppError::Conflict(_)) => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    Err(error) => panic!("service loss must preserve cancellation: {error}"),
                }
            }
        })
        .await
        .expect("service-loss cancellation must converge in time");
        let unknown = if matches!(
            cancelling.interactions.as_slice(),
            [Interaction::Unknown { .. }]
        ) {
            cancelling
        } else {
            Self::wait(&deleting_run).await
        };
        let [Interaction::Unknown { sequence, .. }] = unknown.interactions.as_slice() else {
            panic!("service loss must preserve truthful abort ambiguity")
        };
        Self::write_child_probe(&json!({
            "workspace": workspace.id().to_string(),
            "run": run.id().to_string(),
            "deleting_workspace": deleting.id().to_string(),
            "deleting_run": deleting_run.id().to_string(),
            "deleting_revision": deleting_revision,
            "unknown_sequence": sequence
        }));
        let deletion =
            tokio::time::timeout(Duration::from_secs(5), deleting.delete(deleting_revision))
                .await
                .expect("truthful unknown deletion must return within its operation bound");
        assert!(matches!(deletion, Err(AppError::Conflict(_))));
        std::process::exit(0)
    }

    async fn finish_map(
        workspace: &app_sdk::Workspace<StateStore, Agent>,
        parent_id: RunId,
    ) -> (RunSnapshot, Vec<RunSnapshot>) {
        let parent_workflow = Self::workflow(workspace, "map-parent").await;
        let child_workflow = Self::workflow(workspace, "map-child").await;
        let parent = parent_workflow
            .open_run(RunSelector::Latest(parent_id))
            .await
            .expect("the recovered map parent must open");
        let mut resolved = BTreeSet::new();
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let children = child_workflow
                .list_runs(None, NonZeroUsize::new(16).expect("sixteen is nonzero"))
                .await
                .expect("the recovered map child inventory must be readable");
            for child in &children {
                let [
                    Interaction::Input {
                        sequence,
                        generation,
                        ..
                    },
                ] = child.interactions.as_slice()
                else {
                    continue;
                };
                if !resolved.insert((child.id.clone(), child.occurrence.clone())) {
                    continue;
                }
                child_workflow
                    .open_run(RunSelector::Exact {
                        id: child.id.clone(),
                        occurrence: child.occurrence.clone(),
                    })
                    .await
                    .expect("an admitted map child must reopen")
                    .resolve(*sequence, *generation, Resolution::Input(Value::Bool(true)))
                    .await
                    .expect("an admitted map child input must resolve");
            }
            let snapshot = parent
                .snapshot()
                .await
                .expect("the recovered map parent snapshot must load");
            if matches!(
                snapshot.status,
                RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled
            ) {
                let children = child_workflow
                    .list_runs(None, NonZeroUsize::new(16).expect("sixteen is nonzero"))
                    .await
                    .expect("the terminal map child inventory must be readable");
                return (snapshot, children);
            }
            assert!(
                Instant::now() < deadline,
                "the recovered map and its children must converge"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn probe_id(value: &Value, key: &str) -> String {
        value
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("the recovery probe must contain `{key}`"))
            .to_owned()
    }
}

#[tokio::test]
async fn caller_input_and_exact_real_agent_notification_resolve_publicly()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut journey = RecoveryJourney::new("interactions").await;
    let control = ForwardingControl::observation();
    let agent = OwnedOpenCode::build_agent(
        journey.service.durable_root(),
        &journey.service.endpoint(),
        "agent-state",
    )
    .await;
    let config = RecoveryJourney::recovery_app_config_with_agent(
        OwnedOpenCode::connect_app_store(journey.service.durable_root()).await,
        control.wrap(agent),
    );
    let mut app = App::open(config).await?;
    let observed = std::panic::AssertUnwindSafe(async {
        let workspace = RecoveryJourney::create_workspace(
            &app,
            "real-interactions",
            journey.service.workspace_root(),
        )
        .await?;

        let input = RecoveryJourney::start(&workspace, "caller-input").await;
        let waiting = tokio::time::timeout(Duration::from_secs(120), input.wait()).await??;
        let (sequence, generation, message, schema) = match waiting.interactions.as_slice() {
            [
                Interaction::Input {
                    sequence,
                    generation,
                    message,
                    schema,
                },
            ] => (*sequence, *generation, message, schema),
            _ => panic!("the caller-input workflow must expose one public input"),
        };
        assert_eq!(message, "Continue the workflow?");
        assert_eq!(
            schema.as_ref(),
            Some(&json!({"type": "string", "enum": ["yes"]}))
        );
        let settled = input
            .resolve(
                sequence,
                generation,
                Resolution::Input(Value::String("yes".to_owned())),
            )
            .await?;
        let applied_checkpoint = settled.checkpoint;
        let applied_state = settled.state.clone();
        let applied_ordinal = settled.last_action_ordinal;
        let terminal = OwnedOpenCode::settle_terminal(&input, settled).await;
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(
            terminal.state.values().get("answer"),
            Some(&Value::String("yes".to_owned()))
        );
        let replay = input
            .resolve(
                sequence,
                generation,
                Resolution::Input(Value::String("yes".to_owned())),
            )
            .await?;
        assert_eq!(
            replay.checkpoint, applied_checkpoint,
            "the same input decision replays its original receipt after terminal progress"
        );
        assert_eq!(replay.state, applied_state);
        assert_eq!(replay.last_action_ordinal, applied_ordinal);

        let question_sessions = journey.service.question_sessions();
        let question = RecoveryJourney::start(&workspace, "agent-question").await;
        let waiting = tokio::time::timeout(Duration::from_secs(120), question.wait()).await??;
        let (sequence, generation) = match waiting.interactions.as_slice() {
            [
                Interaction::Notification {
                    sequence,
                    generation,
                    notification,
                },
            ] => {
                match notification.kind() {
                    ConversationNotificationKind::Interaction {
                        title,
                        body,
                        response_schema,
                        dismissible,
                    } => {
                        assert_eq!(title, "Continue");
                        assert_eq!(body, "1. Continue?");
                        assert!(response_schema.is_some());
                        assert!(dismissible);
                    }
                    other => panic!("expected the exact real question notification, got {other:?}"),
                }
                (*sequence, *generation)
            }
            _ => {
                control.report_notification_delivery();
                journey
                    .service
                    .report_question_evidence(&waiting, question_sessions.as_ref());
                RecoveryJourney::fail_unexpected_unknown(&question, &waiting).await;
                panic!("the real Agent workflow must expose its exact notification")
            }
        };
        let settled = RecoveryJourney::submit_yes(&question, sequence, generation).await?;
        let terminal = OwnedOpenCode::settle_terminal(&question, settled).await;
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(
            terminal.state.values().get("reply"),
            Some(&Value::String("Acknowledged.".to_owned()))
        );

        let race_sessions = journey.service.question_sessions();
        let race = RecoveryJourney::start(&workspace, "agent-question").await;
        let waiting = tokio::time::timeout(Duration::from_secs(120), race.wait()).await??;
        let [
            Interaction::Notification {
                sequence,
                generation,
                ..
            },
        ] = waiting.interactions.as_slice()
        else {
            control.report_notification_delivery();
            journey
                .service
                .report_question_evidence(&waiting, race_sessions.as_ref());
            RecoveryJourney::fail_unexpected_unknown(&race, &waiting).await;
            panic!("the cancellation race must expose its real notification")
        };
        let settled = RecoveryJourney::submit_yes(&race, *sequence, *generation).await?;
        let (mut current, cancellation_accepted) = match race.cancel(settled.checkpoint).await {
            Ok(snapshot) => (snapshot, true),
            Err(AppError::Conflict(_)) => (race.snapshot().await?, false),
            Err(error) => return Err(error.into()),
        };
        if let [
            Interaction::Unknown {
                sequence,
                generation,
                ..
            },
        ] = current.interactions.as_slice()
        {
            let (sequence, generation) = (*sequence, *generation);
            current = match race
                .resolve(sequence, generation, Resolution::Accept(Map::new()))
                .await
            {
                Ok(snapshot) => snapshot,
                Err(AppError::Conflict(_)) => race.snapshot().await?,
                Err(error) => return Err(error.into()),
            };
        }
        let terminal = OwnedOpenCode::settle_terminal(&race, current).await;
        assert!(match cancellation_accepted {
            true => matches!(terminal.status, RunStatus::Cancelled),
            false => matches!(terminal.status, RunStatus::Succeeded | RunStatus::Failed(_)),
        });

        drop((input, question, race, workspace));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(150),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(120),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    if matches!(&cleanup, Ok(Ok(Ok(())))) {
        journey.service.cleanup().await;
    } else {
        journey.service.stop_and_retain_paths().await;
    }
    RecoveryJourney::report_cleanup("question App", &cleanup);
    match observed {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("question cleanup does not panic")
        .expect("question cleanup is bounded")?;
    Ok(())
}

#[tokio::test]
async fn real_response_is_not_lost_while_notification_resolution_is_intended()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut journey = RecoveryJourney::new("intended-response-settlement").await;
    let control = ForwardingControl::response_settlement();
    let config = journey
        .service
        .forwarding_app_config(&[OwnedOpenCode::QUESTION_WORKFLOW], &control)
        .await;
    let mut app = App::open(config).await?;
    let observed = std::panic::AssertUnwindSafe(async {
        let workspace = RecoveryJourney::create_workspace(
            &app,
            "intended-response-settlement",
            journey.service.workspace_root(),
        )
        .await?;
        let run = RecoveryJourney::start(&workspace, "agent-question").await;
        let waiting = tokio::time::timeout(Duration::from_secs(120), run.wait()).await??;
        let (sequence, generation) = match waiting.interactions.as_slice() {
            [
                Interaction::Notification {
                    sequence,
                    generation,
                    ..
                },
            ] => (*sequence, *generation),
            _ => panic!("the real Agent must expose its exact notification"),
        };

        let resolving = RecoveryJourney::submit_yes(&run, sequence, generation);
        tokio::pin!(resolving);
        tokio::time::timeout(Duration::from_secs(120), async {
            tokio::select! {
                biased;
                _ = &mut resolving => {
                    panic!("notification resolution returned before its forwarding barrier")
                }
                () = control.wait_until_method_blocked() => {}
            }
        })
        .await
        .expect("the real notification resolution must reach its forwarding barrier");
        tokio::time::timeout(Duration::from_secs(120), control.wait_until_prompt_ready())
            .await
            .expect("the real prompt response must become ready");
        control.release_prompt();
        tokio::time::timeout(
            Duration::from_secs(30),
            control.wait_until_prompt_delivered(),
        )
        .await
        .expect("the real prompt response must be delivered to app-sdk");
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            control.wait_until_prompt_observation_finished(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut resolving)
                .await
                .is_err(),
            "the notification resolution must remain blocked while the response is observed"
        );
        let intended = run.snapshot().await?;
        assert!(intended.checkpoint > waiting.checkpoint);
        assert!(matches!(
            intended.interactions.as_slice(),
            [Interaction::Notification {
                sequence: intended_sequence,
                ..
            }] if *intended_sequence == sequence
        ));

        control.release_method();
        let settled = tokio::time::timeout(Duration::from_secs(30), &mut resolving)
            .await
            .expect("the accepted notification resolution must return after release")?;
        tokio::time::timeout(
            Duration::from_secs(30),
            control.wait_until_prompt_observation_finished(),
        )
        .await
        .expect("app-sdk must finish observing the ready real prompt response");
        let terminal = tokio::time::timeout(
            Duration::from_secs(15),
            OwnedOpenCode::settle_terminal(&run, settled),
        )
        .await
        .expect("the response observed during resolution settlement must not be lost");
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(
            terminal.state.values().get("reply"),
            Some(&Value::String("Acknowledged.".to_owned()))
        );

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    control.release_prompt();
    control.release_method();
    let shutdown = if matches!(&observed, Ok(Ok(()))) {
        Shutdown::Drain {
            timeout: Duration::from_secs(30),
        }
    } else {
        Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }
    };
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(120),
        app.shutdown(shutdown),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let service_cleanup =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            if matches!(&cleanup, Ok(Ok(Ok(())))) {
                journey.service.cleanup().await;
            } else {
                journey.service.stop_and_retain_paths().await;
                eprintln!("strict-live retained paths after unconfirmed App cleanup");
            }
        }))
        .catch_unwind()
        .await;
    RecoveryJourney::report_cleanup("App", &cleanup);
    RecoveryJourney::report_cleanup("service", &service_cleanup);
    match observed {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("App cleanup does not panic")
        .expect("App cleanup joins")?;
    service_cleanup
        .expect("service cleanup does not panic")
        .expect("service cleanup joins");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_during_durable_failure_preserves_original_timeout()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut journey = RecoveryJourney::new("failing-cancellation").await;
    let timeout_question = OwnedOpenCode::QUESTION_WORKFLOW
        .replace(
            "\"id\": \"agent-question\"",
            "\"id\": \"agent-question-timeout\"",
        )
        .replace(
            "\"id\": \"ask-agent\",",
            "\"id\": \"ask-agent\",\n      \"deadline_ms\": 30000,",
        );
    let control = ForwardingControl::abort_settlement();
    let config = journey
        .service
        .forwarding_app_config(&[&timeout_question], &control)
        .await;
    let mut app = App::open(config).await?;
    let observed = std::panic::AssertUnwindSafe(async {
        let workspace = RecoveryJourney::create_workspace(
            &app,
            "failing-cancellation",
            journey.service.workspace_root(),
        )
        .await?;
        let run = RecoveryJourney::start(&workspace, "agent-question-timeout").await;
        tokio::time::timeout(Duration::from_secs(90), control.wait_until_method_blocked())
            .await
            .expect("the expired real attempt must reach its abort settlement barrier");

        let failing = run.snapshot().await?;
        let cancellation = run.cancel(failing.checkpoint).await?;
        assert!(cancellation.checkpoint > failing.checkpoint);
        control.release_method();
        let terminal = tokio::time::timeout(Duration::from_secs(30), async {
            let mut current = cancellation;
            let mut resolved = None;
            loop {
                if matches!(current.status, RunStatus::Failed(_)) {
                    break Ok::<_, AppError>(current);
                }
                if let [
                    Interaction::Unknown {
                        sequence,
                        generation,
                        ..
                    },
                ] = current.interactions.as_slice()
                {
                    let target = (*sequence, *generation);
                    if resolved == Some(target) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        current = run.snapshot().await?;
                        continue;
                    }
                    assert!(
                        resolved.is_none(),
                        "no new lower attempt may replace the failed unknown target"
                    );
                    resolved = Some(target);
                    // An abort exceeding App's one-second grace may publish
                    // Unknown even when the original call later returns. Only
                    // this exact public target is caller-settled; no prompt replays.
                    current = match run.resolve(target.0, target.1, Resolution::Fail).await {
                        Ok(snapshot) => snapshot,
                        Err(AppError::Conflict(_)) => {
                            let snapshot = run.snapshot().await?;
                            assert!(
                                matches!(
                                    snapshot.status,
                                    RunStatus::Failed(WorkflowFailure::Timeout(_))
                                ),
                                "a competing original abort must preserve the primary Timeout"
                            );
                            snapshot
                        }
                        Err(error) => return Err(error),
                    };
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    current = run.snapshot().await?;
                }
            }
        })
        .await
        .expect("the real abort or exact Unknown resolution must settle")?;
        let RunStatus::Failed(WorkflowFailure::Timeout(message)) = terminal.status else {
            panic!("later cancellation cannot replace the original Timeout")
        };
        assert_eq!(message, "agent attempt deadline expired");
        assert_eq!(
            control.prompt_count(),
            1,
            "unknown settlement never replays the prompt"
        );

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    control.release_method();
    let shutdown = if matches!(&observed, Ok(Ok(()))) {
        Shutdown::Drain {
            timeout: Duration::from_secs(30),
        }
    } else {
        Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }
    };
    let mut cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(120),
        app.shutdown(shutdown),
    ))
    .catch_unwind()
    .await;
    if matches!(&cleanup, Ok(Err(_)) | Ok(Ok(Err(AppError::Timeout(_))))) {
        RecoveryJourney::report_cleanup("first App observation", &cleanup);
        // Observation expiry does not end shutdown ownership. Keep the same
        // runtime driven and attach one further observer to the admitted drain.
        cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(150),
            app.shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(120),
            }),
        ))
        .catch_unwind()
        .await;
    }
    drop(app);
    let service_cleanup =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            if matches!(&cleanup, Ok(Ok(Ok(())))) {
                journey.service.cleanup().await;
            } else {
                journey.service.stop_and_retain_paths().await;
                eprintln!("strict-live retained paths after unconfirmed App cleanup");
            }
        }))
        .catch_unwind()
        .await;
    RecoveryJourney::report_cleanup("App", &cleanup);
    RecoveryJourney::report_cleanup("service", &service_cleanup);
    match observed {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("App cleanup does not panic")
        .expect("App cleanup joins")?;
    service_cleanup
        .expect("service cleanup does not panic")
        .expect("service cleanup joins");
    Ok(())
}

#[tokio::test]
async fn wait_projects_retained_notification_while_real_recovery_is_blocked()
-> Result<(), Box<dyn Error + Send + Sync>> {
    if std::env::var(CHILD_SCENARIO).ok().as_deref() == Some(BLOCKED_RECOVERY_SCENARIO) {
        RecoveryJourney::seed_notification_child().await;
    }

    let mut journey = RecoveryJourney::new("blocked-notification-recovery").await;
    let child = journey.spawn_recovery_child(BLOCKED_RECOVERY_TEST, BLOCKED_RECOVERY_SCENARIO);
    let mut child = journey.wait_child_boundary(child, CHILD_PROBE, true).await;
    child
        .kill()
        .expect("the active notification recovery child must accept termination");
    let status = journey.wait_recovery_child(child).await;
    assert!(
        !status.success(),
        "the notification recovery child must be killed at its active boundary"
    );
    let probe = journey.read_probe();
    let workspace_id = RecoveryJourney::probe_id(&probe, "workspace").parse::<WorkspaceId>()?;
    let run_id = RecoveryJourney::probe_id(&probe, "run").parse::<RunId>()?;
    let expected_sequence = probe["sequence"]
        .as_u64()
        .expect("the recovery probe must retain the notification sequence");
    let expected_generation = probe["generation"]
        .as_u64()
        .expect("the recovery probe retains the exact original notification generation");
    let expected_notification = RecoveryJourney::probe_id(&probe, "notification");

    let control = ForwardingControl::conversation_recovery();
    let agent = OwnedOpenCode::build_agent(
        journey.service.durable_root(),
        &journey.service.endpoint(),
        "agent-state",
    )
    .await;
    let mut app = App::open(RecoveryJourney::recovery_app_config_with_agent(
        OwnedOpenCode::connect_app_store(journey.service.durable_root()).await,
        control.wrap(agent),
    ))
    .await?;
    let workspace = app.workspace(workspace_id).await?;
    let run = RecoveryJourney::workflow(&workspace, "agent-question")
        .await
        .open_run(RunSelector::Latest(run_id))
        .await?;
    let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(35), async {
        tokio::time::timeout(Duration::from_secs(30), control.wait_until_method_blocked())
            .await.expect("notification recovery reaches the real get_conversation barrier");
        // The original native recovery stays gated. Projection is entirely
        // supported by the previously delivered immutable portable evidence.
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await.expect("retained notification projection does not await lower recovery")?;
        let [Interaction::Notification { sequence, generation, notification }] = snapshot.interactions.as_slice()
            else { panic!("held native recovery cannot hide or replace the original delivered target") };
        assert_eq!((*sequence, *generation), (expected_sequence, expected_generation));
        assert_eq!(notification.id().to_string(), expected_notification);
        let inserted_at = notification.inserted_at().duration_since(std::time::UNIX_EPOCH)
            .expect("the retained timestamp follows the Unix epoch");
        assert_eq!(json!({"seconds":inserted_at.as_secs(),"nanoseconds":inserted_at.subsec_nanos()}), probe["inserted_at"]);
        let ConversationNotificationKind::Interaction { title, body, response_schema, dismissible } = notification.kind()
            else { panic!("projection preserves the original portable interaction kind") };
        assert_eq!(json!({"title":title,"body":body,"response_schema":response_schema,"dismissible":dismissible}), probe["payload"]);
        Ok::<_, AppError>(())
    })).catch_unwind().await;
    control.release_method();
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(120),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }),
    ))
    .catch_unwind()
    .await;
    drop(run);
    drop(workspace);
    drop(app);
    let service_cleanup =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            if matches!(&cleanup, Ok(Ok(Ok(())))) {
                journey.service.cleanup().await;
            } else {
                journey.service.stop_and_retain_paths().await;
                eprintln!("strict-live retained paths after unconfirmed App cleanup");
            }
        }))
        .catch_unwind()
        .await;
    RecoveryJourney::report_cleanup("App", &cleanup);
    RecoveryJourney::report_cleanup("service", &service_cleanup);
    match observed {
        Ok(result) => result.expect("retained projection proof remains bounded")?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("recovery cleanup does not panic")
        .expect("recovery cleanup joins")?;
    service_cleanup
        .expect("service cleanup does not panic")
        .expect("service cleanup joins");

    Ok(())
}

#[tokio::test]
async fn obsolete_app_epoch_is_rejected_without_mutation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut service = OwnedOpenCode::new("obsolete-app-epoch");
    service.start().await;
    let stored = json!({
        "epoch": "app/v3",
        "generation": 1,
        "run_count": 0,
        "agents": {},
        "definitions": {},
        "shutdown": {"state": "active"}
    });
    let key = StateKey::new("app-sdk", "app")?;
    let receipt_key = StateKey::new("app-sdk", "receipt/obsolete-app-epoch")?;
    let receipt = json!({
        "epoch": "app/v5",
        "id": "obsolete-app-epoch"
    });
    let store = OwnedOpenCode::connect_app_store(service.durable_root()).await;
    let mut transaction = store.begin_transaction().await?;
    transaction
        .put(StateRecord::new(key.clone(), stored.clone()))
        .await?;
    transaction
        .put(StateRecord::new(receipt_key.clone(), receipt.clone()))
        .await?;
    transaction.commit().await?;
    let seeded_revision = store
        .load(key.clone())
        .await?
        .expect("the seeded obsolete app record must exist")
        .revision;
    let receipt_revision = store
        .load(receipt_key.clone())
        .await?
        .expect("the seeded receipt must exist")
        .revision;
    drop(store);

    for _ in 0..2 {
        let error = match App::open(service.app_config(&[]).await).await {
            Ok(_) => panic!("an obsolete App epoch must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            AppError::Integration(ref message)
                if message == "unsupported app record schema version"
        ));
        let store = OwnedOpenCode::connect_app_store(service.durable_root()).await;
        let snapshot = store
            .load(key.clone())
            .await?
            .expect("the rejected app/v3 record must remain present");
        assert_eq!(snapshot.record.value.as_ref(), &stored);
        assert_eq!(snapshot.revision, seeded_revision);
        let receipt_snapshot = store
            .load(receipt_key.clone())
            .await?
            .expect("the rejected app/v3 open must not clean receipts");
        assert_eq!(receipt_snapshot.record.value.as_ref(), &receipt);
        assert_eq!(receipt_snapshot.revision, receipt_revision);
    }

    service.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn unknown_workspace_recovery_never_adopts_another_workspace_claim()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut journey = RecoveryJourney::new("unknown-workspace-claim").await;
    let root = journey.service.workspace_root();
    let endpoint = journey.service.endpoint();
    let durable_root = journey.service.durable_root().to_path_buf();
    let agent_store = OwnedOpenCode::connect_agent_store(&durable_root, "agent-state").await;
    let conflicting = ConflictStore::new(
        agent_store.clone(),
        OwnedOpenCode::connect_agent_store(&durable_root, "agent-state").await,
    );
    let agent = OwnedOpenCode::build_agent_from_store(&endpoint, conflicting.clone(), None).await;
    let mut app = App::open(RecoveryJourney::app_config(&durable_root, agent).await).await?;
    let claimed_app =
        RecoveryJourney::create_workspace(&app, "shared-lower-config", root.clone()).await?;
    let claimed_app_id = claimed_app.id().clone();
    conflicting.arm();
    let creation_started = tokio::time::Instant::now();
    match RecoveryJourney::create_workspace(&app, "shared-lower-config", root.clone()).await {
        Err(AppError::Conflict(_)) => {}
        Err(error) => panic!("the duplicate lower workspace must conflict: {error}"),
        Ok(_) => panic!("the duplicate lower workspace must not be created"),
    }
    conflicting.disarm();
    assert!(
        conflicting.observed() > 1,
        "newly authored creation retries reached actual stale Lance commits"
    );
    assert!(
        creation_started.elapsed() >= Duration::from_secs(120),
        "the original Agent lifecycle deadline expires without an early fabricated failure"
    );
    assert!(matches!(
        RecoveryJourney::drain(&mut app).await,
        Err(AppError::Integration(_))
    ));
    drop(claimed_app);
    drop(app);
    let agent = OwnedOpenCode::build_agent_from_store(&endpoint, agent_store.clone(), None).await;
    let claimed = agent.list_workspaces().await?.remove(0);
    let request = NewWorkspace::new(
        claimed.name().to_owned(),
        claimed.context().clone(),
        claimed.defaults().clone(),
    )?;
    assert!(matches!(
        App::open(RecoveryJourney::app_config(&durable_root, agent).await).await,
        Err(AppError::Conflict(_))
    ));
    let agent = OwnedOpenCode::build_agent_from_store(&endpoint, agent_store.clone(), None).await;
    let unclaimed = agent.new_workspace(request).await?.info().await?;
    let mut app = App::open(RecoveryJourney::app_config(&durable_root, agent).await).await?;
    let next_name = "recovered-lower-config";
    let recovered = app
        .list_workspaces(None, NonZeroUsize::new(3).expect("three is nonzero"))
        .await?
        .into_iter()
        .find(|workspace| workspace.id != claimed_app_id)
        .expect("the formerly unknown app workspace must recover");
    app.workspace(recovered.id)
        .await?
        .replace(
            recovered.revision,
            WorkspaceConfig {
                name: Some(next_name.to_owned()),
                root,
            },
        )
        .await?;
    RecoveryJourney::drain(&mut app).await?;
    let agent = OwnedOpenCode::build_agent_from_store(&endpoint, agent_store.clone(), None).await;
    assert_eq!(agent.workspace_info(claimed.id().clone()).await?, claimed);
    let changed = agent.workspace_info(unclaimed.id().clone()).await?;
    assert_eq!(changed.name(), next_name);
    assert_eq!(changed.revision(), unclaimed.revision() + 1);
    assert_eq!(changed.context(), unclaimed.context());
    agent.shutdown().await?;
    drop(app);
    drop(conflicting);
    drop(agent_store);
    journey.service.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn replacement_compensates_and_failed_open_releases_both_real_agents()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut primary = OwnedOpenCode::new("multiple-agent-cleanup-primary");
    let mut secondary = OwnedOpenCode::new("multiple-agent-cleanup-secondary");
    primary.start().await;
    secondary.start().await;

    let primary_store = ConflictStore::new(
        OwnedOpenCode::connect_agent_store(primary.durable_root(), "agent-state").await,
        OwnedOpenCode::connect_agent_store(primary.durable_root(), "agent-state").await,
    );
    let secondary_store = ConflictStore::new(
        OwnedOpenCode::connect_agent_store(secondary.durable_root(), "agent-state").await,
        OwnedOpenCode::connect_agent_store(secondary.durable_root(), "agent-state").await,
    );
    let mut app = App::open(
        RecoveryJourney::two_agent_config(
            primary.durable_root(),
            "strict-live-v1",
            OwnedOpenCode::build_agent_from_store(&primary.endpoint(), primary_store.clone(), None)
                .await,
            OwnedOpenCode::build_agent_from_store(
                &secondary.endpoint(),
                secondary_store.clone(),
                None,
            )
            .await,
            &[],
        )
        .await,
    )
    .await?;
    let workspace =
        RecoveryJourney::create_workspace(&app, "before", primary.workspace_root()).await?;
    let initial = workspace.snapshot().await?;
    secondary_store.once();
    let replacement = tokio::time::timeout(
        Duration::from_secs(5),
        workspace.replace(
            initial.revision,
            WorkspaceConfig {
                name: Some("after".to_owned()),
                root: primary.workspace_root(),
            },
        ),
    )
    .await;
    assert_eq!(
        secondary_store.observed(),
        1,
        "one native conflict triggers compensation"
    );
    assert!(matches!(
        replacement.expect("partial replacement must settle in time"),
        Err(AppError::Conflict(_))
    ));
    assert_eq!(
        workspace.snapshot().await?.config.name.as_deref(),
        Some("before")
    );
    RecoveryJourney::drain(&mut app).await?;
    drop(workspace);
    drop(app);

    let primary_agent =
        OwnedOpenCode::build_agent_from_store(&primary.endpoint(), primary_store.clone(), None)
            .await;
    let secondary_agent =
        OwnedOpenCode::build_agent_from_store(&secondary.endpoint(), secondary_store.clone(), None)
            .await;
    for (agent, revision) in [(&primary_agent, 3), (&secondary_agent, 1)] {
        let workspaces = agent.list_workspaces().await?;
        assert_eq!(workspaces.len(), 1);
        assert_eq!(workspaces[0].name(), "before");
        assert_eq!(workspaces[0].revision(), revision);
    }
    assert!(matches!(
        App::open(
            RecoveryJourney::two_agent_config(
                primary.durable_root(),
                "strict-live-v2",
                primary_agent,
                secondary_agent,
                &[],
            )
            .await,
        )
        .await,
        Err(AppError::Conflict(_))
    ));
    tokio::try_join!(
        OwnedOpenCode::build_agent_from_store(&primary.endpoint(), primary_store, None)
            .await
            .shutdown(),
        OwnedOpenCode::build_agent_from_store(&secondary.endpoint(), secondary_store, None)
            .await
            .shutdown(),
    )?;

    primary.cleanup().await;
    secondary.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn two_real_agents_parallel_wave_preserves_tool_and_knowledge_isolation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    RecoveryJourney::parallel_isolation_case(false).await
}

#[tokio::test]
async fn lance_state_directory_two_real_agents_preserve_tool_and_knowledge_isolation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    RecoveryJourney::parallel_isolation_case(true).await
}

impl RecoveryJourney {
    async fn parallel_isolation_case(
        shared_catalog: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut primary_service = OwnedOpenCode::new("parallel-isolation-primary");
        let mut secondary_service = OwnedOpenCode::new("parallel-isolation-secondary");
        let primary_tool = uuid::Uuid::new_v4().to_string();
        let secondary_tool = uuid::Uuid::new_v4().to_string();
        let primary_knowledge = uuid::Uuid::new_v4().to_string();
        let secondary_knowledge = uuid::Uuid::new_v4().to_string();
        primary_service.set_tool_canary(primary_tool.clone());
        secondary_service.set_tool_canary(secondary_tool.clone());
        tokio::join!(primary_service.start(), secondary_service.start());
        let knowledge = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
            primary_service.durable_root().join("knowledge.lance"),
        )))
        .await?;
        let primary_agent = OwnedOpenCode::build_agent_from_store(
            &primary_service.endpoint(),
            if shared_catalog {
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
                    primary_service.durable_root().join("knowledge.lance"),
                )))
                .await?
            } else {
                OwnedOpenCode::connect_agent_store(primary_service.durable_root(), "agent-state")
                    .await
            },
            Some(knowledge.clone()),
        )
        .await;
        let secondary_agent = OwnedOpenCode::build_agent_from_store(
            &secondary_service.endpoint(),
            if shared_catalog {
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
                    secondary_service.durable_root().join("state.lance"),
                )))
                .await?
            } else {
                OwnedOpenCode::connect_agent_store(secondary_service.durable_root(), "agent-state")
                    .await
            },
            Some(knowledge.clone()),
        )
        .await;
        // Same database and same source identity: isolation must follow Agent scope,
        // rather than two physical datasets or different source keys.
        for (agent, token) in [
            (&primary_agent, &primary_knowledge),
            (&secondary_agent, &secondary_knowledge),
        ] {
            agent
                .ingest_knowledge(
                    IngestKnowledgeRequest::new(vec![KnowledgeSource::text(
                        "isolation-marker",
                        "integration isolation marker",
                        token.clone(),
                    )?])?
                    .with_scope(KnowledgeScope::Agent),
                )
                .await?;
        }
        let primary = ForwardingControl::observation();
        let secondary = ForwardingControl::observation();
        let definition = RecoveryJourney::parallel_isolation_workflow();
        let mut app = App::open(RecoveryJourney::two_agent_config_with_state(
            if shared_catalog {
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
                    primary_service.durable_root().join("knowledge.lance"),
                )))
                .await?
            } else {
                OwnedOpenCode::connect_app_store(primary_service.durable_root()).await
            },
            "strict-live-v1",
            primary.wrap(primary_agent),
            secondary.wrap(secondary_agent),
            &[&definition],
        ))
        .await?;
        let mut phase = "create_workspace";
        let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let workspace = RecoveryJourney::create_workspace(
                &app,
                "parallel-isolation",
                primary_service.workspace_root(),
            )
            .await?;
            phase = "start_workflow";
            let run = RecoveryJourney::start(&workspace, "parallel-isolation").await;
            phase = "snapshot_waiting_for_questions";
            let waiting = loop {
                let snapshot = run.snapshot().await?;
                if snapshot.interactions.len() == 2 {
                    break snapshot;
                }
                assert!(
                    !matches!(
                        snapshot.status,
                        RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled
                    ),
                    "both native questions must park before either branch finishes"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert_eq!(primary.prompt_count(), 1);
            assert_eq!(secondary.prompt_count(), 1);
            let mut identities = BTreeSet::new();
            for interaction in &waiting.interactions {
                match interaction {
                    Interaction::Notification { notification, .. } => {
                        assert!(
                            matches!(notification.kind(), ConversationNotificationKind::Interaction {
                                title, response_schema: Some(_), ..
                            } if title == "Continue"),
                            "both siblings must expose their native Continue question"
                        );
                        assert!(identities.insert(notification.id().clone()));
                    }
                    Interaction::Unknown {
                        sequence,
                        generation,
                        retry_safe,
                    } => panic!(
                        "parallel sibling is Unknown before native questions: sequence={sequence}, generation={generation}, retry_safe={retry_safe}"
                    ),
                    _ => panic!("both siblings must expose their native question"),
                }
            }
            for control in [&primary, &secondary] {
                let prompt = control
                    .last_prompt()
                    .expect("the real lower prompt is observed");
                for token in [
                    &primary_tool,
                    &secondary_tool,
                    &primary_knowledge,
                    &secondary_knowledge,
                ] {
                    assert!(
                        !prompt.contains(token),
                        "expected markers must not be supplied by App input"
                    );
                }
            }
            // Correct routing of native contexts is tested here; the full-access
            // lower workspaces do not constitute OS sandbox isolation.
            assert_eq!(primary_service.tool_receipt(), format!("{primary_tool}\n"));
            assert_eq!(
                secondary_service.tool_receipt(),
                format!("{secondary_tool}\n")
            );
            assert!(
                primary.process_exit_count() >= 1,
                "Agent A forwards the native process exit"
            );
            assert!(
                secondary.process_exit_count() >= 1,
                "Agent B forwards the native process exit"
            );
            assert_eq!(waiting.state.values()["primary"], Value::Null);
            assert_eq!(waiting.state.values()["secondary"], Value::Null);
            for (index, interaction) in waiting.interactions.iter().enumerate() {
                let Interaction::Notification {
                    sequence,
                    generation,
                    notification,
                } = interaction
                else {
                    panic!("both siblings must expose their native question");
                };
                for attempt in 0..8 {
                    phase = if index == 0 {
                        "resolve_first_notification"
                    } else {
                        "resolve_second_notification"
                    };
                    match RecoveryJourney::submit_yes(&run, *sequence, *generation).await {
                        Ok(_) => break,
                        Err(error @ AppError::Conflict(_)) if attempt < 7 => {
                            phase = "reread_after_resolution_conflict";
                            let current = run.snapshot().await?;
                            let changed_target = current.interactions.iter().any(|current| {
                                match current {
                                    Interaction::Notification {
                                        sequence: current_sequence,
                                        generation: current_generation,
                                        notification: current_notification,
                                    } => {
                                        current_sequence == sequence
                                            && (current_generation != generation
                                                || current_notification.id() != notification.id())
                                    }
                                    Interaction::Input { sequence: current_sequence, .. }
                                    | Interaction::Inputs { sequence: current_sequence, .. }
                                    | Interaction::Unknown { sequence: current_sequence, .. } => {
                                        current_sequence == sequence
                                    }
                                    _ => true,
                                }
                            });
                            if changed_target {
                                return Err(error.into());
                            }
                            // Re-author only this exact decision after a fresh boundary read.
                            // An absent target can replay its retained receipt; it is never
                            // treated as success here. Unknown or changed targets are rejected.
                            eprintln!(
                                "strict-live isolation resolution conflict: notification_index={index}; attempt={}; reattach_same_decision=true",
                                attempt + 1
                            );
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            phase = "snapshot_after_resolutions";
            let snapshot = run.snapshot().await?;
            phase = "wait_for_terminal";
            let terminal = OwnedOpenCode::settle_terminal(&run, snapshot).await;
            phase = "verify_terminal";
            assert!(
                matches!(terminal.status, RunStatus::Succeeded),
                "both native branches must succeed"
            );
            if terminal.state.values()["primary"]
                != json!({"tool":primary_tool,"knowledge":primary_knowledge})
                || terminal.state.values()["secondary"]
                    != json!({"tool":secondary_tool,"knowledge":secondary_knowledge})
            {
                eprintln!(
                    "strict-live isolation lower structured markers: primary={:?}; secondary={:?}",
                    primary.last_structured_markers(),
                    secondary.last_structured_markers()
                );
            }
            assert_eq!(
                terminal.state.values()["primary"],
                json!({"tool":primary_tool,"knowledge":primary_knowledge})
            );
            assert_eq!(
                terminal.state.values()["secondary"],
                json!({"tool":secondary_tool,"knowledge":secondary_knowledge})
            );
            assert_eq!((primary.prompt_count(), secondary.prompt_count()), (1, 1));
            assert_eq!(
                (primary.resolution_count(), secondary.resolution_count()),
                (1, 1)
            );
            // The actual native receipt also proves the model did not replay bash.
            assert_eq!(primary_service.tool_receipt(), format!("{primary_tool}\n"));
            assert_eq!(
                secondary_service.tool_receipt(),
                format!("{secondary_tool}\n")
            );
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }))
        .catch_unwind()
        .await;
        if !matches!(&observed, Ok(Ok(Ok(())))) {
            eprintln!(
                "strict-live isolation failure stage={phase}; prompt_counts={:?}; resolution_counts={:?}; process_exit_counts={:?}",
                (primary.prompt_count(), secondary.prompt_count()),
                (primary.resolution_count(), secondary.resolution_count()),
                (primary.process_exit_count(), secondary.process_exit_count())
            );
        }
        let mut shutdown_timeouts = 0;
        let cleanup =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    let shutdown = if matches!(&observed, Ok(Ok(Ok(())))) {
                        Shutdown::Drain {
                            timeout: Duration::from_secs(30),
                        }
                    } else {
                        Shutdown::CancelAndDrain {
                            timeout: Duration::from_secs(30),
                        }
                    };
                    match app.shutdown(shutdown).await {
                        Err(error @ AppError::Timeout(_)) => {
                            if shutdown_timeouts == 0 {
                                eprintln!("strict-live App shutdown observation: {error:?}");
                            }
                            shutdown_timeouts += 1;
                            // Keep the owning runtime driven even if a retained timeout
                            // returns immediately; the outer bound remains authoritative.
                            tokio::task::yield_now().await;
                        }
                        result => break result,
                    }
                }
            }))
            .catch_unwind()
            .await;
        if shutdown_timeouts > 0 {
            eprintln!(
                "strict-live App shutdown observation timeouts before terminal cleanup/outer bound: {shutdown_timeouts}"
            );
        }
        drop(app);
        drop(knowledge);
        let (primary_cleanup, secondary_cleanup) = tokio::join!(
            std::panic::AssertUnwindSafe(async {
                if matches!(&cleanup, Ok(Ok(Ok(())))) {
                    primary_service.cleanup().await;
                } else {
                    primary_service.stop_and_retain_paths().await;
                    eprintln!("strict-live retained primary paths after unconfirmed App cleanup");
                }
            })
            .catch_unwind(),
            std::panic::AssertUnwindSafe(async {
                if matches!(&cleanup, Ok(Ok(Ok(())))) {
                    secondary_service.cleanup().await;
                } else {
                    secondary_service.stop_and_retain_paths().await;
                    eprintln!("strict-live retained secondary paths after unconfirmed App cleanup");
                }
            })
            .catch_unwind(),
        );
        RecoveryJourney::report_cleanup("App", &cleanup);
        RecoveryJourney::report_cleanup("primary service", &primary_cleanup);
        RecoveryJourney::report_cleanup("secondary service", &secondary_cleanup);
        match observed {
            Ok(result) => result.expect("the real parallel isolation proof remains bounded")?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("App cleanup does not panic")
            .expect("App cleanup joins")?;
        primary_cleanup.expect("primary service cleanup joins");
        secondary_cleanup.expect("secondary service cleanup joins");
        primary.assert_shutdown_once();
        secondary.assert_shutdown_once();
        assert_ne!(
            primary.shutdown_conversation().id(),
            secondary.shutdown_conversation().id()
        );
        Ok(())
    }
}

#[tokio::test]
async fn two_sequential_real_notifications_share_one_prompt_and_conversation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut service = OwnedOpenCode::new("sequential-questions");
    service.start().await;
    let mut definition: Value = serde_json::from_str(OwnedOpenCode::QUESTION_WORKFLOW)?;
    definition["nodes"][0]["step"]["prompt"][0]["text"] = Value::String(r#"Use OpenCode's built-in question tool twice in two SEPARATE tool calls. Do not combine questions in one call and do not invoke other tools.
First call exactly {"questions":[{"header":"First","question":"Supply the token","options":[{"label":"Enter token","description":"Supply the token as a custom answer"}],"multiple":false,"custom":true}]} and wait for its actual reply.
Only after that reply, make the second question call with header "Second", question equal to ONLY the exact token string received in the first answer, options [{"label":"Yes","description":"Continue"},{"label":"No","description":"Stop"}], and multiple false. Preserve every token character; do not add a prefix, suffix, punctuation, quotes or explanation to the question. Wait for this second call's actual reply.
Then output exactly `Acknowledged twice.` and stop."#.to_owned());
    let control = ForwardingControl::observation();
    let mut app = App::open(
        service
            .forwarding_app_config(&[&definition.to_string()], &control)
            .await,
    )
    .await?;
    let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
        let workspace = RecoveryJourney::create_workspace(&app, "sequential-questions", service.workspace_root()).await?;
        let run = RecoveryJourney::start(&workspace, "agent-question").await;
        let first = run.wait().await?;
        let [Interaction::Notification { sequence, generation, notification }] = first.interactions.as_slice() else {
            panic!("the first separate native question must be pending");
        };
        assert!(matches!(notification.kind(), ConversationNotificationKind::Interaction {
            title, body, response_schema: Some(_), ..
        } if title == "First" && body == "1. Supply the token"));
        assert_eq!(control.prompt_count(), 1);
        assert_eq!(control.resolution_count(), 0);
        let first_id = notification.id().clone();
        let first_target = (*sequence, *generation);
        let conversation = control.notification_conversation(&first_id);
        let token = uuid::Uuid::new_v4().to_string();
        assert!(!control.last_prompt().expect("original prompt is observed").contains(&token),
            "the nonce is introduced only by the first actual answer");
        let answer = json!({"answers": [[token]]});
        run.resolve(*sequence, *generation, Resolution::Notification(
            NotificationResolution::Submit { content: answer.clone() },
        )).await?;
        let second = loop {
            let snapshot = run.snapshot().await?;
            if matches!(snapshot.interactions.as_slice(), [Interaction::Notification { notification, .. }]
                if notification.id() != &first_id) {
                break snapshot;
            }
            assert!(!matches!(snapshot.status, RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled),
                "a second separate native question is required");
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let [Interaction::Notification { sequence, generation, notification }] = second.interactions.as_slice() else {
            panic!("the second native question must be pending alone");
        };
        assert_ne!((*sequence, *generation), first_target);
        assert!(matches!(notification.kind(), ConversationNotificationKind::Interaction {
            title, body, response_schema: Some(_), ..
        } if title == "Second" && body == &format!("1. {token}")));
        assert_eq!(control.notification_conversation(notification.id()), conversation,
            "both native questions originate from the same public conversation receiver");
        assert_eq!(second.state, first.state);
        assert_eq!(second.last_action_ordinal, first.last_action_ordinal);
        assert_eq!((control.prompt_count(), control.resolution_count()), (1, 1));
        run.resolve(first_target.0, first_target.1, Resolution::Notification(
            NotificationResolution::Submit { content: answer },
        )).await.expect("the first exact decision has an idempotent historical receipt");
        let after_replay = run.snapshot().await?;
        assert_eq!(after_replay.checkpoint, second.checkpoint);
        assert_eq!(after_replay.state, second.state);
        assert_eq!(after_replay.last_action_ordinal, second.last_action_ordinal);
        assert!(matches!(after_replay.interactions.as_slice(), [Interaction::Notification {
            sequence: current_sequence, generation: current_generation, notification: current,
        }] if current_sequence == sequence && current_generation == generation && current.id() == notification.id()),
            "replaying the first receipt must leave the second target unchanged");
        assert_eq!((control.prompt_count(), control.resolution_count()), (1, 1),
            "historical replay cannot redispatch a prompt or resolve the second question");
        let resumed = RecoveryJourney::submit_yes(&run, *sequence, *generation).await?;
        let terminal = OwnedOpenCode::settle_terminal(&run, resumed).await;
        assert!(matches!(terminal.status, RunStatus::Succeeded), "both native questions must settle");
        assert_eq!(terminal.state.values()["reply"], "Acknowledged twice.");
        assert_eq!((control.prompt_count(), control.resolution_count()), (1, 2));
        Ok::<ConversationId, Box<dyn Error + Send + Sync>>(conversation)
    })).catch_unwind().await;
    let shutdown = if matches!(&observed, Ok(Ok(Ok(_)))) {
        Shutdown::Drain {
            timeout: Duration::from_secs(30),
        }
    } else {
        Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }
    };
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(120),
        app.shutdown(shutdown),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let service_cleanup = std::panic::AssertUnwindSafe(async {
        if matches!(&cleanup, Ok(Ok(Ok(())))) {
            service.cleanup().await;
        } else {
            service.stop_and_retain_paths().await;
            eprintln!("strict-live retained paths after unconfirmed App cleanup");
        }
    })
    .catch_unwind()
    .await;
    RecoveryJourney::report_cleanup("App", &cleanup);
    RecoveryJourney::report_cleanup("service", &service_cleanup);
    let conversation = match observed {
        Ok(result) => result.expect("two sequential native questions remain bounded")?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    cleanup
        .expect("App cleanup does not panic")
        .expect("App cleanup joins")?;
    service_cleanup.expect("owned service cleanup joins");
    control.assert_shutdown_once();
    assert_eq!(
        control.shutdown_conversation().id(),
        &conversation,
        "shutdown observes the same sole conversation that delivered both targets"
    );
    Ok(())
}

#[tokio::test]
async fn two_real_agents_handoff_after_process_loss_and_drain_once()
-> Result<(), Box<dyn Error + Send + Sync>> {
    if std::env::var(CHILD_SCENARIO).ok().as_deref() == Some(TWO_AGENT_SCENARIO) {
        RecoveryJourney::seed_two_agent_child().await;
    }
    let mut journey = RecoveryJourney::new("two-agent-primary").await;
    let mut secondary_service = OwnedOpenCode::new("two-agent-secondary");
    secondary_service.start().await;
    let child = journey
        .recovery_child_command(TWO_AGENT_TEST, TWO_AGENT_SCENARIO)
        .env(CHILD_SECONDARY_ROOT, secondary_service.durable_root())
        .env(CHILD_SECONDARY_ENDPOINT, secondary_service.endpoint())
        .spawn()?;
    let mut child = journey.wait_child_boundary(child, CHILD_PROBE, false).await;
    child.kill()?;
    assert!(!journey.wait_recovery_child(child).await.success());
    let probe = journey.read_probe();
    assert_eq!(probe["primary_prompts"], 1);
    assert_eq!(probe["secondary_prompts"], 0);
    let workspace_id = RecoveryJourney::probe_id(&probe, "workspace").parse::<WorkspaceId>()?;
    let run_id = RecoveryJourney::probe_id(&probe, "run").parse::<RunId>()?;

    let primary_agent = OwnedOpenCode::build_agent(
        journey.service.durable_root(),
        &journey.service.endpoint(),
        "agent-state",
    )
    .await;
    let secondary_agent = OwnedOpenCode::build_agent(
        secondary_service.durable_root(),
        &secondary_service.endpoint(),
        "agent-state",
    )
    .await;
    let primary_workspaces = primary_agent.list_workspaces().await?;
    let secondary_workspaces = secondary_agent.list_workspaces().await?;
    assert_eq!(primary_workspaces.len(), 1);
    assert_eq!(secondary_workspaces.len(), 1);
    let primary_workspace = primary_agent
        .get_workspace(primary_workspaces[0].id().clone())
        .await?;
    let secondary_workspace = secondary_agent
        .get_workspace(secondary_workspaces[0].id().clone())
        .await?;
    let initial_conversations = primary_workspace.list_conversations().await?;
    assert_eq!(initial_conversations.len(), 1);
    assert!(secondary_workspace.list_conversations().await?.is_empty());
    // Agent lookups have one current-revision handle claim. Release inspection
    // handles before App needs to reopen these workspaces for execution.
    drop((primary_workspace, secondary_workspace));

    let primary = ForwardingControl::observation();
    let secondary = ForwardingControl::observation();
    let definition = RecoveryJourney::two_agent_workflow();
    let mut app = App::open(
        RecoveryJourney::two_agent_config(
            journey.service.durable_root(),
            "strict-live-v1",
            primary.wrap(primary_agent),
            secondary.wrap(secondary_agent),
            &[&definition],
        )
        .await,
    )
    .await?;
    let workspace = app.workspace(workspace_id).await?;
    let run = RecoveryJourney::workflow(&workspace, "two-agent-handoff")
        .await
        .open_run(RunSelector::Latest(run_id))
        .await?;
    let snapshot = run.snapshot().await?;
    assert_eq!(json!(snapshot.state.values()), probe["state"]);
    assert_eq!(json!(snapshot.checkpoint), probe["checkpoint"]);
    let [
        Interaction::Input {
            sequence,
            generation,
            ..
        },
    ] = snapshot.interactions.as_slice()
    else {
        panic!("recovery must preserve the committed handoff input");
    };
    let resumed = run
        .resolve(*sequence, *generation, Resolution::Input(Value::Bool(true)))
        .await?;
    let completed = OwnedOpenCode::settle_terminal(&run, resumed).await;
    let outcome = match &completed.status {
        RunStatus::Succeeded => "Succeeded".to_owned(),
        RunStatus::Failed(failure) => {
            let (category, message) = match failure {
                WorkflowFailure::Evaluation(message) => ("Evaluation", message.as_str()),
                WorkflowFailure::State(message) => ("State", message.as_str()),
                WorkflowFailure::Agent(message) => ("Agent", message.as_str()),
                WorkflowFailure::Child(message) => ("Child", message.as_str()),
                WorkflowFailure::Execution(message) => ("Execution", message.as_str()),
                WorkflowFailure::Timeout(message) => ("Timeout", message.as_str()),
                WorkflowFailure::Limit(message) => ("Limit", message.as_str()),
                _ => ("Other", "redacted future failure category"),
            };
            format!("Failed({category}: {message})")
        }
        RunStatus::Cancelled => "Cancelled".to_owned(),
        _ => "nonterminal".to_owned(),
    };
    assert!(
        matches!(completed.status, RunStatus::Succeeded),
        "two-agent outcome={outcome}, checkpoint={}, primary prompts={}, secondary prompts={}",
        completed.checkpoint,
        primary.prompt_count(),
        secondary.prompt_count()
    );
    assert_eq!(
        completed.state.values()["artifact"],
        json!({"token": probe["seed"]})
    );
    assert_eq!(
        completed.state.values()["result"],
        json!({"received": probe["seed"], "agent": "secondary"})
    );
    assert_eq!(
        primary.prompt_count(),
        0,
        "recovery must not replay Agent A"
    );
    assert_eq!(secondary.prompt_count(), 1);
    assert_eq!(
        secondary.last_prompt(),
        Some(format!(
            "{}{}",
            RecoveryJourney::HANDOFF_PROMPT,
            completed.state.values()["artifact"],
        ))
    );
    RecoveryJourney::drain(&mut app).await?;
    primary.assert_shutdown_once();
    secondary.assert_shutdown_once();
    assert_eq!(
        primary.shutdown_conversations(primary_workspaces[0].id()),
        initial_conversations
    );
    let final_conversations = secondary.shutdown_conversations(secondary_workspaces[0].id());
    assert_eq!(final_conversations.len(), 1);
    assert_eq!(
        final_conversations[0].workspace_id(),
        secondary_workspaces[0].id()
    );
    journey.service.assert_running();
    secondary_service.assert_running();
    drop((run, workspace, app));
    journey.service.cleanup().await;
    secondary_service.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn durable_reopen_cancellation_child_map_and_shutdown_converge()
-> Result<(), Box<dyn Error + Send + Sync>> {
    if std::env::var(CHILD_SCENARIO).ok().as_deref() == Some(RECOVERY_SCENARIO) {
        RecoveryJourney::seed_child().await;
    }

    let mut journey = RecoveryJourney::new("durable-recovery").await;
    let child = journey.spawn_recovery_child(RECOVERY_TEST, RECOVERY_SCENARIO);
    let child = journey.wait_child_boundary(child, CHILD_READY, true).await;
    fs::write(journey.service.durable_root().join(CHILD_GO), b"go")
        .expect("the active recovery child must be released");
    let mut child = journey.wait_child_boundary(child, CHILD_PROBE, true).await;
    child
        .kill()
        .expect("the active recovery child must accept termination");
    let status = journey.wait_recovery_child(child).await;
    assert!(
        !status.success(),
        "the public-API recovery child must be killed at its active boundary"
    );
    let probe = journey.read_probe();
    let workspace_id = RecoveryJourney::probe_id(&probe, "workspace").parse::<WorkspaceId>()?;
    let input_id = RecoveryJourney::probe_id(&probe, "input").parse::<RunId>()?;
    let map_id = RecoveryJourney::probe_id(&probe, "map").parse::<RunId>()?;
    let retry_id = RecoveryJourney::probe_id(&probe, "retry").parse::<RunId>()?;
    let notification_sequence = probe["notification_sequence"]
        .as_u64()
        .expect("the recovery probe retains the settled notification sequence");
    let notification_generation = probe["notification_generation"]
        .as_u64()
        .expect("the recovery probe retains the settled notification generation");

    let mut app = journey.reopen_after_process_loss().await?;
    let workspace = app.workspace(workspace_id.clone()).await?;
    let retry_run = RecoveryJourney::workflow(&workspace, "agent-question-retry")
        .await
        .open_run(RunSelector::Latest(retry_id))
        .await?;
    let unknown = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let snapshot = retry_run
                .snapshot()
                .await
                .expect("the crashed attempt must remain observable");
            match (&snapshot.status, snapshot.interactions.as_slice()) {
                (
                    _,
                    [
                        Interaction::Unknown {
                            retry_safe: true, ..
                        },
                    ],
                ) => return snapshot,
                (RunStatus::Running, []) | (_, [Interaction::Unknown { .. }]) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                _ => panic!("the crashed attempt must converge to retry-safe unknown truth"),
            }
        }
    })
    .await
    .expect("authoritative abort evidence must permit retry in time");
    let [
        Interaction::Unknown {
            sequence,
            generation,
            ..
        },
    ] = unknown.interactions.as_slice()
    else {
        unreachable!("the observation loop returns the exact retry-safe target");
    };
    let replay =
        RecoveryJourney::submit_yes(&retry_run, notification_sequence, notification_generation)
            .await?;
    assert_eq!(
        json!(replay.checkpoint),
        probe["notification_applied"]["checkpoint"]
    );
    assert_eq!(
        json!(replay.state.values()),
        probe["notification_applied"]["state"]
    );
    assert_eq!(
        json!(replay.last_action_ordinal),
        probe["notification_applied"]["last_action_ordinal"]
    );
    let retried = retry_run
        .resolve(*sequence, *generation, Resolution::Retry)
        .await?;
    assert!(retried.checkpoint > unknown.checkpoint);
    let retried = tokio::time::timeout(Duration::from_secs(120), async {
        let mut snapshot = retried;
        loop {
            match (&snapshot.status, snapshot.interactions.as_slice()) {
                (_, [Interaction::Notification { sequence, .. }])
                    if *sequence > notification_sequence =>
                {
                    return snapshot;
                }
                (RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled, _) => {
                    panic!("the proven-safe retry terminated before its real interaction")
                }
                _ => {
                    snapshot = retry_run
                        .wait()
                        .await
                        .expect("the retry must remain observable")
                }
            }
        }
    })
    .await
    .expect("the proven-safe retry must issue one fresh attempt in time");
    let [
        Interaction::Notification {
            sequence,
            generation,
            ..
        },
    ] = retried.interactions.as_slice()
    else {
        unreachable!("the retry loop returns only its real notification")
    };
    let settled = RecoveryJourney::submit_yes(&retry_run, *sequence, *generation).await?;
    assert!(matches!(
        OwnedOpenCode::settle_terminal(&retry_run, settled)
            .await
            .status,
        RunStatus::Succeeded
    ));
    let input_workflow = RecoveryJourney::workflow(&workspace, "caller-input").await;
    let input_run = input_workflow
        .open_run(RunSelector::Latest(input_id))
        .await?;
    let recovered_input = input_run.snapshot().await?;
    assert!(matches!(
        recovered_input.interactions.as_slice(),
        [Interaction::Input { .. }]
    ));
    assert!(matches!(
        app.shutdown(Shutdown::RejectActive).await,
        Err(AppError::Busy(_))
    ));
    let cancelled = input_run.cancel(recovered_input.checkpoint).await?;
    let cancelled = OwnedOpenCode::settle_terminal(&input_run, cancelled).await;
    assert!(matches!(cancelled.status, RunStatus::Cancelled));

    let (map, children) = RecoveryJourney::finish_map(&workspace, map_id).await;
    assert!(matches!(map.status, RunStatus::Succeeded));
    assert_eq!(map.state.values().get("total"), Some(&Value::from(10)));
    assert_eq!(children.len(), 4);

    let child_workflow = RecoveryJourney::workflow(&workspace, "map-child").await;
    let prior_children = children
        .into_iter()
        .map(|child| child.id)
        .collect::<BTreeSet<_>>();
    let failing_parent = RecoveryJourney::start(&workspace, "map-parent-timeout").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let admitted = loop {
        let admitted = child_workflow
            .list_runs(None, NonZeroUsize::new(32).expect("thirty-two is nonzero"))
            .await?
            .into_iter()
            .filter(|child| !prior_children.contains(&child.id))
            .collect::<Vec<_>>();
        if admitted.len() == 2 {
            break admitted;
        }
        assert!(
            Instant::now() < deadline,
            "the timeout map must admit both bounded children"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let failed =
        OwnedOpenCode::settle_terminal(&failing_parent, failing_parent.snapshot().await?).await;
    assert!(matches!(
        failed.status,
        RunStatus::Failed(app_sdk::workflow::WorkflowFailure::Timeout(_))
    ));
    for child in admitted {
        let child = child_workflow
            .open_run(RunSelector::Exact {
                id: child.id,
                occurrence: child.occurrence,
            })
            .await?;
        assert!(matches!(
            child.snapshot().await?.status,
            RunStatus::Cancelled
        ));
    }

    RecoveryJourney::drain(&mut app).await?;
    drop((
        failing_parent,
        child_workflow,
        input_run,
        input_workflow,
        retry_run,
        workspace,
        app,
    ));
    journey.service.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn real_service_interruption_preserves_notification_truth()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let mut journey = RecoveryJourney {
        service: OwnedOpenCode::new("service-interruption"),
    };
    let mut app = None;
    let control = ForwardingControl::shutdown_observation();
    let mut expected_drain = None;
    let started = Instant::now();
    let mut stage = "start_native_service";
    let observed = std::panic::AssertUnwindSafe(async {
        journey.service.start().await;
        stage = "open_agent_and_app";
        // Connect the independent App store before constructing the Agent so
        // no fallible setup await separates its return from ownership transfer.
        let store = OwnedOpenCode::connect_app_store(journey.service.durable_root()).await;
        let agent = OwnedOpenCode::build_agent(
            journey.service.durable_root(),
            &journey.service.endpoint(),
            "agent-state",
        )
        .await;
        let config = RecoveryJourney::recovery_app_config_with_agent(store, control.wrap(agent));
        app = Some(App::open(config).await?);
        let app = app.as_mut().expect("the published App remains owned");
        stage = "workspace";
        let workspace = RecoveryJourney::create_workspace(
            app,
            "service-interruption",
            journey.service.workspace_root(),
        )
        .await?;
        let run = RecoveryJourney::start(&workspace, "agent-question").await;
        stage = "wait_for_native_notification";
        let waiting = tokio::time::timeout(Duration::from_secs(120), run.wait()).await??;
        let (sequence, generation, notification_id) = match waiting.interactions.as_slice() {
            [
                Interaction::Notification {
                    sequence,
                    generation,
                    notification,
                },
            ] => {
                assert!(matches!(
                    notification.kind(),
                    ConversationNotificationKind::Interaction { .. }
                ));
                (*sequence, *generation, notification.id().to_string())
            }
            _ => panic!("the real Agent must expose its exact durable notification"),
        };

        stage = "stop_native_service";
        journey.service.stop().await;
        stage = "resolve_while_service_stopped";
        let interrupted = RecoveryJourney::submit_yes(&run, sequence, generation).await;
        let after_interruption = match interrupted {
            Err(AppError::Integration(_) | AppError::Conflict(_)) => run.snapshot().await?,
            Ok(snapshot) => snapshot,
            Err(other) => return Err(other.into()),
        };
        stage = "observe_exact_unknown";
        let unknown = tokio::time::timeout(Duration::from_secs(120), async {
            let mut snapshot = after_interruption;
            loop {
                match (&snapshot.status, snapshot.interactions.as_slice()) {
                    (
                        _,
                        [
                            Interaction::Unknown {
                                sequence: target,
                                retry_safe: false,
                                ..
                            },
                        ],
                    ) if *target == sequence => return snapshot,
                    (
                        _,
                        [
                            Interaction::Notification {
                                sequence: target,
                                notification,
                                ..
                            },
                        ],
                    ) => {
                        assert_eq!(*target, sequence);
                        assert_eq!(notification.id().to_string(), notification_id);
                    }
                    (RunStatus::Running, []) => {}
                    _ => panic!("ambiguous notification resolution must preserve its exact target"),
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                snapshot = run
                    .snapshot()
                    .await
                    .expect("ambiguous notification truth must remain observable");
            }
        })
        .await
        .expect("ambiguous notification resolution must become truthfully unknown");
        let [Interaction::Unknown { generation, .. }] = unknown.interactions.as_slice() else {
            unreachable!("the observation loop returns the exact Unknown target");
        };
        stage = "fail_unknown_and_settle_terminal";
        let failed = run.resolve(sequence, *generation, Resolution::Fail).await?;
        let terminal = OwnedOpenCode::settle_terminal(&run, failed).await;
        assert!(matches!(
            terminal.status,
            RunStatus::Failed(app_sdk::workflow::WorkflowFailure::Agent(_))
        ));
        stage = "restart_native_service";
        journey.service.start().await;
        stage = "original_drain_30s";
        let drain_started = Instant::now();
        let drained = RecoveryJourney::drain(app).await;
        eprintln!(
            "service interruption original drain: {drained:?}; drain_elapsed={:?}; scenario_elapsed={:?}",
            drain_started.elapsed(),
            started.elapsed()
        );
        expected_drain = Some(match drained {
            Ok(()) => {
                control.assert_shutdown_completion(true);
                Ok(())
            }
            Err(AppError::Integration(message)) => {
                // Preserve a completed, truthful lower repair failure. An App
                // timeout or a missing/unclean lower receipt remains a failure.
                control.assert_shutdown_completion(false);
                Err(message)
            }
            Err(error) => return Err(error.into()),
        });
        stage = "repeat_terminal_drain";
        let repeated = RecoveryJourney::drain(app).await;
        match (expected_drain.as_ref().expect("original Drain receipt"), repeated) {
            (Ok(()), Ok(())) => {}
            (Err(expected), Err(AppError::Integration(actual))) => assert_eq!(&actual, expected),
            (_, Err(error)) => return Err(error.into()),
            (Err(_), Ok(())) => panic!("a retained cleanup failure must not become success"),
        }
        control.assert_shutdown_completion(expected_drain.as_ref().unwrap().is_ok());
        stage = "observe_closed_app";
        assert!(matches!(
            app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await,
            Err(AppError::Closed(_))
        ));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    match &observed {
        Ok(result) => eprintln!(
            "service interruption observed stage={stage} elapsed={:?}: {result:?}",
            started.elapsed(),
        ),
        Err(_) => eprintln!(
            "service interruption panicked at stage={stage} elapsed={:?}",
            started.elapsed(),
        ),
    }

    // The original scenario's Drain observation remains authoritative. Keep the
    // native service alive while observing its retained finalizer independently.
    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let service_restore = if app.is_some() {
        Some(
            std::panic::AssertUnwindSafe(tokio::time::timeout_at(
                cleanup_deadline,
                journey.service.start_if_stopped(),
            ))
            .catch_unwind()
            .await,
        )
    } else {
        None
    };
    let (cleanup, escalation) = if let Some(app) = app.as_mut() {
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout_at(
            cleanup_deadline,
            RecoveryJourney::drain(app),
        ))
        .catch_unwind()
        .await;
        let escalation = if matches!(&cleanup, Ok(Ok(Err(AppError::Timeout(_)))))
            && tokio::time::Instant::now() < cleanup_deadline
        {
            Some(
                std::panic::AssertUnwindSafe(tokio::time::timeout_at(
                    cleanup_deadline,
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(30),
                    }),
                ))
                .catch_unwind()
                .await,
            )
        } else {
            None
        };
        (Some(cleanup), escalation)
    } else {
        eprintln!(
            "service interruption setup returned no App handle; partial-owner cleanup still runs"
        );
        (None, None)
    };
    drop(app);
    let service_cleanup =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            if matches!(&cleanup, Some(Ok(Ok(Ok(())))))
                || matches!(&escalation, Some(Ok(Ok(Ok(())))))
            {
                journey.service.cleanup().await;
            } else {
                journey.service.stop_and_retain_paths().await;
                eprintln!("strict-live retained paths after unconfirmed App cleanup");
            }
        }))
        .catch_unwind()
        .await;
    if let Some(result) = &service_restore {
        RecoveryJourney::report_cleanup("service restoration", result);
    }
    if let Some(result) = &cleanup {
        RecoveryJourney::report_cleanup("App retained Drain", result);
    }
    if let Some(result) = &escalation {
        RecoveryJourney::report_cleanup("App CancelAndDrain escalation", result);
    }
    RecoveryJourney::report_cleanup("service", &service_cleanup);
    match observed {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Some(result) = service_restore {
        result
            .expect("service restoration does not panic")
            .expect("service restoration remains bounded");
    }
    if let Some(result) = cleanup {
        let result = result
            .expect("retained App cleanup does not panic")
            .expect("retained App cleanup joins");
        match (
            expected_drain.as_ref().expect("original Drain receipt"),
            result,
        ) {
            (Ok(()), Ok(())) => {}
            (Err(expected), Err(AppError::Integration(actual))) => assert_eq!(&actual, expected),
            (_, Err(error)) => return Err(error.into()),
            (Err(_), Ok(())) => panic!("retained cleanup must preserve the original failure"),
        }
        control.assert_shutdown_completion(expected_drain.as_ref().unwrap().is_ok());
    }
    if let Some(result) = escalation {
        result
            .expect("App cleanup escalation does not panic")
            .expect("App cleanup escalation joins")?;
    }
    service_cleanup
        .expect("service cleanup does not panic")
        .expect("service cleanup joins");
    Ok(())
}

#[tokio::test]
async fn crash_reopen_after_shutdown_escalation_converges()
-> Result<(), Box<dyn Error + Send + Sync>> {
    if std::env::var(CHILD_SCENARIO).ok().as_deref() == Some(SHUTDOWN_SCENARIO) {
        RecoveryJourney::seed_shutdown_child().await;
    }

    let mut journey = RecoveryJourney {
        service: OwnedOpenCode::new("shutdown-escalation"),
    };
    let mut child = None;
    let mut untransferred_agent = None;
    let mut app_owner = None;
    let mut construction_in_progress = false;
    let mut agent_builds = tokio::task::JoinSet::new();
    let mut app_builds = tokio::task::JoinSet::new();
    let mut agent_shutdowns = tokio::task::JoinSet::new();
    let observed = std::panic::AssertUnwindSafe(async {
        journey.service.start().await;
        child = Some(OwnedChild::new(
            journey.spawn_recovery_child(SHUTDOWN_TEST, SHUTDOWN_SCENARIO),
        ));
        let child = child.as_mut().expect("the recovery child remains owned");
        let boundary_deadline = Instant::now() + Duration::from_secs(360);
        while !journey.service.durable_root().join(CHILD_READY).exists() {
            assert!(child.try_wait()?.is_none(), "the recovery child exited before its boundary");
            assert!(Instant::now() < boundary_deadline, "the recovery child did not reach its boundary in time");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        journey.service.stop().await;
        fs::write(journey.service.durable_root().join(CHILD_GO), b"go")
            .expect("the shutdown recovery child must be released");
        let child_deadline = Instant::now() + Duration::from_secs(180);
        let status = loop {
            if let Some(status) = child.try_wait()? { break status; }
            assert!(Instant::now() < child_deadline, "the strict-live recovery child exceeded its process bound");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(status.success(), "the shutdown recovery child must exit cleanly");
        journey.service.start().await;
        let probe = journey.read_probe();
        let workspace_id = RecoveryJourney::probe_id(&probe, "workspace").parse::<WorkspaceId>()?;
        let run_id = RecoveryJourney::probe_id(&probe, "run").parse::<RunId>()?;
        let deleting_workspace_id = RecoveryJourney::probe_id(&probe, "deleting_workspace").parse::<WorkspaceId>()?;
        let deleting_run_id = RecoveryJourney::probe_id(&probe, "deleting_run").parse::<RunId>()?;
        let unknown_sequence = probe["unknown_sequence"].as_u64()
            .expect("the shutdown probe must retain the unknown sequence");
        let deleting_revision = probe["deleting_revision"].as_u64()
            .expect("the shutdown probe must retain the deleting workspace revision");
        let reopen_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        tokio::time::timeout_at(reopen_deadline, async {
            let agent_store = OwnedOpenCode::connect_agent_store(
                journey.service.durable_root(), "agent-state",
            ).await;
            construction_in_progress = true;
            let endpoint = journey.service.endpoint();
            let first_store = agent_store.clone();
            agent_builds.spawn(async move {
                OwnedOpenCode::try_build_agent_from_store(&endpoint, first_store, None).await
            });
            let repair = agent_builds.join_next().await
                .expect("the first construction remains owned")
                .expect("the first construction task must join");
            let error = match repair {
                Err(error) => error,
                Ok(agent) => {
                    untransferred_agent = Some(agent);
                    construction_in_progress = false;
                    panic!("the stopped-service crash must report its first lower integrity repair");
                }
            };
            if error.kind() != ErrorKind::InvalidState
                || error.context().operation != Operation::Reopen
                || error.context().phase != ErrorPhase::Lifecycle
                || error.detail().is_some()
                || error.cause().is_some()
                || error.http_status().is_some()
                || !error.supplementary().is_empty()
                || error.diagnostics_truncated()
            {
                return Err(Box::<dyn Error + Send + Sync>::from(error));
            }
            eprintln!("strict-live expected lower reopen integrity repair: {error:?}");
            // One explicit caller reconstruction follows the observed integrity
            // report. This is not a shared error-category retry policy.
            construction_in_progress = true;
            let endpoint = journey.service.endpoint();
            agent_builds.spawn(async move {
                OwnedOpenCode::try_build_agent_from_store(&endpoint, agent_store, None).await
            });
            let caller_reconstruction = agent_builds.join_next().await
                .expect("the caller reconstruction remains owned")
                .expect("the caller reconstruction task must join");
            untransferred_agent = Some(caller_reconstruction?);
            construction_in_progress = false;
            let app_store = OwnedOpenCode::connect_app_store(journey.service.durable_root()).await;
            let config = RecoveryJourney::recovery_app_config_with_agent(
                app_store, untransferred_agent.take().expect("the caller-reopened Agent is ready"),
            );
            construction_in_progress = true;
            app_builds.spawn(async move { App::open(config).await });
            let opened = app_builds.join_next().await
                .expect("the App construction remains owned")
                .expect("the App construction task must join");
            app_owner = Some(opened?);
            construction_in_progress = false;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }).await.expect("App reopen must not await deleting-workspace convergence")?;
        let app = app_owner.as_mut().expect("the first App is owned");
        let mut deleting = app.workspace(deleting_workspace_id.clone()).await?;
        assert!(matches!(
            deleting.snapshot().await,
            Err(AppError::Closed(_))
        ));
        let deleting_workflow = RecoveryJourney::workflow(&deleting, "agent-question").await;
        let deleting_run = deleting_workflow
            .open_run(RunSelector::Latest(deleting_run_id))
            .await?;
        let unknown = deleting_run.snapshot().await?;
        let [
            Interaction::Unknown {
                sequence,
                generation,
                retry_safe: false,
            },
        ] = unknown.interactions.as_slice()
        else {
            panic!("shutdown recovery retains its unresolved target");
        };
        assert_eq!(*sequence, unknown_sequence);
        assert!(matches!(
            deleting_run
                .resolve(*sequence, *generation, Resolution::Retry)
                .await,
            Err(AppError::Conflict(_))
        ));
        let cancelling = tokio::time::timeout(Duration::from_secs(30), async {
            let mut current = deleting_run.snapshot().await?;
            loop {
                match (&current.status, current.interactions.as_slice()) {
                    (_, [Interaction::Unknown { sequence, generation, retry_safe: false }])
                        if *sequence == unknown_sequence => {
                        let target = (*sequence, *generation);
                        match deleting_run.resolve(target.0, target.1,
                            Resolution::Accept(Map::from_iter([("reply".into(), "accepted".into())]))).await {
                            Ok(snapshot) => break Ok(snapshot),
                            Err(AppError::Conflict(_)) => {
                                let next = deleting_run.snapshot().await?;
                                assert!(!next.interactions.iter().any(|interaction| matches!(interaction,
                                    Interaction::Unknown { sequence, generation, .. }
                                        if (*sequence, *generation) == target)),
                                    "unchanged target authority cannot justify resubmitting a conflicting decision");
                                current = next;
                            }
                            Err(error) => break Err(error),
                        }
                    }
                    (RunStatus::Running | RunStatus::Cancelling, []) => {
                        current = deleting_run.snapshot().await?;
                    }
                    (RunStatus::Cancelled, []) => break Ok(current),
                    _ => panic!("shutdown recovery must retain or cancel the exact unknown effect"),
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("shutdown recovery settles within its operation bound")?;
        let terminal = if matches!(&cancelling.status, RunStatus::Cancelled) {
            cancelling
        } else {
            OwnedOpenCode::settle_terminal(&deleting_run, cancelling).await
        };
        assert!(matches!(terminal.status, RunStatus::Cancelled));
        // INTERACTIONS: cancellation discards uncommitted wave contributions;
        // accepted external-effect settlement is not a partial state publication.
        assert_eq!(terminal.state, unknown.state);
        assert_eq!(terminal.last_action_ordinal, unknown.last_action_ordinal);
        assert!(terminal.outcomes == unknown.outcomes);
        deleting.delete(deleting_revision).await?;
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        })
        .await?;
        drop(deleting_run);
        drop(deleting_workflow);
        drop(deleting);
        drop(app_owner.take());
        let agent_store = OwnedOpenCode::connect_agent_store(
            journey.service.durable_root(), "agent-state",
        ).await;
        construction_in_progress = true;
        let endpoint = journey.service.endpoint();
        agent_builds.spawn(async move {
            OwnedOpenCode::try_build_agent_from_store(&endpoint, agent_store, None).await
        });
        let second_agent = agent_builds.join_next().await
            .expect("the second Agent construction remains owned")
            .expect("the second Agent construction task must join");
        untransferred_agent = Some(second_agent?);
        construction_in_progress = false;
        let app_store = OwnedOpenCode::connect_app_store(journey.service.durable_root()).await;
        let config = RecoveryJourney::recovery_app_config_with_agent(
            app_store, untransferred_agent.take().expect("the second Agent is ready"),
        );
        construction_in_progress = true;
        app_builds.spawn(async move { App::open(config).await });
        let reopened = app_builds.join_next().await
            .expect("the second App construction remains owned")
            .expect("the second App construction task must join");
        app_owner = Some(reopened?);
        construction_in_progress = false;
        let reopened = app_owner.as_mut().expect("the second App is owned");
        let workspace = reopened.workspace(workspace_id).await?;
        match RecoveryJourney::workflow(&workspace, "caller-input")
            .await
            .open_run(RunSelector::Latest(run_id))
            .await
        {
            Ok(run) => assert!(matches!(run.snapshot().await?.status, RunStatus::Cancelled)),
            Err(AppError::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        let history_workspace = reopened.workspace(deleting_workspace_id).await?;
        assert!(matches!(
            history_workspace.snapshot().await,
            Err(AppError::NotFound(_))
        ));
        let retained_run = RecoveryJourney::workflow(&history_workspace, "agent-question")
            .await
            .open_run(RunSelector::Exact {
                id: terminal.id.clone(),
                occurrence: terminal.occurrence.clone(),
            })
            .await?;
        let retained = retained_run.snapshot().await?;
        assert!(matches!(retained.status, RunStatus::Cancelled));
        assert_eq!(retained.checkpoint, terminal.checkpoint);
        assert_eq!(retained.state, terminal.state);
        assert_eq!(retained.last_action_ordinal, terminal.last_action_ordinal);
        assert!(retained.outcomes == terminal.outcomes);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    }).catch_unwind().await;

    let child_cleanup = if let Some(child) = child.as_mut() {
        Some(
            std::panic::AssertUnwindSafe(child.stop())
                .catch_unwind()
                .await,
        )
    } else {
        None
    };
    let service_restore = if app_owner.is_some()
        || untransferred_agent.is_some()
        || !agent_builds.is_empty()
        || !app_builds.is_empty()
    {
        Some(
            std::panic::AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(120),
                journey.service.start_if_stopped(),
            ))
            .catch_unwind()
            .await,
        )
    } else {
        None
    };
    let construction_cleanup =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            while let Some(result) = agent_builds.join_next().await {
                untransferred_agent = Some(result.expect("Agent construction cleanup must join")?);
                construction_in_progress = false;
            }
            while let Some(result) = app_builds.join_next().await {
                app_owner = Some(result.expect("App construction cleanup must join")?);
                construction_in_progress = false;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }))
        .catch_unwind()
        .await;
    agent_builds.abort_all();
    app_builds.abort_all();
    // Aborting cancels only an unfinished construction future. Retain and join
    // its task; never infer SDK cleanup from the cancellation itself.
    let construction_join = std::panic::AssertUnwindSafe(async {
        while let Some(result) = agent_builds.join_next().await {
            match result {
                Ok(Ok(agent)) => {
                    untransferred_agent = Some(agent);
                    construction_in_progress = false;
                }
                Ok(Err(error)) => return Err(Box::<dyn Error + Send + Sync>::from(error)),
                Err(error) => assert!(
                    error.is_cancelled(),
                    "Agent construction task panicked: {error}"
                ),
            }
        }
        while let Some(result) = app_builds.join_next().await {
            match result {
                Ok(Ok(app)) => {
                    app_owner = Some(app);
                    construction_in_progress = false;
                }
                Ok(Err(error)) => return Err(Box::<dyn Error + Send + Sync>::from(error)),
                Err(error) => assert!(
                    error.is_cancelled(),
                    "App construction task panicked: {error}"
                ),
            }
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    let app_cleanup = if let Some(app) = app_owner.as_mut() {
        let shutdown = if matches!(&observed, Ok(Ok(()))) {
            Shutdown::Drain {
                timeout: Duration::from_secs(30),
            }
        } else {
            Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(30),
            }
        };
        Some(
            std::panic::AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(120),
                app.shutdown(shutdown),
            ))
            .catch_unwind()
            .await,
        )
    } else {
        None
    };
    drop(app_owner.take());
    let agent_cleanup = if let Some(agent) = untransferred_agent.take() {
        agent_shutdowns.spawn(async move { agent.shutdown().await });
        Some(
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
                agent_shutdowns
                    .join_next()
                    .await
                    .expect("Agent shutdown remains owned")
                    .expect("Agent shutdown task must join")
            }))
            .catch_unwind()
            .await,
        )
    } else {
        None
    };
    agent_shutdowns.abort_all();
    let agent_join = std::panic::AssertUnwindSafe(async {
        while let Some(result) = agent_shutdowns.join_next().await {
            match result {
                Ok(result) => result?,
                Err(error) => assert!(
                    error.is_cancelled(),
                    "Agent shutdown task panicked: {error}"
                ),
            }
        }
        Ok::<_, agent_sdk::SdkError>(())
    })
    .catch_unwind()
    .await;
    let settled = !construction_in_progress
        && child_cleanup
            .as_ref()
            .is_none_or(|result| matches!(result, Ok(Ok(_))))
        && matches!(&construction_cleanup, Ok(Ok(Ok(()))))
        && matches!(&construction_join, Ok(Ok(())))
        && matches!(&agent_join, Ok(Ok(())))
        && service_restore
            .as_ref()
            .is_none_or(|result| matches!(result, Ok(Ok(()))))
        && app_cleanup
            .as_ref()
            .is_none_or(|result| matches!(result, Ok(Ok(Ok(())))))
        && agent_cleanup
            .as_ref()
            .is_none_or(|result| matches!(result, Ok(Ok(Ok(())))));
    let service_cleanup =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            if settled {
                journey.service.cleanup().await;
            } else {
                journey.service.stop_and_retain_paths().await;
                eprintln!("strict-live shutdown recovery retained paths after unconfirmed cleanup");
            }
        }))
        .catch_unwind()
        .await;
    if let Some(result) = &child_cleanup {
        RecoveryJourney::report_cleanup("recovery child", result);
    }
    if let Some(result) = &service_restore {
        RecoveryJourney::report_cleanup("service restoration", result);
    }
    if let Some(result) = &app_cleanup {
        RecoveryJourney::report_cleanup("App", result);
    }
    if let Some(result) = &agent_cleanup {
        RecoveryJourney::report_cleanup("untransferred Agent", result);
    }
    RecoveryJourney::report_cleanup("constructions", &construction_cleanup);
    RecoveryJourney::report_cleanup("construction joins", &construction_join);
    RecoveryJourney::report_cleanup("Agent shutdown join", &agent_join);
    RecoveryJourney::report_cleanup("service", &service_cleanup);
    match observed {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    construction_cleanup
        .expect("construction cleanup does not panic")
        .expect("construction cleanup joins")?;
    construction_join.expect("construction cancellation joins do not panic")?;
    agent_join.expect("Agent shutdown cancellation join does not panic")?;
    if let Some(result) = child_cleanup {
        result.expect("recovery child cleanup does not panic")?;
    }
    if let Some(result) = service_restore {
        result
            .expect("service restoration does not panic")
            .expect("service restoration remains bounded");
    }
    if let Some(result) = app_cleanup {
        result
            .expect("App cleanup does not panic")
            .expect("App cleanup joins")?;
    }
    if let Some(result) = agent_cleanup {
        result
            .expect("untransferred Agent cleanup does not panic")
            .expect("untransferred Agent cleanup joins")?;
    }
    service_cleanup
        .expect("service cleanup does not panic")
        .expect("service cleanup joins");
    Ok(())
}

// Real-service integration with one controlled App-store read failure. All
// successful rows, native mutations and lower notifications come from the
// original LanceDB/OpenCode owners; this is distinct from undisturbed live proof.
#[derive(Clone, Copy)]
enum RecoveryReadFault {
    Recoverable,
    Permanent,
    Held,
    LowerErrorHeld,
}

struct RecoveryRead {
    fault: RecoveryReadFault,
    lower_error: Option<std::sync::Arc<ForwardingControl>>,
    selected: std::sync::Mutex<Option<StateKey>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    dropped: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

struct RecoveryStore {
    inner: StateStore,
    read: std::sync::Arc<RecoveryRead>,
}

impl StateStoreBackend for RecoveryStore {
    type Transaction = storage_sdk::StateTransaction;

    async fn load(
        &self,
        key: StateKey,
    ) -> Result<Option<storage_sdk::state::record::StateRecordSnapshot>, storage_sdk::StateStoreError>
    {
        let original = self.inner.load(key.clone()).await?;
        let selected = {
            let mut selected = self.read.selected.lock().unwrap();
            if selected.as_ref() == Some(&key)
                && self.read.lower_error.as_ref().is_none_or(|control| {
                    control
                        .recovery_failure_owner()
                        .is_some_and(|owner| Some(owner) == tokio::task::try_id())
                })
            {
                selected.take();
                true
            } else {
                false
            }
        };
        if selected {
            assert!(
                original.is_some(),
                "fault selection names the original present native head"
            );
            let (held, dropped) = tokio::sync::oneshot::channel();
            *self.read.dropped.lock().unwrap() = Some(dropped);
            self.read.entered.notify_one();
            self.read.release.notified().await;
            drop(held);
            return Err(match self.read.fault {
                RecoveryReadFault::Permanent => storage_sdk::StateStoreError::serialization(
                    "selected retained-notification authority is unreadable",
                ),
                RecoveryReadFault::Recoverable
                | RecoveryReadFault::Held
                | RecoveryReadFault::LowerErrorHeld => storage_sdk::StateStoreError::unavailable(
                    "load",
                    "selected retained-notification authority is unavailable",
                ),
            });
        }
        Ok(original)
    }

    async fn list(
        &self,
        request: storage_sdk::state::record::StateListRequest,
    ) -> Result<storage_sdk::state::record::StateListResponse, storage_sdk::StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, storage_sdk::StateStoreError> {
        self.inner.begin_transaction().await
    }
}

impl RecoveryJourney {
    async fn recovered_notification_authority(
        fault: RecoveryReadFault,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let label = match fault {
            RecoveryReadFault::Recoverable => "recovered-notification-transient-read",
            RecoveryReadFault::Permanent => "recovered-notification-permanent-read",
            RecoveryReadFault::Held => "recovered-notification-held-read",
            RecoveryReadFault::LowerErrorHeld => "recovered-notification-lower-error-held-read",
        };
        let mut journey = Self::new(label).await;
        // Reuse the exact existing child entrypoint and actual Pending probe.
        let child = journey.spawn_recovery_child(BLOCKED_RECOVERY_TEST, BLOCKED_RECOVERY_SCENARIO);
        let mut child = journey.wait_child_boundary(child, CHILD_PROBE, true).await;
        child
            .kill()
            .expect("the original real notification process is killed at its accepted boundary");
        assert!(!journey.wait_recovery_child(child).await.success());
        let probe = journey.read_probe();
        let workspace_id = Self::probe_id(&probe, "workspace").parse::<WorkspaceId>()?;
        let run_id = Self::probe_id(&probe, "run").parse::<RunId>()?;
        let sequence = probe["sequence"]
            .as_u64()
            .expect("original notification sequence");
        let generation = probe["generation"]
            .as_u64()
            .expect("original notification generation");
        let inner = OwnedOpenCode::connect_app_store(journey.service.durable_root()).await;
        let control = ForwardingControl::conversation_recovery();
        let read = std::sync::Arc::new(RecoveryRead {
            fault,
            lower_error: matches!(fault, RecoveryReadFault::LowerErrorHeld)
                .then(|| std::sync::Arc::clone(&control)),
            selected: std::sync::Mutex::new(None),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            dropped: std::sync::Mutex::new(None),
        });
        let agent = OwnedOpenCode::build_agent(
            journey.service.durable_root(),
            &journey.service.endpoint(),
            "agent-state",
        )
        .await;
        let mut app = App::open(Self::recovery_app_config_with_agent(
            RecoveryStore {
                inner: inner.clone(),
                read: std::sync::Arc::clone(&read),
            },
            control.wrap(agent),
        ))
        .await?;
        let workspace = app.workspace(workspace_id).await?;
        let workflow = workspace.workflow("agent-question".parse()?).await?;
        let run = workflow.open_run(RunSelector::Latest(run_id)).await?;
        let fatal = !matches!(fault, RecoveryReadFault::Recoverable);
        let mut service_stopped = false;
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(70), async {
            tokio::time::timeout(Duration::from_secs(30), control.wait_until_method_blocked())
                .await.expect("the original recovery reaches real get_conversation before fault arming");
            let before = run.wait().await?;
            let [Interaction::Notification { sequence: observed_sequence, generation: observed_generation, notification }] = before.interactions.as_slice()
                else { panic!("the original retained Pending notification survives process loss") };
            assert_eq!((*observed_sequence, *observed_generation), (sequence, generation));
            assert_eq!(notification.id().to_string(), Self::probe_id(&probe, "notification"));
            let inserted_at = notification.inserted_at().duration_since(std::time::UNIX_EPOCH)?;
            assert_eq!(json!({"seconds":inserted_at.as_secs(),"nanoseconds":inserted_at.subsec_nanos()}), probe["inserted_at"]);
            let ConversationNotificationKind::Interaction { title, body, response_schema, dismissible } = notification.kind()
                else { panic!("retained notification kind remains exact") };
            assert_eq!(json!({"title":title,"body":body,"response_schema":response_schema,"dismissible":dismissible}), probe["payload"]);
            let head_key = StateKey::new("app-sdk", format!(
                "run/{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), before.occurrence,
            ))?;
            let original_head = inner.load(head_key.clone()).await?.expect("native original run head");
            if matches!(fault, RecoveryReadFault::LowerErrorHeld) {
                // This isolated fixture owns this server. The held forwarding
                // gate precedes its actual native get_conversation call.
                journey.service.stop().await;
                service_stopped = true;
            }
            *read.selected.lock().unwrap() = Some(head_key.clone());
            control.release_method();
            tokio::time::timeout(Duration::from_secs(30), read.entered.notified()).await
                .expect("the original recovery observes current authority after lower setup");
            assert!(read.selected.lock().unwrap().is_none());
            assert_eq!(control.prompt_count(), 0, "recovery never dispatches another lower prompt");
            if matches!(fault, RecoveryReadFault::LowerErrorHeld) {
                assert!(control.recovery_failure_owner().is_some(), "the actual lower lookup returned an error before the selected authority read");
            }
            if !matches!(fault, RecoveryReadFault::Held | RecoveryReadFault::LowerErrorHeld) { read.release.notify_one(); }
            if fatal {
                tokio::time::timeout(Duration::from_secs(if matches!(fault, RecoveryReadFault::Held | RecoveryReadFault::LowerErrorHeld) { 33 } else { 3 }), async {
                    loop {
                        match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                            Err(AppError::Closed(_)) => break,
                            Ok(_) => tokio::task::yield_now().await,
                            Err(error) => panic!("unexpected recovery closure observation: {error}"),
                        }
                    }
                }).await.expect("unreadable retained notification authority closes within its pure-read pass");
                let dropped = read.dropped.lock().unwrap().take().expect("the original read owns its destruction receipt");
                assert!(tokio::time::timeout(Duration::from_secs(1), dropped).await
                    .expect("the original held read is destroyed before fatal cleanup").is_err());
                assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
                let current = inner.load(head_key.clone()).await?.expect("native retained authority remains");
                assert_eq!(current.revision, original_head.revision);
                assert_eq!(current.record.value, original_head.record.value);
            } else {
                // Agent reopen cancels a lost lower continuation before it
                // publishes the reopened Agent. App retains its original Pending
                // evidence; that does not make the cancelled lower target resolvable.
                let dropped = read.dropped.lock().unwrap().take()
                    .expect("the original transient read owns its completion receipt");
                assert!(tokio::time::timeout(Duration::from_secs(1), dropped).await
                    .expect("the original transient read completes before cancellation").is_err());
                let retained = run.wait().await?;
                assert_eq!(retained.checkpoint, before.checkpoint);
                assert_eq!(retained.state.values(), before.state.values());
                assert_eq!(retained.last_action_ordinal, before.last_action_ordinal);
                assert!(retained.outcomes == before.outcomes);
                let [Interaction::Notification { sequence: retained_sequence, generation: retained_generation, notification: retained_notification }] = retained.interactions.as_slice()
                    else { panic!("App preserves the original Pending evidence until explicit settlement") };
                assert_eq!((*retained_sequence, *retained_generation), (sequence, generation));
                assert_eq!(retained_notification.id(), notification.id());
                assert_eq!(retained_notification.inserted_at(), notification.inserted_at());
                assert_eq!(retained_notification.kind(), notification.kind());
                let current = inner.load(head_key.clone()).await?.expect("the original native App evidence remains");
                assert_eq!(current.revision, original_head.revision);
                assert_eq!(current.record.value, original_head.record.value);
                assert!(matches!(run.resolve(sequence, generation, Resolution::Notification(
                    NotificationResolution::Submit { content: json!({"answers":[["Yes"]]}) },
                )).await, Err(AppError::Busy(_))), "a cancelled lower continuation cannot admit another decision");
                assert_eq!(control.resolution_count(), 0);
                let cancellation = run.cancel(retained.checkpoint).await?;
                assert!(cancellation.checkpoint > before.checkpoint);
                let terminal = tokio::time::timeout(Duration::from_secs(30), run.wait()).await
                    .expect("the original lower owner settles explicit App cancellation")?;
                assert!(matches!(terminal.status, RunStatus::Cancelled));
                assert_eq!(terminal.state.values(), before.state.values());
                assert_eq!(terminal.last_action_ordinal, before.last_action_ordinal);
                assert!(terminal.outcomes == before.outcomes);
                assert!(terminal.interactions.is_empty());
                let replay = run.cancel(retained.checkpoint).await?;
                assert_eq!(replay.checkpoint, terminal.checkpoint);
                assert!(matches!(replay.status, RunStatus::Cancelled));
                assert_eq!(control.resolution_count(), 0, "cancellation never replays a lower notification decision");
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await?;
            }
            assert_eq!(control.prompt_count(), 0, "only the killed process submitted the original prompt");
            assert_eq!(control.resolution_count(), 0, "recovery does not replay the original lower decision");
            Ok::<_, Box<dyn Error + Send + Sync>> (())
        })).catch_unwind().await;
        *read.selected.lock().unwrap() = None;
        read.release.notify_one();
        control.release_method();
        let restarted = if service_stopped {
            Some(
                std::panic::AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(120),
                    journey.service.start(),
                ))
                .catch_unwind()
                .await,
            )
        } else {
            None
        };
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(120),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(30),
            }),
        ))
        .catch_unwind()
        .await;
        drop(run);
        drop(workflow);
        drop(workspace);
        drop(app);
        drop(inner);
        let service_cleanup =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
                if matches!(&cleanup, Ok(Ok(Ok(())))) {
                    journey.service.cleanup().await;
                } else {
                    journey.service.stop_and_retain_paths().await;
                    eprintln!("strict-live retained paths after unconfirmed App cleanup");
                }
            }))
            .catch_unwind()
            .await;
        if let Some(restarted) = &restarted {
            Self::report_cleanup("service restart", restarted);
        }
        Self::report_cleanup("App", &cleanup);
        Self::report_cleanup("service", &service_cleanup);
        match observed {
            Ok(result) => result.expect("the controlled recovery proof remains bounded")?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(restarted) = restarted {
            restarted
                .expect("the owned service restart does not panic")
                .expect("the owned service restarts before joined lower cleanup");
        }
        let closed = cleanup
            .expect("recovered lower cleanup does not panic")
            .expect("recovered lower cleanup joins");
        if fatal {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        } else {
            closed?;
        }
        service_cleanup
            .expect("real service cleanup does not panic")
            .expect("real service cleanup joins");
        assert_eq!(control.prompt_count(), 0);
        assert_eq!(control.resolution_count(), 0);
        Ok(())
    }
}

#[tokio::test]
async fn recoverable_read_keeps_cancelled_lower_notification_projectable_until_app_cancellation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    RecoveryJourney::recovered_notification_authority(RecoveryReadFault::Recoverable).await
}

#[tokio::test]
async fn permanent_authority_read_closes_real_notification_recovery_without_replay()
-> Result<(), Box<dyn Error + Send + Sync>> {
    RecoveryJourney::recovered_notification_authority(RecoveryReadFault::Permanent).await
}

#[tokio::test]
async fn held_authority_read_expires_real_notification_recovery_without_replay()
-> Result<(), Box<dyn Error + Send + Sync>> {
    RecoveryJourney::recovered_notification_authority(RecoveryReadFault::Held).await
}

#[tokio::test]
async fn held_authority_after_real_recovery_error_expires_without_prompt_replay()
-> Result<(), Box<dyn Error + Send + Sync>> {
    RecoveryJourney::recovered_notification_authority(RecoveryReadFault::LowerErrorHeld).await
}
