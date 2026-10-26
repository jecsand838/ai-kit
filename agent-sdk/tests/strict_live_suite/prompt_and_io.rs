use crate::native_cleanup::NativeCleanup;

use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use agent_sdk::identity::{AgentId, ConversationId, WorkspaceId};
use agent_sdk::notification::{ConversationNotificationKind, NotificationResolution};
use agent_sdk::prompt::{PromptEvent, PromptRequest, StructuredOutputRequest};
use agent_sdk::tool::ToolSelection;
use agent_sdk::{AgentBackend, WorkspaceBackend};
use agent_sdk::{AgentBuilder, ConversationBackend};
use futures_util::{FutureExt, StreamExt};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::StateKey;
use storage_sdk::{StateStore, StateStoreBackend};
use tool_sdk::Toolset;
use tool_sdk::declaration::{ToolInfo, ToolName};
use tool_sdk::execution::ToolResult;
use tool_sdk::interaction::ToolInteractionCapabilities;
use tool_sdk::local::ToolRegistration;
use tool_sdk::schema::JsonSchema;

use super::delivery_and_events::DeliveryScenario;
use super::fixture::{StrictBackend, StrictFixture, StrictOpenCodeProcess};
use crate::opencode_question_evidence::readback::{FreshOutputExpectation, NativeQuestionReadback};

const NATIVE_IDLE_REOPEN_TEST: &str =
    "strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build";
const NATIVE_IDLE_REOPEN_LABEL: &str = "strict-opencode-native-idle-reopen";
const PENDING_INTERACTION_REOPEN_TEST: &str = "strict_live_suite::prompt_and_io::opencode_reopen_cancels_recovered_pending_interaction_before_publication";
const PENDING_INTERACTION_REOPEN_LABEL: &str = "strict-opencode-pending-interaction-reopen";

struct TerminalCrashWake;

impl TerminalCrashWake {
    async fn run_child(handoff: &Path) {
        let endpoint = std::fs::read_to_string(handoff).expect("child endpoint must be readable");
        let fixture = StrictFixture::new().await;
        let (agent, workspace, conversation) = fixture
            .hierarchy(
                StrictOpenCodeProcess::external_config(
                    endpoint.clone(),
                    NATIVE_IDLE_REOPEN_LABEL,
                    Duration::from_secs(60),
                ),
                "strict-native-idle-workspace",
                "native idle terminal waiter",
            )
            .await;
        let model = match std::panic::AssertUnwindSafe(async {
            DeliveryScenario::opencode_model(&agent).await
        })
        .catch_unwind()
        .await
        {
            Ok(model) => model,
            Err(panic) => {
                let _ = agent.shutdown().await;
                std::panic::resume_unwind(panic);
            }
        };
        let facts = serde_json::to_vec(&(agent.id(), workspace.id(), conversation.id())).unwrap();
        std::fs::write(handoff, facts).unwrap();

        let mut receiver = conversation.notification_stream().unwrap();
        let mut prompt = Box::pin(
            conversation.prompt(
                PromptRequest::new(DeliveryScenario::opencode_question_prompt())
                    .unwrap()
                    .with_model(model)
                    .with_timeout(Duration::from_secs(600))
                    .unwrap(),
            ),
        );
        let interaction = loop {
            tokio::select! {
                notification = receiver.recv() => {
                    let notification = notification
                        .expect("native-idle delivery must remain open")
                        .expect("native-idle notification must succeed");
                    if let ConversationNotificationKind::Interaction {
                        response_schema: Some(_),
                        ..
                    } = notification.kind()
                    {
                        break notification;
                    }
                }
                result = &mut prompt => {
                    if let Ok(context) = serde_json::to_string(&(
                        NATIVE_IDLE_REOPEN_TEST, agent.id(), workspace.id(), conversation.id(),
                    )) {
                        let _ = writeln!(std::io::stderr().lock(), "AGENT_SDK_EARLY_CHILD_FAILURE_IDS={context}");
                    }
                    panic!("prompt terminalized before its question: {result:?}")
                }
            }
        };
        let statuses = reqwest::get(format!(
            "{endpoint}/session/status?directory={}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
        assert!(statuses.as_object().is_some_and(|value| !value.is_empty()));
        let waker = Waker::from(Arc::new(Self));
        assert!(matches!(
            prompt.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        let _answer =
            DeliveryScenario::resolve_opencode_question(&conversation, &interaction).await;
        std::future::pending::<()>().await;
    }
}

impl Wake for TerminalCrashWake {
    fn wake(self: Arc<Self>) {
        std::process::exit(0);
    }
}

struct PendingInteractionCrash;

impl PendingInteractionCrash {
    async fn capture_abort_progress(
        process: &StrictOpenCodeProcess,
        store: &StateStore,
        session_id: &str,
        conversation_id: &ConversationId,
        phase: &str,
    ) {
        use std::hash::{Hash, Hasher};

        let state = store
            .load(StateKey::new("agent_sdk_opencode_prompts_v1", conversation_id.as_str()).unwrap())
            .await;
        let state = match state {
            Ok(Some(snapshot)) => {
                let value = &snapshot.record.value;
                let mut generation = std::collections::hash_map::DefaultHasher::new();
                value["message_id"].as_str().hash(&mut generation);
                let mut request_part = std::collections::hash_map::DefaultHasher::new();
                value["request_part_id"].as_str().hash(&mut request_part);
                let phase = match value["phase"]["phase"].as_str() {
                    Some("prepared") => "prepared",
                    Some("dispatched_awaiting_admission") => "dispatched_awaiting_admission",
                    Some("admitted") => "admitted",
                    Some("response_lost_before_admission") => "response_lost_before_admission",
                    Some("response_lost_after_admission") => "response_lost_after_admission",
                    Some("terminal") => "terminal",
                    _ => "unknown",
                };
                let cancellation = match value["cancellation"]["state"].as_str() {
                    Some("not_requested") => "not_requested",
                    Some("requested") => "requested",
                    Some("reserved") => "reserved",
                    Some("dispatching") => "dispatching",
                    Some("accepted") => "accepted",
                    Some("rejected") => "rejected",
                    Some("ambiguous") => "ambiguous",
                    Some("unnecessary") => "unnecessary",
                    _ => "unknown",
                };
                let waiter = match value["waiter"].as_str() {
                    Some("waiting") => "waiting",
                    Some("delivering") => "delivering",
                    Some("delivered") => "delivered",
                    Some("lost") => "lost",
                    Some("timed_out") => "timed_out",
                    _ => "unknown",
                };
                serde_json::json!({"present":true,"revision":snapshot.revision.get(),
                    "generation_fingerprint":format!("{:016x}",generation.finish()),
                    "request_part_present":value["request_part_id"].is_string(),
                    "request_part_fingerprint":format!("{:016x}",request_part.finish()),
                    "input_observed":value["input_observed"].as_bool(),
                    "phase":phase,"cancellation":cancellation,"waiter":waiter})
            }
            Ok(None) => serde_json::json!({"present":false}),
            Err(_) => serde_json::json!({"read":"unavailable"}),
        };
        eprintln!("opencode-generation phase={phase}; durable_snapshot_after_observation={state}");

        let native = process.abort_evidence(session_id).await;
        eprintln!(
            "phase={phase}; evidence={}",
            serde_json::json!({"state":state,"native":native})
        );
    }

    async fn wait_until_native_active(endpoint: &str) {
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let statuses = client
                    .get(format!("{endpoint}/session/status"))
                    .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
                    .send()
                    .await
                    .expect("OpenCode native status must respond")
                    .error_for_status()
                    .expect("OpenCode native status must succeed")
                    .json::<serde_json::Value>()
                    .await
                    .expect("OpenCode native status must decode");
                let statuses = statuses
                    .as_object()
                    .expect("OpenCode native status must be an object");
                assert!(
                    statuses.len() <= 1,
                    "strict-live process must not own multiple active native sessions"
                );
                if statuses.len() == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("OpenCode native activity must become observable");
    }

    async fn wait_until_native_idle(endpoint: &str, session_id: &str) {
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let statuses = client
                    .get(format!("{endpoint}/session/status"))
                    .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
                    .send()
                    .await
                    .expect("OpenCode native status must respond")
                    .error_for_status()
                    .expect("OpenCode native status must succeed")
                    .json::<serde_json::Value>()
                    .await
                    .expect("OpenCode native status must decode");
                let statuses = statuses
                    .as_object()
                    .expect("OpenCode native status must be an object");
                assert!(
                    statuses.len() <= 1,
                    "strict-live process must not own multiple active native sessions"
                );
                if !statuses.contains_key(session_id) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("OpenCode native session must become idle");
    }

    async fn run_child(handoff: &Path) {
        let endpoint = std::fs::read_to_string(handoff)
            .expect("pending-interaction child endpoint must be readable");
        let fixture = StrictFixture::new().await;
        let (agent, workspace, conversation) = fixture
            .hierarchy(
                StrictOpenCodeProcess::external_config(
                    endpoint.clone(),
                    PENDING_INTERACTION_REOPEN_LABEL,
                    Duration::from_secs(60),
                ),
                "strict-pending-interaction-reopen-workspace",
                "pending interaction reopen",
            )
            .await;
        let model = match std::panic::AssertUnwindSafe(async {
            DeliveryScenario::opencode_model(&agent).await
        })
        .catch_unwind()
        .await
        {
            Ok(model) => model,
            Err(panic) => {
                let _ = agent.shutdown().await;
                std::panic::resume_unwind(panic);
            }
        };
        let mut receiver = conversation
            .notification_stream()
            .expect("pending-interaction receiver must open");
        let mut prompt = Box::pin(
            conversation.prompt(
                PromptRequest::new(DeliveryScenario::opencode_question_prompt())
                    .expect("pending-interaction question prompt must be valid")
                    .with_model(model)
                    .with_timeout(Duration::from_secs(600))
                    .expect("pending-interaction question timeout must be valid"),
            ),
        );
        tokio::time::timeout(Duration::from_secs(600), async {
            loop {
                tokio::select! {
                    notification = receiver.recv() => {
                        let notification = notification
                            .expect("pending-interaction delivery must remain open")
                            .expect("pending-interaction notification must succeed");
                        if matches!(
                            notification.kind(),
                            ConversationNotificationKind::Interaction {
                                response_schema: Some(_),
                                ..
                            }
                        ) {
                            break;
                        }
                    }
                    result = &mut prompt => {
                        if let Ok(context) = serde_json::to_string(&(
                            PENDING_INTERACTION_REOPEN_TEST, agent.id(), workspace.id(), conversation.id(),
                        )) {
                            let _ = writeln!(std::io::stderr().lock(), "AGENT_SDK_EARLY_CHILD_FAILURE_IDS={context}");
                        }
                        panic!("prompt terminalized before its pending interaction: {result:?}")
                    }
                }
            }
        })
        .await
        .expect("pending-interaction admission must remain bounded");
        Self::wait_until_native_active(&endpoint).await;
        let facts = serde_json::to_vec(&(agent.id(), workspace.id(), conversation.id()))
            .expect("pending-interaction handoff must encode");
        std::fs::write(handoff, facts).expect("pending-interaction handoff must be written");
        std::process::exit(0);
    }
}

pub(super) struct PromptScenario {
    backend: StrictBackend,
}

#[tokio::test]
async fn codex_reasoning_selection_reset_and_reopen_are_live() {
    PromptScenario::codex_effort_selection_and_reset().await;
}

#[tokio::test]
async fn opencode_cloud_generation_and_streaming_are_live() {
    PromptScenario::opencode_cloud_generation().await;
}

impl PromptScenario {
    async fn codex_effort_selection_and_reset() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1_200);
        let fixture = tokio::time::timeout_at(deadline, StrictFixture::new())
            .await
            .unwrap();
        let label = StrictFixture::unique("strict-codex-effort");
        let config = StrictFixture::config(StrictBackend::Codex, &label, Duration::from_secs(90));
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = NativeCleanup::observe(deadline, async {
            let (agent, workspace, conversation) = fixture
                .hierarchy_before(
                    config.clone(),
                    "strict-codex-effort-workspace",
                    "effort selection and reset",
                    deadline,
                )
                .await;
            let agent = owned_agent.insert(agent);
            let workspace = owned_workspace.insert(workspace);
            let conversation = owned_conversation.insert(conversation);
            let model =
                StrictFixture::model(agent, StrictBackend::Codex, "TEST_CODEX_MODEL_DEFAULT").await;
            let effort = agent_sdk::model::ReasoningEffort::new(StrictFixture::required(
                "TEST_CODEX_REASONING_EFFORT",
            ))
            .unwrap();
            let models = agent.list_models().await.unwrap();
            assert!(
                models
                    .iter()
                    .find(|entry| entry.selector() == &model)
                    .unwrap()
                    .reasoning_efforts()
                    .contains(&effort)
            );
            for explicit in [true, false] {
                let token = StrictFixture::unique("EFFORT_SELECTION");
                let mut request = PromptRequest::new(format!(
                    "Do not call tools. Return only this exact token: {token}"
                ))
                .unwrap()
                .with_model(model.clone())
                .with_timeout(Duration::from_secs(300))
                .unwrap();
                if explicit {
                    request = request.with_reasoning_effort(effort.clone());
                }
                let response = conversation.prompt(request).await.unwrap();
                assert_eq!(response.output(), token);
            }
            (agent.id(), workspace.id(), conversation.id(), model)
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        if matches!(&observed, Ok(Ok(_))) {
            drop(owned_conversation.take());
            drop(owned_workspace.take());
        } else {
            if let Some(conversation) = owned_conversation.take() {
                cleanup.settle(conversation.close()).await;
            }
            if let Some(workspace) = owned_workspace.take() {
                cleanup.settle(workspace.close()).await;
            }
        }
        if let Some(agent) = owned_agent.take() {
            cleanup.settle(agent.shutdown()).await;
        }
        let (agent_id, workspace_id, conversation_id, model) = cleanup.finish(observed);
        let observed = NativeCleanup::observe(deadline, async {
            let agent = owned_agent.insert(fixture.reopen(config, agent_id).await);
            let workspace =
                owned_workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
            let conversation = owned_conversation
                .insert(workspace.get_conversation(conversation_id).await.unwrap());
            let token = StrictFixture::unique("EFFORT_REOPENED");
            let response = conversation
                .prompt(
                    PromptRequest::new(format!(
                        "Do not call tools. Return only this exact token: {token}"
                    ))
                    .unwrap()
                    .with_model(model)
                    .with_timeout(Duration::from_secs(300))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.output(), token);
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        if let Some(conversation) = owned_conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = owned_workspace {
            cleanup.settle(workspace.close()).await;
        }
        if let Some(agent) = owned_agent {
            cleanup.settle(agent.shutdown()).await;
        }
        cleanup.finish(observed);
    }

    async fn opencode_cloud_generation() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(900);
        let fixture = tokio::time::timeout_at(deadline, StrictFixture::new())
            .await
            .unwrap();
        let model = StrictFixture::required("TEST_OPENCODE_CLOUD_MODEL");
        let config = serde_json::json!({
            "share":"disabled", "enabled_providers":["openai"], "plugin":[], "mcp":{},
            "model":format!("openai/{model}"), "small_model":format!("openai/{model}"),
            "permission":{"*":"deny"}, "agent":{"build":{"permission":{"*":"deny"}}},
            "provider":{"openai":{"whitelist":[model.clone()],
                "options":{"apiKey":StrictFixture::required("TEST_OPENAI_API_KEY"), "baseURL":StrictFixture::required("TEST_OPENAI_BASE_URL")},
                "models":{(model.clone()):{"name":model}}
            }}
        });
        let mut process = StrictOpenCodeProcess::configured("strict-cloud", config.to_string());
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = NativeCleanup::observe(deadline, async {
            process.launch().await;
            let (agent, workspace, conversation) = fixture
                .hierarchy_before(
                    process.config("strict-opencode-cloud", Duration::from_secs(90)),
                    "strict-opencode-cloud-workspace",
                    "cloud generation",
                    deadline,
                )
                .await;
            let agent = owned_agent.insert(agent);
            owned_workspace = Some(workspace);
            let conversation = owned_conversation.insert(conversation);
            let model =
                StrictFixture::model(agent, StrictBackend::OpenCode, "TEST_OPENCODE_CLOUD_MODEL")
                    .await;
            let token = StrictFixture::unique("CLOUD_NATIVE");
            let response = conversation
                .prompt(
                    PromptRequest::new(format!(
                        "Do not call tools. Return only this exact token: {token}"
                    ))
                    .unwrap()
                    .with_model(model.clone())
                    .with_timeout(Duration::from_secs(300))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.output(), token);
            let token = StrictFixture::unique("CLOUD_STREAM");
            let mut stream = conversation
                .prompt_stream(
                    PromptRequest::new(format!(
                        "Do not call tools. Return only this exact token: {token}"
                    ))
                    .unwrap()
                    .with_model(model)
                    .with_timeout(Duration::from_secs(300))
                    .unwrap(),
                )
                .await
                .unwrap();
            let mut text = String::new();
            loop {
                match stream.recv().await.unwrap().unwrap() {
                    PromptEvent::OutputDelta { text: delta } => {
                        assert!(!delta.is_empty());
                        assert!(text.len().saturating_add(delta.len()) <= 64 * 1024);
                        text.push_str(&delta);
                    }
                    PromptEvent::Completed(response) => {
                        assert!(!text.is_empty(), "real native streaming text is required");
                        assert_eq!(response.output(), token);
                        assert_eq!(text, response.output());
                        assert!(stream.recv().await.is_none());
                        break;
                    }
                }
            }
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        if let Some(conversation) = owned_conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = owned_workspace {
            cleanup.settle(workspace.close()).await;
        }
        if let Some(agent) = owned_agent {
            cleanup.settle(agent.shutdown()).await;
        }
        cleanup
            .settle::<agent_sdk::SdkError>(async {
                process.shutdown().await;
                Ok(())
            })
            .await;
        cleanup.finish(observed);
    }

    async fn structured_diagnostic(
        endpoint: std::net::SocketAddr,
        plain_prompt: &str,
        structured_prompt: &str,
        schema: &serde_json::Value,
        configured_model: &str,
    ) -> Result<serde_json::Value, &'static str> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| "client")?;
        // Pinned 1.17.18: history is a read-only POST; {} requests full history.
        let mut response = client
            .post(format!("http://{endpoint}/sync/history"))
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|_| "request")?;
        if !response.status().is_success() {
            return Err("http_status");
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "body")? {
            if body.len().saturating_add(chunk.len()) > 16 << 20 {
                return Err("body_bound");
            }
            body.extend_from_slice(&chunk);
        }
        let history: serde_json::Value = serde_json::from_slice(&body).map_err(|_| "json")?;
        let rows = history.as_array().ok_or("history_shape")?;
        if rows.len() > 16_384 {
            return Err("row_bound");
        }
        let mut messages = std::collections::BTreeMap::new();
        let mut parts = std::collections::BTreeMap::new();
        let mut sequences = std::collections::BTreeSet::new();
        let mut removals = std::collections::BTreeSet::new();
        for row in rows {
            let session = row["aggregate_id"].as_str().ok_or("aggregate")?;
            let seq = row["seq"].as_u64().ok_or("sequence")?;
            if !session.starts_with("ses") || !sequences.insert((session, seq)) {
                return Err("history_identity");
            }
            let (snapshot, target) = match row["type"].as_str().ok_or("event_type")? {
                "message.updated.1" => (&row["data"]["info"], &mut messages),
                "message.part.updated.1" => (&row["data"]["part"], &mut parts),
                "message.removed.1" | "message.part.removed.1" | "session.deleted.1" => {
                    removals.insert(session);
                    continue;
                }
                _ => continue,
            };
            let id = snapshot["id"].as_str().ok_or("snapshot_identity")?;
            if id.is_empty()
                || snapshot["sessionID"].as_str() != Some(session)
                || row["data"]["sessionID"].as_str() != Some(session)
            {
                return Err("snapshot_ownership");
            }
            let entry = target.entry((session, id)).or_insert((seq, snapshot));
            if seq > entry.0 {
                *entry = (seq, snapshot);
            }
        }
        let plain: Vec<_> = parts
            .iter()
            .filter(|(_, (_, part))| {
                part["type"] == "text" && part["synthetic"] != true && part["text"] == plain_prompt
            })
            .collect();
        if plain.len() != 1 {
            return Err("plain_correlation");
        }
        let ((session, _), (_, plain_part)) = plain[0];
        if removals.contains(session) {
            return Err("selected_history_removal");
        }
        let plain_id = plain_part["messageID"].as_str().ok_or("plain_parent")?;
        let (plain_seq, plain_user) = messages.get(&(*session, plain_id)).ok_or("plain_user")?;
        if plain_user["role"] != "user" {
            return Err("plain_role");
        }
        let structured: Vec<_> = parts
            .iter()
            .filter(|((candidate, _), (_, part))| {
                candidate == session
                    && part["type"] == "text"
                    && part["synthetic"] != true
                    && part["text"] == structured_prompt
            })
            .collect();
        if structured.len() != 1 {
            return Err("structured_correlation");
        }
        let (_, (_, part)) = structured[0];
        let user_id = part["messageID"].as_str().ok_or("structured_parent")?;
        let (seq, user) = messages
            .get(&(*session, user_id))
            .ok_or("structured_user")?;
        if user["role"] != "user" || seq <= plain_seq || user_id == plain_id {
            return Err("structured_order");
        }
        let validator = jsonschema::draft202012::options()
            .build(schema)
            .map_err(|_| "schema")?;
        let fingerprint = |id: &str| {
            use std::hash::{Hash, Hasher};
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            id.hash(&mut hash);
            format!("{:016x}", hash.finish())
        };
        let mut assistants = Vec::new();
        for ((candidate, id), (_, message)) in &messages {
            if candidate != session
                || message["role"] != "assistant"
                || message["parentID"].as_str() != Some(user_id)
            {
                continue;
            }
            if assistants.len() == 4 {
                return Err("assistant_bound");
            }
            let mut tool_states = [0_usize; 5];
            let mut valid_tool_inputs = 0_usize;
            for ((part_session, _), (_, part)) in &parts {
                if part_session != session || part["messageID"].as_str() != Some(*id) {
                    continue;
                }
                if part["type"] == "tool" && part["tool"] == "StructuredOutput" {
                    let index = match part["state"]["status"].as_str() {
                        Some("pending") => 0,
                        Some("running") => 1,
                        Some("completed") => 2,
                        Some("error") => 3,
                        _ => 4,
                    };
                    tool_states[index] += 1;
                    valid_tool_inputs += usize::from(validator.is_valid(&part["state"]["input"]));
                }
            }
            let finish = match message["finish"].as_str() {
                Some("stop") => "stop",
                Some("tool-calls") => "tool-calls",
                Some("length") => "length",
                Some("content-filter") => "content-filter",
                None => "absent",
                _ => "unknown",
            };
            let error = match message["error"]["name"].as_str() {
                Some("StructuredOutputError") => "structured_output",
                Some("APIError") => "api",
                None if message.get("error").is_none() => "absent",
                _ => "other",
            };
            assistants.push(serde_json::json!({
                "assistant":fingerprint(id),"parent":fingerprint(user_id),
                "finish":finish,"error":error,"completed":message["time"]["completed"].is_number(),
                "structured_present":message.get("structured").is_some(),
                "structured_valid":message.get("structured").is_some_and(|value| validator.is_valid(value)),
                "model_matches_user":message["modelID"].as_str().zip(user["model"]["modelID"].as_str()).is_some_and(|(actual, expected)| actual == expected)
                    && message["providerID"].as_str().zip(user["model"]["providerID"].as_str()).is_some_and(|(actual, expected)| actual == expected),
                "structured_tool_states":tool_states,"valid_tool_inputs":valid_tool_inputs,
                "native_retries":message["error"]["data"]["retries"].as_u64(),
            }));
        }
        if assistants.is_empty() {
            return Err("assistant_missing");
        }
        Ok(serde_json::json!({
            "capture":"complete","plain_root":true,"structured_root":true,
            "session":fingerprint(session),"plain_user":fingerprint(plain_id),"structured_user":fingerprint(user_id),
            "format_matches":user["format"]["type"] == "json_schema" && user["format"]["schema"] == *schema,
            "retry_count":user["format"]["retryCount"].as_u64(),
            "model_present":user["model"]["providerID"].as_str().is_some_and(|id| !id.is_empty()) && user["model"]["modelID"].as_str().is_some_and(|id| !id.is_empty()),
            "model_id_matches_configured_name":user["model"]["modelID"] == configured_model,
            "assistants":assistants,
        }))
    }

    pub(super) async fn abort_stream(
        conversation: &impl ConversationBackend,
        request: PromptRequest,
    ) {
        let mut stream = conversation
            .prompt_stream(request)
            .await
            .expect("live stream admission");
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(300), stream.recv()).await.expect("first live fragment").expect("first event").expect("real source output"),
                PromptEvent::OutputDelta { text } if !text.is_empty()
            ),
            "abort proof requires real output from an accepted native generation"
        );
        NativeCleanup::abort_stream(&mut stream, conversation.abort()).await;
    }

    async fn run_stream(self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1_200);
        let fixture = tokio::time::timeout_at(deadline, StrictFixture::new())
            .await
            .expect("strict-live storage setup must stay within the observation deadline");
        let label = StrictFixture::unique(match self.backend {
            StrictBackend::Codex => "strict-codex-stream",
            StrictBackend::OpenCode => "strict-opencode-stream",
        });
        let config = StrictFixture::config(self.backend, &label, Duration::from_secs(90));
        let (agent, workspace, conversation) = fixture
            .hierarchy_before(
                config.clone(),
                &StrictFixture::unique("strict-stream-workspace"),
                "stream and reopen",
                deadline,
            )
            .await;
        let agent_id = agent.id();
        let workspace_id = workspace.id();
        let conversation_id = conversation.id();
        let observed = NativeCleanup::observe(deadline, async {
            let selected = match self.backend {
                StrictBackend::Codex => StrictFixture::model(&agent, StrictBackend::Codex, "TEST_CODEX_MODEL_DEFAULT").await,
                StrictBackend::OpenCode => DeliveryScenario::opencode_model(&agent).await,
            };
            let stream = conversation.prompt_stream(
                PromptRequest::new("Without calling tools, explain how a Rust iterator works in sixteen numbered sentences.")
                    .expect("stream prompt").with_model(selected.clone()).with_timeout(Duration::from_secs(300)).expect("stream deadline"),
            ).await.expect("native incremental capability must be available");
            NativeCleanup::observe_stream(stream).await;
            Self::abort_stream(&conversation,
                PromptRequest::new("Without tools, write five thousand words explaining the history of computing, starting immediately with prose and continuing until all five thousand words are written.")
                    .expect("abort prompt").with_model(selected.clone()).with_timeout(Duration::from_secs(300)).expect("abort deadline"),
            ).await;
            selected
        }).await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        let successful = matches!(&observed, Ok(Ok(_)));
        if successful {
            // Successful first-phase records must survive the deliberate reopen.
            drop(conversation);
            drop(workspace);
        } else {
            cleanup.settle(conversation.close()).await;
            cleanup.settle(workspace.close()).await;
        }
        cleanup.settle(agent.shutdown()).await;
        let selected = cleanup.finish(observed);
        let agent = tokio::time::timeout_at(deadline, fixture.reopen(config, agent_id))
            .await
            .expect("strict-live Agent reopen must stay within the observation deadline");
        let observed = NativeCleanup::observe(deadline, async {
            agent
                .get_workspace(workspace_id)
                .await
                .expect("stream workspace reopen")
        })
        .await;
        let workspace = match observed {
            Ok(Ok(workspace)) => workspace,
            failed => {
                let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
                cleanup.settle(agent.shutdown()).await;
                cleanup.finish(failed);
                unreachable!("failed workspace reopen cannot succeed")
            }
        };
        let observed = NativeCleanup::observe(deadline, async {
            workspace
                .get_conversation(conversation_id)
                .await
                .expect("stream conversation reopen")
        })
        .await;
        let conversation = match observed {
            Ok(Ok(conversation)) => conversation,
            failed => {
                let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
                cleanup.settle(workspace.close()).await;
                cleanup.settle(agent.shutdown()).await;
                cleanup.finish(failed);
                unreachable!("failed conversation reopen cannot succeed")
            }
        };
        let observed = NativeCleanup::observe(deadline, async {
            NativeCleanup::observe_stream(conversation.prompt_stream(
                PromptRequest::new("Without tools, explain what an owned Rust value is in eight numbered sentences.")
                    .expect("reopened prompt").with_model(selected).with_timeout(Duration::from_secs(300)).expect("reopened deadline"),
            ).await.expect("reopened native stream must be usable")).await;
        }).await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        cleanup.settle(conversation.close()).await;
        cleanup.settle(workspace.close()).await;
        cleanup.settle(agent.shutdown()).await;
        cleanup.finish(observed);
    }

    async fn run(self) {
        let deadline = tokio::time::Instant::now()
            + match self.backend {
                StrictBackend::Codex => Duration::from_secs(300),
                StrictBackend::OpenCode => Duration::from_secs(900),
            };
        let fixture = tokio::time::timeout_at(deadline, StrictFixture::new())
            .await
            .expect("strict-live storage setup must stay within the observation deadline");
        let label = StrictFixture::unique(match self.backend {
            StrictBackend::Codex => "strict-codex-prompt",
            StrictBackend::OpenCode => "strict-opencode-prompt",
        });
        let (config, opencode_endpoint) = match self.backend {
            StrictBackend::Codex => (
                StrictFixture::config(self.backend, &label, Duration::from_secs(60)),
                None,
            ),
            StrictBackend::OpenCode => {
                let (config, endpoint) =
                    StrictFixture::opencode_config(&label, Duration::from_secs(60));
                (config, Some(endpoint))
            }
        };
        let (agent, workspace, conversation) = fixture
            .hierarchy_before(
                config,
                &StrictFixture::unique("strict-prompt-workspace"),
                "strict prompt",
                deadline,
            )
            .await;
        let relative = PathBuf::from(format!(
            "target/strict-live/{}.txt",
            StrictFixture::unique("file")
        ));
        let absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
        // Keep the SDK failure outside the future cancelled by the scenario deadline.
        let mut structured_failure = None;
        let observed = NativeCleanup::observe(deadline, async {
            let selected = StrictFixture::model(
                &agent,
                self.backend,
                match self.backend {
                    StrictBackend::Codex => "TEST_CODEX_MODEL_DEFAULT",
                    StrictBackend::OpenCode => "TEST_OLLAMA_MODEL_OPENCODE_DEFAULT",
                },
            )
            .await;
            if matches!(self.backend, StrictBackend::Codex) {
                let models = agent.list_models().await.expect("selected native catalog");
                assert!(models.iter().all(|model| model.personalities().is_empty()));
                let error = conversation
                    .prompt(
                        PromptRequest::new("This personality must be rejected before native work")
                            .unwrap()
                            .with_model(selected.clone())
                            .with_personality(
                                agent_sdk::model::PersonalitySelector::new("friendly").unwrap(),
                            ),
                    )
                    .await
                    .expect_err("Codex personality selection is unsupported");
                assert_eq!(error.kind(), agent_sdk::error::ErrorKind::Unsupported);
            }
            let token = StrictFixture::unique("PROMPT_TERMINAL");
            let plain_prompt = format!("Reply with exactly this token and nothing else: {token}");
            let response = conversation
                .prompt(
                    PromptRequest::new(plain_prompt.clone())
                        .expect("plain prompt must be valid")
                        .with_model(selected.clone())
                        .with_timeout(Duration::from_secs(300))
                        .expect("plain prompt timeout must be valid"),
                )
                .await
                .expect("strict-live plain prompt must complete");
            assert!(
                !response.output().is_empty(),
                "strict-live terminal output must not be empty"
            );
            let plain_output = response.output().to_owned();

            let structured_model = match self.backend {
                StrictBackend::Codex => selected,
                StrictBackend::OpenCode => DeliveryScenario::opencode_model(&agent).await,
            };
            let structured = StructuredOutputRequest::new(serde_json::json!({
                "type": "object",
                "properties": {
                    "ack": {"type": "string", "const": "STRUCTURED_ACK"}
                },
                "required": ["ack"],
                "additionalProperties": false
            }))
            .expect("strict-live output schema must be valid");
            let schema = structured.schema().clone();
            let structured_prompt =
                "Return the requested structured output with ack set to STRUCTURED_ACK.";
            let result = conversation
                .prompt(
                    PromptRequest::new(structured_prompt)
                        .expect("structured prompt must be valid")
                        .with_model(structured_model)
                        .with_structured_output(structured)
                        .with_timeout(Duration::from_secs(600))
                        .expect("structured prompt timeout must be valid"),
                )
                .await;
            let response = match result {
                Ok(response) => response,
                Err(error) => {
                    structured_failure = Some(error);
                    if let Some(endpoint) = opencode_endpoint {
                        let configured =
                            StrictFixture::required("TEST_OLLAMA_MODEL_OPENCODE_SHELL");
                        let diagnostic = tokio::time::timeout_at(
                            deadline.min(tokio::time::Instant::now() + Duration::from_secs(10)),
                            Self::structured_diagnostic(
                                endpoint,
                                &plain_prompt,
                                structured_prompt,
                                &schema,
                                &configured,
                            ),
                        )
                        .await;
                        let context = match diagnostic {
                            Ok(Ok(context)) => context,
                            Ok(Err(reason)) => {
                                serde_json::json!({"capture":"incomplete","reason":reason})
                            }
                            Err(_) => {
                                serde_json::json!({"capture":"incomplete","reason":"deadline"})
                            }
                        }
                        .to_string();
                        let context = if context.len() <= 4 * 1024 {
                            context.as_str()
                        } else {
                            "{\"capture\":\"incomplete\",\"reason\":\"output_bound\"}"
                        };
                        let _ = writeln!(
                            std::io::stderr().lock(),
                            "AGENT_SDK_STRUCTURED_DIAGNOSTIC={context}"
                        );
                    }
                    return;
                }
            };
            assert_eq!(
                response
                    .structured_output()
                    .and_then(|value| value.get("ack"))
                    .and_then(serde_json::Value::as_str),
                Some("STRUCTURED_ACK")
            );

            std::fs::create_dir_all(
                absolute
                    .parent()
                    .expect("strict-live artifact must have a parent"),
            )
            .expect("strict-live artifact directory must be created");
            std::fs::write(&absolute, b"strict-live-file\n")
                .expect("strict-live artifact must be written");
            match self.backend {
                StrictBackend::Codex => {
                    let file = conversation
                        .read_file(relative.clone())
                        .await
                        .expect("Codex must preserve exact portable file bytes");
                    assert_eq!(file.path(), relative);
                    assert_eq!(file.text(), Some("strict-live-file\n"));
                }
                StrictBackend::OpenCode => {
                    assert!(matches!(
                        (conversation
                            .read_file(relative.clone())
                            .await
                            .expect_err("OpenCode selected file route is not byte-exact"))
                        .kind(),
                        agent_sdk::error::ErrorKind::Unsupported
                    ));
                }
            }
            assert_eq!(plain_output, token);
        })
        .await;
        let observed = match structured_failure {
            Some(error) => {
                let failure = format!("strict-live structured prompt must complete: {error:?}");
                // finish resumes this panic after cleanup without invoking the hook.
                let _ = writeln!(std::io::stderr().lock(), "{failure}");
                Err(Box::new(failure) as Box<dyn std::any::Any + Send>)
            }
            None => observed,
        };
        let mut cleanup = NativeCleanup::new(Duration::from_secs(60));
        cleanup.settle(conversation.close()).await;
        cleanup.settle(workspace.close()).await;
        cleanup.settle(agent.shutdown()).await;
        cleanup
            .settle(async {
                match std::fs::remove_file(absolute) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(error),
                }
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_incremental_output_abort_and_reopen_are_live() {
    PromptScenario {
        backend: StrictBackend::Codex,
    }
    .run_stream()
    .await;
}

#[tokio::test]
async fn opencode_incremental_output_abort_and_reopen_are_live() {
    PromptScenario {
        backend: StrictBackend::OpenCode,
    }
    .run_stream()
    .await;
}

struct CodexCallerLossScenario;

impl CodexCallerLossScenario {
    async fn run() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(390);
        let fixture = tokio::time::timeout_at(deadline, StrictFixture::new())
            .await
            .expect("caller-loss storage setup must remain bounded");
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let name =
            ToolName::new("strict_prompt_gate").expect("strict-live gate tool name must be valid");
        let info = ToolInfo::new(
            name,
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "additionalProperties": false
            })),
        );
        let registration = ToolRegistration::function(info, {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move |_invocation| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    entered.add_permits(1);
                    release.notified().await;
                    Ok(ToolResult::text("gate released"))
                }
            }
        });
        let interaction_calls = Arc::new(AtomicUsize::new(0));
        let mut question = ToolInfo::new(
            ToolName::new("strict_stream_question").expect("question tool name"),
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "additionalProperties": false
            })),
        );
        question.description = Some(
            "Ask the caller for an answer and return that answer as plain text. In Code Mode, the return value is a string, not an object with a content array. Emit the returned string directly."
                .to_owned(),
        );
        question.interactions = ToolInteractionCapabilities {
            text: true,
            multiline_text: false,
            form: false,
            form_schema_validation: false,
        };
        let question = ToolRegistration::interactive(question, {
            let calls = Arc::clone(&interaction_calls);
            move |_invocation, context| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let answer = context
                        .request_text("Answer the strict Codex stream question", false)
                        .await?;
                    Ok(ToolResult::text(answer))
                }
            }
        });
        let toolset = Toolset::local(vec![registration, question])
            .expect("strict-live gate and question Toolset must build");
        let config = StrictFixture::config(
            StrictBackend::Codex,
            &StrictFixture::unique("strict-codex-caller-loss"),
            Duration::from_secs(60),
        );
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let mut owned_notifications = None;
        let mut owned_waiter = None;
        let observed = NativeCleanup::observe(deadline, async {
        let agent = owned_agent.insert(AgentBuilder::create(config, fixture.storage())
            .with_toolset(toolset)
            .build()
            .await
            .expect("strict-live Codex agent with tool must build"));
        let model = StrictFixture::model(agent, StrictBackend::Codex, "TEST_CODEX_MODEL_DEFAULT").await;
        let workspace = owned_workspace.insert(agent
            .new_workspace(StrictFixture::workspace(&StrictFixture::unique(
                "strict-caller-loss-workspace",
            )))
            .await
            .expect("strict-live caller-loss workspace must build"));
        let conversation = owned_conversation.insert(Arc::new(
            workspace
                .new_conversation(StrictFixture::conversation("strict caller loss"))
                .await
                .expect("strict-live caller-loss conversation must build"),
        ));
        let prompt = owned_waiter.insert({
            let conversation = Arc::clone(conversation);
            let model = model.clone();
            tokio::spawn(async move {
                conversation
                    .prompt(
                        PromptRequest::new(
                            "Call strict_prompt_gate exactly once, wait for its result, then reply.",
                        )
                        .expect("strict-live gate prompt must be valid")
                        .with_model(model)
                        .with_tool_selection(ToolSelection::Auto)
                        .expect("strict-live gate selection must be valid"),
                    )
                    .await
            })
        });
        entered
            .acquire()
            .await
            .expect("strict-live gate entry must be observable")
            .forget();
        prompt.abort();
        let joined = owned_waiter.as_mut().expect("prompt waiter must remain owned").await;
        owned_waiter.take();
        assert!(joined.expect_err("dropped prompt waiter must cancel locally").is_cancelled());
        release.notify_one();
        conversation
            .abort()
            .await
            .expect("runtime-owned prompt must remain abortable after waiter loss");

        let follow_up = conversation
            .prompt(
                PromptRequest::new("Do not call any tools. Reply with exactly RECOVERED.")
                    .expect("strict-live recovery prompt must be valid")
                    .with_model(model.clone())
                    .with_tool_selection(ToolSelection::Auto)
                    .expect("strict-live recovery selection must be valid")
                    .with_timeout(Duration::from_secs(90))
                    .expect("strict-live recovery timeout must be valid"),
            )
            .await
            .expect("conversation must remain usable after owned cancellation");

        let notifications = owned_notifications.insert(conversation
            .notification_stream()
            .expect("streamed Codex interaction receiver"));
        let stream = conversation.prompt_stream(
            PromptRequest::new(
                "Call strict_stream_question exactly once with empty arguments and wait for its real caller-supplied answer. Then reply with exactly that answer. Do not call strict_prompt_gate or any other tool."
            ).expect("streamed interaction prompt")
                .with_model(model)
                .with_tool_selection(ToolSelection::Auto).expect("streamed tool selection")
                .with_timeout(Duration::from_secs(90)).expect("streamed interaction deadline")
        ).await.expect("streamed Codex interaction admission");
        let mut observation = Box::pin(NativeCleanup::observe_stream(stream));
        let notification = tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                tokio::select! {
                    notification = notifications.recv() => {
                        let notification = notification.expect("interaction delivery open")
                            .expect("interaction delivery succeeds");
                        if matches!(notification.kind(), ConversationNotificationKind::Interaction { response_schema: Some(_), .. }) {
                            break notification;
                        }
                    }
                    response = &mut observation => panic!("stream completed before its interactive tool: {response:?}"),
                }
            }
        }).await.expect("real streamed tool interaction must arrive");
        assert_eq!(interaction_calls.load(Ordering::SeqCst), 1);
        let answer = StrictFixture::unique("CODEX_STREAM_ANSWER");
        conversation
            .resolve_notification(
                notification.id().clone(),
                NotificationResolution::Submit {
                    content: serde_json::Value::String(answer.clone()),
                },
            )
            .await
            .expect("streamed Codex tool must accept the caller answer");
        let response = tokio::time::timeout(Duration::from_secs(90), &mut observation)
            .await
            .expect("resolved streamed Codex tool must reach its authoritative terminal");
        assert_eq!(
            interaction_calls.load(Ordering::SeqCst),
            1,
            "one durable resolution must not replay its interactive handler"
        );
        drop(observation);
        assert_eq!(follow_up.output(), "RECOVERED");
        assert_eq!(response.output(), answer);
        }).await;
        release.notify_one();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        if let Some(waiter) = owned_waiter {
            waiter.abort();
            cleanup
                .settle(async {
                    match waiter.await {
                        Ok(_) => Ok(()),
                        Err(error) if error.is_cancelled() => Ok(()),
                        Err(error) => Err(error),
                    }
                })
                .await;
        }
        if let Some(conversation) = owned_conversation {
            cleanup
                .settle(async {
                    let conversation = Arc::try_unwrap(conversation).unwrap_or_else(|_| {
                        panic!("strict-live conversation owner must be unique after waiter join")
                    });
                    conversation.close().await
                })
                .await;
        }
        if let Some(mut notifications) = owned_notifications {
            cleanup
                .settle::<agent_sdk::SdkError>(async {
                    DeliveryScenario::drain(&mut notifications).await;
                    Ok(())
                })
                .await;
        }
        if let Some(workspace) = owned_workspace {
            cleanup.settle(workspace.close()).await;
        }
        if let Some(agent) = owned_agent {
            cleanup.settle(agent.shutdown()).await;
        }
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_prompt_model_structured_output_and_files_are_live() {
    PromptScenario {
        backend: StrictBackend::Codex,
    }
    .run()
    .await;
}

#[tokio::test]
async fn opencode_prompt_model_and_structured_output_are_live() {
    PromptScenario {
        backend: StrictBackend::OpenCode,
    }
    .run()
    .await;
}

#[tokio::test]
async fn codex_prompt_caller_loss_preserves_runtime_owned_cancellation() {
    // The existing caller-loss proof now includes one additional 90-second
    // streamed interactive turn on the same owned hierarchy.
    CodexCallerLossScenario::run().await;
}

#[tokio::test]
async fn opencode_native_idle_terminal_waiter_reopens_in_one_build() {
    if let Some(handoff) = StrictFixture::crash_handoff() {
        TerminalCrashWake::run_child(&handoff).await;
        return;
    }

    let fixture = StrictFixture::new().await;
    let mut process = StrictOpenCodeProcess::start("native-idle-reopen").await;
    let endpoint = process.endpoint();
    let mut owned_agent = None;
    let mut owned_workspace = None;
    let mut owned_conversation = None;
    let mut owned_receiver = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2_400);
    let mut primary_failure = None;
    let mut observed = NativeCleanup::observe(
        deadline,
        async {
    let handoff = match std::panic::AssertUnwindSafe(fixture.crash_child(
        NATIVE_IDLE_REOPEN_TEST, endpoint.as_bytes(), Duration::from_secs(900),
    )).catch_unwind().await {
        Ok(handoff) => handoff,
        Err(failure) => {
            primary_failure = Some(failure);
            let native = NativeQuestionReadback::new(&endpoint, deadline).await;
            let sessions = native.sessions().await;
            assert_eq!(sessions.len(), 1, "failed child must have one attributable native session");
            let session = sessions.first().expect("one native session remains");
            let failure = primary_failure.take().expect("original child failure remains owned");
            let context = native.capture_failure(session, DeliveryScenario::opencode_question_prompt(),
                &mut primary_failure, failure).await;
            let _ = writeln!(std::io::stderr().lock(), "question native child failure evidence={context}");
            std::panic::resume_unwind(primary_failure.take().expect("original child failure remains owned"));
        }
    };
    let (agent_id, workspace_id, conversation_id) = serde_json::from_slice::<(AgentId, WorkspaceId, ConversationId)>(
        &handoff,
    ).expect("native-idle handoff must decode");
    process.stop().await;
    process.restart().await;

    let agent = owned_agent.insert(fixture
        .reopen(
            process.config(NATIVE_IDLE_REOPEN_LABEL, Duration::from_secs(60)),
            agent_id,
        )
        .await);
    let model = DeliveryScenario::opencode_model(agent).await;
    let workspace = owned_workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
    let recovered = workspace.get_conversation(conversation_id).await.unwrap();
    recovered
        .close()
        .await
        .expect("recovered native-idle conversation must close");
    let native = NativeQuestionReadback::new(&process.endpoint(), deadline).await;
    let before = native.sessions().await;
    let conversation = owned_conversation.insert(workspace
        .new_conversation(StrictFixture::conversation(
            "post-restart interaction delivery",
        ))
        .await
        .expect("post-restart OpenCode conversation must be created"));
    let session = native.created_session(&before).await;
    let receiver = owned_receiver.insert(conversation
        .notification_stream()
        .expect("post-restart OpenCode receiver must open"));
    let mut prompt = Box::pin(
        conversation.prompt(
            PromptRequest::new(DeliveryScenario::opencode_question_prompt())
                .expect("post-restart OpenCode question prompt must be valid")
                .with_model(model)
                .with_timeout(Duration::from_secs(600))
                .expect("post-restart OpenCode question timeout must be valid"),
        ),
    );
    let interaction = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(600), async {
        loop {
            tokio::select! {
                notification = receiver.recv() => {
                    let notification = notification
                        .expect("post-restart OpenCode delivery must remain open")
                        .expect("post-restart OpenCode notification must succeed");
                    if matches!(
                        notification.kind(),
                        ConversationNotificationKind::Interaction {
                            response_schema: Some(_),
                            ..
                        }
                    ) {
                        break notification;
                    }
                }
                result = &mut prompt => {
                    panic!("post-restart OpenCode prompt completed before its interaction: {result:?}")
                }
            }
        }
    })
    .await
    .expect("post-restart OpenCode interaction must arrive")
    }).catch_unwind().await;
    let interaction = match interaction {
        Ok(interaction) => interaction,
        Err(failure) => {
            // Retain the original admission assertion before diagnostic reads.
            // These are after-failure snapshots, not an atomic history oracle.
            primary_failure = Some(failure);
            let state_capture = tokio::time::timeout(Duration::from_secs(10), async {
                match StateStore::connect(StateStoreConfig::Lance(
                    LanceStateStoreConfig::directory(fixture.directory()),
                )).await {
                    Ok(store) => PendingInteractionCrash::capture_abort_progress(
                        &process, &store, &session, &conversation.id(),
                        "post-restart-interaction-failure",
                    ).await,
                    Err(_) => eprintln!("post-restart question state capture=unavailable"),
                }
            }).await;
            if state_capture.is_err() {
                eprintln!("post-restart question state capture=deadline");
            }
            let failure = primary_failure.take().expect("original interaction failure remains owned");
            let context = native.capture_failure(&session, DeliveryScenario::opencode_question_prompt(),
                &mut primary_failure, failure).await;
            eprintln!("post-restart question admission failure evidence={context}");
            std::panic::resume_unwind(primary_failure.take().expect("original interaction failure remains owned"));
        }
    };
    let evidence = std::panic::AssertUnwindSafe(native.pending(&session, DeliveryScenario::opencode_question_prompt(),
        &StrictFixture::required("TEST_OLLAMA_MODEL_OPENCODE_SHELL"))).catch_unwind().await;
    let evidence = match evidence {
        Ok(evidence) => evidence,
        Err(failure) => {
            let context = native.capture_failure(&session, DeliveryScenario::opencode_question_prompt(),
                &mut primary_failure, failure).await;
            eprintln!("question native failure evidence={context}");
            std::panic::resume_unwind(primary_failure.take().expect("original question failure remains owned"));
        }
    };
    let answer = DeliveryScenario::resolve_opencode_question(conversation, &interaction).await;

    let response = tokio::time::timeout(Duration::from_secs(660), &mut prompt)
        .await
        .expect("post-restart OpenCode prompt must reach terminal delivery")
        .expect("post-restart OpenCode prompt must succeed");
    drop(prompt);

    let grade = native.completed(evidence, &session, &answer, response.output()).await;
    (grade, native.version().to_owned())
        },
    ).await;
    if let Some(failure) = primary_failure {
        let _ = writeln!(
            std::io::stderr().lock(),
            "question native failure capture=incomplete; original_failure_retained=true"
        );
        observed = Err(failure);
    }
    let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
    if let Some(conversation) = owned_conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(mut receiver) = owned_receiver {
        cleanup
            .settle::<agent_sdk::SdkError>(async {
                DeliveryScenario::drain(&mut receiver).await;
                Ok(())
            })
            .await;
    }
    if let Some(workspace) = owned_workspace {
        cleanup.settle(workspace.close()).await;
    }
    if let Some(agent) = owned_agent {
        cleanup.settle(agent.shutdown()).await;
    }
    cleanup
        .settle::<agent_sdk::SdkError>(async {
            process.shutdown().await;
            Ok(())
        })
        .await;
    let (grade, version) = cleanup.finish(observed);
    grade.emit(NATIVE_IDLE_REOPEN_TEST, &version);
}

#[tokio::test]
async fn opencode_reopen_cancels_recovered_pending_interaction_before_publication() {
    if let Some(handoff) = StrictFixture::crash_handoff() {
        PendingInteractionCrash::run_child(&handoff).await;
        return;
    }

    let fixture = StrictFixture::new().await;
    let process = StrictOpenCodeProcess::start("pending-interaction-reopen").await;
    let endpoint = process.endpoint();
    let mut owned_agent = None;
    let mut owned_workspace = None;
    let mut owned_conversation = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1_290);
    let mut primary_failure = None;
    let mut output_capture = None;
    let mut observed = NativeCleanup::observe(deadline, async {
        let handoff = match std::panic::AssertUnwindSafe(fixture.crash_child(
            PENDING_INTERACTION_REOPEN_TEST,
            endpoint.as_bytes(),
            Duration::from_secs(900),
        ))
        .catch_unwind()
        .await
        {
            Ok(handoff) => handoff,
            Err(failure) => {
                primary_failure = Some(failure);
                let native = NativeQuestionReadback::new(&endpoint, deadline).await;
                let sessions = native.sessions().await;
                assert_eq!(
                    sessions.len(),
                    1,
                    "failed child must have one attributable native session"
                );
                let session = sessions.first().expect("one native session remains");
                let failure = primary_failure
                    .take()
                    .expect("original child failure remains owned");
                let context = native
                    .capture_failure(
                        session,
                        DeliveryScenario::opencode_question_prompt(),
                        &mut primary_failure,
                        failure,
                    )
                    .await;
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "question native child failure evidence={context}"
                );
                std::panic::resume_unwind(
                    primary_failure
                        .take()
                        .expect("original child failure remains owned"),
                );
            }
        };
        let (agent_id, workspace_id, conversation_id) =
            serde_json::from_slice::<(AgentId, WorkspaceId, ConversationId)>(&handoff)
                .expect("pending-interaction handoff must decode");
        PendingInteractionCrash::wait_until_native_active(&endpoint).await;

        let agent = owned_agent.insert(
            fixture
                .reopen(
                    process.config(PENDING_INTERACTION_REOPEN_LABEL, Duration::from_secs(60)),
                    agent_id,
                )
                .await,
        );
        let model = DeliveryScenario::opencode_model(agent).await;
        let workspace = owned_workspace.insert(
            agent
                .get_workspace(workspace_id)
                .await
                .expect("pending-interaction workspace must reopen"),
        );
        let conversation = owned_conversation.insert(
            workspace
                .get_conversation(conversation_id)
                .await
                .expect("pending-interaction conversation must reopen"),
        );
        let fresh =
            FreshOutputExpectation::new(StrictFixture::unique("PENDING_INTERACTION_REOPENED"));
        let token = fresh.expected();
        let prompt = fresh.prompt();
        let response = conversation
            .prompt(
                PromptRequest::new(prompt)
                    .expect("following pending-interaction prompt must be valid")
                    .with_model(model)
                    .with_timeout(Duration::from_secs(180))
                    .expect("following pending-interaction timeout must be valid"),
            )
            .await
            .expect("reopen must cancel the recovered interaction before publication");

        // Preserve the original assertion and its panic before any cancellable
        // diagnostic work. The outer slot survives the original absolute timeout.
        if let Err(failure) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_eq!(response.output(), token);
        })) {
            primary_failure = Some(failure);
        }
        let capture_deadline = deadline.min(tokio::time::Instant::now() + Duration::from_secs(10));
        let diagnostic = NativeCleanup::observe(capture_deadline, async {
            let native = NativeQuestionReadback::new(&endpoint, capture_deadline).await;
            native
                .prompt_output(
                    prompt,
                    "ollama",
                    &StrictFixture::required("TEST_OLLAMA_MODEL_OPENCODE_SHELL"),
                    token,
                    response.output(),
                )
                .await
        })
        .await;
        let context = match diagnostic {
            Ok(Ok(Ok(context))) => context,
            Ok(Ok(Err(reason))) => serde_json::json!({"capture":"incomplete","reason":reason}),
            Ok(Err(_)) => serde_json::json!({"capture":"incomplete","reason":"deadline"}),
            Err(_) => serde_json::json!({"capture":"incomplete","reason":"readback_panic"}),
        };
        if primary_failure.is_none()
            && (context["capture"] != "complete" || context["native_equals_portable"] != true)
        {
            primary_failure = Some(Box::new(
                "recovered native output evidence is incomplete or differs from portable output",
            ));
        }
        output_capture = Some(context);
    })
    .await;
    let mut context = output_capture.unwrap_or_else(
        || serde_json::json!({"capture":"incomplete","reason":"followup_unavailable"}),
    );
    context["version"] = serde_json::json!(1);
    context["case"] = serde_json::json!(PENDING_INTERACTION_REOPEN_TEST);
    let mut context = context.to_string();
    if context.len() > 4 * 1024 {
        context = serde_json::json!({"version":1,"case":PENDING_INTERACTION_REOPEN_TEST,
            "capture":"incomplete","reason":"diagnostic_bound"})
        .to_string();
        if primary_failure.is_none() {
            primary_failure = Some(Box::new(
                "recovered native output diagnostic exceeded its bound",
            ));
        }
    }
    let _ = writeln!(
        std::io::stderr().lock(),
        "\nAGENT_SDK_RECOVERY_OUTPUT_DIAGNOSTIC={context}"
    );
    if let Some(failure) = primary_failure {
        observed = Err(failure);
    }
    let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
    if let Some(conversation) = owned_conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(workspace) = owned_workspace {
        cleanup.settle(workspace.close()).await;
    }
    if let Some(agent) = owned_agent {
        cleanup.settle(agent.shutdown()).await;
    }
    cleanup
        .settle::<agent_sdk::SdkError>(async {
            process.shutdown().await;
            // The existing final cleanup phase also observes the single fixture
            // drop before restoring an earlier assertion panic.
            drop(fixture);
            Ok(())
        })
        .await;
    let _ = writeln!(
        std::io::stderr().lock(),
        "\nAGENT_SDK_RECOVERY_CLEANUP={}",
        serde_json::json!({"version":1,"case":PENDING_INTERACTION_REOPEN_TEST,
            "complete":cleanup.diagnostics().is_empty(),
            "scenario_cleanup_succeeded":cleanup.diagnostics().is_empty()})
    );
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_repaired_abort_releases_generation_and_later_abort_is_idle() {
    let fixture = StrictFixture::new().await;
    let mut process = StrictOpenCodeProcess::start("repaired-abort").await;
    let config = process.config(
        &StrictFixture::unique("strict-opencode-repaired-abort"),
        Duration::from_secs(60),
    );
    let mut owned_agent = None;
    let mut owned_workspace = None;
    let mut owned_conversation = None;
    let mut owned_receiver = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1_200);
    let mut primary_failure = None;
    let mut observed = NativeCleanup::observe(deadline, async {
    let agent = owned_agent.insert(AgentBuilder::create(config, fixture.storage()).build().await
        .expect("repaired-abort Agent must build"));
    let workspace = owned_workspace.insert(agent.new_workspace(StrictFixture::workspace(
        &StrictFixture::unique("strict-opencode-repaired-abort-workspace"),
    )).await.expect("repaired-abort workspace must build"));
    let conversation = owned_conversation.insert(workspace.new_conversation(
        StrictFixture::conversation("repaired abort"),
    ).await.expect("repaired-abort conversation must build"));
    let model = DeliveryScenario::opencode_model(agent).await;
    let native = NativeQuestionReadback::new(&process.endpoint(), deadline).await;
    let session = native.created_session(&std::collections::BTreeSet::new()).await;
    let receiver = owned_receiver.insert(conversation
        .notification_stream()
        .expect("repaired-abort receiver must open"));
    let request = PromptRequest::new(DeliveryScenario::opencode_question_prompt())
        .expect("repaired-abort question prompt must be valid")
        .with_model(model.clone())
        .with_timeout(Duration::from_secs(600))
        .expect("repaired-abort question timeout must be valid");
    let mut prompt = Box::pin(conversation.prompt(request));
    let interaction = std::panic::AssertUnwindSafe(async {
    tokio::time::timeout(Duration::from_secs(600), async {
        loop {
            tokio::select! {
                notification = receiver.recv() => {
                    let notification = notification
                        .expect("repaired-abort delivery must remain open")
                        .expect("repaired-abort notification must succeed");
                    if matches!(
                        notification.kind(),
                        ConversationNotificationKind::Interaction { .. }
                    ) {
                        break notification;
                    }
                }
                result = &mut prompt => {
                    panic!("repaired-abort prompt completed before admission: {result:?}")
                }
            }
        }
    })
    .await
    .expect("repaired-abort interaction admission must arrive")
    }).catch_unwind().await;
    let interaction = match interaction {
        Ok(interaction) => interaction,
        Err(failure) => {
            eprintln!("repaired-abort native failure capture=started");
            let context = native.capture_failure(&session, DeliveryScenario::opencode_question_prompt(),
                &mut primary_failure, failure).await;
            eprintln!("repaired-abort native failure evidence={context}");
            std::panic::resume_unwind(primary_failure.take().expect("original repaired-abort failure remains owned"));
        }
    };
    drop(prompt);
    let _answer = DeliveryScenario::resolve_opencode_question(conversation, &interaction).await;
    let endpoint = process.endpoint();
    PendingInteractionCrash::wait_until_native_active(&endpoint).await;
    process.stop().await;
    let mut owner = Box::pin(conversation.abort());
    let mut joiner = Box::pin(conversation.abort());
    tokio::select! {
        result = &mut owner => panic!("abort owner completed while OpenCode was stopped: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
    }
    tokio::select! {
        result = &mut joiner => panic!("abort joiner completed while OpenCode was stopped: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
    }
    let reconciliation_deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    let ((owner_completed_first, first_result), (native_events, session_id)) =
        tokio::time::timeout_at(reconciliation_deadline, async {
            tokio::join!(
                async {
                    tokio::select! {
                        result = &mut owner => (true, result),
                        result = &mut joiner => (false, result),
                    }
                },
                async {
                    process.restart().await;
                    let native_events = process.native_events().await;
                    let session_id = process.abort_only_session().await;
                    (native_events, session_id)
                }
            )
        })
        .await
        .expect("abort recovery and native repair must remain bounded");
    let first_error =
        first_result.expect_err("quiescent admitted work must report repaired invalid state");
    assert!(
        matches!(
            (first_error).kind(),
            agent_sdk::error::ErrorKind::InvalidState
        ),
        "first abort observer must observe repaired invalid state: {first_error:?}"
    );
    tokio::time::timeout_at(reconciliation_deadline, conversation.abort())
        .await
        .expect("later abort observation must remain within the lifecycle deadline")
        .expect("later abort must observe an idle repaired generation");
    let remaining_result = if owner_completed_first {
        tokio::time::timeout_at(reconciliation_deadline, &mut joiner).await
    } else {
        tokio::time::timeout_at(reconciliation_deadline, &mut owner).await
    };
    let remaining_error = remaining_result
        .expect("existing abort observer must remain within the lifecycle deadline")
        .expect_err("existing abort observer must preserve repaired terminal truth");
    assert!(
        matches!(
            (remaining_error).kind(),
            agent_sdk::error::ErrorKind::InvalidState
        ),
        "existing abort observer must share repaired invalid state: {remaining_error:?}"
    );
    let mut native_events = native_events.bytes_stream();
    let mut native_event_body = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(1), native_events.next()).await {
            Ok(Some(Ok(bytes))) => native_event_body.extend_from_slice(&bytes),
            Ok(Some(Err(error))) => panic!("native event observation failed: {error}"),
            Ok(None) | Err(_) => break,
        }
    }
    let native_event_body =
        String::from_utf8(native_event_body).expect("native events must be UTF-8 SSE data");
    assert_eq!(
        native_event_body
            .matches("\"type\":\"session.idle\"")
            .count(),
        1,
        "one native abort must emit exactly one terminal idle event"
    );
    process.assert_single_native_abort(&session_id);
    drop(owner);
    drop(joiner);
    let mut following = Box::pin(
        conversation.prompt(
            PromptRequest::new(format!(
                "Do not call any tool. Reply with exactly {} and nothing else.",
                StrictFixture::unique("REPAIRED_ABORT_REUSED")
            ))
            .expect("following repaired-abort prompt must be valid")
            .with_model(model)
            .with_timeout(Duration::from_secs(600))
            .expect("following repaired-abort timeout must be valid"),
        ),
    );
    tokio::select! {
        result = &mut following => {
            panic!("following repaired-abort prompt completed before native admission: {result:?}")
        }
        _ = PendingInteractionCrash::wait_until_native_active(&endpoint) => {}
    }
    conversation
        .abort()
        .await
        .expect("following repaired-abort generation must accept cancellation");
    let following_error = tokio::time::timeout(Duration::from_secs(180), &mut following)
        .await
        .expect("following repaired-abort prompt must terminalize after cancellation")
        .expect_err("following repaired-abort prompt cannot succeed after cancellation");
    assert!(
        matches!(
            (following_error).kind(),
            agent_sdk::error::ErrorKind::Cancelled
        ),
        "following repaired-abort prompt must preserve cancellation: {following_error:?}"
    );
    PendingInteractionCrash::wait_until_native_idle(&endpoint, &session_id).await;
    drop(following);
    DeliveryScenario::assert_no_interaction(receiver).await;

        },
    ).await;
    if let Some(failure) = primary_failure {
        eprintln!(
            "repaired-abort native failure capture=incomplete; original_failure_retained=true"
        );
        observed = Err(failure);
    }
    let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
    if let Some(conversation) = owned_conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(mut receiver) = owned_receiver {
        cleanup
            .settle::<agent_sdk::SdkError>(async {
                DeliveryScenario::drain(&mut receiver).await;
                Ok(())
            })
            .await;
    }
    if let Some(workspace) = owned_workspace {
        cleanup.settle(workspace.close()).await;
    }
    if let Some(agent) = owned_agent {
        cleanup.settle(agent.shutdown()).await;
    }
    cleanup
        .settle::<agent_sdk::SdkError>(async {
            process.shutdown().await;
            Ok(())
        })
        .await;
    cleanup.finish(observed);
}
