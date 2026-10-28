use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;

use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
use agents_kit::conversation::NewConversation;
use agents_kit::identity::{AgentId, ConversationId, NotificationId, WorkspaceId};
use agents_kit::model::ModelSelector;
use agents_kit::notification::{
    ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver,
    NotificationResolution,
};
use agents_kit::prompt::{PromptEvent, PromptRequest};
use agents_kit::tool::ToolSelection;
use agents_kit::workspace::{
    NewWorkspace, WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
    WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
};
use agents_kit::{Agent, AgentBuilder, Conversation, ConversationBackend, Workspace};
use agents_kit::{AgentBackend, WorkspaceBackend};
use providers_kit::config::{
    AnthropicConfig, ApiKey, GeminiConfig, GeminiServiceTier, OpenAiConfig, ProviderKind,
    RetryPolicy, TransportConfig,
};
use providers_kit::{Provider, ProviderBackend, ProviderConfig};
use storage_kit::{Database, StateStore};
use tools_kit::Toolset;
use tools_kit::declaration::{ToolInfo, ToolName};
use tools_kit::execution::{ToolInvocation, ToolResult};
use tools_kit::interaction::ToolInteractionCapabilities;
use tools_kit::local::ToolRegistration;
use tools_kit::schema::JsonSchema;

use super::fixture::StrictFixture;
use super::prompt_and_io::PromptScenario;
use crate::caller_shell::{CallerShell, CallerShellObserver};
use crate::custom_provider_evidence::{AnthropicEvidence, CloudPhase};
use crate::custom_provider_observation::{
    ConfiguredProvider, GenerationDiagnostics, ObservedProvider, ScenarioStage,
};
use crate::native_cleanup::NativeCleanup;

const PROMPT_TIMEOUT: Duration = Duration::from_secs(240);
const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(180);
const RUNTIME_LOSS_TIMEOUT: Duration = Duration::from_secs(360);
const RUNTIME_LOSS_TEST: &str =
    "strict_live_suite::custom::custom_provider_model_tool_loop_and_runtime_ownership_are_live";

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct RuntimeLossFacts {
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    notification_id: NotificationId,
    root: PathBuf,
}

enum ScenarioBuildMode {
    Create,
    Reopen(AgentId),
}

struct ParallelCallGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for ParallelCallGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct StrictCustomScenario {
    root: PathBuf,
    fixture: StrictFixture,
    continuation_calls: Arc<AtomicUsize>,
    parallel_calls: Arc<AtomicUsize>,
    parallel_active: Arc<AtomicUsize>,
    parallel_peak: Arc<AtomicUsize>,
    interaction_calls: Arc<AtomicUsize>,
    anthropic_evidence: Option<Arc<Mutex<AnthropicEvidence>>>,
    diagnostics: Arc<GenerationDiagnostics>,
    shell_observer: Arc<CallerShellObserver>,
}

impl StrictCustomScenario {
    async fn new(label: &str) -> Self {
        let fixture = StrictFixture::new().await;
        let root = std::env::temp_dir().join(format!(
            "agents-kit-custom-reduced-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&root).expect("strict-live Custom temporary root must be created");
        let root = std::fs::canonicalize(root)
            .expect("strict-live Custom temporary root must canonicalize");
        let shell_observer = Arc::new(CallerShellObserver::default());
        Self {
            root,
            fixture,
            continuation_calls: Arc::new(AtomicUsize::new(0)),
            parallel_calls: Arc::new(AtomicUsize::new(0)),
            parallel_active: Arc::new(AtomicUsize::new(0)),
            parallel_peak: Arc::new(AtomicUsize::new(0)),
            interaction_calls: Arc::new(AtomicUsize::new(0)),
            anthropic_evidence: None,
            diagnostics: Arc::new(GenerationDiagnostics::new(Arc::clone(&shell_observer))),
            shell_observer,
        }
    }

    async fn run() {
        if let Some(path) = StrictFixture::crash_handoff() {
            Self::runtime_loss_child(path).await;
            return;
        }

        let scenario = Self::new("parent").await;
        let observed = std::panic::AssertUnwindSafe(async {
            scenario.prove_provider_model_and_tool_loop().await;
            scenario.prove_durable_approval().await;
            scenario.prove_runtime_loss_and_reopen().await;
        })
        .catch_unwind()
        .await;
        let mut cleanup = NativeCleanup::new(LIFECYCLE_TIMEOUT);
        cleanup
            .settle(async { std::fs::remove_dir_all(&scenario.root) })
            .await;
        cleanup.finish(observed.map(Ok));
    }

    fn provider(&self) -> ObservedProvider {
        let api_key = ApiKey::new(StrictFixture::required("TEST_GEMINI_API_KEY"))
            .expect("strict-live Gemini API key must be valid");
        let base_url = reqwest::Url::parse(&StrictFixture::required("TEST_GEMINI_BASE_URL"))
            .expect("strict-live Gemini base URL must parse");
        let service_tier = match StrictFixture::required("TEST_GEMINI_SERVICE_TIER").as_str() {
            "standard" => GeminiServiceTier::Standard,
            "flex" => GeminiServiceTier::Flex,
            "priority" => GeminiServiceTier::Priority,
            other => {
                panic!("TEST_GEMINI_SERVICE_TIER must be standard, flex, or priority; got {other}")
            }
        };
        let request_timeout = Duration::from_secs(180);
        let connect_timeout = Duration::from_secs(15);
        let transport = TransportConfig::new(
            request_timeout,
            connect_timeout,
            Duration::from_secs(60),
            RetryPolicy::default(),
        )
        .expect("strict-live transport policy must be valid");
        let kind = ProviderKind::Gemini(
            GeminiConfig::new(api_key)
                .with_base_url(base_url)
                .with_service_tier(service_tier),
        );
        let configured = ConfiguredProvider::new(&kind, request_timeout, connect_timeout);
        let provider = Provider::from_config(ProviderConfig::new(kind, transport))
            .expect("strict-live Gemini Provider must build");
        ObservedProvider::new(provider, Arc::clone(&self.diagnostics), configured)
    }

    fn config(&self, display_name: &str, approval: CustomToolApproval) -> AgentConfig {
        self.config_for_model(
            display_name,
            approval,
            ModelSelector::new(StrictFixture::required("TEST_GEMINI_MODEL_DEFAULT"))
                .expect("strict-live Gemini model selector must be valid"),
        )
    }

    fn config_for_model(
        &self,
        display_name: &str,
        approval: CustomToolApproval,
        model: ModelSelector,
    ) -> AgentConfig {
        let custom = CustomAgentConfig::new(
            model,
            ToolSelection::Auto,
            CustomLoopLimits::new(6, 12, 2).expect("strict-live Custom loop limits must be valid"),
            approval,
        )
        .expect("strict-live Custom policy must be valid");
        AgentConfig::new(
            custom.into(),
            LIFECYCLE_TIMEOUT,
            NonZeroUsize::new(2).expect("two prompts are nonzero"),
        )
        .expect("strict-live Custom Agent config must be valid")
        .with_display_name(display_name)
        .expect("strict-live Custom display name must be valid")
    }

    fn toolset(&self) -> Toolset {
        let continuation_name =
            ToolName::new("strict_continue").expect("strict continuation name must be valid");
        let mut continuation_info = ToolInfo::new(
            continuation_name,
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "properties": {
                    "value": {"type": "string", "minLength": 1}
                },
                "required": ["value"],
                "additionalProperties": false
            })),
        );
        continuation_info.description =
            Some("Returns a marker that must be continued through the selected model.".to_owned());
        let continuation_calls = Arc::clone(&self.continuation_calls);
        let continuation =
            ToolRegistration::function(continuation_info, move |invocation: ToolInvocation| {
                let continuation_calls = Arc::clone(&continuation_calls);
                async move {
                    continuation_calls.fetch_add(1, Ordering::SeqCst);
                    let value = invocation
                        .arguments
                        .get("value")
                        .and_then(serde_json::Value::as_str)
                        .expect("validated strict continuation arguments must contain value");
                    Ok(ToolResult::text(format!(
                        "STRICT_CONTINUATION_RESULT_{value}"
                    )))
                }
            });

        let parallel_name =
            ToolName::new("strict_parallel_probe").expect("strict parallel name must be valid");
        let mut parallel_info = ToolInfo::new(
            parallel_name,
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "properties": {
                    "slot": {"type": "string", "enum": ["alpha", "beta", "gamma"]}
                },
                "required": ["slot"],
                "additionalProperties": false
            })),
        );
        parallel_info.description = Some(
            "Sleeps briefly while recording the Custom loop's live tool concurrency.".to_owned(),
        );
        let parallel_calls = Arc::clone(&self.parallel_calls);
        let parallel_active = Arc::clone(&self.parallel_active);
        let parallel_peak = Arc::clone(&self.parallel_peak);
        let parallel =
            ToolRegistration::function(parallel_info, move |invocation: ToolInvocation| {
                let parallel_calls = Arc::clone(&parallel_calls);
                let parallel_active = Arc::clone(&parallel_active);
                let parallel_peak = Arc::clone(&parallel_peak);
                async move {
                    parallel_calls.fetch_add(1, Ordering::SeqCst);
                    let active = parallel_active.fetch_add(1, Ordering::SeqCst) + 1;
                    parallel_peak.fetch_max(active, Ordering::SeqCst);
                    let _guard = ParallelCallGuard {
                        active: Arc::clone(&parallel_active),
                    };
                    let slot = invocation
                        .arguments
                        .get("slot")
                        .and_then(serde_json::Value::as_str)
                        .expect("validated strict parallel arguments must contain slot")
                        .to_owned();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(ToolResult::text(format!("STRICT_PARALLEL_RESULT_{slot}")))
                }
            });

        let interaction_name =
            ToolName::new("strict_interaction").expect("strict interaction name must be valid");
        let mut interaction_info = ToolInfo::new(
            interaction_name,
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "properties": {
                    "request": {"type": "string"}
                },
                "required": ["request"],
                "additionalProperties": false
            })),
        );
        interaction_info.description =
            Some("Requests one durable caller-supplied text value.".to_owned());
        interaction_info.interactions = ToolInteractionCapabilities {
            text: true,
            multiline_text: false,
            form: false,
            form_schema_validation: false,
        };
        let interaction_calls = Arc::clone(&self.interaction_calls);
        let interaction =
            ToolRegistration::interactive(interaction_info, move |_invocation, context| {
                let interaction_calls = Arc::clone(&interaction_calls);
                async move {
                    interaction_calls.fetch_add(1, Ordering::SeqCst);
                    let answer = context
                        .request_text("Provide the strict Custom continuation", false)
                        .await?;
                    Ok(ToolResult::text(format!(
                        "STRICT_INTERACTION_RESULT_{answer}"
                    )))
                }
            });

        Toolset::local(vec![continuation, parallel, interaction])
            .expect("strict-live Local Toolset must build")
    }

    async fn build_agent(
        &self,
        display_name: &str,
        approval: CustomToolApproval,
        shell: bool,
        mode: ScenarioBuildMode,
    ) -> Agent<StateStore, Database, ObservedProvider> {
        let config = self.config(display_name, approval);
        self.build_with_provider(config, self.provider(), shell, mode)
            .await
    }

    async fn build_with_provider<P: ProviderBackend>(
        &self,
        config: AgentConfig,
        provider: P,
        shell: bool,
        mode: ScenarioBuildMode,
    ) -> Agent<StateStore, Database, P> {
        let mut builder = match mode {
            ScenarioBuildMode::Create => AgentBuilder::create(config, self.fixture.storage()),
            ScenarioBuildMode::Reopen(id) => {
                AgentBuilder::reopen(config, self.fixture.storage(), id)
            }
        }
        .with_provider(provider)
        .with_toolset(self.toolset());
        if shell {
            builder = builder.with_toolset(CallerShell::toolset(
                self.root.clone(),
                Duration::from_secs(30),
                16 * 1024,
                Some(Arc::clone(&self.shell_observer)),
            ));
        }
        tokio::time::timeout(LIFECYCLE_TIMEOUT, builder.build())
            .await
            .expect("strict-live Custom Agent build must remain bounded")
            .expect("strict-live Custom Agent must build")
    }

    async fn workspace<P: ProviderBackend>(
        &self,
        agent: &Agent<StateStore, Database, P>,
        label: &str,
    ) -> Workspace<StateStore, Database, P> {
        let context = WorkspaceContext::new(
            self.root.clone(),
            vec![
                WorkspaceRoot::new(self.root.clone(), WorkspaceRootAccess::ReadWrite)
                    .expect("strict-live Custom root must be valid"),
            ],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .expect("strict-live Custom workspace context must be valid");
        let request = NewWorkspace::new(label, context, WorkspaceDefaults::empty())
            .expect("strict-live Custom workspace request must be valid");
        agent
            .new_workspace(request)
            .await
            .expect("strict-live Custom workspace must be created")
    }

    async fn conversation<P: ProviderBackend>(
        &self,
        workspace: &Workspace<StateStore, Database, P>,
        label: &str,
    ) -> Conversation<StateStore, Database, P> {
        workspace
            .new_conversation(
                NewConversation::new(Some(label.to_owned()), None, Vec::new())
                    .expect("strict-live Custom conversation request must be valid"),
            )
            .await
            .expect("strict-live Custom conversation must be created")
    }

    async fn prompt(
        &self,
        conversation: &Conversation<StateStore, Database, ObservedProvider>,
        text: String,
        selection: ToolSelection,
        expected: &str,
    ) {
        let request = PromptRequest::new(text)
            .expect("strict-live Custom prompt must be valid")
            .with_tool_selection(selection)
            .expect("strict-live Custom tool selection must be valid");
        let response = tokio::time::timeout(PROMPT_TIMEOUT, conversation.prompt(request))
            .await
            .expect("strict-live Custom prompt must remain bounded")
            .expect("strict-live Custom prompt must succeed");
        assert!(
            !response.output().is_empty(),
            "real Custom model generation must return terminal text"
        );
        assert_eq!(response.output(), expected);
    }

    async fn receive_interaction(
        &self,
        receiver: &mut ConversationNotificationReceiver,
    ) -> ConversationNotification {
        tokio::time::timeout(PROMPT_TIMEOUT, async {
            loop {
                let notification = receiver
                    .recv()
                    .await
                    .expect("strict-live Custom notification stream must remain open")
                    .expect("strict-live Custom notification delivery must succeed");
                if matches!(
                    notification.kind(),
                    ConversationNotificationKind::Interaction { .. }
                ) {
                    return notification;
                }
            }
        })
        .await
        .expect("strict-live Custom interaction must arrive")
    }

    async fn settle_hierarchy<P: ProviderBackend>(
        agent: Option<Agent<StateStore, Database, P>>,
        workspace: Option<Workspace<StateStore, Database, P>>,
        conversation: Option<Conversation<StateStore, Database, P>>,
    ) -> NativeCleanup {
        let mut cleanup = NativeCleanup::new(LIFECYCLE_TIMEOUT);
        if let Some(conversation) = conversation {
            cleanup.join(&mut tokio::spawn(conversation.close())).await;
        }
        if let Some(workspace) = workspace {
            cleanup.join(&mut tokio::spawn(workspace.close())).await;
        }
        if let Some(agent) = agent {
            cleanup.join(&mut tokio::spawn(agent.shutdown())).await;
        }
        cleanup
    }

    async fn prove_provider_model_and_tool_loop(&self) {
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = std::panic::AssertUnwindSafe(async {
            let agent = owned_agent.insert(self
                .build_agent(
                    "strict-custom-reduced-main",
                    CustomToolApproval::Allow,
                    true,
                    ScenarioBuildMode::Create,
                )
                .await);
            let workspace = owned_workspace.insert(self.workspace(agent, "strict-custom-reduced-main").await);
        let names = workspace
            .list_tools()
            .await
            .expect("strict-live Custom catalog must load")
            .into_iter()
            .map(|tool| tool.name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "shell",
                "strict_continue",
                "strict_interaction",
                "strict_parallel_probe",
            ],
            "Custom shell must be an ordinary sorted Local tool registration"
        );

        let generation = owned_conversation.insert(self.conversation(workspace, "provider-model").await);
        let generation_token = format!("CUSTOM_MODEL_GENERATION_{}", uuid::Uuid::new_v4().simple());
        self.diagnostics.enter(ScenarioStage::PlainGeneration);
        self.prompt(
            generation,
            format!("Reply with exactly {generation_token} and no other text."),
            ToolSelection::None,
            &generation_token,
        )
        .await;

        let continuation_before = self.continuation_calls.load(Ordering::SeqCst);
        self.diagnostics.enter(ScenarioStage::FunctionContinuation);
        let continuation_token = format!(
            "CUSTOM_FUNCTION_CONTINUATION_{}",
            uuid::Uuid::new_v4().simple()
        );
        self.prompt(
            generation,
            format!(
                "Call strict_continue exactly once with value `confirmed`. After its real result, reply with exactly {continuation_token} and no other text."
            ),
            ToolSelection::Allowed(vec!["strict_continue".to_owned()]),
            &continuation_token,
        )
        .await;
        assert_eq!(
            self.continuation_calls.load(Ordering::SeqCst),
            continuation_before + 1,
            "one normalized function call must execute through Toolset -> ToolHandler"
        );
        Self::settle_hierarchy(None, None, owned_conversation.take())
            .await
            .finish(Ok(Ok(())));

        self.prove_parallel_bound(workspace).await;
        self.prove_caller_cancellation_and_reuse(workspace).await;
        self.prove_shell_as_local_tool(workspace).await;

        }).catch_unwind().await;
        Self::settle_hierarchy(owned_agent, owned_workspace, owned_conversation)
            .await
            .finish(observed.map(Ok));
    }

    async fn prove_stream_and_reopen(&self, deadline: tokio::time::Instant) {
        self.diagnostics.enter(ScenarioStage::Streaming);
        let label = "strict-custom-incremental";
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = NativeCleanup::observe(deadline, async {
            let agent = owned_agent.insert(self
                .build_agent(label, CustomToolApproval::Allow, false, ScenarioBuildMode::Create)
                .await);
            let workspace = owned_workspace.insert(self.workspace(agent, label).await);
            let conversation = owned_conversation.insert(self
                .conversation(workspace, "incremental and reopen")
                .await);
            let identities = (agent.id(), workspace.id(), conversation.id());
        let request =
            PromptRequest::new("Explain how a Rust iterator works in sixteen numbered sentences.")
                .expect("Custom streaming prompt")
                .with_tool_selection(ToolSelection::None)
                .expect("generation-only tool policy")
                .with_timeout(PROMPT_TIMEOUT)
                .expect("stream deadline");
        NativeCleanup::observe_stream(
            conversation
                .prompt_stream(request)
                .await
                .expect("real Custom provider must stream"),
        )
        .await;
        PromptScenario::abort_stream(conversation,
            PromptRequest::new("Write five thousand words explaining the history of computing, starting immediately with prose and continuing until all five thousand words are written.")
                .expect("Custom stream abort prompt")
                .with_tool_selection(ToolSelection::None).expect("generation-only tool policy")
                .with_timeout(PROMPT_TIMEOUT).expect("abort deadline"),
        ).await;
            identities
        }).await;
        let cleanup = if matches!(&observed, Ok(Ok(_))) {
            // Only a successful first phase preserves quiescent durable children
            // for reopen. Failure instead explicitly settles every acquired owner.
            drop(owned_conversation.take());
            drop(owned_workspace.take());
            Self::settle_hierarchy(owned_agent.take(), None, None).await
        } else {
            Self::settle_hierarchy(
                owned_agent.take(),
                owned_workspace.take(),
                owned_conversation.take(),
            )
            .await
        };
        let (agent_id, workspace_id, conversation_id) = cleanup.finish(observed);
        let observed = NativeCleanup::observe(deadline, async {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Custom reopen must start within the original streaming deadline"
            );
            let agent = owned_agent.insert(
                self.build_agent(
                    label,
                    CustomToolApproval::Allow,
                    false,
                    ScenarioBuildMode::Reopen(agent_id),
                )
                .await,
            );
            let workspace = owned_workspace.insert(
                agent
                    .get_workspace(workspace_id)
                    .await
                    .expect("Custom stream workspace reopen"),
            );
            let conversation = owned_conversation.insert(
                workspace
                    .get_conversation(conversation_id)
                    .await
                    .expect("Custom stream conversation reopen"),
            );
            NativeCleanup::observe_stream(
                conversation
                    .prompt_stream(
                        PromptRequest::new(
                            "Explain what an owned Rust value is in eight numbered sentences.",
                        )
                        .expect("reopened Custom stream request")
                        .with_tool_selection(ToolSelection::None)
                        .expect("generation-only tool policy")
                        .with_timeout(PROMPT_TIMEOUT)
                        .expect("reopened stream deadline"),
                    )
                    .await
                    .expect("fresh Custom provider must stream after reopen"),
            )
            .await;
            assert_eq!(self.continuation_calls.load(Ordering::SeqCst), 0);
            assert_eq!(self.interaction_calls.load(Ordering::SeqCst), 0);
        })
        .await;
        Self::settle_hierarchy(owned_agent, owned_workspace, owned_conversation)
            .await
            .finish(observed);
    }

    async fn prove_cloud_continuation<P: ProviderBackend>(
        &self,
        provider: ProviderConfig,
        model: ModelSelector,
        deadline: tokio::time::Instant,
        make_provider: impl Fn(Provider) -> P,
    ) {
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let token = StrictFixture::unique("CLOUD_CONTINUATION");
        let expected = format!("STRICT_CONTINUATION_RESULT_{token}");
        let config = self.config_for_model("strict-custom-cloud", CustomToolApproval::Allow, model);
        let observed = NativeCleanup::observe(deadline, async {
            let agent = owned_agent.insert(self.build_with_provider(
                config.clone(), make_provider(Provider::from_config(provider.clone()).unwrap()), false, ScenarioBuildMode::Create,
            ).await);
            let workspace = owned_workspace.insert(self.workspace(agent, "strict-custom-cloud").await);
            let conversation = owned_conversation.insert(self.conversation(workspace, "real cloud continuation").await);
            let identities = (agent.id(), workspace.id(), conversation.id());
            self.exact_stream(conversation, PromptRequest::new(format!(
                "Call strict_continue exactly once with value `{token}`. After it returns, end with a final assistant answer consisting only of its complete returned text."
            )).unwrap().with_tool_selection(ToolSelection::Allowed(vec!["strict_continue".to_owned()])).unwrap(), &expected).await;
            assert_eq!(self.continuation_calls.load(Ordering::SeqCst), 1);
            assert_eq!(self.parallel_calls.load(Ordering::SeqCst), 0);
            assert_eq!(self.interaction_calls.load(Ordering::SeqCst), 0);
            identities
        }).await;
        let cleanup = if matches!(&observed, Ok(Ok(_))) {
            drop(owned_conversation.take());
            drop(owned_workspace.take());
            Self::settle_hierarchy(owned_agent.take(), None, None).await
        } else {
            Self::settle_hierarchy(
                owned_agent.take(),
                owned_workspace.take(),
                owned_conversation.take(),
            )
            .await
        };
        if let Some(evidence) = &self.anthropic_evidence {
            evidence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .phase_cleanup(CloudPhase::Initial, cleanup.diagnostics().is_empty());
        }
        let (agent_id, workspace_id, conversation_id) = cleanup.finish(observed);
        let observed = NativeCleanup::observe(deadline, async {
            let agent = owned_agent.insert(self.build_with_provider(
                config, make_provider(Provider::from_config(provider).unwrap()), false, ScenarioBuildMode::Reopen(agent_id),
            ).await);
            let workspace = owned_workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
            let conversation = owned_conversation.insert(workspace.get_conversation(conversation_id).await.unwrap());
            self.exact_stream(conversation, PromptRequest::new(
                "Copy your previous final assistant answer exactly, with no other text. Do not call tools."
            ).unwrap().with_tool_selection(ToolSelection::None).unwrap(), &expected).await;
            assert_eq!(self.continuation_calls.load(Ordering::SeqCst), 1, "reopen must not replay accepted tools");
        }).await;
        let cleanup =
            Self::settle_hierarchy(owned_agent, owned_workspace, owned_conversation).await;
        if let Some(evidence) = &self.anthropic_evidence {
            evidence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .phase_cleanup(CloudPhase::Reopened, cleanup.diagnostics().is_empty());
        }
        cleanup.finish(observed);
    }

    async fn exact_stream<P: ProviderBackend>(
        &self,
        conversation: &Conversation<StateStore, Database, P>,
        request: PromptRequest,
        expected: &str,
    ) {
        let deadline = tokio::time::Instant::now() + PROMPT_TIMEOUT;
        let mut stream = conversation
            .prompt_stream(request.with_timeout(PROMPT_TIMEOUT).unwrap())
            .await
            .unwrap();
        let mut text = String::new();
        loop {
            match tokio::time::timeout_at(deadline, stream.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
            {
                PromptEvent::OutputDelta { text: delta } => {
                    assert!(!delta.is_empty());
                    assert!(
                        text.len().saturating_add(delta.len()) <= 64 * 1024,
                        "bounded live output"
                    );
                    text.push_str(&delta);
                }
                PromptEvent::Completed(response) => {
                    assert!(!text.is_empty(), "real provider text deltas are required");
                    assert_eq!(response.output(), expected);
                    assert_eq!(
                        text,
                        response.output(),
                        "stream output must equal the exact durable answer"
                    );
                    assert!(stream.recv().await.is_none());
                    break;
                }
            }
        }
    }

    async fn run_cloud_continuation<P: ProviderBackend>(
        provider: ProviderConfig,
        role: &str,
        make_provider: impl Fn(Provider) -> P,
        evidence: Option<Arc<Mutex<AnthropicEvidence>>>,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1_200);
        let mut scenario = tokio::time::timeout_at(deadline, Self::new(role))
            .await
            .unwrap();
        scenario.anthropic_evidence = evidence;
        let model = ModelSelector::new(StrictFixture::required(role)).unwrap();
        let observed = std::panic::AssertUnwindSafe(scenario.prove_cloud_continuation(
            provider,
            model,
            deadline,
            make_provider,
        ))
        .catch_unwind()
        .await;
        let mut cleanup = NativeCleanup::new(LIFECYCLE_TIMEOUT);
        cleanup
            .settle(async { std::fs::remove_dir_all(&scenario.root) })
            .await;
        if let Some(evidence) = &scenario.anthropic_evidence {
            let cleaned = cleanup.diagnostics().is_empty();
            if let Some(path) = std::env::var_os("AGENTS_KIT_ANTHROPIC_EVIDENCE_PATH") {
                cleanup
                    .settle(async {
                        evidence
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .export_after_cleanup(&PathBuf::from(path), observed.is_ok(), cleaned)
                    })
                    .await;
            }
        }
        cleanup.finish(observed.map(Ok));
    }

    async fn prove_parallel_bound(
        &self,
        workspace: &Workspace<StateStore, Database, ObservedProvider>,
    ) {
        self.diagnostics.enter(ScenarioStage::ParallelTools);
        let mut owned_conversation = None;
        let observed = std::panic::AssertUnwindSafe(async {
        let conversation = owned_conversation.insert(self.conversation(workspace, "parallel-bound").await);
        let calls_before = self.parallel_calls.load(Ordering::SeqCst);
        self.parallel_peak.store(0, Ordering::SeqCst);
        let terminal = format!("CUSTOM_PARALLEL_{}", uuid::Uuid::new_v4().simple());
        self.prompt(
            conversation,
            format!(
                "Before replying, issue exactly three strict_parallel_probe calls in the same assistant turn: one each for slot alpha, beta, and gamma. Do not wait between calls. After all three real results, reply with exactly {terminal} and no other text."
            ),
            ToolSelection::Allowed(vec!["strict_parallel_probe".to_owned()]),
            &terminal,
        )
        .await;
        assert_eq!(
            self.parallel_calls.load(Ordering::SeqCst),
            calls_before + 3,
            "the real model must request all three parallel tool actions exactly once"
        );
        assert_eq!(
            self.parallel_active.load(Ordering::SeqCst),
            0,
            "all parallel ToolHandlers must settle before terminal publication"
        );
        assert_eq!(
            self.parallel_peak.load(Ordering::SeqCst),
            2,
            "CustomLoopLimits must admit two actions concurrently and never the third"
        );
        }).catch_unwind().await;
        Self::settle_hierarchy(None, None, owned_conversation)
            .await
            .finish(observed.map(Ok));
    }

    async fn prove_caller_cancellation_and_reuse(
        &self,
        workspace: &Workspace<StateStore, Database, ObservedProvider>,
    ) {
        self.diagnostics.enter(ScenarioStage::CallerCancellation);
        let mut owned_conversation = None;
        let observed = std::panic::AssertUnwindSafe(async {
        let conversation = owned_conversation.insert(self.conversation(workspace, "caller-cancellation").await);
        let mut receiver = conversation
            .notification_stream()
            .expect("strict-live Custom receiver must open");
        let calls_before = self.interaction_calls.load(Ordering::SeqCst);
        let notification = {
            let mut prompt = Box::pin(
                conversation.prompt(
                    PromptRequest::new(
                        r#"Call strict_interaction exactly once with arguments {"request":"wait for the strict caller continuation"}. Wait for its real caller-supplied result before replying."#,
                    )
                    .expect("strict-live caller-loss prompt must be valid")
                    .with_tool_selection(ToolSelection::Allowed(vec![
                        "strict_interaction".to_owned(),
                    ]))
                    .expect("strict-live caller-loss selection must be valid"),
                ),
            );
            tokio::select! {
                notification = self.receive_interaction(&mut receiver) => notification,
                result = &mut prompt => {
                    panic!("Custom prompt terminalized before caller cancellation: {result:?}")
                }
            }
        };
        assert_eq!(
            self.interaction_calls.load(Ordering::SeqCst),
            calls_before + 1,
            "caller-future cancellation must not erase accepted ToolHandler work"
        );
        conversation
            .resolve_notification(
                notification.id().clone(),
                NotificationResolution::Submit {
                    content: serde_json::Value::String("CALLER_RETURNED".to_owned()),
                },
            )
            .await
            .expect("runtime-owned interaction must resolve after caller cancellation");

        let reuse_token = format!(
            "CUSTOM_AFTER_CALLER_CANCELLATION_{}",
            uuid::Uuid::new_v4().simple()
        );
        tokio::time::timeout(PROMPT_TIMEOUT, async {
            loop {
                let request = PromptRequest::new(format!(
                    "Reply with exactly {reuse_token} and no other text."
                ))
                .expect("strict-live reuse prompt must be valid")
                .with_tool_selection(ToolSelection::None)
                .expect("strict-live reuse selection must be valid");
                match conversation.prompt(request).await {
                    Err(error) if error.kind() == agents_kit::error::ErrorKind::Busy => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Ok(response) => {
                        assert!(!response.output().is_empty());
                        assert_eq!(response.output(), &reuse_token);
                        break;
                    }
                    Err(error) => panic!(
                        "Custom conversation failed to recover after caller cancellation: {error}"
                    ),
                }
            }
        })
        .await
        .expect("caller-cancelled Custom work must settle for clean reuse");
        assert_eq!(
            self.interaction_calls.load(Ordering::SeqCst),
            calls_before + 1,
            "clean reuse must not replay the accepted interactive action"
        );
        drop(receiver);
        }).catch_unwind().await;
        Self::settle_hierarchy(None, None, owned_conversation)
            .await
            .finish(observed.map(Ok));
    }

    async fn prove_shell_as_local_tool(
        &self,
        workspace: &Workspace<StateStore, Database, ObservedProvider>,
    ) {
        self.diagnostics.enter(ScenarioStage::Shell);
        let mut owned_conversation = None;
        let mut owned_child = None;
        let observed = std::panic::AssertUnwindSafe(async {
        let conversation = owned_conversation.insert(self.conversation(workspace, "shell-local-tool").await);
        let terminal = format!("CUSTOM_SHELL_{}", uuid::Uuid::new_v4().simple());
        let child_name = format!("strict_custom_child_{}", uuid::Uuid::new_v4().simple());
        let child = self.root.join(&child_name);
        std::fs::create_dir(&child).expect("strict-live Custom shell child must be created");
        owned_child = Some(child.clone());
        let file = "sentinel.txt";
        let path = child.join(file);
        let workspace_path = self.root.join(file);
        #[cfg(unix)]
        let command = format!("printf '%s' '{terminal}' > '{file}'");
        #[cfg(windows)]
        let command = format!("echo {terminal}>{file}");
        let request = PromptRequest::new(format!(
                "Call the ordinary shell function tool exactly once with command `{command}` and cwd `{child_name}`. After its real result, reply with exactly {terminal} and no other text."
            ))
            .expect("strict-live Custom shell prompt must be valid")
            .with_cwd(PathBuf::from(&child_name))
            .expect("strict-live Custom shell child cwd must be valid")
            .with_tool_selection(ToolSelection::Allowed(vec!["shell".to_owned()]))
            .expect("strict-live Custom shell selection must be valid");
        let response = tokio::time::timeout(PROMPT_TIMEOUT, conversation.prompt(request))
            .await
            .expect("strict-live Custom shell prompt must remain bounded")
            .expect("strict-live Custom shell prompt must succeed");
        assert!(!response.output().is_empty());
        assert_eq!(response.output(), &terminal);
        #[cfg(unix)]
        let expected_sentinel = terminal.clone();
        #[cfg(windows)]
        let expected_sentinel = format!("{terminal}\r\n");
        assert_eq!(
            std::fs::read_to_string(path)
                .expect("strict-live Custom shell must write its sentinel"),
            expected_sentinel,
            "the registered Local shell tool must execute through the Custom function loop"
        );
        assert!(
            !workspace_path.exists(),
            "the relative shell sentinel must not be written at the workspace root"
        );

        }).catch_unwind().await;
        let mut cleanup = Self::settle_hierarchy(None, None, owned_conversation).await;
        if let Some(child) = owned_child {
            cleanup
                .settle(async { std::fs::remove_dir_all(child) })
                .await;
        }
        cleanup.finish(observed.map(Ok));
    }

    async fn prove_durable_approval(&self) {
        self.diagnostics.enter(ScenarioStage::Approval);
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = std::panic::AssertUnwindSafe(async {
            let agent = owned_agent.insert(self
                .build_agent("strict-custom-reduced-approval", CustomToolApproval::Interaction, false, ScenarioBuildMode::Create)
                .await);
            let workspace = owned_workspace.insert(self.workspace(agent, "strict-custom-reduced-approval").await);
            let conversation = owned_conversation.insert(self.conversation(workspace, "durable-approval").await);
        let mut receiver = conversation
            .notification_stream()
            .expect("strict-live approval receiver must open");
        let calls_before = self.continuation_calls.load(Ordering::SeqCst);
        let terminal = format!("CUSTOM_APPROVAL_{}", uuid::Uuid::new_v4().simple());
        let stream = conversation.prompt_stream(
            PromptRequest::new(format!(
                "Call strict_continue exactly once with value `approved`. After its real result, reply with exactly {terminal} and no other text."
            ))
            .expect("strict-live approval prompt must be valid")
            .with_tool_selection(ToolSelection::Allowed(vec![
                "strict_continue".to_owned(),
            ]))
            .expect("strict-live approval selection must be valid"),
        ).await.expect("strict-live approval stream must be admitted");
        let mut prompt = Box::pin(NativeCleanup::observe_stream(stream));
        let notification = tokio::select! {
            notification = self.receive_interaction(&mut receiver) => notification,
            result = &mut prompt => {
                panic!("Custom approval prompt terminalized before resolution: {result:?}")
            }
        };
        match notification.kind() {
            ConversationNotificationKind::Interaction {
                response_schema,
                dismissible,
                ..
            } => {
                assert!(response_schema.is_none());
                assert!(!*dismissible);
            }
            _ => unreachable!("interaction receiver guarantees the notification kind"),
        }
        assert_eq!(
            self.continuation_calls.load(Ordering::SeqCst),
            calls_before,
            "approval-gated ToolHandler work must not execute before Allow"
        );
        conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow)
            .await
            .expect("strict-live Custom approval must resolve");
        let response = tokio::time::timeout(PROMPT_TIMEOUT, async {
            loop {
                tokio::select! {
                    result = &mut prompt => break result,
                    notification = receiver.recv() => {
                        let notification = notification
                            .expect("strict-live Custom approval stream must remain open")
                            .expect("strict-live Custom approval delivery must succeed");
                        if matches!(
                            notification.kind(),
                            ConversationNotificationKind::Interaction { .. }
                        ) {
                            conversation
                                .resolve_notification(
                                    notification.id().clone(),
                                    NotificationResolution::Deny {
                                        reason: "strict-live proof admits exactly one tool execution"
                                            .to_owned(),
                                    },
                                )
                                .await
                                .expect("additional Custom approval must deny durably");
                        }
                    }
                }
            }
        })
            .await
            .expect("approved Custom prompt must remain bounded");
        assert!(!response.output().is_empty());
        assert_eq!(response.output(), &terminal);
        drop(prompt);
        assert_eq!(
            self.continuation_calls.load(Ordering::SeqCst),
            calls_before + 1,
            "resolved approval must continue exactly one normalized function call"
        );
        drop(receiver);
        }).catch_unwind().await;
        Self::settle_hierarchy(owned_agent, owned_workspace, owned_conversation)
            .await
            .finish(observed.map(Ok));
    }

    async fn runtime_loss_child(handoff: PathBuf) {
        let scenario = Self::new("runtime-loss-child").await;
        scenario.diagnostics.enter(ScenarioStage::RuntimeLoss);
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = std::panic::AssertUnwindSafe(async {
            let agent = owned_agent.insert(scenario
                .build_agent("strict-custom-reduced-runtime-loss", CustomToolApproval::Allow, false, ScenarioBuildMode::Create)
                .await);
            let workspace = owned_workspace.insert(scenario.workspace(agent, "strict-custom-reduced-runtime-loss").await);
            let conversation = owned_conversation.insert(scenario.conversation(workspace, "runtime-loss").await);
        let mut receiver = conversation
            .notification_stream()
            .expect("runtime-loss child receiver must open");
        let mut prompt = Box::pin(
            conversation.prompt(
                PromptRequest::new(
                    r#"Call strict_interaction exactly once with arguments {"request":"wait for the strict caller continuation"}. Wait for its real caller-supplied result before replying."#,
                )
                .expect("runtime-loss child prompt must be valid")
                .with_tool_selection(ToolSelection::Allowed(vec![
                    "strict_interaction".to_owned(),
                ]))
                .expect("runtime-loss child selection must be valid"),
            ),
        );
        let notification = tokio::select! {
            notification = scenario.receive_interaction(&mut receiver) => notification,
            result = &mut prompt => {
                panic!("runtime-loss child prompt terminalized before handoff: {result:?}")
            }
        };
        let facts = RuntimeLossFacts {
            agent_id: agent.id(),
            workspace_id: workspace.id(),
            conversation_id: conversation.id(),
            notification_id: notification.id().clone(),
            root: scenario.root.clone(),
        };
        std::fs::write(
            handoff,
            serde_json::to_vec(&facts).expect("runtime-loss facts must encode"),
        )
        .expect("runtime-loss handoff must be written before process exit");
        // Preserve the original crash boundary while prompt/receiver are still live.
        std::process::exit(0);
        }).catch_unwind().await;
        let mut cleanup =
            Self::settle_hierarchy(owned_agent, owned_workspace, owned_conversation).await;
        cleanup
            .settle(async { std::fs::remove_dir_all(&scenario.root) })
            .await;
        cleanup.finish(observed.map(Ok));
    }

    async fn prove_runtime_loss_and_reopen(&self) {
        self.diagnostics.enter(ScenarioStage::RuntimeLoss);
        let facts = serde_json::from_slice::<RuntimeLossFacts>(
            &self
                .fixture
                .crash_child(RUNTIME_LOSS_TEST, &[], RUNTIME_LOSS_TIMEOUT)
                .await,
        )
        .expect("runtime-loss handoff must decode");

        let calls_before = self.interaction_calls.load(Ordering::SeqCst);
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = std::panic::AssertUnwindSafe(async {
            let agent = owned_agent.insert(self
                .build_agent("strict-custom-reduced-runtime-loss", CustomToolApproval::Allow, false, ScenarioBuildMode::Reopen(facts.agent_id))
                .await);
            let workspace = owned_workspace.insert(agent.get_workspace(facts.workspace_id).await.expect("runtime-loss Custom workspace must reopen"));
            let conversation = owned_conversation.insert(workspace.get_conversation(facts.conversation_id).await.expect("runtime-loss Custom conversation must reopen"));
            assert_eq!(
                self.interaction_calls.load(Ordering::SeqCst),
                calls_before,
                "reopen must not replay a lost ToolHandler action"
            );
            assert!(matches!(conversation
                    .resolve_notification(
                        facts.notification_id.clone(),
                        NotificationResolution::Submit {
                            content: serde_json::Value::String("MUST_NOT_RESUME".to_owned()),
                        },
                    )
                    .await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::NotFound)));

            let mut receiver = conversation
                .notification_stream()
                .expect("reopened runtime-loss receiver must open");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            loop {
                match tokio::time::timeout_at(deadline, receiver.recv()).await {
                    Err(_) => break,
                    Ok(Some(Ok(notification))) => {
                        assert_ne!(
                            notification.id(),
                            &facts.notification_id,
                            "lost one-shot Custom interaction must never replay"
                        );
                        assert!(
                            !matches!(
                                notification.kind(),
                                ConversationNotificationKind::Interaction { .. }
                            ),
                            "Custom reopen must not fabricate a replacement interaction"
                        );
                    }
                    Ok(Some(Err(error))) => {
                        panic!("reopened runtime-loss delivery failed: {error}")
                    }
                    Ok(None) => panic!("reopened runtime-loss receiver closed unexpectedly"),
                }
            }
            drop(receiver);

            let terminal = format!(
                "CUSTOM_AFTER_RUNTIME_LOSS_{}",
                uuid::Uuid::new_v4().simple()
            );
            self.prompt(
                conversation,
                format!("Reply with exactly {terminal} and no other text."),
                ToolSelection::None,
                &terminal,
            )
            .await;
            assert_eq!(
                self.interaction_calls.load(Ordering::SeqCst),
                calls_before,
                "post-loss reuse must not replay the abandoned interaction"
            );
        })
        .catch_unwind()
        .await;
        let mut cleanup =
            Self::settle_hierarchy(owned_agent, owned_workspace, owned_conversation).await;
        cleanup
            .settle(async { std::fs::remove_dir_all(facts.root) })
            .await;
        cleanup.finish(observed.map(Ok));
    }
}

#[tokio::test]
async fn custom_provider_model_tool_loop_and_runtime_ownership_are_live() {
    StrictCustomScenario::run().await;
}

#[tokio::test]
async fn custom_incremental_output_abort_and_reopen_are_live() {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1_200);
    let scenario = tokio::time::timeout_at(deadline, StrictCustomScenario::new("stream"))
        .await
        .expect("Custom streaming setup must remain inside its original deadline");
    // Each hierarchy owns the deadline internally; it cannot cancel its own cleanup.
    let observed = std::panic::AssertUnwindSafe(scenario.prove_stream_and_reopen(deadline))
        .catch_unwind()
        .await;
    let mut cleanup = NativeCleanup::new(LIFECYCLE_TIMEOUT);
    cleanup
        .settle(async { std::fs::remove_dir_all(&scenario.root) })
        .await;
    cleanup.finish(observed.map(Ok));
}

#[tokio::test]
async fn custom_openai_continuation_stream_and_reopen_are_live() {
    let config =
        OpenAiConfig::new(ApiKey::new(StrictFixture::required("TEST_OPENAI_API_KEY")).unwrap())
            .with_base_url(
                reqwest::Url::parse(&StrictFixture::required("TEST_OPENAI_BASE_URL")).unwrap(),
            );
    let transport = TransportConfig::new(
        Duration::from_secs(180),
        Duration::from_secs(15),
        Duration::from_secs(60),
        RetryPolicy::default(),
    )
    .unwrap();
    StrictCustomScenario::run_cloud_continuation(
        ProviderConfig::new(ProviderKind::OpenAi(config), transport),
        "TEST_CUSTOM_OPENAI_MODEL",
        |provider| provider,
        None,
    )
    .await;
}

#[tokio::test]
async fn custom_anthropic_continuation_stream_and_reopen_are_live() {
    let evidence = Arc::new(Mutex::new(AnthropicEvidence::new(
        &StrictFixture::required("TEST_CUSTOM_ANTHROPIC_MODEL"),
    )));
    let config = AnthropicConfig::new(
        ApiKey::new(StrictFixture::required("TEST_ANTHROPIC_API_KEY")).unwrap(),
    )
    .with_base_url(
        reqwest::Url::parse(&StrictFixture::required("TEST_ANTHROPIC_BASE_URL")).unwrap(),
    );
    let transport = TransportConfig::new(
        Duration::from_secs(180),
        Duration::from_secs(15),
        Duration::from_secs(60),
        RetryPolicy::default(),
    )
    .unwrap();
    StrictCustomScenario::run_cloud_continuation(
        ProviderConfig::new(ProviderKind::Anthropic(config), transport),
        "TEST_CUSTOM_ANTHROPIC_MODEL",
        {
            let evidence = Arc::clone(&evidence);
            move |provider| ObservedProvider::anthropic(provider, Arc::clone(&evidence))
        },
        Some(evidence),
    )
    .await;
}
