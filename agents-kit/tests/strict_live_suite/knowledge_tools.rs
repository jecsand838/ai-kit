use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::knowledge_evidence::KnowledgeEvidence;
use crate::native_cleanup::NativeCleanup;
use tokio::task::JoinHandle;

use agents_kit::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
use agents_kit::conversation::NewConversation;
use agents_kit::identity::{AgentId, ConversationId, WorkspaceId};
use agents_kit::knowledge::{
    IngestKnowledgeRequest, KnowledgePolicy, KnowledgeScope, KnowledgeSource,
};
use agents_kit::model::ModelSelector;
use agents_kit::prompt::PromptRequest;
use agents_kit::tool::ToolSelection;
use agents_kit::workspace::{
    NewWorkspace, WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
    WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
};
use agents_kit::{Agent, AgentBuilder, AgentStorage, Conversation, KitError, Workspace};
use agents_kit::{AgentBackend, ConversationBackend, WorkspaceBackend};
use providers_kit::config::{OllamaConfig, ProviderKind, RetryPolicy, TransportConfig};
use providers_kit::generation::{
    GenerateEventStream, GenerateRequest, GenerateResponse, GenerationContinuation,
};
use providers_kit::models::{ModelListRequest, ModelPage};
use providers_kit::{
    KitError as ProviderError, Model, ModelBackend, ModelId, Provider, ProviderBackend,
    ProviderConfig, Session,
};
use storage_kit::database::config::{DatabaseConfig, LanceConfig};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::{Database, StateStore};

const BUILD_TIMEOUT: Duration = Duration::from_secs(180);
const PROMPT_TIMEOUT: Duration = Duration::from_secs(180);
const PROMPT_WAIT_BOUND: Duration = Duration::from_secs(210);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(150);

struct StrictKnowledgeInputs {
    storage: AgentStorage,
    agent: Option<Agent<StateStore, Database, KnowledgeProvider>>,
    workspace: Option<Workspace<StateStore, Database, KnowledgeProvider>>,
    conversation: Option<Conversation<StateStore, Database, KnowledgeProvider>>,
    pending_cleanup: Option<JoinHandle<Result<(), KitError>>>,
    database_path: StrictDatabasePath,
    state_path: Option<StrictDatabasePath>,
    evidence: Arc<Mutex<KnowledgeEvidence>>,
}

// These owners observe only this fixture's real unary generation seam. They do
// not substitute responses, replay calls, or retain provider continuation data.
struct KnowledgeProvider {
    inner: Provider,
    evidence: Arc<Mutex<KnowledgeEvidence>>,
}

struct KnowledgeModel {
    inner: Model,
    evidence: Arc<Mutex<KnowledgeEvidence>>,
}

impl ProviderBackend for KnowledgeProvider {
    type Model = KnowledgeModel;

    fn model(&self, id: ModelId) -> Result<Self::Model, ProviderError> {
        self.inner.model(id).map(|inner| KnowledgeModel {
            inner,
            evidence: Arc::clone(&self.evidence),
        })
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, ProviderError> {
        self.inner.list_models(request).await
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        self.inner.shutdown().await
    }
}

impl ModelBackend for KnowledgeModel {
    type Continuation = GenerationContinuation;
    type Session = Session;

    fn model_id(&self) -> &ModelId {
        self.inner.model_id()
    }

    async fn generate(
        &self,
        request: &GenerateRequest<Self::Continuation>,
    ) -> Result<GenerateResponse<Self::Continuation>, ProviderError> {
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .request(self.model_id(), request, false);
        let result = self.inner.generate(request).await;
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .response(&result);
        result
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<Self::Continuation>,
    ) -> Result<GenerateEventStream<Self::Continuation>, ProviderError> {
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .request(self.model_id(), request, true);
        self.inner.generate_stream(request).await
    }
}

struct StrictDatabasePath {
    path: Option<PathBuf>,
}

struct ScopeTokens {
    database: String,
    agent_a: String,
    workspace_a: String,
    conversation_a: String,
    conversation_reopened: String,
    agent_b: String,
}

struct ScopeObservation {
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    output: String,
}

impl StrictDatabasePath {
    fn new(label: &str) -> Self {
        Self {
            path: Some(std::env::temp_dir().join(format!(
                "agents-kit-strict-{label}-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            ))),
        }
    }

    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("strict-live Database path must remain owned")
    }

    fn replace_with_blocking_file(&self) {
        Self::remove(self.path()).expect("strict-live Lance path must be cleared before blocking");
        std::fs::write(
            self.path(),
            b"this file intentionally blocks a Lance directory",
        )
        .expect("strict-live blocked Lance path must be created");
    }

    fn cleanup(mut self) {
        if let Some(path) = self.path.take() {
            Self::remove(&path)
                .unwrap_or_else(|error| panic!("failed to remove {}: {error}", path.display()));
        }
    }

    fn remove(path: &Path) -> std::io::Result<()> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path),
            Ok(_) => std::fs::remove_file(path),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl Drop for StrictDatabasePath {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = Self::remove(&path);
        }
    }
}

impl ScopeTokens {
    fn markers(&self) -> [String; 6] {
        [
            self.database.clone(),
            self.agent_a.clone(),
            self.workspace_a.clone(),
            self.conversation_a.clone(),
            self.conversation_reopened.clone(),
            self.agent_b.clone(),
        ]
    }

    fn new() -> Self {
        Self {
            database: Self::token("DATABASE"),
            agent_a: Self::token("AGENT_A"),
            workspace_a: Self::token("WORKSPACE_A"),
            conversation_a: Self::token("CONVERSATION_A"),
            conversation_reopened: Self::token("CONVERSATION_REOPENED"),
            agent_b: Self::token("AGENT_B"),
        }
    }

    fn token(scope: &str) -> String {
        format!(
            "STRICT_{scope}_{}",
            uuid::Uuid::new_v4().simple().to_string().to_uppercase()
        )
    }

    fn primary(&self) -> String {
        let markers: [&str; 4] = [
            &self.database,
            &self.agent_a,
            &self.workspace_a,
            &self.conversation_a,
        ];
        markers.join(" ")
    }

    fn reopened(&self) -> String {
        let markers: [&str; 4] = [
            &self.database,
            &self.agent_a,
            &self.workspace_a,
            &self.conversation_reopened,
        ];
        markers.join(" ")
    }
}

impl StrictKnowledgeInputs {
    fn required(name: &str) -> String {
        let value = std::env::var(name)
            .unwrap_or_else(|_| panic!("missing required strict-live environment: {name}"));
        assert_eq!(
            value.trim(),
            value,
            "{name} contains surrounding whitespace"
        );
        assert!(!value.is_empty(), "{name} must not be empty");
        value
    }

    async fn new(label: &str) -> Self {
        let state_path = StrictDatabasePath::new(&format!("{label}-state"));
        let state = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            state_path.path(),
        )))
        .await
        .expect("strict-live separate-directory Lance StateStore must connect");
        Self {
            storage: AgentStorage::new(state),
            database_path: StrictDatabasePath::new(label),
            state_path: Some(state_path),
            agent: None,
            workspace: None,
            conversation: None,
            pending_cleanup: None,
            evidence: Arc::new(Mutex::new(KnowledgeEvidence::default())),
        }
    }

    async fn lance(label: &str) -> Self {
        let database_path = StrictDatabasePath::new(label);
        let state = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            database_path.path(),
        )))
        .await
        .expect("strict-live same-directory Lance StateStore must connect");
        Self {
            storage: AgentStorage::new(state),
            database_path,
            state_path: None,
            agent: None,
            workspace: None,
            conversation: None,
            pending_cleanup: None,
            evidence: Arc::new(Mutex::new(KnowledgeEvidence::default())),
        }
    }

    async fn reopen_lance_state(&mut self) {
        assert!(self.agent.is_none() && self.workspace.is_none() && self.conversation.is_none());
        assert!(self.pending_cleanup.is_none());
        self.storage = AgentStorage::new(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
                self.database_path.path(),
            )))
            .await
            .expect("reopen Lance state beside existing knowledge collections"),
        );
    }

    fn provider(&self) -> KnowledgeProvider {
        let base_url = reqwest::Url::parse(&Self::required("TEST_CUSTOM_OLLAMA_URL"))
            .expect("TEST_CUSTOM_OLLAMA_URL must be a URL");
        let ollama = OllamaConfig::local()
            .with_base_url(base_url)
            .expect("TEST_CUSTOM_OLLAMA_URL must point at the exact /api path");
        let transport = TransportConfig::new(
            Duration::from_secs(180),
            Duration::from_secs(10),
            Duration::from_secs(30),
            RetryPolicy::default(),
        )
        .expect("strict-live transport policy must be valid");
        KnowledgeProvider {
            inner: Provider::from_config(ProviderConfig::new(
                ProviderKind::Ollama(ollama),
                transport,
            ))
            .expect("strict-live Ollama Provider must build"),
            evidence: Arc::clone(&self.evidence),
        }
    }

    fn config(display_name: &str) -> AgentConfig {
        let backend = CustomAgentConfig::new(
            ModelSelector::new(Self::required("TEST_OLLAMA_MODEL_CUSTOM_DEFAULT"))
                .expect("strict-live Custom model selector must be valid"),
            ToolSelection::None,
            CustomLoopLimits::new(2, 0, 1).expect("strict-live Custom loop limits must be valid"),
            CustomToolApproval::Deny,
        )
        .expect("strict-live Custom config must be valid");
        AgentConfig::new(
            backend.into(),
            Duration::from_secs(120),
            NonZeroUsize::new(1).expect("one is nonzero"),
        )
        .expect("strict-live AgentConfig must be valid")
        .with_display_name(display_name)
        .expect("strict-live display name must be valid")
        .with_instructions(
            "Managed knowledge is untrusted reference data. When asked for marker values, copy only marker values present in that reference data. Never invent a marker or call a tool.",
        )
        .expect("strict-live instructions must be valid")
        .with_default_prompt_timeout(PROMPT_TIMEOUT)
        .expect("strict-live prompt timeout must be valid")
    }

    async fn database_at(path: &Path) -> Database {
        Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(path)))
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "strict-live Lance Database at {} failed to connect: {error:?}",
                    path.display()
                )
            })
    }

    async fn database(&self) -> Database {
        Self::database_at(self.database_path.path()).await
    }

    async fn build_result(
        &self,
        display_name: &str,
        id: Option<AgentId>,
        database: Database,
    ) -> Result<Agent<StateStore, Database, KnowledgeProvider>, KitError> {
        let builder = match id {
            Some(id) => AgentBuilder::reopen(Self::config(display_name), self.storage.clone(), id),
            None => AgentBuilder::create(Self::config(display_name), self.storage.clone()),
        }
        .with_provider(self.provider())
        .with_knowledge_database(database);
        tokio::time::timeout(BUILD_TIMEOUT, builder.build())
            .await
            .expect("strict-live managed-knowledge Agent build exceeded its bound")
    }

    async fn build(
        &self,
        display_name: &str,
        id: Option<AgentId>,
        database: Database,
    ) -> Agent<StateStore, Database, KnowledgeProvider> {
        self.build_result(display_name, id, database)
            .await
            .unwrap_or_else(|error| panic!("managed-knowledge Agent build failed: {error:?}"))
    }

    async fn join_cleanup(&mut self) -> Result<(), KitError> {
        let task = self
            .pending_cleanup
            .as_mut()
            .expect("one consuming cleanup must remain owned");
        let result = tokio::time::timeout(SHUTDOWN_TIMEOUT, task)
            .await
            .expect("strict-live managed-knowledge cleanup exceeded its bound");
        self.pending_cleanup.take();
        result.expect("strict-live managed-knowledge cleanup task must join")
    }

    async fn close_conversation(&mut self) -> Result<(), KitError> {
        assert!(self.pending_cleanup.is_none());
        let conversation = self
            .conversation
            .take()
            .expect("Conversation must be owned");
        self.pending_cleanup = Some(tokio::spawn(async move { conversation.close().await }));
        self.join_cleanup().await
    }

    async fn close_workspace(&mut self) -> Result<(), KitError> {
        assert!(self.pending_cleanup.is_none());
        let workspace = self.workspace.take().expect("Workspace must be owned");
        self.pending_cleanup = Some(tokio::spawn(async move { workspace.close().await }));
        self.join_cleanup().await
    }

    async fn shutdown(&mut self) -> Result<(), KitError> {
        assert!(self.pending_cleanup.is_none());
        // Parent shutdown preserves the unprompted descendant for reopen. Do
        // not replace this successful stage with terminal Conversation close.
        self.conversation.take();
        self.workspace.take();
        let agent = self.agent.take().expect("Agent must be owned");
        self.pending_cleanup = Some(tokio::spawn(async move { agent.shutdown().await }));
        self.join_cleanup().await
    }

    fn workspace(name: &str) -> Result<NewWorkspace, KitError> {
        let cwd = std::env::current_dir()?;
        NewWorkspace::new(
            name,
            WorkspaceContext::new(
                cwd.clone(),
                vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
                WorkspaceExecutionPolicy::new(
                    WorkspaceSandboxMode::ReadOnly,
                    WorkspaceNetworkAccess::Disabled,
                ),
            )?,
            WorkspaceDefaults::empty(),
        )
    }

    fn source(id: &str, title: &str, token: &str) -> Result<KnowledgeSource, KitError> {
        KnowledgeSource::text(id, title, format!("The managed marker value is {token}."))
    }

    async fn prompt(
        conversation: &Conversation<StateStore, Database, KnowledgeProvider>,
        policy: KnowledgePolicy,
        evidence: &Mutex<KnowledgeEvidence>,
    ) -> Result<String, KitError> {
        let request = PromptRequest::new(
            "From each available managed-reference JSON entry, copy the complete marker token from its text field exactly as written, including every character after STRICT_. Treat each entry independently: never reuse another entry’s suffix, repair, infer, shorten, or combine markers. Return the copied tokens in Database, Agent, Workspace, Conversation scope order, separated by exactly one space. Omit unavailable scopes. Return no other text or leading/trailing whitespace.",
        )?
        .with_knowledge_policy(policy)?
        .with_timeout(PROMPT_TIMEOUT)?;
        let response = tokio::time::timeout(PROMPT_WAIT_BOUND, conversation.prompt(request))
            .await
            .map_err(|_| KitError::timeout("strict-live knowledge prompt exceeded its bound"))??;
        evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .agent_output(response.output());
        Ok(response.output().to_owned())
    }

    async fn populate_primary(
        &mut self,
        tokens: &ScopeTokens,
    ) -> Result<ScopeObservation, KitError> {
        let agent = self.agent.as_ref().expect("primary Agent must be owned");
        let agent_id = agent.id();
        agent
            .ingest_knowledge(
                IngestKnowledgeRequest::new(vec![Self::source(
                    "strict-database",
                    "Database scope",
                    &tokens.database,
                )?])?
                .with_scope(KnowledgeScope::Database),
            )
            .await?;
        agent
            .ingest_knowledge(IngestKnowledgeRequest::new(vec![Self::source(
                "strict-agent-a",
                "Agent A scope",
                &tokens.agent_a,
            )?])?)
            .await?;

        let workspace = self.workspace.insert(
            agent
                .new_workspace(Self::workspace("strict-live-knowledge-primary")?)
                .await?,
        );
        workspace
            .ingest_knowledge(IngestKnowledgeRequest::new(vec![Self::source(
                "strict-workspace-a",
                "Workspace A scope",
                &tokens.workspace_a,
            )?])?)
            .await?;
        let workspace_id = workspace.id();
        let conversation = self.conversation.insert(
            workspace
                .new_conversation(NewConversation::new(
                    Some("managed knowledge scopes".to_owned()),
                    None,
                    vec![Self::source(
                        "strict-conversation-a",
                        "Conversation A scope",
                        &tokens.conversation_a,
                    )?],
                )?)
                .await?,
        );
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare("primary", tokens.markers(), [1, 1, 1, 1, 0, 0]);
        let output = Self::prompt(
            conversation,
            KnowledgePolicy::only(vec![
                KnowledgeScope::Database,
                KnowledgeScope::Agent,
                KnowledgeScope::Workspace,
                KnowledgeScope::Conversation,
            ])?,
            &self.evidence,
        )
        .await?;
        self.close_conversation().await?;
        // This descendant has never been prompted. Its fresh marker can only
        // reach the reopened answer through persisted managed knowledge.
        let reopened_conversation = self
            .workspace
            .as_ref()
            .expect("primary Workspace must remain owned")
            .new_conversation(NewConversation::new(
                Some("unprompted managed knowledge reopen".to_owned()),
                None,
                vec![Self::source(
                    "strict-conversation-reopened",
                    "Conversation reopen scope",
                    &tokens.conversation_reopened,
                )?],
            )?)
            .await?;
        let conversation_id = reopened_conversation.id();
        self.conversation = Some(reopened_conversation);
        Ok(ScopeObservation {
            agent_id,
            workspace_id,
            conversation_id,
            output,
        })
    }

    async fn observe_reopened(
        &mut self,
        observation: &ScopeObservation,
        tokens: &ScopeTokens,
    ) -> Result<String, KitError> {
        let workspace = self.workspace.insert(
            self.agent
                .as_ref()
                .expect("reopened Agent must be owned")
                .get_workspace(observation.workspace_id.clone())
                .await?,
        );
        let conversation = self.conversation.insert(
            workspace
                .get_conversation(observation.conversation_id.clone())
                .await?,
        );
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare("reopened", tokens.markers(), [1, 1, 1, 0, 1, 0]);
        let output = Self::prompt(
            conversation,
            KnowledgePolicy::only(vec![
                KnowledgeScope::Database,
                KnowledgeScope::Agent,
                KnowledgeScope::Workspace,
                KnowledgeScope::Conversation,
            ])?,
            &self.evidence,
        )
        .await?;
        self.close_conversation().await?;
        self.close_workspace().await?;
        Ok(output)
    }

    async fn populate_secondary(&mut self, tokens: &ScopeTokens) -> Result<String, KitError> {
        let agent = self.agent.as_ref().expect("secondary Agent must be owned");
        agent
            .ingest_knowledge(IngestKnowledgeRequest::new(vec![Self::source(
                "strict-agent-b",
                "Agent B scope",
                &tokens.agent_b,
            )?])?)
            .await?;
        let workspace = self.workspace.insert(
            agent
                .new_workspace(Self::workspace("strict-live-knowledge-secondary")?)
                .await?,
        );
        let conversation = self.conversation.insert(
            workspace
                .new_conversation(NewConversation::new(None, None, Vec::new())?)
                .await?,
        );
        self.evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare("secondary", tokens.markers(), [1, 0, 0, 0, 0, 1]);
        let output = Self::prompt(
            conversation,
            KnowledgePolicy::only(vec![KnowledgeScope::Database, KnowledgeScope::Agent])?,
            &self.evidence,
        )
        .await?;
        self.close_conversation().await?;
        self.close_workspace().await?;
        Ok(output)
    }

    async fn cleanup(mut self, cleanup: &mut NativeCleanup) {
        if let Some(mut task) = self.pending_cleanup.take() {
            cleanup.join(&mut task).await;
        }
        if let Some(conversation) = self.conversation.take() {
            let mut task = tokio::spawn(async move { conversation.close().await });
            cleanup.join(&mut task).await;
        }
        if let Some(workspace) = self.workspace.take() {
            let mut task = tokio::spawn(async move { workspace.close().await });
            cleanup.join(&mut task).await;
        }
        if let Some(agent) = self.agent.take() {
            let mut task = tokio::spawn(async move { agent.shutdown().await });
            cleanup.join(&mut task).await;
        }
        drop(self.storage);
        cleanup
            .settle(async {
                self.database_path.cleanup();
                if let Some(state_path) = self.state_path {
                    state_path.cleanup();
                }
                Ok::<(), KitError>(())
            })
            .await;
    }
}

#[tokio::test]
async fn managed_knowledge_scopes_reopen_and_preserve_owner_integrity() {
    StrictKnowledgeInputs::scope_case(false).await;
}

#[tokio::test]
async fn lance_state_directory_managed_knowledge_scopes_reopen_and_preserve_owner_integrity() {
    StrictKnowledgeInputs::scope_case(true).await;
}

impl StrictKnowledgeInputs {
    async fn scope_case(same_directory: bool) {
        let mut owned_inputs = None;
        let mut wrong_path = None;
        let mut retained_evidence = None;
        let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(1_200),
        async {
            let inputs = owned_inputs.insert(if same_directory {
                StrictKnowledgeInputs::lance("knowledge-state-scopes").await
            } else {
                StrictKnowledgeInputs::new("knowledge-scopes").await
            });
            retained_evidence = Some(Arc::clone(&inputs.evidence));
            let tokens = ScopeTokens::new();
            let display_name = "strict-live-custom-managed-knowledge";

            inputs.agent = Some(
                inputs
                    .build(display_name, None, inputs.database().await)
                    .await,
            );
            let primary = inputs
                .populate_primary(&tokens)
                .await
                .expect("initial managed-knowledge flow must succeed");
            assert_eq!(primary.output, tokens.primary());
            inputs.evidence.lock().unwrap_or_else(std::sync::PoisonError::into_inner).assert_phase();
            inputs
                .shutdown()
                .await
                .expect("primary managed-knowledge Agent shutdown must succeed");
            if same_directory {
                inputs.reopen_lance_state().await;
            }

            let wrong_path = wrong_path.insert(StrictDatabasePath::new("knowledge-wrong-database"));
            let wrong_database = StrictKnowledgeInputs::database_at(wrong_path.path()).await;
            let wrong_reopen = inputs
                .build_result(display_name, Some(primary.agent_id.clone()), wrong_database)
                .await;
            let wrong_error = match wrong_reopen {
                Err(error) => error,
                Ok(agent) => {
                    inputs.agent = Some(agent);
                    panic!("reopen with a Database missing durable knowledge must fail")
                }
            };
            assert!(
                matches!(wrong_error.kind(), agents_kit::error::ErrorKind::Conflict),
                "missing durable managed-knowledge records must be Conflict, got {wrong_error:?}"
            );
            inputs.agent = Some(
                inputs
                    .build(
                        display_name,
                        Some(primary.agent_id.clone()),
                        inputs.database().await,
                    )
                    .await,
            );
            let reopened_output = inputs
                .observe_reopened(&primary, &tokens)
                .await
                .expect("durable managed-knowledge descendants must reopen");
            assert_eq!(reopened_output, tokens.reopened());
            inputs.evidence.lock().unwrap_or_else(std::sync::PoisonError::into_inner).assert_phase();
            inputs
                .shutdown()
                .await
                .expect("reopened managed-knowledge Agent shutdown must succeed");

            inputs.agent = Some(
                inputs
                    .build(
                        "strict-live-custom-managed-knowledge-secondary",
                        None,
                        inputs.database().await,
                    )
                    .await,
            );
            let secondary_output = inputs
                .populate_secondary(&tokens)
                .await
                .expect("cross-Agent Database-scope flow must succeed");
            assert_eq!(
                secondary_output,
                [tokens.database.as_str(), tokens.agent_b.as_str()].join(" ")
            );
            inputs.evidence.lock().unwrap_or_else(std::sync::PoisonError::into_inner).assert_phase();

            inputs
                .shutdown()
                .await
                .expect("secondary managed-knowledge Agent shutdown must succeed");
        },
    )
    .await;
        let mut cleanup = NativeCleanup::new(SHUTDOWN_TIMEOUT);
        if let Some(inputs) = owned_inputs {
            inputs.cleanup(&mut cleanup).await;
        }
        if let Some(path) = wrong_path {
            cleanup
                .settle(async {
                    path.cleanup();
                    Ok::<(), KitError>(())
                })
                .await;
        }
        if let Some(evidence) = retained_evidence {
            let cleaned = cleanup.diagnostics().is_empty();
            if let Some(path) = std::env::var_os("AGENTS_KIT_KNOWLEDGE_EVIDENCE_PATH") {
                let case = if same_directory {
                    "strict_live_suite::knowledge_tools::lance_state_directory_managed_knowledge_scopes_reopen_and_preserve_owner_integrity"
                } else {
                    "strict_live_suite::knowledge_tools::managed_knowledge_scopes_reopen_and_preserve_owner_integrity"
                };
                cleanup
                    .settle(async {
                        evidence
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .export_after_cleanup(
                                &PathBuf::from(path),
                                case,
                                matches!(&observed, Ok(Ok(()))),
                                cleaned,
                            )
                    })
                    .await;
            }
        }
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn managed_knowledge_database_failures_remain_non_conflict_integration_errors() {
    let mut owned_inputs = None;
    let mut blocked_path = None;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(300),
        async {
            let inputs =
                owned_inputs.insert(StrictKnowledgeInputs::new("knowledge-integration").await);
            let blocked_path =
                blocked_path.insert(StrictDatabasePath::new("knowledge-blocked-database"));
            let blocked_database = StrictKnowledgeInputs::database_at(blocked_path.path()).await;
            blocked_path.replace_with_blocking_file();
            inputs.agent = Some(
                inputs
                    .build(
                        "strict-live-custom-managed-knowledge-integration",
                        None,
                        blocked_database,
                    )
                    .await,
            );
            let ingestion = inputs
                .agent
                .as_ref()
                .expect("integration Agent must be owned")
                .ingest_knowledge(
                    IngestKnowledgeRequest::new(vec![
                        StrictKnowledgeInputs::source(
                            "strict-integration-failure",
                            "Integration failure",
                            "STRICT_INTEGRATION_FAILURE",
                        )
                        .expect("strict-live integration source must be valid"),
                    ])
                    .expect("strict-live integration request must be valid"),
                )
                .await;
            let error = ingestion.expect_err("blocked real Database ingestion must fail");
            assert!(
                matches!(error.kind(), agents_kit::error::ErrorKind::Integration),
                "non-conflict Database failure must remain Integration, got {error:?}"
            );

            inputs
                .shutdown()
                .await
                .expect("integration managed-knowledge Agent shutdown must succeed");
        },
    )
    .await;
    let mut cleanup = NativeCleanup::new(SHUTDOWN_TIMEOUT);
    if let Some(inputs) = owned_inputs {
        inputs.cleanup(&mut cleanup).await;
    }
    if let Some(path) = blocked_path {
        cleanup
            .settle(async {
                path.cleanup();
                Ok::<(), KitError>(())
            })
            .await;
    }
    cleanup.finish(observed);
}
