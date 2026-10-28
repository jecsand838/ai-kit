use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, UNIX_EPOCH};

use agents_kit::conversation::{ConversationInfo, FileRead, NewConversation};
use agents_kit::identity::{AgentId, AgentInfo, ConversationId, NotificationId, WorkspaceId};
use agents_kit::knowledge::IngestKnowledgeRequest;
use agents_kit::model::{ModelInfo, ModelSelector, ReasoningEffort};
use agents_kit::notification::{
    ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver,
    NotificationResolution,
};
use agents_kit::prompt::{PromptRequest, PromptResponse};
use agents_kit::tool::ToolInfo;
use agents_kit::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceContext,
    WorkspaceDefaults, WorkspaceExecutionPolicy, WorkspaceInfo, WorkspaceNetworkAccess,
    WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode, WorkspaceUpdateReadiness,
};
use agents_kit::{AgentBackend, ConversationBackend, KitError, WorkspaceBackend};

struct DirectHierarchyFixture {
    state: Arc<Mutex<DirectHierarchyState>>,
}

struct DirectHierarchyState {
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    notification_id: NotificationId,
    workspace: Option<WorkspaceRecord>,
    conversation: Option<ConversationRecord>,
    receiver_taken: bool,
    notification_resolved: bool,
    aborts: usize,
    shutdown: bool,
    agent_knowledge: Vec<IngestKnowledgeRequest>,
    workspace_knowledge: Vec<IngestKnowledgeRequest>,
}

struct WorkspaceRecord {
    name: String,
    context: WorkspaceContext,
    defaults: WorkspaceDefaults,
    revision: u64,
}

struct ConversationRecord {
    title: Option<String>,
    workspace_revision: u64,
}

struct DirectAgent {
    state: Arc<Mutex<DirectHierarchyState>>,
}

struct DirectWorkspace {
    state: Arc<Mutex<DirectHierarchyState>>,
    revision: u64,
    closed: Arc<AtomicBool>,
}

struct DirectConversation {
    state: Arc<Mutex<DirectHierarchyState>>,
    workspace_revision: u64,
    workspace_closed: Arc<AtomicBool>,
}

impl DirectHierarchyFixture {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(DirectHierarchyState {
                agent_id: Self::id("agent", 0x21),
                workspace_id: Self::id("workspace", 0x22),
                conversation_id: Self::id("conversation", 0x23),
                notification_id: Self::id("notification", 0x24),
                workspace: None,
                conversation: None,
                receiver_taken: false,
                notification_resolved: false,
                aborts: 0,
                shutdown: false,
                agent_knowledge: Vec::new(),
                workspace_knowledge: Vec::new(),
            })),
        }
    }

    fn id<T>(prefix: &str, suffix: u8) -> T
    where
        T: std::str::FromStr<Err = KitError>,
    {
        format!("{prefix}_{suffix:032x}")
            .parse()
            .expect("fixture identity must be canonical")
    }

    fn agent(&self) -> DirectAgent {
        DirectAgent {
            state: Arc::clone(&self.state),
        }
    }

    fn workspace_request(name: &str) -> NewWorkspace {
        let cwd = std::env::current_dir().expect("fixture cwd must exist");
        let root = WorkspaceRoot::new(cwd.clone(), WorkspaceRootAccess::ReadWrite)
            .expect("fixture root must be valid");
        let context = WorkspaceContext::new(
            cwd,
            vec![root],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Disabled,
            ),
        )
        .expect("fixture context must be valid");
        NewWorkspace::new(name, context, WorkspaceDefaults::empty())
            .expect("fixture workspace request must be valid")
    }

    fn state(&self) -> MutexGuard<'_, DirectHierarchyState> {
        self.state
            .lock()
            .expect("direct hierarchy state must not be poisoned")
    }
}

impl DirectHierarchyState {
    fn agent_info(&self) -> Result<AgentInfo, KitError> {
        self.ensure_agent_open()?;
        AgentInfo::new(self.agent_id.clone(), Some("Direct agent".to_owned()))
    }

    fn workspace_info(&self) -> Result<WorkspaceInfo, KitError> {
        self.ensure_agent_open()?;
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| KitError::not_found("fixture workspace does not exist"))?;
        WorkspaceInfo::new(
            self.workspace_id.clone(),
            workspace.name.clone(),
            workspace.context.clone(),
            workspace.defaults.clone(),
            workspace.revision,
        )
    }

    fn workspace_for_revision(&self, revision: u64) -> Result<&WorkspaceRecord, KitError> {
        self.ensure_agent_open()?;
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| KitError::not_found("fixture workspace does not exist"))?;
        if workspace.revision != revision {
            return Err(KitError::conflict("fixture workspace handle is stale"));
        }
        Ok(workspace)
    }

    fn conversation_info(&self) -> Result<ConversationInfo, KitError> {
        let conversation = self
            .conversation
            .as_ref()
            .ok_or_else(|| KitError::not_found("fixture conversation does not exist"))?;
        ConversationInfo::new(
            self.conversation_id.clone(),
            self.workspace_id.clone(),
            conversation.title.clone(),
        )
    }

    fn ensure_conversation_revision(&self, revision: u64) -> Result<(), KitError> {
        self.workspace_for_revision(revision)?;
        let conversation = self
            .conversation
            .as_ref()
            .ok_or_else(|| KitError::not_found("fixture conversation does not exist"))?;
        if conversation.workspace_revision != revision {
            return Err(KitError::conflict("fixture conversation handle is stale"));
        }
        Ok(())
    }

    fn ensure_agent_open(&self) -> Result<(), KitError> {
        if self.shutdown {
            Err(KitError::closed("fixture agent is shut down"))
        } else {
            Ok(())
        }
    }
}

impl DirectWorkspace {
    fn ensure_open<'a>(
        &self,
        state: &'a DirectHierarchyState,
    ) -> Result<&'a WorkspaceRecord, KitError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(KitError::closed("fixture workspace handle is closed"));
        }
        state.workspace_for_revision(self.revision)
    }
}

impl DirectConversation {
    fn ensure_open(&self, state: &DirectHierarchyState) -> Result<(), KitError> {
        if self.workspace_closed.load(Ordering::SeqCst) {
            return Err(KitError::closed(
                "fixture conversation parent workspace is closed",
            ));
        }
        state.ensure_conversation_revision(self.workspace_revision)
    }
}

impl AgentBackend for DirectAgent {
    type Workspace = DirectWorkspace;

    fn id(&self) -> AgentId {
        self.state
            .lock()
            .expect("direct hierarchy state must not be poisoned")
            .agent_id
            .clone()
    }

    async fn info(&self) -> Result<AgentInfo, KitError> {
        self.state
            .lock()
            .expect("direct hierarchy state must not be poisoned")
            .agent_info()
    }

    async fn new_workspace(&self, request: NewWorkspace) -> Result<DirectWorkspace, KitError> {
        let revision = {
            let mut state = self
                .state
                .lock()
                .expect("direct hierarchy state must not be poisoned");
            state.ensure_agent_open()?;
            if state.workspace.is_some() {
                return Err(KitError::conflict("fixture workspace already exists"));
            }
            state.workspace = Some(WorkspaceRecord {
                name: request.name().to_owned(),
                context: request.context().clone(),
                defaults: request.defaults().clone(),
                revision: 1,
            });
            1
        };
        Ok(DirectWorkspace {
            state: Arc::clone(&self.state),
            revision,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        state.ensure_agent_open()?;
        match state.workspace {
            Some(_) => Ok(vec![state.workspace_info()?]),
            None => Ok(Vec::new()),
        }
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        if id != state.workspace_id {
            return Err(KitError::not_found("fixture workspace id is unknown"));
        }
        state.workspace_info()
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<DirectWorkspace, KitError> {
        let revision = {
            let state = self
                .state
                .lock()
                .expect("direct hierarchy state must not be poisoned");
            if id != state.workspace_id {
                return Err(KitError::not_found("fixture workspace id is unknown"));
            }
            state.workspace_info()?.revision()
        };
        Ok(DirectWorkspace {
            state: Arc::clone(&self.state),
            revision,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_
    {
        let request = request.clone();
        async move {
            let state = self
                .state
                .lock()
                .expect("direct hierarchy state must not be poisoned");
            if request.id() != &state.workspace_id {
                return Err(KitError::not_found("fixture workspace id is unknown"));
            }
            state.workspace_for_revision(request.expected_revision())?;
            Ok(WorkspaceUpdateReadiness::Ready)
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        if request.id() != &state.workspace_id {
            return Err(KitError::not_found("fixture workspace id is unknown"));
        }
        state.workspace_for_revision(request.expected_revision())?;
        let next_revision = request
            .expected_revision()
            .checked_add(1)
            .ok_or_else(|| KitError::invalid_state("fixture revision overflow"))?;
        state.workspace = Some(WorkspaceRecord {
            name: request.name().to_owned(),
            context: request.context().clone(),
            defaults: request.defaults().clone(),
            revision: next_revision,
        });
        if let Some(conversation) = state.conversation.as_mut() {
            conversation.workspace_revision = next_revision;
        }
        state.workspace_info()
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        if request.id() != &state.workspace_id {
            return Err(KitError::not_found("fixture workspace id is unknown"));
        }
        state.workspace_for_revision(request.expected_revision())?;
        if state.conversation.is_some() {
            return Err(KitError::busy(
                "fixture workspace still owns a conversation",
            ));
        }
        state.workspace = None;
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        self.state
            .lock()
            .expect("direct hierarchy state must not be poisoned")
            .ensure_agent_open()?;
        Ok(vec![ModelInfo::new(
            ModelSelector::new("direct-model")?,
            Some("Direct model".to_owned()),
            vec![ReasoningEffort::low(), ReasoningEffort::high()],
            true,
        )?])
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        state.ensure_agent_open()?;
        state.agent_knowledge.push(request);
        Ok(())
    }

    async fn shutdown(self) -> Result<(), KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        state.shutdown = true;
        Ok(())
    }
}

impl WorkspaceBackend for DirectWorkspace {
    type Conversation = DirectConversation;

    fn id(&self) -> WorkspaceId {
        self.state
            .lock()
            .expect("direct hierarchy state must not be poisoned")
            .workspace_id
            .clone()
    }

    async fn info(&self) -> Result<WorkspaceInfo, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        state.workspace_info()
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<DirectConversation, KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        if state.conversation.is_some() {
            return Err(KitError::conflict("fixture conversation already exists"));
        }
        state.conversation = Some(ConversationRecord {
            title: request.title().map(str::to_owned),
            workspace_revision: self.revision,
        });
        state.receiver_taken = false;
        Ok(DirectConversation {
            state: Arc::clone(&self.state),
            workspace_revision: self.revision,
            workspace_closed: Arc::clone(&self.closed),
        })
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        match state.conversation {
            Some(_) => Ok(vec![state.conversation_info()?]),
            None => Ok(Vec::new()),
        }
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        if id != state.conversation_id {
            return Err(KitError::not_found("fixture conversation id is unknown"));
        }
        state.conversation_info()
    }

    async fn get_conversation(&self, id: ConversationId) -> Result<DirectConversation, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        if id != state.conversation_id {
            return Err(KitError::not_found("fixture conversation id is unknown"));
        }
        Ok(DirectConversation {
            state: Arc::clone(&self.state),
            workspace_revision: self.revision,
            workspace_closed: Arc::clone(&self.closed),
        })
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        Ok(vec![ToolInfo::new(
            "direct.lookup",
            Some("Direct caller-owned lookup".to_owned()),
            Some(serde_json::json!({"type": "object"})),
        )?])
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        state.workspace_knowledge.push(request);
        Ok(())
    }

    async fn close(self) -> Result<(), KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl ConversationBackend for DirectConversation {
    fn id(&self) -> ConversationId {
        self.state
            .lock()
            .expect("direct hierarchy state must not be poisoned")
            .conversation_id
            .clone()
    }

    async fn info(&self) -> Result<ConversationInfo, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        state.conversation_info()
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        match request.text() {
            "backend-error" => Err(KitError::integration("direct prompt backend failure")),
            "cancel" => Err(KitError::cancelled("direct prompt cancelled")),
            "timeout" => Err(KitError::timeout("direct prompt timed out")),
            text => Ok(PromptResponse::new(format!("direct: {text}"), None)),
        }
    }

    async fn abort(&self) -> Result<(), KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        state.aborts += 1;
        Ok(())
    }

    async fn read_file(&self, path: PathBuf) -> Result<FileRead, KitError> {
        let state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        FileRead::from_bytes(path, b"direct file\n".to_vec())
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        let notification = {
            let mut state = self
                .state
                .lock()
                .expect("direct hierarchy state must not be poisoned");
            self.ensure_open(&state)?;
            if state.receiver_taken {
                return Err(KitError::busy(
                    "fixture notification receiver is already acquired",
                ));
            }
            state.receiver_taken = true;
            ConversationNotification::new(
                state.notification_id.clone(),
                UNIX_EPOCH + Duration::from_secs(24),
                ConversationNotificationKind::Interaction {
                    title: "Direct decision".to_owned(),
                    body: "Choose a deterministic result".to_owned(),
                    response_schema: Some(serde_json::json!({
                        "type": "object",
                        "properties": {"approved": {"type": "boolean"}}
                    })),
                    dismissible: true,
                },
            )?
        };
        Ok(ConversationNotificationReceiver::from_stream(
            futures_util::stream::iter([
                Ok(notification),
                Err(KitError::protocol("direct notification sentinel")),
            ]),
        ))
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        _resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        if id != state.notification_id {
            return Err(KitError::not_found("fixture notification id is unknown"));
        }
        state.notification_resolved = true;
        Ok(())
    }

    async fn close(self) -> Result<(), KitError> {
        let mut state = self
            .state
            .lock()
            .expect("direct hierarchy state must not be poisoned");
        self.ensure_open(&state)?;
        state.conversation = None;
        Ok(())
    }
}

#[tokio::test]
async fn direct_hierarchy_exercises_the_complete_root_contract() {
    fn assert_hierarchy<A, W, C>()
    where
        A: AgentBackend<Workspace = W>,
        W: WorkspaceBackend<Conversation = C>,
        C: ConversationBackend,
    {
    }

    assert_hierarchy::<DirectAgent, DirectWorkspace, DirectConversation>();
    let fixture = DirectHierarchyFixture::new();
    let agent = fixture.agent();
    let agent_id = agent.id();
    assert_eq!(agent.info().await.expect("agent info").id(), &agent_id);
    assert!(
        agent
            .list_workspaces()
            .await
            .expect("empty inventory")
            .is_empty()
    );

    let workspace = agent
        .new_workspace(DirectHierarchyFixture::workspace_request(
            "Direct workspace",
        ))
        .await
        .expect("direct workspace must open");
    let original = workspace.info().await.expect("workspace info");
    assert_eq!(workspace.id(), original.id().clone());
    assert_eq!(
        agent.list_workspaces().await.expect("workspace inventory"),
        vec![original.clone()]
    );
    assert_eq!(
        agent
            .workspace_info(original.id().clone())
            .await
            .expect("workspace lookup"),
        original
    );

    let update = UpdateWorkspaceRequest::new(
        original.id().clone(),
        original.revision(),
        "Updated direct workspace",
        original.context().clone(),
        original.defaults().clone(),
    )
    .expect("update request must be valid");
    assert_eq!(
        agent
            .check_workspace_update(&update)
            .await
            .expect("update readiness"),
        WorkspaceUpdateReadiness::Ready
    );
    let updated = agent
        .update_workspace(update)
        .await
        .expect("workspace update");
    assert_eq!(updated.revision(), original.revision() + 1);
    assert!(
        matches!(workspace.info().await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Conflict))
    );
    drop(workspace);

    let workspace = agent
        .get_workspace(updated.id().clone())
        .await
        .expect("updated workspace must reopen");
    let models = agent.list_models().await.expect("model inventory");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].selector().as_str(), "direct-model");
    let tools = workspace.list_tools().await.expect("tool inventory");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name(), "direct.lookup");

    let conversation = workspace
        .new_conversation(
            NewConversation::new(Some("Direct conversation".to_owned()), None, Vec::new())
                .expect("conversation request must be valid"),
        )
        .await
        .expect("direct conversation must open");
    let conversation_info = conversation.info().await.expect("conversation info");
    assert_eq!(conversation.id(), conversation_info.id().clone());
    assert_eq!(
        workspace
            .list_conversations()
            .await
            .expect("conversation inventory"),
        vec![conversation_info.clone()]
    );
    assert_eq!(
        workspace
            .conversation_info(conversation_info.id().clone())
            .await
            .expect("conversation lookup"),
        conversation_info
    );
    let reopened = workspace
        .get_conversation(conversation_info.id().clone())
        .await
        .expect("conversation must reopen");
    assert_eq!(reopened.id(), conversation.id());
    drop(reopened);

    let response = conversation
        .prompt(PromptRequest::new("hello").expect("prompt request"))
        .await
        .expect("prompt must complete");
    assert_eq!(response.output(), "direct: hello");
    assert!(matches!(conversation
            .prompt(PromptRequest::new("backend-error").expect("prompt request"))
            .await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Integration)));
    assert!(matches!(conversation
            .prompt(PromptRequest::new("cancel").expect("prompt request"))
            .await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Cancelled)));
    assert!(matches!(conversation
            .prompt(PromptRequest::new("timeout").expect("prompt request"))
            .await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Timeout)));
    conversation.abort().await.expect("abort must succeed");
    let file = conversation
        .read_file(PathBuf::from("mixed\\path/file.txt"))
        .await
        .expect("file read must succeed");
    assert_eq!(file.path(), PathBuf::from("mixed\\path/file.txt"));
    assert_eq!(file.text(), Some("direct file\n"));

    let mut notifications = conversation
        .notification_stream()
        .expect("first notification receiver must be acquired");
    assert!(
        matches!(conversation.notification_stream(), Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Busy))
    );
    let notification = notifications
        .recv()
        .await
        .expect("notification item")
        .expect("notification success");
    let notification_id = notification.id().clone();
    let error = notifications
        .recv()
        .await
        .expect("notification error item")
        .expect_err("notification error must be retained");
    assert!(matches!(
        (error).kind(),
        agents_kit::error::ErrorKind::Protocol
    ));
    assert!(notifications.recv().await.is_none());
    conversation
        .resolve_notification(
            notification_id,
            NotificationResolution::Submit {
                content: serde_json::json!({"approved": true}),
            },
        )
        .await
        .expect("notification resolution must succeed");
    conversation.close().await.expect("conversation must close");
    assert!(
        workspace
            .list_conversations()
            .await
            .expect("post-close inventory")
            .is_empty()
    );

    workspace
        .close()
        .await
        .expect("workspace handle must close");
    let durable = agent
        .get_workspace(updated.id().clone())
        .await
        .expect("workspace durable record must remain reopenable");
    durable
        .close()
        .await
        .expect("reopened workspace must close");
    agent
        .delete_workspace(
            DeleteWorkspaceRequest::new(updated.id().clone(), updated.revision())
                .expect("delete request must be valid"),
        )
        .await
        .expect("workspace deletion must succeed");
    assert!(
        agent
            .list_workspaces()
            .await
            .expect("empty inventory")
            .is_empty()
    );
    agent.shutdown().await.expect("agent shutdown must succeed");

    let state = fixture.state();
    assert!(state.shutdown);
    assert_eq!(state.aborts, 1);
    assert!(state.notification_resolved);
}

#[tokio::test]
async fn workspace_revision_update_stales_live_conversations_but_preserves_reopenability() {
    let fixture = DirectHierarchyFixture::new();
    let agent = fixture.agent();
    let workspace = agent
        .new_workspace(DirectHierarchyFixture::workspace_request(
            "Revision workspace",
        ))
        .await
        .expect("revision workspace must open");
    let original = workspace.info().await.expect("original workspace info");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(Some("Revision conversation".to_owned()), None, Vec::new())
                .expect("revision conversation request must be valid"),
        )
        .await
        .expect("revision conversation must open");
    let conversation_id = conversation.id();

    let updated = agent
        .update_workspace(
            UpdateWorkspaceRequest::new(
                original.id().clone(),
                original.revision(),
                "Updated revision workspace",
                original.context().clone(),
                original.defaults().clone(),
            )
            .expect("revision update request must be valid"),
        )
        .await
        .expect("workspace revision update must succeed");
    assert!(
        matches!(conversation.info().await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Conflict))
    );

    let reopened_workspace = agent
        .get_workspace(updated.id().clone())
        .await
        .expect("updated workspace must reopen");
    let reopened_conversation = reopened_workspace
        .get_conversation(conversation_id)
        .await
        .expect("durable conversation must reopen under the new revision");
    reopened_conversation
        .close()
        .await
        .expect("reopened conversation must close");
    reopened_workspace
        .close()
        .await
        .expect("reopened workspace must close");
    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("fixture agent must shut down");
}

#[tokio::test]
async fn workspace_close_cascades_to_live_conversations_without_deleting_durable_records() {
    let fixture = DirectHierarchyFixture::new();
    let agent = fixture.agent();
    let workspace = agent
        .new_workspace(DirectHierarchyFixture::workspace_request(
            "Cascade workspace",
        ))
        .await
        .expect("cascade workspace must open");
    let workspace_id = workspace.id();
    let conversation = workspace
        .new_conversation(
            NewConversation::new(Some("Cascade conversation".to_owned()), None, Vec::new())
                .expect("cascade conversation request must be valid"),
        )
        .await
        .expect("cascade conversation must open");
    let conversation_id = conversation.id();

    workspace
        .close()
        .await
        .expect("workspace close must complete");
    assert!(
        matches!(conversation.info().await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Closed))
    );

    let reopened_workspace = agent
        .get_workspace(workspace_id)
        .await
        .expect("closed workspace durable record must reopen");
    let reopened_conversation = reopened_workspace
        .get_conversation(conversation_id)
        .await
        .expect("workspace close must preserve its durable conversation");
    reopened_conversation
        .close()
        .await
        .expect("reopened conversation must close");
    reopened_workspace
        .close()
        .await
        .expect("reopened workspace must close");
    drop(conversation);
    agent
        .shutdown()
        .await
        .expect("fixture agent must shut down");
}

#[tokio::test]
async fn agent_shutdown_cascades_closed_state_to_live_descendants() {
    let fixture = DirectHierarchyFixture::new();
    let agent = fixture.agent();
    let workspace = agent
        .new_workspace(DirectHierarchyFixture::workspace_request(
            "Shutdown workspace",
        ))
        .await
        .expect("shutdown workspace must open");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(Some("Shutdown conversation".to_owned()), None, Vec::new())
                .expect("shutdown conversation request must be valid"),
        )
        .await
        .expect("shutdown conversation must open");

    agent
        .shutdown()
        .await
        .expect("fixture agent must shut down");
    assert!(
        matches!(workspace.info().await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Closed))
    );
    assert!(
        matches!(conversation.info().await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Closed))
    );
}

// Both branches are caller-owned concrete hierarchies. Owners deliberately do
// not implement Clone; observations may outlive each independently owned handle.
enum CallerAgent {
    Primary(DirectAgent),
    Alternate(DirectAgent),
}

enum CallerWorkspace {
    Primary(DirectWorkspace),
    Alternate(DirectWorkspace),
}

enum CallerConversation {
    Primary(DirectConversation),
    Alternate(DirectConversation),
}

impl AgentBackend for CallerAgent {
    type Workspace = CallerWorkspace;

    fn id(&self) -> AgentId {
        match self {
            Self::Primary(value) => value.id(),
            Self::Alternate(value) => value.id(),
        }
    }

    async fn info(&self) -> Result<AgentInfo, KitError> {
        match self {
            Self::Primary(value) => value.info().await,
            Self::Alternate(value) => value.info().await,
        }
    }

    async fn new_workspace(&self, request: NewWorkspace) -> Result<Self::Workspace, KitError> {
        match self {
            Self::Primary(value) => value
                .new_workspace(request)
                .await
                .map(CallerWorkspace::Primary),
            Self::Alternate(value) => value
                .new_workspace(request)
                .await
                .map(CallerWorkspace::Alternate),
        }
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        match self {
            Self::Primary(value) => value.list_workspaces().await,
            Self::Alternate(value) => value.list_workspaces().await,
        }
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        match self {
            Self::Primary(value) => value.workspace_info(id).await,
            Self::Alternate(value) => value.workspace_info(id).await,
        }
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<Self::Workspace, KitError> {
        match self {
            Self::Primary(value) => value.get_workspace(id).await.map(CallerWorkspace::Primary),
            Self::Alternate(value) => value
                .get_workspace(id)
                .await
                .map(CallerWorkspace::Alternate),
        }
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_
    {
        let request = request.clone();
        async move {
            match self {
                Self::Primary(value) => value.check_workspace_update(&request).await,
                Self::Alternate(value) => value.check_workspace_update(&request).await,
            }
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        match self {
            Self::Primary(value) => value.update_workspace(request).await,
            Self::Alternate(value) => value.update_workspace(request).await,
        }
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.delete_workspace(request).await,
            Self::Alternate(value) => value.delete_workspace(request).await,
        }
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        match self {
            Self::Primary(value) => value.list_models().await,
            Self::Alternate(value) => value.list_models().await,
        }
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.ingest_knowledge(request).await,
            Self::Alternate(value) => value.ingest_knowledge(request).await,
        }
    }

    async fn shutdown(self) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.shutdown().await,
            Self::Alternate(value) => value.shutdown().await,
        }
    }
}

impl WorkspaceBackend for CallerWorkspace {
    type Conversation = CallerConversation;

    fn id(&self) -> WorkspaceId {
        match self {
            Self::Primary(value) => value.id(),
            Self::Alternate(value) => value.id(),
        }
    }

    async fn info(&self) -> Result<WorkspaceInfo, KitError> {
        match self {
            Self::Primary(value) => value.info().await,
            Self::Alternate(value) => value.info().await,
        }
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<Self::Conversation, KitError> {
        match self {
            Self::Primary(value) => value
                .new_conversation(request)
                .await
                .map(CallerConversation::Primary),
            Self::Alternate(value) => value
                .new_conversation(request)
                .await
                .map(CallerConversation::Alternate),
        }
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, KitError> {
        match self {
            Self::Primary(value) => value.list_conversations().await,
            Self::Alternate(value) => value.list_conversations().await,
        }
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, KitError> {
        match self {
            Self::Primary(value) => value.conversation_info(id).await,
            Self::Alternate(value) => value.conversation_info(id).await,
        }
    }

    async fn get_conversation(&self, id: ConversationId) -> Result<Self::Conversation, KitError> {
        match self {
            Self::Primary(value) => value
                .get_conversation(id)
                .await
                .map(CallerConversation::Primary),
            Self::Alternate(value) => value
                .get_conversation(id)
                .await
                .map(CallerConversation::Alternate),
        }
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, KitError> {
        match self {
            Self::Primary(value) => value.list_tools().await,
            Self::Alternate(value) => value.list_tools().await,
        }
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.ingest_knowledge(request).await,
            Self::Alternate(value) => value.ingest_knowledge(request).await,
        }
    }

    async fn close(self) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.close().await,
            Self::Alternate(value) => value.close().await,
        }
    }
}

impl ConversationBackend for CallerConversation {
    fn id(&self) -> ConversationId {
        match self {
            Self::Primary(value) => value.id(),
            Self::Alternate(value) => value.id(),
        }
    }

    async fn info(&self) -> Result<ConversationInfo, KitError> {
        match self {
            Self::Primary(value) => value.info().await,
            Self::Alternate(value) => value.info().await,
        }
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, KitError> {
        match self {
            Self::Primary(value) => value.prompt(request).await,
            Self::Alternate(value) => value.prompt(request).await,
        }
    }

    async fn prompt_stream(
        &self,
        request: PromptRequest,
    ) -> Result<agents_kit::prompt::PromptStream, KitError> {
        match self {
            Self::Primary(value) => value.prompt_stream(request).await,
            Self::Alternate(value) => value.prompt_stream(request).await,
        }
    }

    async fn abort(&self) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.abort().await,
            Self::Alternate(value) => value.abort().await,
        }
    }

    async fn read_file(&self, path: PathBuf) -> Result<FileRead, KitError> {
        match self {
            Self::Primary(value) => value.read_file(path).await,
            Self::Alternate(value) => value.read_file(path).await,
        }
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        match self {
            Self::Primary(value) => value.notification_stream(),
            Self::Alternate(value) => value.notification_stream(),
        }
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.resolve_notification(id, resolution).await,
            Self::Alternate(value) => value.resolve_notification(id, resolution).await,
        }
    }

    async fn close(self) -> Result<(), KitError> {
        match self {
            Self::Primary(value) => value.close().await,
            Self::Alternate(value) => value.close().await,
        }
    }
}

#[tokio::test]
async fn caller_enum_arms_preserve_exact_associated_children_and_request_values() {
    for alternate in [false, true] {
        let fixture = DirectHierarchyFixture::new();
        let agent = if alternate {
            CallerAgent::Alternate(fixture.agent())
        } else {
            CallerAgent::Primary(fixture.agent())
        };
        assert_eq!(agent.id(), agent.info().await.expect("info").id().clone());
        assert!(agent.list_workspaces().await.expect("inventory").is_empty());
        let workspace = agent
            .new_workspace(DirectHierarchyFixture::workspace_request("enum workspace"))
            .await
            .expect("workspace");
        assert_eq!(
            matches!(workspace, CallerWorkspace::Alternate(_)),
            alternate
        );
        let info = workspace.info().await.expect("info");
        assert_eq!(info.name(), "enum workspace");
        assert_eq!(
            agent.workspace_info(workspace.id()).await.expect("lookup"),
            info
        );
        assert_eq!(
            agent.list_workspaces().await.expect("inventory"),
            vec![info.clone()]
        );
        assert_eq!(
            agent.get_workspace(workspace.id()).await.expect("get").id(),
            workspace.id()
        );
        assert_eq!(
            agent.list_models().await.expect("models")[0]
                .selector()
                .as_str(),
            "direct-model"
        );
        assert_eq!(
            workspace.list_tools().await.expect("tools")[0].name(),
            "direct.lookup"
        );
        let knowledge = IngestKnowledgeRequest::new(vec![
            agents_kit::knowledge::KnowledgeSource::text(
                "enum-source",
                "Enum source",
                "Retained caller knowledge",
            )
            .expect("source"),
        ])
        .expect("knowledge request");
        agent
            .ingest_knowledge(knowledge.clone())
            .await
            .expect("agent knowledge");
        workspace
            .ingest_knowledge(knowledge.clone())
            .await
            .expect("workspace knowledge");
        assert_eq!(
            fixture.state().agent_knowledge[0].sources(),
            knowledge.sources()
        );
        assert_eq!(
            fixture.state().workspace_knowledge[0].sources(),
            knowledge.sources()
        );
        let conversation = workspace
            .new_conversation(
                NewConversation::new(Some("enum conversation".into()), None, Vec::new())
                    .expect("request"),
            )
            .await
            .expect("conversation");
        assert_eq!(
            matches!(conversation, CallerConversation::Alternate(_)),
            alternate
        );
        let ci = conversation.info().await.expect("info");
        assert_eq!(
            workspace
                .conversation_info(conversation.id())
                .await
                .expect("lookup"),
            ci
        );
        assert_eq!(
            workspace.list_conversations().await.expect("list"),
            vec![ci]
        );
        assert_eq!(
            workspace
                .get_conversation(conversation.id())
                .await
                .expect("get")
                .id(),
            conversation.id()
        );
        assert_eq!(
            conversation
                .prompt(PromptRequest::new("enum input").expect("request"))
                .await
                .expect("response")
                .output(),
            "direct: enum input"
        );
        assert!(
            matches!(conversation.prompt_stream(PromptRequest::new("stream").expect("request")).await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Unsupported))
        );
        assert!(
            matches!(conversation.prompt(PromptRequest::new("backend-error").expect("request")).await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Integration))
        );
        assert_eq!(
            conversation
                .read_file(PathBuf::from("enum/input"))
                .await
                .expect("file")
                .path(),
            PathBuf::from("enum/input")
        );
        conversation.abort().await.expect("abort");
        let mut notifications = conversation.notification_stream().expect("notifications");
        let notification = notifications
            .recv()
            .await
            .expect("item")
            .expect("notification");
        conversation
            .resolve_notification(
                notification.id().clone(),
                NotificationResolution::Submit {
                    content: serde_json::json!({"approved": true}),
                },
            )
            .await
            .expect("resolution");
        assert!(notifications.recv().await.expect("error item").is_err());
        assert!(notifications.recv().await.is_none());
        conversation.close().await.expect("close conversation");
        workspace.close().await.expect("close workspace");
        let update = UpdateWorkspaceRequest::new(
            info.id().clone(),
            info.revision(),
            "updated enum",
            info.context().clone(),
            info.defaults().clone(),
        )
        .expect("update");
        assert_eq!(
            agent
                .check_workspace_update(&update)
                .await
                .expect("readiness"),
            WorkspaceUpdateReadiness::Ready
        );
        let updated = agent.update_workspace(update).await.expect("update");
        agent
            .delete_workspace(
                DeleteWorkspaceRequest::new(updated.id().clone(), updated.revision())
                    .expect("delete"),
            )
            .await
            .expect("delete");
        agent.shutdown().await.expect("shutdown");
        assert!(fixture.state().shutdown);
        assert_eq!(fixture.state().aborts, 1);
    }
}
