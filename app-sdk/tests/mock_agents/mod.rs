//! A caller-owned Agent hierarchy. Only observation records are shared.
use agent_sdk::conversation::{ConversationInfo, FileRead, NewConversation};
use agent_sdk::identity::{AgentId, AgentInfo, ConversationId, NotificationId, WorkspaceId};
use agent_sdk::knowledge::IngestKnowledgeRequest;
use agent_sdk::model::ModelInfo;
use agent_sdk::notification::{ConversationNotificationReceiver, NotificationResolution};
use agent_sdk::prompt::{PromptRequest, PromptResponse};
use agent_sdk::tool::ToolInfo;
use agent_sdk::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceInfo,
    WorkspaceUpdateReadiness,
};
use agent_sdk::{AgentBackend, ConversationBackend, SdkError, WorkspaceBackend};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct Peer {
    id: AgentId,
    workspace_id: WorkspaceId,
    workspace: Mutex<Option<WorkspaceInfo>>,
    conversations: Mutex<BTreeMap<String, ConversationInfo>>,
    closed_conversations: Mutex<BTreeSet<String>>,
    conversation_requests: Mutex<Vec<NewConversation>>,
    closed: AtomicBool,
    replies: Mutex<VecDeque<(String, PromptPlan)>>,
    prompts: Mutex<Vec<String>>,
    creations: AtomicUsize,
    closes: AtomicUsize,
    shutdowns: AtomicUsize,
    rejected_updates: AtomicUsize,
}
enum PromptPlan {
    Reply(String),
    Uncertain,
}
pub struct CallerAgent {
    peer: Arc<Peer>,
}
pub struct CallerWorkspace {
    peer: Arc<Peer>,
    closed: Arc<AtomicBool>,
    opened_revision: u64,
}
pub struct CallerConversation {
    peer: Arc<Peer>,
    id: ConversationId,
    workspace_closed: Arc<AtomicBool>,
    opened_revision: u64,
}
pub enum AgentChoice {
    First(CallerAgent),
    Second(CallerAgent),
}
pub enum WorkspaceChoice {
    First(CallerWorkspace),
    Second(CallerWorkspace),
}
pub enum ConversationChoice {
    First(CallerConversation),
    Second(CallerConversation),
}
impl Peer {
    pub fn new(suffix: u64) -> Arc<Self> {
        Arc::new(Self {
            id: format!("agent_{suffix:032x}").parse().unwrap(),
            workspace_id: format!("workspace_{suffix:032x}").parse().unwrap(),
            workspace: Mutex::new(None),
            conversations: Mutex::new(BTreeMap::new()),
            closed_conversations: Mutex::new(BTreeSet::new()),
            conversation_requests: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            replies: Mutex::new(VecDeque::new()),
            prompts: Mutex::new(Vec::new()),
            creations: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            shutdowns: AtomicUsize::new(0),
            rejected_updates: AtomicUsize::new(0),
        })
    }
    pub fn agent(self: &Arc<Self>) -> CallerAgent {
        CallerAgent {
            peer: Arc::clone(self),
        }
    }
    pub fn expect_prompt(&self, text: &str, reply: &str) {
        self.replies
            .lock()
            .unwrap()
            .push_back((text.into(), PromptPlan::Reply(reply.into())));
    }
    pub fn expect_uncertain_prompt(&self, text: &str) {
        self.replies
            .lock()
            .unwrap()
            .push_back((text.into(), PromptPlan::Uncertain));
    }
    pub fn prompts(&self) -> Vec<String> {
        self.prompts.lock().unwrap().clone()
    }
    pub fn creations(&self) -> usize {
        self.creations.load(Ordering::SeqCst)
    }
    pub fn closes(&self) -> usize {
        self.closes.load(Ordering::SeqCst)
    }
    pub fn reject_next_update(&self) {
        self.rejected_updates.store(1, Ordering::SeqCst);
    }
    pub fn workspace_observation(&self) -> WorkspaceInfo {
        self.info().unwrap()
    }
    pub fn shutdowns(&self) -> usize {
        self.shutdowns.load(Ordering::SeqCst)
    }
    pub fn assert_drained(&self) {
        assert!(
            self.replies.lock().unwrap().is_empty(),
            "unconsumed required prompt"
        );
    }
    pub fn conversation_requests(&self) -> Vec<NewConversation> {
        self.conversation_requests.lock().unwrap().clone()
    }
    fn ensure_open(&self) -> Result<(), SdkError> {
        if self.closed.load(Ordering::SeqCst) {
            Err(SdkError::closed("mock agent closed"))
        } else {
            Ok(())
        }
    }
    fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        self.ensure_open()?;
        self.workspace
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| SdkError::not_found("mock workspace absent"))
    }
}
impl AgentBackend for CallerAgent {
    type Workspace = CallerWorkspace;
    fn id(&self) -> AgentId {
        self.peer.id.clone()
    }
    async fn info(&self) -> Result<AgentInfo, SdkError> {
        self.peer.ensure_open()?;
        AgentInfo::new(self.id(), Some("caller mock".into()))
    }
    async fn new_workspace(&self, r: NewWorkspace) -> Result<Self::Workspace, SdkError> {
        self.peer.ensure_open()?;
        let mut current = self.peer.workspace.lock().unwrap();
        if current.is_some() {
            return Err(SdkError::conflict("mock already materialized"));
        }
        *current = Some(WorkspaceInfo::new(
            self.peer.workspace_id.clone(),
            r.name().into(),
            r.context().clone(),
            r.defaults().clone(),
            1,
        )?);
        self.peer.creations.fetch_add(1, Ordering::SeqCst);
        Ok(CallerWorkspace {
            peer: Arc::clone(&self.peer),
            closed: Arc::new(AtomicBool::new(false)),
            opened_revision: 1,
        })
    }
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, SdkError> {
        self.peer.ensure_open()?;
        Ok(self
            .peer
            .workspace
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect())
    }
    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, SdkError> {
        if id != self.peer.workspace_id {
            return Err(SdkError::not_found("wrong workspace"));
        }
        self.peer.info()
    }
    async fn get_workspace(&self, id: WorkspaceId) -> Result<Self::Workspace, SdkError> {
        let info = self.workspace_info(id).await?;
        Ok(CallerWorkspace {
            peer: Arc::clone(&self.peer),
            closed: Arc::new(AtomicBool::new(false)),
            opened_revision: info.revision(),
        })
    }
    fn check_workspace_update(
        &self,
        r: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, SdkError>> + Send + '_
    {
        let id = r.id().clone();
        let revision = r.expected_revision();
        async move {
            let info = self.workspace_info(id).await?;
            if info.revision() != revision {
                return Err(SdkError::conflict("stale preflight"));
            }
            Ok(WorkspaceUpdateReadiness::Ready)
        }
    }
    async fn update_workspace(&self, r: UpdateWorkspaceRequest) -> Result<WorkspaceInfo, SdkError> {
        self.peer.ensure_open()?;
        if self.peer.rejected_updates.swap(0, Ordering::SeqCst) > 0 {
            return Err(SdkError::conflict("injected lower update rejection"));
        }
        let mut current = self.peer.workspace.lock().unwrap();
        let info = current
            .as_ref()
            .ok_or_else(|| SdkError::not_found("workspace"))?;
        if info.id() != r.id() || info.revision() != r.expected_revision() {
            return Err(SdkError::conflict("stale workspace"));
        }
        let replacement = WorkspaceInfo::new(
            r.id().clone(),
            r.name().into(),
            r.context().clone(),
            r.defaults().clone(),
            info.revision() + 1,
        )?;
        *current = Some(replacement.clone());
        Ok(replacement)
    }
    async fn delete_workspace(&self, r: DeleteWorkspaceRequest) -> Result<(), SdkError> {
        self.peer.ensure_open()?;
        let mut current = self.peer.workspace.lock().unwrap();
        let info = current
            .as_ref()
            .ok_or_else(|| SdkError::not_found("workspace"))?;
        if info.id() != r.id() || info.revision() != r.expected_revision() {
            return Err(SdkError::conflict("stale delete"));
        }
        *current = None;
        Ok(())
    }
    async fn list_models(&self) -> Result<Vec<ModelInfo>, SdkError> {
        self.peer.ensure_open()?;
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _: IngestKnowledgeRequest) -> Result<(), SdkError> {
        self.peer.ensure_open()?;
        Err(SdkError::unsupported("mock knowledge"))
    }
    async fn shutdown(self) -> Result<(), SdkError> {
        if !self.peer.closed.swap(true, Ordering::SeqCst) {
            self.peer.shutdowns.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}
impl CallerWorkspace {
    fn ensure_open(&self) -> Result<(), SdkError> {
        let info = self.peer.info()?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(SdkError::closed("mock workspace handle closed"));
        }
        if info.revision() != self.opened_revision {
            return Err(SdkError::conflict("mock workspace revision stale"));
        }
        Ok(())
    }
}
impl CallerConversation {
    fn ensure_open(&self) -> Result<(), SdkError> {
        let info = self.peer.info()?;
        if self.workspace_closed.load(Ordering::SeqCst)
            || self
                .peer
                .closed_conversations
                .lock()
                .unwrap()
                .contains(&self.id.to_string())
        {
            return Err(SdkError::closed("mock conversation closed"));
        }
        if info.revision() != self.opened_revision {
            return Err(SdkError::conflict("mock conversation revision stale"));
        }
        Ok(())
    }
}
impl WorkspaceBackend for CallerWorkspace {
    type Conversation = CallerConversation;
    fn id(&self) -> WorkspaceId {
        self.peer.workspace_id.clone()
    }
    async fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        self.ensure_open()?;
        self.peer.info()
    }
    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<Self::Conversation, SdkError> {
        self.ensure_open()?;
        self.peer
            .conversation_requests
            .lock()
            .unwrap()
            .push(request.clone());
        let mut conversations = self.peer.conversations.lock().unwrap();
        let id: ConversationId =
            format!("conversation_{:032x}", conversations.len() + 1).parse()?;
        let info =
            ConversationInfo::new(id.clone(), self.id(), request.title().map(str::to_owned))?;
        conversations.insert(id.to_string(), info);
        Ok(CallerConversation {
            peer: Arc::clone(&self.peer),
            id,
            workspace_closed: Arc::clone(&self.closed),
            opened_revision: self.opened_revision,
        })
    }
    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, SdkError> {
        self.ensure_open()?;
        let closed = self.peer.closed_conversations.lock().unwrap();
        Ok(self
            .peer
            .conversations
            .lock()
            .unwrap()
            .values()
            .filter(|info| !closed.contains(&info.id().to_string()))
            .cloned()
            .collect())
    }
    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, SdkError> {
        self.ensure_open()?;
        if self
            .peer
            .closed_conversations
            .lock()
            .unwrap()
            .contains(&id.to_string())
        {
            return Err(SdkError::closed("mock conversation permanently closed"));
        }
        self.peer
            .conversations
            .lock()
            .unwrap()
            .get(&id.to_string())
            .cloned()
            .ok_or_else(|| SdkError::not_found("mock conversation"))
    }
    async fn get_conversation(&self, id: ConversationId) -> Result<Self::Conversation, SdkError> {
        self.conversation_info(id.clone()).await?;
        Ok(CallerConversation {
            peer: Arc::clone(&self.peer),
            id,
            workspace_closed: Arc::clone(&self.closed),
            opened_revision: self.opened_revision,
        })
    }
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, SdkError> {
        self.ensure_open()?;
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _: IngestKnowledgeRequest) -> Result<(), SdkError> {
        self.ensure_open()?;
        Err(SdkError::unsupported("mock knowledge"))
    }
    async fn close(self) -> Result<(), SdkError> {
        self.ensure_open()?;
        self.closed.store(true, Ordering::SeqCst);
        self.peer.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl ConversationBackend for CallerConversation {
    fn id(&self) -> ConversationId {
        self.id.clone()
    }
    async fn info(&self) -> Result<ConversationInfo, SdkError> {
        self.ensure_open()?;
        self.peer
            .conversations
            .lock()
            .unwrap()
            .get(&self.id.to_string())
            .cloned()
            .ok_or_else(|| SdkError::not_found("mock conversation"))
    }
    async fn prompt(&self, r: PromptRequest) -> Result<PromptResponse, SdkError> {
        self.ensure_open()?;
        self.peer.prompts.lock().unwrap().push(r.text().to_owned());
        let (expected, reply) = self
            .peer
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| SdkError::conflict("unexpected prompt: script exhausted"))?;
        if r.text() != expected {
            return Err(SdkError::conflict("wrong prompt arguments"));
        }
        match reply {
            PromptPlan::Reply(text) => Ok(PromptResponse::new(text, None)),
            PromptPlan::Uncertain => Err(SdkError::integration("accepted mock prompt lost reply")),
        }
    }
    async fn abort(&self) -> Result<(), SdkError> {
        self.ensure_open()?;
        Ok(())
    }
    async fn read_file(&self, _: PathBuf) -> Result<FileRead, SdkError> {
        self.ensure_open()?;
        Err(SdkError::not_found("mock file absent"))
    }
    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, SdkError> {
        self.ensure_open()?;
        Ok(ConversationNotificationReceiver::from_stream(
            futures_util::stream::pending(),
        ))
    }
    async fn resolve_notification(
        &self,
        _: NotificationId,
        _: NotificationResolution,
    ) -> Result<(), SdkError> {
        self.ensure_open()?;
        Err(SdkError::not_found("mock notification absent"))
    }
    async fn close(self) -> Result<(), SdkError> {
        self.ensure_open()?;
        self.peer
            .closed_conversations
            .lock()
            .unwrap()
            .insert(self.id.to_string());
        self.peer.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl AgentBackend for AgentChoice {
    type Workspace = WorkspaceChoice;
    fn id(&self) -> AgentId {
        match self {
            Self::First(a) => a.id(),
            Self::Second(a) => a.id(),
        }
    }
    async fn info(&self) -> Result<AgentInfo, SdkError> {
        match self {
            Self::First(a) => a.info().await,
            Self::Second(a) => a.info().await,
        }
    }
    async fn new_workspace(&self, r: NewWorkspace) -> Result<Self::Workspace, SdkError> {
        match self {
            Self::First(a) => a.new_workspace(r).await.map(WorkspaceChoice::First),
            Self::Second(a) => a.new_workspace(r).await.map(WorkspaceChoice::Second),
        }
    }
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, SdkError> {
        match self {
            Self::First(a) => a.list_workspaces().await,
            Self::Second(a) => a.list_workspaces().await,
        }
    }
    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, SdkError> {
        match self {
            Self::First(a) => a.workspace_info(id).await,
            Self::Second(a) => a.workspace_info(id).await,
        }
    }
    async fn get_workspace(&self, id: WorkspaceId) -> Result<Self::Workspace, SdkError> {
        match self {
            Self::First(a) => a.get_workspace(id).await.map(WorkspaceChoice::First),
            Self::Second(a) => a.get_workspace(id).await.map(WorkspaceChoice::Second),
        }
    }
    fn check_workspace_update(
        &self,
        r: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, SdkError>> + Send + '_
    {
        match self {
            Self::First(a) => futures_util::future::Either::Left(a.check_workspace_update(r)),
            Self::Second(a) => futures_util::future::Either::Right(a.check_workspace_update(r)),
        }
    }
    async fn update_workspace(&self, r: UpdateWorkspaceRequest) -> Result<WorkspaceInfo, SdkError> {
        match self {
            Self::First(a) => a.update_workspace(r).await,
            Self::Second(a) => a.update_workspace(r).await,
        }
    }
    async fn delete_workspace(&self, r: DeleteWorkspaceRequest) -> Result<(), SdkError> {
        match self {
            Self::First(a) => a.delete_workspace(r).await,
            Self::Second(a) => a.delete_workspace(r).await,
        }
    }
    async fn list_models(&self) -> Result<Vec<ModelInfo>, SdkError> {
        match self {
            Self::First(a) => a.list_models().await,
            Self::Second(a) => a.list_models().await,
        }
    }
    async fn ingest_knowledge(&self, r: IngestKnowledgeRequest) -> Result<(), SdkError> {
        match self {
            Self::First(a) => a.ingest_knowledge(r).await,
            Self::Second(a) => a.ingest_knowledge(r).await,
        }
    }
    async fn shutdown(self) -> Result<(), SdkError> {
        match self {
            Self::First(a) => a.shutdown().await,
            Self::Second(a) => a.shutdown().await,
        }
    }
}
impl WorkspaceBackend for WorkspaceChoice {
    type Conversation = ConversationChoice;
    fn id(&self) -> WorkspaceId {
        match self {
            Self::First(w) => w.id(),
            Self::Second(w) => w.id(),
        }
    }
    async fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        match self {
            Self::First(w) => w.info().await,
            Self::Second(w) => w.info().await,
        }
    }
    async fn new_conversation(&self, r: NewConversation) -> Result<Self::Conversation, SdkError> {
        match self {
            Self::First(w) => w.new_conversation(r).await.map(ConversationChoice::First),
            Self::Second(w) => w.new_conversation(r).await.map(ConversationChoice::Second),
        }
    }
    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, SdkError> {
        match self {
            Self::First(w) => w.list_conversations().await,
            Self::Second(w) => w.list_conversations().await,
        }
    }
    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, SdkError> {
        match self {
            Self::First(w) => w.conversation_info(id).await,
            Self::Second(w) => w.conversation_info(id).await,
        }
    }
    async fn get_conversation(&self, id: ConversationId) -> Result<Self::Conversation, SdkError> {
        match self {
            Self::First(w) => w.get_conversation(id).await.map(ConversationChoice::First),
            Self::Second(w) => w.get_conversation(id).await.map(ConversationChoice::Second),
        }
    }
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, SdkError> {
        match self {
            Self::First(w) => w.list_tools().await,
            Self::Second(w) => w.list_tools().await,
        }
    }
    async fn ingest_knowledge(&self, r: IngestKnowledgeRequest) -> Result<(), SdkError> {
        match self {
            Self::First(w) => w.ingest_knowledge(r).await,
            Self::Second(w) => w.ingest_knowledge(r).await,
        }
    }
    async fn close(self) -> Result<(), SdkError> {
        match self {
            Self::First(w) => w.close().await,
            Self::Second(w) => w.close().await,
        }
    }
}
impl ConversationBackend for ConversationChoice {
    fn id(&self) -> ConversationId {
        match self {
            Self::First(c) => c.id(),
            Self::Second(c) => c.id(),
        }
    }
    async fn info(&self) -> Result<ConversationInfo, SdkError> {
        match self {
            Self::First(c) => c.info().await,
            Self::Second(c) => c.info().await,
        }
    }
    async fn prompt(&self, r: PromptRequest) -> Result<PromptResponse, SdkError> {
        match self {
            Self::First(c) => c.prompt(r).await,
            Self::Second(c) => c.prompt(r).await,
        }
    }
    async fn abort(&self) -> Result<(), SdkError> {
        match self {
            Self::First(c) => c.abort().await,
            Self::Second(c) => c.abort().await,
        }
    }
    async fn read_file(&self, p: PathBuf) -> Result<FileRead, SdkError> {
        match self {
            Self::First(c) => c.read_file(p).await,
            Self::Second(c) => c.read_file(p).await,
        }
    }
    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, SdkError> {
        match self {
            Self::First(c) => c.notification_stream(),
            Self::Second(c) => c.notification_stream(),
        }
    }
    async fn resolve_notification(
        &self,
        id: NotificationId,
        r: NotificationResolution,
    ) -> Result<(), SdkError> {
        match self {
            Self::First(c) => c.resolve_notification(id, r).await,
            Self::Second(c) => c.resolve_notification(id, r).await,
        }
    }
    async fn close(self) -> Result<(), SdkError> {
        match self {
            Self::First(c) => c.close().await,
            Self::Second(c) => c.close().await,
        }
    }
}
