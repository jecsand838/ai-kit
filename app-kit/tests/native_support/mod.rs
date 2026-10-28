//! Ordinary test-local direct Agent fixture shared by native result cases.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::conversation::{ConversationInfo, FileRead, NewConversation};
use agents_kit::identity::{AgentId, AgentInfo, ConversationId, NotificationId, WorkspaceId};
use agents_kit::knowledge::IngestKnowledgeRequest;
use agents_kit::model::ModelInfo;
use agents_kit::notification::{ConversationNotificationReceiver, NotificationResolution};
use agents_kit::prompt::{PromptEvent, PromptRequest, PromptResponse, PromptStream};
use agents_kit::tool::ToolInfo;
use agents_kit::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceInfo,
    WorkspaceUpdateReadiness,
};
use agents_kit::{AgentBackend, ConversationBackend, KitError, WorkspaceBackend};
use app_kit::workflow::{
    JsonState, LifecycleHooks, NoActions, NoConditions, NoHooks, StateTyping, Workflow,
    WorkflowDefinition,
};
use app_kit::{AgentSpec, App, AppConfig, Shutdown, WorkspaceConfig};
use futures_util::StreamExt;
use serde_json::{Value, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use tokio::sync::{Notify, Semaphore};

pub struct PromptPeer {
    workspace: Mutex<Option<WorkspaceInfo>>,
    conversations: Mutex<BTreeMap<ConversationId, ConversationInfo>>,
    response: PromptResponse,
    pub terminal: Arc<Semaphore>,
    pub hold_stream_return: AtomicBool,
    pub stream_return_entered: Notify,
    pub stream_return: Semaphore,
    pub ordinary_calls: AtomicUsize,
    pub streaming_calls: AtomicUsize,
    pub fail_next_prompt: AtomicBool,
    stream: Mutex<NativeStream>,
    pub accepted_streams: AtomicUsize,
    pub produced_deltas: AtomicUsize,
    pub stream_drops: AtomicUsize,
    pub aborts: AtomicUsize,
    shutdowns: AtomicUsize,
}

#[derive(Clone, Copy)]
pub enum NativeStream {
    Terminal,
    Unsupported,
    AcceptedError,
    AcceptedUnsupported,
    AcceptedInvalidInput,
    AcceptedClosed,
    AcceptedCancelled,
    Eof,
    ManyDeltas { count: usize },
}

struct StreamObservation(Arc<PromptPeer>);

impl Drop for StreamObservation {
    fn drop(&mut self) {
        self.0.stream_drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct NativeAgent(Arc<PromptPeer>);
pub struct NativeWorkspace(Arc<PromptPeer>);
pub struct NativeConversation {
    peer: Arc<PromptPeer>,
    id: ConversationId,
}

impl PromptPeer {
    fn new(response: PromptResponse) -> Arc<Self> {
        Arc::new(Self {
            workspace: Mutex::new(None),
            conversations: Mutex::new(BTreeMap::new()),
            response,
            terminal: Arc::new(Semaphore::new(0)),
            hold_stream_return: AtomicBool::new(false),
            stream_return_entered: Notify::new(),
            stream_return: Semaphore::new(0),
            ordinary_calls: AtomicUsize::new(0),
            streaming_calls: AtomicUsize::new(0),
            fail_next_prompt: AtomicBool::new(false),
            stream: Mutex::new(NativeStream::Terminal),
            accepted_streams: AtomicUsize::new(0),
            produced_deltas: AtomicUsize::new(0),
            stream_drops: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            shutdowns: AtomicUsize::new(0),
        })
    }
    pub fn configure_stream(&self, stream: NativeStream) {
        *self.stream.lock().unwrap() = stream;
    }
    fn workspace_id(&self) -> WorkspaceId {
        format!("workspace_{:032x}", 0xe01_u128).parse().unwrap()
    }
    fn workspace_info(&self) -> Result<WorkspaceInfo, KitError> {
        self.workspace
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| KitError::not_found("fixture workspace"))
    }
    fn conversation_info(&self, id: &ConversationId) -> Result<ConversationInfo, KitError> {
        self.conversations
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| KitError::not_found("fixture conversation"))
    }
}

impl AgentBackend for NativeAgent {
    type Workspace = NativeWorkspace;
    fn id(&self) -> AgentId {
        format!("agent_{:032x}", 0xe01_u128).parse().unwrap()
    }
    async fn info(&self) -> Result<AgentInfo, KitError> {
        AgentInfo::new(self.id(), None)
    }
    async fn new_workspace(&self, request: NewWorkspace) -> Result<Self::Workspace, KitError> {
        let mut current = self.0.workspace.lock().unwrap();
        if current.is_some() {
            return Err(KitError::conflict("fixture workspace exists"));
        }
        *current = Some(WorkspaceInfo::new(
            self.0.workspace_id(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            1,
        )?);
        Ok(NativeWorkspace(Arc::clone(&self.0)))
    }
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        Ok(self.0.workspace.lock().unwrap().iter().cloned().collect())
    }
    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        if id != self.0.workspace_id() {
            return Err(KitError::not_found("fixture workspace id"));
        }
        self.0.workspace_info()
    }
    async fn get_workspace(&self, id: WorkspaceId) -> Result<Self::Workspace, KitError> {
        self.workspace_info(id).await?;
        Ok(NativeWorkspace(Arc::clone(&self.0)))
    }
    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_
    {
        let id = request.id().clone();
        let revision = request.expected_revision();
        async move {
            if self.workspace_info(id).await?.revision() != revision {
                return Err(KitError::conflict("fixture workspace fence"));
            }
            Ok(WorkspaceUpdateReadiness::Ready)
        }
    }
    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        self.check_workspace_update(&request).await?;
        let info = WorkspaceInfo::new(
            request.id().clone(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            request.expected_revision() + 1,
        )?;
        *self.0.workspace.lock().unwrap() = Some(info.clone());
        Ok(info)
    }
    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        let mut current = self.0.workspace.lock().unwrap();
        let info = current
            .as_ref()
            .ok_or_else(|| KitError::not_found("fixture workspace"))?;
        if info.id() != request.id() || info.revision() != request.expected_revision() {
            return Err(KitError::conflict("fixture delete fence"));
        }
        *current = None;
        Ok(())
    }
    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _: IngestKnowledgeRequest) -> Result<(), KitError> {
        Err(KitError::unsupported("unused fixture knowledge"))
    }
    async fn shutdown(self) -> Result<(), KitError> {
        self.0.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl WorkspaceBackend for NativeWorkspace {
    type Conversation = NativeConversation;
    fn id(&self) -> WorkspaceId {
        self.0.workspace_id()
    }
    async fn info(&self) -> Result<WorkspaceInfo, KitError> {
        self.0.workspace_info()
    }
    async fn new_conversation(&self, _: NewConversation) -> Result<Self::Conversation, KitError> {
        let mut conversations = self.0.conversations.lock().unwrap();
        let id: ConversationId =
            format!("conversation_{:032x}", 0xe10 + conversations.len()).parse()?;
        conversations.insert(
            id.clone(),
            ConversationInfo::new(id.clone(), self.id(), None)?,
        );
        Ok(NativeConversation {
            peer: Arc::clone(&self.0),
            id,
        })
    }
    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, KitError> {
        Ok(self
            .0
            .conversations
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect())
    }
    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, KitError> {
        self.0.conversation_info(&id)
    }
    async fn get_conversation(&self, id: ConversationId) -> Result<Self::Conversation, KitError> {
        self.0.conversation_info(&id)?;
        Ok(NativeConversation {
            peer: Arc::clone(&self.0),
            id,
        })
    }
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, KitError> {
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _: IngestKnowledgeRequest) -> Result<(), KitError> {
        Err(KitError::unsupported("unused fixture knowledge"))
    }
    async fn close(self) -> Result<(), KitError> {
        Ok(())
    }
}

impl ConversationBackend for NativeConversation {
    fn id(&self) -> ConversationId {
        self.id.clone()
    }
    async fn info(&self) -> Result<ConversationInfo, KitError> {
        self.peer.conversation_info(&self.id)
    }
    async fn prompt(&self, _: PromptRequest) -> Result<PromptResponse, KitError> {
        self.peer.ordinary_calls.fetch_add(1, Ordering::SeqCst);
        if self.peer.fail_next_prompt.swap(false, Ordering::SeqCst) {
            return Err(KitError::integration(
                "accepted fixture prompt lost its receipt",
            ));
        }
        Ok(self.peer.response.clone())
    }
    async fn prompt_stream(&self, _: PromptRequest) -> Result<PromptStream, KitError> {
        self.peer.streaming_calls.fetch_add(1, Ordering::SeqCst);
        let behavior = *self.peer.stream.lock().unwrap();
        if matches!(behavior, NativeStream::Unsupported) {
            return Err(KitError::unsupported("private unsupported stream detail"));
        }
        self.peer.accepted_streams.fetch_add(1, Ordering::SeqCst);
        let response = self.peer.response.clone();
        let terminal = Arc::clone(&self.peer.terminal);
        let peer = Arc::clone(&self.peer);
        let observation = StreamObservation(Arc::clone(&self.peer));
        let count = match behavior {
            NativeStream::ManyDeltas { count } => count,
            _ => 2,
        };
        // The already accepted fixture result is fixed. The gate delays only its
        // public observation; dropping observation does not cancel accepted work.
        // The lease exists before any source poll, including the terminal poll.
        let deltas = futures_util::stream::iter(0..count).map(move |index| {
            peer.produced_deltas.fetch_add(1, Ordering::SeqCst);
            let text = match behavior {
                NativeStream::ManyDeltas { .. } => format!("delta-{index}"),
                _ => ["draft ", "not the final answer"][index].to_owned(),
            };
            Ok(PromptEvent::OutputDelta { text })
        });
        let stream = PromptStream::from_stream(
            deltas.chain(
                futures_util::stream::once(async move {
                    let _observation = observation;
                    terminal
                        .acquire()
                        .await
                        .expect("test owns terminal gate")
                        .forget();
                    match behavior {
                        NativeStream::AcceptedError => Some(Err(KitError::integration(
                            "private accepted stream receipt loss",
                        ))),
                        NativeStream::AcceptedUnsupported => Some(Err(KitError::unsupported(
                            "private accepted stream unsupported item",
                        ))),
                        NativeStream::AcceptedInvalidInput => Some(Err(KitError::invalid_input(
                            "private accepted stream invalid input item",
                        ))),
                        NativeStream::AcceptedClosed => {
                            Some(Err(KitError::closed("private accepted stream closed item")))
                        }
                        NativeStream::AcceptedCancelled => Some(Err(KitError::cancelled(
                            "private accepted stream cancelled item",
                        ))),
                        NativeStream::Eof => None,
                        NativeStream::Terminal | NativeStream::ManyDeltas { .. } => {
                            Some(Ok(PromptEvent::Completed(response)))
                        }
                        NativeStream::Unsupported => unreachable!("rejected before acceptance"),
                    }
                })
                .filter_map(futures_util::future::ready),
            ),
        );
        if self.peer.hold_stream_return.swap(false, Ordering::SeqCst) {
            // The actual source and its observation lease already exist. This
            // gate delays the accepted call's return, never a native writer.
            self.peer.stream_return_entered.notify_one();
            self.peer
                .stream_return
                .acquire()
                .await
                .expect("test owns stream return gate")
                .forget();
        }
        Ok(stream)
    }
    async fn abort(&self) -> Result<(), KitError> {
        self.peer.aborts.fetch_add(1, Ordering::SeqCst);
        self.peer.terminal.add_permits(1);
        Ok(())
    }
    async fn read_file(&self, _: PathBuf) -> Result<FileRead, KitError> {
        Err(KitError::not_found("fixture file"))
    }
    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        Ok(ConversationNotificationReceiver::from_stream(
            futures_util::stream::pending(),
        ))
    }
    async fn resolve_notification(
        &self,
        _: NotificationId,
        _: NotificationResolution,
    ) -> Result<(), KitError> {
        Err(KitError::not_found("fixture notification"))
    }
    async fn close(self) -> Result<(), KitError> {
        Ok(())
    }
}

pub struct NativeCase<H: LifecycleHooks = NoHooks, T: StateTyping = JsonState> {
    pub app: App<StateStore, NativeAgent, NoActions, NoConditions, T, H>,
    pub workflow: Workflow<StateStore, NativeAgent, NoActions, NoConditions, T, H>,
    pub peer: Arc<PromptPeer>,
    supplied_agent: bool,
}

impl NativeCase {
    pub fn definition(
        id: &str,
        state: Value,
        nodes: Vec<Value>,
        routes: Value,
        reducers: Value,
    ) -> WorkflowDefinition {
        WorkflowDefinition::parse(&json!({
            "schema":"app-kit.workflow/v2", "id":id, "revision":"r1", "state":state,
            "required":[], "reducers":reducers, "entry":nodes[0]["id"], "nodes":nodes, "routes":routes,
        }).to_string()).expect("native workflow syntax")
    }
    pub fn linear(nodes: &[Value]) -> Value {
        Value::Object(nodes.windows(2).map(|pair| (pair[0]["id"].as_str().unwrap().to_owned(), json!({
            "mode":"first", "branches":[{"target":{"kind":"node","id":pair[1]["id"]}}]
        }))).collect())
    }
    pub async fn open(
        definitions: Vec<WorkflowDefinition>,
        supplied_agent: bool,
        response: PromptResponse,
    ) -> Self {
        Self::open_with_hooks(definitions, supplied_agent, response, NoHooks).await
    }
    pub async fn open_with_hooks<H: LifecycleHooks>(
        definitions: Vec<WorkflowDefinition>,
        supplied_agent: bool,
        response: PromptResponse,
        hooks: H,
    ) -> NativeCase<H> {
        Self::open_with_typing(definitions, supplied_agent, response, JsonState, hooks).await
    }
    pub async fn open_with_typing<T: StateTyping, H: LifecycleHooks>(
        definitions: Vec<WorkflowDefinition>,
        supplied_agent: bool,
        response: PromptResponse,
        typing: T,
        hooks: H,
    ) -> NativeCase<H, T> {
        let id = "native".parse().expect("fixture workflow identity");
        let peer = PromptPeer::new(response);
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let agents = if supplied_agent {
            vec![AgentSpec::new(
                "native".into(),
                "r1".into(),
                NativeAgent(Arc::clone(&peer)),
            )]
        } else {
            Vec::new()
        };
        let app = App::open(
            AppConfig::new(store, agents, definitions)
                .with_state_typing(typing)
                .with_hooks(hooks),
        )
        .await
        .expect("native App");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace.workflow(id).await.unwrap();
        NativeCase {
            app,
            workflow,
            peer,
            supplied_agent,
        }
    }
    pub fn agent_definition(stream: bool, targets: bool) -> WorkflowDefinition {
        let mut step = json!({"kind":"agent", "agent":"native",  "prompt":[{"kind":"text", "text":"Return the authoritative answer"}]});
        step["stream"] = json!(stream);
        if targets {
            step["text_target"] = json!("text");
            step["structured_target"] = json!("structured");
            step["structured_schema"] = json!({"type":["object", "null"]});
        }
        Self::definition(
            "native",
            json!({"sentinel":7,"text":"before","structured":"before"}),
            vec![json!({"id":"agent","step":step})],
            json!({}),
            json!({"sentinel":"reject_concurrent","text":"reject_concurrent","structured":"reject_concurrent"}),
        )
    }
}

impl<H: LifecycleHooks, T: StateTyping> NativeCase<H, T> {
    pub async fn close(mut self) {
        self.app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await
            .expect("native fixture cleanup");
        assert_eq!(
            self.peer.shutdowns.load(Ordering::SeqCst),
            usize::from(self.supplied_agent)
        );
    }
}
