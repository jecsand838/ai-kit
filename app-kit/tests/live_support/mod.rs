use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    fs,
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use agents_kit::config::{AgentConfig, OpenCodeConfig};
use agents_kit::conversation::{ConversationInfo, FileRead, NewConversation};
use agents_kit::error::{ErrorKind, ErrorPhase, Operation};
use agents_kit::identity::{AgentId, AgentInfo, ConversationId, NotificationId, WorkspaceId};
use agents_kit::knowledge::IngestKnowledgeRequest;
use agents_kit::model::ModelInfo;
use agents_kit::notification::{
    ConversationNotificationKind, ConversationNotificationReceiver, NotificationResolution,
    ProcessState,
};
use agents_kit::prompt::{PromptRequest, PromptResponse};
use agents_kit::tool::ToolInfo;
use agents_kit::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceInfo,
    WorkspaceUpdateReadiness,
};
use agents_kit::{
    Agent, AgentBackend, AgentBuilder, AgentStorage, Conversation, ConversationBackend, KitError,
    Workspace, WorkspaceBackend,
};
use app_kit::workflow::{
    Interaction, RunSnapshot, RunStatus, WorkflowDefinition, WorkflowFailure, WorkflowRun,
};
use app_kit::{AgentSpec, AppConfig};
use serde_json::Value;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::{Database, StateStore, StateStoreBackend};
use tokio::sync::watch;

use crate::owned_child::OwnedChild;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ForwardingPoint {
    Observe,
    ShutdownObservation,
    ResolveNotification,
    GetConversation,
    Abort,
}

struct TestSignal {
    sender: watch::Sender<bool>,
}

impl TestSignal {
    fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    fn publish(&self) {
        self.sender.send_replace(true);
    }

    async fn wait(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        loop {
            receiver
                .changed()
                .await
                .expect("the strict-live forwarding signal must remain owned");
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }
}

struct TestGate {
    entered: TestSignal,
    released: TestSignal,
}

impl TestGate {
    fn new() -> Self {
        Self {
            entered: TestSignal::new(),
            released: TestSignal::new(),
        }
    }

    async fn enter(&self) {
        self.entered.publish();
        self.released.wait().await;
    }

    async fn wait_until_entered(&self) {
        self.entered.wait().await;
    }

    fn release(&self) {
        self.released.publish();
    }
}

pub(super) struct ForwardingControl {
    point: ForwardingPoint,
    method: TestGate,
    prompt: TestGate,
    prompt_delivered: TestSignal,
    prompt_observation_finished: TestSignal,
    prompts: AtomicU64,
    resolutions: AtomicU64,
    process_exits: AtomicU64,
    notification_conversations: Mutex<BTreeMap<NotificationId, ConversationId>>,
    last_prompt: Mutex<Option<String>>,
    last_structured_markers: Mutex<Option<Value>>,
    recovery_failure_owner: Mutex<Option<tokio::task::Id>>,
    shutdown_started: AtomicU64,
    shutdown_finished: AtomicU64,
    shutdown_result: Mutex<Option<Result<(), KitError>>>,
    shutdown_conversations: Mutex<BTreeMap<WorkspaceId, Vec<ConversationInfo>>>,
    shutdown_closed_workspaces: AtomicU64,
}

impl ForwardingControl {
    pub(super) fn observation() -> Arc<Self> {
        Arc::new(Self::new(ForwardingPoint::Observe))
    }

    pub(super) fn shutdown_observation() -> Arc<Self> {
        Arc::new(Self::new(ForwardingPoint::ShutdownObservation))
    }

    pub(super) fn prompt_count(&self) -> u64 {
        self.prompts.load(Ordering::SeqCst)
    }

    pub(super) fn resolution_count(&self) -> u64 {
        self.resolutions.load(Ordering::SeqCst)
    }

    pub(super) fn process_exit_count(&self) -> u64 {
        self.process_exits.load(Ordering::SeqCst)
    }

    pub(super) fn report_notification_delivery(&self) {
        let delivered = self
            .notification_conversations
            .lock()
            .expect("notification conversation lock");
        eprintln!(
            "strict-live Agent notification evidence: prompts={} resolutions={} delivered={delivered:?}",
            self.prompt_count(),
            self.resolution_count()
        );
    }

    pub(super) fn notification_conversation(&self, id: &NotificationId) -> ConversationId {
        self.notification_conversations
            .lock()
            .expect("notification conversation lock")
            .get(id)
            .expect("the original public receiver observed this notification")
            .clone()
    }

    pub(super) fn shutdown_conversation(&self) -> ConversationInfo {
        let workspaces = self
            .shutdown_conversations
            .lock()
            .expect("shutdown inventory lock");
        assert_eq!(workspaces.len(), 1);
        let conversations = workspaces.values().next().expect("shutdown workspace");
        assert_eq!(conversations.len(), 1);
        conversations[0].clone()
    }

    pub(super) fn last_prompt(&self) -> Option<String> {
        self.last_prompt
            .lock()
            .expect("prompt observation lock")
            .clone()
    }

    pub(super) fn last_structured_markers(&self) -> Option<Value> {
        self.last_structured_markers
            .lock()
            .expect("structured marker observation lock")
            .clone()
    }

    fn observe_structured_markers(&self, response: &PromptResponse) {
        // Record only the two UUID fields used by the isolation fixture. Never
        // retain or print the full native response or arbitrary response text.
        let markers = response.structured_output().map(|value| {
            let mut markers = serde_json::Map::new();
            for key in ["tool", "knowledge"] {
                let marker = match value.get(key) {
                    Some(Value::String(marker))
                        if marker.len() <= 64 && uuid::Uuid::parse_str(marker).is_ok() =>
                    {
                        marker.clone()
                    }
                    Some(_) => "<not a UUID string>".to_owned(),
                    None => "<missing>".to_owned(),
                };
                markers.insert(key.to_owned(), Value::String(marker));
            }
            Value::Object(markers)
        });
        *self
            .last_structured_markers
            .lock()
            .expect("structured marker observation lock") = markers;
    }

    pub(super) fn assert_shutdown_completion(&self, expected_success: bool) {
        assert_eq!(self.point, ForwardingPoint::ShutdownObservation);
        assert_eq!(self.shutdown_started.load(Ordering::SeqCst), 1);
        assert_eq!(self.shutdown_finished.load(Ordering::SeqCst), 1);
        let result = self
            .shutdown_result
            .lock()
            .expect("shutdown result observation lock")
            .clone()
            .expect("the actual lower shutdown must have returned");
        if expected_success {
            result.expect("App success requires actual lower shutdown success");
        } else {
            let error = result.expect_err("App Integration requires the actual repaired error");
            assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
            assert_eq!(error.context().operation, Operation::Shutdown, "{error:?}");
            assert_eq!(error.context().phase, ErrorPhase::Cleanup, "{error:?}");
            assert_eq!(error.detail(), None, "{error:?}");
            assert_eq!(error.http_status(), None, "{error:?}");
            assert!(error.cause().is_none(), "{error:?}");
            assert!(error.supplementary().is_empty(), "{error:?}");
            assert!(!error.diagnostics_truncated(), "{error:?}");
        }
    }

    pub(super) fn assert_shutdown_once(&self) {
        assert_eq!(self.shutdown_started.load(Ordering::SeqCst), 1);
        assert_eq!(self.shutdown_finished.load(Ordering::SeqCst), 1);
        assert_eq!(self.shutdown_closed_workspaces.load(Ordering::SeqCst), 1);
        assert_eq!(
            self.shutdown_conversations
                .lock()
                .expect("shutdown inventory lock")
                .len(),
            1
        );
    }

    pub(super) fn shutdown_conversations(&self, workspace: &WorkspaceId) -> Vec<ConversationInfo> {
        self.shutdown_conversations
            .lock()
            .expect("shutdown inventory lock")
            .get(workspace)
            .expect("the exact workspace inventory must be observed before shutdown")
            .clone()
    }

    pub(super) fn response_settlement() -> Arc<Self> {
        Arc::new(Self::new(ForwardingPoint::ResolveNotification))
    }

    pub(super) fn conversation_recovery() -> Arc<Self> {
        Arc::new(Self::new(ForwardingPoint::GetConversation))
    }

    pub(super) fn recovery_failure_owner(&self) -> Option<tokio::task::Id> {
        *self
            .recovery_failure_owner
            .lock()
            .expect("recovery return observation lock")
    }

    pub(super) fn abort_settlement() -> Arc<Self> {
        Arc::new(Self::new(ForwardingPoint::Abort))
    }

    fn new(point: ForwardingPoint) -> Self {
        Self {
            point,
            method: TestGate::new(),
            prompt: TestGate::new(),
            prompt_delivered: TestSignal::new(),
            prompt_observation_finished: TestSignal::new(),
            prompts: AtomicU64::new(0),
            resolutions: AtomicU64::new(0),
            process_exits: AtomicU64::new(0),
            notification_conversations: Mutex::new(BTreeMap::new()),
            last_prompt: Mutex::new(None),
            last_structured_markers: Mutex::new(None),
            recovery_failure_owner: Mutex::new(None),
            shutdown_started: AtomicU64::new(0),
            shutdown_finished: AtomicU64::new(0),
            shutdown_result: Mutex::new(None),
            shutdown_conversations: Mutex::new(BTreeMap::new()),
            shutdown_closed_workspaces: AtomicU64::new(0),
        }
    }

    pub(super) fn wrap(self: &Arc<Self>, inner: Agent) -> ForwardingAgent {
        ForwardingAgent {
            inner,
            control: Arc::clone(self),
        }
    }

    pub(super) async fn wait_until_method_blocked(&self) {
        self.method.wait_until_entered().await;
    }

    pub(super) fn release_method(&self) {
        self.method.release();
    }

    pub(super) async fn wait_until_prompt_ready(&self) {
        assert_eq!(self.point, ForwardingPoint::ResolveNotification);
        self.prompt.wait_until_entered().await;
    }

    pub(super) fn release_prompt(&self) {
        assert_eq!(self.point, ForwardingPoint::ResolveNotification);
        self.prompt.release();
    }

    pub(super) async fn wait_until_prompt_delivered(&self) {
        assert_eq!(self.point, ForwardingPoint::ResolveNotification);
        self.prompt_delivered.wait().await;
    }

    pub(super) async fn wait_until_prompt_observation_finished(&self) {
        assert_eq!(self.point, ForwardingPoint::ResolveNotification);
        self.prompt_observation_finished.wait().await;
    }

    async fn before_get_conversation(&self) {
        if self.point == ForwardingPoint::GetConversation {
            self.method.enter().await;
        }
    }

    async fn before_abort(&self) {
        if self.point == ForwardingPoint::Abort {
            self.method.enter().await;
        }
    }

    async fn after_resolve_notification(&self) {
        if self.point == ForwardingPoint::ResolveNotification {
            self.method.enter().await;
        }
    }

    async fn after_prompt(&self) {
        if self.point == ForwardingPoint::ResolveNotification {
            self.prompt.enter().await;
        }
    }

    fn finish_prompt_observation(&self) {
        if self.point == ForwardingPoint::ResolveNotification {
            self.prompt_observation_finished.publish();
        }
    }

    fn publish_prompt_delivery(&self) {
        if self.point == ForwardingPoint::ResolveNotification {
            self.prompt_delivered.publish();
        }
    }
}

pub(super) struct ForwardingAgent {
    inner: Agent,
    control: Arc<ForwardingControl>,
}

pub(super) struct ForwardingWorkspace {
    inner: Workspace,
    control: Arc<ForwardingControl>,
}

pub(super) struct ForwardingConversation {
    inner: Conversation,
    control: Arc<ForwardingControl>,
}

struct ForwardedPrompt<'a> {
    inner: Pin<Box<dyn Future<Output = Result<PromptResponse, KitError>> + Send + 'a>>,
    control: Arc<ForwardingControl>,
}

impl Future for ForwardedPrompt<'_> {
    type Output = Result<PromptResponse, KitError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.inner.as_mut().poll(context);
        if result.is_ready() {
            self.control.publish_prompt_delivery();
        }
        result
    }
}

impl Drop for ForwardedPrompt<'_> {
    fn drop(&mut self) {
        self.control.finish_prompt_observation();
    }
}

impl AgentBackend for ForwardingAgent {
    type Workspace = ForwardingWorkspace;

    fn id(&self) -> AgentId {
        self.inner.id()
    }

    async fn info(&self) -> Result<AgentInfo, KitError> {
        self.inner.info().await
    }

    async fn new_workspace(&self, request: NewWorkspace) -> Result<Self::Workspace, KitError> {
        self.inner
            .new_workspace(request)
            .await
            .map(|inner| ForwardingWorkspace {
                inner,
                control: Arc::clone(&self.control),
            })
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        self.inner.list_workspaces().await
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        self.inner.workspace_info(id).await
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<Self::Workspace, KitError> {
        self.inner
            .get_workspace(id)
            .await
            .map(|inner| ForwardingWorkspace {
                inner,
                control: Arc::clone(&self.control),
            })
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_ {
        let request = request.clone();
        async move { self.inner.check_workspace_update(&request).await }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        self.inner.update_workspace(request).await
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        self.inner.delete_workspace(request).await
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        self.inner.list_models().await
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        self.inner.ingest_knowledge(request).await
    }

    async fn shutdown(self) -> Result<(), KitError> {
        let started = Instant::now();
        if self.control.point == ForwardingPoint::ShutdownObservation {
            eprintln!("strict-live lower Agent shutdown entered");
        }
        // App has drained before consuming its Agents. Acquire inspection
        // handles here so they cannot block workflow workspace admission.
        let observed = async {
            let mut workspaces = Vec::new();
            if self.control.point == ForwardingPoint::Observe {
                for info in self.inner.list_workspaces().await? {
                    let workspace = self.inner.get_workspace(info.id().clone()).await?;
                    let conversations = workspace.list_conversations().await?;
                    workspaces.push((workspace, conversations));
                }
            }
            Ok::<_, KitError>(workspaces)
        }
        .await;
        self.control.shutdown_started.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.shutdown().await;
        if self.control.point == ForwardingPoint::ShutdownObservation {
            *self
                .control
                .shutdown_result
                .lock()
                .expect("shutdown result observation lock") = Some(result.clone());
            eprintln!(
                "strict-live lower Agent shutdown returned success={} elapsed={:?}",
                result.is_ok(),
                started.elapsed(),
            );
            if let Err(error) = &result {
                // Preserve typed primary and secondary facts without native
                // messages, paths, identity values or configured credentials.
                let mut failures = vec![("primary", error)];
                while let Some((relation, failure)) = failures.pop() {
                    eprintln!(
                        "strict-live lower Agent shutdown {relation}: kind={:?} context={:?} detail={:?} truncated={}",
                        failure.kind(),
                        failure.context(),
                        failure.detail(),
                        failure.diagnostics_truncated(),
                    );
                    if let Some(cause) = failure.cause() {
                        failures.push(("cause", cause));
                    }
                    failures.extend(
                        failure
                            .supplementary()
                            .iter()
                            .map(|error| ("supplementary", error)),
                    );
                }
            }
        }
        self.control
            .shutdown_finished
            .fetch_add(1, Ordering::SeqCst);
        // An observation error must never skip the real consuming shutdown.
        // Missing observations fail the calling test's exact assertions.
        if let Ok(workspaces) = observed {
            for (workspace, conversations) in workspaces {
                if matches!(workspace.info().await, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Closed))
                {
                    self.control
                        .shutdown_closed_workspaces
                        .fetch_add(1, Ordering::SeqCst);
                }
                self.control
                    .shutdown_conversations
                    .lock()
                    .expect("shutdown inventory lock")
                    .insert(workspace.id(), conversations);
            }
        }
        result
    }
}

impl WorkspaceBackend for ForwardingWorkspace {
    type Conversation = ForwardingConversation;

    fn id(&self) -> WorkspaceId {
        self.inner.id()
    }

    async fn info(&self) -> Result<WorkspaceInfo, KitError> {
        self.inner.info().await
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<Self::Conversation, KitError> {
        eprintln!(
            "strict-live conversation preparation entered: workspace={:?}",
            self.inner.id()
        );
        let result = self.inner.new_conversation(request).await;
        match &result {
            Ok(conversation) => eprintln!(
                "strict-live conversation preparation returned: workspace={:?} conversation={:?}",
                self.inner.id(),
                conversation.id()
            ),
            Err(error) => {
                let mut failures = vec![("primary", error)];
                for ordinal in 0..8 {
                    let Some((relation, failure)) = failures.pop() else {
                        break;
                    };
                    eprintln!(
                        "strict-live conversation preparation failed: workspace={:?} ordinal={ordinal} relation={relation} kind={:?} operation={:?} phase={:?} detail={:?} truncated={}",
                        self.inner.id(),
                        failure.kind(),
                        failure.context().operation,
                        failure.context().phase,
                        failure.detail(),
                        failure.diagnostics_truncated(),
                    );
                    failures.extend(
                        failure
                            .supplementary()
                            .iter()
                            .take(8)
                            .rev()
                            .map(|failure| ("supplementary", failure)),
                    );
                    if let Some(cause) = failure.cause() {
                        failures.push(("cause", cause));
                    }
                }
                if !failures.is_empty() {
                    eprintln!(
                        "strict-live conversation preparation diagnostics reached eight failures"
                    );
                }
            }
        }
        result.map(|inner| ForwardingConversation {
            inner,
            control: Arc::clone(&self.control),
        })
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, KitError> {
        self.inner.list_conversations().await
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, KitError> {
        self.inner.conversation_info(id).await
    }

    async fn get_conversation(&self, id: ConversationId) -> Result<Self::Conversation, KitError> {
        self.control.before_get_conversation().await;
        let result = self.inner.get_conversation(id).await;
        if self.control.point == ForwardingPoint::GetConversation && result.is_err() {
            let mut owner = self
                .control
                .recovery_failure_owner
                .lock()
                .expect("recovery return observation lock");
            if owner.is_none() {
                *owner = tokio::task::try_id();
            }
        }
        result.map(|inner| ForwardingConversation {
            inner,
            control: Arc::clone(&self.control),
        })
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, KitError> {
        self.inner.list_tools().await
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), KitError> {
        self.inner.ingest_knowledge(request).await
    }

    async fn close(self) -> Result<(), KitError> {
        self.inner.close().await
    }
}

impl ConversationBackend for ForwardingConversation {
    fn id(&self) -> ConversationId {
        self.inner.id()
    }

    async fn info(&self) -> Result<ConversationInfo, KitError> {
        self.inner.info().await
    }

    fn prompt(
        &self,
        request: PromptRequest,
    ) -> impl Future<Output = Result<PromptResponse, KitError>> + Send + '_ {
        let control = Arc::clone(&self.control);
        ForwardedPrompt {
            inner: Box::pin(async move {
                control.prompts.fetch_add(1, Ordering::SeqCst);
                *control.last_prompt.lock().expect("prompt observation lock") =
                    Some(request.text().to_owned());
                let result = self.inner.prompt(request).await;
                if let Ok(response) = &result {
                    control.observe_structured_markers(response);
                }
                if let Err(error) = &result
                    && control.point != ForwardingPoint::ShutdownObservation
                {
                    eprintln!(
                        "strict-live forwarded prompt failed for {:?}: {error:?}",
                        self.inner.id()
                    );
                }
                control.after_prompt().await;
                result
            }),
            control: Arc::clone(&self.control),
        }
    }

    async fn abort(&self) -> Result<(), KitError> {
        self.control.before_abort().await;
        self.inner.abort().await
    }

    async fn read_file(&self, path: PathBuf) -> Result<FileRead, KitError> {
        self.inner.read_file(path).await
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        let receiver = self.inner.notification_stream()?;
        let control = Arc::clone(&self.control);
        let conversation = self.inner.id();
        Ok(ConversationNotificationReceiver::from_stream(
            futures_util::stream::unfold(
                (receiver, control, conversation),
                |(mut receiver, control, conversation)| async move {
                    let item = receiver.recv().await?;
                    if let Err(error) = &item
                        && control.point != ForwardingPoint::ShutdownObservation
                    {
                        eprintln!(
                            "strict-live forwarded notification failed for {conversation:?}: {error:?}"
                        );
                    }
                    if let Ok(notification) = &item
                        && matches!(
                            notification.kind(),
                            ConversationNotificationKind::Interaction { .. }
                        )
                    {
                        control
                            .notification_conversations
                            .lock()
                            .expect("notification conversation lock")
                            .insert(notification.id().clone(), conversation.clone());
                    }
                    if let Ok(notification) = &item
                        && matches!(
                            notification.kind(),
                            ConversationNotificationKind::Process {
                                state: ProcessState::Exited { .. },
                                ..
                            }
                        )
                    {
                        control.process_exits.fetch_add(1, Ordering::SeqCst);
                    }
                    Some((item, (receiver, control, conversation)))
                },
            ),
        ))
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        self.control.resolutions.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.resolve_notification(id, resolution).await;
        self.control.after_resolve_notification().await;
        result
    }

    async fn close(self) -> Result<(), KitError> {
        self.inner.close().await
    }
}

pub struct OwnedOpenCode {
    root: PathBuf,
    address: SocketAddr,
    config_content: String,
    tool_canary: Option<String>,
    child: Option<OwnedChild>,
    cleanup_root: bool,
}

impl OwnedOpenCode {
    pub const INPUT_WORKFLOW: &'static str = r#"
{
  "schema": "app-kit.workflow/v2",
  "id": "caller-input",
  "revision": "v1",
  "state": {"answer": null},
  "required": ["answer"],
  "reducers": {"answer": "reject_concurrent"},
  "entry": "ask",
  "nodes": [
    {
      "id": "ask",
      "step": {
        "kind": "input",
        "message": [{"kind": "text", "text": "Continue the workflow?"}],
        "schema": {"type": "string", "enum": ["yes"]},
        "target": "answer"
      }
    }
  ],
  "routes": {
    "ask": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
  }
}
"#;

    pub const QUESTION_WORKFLOW: &'static str = r#"
{
  "schema": "app-kit.workflow/v2",
  "id": "agent-question",
  "revision": "v1",
  "state": {"reply": null},
  "required": ["reply"],
  "reducers": {"reply": "reject_concurrent"},
  "entry": "ask-agent",
  "nodes": [
    {
      "id": "ask-agent",
      "step": {
        "kind": "agent",
        "agent": "opencode",
        "prompt": [{"kind": "text", "text": "Use OpenCode's built-in `question` tool exactly once and do not invoke any other tool. Issue a real tool call; do not print, quote, describe, or imitate it in response text. Pass exactly this tool argument: {\"questions\":[{\"header\":\"Continue\",\"question\":\"Continue?\",\"options\":[{\"label\":\"Yes\",\"description\":\"Continue\"},{\"label\":\"No\",\"description\":\"Stop\"}],\"multiple\":false}]}. Wait for the tool reply. Then output exactly `Acknowledged.` and stop."}],
        "text_target": "reply"
      }
    }
  ],
  "routes": {
    "ask-agent": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
  }
}
"#;

    pub fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the strict-live clock must follow the Unix epoch")
            .as_nanos();
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "app-kit-live-{label}-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        for directory in [
            root.join("workspace"),
            root.join("opencode/database"),
            root.join("opencode/xdg/config"),
            root.join("opencode/xdg/data"),
            root.join("opencode/xdg/cache"),
            root.join("opencode/xdg/state"),
        ] {
            fs::create_dir_all(directory).expect("the strict-live durable domain must be created");
        }
        let root = fs::canonicalize(root)
            .expect("the strict-live durable domain must have an absolute host path");
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("the strict-live OpenCode process must reserve a loopback port");
        let address = listener
            .local_addr()
            .expect("the strict-live listener must expose its address");
        drop(listener);
        let _ = Self::required("TEST_OLLAMA_MODEL_OPENCODE_DEFAULT");
        Self {
            root,
            address,
            config_content: Self::required("OPENCODE_CONFIG_CONTENT"),
            tool_canary: None,
            child: None,
            cleanup_root: true,
        }
    }

    pub(super) fn set_tool_canary(&mut self, canary: String) {
        assert!(
            self.child.is_none(),
            "configure tool context before service start"
        );
        self.tool_canary = Some(canary);
    }

    pub(super) fn tool_receipt(&self) -> String {
        fs::read_to_string(self.root.join("tool-receipt.txt")).unwrap_or_else(|error| {
            let inventory = Self::request(
                self.address,
                "/experimental/session?archived=true&limit=10",
                None,
            );
            eprintln!(
                "missing native tool receipt at {}: {error}; sessions: {inventory:?}",
                self.root.display()
            );
            if let Some(sessions) = inventory
                .as_deref()
                .and_then(|body| serde_json::from_str::<Value>(body).ok())
                .and_then(|value| value.as_array().cloned())
            {
                for session in sessions {
                    if let Some(id) = session.get("id").and_then(Value::as_str) {
                        let messages = Self::request(
                            self.address,
                            &format!("/session/{id}/message?limit=10"),
                            None,
                        );
                        eprintln!("native tool receipt diagnostic {id}: {messages:?}");
                    }
                }
            }
            eprintln!(
                "native pending questions: {:?}",
                Self::request(self.address, "/question", None)
            );
            eprintln!(
                "native durable events: {:?}",
                Self::request(self.address, "/sync/history", Some("{}"))
            );
            panic!("the actual native tool must write its receipt: {error}")
        })
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn durable_root(&self) -> &Path {
        &self.root
    }

    pub fn workspace_root(&self) -> PathBuf {
        self.durable_root().join("workspace")
    }

    pub fn has_active_session(&self) -> bool {
        let path = format!(
            "/session/status?directory={}",
            self.workspace_root().display()
        );
        Self::request(self.address, &path, None)
            .and_then(|body| serde_json::from_str::<serde_json::Map<_, _>>(&body).ok())
            .is_some_and(|statuses| !statuses.is_empty())
    }

    pub(super) fn question_sessions(&self) -> Option<BTreeSet<String>> {
        let body = Self::request(
            self.address,
            "/experimental/session?archived=true&limit=100",
            None,
        )?;
        let sessions: Value = serde_json::from_str(&body).ok()?;
        Some(
            sessions
                .as_array()?
                .iter()
                .filter_map(|session| session.get("id").and_then(Value::as_str).map(str::to_owned))
                .collect(),
        )
    }

    pub(super) fn report_question_evidence(
        &self,
        snapshot: &RunSnapshot,
        before: Option<&BTreeSet<String>>,
    ) {
        let status = match &snapshot.status {
            RunStatus::Running => "running",
            RunStatus::Paused => "paused",
            RunStatus::Halted { .. } => "halted",
            RunStatus::Cancelling => "cancelling",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed(failure) => match failure {
                WorkflowFailure::Evaluation(_) => "failed/evaluation",
                WorkflowFailure::State(_) => "failed/state",
                WorkflowFailure::Agent(_) => "failed/agent",
                WorkflowFailure::Action(_) => "failed/action",
                WorkflowFailure::Hook { .. } => "failed/hook",
                WorkflowFailure::Child(_) => "failed/child",
                WorkflowFailure::Execution(_) => "failed/execution",
                WorkflowFailure::Timeout(_) => "failed/timeout",
                WorkflowFailure::Limit(_) => "failed/limit",
                _ => "failed/unrecognized",
            },
            RunStatus::Cancelled => "cancelled",
            _ => "unrecognized public status",
        };
        let interactions = snapshot.interactions.iter().map(|interaction| match interaction {
            Interaction::Notification { sequence, generation, notification } => serde_json::json!({
                "kind":"notification", "sequence":sequence, "generation":generation, "id":notification.id().as_str(),
            }),
            Interaction::Input { sequence, generation, .. } => serde_json::json!({"kind":"input", "sequence":sequence, "generation":generation}),
            Interaction::Unknown { sequence, generation, retry_safe } => serde_json::json!({"kind":"unknown", "sequence":sequence, "generation":generation, "retry_safe":retry_safe}),
            Interaction::Inputs { sequence, generation, .. } => serde_json::json!({"kind":"inputs", "sequence":sequence, "generation":generation}),
            _ => serde_json::json!({"kind":"unrecognized public interaction"}),
        }).collect::<Vec<_>>();
        eprintln!(
            "strict-live question evidence: status={status} checkpoint={} interactions={interactions:?}",
            snapshot.checkpoint
        );
        let after = self.question_sessions();
        let statuses = Self::request(self.address, "/session/status", None)
            .and_then(|body| serde_json::from_str::<Value>(&body).ok());
        match (before, after) {
            (Some(before), Some(after)) => {
                let added = after.difference(before).collect::<Vec<_>>();
                eprintln!(
                    "strict-live question evidence: new_native_sessions={}",
                    added.len()
                );
                for id in added.into_iter().take(4) {
                    let status = match statuses.as_ref() {
                        Some(statuses) => match statuses
                            .get(id)
                            .and_then(|value| value.get("type"))
                            .and_then(Value::as_str)
                        {
                            Some("idle") => "idle",
                            Some("busy") => "busy",
                            Some("retry") => "retry",
                            Some(_) => "unrecognized",
                            None => "absent",
                        },
                        None => "unavailable",
                    };
                    let messages = Self::request(
                        self.address,
                        &format!("/session/{id}/message?limit=10"),
                        None,
                    )
                    .and_then(|body| serde_json::from_str::<Value>(&body).ok());
                    let mut questions = Vec::new();
                    let mut text_parts = 0;
                    if let Some(messages) = messages.as_ref().and_then(Value::as_array) {
                        for message in messages {
                            for part in message
                                .get("parts")
                                .and_then(Value::as_array)
                                .into_iter()
                                .flatten()
                            {
                                if part.get("type").and_then(Value::as_str) == Some("text") {
                                    text_parts += 1;
                                }
                                if part.get("tool").and_then(Value::as_str) == Some("question") {
                                    questions.push(serde_json::json!({"call":part.get("callID"), "message":part.get("messageID"), "status":part.pointer("/state/status")}));
                                }
                            }
                        }
                    }
                    eprintln!(
                        "strict-live question evidence: session={id} native_status={status} messages_available={} text_parts={text_parts} native_questions={questions:?}",
                        messages.is_some()
                    );
                }
            }
            _ => eprintln!("strict-live question evidence: native session correlation unavailable"),
        }
        let pending = Self::request(self.address, "/question", None)
            .and_then(|body| serde_json::from_str::<Value>(&body).ok())
            .and_then(|value| value.as_array().cloned())
            .map(|questions| questions.into_iter().take(16).map(|question| serde_json::json!({
                "request":question.get("id"), "session":question.get("sessionID"),
                "call":question.pointer("/tool/callID"), "message":question.pointer("/tool/messageID"),
            })).collect::<Vec<_>>());
        eprintln!("strict-live question evidence: pending={pending:?}");
    }

    pub(super) fn assert_running(&mut self) {
        assert!(
            self.child
                .as_mut()
                .expect("owned OpenCode process")
                .try_wait()
                .expect("owned process status")
                .is_none()
        );
        assert!(
            Self::healthy(self.address),
            "caller-owned service remains available"
        );
    }

    pub async fn start(&mut self) {
        assert!(
            self.child.is_none(),
            "the owned OpenCode process is already running"
        );
        let mut command = Command::new("opencode");
        command
            .arg("serve")
            .arg("--hostname")
            .arg(self.address.ip().to_string())
            .arg("--port")
            .arg(self.address.port().to_string())
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env(
                "OPENCODE_DB",
                self.root.join("opencode/database/opencode.db"),
            )
            .env("XDG_CONFIG_HOME", self.root.join("opencode/xdg/config"))
            .env("XDG_DATA_HOME", self.root.join("opencode/xdg/data"))
            .env("XDG_CACHE_HOME", self.root.join("opencode/xdg/cache"))
            .env("XDG_STATE_HOME", self.root.join("opencode/xdg/state"))
            .env("OPENCODE_CONFIG_CONTENT", &self.config_content)
            .env("OPENCODE_AUTO_SHARE", "false")
            .env("OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS", "false")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(canary) = &self.tool_canary {
            command
                .env("AI_KIT_TOOL_CANARY", canary)
                .env("AI_KIT_TOOL_RECEIPT", self.root.join("tool-receipt.txt"));
        }
        self.child = Some(OwnedChild::new(
            command
                .spawn()
                .expect("the real owned OpenCode process must start"),
        ));
        self.wait_until_healthy().await;
    }

    pub(super) async fn start_if_stopped(&mut self) {
        if self.child.as_mut().is_some_and(|child| {
            child
                .try_wait()
                .expect("the owned OpenCode process status must remain observable")
                .is_some()
        }) {
            drop(self.child.take());
        }
        if self.child.is_none() {
            self.start().await;
        }
    }

    pub async fn stop(&mut self) {
        let _ = self
            .child
            .as_mut()
            .expect("the owned OpenCode process must be running")
            .stop()
            .await
            .expect("the owned OpenCode process must terminate and reap within its cleanup bound");
        drop(self.child.take());
        assert!(
            !Self::healthy(self.address),
            "the owned OpenCode endpoint must close with its process"
        );
    }

    pub async fn app_config(&self, sources: &[&str]) -> AppConfig<StateStore, Agent> {
        Self::build_config_with_revision(
            self.durable_root().to_path_buf(),
            self.endpoint(),
            "strict-live-v1",
            sources,
        )
        .await
    }

    pub(super) async fn forwarding_app_config(
        &self,
        sources: &[&str],
        control: &Arc<ForwardingControl>,
    ) -> AppConfig<StateStore, ForwardingAgent> {
        let agent = Self::build_agent(self.durable_root(), &self.endpoint(), "agent-state").await;
        AppConfig::new(
            Self::connect_app_store(self.durable_root()).await,
            vec![AgentSpec::new(
                "opencode".to_owned(),
                "strict-live-v1".to_owned(),
                control.wrap(agent),
            )],
            Self::definitions(sources),
        )
    }

    pub(super) async fn stop_and_retain_paths(&mut self) {
        self.cleanup_root = false;
        if self.child.is_some() {
            self.stop().await;
        }
    }

    pub async fn cleanup(&mut self) {
        if self.child.is_some() {
            self.stop().await;
        }
        fs::remove_dir_all(&self.root)
            .expect("the strict-live durable domain must be removed after shutdown");
        self.cleanup_root = false;
    }

    pub async fn build_config_with_revision(
        root: PathBuf,
        endpoint: String,
        revision: &str,
        sources: &[&str],
    ) -> AppConfig<StateStore, Agent> {
        let agent = Self::build_agent(&root, &endpoint, "agent-state").await;
        let workflows = Self::definitions(sources);
        AppConfig::new(
            Self::connect_app_store(&root).await,
            vec![AgentSpec::new(
                "opencode".to_owned(),
                revision.to_owned(),
                agent,
            )],
            workflows,
        )
    }

    pub async fn build_agent(root: &Path, endpoint: &str, store_name: &str) -> Agent {
        let agent_store = Self::connect_agent_store(root, store_name).await;
        Self::build_agent_from_store(endpoint, agent_store, None).await
    }

    pub(super) async fn connect_agent_store(root: &Path, store_name: &str) -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            root.join(store_name),
        )))
        .await
        .expect("the durable Agent StateStore must open")
    }

    pub(super) async fn build_agent_from_store<S: StateStoreBackend>(
        endpoint: &str,
        agent_store: S,
        knowledge: Option<Database>,
    ) -> Agent<S> {
        Self::try_build_agent_from_store(endpoint, agent_store, knowledge)
            .await
            .expect("the real durable Agent must create or reopen")
    }

    pub(super) async fn try_build_agent_from_store<S: StateStoreBackend>(
        endpoint: &str,
        agent_store: S,
        knowledge: Option<Database>,
    ) -> Result<Agent<S>, KitError> {
        let agent = Self::agent_config(endpoint);
        let storage = AgentStorage::new(agent_store);
        let inventory = storage
            .list_agents()
            .await
            .expect("the durable Agent inventory must load");
        let builder = match inventory.as_slice() {
            [] => AgentBuilder::create(agent, storage),
            [info] => AgentBuilder::reopen(agent, storage, info.id().clone()),
            _ => panic!("one strict-live Agent store must contain at most one Agent"),
        };
        let builder = match knowledge {
            Some(database) => builder.with_knowledge_database(database),
            None => builder,
        };
        builder.build().await
    }

    pub(super) async fn try_reopen_agent(
        endpoint: &str,
        agent_store: StateStore,
        id: AgentId,
    ) -> Result<Agent, KitError> {
        AgentBuilder::reopen(
            Self::agent_config(endpoint),
            AgentStorage::new(agent_store),
            id,
        )
        .build()
        .await
    }

    fn agent_config(endpoint: &str) -> AgentConfig {
        let backend = OpenCodeConfig::external(
            endpoint,
            Duration::from_secs(10),
            Duration::from_secs(120),
            Duration::from_secs(30),
        )
        .expect("the owned OpenCode endpoint must be valid")
        .into();
        AgentConfig::new(
            backend,
            Duration::from_secs(120),
            NonZeroUsize::new(2).expect("two is nonzero"),
        )
        .expect("the owned OpenCode Agent configuration must be valid")
    }

    pub async fn connect_app_store(root: &Path) -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            root.join("app-state"),
        )))
        .await
        .expect("the durable App StateStore must open")
    }

    pub fn definitions(sources: &[&str]) -> Vec<WorkflowDefinition> {
        sources
            .iter()
            .map(|source| {
                WorkflowDefinition::parse(source)
                    .expect("the strict-live workflow document must parse")
            })
            .collect()
    }

    pub async fn settle_terminal<A: AgentBackend>(
        run: &WorkflowRun<StateStore, A>,
        mut snapshot: RunSnapshot,
    ) -> RunSnapshot {
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                if matches!(
                    snapshot.status,
                    RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled
                ) {
                    return snapshot;
                }
                let observed = run
                    .wait()
                    .await
                    .expect("the strict-live run observation must succeed");
                if observed.checkpoint == snapshot.checkpoint {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                snapshot = observed;
            }
        })
        .await
        .expect("the strict-live run must reach a terminal outcome in time")
    }

    async fn wait_until_healthy(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            assert!(
                self.child
                    .as_mut()
                    .expect("the OpenCode process must exist during readiness")
                    .try_wait()
                    .expect("the OpenCode readiness state must be readable")
                    .is_none(),
                "the real OpenCode process exited before readiness"
            );
            if Self::healthy(self.address) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the real OpenCode process must become healthy within its startup bound"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn healthy(address: SocketAddr) -> bool {
        Self::request(address, "/global/health", None).is_some()
    }

    fn request(address: SocketAddr, path: &str, body: Option<&str>) -> Option<String> {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500)).ok()?;
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .ok()?;
        stream
            .set_write_timeout(Some(Duration::from_millis(500)))
            .ok()?;
        match body {
            Some(body) => write!(stream, "POST {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()),
            None => write!(stream, "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"),
        }.ok()?;
        let mut response = String::new();
        stream.take(65_537).read_to_string(&mut response).ok()?;
        (response.len() <= 65_536).then_some(())?;
        let (head, body) = response.split_once("\r\n\r\n")?;
        let status = head.lines().next()?;
        (status.starts_with("HTTP/1.1 200") || status.starts_with("HTTP/1.0 200"))
            .then(|| body.to_owned())
    }

    pub fn required(name: &str) -> String {
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
}

impl Drop for OwnedOpenCode {
    fn drop(&mut self) {
        let reaped = self.child.as_mut().is_none_or(OwnedChild::settle_on_drop);
        if self.cleanup_root && reaped {
            if let Err(error) = fs::remove_dir_all(&self.root) {
                eprintln!("owned OpenCode path cleanup failed: {error}");
            }
        } else if !reaped {
            eprintln!("owned OpenCode process reaping is unconfirmed; retaining its paths");
        }
    }
}
