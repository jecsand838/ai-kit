use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::conversation::{ConversationInfo, FileRead, NewConversation};
use agents_kit::identity::{AgentId, AgentInfo, ConversationId, NotificationId, WorkspaceId};
use agents_kit::knowledge::IngestKnowledgeRequest;
use agents_kit::model::ModelInfo;
use agents_kit::notification::{ConversationNotificationReceiver, NotificationResolution};
use agents_kit::prompt::{PromptRequest, PromptResponse};
use agents_kit::tool::ToolInfo;
use agents_kit::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceInfo,
    WorkspaceUpdateReadiness,
};
use agents_kit::{AgentBackend, ConversationBackend, KitError, WorkspaceBackend};
use app_kit::workflow::{
    CheckpointSelector, ExecutionOptions, Interaction, RestoreOptions, ResumePosition, RunSelector,
    RunStatus, WorkflowDefinition, WorkflowState,
};
use app_kit::{AgentSpec, App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::FutureExt;
use serde_json::json;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::Notify;

#[derive(Clone, Copy)]
enum Behavior {
    Reply(&'static str),
    ApplyFencedUpdates,
    BlockPreflightThenApply,
    BlockFirstThenApply,
    BlockThenRejectUpdate,
    ApplyThenPanic,
}

struct Peer {
    behavior: Behavior,
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    workspace: Mutex<Option<WorkspaceInfo>>,
    update_fences: Mutex<Vec<u64>>,
    shutdowns: AtomicUsize,
    creations: AtomicUsize,
    prompts: AtomicUsize,
    entered: Notify,
    release: Notify,
}

struct ForeignAgent(Arc<Peer>);
struct ForeignWorkspace(Arc<Peer>);
struct ForeignConversation(Arc<Peer>);

impl Peer {
    fn new(behavior: Behavior, suffix: u8) -> Result<Arc<Self>, KitError> {
        Ok(Arc::new(Self {
            behavior,
            agent_id: format!("agent_{suffix:032x}").parse()?,
            workspace_id: format!("workspace_{suffix:032x}").parse()?,
            conversation_id: format!("conversation_{suffix:032x}").parse()?,
            workspace: Mutex::new(None),
            update_fences: Mutex::new(Vec::new()),
            shutdowns: AtomicUsize::new(0),
            creations: AtomicUsize::new(0),
            prompts: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        }))
    }

    fn workspace_info(&self) -> Result<WorkspaceInfo, KitError> {
        self.workspace
            .lock()
            .expect("test workspace mutex")
            .clone()
            .ok_or_else(|| KitError::not_found("fixture workspace absent"))
    }

    fn apply(&self, request: UpdateWorkspaceRequest) -> Result<WorkspaceInfo, KitError> {
        let mut workspace = self.workspace.lock().expect("test workspace mutex");
        let current = workspace
            .as_ref()
            .ok_or_else(|| KitError::not_found("fixture workspace absent"))?;
        if current.id() != request.id() || current.revision() != request.expected_revision() {
            return Err(KitError::conflict(
                "fixture workspace revision fence rejected",
            ));
        }
        let info = WorkspaceInfo::new(
            request.id().clone(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            current.revision() + 1,
        )?;
        *workspace = Some(info.clone());
        Ok(info)
    }
}

impl AgentBackend for ForeignAgent {
    type Workspace = ForeignWorkspace;

    fn id(&self) -> AgentId {
        self.0.agent_id.clone()
    }
    async fn info(&self) -> Result<AgentInfo, KitError> {
        AgentInfo::new(self.id(), Some("fixture".to_owned()))
    }
    async fn new_workspace(&self, request: NewWorkspace) -> Result<Self::Workspace, KitError> {
        self.0.creations.fetch_add(1, Ordering::SeqCst);
        let info = WorkspaceInfo::new(
            self.0.workspace_id.clone(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            1,
        )?;
        *self.0.workspace.lock().expect("test workspace mutex") = Some(info);
        Ok(ForeignWorkspace(Arc::clone(&self.0)))
    }
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, KitError> {
        Ok(self
            .0
            .workspace
            .lock()
            .expect("test workspace mutex")
            .iter()
            .cloned()
            .collect())
    }
    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, KitError> {
        if id != self.0.workspace_id {
            return Err(KitError::not_found("fixture workspace id"));
        }
        self.0.workspace_info()
    }
    async fn get_workspace(&self, id: WorkspaceId) -> Result<Self::Workspace, KitError> {
        self.workspace_info(id).await?;
        Ok(ForeignWorkspace(Arc::clone(&self.0)))
    }
    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, KitError>> + Send + '_
    {
        let id = request.id().clone();
        let expected = request.expected_revision();
        async move {
            let info = self.workspace_info(id).await?;
            if info.revision() != expected {
                return Err(KitError::conflict("fixture preflight fence rejected"));
            }
            if matches!(self.0.behavior, Behavior::BlockPreflightThenApply) {
                self.0.entered.notify_one();
                self.0.release.notified().await;
            }
            Ok(WorkspaceUpdateReadiness::Ready)
        }
    }
    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, KitError> {
        let ordinal = {
            let mut fences = self.0.update_fences.lock().expect("test fence mutex");
            fences.push(request.expected_revision());
            fences.len()
        };
        match self.0.behavior {
            Behavior::ApplyFencedUpdates
            | Behavior::BlockPreflightThenApply
            | Behavior::Reply(_) => self.0.apply(request),
            Behavior::ApplyThenPanic => {
                self.0.apply(request)?;
                panic!("injected panic after lower workspace update acceptance");
            }
            Behavior::BlockFirstThenApply => {
                if ordinal == 1 {
                    self.0.entered.notify_one();
                    self.0.release.notified().await;
                }
                self.0.apply(request)
            }
            Behavior::BlockThenRejectUpdate => {
                self.0.entered.notify_one();
                self.0.release.notified().await;
                Err(KitError::conflict(
                    "fixture second agent rejected replacement",
                ))
            }
        }
    }
    async fn delete_workspace(&self, _request: DeleteWorkspaceRequest) -> Result<(), KitError> {
        Ok(())
    }
    async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _request: IngestKnowledgeRequest) -> Result<(), KitError> {
        Ok(())
    }
    async fn shutdown(self) -> Result<(), KitError> {
        self.0.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl WorkspaceBackend for ForeignWorkspace {
    type Conversation = ForeignConversation;

    fn id(&self) -> WorkspaceId {
        self.0.workspace_id.clone()
    }
    async fn info(&self) -> Result<WorkspaceInfo, KitError> {
        self.0.workspace_info()
    }
    async fn new_conversation(
        &self,
        _request: NewConversation,
    ) -> Result<Self::Conversation, KitError> {
        if matches!(self.0.behavior, Behavior::Reply(_)) {
            Ok(ForeignConversation(Arc::clone(&self.0)))
        } else {
            Err(KitError::unsupported("fixture unused conversation"))
        }
    }
    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, KitError> {
        Ok(Vec::new())
    }
    async fn conversation_info(&self, _id: ConversationId) -> Result<ConversationInfo, KitError> {
        Err(KitError::not_found("fixture conversation absent"))
    }
    async fn get_conversation(&self, _id: ConversationId) -> Result<Self::Conversation, KitError> {
        Err(KitError::not_found("fixture conversation absent"))
    }
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, KitError> {
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _request: IngestKnowledgeRequest) -> Result<(), KitError> {
        Ok(())
    }
    async fn close(self) -> Result<(), KitError> {
        Ok(())
    }
}

impl ConversationBackend for ForeignConversation {
    fn id(&self) -> ConversationId {
        self.0.conversation_id.clone()
    }
    async fn info(&self) -> Result<ConversationInfo, KitError> {
        ConversationInfo::new(self.id(), self.0.workspace_id.clone(), None)
    }
    async fn prompt(&self, _request: PromptRequest) -> Result<PromptResponse, KitError> {
        match self.0.behavior {
            Behavior::Reply(text) => {
                self.0.prompts.fetch_add(1, Ordering::SeqCst);
                Ok(PromptResponse::new(text.to_owned(), None))
            }
            _ => Err(KitError::unsupported("fixture unused prompt")),
        }
    }
    async fn abort(&self) -> Result<(), KitError> {
        Ok(())
    }
    async fn read_file(&self, _path: PathBuf) -> Result<FileRead, KitError> {
        Err(KitError::not_found("fixture file absent"))
    }
    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        if matches!(self.0.behavior, Behavior::Reply(_)) {
            Ok(ConversationNotificationReceiver::from_stream(
                futures_util::stream::pending(),
            ))
        } else {
            Err(KitError::busy("unused fixture notification stream"))
        }
    }
    async fn resolve_notification(
        &self,
        _id: NotificationId,
        _resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        Ok(())
    }
    async fn close(self) -> Result<(), KitError> {
        Ok(())
    }
}

pub struct AgentRevisionCase;

#[derive(Clone, Copy)]
pub enum AdmissionBoundary {
    BeforeSnapshot,
    AfterSnapshot,
}

struct AdmissionStore {
    native: StateStore,
    gate: Arc<AdmissionGate>,
}

struct AdmissionGate {
    boundary: Mutex<Option<AdmissionBoundary>>,
    entered: Notify,
    release: Notify,
    native_conflicts: AtomicUsize,
}

struct AdmissionTransaction {
    native: StateTransaction,
    observed: Option<Arc<AdmissionGate>>,
}

impl StateStoreBackend for AdmissionStore {
    type Transaction = AdmissionTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.native.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.native.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        let boundary = self.gate.boundary.lock().unwrap().take();
        if matches!(boundary, Some(AdmissionBoundary::BeforeSnapshot)) {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        let transaction = self.native.begin_transaction().await?;
        if matches!(boundary, Some(AdmissionBoundary::AfterSnapshot)) {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        Ok(AdmissionTransaction {
            native: transaction,
            observed: boundary.map(|_| Arc::clone(&self.gate)),
        })
    }
}

impl StateTransactionBackend for AdmissionTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.native.load(key).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.native.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.native.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let result = self.native.commit().await;
        if matches!(result, Err(StateStoreError::TransactionConflict { .. }))
            && let Some(observed) = self.observed
        {
            observed.native_conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.native.rollback().await
    }
}

impl AgentRevisionCase {
    pub async fn replacement_preserves_fence_across_snapshot(boundary: AdmissionBoundary) {
        let peer = Peer::new(Behavior::BlockPreflightThenApply, 104).unwrap();
        let gate = Arc::new(AdmissionGate {
            boundary: Mutex::new(None),
            entered: Notify::new(),
            release: Notify::new(),
            native_conflicts: AtomicUsize::new(0),
        });
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(AppConfig::new(
            AdmissionStore {
                native: native.clone(),
                gate: Arc::clone(&gate),
            },
            vec![AgentSpec::new(
                "writer".into(),
                "v1".into(),
                ForeignAgent(Arc::clone(&peer)),
            )],
            vec![WorkflowDefinition::parse(crate::WORKFLOW).unwrap()],
        ))
        .await
        .unwrap();
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: Some("before".into()), root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let revision = workspace.snapshot().await.unwrap().revision;
            let workflow = workspace.workflow("workspace-flow".parse().unwrap()).await.unwrap();
            let mut replacing = Box::pin(workspace.replace(revision, WorkspaceConfig {
                name: Some("after".into()), root: std::env::current_dir().unwrap(),
            }));
            tokio::select! {
                _ = peer.entered.notified() => {}
                outcome = &mut replacing => panic!("replacement bypassed preflight: {}", outcome.is_ok()),
            }
            *gate.boundary.lock().unwrap() = Some(boundary);
            peer.release.notify_one();
            tokio::select! {
                _ = gate.entered.notified() => {}
                outcome = &mut replacing => panic!("replacement bypassed native admission: {}", outcome.is_ok()),
            }
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let waiting = run.wait().await.unwrap();
            assert!(matches!(waiting.interactions.as_slice(), [Interaction::Input { .. }]));
            let capacity_key = StateKey::new("app-kit", "app").unwrap();
            assert_eq!(native.load(capacity_key.clone()).await.unwrap().unwrap().record.value["run_count"], 1);
            gate.release.notify_one();
            let outcome = replacing.await;
            let current = workspace.snapshot().await.unwrap();
            assert!(matches!(outcome, Err(AppError::Busy(_))), "unchanged workspace progress retains its existing retryable Busy outcome");
            assert_eq!(gate.native_conflicts.load(Ordering::SeqCst), match boundary {
                AdmissionBoundary::BeforeSnapshot => 0,
                AdmissionBoundary::AfterSnapshot => 1,
            }, "a stale native snapshot produces an actual Lance conflict; a fresh snapshot rejects the stale App revision before commit");
            assert_eq!(current.revision, revision);
            assert!(peer.update_fences.lock().unwrap().is_empty(), "a stale admission cannot mutate lower workspaces");
            run.cancel(waiting.checkpoint).await.unwrap();
            assert!(matches!(run.wait().await.unwrap().status, RunStatus::Cancelled));
            // Terminal projection may precede capacity retirement. Observe the real
            // atomic App-count/active-marker release before the single progress
            // attempt; the existing outer budget also bounds this observation.
            loop {
                let capacity = native.load(capacity_key.clone()).await.unwrap().unwrap();
                let count = capacity.record.value["run_count"].as_u64().unwrap();
                if count == 0 {
                    break;
                }
                assert_eq!(count, 1, "only the cancelled run owns capacity");
                tokio::task::yield_now().await;
            }
            // The rejected snapshot does not poison future lifecycle work.
            let mut progress = Box::pin(workspace.replace(revision, WorkspaceConfig {
                name: Some("progress".into()), root: std::env::current_dir().unwrap(),
            }));
            tokio::select! {
                _ = peer.entered.notified() => {}
                outcome = &mut progress => panic!("later replacement bypassed preflight: success={}, error={:?}", outcome.is_ok(), outcome.as_ref().err()),
            }
            peer.release.notify_one();
            assert_eq!(progress.await.unwrap().revision, revision + 1);
        })).catch_unwind().await;
        gate.release.notify_one();
        peer.release.notify_one();
        let cleanup = tokio::time::timeout(
            Duration::from_secs(15),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        )
        .await;
        match observed {
            Ok(result) => result.expect("coordinated native admission is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.expect("App cleanup is bounded").unwrap();
        assert_eq!(peer.shutdowns.load(Ordering::SeqCst), 1);
    }

    pub async fn replacement_rechecks_admission_after_preflight() {
        let peer = Peer::new(Behavior::BlockPreflightThenApply, 103).unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new(
                "writer".into(),
                "v1".into(),
                ForeignAgent(Arc::clone(&peer)),
            )],
            vec![WorkflowDefinition::parse(crate::WORKFLOW).unwrap()],
        ))
        .await
        .unwrap();
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(10),
            async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: Some("before".into()),
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("workspace-flow".parse().unwrap())
                    .await
                    .unwrap();
                let revision = workspace.snapshot().await.unwrap().revision;
                let mut replacing = Box::pin(workspace.replace(
                    revision,
                    WorkspaceConfig {
                        name: Some("after".into()),
                        root: std::env::current_dir().unwrap(),
                    },
                ));
                tokio::select! {
                    _ = peer.entered.notified() => {}
                    result = &mut replacing => panic!("replacement bypassed its advisory lower preflight: {}", result.is_ok()),
                }
                // Native admission and a public input boundary both complete
                // while replacement is still at advisory lower preflight.
                let run = workflow.start(ExecutionOptions::default()).await.unwrap();
                let waiting = run.wait().await.unwrap();
                assert!(
                    matches!(waiting.status, RunStatus::Running),
                    "the admitted input remains Running while preflight is held; paused={}, failed={}",
                    matches!(waiting.status, RunStatus::Paused),
                    matches!(waiting.status, RunStatus::Failed(_)),
                );
                assert!(matches!(waiting.interactions.as_slice(), [Interaction::Input { .. }]));
                assert!(peer.update_fences.lock().unwrap().is_empty());
                peer.release.notify_one();
                let outcome = replacing.await;
                let after = workspace.snapshot().await.unwrap();
                (outcome, after.revision, revision)
            },
        ))
        .catch_unwind()
        .await;
        // Release preflight and settle all accepted owners even when the
        // contract assertion fails on the current production implementation.
        peer.release.notify_one();
        let cleanup = tokio::time::timeout(
            Duration::from_secs(10),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(5),
            }),
        )
        .await;
        drop(app);
        let (outcome, after, before) = match observed {
            Ok(result) => result.expect("preflight and admitted input are bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        assert!(
            matches!(outcome, Err(AppError::Busy(_) | AppError::Conflict(_))),
            "replacement must reject the nonterminal run admitted during advisory preflight; replacement_succeeded={}",
            outcome.is_ok(),
        );
        assert_eq!(
            after, before,
            "rejected replacement preserves the workspace fence"
        );
        assert!(
            peer.update_fences.lock().unwrap().is_empty(),
            "rejection precedes lower mutation"
        );
        cleanup.expect("original owners join").unwrap();
        assert_eq!(peer.shutdowns.load(Ordering::SeqCst), 1);
    }

    fn definition() -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"agent-revisions", "revision":"workflow-r1",
                "entry":"answer", "state":{"answer":""}, "required":["answer"],
                "reducers":{"answer":"reject_concurrent"},
                "nodes":[{"id":"answer", "deadline_ms":2000, "step":{
                    "kind":"agent", "agent":"writer", "prompt":[{"kind":"text", "text":"reply"}],
                    "text_target":"answer"
                }}], "routes":{}
            })
            .to_string(),
        )
        .unwrap()
    }

    pub async fn caller_order_selects_new_work() {
        let old = Peer::new(Behavior::Reply("old"), 101).unwrap();
        let new = Peer::new(Behavior::Reply("new"), 102).unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let opened = App::open(AppConfig::new(
            store,
            vec![
                AgentSpec::new(
                    "writer".into(),
                    "z-old".into(),
                    ForeignAgent(Arc::clone(&old)),
                ),
                AgentSpec::new(
                    "writer".into(),
                    "a-new".into(),
                    ForeignAgent(Arc::clone(&new)),
                ),
            ],
            vec![Self::definition()],
        ))
        .await;
        let mut app = match opened {
            Ok(app) => app,
            Err(error) => {
                assert_eq!(old.shutdowns.load(Ordering::SeqCst), 1);
                assert_eq!(new.shutdowns.load(Ordering::SeqCst), 1);
                assert_eq!(old.creations.load(Ordering::SeqCst), 0);
                assert_eq!(new.creations.load(Ordering::SeqCst), 0);
                panic!("distinct caller Agent revisions must coexist: {error}");
            }
        };
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: Some("agent revisions".into()),
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("agent-revisions".parse().unwrap())
                    .await
                    .unwrap();
                let run = workflow.start(ExecutionOptions::default()).await.unwrap();
                let terminal = run.wait().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert_eq!(
                    terminal.state.values()["answer"],
                    "new",
                    "caller order selects admission, not revision lexical order"
                );
                assert_eq!(old.prompts.load(Ordering::SeqCst), 0);
                assert_eq!(new.prompts.load(Ordering::SeqCst), 1);
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
            Ok(result) => result.expect("Agent revision execution is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed.expect("Agent revision cleanup joins").unwrap();
        assert_eq!(old.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(new.shutdowns.load(Ordering::SeqCst), 1);
    }

    pub async fn deployment_preserves_prepared_and_restored_bindings() {
        Self::deployment(None, false).await;
    }

    pub async fn interrupted_deployment_preserves_applied_bindings() {
        Self::deployment(Some("applied"), false).await;
    }

    pub async fn interrupted_deployment_rejects_unproven_bindings() {
        for progress in ["pending", "unknown"] {
            Self::deployment(Some(progress), false).await;
        }
    }

    pub async fn transitive_children_preserve_agent_bindings() {
        Self::deployment(None, true).await;
    }

    fn deployment_definitions(through_children: bool) -> Vec<WorkflowDefinition> {
        let mut definitions = vec![Self::definition()];
        if through_children {
            for (id, child) in [
                ("revision-middle", "agent-revisions"),
                ("revision-parent", "revision-middle"),
            ] {
                definitions.push(WorkflowDefinition::parse(&json!({
                    "schema":"app-kit.workflow/v2", "id":id, "revision":"workflow-r1",
                    "entry":"child", "state":{"answer":""}, "required":["answer"],
                    "reducers":{"answer":"reject_concurrent"},
                    "nodes":[{"id":"child", "deadline_ms":2000, "step":{
                        "kind":"child", "workflow":child, "input":{},
                        "reductions":[{"source":"answer", "target":"answer", "reducer":"reject_concurrent"}]
                    }}], "routes":{}
                }).to_string()).expect("existing Child grammar"));
            }
        }
        definitions
    }

    async fn deployment(interrupted: Option<&str>, through_children: bool) {
        let old = Peer::new(Behavior::Reply("old"), 103).unwrap();
        let new = Peer::new(Behavior::Reply("new"), 104).unwrap();
        let workflow_id = if through_children {
            "revision-parent"
        } else {
            "agent-revisions"
        };
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut original = App::open(AppConfig::new(
            store.clone(),
            vec![AgentSpec::new(
                "writer".into(),
                "z-old".into(),
                ForeignAgent(Arc::clone(&old)),
            )],
            Self::deployment_definitions(through_children),
        ))
        .await
        .unwrap();
        let mut reopened = None;
        let mut restore = None;
        let unproven = matches!(interrupted, Some("pending" | "unknown"));
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
                let workspace = original
                    .create_workspace(WorkspaceConfig {
                        name: Some("revision deployment".into()),
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow(workflow_id.parse().unwrap())
                    .await
                    .unwrap();
                let saved = workflow.prepare(WorkflowState::default()).await.unwrap();
                let saved_id = saved.id().clone();
                let saved_occurrence = saved.snapshot().await.unwrap().occurrence;
                assert_eq!(old.prompts.load(Ordering::SeqCst), 0);
                if through_children {
                    let leaf = workspace.workflow("agent-revisions".parse().unwrap()).await.unwrap();
                    assert!(leaf.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap().is_empty(),
                        "the parent's initial admission captures Agent selections before any leaf exists");
                }
                if let Some(progress) = interrupted {
                    // Reproduce the persisted extension boundary with the
                    // selected old-pair evidence and an untouched new pair.
                    // Applied permits recovery; Pending/Unknown must not.
                    // This probes journals, not process-kill durability.
                    let mut journal = store
                        .load(
                            StateKey::new("app-kit", format!("workspace/{}", workspace.id()))
                                .unwrap(),
                        )
                        .await
                        .unwrap()
                        .unwrap()
                        .record;
                    assert_eq!(journal.value["state"]["state"], "active");
                    let materialization =
                        journal.value["state"]["materializations"]["writer"]["z-old"].clone();
                    assert!(materialization.is_object());
                    let config = journal.value["state"]["config"].clone();
                    if unproven {
                        restore = Some(journal.clone());
                    }
                    let old_progress = if progress == "applied" {
                        json!({"state":"applied", "materialization":materialization})
                    } else {
                        json!({"state":progress})
                    };
                    Arc::make_mut(&mut journal.value)["state"] = json!({
                        "state":"creating", "config":config,
                        "targets":{"writer":{
                            "z-old":old_progress,
                            "a-new":{"state":"pending"}
                        }}
                    });
                    let mut transaction = store.begin_transaction().await.unwrap();
                    transaction.put(journal).await.unwrap();
                    transaction.commit().await.unwrap();
                    assert_eq!(old.creations.load(Ordering::SeqCst), 1);
                    assert_eq!(new.creations.load(Ordering::SeqCst), 0);
                }
                let opened = App::open(AppConfig::new(
                    store.clone(),
                    vec![
                        AgentSpec::new(
                            "writer".into(),
                            "z-old".into(),
                            ForeignAgent(Arc::clone(&old)),
                        ),
                        AgentSpec::new(
                            "writer".into(),
                            "a-new".into(),
                            ForeignAgent(Arc::clone(&new)),
                        ),
                    ],
                    Self::deployment_definitions(through_children),
                ))
                .await;
                if unproven {
                    match opened {
                        Err(error) => assert!(
                            matches!(error, AppError::Conflict(_)),
                            "an unproven required old materialization must fail recovery: {error}"
                        ),
                        Ok(app) => {
                            reopened = Some(app);
                            panic!("recovery adopted an unproven required old Agent pair");
                        }
                    }
                    assert_eq!(old.creations.load(Ordering::SeqCst), 1);
                    assert_eq!(new.creations.load(Ordering::SeqCst), 0);
                    assert_eq!(old.prompts.load(Ordering::SeqCst), 0);
                    assert_eq!(new.prompts.load(Ordering::SeqCst), 0);
                    assert!(old.update_fences.lock().unwrap().is_empty());
                    assert!(new.update_fences.lock().unwrap().is_empty());
                    let head = store
                        .load(
                            StateKey::new(
                                "app-kit",
                                format!(
                                    "run/{}/{}/{}/{}",
                                    workspace.id(),
                                    "agent-revisions",
                                    saved_id, saved_occurrence,
                                ),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        head.record.value["sequence"], 0,
                        "failed recovery cannot advance the saved run"
                    );
                    let initial = store
                        .load(
                            StateKey::new(
                                "app-kit",
                                format!(
                                    "history/{}/{}/{}/{}/checkpoint/{:020}",
                                    workspace.id(),
                                    "agent-revisions",
                                    saved_id, saved_occurrence,
                                    0,
                                ),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(initial.record.value["checkpoint"]["sequence"], 0);
                    assert_eq!(
                        initial.record.value["checkpoint"]["state"],
                        json!({"answer":""})
                    );
                    assert!(
                        initial.record.value["checkpoint"]["outcomes"]
                            .as_array()
                            .unwrap()
                            .is_empty()
                    );
                    return;
                }
                reopened =
                    Some(opened.expect(
                        "a new Agent revision must coexist with a prepared run's old binding",
                    ));
                let current = reopened
                    .as_ref()
                    .unwrap()
                    .workspace(workspace.id().clone())
                    .await
                    .unwrap()
                    .workflow(workflow_id.parse().unwrap())
                    .await
                    .unwrap();
                let mut saved = current.open_run(RunSelector::Exact { id: saved_id.clone(), occurrence: saved_occurrence.clone() }).await.unwrap();
                let completed = saved
                    .execution(ExecutionOptions::default())
                    .await
                    .unwrap()
                    .finish()
                    .await
                    .unwrap();
                assert!(matches!(completed.status, RunStatus::Succeeded));
                assert_eq!(
                    completed.state.values()["answer"],
                    "old",
                    "admission pins precede the first Agent node"
                );
                let fresh = current
                    .start(ExecutionOptions::default())
                    .await
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                assert!(matches!(fresh.status, RunStatus::Succeeded));
                assert_eq!(
                    fresh.state.values()["answer"],
                    "new",
                    "existing App workspace receives the newly selected revision"
                );
                let mut fork = current
                    .restore(RestoreOptions {
                        source: RunSelector::Exact { id: saved_id, occurrence: saved_occurrence },
                        checkpoint: CheckpointSelector::Sequence(0),
                        position: ResumePosition::AtSaved,
                        overrides: WorkflowState::default(),
                        fallback: None,
                    })
                    .await
                    .unwrap();
                let restored = fork
                    .execution(ExecutionOptions::default())
                    .await
                    .unwrap()
                    .finish()
                    .await
                    .unwrap();
                assert!(matches!(restored.status, RunStatus::Succeeded));
                assert_eq!(
                    restored.state.values()["answer"],
                    "old",
                    "restoring history keeps its exact Agent revision"
                );
                assert_eq!(old.prompts.load(Ordering::SeqCst), 2);
                assert_eq!(new.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(
                    old.creations.load(Ordering::SeqCst),
                    1,
                    "old materialization is reused"
                );
                assert_eq!(
                    new.creations.load(Ordering::SeqCst),
                    1,
                    "new revision owns a distinct lower workspace"
                );
                if through_children {
                    let leaf = reopened.as_ref().unwrap()
                        .workspace(workspace.id().clone()).await.unwrap()
                        .workflow("agent-revisions".parse().unwrap()).await.unwrap();
                    let leaves = leaf.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap();
                    assert_eq!(leaves.len(), 3, "each parent journey admits its actual Agent leaf");
                    assert_eq!(leaves.iter().map(|run| &run.id).collect::<BTreeSet<_>>().len(), 3);
                    assert!(leaves.iter().all(|run| matches!(run.status, RunStatus::Succeeded)));
                    let mut answers = leaves.iter().map(|run| run.state.values()["answer"].as_str().unwrap()).collect::<Vec<_>>();
                    answers.sort_unstable();
                    assert_eq!(answers, ["new", "old", "old"]);
                }
            }))
            .catch_unwind()
            .await;
        // Restore the exact active journal before cleanup and before propagating
        // any captured failure; a returned failed App has already joined its owners.
        let restored = match restore {
            Some(record) => Some(
                std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                    let mut transaction = store.begin_transaction().await?;
                    transaction.put(record).await?;
                    transaction.commit().await
                }))
                .catch_unwind()
                .await,
            ),
            None => None,
        };
        let reopened_cleanup = match reopened.as_mut() {
            Some(app) => Some(
                tokio::time::timeout(
                    Duration::from_secs(6),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(4),
                    }),
                )
                .await,
            ),
            None => None,
        };
        let original_cleanup = tokio::time::timeout(
            Duration::from_secs(6),
            original.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        )
        .await;
        drop(reopened);
        drop(original);
        match observed {
            Ok(result) => result.expect("Agent revision deployment is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(restored) = restored {
            match restored {
                Ok(result) => result
                    .expect("exact journal restoration is bounded")
                    .expect("exact Active journal is restored"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        if let Some(cleanup) = reopened_cleanup {
            cleanup.expect("new generation cleanup joins").unwrap();
        } else {
            assert!(
                unproven,
                "successful deployment retains its new App for cleanup"
            );
        }
        assert!(
            matches!(original_cleanup, Ok(Ok(()) | Err(AppError::Closed(_)))),
            "superseded generation cleanup joins"
        );
        assert_eq!(old.shutdowns.load(Ordering::SeqCst), 2);
        assert_eq!(new.shutdowns.load(Ordering::SeqCst), 1);
    }

    pub async fn missing_pin_rejects_before_claim_or_lower_effects() {
        let old = Peer::new(Behavior::Reply("old"), 105).unwrap();
        let new = Peer::new(Behavior::Reply("new"), 106).unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut original = App::open(AppConfig::new(
            store.clone(),
            vec![AgentSpec::new(
                "writer".into(),
                "z-old".into(),
                ForeignAgent(Arc::clone(&old)),
            )],
            vec![Self::definition()],
        ))
        .await
        .unwrap();
        let mut unexpected = None;
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
                let workspace = original
                    .create_workspace(WorkspaceConfig {
                        name: None,
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("agent-revisions".parse().unwrap())
                    .await
                    .unwrap();
                let saved = workflow.prepare(WorkflowState::default()).await.unwrap();
                let key = StateKey::new("app-kit", "app").unwrap();
                let before = store.load(key.clone()).await.unwrap().unwrap();
                let rejected = App::open(AppConfig::new(
                    store.clone(),
                    vec![AgentSpec::new(
                        "writer".into(),
                        "a-new".into(),
                        ForeignAgent(Arc::clone(&new)),
                    )],
                    vec![Self::definition()],
                ))
                .await;
                let error = match rejected {
                    Err(error) => Some(error),
                    Ok(app) => {
                        unexpected = Some(app);
                        None
                    }
                };
                let after = store.load(key).await.unwrap().unwrap();
                assert!(
                    matches!(error, Some(AppError::Conflict(_))),
                    "a missing required Agent revision is a deployment conflict"
                );
                assert_eq!(
                    after.revision, before.revision,
                    "missing Agent pin rejects before generation claim"
                );
                assert_eq!(after.record.value, before.record.value);
                assert_eq!(new.creations.load(Ordering::SeqCst), 0);
                assert_eq!(new.prompts.load(Ordering::SeqCst), 0);
                assert_eq!(new.shutdowns.load(Ordering::SeqCst), 1);
                assert_eq!(old.creations.load(Ordering::SeqCst), 1);
                assert_eq!(old.prompts.load(Ordering::SeqCst), 0);
                assert!(
                    matches!(saved.snapshot().await.unwrap().status, RunStatus::Paused),
                    "rejected deployment leaves the original generation usable"
                );
            }))
            .catch_unwind()
            .await;
        if let Some(app) = unexpected.as_mut() {
            let _ = tokio::time::timeout(
                Duration::from_secs(6),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(4),
                }),
            )
            .await;
        }
        let closed = tokio::time::timeout(
            Duration::from_secs(6),
            original.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        )
        .await;
        drop(unexpected);
        drop(original);
        match observed {
            Ok(result) => result.expect("missing Agent revision validation is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed.expect("original generation cleanup joins").unwrap();
        assert_eq!(old.shutdowns.load(Ordering::SeqCst), 1);
    }
}

pub struct CompensationCase;

impl CompensationCase {
    pub async fn operation_panic_is_observed() -> Result<(), Box<dyn std::error::Error>> {
        let first = Peer::new(Behavior::ApplyThenPanic, 95)?;
        let second = Peer::new(Behavior::ApplyFencedUpdates, 96)?;
        let mut app = Self::app(&first, &second).await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("prior".to_owned()),
                root: std::env::current_dir()?,
            })
            .await?;
        let revision = workspace.snapshot().await?.revision;
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            workspace.replace(
                revision,
                WorkspaceConfig {
                    name: Some("next".to_owned()),
                    root: std::env::current_dir()?,
                },
            ),
        )
        .await;
        // Settle owned cleanup before making assertions, including on the old
        // implementation where the observer never receives its panic outcome.
        let cleanup = tokio::time::timeout(
            Duration::from_secs(3),
            app.shutdown(Shutdown::Drain {
                timeout: Duration::from_millis(100),
            }),
        )
        .await;
        assert!(
            matches!(outcome, Ok(Err(AppError::Integration(_)))),
            "workspace task panic must settle its exact operation observer with Integration"
        );
        assert!(cleanup.is_ok(), "panic cleanup must remain bounded");
        assert_eq!(first.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(second.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(
            *first.update_fences.lock().expect("test fences"),
            [1],
            "lost accepted lower receipt cannot authorize update replay"
        );
        assert_eq!(first.workspace_info()?.revision(), 2);
        assert_eq!(first.workspace_info()?.name(), "next");
        Ok(())
    }
    async fn app(
        first: &Arc<Peer>,
        second: &Arc<Peer>,
    ) -> Result<App<StateStore, ForeignAgent>, Box<dyn std::error::Error>> {
        let config = AppConfig::new(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?,
            vec![
                AgentSpec::new(
                    "a".to_owned(),
                    "v1".to_owned(),
                    ForeignAgent(Arc::clone(first)),
                ),
                AgentSpec::new(
                    "b".to_owned(),
                    "v1".to_owned(),
                    ForeignAgent(Arc::clone(second)),
                ),
            ],
            Vec::new(),
        );
        Ok(App::open(config).await?)
    }

    pub async fn rejects_foreign_forward_receipt() -> Result<(), Box<dyn std::error::Error>> {
        let first = Peer::new(Behavior::BlockFirstThenApply, 91)?;
        let second = Peer::new(Behavior::BlockThenRejectUpdate, 92)?;
        let foreign_caller = ForeignAgent(Arc::clone(&first));
        let mut app = Self::app(&first, &second).await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("prior".to_owned()),
                root: std::env::current_dir()?,
            })
            .await?;
        let revision = workspace.snapshot().await?.revision;
        let mut replacing = Box::pin(workspace.replace(
            revision,
            WorkspaceConfig {
                name: Some("next".to_owned()),
                root: std::env::current_dir()?,
            },
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = first.entered.notified() => {}
                result = &mut replacing => panic!("replacement skipped gated update: {}", result.is_ok()),
            }
        }).await.expect("replacement must reach the first lower update");
        let prior = foreign_caller
            .workspace_info(first.workspace_id.clone())
            .await?;
        let foreign = foreign_caller
            .update_workspace(UpdateWorkspaceRequest::new(
                prior.id().clone(),
                prior.revision(),
                "next".to_owned(),
                prior.context().clone(),
                prior.defaults().clone(),
            )?)
            .await?;
        assert_eq!(foreign.revision(), 2);
        // An incorrect adoption can continue to the second leg; let that leg
        // fail so the regression exposes any unauthorized compensation too.
        second.release.notify_one();
        first.release.notify_one();
        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut replacing)
            .await
            .expect("foreign receipt rejection must settle");
        drop(replacing);
        let observed = foreign_caller
            .workspace_info(first.workspace_id.clone())
            .await?;
        let cleanup = app
            .shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(2),
            })
            .await;
        assert!(matches!(outcome, Err(AppError::Conflict(_))));
        assert_eq!(
            (observed.name(), observed.revision()),
            ("next", 2),
            "a diagnostic read after rejected mutation cannot authorize compensation"
        );
        assert!(
            second
                .update_fences
                .lock()
                .expect("test fence mutex")
                .is_empty(),
            "replacement cannot advance after adopting another caller's receipt"
        );
        assert!(matches!(
            cleanup,
            Err(AppError::Conflict(_) | AppError::Integration(_))
        ));
        assert_eq!(first.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(second.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    pub async fn preserves_foreign_change(
        foreign_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let first = Peer::new(Behavior::ApplyFencedUpdates, 81)?;
        let second = Peer::new(Behavior::BlockThenRejectUpdate, 82)?;
        // Separate public lower handle represents another legitimate lower caller.
        let foreign_caller = ForeignAgent(Arc::clone(&first));
        let mut app = Self::app(&first, &second).await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("prior".to_owned()),
                root: std::env::current_dir()?,
            })
            .await?;
        let revision = workspace.snapshot().await?.revision;
        let mut replacing = Box::pin(workspace.replace(
            revision,
            WorkspaceConfig {
                name: Some("next".to_owned()),
                root: std::env::current_dir()?,
            },
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = second.entered.notified() => {}
            result = &mut replacing => panic!("replacement settled before the gated second update: {}", result.is_ok()),
        }
    }).await.expect("replacement must reach second lower update");

        let applied = foreign_caller
            .workspace_info(first.workspace_id.clone())
            .await?;
        assert_eq!(applied.name(), "next");
        assert_eq!(applied.revision(), 2);
        let foreign = foreign_caller
            .update_workspace(UpdateWorkspaceRequest::new(
                applied.id().clone(),
                applied.revision(),
                foreign_name.to_owned(),
                applied.context().clone(),
                applied.defaults().clone(),
            )?)
            .await?;
        assert_eq!(foreign.revision(), 3);
        second.release.notify_one();

        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut replacing)
            .await
            .expect("compensation must settle within probe bound");
        assert!(matches!(outcome, Err(AppError::Conflict(_))));
        drop(replacing);
        let observed = foreign_caller
            .workspace_info(first.workspace_id.clone())
            .await?;
        let fences = first
            .update_fences
            .lock()
            .expect("test fence mutex")
            .clone();
        // Settle App before assertions so a failing regression still cleans up.
        let cleanup = app
            .shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(2),
            })
            .await;
        assert_eq!(
            (observed.name(), observed.revision()),
            (foreign_name, 3),
            "compensation replaced a foreign lower revision; observed lower update fences were {fences:?}"
        );
        assert!(
            matches!(
                cleanup,
                Err(AppError::Integration(_) | AppError::Conflict(_))
            ),
            "unresolved foreign compensation must remain an explicit shutdown failure"
        );
        assert_eq!(first.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(second.shutdowns.load(Ordering::SeqCst), 1);
        assert!(
            fences.iter().skip(2).all(|revision| *revision == 2),
            "a compensation attempt must retain its recorded revision: {fences:?}"
        );
        Ok(())
    }
}
