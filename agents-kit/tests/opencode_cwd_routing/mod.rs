//! Native-directory identity is separate from passive caller cwd. These wire
//! fixtures prove adapter behavior; the independently executed stock OpenCode
//! canonical/alias pair supplies the corresponding native-service evidence.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::conversation::{ConversationBackend, NewConversation};
use agents_kit::error::ErrorKind;
use agents_kit::workspace::{
    NewWorkspace, UpdateWorkspaceRequest, WorkspaceContext, WorkspaceDefaults,
    WorkspaceExecutionPolicy, WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess,
    WorkspaceSandboxMode,
};
use agents_kit::{AgentBackend, AgentBuilder, WorkspaceBackend};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::native_cleanup::NativeCleanup;
use super::{OpenCodeEventProbe, ProbeRequest, PromptFaultStore};

#[derive(Clone)]
struct RoutingObservation {
    method: String,
    path: String,
    directory: Option<String>,
}

pub(super) struct CwdRoutingFixture {
    mappings: Mutex<BTreeMap<String, String>>,
    replies: Mutex<VecDeque<serde_json::Value>>,
    requests: Mutex<Vec<RoutingObservation>>,
    held_replies: Mutex<VecDeque<String>>,
    entered: Semaphore,
    release: Semaphore,
}

impl CwdRoutingFixture {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            mappings: Mutex::new(BTreeMap::new()),
            replies: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
            held_replies: Mutex::new(VecDeque::new()),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }

    fn map(&self, requested: &str, native: &str) {
        self.mappings
            .lock()
            .unwrap()
            .insert(requested.to_owned(), native.to_owned());
    }

    pub(super) fn record(&self, request: &ProbeRequest) {
        self.requests.lock().unwrap().push(RoutingObservation {
            method: request.method.clone(),
            path: request.path.clone(),
            directory: request.directory.clone(),
        });
    }

    pub(super) fn native_directory(&self, requested: &str) -> String {
        self.mappings
            .lock()
            .unwrap()
            .get(requested)
            .cloned()
            .unwrap_or_else(|| requested.to_owned())
    }

    pub(super) async fn reply(&self, stream: &mut TcpStream, request: &ProbeRequest) {
        let held = self.held_replies.lock().unwrap().pop_front();
        let body = if let Some(directory) = held {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("owned path gate remains open")
                .forget();
            serde_json::json!({"directory": directory})
        } else if let Some(body) = self.replies.lock().unwrap().pop_front() {
            body
        } else {
            serde_json::json!({"directory": self.native_directory(request.directory.as_deref().expect("workspace path request carries exact caller cwd"))})
        };
        OpenCodeEventProbe::json(stream, &body.to_string()).await;
    }

    fn session_routes(&self) -> Vec<RoutingObservation> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.path.starts_with("/session"))
            .cloned()
            .collect()
    }

    fn context(directory: &str) -> WorkspaceContext {
        let root = PathBuf::from(directory);
        WorkspaceContext::new(
            root.clone(),
            vec![WorkspaceRoot::new(root, WorkspaceRootAccess::ReadWrite).unwrap()],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .unwrap()
    }

    fn workspace(directory: &str) -> NewWorkspace {
        NewWorkspace::new(
            "cwd-routing",
            Self::context(directory),
            WorkspaceDefaults::empty(),
        )
        .unwrap()
    }

    fn conversation() -> NewConversation {
        NewConversation::new(None, None, Vec::new()).unwrap()
    }
}

#[derive(Clone, Copy)]
enum CwdCase {
    Canonical,
    WindowsDrive,
    WindowsUnc,
    WorkspaceObserverLoss,
    AliasReopen,
    AliasReceiptLoss,
    RetargetCreation,
    RetargetCatalog,
    RetargetReopen,
    EmptyRetarget,
    ConcurrentRetarget,
    RevisionUpdate,
}

impl CwdCase {
    async fn verify(self) {
        let mut probe = OpenCodeEventProbe::start(0).await;
        let stats = Arc::clone(&probe.stats);
        let routing = CwdRoutingFixture::new();
        *stats.cwd_routing.lock().unwrap() = Some(Arc::clone(&routing));
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let config = probe.config(Duration::from_secs(3), Duration::from_secs(8));
        let canonical = match self {
            Self::WindowsDrive => r"C:\opencode\canonical".to_owned(),
            Self::WindowsUnc => r"\\backend-host\share\opencode".to_owned(),
            _ => env!("CARGO_MANIFEST_DIR").to_owned(),
        };
        let alias = format!("{canonical}/caller-alias");
        let other = format!("{canonical}/other-native");
        let requested = if matches!(
            self,
            Self::AliasReopen | Self::AliasReceiptLoss | Self::WorkspaceObserverLoss
        ) {
            alias.as_str()
        } else {
            canonical.as_str()
        };
        routing.map(requested, &canonical);
        let context = CwdRoutingFixture::context(requested);
        let mut agent = None;
        let mut workspace = None;
        let mut conversations = Vec::new();
        let mut creations = JoinSet::new();
        let mut shutdowns = JoinSet::new();
        let mut routing_change_at = None;
        let observation = NativeCleanup::observe(Instant::now() + Duration::from_secs(30), async {
            agent = Some(
                AgentBuilder::create(config.clone(), faults.storage())
                    .build()
                    .await
                    .expect("Agent setup succeeds"),
            );
            let agent_id = agent.as_ref().unwrap().id();
            let models = agent
                .as_ref()
                .unwrap()
                .list_models()
                .await
                .expect("fixture model inventory is available");
            assert_eq!(models.len(), 1, "fixture advertises one exact model");
            let defaults =
                WorkspaceDefaults::new(Some(models[0].selector().clone()), None, None).unwrap();
            let request = NewWorkspace::new("cwd-routing", context.clone(), defaults).unwrap();
            if matches!(self, Self::WorkspaceObserverLoss) {
                routing.held_replies.lock().unwrap().push_back(canonical.clone());
                let mut creation = Box::pin(agent.as_ref().unwrap().new_workspace(request));
                tokio::select! {
                    permit = routing.entered.acquire() => permit.expect("held native path entered").forget(),
                    result = &mut creation => {
                        match result {
                            Ok(unexpected) => { workspace = Some(Arc::new(unexpected)); panic!("workspace publication cannot precede native routing evidence"); }
                            Err(error) => panic!("workspace failed before held native routing entry: {error:?}"),
                        }
                    }
                }
                drop(creation);
                routing.release.add_permits(1);
                let committed = loop {
                    let inventory = agent.as_ref().unwrap().list_workspaces().await.unwrap();
                    if !inventory.is_empty() {
                        assert_eq!(inventory.len(), 1, "one accepted creation survives observer loss");
                        break inventory.into_iter().next().unwrap();
                    }
                    tokio::task::yield_now().await;
                };
                assert_eq!(committed.context(), &context);
                loop {
                    match agent.as_ref().unwrap().get_workspace(committed.id().clone()).await {
                        Ok(ready) => { workspace = Some(Arc::new(ready)); break; }
                        Err(error) if error.kind() == ErrorKind::Busy => {
                            // Commit inventory can precede the detached creator's
                            // final publication/drop of its live Workspace claim.
                            // Only read-only reopen is repeated within this observation.
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("accepted workspace cannot reopen: {error:?}"),
                    }
                }
            } else {
                workspace = Some(Arc::new(agent.as_ref().unwrap().new_workspace(request).await.expect("native path must admit the workspace")));
            }
            let workspace_id = workspace.as_ref().unwrap().id();
            let initial = workspace.as_ref().unwrap().info().await.unwrap();
            assert_eq!(
                initial.context(),
                &context,
                "native canonicalization cannot rewrite portable context"
            );
            if matches!(self, Self::AliasReceiptLoss) {
                faults.obscure_next_bound_session_commit();
            }
            let conversation = workspace
                .as_ref()
                .unwrap()
                .new_conversation(CwdRoutingFixture::conversation())
                .await
                .expect("native identity matching the alias must construct");
            let conversation_id = conversation.id();
            conversations.push(conversation);
            assert_eq!(workspace.as_ref().unwrap().info().await.unwrap(), initial);
            assert_eq!(stats.next_session.load(Ordering::SeqCst), 1);
            let tools = workspace
                .as_ref()
                .unwrap()
                .list_tools()
                .await
                .expect("initial catalog uses the bound native directory");
            assert_eq!(tools.len(), 1);
            match self {
                Self::Canonical | Self::WindowsDrive | Self::WindowsUnc | Self::WorkspaceObserverLoss => {}
                Self::AliasReopen | Self::AliasReceiptLoss | Self::RetargetReopen => {
                    conversations.clear();
                    drop(workspace.take());
                    let owned = agent.take().unwrap();
                    shutdowns.spawn(async move { owned.shutdown().await });
                    shutdowns
                        .join_next()
                        .await
                        .unwrap()
                        .unwrap()
                        .expect("quiescent shutdown preserves durable conversation");
                    if matches!(self, Self::RetargetReopen) {
                        routing.map(requested, &other);
                    }
                    match AgentBuilder::reopen(config.clone(), faults.storage(), agent_id.clone())
                        .build()
                        .await
                    {
                        Ok(reopened) => {
                            agent = Some(reopened);
                            assert!(
                                !matches!(self, Self::RetargetReopen),
                                "changed native workspace identity must reject reopen"
                            );
                        }
                        Err(error) => {
                            assert!(
                                matches!(self, Self::RetargetReopen),
                                "unchanged alias must reopen: {error:?}"
                            );
                            assert_eq!(
                                error.kind(),
                                ErrorKind::InvalidState,
                                "retarget retains a precise identity contradiction: {error:?}"
                            );
                            assert_eq!(stats.next_session.load(Ordering::SeqCst), 1);
                            // Restore only after recording rejection, so fixture cleanup can
                            // independently settle the original durable native session.
                            routing.map(requested, &canonical);
                            agent = Some(
                                AgentBuilder::reopen(config.clone(), faults.storage(), agent_id)
                                    .build()
                                    .await
                                    .unwrap(),
                            );
                        }
                    }
                    workspace = Some(Arc::new(
                        agent
                            .as_ref()
                            .unwrap()
                            .get_workspace(workspace_id)
                            .await
                            .unwrap(),
                    ));
                    assert_eq!(workspace.as_ref().unwrap().info().await.unwrap(), initial);
                    conversations.push(
                        workspace
                            .as_ref()
                            .unwrap()
                            .get_conversation(conversation_id)
                            .await
                            .expect("exact original conversation remains recoverable"),
                    );
                    assert_eq!(stats.next_session.load(Ordering::SeqCst), 1);
                }
                Self::RetargetCreation | Self::RetargetCatalog | Self::EmptyRetarget => {
                    if matches!(self, Self::EmptyRetarget) {
                        conversations.pop().unwrap().close().await.unwrap();
                        assert!(
                            workspace
                                .as_ref()
                                .unwrap()
                                .list_conversations()
                                .await
                                .unwrap()
                                .is_empty()
                        );
                    }
                    routing.map(requested, &other);
                    let result = if matches!(self, Self::RetargetCatalog) {
                        workspace.as_ref().unwrap().list_tools().await.map(|_| ())
                    } else {
                        match workspace
                            .as_ref()
                            .unwrap()
                            .new_conversation(CwdRoutingFixture::conversation())
                            .await
                        {
                            Ok(conversation) => {
                                conversations.push(conversation);
                                Ok(())
                            }
                            Err(error) => Err(error),
                        }
                    };
                    assert_eq!(
                        stats.next_session.load(Ordering::SeqCst),
                        1,
                        "retarget must fail before creating a native session"
                    );
                    let error =
                        result.expect_err("same-revision native identity cannot silently change");
                    assert_eq!(
                        error.kind(),
                        ErrorKind::InvalidState,
                        "native retarget retains identity contradiction: {error:?}"
                    );
                    assert_eq!(workspace.as_ref().unwrap().info().await.unwrap(), initial);
                }
                Self::ConcurrentRetarget => {
                    routing
                        .held_replies
                        .lock()
                        .unwrap()
                        .extend([canonical.clone(), other.clone()]);
                    for _ in 0..2 {
                        let owned = workspace.as_ref().unwrap().clone();
                        creations.spawn(async move {
                            owned
                                .new_conversation(CwdRoutingFixture::conversation())
                                .await
                        });
                    }
                    tokio::select! {
                        permit = routing.entered.acquire_many(2) => permit.unwrap().forget(),
                        result = creations.join_next() => {
                            match result.expect("creation observer remains owned").expect("creation observer joins") {
                                Ok(unexpected) => { conversations.push(unexpected); panic!("creation completed without checking current native workspace identity"); }
                                Err(error) => panic!("creation failed before overlapping native observations: {error:?}"),
                            }
                        }
                    }
                    routing.release.add_permits(2);
                    let mut success = 0;
                    let mut conflicts = 0;
                    while let Some(joined) = creations.join_next().await {
                        match joined.unwrap() {
                            Ok(conversation) => {
                                conversations.push(conversation);
                                success += 1;
                            }
                            Err(error) => {
                                assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                                conflicts += 1;
                            }
                        }
                    }
                    assert_eq!((success, conflicts), (1, 1));
                    assert_eq!(
                        stats.next_session.load(Ordering::SeqCst),
                        2,
                        "only matching identity may dispatch"
                    );
                    assert_eq!(workspace.as_ref().unwrap().info().await.unwrap(), initial);
                }
                Self::RevisionUpdate => {
                    // A descriptive/default revision keeps the original native anchor.
                    let updated = agent
                        .as_ref()
                        .unwrap()
                        .update_workspace(
                            UpdateWorkspaceRequest::new(
                                workspace_id.clone(),
                                initial.revision(),
                                "renamed",
                                context.clone(),
                                initial.defaults().clone(),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    drop(workspace.take());
                    conversations.clear();
                    workspace = Some(Arc::new(
                        agent
                            .as_ref()
                            .unwrap()
                            .get_workspace(workspace_id.clone())
                            .await
                            .unwrap(),
                    ));
                    assert_eq!(updated.revision(), initial.revision() + 1);
                    routing.map(requested, &other);
                    let error =
                        workspace.as_ref().unwrap().list_tools().await.expect_err(
                            "revision without cwd change cannot rebind native identity",
                        );
                    assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                    routing.map(requested, &canonical);
                    conversations.push(
                        workspace
                            .as_ref()
                            .unwrap()
                            .get_conversation(conversation_id)
                            .await
                            .unwrap(),
                    );
                    conversations.pop().unwrap().close().await.unwrap();
                    routing_change_at = Some(routing.session_routes().len());
                    let changed = agent
                        .as_ref()
                        .unwrap()
                        .update_workspace(
                            UpdateWorkspaceRequest::new(
                                workspace_id.clone(),
                                updated.revision(),
                                "moved",
                                CwdRoutingFixture::context(&other),
                                initial.defaults().clone(),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    drop(workspace.take());
                    workspace = Some(Arc::new(
                        agent
                            .as_ref()
                            .unwrap()
                            .get_workspace(workspace_id)
                            .await
                            .unwrap(),
                    ));
                    assert_eq!(changed.context(), &CwdRoutingFixture::context(&other));
                    conversations.push(
                        workspace
                            .as_ref()
                            .unwrap()
                            .new_conversation(CwdRoutingFixture::conversation())
                            .await
                            .unwrap(),
                    );
                    assert_eq!(stats.next_session.load(Ordering::SeqCst), 2);
                }
            }
        })
        .await;
        // Release every fixture boundary before cleanup; observation remains the
        // primary failure even when cleanup reports additional diagnostics.
        routing.release.add_permits(8);
        routing.held_replies.lock().unwrap().clear();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(12));
        cleanup
            .settle(async {
                while let Some(joined) = creations.join_next().await {
                    if let Ok(conversation) = joined.expect("creation observer joins") {
                        conversations.push(conversation);
                    }
                }
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        if !creations.is_empty() {
            creations.abort_all();
            while creations.join_next().await.is_some() {}
        }
        for conversation in conversations {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = workspace {
            cleanup
                .settle(async {
                    Arc::try_unwrap(workspace)
                        .unwrap_or_else(|_| panic!("all workspace observers joined before close"))
                        .close()
                        .await
                })
                .await;
        }
        if let Some(agent) = agent {
            shutdowns.spawn(async move { agent.shutdown().await });
        }
        cleanup
            .settle(async {
                while let Some(joined) = shutdowns.join_next().await {
                    joined.expect("Agent shutdown observer joins")?;
                }
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        if !shutdowns.is_empty() {
            shutdowns.abort_all();
            while shutdowns.join_next().await.is_some() {}
        }
        let routes = routing.session_routes();
        if let Some(stop) = probe.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = probe.task.take() {
            cleanup.settle(async { (&mut task).await }).await;
            if !task.is_finished() {
                task.abort();
                let _ = task.await;
            }
        }
        cleanup.finish(observation);
        assert_eq!(stats.active.load(Ordering::SeqCst), 0);
        assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
        assert_eq!(
            stats.messages.load(Ordering::SeqCst),
            0,
            "cwd tests never generate model output"
        );
        for (index, route) in routes.into_iter().enumerate() {
            let expected = if routing_change_at.is_some_and(|boundary| index >= boundary) {
                other.as_str()
            } else {
                canonical.as_str()
            };
            assert_eq!(
                route.directory.as_deref(),
                Some(expected),
                "{} {} must retain the native identity owned at this lifecycle phase",
                route.method,
                route.path
            );
        }
    }
}

#[tokio::test]
async fn opencode_cwd_canonical_routing_control() {
    CwdCase::Canonical.verify().await;
}
#[tokio::test]
async fn opencode_cwd_alias_preserves_context_and_reopens_original_session() {
    CwdCase::AliasReopen.verify().await;
}
#[tokio::test]
async fn opencode_cwd_retarget_rejects_creation_before_native_effects() {
    CwdCase::RetargetCreation.verify().await;
}
#[tokio::test]
async fn opencode_cwd_retarget_rejects_catalog_discovery() {
    CwdCase::RetargetCatalog.verify().await;
}
#[tokio::test]
async fn opencode_cwd_retarget_rejects_reopen_without_losing_original_session() {
    CwdCase::RetargetReopen.verify().await;
}
#[tokio::test]
async fn opencode_cwd_anchor_survives_last_conversation_close() {
    CwdCase::EmptyRetarget.verify().await;
}
#[tokio::test]
async fn opencode_cwd_concurrent_retarget_rejects_only_changed_identity() {
    CwdCase::ConcurrentRetarget.verify().await;
}
#[tokio::test]
async fn opencode_cwd_revision_preserves_anchor_until_explicit_empty_move() {
    CwdCase::RevisionUpdate.verify().await;
}

#[tokio::test]
async fn opencode_cwd_alias_lost_commit_receipt_recovers_exact_native_session() {
    CwdCase::AliasReceiptLoss.verify().await;
}

impl CwdRoutingFixture {
    async fn verify_invalid_path(body: serde_json::Value) {
        use storage_kit::StateStoreBackend;
        use storage_kit::state::record::StateListRequest;

        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let mut probe = OpenCodeEventProbe::start(0).await;
        let routing = Self::new();
        let stats = Arc::clone(&probe.stats);
        *stats.cwd_routing.lock().unwrap() = Some(Arc::clone(&routing));
        let mut agent = None;
        let mut workspace = None;
        let mut shutdowns = JoinSet::new();
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(15), async {
            agent = Some(
                AgentBuilder::create(
                    probe.config(Duration::from_secs(2), Duration::from_secs(5)),
                    faults.storage(),
                )
                .build()
                .await
                .unwrap(),
            );
            routing.replies.lock().unwrap().push_back(body);
            let result = agent
                .as_ref()
                .unwrap()
                .new_workspace(Self::workspace(env!("CARGO_MANIFEST_DIR")))
                .await;
            let error = match result {
                Ok(unexpected) => {
                    workspace = Some(Arc::new(unexpected));
                    panic!("malformed native path must reject workspace construction");
                }
                Err(error) => error,
            };
            assert_eq!(
                error.kind(),
                ErrorKind::Protocol,
                "malformed native path retains protocol classification: {error:?}"
            );
            assert!(
                agent
                    .as_ref()
                    .unwrap()
                    .list_workspaces()
                    .await
                    .unwrap()
                    .is_empty(),
                "failed path validation cannot publish durable workspace state"
            );
            assert_eq!(stats.next_session.load(Ordering::SeqCst), 0);
            assert!(
                faults
                    .inner
                    .list(
                        StateListRequest::new("agents_kit_opencode_workspace_routing_v1").unwrap()
                    )
                    .await
                    .unwrap()
                    .records
                    .is_empty(),
                "failed path validation cannot leave an orphaned routing attachment"
            );
            workspace = Some(Arc::new(
                agent
                    .as_ref()
                    .unwrap()
                    .new_workspace(Self::workspace(env!("CARGO_MANIFEST_DIR")))
                    .await
                    .expect("a later valid path remains usable"),
            ));
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(8));
        if let Some(workspace) = workspace {
            cleanup
                .settle(async {
                    Arc::try_unwrap(workspace)
                        .unwrap_or_else(|_| panic!("all workspace observers joined before close"))
                        .close()
                        .await
                })
                .await;
        }
        if let Some(agent) = agent {
            shutdowns.spawn(async move { agent.shutdown().await });
        }
        cleanup
            .settle(async {
                while let Some(result) = shutdowns.join_next().await {
                    result.expect("shutdown observer joins")?;
                }
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        if !shutdowns.is_empty() {
            shutdowns.abort_all();
            while shutdowns.join_next().await.is_some() {}
        }
        if let Some(stop) = probe.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = probe.task.take() {
            cleanup.settle(async { (&mut task).await }).await;
            if !task.is_finished() {
                task.abort();
                let _ = task.await;
            }
        }
        cleanup.finish(observed);
        assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
        assert_eq!(stats.messages.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn opencode_cwd_missing_native_directory_rejects_without_durable_workspace() {
    CwdRoutingFixture::verify_invalid_path(serde_json::json!({})).await;
}
#[tokio::test]
async fn opencode_cwd_null_native_directory_rejects_without_durable_workspace() {
    CwdRoutingFixture::verify_invalid_path(serde_json::json!({"directory": null})).await;
}
#[tokio::test]
async fn opencode_cwd_relative_native_directory_rejects_without_durable_workspace() {
    CwdRoutingFixture::verify_invalid_path(serde_json::json!({"directory": "relative"})).await;
}
#[tokio::test]
async fn opencode_cwd_empty_native_directory_rejects_without_durable_workspace() {
    CwdRoutingFixture::verify_invalid_path(serde_json::json!({"directory": ""})).await;
}
#[tokio::test]
async fn opencode_cwd_non_string_native_directory_rejects_without_durable_workspace() {
    CwdRoutingFixture::verify_invalid_path(serde_json::json!({"directory": 7})).await;
}

#[derive(Clone, Copy)]
enum PredecessorSession {
    Bound,
    Creating,
}

impl CwdRoutingFixture {
    async fn verify_old_attachment(session: PredecessorSession) {
        use storage_kit::state::record::{StateKey, StateRecord};
        use storage_kit::{StateStoreBackend, StateTransactionBackend};

        let mut probe = OpenCodeEventProbe::start(0).await;
        let stats = Arc::clone(&probe.stats);
        let routing = Self::new();
        *stats.cwd_routing.lock().unwrap() = Some(Arc::clone(&routing));
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let config = probe.config(Duration::from_secs(2), Duration::from_secs(5));
        let mut agent = None;
        let mut workspace = None;
        let mut conversation = None;
        let mut shutdowns = JoinSet::new();
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(20), async {
            agent = Some(
                AgentBuilder::create(config.clone(), faults.storage())
                    .build()
                    .await
                    .unwrap(),
            );
            let agent_id = agent.as_ref().unwrap().id();
            workspace = Some(Arc::new(
                agent
                    .as_ref()
                    .unwrap()
                    .new_workspace(Self::workspace(env!("CARGO_MANIFEST_DIR")))
                    .await
                    .unwrap(),
            ));
            conversation = Some(
                workspace
                    .as_ref()
                    .unwrap()
                    .new_conversation(Self::conversation())
                    .await
                    .unwrap(),
            );
            let conversation_id = conversation.as_ref().unwrap().id();
            let workspace_id = workspace.as_ref().unwrap().id();
            drop(conversation.take());
            drop(workspace.take());
            let original = agent.take().unwrap();
            shutdowns.spawn(async move { original.shutdown().await });
            shutdowns.join_next().await.unwrap().unwrap().unwrap();

            // This is an explicit predecessor-format persistence fixture. It
            // does not claim to simulate a real lost commit. Version 1 predates
            // authoritative workspace routing attachments; neither Bound nor
            // Creating rows in that snapshot can be silently upgraded.
            let attachment_key =
                StateKey::new("agents_kit_opencode_attachments_v1", agent_id.to_string()).unwrap();
            let original = faults
                .inner
                .load(attachment_key.clone())
                .await
                .unwrap()
                .unwrap();
            let mut value = original.record.value.as_ref().clone();
            value["version"] = serde_json::json!(1);
            let binding_key = StateKey::new(
                "agents_kit_opencode_sessions_v1",
                format!("{workspace_id}:{conversation_id}"),
            )
            .unwrap();
            let mut transaction = faults.inner.begin_transaction().await.unwrap();
            transaction
                .put(StateRecord::new(attachment_key.clone(), value))
                .await
                .unwrap();
            // This key did not exist in the predecessor format. Remove it if
            // the current constructor authored it; no default native identity
            // may be manufactured when reopening the old attachment.
            let routing_key = StateKey::new(
                "agents_kit_opencode_workspace_routing_v1",
                format!("{agent_id}:{workspace_id}"),
            )
            .unwrap();
            transaction.delete(routing_key.clone()).await.unwrap();
            if matches!(session, PredecessorSession::Creating) {
                let binding = transaction
                    .load(binding_key.clone())
                    .await
                    .unwrap()
                    .expect("actual session binding exists");
                let mut value = binding.record.value.as_ref().clone();
                let correlation = value["state"]["correlation"].clone();
                value["state"] =
                    serde_json::json!({"phase": "creating", "correlation": correlation});
                transaction
                    .put(StateRecord::new(binding_key.clone(), value))
                    .await
                    .unwrap();
                // Bound publication initializes notification delivery in that
                // same transaction. A valid predecessor Creating snapshot has
                // no notification owner yet; remove that later-phase artifact.
                let notification_key = StateKey::new(
                    "agents_kit_conversation_notifications_v1",
                    conversation_id.to_string(),
                )
                .unwrap();
                assert!(
                    transaction
                        .load(notification_key.clone())
                        .await
                        .unwrap()
                        .is_some()
                );
                transaction.delete(notification_key).await.unwrap();
            }
            transaction.commit().await.unwrap();
            let before_attachment = faults.inner.load(attachment_key.clone()).await.unwrap();
            let before_binding = faults.inner.load(binding_key.clone()).await.unwrap();
            assert!(
                faults
                    .inner
                    .load(routing_key.clone())
                    .await
                    .unwrap()
                    .is_none()
            );
            let native_requests = stats.requests.load(Ordering::SeqCst);
            match AgentBuilder::reopen(config.clone(), faults.storage(), agent_id)
                .build()
                .await
            {
                Ok(unexpected) => {
                    agent = Some(unexpected);
                    panic!("predecessor attachment cannot synthesize routing authority");
                }
                Err(error) => assert_eq!(
                    error.kind(),
                    ErrorKind::InvalidState,
                    "old format must fail explicitly: {error:?}"
                ),
            }
            assert_eq!(
                stats.requests.load(Ordering::SeqCst),
                native_requests,
                "old format must reject before native initialization"
            );
            assert!(
                faults.inner.load(routing_key).await.unwrap().is_none(),
                "rejected old snapshot cannot manufacture native routing evidence"
            );
            assert_eq!(
                faults.inner.load(attachment_key).await.unwrap(),
                before_attachment,
                "rejection must not migrate the attachment"
            );
            assert_eq!(
                faults.inner.load(binding_key).await.unwrap(),
                before_binding,
                "rejection must not bind, delete, or rewrite old creation evidence"
            );
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(8));
        if let Some(conversation) = conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = workspace {
            cleanup
                .settle(async {
                    Arc::try_unwrap(workspace)
                        .unwrap_or_else(|_| panic!("all workspace observers joined before close"))
                        .close()
                        .await
                })
                .await;
        }
        if let Some(agent) = agent {
            shutdowns.spawn(async move { agent.shutdown().await });
        }
        cleanup
            .settle(async {
                while let Some(joined) = shutdowns.join_next().await {
                    joined.expect("shutdown task joins")?;
                }
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        if !shutdowns.is_empty() {
            shutdowns.abort_all();
            while shutdowns.join_next().await.is_some() {}
        }
        if let Some(stop) = probe.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = probe.task.take() {
            cleanup.settle(async { (&mut task).await }).await;
            if !task.is_finished() {
                task.abort();
                let _ = task.await;
            }
        }
        cleanup.finish(observed);
        assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn opencode_cwd_predecessor_bound_snapshot_rejects_before_native_or_state_changes() {
    CwdRoutingFixture::verify_old_attachment(PredecessorSession::Bound).await;
}
#[tokio::test]
async fn opencode_cwd_predecessor_creating_snapshot_rejects_without_adopting_native_identity() {
    CwdRoutingFixture::verify_old_attachment(PredecessorSession::Creating).await;
}

#[tokio::test]
async fn opencode_cwd_workspace_observer_loss_retains_one_accepted_native_binding() {
    CwdCase::WorkspaceObserverLoss.verify().await;
}

#[tokio::test]
async fn opencode_cwd_backend_windows_drive_uses_server_path_flavor() {
    CwdCase::WindowsDrive.verify().await;
}
#[tokio::test]
async fn opencode_cwd_backend_windows_unc_uses_server_path_flavor() {
    CwdCase::WindowsUnc.verify().await;
}
