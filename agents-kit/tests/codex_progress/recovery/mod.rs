use super::{
    JOURNAL, PREPARING, ProgressFault, ProgressGate, ProgressRun, ProgressStore, ProgressTools,
    ProgressTransaction, TOOLS,
};
use crate::{CodexFixture, CodexPeer, FirstStart, NativeCleanup};
use agents_kit::config::{AgentConfig, CodexConfig};
use agents_kit::error::ErrorKind;
use agents_kit::prompt::{PromptRequest, StructuredOutputRequest};
use agents_kit::{AgentBackend, AgentBuilder, AgentStorage, ConversationBackend, WorkspaceBackend};
use serde_json::json;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend};
use tokio::sync::Semaphore;
use tools_kit::Toolset;

struct RecoveryGate {
    // 0 rejects the original owner's clear; 1 holds one reconstructed owner's
    // repair commit receipt; 2 delegates normally after that receipt has been released.
    phase: AtomicUsize,
    rejected: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
}

#[derive(Clone)]
struct RecoveryStore {
    inner: ProgressStore,
    gate: Arc<RecoveryGate>,
}

struct RecoveryTransaction {
    inner: ProgressTransaction,
    gate: Arc<RecoveryGate>,
    hold_repair_receipt: bool,
}

impl StateStoreBackend for RecoveryStore {
    type Transaction = RecoveryTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(RecoveryTransaction {
            inner: self.inner.begin_transaction().await?,
            gate: Arc::clone(&self.gate),
            hold_repair_receipt: false,
        })
    }
}

impl StateTransactionBackend for RecoveryTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        if key.namespace().as_str() == "agents_kit_codex_prompts_v1" {
            if self.gate.phase.load(Ordering::SeqCst) == 0 {
                self.gate.rejected.fetch_add(1, Ordering::SeqCst);
                return Err(StateStoreError::unavailable(
                    "delete",
                    "injected original terminal clear failure",
                ));
            }
            if self
                .gate
                .phase
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.hold_repair_receipt = true;
            }
        }
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        self.inner.commit().await?;
        if self.hold_repair_receipt {
            self.gate.entered.add_permits(1);
            self.gate.release.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

#[tokio::test]
async fn codex_progress_cold_agent_recovery_never_promotes_a_reconstructed_context() {
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let progress = Arc::new(ProgressGate {
        fault: ProgressFault::None,
        selected: TOOLS,
        attempts: AtomicUsize::new(0),
        uncertain: AtomicBool::new(false),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        attempted_ids: std::sync::Mutex::new(Vec::new()),
        committed: std::sync::Mutex::new(Vec::new()),
        handler_labels: std::sync::Mutex::new(Vec::new()),
    });
    let recovery = Arc::new(RecoveryGate {
        phase: AtomicUsize::new(0),
        rejected: AtomicUsize::new(0),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let store = RecoveryStore {
        inner: ProgressStore {
            inner: raw.clone(),
            gate: Arc::clone(&progress),
        },
        gate: Arc::clone(&recovery),
    };
    let handlers = Arc::new(AtomicUsize::new(0));
    let effects = Arc::new(AtomicUsize::new(0));
    let tools = Toolset::from_backend(ProgressTools {
        gate: Arc::clone(&progress),
        handlers: Arc::clone(&handlers),
        effects: Arc::clone(&effects),
    });
    let CodexFixture {
        mut peer,
        agent,
        workspace,
        conversation,
        root,
    } = CodexFixture::build_with_toolset(
        CodexPeer::start_with(FirstStart::Reopenable).await,
        store.clone(),
        Some(tools),
    )
    .await;
    let agent_id = agent.id();
    let workspace_id = workspace.id();
    let conversation_id = conversation.id();
    let mut owned_agent = Some(agent);
    let mut owned_workspace = Some(workspace);
    let mut owned_conversation = Some(conversation);
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(20), async {
        let conversation = owned_conversation.as_ref().unwrap();
        let mut receiver = conversation.notification_stream().unwrap();
        let request = PromptRequest::new("structured terminal retained for reconstruction").unwrap()
            .with_structured_output(StructuredOutputRequest::new(json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false})).unwrap());
        let mut stream = conversation.prompt_stream(request).await.unwrap();
        let preparing = receiver.recv().await.unwrap().unwrap();
        ProgressRun::assert_progress(&preparing, PREPARING);
        let turn = peer.turn().await;
        peer.complete(&turn, r#"{"value":1}"#).await;
        let result = tokio::time::timeout(Duration::from_secs(8), stream.recv()).await.unwrap().unwrap();
        assert!(matches!(result, Err(error) if matches!(error.kind(), ErrorKind::Timeout | ErrorKind::Integration)), "failed durable cleanup cannot report terminal success");
        assert!(stream.recv().await.is_none());
        assert!(recovery.rejected.load(Ordering::SeqCst) > 0);
        let retained = raw.list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap()).await.unwrap();
        assert_eq!(retained.records.len(), 1);
        let record = &retained.records[0].record.value;
        assert_eq!(record["state"]["state"], "terminal");
        assert_eq!(record["state"]["outcome"]["outcome"], "success");
        assert_eq!(record["state"]["outcome"]["structured_output"], json!({"value":1}));
        assert!(record["output_schema"].is_object());
        assert_eq!(peer.history.lock().unwrap()["turns"][0]["status"], "completed");
        drop(receiver);
        drop(stream);
        drop(owned_conversation.take());
        drop(owned_workspace.take());
        drop(owned_agent.take());
        tokio::time::timeout(Duration::from_secs(4), async {
            while tokio::runtime::Handle::current().metrics().num_alive_tasks() > baseline + 2 {
                tokio::task::yield_now().await;
            }
        }).await.expect("original Agent runtime must join before reconstruction; only the peer and retained Lance actor remain");
        recovery.phase.store(1, Ordering::SeqCst);
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3)).unwrap().into(),
            Duration::from_secs(3), NonZeroUsize::new(1).unwrap(),
        ).unwrap();
        let tools = Toolset::from_backend(ProgressTools { gate: Arc::clone(&progress), handlers: Arc::clone(&handlers), effects: Arc::clone(&effects) });
        let rebuild = AgentBuilder::reopen(config, AgentStorage::new(store.clone()), agent_id.clone()).with_toolset(tools).build();
        tokio::pin!(rebuild);
        tokio::select! {
            result = &mut rebuild => panic!("rebuild returned before held terminal repair: {}", result.is_ok()),
            entered = recovery.entered.acquire() => entered.unwrap().forget(),
        }
        // The native transcript above is real fixture evidence, not rewritten
        // durable Kit state. Its structured success makes reopen instantiate a
        // validator context before this held repair receipt. The native commit
        // has settled, so a legitimate Process write cannot conflict with the
        // repair's old Lance snapshot. The reconstruction context remains owned.
        assert!(raw.list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap()).await.unwrap().records.is_empty(),
            "the authoritative repair commit precedes injected Process publication");
        peer.notify("item/started", ProgressRun::command("stream-thread", &turn, &root)).await;
        peer.request(1301, "currentTime/read", json!({"threadId":"stream-thread"})).await;
        tokio::select! {
            result = &mut rebuild => panic!("rebuild escaped the held repair: {}", result.is_ok()),
            response = peer.responses.recv() => assert_eq!(response.unwrap()["id"], 1301),
        }
        assert_eq!(progress.committed.lock().unwrap().iter().map(|(label, _)| label.clone()).collect::<Vec<_>>(), [PREPARING]);
        assert_eq!(progress.attempts.load(Ordering::SeqCst), 0, "recovery-only context cannot attempt tool progress");
        assert_eq!(handlers.load(Ordering::SeqCst), 0);
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        recovery.release.add_permits(1);
        let agent = owned_agent.insert(rebuild.await.unwrap());
        assert_eq!(peer.initializations.load(Ordering::SeqCst), 2);
        let workspace = owned_workspace.insert(agent.get_workspace(workspace_id.clone()).await.unwrap());
        let conversation = owned_conversation.insert(workspace.get_conversation(conversation_id.clone()).await.unwrap());
        let mut receiver = conversation.notification_stream().unwrap();
        let replay = receiver.recv().await.unwrap().unwrap();
        assert_eq!(replay, preparing);
        assert!(raw.list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap()).await.unwrap().records.is_empty());
        assert_eq!(peer.requests.lock().unwrap().iter().filter(|request| request["method"] == "turn/start").count(), 1, "reconstruction cannot reexecute the original prompt");
        assert_eq!(peer.interrupts.load(Ordering::SeqCst), 0, "completed native work needs no invented cancellation");
        let persisted = raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
        assert_eq!(persisted.records[0].record.value["pending"].as_array().unwrap().iter().filter(|entry| entry["kind"]["kind"] == "progress").count(), 1);
        drop(receiver);
    }).await;
    recovery.phase.store(2, Ordering::SeqCst);
    recovery.release.add_permits(1);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
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
        .settle(async {
            peer.shutdown().await;
            Ok::<(), agents_kit::KitError>(())
        })
        .await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed);
}
