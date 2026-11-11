mod causal;
mod recovery;

use crate::{CodexFixture, CodexPeer, FirstStart, NativeCleanup};
use agents_kit::error::ErrorKind;
use agents_kit::notification::{
    ConversationNotification, ConversationNotificationKind, NotificationResolution, ProcessState,
};
use agents_kit::prompt::{PromptEvent, PromptRequest, PromptStream, StructuredOutputRequest};
use agents_kit::{AgentBackend, ConversationBackend, WorkspaceBackend};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::{Semaphore, oneshot};
use tools_kit::declaration::{ToolInfo, ToolName};
use tools_kit::execution::{ToolHandlerInput, ToolHandlerOutput, ToolResult};
use tools_kit::schema::JsonSchema;
use tools_kit::{ToolError, ToolHandlerBackend, Toolset, ToolsetBackend};

const PREPARING: &str = "Preparing response";
const TOOLS: &str = "Tool calls requested";
const JOURNAL: &str = "agents_kit_conversation_notifications_v1";

#[derive(Clone, Copy, Debug)]
enum ProgressFault {
    None,
    ConflictOnce,
    LostCommitReply,
    RejectCommit,
    HoldCommit,
    HoldCommitReceipt,
    HoldThenReject,
    UnknownCommit,
}

struct ProgressGate {
    fault: ProgressFault,
    selected: &'static str,
    attempts: AtomicUsize,
    uncertain: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
    attempted_ids: std::sync::Mutex<Vec<Value>>,
    committed: std::sync::Mutex<Vec<(String, Value)>>,
    handler_labels: std::sync::Mutex<Vec<Vec<String>>>,
}

#[derive(Clone)]
struct ProgressStore {
    inner: StateStore,
    gate: Arc<ProgressGate>,
}

struct ProgressTransaction {
    inner: StateTransaction,
    gate: Arc<ProgressGate>,
    previous_ids: BTreeSet<String>,
    appended: Vec<(String, Value)>,
}

impl StateStoreBackend for ProgressStore {
    type Transaction = ProgressTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if key.namespace().as_str() == JOURNAL && self.gate.uncertain.load(Ordering::SeqCst) {
            return Err(StateStoreError::unavailable(
                "load",
                "injected unreadable publication receipt",
            ));
        }
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(ProgressTransaction {
            inner: self.inner.begin_transaction().await?,
            gate: Arc::clone(&self.gate),
            previous_ids: BTreeSet::new(),
            appended: Vec::new(),
        })
    }
}

impl StateTransactionBackend for ProgressTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let journal = key.namespace().as_str() == JOURNAL;
        let snapshot = self.inner.load(key).await?;
        if journal && let Some(snapshot) = &snapshot {
            self.previous_ids = snapshot.record.value["pending"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["id"].as_str().unwrap().to_owned())
                .collect();
        }
        Ok(snapshot)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        if record.key.namespace().as_str() == JOURNAL {
            self.appended = record.value["pending"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|entry| !self.previous_ids.contains(entry["id"].as_str().unwrap()))
                .filter(|entry| entry["kind"]["kind"] == "progress")
                .map(|entry| {
                    (
                        entry["kind"]["label"].as_str().unwrap().to_owned(),
                        entry["id"].clone(),
                    )
                })
                .collect();
            // Recognize new Progress publication, not every later journal rewrite
            // that still contains the same pending entry.
        }
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let selected = self
            .appended
            .iter()
            .find(|(label, _)| label == self.gate.selected);
        let attempt = selected.map(|(_, id)| {
            self.gate.attempted_ids.lock().unwrap().push(id.clone());
            self.gate.attempts.fetch_add(1, Ordering::SeqCst)
        });
        if let Some(attempt) = attempt {
            match self.gate.fault {
                ProgressFault::ConflictOnce if attempt == 0 => {
                    self.inner.rollback().await?;
                    return Err(StateStoreError::transaction_conflict(
                        "commit",
                        "injected progress conflict",
                    ));
                }
                ProgressFault::RejectCommit => {
                    self.inner.rollback().await?;
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "injected rejected progress publication",
                    ));
                }
                ProgressFault::HoldCommit | ProgressFault::HoldThenReject => {
                    self.gate.entered.add_permits(1);
                    self.gate.release.acquire().await.unwrap().forget();
                    if matches!(self.gate.fault, ProgressFault::HoldThenReject) {
                        self.inner.rollback().await?;
                        return Err(StateStoreError::unavailable(
                            "commit",
                            "injected rejected progress publication",
                        ));
                    }
                }
                ProgressFault::None
                | ProgressFault::ConflictOnce
                | ProgressFault::LostCommitReply
                | ProgressFault::HoldCommitReceipt
                | ProgressFault::UnknownCommit => {}
            }
        }
        self.inner.commit().await?;
        self.gate.committed.lock().unwrap().extend(self.appended);
        if attempt.is_some() && matches!(self.gate.fault, ProgressFault::HoldCommitReceipt) {
            // The native commit is already authoritative. Only its caller's
            // receipt is held while later cancellation mutates other records.
            self.gate.entered.add_permits(1);
            self.gate.release.acquire().await.unwrap().forget();
        }
        if attempt.is_some()
            && matches!(
                self.gate.fault,
                ProgressFault::LostCommitReply | ProgressFault::UnknownCommit
            )
        {
            self.gate.uncertain.store(
                matches!(self.gate.fault, ProgressFault::UnknownCommit),
                Ordering::SeqCst,
            );
            return Err(StateStoreError::unavailable(
                "commit",
                "injected lost progress commit receipt",
            ));
        }
        Ok(())
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

struct ProgressTools {
    gate: Arc<ProgressGate>,
    handlers: Arc<AtomicUsize>,
    effects: Arc<AtomicUsize>,
}

struct ProgressHandler {
    effects: Arc<AtomicUsize>,
}

impl ToolsetBackend for ProgressTools {
    type Handler = ProgressHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        Ok(vec![ToolInfo::new(
            ToolName::new("progress_probe").unwrap(),
            JsonSchema::strict(json!({
                "type":"object", "properties":{}, "additionalProperties":false
            })),
        )])
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        if name != ToolName::new("progress_probe").unwrap() {
            return Err(ToolError::UnknownTool { name });
        }
        let labels = self
            .gate
            .committed
            .lock()
            .unwrap()
            .iter()
            .map(|(label, _)| label.clone())
            .collect();
        self.gate.handler_labels.lock().unwrap().push(labels);
        self.handlers.fetch_add(1, Ordering::SeqCst);
        Ok(ProgressHandler {
            effects: Arc::clone(&self.effects),
        })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        Ok(())
    }
}

impl ToolHandlerBackend for ProgressHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Invoke(_) => {
                self.effects.fetch_add(1, Ordering::SeqCst);
                Ok(ToolHandlerOutput::Completed(ToolResult::text(
                    "tool complete",
                )))
            }
            _ => Err(ToolError::Unsupported {
                operation: "unexpected fixture continuation".into(),
            }),
        }
    }
}

struct ProgressRun {
    fixture: CodexFixture<ProgressStore>,
    raw: StateStore,
    gate: Arc<ProgressGate>,
    handlers: Arc<AtomicUsize>,
    effects: Arc<AtomicUsize>,
    baseline: usize,
}

impl ProgressRun {
    async fn new(peer: CodexPeer, fault: ProgressFault, selected: &'static str) -> Self {
        // Peer already owns one task; the raw Lance actor remains alive through
        // revocation checks because this test deliberately retains its state.
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let gate = Arc::new(ProgressGate {
            fault,
            selected,
            attempts: AtomicUsize::new(0),
            uncertain: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            attempted_ids: std::sync::Mutex::new(Vec::new()),
            committed: std::sync::Mutex::new(Vec::new()),
            handler_labels: std::sync::Mutex::new(Vec::new()),
        });
        let handlers = Arc::new(AtomicUsize::new(0));
        let effects = Arc::new(AtomicUsize::new(0));
        let tools = Toolset::from_backend(ProgressTools {
            gate: Arc::clone(&gate),
            handlers: Arc::clone(&handlers),
            effects: Arc::clone(&effects),
        });
        let fixture = CodexFixture::build_with_toolset(
            peer,
            ProgressStore {
                inner: raw.clone(),
                gate: Arc::clone(&gate),
            },
            Some(tools),
        )
        .await;
        Self {
            fixture,
            raw,
            gate,
            handlers,
            effects,
            baseline,
        }
    }

    fn assert_progress(notification: &ConversationNotification, label: &str) {
        assert!(
            matches!(notification.kind(), ConversationNotificationKind::Progress { label: actual, detail: None } if actual == label),
            "{notification:?}"
        );
    }

    async fn accept_turn(&mut self, stream: &mut PromptStream) -> String {
        let turn = tokio::select! {
            turn = self.fixture.peer.turn() => turn,
            outcome = stream.recv() => panic!("prompt terminated before native acceptance: {outcome:?}"),
        };
        self.fixture.peer.notify("item/agentMessage/delta", json!({
            "threadId":"stream-thread", "turnId":turn, "itemId":"receipt-fence", "delta":"accepted receipt"
        })).await;
        assert!(
            matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::OutputDelta { text })) if text == "accepted receipt")
        );
        turn
    }

    async fn barrier(&mut self, id: i64) {
        self.fixture
            .peer
            .request(id, "currentTime/read", json!({"threadId":"stream-thread"}))
            .await;
        let response =
            tokio::time::timeout(Duration::from_secs(3), self.fixture.peer.responses.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(response["id"], id);
        assert!(response["result"]["currentTimeAt"].is_i64());
    }

    fn command(thread: &str, turn: &str, root: &std::path::Path) -> Value {
        json!({"threadId":thread,"turnId":turn,"startedAtMs":1,"item":{
            "type":"commandExecution","id":"command","status":"inProgress","command":"private command",
            "cwd":root,"commandActions":[],"aggregatedOutput":null,"exitCode":null
        }})
    }

    fn progress_labels(&self) -> Vec<String> {
        self.gate
            .committed
            .lock()
            .unwrap()
            .iter()
            .map(|(label, _)| label.clone())
            .collect()
    }

    fn native_starts(&self) -> usize {
        self.fixture
            .peer
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count()
    }

    async fn finish(
        self,
        observed: Result<Result<(), tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>,
        revoked: bool,
    ) {
        self.gate.uncertain.store(false, Ordering::SeqCst);
        self.gate.release.add_permits(1);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        if revoked {
            cleanup
                .settle(async {
                    self.fixture.revoke(self.baseline + 1).await;
                    Ok::<(), agents_kit::KitError>(())
                })
                .await;
        } else {
            cleanup.settle(self.fixture.conversation.close()).await;
            cleanup.settle(self.fixture.workspace.close()).await;
            cleanup.settle(self.fixture.agent.shutdown()).await;
            cleanup
                .settle(async {
                    self.fixture.peer.shutdown().await;
                    Ok::<(), agents_kit::KitError>(())
                })
                .await;
            cleanup
                .settle(async { std::fs::remove_dir_all(self.fixture.root) })
                .await;
        }
        cleanup.finish(observed);
    }
}

#[derive(Clone, Copy, Debug)]
enum ToolEvidence {
    Command,
    CommandApproval,
    FileApproval,
    Dynamic,
}

impl ToolEvidence {
    async fn emit(self, run: &ProgressRun, turn: &str, request_id: i64) {
        match self {
            Self::Command => run.fixture.peer.notify("item/started", ProgressRun::command("stream-thread", turn, &run.fixture.root)).await,
            Self::CommandApproval => run.fixture.peer.request(request_id, "item/commandExecution/requestApproval", json!({
                "threadId":"stream-thread","turnId":turn,"itemId":"command","startedAtMs":1,
                "kind":"command","availableDecisions":["accept","cancel"]
            })).await,
            Self::FileApproval => run.fixture.peer.request(request_id, "item/fileChange/requestApproval", json!({
                "threadId":"stream-thread","turnId":turn,"itemId":"file","startedAtMs":1,"reason":"change"
            })).await,
            Self::Dynamic => run.fixture.peer.request(request_id, "item/tool/call", json!({
                "threadId":"stream-thread","turnId":turn,"callId":format!("call-{request_id}"),
                "tool":"progress_probe","arguments":{}
            })).await,
        }
    }
}

#[tokio::test]
async fn codex_progress_is_durable_bounded_and_precedes_owned_tool_effects() {
    for evidence in [
        ToolEvidence::Command,
        ToolEvidence::CommandApproval,
        ToolEvidence::FileApproval,
        ToolEvidence::Dynamic,
    ] {
        let mut run = ProgressRun::new(CodexPeer::start().await, ProgressFault::None, TOOLS).await;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(15), async {
            let mut receiver = run.fixture.conversation.notification_stream().unwrap();
            let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("milestones").unwrap()).await.unwrap();
            let preparing = tokio::time::timeout(Duration::from_secs(3), receiver.recv()).await.unwrap().unwrap().unwrap();
            ProgressRun::assert_progress(&preparing, PREPARING);
            let turn = run.accept_turn(&mut stream).await;
            evidence.emit(&run, &turn, 501).await;
            let tools = tokio::time::timeout(Duration::from_secs(3), receiver.recv()).await.unwrap().unwrap().unwrap();
            ProgressRun::assert_progress(&tools, TOOLS);
            drop(receiver);
            let mut receiver = run.fixture.conversation.notification_stream().unwrap();
            let replay = receiver.recv().await.unwrap().unwrap();
            assert_eq!(replay, tools);
            match evidence {
                ToolEvidence::Command => assert!(matches!(receiver.recv().await.unwrap().unwrap().kind(), ConversationNotificationKind::Process { state: ProcessState::Running, .. })),
                ToolEvidence::CommandApproval | ToolEvidence::FileApproval => {
                    let interaction = receiver.recv().await.unwrap().unwrap();
                    assert!(matches!(interaction.kind(), ConversationNotificationKind::Interaction { .. }));
                    assert_eq!(run.fixture.conversation.resolve_notification(interaction.id().clone(), NotificationResolution::Allow).await.unwrap_err().kind(), ErrorKind::Conflict);
                    assert_eq!(run.fixture.peer.responses.recv().await.unwrap()["id"], 501);
                }
                ToolEvidence::Dynamic => {
                    assert_eq!(run.fixture.peer.responses.recv().await.unwrap()["id"], 501);
                    assert_eq!(run.handlers.load(Ordering::SeqCst), 1);
                    assert_eq!(run.effects.load(Ordering::SeqCst), 1);
                    assert_eq!(*run.gate.handler_labels.lock().unwrap(), vec![vec![PREPARING.to_owned(), TOOLS.to_owned()]], "owned handler creation follows committed milestones");
                }
            }
            for _ in 0..3 {
                run.fixture.peer.notify("item/started", ProgressRun::command("stream-thread", &turn, &run.fixture.root)).await;
            }
            run.barrier(502).await;
            assert_eq!(run.progress_labels(), [PREPARING, TOOLS]);
            run.fixture.peer.complete(&turn, "done").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "done"));
            assert!(stream.recv().await.is_none());
            assert_eq!(run.native_starts(), 1);
            drop(receiver);
        }).await;
        run.finish(observed, false).await;
    }
}

#[tokio::test]
async fn codex_progress_omits_wrong_descendant_early_late_and_historical_tool_facts() {
    let (release, released) = oneshot::channel();
    let root = std::env::temp_dir();
    let peer = CodexPeer::start_with(FirstStart::EarlyProgress {
        events: vec![json!({"method":"item/started","params":ProgressRun::command("stream-thread", "turn-1", &root)})],
        release: released,
    }).await;
    let mut run = ProgressRun::new(peer, ProgressFault::None, TOOLS).await;
    let mut release = Some(release);
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let mut receiver = run.fixture.conversation.notification_stream().unwrap();
            let mut stream = run
                .fixture
                .conversation
                .prompt_stream(PromptRequest::new("owner exclusions").unwrap())
                .await
                .unwrap();
            ProgressRun::assert_progress(&receiver.recv().await.unwrap().unwrap(), PREPARING);
            // The old Process contract can observe the early command, but progress
            // cannot invent its prompt attribution before authoritative acceptance.
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .kind(),
                ConversationNotificationKind::Process { .. }
            ));
            assert_eq!(run.progress_labels(), [PREPARING]);
            release.take().unwrap().send(()).unwrap();
            let turn = run.accept_turn(&mut stream).await;
            for (thread, wrong_turn) in [
                ("stream-thread", "foreign-turn"),
                ("child-thread", "child-turn"),
                ("child-thread", turn.as_str()),
            ] {
                run.fixture
                    .peer
                    .notify(
                        "item/started",
                        ProgressRun::command(thread, wrong_turn, &run.fixture.root),
                    )
                    .await;
            }
            run.barrier(601).await;
            assert_eq!(run.progress_labels(), [PREPARING]);
            run.fixture.peer.complete(&turn, "done").await;
            assert!(matches!(
                stream.recv().await,
                Some(Ok(PromptEvent::Completed(_)))
            ));
            run.fixture
                .peer
                .notify(
                    "item/started",
                    ProgressRun::command("stream-thread", &turn, &run.fixture.root),
                )
                .await;
            run.barrier(602).await;
            assert_eq!(run.progress_labels(), [PREPARING]);
            drop(receiver);
        },
    )
    .await;
    if let Some(release) = release {
        let _ = release.send(());
    }
    run.finish(observed, false).await;

    let peer = CodexPeer::start_with(FirstStart::RecoveredTurn {
        status: "completed".into(), error: Value::Null,
        items: vec![json!({"type":"commandExecution","id":"old-command","status":"completed","command":"private","cwd":std::env::temp_dir(),"commandActions":[],"exitCode":0}),
            json!({"type":"agentMessage","id":"answer","text":"historical terminal","phase":"final_answer"})],
    }).await;
    let run = ProgressRun::new(peer, ProgressFault::None, TOOLS).await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let response = tokio::time::timeout(
                Duration::from_secs(3),
                run.fixture
                    .conversation
                    .prompt(PromptRequest::new("history").unwrap()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(response.output(), "historical terminal");
            assert_eq!(
                run.progress_labels(),
                [PREPARING],
                "history cannot backfill native tool milestones"
            );
        },
    )
    .await;
    run.finish(observed, false).await;
}

#[tokio::test]
async fn codex_progress_preparing_failure_prevents_native_dispatch() {
    let run = ProgressRun::new(
        CodexPeer::start().await,
        ProgressFault::RejectCommit,
        PREPARING,
    )
    .await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let error = tokio::time::timeout(
                Duration::from_secs(4),
                run.fixture
                    .conversation
                    .prompt(PromptRequest::new("must not dispatch").unwrap()),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert_eq!(run.native_starts(), 0);
            assert_eq!(run.gate.attempts.load(Ordering::SeqCst), 1);
            assert!(run.progress_labels().is_empty());
            assert!(
                run.raw
                    .list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap())
                    .await
                    .unwrap()
                    .records
                    .is_empty()
            );
        },
    )
    .await;
    run.finish(observed, false).await;
}

#[tokio::test]
async fn codex_progress_conflict_and_lost_receipt_preserve_one_identity_and_turn() {
    for fault in [ProgressFault::ConflictOnce, ProgressFault::LostCommitReply] {
        let mut run = ProgressRun::new(CodexPeer::start().await, fault, TOOLS).await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                let mut receiver = run.fixture.conversation.notification_stream().unwrap();
                let mut stream = run
                    .fixture
                    .conversation
                    .prompt_stream(PromptRequest::new("one publication").unwrap())
                    .await
                    .unwrap();
                ProgressRun::assert_progress(&receiver.recv().await.unwrap().unwrap(), PREPARING);
                let turn = run.accept_turn(&mut stream).await;
                ToolEvidence::Command.emit(&run, &turn, 701).await;
                ProgressRun::assert_progress(
                    &tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                    TOOLS,
                );
                let ids = run.gate.attempted_ids.lock().unwrap().clone();
                assert_eq!(
                    ids.len(),
                    if matches!(fault, ProgressFault::ConflictOnce) {
                        2
                    } else {
                        1
                    }
                );
                assert!(ids.iter().all(|id| id == &ids[0]));
                assert_eq!(run.progress_labels(), [PREPARING, TOOLS]);
                run.fixture.peer.complete(&turn, "done").await;
                assert!(matches!(
                    stream.recv().await,
                    Some(Ok(PromptEvent::Completed(_)))
                ));
                assert_eq!(run.native_starts(), 1);
                drop(receiver);
            },
        )
        .await;
        run.finish(observed, false).await;
    }
}

#[tokio::test]
async fn codex_progress_pending_publication_blocks_terminal_success() {
    let mut run =
        ProgressRun::new(CodexPeer::start().await, ProgressFault::HoldCommit, TOOLS).await;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(15), async {
        let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("pending publication").unwrap()).await.unwrap();
        let turn = run.accept_turn(&mut stream).await;
        ToolEvidence::Command.emit(&run, &turn, 801).await;
        tokio::time::timeout(Duration::from_secs(3), run.gate.entered.acquire()).await.unwrap().unwrap().forget();
        run.fixture.peer.complete(&turn, "terminal pending publication").await;
        assert!(tokio::time::timeout(Duration::from_millis(40), stream.recv()).await.is_err(), "terminal cannot overtake an admitted publication");
        run.gate.release.add_permits(1);
        assert!(matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::Completed(response))) if response.output() == "terminal pending publication"));
        assert_eq!(run.progress_labels(), [PREPARING, TOOLS]);
    }).await;
    run.finish(observed, false).await;
}

#[tokio::test]
async fn codex_progress_failure_cancels_without_replaying_or_executing_callbacks() {
    for evidence in [
        ToolEvidence::Command,
        ToolEvidence::CommandApproval,
        ToolEvidence::FileApproval,
        ToolEvidence::Dynamic,
    ] {
        let mut run = ProgressRun::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await,
            ProgressFault::HoldThenReject,
            TOOLS,
        )
        .await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                let mut stream = run
                    .fixture
                    .conversation
                    .prompt_stream(PromptRequest::new("publication failure").unwrap())
                    .await
                    .unwrap();
                let turn = run.accept_turn(&mut stream).await;
                evidence.emit(&run, &turn, 901).await;
                tokio::time::timeout(Duration::from_secs(3), run.gate.entered.acquire())
                    .await
                    .unwrap()
                    .unwrap()
                    .forget();
                assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
                assert_eq!(run.effects.load(Ordering::SeqCst), 0);
                assert!(run.fixture.peer.responses.try_recv().is_err());
                evidence.emit(&run, &turn, 902).await;
                run.gate.release.add_permits(1);
                let error = tokio::time::timeout(Duration::from_secs(5), stream.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
            eprintln!("publication-failure terminal before next admission: {error:?}");
                assert_eq!(
                    error.kind(),
                    ErrorKind::Integration,
                    "publication remains the primary failure"
                );
                assert!(stream.recv().await.is_none());
                assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), 1);
                assert_eq!(run.native_starts(), 1);
                assert_eq!(run.gate.attempts.load(Ordering::SeqCst), 1);
                assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
                assert_eq!(run.effects.load(Ordering::SeqCst), 0);
                assert!(
                    run.fixture.peer.responses.try_recv().is_err(),
                    "failure does not send a second native callback mutation"
                );
                assert_eq!(run.progress_labels(), [PREPARING]);
            let retained = run.raw.list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap()).await.unwrap();
            eprintln!("retained prompt disposition: {:?}", retained.records.iter().map(|snapshot| {
                let value = &snapshot.record.value;
                (&value["state"]["state"], &value["state"]["outcome"]["outcome"], &value["cancellation"], &value["waiter"])
            }).collect::<Vec<_>>());
            let mut next = run.fixture.conversation.prompt_stream(PromptRequest::new("after confirmed cancellation").unwrap()).await.unwrap();
            let next_turn = run.accept_turn(&mut next).await;
            run.fixture.peer.complete(&next_turn, "new accepted work").await;
            assert!(matches!(next.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "new accepted work"));
            assert_eq!(run.native_starts(), 2);
            assert_eq!(run.progress_labels(), [PREPARING, PREPARING]);
            assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.effects.load(Ordering::SeqCst), 0);
            assert!(run.fixture.peer.responses.try_recv().is_err(), "old callbacks stay suppressed after new admission");
            },
        )
        .await;
        run.finish(observed, false).await;
    }
}

#[tokio::test]
async fn codex_progress_uncertain_publication_and_interrupt_retain_native_ownership() {
    let peer = CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await;
    peer.rejections.lock().unwrap().insert(
        "turn/interrupt".into(),
        json!({"code":-32603,"message":"interruption could not be confirmed"}),
    );
    let mut run = ProgressRun::new(peer, ProgressFault::UnknownCommit, TOOLS).await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let mut stream = run
                .fixture
                .conversation
                .prompt_stream(PromptRequest::new("uncertain publication").unwrap())
                .await
                .unwrap();
            let turn = run.accept_turn(&mut stream).await;
            ToolEvidence::Dynamic.emit(&run, &turn, 1001).await;
            let error = tokio::time::timeout(Duration::from_secs(8), stream.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert!(matches!(
                error.kind(),
                ErrorKind::Timeout | ErrorKind::Integration
            ));
            assert!(stream.recv().await.is_none());
            assert_eq!(run.native_starts(), 1);
            assert_eq!(
                run.gate.attempts.load(Ordering::SeqCst),
                1,
                "unknown publication cannot be replayed"
            );
            assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.effects.load(Ordering::SeqCst), 0);
            assert!(run.fixture.peer.responses.try_recv().is_err());
            let retained = run
                .raw
                .list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap())
                .await
                .unwrap();
            assert_eq!(
                retained.records.len(),
                1,
                "unconfirmed native work retains its durable owner"
            );
            let next = tokio::time::timeout(
                Duration::from_secs(2),
                run.fixture
                    .conversation
                    .prompt(PromptRequest::new("cannot replace uncertainty").unwrap()),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert!(matches!(
                next.kind(),
                ErrorKind::Busy | ErrorKind::Closed | ErrorKind::InvalidState
            ));
            assert_eq!(run.native_starts(), 1);
        },
    )
    .await;
    run.finish(observed, true).await;
}

#[tokio::test]
async fn codex_progress_failed_publication_cannot_be_hidden_by_a_native_terminal() {
    let peer = CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await;
    peer.rejections.lock().unwrap().insert(
        "turn/interrupt".into(),
        json!({"code":-32603,"message":"turn already terminal"}),
    );
    let mut run = ProgressRun::new(peer, ProgressFault::HoldThenReject, TOOLS).await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let mut stream = run
                .fixture
                .conversation
                .prompt_stream(PromptRequest::new("terminal race").unwrap())
                .await
                .unwrap();
            let turn = run.accept_turn(&mut stream).await;
            ToolEvidence::Command.emit(&run, &turn, 1101).await;
            tokio::time::timeout(Duration::from_secs(3), run.gate.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            run.fixture
                .peer
                .complete(&turn, "native terminal cannot erase local failure")
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(40), stream.recv())
                    .await
                    .is_err()
            );
            while run.fixture.peer.history.lock().unwrap()["turns"][0]["status"] != "completed" { tokio::task::yield_now().await; }
            run.gate.release.add_permits(1);
            let error = tokio::time::timeout(Duration::from_secs(5), stream.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            eprintln!("publication-failure terminal before next admission: {error:?}");
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert!(stream.recv().await.is_none());
            assert_eq!(run.native_starts(), 1);
            assert_eq!(run.gate.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(run.progress_labels(), [PREPARING]);
            assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "completed", "native completion remains authoritative even if interruption was rejected");
            let retained = run.raw.list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap()).await.unwrap();
            eprintln!("retained prompt disposition: {:?}", retained.records.iter().map(|snapshot| {
                let value = &snapshot.record.value;
                (&value["state"]["state"], &value["state"]["outcome"]["outcome"], &value["cancellation"], &value["waiter"])
            }).collect::<Vec<_>>());
            let mut next = run.fixture.conversation.prompt_stream(PromptRequest::new("after reconciled native completion").unwrap()).await.unwrap();
            let next_turn = run.accept_turn(&mut next).await;
            run.fixture.peer.complete(&next_turn, "new accepted work").await;
            assert!(matches!(next.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "new accepted work"));
            assert_eq!(run.native_starts(), 2);
            assert_eq!(run.gate.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(run.progress_labels(), [PREPARING, PREPARING]);
            assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.effects.load(Ordering::SeqCst), 0);
            assert!(run.fixture.peer.responses.try_recv().is_err());
        },
    )
    .await;
    run.finish(observed, false).await;
}

#[tokio::test]
async fn codex_progress_structured_terminal_validation_preserves_publication_authority() {
    for fault in [ProgressFault::HoldCommit, ProgressFault::HoldThenReject] {
        for (native_output, valid) in [
            (r#"{"answer":"valid"}"#, true),
            ("not JSON", false),
            (r#"{"answer":2}"#, false),
        ] {
            let peer = CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await;
            peer.rejections.lock().unwrap().insert(
                "turn/interrupt".into(),
                json!({"code":-32603,"message":"turn already terminal"}),
            );
            let mut run = ProgressRun::new(peer, fault, TOOLS).await;
            let observed = NativeCleanup::observe(
                tokio::time::Instant::now() + Duration::from_secs(15),
                async {
                    let request = PromptRequest::new("structured terminal authority")
                        .unwrap()
                        .with_structured_output(
                            StructuredOutputRequest::new(json!({
                                "type":"object",
                                "properties":{"answer":{"type":"string"}},
                                "required":["answer"],
                                "additionalProperties":false
                            }))
                            .unwrap(),
                        );
                    let mut stream = run.fixture.conversation.prompt_stream(request).await.unwrap();
                    let turn = run.accept_turn(&mut stream).await;
                    ToolEvidence::Command.emit(&run, &turn, 1151).await;
                    tokio::time::timeout(Duration::from_secs(3), run.gate.entered.acquire())
                        .await.unwrap().unwrap().forget();
                    run.fixture.peer.complete(&turn, native_output).await;
                    while run.fixture.peer.history.lock().unwrap()["turns"][0]["status"] != "completed" {
                        tokio::task::yield_now().await;
                    }
                    assert!(
                        tokio::time::timeout(Duration::from_millis(40), stream.recv()).await.is_err(),
                        "neither valid nor invalid structured output settles a pending publication"
                    );
                    run.gate.release.add_permits(1);
                    let result = tokio::time::timeout(Duration::from_secs(5), stream.recv()).await.unwrap().unwrap();
                    eprintln!("structured native output {native_output}, fault {fault:?}: {result:?}");
                    let failed_publication = matches!(fault, ProgressFault::HoldThenReject);
                    if failed_publication {
                        let error = result.unwrap_err();
                        assert_eq!(error.kind(), ErrorKind::Integration, "publication remains primary for native output {native_output}");
                    } else if valid {
                        let PromptEvent::Completed(response) = result.unwrap() else { panic!("expected structured completion"); };
                        assert_eq!(response.structured_output(), Some(&json!({"answer":"valid"})));
                    } else {
                        assert_eq!(result.unwrap_err().kind(), ErrorKind::Protocol, "successful publication preserves independent native output validation");
                    }
                    assert!(stream.recv().await.is_none());
                    assert_eq!(run.gate.attempts.load(Ordering::SeqCst), 1);
                    assert_eq!(run.native_starts(), 1);
                    assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "completed");
                    assert_eq!(run.progress_labels(), if failed_publication { vec![PREPARING] } else { vec![PREPARING, TOOLS] });
                    assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
                    assert_eq!(run.effects.load(Ordering::SeqCst), 0);
                    assert!(run.fixture.peer.responses.try_recv().is_err());
                    let retained = run.raw.list(StateListRequest::new("agents_kit_codex_prompts_v1").unwrap()).await.unwrap();
            eprintln!("retained prompt disposition: {:?}", retained.records.iter().map(|snapshot| {
                let value = &snapshot.record.value;
                (&value["state"]["state"], &value["state"]["outcome"]["outcome"], &value["cancellation"], &value["waiter"])
            }).collect::<Vec<_>>());
            let mut next = run.fixture.conversation.prompt_stream(PromptRequest::new("after structured native settlement").unwrap()).await.unwrap();
                    let next_turn = run.accept_turn(&mut next).await;
                    run.fixture.peer.complete(&next_turn, "next prompt completed").await;
                    assert!(matches!(next.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "next prompt completed"));
                    assert!(next.recv().await.is_none());
                    assert_eq!(run.native_starts(), 2);
                    assert_eq!(run.gate.attempts.load(Ordering::SeqCst), 1);
                },
            ).await;
            run.finish(observed, false).await;
        }
    }
}

#[tokio::test]
async fn codex_progress_observer_drop_keeps_the_original_prompt_and_milestone_identity() {
    let mut run = ProgressRun::new(CodexPeer::start().await, ProgressFault::None, TOOLS).await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let mut receiver = run.fixture.conversation.notification_stream().unwrap();
            let mut stream = run
                .fixture
                .conversation
                .prompt_stream(PromptRequest::new("detached observer").unwrap())
                .await
                .unwrap();
            let preparing = receiver.recv().await.unwrap().unwrap();
            ProgressRun::assert_progress(&preparing, PREPARING);
            let turn = run.accept_turn(&mut stream).await;
            drop(stream);
            drop(receiver);
            ToolEvidence::Dynamic.emit(&run, &turn, 1201).await;
            let response =
                tokio::time::timeout(Duration::from_secs(3), run.fixture.peer.responses.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(response["id"], 1201);
            assert_eq!(run.effects.load(Ordering::SeqCst), 1);
            let mut receiver = run.fixture.conversation.notification_stream().unwrap();
            let replay = receiver.recv().await.unwrap().unwrap();
            assert_eq!(replay, preparing);
            ProgressRun::assert_progress(&receiver.recv().await.unwrap().unwrap(), TOOLS);
            run.fixture
                .peer
                .complete(&turn, "completed without observer")
                .await;
            run.barrier(1202).await;
            assert_eq!(run.progress_labels(), [PREPARING, TOOLS]);
            assert_eq!(run.native_starts(), 1);
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), 0);
            drop(receiver);
        },
    )
    .await;
    run.finish(observed, false).await;
}

#[tokio::test]
async fn codex_progress_full_journal_keeps_unacknowledged_facts_and_blocks_dispatch() {
    let mut run = ProgressRun::new(CodexPeer::start().await, ProgressFault::None, TOOLS).await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let mut first = run
                .fixture
                .conversation
                .prompt_stream(PromptRequest::new("create real journal").unwrap())
                .await
                .unwrap();
            let turn = run.accept_turn(&mut first).await;
            run.fixture.peer.complete(&turn, "ready").await;
            assert!(matches!(
                first.recv().await,
                Some(Ok(PromptEvent::Completed(_)))
            ));
            let journals = run
                .raw
                .list(StateListRequest::new(JOURNAL).unwrap())
                .await
                .unwrap();
            assert_eq!(journals.records.len(), 1);
            let key = journals.records[0].record.key.clone();
            let mut transaction = run.raw.begin_transaction().await.unwrap();
            let mut record = transaction.load(key.clone()).await.unwrap().unwrap().record;
            let value = Arc::make_mut(&mut record.value);
            let original = value["pending"][0].clone();
            assert_eq!(original["kind"]["label"], PREPARING);
            let mut pending = vec![original.clone()];
            for sequence in 1..4096 {
                let mut entry = original.clone();
                entry["id"] = json!(format!("notification_{:032x}", sequence));
                entry["sequence"] = json!(sequence);
                entry["kind"] = json!({"kind":"message","text":"retained caller fact"});
                pending.push(entry);
            }
            value["pending"] = json!(pending);
            value["next_sequence"] = json!(4096);
            let expected = record.value.clone();
            transaction.put(record).await.unwrap();
            transaction.commit().await.unwrap();
            let error = run
                .fixture
                .conversation
                .prompt(PromptRequest::new("must preserve capacity").unwrap())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert_eq!(
                run.native_starts(),
                1,
                "capacity failure precedes another native dispatch"
            );
            assert_eq!(run.progress_labels(), [PREPARING]);
            assert_eq!(
                run.raw.load(key).await.unwrap().unwrap().record.value,
                expected,
                "unacknowledged facts cannot be evicted to admit progress"
            );
        },
    )
    .await;
    run.finish(observed, false).await;
}

impl ToolEvidence {
    fn callback(self, thread: &str, turn: &str, id: i64) -> Value {
        match self {
            Self::Command => panic!("command item is a notification, not a callback"),
            Self::CommandApproval => {
                json!({"id":id,"method":"item/commandExecution/requestApproval","params":{
                    "threadId":thread,"turnId":turn,"itemId":"callback-command","startedAtMs":1,
                    "kind":"command","availableDecisions":["accept","cancel"]
                }})
            }
            Self::FileApproval => {
                json!({"id":id,"method":"item/fileChange/requestApproval","params":{
                    "threadId":thread,"turnId":turn,"itemId":"callback-file","startedAtMs":1,"reason":"change"
                }})
            }
            Self::Dynamic => json!({"id":id,"method":"item/tool/call","params":{
                "threadId":thread,"turnId":turn,"callId":format!("callback-{id}"),
                "tool":"progress_probe","arguments":{}
            }}),
        }
    }
}

#[tokio::test]
async fn codex_progress_early_callbacks_keep_existing_effects_without_claiming_a_milestone() {
    for evidence in [
        ToolEvidence::CommandApproval,
        ToolEvidence::FileApproval,
        ToolEvidence::Dynamic,
    ] {
        let (release, released) = oneshot::channel();
        let peer = CodexPeer::start_with(FirstStart::EarlyProgress {
            events: vec![evidence.callback("stream-thread", "turn-1", 1401)],
            release: released,
        })
        .await;
        let mut release = Some(release);
        let mut run = ProgressRun::new(peer, ProgressFault::None, TOOLS).await;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(15), async {
            let mut receiver = run.fixture.conversation.notification_stream().unwrap();
            let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("early callback remains observable").unwrap()).await.unwrap();
            ProgressRun::assert_progress(&receiver.recv().await.unwrap().unwrap(), PREPARING);
            let interaction = match evidence {
                ToolEvidence::CommandApproval | ToolEvidence::FileApproval => {
                    let item = receiver.recv().await.unwrap().unwrap();
                    assert!(matches!(item.kind(), ConversationNotificationKind::Interaction { .. }), "early approval retains its existing Interaction, without invented Progress");
                    Some(item)
                }
                ToolEvidence::Dynamic => {
                    while run.effects.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
                    assert_eq!(run.handlers.load(Ordering::SeqCst), 1);
                    assert_eq!(run.effects.load(Ordering::SeqCst), 1);
                    assert_eq!(*run.gate.handler_labels.lock().unwrap(), vec![vec![PREPARING.to_owned()]], "early native tool work does not imply an authoritative live prompt turn receipt");
                    None
                }
                ToolEvidence::Command => unreachable!("callbacks only"),
            };
            assert_eq!(run.progress_labels(), [PREPARING]);
            release.take().unwrap().send(()).unwrap();
            let turn = run.accept_turn(&mut stream).await;
            assert_eq!(turn, "turn-1");
            assert_eq!(run.progress_labels(), [PREPARING], "receiving the turn receipt cannot backfill an earlier callback milestone");
            if let Some(interaction) = interaction {
                assert_eq!(run.fixture.conversation.resolve_notification(interaction.id().clone(), NotificationResolution::Allow).await.unwrap_err().kind(), ErrorKind::Conflict);
            }
            let response = run.fixture.peer.responses.recv().await.unwrap();
            assert_eq!(response["id"], 1401);
            assert!(response["result"].is_object());
            run.fixture.peer.complete(&turn, "completed after early native callback").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(_)))));
            run.barrier(1402).await;
            assert_eq!(run.progress_labels(), [PREPARING]);
            assert_eq!(run.native_starts(), 1);
            drop(receiver);
        }).await;
        if let Some(release) = release {
            let _ = release.send(());
        }
        run.finish(observed, false).await;
    }
}

#[tokio::test]
async fn codex_progress_wrong_descendant_and_late_callbacks_cannot_claim_live_tool_work() {
    for evidence in [
        ToolEvidence::CommandApproval,
        ToolEvidence::FileApproval,
        ToolEvidence::Dynamic,
    ] {
        for (thread, turn) in [
            ("stream-thread", "wrong-turn"),
            ("child-thread", "child-turn"),
        ] {
            let mut run =
                ProgressRun::new(CodexPeer::start().await, ProgressFault::None, TOOLS).await;
            let effects = Arc::clone(&run.effects);
            let handlers = Arc::clone(&run.handlers);
            let observed = NativeCleanup::observe(
                tokio::time::Instant::now() + Duration::from_secs(15),
                async {
                    let mut stream = run
                        .fixture
                        .conversation
                        .prompt_stream(PromptRequest::new("callback scope exclusions").unwrap())
                        .await
                        .unwrap();
                    let owned_turn = run.accept_turn(&mut stream).await;
                    run.fixture
                        .peer
                        .events
                        .send(evidence.callback(thread, turn, 1501))
                        .await
                        .unwrap();
                    if matches!(
                        evidence,
                        ToolEvidence::CommandApproval | ToolEvidence::FileApproval
                    ) {
                        let response = run.fixture.peer.responses.recv().await.unwrap();
                        assert_eq!(response["id"], 1501);
                        assert!(
                            response["error"].is_object(),
                            "wrong-turn approval keeps its existing callback rejection"
                        );
                    }
                    run.barrier(1502).await;
                    assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
                    assert_eq!(run.effects.load(Ordering::SeqCst), 0);
                    assert_eq!(run.progress_labels(), [PREPARING]);
                    run.fixture
                        .peer
                        .complete(&owned_turn, "exact owning turn completes")
                        .await;
                    assert!(matches!(
                        stream.recv().await,
                        Some(Ok(PromptEvent::Completed(_)))
                    ));
                    run.fixture
                        .peer
                        .events
                        .send(evidence.callback("stream-thread", &owned_turn, 1503))
                        .await
                        .unwrap();
                    if matches!(
                        evidence,
                        ToolEvidence::CommandApproval | ToolEvidence::FileApproval
                    ) {
                        let response = run.fixture.peer.responses.recv().await.unwrap();
                        assert_eq!(response["id"], 1503);
                        assert!(
                            response["error"].is_object(),
                            "late approval keeps its existing missing-owner rejection"
                        );
                    }
                    run.barrier(1504).await;
                    assert!(
                        run.fixture.peer.responses.try_recv().is_err(),
                        "suppressed dynamic callbacks have no synthetic reply"
                    );
                    assert_eq!(run.handlers.load(Ordering::SeqCst), 0);
                    assert_eq!(run.effects.load(Ordering::SeqCst), 0);
                    assert_eq!(run.progress_labels(), [PREPARING]);
                    assert_eq!(run.native_starts(), 1);
                    assert!(stream.recv().await.is_none());
                },
            )
            .await;
            run.finish(observed, false).await;
            assert_eq!(handlers.load(Ordering::SeqCst), 0);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
    }
}
