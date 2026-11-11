use super::{
    JOURNAL, PREPARING, ProgressFault, ProgressGate, ProgressRun, ProgressStore,
    ProgressTransaction, TOOLS, ToolEvidence,
};
use crate::{CodexFixture, CodexPeer, FirstStart, NativeCleanup};
use agents_kit::error::{ErrorKind, ErrorPhase};
use agents_kit::notification::ConversationNotificationKind;
use agents_kit::prompt::{PromptEvent, PromptRequest, PromptStream};
use agents_kit::{AgentBackend, ConversationBackend, WorkspaceBackend};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend};
use tokio::sync::{Semaphore, oneshot};
use tools_kit::declaration::{ToolInfo, ToolName};
use tools_kit::execution::{ToolHandlerInput, ToolHandlerOutput, ToolResult};
use tools_kit::interaction::{
    ToolInteractionCapabilities, ToolInteractionId, ToolInteractionKind, ToolInteractionRequest,
};
use tools_kit::schema::JsonSchema;
use tools_kit::{ToolError, ToolHandlerBackend, Toolset, ToolsetBackend};

const PROMPTS: &str = "agents_kit_codex_prompts_v1";
const ROUTES: &str = "agents_kit_codex_thread_routes_v1";
const CONTINUATIONS: &str = "agents_kit_codex_continuations_v1";

#[derive(Clone)]
enum ReadStage {
    Route(StateKey),
    Prompt,
    TerminalTransaction,
    Continuations,
    CancellationCommit(&'static str),
    TimeoutIntent,
    AbortIntentReceipt(StateKey),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ToolPause {
    Acquisition,
    Invocation,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ToolCancellation {
    Completed,
    Cancelled,
    Failed,
    Interaction,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RetirementFault {
    None,
    NativeConflict,
    UnknownBeforeCommit,
    UnknownAfterCommit,
    HeldReceipt,
    HeldFailureReceipt,
    ResolvingReceipt,
}

enum RetirementGate {
    Idle,
    Armed {
        fault: RetirementFault,
        obligation: StateKey,
        notification: Value,
    },
}

struct CausalGate {
    next: Mutex<Option<ReadStage>>,
    entered: Semaphore,
    release: Semaphore,
    suspended_tool: bool,
    tool_pause: Mutex<Option<ToolPause>>,
    tool_cancellation: Mutex<ToolCancellation>,
    tool_entered: Semaphore,
    tool_release: Semaphore,
    handlers: AtomicUsize,
    active_handlers: AtomicUsize,
    effects: AtomicUsize,
    cancellations: AtomicUsize,
    tool_shutdown_fails: AtomicBool,
    tool_shutdowns: AtomicUsize,
    cancellation_commit_conflicts: AtomicUsize,
    timeout_intent_conflicts: AtomicUsize,
    retirement: Mutex<RetirementGate>,
    retirement_attempts: AtomicUsize,
    retirement_receipts: AtomicUsize,
    retirement_conflicts: AtomicUsize,
}

impl CausalGate {
    fn arm(&self, stage: ReadStage) {
        assert!(self.next.lock().unwrap().replace(stage).is_none());
    }

    fn claim(&self, key: &StateKey, transactional: bool, terminal: bool) -> bool {
        let mut next = self.next.lock().unwrap();
        let matches = match next.as_ref() {
            Some(ReadStage::Route(route)) => !transactional && key == route,
            Some(ReadStage::Prompt) => !transactional && key.namespace().as_str() == PROMPTS,
            Some(ReadStage::TerminalTransaction) => {
                transactional && terminal && key.namespace().as_str() == PROMPTS
            }
            Some(
                ReadStage::Continuations
                | ReadStage::CancellationCommit(_)
                | ReadStage::TimeoutIntent
                | ReadStage::AbortIntentReceipt(_),
            )
            | None => false,
        };
        if matches {
            next.take();
        }
        matches
    }

    fn claim_list(&self, request: &StateListRequest) -> bool {
        let mut next = self.next.lock().unwrap();
        let matches = matches!(next.as_ref(), Some(ReadStage::Continuations))
            && request.namespace().as_str() == CONTINUATIONS;
        if matches {
            next.take();
        }
        matches
    }

    fn claim_cancellation_commit(&self, record: &StateRecord) -> bool {
        let mut next = self.next.lock().unwrap();
        let matches = matches!(next.as_ref(), Some(ReadStage::CancellationCommit(cancellation))
            if record.key.namespace().as_str() == PROMPTS
                && record.value["state"]["state"] == "running"
                && record.value["cancellation"] == *cancellation);
        if matches {
            next.take();
        }
        matches
    }

    fn claim_timeout_intent(&self, record: &StateRecord) -> bool {
        let mut next = self.next.lock().unwrap();
        let matches = matches!(next.as_ref(), Some(ReadStage::TimeoutIntent))
            && record.key.namespace().as_str() == PROMPTS
            && record.value["state"]["state"] == "running"
            && record.value["waiter"] == "timed_out"
            && record.value["cancellation"] == "requested";
        if matches {
            next.take();
        }
        matches
    }

    fn claim_abort_intent_receipt(&self, record: &StateRecord) -> bool {
        let mut next = self.next.lock().unwrap();
        let matches = matches!(next.as_ref(), Some(ReadStage::AbortIntentReceipt(expected))
            if &record.key == expected
                && record.value["state"]["state"] == "running"
                && record.value["waiter"] == "waiting"
                && record.value["cancellation"] == "requested");
        if matches {
            next.take();
        }
        matches
    }

    async fn hold_tool(&self, stage: ToolPause) {
        let claimed = {
            let mut next = self.tool_pause.lock().unwrap();
            if next.as_ref() == Some(&stage) {
                next.take();
                true
            } else {
                false
            }
        };
        if claimed {
            self.tool_entered.add_permits(1);
            self.tool_release.acquire().await.unwrap().forget();
        }
    }

    async fn hold(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
}

#[derive(Clone)]
struct CausalStore {
    inner: ProgressStore,
    gate: Arc<CausalGate>,
}

struct CausalTransaction {
    inner: ProgressTransaction,
    gate: Arc<CausalGate>,
    hold_cancellation_commit: bool,
    hold_abort_intent_receipt: bool,
    held_timeout_intent: bool,
    retiring: bool,
    resolving_receipt: bool,
}

impl StateStoreBackend for CausalStore {
    type Transaction = CausalTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if self.gate.claim(&key, false, false) {
            self.gate.hold().await;
        }
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        if self.gate.claim_list(&request) {
            self.gate.hold().await;
        }
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(CausalTransaction {
            inner: self.inner.begin_transaction().await?,
            gate: Arc::clone(&self.gate),
            hold_cancellation_commit: false,
            hold_abort_intent_receipt: false,
            held_timeout_intent: false,
            retiring: false,
            resolving_receipt: false,
        })
    }
}

impl StateTransactionBackend for CausalTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let snapshot = self.inner.load(key.clone()).await?;
        let terminal = snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.record.value["state"]["state"] == "terminal");
        if self.gate.claim(&key, true, terminal) {
            self.gate.hold().await;
        }
        Ok(snapshot)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.hold_cancellation_commit |= self.gate.claim_cancellation_commit(&record);
        self.hold_abort_intent_receipt |= self.gate.claim_abort_intent_receipt(&record);
        {
            let retirement = self.gate.retirement.lock().unwrap();
            if let RetirementGate::Armed {
                fault: RetirementFault::ResolvingReceipt,
                notification,
                ..
            } = &*retirement
                && record.key.namespace().as_str() == JOURNAL
            {
                self.resolving_receipt |=
                    record.value["pending"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|entry| {
                            &entry["id"] == notification && entry["state"]["state"] == "resolving"
                        });
            }
        }
        if self.gate.claim_timeout_intent(&record) {
            self.held_timeout_intent = true;
            self.gate.hold().await;
        }
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.retiring |= matches!(&*self.gate.retirement.lock().unwrap(), RetirementGate::Armed { obligation, .. } if obligation == &key);
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        if self.hold_abort_intent_receipt {
            self.inner.commit().await?;
            self.gate.hold().await;
            return Ok(());
        }
        if self.hold_cancellation_commit {
            self.gate.hold().await;
        }
        let retirement_fault = match &*self.gate.retirement.lock().unwrap() {
            RetirementGate::Idle => RetirementFault::None,
            RetirementGate::Armed { fault, .. } => *fault,
        };
        if self.resolving_receipt {
            self.inner.commit().await?;
            self.gate.hold().await;
            return Ok(());
        }
        if self.retiring {
            self.gate.retirement_attempts.fetch_add(1, Ordering::SeqCst);
            match retirement_fault {
                RetirementFault::NativeConflict => self.gate.hold().await,
                RetirementFault::UnknownBeforeCommit => {
                    self.inner.rollback().await?;
                    self.gate.retirement_receipts.fetch_add(1, Ordering::SeqCst);
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "retirement reply lost before application",
                    ));
                }
                RetirementFault::UnknownAfterCommit => {
                    self.inner.commit().await?;
                    self.gate.retirement_receipts.fetch_add(1, Ordering::SeqCst);
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "retirement reply lost after application",
                    ));
                }
                RetirementFault::HeldReceipt => {
                    self.inner.commit().await?;
                    self.gate.hold().await;
                    self.gate.retirement_receipts.fetch_add(1, Ordering::SeqCst);
                    return Ok(());
                }
                RetirementFault::HeldFailureReceipt => {
                    self.inner.rollback().await?;
                    self.gate.hold().await;
                    self.gate.retirement_receipts.fetch_add(1, Ordering::SeqCst);
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "retirement failure reply held after definite rollback",
                    ));
                }
                RetirementFault::None | RetirementFault::ResolvingReceipt => {}
            }
        }
        let result = self.inner.commit().await;
        if self.retiring && matches!(&result, Err(StateStoreError::TransactionConflict { .. })) {
            self.gate
                .retirement_conflicts
                .fetch_add(1, Ordering::SeqCst);
        }
        if self.hold_cancellation_commit
            && matches!(&result, Err(StateStoreError::TransactionConflict { .. }))
        {
            self.gate
                .cancellation_commit_conflicts
                .fetch_add(1, Ordering::SeqCst);
        }
        if self.held_timeout_intent
            && matches!(&result, Err(StateStoreError::TransactionConflict { .. }))
        {
            self.gate
                .timeout_intent_conflicts
                .fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

struct CausalTools {
    gate: Arc<CausalGate>,
}

struct CausalHandler {
    gate: Arc<CausalGate>,
}

impl Drop for CausalHandler {
    fn drop(&mut self) {
        self.gate.active_handlers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ToolsetBackend for CausalTools {
    type Handler = CausalHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        let mut tool = ToolInfo::new(
            ToolName::new("progress_probe").unwrap(),
            JsonSchema::strict(json!({
                "type":"object", "properties":{}, "additionalProperties":false
            })),
        );
        tool.interactions = ToolInteractionCapabilities {
            text: true,
            ..Default::default()
        };
        Ok(vec![tool])
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        assert_eq!(name.as_str(), "progress_probe");
        self.gate.handlers.fetch_add(1, Ordering::SeqCst);
        self.gate.hold_tool(ToolPause::Acquisition).await;
        self.gate.active_handlers.fetch_add(1, Ordering::SeqCst);
        Ok(CausalHandler {
            gate: Arc::clone(&self.gate),
        })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        self.gate.tool_shutdowns.fetch_add(1, Ordering::SeqCst);
        if self.gate.tool_shutdown_fails.load(Ordering::SeqCst) {
            return Err(ToolError::InvalidConfiguration {
                message: "distinct owned Toolset shutdown failure".to_owned(),
            });
        }
        Ok(())
    }
}

impl ToolHandlerBackend for CausalHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Invoke(invocation) => {
                self.gate.effects.fetch_add(1, Ordering::SeqCst);
                self.gate.hold_tool(ToolPause::Invocation).await;
                if self.gate.suspended_tool {
                    Ok(ToolHandlerOutput::Interaction(ToolInteractionRequest {
                        id: ToolInteractionId::new(),
                        tool_name: ToolName::new("progress_probe").unwrap(),
                        call_id: invocation.call_id,
                        prompt: "A caller answer is required".into(),
                        kind: ToolInteractionKind::Text { multiline: false },
                    }))
                } else {
                    Ok(ToolHandlerOutput::Completed(ToolResult::text(
                        "tool complete",
                    )))
                }
            }
            ToolHandlerInput::Cancel { .. } => {
                self.gate.cancellations.fetch_add(1, Ordering::SeqCst);
                match *self.gate.tool_cancellation.lock().unwrap() {
                    ToolCancellation::Completed => Ok(ToolHandlerOutput::Completed(
                        ToolResult::text("tool cancelled"),
                    )),
                    ToolCancellation::Cancelled => Err(ToolError::Cancelled {
                        tool: ToolName::new("progress_probe").unwrap(),
                        reason: Some("fixture cancellation settled".to_owned()),
                    }),
                    ToolCancellation::Failed => Err(ToolError::InvalidConfiguration {
                        message: "fixture cancellation failure".to_owned(),
                    }),
                    ToolCancellation::Interaction => {
                        Ok(ToolHandlerOutput::Interaction(ToolInteractionRequest {
                            id: ToolInteractionId::new(),
                            tool_name: ToolName::new("progress_probe").unwrap(),
                            call_id: None,
                            prompt: "Cancellation must not request another answer".into(),
                            kind: ToolInteractionKind::Text { multiline: false },
                        }))
                    }
                }
            }
            ToolHandlerInput::Interaction(_) => Err(ToolError::Unsupported {
                operation: "unexpected fixture answer".into(),
            }),
        }
    }
}

struct CausalRun {
    fixture: CodexFixture<CausalStore>,
    raw: StateStore,
    progress: Arc<ProgressGate>,
    gate: Arc<CausalGate>,
}

impl CausalRun {
    async fn new(peer: CodexPeer, fault: ProgressFault, suspended_tool: bool) -> Self {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let progress = Arc::new(ProgressGate {
            fault,
            selected: TOOLS,
            attempts: AtomicUsize::new(0),
            uncertain: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            attempted_ids: Mutex::new(Vec::new()),
            committed: Mutex::new(Vec::new()),
            handler_labels: Mutex::new(Vec::new()),
        });
        let gate = Arc::new(CausalGate {
            next: Mutex::new(None),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            suspended_tool,
            tool_pause: Mutex::new(None),
            tool_cancellation: Mutex::new(ToolCancellation::Completed),
            tool_entered: Semaphore::new(0),
            tool_release: Semaphore::new(0),
            handlers: AtomicUsize::new(0),
            active_handlers: AtomicUsize::new(0),
            effects: AtomicUsize::new(0),
            cancellations: AtomicUsize::new(0),
            tool_shutdown_fails: AtomicBool::new(false),
            tool_shutdowns: AtomicUsize::new(0),
            cancellation_commit_conflicts: AtomicUsize::new(0),
            timeout_intent_conflicts: AtomicUsize::new(0),
            retirement: Mutex::new(RetirementGate::Idle),
            retirement_attempts: AtomicUsize::new(0),
            retirement_receipts: AtomicUsize::new(0),
            retirement_conflicts: AtomicUsize::new(0),
        });
        let store = CausalStore {
            inner: ProgressStore {
                inner: raw.clone(),
                gate: Arc::clone(&progress),
            },
            gate: Arc::clone(&gate),
        };
        let tools = Toolset::from_backend(CausalTools {
            gate: Arc::clone(&gate),
        });
        let fixture = CodexFixture::build_with_toolset(peer, store, Some(tools)).await;
        Self {
            fixture,
            raw,
            progress,
            gate,
        }
    }

    fn labels(&self) -> Vec<String> {
        self.progress
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

    async fn prompt_record(&self) -> Option<Value> {
        let records = self
            .raw
            .list(StateListRequest::new(PROMPTS).unwrap())
            .await
            .unwrap();
        assert!(records.records.len() <= 1);
        records
            .records
            .first()
            .map(|record| record.record.value.as_ref().clone())
    }

    async fn wait_terminal(&self) {
        while self
            .prompt_record()
            .await
            .is_none_or(|record| record["state"]["state"] != "terminal")
        {
            tokio::task::yield_now().await;
        }
    }

    async fn accepted(&mut self, stream: &mut PromptStream) -> String {
        let turn = tokio::select! {
            turn = self.fixture.peer.turn() => turn,
            observed = stream.recv() => panic!("prompt ended before native acceptance: {observed:?}"),
        };
        self.fixture.peer.notify("item/agentMessage/delta", json!({
            "threadId":"stream-thread", "turnId":turn, "itemId":"acceptance", "delta":"accepted"
        })).await;
        assert!(
            matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "accepted")
        );
        while self
            .prompt_record()
            .await
            .is_none_or(|record| record["state"]["state"] != "running")
        {
            tokio::task::yield_now().await;
        }
        turn
    }

    async fn barrier(peer: &mut CodexPeer, id: i64) -> Vec<Value> {
        peer.request(id, "currentTime/read", json!({"threadId":"stream-thread"}))
            .await;
        Self::responses_before(peer, id).await
    }

    async fn responses_before(peer: &mut CodexPeer, id: i64) -> Vec<Value> {
        let mut other = Vec::new();
        loop {
            let response = peer.responses.recv().await.unwrap();
            if response["id"] == id {
                return other;
            }
            other.push(response);
        }
    }

    async fn command(&self, turn: &str) {
        self.fixture
            .peer
            .notify(
                "item/started",
                ProgressRun::command("stream-thread", turn, &self.fixture.root),
            )
            .await;
    }

    async fn next_prompt(&mut self) {
        let mut next = self
            .fixture
            .conversation
            .prompt_stream(PromptRequest::new("next generation").unwrap())
            .await
            .unwrap();
        let turn = self.accepted(&mut next).await;
        self.fixture.peer.complete(&turn, "next complete").await;
        assert!(
            matches!(next.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "next complete")
        );
        assert!(next.recv().await.is_none());
    }

    async fn finish(
        self,
        observed: Result<Result<(), tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>,
    ) {
        self.gate.next.lock().unwrap().take();
        *self.gate.retirement.lock().unwrap() = RetirementGate::Idle;
        self.gate.release.add_permits(16);
        self.gate.tool_pause.lock().unwrap().take();
        self.gate.tool_release.add_permits(16);
        self.progress.release.add_permits(16);
        self.progress.uncertain.store(false, Ordering::SeqCst);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
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
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_progress_early_terminal_stays_ineligible_after_the_start_receipt() {
    let (release, released) = oneshot::channel();
    let mut release = Some(release);
    let mut run = CausalRun::new(
        CodexPeer::start_with(FirstStart::DeferredStart(released)).await,
        ProgressFault::None,
        false,
    )
    .await;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(15), async {
        let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("early native completion").unwrap()).await.unwrap();
        let turn = run.fixture.peer.turn().await;
        run.fixture.peer.complete(&turn, "early complete").await;
        run.wait_terminal().await;
        assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "completed");
        run.gate.arm(ReadStage::TerminalTransaction);
        release.take().unwrap().send(()).unwrap();
        run.gate.entered.acquire().await.unwrap().forget();
        // Native acceptance is known while its terminal transaction load is
        // held. The preceding terminal is real correlated native history.
        run.command(&turn).await;
        assert!(CausalRun::barrier(&mut run.fixture.peer, 2101).await.is_empty());
        let labels = run.labels();
        run.gate.release.add_permits(1);
        assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "early complete"));
        assert!(stream.recv().await.is_none());
        assert_eq!(labels, [PREPARING], "late work cannot revive milestone eligibility after native terminal evidence");
        assert_eq!(run.progress.attempts.load(Ordering::SeqCst), 0);
        assert_eq!(run.gate.handlers.load(Ordering::SeqCst), 0);
        assert_eq!(run.native_starts(), 1);
    }).await;
    if let Some(release) = release {
        let _ = release.send(());
    }
    run.finish(observed).await;
}

#[tokio::test]
async fn codex_progress_observed_callback_retains_its_failed_owner_across_route_lookup() {
    for evidence in [
        ToolEvidence::CommandApproval,
        ToolEvidence::FileApproval,
        ToolEvidence::Dynamic,
    ] {
        let peer = CodexPeer::start_with(FirstStart::ResponsiveHistory).await;
        let mut run = CausalRun::new(peer, ProgressFault::HoldThenReject, false).await;
        let (history_release, history_released) = oneshot::channel();
        let mut history_release = Some(history_release);
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
            let mut first = run.fixture.conversation.prompt_stream(PromptRequest::new("failed original generation").unwrap()).await.unwrap();
            let first_turn = run.accepted(&mut first).await;
            run.command(&first_turn).await;
            run.progress.entered.acquire().await.unwrap().forget();
            run.fixture.peer.events.send(evidence.callback("stream-thread", &first_turn, 2201)).await.unwrap();
            run.fixture.peer.request_sent(2201).await;
            // This foreign-scope control follows the old callback on the same
            // native stream, but never loads the exact root route key.
            run.fixture.peer.request(2200, "currentTime/read", json!({"threadId":"foreign-thread"})).await;
            run.fixture.peer.request_sent(2200).await;
            *run.fixture.peer.held_history.lock().unwrap() = Some(history_released);
            let routes = run.raw.list(StateListRequest::new(ROUTES).unwrap()).await.unwrap();
            assert_eq!(routes.records.len(), 1);
            run.gate.arm(ReadStage::Route(routes.records[0].record.key.clone()));
            run.progress.release.add_permits(1);
            let held_route = tokio::select! {
                entered = run.gate.entered.acquire() => {
                    entered.unwrap().forget();
                    assert!(run.prompt_record().await.is_some(), "callback lookup is already observed while its original owner is retained");
                    true
                }
                response = run.fixture.peer.responses.recv() => {
                    let response = response.unwrap();
                    assert_eq!(response["id"], 2200, "only the ordered foreign control may respond after safe early suppression");
                    assert!(response.get("error").is_some());
                    run.gate.next.lock().unwrap().take();
                    false
                }
            };
            history_release.take().unwrap().send(()).unwrap();
            let error = first.recv().await.unwrap().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert_eq!(error.context().phase, ErrorPhase::StateCommit);
            assert!(first.recv().await.is_none());
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), 1);
            assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "interrupted", "native history independently proves original settlement while callback routing is held");
            assert!(run.prompt_record().await.is_none());
            let mut next = run.fixture.conversation.prompt_stream(PromptRequest::new("replacement generation").unwrap()).await.unwrap();
            let next_turn = run.fixture.peer.turn().await;
            assert_eq!(run.native_starts(), 2);
            assert_eq!(run.labels(), [PREPARING, PREPARING]);
            run.gate.release.add_permits(1);
            let mut responses = if held_route {
                CausalRun::responses_before(&mut run.fixture.peer, 2200).await
            } else { Vec::new() };
            responses.extend(CausalRun::barrier(&mut run.fixture.peer, 2202).await);
            run.fixture.peer.complete(&next_turn, "replacement complete").await;
            assert!(matches!(next.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "replacement complete"));
            assert!(next.recv().await.is_none());
            responses.extend(CausalRun::barrier(&mut run.fixture.peer, 2203).await);
            assert!(responses.is_empty(), "an already-observed failed callback cannot become a native response on a replacement generation: {responses:?}");
            assert_eq!(run.gate.handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 0);
        }).await;
        if let Some(release) = history_release {
            let _ = release.send(());
        }
        run.finish(observed).await;
    }
}

#[tokio::test]
async fn codex_progress_held_publication_rechecks_deadline_and_abort_before_effects() {
    for expires in [true, false] {
        for evidence in [
            ToolEvidence::CommandApproval,
            ToolEvidence::FileApproval,
            ToolEvidence::Dynamic,
        ] {
            let mut run = CausalRun::new(
                CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
                ProgressFault::HoldCommitReceipt,
                false,
            )
            .await;
            let checked_gate = Arc::clone(&run.gate);
            let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
                let request = PromptRequest::new("publication before cancellation").unwrap();
                let request = if expires { request.with_timeout(Duration::from_secs(1)).unwrap() } else { request };
                let mut stream = run.fixture.conversation.prompt_stream(request).await.unwrap();
                let turn = run.accepted(&mut stream).await;
                run.fixture.peer.events.send(evidence.callback("stream-thread", &turn, 2301)).await.unwrap();
                run.progress.entered.acquire().await.unwrap().forget();
                let before = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
                let committed_tool = before.records[0].record.value["pending"].as_array().unwrap().iter()
                    .find(|entry| entry["kind"]["kind"] == "progress" && entry["kind"]["label"] == TOOLS)
                    .expect("the exact native Tool milestone commit precedes cancellation").clone();
                assert_eq!(committed_tool["id"], run.progress.attempted_ids.lock().unwrap()[0]);
                assert_eq!(run.progress.attempts.load(Ordering::SeqCst), 1);
                assert_eq!(run.prompt_record().await.unwrap()["cancellation"], "not_requested", "the durable publication receipt is held before cancellation begins");
                let responses = {
                    let cancellation = async {
                        if expires { std::future::pending().await } else { run.fixture.conversation.abort().await }
                    };
                    tokio::pin!(cancellation);
                    tokio::select! {
                        result = &mut cancellation => panic!("abort settled before native terminal evidence: {result:?}"),
                        _ = async { while run.fixture.peer.interrupts.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {}
                    }
                    let intent = run.prompt_record().await.unwrap();
                    assert_ne!(intent["cancellation"], "not_requested");
                    if expires { assert_eq!(intent["waiter"], "timed_out"); }
                    assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "inProgress", "interrupt acknowledgment alone does not fabricate native settlement");
                    run.progress.release.add_permits(1);
                    let responses = CausalRun::barrier(&mut run.fixture.peer, 2302).await;
                    let journal = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
                    let interactions = journal.records[0].record.value["pending"].as_array().unwrap().iter().filter(|entry| entry["kind"]["kind"] == "interaction").count();
                    run.fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{"id":turn,"status":"interrupted","items":[],"error":null}})).await;
                    if !expires { cancellation.await.unwrap(); }
                    (responses, interactions)
                };
                let error = stream.recv().await.unwrap().unwrap_err();
                assert_eq!(error.kind(), if expires { ErrorKind::Timeout } else { ErrorKind::Cancelled });
                assert!(stream.recv().await.is_none());
                while run.prompt_record().await.is_some() { tokio::task::yield_now().await; }
                run.next_prompt().await;
                let mut unexpected = responses.0;
                unexpected.extend(CausalRun::barrier(&mut run.fixture.peer, 2303).await);
                assert!(unexpected.is_empty(), "cancellation cannot enqueue a stale native callback decision: {unexpected:?}");
                assert_eq!(responses.1, 0, "an expired or cancelled prompt cannot publish a fresh actionable Interaction");
                assert_eq!(run.gate.handlers.load(Ordering::SeqCst), 0);
                assert_eq!(run.gate.effects.load(Ordering::SeqCst), 0);
                assert_eq!(run.progress.attempts.load(Ordering::SeqCst), 1);
                assert_eq!(run.labels(), [PREPARING, TOOLS, PREPARING], "an admitted publication retains its receipt while subsequent effects are fenced");
                let after = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
                let replayed_tool = after.records[0].record.value["pending"].as_array().unwrap().iter()
                    .find(|entry| entry["id"] == committed_tool["id"]).unwrap();
                assert_eq!(replayed_tool, &committed_tool, "ID, sequence, timestamp and presentation survive delayed receipt and cancellation unchanged");
            }).await;
            run.finish(observed).await;
            assert_eq!(checked_gate.handlers.load(Ordering::SeqCst), 0);
            assert_eq!(checked_gate.effects.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn codex_progress_failure_found_during_terminal_read_still_joins_prior_tool_work() {
    let (release, released) = oneshot::channel();
    let mut release = Some(release);
    let mut run = CausalRun::new(
        CodexPeer::start_with(FirstStart::DeferredStart(released)).await,
        ProgressFault::HoldThenReject,
        true,
    )
    .await;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
        let mut receiver = run.fixture.conversation.notification_stream().unwrap();
        let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("early managed interaction").unwrap()).await.unwrap();
        let turn = run.fixture.peer.turn().await;
        run.fixture.peer.events.send(ToolEvidence::Dynamic.callback("stream-thread", &turn, 2401)).await.unwrap();
        ProgressRun::assert_progress(&receiver.recv().await.unwrap().unwrap(), PREPARING);
        let interaction = receiver.recv().await.unwrap().unwrap();
        assert!(matches!(interaction.kind(), ConversationNotificationKind::Interaction { .. }));
        assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 1);
        assert_eq!(run.labels(), [PREPARING]);
        drop(receiver);
        run.gate.arm(ReadStage::Prompt);
        release.take().unwrap().send(()).unwrap();
        run.gate.entered.acquire().await.unwrap().forget();
        run.command(&turn).await;
        run.progress.entered.acquire().await.unwrap().forget();
        run.fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{"id":turn,"status":"interrupted","items":[],"error":null}})).await;
        run.progress.release.add_permits(1);
        run.wait_terminal().await;
        assert!(CausalRun::barrier(&mut run.fixture.peer, 2402).await.is_empty());
        run.gate.release.add_permits(1);
        let error = stream.recv().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Integration);
        assert_eq!(error.context().phase, ErrorPhase::StateCommit);
        assert!(stream.recv().await.is_none());
        assert_eq!(run.gate.cancellations.load(Ordering::SeqCst), 1, "terminal discovery cannot bypass cleanup of previously accepted tool work");
        assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 0);
        assert_eq!(run.gate.effects.load(Ordering::SeqCst), 1, "the original early invocation is never repeated");
        run.next_prompt().await;
        assert!(CausalRun::barrier(&mut run.fixture.peer, 2403).await.is_empty(), "cancelled early work cannot send a delayed callback result");
    }).await;
    if let Some(release) = release {
        let _ = release.send(());
    }
    run.finish(observed).await;
}

#[tokio::test]
async fn codex_progress_interaction_transaction_rechecks_cancellation_before_publication() {
    for evidence in [ToolEvidence::CommandApproval, ToolEvidence::FileApproval] {
        let mut run = CausalRun::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
            ProgressFault::None,
            false,
        )
        .await;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
            let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("atomic interaction admission").unwrap()).await.unwrap();
            let turn = run.accepted(&mut stream).await;
            // A genuinely foreign turn retains the existing native error response.
            run.fixture.peer.events.send(evidence.callback("stream-thread", "foreign-turn", 2500)).await.unwrap();
            let foreign = CausalRun::barrier(&mut run.fixture.peer, 2501).await;
            assert_eq!(foreign.len(), 1);
            assert_eq!(foreign[0]["id"], 2500);
            assert!(foreign[0].get("error").is_some());
            assert_eq!(run.labels(), [PREPARING]);

            // This public continuation scan occurs after the owning callback's
            // milestone/recheck and before its interaction transaction begins.
            run.gate.arm(ReadStage::Continuations);
            run.fixture.peer.events.send(evidence.callback("stream-thread", &turn, 2502)).await.unwrap();
            run.gate.entered.acquire().await.unwrap().forget();
            assert_eq!(run.labels(), [PREPARING, TOOLS]);
            assert_eq!(run.prompt_record().await.unwrap()["cancellation"], "not_requested");
            let before = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
            assert!(before.records[0].record.value["pending"].as_array().unwrap().iter().all(|entry| entry["kind"]["kind"] != "interaction"));
            let (responses, interactions) = {
                let cancellation = run.fixture.conversation.abort();
                tokio::pin!(cancellation);
                tokio::select! {
                    result = &mut cancellation => panic!("abort settled without native terminal evidence: {result:?}"),
                    _ = async { while run.fixture.peer.interrupts.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {}
                }
                let intent = run.prompt_record().await.unwrap();
                assert_ne!(intent["cancellation"], "not_requested");
                assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "inProgress");
                // Cancellation is durable before the new interaction transaction
                // can be admitted; no transaction or commit is already in flight.
                run.gate.release.add_permits(1);
                let responses = CausalRun::barrier(&mut run.fixture.peer, 2503).await;
                let journal = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
                let interactions = journal.records[0].record.value["pending"].as_array().unwrap().iter().filter(|entry| entry["kind"]["kind"] == "interaction").count();
                run.fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread", "turn":{"id":turn,"status":"interrupted","items":[],"error":null}})).await;
                cancellation.await.unwrap();
                (responses, interactions)
            };
            assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
            assert!(stream.recv().await.is_none());
            while run.prompt_record().await.is_some() { tokio::task::yield_now().await; }
            run.next_prompt().await;
            let mut responses = responses;
            responses.extend(CausalRun::barrier(&mut run.fixture.peer, 2504).await);
            assert!(responses.is_empty(), "the cancelled callback cannot send a decision: {responses:?}");
            assert_eq!(interactions, 0, "an interaction transaction admitted after durable cancellation cannot publish actionable work");
            assert_eq!(run.labels(), [PREPARING, TOOLS, PREPARING]);
            assert_eq!(run.gate.handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 0);
        }).await;
        run.finish(observed).await;
    }
}

#[tokio::test]
async fn codex_progress_claim_conflict_reconciles_proven_native_terminal_without_interrupt() {
    CausalRun::cancellation_commit_conflict("dispatching", 0).await;
}

#[tokio::test]
async fn codex_progress_settlement_conflict_reconciles_proven_native_terminal_without_resend() {
    CausalRun::cancellation_commit_conflict("settled", 1).await;
}

impl CausalRun {
    async fn cancellation_commit_conflict(cancellation: &'static str, expected_interrupts: usize) {
        let mut run = Self::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
            ProgressFault::HoldThenReject,
            false,
        )
        .await;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
            let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("terminal conflicts with cancellation bookkeeping").unwrap()).await.unwrap();
            let turn = run.accepted(&mut stream).await;
            run.gate.arm(ReadStage::CancellationCommit(cancellation));
            run.command(&turn).await;
            run.progress.entered.acquire().await.unwrap().forget();
            run.progress.release.add_permits(1);
            run.gate.entered.acquire().await.unwrap().forget();
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), expected_interrupts);
            let held = run.prompt_record().await.unwrap();
            assert_eq!(held["state"]["state"], "running");
            assert_eq!(held["state"]["turn_id"], turn);
            assert_eq!(held["cancellation"], if expected_interrupts == 0 { "requested" } else { "dispatching" });
            run.fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread", "turn":{"id":turn,"status":"interrupted","items":[],"error":null}})).await;
            run.wait_terminal().await;
            let terminal = run.prompt_record().await.unwrap();
            assert_eq!(terminal["client_message_id"], held["client_message_id"]);
            assert_eq!(terminal["state"]["turn_id"], turn);
            assert_eq!(terminal["cancellation"], if expected_interrupts == 0 { "unnecessary" } else { "settled" });
            assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "interrupted");
            // The real Lance terminal commit invalidates the older selected
            // cancellation transaction; the fixture never manufactures Conflict.
            run.gate.release.add_permits(1);
            let error = stream.recv().await.unwrap().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert_eq!(error.detail(), Some(&agents_kit::error::ErrorDetail::ServiceUnavailable));
            assert_eq!(error.context().phase, ErrorPhase::StateCommit);
            assert!(stream.recv().await.is_none());
            assert_eq!(run.gate.cancellation_commit_conflicts.load(Ordering::SeqCst), 1, "the exact selected native commit must prove rollback conflict");
            assert!(run.prompt_record().await.is_none(), "same-generation native terminal authority must reconcile the cancellation conflict before releasing admission: {error:?}");
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), expected_interrupts, "a definite bookkeeping rollback cannot resend native interruption");
            run.next_prompt().await;
            assert_eq!(run.native_starts(), 2);
            assert_eq!(run.labels(), [PREPARING, PREPARING]);
            assert_eq!(run.gate.handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 0);
            assert!(Self::barrier(&mut run.fixture.peer, 2600).await.is_empty());
        }).await;
        run.finish(observed).await;
    }
}

#[tokio::test]
async fn codex_progress_deadline_cancels_a_late_acquired_handler_before_invocation() {
    CausalRun::tool_deadline(ToolPause::Acquisition, ToolCancellation::Completed).await;
}

#[tokio::test]
async fn codex_progress_deadline_cancels_a_late_handler_interaction_before_publication() {
    CausalRun::tool_deadline(ToolPause::Invocation, ToolCancellation::Completed).await;
}

impl CausalRun {
    async fn tool_deadline(stage: ToolPause, cancellation: ToolCancellation) {
        let mut run = Self::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await,
            ProgressFault::None,
            stage == ToolPause::Invocation,
        )
        .await;
        *run.gate.tool_pause.lock().unwrap() = Some(stage);
        *run.gate.tool_cancellation.lock().unwrap() = cancellation;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
            let request = PromptRequest::new("late tool effect after the original deadline").unwrap()
                .with_timeout(Duration::from_secs(1)).unwrap();
            let mut stream = run.fixture.conversation.prompt_stream(request).await.unwrap();
            let turn = run.accepted(&mut stream).await;
            run.gate.arm(ReadStage::TimeoutIntent);
            run.fixture.peer.events.send(ToolEvidence::Dynamic.callback("stream-thread", &turn, 2700)).await.unwrap();
            run.gate.tool_entered.acquire().await.unwrap().forget();
            let admitted_effects = usize::from(stage == ToolPause::Invocation);
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), admitted_effects);
            assert_eq!(run.labels(), [PREPARING, TOOLS]);
            assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Timeout);
            assert!(stream.recv().await.is_none());
            run.gate.entered.acquire().await.unwrap().forget();
            let before = run.prompt_record().await.unwrap();
            assert_eq!(before["waiter"], "waiting");
            assert_eq!(before["cancellation"], "not_requested");
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), 0);
            // Timeout is public, but its durable cancellation write is held.
            // Only the original live deadline can fence this next tool effect.
            run.gate.tool_release.add_permits(1);
            let interactions = loop {
                let journal = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
                let interactions = journal.records[0].record.value["pending"].as_array().unwrap().iter()
                    .filter(|entry| entry["kind"]["kind"] == "interaction").count();
                if run.gate.cancellations.load(Ordering::SeqCst) > 0
                    || run.gate.effects.load(Ordering::SeqCst) > admitted_effects
                    || interactions > 0 { break interactions; }
                tokio::task::yield_now().await;
            };
            let effects = run.gate.effects.load(Ordering::SeqCst);
            let cancellations = run.gate.cancellations.load(Ordering::SeqCst);
            // Capture the boundary before permitting durable cancellation to
            // clean up an incorrectly admitted late effect and conceal the bug.
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), 0);
            run.gate.release.add_permits(1);
            assert_eq!(effects, admitted_effects, "an acquired handler cannot begin a fresh Invoke after the original prompt deadline");
            assert_eq!(interactions, 0, "a late handler result cannot publish a new actionable Interaction");
            assert_eq!(cancellations, 1, "the unique accepted handler must settle cancellation while timeout intent remains blocked");
            if matches!(cancellation, ToolCancellation::Failed | ToolCancellation::Interaction) {
                loop {
                    match run.fixture.agent.info().await {
                        Ok(_) => tokio::task::yield_now().await,
                        Err(error) => {
                            assert_eq!(error.kind(), ErrorKind::Closed);
                            break;
                        }
                    }
                }
                assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 0);
                assert_eq!(run.native_starts(), 1);
                assert!(run.fixture.peer.responses.try_recv().is_err());
                return;
            }
            let aborted = run.fixture.conversation.abort().await;
            eprintln!("post-deadline fixture cleanup: {stage:?}, timeout-intent conflicts={}, abort={aborted:?}", run.gate.timeout_intent_conflicts.load(Ordering::SeqCst));
            aborted.unwrap();
            assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), 1);
            assert!(Self::barrier(&mut run.fixture.peer, 2701).await.is_empty(), "late managed work cannot send a native callback result");
        }).await;
        if matches!(
            cancellation,
            ToolCancellation::Failed | ToolCancellation::Interaction
        ) {
            run.finish_cancellation_failure(observed, cancellation)
                .await;
        } else {
            run.finish(observed).await;
        }
    }

    async fn finish_cancellation_failure(
        self,
        observed: Result<Result<(), tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>,
        cancellation: ToolCancellation,
    ) {
        self.gate.next.lock().unwrap().take();
        *self.gate.retirement.lock().unwrap() = RetirementGate::Idle;
        self.gate.release.add_permits(16);
        self.gate.tool_pause.lock().unwrap().take();
        self.gate.tool_release.add_permits(16);
        self.progress.release.add_permits(16);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        drop(self.fixture.conversation);
        drop(self.fixture.workspace);
        cleanup
            .settle(async {
                let failure = self
                    .fixture
                    .agent
                    .shutdown()
                    .await
                    .expect_err("runtime shutdown must retain handler cancellation failure");
                let mut errors = vec![&failure];
                let mut retained = false;
                while let Some(error) = errors.pop() {
                    if error.kind() == ErrorKind::Timeout
                        && error.message() == "prompt deadline elapsed"
                    {
                        retained =
                            error.supplementary().iter().any(|cleanup| {
                                cleanup.context().phase == ErrorPhase::Cleanup
                                    && match cancellation {
                                        ToolCancellation::Failed => cleanup.kind()
                                            == ErrorKind::Integration
                                            && cleanup.detail()
                                                == Some(
                                                    &agents_kit::error::ErrorDetail::InvalidData,
                                                ),
                                        ToolCancellation::Interaction => {
                                            cleanup.kind() == ErrorKind::InvalidState
                                        }
                                        ToolCancellation::Completed
                                        | ToolCancellation::Cancelled => false,
                                    }
                            });
                        if retained {
                            break;
                        }
                    }
                    errors.extend(error.supplementary());
                    errors.extend(error.cause());
                }
                assert!(
                    retained,
                    "shutdown lost the original timeout and typed cleanup failure: {failure:?}"
                );
                assert_eq!(self.gate.active_handlers.load(Ordering::SeqCst), 0);
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        let mut peer = self.fixture.peer;
        cleanup
            .settle(async {
                assert!(
                    peer.responses.recv().await.is_none(),
                    "failed cleanup cannot emit a native tool response"
                );
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup
            .settle(async {
                peer.shutdown().await;
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(self.fixture.root) })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_progress_tool_deadline_accepts_typed_handler_cancellation() {
    CausalRun::tool_deadline(ToolPause::Acquisition, ToolCancellation::Cancelled).await;
}

#[tokio::test]
async fn codex_progress_tool_deadline_retains_handler_cancellation_failure() {
    CausalRun::tool_deadline(ToolPause::Acquisition, ToolCancellation::Failed).await;
}

#[tokio::test]
async fn codex_progress_tool_deadline_rejects_an_interaction_from_cancellation() {
    CausalRun::tool_deadline(ToolPause::Acquisition, ToolCancellation::Interaction).await;
}

#[tokio::test]
async fn codex_progress_unacknowledged_facts_do_not_deadlock_tool_cancellation() {
    const CHILD_ROOT_RECEIPT: &str = "AI_KIT_CODEX_CANCELLATION_CHILD_ROOT";
    if let Some(receipt) = std::env::var_os(CHILD_ROOT_RECEIPT) {
        let mut run = CausalRun::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await,
            ProgressFault::None,
            true,
        )
        .await;
        std::fs::write(&receipt, run.fixture.root.to_string_lossy().as_bytes()).unwrap();
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(12), async {
            let mut stream = run.fixture.conversation.prompt_stream(PromptRequest::new("cancel an unobserved interaction").unwrap()).await.unwrap();
            let turn = run.accepted(&mut stream).await;
            run.fixture.peer.events.send(ToolEvidence::Dynamic.callback("stream-thread", &turn, 2800)).await.unwrap();
            loop {
                let journal = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
                let pending = journal.records[0].record.value["pending"].as_array().unwrap();
                if pending.len() == 3 {
                    assert_eq!(pending[0]["kind"], json!({"kind":"progress","label":PREPARING,"detail":null}));
                    assert_eq!(pending[1]["kind"], json!({"kind":"progress","label":TOOLS,"detail":null}));
                    assert_eq!(pending[2]["kind"]["kind"], "interaction");
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 1);
            assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 1);
            // No notification receiver has acknowledged the two passive facts.
            // Cancellation still owns the real handler waiting behind them.
            eprintln!("unobserved Interaction exists behind both committed Progress facts; entering abort");
            run.fixture.conversation.abort().await.unwrap();
            assert_eq!(run.gate.cancellations.load(Ordering::SeqCst), 1);
            assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 0);
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 1);
            assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
            assert!(stream.recv().await.is_none());
            assert_eq!(run.fixture.peer.interrupts.load(Ordering::SeqCst), 1);
            assert!(CausalRun::barrier(&mut run.fixture.peer, 2801).await.is_empty());
            let journal = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
            let pending = journal.records[0].record.value["pending"].as_array().unwrap();
            eprintln!("post-abort unread journal: entries={}, states={:?}", pending.len(), pending.iter().map(|entry| (&entry["kind"]["kind"], &entry["state"])).collect::<Vec<_>>());
            run.next_prompt().await;
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 1);
            assert_eq!(run.gate.cancellations.load(Ordering::SeqCst), 1);
        }).await;
        run.finish(observed).await;
        return;
    }
    // Synchronous lock regressions cannot yield to an in-runtime timeout.
    // The parent owns, kills if necessary, and reaps this exact test child.
    let receipt = std::env::temp_dir().join(format!(
        "agents-kit-codex-cancellation-child-{}",
        uuid::Uuid::new_v4()
    ));
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "codex_progress::causal::codex_progress_unacknowledged_facts_do_not_deadlock_tool_cancellation",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ROOT_RECEIPT, &receipt)
        .kill_on_drop(true)
        .spawn()
        .expect("owned cancellation child must start");
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(30),
        child.wait(),
    )
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    cleanup
        .settle(async {
            if child.try_wait()?.is_none() {
                child.start_kill()?;
            }
            Ok::<(), std::io::Error>(())
        })
        .await;
    let mut reaped = false;
    cleanup
        .settle(async {
            child.wait().await?;
            reaped = true;
            Ok::<(), std::io::Error>(())
        })
        .await;
    if reaped {
        cleanup
            .settle(async {
                // This receipt also proves the exact child test ran rather than a
                // successful executable invocation matching zero tests.
                let root = std::path::PathBuf::from(std::fs::read_to_string(&receipt)?);
                if root.exists() {
                    std::fs::remove_dir_all(root)?;
                }
                std::fs::remove_file(&receipt)?;
                Ok::<(), std::io::Error>(())
            })
            .await;
    } else {
        cleanup
            .settle(async {
                Err::<(), _>(
                    "child reap is unproven; owned fixture directory and receipt are retained",
                )
            })
            .await;
    }
    let status = cleanup
        .finish(observed)
        .expect("owned cancellation child wait must succeed");
    assert!(
        status.success(),
        "owned cancellation child failed with {status}"
    );
}

impl CausalRun {
    fn retains_failure(error: &agents_kit::KitError, expected: &agents_kit::KitError) -> bool {
        let mut errors = vec![error];
        while let Some(error) = errors.pop() {
            if error.kind() == expected.kind()
                && error.detail() == expected.detail()
                && error.message() == expected.message()
                && error.context().phase == expected.context().phase
            {
                return true;
            }
            errors.extend(error.supplementary());
            errors.extend(error.cause());
        }
        false
    }

    async fn finish_retirement(
        self,
        observed: Result<Result<(), tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>,
        retained: Option<agents_kit::KitError>,
    ) {
        self.gate.next.lock().unwrap().take();
        *self.gate.retirement.lock().unwrap() = RetirementGate::Idle;
        self.gate.release.add_permits(16);
        self.gate.tool_release.add_permits(16);
        self.progress.release.add_permits(16);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        cleanup
            .settle(async {
                match (self.fixture.conversation.close().await, retained.as_ref()) {
                    (Err(error), Some(expected)) if Self::retains_failure(&error, expected) => {
                        Ok(())
                    }
                    (result, _) => result,
                }
            })
            .await;
        cleanup
            .settle(async {
                match (self.fixture.workspace.close().await, retained.as_ref()) {
                    (Err(error), Some(expected)) if Self::retains_failure(&error, expected) => {
                        Ok(())
                    }
                    (result, _) => result,
                }
            })
            .await;
        cleanup
            .settle(async {
                let result = self.fixture.agent.shutdown().await;
                match (result, retained.as_ref()) {
                    (Err(error), Some(expected)) if Self::retains_failure(&error, expected) => {
                        Ok(())
                    }
                    (Ok(()), Some(_)) => {
                        panic!("consuming shutdown lost exact retained retirement failure")
                    }
                    (result, _) => result,
                }
            })
            .await;
        let mut peer = self.fixture.peer;
        cleanup
            .settle(async {
                assert!(
                    peer.responses.recv().await.is_none(),
                    "retired managed interaction cannot send a native callback decision"
                );
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup
            .settle(async {
                peer.shutdown().await;
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(self.fixture.root) })
            .await;
        cleanup.finish(observed);
    }

    async fn journal_entries(&self) -> Vec<Value> {
        self.raw
            .list(StateListRequest::new(JOURNAL).unwrap())
            .await
            .unwrap()
            .records[0]
            .record
            .value["pending"]
            .as_array()
            .unwrap()
            .clone()
    }

    async fn retirement_obligation(&self) -> Option<StateRecordSnapshot> {
        let records = self
            .raw
            .list(StateListRequest::new("agents_kit_codex_tool_interactions_v1").unwrap())
            .await
            .unwrap();
        assert!(records.records.len() <= 1);
        records.records.into_iter().next()
    }

    async fn pending_retirement(
        &mut self,
        fault: RetirementFault,
    ) -> (PromptStream, String, Vec<Value>) {
        let mut stream = self
            .fixture
            .conversation
            .prompt_stream(PromptRequest::new("retire the exact managed interaction").unwrap())
            .await
            .unwrap();
        let turn = self.accepted(&mut stream).await;
        self.fixture
            .peer
            .events
            .send(ToolEvidence::Dynamic.callback("stream-thread", &turn, 2900))
            .await
            .unwrap();
        let before = loop {
            let entries = self.journal_entries().await;
            if entries.len() == 3 {
                assert_eq!(entries[0]["kind"]["label"], PREPARING);
                assert_eq!(entries[1]["kind"]["label"], TOOLS);
                assert_eq!(entries[2]["kind"]["kind"], "interaction");
                assert_eq!(entries[2]["state"]["state"], "pending");
                break entries;
            }
            tokio::task::yield_now().await;
        };
        let obligation = self.retirement_obligation().await.unwrap();
        assert_eq!(obligation.record.value["notification_id"], before[2]["id"]);
        *self.gate.retirement.lock().unwrap() = RetirementGate::Armed {
            fault,
            obligation: obligation.record.key,
            notification: before[2]["id"].clone(),
        };
        (stream, turn, before)
    }

    async fn arm_retirement_abort(&self) {
        let prompt = self
            .raw
            .list(StateListRequest::new(PROMPTS).unwrap())
            .await
            .unwrap();
        assert_eq!(prompt.records.len(), 1);
        self.gate.arm(ReadStage::AbortIntentReceipt(
            prompt.records[0].record.key.clone(),
        ));
    }

    async fn settled_native_cancellation(&self, turn: &str) {
        let record = self.prompt_record().await.unwrap();
        assert!(
            matches!(
                record["cancellation"].as_str(),
                Some("requested" | "dispatching" | "settled")
            ),
            "the exact committed abort intent may already have advanced: {record:?}"
        );
        self.fixture.peer.notify("item/completed", json!({
            "threadId":"stream-thread", "turnId":turn, "completedAtMs":1,
            "item":{"id":"cancellation-wakeup", "type":"agentMessage", "text":"pending cancellation", "phase":"final_answer"}
        })).await;
        loop {
            if self
                .prompt_record()
                .await
                .is_some_and(|record| record["cancellation"] == "settled")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(self.fixture.peer.interrupts.load(Ordering::SeqCst), 1);
        assert_eq!(
            self.fixture.peer.history.lock().unwrap()["turns"][0]["status"],
            "inProgress"
        );
    }

    async fn interrupted(&self, turn: &str) {
        self.fixture
            .peer
            .notify(
                "turn/completed",
                json!({"threadId":"stream-thread","turn":{
                    "id":turn,"status":"interrupted","items":[],"error":null
                }}),
            )
            .await;
    }

    async fn assert_retirement_receipt(&self, before: &[Value]) {
        let after = self.journal_entries().await;
        assert_eq!(after.len(), before.len());
        assert_eq!(
            &after[..2],
            &before[..2],
            "unread passive facts must retain their complete identity"
        );
        let mut terminal = before[2].clone();
        terminal["state"] = json!({"state":"terminal","detail":{"outcome":"reconciled_cancelled"}});
        assert_eq!(
            after[2], terminal,
            "only the exact owning Interaction can be terminalized"
        );
        assert!(self.retirement_obligation().await.is_none());
    }
}

#[tokio::test]
async fn codex_progress_tool_retirement_preserves_a_real_commit_conflict() {
    let mut run = CausalRun::new(
        CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
        ProgressFault::None,
        true,
    )
    .await;
    let mut retained = None;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
        let (mut stream, turn, before) = run.pending_retirement(RetirementFault::NativeConflict).await;
        run.arm_retirement_abort().await;
        let aborted = {
            let abort = run.fixture.conversation.abort(); tokio::pin!(abort);
            tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not retain its committed native intent receipt: {result:?}") }
            run.settled_native_cancellation(&turn).await;
            run.gate.release.add_permits(1);
            tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not own retirement commit: {result:?}") }
            run.command(&turn).await;
            while run.journal_entries().await.len() != 4 { tokio::task::yield_now().await; }
            assert_eq!(run.journal_entries().await[3]["kind"]["kind"], "process");
            run.gate.release.add_permits(1);
            tokio::select! {
                _ = async { while run.gate.cancellations.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; } } => {},
                result = &mut abort => panic!("abort ended before its retained responder settled: {result:?}"),
            }
            run.interrupted(&turn).await;
            abort.await.unwrap_err()
        };
        assert_eq!(aborted.detail(), Some(&agents_kit::error::ErrorDetail::TransactionConflict));
        retained = Some(aborted.clone());
        assert_eq!(run.gate.retirement_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(run.gate.retirement_conflicts.load(Ordering::SeqCst), 1);
        assert_eq!(&run.journal_entries().await[..3], before.as_slice());
        assert!(run.retirement_obligation().await.is_some());
        assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
        assert!(stream.recv().await.is_none());
        assert_eq!(run.fixture.conversation.prompt_stream(PromptRequest::new("must remain sealed").unwrap()).await.err().expect("prompt admission must fail").kind(), ErrorKind::Closed);
        assert_eq!(run.native_starts(), 1);
        assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 0);
        assert!(CausalRun::barrier(&mut run.fixture.peer, 2901).await.is_empty());
    }).await;
    run.finish_retirement(observed, retained).await;
}

#[tokio::test]
async fn codex_progress_tool_retirement_observes_unknown_commits_without_replay() {
    for fault in [
        RetirementFault::UnknownBeforeCommit,
        RetirementFault::UnknownAfterCommit,
    ] {
        let mut run = CausalRun::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
            ProgressFault::None,
            true,
        )
        .await;
        let mut retained = None;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
            let (mut stream, turn, before) = run.pending_retirement(fault).await;
            run.arm_retirement_abort().await;
            let aborted = {
                let abort = run.fixture.conversation.abort(); tokio::pin!(abort);
                tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not retain its committed native intent receipt: {result:?}") }
                run.settled_native_cancellation(&turn).await;
                run.gate.release.add_permits(1);
                tokio::select! {
                _ = async { while run.gate.cancellations.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; } } => {},
                result = &mut abort => panic!("abort ended before its retained responder settled: {result:?}"),
            }
                while run.gate.retirement_receipts.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; }
                run.interrupted(&turn).await;
                abort.await
            };
            assert_eq!(run.gate.retirement_attempts.load(Ordering::SeqCst), 1);
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 1);
            assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 0);
            assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
            assert!(stream.recv().await.is_none());
            assert!(CausalRun::barrier(&mut run.fixture.peer, 2902).await.is_empty());
            if fault == RetirementFault::UnknownAfterCommit {
                aborted.unwrap();
                run.assert_retirement_receipt(&before).await;
                run.next_prompt().await;
            } else {
                let error = aborted.unwrap_err();
                assert_eq!(error.detail(), Some(&agents_kit::error::ErrorDetail::ServiceUnavailable));
                retained = Some(error);
                assert_eq!(run.journal_entries().await, before);
                assert!(run.retirement_obligation().await.is_some());
                assert_eq!(run.fixture.conversation.prompt_stream(PromptRequest::new("must remain sealed").unwrap()).await.err().expect("prompt admission must fail").kind(), ErrorKind::Closed);
                assert_eq!(run.native_starts(), 1);
            }
        }).await;
        run.finish_retirement(observed, retained).await;
    }
}

#[tokio::test]
async fn codex_progress_tool_retirement_owned_receipt_fences_replacement_dispatch() {
    CausalRun::held_retirement(false).await;
}

#[tokio::test]
async fn codex_progress_tool_retirement_survives_a_dropped_abort_observer() {
    CausalRun::held_retirement(true).await;
}

impl CausalRun {
    async fn held_retirement(drop_observer: bool) {
        let mut run = Self::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
            ProgressFault::None,
            true,
        )
        .await;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
            let (mut stream, turn, before) = run.pending_retirement(RetirementFault::HeldReceipt).await;
            run.arm_retirement_abort().await;
            {
                let mut abort = Box::pin(run.fixture.conversation.abort());
                tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not retain committed intent: {result:?}") }
                run.settled_native_cancellation(&turn).await;
                run.gate.release.add_permits(1);
                tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not admit retirement: {result:?}") }
                run.assert_retirement_receipt(&before).await;
                if drop_observer { drop(abort); } else {
                    // The observer may time out; the admitted receipt must remain
                    // owned independently and retain the conversation fence.
                    assert_eq!(tokio::time::timeout(Duration::from_millis(3500), &mut abort).await.expect("original abort bound").unwrap_err().kind(), ErrorKind::Timeout);
                }
                run.interrupted(&turn).await;
                assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
                assert!(stream.recv().await.is_none());
                while run.prompt_record().await.is_some() { tokio::task::yield_now().await; }
                while run.gate.cancellations.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; }
                assert_eq!(run.fixture.peer.history.lock().unwrap()["turns"][0]["status"], "interrupted");
                assert_eq!(run.fixture.conversation.prompt_stream(PromptRequest::new("cannot overtake owned receipt").unwrap()).await.err().expect("prompt admission must fail").kind(), ErrorKind::Busy);
                assert_eq!(run.native_starts(), 1);
                run.assert_retirement_receipt(&before).await;
                run.gate.release.add_permits(1);
            }
            while run.gate.retirement_receipts.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; }
            while run.gate.active_handlers.load(Ordering::SeqCst) != 0 { tokio::task::yield_now().await; }
            assert_eq!(run.gate.retirement_attempts.load(Ordering::SeqCst), 1);
            assert_eq!(run.gate.cancellations.load(Ordering::SeqCst), 1);
            assert_eq!(run.gate.effects.load(Ordering::SeqCst), 1);
            run.assert_retirement_receipt(&before).await;
            assert!(Self::barrier(&mut run.fixture.peer, 2903).await.is_empty());
            run.next_prompt().await;
        }).await;
        run.finish(observed).await;
    }
}

#[tokio::test]
async fn codex_progress_tool_retirement_preserves_foreign_interaction_content() {
    let mut run = CausalRun::new(
        CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
        ProgressFault::None,
        true,
    )
    .await;
    let mut retained = None;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
        let (mut stream, turn, _) = run.pending_retirement(RetirementFault::None).await;
        let record = run.raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap().records.remove(0).record;
        let mut changed = record.value.as_ref().clone();
        changed["pending"][2]["kind"]["body"] = json!("changed authoritative interaction content");
        let mut mutation = run.raw.begin_transaction().await.unwrap();
        mutation.put(StateRecord::new(record.key, changed)).await.unwrap();
        mutation.commit().await.unwrap();
        let foreign = run.journal_entries().await;
        run.arm_retirement_abort().await;
        let aborted = {
            let abort = run.fixture.conversation.abort(); tokio::pin!(abort);
            tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not retain its committed native intent receipt: {result:?}") }
            run.settled_native_cancellation(&turn).await;
            run.gate.release.add_permits(1);
            tokio::select! {
                _ = async { while run.gate.cancellations.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; } } => {},
                result = &mut abort => panic!("abort ended before its retained responder settled: {result:?}"),
            }
            run.interrupted(&turn).await;
            abort.await.unwrap_err()
        };
        assert_eq!(aborted.kind(), ErrorKind::Protocol);
        retained = Some(aborted.clone());
        assert_eq!(run.gate.retirement_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(run.journal_entries().await, foreign);
        assert!(run.retirement_obligation().await.is_some());
        assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
        assert_eq!(run.fixture.conversation.prompt_stream(PromptRequest::new("foreign content must not reopen admission").unwrap()).await.err().expect("prompt admission must fail").kind(), ErrorKind::Closed);
        assert_eq!(run.native_starts(), 1);
        assert!(CausalRun::barrier(&mut run.fixture.peer, 2904).await.is_empty());
    }).await;
    run.finish_retirement(observed, retained).await;
}

#[tokio::test]
async fn codex_progress_tool_retirement_preserves_a_raced_resolution_intent() {
    let mut run = CausalRun::new(
        CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
        ProgressFault::None,
        true,
    )
    .await;
    let mut retained = None;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
        let (mut stream, turn, before) = run.pending_retirement(RetirementFault::ResolvingReceipt).await;
        let mut notifications = run.fixture.conversation.notification_stream().unwrap();
        CodexFixture::next_progress(&mut notifications, PREPARING).await;
        CodexFixture::next_progress(&mut notifications, TOOLS).await;
        let interaction = notifications.recv().await.unwrap().unwrap();
        assert_eq!(serde_json::to_value(interaction.id()).unwrap(), before[2]["id"]);
        drop(notifications);
        let (aborted, resolved, intent) = {
            let resolve = run.fixture.conversation.resolve_notification(interaction.id().clone(), agents_kit::notification::NotificationResolution::Submit { content: json!("caller answer") });
            tokio::pin!(resolve);
            tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut resolve => panic!("caller resolution did not retain committed intent: {result:?}") }
            let intent = run.journal_entries().await;
            assert_eq!(intent.len(), 1);
            assert_eq!(intent[0]["id"], before[2]["id"]);
            assert_eq!(intent[0]["state"]["state"], "resolving");
            let abort = run.fixture.conversation.abort(); tokio::pin!(abort);
            tokio::select! {
                _ = async { while run.gate.cancellations.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; } } => {},
                result = &mut abort => panic!("abort ended before owning responder cancellation: {result:?}"),
            }
            run.interrupted(&turn).await;
            let aborted = abort.await.unwrap_err();
            assert_eq!(run.journal_entries().await, intent);
            run.gate.release.add_permits(1);
            let resolved = resolve.await.unwrap_err();
            (aborted, resolved, intent)
        };
        assert_eq!(aborted.kind(), ErrorKind::Conflict);
        retained = Some(aborted.clone());
        assert_eq!(resolved.kind(), ErrorKind::Conflict);
        assert_eq!(run.gate.retirement_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(run.journal_entries().await, intent, "a committed caller intent cannot be rewritten as acceptance or cancellation");
        assert!(run.retirement_obligation().await.is_some());
        assert_eq!(run.gate.effects.load(Ordering::SeqCst), 1);
        assert_eq!(run.gate.cancellations.load(Ordering::SeqCst), 1);
        assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst), 0);
        assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
        assert!(stream.recv().await.is_none());
        assert_eq!(run.fixture.conversation.prompt_stream(PromptRequest::new("resolution uncertainty must remain sealed").unwrap()).await.err().expect("prompt admission must fail").kind(), ErrorKind::Closed);
        assert_eq!(run.native_starts(), 1);
        assert!(CausalRun::barrier(&mut run.fixture.peer, 2905).await.is_empty());
    }).await;
    run.finish_retirement(observed, retained).await;
}

#[tokio::test]
async fn codex_progress_tool_retirement_survives_the_reader_shutdown_path() {
    CausalRun::retirement_shutdown(true, false).await;
}

#[tokio::test]
async fn codex_progress_tool_retirement_survives_the_consuming_shutdown_deadline() {
    CausalRun::retirement_shutdown(false, false).await;
}

#[tokio::test]
async fn codex_progress_tool_retirement_failure_survives_the_consuming_shutdown_deadline() {
    CausalRun::retirement_shutdown(false, true).await;
}

impl CausalRun {
    async fn retirement_shutdown(disconnect: bool, failed_receipt: bool) {
        let mut run = Self::new(
            CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
            ProgressFault::None,
            true,
        )
        .await;
        let mut before = None;
        let prepared = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
            let (mut stream, turn, journal) = run.pending_retirement(if failed_receipt { RetirementFault::HeldFailureReceipt } else { RetirementFault::HeldReceipt }).await;
            run.arm_retirement_abort().await;
            {
                let mut abort = Box::pin(run.fixture.conversation.abort());
                tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not retain committed intent: {result:?}") }
                run.settled_native_cancellation(&turn).await;
                run.gate.release.add_permits(1);
                tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("retirement receipt was never admitted: {result:?}") }
                if failed_receipt {
                    assert_eq!(run.journal_entries().await, journal);
                    assert!(run.retirement_obligation().await.is_some());
                } else { run.assert_retirement_receipt(&journal).await; }
                drop(abort);
            }
            run.interrupted(&turn).await;
            assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
            assert!(stream.recv().await.is_none());
            while run.prompt_record().await.is_some() { tokio::task::yield_now().await; }
            while run.gate.cancellations.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; }
            assert_eq!(run.fixture.conversation.prompt_stream(PromptRequest::new("owned retirement blocks reuse").unwrap()).await.err().expect("prompt admission must fail").kind(), ErrorKind::Busy);
            assert_eq!(run.native_starts(), 1);
            before = Some(journal);
        }).await;
        if !matches!(&prepared, Ok(Ok(()))) {
            run.finish(prepared).await;
            return;
        }
        let before = before.unwrap();
        let CausalRun {
            fixture:
                CodexFixture {
                    agent,
                    workspace,
                    conversation,
                    mut peer,
                    root,
                },
            raw,
            progress,
            gate,
        } = run;
        drop(conversation);
        drop(workspace);
        if disconnect {
            peer.stop.take().unwrap().send(()).unwrap();
        }
        let mut shutdown = Box::pin(agent.shutdown());
        let mut outcome = None;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(12), async {
            if disconnect {
                // Native socket drain is positive evidence that the event reader
                // must enter its shutdown branch while the receipt is still held.
                assert!(peer.responses.recv().await.is_none());
            }
            tokio::select! {
                result = &mut shutdown => { outcome = Some(result); panic!("shutdown dropped an admitted managed retirement before its receipt"); },
                _ = tokio::time::sleep(Duration::from_millis(3500)) => {},
            }
            assert_eq!(gate.retirement_receipts.load(Ordering::SeqCst), 0);
            assert_eq!(gate.retirement_attempts.load(Ordering::SeqCst), 1);
            assert_eq!(gate.cancellations.load(Ordering::SeqCst), 1);
            assert_eq!(gate.effects.load(Ordering::SeqCst), 1);
            gate.release.add_permits(1);
            outcome = Some((&mut shutdown).await);
            let error = outcome.as_ref().unwrap().as_ref().expect_err("original shutdown/transport failure must remain visible");
            if !disconnect { assert_eq!(error.kind(), ErrorKind::Timeout); }
            if failed_receipt {
                let mut errors = vec![error];
                let mut retained = false;
                while let Some(error) = errors.pop() {
                    retained |= error.kind() == ErrorKind::Integration
                        && error.detail() == Some(&agents_kit::error::ErrorDetail::ServiceUnavailable)
                        && error.context().phase == ErrorPhase::StateCommit
                        && error.message() == "tool interaction retirement could not commit";
                    errors.extend(error.supplementary()); errors.extend(error.cause());
                }
                assert!(retained, "shutdown lost the original commit failure after its deadline: {error:?}");
            }
            assert_eq!(gate.retirement_receipts.load(Ordering::SeqCst), 1, "reader must join the actual admitted commit receipt");
            assert_eq!(gate.active_handlers.load(Ordering::SeqCst), 0);
            let journal = raw.list(StateListRequest::new(JOURNAL).unwrap()).await.unwrap();
            let entries = journal.records[0].record.value["pending"].as_array().unwrap();
            assert_eq!(&entries[..2], &before[..2]);
            let mut terminal = before[2].clone();
            terminal["state"] = json!({"state":"terminal","detail":{"outcome":"reconciled_cancelled"}});
            assert_eq!(entries[2], if failed_receipt { before[2].clone() } else { terminal });
            let obligations = raw.list(StateListRequest::new("agents_kit_codex_tool_interactions_v1").unwrap()).await.unwrap();
            assert_eq!(obligations.records.len(), usize::from(failed_receipt));
            assert_eq!(peer.requests.lock().unwrap().iter().filter(|request| request["method"] == "turn/start").count(), 1);
        }).await;
        gate.next.lock().unwrap().take();
        *gate.retirement.lock().unwrap() = RetirementGate::Idle;
        gate.release.add_permits(16);
        gate.tool_release.add_permits(16);
        progress.release.add_permits(16);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        if outcome.is_none() {
            cleanup
                .settle(async {
                    outcome = Some((&mut shutdown).await);
                    Ok::<(), agents_kit::KitError>(())
                })
                .await;
        }
        drop(shutdown);
        cleanup
            .settle(async {
                assert!(
                    peer.responses.recv().await.is_none(),
                    "no native callback decision after cancellation"
                );
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
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
}

#[tokio::test]
async fn codex_progress_tool_retirement_does_not_seal_an_independent_conversation() {
    let mut run = CausalRun::new(
        CodexPeer::start_with(FirstStart::IndependentThreads).await,
        ProgressFault::None,
        true,
    )
    .await;
    let mut independent = None;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
        let (mut stream, turn, before) = run.pending_retirement(RetirementFault::HeldReceipt).await;
        run.arm_retirement_abort().await;
        {
            let mut abort = Box::pin(run.fixture.conversation.abort());
            tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not retain committed intent: {result:?}") }
            run.settled_native_cancellation(&turn).await;
            run.gate.release.add_permits(1);
            tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("retirement was not admitted: {result:?}") }
            run.assert_retirement_receipt(&before).await;
            drop(abort);
        }
        run.interrupted(&turn).await;
        assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
        assert!(stream.recv().await.is_none());
        while run.prompt_record().await.is_some() { tokio::task::yield_now().await; }
        assert_eq!(run.fixture.conversation.prompt_stream(PromptRequest::new("same owner remains fenced").unwrap()).await.err().expect("prompt admission must fail").kind(), ErrorKind::Busy);
        independent = Some(run.fixture.workspace.new_conversation(agents_kit::conversation::NewConversation::new(None,None,vec![]).unwrap()).await.unwrap());
        let other = independent.as_ref().unwrap();
        let mut prompt = other.prompt_stream(PromptRequest::new("independent root remains available").unwrap()).await.unwrap();
        let other_turn = tokio::select! { turn = run.fixture.peer.turn() => turn, observed = prompt.recv() => panic!("independent prompt rejected before native admission: {observed:?}") };
        let other_thread = run.fixture.peer.requests.lock().unwrap().iter().rfind(|request| request["method"] == "turn/start").unwrap()["params"]["threadId"].as_str().unwrap().to_owned();
        assert_eq!(other_thread, "independent-thread-1");
        run.fixture.peer.notify("item/agentMessage/delta", json!({"threadId":other_thread,"turnId":other_turn,"itemId":"independent-acceptance","delta":"independent accepted"})).await;
        assert!(matches!(prompt.recv().await,Some(Ok(PromptEvent::OutputDelta{text})) if text == "independent accepted"));
        run.fixture.peer.notify("item/completed", json!({"threadId":other_thread,"turnId":other_turn,"completedAtMs":1,"item":{"id":"independent-answer","type":"agentMessage","text":"independent complete","phase":"final_answer"}})).await;
        run.fixture.peer.notify("turn/completed", json!({"threadId":other_thread,"turn":{"id":other_turn,"status":"completed","items":[],"error":null}})).await;
        assert!(matches!(prompt.recv().await,Some(Ok(PromptEvent::Completed(response))) if response.output() == "independent complete"));
        assert!(prompt.recv().await.is_none());
        assert_eq!(run.gate.retirement_receipts.load(Ordering::SeqCst),0);
        assert_eq!(run.native_starts(),2);
        run.gate.release.add_permits(1);
        while run.gate.retirement_receipts.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; }
        while run.gate.active_handlers.load(Ordering::SeqCst) != 0 { tokio::task::yield_now().await; }
        assert_eq!(run.gate.cancellations.load(Ordering::SeqCst),1);
        assert_eq!(run.gate.effects.load(Ordering::SeqCst),1);
        assert!(CausalRun::barrier(&mut run.fixture.peer,2906).await.is_empty());
        run.next_prompt().await;
    }).await;
    // Release owned storage gates before closing either conversation, including
    // assertion failures; never let cleanup obscure the original observation.
    run.gate.next.lock().unwrap().take();
    run.gate.release.add_permits(16);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    if let Some(other) = independent {
        cleanup.settle(other.close()).await;
    }
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(1),
        async {
            cleanup.finish(observed);
        },
    )
    .await;
    run.finish(observed).await;
}

#[tokio::test]
async fn codex_progress_tool_retirement_retains_a_distinct_toolset_shutdown_failure() {
    let mut run = CausalRun::new(
        CodexPeer::start_with(FirstStart::DeferredInterruptSettlement).await,
        ProgressFault::None,
        true,
    )
    .await;
    let mut retained = None;
    let prepared = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(18), async {
        let (mut stream, turn, before) = run.pending_retirement(RetirementFault::UnknownBeforeCommit).await;
        run.arm_retirement_abort().await;
        let failure = {
            let mut abort = Box::pin(run.fixture.conversation.abort());
            tokio::select! { entry = run.gate.entered.acquire() => entry.unwrap().forget(), result = &mut abort => panic!("abort did not retain committed intent: {result:?}") }
            run.settled_native_cancellation(&turn).await;
            run.gate.release.add_permits(1);
            tokio::select! {
                _ = async { while run.gate.retirement_receipts.load(Ordering::SeqCst) != 1 { tokio::task::yield_now().await; } } => {},
                result = &mut abort => panic!("retirement outcome did not settle: {result:?}"),
            }
            run.interrupted(&turn).await;
            abort.await.unwrap_err()
        };
        assert_eq!(failure.kind(), ErrorKind::Integration);
        assert_eq!(failure.detail(), Some(&agents_kit::error::ErrorDetail::ServiceUnavailable));
        assert_eq!(failure.context().phase, ErrorPhase::StateCommit);
        assert_eq!(stream.recv().await.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
        assert!(stream.recv().await.is_none());
        assert_eq!(run.journal_entries().await, before);
        assert!(run.retirement_obligation().await.is_some());
        assert_eq!(run.gate.retirement_attempts.load(Ordering::SeqCst),1);
        assert_eq!(run.gate.cancellations.load(Ordering::SeqCst),1);
        assert_eq!(run.gate.active_handlers.load(Ordering::SeqCst),0);
        assert!(CausalRun::barrier(&mut run.fixture.peer,2907).await.is_empty());
        retained = Some(failure);
    }).await;
    if !matches!(&prepared, Ok(Ok(()))) {
        run.finish(prepared).await;
        return;
    }
    let retained = retained.unwrap();
    run.gate.tool_shutdown_fails.store(true, Ordering::SeqCst);
    let CausalRun {
        fixture:
            CodexFixture {
                agent,
                workspace,
                conversation,
                mut peer,
                root,
            },
        raw: _,
        progress,
        gate,
    } = run;
    drop(conversation);
    drop(workspace);
    assert_eq!(gate.tool_shutdowns.load(Ordering::SeqCst), 0);
    let mut shutdown = Box::pin(agent.shutdown());
    let mut outcome = None;
    let observed = NativeCleanup::observe(tokio::time::Instant::now()+Duration::from_secs(8),async {
        outcome = Some((&mut shutdown).await);
        let error = outcome.as_ref().unwrap().as_ref().expect_err("both owned cleanup failures must remain observable");
        assert_eq!(error.kind(),retained.kind());
        assert_eq!(error.detail(),retained.detail());
        assert_eq!(error.message(),retained.message());
        assert_eq!(error.context().phase,retained.context().phase);
        assert_eq!(gate.tool_shutdowns.load(Ordering::SeqCst),1,"distinct Toolset cleanup actually ran exactly once");
        let mut errors = error.supplementary().iter().collect::<Vec<_>>();
        let mut found = false;
        while let Some(error) = errors.pop() {
            found |= error.kind() == ErrorKind::Integration
                && error.detail() == Some(&agents_kit::error::ErrorDetail::InvalidData)
                && error.message() == "tool configuration is invalid";
            errors.extend(error.supplementary()); errors.extend(error.cause());
        }
        assert!(found,"retirement remained primary but distinct Toolset shutdown failure was lost: {error:?}");
        assert_eq!(gate.cancellations.load(Ordering::SeqCst),1);
        assert_eq!(gate.effects.load(Ordering::SeqCst),1);
        assert_eq!(gate.active_handlers.load(Ordering::SeqCst),0);
    }).await;
    gate.next.lock().unwrap().take();
    *gate.retirement.lock().unwrap() = RetirementGate::Idle;
    gate.release.add_permits(16);
    gate.tool_release.add_permits(16);
    progress.release.add_permits(16);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    if outcome.is_none() {
        cleanup
            .settle(async {
                outcome = Some((&mut shutdown).await);
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
    }
    drop(shutdown);
    cleanup
        .settle(async {
            assert!(peer.responses.recv().await.is_none());
            Ok::<(), agents_kit::KitError>(())
        })
        .await;
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
