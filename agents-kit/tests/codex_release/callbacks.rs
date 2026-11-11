use crate::{CodexFixture, CodexPeer, NativeCleanup};
use agents_kit::error::{ErrorDetail, ErrorKind};
use agents_kit::notification::{ConversationNotificationKind, NotificationResolution};
use agents_kit::prompt::{PromptEvent, PromptRequest};
use agents_kit::{AgentBackend, ConversationBackend, WorkspaceBackend};
use futures_util::FutureExt;
use serde_json::json;
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
use tokio::sync::Semaphore;

#[tokio::test]
async fn disappeared_callback_cancels_only_exact_pending_identity() {
    for delivered in [false, true] {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut fixture =
            CodexFixture::build_with_store(CodexPeer::start().await, raw.clone()).await;
        let observed = std::panic::AssertUnwindSafe(async { tokio::time::timeout(Duration::from_secs(3), async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("disappearing callback").unwrap()).await.unwrap();
            CodexFixture::next_progress(&mut notifications, "Preparing response").await;
            let turn = fixture.peer.accepted_turn(&mut stream).await;
            for id in [json!(81), json!("81")] {
                fixture.peer.events.send(json!({"id":id,"method":"item/commandExecution/requestApproval","params":{
                    "threadId":"stream-thread","turnId":turn,"itemId":"same-item","approvalId":"same-approval",
                    "kind":"command","startedAtMs":0,"availableDecisions":["accept","cancel"]}})).await.unwrap();
            }
            let tool_progress = notifications.recv().await.unwrap().unwrap();
            assert!(matches!(tool_progress.kind(), ConversationNotificationKind::Progress { label, detail: None } if label == "Tool calls requested"));
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"barrier","delta":"published"})).await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "published"));
            let before = raw.list(StateListRequest::new("agents_kit_codex_continuations_v1").unwrap()).await.unwrap();
            assert_eq!(before.records.len(), 2);
            let callback = before.records.iter().find(|record| record.record.value["request_id"] == 81).unwrap();
            let id = serde_json::from_value(callback.record.value["notification_id"].clone()).unwrap();
            if delivered {
                let first = notifications.recv().await.unwrap().unwrap();
                assert_eq!(first.id(), &id);
                assert!(matches!(first.kind(), ConversationNotificationKind::Interaction { .. }));
            }
            for (thread, request) in [("foreign-thread", json!(81)), ("stream-thread", json!("absent"))] {
                fixture.peer.notify("serverRequest/resolved", json!({"threadId":thread,"requestId":request})).await;
            }
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"barrier","delta":"foreign ignored"})).await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "foreign ignored"));
            assert_eq!(raw.load(callback.record.key.clone()).await.unwrap().unwrap().record.value, callback.record.value);
            for _ in 0..2 {
                fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread","requestId":81})).await;
            }
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"barrier","delta":"retired"})).await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "retired"));
            let retired = raw.load(callback.record.key.clone()).await.unwrap().unwrap();
            assert_eq!(retired.record.value["state"], "cancelled", "disappearance must retire the pending continuation");
            assert_eq!(fixture.conversation.resolve_notification(id, NotificationResolution::Allow).await.unwrap_err().kind(), ErrorKind::Conflict);
            assert!(fixture.peer.responses.try_recv().is_err(), "retired request must receive no stale reply");
            fixture.peer.events.send(json!({"id":81,"method":"item/commandExecution/requestApproval","params":{
                "threadId":"stream-thread","turnId":turn,"itemId":"same-item","approvalId":"same-approval",
                "kind":"command","startedAtMs":0,"availableDecisions":["accept","cancel"]}})).await.unwrap();
            let duplicate = fixture.peer.responses.recv().await.unwrap();
            assert_eq!(duplicate["id"], 81);
            assert_eq!(duplicate["error"]["code"], -32602, "retired identity cannot be republished");
            drop(notifications);
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            if !delivered {
                // Callback retirement must not acknowledge the preceding passive fact.
                let replayed = notifications.recv().await.unwrap().unwrap();
                assert_eq!(replayed, tool_progress);
            }
            let remaining = notifications.recv().await.unwrap().unwrap();
            let other = before.records.iter().find(|record| record.record.value["request_id"] == "81").unwrap();
            assert_eq!(json!(remaining.id()), other.record.value["notification_id"]);
            assert!(matches!(remaining.kind(), ConversationNotificationKind::Interaction { .. }));
            assert_eq!(fixture.conversation.resolve_notification(remaining.id().clone(), NotificationResolution::Allow).await.unwrap_err().kind(), ErrorKind::Conflict);
            assert_eq!(fixture.peer.responses.recv().await.unwrap(), json!({"id":"81","result":{"decision":"accept"}}));
            fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread","requestId":"81"})).await;
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"barrier","delta":"ambiguous"})).await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "ambiguous"));
            assert_eq!(raw.load(other.record.key.clone()).await.unwrap().unwrap().record.value["state"], "ambiguous");
            assert_eq!(fixture.conversation.resolve_notification(remaining.id().clone(), NotificationResolution::Allow).await.unwrap_err().kind(), ErrorKind::Conflict);
            fixture.peer.complete(&turn, "exact terminal").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "exact terminal"));
            assert!(stream.recv().await.is_none());
            fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread","requestId":81})).await;
            assert!(fixture.peer.responses.try_recv().is_err());
            drop(notifications);
            fixture.finish_next_prompt().await;
        }).await.expect("callback case bound"); }).catch_unwind().await;
        fixture.finish_release(observed, None).await;
    }
}

#[derive(Clone, Copy)]
enum CallbackRace {
    IntentCommitted,
    CancellationStaged,
    CancellationConflict,
    InventoryStalled,
    ConflictUntilDeadline,
    AmbiguousCommit,
    StagingAndRollbackFailure,
}

struct CallbackGate {
    race: CallbackRace,
    armed: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
    conflicts: AtomicUsize,
    unrelated_conflicts: AtomicUsize,
}

#[derive(Clone)]
struct CallbackStore {
    inner: StateStore,
    gate: Arc<CallbackGate>,
}

struct CallbackTransaction {
    inner: StateTransaction,
    gate: Arc<CallbackGate>,
    intent: bool,
    cancellation: bool,
}

impl StateStoreBackend for CallbackStore {
    type Transaction = CallbackTransaction;
    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }
    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        if matches!(self.gate.race, CallbackRace::InventoryStalled)
            && request.namespace().as_str() == "agents_kit_codex_continuations_v1"
            && self.gate.armed.swap(false, Ordering::SeqCst)
        {
            self.gate.entered.add_permits(1);
            self.gate.release.acquire().await.unwrap().forget();
        }
        self.inner.list(request).await
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(CallbackTransaction {
            inner: self.inner.begin_transaction().await?,
            gate: Arc::clone(&self.gate),
            intent: false,
            cancellation: false,
        })
    }
}

impl StateTransactionBackend for CallbackTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }
    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        if record.key.namespace().as_str() == "agents_kit_conversation_notifications_v1" {
            self.intent |= record.value["pending"].as_array().is_some_and(|pending| {
                pending
                    .iter()
                    .any(|entry| entry["state"]["state"] == "resolving")
            });
        }
        self.cancellation |= record.key.namespace().as_str() == "agents_kit_codex_continuations_v1"
            && record.value["state"] == "cancelled";
        if self.cancellation
            && self.gate.armed.load(Ordering::SeqCst)
            && matches!(self.gate.race, CallbackRace::StagingAndRollbackFailure)
        {
            self.gate.conflicts.fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::transaction_conflict(
                "put",
                "injected staging failure",
            ));
        }
        self.inner.put(record).await
    }
    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await
    }
    async fn commit(self) -> Result<(), StateStoreError> {
        let selected = match self.gate.race {
            CallbackRace::IntentCommitted => self.intent,
            CallbackRace::CancellationStaged
            | CallbackRace::CancellationConflict
            | CallbackRace::ConflictUntilDeadline
            | CallbackRace::AmbiguousCommit
            | CallbackRace::StagingAndRollbackFailure => self.cancellation,
            CallbackRace::InventoryStalled => false,
        };
        if selected
            && self.gate.armed.load(Ordering::SeqCst)
            && matches!(self.gate.race, CallbackRace::ConflictUntilDeadline)
        {
            self.inner.rollback().await?;
            self.gate.conflicts.fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::transaction_conflict(
                "commit",
                "injected persistent conflict",
            ));
        }
        if selected && self.gate.armed.swap(false, Ordering::SeqCst) {
            match self.gate.race {
                CallbackRace::IntentCommitted => {
                    self.inner.commit().await?;
                    self.gate.entered.add_permits(1);
                    self.gate.release.acquire().await.unwrap().forget();
                    return Ok(());
                }
                CallbackRace::CancellationStaged => {
                    self.gate.entered.add_permits(1);
                    self.gate.release.acquire().await.unwrap().forget();
                }
                CallbackRace::CancellationConflict => {
                    self.inner.rollback().await?;
                    self.gate.conflicts.fetch_add(1, Ordering::SeqCst);
                    return Err(StateStoreError::transaction_conflict(
                        "commit",
                        "injected callback retirement conflict",
                    ));
                }
                CallbackRace::AmbiguousCommit => {
                    self.inner.commit().await?;
                    self.gate.conflicts.fetch_add(1, Ordering::SeqCst);
                    return Err(StateStoreError::unavailable(
                        "commit",
                        "injected missing receipt",
                    ));
                }
                CallbackRace::InventoryStalled
                | CallbackRace::ConflictUntilDeadline
                | CallbackRace::StagingAndRollbackFailure => unreachable!("handled before commit"),
            }
        }
        let result = self.inner.commit().await;
        if matches!(&result, Err(StateStoreError::TransactionConflict { .. })) {
            // Progress publication/acknowledgment may conflict during setup.
            // Only this race's selected callback mutation is a retirement or
            // resolution attempt; retain other conflicts in a separate counter.
            if selected {
                self.gate.conflicts.fetch_add(1, Ordering::SeqCst);
            } else {
                self.gate.unrelated_conflicts.fetch_add(1, Ordering::SeqCst);
            }
        }
        result
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await?;
        if self.cancellation
            && self.gate.armed.swap(false, Ordering::SeqCst)
            && matches!(self.gate.race, CallbackRace::StagingAndRollbackFailure)
        {
            return Err(StateStoreError::unavailable(
                "rollback",
                "injected rollback receipt loss",
            ));
        }
        Ok(())
    }
}

impl CallbackRace {
    async fn failure(self) {
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let gate = Arc::new(CallbackGate {
            race: self,
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            conflicts: AtomicUsize::new(0),
            unrelated_conflicts: AtomicUsize::new(0),
        });
        let store = CallbackStore {
            inner: raw.clone(),
            gate: Arc::clone(&gate),
        };
        let mut fixture = CodexFixture::build_with_store(CodexPeer::start().await, store).await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("callback retirement failure").unwrap()).await.unwrap();
            CodexFixture::next_progress(&mut notifications, "Preparing response").await;
            let turn = fixture.peer.accepted_turn(&mut stream).await;
            fixture.peer.request(101, "item/commandExecution/requestApproval", json!({"threadId":"stream-thread","turnId":turn,"itemId":"command","startedAtMs":0,"availableDecisions":["accept","cancel"]})).await;
            CodexFixture::next_progress(&mut notifications, "Tool calls requested").await;
            let interaction = tokio::time::timeout(Duration::from_secs(3), notifications.recv()).await.unwrap().unwrap().unwrap();
            assert!(matches!(interaction.kind(), ConversationNotificationKind::Interaction { .. }));
            let key = raw.list(StateListRequest::new("agents_kit_codex_continuations_v1").unwrap()).await.unwrap().records[0].record.key.clone();
            assert_eq!(gate.conflicts.load(Ordering::SeqCst), 0, "setup cannot attempt the selected callback mutation");
            let unrelated_conflicts = gate.unrelated_conflicts.load(Ordering::SeqCst);
            gate.armed.store(true, Ordering::SeqCst);
            fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread","requestId":101})).await;
            if matches!(self, Self::InventoryStalled) {
                tokio::time::timeout(Duration::from_secs(1), gate.entered.acquire()).await.unwrap().unwrap().forget();
            }
            let error = tokio::time::timeout(Duration::from_secs(4), stream.recv()).await.expect("retirement must retain its original lifecycle bound").unwrap().unwrap_err();
            match self {
                Self::InventoryStalled => assert_eq!(error.kind(), ErrorKind::Timeout),
                Self::ConflictUntilDeadline => {
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert!((2..=125).contains(&gate.conflicts.load(Ordering::SeqCst)), "one original deadline bounds retries");
                }
                Self::AmbiguousCommit => {
                    assert_eq!(error.kind(), ErrorKind::Integration);
                    assert_eq!(gate.conflicts.load(Ordering::SeqCst), 1, "uncertain receipt cannot replay commit");
                }
                Self::StagingAndRollbackFailure => {
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert_eq!(error.supplementary().len(), 1);
                    assert_eq!(gate.conflicts.load(Ordering::SeqCst), 1, "cleanup failure prevents retry");
                }
                Self::IntentCommitted | Self::CancellationStaged | Self::CancellationConflict => unreachable!("separate race case"),
            }
            assert_eq!(gate.unrelated_conflicts.load(Ordering::SeqCst), unrelated_conflicts, "no unrelated transaction may consume the callback fault window");
            assert!(fixture.peer.responses.try_recv().is_err(), "disappearance never sends approval");
            let state = raw.load(key).await.unwrap().unwrap().record.value["state"].clone();
            assert_eq!(state, if matches!(self, Self::AmbiguousCommit) { "cancelled" } else { "pending" });
            assert!(stream.recv().await.is_none());
        }).catch_unwind().await;
        gate.armed.store(false, Ordering::SeqCst);
        gate.release.add_permits(1);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        cleanup
            .settle(async {
                fixture.revoke(baseline + 1).await;
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup.finish(observed.map(Ok));
    }

    async fn run(self) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let gate = Arc::new(CallbackGate {
            race: self,
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            conflicts: AtomicUsize::new(0),
            unrelated_conflicts: AtomicUsize::new(0),
        });
        let store = CallbackStore {
            inner: raw.clone(),
            gate: Arc::clone(&gate),
        };
        let mut fixture = CodexFixture::build_with_store(CodexPeer::start().await, store).await;
        let observed = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                let mut notifications = fixture.conversation.notification_stream().unwrap();
                let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("callback race").unwrap()).await.unwrap();
                CodexFixture::next_progress(&mut notifications, "Preparing response").await;
                let turn = fixture.peer.accepted_turn(&mut stream).await;
                fixture.peer.request(99, "item/commandExecution/requestApproval", json!({"threadId":"stream-thread","turnId":turn,"itemId":"command","startedAtMs":0,"availableDecisions":["accept","cancel"]})).await;
                CodexFixture::next_progress(&mut notifications, "Tool calls requested").await;
                let notification = notifications.recv().await.unwrap().unwrap();
                assert!(matches!(notification.kind(), ConversationNotificationKind::Interaction { .. }));
                let key = raw.list(StateListRequest::new("agents_kit_codex_continuations_v1").unwrap()).await.unwrap().records[0].record.key.clone();
                assert_eq!(gate.conflicts.load(Ordering::SeqCst), 0, "setup cannot attempt the selected callback mutation");
                let unrelated_conflicts = gate.unrelated_conflicts.load(Ordering::SeqCst);
                gate.armed.store(true, Ordering::SeqCst);
                let mut resolve = Box::pin(fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow));
                match self {
                    Self::IntentCommitted => {
                        tokio::select! {
                            entered = gate.entered.acquire() => entered.unwrap().forget(),
                            result = &mut resolve => panic!("intent observation should be held: {result:?}"),
                        }
                        assert!(fixture.peer.responses.try_recv().is_err(), "no response before committed intent observation");
                        fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread","requestId":99})).await;
                        fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"barrier","delta":"intent retained"})).await;
                        assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "intent retained"));
                        assert_eq!(raw.load(key.clone()).await.unwrap().unwrap().record.value["state"], "pending", "pending continuation with resolving journal is not cancellable");
                        gate.release.add_permits(1);
                        assert_eq!(resolve.await.unwrap_err().kind(), ErrorKind::Conflict);
                    }
                    Self::CancellationStaged => {
                        fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread","requestId":99})).await;
                        gate.entered.acquire().await.unwrap().forget();
                        assert_eq!(resolve.await.unwrap_err().kind(), ErrorKind::Conflict);
                        gate.release.add_permits(1);
                    }
                    Self::CancellationConflict => {
                        fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread","requestId":99})).await;
                        drop(resolve);
                    }
                    Self::InventoryStalled | Self::ConflictUntilDeadline | Self::AmbiguousCommit
                    | Self::StagingAndRollbackFailure => unreachable!("separate failure case"),
                }
                fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"barrier","delta":"settled race"})).await;
                assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "settled race"));
                let expected = if matches!(self, Self::CancellationConflict) { "cancelled" } else { "ambiguous" };
                assert_eq!(raw.load(key).await.unwrap().unwrap().record.value["state"], expected);
                if matches!(self, Self::CancellationConflict) {
                    assert!(fixture.peer.responses.try_recv().is_err());
                } else {
                    assert_eq!(fixture.peer.responses.recv().await.unwrap(), json!({"id":99,"result":{"decision":"accept"}}));
                }
                assert_eq!(gate.unrelated_conflicts.load(Ordering::SeqCst), unrelated_conflicts, "no unrelated transaction may consume the callback fault window");
                assert_eq!(gate.conflicts.load(Ordering::SeqCst), usize::from(!matches!(self, Self::IntentCommitted)));
                assert_eq!(fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await.unwrap_err().kind(), ErrorKind::Conflict);
                assert!(fixture.peer.responses.try_recv().is_err());
                fixture.peer.complete(&turn, "race terminal").await;
                assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "race terminal"));
                assert!(stream.recv().await.is_none());
            }).await.expect("callback race bound");
        }).catch_unwind().await;
        gate.armed.store(false, Ordering::SeqCst);
        gate.release.add_permits(1);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        cleanup.settle(fixture.conversation.close()).await;
        cleanup.settle(fixture.workspace.close()).await;
        cleanup.settle(fixture.agent.shutdown()).await;
        cleanup
            .settle(async {
                fixture.peer.shutdown().await;
                Ok::<(), agents_kit::KitError>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(fixture.root) })
            .await;
        cleanup.finish(observed.map(Ok));
    }
}

#[tokio::test]
async fn disappeared_callback_preserves_admitted_resolution_intent() {
    CallbackRace::IntentCommitted.run().await;
}

#[tokio::test]
async fn disappeared_callback_rechecks_intent_after_transaction_conflict() {
    CallbackRace::CancellationStaged.run().await;
}

#[tokio::test]
async fn disappeared_callback_retries_only_definite_retirement_conflict() {
    CallbackRace::CancellationConflict.run().await;
}

#[tokio::test]
async fn disappeared_callback_bounds_inventory_observation() {
    CallbackRace::InventoryStalled.failure().await;
}

#[tokio::test]
async fn disappeared_callback_bounds_conflicts_and_preserves_uncertain_failures() {
    for case in [
        CallbackRace::ConflictUntilDeadline,
        CallbackRace::AmbiguousCommit,
        CallbackRace::StagingAndRollbackFailure,
    ] {
        case.failure().await;
    }
}
