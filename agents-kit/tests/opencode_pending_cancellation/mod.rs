//! Caller-Store fault evidence for cleanup of one retained native question.
//!
//! The post-abort inventory gate selects the first changed continuation write,
//! not a private dispatch enum. Native effects and real delegated transaction
//! receipts are the oracle. These protocol fixtures are not strict-live proof.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use agents_kit::KitError;
use agents_kit::WorkspaceBackend;
use agents_kit::conversation::{ConversationBackend, NewConversation};
use agents_kit::error::{ErrorDetail, ErrorKind, ErrorPhase, Operation};
use agents_kit::notification::ConversationNotificationKind;
use agents_kit::prompt::PromptRequest;
use storage_kit::StateStoreBackend;
use storage_kit::state::record::{StateListRequest, StateRecord, StateRecordSnapshot};
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::native_cleanup::NativeCleanup;
use super::{
    OPENCODE_CONTINUATION_NAMESPACE, OPENCODE_EVENT_PROGRESS_NAMESPACE, OpenCodeEventProbe,
    OpenCodeStreamCase, PromptPersistenceFault, RetainedRejection,
};

/// Actual caller-owned record observations, with no private state-name oracle.
pub(super) struct NativeCleanupReceipt {
    pub(super) before: StateRecordSnapshot,
    pub(super) attempt: Option<StateRecord>,
    pub(super) writes: usize,
    pub(super) repeated_attempts: usize,
    pub(super) awaiting_rollback: bool,
    pub(super) rollback_attempts: usize,
    pub(super) rollback_confirmed: bool,
    pub(super) begin_conflict_task: Option<tokio::task::Id>,
    pub(super) begin_conflicts: usize,
    pub(super) reads_before_rollback: usize,
    pub(super) writes_before_rollback: usize,
    pub(super) committed_receipt_lost: bool,
    pub(super) commits: usize,
    pub(super) dispatch_before_commit: usize,
    pub(super) readbacks: usize,
    pub(super) dispatch_readbacks: Vec<usize>,
}

impl NativeCleanupReceipt {
    fn new(before: StateRecordSnapshot) -> Self {
        Self {
            before,
            attempt: None,
            writes: 0,
            repeated_attempts: 0,
            awaiting_rollback: false,
            rollback_attempts: 0,
            rollback_confirmed: false,
            begin_conflict_task: None,
            begin_conflicts: 0,
            reads_before_rollback: 0,
            writes_before_rollback: 0,
            committed_receipt_lost: false,
            commits: 0,
            dispatch_before_commit: 0,
            readbacks: 0,
            dispatch_readbacks: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum NativeCleanupCase {
    LoadConflict,
    LoadThenBeginConflict,
    PersistentLoadConflict,
    LoadRollbackFailure,
    PutConflict,
    PutRollbackFailure,
    CommitConflict,
    CommittedReceiptLost,
    UncommittedReceiptLost,
    UncommittedReceiptLostAfterPeerSettlement,
    UncommittedReceiptLostThenSelectedSettlement,
    UncommittedReceiptLostDuringCloseReconnect,
    UncommittedReceiptLostWhileOtherConversationCloses,
    UncommittedReceiptLostWhileOtherConversationSynchronizes,
}

impl NativeCleanupCase {
    async fn verify_idle_receipt(self) {
        assert!(matches!(
            self,
            Self::CommittedReceiptLost
                | Self::UncommittedReceiptLost
                | Self::PersistentLoadConflict
        ));
        let case = OpenCodeStreamCase::start().await;
        case.probe
            .stats
            .retained_question_history
            .store(true, Ordering::SeqCst);
        case.probe
            .stats
            .retain_question_after_abort
            .store(true, Ordering::SeqCst);
        case.probe
            .stats
            .malformed_stream_response
            .store(true, Ordering::SeqCst);
        case.probe.stats.reject_abort.store(true, Ordering::SeqCst);
        *case.probe.stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
        let mut aborts = JoinSet::new();
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(25), async {
            let runtime = tokio::runtime::Handle::current();
            let prompt_baseline = runtime.metrics().num_alive_tasks()
                - case.probe.stats.connections.load(Ordering::SeqCst);
            let mut notifications = case.conversation.notification_stream().unwrap();
            let mut output = case.stream().await;
            loop {
                tokio::select! {
                    item = notifications.recv() => {
                        if matches!(item.unwrap().unwrap().kind(), ConversationNotificationKind::Interaction { .. }) {
                            break;
                        }
                    }
                    item = output.recv() => panic!("the exact native question must precede idle cleanup: {item:?}"),
                }
            }
            case.wait_for_admission().await;
            let admitted_prompt = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
            let generation = admitted_prompt.record.value["message_id"].as_str()
                .filter(|value| !value.is_empty()).expect("the real prompt generation is serialized as message_id").to_owned();
            let request_part = admitted_prompt.record.value["request_part_id"].as_str()
                .filter(|value| !value.is_empty()).expect("the real prompt retains its native input-part identity").to_owned();
            let mut pending = case.faults.inner.list(
                StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap(),
            ).await.unwrap().records;
            assert_eq!(pending.len(), 1, "one real Pending interaction owns this preparation");
            let before = pending.pop().unwrap();
            assert_eq!(before.record.value.pointer("/request/question/id").and_then(serde_json::Value::as_str),
                Some("que_probe_pending"));
            *case.faults.state.native_cleanup_receipt.lock().unwrap() =
                Some(NativeCleanupReceipt::new(before.clone()));
            case.faults.set_persistence_fault(self.fault());
            *case.probe.stats.pending_cleanup_faults.lock().unwrap() = Some(case.faults.clone());
            // Arm against the already-admitted exact record before releasing
            // malformed HTTP. This selects idle preparation, before any native
            // pending-inventory request could arm the ordinary dispatch fixture.
            case.faults.arm_native_cleanup_receipt();
            case.release();
            let primary = OpenCodeStreamCase::next(&mut output).await
                .expect_err("lost HTTP keeps its original Protocol observation");
            assert_eq!(primary.kind(), ErrorKind::Protocol);
            assert!(output.recv().await.is_none());
            tokio::time::timeout(Duration::from_secs(6), async {
                while case.faults.state.persistence_attempts.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            }).await.expect("the selected idle preparation fault must actually occur");
            let preparation_observed = Instant::now();
            if matches!(self, Self::PersistentLoadConflict) {
                case.assert_persistence_retries_stopped(preparation_observed).await;
                tokio::time::timeout(Duration::from_secs(3), async {
                    while runtime.metrics().num_alive_tasks()
                        > prompt_baseline + case.probe.stats.connections.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                }).await.expect("expired foreground cleanup must end before new recovery authority is requested");
                let attempts = case.faults.state.persistence_attempts.load(Ordering::SeqCst);
                {
                    let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_ref().unwrap();
                    assert_eq!(receipt.rollback_attempts, attempts);
                    assert!(receipt.rollback_confirmed);
                    assert_eq!(receipt.writes, 0);
                    assert!(receipt.attempt.is_none());
                    assert!(receipt.dispatch_readbacks.is_empty());
                }
                assert_eq!(case.faults.inner.load(before.record.key.clone()).await.unwrap().unwrap(), before);
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
                let retained = case.faults.prompt_snapshot(&case.conversation.id()).await
                    .expect("the failed live owner must retain the original response-loss parent");
                assert_eq!(retained.record.value["message_id"].as_str(), Some(generation.as_str()));
                assert_eq!(retained.record.value["request_part_id"].as_str(), Some(request_part.as_str()));
                // New public recovery begins only after the previous retained
                // owner ended. It is distinct from retrying an unknown commit.
                *case.probe.stats.pending_cleanup_faults.lock().unwrap() = None;
                case.faults.state.native_cleanup_armed.store(false, Ordering::SeqCst);
                case.faults.set_persistence_fault(PromptPersistenceFault::None);
                let conversation = Arc::clone(&case.conversation);
                aborts.spawn(async move { conversation.abort().await });
                tokio::time::timeout(Duration::from_secs(3), async {
                    tokio::select! {
                        biased;
                        result = aborts.join_next() => panic!("fresh recovery ended before initial Pending native cleanup entered: {result:?}"),
                        permit = case.probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
                    }
                }).await.expect("fresh recovery must prepare and retain the original native interaction");
                let held = case.faults.prompt_snapshot(&case.conversation.id()).await
                    .expect("recovery cannot delete the native cleanup's parent before acknowledgment");
                assert_eq!(held.record.key, retained.record.key);
                assert_eq!(held.record.value["message_id"].as_str(), Some(generation.as_str()));
                assert_eq!(held.record.value["request_part_id"].as_str(), Some(request_part.as_str()));
                let busy = case.conversation.prompt(PromptRequest::new("recovered cleanup is still pending").unwrap())
                    .await.expect_err("held native recovery must retain admission");
                assert_eq!(busy.kind(), ErrorKind::Busy);
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
                case.probe.stats.rejection_release.add_permits(1);
                let repaired = aborts.join_next().await.unwrap().expect("recovery caller must join")
                    .expect_err("idle repair without accepted native abort remains an explicit repaired-invalid-state outcome");
                assert_eq!(repaired.kind(), ErrorKind::InvalidState);
                assert_eq!(repaired.context().operation, Operation::Abort);
                assert_eq!(repaired.context().phase, ErrorPhase::Cancellation);
                case.settled().await;
                let response = case.conversation.prompt(PromptRequest::new("return the reuse marker").unwrap()).await.unwrap();
                assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 0);
                assert!(!case.probe.stats.pending_question.load(Ordering::SeqCst));
                assert!(output.recv().await.is_none());
                return;
            }
            assert_eq!(case.faults.state.persistence_attempts.load(Ordering::SeqCst), 1);

            if matches!(self, Self::CommittedReceiptLost) {
                tokio::time::timeout(Duration::from_secs(3), case.probe.stats.rejection_entered.acquire())
                    .await.expect("exact observed preparation must permit one native rejection")
                    .unwrap().forget();
            }
            let retained_prompt = case.faults.prompt_snapshot(&case.conversation.id()).await
                .expect("native cleanup must retain its original prompt parent");
            assert_eq!(retained_prompt.record.key, admitted_prompt.record.key);
            assert_eq!(retained_prompt.record.value["message_id"].as_str(), Some(generation.as_str()));
            assert_eq!(retained_prompt.record.value["request_part_id"].as_str(), Some(request_part.as_str()));
            let busy = case.conversation.prompt(PromptRequest::new("idle cleanup still owns admission").unwrap())
                .await.expect_err("neither uncertainty nor held native cleanup can release admission");
            assert_eq!(busy.kind(), ErrorKind::Busy);

            if matches!(self, Self::UncommittedReceiptLost) {
                // Observe beyond the unchanged five-second lifecycle budget.
                // A new public Abort would confer distinct recovery authority,
                // so this no-replay control deliberately introduces none.
                tokio::time::sleep_until(preparation_observed + Duration::from_secs(6)).await;
                assert_eq!(case.faults.inner.load(before.record.key.clone()).await.unwrap().unwrap(), before,
                    "an unchanged prompt parent cannot confirm this unapplied continuation mutation");
            }
            assert_eq!(case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap(), retained_prompt,
                "the parent and revision remain retained while cleanup is held or uncertain");
            {
                let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                let receipt = receipt.as_ref().unwrap();
                assert!(receipt.attempt.is_some(), "the exact changed continuation was staged");
                eprintln!("idle cleanup receipt: writes={}, repeated_attempts={}, commits={}, committed_receipt_lost={}, readbacks={}, dispatch_readbacks={:?}",
                    receipt.writes, receipt.repeated_attempts, receipt.commits,
                    receipt.committed_receipt_lost, receipt.readbacks, receipt.dispatch_readbacks);
                assert_eq!(receipt.repeated_attempts, 0, "uncertain preparation cannot be rewritten");
                assert_eq!(receipt.dispatch_before_commit, 0);
                if matches!(self, Self::CommittedReceiptLost) {
                    assert!(receipt.committed_receipt_lost);
                    assert!(receipt.commits > 0);
                    assert_eq!(receipt.dispatch_readbacks.len(), 1);
                    assert!(receipt.dispatch_readbacks[0] > 0,
                        "native dispatch needs exact changed-continuation readback, not unchanged-parent evidence");
                } else {
                    assert!(!receipt.committed_receipt_lost);
                    assert_eq!(receipt.commits, 0);
                    assert_eq!(receipt.writes, 1);
                    assert!(receipt.dispatch_readbacks.is_empty());
                }
            }
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), usize::from(self.succeeds()));
            assert!(case.probe.stats.pending_question.load(Ordering::SeqCst));
            if self.succeeds() {
                case.probe.stats.rejection_release.add_permits(1);
                case.settled().await;
                let response = case.conversation.prompt(PromptRequest::new("return the reuse marker").unwrap()).await.unwrap();
                assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                assert!(!case.probe.stats.pending_question.load(Ordering::SeqCst));
            }
            assert!(output.recv().await.is_none(), "cleanup cannot replace the original fused Protocol observation");
        }).await;

        // Teardown may release test-owned native work only after the original
        // receipt/effect observations. It does not establish Kit settlement.
        *case.probe.stats.pending_cleanup_faults.lock().unwrap() = None;
        case.faults
            .state
            .native_cleanup_armed
            .store(false, Ordering::SeqCst);
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.probe.stats.rejection_release.add_permits(2);
        case.probe.stats.prompt_release.add_permits(2);
        case.probe.release_history();
        case.probe
            .stats
            .malformed_stream_response
            .store(false, Ordering::SeqCst);
        case.probe.stats.reject_abort.store(false, Ordering::SeqCst);
        case.probe
            .stats
            .pending_question
            .store(false, Ordering::SeqCst);
        let _ = case.probe.events.send(
            serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                    "id": "evt_idle_receipt_fixture_cleanup", "type": "question.rejected",
                    "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
                }
            })
            .to_string(),
        );
        let stats = Arc::clone(&case.probe.stats);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    result.expect("the recovery caller must join during independent teardown")?;
                }
                Ok::<(), KitError>(())
            })
            .await;
        aborts.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "recovery caller panicked: {error}")
                        }
                    }
                }
                Ok::<(), KitError>(())
            })
            .await;
        cleanup
            .settle(async {
                case.revoke().await;
                Ok::<(), KitError>(())
            })
            .await;
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), KitError>(())
            })
            .await;
        cleanup.finish(observed);
    }

    fn fault(self) -> PromptPersistenceFault {
        match self {
            Self::LoadConflict | Self::LoadRollbackFailure => {
                PromptPersistenceFault::NativeCleanupLoadConflictOnce
            }
            Self::LoadThenBeginConflict => {
                PromptPersistenceFault::NativeCleanupLoadThenBeginConflict
            }
            Self::PersistentLoadConflict => PromptPersistenceFault::NativeCleanupLoadConflict,
            Self::PutConflict | Self::PutRollbackFailure => {
                PromptPersistenceFault::NativeCleanupConflictOnce
            }
            Self::CommitConflict => PromptPersistenceFault::NativeCleanupCommitConflictOnce,
            Self::CommittedReceiptLost => PromptPersistenceFault::NativeCleanupCommitReceiptLost,
            Self::UncommittedReceiptLost
            | Self::UncommittedReceiptLostAfterPeerSettlement
            | Self::UncommittedReceiptLostThenSelectedSettlement
            | Self::UncommittedReceiptLostDuringCloseReconnect
            | Self::UncommittedReceiptLostWhileOtherConversationCloses
            | Self::UncommittedReceiptLostWhileOtherConversationSynchronizes => {
                PromptPersistenceFault::NativeCleanupCommitUnavailableOnce
            }
        }
    }

    fn fails_rollback(self) -> bool {
        matches!(self, Self::LoadRollbackFailure | Self::PutRollbackFailure)
    }

    fn succeeds(self) -> bool {
        !self.fails_rollback()
            && !matches!(
                self,
                Self::UncommittedReceiptLost
                    | Self::UncommittedReceiptLostAfterPeerSettlement
                    | Self::UncommittedReceiptLostThenSelectedSettlement
                    | Self::UncommittedReceiptLostDuringCloseReconnect
                    | Self::UncommittedReceiptLostWhileOtherConversationCloses
                    | Self::UncommittedReceiptLostWhileOtherConversationSynchronizes
                    | Self::PersistentLoadConflict
            )
    }

    async fn verify(self) {
        let mut case = if matches!(
            self,
            Self::UncommittedReceiptLostWhileOtherConversationSynchronizes
        ) {
            OpenCodeStreamCase::start_with_prompt_limit(
                NonZeroUsize::new(2).unwrap(),
                Duration::from_secs(15),
            )
            .await
        } else {
            OpenCodeStreamCase::start().await
        };
        case.probe
            .stats
            .retained_question_history
            .store(true, Ordering::SeqCst);
        case.probe
            .stats
            .retain_question_after_abort
            .store(true, Ordering::SeqCst);
        *case.probe.stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
        case.probe.stats.retained_parallel_questions.store(
            matches!(
                self,
                Self::UncommittedReceiptLostAfterPeerSettlement
                    | Self::UncommittedReceiptLostThenSelectedSettlement
            ),
            Ordering::SeqCst,
        );
        let mut aborts = JoinSet::new();
        let mut closes = JoinSet::new();
        let mut other = None;
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(25), async {
            if matches!(self, Self::UncommittedReceiptLostDuringCloseReconnect | Self::UncommittedReceiptLostWhileOtherConversationCloses | Self::UncommittedReceiptLostWhileOtherConversationSynchronizes) {
                other = Some(case.workspace.new_conversation(NewConversation::new(None, None, Vec::new()).unwrap()).await.unwrap());
            }
            let mut notifications = case.conversation.notification_stream().unwrap();
            let mut first = case.conversation.prompt_stream(
                PromptRequest::new("ask one owned question").unwrap()
            ).await.unwrap();
            loop {
                tokio::select! {
                    item = notifications.recv() => {
                        let item = item.unwrap().unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break;
                        }
                    }
                    result = first.recv() => panic!("question must be admitted before its cleanup fault: {result:?}"),
                }
            }
            let parallel = matches!(self, Self::UncommittedReceiptLostAfterPeerSettlement | Self::UncommittedReceiptLostThenSelectedSettlement);
            let mut records = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let records = case.faults.inner.list(
                        StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()
                    ).await.unwrap().records;
                    if records.len() == if parallel { 2 } else { 1 } { break records; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("every selected native question must be durably admitted");
            let selected = records.iter().position(|record|
                record.record.value.pointer("/request/question/id").and_then(serde_json::Value::as_str)
                    == Some("que_probe_pending")
            ).expect("the selected native question keeps its exact identity");
            let selected = records.remove(selected);
            let peer = records.pop();
            assert!(records.is_empty());
            assert_eq!(peer.is_some(), parallel);
            *case.faults.state.native_cleanup_receipt.lock().unwrap() =
                Some(NativeCleanupReceipt::new(selected));
            case.faults.set_persistence_fault(self.fault());
            case.faults.state.persistence_rollback_failure.store(self.fails_rollback(), Ordering::SeqCst);
            *case.probe.stats.pending_cleanup_faults.lock().unwrap() = Some(case.faults.clone());

            let conversation = Arc::clone(&case.conversation);
            let abort_started = Instant::now();
            aborts.spawn(async move { conversation.abort().await });
            if self.succeeds() {
                tokio::select! {
                    biased;
                    permit = case.probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
                    result = aborts.join_next() => panic!(
                        "native cleanup must reach its retained request; case={self:?}, injections={}, result={result:?}",
                        case.faults.state.persistence_attempts.load(Ordering::SeqCst)
                    ),
                }
                assert_eq!(case.faults.state.persistence_attempts.load(Ordering::SeqCst),
                    if matches!(self, Self::LoadThenBeginConflict) { 2 } else { 1 },
                    "every selected caller-Store failure must actually occur");
                {
                    let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_ref().unwrap();
                    assert_eq!(receipt.dispatch_readbacks.len(), 1);
                    assert_eq!(receipt.begin_conflicts, usize::from(matches!(self, Self::LoadThenBeginConflict)),
                        "only the exact rollback-confirmed task may consume the retry-begin failure");
                    assert!(receipt.begin_conflict_task.is_none());
                    assert!(receipt.attempt.is_some());
                    assert!(receipt.commits > 0, "a selected preparation must actually commit");
                    assert_eq!(receipt.dispatch_before_commit, 0,
                        "native rejection cannot precede durable preparation");
                    assert_eq!(receipt.reads_before_rollback, 0,
                        "fresh preparation cannot read before confirmed cleanup");
                    assert_eq!(receipt.writes_before_rollback, 0,
                        "fresh staging cannot start before confirmed cleanup");
                    if matches!(self, Self::CommittedReceiptLost) {
                        assert!(receipt.committed_receipt_lost);
                        assert!(receipt.dispatch_readbacks[0] > 0,
                            "native rejection must follow exact committed-receipt observation");
                        assert_eq!(receipt.repeated_attempts, 0,
                            "an ambiguous committed mutation cannot be replayed");
                    } else {
                        assert!(receipt.rollback_confirmed,
                            "the failed attempt must settle before successful retry");
                        assert_eq!(receipt.rollback_attempts, 1);
                    }
                }
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                case.probe.stats.rejection_release.add_permits(1);
                aborts.join_next().await.unwrap().expect("abort observer must join")
                    .expect("safe preparation retry or exact receipt must settle cancellation");
                let terminal = first.recv().await.unwrap().expect_err("cancelled prompt cannot succeed");
                assert_eq!(terminal.kind(), ErrorKind::Cancelled);
                assert!(first.recv().await.is_none());
                let response = case.conversation.prompt(
                    PromptRequest::new("return the reuse marker").unwrap()
                ).await.unwrap();
                assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
            } else {
                let result = if matches!(self, Self::PersistentLoadConflict) {
                    tokio::time::timeout_at(abort_started + Duration::from_secs(7), aborts.join_next())
                        .await.expect("permanent conflicts must honor the original five-second lifecycle deadline")
                } else {
                    aborts.join_next().await
                }.unwrap().expect("abort observer must join");
                let attempts = case.faults.state.persistence_attempts.load(Ordering::SeqCst);
                if matches!(self, Self::PersistentLoadConflict) {
                    assert!(attempts >= 2, "the exact typed conflict must reach bounded fresh retries");
                } else {
                    assert_eq!(attempts, 1,
                        "a pre-fault success is missing cleanup coverage, not transaction evidence");
                }
                let error = result.expect_err("failed or uncertain preparation cannot authorize rejection");
                let phase = if matches!(self, Self::LoadRollbackFailure | Self::PersistentLoadConflict) {
                    ErrorPhase::StateRead
                } else if matches!(self, Self::PutRollbackFailure) {
                    ErrorPhase::StateWrite
                } else {
                    ErrorPhase::StateCommit
                };
                if matches!(self, Self::PersistentLoadConflict) {
                    assert_eq!(error.kind(), ErrorKind::Timeout);
                    assert!(error.supplementary().iter().any(|error|
                        error.kind() == ErrorKind::Conflict
                            && error.context().phase == ErrorPhase::StateRead
                            && error.detail() == Some(&ErrorDetail::TransactionConflict)),
                        "deadline exhaustion must retain the last exact conflict: {error:?}");
                } else if self.fails_rollback() {
                    assert_eq!(error.kind(), ErrorKind::Conflict);
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert_eq!(error.context().phase, phase);
                    assert!(error.supplementary().iter().any(|error|
                        error.context().phase == ErrorPhase::Cleanup
                            && error.detail() == Some(&ErrorDetail::ServiceUnavailable)),
                        "rollback failure must remain separate: {error:?}");
                } else {
                    assert!(std::iter::once(&error).chain(error.supplementary()).any(|error|
                        error.context().phase == phase
                            && error.detail() == Some(&ErrorDetail::ServiceUnavailable)),
                        "uncertain commit must retain its typed failure: {error:?}");
                }
                let before = {
                    let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_ref().unwrap();
                    assert_eq!(receipt.writes, usize::from(!matches!(self, Self::LoadRollbackFailure | Self::PersistentLoadConflict)),
                        "failed rollback or ambiguous commit cannot begin another mutation");
                    assert_eq!(receipt.repeated_attempts, 0);
                    assert_eq!(receipt.reads_before_rollback, 0);
                    assert_eq!(receipt.writes_before_rollback, 0);
                    assert!(receipt.dispatch_readbacks.is_empty());
                    if matches!(self, Self::PersistentLoadConflict) {
                        assert!(receipt.rollback_confirmed);
                        assert_eq!(receipt.rollback_attempts, attempts,
                            "each admitted failed transaction must finish rollback before exhaustion");
                    } else if self.fails_rollback() {
                        assert_eq!(receipt.rollback_attempts, 1);
                        assert!(!receipt.rollback_confirmed);
                    }
                    receipt.before.clone()
                };
                let current = case.faults.inner.load(before.record.key.clone()).await.unwrap().unwrap();
                assert_eq!(current, before, "the injected unapplied attempt leaves committed state and revision unchanged");
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                assert!(case.probe.stats.pending_question.load(Ordering::SeqCst));
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                if matches!(self, Self::UncommittedReceiptLostDuringCloseReconnect | Self::UncommittedReceiptLostWhileOtherConversationCloses) {
                    let reconnect = matches!(self, Self::UncommittedReceiptLostDuringCloseReconnect);
                    let history = case.probe.stats.history.load(Ordering::SeqCst);
                    let subscriptions = case.probe.stats.subscriptions.load(Ordering::SeqCst);
                    let retired = subscriptions - case.probe.stats.active.load(Ordering::SeqCst);
                    let closing = if reconnect {
                        // Keep B owned by the existing fixture so failure teardown
                        // still retains the hierarchy after consuming A.
                        let original = std::mem::replace(&mut case.conversation, Arc::new(other.take().unwrap()));
                        Arc::try_unwrap(original).ok().expect("the completed Abort observer must release its borrowed conversation")
                    } else {
                        other.take().unwrap()
                    };
                    let closed_id = closing.id();
                    case.probe.block_history();
                    closes.spawn(async move { closing.close().await });
                    case.probe.wait_for_history(history + 1).await;
                    let premature = closes.try_join_next();
                    assert!(premature.is_none(), "consuming Close must remain pending while its actual history is held: {premature:?}");
                    if reconnect {
                        case.probe.disconnect_current();
                        tokio::time::timeout(Duration::from_secs(2), async {
                            while case.probe.stats.subscriptions.load(Ordering::SeqCst)
                                - case.probe.stats.active.load(Ordering::SeqCst) <= retired {
                                tokio::task::yield_now().await;
                            }
                        }).await.expect("the command's original native stream must retire before history is released");
                    }
                    case.probe.release_history();
                    let closed = closes.join_next().await.unwrap().expect("consuming Close task must join");
                    if reconnect {
                        let closed = closed.expect_err("Close must retain A's exact parked preparation failure after reconnect");
                        assert_eq!(closed.kind(), error.kind());
                        assert_eq!(closed.context().phase, error.context().phase);
                        assert_eq!(closed.detail(), error.detail());
                        tokio::time::timeout(Duration::from_secs(3), async {
                            while case.probe.stats.subscriptions.load(Ordering::SeqCst) <= subscriptions
                                || case.probe.stats.active.load(Ordering::SeqCst) != 1 {
                                tokio::task::yield_now().await;
                            }
                        }).await.expect("a parked command must retain its healthy replacement stream");
                    } else {
                        closed.expect("A's parked cleanup failure must not fail unrelated B's terminal Close");
                        assert!(matches!(case.workspace.get_conversation(closed_id).await.as_ref().map_err(KitError::kind), Err(ErrorKind::NotFound | ErrorKind::Closed)));
                    }
                    case.workspace.info().await.expect("an unrelated workspace remains usable after a parked cleanup");
                    case.conversation.info().await.expect("the still-owned conversation remains usable after a parked cleanup");
                    let sentinel = "evt_after_parked_close_command";
                    case.probe.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                        "id": sentinel, "type": "session.status",
                        "properties": {"sessionID": "ses_probe", "status": {"type": "idle"}}
                    }}).to_string()).unwrap();
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let progress = case.faults.inner.list(StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap()).await.unwrap();
                            if progress.records.iter().any(|record| record.record.value["recent"].as_array().is_some_and(|recent| recent.iter().any(|event| event["id"] == sentinel))) { break; }
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("unrelated native event delivery remains healthy after the command");
                    assert_eq!(case.faults.inner.load(before.record.key.clone()).await.unwrap().unwrap(), before);
                    let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_ref().unwrap();
                    assert_eq!(receipt.writes, 1, "no later command may replay A's unknown preparation");
                    assert_eq!(receipt.repeated_attempts, 0);
                    assert!(receipt.dispatch_readbacks.is_empty());
                    assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                    assert!(case.probe.stats.pending_question.load(Ordering::SeqCst));
                }
                if matches!(self, Self::UncommittedReceiptLostWhileOtherConversationSynchronizes) {
                    let history = case.probe.stats.history.load(Ordering::SeqCst);
                    let subscriptions = case.probe.stats.subscriptions.load(Ordering::SeqCst);
                    let response = other.as_ref().unwrap().prompt(
                        OpenCodeEventProbe::structured_request(Duration::from_secs(5))
                    ).await.expect("A's parked cleanup must not fail B's structured parent reconciliation");
                    assert_eq!(response.structured_output().and_then(|value| value.get("ack")).and_then(serde_json::Value::as_str), Some("STRUCTURED_ACK"));
                    assert!(case.probe.stats.history.load(Ordering::SeqCst) > history,
                        "B's exact parent must come from a history read after B's HTTP request");
                    assert_eq!(case.probe.stats.subscriptions.load(Ordering::SeqCst), subscriptions);
                    assert_eq!(case.probe.stats.active.load(Ordering::SeqCst), 1);
                    assert_eq!(case.faults.inner.load(before.record.key.clone()).await.unwrap().unwrap(), before);
                    let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_ref().unwrap();
                    assert_eq!(receipt.writes, 1);
                    assert_eq!(receipt.repeated_attempts, 0);
                    assert!(receipt.dispatch_readbacks.is_empty());
                    assert!(case.probe.stats.pending_question.load(Ordering::SeqCst));
                    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
                    assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                    return;
                }
                if matches!(self, Self::UncommittedReceiptLostThenSelectedSettlement) {
                    let peer = peer.as_ref().expect("the same-generation peer must remain owned");
                    let peer_before = case.faults.inner.load(peer.record.key.clone()).await.unwrap().unwrap();
                    assert_eq!(peer_before.record.value["generation_id"], before.record.value["generation_id"]);
                    case.probe.stats.pending_question.store(false, Ordering::SeqCst);
                    case.probe.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                        "id": "evt_independent_selected_rejected", "type": "question.rejected",
                        "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
                    }}).to_string()).unwrap();
                    // A terminal event settles its own request. After the first
                    // caller failed, a new explicit Abort owns the remaining
                    // pending-list reconciliation; no autonomous sweep is assumed.
                    let sentinel = "evt_after_selected_rejection";
                    case.probe.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                        "id": sentinel, "type": "session.status",
                        "properties": {"sessionID": "ses_probe", "status": {"type": "idle"}}
                    }}).to_string()).unwrap();
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let progress = case.faults.inner.list(StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap()).await.unwrap();
                            if progress.records.iter().any(|record| record.record.value["recent"].as_array()
                                .is_some_and(|recent| recent.iter().any(|event| event["id"] == sentinel))) { break; }
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("A's native settlement and postcommit owner update must finish");
                    let conversation = Arc::clone(&case.conversation);
                    aborts.spawn(async move { conversation.abort().await });
                    tokio::time::timeout(Duration::from_secs(3), async {
                        tokio::select! {
                            biased;
                            result = aborts.join_next() => panic!(
                                "fresh Abort ended before B's native cleanup entered: {result:?}"
                            ),
                            permit = case.probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
                        }
                    }).await.expect("fresh Abort must reconcile B after failed A authoritatively settles");
                    let selected_after = case.faults.inner.load(before.record.key.clone()).await.unwrap().unwrap();
                    assert_ne!(selected_after.record, before.record, "A's authoritative native settlement must really commit");
                    assert_eq!(selected_after.record.value["request"], before.record.value["request"]);
                    assert_eq!(selected_after.record.value["generation_id"], before.record.value["generation_id"]);
                    {
                        let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                        let receipt = receipt.as_ref().unwrap();
                        assert_ne!(Some(&selected_after.record), receipt.attempt.as_ref(),
                            "native settlement must not manufacture the uncommitted preparation receipt");
                        assert_eq!(receipt.repeated_attempts, 0, "A's uncertain mutation cannot be replayed");
                        assert!(receipt.dispatch_readbacks.is_empty(), "only B may dispatch after A independently settles");
                    }
                    assert!(!case.probe.stats.pending_question.load(Ordering::SeqCst));
                    assert!(case.probe.stats.pending_peer_question.load(Ordering::SeqCst));
                    assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                    let reuse = case.conversation.prompt(PromptRequest::new("B still owns cancellation").unwrap())
                        .await.expect_err("B's held native cleanup must retain generation admission");
                    assert_eq!(reuse.kind(), ErrorKind::Busy);
                    case.probe.stats.rejection_release.add_permits(1);
                    aborts.join_next().await.unwrap().expect("the later abort observer must join")
                        .expect("A's exact settlement permits the remaining B obligation to finish");
                    let terminal = first.recv().await.unwrap().expect_err("cancellation keeps its original terminal outcome");
                    assert_eq!(terminal.kind(), ErrorKind::Cancelled);
                    assert!(first.recv().await.is_none());
                    assert!(!case.probe.stats.pending_peer_question.load(Ordering::SeqCst));
                    let peer_after = case.faults.inner.load(peer.record.key.clone()).await.unwrap().unwrap();
                    assert_ne!(peer_after.record, peer_before.record, "B must durably settle after its native receipt");
                    assert_eq!(peer_after.record.value["request"], peer_before.record.value["request"]);
                    let response = case.conversation.prompt(PromptRequest::new("return the reuse marker").unwrap()).await.unwrap();
                    assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
                    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
                    assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
                    assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 0);
                    assert_eq!(case.probe.stats.permission_rejections.load(Ordering::SeqCst), 0);
                    assert_eq!(case.faults.state.native_cleanup_receipt.lock().unwrap().as_ref().unwrap().repeated_attempts, 0);
                    return;
                }
                if let Some(peer) = peer {
                    let peer_before = case.faults.inner.load(peer.record.key.clone()).await.unwrap().unwrap();
                    assert_eq!(peer_before.record.value["generation_id"], before.record.value["generation_id"],
                        "the independently settled peer must belong to the same prompt generation");
                    case.probe.stats.pending_peer_question.store(false, Ordering::SeqCst);
                    case.probe.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                        "id": "evt_independent_peer_rejected", "type": "question.rejected",
                        "properties": {"sessionID": "ses_probe", "requestID": "que_probe_peer"}
                    }}).to_string()).unwrap();
                    // The following same-stream event must be durably processed,
                    // so observing B's write alone cannot race its postcommit owner.
                    case.probe.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                        "id": "evt_after_independent_peer_rejection", "type": "session.status",
                        "properties": {"sessionID": "ses_probe", "status": {"type": "idle"}}
                    }}).to_string()).unwrap();
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let progress = case.faults.inner.list(StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap()).await.unwrap();
                            if progress.records.iter().any(|record|
                                record.record.value["recent"].as_array().is_some_and(|recent|
                                    recent.iter().any(|event| event["id"] == "evt_after_independent_peer_rejection"))) {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("independent peer settlement and its following event must complete");
                    let peer_after = case.faults.inner.load(peer.record.key).await.unwrap().unwrap();
                    assert_ne!(peer_after.record, peer_before.record, "B must really settle independently");
                    assert_eq!(peer_after.record.value["request"], peer_before.record.value["request"]);
                    let repeated = case.conversation.abort().await.expect_err("settling B cannot confirm A's unknown transaction");
                    assert_eq!(repeated.kind(), error.kind());
                    assert_eq!(repeated.context().phase, error.context().phase);
                    assert_eq!(repeated.detail(), error.detail());
                    assert_eq!(case.faults.inner.load(before.record.key.clone()).await.unwrap().unwrap(), before,
                        "unrelated native settlement cannot authorize another A mutation");
                    let receipt = case.faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_ref().unwrap();
                    assert_eq!(receipt.writes, 1);
                    assert_eq!(receipt.repeated_attempts, 0);
                    assert!(receipt.dispatch_readbacks.is_empty());
                    assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                    assert!(case.probe.stats.pending_question.load(Ordering::SeqCst));
                    assert!(!case.probe.stats.pending_peer_question.load(Ordering::SeqCst));
                }
            }
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(case.probe.stats.permission_rejections.load(Ordering::SeqCst), 0);
        }).await;

        // Complete test-owned native work only after capturing every effect and
        // receipt assertion. This is independent teardown, not Kit success.
        *case.probe.stats.pending_cleanup_faults.lock().unwrap() = None;
        case.faults
            .state
            .native_cleanup_armed
            .store(false, Ordering::SeqCst);
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.faults
            .state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
        case.probe.stats.rejection_release.add_permits(2);
        case.probe.stats.prompt_release.add_permits(2);
        case.probe.release_history();
        case.probe
            .stats
            .pending_question
            .store(false, Ordering::SeqCst);
        let _ = case.probe.events.send(
            serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                    "id": "evt_store_fault_cleanup_question_rejected", "type": "question.rejected",
                    "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
                }
            })
            .to_string(),
        );
        if case
            .probe
            .stats
            .pending_peer_question
            .swap(false, Ordering::SeqCst)
        {
            let _ = case.probe.events.send(
                serde_json::json!({
                    "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                        "id": "evt_store_fault_cleanup_peer_rejected", "type": "question.rejected",
                        "properties": {"sessionID": "ses_probe", "requestID": "que_probe_peer"}
                    }
                })
                .to_string(),
            );
        }
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = closes.join_next().await {
                    result.expect("Close observer must join after its captured observation")?;
                }
                Ok::<(), KitError>(())
            })
            .await;
        closes.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = closes.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "Close observer panicked: {error}")
                        }
                    }
                }
                Ok::<(), KitError>(())
            })
            .await;
        drop(other);
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    result.expect("abort observer must join")?;
                }
                Ok::<(), KitError>(())
            })
            .await;
        aborts.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "abort owner panicked: {error}")
                        }
                    }
                }
                Ok::<(), KitError>(())
            })
            .await;
        let stats = Arc::clone(&case.probe.stats);
        cleanup
            .settle(async {
                case.revoke().await;
                Ok::<(), KitError>(())
            })
            .await;
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), KitError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn native_cleanup_load_conflict_retries_after_rollback() {
    NativeCleanupCase::LoadConflict.verify().await;
}

#[tokio::test]
async fn native_cleanup_load_conflict_failed_rollback_does_not_reject() {
    NativeCleanupCase::LoadRollbackFailure.verify().await;
}

#[tokio::test]
async fn native_cleanup_put_conflict_retries_after_rollback() {
    NativeCleanupCase::PutConflict.verify().await;
}

#[tokio::test]
async fn native_cleanup_put_conflict_failed_rollback_does_not_reject() {
    NativeCleanupCase::PutRollbackFailure.verify().await;
}

#[tokio::test]
async fn native_cleanup_commit_conflict_recomputes_before_one_reject() {
    NativeCleanupCase::CommitConflict.verify().await;
}

#[tokio::test]
async fn native_cleanup_committed_lost_receipt_is_observed_without_rewrite() {
    NativeCleanupCase::CommittedReceiptLost.verify().await;
}

#[tokio::test]
async fn native_cleanup_uncommitted_lost_receipt_cannot_reject_or_rewrite() {
    NativeCleanupCase::UncommittedReceiptLost.verify().await;
}

#[tokio::test]
async fn native_cleanup_retry_begin_conflict_is_scoped_to_rollback_confirmed_owner() {
    NativeCleanupCase::LoadThenBeginConflict.verify().await;
}

#[tokio::test]
async fn native_cleanup_persistent_load_conflict_preserves_deadline_and_last_failure() {
    NativeCleanupCase::PersistentLoadConflict.verify().await;
}

#[tokio::test]
async fn native_cleanup_unrelated_same_generation_settlement_does_not_retry_unknown_commit() {
    NativeCleanupCase::UncommittedReceiptLostAfterPeerSettlement
        .verify()
        .await;
}

#[tokio::test]
async fn native_cleanup_parked_close_error_survives_command_reconnect() {
    NativeCleanupCase::UncommittedReceiptLostDuringCloseReconnect
        .verify()
        .await;
}

#[tokio::test]
async fn native_cleanup_parked_generation_does_not_fail_another_conversation_close() {
    NativeCleanupCase::UncommittedReceiptLostWhileOtherConversationCloses
        .verify()
        .await;
}

#[tokio::test]
async fn native_cleanup_failed_request_settlement_allows_remaining_same_generation_peer() {
    NativeCleanupCase::UncommittedReceiptLostThenSelectedSettlement
        .verify()
        .await;
}

#[tokio::test]
async fn native_cleanup_parked_generation_does_not_fail_another_prompt_synchronization() {
    NativeCleanupCase::UncommittedReceiptLostWhileOtherConversationSynchronizes
        .verify()
        .await;
}

#[tokio::test]
async fn idle_cleanup_committed_receipt_requires_changed_continuation_observation() {
    NativeCleanupCase::CommittedReceiptLost
        .verify_idle_receipt()
        .await;
}

#[tokio::test]
async fn idle_cleanup_unknown_receipt_cannot_use_unchanged_parent_or_replay() {
    NativeCleanupCase::UncommittedReceiptLost
        .verify_idle_receipt()
        .await;
}

#[tokio::test]
async fn idle_cleanup_recovered_pending_retains_parent_until_native_rejection() {
    NativeCleanupCase::PersistentLoadConflict
        .verify_idle_receipt()
        .await;
}
