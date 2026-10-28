//! Recovered Abort transaction authority through a caller StateStore. The
//! selected task/key is armed by a real read and confirmed rollback; native
//! protocol effects remain observable independently of transaction receipts.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use agents_kit::KitError;
use agents_kit::conversation::ConversationBackend;
use agents_kit::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase, Operation};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::native_cleanup::NativeCleanup;
use super::{OpenCodeStreamCase, PreparedCancelFault, PromptFaultStore, PromptPersistenceFault};

#[derive(Clone, Copy, Debug)]
enum AbortReconciliationCase {
    ReadConflict,
    ReadRollbackFailure,
    PersistentReadConflict,
    NoopRollbackFailure,
    BeginConflict,
    DeleteConflict,
    CommitConflict,
    LostReceipt,
    UnknownReceipt,
    ForeignReceipt,
    LostReceiptReadFailure,
    UnknownReceiptReadFailure,
    TerminalDelete,
    TerminalUnknownReceipt,
    DroppedCommitObserver,
    DroppedUnknownObserver,
}

impl AbortReconciliationCase {
    fn terminal(self) -> bool {
        matches!(self, Self::TerminalDelete | Self::TerminalUnknownReceipt)
    }

    fn idle(self) -> bool {
        !matches!(
            self,
            Self::ReadConflict
                | Self::ReadRollbackFailure
                | Self::PersistentReadConflict
                | Self::NoopRollbackFailure
                | Self::BeginConflict
        )
    }

    fn fault(self) -> PreparedCancelFault {
        match self {
            Self::ReadConflict | Self::ReadRollbackFailure => PreparedCancelFault::LoadConflictOnce,
            Self::PersistentReadConflict => PreparedCancelFault::LoadConflict,
            Self::NoopRollbackFailure | Self::TerminalDelete => PreparedCancelFault::None,
            Self::BeginConflict => PreparedCancelFault::BeginConflictOnce,
            Self::DeleteConflict => PreparedCancelFault::DeleteConflictOnce,
            Self::CommitConflict => PreparedCancelFault::CommitConflictOnce,
            Self::LostReceipt => PreparedCancelFault::CommitReceiptLost,
            Self::UnknownReceipt | Self::TerminalUnknownReceipt => {
                PreparedCancelFault::CommitUnavailableOnce
            }
            Self::ForeignReceipt => PreparedCancelFault::CommitUnavailableForeignReceipt,
            Self::LostReceiptReadFailure => {
                PreparedCancelFault::CommitReceiptLostReadUnavailableOnce
            }
            Self::UnknownReceiptReadFailure => {
                PreparedCancelFault::CommitUnavailableReadUnavailableOnce
            }
            Self::DroppedCommitObserver => PreparedCancelFault::CommitReceiptHeld,
            Self::DroppedUnknownObserver => PreparedCancelFault::CommitUnavailableReadHeld,
        }
    }

    async fn verify(self) {
        let case = OpenCodeStreamCase::start().await;
        let stats = Arc::clone(&case.probe.stats);
        let mut aborts = JoinSet::new();
        let mut prompt_callers = JoinSet::new();
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(25), async {
            let runtime = tokio::runtime::Handle::current();
            let baseline = runtime.metrics().num_alive_tasks() - stats.connections.load(Ordering::SeqCst);
            let mut stream = if matches!(self, Self::NoopRollbackFailure) {
                let conversation = Arc::clone(&case.conversation);
                let caller = prompt_callers.spawn(async move {
                    conversation.prompt(OpenCodeStreamCase::request()).await
                });
                case.probe.wait_for_messages(1).await;
                case.begin_text();
                case.wait_for_admission().await;
                caller.abort();
                let joined = prompt_callers.join_next().await.unwrap()
                    .expect_err("only the unary prompt observer is cancelled");
                assert!(joined.is_cancelled());
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let record = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                        if record.record.value["waiter"] == "lost" { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("dropping the actual unary observer must durably end its waiter");
                case.probe.stats.malformed_stream_response.store(true, Ordering::SeqCst);
                case.faults.set_persistence_fault(PromptPersistenceFault::ResponseLossSettle);
                case.release();
                None
            } else if self.terminal() {
                // Native success is actually persisted, but delivery settlement
                // fails under its original budget. No durable row is rewritten.
                case.faults.state.waiter_fault.store(4, Ordering::SeqCst);
                let stream = case.stream().await;
                case.begin_text();
                case.release();
                Some(stream)
            } else if self.idle() {
                // Preserve a genuinely admitted original generation when its
                // malformed final HTTP response cannot be durably recorded.
                case.probe.stats.malformed_stream_response.store(true, Ordering::SeqCst);
                case.faults.set_persistence_fault(PromptPersistenceFault::ResponseLoss);
                let mut stream = case.stream().await;
                case.begin_text();
                case.delta("evt_reconciliation_admitted", OpenCodeStreamCase::ASSISTANT,
                    "prt_stream_text", "text", "accepted");
                OpenCodeStreamCase::next(&mut stream).await.expect("real admitted delta must be observed");
                case.wait_for_admission().await;
                case.release();
                Some(stream)
            } else {
                Some(case.stream_with_admission_fault(PromptPersistenceFault::Admission).await)
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let attempts = if self.terminal() {
                        case.faults.state.waiter_fault_attempts.load(Ordering::SeqCst)
                    } else {
                        case.faults.state.persistence_attempts.load(Ordering::SeqCst)
                    };
                    if attempts != 0 { break; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("setup must enter its actual durable failure");
            let first_attempt = Instant::now();
            if let Some(stream) = &mut stream {
                let error = tokio::time::timeout(Duration::from_secs(7), stream.recv()).await
                    .expect("original observation must end within its lifecycle bound")
                    .expect("one original terminal is required").expect_err("setup deliberately retains recovery work");
                assert_eq!(error.kind(), if self.idle() && !self.terminal() { ErrorKind::Protocol } else { ErrorKind::Timeout }, "{error:?}");
                assert!(stream.recv().await.is_none());
            } else {
                assert_eq!(stats.prompt_responses.load(Ordering::SeqCst), 1,
                    "the real malformed HTTP response must precede its failed persistence");
            }
            if !self.terminal() {
                case.assert_persistence_retries_stopped(first_attempt).await;
            }
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks() > baseline + stats.connections.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            }).await.expect("original foreground must release runtime ownership before recovery");
            let original = case.faults.prompt_snapshot(&case.conversation.id()).await
                .expect("real original prompt state must remain");
            assert_eq!(original.record.value["phase"]["phase"] == "terminal", self.terminal());
            if self.terminal() {
                assert_eq!(original.record.value["phase"]["outcome"]["outcome"], "success");
                assert_eq!(original.record.value["waiter"], "delivering");
            } else if matches!(self, Self::NoopRollbackFailure) {
                assert_eq!(original.record.value["phase"]["phase"], "admitted");
                assert_eq!(original.record.value["cancellation"]["state"], "not_requested");
                assert_eq!(original.record.value["waiter"], "lost");
            } else if self.idle() {
                assert_eq!(original.record.value["phase"]["phase"], "admitted");
                assert_eq!(original.record.value["cancellation"]["state"], "not_requested");
                assert_eq!(original.record.value["waiter"], "waiting",
                    "the failed response-loss transaction must not fabricate a durable waiter transition");
            }
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            case.faults.set_persistence_fault(PromptPersistenceFault::None);
            case.faults.state.waiter_fault.store(0, Ordering::SeqCst);
            if self.idle() { case.probe.mark_native_idle(); } else { case.probe.mark_native_active(); }
            case.faults.state.persistence_rollback_failure.store(
                matches!(self, Self::ReadRollbackFailure | Self::NoopRollbackFailure), Ordering::SeqCst);
            let (start, admitted) = oneshot::channel();
            let conversation = Arc::clone(&case.conversation);
            let observer = aborts.spawn(async move {
                admitted.await.expect("selected Abort must be admitted");
                conversation.abort().await
            });
            let key = PromptFaultStore::prompt_key(&case.conversation.id());
            if self.terminal() {
                case.faults.arm_terminal_reconciliation_task(self.fault(), observer.id(), key);
            } else {
                case.faults.arm_abort_reconciliation(self.fault(), observer.id(), key);
            }
            let started = Instant::now();
            start.send(()).unwrap();
            let mut uncertain_record = None;
            let result = if matches!(self, Self::DroppedCommitObserver | Self::DroppedUnknownObserver) {
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        biased;
                        result = aborts.join_next() => panic!("original Abort returned before the deletion receipt boundary: {result:?}"),
                        permit = case.faults.state.blocked.acquire() => permit.unwrap().forget(),
                    }
                }).await.expect("the actual deletion receipt boundary must be observed before caller loss");
                let gate_record = case.faults.prompt_snapshot(&case.conversation.id()).await;
                assert_eq!(gate_record.is_none(), matches!(self, Self::DroppedCommitObserver));
                if matches!(self, Self::DroppedUnknownObserver) {
                    uncertain_record = gate_record;
                    let receipt = case.faults.prepared_cancel_receipt();
                    assert_eq!(receipt.deletes, 1);
                    assert_eq!(receipt.commits, 0);
                    assert_eq!(receipt.receipt_reads, 1,
                        "first observer must already hold the actual unknown-commit readback");
                }
                let conversation = Arc::clone(&case.conversation);
                let (start_second, admitted_second) = oneshot::channel();
                let second = aborts.spawn(async move {
                    admitted_second.await.expect("second observer must be admitted");
                    conversation.abort().await
                });
                case.faults.arm_reconciliation_observer(
                    second.id(), PromptFaultStore::prompt_key(&case.conversation.id()));
                start_second.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(3), async {
                    tokio::select! {
                        biased;
                        result = aborts.join_next() => panic!("an observer returned before its exact prompt read was held: {result:?}"),
                        () = case.faults.wait_reconciliation_observer() => {}
                    }
                }).await.expect("second observer must attach to the retained generation before first observer loss");
                observer.abort();
                let stopped = aborts.join_next_with_id().await.unwrap()
                    .expect_err("only the first public observer must be cancelled");
                assert_eq!(stopped.id(), observer.id());
                assert!(stopped.is_cancelled());
                case.faults.release();
                case.faults.release_reconciliation_observer();
                let mut second_result = None;
                tokio::time::timeout(Duration::from_secs(7), async {
                    while let Some(result) = aborts.join_next_with_id().await {
                        match result {
                            Ok((id, result)) => {
                                assert_eq!(id, second.id(), "the original observer was explicitly cancelled");
                                second_result = Some(result);
                            }
                            Err(error) => panic!("second observer must join normally: {error}"),
                        }
                    }
                }).await.expect("both owned observers must join after the receipt is released");
                second_result.expect("the already participating second observer must retain the original outcome")
            } else {
                tokio::time::timeout(Duration::from_secs(7), aborts.join_next()).await
                    .expect("recovered Abort must keep its original lifecycle deadline")
                    .unwrap().expect("the selected observer must join")
            };
            if let Some(expected) = &uncertain_record {
                let actual = case.faults.prompt_snapshot(&case.conversation.id()).await;
                eprintln!("unknown overlapping observer: backing_record_unchanged={} backing_record_present={}",
                    actual.as_ref() == Some(expected), actual.is_some());
            }
            let receipt = case.faults.prepared_cancel_receipt();
            eprintln!("abort reconciliation {self:?}: result={result:?}; activation_reads={} activation_rollbacks={} begins={} reads={} puts={} deletes={} commit_attempts={} commits={} rollbacks={} rollback_failures={} faults={} receipt_reads={} receipt_read_failures={}",
                receipt.activation_reads, receipt.activation_rollbacks, receipt.begins, receipt.reads,
                receipt.puts, receipt.deletes, receipt.commit_attempts, receipt.commits, receipt.rollbacks,
                receipt.rollback_failures, receipt.faults, receipt.receipt_reads, receipt.receipt_read_failures);
            let before = receipt.before.as_ref().expect("fault boundary must read the actual retained generation");
            assert_eq!(before.record.key, original.record.key);
            assert_eq!(before.record.value["message_id"], original.record.value["message_id"]);
            assert_eq!(before.record.value["request_part_id"], original.record.value["request_part_id"]);
            if !self.terminal() {
                assert!(receipt.activation_reads >= 1);
                assert!(receipt.activation_rollbacks >= 1);
                let intent = receipt.intent_record.as_ref().expect("the original cancellation intent must be committed");
                assert_eq!(intent.value["cancellation"]["state"], "requested");
                assert_eq!(intent.value["message_id"], original.record.value["message_id"]);
            }
            assert_eq!(receipt.accesses_before_rollback, 0);
            assert_eq!(receipt.deletes_after_commit, 0);
            assert_eq!(receipt.writes_after_commit, 0);
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
            match self {
                Self::ReadRollbackFailure => {
                    let error = result.expect_err("failed rollback cannot authorize another attempt");
                    assert_eq!(error.kind(), ErrorKind::Conflict);
                    assert_eq!(error.context().phase, ErrorPhase::StateRead);
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    let [cleanup] = error.supplementary() else { panic!("rollback failure must remain: {error:?}") };
                    assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                    assert_eq!(cleanup.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(receipt.faults, 1);
                    assert_eq!(receipt.rollback_failures, 1);
                }
                Self::NoopRollbackFailure => {
                    let error = result.expect_err("unchanged reconciliation must settle its transaction");
                    assert_eq!(error.kind(), ErrorKind::Integration);
                    assert_eq!(error.context().phase, ErrorPhase::Cleanup);
                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(receipt.rollback_failures, 1);
                    assert_eq!(receipt.deletes, 0);
                    assert_eq!(receipt.puts, 0, "the selected reconciliation must really be unchanged");
                    assert_eq!(before.record.value["waiter"], "lost");
                }
                Self::PersistentReadConflict => {
                    let error = result.expect_err("persistent definite read conflicts must remain bounded");
                    assert_eq!(error.kind(), ErrorKind::Timeout);
                    assert_eq!(error.context().operation, Operation::Abort);
                    assert_eq!(error.context().phase, ErrorPhase::Reconciliation);
                    assert_eq!(error.detail(), Some(&ErrorDetail::DeadlineElapsed { kind: DeadlineKind::Operation, timeout: None }));
                    let [last] = error.supplementary() else { panic!("last read conflict must survive: {error:?}") };
                    assert_eq!(last.context().phase, ErrorPhase::StateRead);
                    assert_eq!(last.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert!(receipt.faults > 1);
                    assert_eq!(receipt.rollbacks, receipt.faults);
                    assert!(started.elapsed() >= Duration::from_secs(5));
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    let after = case.faults.prepared_cancel_receipt();
                    assert_eq!(after.reads, receipt.reads);
                    assert_eq!(after.begins, receipt.begins);
                    assert_eq!(after.deletes, receipt.deletes);
                }
                Self::UnknownReceipt | Self::ForeignReceipt | Self::UnknownReceiptReadFailure | Self::TerminalUnknownReceipt | Self::DroppedUnknownObserver => {
                    let error = result.expect_err("unconfirmed commit cannot release or repeat this transition");
                    let uncertainty = if matches!(self, Self::DroppedUnknownObserver) && error.kind() == ErrorKind::Timeout {
                        assert_eq!(error.context().phase, ErrorPhase::Reconciliation, "{error:?}");
                        assert_eq!(error.detail(), Some(&ErrorDetail::DeadlineElapsed { kind: DeadlineKind::Operation, timeout: None }));
                        let [original] = error.supplementary() else { panic!("known commit uncertainty must remain: {error:?}") };
                        original
                    } else { &error };
                    assert_eq!(uncertainty.kind(), ErrorKind::Integration, "{error:?}");
                    assert_eq!(uncertainty.context().phase, ErrorPhase::StateCommit, "{error:?}");
                    assert_eq!(uncertainty.detail(), Some(&ErrorDetail::ServiceUnavailable), "{error:?}");
                    assert_eq!(receipt.deletes, 1);
                    assert_eq!(receipt.commit_attempts, 1);
                    assert_eq!(receipt.commits, 0);
                    assert!(receipt.receipt_reads >= 1);
                    let retained = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                    assert_eq!(retained.record.value["message_id"], original.record.value["message_id"]);
                    if let Some(expected) = &uncertain_record {
                        assert_eq!(&retained, expected, "unknown observer loss cannot rewrite the real retained record");
                    }
                    if matches!(self, Self::UnknownReceiptReadFailure) {
                        assert_eq!(receipt.receipt_read_failures, 1);
                    }
                }
                _ => {
                    if self.idle() && !self.terminal() {
                        let error = result.expect_err("quiescent lost native work must preserve repaired InvalidState");
                        assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                        assert_eq!(error.context().operation, Operation::Abort);
                        if matches!(self, Self::DroppedCommitObserver) {
                            assert_eq!(error.context().phase, ErrorPhase::Cancellation);
                        }
                    } else {
                        result.expect("confirmed cleanup must preserve its original completion");
                    }
                    if self.idle() {
                        assert_eq!(receipt.commits, 1);
                        assert_eq!(receipt.deletes, if matches!(self, Self::DeleteConflict | Self::CommitConflict) { 2 } else { 1 });
                        assert!(case.faults.prompt_snapshot(&case.conversation.id()).await.is_none());
                    }
                    if matches!(self, Self::LostReceipt | Self::LostReceiptReadFailure) {
                        assert!(receipt.absent_receipts >= 1);
                    }
                    if matches!(self, Self::LostReceiptReadFailure) {
                        assert_eq!(receipt.receipt_read_failures, 1);
                    }
                    if matches!(self, Self::DroppedCommitObserver) {
                        case.conversation.abort().await.expect("a genuinely later Abort must be idle after repaired outcome publication");
                    }
                }
            }
            assert_eq!(stats.aborts.load(Ordering::SeqCst), usize::from(matches!(self, Self::ReadConflict | Self::BeginConflict)));
        }).await;
        case.faults.disarm_prepared_cancel();
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.faults.state.waiter_fault.store(0, Ordering::SeqCst);
        case.faults
            .state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
        case.faults.release();
        case.faults.release_reconciliation_observer();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        prompt_callers.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = prompt_callers.join_next().await {
                    match result {
                        Ok(result) => {
                            let _ = result?;
                        }
                        Err(error) => {
                            assert!(error.is_cancelled(), "prompt observer panicked: {error}")
                        }
                    }
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
                            assert!(error.is_cancelled(), "Abort observer panicked: {error}")
                        }
                    }
                }
                Ok::<(), KitError>(())
            })
            .await;
        cleanup
            .settle(async {
                case.revoke().await;
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), KitError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn recovered_reconciliation_read_conflict_retries_after_rollback() {
    AbortReconciliationCase::ReadConflict.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_read_retains_failed_rollback() {
    AbortReconciliationCase::ReadRollbackFailure.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_conflicts_preserve_original_deadline() {
    AbortReconciliationCase::PersistentReadConflict
        .verify()
        .await;
}
#[tokio::test]
async fn recovered_reconciliation_noop_requires_confirmed_rollback() {
    AbortReconciliationCase::NoopRollbackFailure.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_begin_conflict_retries_without_effects() {
    AbortReconciliationCase::BeginConflict.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_delete_conflict_recomputes_safely() {
    AbortReconciliationCase::DeleteConflict.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_commit_conflict_recomputes_only_uncommitted_work() {
    AbortReconciliationCase::CommitConflict.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_lost_receipt_preserves_repaired_outcome() {
    AbortReconciliationCase::LostReceipt.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_unknown_receipt_never_redeletes() {
    AbortReconciliationCase::UnknownReceipt.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_foreign_receipt_cannot_confirm_delete() {
    AbortReconciliationCase::ForeignReceipt.verify().await;
}
#[tokio::test]
async fn recovered_reconciliation_lost_receipt_read_failure_keeps_exact_outcome() {
    AbortReconciliationCase::LostReceiptReadFailure
        .verify()
        .await;
}
#[tokio::test]
async fn recovered_reconciliation_unknown_receipt_read_failure_keeps_commit_primary() {
    AbortReconciliationCase::UnknownReceiptReadFailure
        .verify()
        .await;
}
#[tokio::test]
async fn recovered_terminal_reconciliation_clears_actual_success() {
    AbortReconciliationCase::TerminalDelete.verify().await;
}
#[tokio::test]
async fn recovered_terminal_reconciliation_unknown_receipt_never_redeletes() {
    AbortReconciliationCase::TerminalUnknownReceipt
        .verify()
        .await;
}

#[tokio::test]
async fn recovered_reconciliation_lost_observer_preserves_overlapping_repaired_outcome() {
    AbortReconciliationCase::DroppedCommitObserver
        .verify()
        .await;
}

#[tokio::test]
async fn recovered_reconciliation_unknown_delete_survives_overlapping_observer_loss() {
    AbortReconciliationCase::DroppedUnknownObserver
        .verify()
        .await;
}
