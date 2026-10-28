//! Caller-StateStore coverage of recovered prepared cancellation. The read
//! projections are explicit backend ownership guards, not native engine proof.

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
enum PreparedCancellationCase {
    RecoveredReadConflict,
    RecoveredReadRollbackFailure,
    RecoveredReadDeadline,
    RecoveredNoopRollbackFailure,
    Prepared,
    BeginConflict,
    DeleteConflict,
    CommitConflict,
    LostReceipt,
    UnknownReceipt,
    ForeignReceipt,
    MissingInitialRead,
    ForeignInitialRead,
    DroppedCommitObserver,
}

impl PreparedCancellationCase {
    fn prepared(self) -> bool {
        !matches!(
            self,
            Self::RecoveredReadConflict
                | Self::RecoveredReadRollbackFailure
                | Self::RecoveredReadDeadline
                | Self::RecoveredNoopRollbackFailure
        )
    }

    fn fault(self) -> PreparedCancelFault {
        match self {
            Self::RecoveredReadConflict | Self::RecoveredReadRollbackFailure => {
                PreparedCancelFault::LoadConflictOnce
            }
            Self::RecoveredReadDeadline => PreparedCancelFault::LoadConflict,
            Self::RecoveredNoopRollbackFailure | Self::Prepared => PreparedCancelFault::None,
            Self::BeginConflict => PreparedCancelFault::BeginConflictOnce,
            Self::DeleteConflict => PreparedCancelFault::DeleteConflictOnce,
            Self::CommitConflict => PreparedCancelFault::CommitConflictOnce,
            Self::LostReceipt => PreparedCancelFault::CommitReceiptLost,
            Self::UnknownReceipt => PreparedCancelFault::CommitUnavailableOnce,
            Self::ForeignReceipt => PreparedCancelFault::CommitUnavailableForeignReceipt,
            Self::MissingInitialRead => PreparedCancelFault::InitialMissing,
            Self::ForeignInitialRead => PreparedCancelFault::InitialForeign,
            Self::DroppedCommitObserver => PreparedCancelFault::CommitReceiptHeld,
        }
    }

    async fn verify(self) {
        let case = OpenCodeStreamCase::start().await;
        let stats = Arc::clone(&case.probe.stats);
        let mut aborts = JoinSet::new();
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(25), async {
            let runtime = tokio::runtime::Handle::current();
            let baseline = runtime.metrics().num_alive_tasks() - stats.connections.load(Ordering::SeqCst);
            let mut stream = if self.prepared() {
                // Real native Busy prevents dispatch; failed Kit cleanup keeps
                // the original committed Prepared record without rewinding it.
                case.probe.mark_native_active();
                case.faults.set_persistence_fault(PromptPersistenceFault::PreDispatchCleanup);
                case.conversation.prompt_stream(OpenCodeStreamCase::request()).await.unwrap()
            } else {
                // The real foreground ends after its original admission budget;
                // its accepted native work and exact durable generation remain.
                case.stream_with_admission_fault(PromptPersistenceFault::Admission).await
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                while case.faults.state.persistence_attempts.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            }).await.expect("setup must reach its actual persistence failure");
            let first_failure = Instant::now();
            let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv()).await
                .expect("the original foreground must end observation")
                .expect("one foreground terminal is required").expect_err("setup deliberately fails persistence");
            assert_eq!(terminal.kind(), if self.prepared() { ErrorKind::Busy } else { ErrorKind::Timeout });
            assert!(stream.recv().await.is_none());
            case.assert_persistence_retries_stopped(first_failure).await;
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks() > baseline + stats.connections.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            }).await.expect("original foreground tasks must end before public recovery");
            let original = case.faults.prompt_snapshot(&case.conversation.id()).await
                .expect("the real original generation must remain recoverable");
            assert_eq!(original.record.value["phase"]["phase"] == "prepared", self.prepared());
            assert_eq!(original.record.value["cancellation"]["state"], "not_requested");
            assert_eq!(stats.messages.load(Ordering::SeqCst), usize::from(!self.prepared()));
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            case.faults.set_persistence_fault(PromptPersistenceFault::None);
            if self.prepared() { case.probe.mark_native_idle(); }
            case.faults.state.persistence_rollback_failure.store(
                matches!(self, Self::RecoveredReadRollbackFailure | Self::RecoveredNoopRollbackFailure), Ordering::SeqCst);
            let (start, admitted) = oneshot::channel();
            let conversation = Arc::clone(&case.conversation);
            let observer = aborts.spawn(async move {
                admitted.await.expect("the selected Abort observer must be admitted");
                conversation.abort().await
            });
            case.faults.arm_prepared_cancel(self.fault(), observer.id(), PromptFaultStore::prompt_key(&case.conversation.id()));
            let started = Instant::now();
            start.send(()).unwrap();
            let result = if matches!(self, Self::DroppedCommitObserver) {
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        biased;
                        result = aborts.join_next() => panic!("Abort returned before its committed receipt was held: {result:?}"),
                        permit = case.faults.state.blocked.acquire() => permit.unwrap().forget(),
                    }
                }).await.expect("the actual delete receipt must reach its gate");
                assert!(case.faults.prompt_snapshot(&case.conversation.id()).await.is_none(), "the native delete is committed before the observer is lost");
                observer.abort();
                let joined = aborts.join_next().await.unwrap().expect_err("only the public observer is dropped");
                assert!(joined.is_cancelled());
                case.faults.release();
                tokio::time::timeout(Duration::from_secs(7), case.conversation.abort()).await
                    .expect("fresh recovery must observe the committed deletion")
            } else {
                tokio::time::timeout(Duration::from_secs(7), aborts.join_next()).await
                    .expect("the public Abort retains its original lifecycle bound")
                    .unwrap().expect("the public Abort observer must join")
            };
            let receipt = case.faults.prepared_cancel_receipt();
            eprintln!("prepared cancellation {self:?}: result={result:?}; begins={} reads={} deletes={} commits={} rollbacks={} rollback_failures={} faults={} receipt_reads={} absent_receipts={}",
                receipt.begins, receipt.reads, receipt.deletes, receipt.commits, receipt.rollbacks,
                receipt.rollback_failures, receipt.faults, receipt.receipt_reads, receipt.absent_receipts);
            let intent = receipt.intent_record.as_ref().expect("fault scope requires this caller's real committed cancellation intent");
            assert_eq!(intent.key, original.record.key);
            assert_eq!(intent.value["message_id"], original.record.value["message_id"]);
            assert_eq!(intent.value["request_part_id"], original.record.value["request_part_id"]);
            assert_eq!(intent.value["cancellation"]["state"], "requested");
            assert!(receipt.before.is_some(), "the selected cancellation read must actually occur");
            assert_eq!(receipt.accesses_before_rollback, 0, "no recomputation before acknowledged rollback");
            assert_eq!(receipt.deletes_after_commit, 0, "committed deletion cannot be replayed by any observer");
            assert_eq!(stats.messages.load(Ordering::SeqCst), usize::from(!self.prepared()));
            match self {
                Self::RecoveredReadRollbackFailure => {
                    let error = result.expect_err("failed rollback must preserve the read failure");
                    assert_eq!(error.kind(), ErrorKind::Conflict);
                    assert_eq!(error.context().phase, ErrorPhase::StateRead);
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    let [cleanup] = error.supplementary() else { panic!("rollback error must remain attached: {error:?}") };
                    assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                    assert_eq!(cleanup.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(receipt.faults, 1);
                    assert_eq!(receipt.reads, 1);
                    assert_eq!(receipt.rollback_failures, 1);
                }
                Self::RecoveredNoopRollbackFailure => {
                    let error = result.expect_err("an unchanged read requires confirmed rollback");
                    assert_eq!(error.kind(), ErrorKind::Integration);
                    assert_eq!(error.context().phase, ErrorPhase::Cleanup);
                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(receipt.rollback_failures, 1);
                    assert_eq!(receipt.reads, 1);
                }
                Self::RecoveredReadDeadline => {
                    let error = result.expect_err("persistent conflicts must exhaust the original budget");
                    assert_eq!(error.kind(), ErrorKind::Timeout);
                    assert_eq!(error.context().operation, Operation::Abort);
                    assert_eq!(error.context().phase, ErrorPhase::Reconciliation);
                    assert_eq!(error.detail(), Some(&ErrorDetail::DeadlineElapsed { kind: DeadlineKind::Operation, timeout: None }));
                    let [last] = error.supplementary() else { panic!("last read conflict must survive deadline: {error:?}") };
                    assert_eq!(last.kind(), ErrorKind::Conflict);
                    assert_eq!(last.context().phase, ErrorPhase::StateRead);
                    assert_eq!(last.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert!(receipt.faults > 1);
                    assert_eq!(receipt.rollbacks, receipt.faults);
                    assert!(started.elapsed() >= Duration::from_secs(5));
                    let reads = receipt.reads;
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    assert_eq!(case.faults.prepared_cancel_receipt().reads, reads);
                }
                Self::UnknownReceipt | Self::ForeignReceipt => {
                    let error = result.expect_err("unconfirmed deletion cannot be restaged or called successful");
                    assert_eq!(error.kind(), ErrorKind::Integration);
                    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(receipt.faults, 1);
                    assert_eq!(receipt.deletes, 1);
                    assert_eq!(receipt.commit_attempts, 1);
                    assert_eq!(receipt.commits, 0);
                    assert!(receipt.receipt_reads >= 1);
                    let durable = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                    assert_eq!(&durable.record, intent, "unapplied delete leaves the actual committed intent unchanged");
                }
                _ => {
                    result.expect("confirmed local recovery must preserve native effect authority");
                    let expected_faults = usize::from(!matches!(self, Self::Prepared));
                    assert_eq!(receipt.faults, expected_faults);
                    if matches!(self, Self::RecoveredReadConflict) {
                        assert_eq!(receipt.reads, 2);
                        assert_eq!(receipt.rollbacks, 2);
                        assert_eq!(receipt.deletes, 0);
                    } else if matches!(self, Self::MissingInitialRead | Self::ForeignInitialRead) {
                        assert_eq!(receipt.reads, 1);
                        assert_eq!(receipt.rollbacks, 1);
                        assert_eq!(receipt.deletes, 0);
                        assert_eq!(case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap().record, *intent);
                    } else {
                        assert!(case.faults.prompt_snapshot(&case.conversation.id()).await.is_none());
                        assert_eq!(receipt.commits, 1);
                        assert_eq!(receipt.deletes, if matches!(self, Self::DeleteConflict | Self::CommitConflict) { 2 } else { 1 });
                        if matches!(self, Self::LostReceipt) {
                            assert!(receipt.receipt_reads >= 1);
                            assert!(receipt.absent_receipts >= 1);
                        }
                    }
                }
            }
            assert_eq!(stats.aborts.load(Ordering::SeqCst), usize::from(matches!(self, Self::RecoveredReadConflict)));
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
        }).await;
        case.faults.disarm_prepared_cancel();
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.faults
            .state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
        case.faults.release();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
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
async fn recovered_load_conflict_retries_after_confirmed_rollback() {
    PreparedCancellationCase::RecoveredReadConflict
        .verify()
        .await;
}
#[tokio::test]
async fn recovered_load_failure_preserves_failed_rollback() {
    PreparedCancellationCase::RecoveredReadRollbackFailure
        .verify()
        .await;
}
#[tokio::test]
async fn recovered_load_conflicts_preserve_original_deadline_and_last_error() {
    PreparedCancellationCase::RecoveredReadDeadline
        .verify()
        .await;
}
#[tokio::test]
async fn recovered_nonprepared_noop_requires_successful_rollback() {
    PreparedCancellationCase::RecoveredNoopRollbackFailure
        .verify()
        .await;
}
#[tokio::test]
async fn prepared_delete_releases_without_native_effect() {
    PreparedCancellationCase::Prepared.verify().await;
}
#[tokio::test]
async fn prepared_begin_conflict_retries_before_delete() {
    PreparedCancellationCase::BeginConflict.verify().await;
}
#[tokio::test]
async fn prepared_delete_conflict_rolls_back_before_retry() {
    PreparedCancellationCase::DeleteConflict.verify().await;
}
#[tokio::test]
async fn prepared_commit_conflict_recomputes_only_uncommitted_delete() {
    PreparedCancellationCase::CommitConflict.verify().await;
}
#[tokio::test]
async fn prepared_lost_commit_receipt_observes_absence_without_redelete() {
    PreparedCancellationCase::LostReceipt.verify().await;
}
#[tokio::test]
async fn prepared_unknown_unapplied_commit_does_not_redelete() {
    PreparedCancellationCase::UnknownReceipt.verify().await;
}
#[tokio::test]
async fn prepared_unknown_foreign_receipt_cannot_confirm_delete() {
    PreparedCancellationCase::ForeignReceipt.verify().await;
}
#[tokio::test]
async fn initial_missing_read_is_rollback_confirmed_noop() {
    PreparedCancellationCase::MissingInitialRead.verify().await;
}
#[tokio::test]
async fn initial_foreign_read_is_rollback_confirmed_noop() {
    PreparedCancellationCase::ForeignInitialRead.verify().await;
}
#[tokio::test]
async fn dropped_commit_observer_preserves_committed_delete_authority() {
    PreparedCancellationCase::DroppedCommitObserver
        .verify()
        .await;
}
