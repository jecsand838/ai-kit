//! Recovered waiter writes through a caller StateStore. Receipt projections
//! exercise the public backend boundary; they do not alter the backing record.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use agent_sdk::SdkError;
use agent_sdk::conversation::ConversationBackend;
use agent_sdk::error::{ErrorDetail, ErrorKind, ErrorPhase, Operation};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::native_cleanup::NativeCleanup;
use super::{OpenCodeStreamCase, PreparedCancelFault, PromptFaultStore, PromptPersistenceFault};

#[derive(Clone, Copy, Debug)]
enum RecoveredWaiterCase {
    PutConflict,
    PutRollbackFailure,
    CommitConflict,
    LostReceipt,
    LostReceiptReadFailure,
    UnknownReceipt,
    ForeignReceipt,
    MissingReceipt,
    MalformedReceipt,
    UnknownReceiptReadFailure,
}

impl RecoveredWaiterCase {
    fn fault(self) -> PreparedCancelFault {
        match self {
            Self::PutConflict | Self::PutRollbackFailure => PreparedCancelFault::PutConflictOnce,
            Self::CommitConflict => PreparedCancelFault::CommitConflictOnce,
            Self::LostReceipt => PreparedCancelFault::CommitReceiptLost,
            Self::LostReceiptReadFailure => {
                PreparedCancelFault::CommitReceiptLostReadUnavailableOnce
            }
            Self::UnknownReceipt => PreparedCancelFault::CommitUnavailableOnce,
            Self::ForeignReceipt => PreparedCancelFault::CommitUnavailableForeignReceipt,
            Self::MissingReceipt => PreparedCancelFault::CommitUnavailableMissingReceipt,
            Self::MalformedReceipt => PreparedCancelFault::CommitUnavailableMalformedReceipt,
            Self::UnknownReceiptReadFailure => {
                PreparedCancelFault::CommitUnavailableReadUnavailableOnce
            }
        }
    }

    fn uncertain(self) -> bool {
        matches!(
            self,
            Self::UnknownReceipt
                | Self::ForeignReceipt
                | Self::MissingReceipt
                | Self::MalformedReceipt
                | Self::UnknownReceiptReadFailure
        )
    }

    async fn verify(self) {
        let case = OpenCodeStreamCase::start().await;
        let stats = Arc::clone(&case.probe.stats);
        let mut aborts = JoinSet::new();
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(25), async {
            let runtime = tokio::runtime::Handle::current();
            let baseline = runtime.metrics().num_alive_tasks()
                - stats.connections.load(Ordering::SeqCst);
            case.probe.stats.malformed_stream_response.store(true, Ordering::SeqCst);
            case.faults.set_persistence_fault(PromptPersistenceFault::ResponseLoss);
            let mut stream = case.stream().await;
            case.begin_text();
            case.delta(
                "evt_recovered_waiter_admitted",
                OpenCodeStreamCase::ASSISTANT,
                "prt_stream_text",
                "text",
                "accepted",
            );
            OpenCodeStreamCase::next(&mut stream).await
                .expect("a real admitted delta must be observed");
            case.wait_for_admission().await;
            case.release();
            tokio::time::timeout(Duration::from_secs(3), async {
                while case.faults.state.persistence_attempts.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            }).await.expect("the malformed response must reach its actual persistence failure");
            let first_failure = Instant::now();
            let error = tokio::time::timeout(Duration::from_secs(7), stream.recv()).await
                .expect("the original observation must keep its lifecycle bound")
                .expect("the original prompt must publish a terminal result")
                .expect_err("the native final response is deliberately malformed");
            assert_eq!(error.kind(), ErrorKind::Protocol, "{error:?}");
            assert!(stream.recv().await.is_none(), "the original observation must be fused");
            case.assert_persistence_retries_stopped(first_failure).await;
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks()
                    > baseline + stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
            }).await.expect("the original foreground must release ownership before recovery");

            let original = case.faults.prompt_snapshot(&case.conversation.id()).await
                .expect("the actual admitted prompt must remain");
            assert_eq!(original.record.value["phase"]["phase"], "admitted");
            assert_eq!(original.record.value["cancellation"]["state"], "not_requested");
            assert_eq!(original.record.value["waiter"], "waiting");
            let generation = original.record.value["message_id"].as_str()
                .filter(|id| !id.is_empty()).expect("the original generation must be explicit");
            let request_part = original.record.value["request_part_id"].as_str()
                .filter(|id| !id.is_empty()).expect("the original request part must be explicit");
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            case.faults.set_persistence_fault(PromptPersistenceFault::None);
            case.probe.mark_native_idle();
            case.faults.state.persistence_rollback_failure.store(
                matches!(self, Self::PutRollbackFailure), Ordering::SeqCst);

            let (start, admitted) = oneshot::channel();
            let conversation = Arc::clone(&case.conversation);
            let observer = aborts.spawn(async move {
                admitted.await.expect("the selected Abort must be admitted");
                conversation.abort().await
            });
            case.faults.arm_recovered_waiter(
                self.fault(), observer.id(), PromptFaultStore::prompt_key(&case.conversation.id()));
            start.send(()).unwrap();
            let result = tokio::time::timeout(Duration::from_secs(7), aborts.join_next()).await
                .expect("recovered Abort must retain its original lifecycle deadline")
                .expect("the selected public observer must remain owned")
                .expect("the selected public observer must join normally");
            let receipt = case.faults.prepared_cancel_receipt();
            eprintln!(
                "recovered waiter {self:?}: result={result:?}; activation_reads={} activation_rollbacks={} reads={} puts={} deletes={} commit_attempts={} commits={} rollbacks={} rollback_failures={} faults={} receipt_reads={} exact_receipts={} receipt_read_failures={}",
                receipt.activation_reads, receipt.activation_rollbacks, receipt.reads,
                receipt.puts, receipt.deletes, receipt.commit_attempts, receipt.commits,
                receipt.rollbacks, receipt.rollback_failures, receipt.faults,
                receipt.receipt_reads, receipt.exact_receipts, receipt.receipt_read_failures,
            );
            let intent = receipt.intent_record.as_ref()
                .expect("selection requires this public Abort's actual Requested commit");
            assert_eq!(intent.key, original.record.key);
            assert_eq!(intent.value["message_id"].as_str(), Some(generation));
            assert_eq!(intent.value["request_part_id"].as_str(), Some(request_part));
            assert_eq!(intent.value["cancellation"]["state"], "requested");
            assert!(receipt.activation_reads >= 1);
            assert!(receipt.activation_rollbacks >= 1);
            let before = receipt.before.as_ref()
                .expect("the selected recovery must read the actual original prompt");
            assert_eq!(before.record, *intent);
            assert_eq!(before.record.value["waiter"], "waiting");
            let attempted = receipt.attempted_record.as_ref()
                .expect("the selected owner must attempt its actual waiter update");
            let mut expected = (*before.record.value).clone();
            expected["waiter"] = serde_json::json!("lost");
            assert_eq!(attempted.key, before.record.key);
            assert_eq!(*attempted.value, expected,
                "the exact intended record changes only the ended waiter");
            assert_eq!(receipt.faults, 1);
            assert_eq!(receipt.accesses_before_rollback, 0);
            assert_eq!(receipt.writes_after_commit, 0);
            assert_eq!(receipt.deletes_after_commit, 0);
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);

            if matches!(self, Self::PutRollbackFailure) {
                let error = result.expect_err("failed rollback cannot authorize recomputation");
                assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
                assert_eq!(error.context().operation, Operation::Abort);
                assert_eq!(error.context().phase, ErrorPhase::StateWrite);
                assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                let [cleanup] = error.supplementary() else {
                    panic!("the rollback failure must remain attached: {error:?}")
                };
                assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                assert_eq!(cleanup.detail(), Some(&ErrorDetail::ServiceUnavailable));
                assert_eq!(receipt.rollback_failures, 1);
                assert_eq!(receipt.puts, 1);
                assert_eq!(receipt.commit_attempts, 0);
                assert_eq!(receipt.commits, 0);
                assert_eq!(receipt.deletes, 0);
                let retained = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                assert_eq!(retained, *before);
            } else if self.uncertain() {
                let error = result.expect_err("unconfirmed waiter persistence cannot be replayed");
                assert_eq!(error.kind(), ErrorKind::Integration, "{error:?}");
                assert_eq!(error.context().operation, Operation::Abort);
                assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                assert_eq!(receipt.puts, 1);
                assert_eq!(receipt.commit_attempts, 1);
                assert_eq!(receipt.commits, 0);
                assert_eq!(receipt.deletes, 0);
                assert_eq!(receipt.exact_receipts, 0);
                assert!(receipt.receipt_reads >= 1);
                assert_eq!(receipt.receipt_read_failures,
                    usize::from(matches!(self, Self::UnknownReceiptReadFailure)));
                // This bypasses projections and cannot satisfy the SDK's receipt.
                let retained = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                assert_eq!(retained, *before,
                    "the actual stored record must remain Waiting after an unapplied commit");
                tokio::time::sleep(Duration::from_millis(150)).await;
                let after = case.faults.prepared_cancel_receipt();
                assert_eq!(after.puts, receipt.puts);
                assert_eq!(after.commit_attempts, receipt.commit_attempts);
                assert_eq!(after.deletes, receipt.deletes);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            } else {
                let error = result.expect_err("the real idle repair must preserve InvalidState");
                assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                assert_eq!(error.context().operation, Operation::Abort);
                assert_eq!(error.context().phase, ErrorPhase::Cancellation);
                assert_eq!(receipt.puts,
                    if matches!(self, Self::PutConflict | Self::CommitConflict) { 2 } else { 1 });
                assert_eq!(receipt.commit_attempts,
                    if matches!(self, Self::CommitConflict) { 2 } else { 1 });
                assert_eq!(receipt.commits, 1);
                assert_eq!(receipt.rollback_failures, 0);
                if matches!(self, Self::PutConflict | Self::CommitConflict) {
                    assert!(receipt.rollbacks >= 1);
                }
                if matches!(self, Self::LostReceipt | Self::LostReceiptReadFailure) {
                    assert!(receipt.receipt_reads >= 1);
                    assert!(receipt.exact_receipts >= 1,
                        "a matching full updated record must confirm the lost commit receipt");
                    assert_eq!(receipt.receipt_read_failures,
                        usize::from(matches!(self, Self::LostReceiptReadFailure)));
                }
                assert_eq!(receipt.deletes, 1,
                    "only the later lawful idle reconciliation may delete the prompt");
                assert!(case.faults.prompt_snapshot(&case.conversation.id()).await.is_none());
                case.conversation.abort().await.expect("a genuinely later Abort must be idle");
            }
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
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                case.revoke().await;
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn recovered_waiter_put_conflict_retries_after_rollback() {
    RecoveredWaiterCase::PutConflict.verify().await;
}

#[tokio::test]
async fn recovered_waiter_put_conflict_retains_failed_rollback() {
    RecoveredWaiterCase::PutRollbackFailure.verify().await;
}

#[tokio::test]
async fn recovered_waiter_commit_conflict_recomputes_uncommitted_update() {
    RecoveredWaiterCase::CommitConflict.verify().await;
}

#[tokio::test]
async fn recovered_waiter_lost_receipt_requires_exact_updated_record() {
    RecoveredWaiterCase::LostReceipt.verify().await;
}

#[tokio::test]
async fn recovered_waiter_lost_receipt_read_failure_retries_only_observation() {
    RecoveredWaiterCase::LostReceiptReadFailure.verify().await;
}

#[tokio::test]
async fn recovered_waiter_unknown_receipt_never_repeats_update() {
    RecoveredWaiterCase::UnknownReceipt.verify().await;
}

#[tokio::test]
async fn recovered_waiter_foreign_receipt_cannot_confirm_update() {
    RecoveredWaiterCase::ForeignReceipt.verify().await;
}

#[tokio::test]
async fn recovered_waiter_missing_receipt_cannot_confirm_update() {
    RecoveredWaiterCase::MissingReceipt.verify().await;
}

#[tokio::test]
async fn recovered_waiter_malformed_receipt_preserves_commit_primary() {
    RecoveredWaiterCase::MalformedReceipt.verify().await;
}

#[tokio::test]
async fn recovered_waiter_unknown_receipt_read_failure_preserves_commit_primary() {
    RecoveredWaiterCase::UnknownReceiptReadFailure
        .verify()
        .await;
}
