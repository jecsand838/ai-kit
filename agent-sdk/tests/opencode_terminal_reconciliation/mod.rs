//! Postcommit terminal-prompt cleanup through a real answered-question event.
//! The event, continuation and successful prompt are real fixture operations;
//! only the caller StateStore injects faults. This is not strict-live evidence.
//! It reaches the same terminal cleanup owner as pending-inventory recovery,
//! through its postcommit event caller rather than claiming identical routing.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use agent_sdk::AgentBackend;
use agent_sdk::SdkError;
use agent_sdk::conversation::ConversationBackend;
use agent_sdk::error::{ErrorDetail, ErrorKind, ErrorPhase};
use agent_sdk::notification::ConversationNotificationKind;
use agent_sdk::prompt::PromptRequest;
use storage_sdk::StateStoreBackend;
use storage_sdk::state::record::StateListRequest;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::native_cleanup::NativeCleanup;
use super::{
    OPENCODE_CONTINUATION_NAMESPACE, OpenCodeEventProbe, OpenCodeStreamCase, PreparedCancelFault,
    PromptFaultStore, RetainedIdentityChange,
};

#[derive(Clone, Copy, Debug)]
enum TerminalReconciliationCase {
    Complete,
    ReadConflict,
    ReadRollbackFailure,
    ReadDeadline,
    DeleteConflict,
    CommitConflict,
    LostReceipt,
    UnknownReceipt,
    ForeignReceipt,
    UnknownReceiptReadFailure,
    UnknownReceiptReadHeld,
}

impl TerminalReconciliationCase {
    fn fault(self) -> PreparedCancelFault {
        match self {
            Self::Complete => PreparedCancelFault::None,
            Self::ReadConflict | Self::ReadRollbackFailure => PreparedCancelFault::LoadConflictOnce,
            Self::ReadDeadline => PreparedCancelFault::LoadConflict,
            Self::DeleteConflict => PreparedCancelFault::DeleteConflictOnce,
            Self::CommitConflict => PreparedCancelFault::CommitConflictOnce,
            Self::LostReceipt => PreparedCancelFault::CommitReceiptLost,
            Self::UnknownReceipt => PreparedCancelFault::CommitUnavailableOnce,
            Self::ForeignReceipt => PreparedCancelFault::CommitUnavailableForeignReceipt,
            Self::UnknownReceiptReadFailure => {
                PreparedCancelFault::CommitUnavailableReadUnavailableOnce
            }
            Self::UnknownReceiptReadHeld => PreparedCancelFault::CommitUnavailableReadHeld,
        }
    }

    fn succeeds(self) -> bool {
        matches!(
            self,
            Self::Complete
                | Self::ReadConflict
                | Self::DeleteConflict
                | Self::CommitConflict
                | Self::LostReceipt
        )
    }

    fn assert_shutdown_failure(self, error: &SdkError) {
        let mut pending = vec![error];
        let mut retained = false;
        let mut rollback_retained = false;
        let mut receipt_deadline_retained = false;
        while let Some(error) = pending.pop() {
            assert!(!error.diagnostics_truncated(), "{error:?}");
            let original = match self {
                Self::ReadRollbackFailure | Self::ReadDeadline => {
                    error.kind() == ErrorKind::Conflict
                        && error.context().phase == ErrorPhase::StateRead
                        && error.detail() == Some(&ErrorDetail::TransactionConflict)
                }
                Self::UnknownReceipt
                | Self::ForeignReceipt
                | Self::UnknownReceiptReadFailure
                | Self::UnknownReceiptReadHeld => {
                    error.kind() == ErrorKind::Integration
                        && error.context().phase == ErrorPhase::StateCommit
                        && error.detail() == Some(&ErrorDetail::ServiceUnavailable)
                }
                _ => panic!("successful case cannot classify shutdown failure"),
            };
            retained |= original;
            let deadline = error.kind() == ErrorKind::Timeout
                && matches!(error.detail(), Some(ErrorDetail::DeadlineElapsed { .. }));
            receipt_deadline_retained |=
                deadline && error.context().phase == ErrorPhase::EventSynchronization;
            let rollback = matches!(self, Self::ReadRollbackFailure)
                && error.kind() == ErrorKind::Integration
                && error.context().phase == ErrorPhase::Cleanup
                && error.detail() == Some(&ErrorDetail::ServiceUnavailable);
            let receipt_read = matches!(self, Self::UnknownReceiptReadFailure)
                && error.kind() == ErrorKind::Integration
                && error.context().phase == ErrorPhase::StateRead
                && error.detail() == Some(&ErrorDetail::ServiceUnavailable);
            let unconfirmed_receipt = matches!(
                self,
                Self::UnknownReceipt | Self::ForeignReceipt | Self::UnknownReceiptReadFailure
            ) && matches!(
                error.kind(),
                ErrorKind::Integration | ErrorKind::InvalidState
            ) && error.context().phase == ErrorPhase::Reconciliation
                && error.detail().is_none();
            rollback_retained |= rollback;
            assert!(
                original || deadline || rollback || receipt_read || unconfirmed_receipt,
                "unrelated error cannot stand in for the selected cleanup failure: {error:?}"
            );
            pending.extend(error.supplementary());
        }
        assert!(
            retained,
            "the exact original transaction failure must survive: {error:?}"
        );
        if matches!(self, Self::ReadRollbackFailure) {
            assert!(
                rollback_retained,
                "failed rollback must remain supplementary: {error:?}"
            );
        }
        if matches!(self, Self::ReadDeadline) {
            assert_eq!(error.kind(), ErrorKind::Timeout, "{error:?}");
        }
        if matches!(self, Self::UnknownReceiptReadHeld) {
            assert!(
                receipt_deadline_retained,
                "the original receipt deadline must survive: {error:?}"
            );
        }
    }

    async fn verify(self) {
        let case = OpenCodeStreamCase::start().await;
        case.probe
            .stats
            .retained_question_history
            .store(true, Ordering::SeqCst);
        case.probe
            .stats
            .retain_question_after_abort
            .store(true, Ordering::SeqCst);
        *case.probe.stats.retained_identity_change.lock().unwrap() =
            Some(RetainedIdentityChange::OrdinaryCompletion);
        let OpenCodeStreamCase {
            probe,
            agent,
            workspace,
            conversation,
            faults,
            runtime_baseline,
        } = case;
        let stats = Arc::clone(&probe.stats);
        let mut agent = Some(agent);
        let mut shutdowns = JoinSet::new();
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(30), async {
            let runtime = tokio::runtime::Handle::current();
            let foreground_baseline = runtime.metrics().num_alive_tasks()
                - stats.connections.load(Ordering::SeqCst);
            let mut notifications = conversation.notification_stream().unwrap();
            let mut output = conversation.prompt_stream(PromptRequest::new("ask one question").unwrap())
                .await.unwrap();
            let interaction = loop {
                tokio::select! {
                    item = notifications.recv() => {
                        let item = item.unwrap().unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break item; }
                    }
                    item = output.recv() => panic!("question admission must precede terminal output: {item:?}"),
                }
            };
            let continuations = faults.inner.list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap())
                .await.unwrap().records;
            let [pending] = continuations.as_slice() else { panic!("one exact owned question is required") };
            assert_eq!(pending.record.value["state"], "pending");
            assert_eq!(pending.record.value["notification_id"], interaction.id().as_str());
            assert!(stats.pending_question.load(Ordering::SeqCst));
            stats.native_active.store(false, Ordering::SeqCst);
            OpenCodeEventProbe::retained_question_abort(&stats);
            stats.prompt_release.add_permits(1);
            // Native Success is durable, but its unresolved interaction keeps
            // prompt-slot cleanup Busy. This existing setup is not relabeled as
            // successful public prompt delivery.
            let terminal = tokio::time::timeout(Duration::from_secs(7), output.recv()).await
                .expect("ordinary retained question must finish its original observation")
                .expect("the original stream must publish one terminal")
                .expect_err("a still-Pending question prevents prompt-slot cleanup");
            assert_eq!(terminal.kind(), ErrorKind::Busy, "{terminal:?}");
            assert!(output.recv().await.is_none());
            drop(output);
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks()
                    > foreground_baseline + stats.connections.load(Ordering::SeqCst)
                { tokio::task::yield_now().await; }
            }).await.expect("the original foreground must end before event cleanup faults");
            let original = faults.prompt_snapshot(&conversation.id()).await.unwrap();
            assert_eq!(original.record.value["phase"]["phase"], "terminal");
            assert_eq!(original.record.value["phase"]["outcome"]["outcome"], "success");
            assert_eq!(original.record.value["cancellation"]["state"], "unnecessary");
            assert!(matches!(original.record.value["waiter"].as_str(), Some("delivered" | "lost" | "timed_out")));
            faults.arm_terminal_event_reconciliation(self.fault(),
                PromptFaultStore::prompt_key(&conversation.id()), pending.record.key.clone());
            faults.state.persistence_rollback_failure.store(
                matches!(self, Self::ReadRollbackFailure), Ordering::SeqCst);
            // A real native answer independently settles only this exact ask.
            stats.pending_question.store(false, Ordering::SeqCst);
            let event_sent = Instant::now();
            stats.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {"id": "evt_terminal_cleanup_answer", "type": "question.replied", "properties": {
                    "sessionID": "ses_probe", "requestID": "que_probe_pending", "answers": [["Yes"]]
                }}
            }).to_string()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let receipt = faults.prepared_cancel_receipt();
                    if receipt.event_commits == 1 && (receipt.faults != 0 || receipt.commits != 0) { break; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("the exact event must commit before selected prompt cleanup is observed");
            let first_attempt = Instant::now();
            if matches!(self, Self::UnknownReceiptReadHeld) {
                faults.wait_until_blocked().await;
            }
            if self.succeeds() {
                // A committed deletion can become readable before the caller's
                // commit future resumes. Observe the next ordered event through
                // the public receiver before inspecting commit-return receipts.
                stats.events.send(serde_json::json!({
                    "directory": env!("CARGO_MANIFEST_DIR"),
                    "payload": {"id": "evt_terminal_cleanup_following_file", "type": "session.diff", "properties": {
                        "sessionID": "ses_probe", "diff": [{
                            "file": "terminal-cleanup-observed.txt", "additions": 1, "deletions": 0
                        }]
                    }}
                }).to_string()).unwrap();
                tokio::time::timeout(Duration::from_secs(7), async {
                    let item = notifications.recv().await
                        .expect("the ordered event owner must remain open after successful cleanup")
                        .expect("the following event must publish successfully");
                    match item.kind() {
                        ConversationNotificationKind::FileChanged { path, summary } => {
                            assert_eq!(path, std::path::Path::new("terminal-cleanup-observed.txt"));
                            assert!(summary.is_none());
                        }
                        other => panic!("expected the exact following file event, got {other:?}"),
                    }
                }).await.expect("finite local cleanup failures must not strand the accepted event");
                assert!(faults.prompt_snapshot(&conversation.id()).await.is_none(),
                    "the successful event must have removed the exact terminal prompt");
            } else {
                // Observe public owner failure if it arrives; Closed/EOF alone
                // is never the error-authority oracle below. A parked owner may
                // remain live, so the original episode plus one second is also
                // bounded observation, with the fault still enabled.
                let until = first_attempt + Duration::from_secs(6);
                let mut owner_terminated = false;
                loop {
                    tokio::select! {
                        item = notifications.recv() => {
                            match item {
                                Some(Ok(item)) => assert!(!matches!(item.kind(), ConversationNotificationKind::Interaction { .. }),
                                    "the accepted question cannot be replayed"),
                                terminal => {
                                    eprintln!("event terminal {self:?}: public receiver={terminal:?}");
                                    owner_terminated = true;
                                    break;
                                }
                            }
                        }
                        _ = tokio::time::sleep_until(until) => break,
                    }
                }
                if matches!(self, Self::UnknownReceiptReadHeld) {
                    assert!(owner_terminated,
                        "the event owner must end its receipt observation before shutdown or gate release");
                    assert!(event_sent.elapsed() >= Duration::from_secs(5),
                        "receipt observation must retain the original application deadline");
                }
            }
            let receipt = faults.prepared_cancel_receipt();
            eprintln!("event terminal {self:?}: reads={} deletes={} commits={} rollbacks={} rollback_failures={} faults={} receipt_reads={} read_failures={}",
                receipt.reads, receipt.deletes, receipt.commits, receipt.rollbacks,
                receipt.rollback_failures, receipt.faults, receipt.receipt_reads, receipt.receipt_read_failures);
            assert_eq!(receipt.event_commits, 1);
            assert_eq!(receipt.event_before.as_ref().unwrap(), pending);
            let accepted = faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap();
            assert_eq!(accepted.record, *receipt.event_record.as_ref().unwrap());
            assert_eq!(accepted.record.value["state"], "accepted");
            assert_eq!(accepted.revision.get(), pending.revision.get() + 1,
                "cleanup cannot reapply the already committed answer");
            assert_eq!(receipt.before.as_ref().unwrap().record, original.record);
            assert_eq!(receipt.accesses_before_rollback, 0);
            assert_eq!(receipt.deletes_after_commit, 0);
            assert_eq!(receipt.writes_after_commit, 0);
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
            match self {
                Self::ReadRollbackFailure => {
                    assert_eq!(receipt.faults, 1);
                    assert_eq!(receipt.rollbacks, 1);
                    assert_eq!(receipt.rollback_failures, 1);
                    assert_eq!(receipt.deletes, 0);
                    assert_eq!(faults.prompt_snapshot(&conversation.id()).await.unwrap().record, original.record);
                }
                Self::ReadDeadline => {
                    assert!(event_sent.elapsed() >= Duration::from_secs(5),
                        "the application episode cannot invent an earlier deadline");
                    assert!(receipt.faults > 1);
                    assert_eq!(receipt.rollbacks, receipt.faults);
                    assert_eq!(receipt.deletes, 0);
                    assert_eq!(faults.prompt_snapshot(&conversation.id()).await.unwrap().record, original.record);
                    let settled = (receipt.reads, receipt.begins, receipt.deletes, receipt.rollbacks);
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    let after = faults.prepared_cancel_receipt();
                    // This exact postcommit apply_event episode retains the
                    // advance_live deadline; it is not transport reconnect's
                    // separate episode authority.
                    assert_eq!((after.reads, after.begins, after.deletes, after.rollbacks), settled,
                        "shutdown must not supply the missing bound for a still-retrying event episode");
                }
                Self::UnknownReceipt | Self::ForeignReceipt | Self::UnknownReceiptReadFailure
                | Self::UnknownReceiptReadHeld => {
                    assert_eq!(receipt.deletes, 1);
                    assert_eq!(receipt.commit_attempts, 1);
                    assert_eq!(receipt.commits, 0);
                    assert!(receipt.receipt_reads >= 1);
                    assert_eq!(faults.prompt_snapshot(&conversation.id()).await.unwrap().record, original.record);
                    if matches!(self, Self::UnknownReceiptReadFailure) { assert_eq!(receipt.receipt_read_failures, 1); }
                    if matches!(self, Self::UnknownReceiptReadHeld) {
                        assert_eq!(receipt.receipt_reads, 1);
                        assert_eq!(receipt.receipt_read_failures, 0);
                    }
                }
                _ => {
                    assert_eq!(receipt.commits, 1);
                    assert_eq!(receipt.deletes, if matches!(self, Self::DeleteConflict | Self::CommitConflict) { 2 } else { 1 });
                    if matches!(self, Self::ReadConflict | Self::DeleteConflict | Self::CommitConflict) {
                        assert_eq!(receipt.faults, 1);
                        assert!(receipt.rollbacks >= 1);
                    }
                    if matches!(self, Self::LostReceipt) { assert!(receipt.absent_receipts >= 1); }
                    conversation.info().await.expect("finite cleanup conflict cannot close the hierarchy");
                    let reused = conversation.prompt(PromptRequest::new("return the reuse marker").unwrap())
                        .await.expect("confirmed event cleanup must release prompt admission");
                    assert_eq!(reused.output(), "RETAINED_HISTORY_REUSE");
                    assert_eq!(stats.messages.load(Ordering::SeqCst), 2);
                }
            }
            drop(notifications);
            shutdowns.spawn(agent.take().unwrap().shutdown());
            let shutdown = tokio::time::timeout(Duration::from_secs(7), shutdowns.join_next()).await
                .expect("public shutdown must settle the event owner")
                .unwrap().expect("shutdown observer must join");
            if self.succeeds() {
                shutdown.expect("confirmed cleanup must allow successful shutdown");
            } else {
                self.assert_shutdown_failure(&shutdown.expect_err("unsettled cleanup must retain its exact failure"));
            }
        }).await;

        // This existing disarm also clears persistence_rollback_failure before
        // any independent consuming shutdown or process/connection cleanup.
        faults.disarm_prepared_cancel();
        faults.release();
        probe.release_history();
        stats.prompt_release.add_permits(2);
        stats.rejection_release.add_permits(2);
        drop(conversation);
        drop(workspace);
        if let Some(agent) = agent.take() {
            shutdowns.spawn(agent.shutdown());
        }
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = shutdowns.join_next().await {
                    result.expect("independent shutdown observer must join")?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        shutdowns.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = shutdowns.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "shutdown observer panicked: {error}")
                        }
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                let runtime = tokio::runtime::Handle::current();
                while runtime.metrics().num_alive_tasks()
                    > runtime_baseline + stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        drop(faults);
        cleanup
            .settle(async {
                probe.shutdown().await;
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn answered_event_clears_releasable_success_once() {
    TerminalReconciliationCase::Complete.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_retries_confirmed_load_conflict() {
    TerminalReconciliationCase::ReadConflict.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_retains_failed_read_rollback() {
    TerminalReconciliationCase::ReadRollbackFailure
        .verify()
        .await;
}
#[tokio::test]
async fn answered_event_cleanup_keeps_original_conflict_deadline() {
    TerminalReconciliationCase::ReadDeadline.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_retries_definite_delete_conflict() {
    TerminalReconciliationCase::DeleteConflict.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_retries_only_uncommitted_delete() {
    TerminalReconciliationCase::CommitConflict.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_confirms_applied_delete_receipt() {
    TerminalReconciliationCase::LostReceipt.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_never_replays_unknown_delete() {
    TerminalReconciliationCase::UnknownReceipt.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_rejects_foreign_delete_receipt() {
    TerminalReconciliationCase::ForeignReceipt.verify().await;
}
#[tokio::test]
async fn answered_event_cleanup_preserves_commit_error_after_failed_read() {
    TerminalReconciliationCase::UnknownReceiptReadFailure
        .verify()
        .await;
}

#[tokio::test]
async fn answered_event_cleanup_bounds_unknown_receipt_observation() {
    TerminalReconciliationCase::UnknownReceiptReadHeld
        .verify()
        .await;
}
