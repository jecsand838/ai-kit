//! Workspace deletion retains the caller StateStore's commit evidence while
//! reconciling its actual outcome. These are wire fixtures, not live evidence.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use agents_kit::conversation::NewConversation;
use agents_kit::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase, Operation};
use agents_kit::workspace::{DeleteWorkspaceRequest, WorkspaceDefaults};
use agents_kit::{AgentBackend, AgentBuilder, KitError, WorkspaceBackend};
use tokio::time::Instant;

use super::native_cleanup::NativeCleanup;
use super::{OpenCodeEventProbe, PromptFaultStore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkspaceDeleteCommitFault {
    None,
    TimeoutBeforeCommit,
    TimeoutAfterCommit,
    UnavailableAfterCommit,
}

#[derive(Clone, Copy, Debug)]
enum WorkspaceDeleteCase {
    Unchanged,
    Unobservable,
    ConfirmedAbsent,
}

impl WorkspaceDeleteCase {
    fn assert_commit_error(error: &KitError) {
        assert_eq!(error.kind(), ErrorKind::Timeout, "{error:?}");
        assert_eq!(error.context().operation, Operation::DeleteWorkspace);
        assert_eq!(error.context().phase, ErrorPhase::StateCommit);
        assert_eq!(
            error.detail(),
            Some(&ErrorDetail::DeadlineElapsed {
                kind: DeadlineKind::Operation,
                timeout: Some(Duration::from_millis(77)),
            }),
        );
        // The lower error is flat. Observation/cleanup errors are independent
        // evidence, never a fabricated cause of the earlier commit failure.
        assert!(error.cause().is_none(), "{error:?}");
        assert!(error.http_status().is_none());
        assert!(!error.diagnostics_truncated());
        assert!(!format!("{error:?}").contains("PRIVATE_WORKSPACE_DELETE"));
    }

    async fn verify(self) {
        let mut probe = OpenCodeEventProbe::start(0).await;
        let stats = Arc::clone(&probe.stats);
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let config = probe.config(Duration::from_secs(1), Duration::from_secs(2));
        let mut agent = None;
        let mut workspace = None;
        let mut conversation = None;
        let observation = NativeCleanup::observe(Instant::now() + Duration::from_secs(15), async {
            agent = Some(
                AgentBuilder::create(config, faults.storage())
                    .build()
                    .await
                    .unwrap(),
            );
            let models = agent.as_ref().unwrap().list_models().await.unwrap();
            assert_eq!(models.len(), 1, "fixture advertises one exact model");
            let defaults =
                WorkspaceDefaults::new(Some(models[0].selector().clone()), None, None).unwrap();
            workspace = Some(
                agent
                    .as_ref()
                    .unwrap()
                    .new_workspace(OpenCodeEventProbe::workspace_request_with_defaults(
                        defaults,
                    ))
                    .await
                    .unwrap(),
            );
            let initial = workspace.as_ref().unwrap().info().await.unwrap();
            conversation = Some(
                workspace
                    .as_ref()
                    .unwrap()
                    .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                    .await
                    .unwrap(),
            );

            *faults.state.workspace_delete_commit_fault.lock().unwrap() = match self {
                Self::Unchanged => WorkspaceDeleteCommitFault::TimeoutBeforeCommit,
                Self::Unobservable | Self::ConfirmedAbsent => {
                    WorkspaceDeleteCommitFault::TimeoutAfterCommit
                }
            };
            match self {
                Self::Unchanged => {}
                Self::Unobservable => {
                    faults.fail_workspace_delete_commit_observation_until_released()
                }
                Self::ConfirmedAbsent => faults.fail_next_workspace_delete_commit_observation(),
            }
            let result = agent
                .as_ref()
                .unwrap()
                .delete_workspace(
                    DeleteWorkspaceRequest::new(initial.id().clone(), initial.revision()).unwrap(),
                )
                .await;
            assert_eq!(
                faults.workspace_delete_commit_attempts(),
                1,
                "deletion must not replay after any commit error"
            );
            match self {
                Self::Unchanged => {
                    let error = result.expect_err(
                        "unchanged durable state cannot turn a failed commit into success",
                    );
                    Self::assert_commit_error(&error);
                    assert!(
                        error.supplementary().is_empty(),
                        "successful readback invents no secondary failure"
                    );
                    assert_eq!(workspace.as_ref().unwrap().info().await.unwrap(), initial);
                    assert_eq!(
                        agent.as_ref().unwrap().list_workspaces().await.unwrap(),
                        vec![initial]
                    );
                    assert_eq!(faults.ambiguous_workspace_deletes(), 0);
                    assert_eq!(stats.deletes.load(Ordering::SeqCst), 0);
                }
                Self::Unobservable => {
                    let error = result.expect_err("unobserved deletion cannot publish success");
                    Self::assert_commit_error(&error);
                    assert_eq!(faults.ambiguous_workspace_deletes(), 1);
                    assert!(faults.workspace_delete_observation_failures() > 0);
                    let mut remaining = error.supplementary().iter().collect::<Vec<_>>();
                    let mut read_failure = false;
                    while let Some(secondary) = remaining.pop() {
                        assert!(secondary.cause().is_none(), "{error:?}");
                        if secondary.kind() == ErrorKind::Integration
                            && secondary.detail() == Some(&ErrorDetail::ServiceUnavailable)
                            && matches!(
                                secondary.context().phase,
                                ErrorPhase::StateRead | ErrorPhase::Reconciliation
                            )
                        {
                            read_failure = true;
                        }
                        remaining.extend(secondary.supplementary());
                    }
                    assert!(
                        read_failure,
                        "last failed readback must remain supplementary evidence: {error:?}"
                    );
                    assert_eq!(
                        stats.deletes.load(Ordering::SeqCst),
                        0,
                        "uncertain outcome cannot trigger native cleanup"
                    );
                    assert_eq!(
                        workspace.as_ref().unwrap().info().await.unwrap_err().kind(),
                        ErrorKind::Closed
                    );
                }
                Self::ConfirmedAbsent => {
                    result.expect(
                        "confirmed committed deletion succeeds despite the lost commit receipt",
                    );
                    assert_eq!(faults.ambiguous_workspace_deletes(), 1);
                    assert_eq!(faults.workspace_delete_observation_failures(), 1);
                    assert!(
                        agent
                            .as_ref()
                            .unwrap()
                            .list_workspaces()
                            .await
                            .unwrap()
                            .is_empty()
                    );
                    assert_eq!(
                        stats.deletes.load(Ordering::SeqCst),
                        1,
                        "confirmed deletion cleans the owned native session once"
                    );
                    assert_eq!(
                        workspace.as_ref().unwrap().info().await.unwrap_err().kind(),
                        ErrorKind::Closed
                    );
                }
            }
        })
        .await;

        // The failed observation is retained while every owned runtime settles.
        // Quiescent child drops unregister their claims; Agent shutdown owns the
        // native event loop. Durable records are not rewritten by this fixture.
        faults.release_workspace_delete_commit_observation();
        drop(conversation.take());
        drop(workspace.take());
        let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
        if let Some(agent) = agent {
            cleanup.settle(agent.shutdown()).await;
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
        eprintln!(
            "workspace delete {self:?}: commit_attempts={}, committed={}, read_failures={}, active={}, connections={}, cleanup={:?}",
            faults.workspace_delete_commit_attempts(),
            faults.ambiguous_workspace_deletes(),
            faults.workspace_delete_observation_failures(),
            stats.active.load(Ordering::SeqCst),
            stats.connections.load(Ordering::SeqCst),
            cleanup.diagnostics()
        );
        cleanup.finish(observation);
        assert_eq!(stats.active.load(Ordering::SeqCst), 0);
        assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
        assert_eq!(
            stats.messages.load(Ordering::SeqCst),
            0,
            "deletion regressions perform no generation"
        );
    }
}

#[tokio::test]
async fn opencode_workspace_delete_preserves_failed_commit_when_record_unchanged() {
    WorkspaceDeleteCase::Unchanged.verify().await;
}

#[tokio::test]
async fn opencode_workspace_delete_preserves_commit_and_unavailable_readback_evidence() {
    WorkspaceDeleteCase::Unobservable.verify().await;
}

#[tokio::test]
async fn opencode_workspace_delete_confirmed_absent_preserves_success_boundary() {
    WorkspaceDeleteCase::ConfirmedAbsent.verify().await;
}
