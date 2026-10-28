//! Backend-host file paths must not be interpreted through the Kit host OS.
//! These typed session events exercise public notification delivery only.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use agents_kit::conversation::{ConversationBackend, NewConversation};
use agents_kit::error::ErrorKind;
use agents_kit::notification::ConversationNotificationKind;
use agents_kit::workspace::{
    NewWorkspace, WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
    WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
};
use agents_kit::{AgentBackend, AgentBuilder, KitError, WorkspaceBackend};
use tokio::time::Instant;

use super::OpenCodeEventProbe;
use super::native_cleanup::NativeCleanup;
use super::opencode_cwd_routing::CwdRoutingFixture;

#[derive(Clone, Copy, Debug)]
enum FilePathCase {
    Posix,
    WindowsDrive,
    WindowsUnc,
}

impl FilePathCase {
    fn directory(self) -> &'static str {
        match self {
            Self::Posix => "/backend/project",
            Self::WindowsDrive => r"C:\backend\project",
            Self::WindowsUnc => r"\\server\share\project",
        }
    }

    fn events(self) -> Vec<(&'static str, &'static str)> {
        match self {
            Self::Posix => vec![
                ("src/relative.rs", "src/relative.rs"),
                ("/backend/project/src/absolute.rs", "src/absolute.rs"),
                ("src/old/../normalized.rs", "src/normalized.rs"),
            ],
            Self::WindowsDrive => vec![
                (r"src\relative.rs", "src/relative.rs"),
                ("C:/backend/project/src/absolute.rs", "src/absolute.rs"),
                (r"src\old\..\normalized.rs", "src/normalized.rs"),
            ],
            Self::WindowsUnc => vec![
                (r"src\relative.rs", "src/relative.rs"),
                ("//server/share/project/src/absolute.rs", "src/absolute.rs"),
                (r"src\old\..\normalized.rs", "src/normalized.rs"),
            ],
        }
    }

    async fn verify(self, rejected: Option<&str>) {
        let mut probe = OpenCodeEventProbe::start(0).await;
        let stats = Arc::clone(&probe.stats);
        *stats.cwd_routing.lock().unwrap() = Some(CwdRoutingFixture::new());
        let mut agent = None;
        let mut workspace = None;
        let mut conversation = None;
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(15), async {
            agent = Some(
                AgentBuilder::create(
                    probe.config(Duration::from_secs(2), Duration::from_secs(3)),
                    OpenCodeEventProbe::storage().await,
                )
                .build()
                .await
                .expect("native fixture accepts Agent construction"),
            );
            let models = agent.as_ref().unwrap().list_models().await.unwrap();
            assert_eq!(models.len(), 1, "fixture declares one exact model");
            let defaults =
                WorkspaceDefaults::new(Some(models[0].selector().clone()), None, None).unwrap();
            let directory = PathBuf::from(self.directory());
            let context = WorkspaceContext::new(
                directory.clone(),
                vec![WorkspaceRoot::new(directory, WorkspaceRootAccess::ReadWrite).unwrap()],
                WorkspaceExecutionPolicy::new(
                    WorkspaceSandboxMode::FullAccess,
                    WorkspaceNetworkAccess::Enabled,
                ),
            )
            .unwrap();
            workspace = Some(
                agent
                    .as_ref()
                    .unwrap()
                    .new_workspace(
                        NewWorkspace::new("backend-file-path", context.clone(), defaults).unwrap(),
                    )
                    .await
                    .expect("backend-host workspace must construct"),
            );
            conversation = Some(
                workspace
                    .as_ref()
                    .unwrap()
                    .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                    .await
                    .expect("backend-host conversation must construct"),
            );
            assert_eq!(
                workspace.as_ref().unwrap().info().await.unwrap().context(),
                &context
            );
            let mut stream = conversation
                .as_ref()
                .unwrap()
                .notification_stream()
                .unwrap();
            if let Some(native) = rejected {
                probe
                    .events
                    .send(
                        serde_json::json!({
                            "directory": self.directory(),
                            "payload": {"id": "evt_rejected_file_path", "type": "session.diff",
                                "properties": {"sessionID": "ses_probe", "diff": [{
                                    "file": native, "additions": 1, "deletions": 0,
                                }]},
                            },
                        })
                        .to_string(),
                    )
                    .unwrap();
                let error = stream
                    .recv()
                    .await
                    .expect("invalid file facts surface a terminal error")
                    .expect_err("invalid paths cannot produce portable notifications");
                assert_eq!(error.kind(), ErrorKind::Closed, "{error:?}");
                assert!(stream.recv().await.is_none());
                return;
            }
            for (index, (native, expected)) in self.events().into_iter().enumerate() {
                // Selected snapshot/index.ts emits absolute slash-separated Patch
                // paths. Relative SessionDiff paths exercise the other native form.
                let (kind, properties) = if index == 1 {
                    (
                        "message.part.updated",
                        serde_json::json!({
                            "sessionID": "ses_probe", "time": 1, "part": {
                                "id": "prt_file_path", "sessionID": "ses_probe",
                                "messageID": "msg_file_path", "type": "patch",
                                "hash": "native_snapshot_hash", "files": [native],
                            },
                        }),
                    )
                } else {
                    (
                        "session.diff",
                        serde_json::json!({
                            "sessionID": "ses_probe", "diff": [{
                                "file": native, "additions": 1, "deletions": 0,
                            }],
                        }),
                    )
                };
                probe
                    .events
                    .send(
                        serde_json::json!({
                            "directory": self.directory(),
                            "payload": {
                                "id": format!("evt_file_path_{index}"), "type": kind,
                                "properties": properties,
                            },
                        })
                        .to_string(),
                    )
                    .expect("the owned global event subscription exists");
                let item = stream
                    .recv()
                    .await
                    .expect("event failure cannot silently close delivery")
                    .expect("valid backend-host file event must remain deliverable");
                match item.kind() {
                    ConversationNotificationKind::FileChanged { path, summary } => {
                        // Both separators are portable. Compare the full segment
                        // sequence without relying on the Kit host Path parser.
                        let text = path.to_str().expect("native fixture paths are UTF-8");
                        assert_eq!(
                            text.split(['/', '\\']).collect::<Vec<_>>(),
                            expected.split('/').collect::<Vec<_>>()
                        );
                        assert!(summary.is_none());
                    }
                    other => panic!("expected the exact file notification, got {other:?}"),
                }
            }
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
        let mut shutdown = None;
        if rejected.is_some() {
            // Invalid native facts seal delivery. Child handles carry no new work;
            // Agent shutdown joins the failed owner and retains its exact error.
            drop(conversation);
            drop(workspace);
        } else {
            if let Some(conversation) = conversation {
                cleanup.settle(conversation.close()).await;
            }
            if let Some(workspace) = workspace {
                cleanup.settle(workspace.close()).await;
            }
        }
        if let Some(agent) = agent {
            cleanup
                .settle(async {
                    shutdown = Some(agent.shutdown().await);
                    Ok::<(), KitError>(())
                })
                .await;
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
            "file path {self:?}: active={}, connections={}, cleanup={:?}",
            stats.active.load(Ordering::SeqCst),
            stats.connections.load(Ordering::SeqCst),
            cleanup.diagnostics()
        );
        eprintln!("file path {self:?} rejected={rejected:?}: shutdown={shutdown:?}");
        cleanup.finish(observed);
        let shutdown = shutdown.expect("Agent shutdown owner was observed");
        if rejected.is_some() {
            let error = shutdown.expect_err("native path rejection must survive shutdown");
            assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
            assert!(
                error.supplementary().is_empty(),
                "unrelated cleanup failures remain failures: {error:?}"
            );
        } else {
            shutdown.expect("positive file notification owners settle cleanly");
        }
        assert_eq!(stats.active.load(Ordering::SeqCst), 0);
        assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
        assert_eq!(
            stats.messages.load(Ordering::SeqCst),
            0,
            "file events perform no generation"
        );
    }
}

#[tokio::test]
async fn opencode_file_notifications_preserve_posix_backend_paths() {
    FilePathCase::Posix.verify(None).await;
}

#[tokio::test]
async fn opencode_file_notifications_preserve_windows_drive_backend_paths() {
    FilePathCase::WindowsDrive.verify(None).await;
}

#[tokio::test]
async fn opencode_file_notifications_preserve_windows_unc_backend_paths() {
    FilePathCase::WindowsUnc.verify(None).await;
}

#[tokio::test]
async fn opencode_file_notifications_reject_posix_escape_and_non_files() {
    for path in [
        "../outside.rs",
        "/backend/project-neighbor/file.rs",
        "../../../escape.rs",
        "/backend/project",
    ] {
        FilePathCase::Posix.verify(Some(path)).await;
    }
}

#[tokio::test]
async fn opencode_file_notifications_reject_windows_drive_escape_and_foreign_anchors() {
    for path in [
        r"..\outside.rs",
        r"D:\backend\project\file.rs",
        r"C:relative.rs",
        r"C:\backend\project-neighbor\file.rs",
        r"\rooted-without-drive.rs",
    ] {
        FilePathCase::WindowsDrive.verify(Some(path)).await;
    }
}

#[tokio::test]
async fn opencode_file_notifications_reject_unc_escape_and_foreign_shares() {
    for path in [
        r"..\outside.rs",
        "//other/share/project/file.rs",
        "//server/other/project/file.rs",
        r"\\server\share\project-neighbor\file.rs",
    ] {
        FilePathCase::WindowsUnc.verify(Some(path)).await;
    }
}
