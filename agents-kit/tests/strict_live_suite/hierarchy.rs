use crate::native_cleanup::NativeCleanup;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::Stdio;

use std::time::Duration;
use tokio::io::AsyncReadExt;

use agents_kit::AgentBuilder;
use agents_kit::config::{AgentConfig, OpenCodeConfig};
use agents_kit::workspace::{
    DeleteWorkspaceRequest, UpdateWorkspaceRequest, WorkspaceUpdateReadiness,
};
use agents_kit::{AgentBackend, ConversationBackend, WorkspaceBackend};

use super::fixture::{StrictBackend, StrictFixture, StrictOpenCodeProcess};

struct HierarchyScenario {
    backend: StrictBackend,
}

impl HierarchyScenario {
    async fn command_output(
        command: &mut tokio::process::Command,
        owned: &mut Option<tokio::process::Child>,
    ) -> std::io::Result<std::process::Output> {
        assert!(
            owned.is_none(),
            "a utility child must settle before its successor starts"
        );
        let child = owned.insert(
            command
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()?,
        );
        let mut stdout = child.stdout.take().expect("command stdout must be piped");
        let mut stderr = child.stderr.take().expect("command stderr must be piped");
        let mut output = Vec::new();
        let mut error = Vec::new();
        let (status, read_output, read_error) = tokio::join!(
            child.wait(),
            stdout.read_to_end(&mut output),
            stderr.read_to_end(&mut error)
        );
        let status = status?;
        read_output?;
        read_error?;
        owned.take();
        Ok(std::process::Output {
            status,
            stdout: output,
            stderr: error,
        })
    }

    async fn verify_release_configuration(fixture: &StrictFixture, deadline: tokio::time::Instant) {
        let mut owned_process = None;
        let mut owned_agent = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        let observed = NativeCleanup::observe(deadline, async {
            let label = StrictFixture::unique("strict-release-config");
            let process = owned_process.insert(StrictOpenCodeProcess::configured(
                &label,
                StrictOpenCodeProcess::release_config(),
            ));
            // Retain the owner before startup can yield or time out.
            process.restart().await;
            process.verify_rejected_permissions().await;
            let agent = owned_agent.insert(
                AgentBuilder::create(
                    process.config(&label, Duration::from_secs(60)),
                    fixture.storage(),
                )
                .build()
                .await
                .expect("isolated release Agent must build"),
            );
            process.verify_release_inventory(agent).await;
            let workspace = owned_workspace.insert(
                agent
                    .new_workspace(StrictFixture::workspace(&label))
                    .await
                    .expect("isolated policy workspace must build"),
            );
            let conversation = owned_conversation.insert(
                workspace
                    .new_conversation(StrictFixture::conversation(&label))
                    .await
                    .expect("unrestricted session must override restrictive native defaults"),
            );
            conversation
                .info()
                .await
                .expect("portable conversation must remain valid");
            process.verify_release_policy(&label).await;
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        if let Some(conversation) = owned_conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = owned_workspace {
            let id = workspace.id();
            cleanup.settle(workspace.close()).await;
            if let Some(agent) = &owned_agent {
                cleanup
                    .settle(async {
                        let info = agent.workspace_info(id.clone()).await?;
                        agent
                            .delete_workspace(
                                DeleteWorkspaceRequest::new(id, info.revision()).unwrap(),
                            )
                            .await
                    })
                    .await;
            }
        }
        if let Some(agent) = owned_agent {
            cleanup.settle(agent.shutdown()).await;
        }
        if let Some(process) = owned_process {
            cleanup
                .settle::<std::io::Error>(async {
                    process.shutdown().await;
                    Ok(())
                })
                .await;
        }
        cleanup.finish(observed);
    }

    async fn run(self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        let fixture = tokio::time::timeout_at(deadline, StrictFixture::new())
            .await
            .expect("strict-live hierarchy storage setup must remain bounded");
        let mut owned_agent = None;
        let mut owned_shutdown = None;
        let mut owned_workspace = None;
        let mut owned_conversation = None;
        if matches!(self.backend, StrictBackend::OpenCode) {
            Self::verify_release_configuration(&fixture, deadline).await;
        }
        let observed = NativeCleanup::observe(deadline, async {
            let label = StrictFixture::unique(match self.backend {
                StrictBackend::Codex => "strict-codex-hierarchy",
                StrictBackend::OpenCode => "strict-opencode-hierarchy",
            });
            let (config, created) = fixture.create(self.backend, &label).await;
            let agent = owned_agent.insert(created);
            let agent_id = agent.id();

            let info = agent
                .info()
                .await
                .expect("strict-live agent info must load");
            assert_eq!(info.id(), &agent_id);
            assert_eq!(info.display_name(), Some(label.as_str()));
            assert!(
                !agent
                    .list_models()
                    .await
                    .expect("strict-live model inventory must load")
                    .is_empty(),
                "selected backend must advertise at least one model"
            );

            let workspace_name = StrictFixture::unique("strict-workspace");
            let workspace = owned_workspace.insert(
                agent
                    .new_workspace(StrictFixture::workspace(&workspace_name))
                    .await
                    .expect("strict-live workspace must be created"),
            );
            let initial = workspace
                .info()
                .await
                .expect("created workspace info must load");
            let workspace_id = initial.id().clone();
            assert_eq!(initial.revision(), 1);
            assert!(
                agent
                    .list_workspaces()
                    .await
                    .expect("workspace inventory must load")
                    .iter()
                    .any(|candidate| candidate.id() == &workspace_id)
            );

            let updated_name = format!("{workspace_name}-updated");
            let update = UpdateWorkspaceRequest::new(
                workspace_id.clone(),
                initial.revision(),
                updated_name.clone(),
                initial.context().clone(),
                initial.defaults().clone(),
            )
            .expect("workspace update must be valid");
            assert_eq!(
                agent
                    .check_workspace_update(&update)
                    .await
                    .expect("workspace update preflight must complete"),
                WorkspaceUpdateReadiness::Ready
            );
            let updated = agent
                .update_workspace(update)
                .await
                .expect("workspace update must commit");
            assert_eq!(updated.revision(), initial.revision() + 1);
            assert_eq!(updated.name(), updated_name);
            assert!(
                matches!(
                    (workspace
                        .info()
                        .await
                        .expect_err("pre-update workspace handle must become stale"))
                    .kind(),
                    agents_kit::error::ErrorKind::Conflict | agents_kit::error::ErrorKind::Closed
                ),
                "stale handle must not remain usable"
            );

            let workspace = owned_workspace.insert(
                agent
                    .get_workspace(workspace_id.clone())
                    .await
                    .expect("updated workspace must reopen"),
            );
            let conversation_name = StrictFixture::unique("strict-conversation");
            let conversation = owned_conversation.insert(
                workspace
                    .new_conversation(StrictFixture::conversation(&conversation_name))
                    .await
                    .expect("strict-live conversation must be created"),
            );
            let conversation_id = conversation.id();
            assert_eq!(
                conversation
                    .info()
                    .await
                    .expect("conversation info must load")
                    .title(),
                Some(conversation_name.as_str())
            );
            assert!(
                workspace
                    .list_conversations()
                    .await
                    .expect("conversation inventory must load")
                    .iter()
                    .any(|candidate| candidate.id() == &conversation_id)
            );

            owned_workspace
                .take()
                .expect("workspace owner must remain available")
                .close()
                .await
                .expect("workspace close must settle descendants");
            assert!(
                matches!(
                    (conversation
                        .info()
                        .await
                        .expect_err("workspace close must invalidate a live descendant"))
                    .kind(),
                    agents_kit::error::ErrorKind::Closed | agents_kit::error::ErrorKind::Conflict
                ),
                "closed-parent descendant must reject new work"
            );

            let workspace = owned_workspace.insert(
                agent
                    .get_workspace(workspace_id.clone())
                    .await
                    .expect("quiescent durable workspace must reopen"),
            );
            drop(owned_conversation.take());
            let _conversation = owned_conversation.insert(
                workspace
                    .get_conversation(conversation_id.clone())
                    .await
                    .expect("quiescent durable conversation must reopen"),
            );
            owned_conversation
                .take()
                .expect("conversation owner must remain available")
                .close()
                .await
                .expect("conversation close must reach a terminal state");
            owned_workspace
                .take()
                .expect("workspace owner must remain available")
                .close()
                .await
                .expect("reopened workspace must close");

            let shutdown = owned_shutdown.insert(tokio::spawn(
                owned_agent
                    .take()
                    .expect("Agent owner must remain available")
                    .shutdown(),
            ));
            let result = shutdown.await;
            owned_shutdown.take();
            result
                .expect("intermediate shutdown observer must join")
                .expect("strict-live agent shutdown must settle");
            let reopened = owned_agent.insert(fixture.reopen(config, agent_id).await);
            let workspace = owned_workspace.insert(
                reopened
                    .get_workspace(workspace_id.clone())
                    .await
                    .expect("workspace must remain durable across agent shutdown"),
            );
            let terminal_error = match workspace.get_conversation(conversation_id).await {
                Ok(conversation) => {
                    owned_conversation = Some(conversation);
                    panic!("terminal conversation must not reopen")
                }
                Err(error) => error,
            };
            assert!(matches!(
                (terminal_error).kind(),
                agents_kit::error::ErrorKind::NotFound | agents_kit::error::ErrorKind::Closed
            ));
            owned_workspace
                .take()
                .expect("workspace owner must remain available")
                .close()
                .await
                .expect("final workspace close must settle");
            let current = reopened
                .workspace_info(workspace_id.clone())
                .await
                .expect("final workspace info must load");
            reopened
                .delete_workspace(
                    DeleteWorkspaceRequest::new(workspace_id, current.revision())
                        .expect("workspace deletion must be valid"),
                )
                .await
                .expect("quiescent workspace must delete");
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
        if let Some(conversation) = owned_conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = owned_workspace {
            cleanup.settle(workspace.close()).await;
        }
        if let Some(agent) = owned_agent {
            cleanup.settle(agent.shutdown()).await;
        }
        if let Some(mut shutdown) = owned_shutdown {
            cleanup.join(&mut shutdown).await;
        }
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_hierarchy_is_revisioned_and_durable() {
    HierarchyScenario {
        backend: StrictBackend::Codex,
    }
    .run(Duration::from_secs(240))
    .await;
}

#[tokio::test]
async fn opencode_hierarchy_is_revisioned_and_durable() {
    HierarchyScenario {
        backend: StrictBackend::OpenCode,
    }
    .run(Duration::from_secs(300))
    .await;
}

#[tokio::test]
async fn opencode_spawned_shutdown_settles_process_after_hierarchy_cutoff() {
    let fixture = StrictFixture::new().await;
    let address = StrictFixture::available_loopback_address();
    let lifecycle_timeout = Duration::from_secs(60);
    let config = AgentConfig::new(
        OpenCodeConfig::spawn(
            PathBuf::from("opencode"),
            Vec::new(),
            address,
            Duration::from_secs(30),
            Duration::from_secs(600),
            Duration::from_secs(30),
        )
        .expect("strict-live spawned OpenCode config must be valid")
        .into(),
        lifecycle_timeout,
        NonZeroUsize::new(1).expect("one prompt is nonzero"),
    )
    .expect("strict-live cutoff Agent config must be valid")
    .with_display_name(StrictFixture::unique("strict-opencode-shutdown-cutoff"))
    .expect("strict-live cutoff display name must be valid");
    let mut owned_agent = None;
    let mut owned_workspace = None;
    let mut owned_creation = None;
    let mut owned_shutdown = None;
    let mut owned_process_id = None;
    let mut owned_command = None;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(180), async {
    let agent = owned_agent.insert(AgentBuilder::create(config, fixture.storage())
        .build()
        .await
        .expect("strict-live cutoff Agent must build a stock OpenCode process"));
    let workspace = owned_workspace.insert(agent
        .new_workspace(StrictFixture::workspace(
            "strict-opencode-shutdown-cutoff-workspace",
        ))
        .await
        .expect("strict-live cutoff workspace must be created"));
    let current = workspace
        .info()
        .await
        .expect("strict-live cutoff workspace info must load");
    let update = UpdateWorkspaceRequest::new(
        current.id().clone(),
        current.revision(),
        current.name(),
        current.context().clone(),
        current.defaults().clone(),
    )
    .expect("strict-live cutoff readiness request must be valid");
    assert_eq!(
        agent
            .check_workspace_update(&update)
            .await
            .expect("strict-live idle readiness must be observable"),
        WorkspaceUpdateReadiness::Ready,
        "idle descendant handles must not count as admitted work"
    );

    let listener = format!("-iTCP@{}:{}", address.ip(), address.port());
    let output = HierarchyScenario::command_output(
        tokio::process::Command::new("lsof").args(["-nP", "-t", listener.as_str(), "-sTCP:LISTEN"]),
        &mut owned_command,
    ).await.expect("strict-live must be able to locate the spawned OpenCode listener");
    assert!(
        output.status.success(),
        "strict-live spawned OpenCode listener must have an owning process: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let process_id = String::from_utf8(output.stdout)
        .expect("strict-live listener process id must be UTF-8")
        .trim()
        .parse::<u32>()
        .expect("strict-live listener process id must be numeric");
    let process_id = owned_process_id.insert(process_id.to_string());
    let suspended = HierarchyScenario::command_output(
        tokio::process::Command::new("kill").args(["-STOP", process_id.as_str()]),
        &mut owned_command,
    ).await.expect("strict-live must be able to suspend the spawned OpenCode process");
    assert!(
        suspended.status.success(),
        "strict-live must suspend the exact spawned OpenCode process: {}",
        String::from_utf8_lossy(&suspended.stderr)
    );

    let workspace = owned_workspace.take().expect("cutoff workspace must remain owned");
    let creation = owned_creation.insert(tokio::spawn(async move {
        workspace
            .new_conversation(StrictFixture::conversation(
                "strict OpenCode suspended creation",
            ))
            .await
    }));
    let admission = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match agent.check_workspace_update(&update).await {
                Ok(WorkspaceUpdateReadiness::Busy) => break Ok(()),
                Ok(WorkspaceUpdateReadiness::Ready) => tokio::task::yield_now().await,
                Err(error) => break Err(error),
            }
        }
    })
    .await;
    let creation_finished_before_cleanup = creation.is_finished();
    match admission {
        Ok(result) => result.expect("strict-live cutoff readiness must be observable"),
        Err(_) => panic!(
            "the suspended conversation creation must register its hierarchy admission; creation finished before cleanup: {creation_finished_before_cleanup}"
        ),
    }

    let shutdown_started = tokio::time::Instant::now();
    let shutdown_task = owned_shutdown.insert(tokio::spawn(
        owned_agent.take().expect("cutoff Agent must remain owned").shutdown(),
    ));
    let shutdown = tokio::time::timeout(lifecycle_timeout + Duration::from_secs(5), shutdown_task).await;
    let shutdown = match shutdown {
        Ok(result) => { owned_shutdown.take(); Ok(result.expect("shutdown observer must join")) },
        Err(error) => Err(error),
    };
    let shutdown_elapsed = shutdown_started.elapsed();
    let connection = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(address),
    )
    .await;
    let process = HierarchyScenario::command_output(
        tokio::process::Command::new("kill").args(["-0", process_id.as_str()]),
        &mut owned_command,
    ).await;
    let creation_result = tokio::time::timeout(Duration::from_secs(5), owned_creation.as_mut().expect("creation observer must remain owned")).await;
    if creation_result.is_ok() { owned_creation.take(); }

    let result = shutdown.expect("spawned shutdown must remain within its lifecycle bound");
    assert!(
        matches!(&result, Err(error) if matches!(error.kind(), agents_kit::error::ErrorKind::Timeout)),
        "the admitted conversation creation must exhaust the settlement phase: {result:?}"
    );
    assert!(
        shutdown_elapsed >= lifecycle_timeout / 2,
        "spawned shutdown must reach the hierarchy settlement cutoff"
    );
    assert!(
        connection
            .expect("the spawned OpenCode listener probe must remain bounded")
            .is_err(),
        "Agent shutdown must stop its stock OpenCode process before returning"
    );
    let process = process.expect("strict-live must be able to observe spawned process exit");
    assert!(
        !process.status.success(),
        "Agent shutdown must reap the exact stock OpenCode listener process"
    );
    assert!(
        matches!(creation_result, Ok(Ok(Err(_)))),
        "runtime teardown must terminalize the admitted conversation creation"
    );
    }).await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
    if let Some(child) = owned_command.as_mut() {
        cleanup.reap(child).await;
        if child.try_wait().is_ok_and(|status| status.is_some()) {
            owned_command.take();
        }
    }
    if let Some(workspace) = owned_workspace {
        cleanup.settle(workspace.close()).await;
    }
    if let Some(agent) = owned_agent {
        cleanup.settle(agent.shutdown()).await;
    }
    if let Some(mut shutdown) = owned_shutdown {
        cleanup.join(&mut shutdown).await;
    }
    if let Some(process_id) = owned_process_id
        && owned_command.is_none()
    {
        cleanup
            .settle::<std::io::Error>(async {
                let present = HierarchyScenario::command_output(
                    tokio::process::Command::new("kill").args(["-0", process_id.as_str()]),
                    &mut owned_command,
                )
                .await?;
                if present.status.success() {
                    let killed = HierarchyScenario::command_output(
                        tokio::process::Command::new("kill").args(["-KILL", process_id.as_str()]),
                        &mut owned_command,
                    )
                    .await?;
                    assert!(
                        killed.status.success(),
                        "fallback must stop the exact owned listener"
                    );
                    loop {
                        let present = HierarchyScenario::command_output(
                            tokio::process::Command::new("kill").args(["-0", process_id.as_str()]),
                            &mut owned_command,
                        )
                        .await?;
                        if !present.status.success() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }
                Ok(())
            })
            .await;
    }
    if let Some(creation) = owned_creation {
        creation.abort();
        cleanup
            .settle(async {
                match creation.await {
                    Ok(Ok(conversation)) => {
                        conversation
                            .close()
                            .await
                            .expect("unexpected returned child must settle");
                        Ok(())
                    }
                    Ok(Err(_)) => Ok(()),
                    Err(error) if error.is_cancelled() => Ok(()),
                    Err(error) => Err(error),
                }
            })
            .await;
    }
    if let Some(child) = owned_command.as_mut() {
        cleanup.reap(child).await;
    }
    cleanup.finish(observed);
}
