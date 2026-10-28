use std::path::Path;
use std::time::Duration;

use agents_kit::conversation::NewConversation;
use agents_kit::workspace::{NewWorkspace, WorkspaceDefaults};
use agents_kit::{AgentBackend, AgentBuilder, AgentStorage, ConversationBackend, WorkspaceBackend};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use tokio::time::Instant;

use super::fixture::{StrictFixture, StrictOpenCodeProcess};
use crate::native_cleanup::NativeCleanup;

#[tokio::test]
async fn opencode_backend_host_cwd_alias_preserves_public_construction() {
    // This is a real native construction case, with no prompt or model generation.
    // Only this fixture's own local root is canonicalized. The alias supplied to
    // WorkspaceContext below is deliberately preserved byte-for-byte.
    let mut root = std::env::temp_dir().join(StrictFixture::unique("agent-cwd-alias"));
    let mut paths = None;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(90));
    let mut observation = NativeCleanup::observe(Instant::now() + Duration::from_secs(90), async {
        std::fs::create_dir_all(root.join("real")).unwrap();
        root = std::fs::canonicalize(&root).unwrap();
        let canonical = root.join("real");
        let alias = root.join("alias");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&canonical, &alias)
            .expect("the owned native-host directory alias must be created");
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&canonical, &alias)
            .expect("the owned native-host directory alias must be created");
        assert!(alias != canonical);
        assert!(
            std::fs::canonicalize(&alias).unwrap() == canonical,
            "the fixture alias must point to its owned native-host directory"
        );
        paths = Some([("canonical", canonical), ("alias", alias)]);
    })
    .await;
    let mut all_owners_settled = true;
    if let Some(paths) = paths {
        for (label, requested) in paths {
            // Each row owns a separate native inventory and StateStore. Parent
            // shutdown preserves native sessions, so sharing a process would let
            // the canonical control become a foreign session in the alias row.
            let mut process = None;
            let mut process_start_unobserved = false;
            let mut store = None;
            let mut agent = None;
            let mut build_unobserved = false;
            let mut workspace = None;
            let mut conversation = None;
            let mut stage = "start_process";
            let row = NativeCleanup::observe(
                Instant::now() + Duration::from_secs(120),
                async {
                    process_start_unobserved = true;
                    process = Some(StrictOpenCodeProcess::start(&format!("cwd-{label}")).await);
                    process_start_unobserved = false;
                    stage = "native_path";
                    let client = reqwest::Client::builder()
                        .connect_timeout(Duration::from_secs(2))
                        .timeout(Duration::from_secs(5))
                        .redirect(reqwest::redirect::Policy::none())
                        .retry(reqwest::retry::never())
                        .build()
                        .unwrap();
                    let canonical = root.join("real");
                    for spelling in [&canonical, &root.join("alias")] {
                        // Native-host corroboration only. Never replace the
                        // caller's portable context with this observed directory.
                        let native = client
                            .get(format!("{}/path", process.as_ref().unwrap().endpoint()))
                            .query(&[("directory", spelling.to_str().unwrap())])
                            .send()
                            .await
                            .expect("native path observation must respond")
                            .error_for_status()
                            .expect("native path observation must succeed")
                            .json::<serde_json::Value>()
                            .await
                            .expect("native path observation must decode");
                        let directory = native["directory"]
                            .as_str()
                            .expect("native path observation must contain its directory");
                        assert!(
                            Path::new(directory) == canonical,
                            "both caller spellings must identify the exact owned native directory"
                        );
                    }
                    eprintln!("cwd alias row={label}; native_directories_equal=true; canonical_target_matches=true");
                    stage = "connect_store";
                    store = Some(
                        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
                            root.join(format!("{label}-state")),
                        )))
                        .await
                        .expect("the independent caller-owned Agent store must open"),
                    );
                    stage = "build_agent";
                    let builder = AgentBuilder::create(
                        process.as_ref().unwrap().config(label, Duration::from_secs(30)),
                        AgentStorage::new(store.as_ref().unwrap().clone()),
                    );
                    build_unobserved = true;
                    let built = builder.build().await;
                    build_unobserved = false;
                    agent = Some(built.expect("native Agent construction must succeed"));
                    let context = StrictFixture::workspace_context(requested);
                    stage = "new_workspace";
                    workspace = Some(
                        agent
                            .as_ref()
                            .unwrap()
                            .new_workspace(
                                NewWorkspace::new(label, context.clone(), WorkspaceDefaults::empty())
                                    .unwrap(),
                            )
                            .await
                            .expect("the complete backend-host workspace context must be accepted"),
                    );
                    stage = "workspace_info";
                    let info = workspace.as_ref().unwrap().info().await.unwrap();
                    assert!(
                        info.context() == &context,
                        "public workspace context must preserve the caller's exact cwd and roots"
                    );
                    stage = "new_conversation";
                    conversation = Some(
                        workspace
                            .as_ref()
                            .unwrap()
                            .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                            .await
                            .expect("a representable native cwd alias must support public conversation creation"),
                    );
                    stage = "conversation_info";
                    let current = conversation.as_ref().unwrap();
                    assert!(current.info().await.unwrap().id() == &current.id());
                    stage = "complete";
                },
            )
            .await;
            eprintln!(
                "cwd alias row={label}; stage={stage}; public_construction_ok={}",
                matches!(&row, Ok(Ok(())))
            );
            // Catching an observation failure never passes a row. Keep the first
            // failure primary while attempting every independent owner cleanup.
            if matches!(&observation, Ok(Ok(()))) {
                observation = row;
            }
            let finalizer_required = agent.is_some() || build_unobserved;
            let mut finalizer_observed = false;
            if let Some(owned_agent) = agent.take() {
                let mut shutdown = tokio::spawn(async move { owned_agent.shutdown().await });
                let mut joined = false;
                cleanup
                    .settle::<String>(async {
                        let result = (&mut shutdown).await;
                        joined = true;
                        match result {
                            Ok(result) => {
                                finalizer_observed = true;
                                result.map_err(|error| format!("{label} Agent shutdown: {error:?}"))
                            }
                            Err(error) => Err(format!("{label} Agent shutdown join: {error:?}")),
                        }
                    })
                    .await;
                if !joined {
                    shutdown.abort();
                    cleanup
                        .settle::<String>(async {
                            match (&mut shutdown).await {
                                Ok(result) => {
                                    finalizer_observed = true;
                                    result.map_err(|error| {
                                        format!("{label} late Agent shutdown: {error:?}")
                                    })
                                }
                                Err(error) if error.is_cancelled() => Ok(()),
                                Err(error) => {
                                    Err(format!("{label} fallback shutdown join: {error:?}"))
                                }
                            }
                        })
                        .await;
                }
            }
            drop(conversation);
            drop(workspace);
            drop(store);
            let mut process_reaped = process.is_none() && !process_start_unobserved;
            if let Some(owned_process) = process.as_mut() {
                cleanup
                    .settle::<String>(async {
                        owned_process.stop().await;
                        process_reaped = true;
                        Ok(())
                    })
                    .await;
            }
            if let Some(owned_process) = process.take() {
                cleanup
                    .settle::<String>(async {
                        owned_process.shutdown().await;
                        process_reaped = true;
                        Ok(())
                    })
                    .await;
            }
            all_owners_settled &= (!finalizer_required || finalizer_observed) && process_reaped;
            eprintln!(
                "cwd alias row={label}; finalizer_required={finalizer_required}; finalizer_receipt={finalizer_observed}; native_reap_receipt={process_reaped}; build_unobserved={build_unobserved}; process_start_unobserved={process_start_unobserved}"
            );
        }
    }
    if all_owners_settled {
        cleanup
            .settle(async { std::fs::remove_dir_all(&root) })
            .await;
    } else {
        eprintln!("cwd alias retained_owned_root=true; all_owner_receipts=false");
    }
    cleanup.finish(observation);
}
