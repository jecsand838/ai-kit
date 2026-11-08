use std::{fs, path::PathBuf, process::Command, time::Duration};

use storage_kit::{
    Database, StorageError,
    database::config::{CommonConfig, DatabaseConfig, SurrealConfig, SurrealModeConfig},
};

#[test]
fn surrealdb_legacy_store_requires_native_upgrade() {
    if let Some(path) = std::env::var_os("STORAGE_LEGACY_ATTEMPT") {
        // The child owns the whole native runtime; process exit settles it before
        // the pinned old reader is allowed to open the attempted copy.
        let thread = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("legacy attempt runtime");
                let outcome = runtime.block_on(async {
                    let config = DatabaseConfig::surreal(SurrealConfig {
                        mode: SurrealModeConfig::SurrealKv {
                            path: PathBuf::from(path),
                        },
                        namespace: Some("vendor_migration".to_owned()),
                        database: Some("surreal_330".to_owned()),
                        ..SurrealConfig::default()
                    })
                    .with_common(CommonConfig {
                        connect_timeout_ms: 10_000,
                        ..CommonConfig::default()
                    });
                    match tokio::time::timeout(Duration::from_secs(15), Database::connect(config))
                        .await
                    {
                        Ok(Ok(database)) => {
                            drop(database);
                            serde_json::json!({"connected": true})
                        }
                        Ok(Err(StorageError::BackendOperation { operation, message })) => {
                            serde_json::json!({"operation": operation, "error": message})
                        }
                        Ok(Err(error)) => {
                            serde_json::json!({"unexpected_error": error.to_string()})
                        }
                        Err(error) => serde_json::json!({"timeout": error.to_string()}),
                    }
                });
                drop(runtime);
                fs::write(
                    std::env::var_os("STORAGE_LEGACY_OUTCOME").expect("owned outcome path"),
                    serde_json::to_vec(&outcome).expect("serialize attempt outcome"),
                )
                .expect("record attempt after runtime cleanup");
            })
            .expect("owned legacy attempt thread");
        thread.join().expect("legacy attempt thread settled");
        return;
    }
    let output = Command::new("python3")
        .arg(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/surreal_legacy_store/fixture.py"),
        )
        .arg("check")
        .arg("--executable")
        .arg(std::env::current_exe().expect("current integration executable"))
        .output()
        .expect("start owned legacy-store verifier");
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "legacy boundary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
