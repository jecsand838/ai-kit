use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

#[cfg(unix)]
use std::process::{Child, ExitStatus, Stdio};
#[cfg(unix)]
use std::time::Duration;

use tempfile::TempDir;
#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::time::Instant;

struct VerifierFixture {
    directory: TempDir,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
enum ReadinessStall {
    SurrealHeaders,
    QdrantBody,
}

impl VerifierFixture {
    const REQUIRED: [&'static str; 17] = [
        "TEST_LANCEDB_EMBEDDED_PATH",
        "TEST_SURREALDB_ROCKSDB_PATH",
        "TEST_SURREALDB_SURREALKV_PATH",
        "TEST_SURREALDB_REMOTE_WS",
        "TEST_SURREALDB_REMOTE_HTTP",
        "TEST_SURREALDB_ROOT_USER",
        "TEST_SURREALDB_ROOT_PASS",
        "TEST_SURREALDB_NS",
        "TEST_SURREALDB_DB",
        "TEST_QDRANT_EDGE_PATH",
        "TEST_QDRANT_REMOTE_REST",
        "TEST_POSTGRES_STATE_STORE_HOST",
        "TEST_POSTGRES_STATE_STORE_PORT",
        "TEST_POSTGRES_STATE_STORE_DB",
        "TEST_POSTGRES_STATE_STORE_USER",
        "TEST_POSTGRES_STATE_STORE_PASS",
        "COMPOSE_PROJECT_NAME",
    ];

    fn new() -> Self {
        let fixture = Self {
            directory: tempfile::tempdir().unwrap(),
        };
        fs::create_dir_all(fixture.directory.path().join("storage-sdk/scripts")).unwrap();
        fs::create_dir(fixture.directory.path().join("bin")).unwrap();
        fs::create_dir(fixture.directory.path().join("scripts")).unwrap();
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("scripts/release_stage.sh"),
            fixture.directory.path().join("scripts/release_stage.sh"),
        )
        .unwrap();
        for name in [
            "check.sh",
            "feature_policy.py",
            "verify_strict_live.sh",
            "verify_lancedb_state_store.sh",
        ] {
            fs::copy(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("scripts")
                    .join(name),
                fixture
                    .directory
                    .path()
                    .join("storage-sdk/scripts")
                    .join(name),
            )
            .unwrap();
        }
        fs::create_dir_all(fixture.directory.path().join("storage-sdk/scripts/tests")).unwrap();
        // Exercise actual unittest discovery inside the copied-script fixture.
        // This small contract proves orchestration only; it avoids recursively
        // rerunning the repository's full verifier contract suite.
        fs::write(
            fixture.directory.path().join("storage-sdk/scripts/tests/test_fixture_contract.py"),
            r#"import importlib.util, os, pathlib, unittest
class FixtureContract(unittest.TestCase):
    def test_copied_selector_default(self):
        root = pathlib.Path(__file__).resolve().parents[3]
        spec = importlib.util.spec_from_file_location('fixture_policy', root / 'storage-sdk/scripts/feature_policy.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.StorageSelection(root).result()['backends'], ['lance'])
        (root / 'script-contract-ran').write_text('executed')
        self.assertNotEqual(os.environ.get('STORAGE_FAILURE'), 'script_contract')
"#,
        ).unwrap();
        fs::create_dir_all(fixture.directory.path().join("storage-sdk/src")).unwrap();
        fs::write(
            fixture.directory.path().join("Cargo.lock"),
            "fixture lock\n",
        )
        .unwrap();
        fs::write(
            fixture.directory.path().join("storage-sdk/src/lib.rs"),
            "// fixture source\n",
        )
        .unwrap();
        fixture.executable(
            "rustc",
            r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "${STORAGE_FAILURE}" == embedded_compiler ]]; then
    printf 'rustc 1.94.1 (unsupported fixture compiler)\n'
else
    printf 'rustc 1.99.0 (fixture compiler)\nhost: fixture-host\n'
fi
"#,
        );
        // These commands prove script orchestration only. Real strict-live
        // scripts never read this fixture's controls or use its command PATH.
        fixture.executable(
            "cargo",
            r#"#!/usr/bin/env bash
set -euo pipefail
printf 'cargo' >> "${STORAGE_CALLS}"
printf '\t%s' "$@" >> "${STORAGE_CALLS}"
printf '\n' >> "${STORAGE_CALLS}"
target=''
inventory=0
while (( $# > 0 )); do
    if [[ "$1" == --test ]]; then shift; target="$1"; fi
    if [[ "$1" == --list ]]; then inventory=1; fi
    shift
done
if [[ "${STORAGE_FAILURE}" == deterministic && "${target}" == database_backend ]]; then
    printf 'fixture deterministic failure\n' >&2
    exit 23
fi
if [[ "${target}" == strict_live && "${STORAGE_FAILURE}" == live* && "${inventory}" == 0 ]]; then
    printf 'fixture live failure\n' >&2
    exit 29
fi
if [[ "${target}" == strict_live ]]; then
    if (( inventory )); then printf "fixture_case: test\n"; else printf "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"; fi
fi
if [[ "${STORAGE_FAILURE}" == embedded* ]]; then
    if [[ "${STORAGE_FAILURE}" == embedded_failure && "${target}" == lancedb_durability_feasibility ]]; then
        printf 'fixture embedded test failure\n' >&2
        exit 23
    fi
    if [[ "${STORAGE_FAILURE}" == embedded_changed ]]; then
        printf '// changed during verification\n' >> "${STORAGE_SOURCE}"
    fi
    if [[ "${STORAGE_FAILURE}" == embedded_interrupted ]]; then
        sleep 30 &
        pending="$!"
        trap 'kill -TERM "${pending}" 2>/dev/null || true; wait "${pending}" 2>/dev/null || true; printf settled > "${STORAGE_SETTLED}"; exit 130' TERM
        printf '%s\n' "$$" > "${STORAGE_READY}"
        wait "${pending}"
    fi
    if [[ "${STORAGE_FAILURE}" == embedded_empty ]]; then
        printf 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n'
    else
        printf 'test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n'
    fi
fi
"#,
        );
        fixture.executable("docker", r#"#!/usr/bin/env bash
set -euo pipefail
printf 'docker' >> "${STORAGE_CALLS}"
printf '\t%s' "$@" >> "${STORAGE_CALLS}"
printf '\n' >> "${STORAGE_CALLS}"
if [[ "$1" != compose || "$2" != -f || "${COMPOSE_PROJECT_NAME}" != storage-sdk-strict-* ]]; then exit 91; fi
case "$4" in
    down)
        count=0
        if [[ -e "${STORAGE_DOWNS}" ]]; then read -r count < "${STORAGE_DOWNS}"; fi
        count=$((count + 1))
        printf '%s\n' "${count}" > "${STORAGE_DOWNS}"
        if [[ "${count}" == 1 && "${STORAGE_FAILURE}" == *cleanup ]]; then
            printf 'fixture cleanup failure\n' >&2
            exit 37
        fi
        ;;
    up) if [[ "${STORAGE_FAILURE}" == startup_cleanup ]]; then printf 'fixture startup failure\n' >&2; exit 31; fi;;
    images) printf 'fixture image identities\n';;
    exec)
        if [[ "${7:-}" != pg_isready ]]; then
            printf 'fixture %s native version\n' "${6}"
        fi;;
    *) exit 92 ;;
esac
"#);
        fixture.executable(
            "curl",
            r#"#!/usr/bin/env bash
set -euo pipefail
printf 'curl\n' >> "${STORAGE_CALLS}"
"#,
        );
        fixture
    }

    fn executable(&self, name: &str, contents: &str) {
        let path = self.directory.path().join("bin").join(name);
        fs::write(&path, contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    fn command(&self, failure: &str, missing: Option<&str>) -> Command {
        let original = std::env::var_os("PATH").unwrap();
        let path = std::env::join_paths(
            std::iter::once(self.directory.path().join("bin"))
                .chain(std::env::split_paths(&original)),
        )
        .unwrap();
        let mut command = Command::new("bash");
        command
            .arg(
                self.directory
                    .path()
                    .join("storage-sdk/scripts/verify_strict_live.sh"),
            )
            .args([
                "--backend",
                "surrealdb",
                "--backend",
                "qdrant",
                "--backend",
                "postgres",
            ])
            .env_clear()
            .env("PATH", path)
            .envs(Self::REQUIRED.map(|name| (name, "private-fixture-value")))
            .env("COMPOSE_PROJECT_NAME", "fixture-owned-project")
            .env("TEST_SURREALDB_REMOTE_WS", "ws://127.0.0.1:8000")
            .env("TEST_SURREALDB_REMOTE_HTTP", "http://127.0.0.1:8000")
            .env("TEST_QDRANT_REMOTE_REST", "http://127.0.0.1:6333")
            .env("TEST_POSTGRES_STATE_STORE_HOST", "127.0.0.1")
            .env("TEST_POSTGRES_STATE_STORE_PORT", "15432")
            .env("STORAGE_CALLS", self.directory.path().join("calls"))
            .env("STORAGE_DOWNS", self.directory.path().join("downs"))
            .env("STORAGE_FAILURE", failure);
        for name in [
            "TEST_LANCEDB_EMBEDDED_PATH",
            "TEST_SURREALDB_ROCKSDB_PATH",
            "TEST_SURREALDB_SURREALKV_PATH",
            "TEST_QDRANT_EDGE_PATH",
        ] {
            command.env(name, self.directory.path().join(name));
        }
        if let Some(missing) = missing {
            command.env_remove(missing);
        }
        command
    }

    fn run(&self, failure: &str, missing: Option<&str>) -> Output {
        self.command(failure, missing).output().unwrap()
    }

    fn embedded_command(&self, failure: &str) -> Command {
        let original = std::env::var_os("PATH").unwrap();
        let path = std::env::join_paths(
            std::iter::once(self.directory.path().join("bin"))
                .chain(std::env::split_paths(&original)),
        )
        .unwrap();
        let mut command = Command::new("bash");
        command
            .arg(
                self.directory
                    .path()
                    .join("storage-sdk/scripts/verify_lancedb_state_store.sh"),
            )
            .arg(self.directory.path().join("evidence"))
            .env_clear()
            .env("PATH", path)
            .env("STORAGE_CALLS", self.directory.path().join("calls"))
            .env("STORAGE_FAILURE", failure)
            .env(
                "STORAGE_SOURCE",
                self.directory.path().join("storage-sdk/src/lib.rs"),
            )
            .env("STORAGE_READY", self.directory.path().join("ready"))
            .env("STORAGE_SETTLED", self.directory.path().join("settled"));
        command
    }

    fn embedded_receipt(&self) -> serde_json::Value {
        serde_json::from_slice(
            &fs::read(self.directory.path().join("evidence/summary.json"))
                .expect("every started embedded run records a receipt"),
        )
        .expect("embedded receipt is JSON")
    }

    #[cfg(unix)]
    fn process_group_exists(group: u32) -> Result<bool, String> {
        Command::new("bash")
            .args([
                "-c",
                "kill -0 -- \"-$1\" 2>/dev/null",
                "owned-command-status",
            ])
            .arg(group.to_string())
            .status()
            .map(|status| status.success())
            .map_err(|error| format!("owned command observation failed: {error}"))
    }

    #[cfg(unix)]
    async fn wait_for_process(child: &mut Child, deadline: Instant) -> Result<ExitStatus, String> {
        loop {
            if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err("owned verifier did not exit before its observation deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[cfg(unix)]
    async fn read_request(stream: &mut TcpStream) -> Result<&'static str, String> {
        let mut request = Vec::new();
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut bytes = [0; 1024];
            let read = stream
                .read(&mut bytes)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 || request.len() + read > 16_384 {
                return Err("readiness request ended early or exceeded its header bound".into());
            }
            request.extend_from_slice(&bytes[..read]);
        }
        for path in ["/ready", "/readyz"] {
            if request.starts_with(format!("GET {path} HTTP/1.1\r\n").as_bytes()) {
                return Ok(path);
            }
        }
        Err("readiness requested an unexpected method or endpoint".into())
    }

    #[cfg(unix)]
    async fn respond_ready(stream: &mut TcpStream) -> Result<(), String> {
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .map_err(|error| error.to_string())?;
        stream.shutdown().await.map_err(|error| error.to_string())
    }

    #[cfg(unix)]
    async fn assert_readiness_timeout(&self, stall: ReadinessStall) {
        use std::os::unix::process::CommandExt;

        // Cargo and Compose still prove orchestration only. The copied script
        // invokes the real curl binary, with its arguments completely unchanged.
        fs::remove_file(self.directory.path().join("bin/curl")).unwrap();
        fs::write(self.directory.path().join(".curlrc"), "").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let stdout_path = self.directory.path().join("readiness.stdout");
        let stderr_path = self.directory.path().join("readiness.stderr");
        // command() clears inherited environment, including proxy variables.
        // CURL_HOME below isolates the real curl from caller configuration.
        let mut command = self.command("none", None);
        command
            .env("TEST_SURREALDB_REMOTE_HTTP", &base)
            .env(
                "TEST_SURREALDB_REMOTE_WS",
                format!("ws://{}", listener.local_addr().unwrap()),
            )
            .env(
                "TEST_SURREALDB_REMOTE_PORT",
                listener.local_addr().unwrap().port().to_string(),
            )
            .env("TEST_QDRANT_REMOTE_REST", &base)
            .env(
                "TEST_QDRANT_REMOTE_PORT",
                listener.local_addr().unwrap().port().to_string(),
            )
            .env("CURL_HOME", self.directory.path())
            .stdin(Stdio::null())
            .stdout(fs::File::create(&stdout_path).unwrap())
            .stderr(fs::File::create(&stderr_path).unwrap())
            .process_group(0);
        let mut child = command
            .spawn()
            .expect("start actual copied Storage verifier");
        let mut held = None;
        let mut requests = Vec::new();
        let mut stalled_request_entered = false;
        let mut peer_closed_stall = false;
        let deadline = Instant::now() + Duration::from_secs(15);
        let observed = tokio::time::timeout_at(deadline, async {
            if matches!(stall, ReadinessStall::QdrantBody) {
                let mut stream = listener
                    .accept()
                    .await
                    .map_err(|error| error.to_string())?
                    .0;
                requests.push(Self::read_request(&mut stream).await?);
                if requests.last() != Some(&"/ready") {
                    return Err("Surreal readiness must precede Qdrant readiness".into());
                }
                Self::respond_ready(&mut stream).await?;
            }
            held = Some(
                listener
                    .accept()
                    .await
                    .map_err(|error| error.to_string())?
                    .0,
            );
            let stream = held.as_mut().unwrap();
            let expected = if matches!(stall, ReadinessStall::QdrantBody) {
                "/readyz"
            } else {
                "/ready"
            };
            requests.push(Self::read_request(stream).await?);
            if requests.last() != Some(&expected) {
                return Err("the selected readiness endpoint was not requested".into());
            }
            if matches!(stall, ReadinessStall::QdrantBody) {
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{",
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
            stalled_request_entered = true;
            // Keep the server half alive. Only the real client's deadline may
            // end this attempt; a connect-only timeout cannot satisfy the case.
            if stream
                .read(&mut [0; 1])
                .await
                .map_err(|error| error.to_string())?
                != 0
            {
                return Err("unexpected bytes after the complete readiness request".into());
            }
            peer_closed_stall = true;
            let remaining = if matches!(stall, ReadinessStall::QdrantBody) {
                &["/readyz"][..]
            } else {
                &["/ready", "/readyz"][..]
            };
            for expected in remaining {
                let mut stream = listener
                    .accept()
                    .await
                    .map_err(|error| error.to_string())?
                    .0;
                requests.push(Self::read_request(&mut stream).await?);
                if requests.last() != Some(expected) {
                    return Err("timed-out readiness must retry before advancing services".into());
                }
                Self::respond_ready(&mut stream).await?;
            }
            Self::wait_for_process(&mut child, deadline).await
        })
        .await;
        let live_before_cleanup = self
            .calls()
            .iter()
            .any(|call| Self::target(call, "--test") == Some("strict_live"));

        // The baseline has no curl deadline. After capturing that failure,
        // release its held socket and serve remaining readiness requests solely
        // for cleanup. These late requests cannot satisfy the observation above.
        drop(held.take());
        let mut cleanup_requests = Vec::new();
        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
                    return Ok::<ExitStatus, String>(status);
                }
                tokio::select! {
                    accepted = listener.accept() => {
                        let mut stream = accepted.map_err(|error| error.to_string())?.0;
                        cleanup_requests.push(Self::read_request(&mut stream).await?);
                        Self::respond_ready(&mut stream).await?;
                    }
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
        })
        .await;
        drop(listener);
        let mut cleanup_errors = Vec::new();
        if !matches!(settled, Ok(Ok(_))) {
            cleanup_errors.push(format!("readiness cleanup did not settle: {settled:?}"));
            // Kill the exact owned process group, including its curl descendant,
            // then reap both direct process handles. Fallback is always a failure.
            let signal = Command::new("bash")
                .args(["-c", "kill -KILL -- \"$1\"", "verifier-cleanup"])
                .arg(format!("-{}", child.id()))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn();
            match signal {
                Ok(mut signal) => {
                    match Self::wait_for_process(
                        &mut signal,
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    {
                        Ok(status) if status.success() => {}
                        Ok(status) => cleanup_errors
                            .push(format!("process-group termination failed: {status}")),
                        Err(error) => {
                            cleanup_errors.push(error);
                            if let Err(error) = signal.kill() {
                                cleanup_errors.push(error.to_string());
                            }
                            if let Err(error) = Self::wait_for_process(
                                &mut signal,
                                Instant::now() + Duration::from_secs(2),
                            )
                            .await
                            {
                                cleanup_errors.push(error);
                            }
                        }
                    }
                }
                Err(error) => cleanup_errors.push(error.to_string()),
            }
            if let Err(error) =
                Self::wait_for_process(&mut child, Instant::now() + Duration::from_secs(3)).await
            {
                cleanup_errors.push(error);
            }
        }
        let receipt = child.try_wait();
        let stdout = fs::read_to_string(stdout_path).unwrap_or_else(|error| {
            cleanup_errors.push(format!("readiness stdout diagnostic unavailable: {error}"));
            String::new()
        });
        let stderr = fs::read_to_string(stderr_path).unwrap_or_else(|error| {
            cleanup_errors.push(format!("readiness stderr diagnostic unavailable: {error}"));
            String::new()
        });
        eprintln!(
            "readiness stall={stall:?}; requests={requests:?}; stalled_request_entered={stalled_request_entered}; peer_closed_stall={peer_closed_stall}; live_before_cleanup={live_before_cleanup}; observed={observed:?}; cleanup_requests={cleanup_requests:?}; receipt={receipt:?}; cleanup_errors={cleanup_errors:?}"
        );
        // First distinguish failure to enter the intended stall from a genuine
        // readiness failure. Cleanup diagnostics are already printed; restore
        // the primary observation before asserting secondary cleanup evidence.
        assert!(
            stalled_request_entered,
            "the intended stalled response must be reached: observed={observed:?}; stdout={stdout}; stderr={stderr}"
        );
        let status = observed
            .expect("readiness must retry before the independent watchdog")
            .expect("readiness must terminate the stalled attempt and continue");
        assert!(status.success(), "stdout={stdout}; stderr={stderr}");
        assert!(cleanup_errors.is_empty(), "{cleanup_errors:?}");
        assert!(
            matches!(receipt, Ok(Some(_))),
            "owned verifier must have an exit receipt"
        );
        self.assert_live_once();
        assert_eq!(
            fs::read_to_string(self.directory.path().join("downs"))
                .unwrap()
                .trim(),
            "1"
        );
        assert!(peer_closed_stall);
        assert!(
            live_before_cleanup,
            "live selection must occur before cleanup releases the stalled socket"
        );
        assert!(
            cleanup_requests.is_empty(),
            "cleanup cannot supply readiness success"
        );
        assert_eq!(
            requests,
            if matches!(stall, ReadinessStall::QdrantBody) {
                vec!["/ready", "/readyz", "/readyz"]
            } else {
                vec!["/ready", "/ready", "/readyz"]
            }
        );
    }

    fn calls(&self) -> Vec<Vec<String>> {
        fs::read_to_string(self.directory.path().join("calls"))
            .unwrap_or_default()
            .lines()
            .map(|line| line.split('\t').map(str::to_owned).collect())
            .collect()
    }

    fn target<'a>(call: &'a [String], flag: &str) -> Option<&'a str> {
        call.windows(2)
            .find_map(|pair| (pair[0] == flag).then_some(pair[1].as_str()))
    }

    fn assert_live_once(&self) {
        let calls = self.calls();
        let live = calls
            .iter()
            .filter(|call| {
                Self::target(call, "--test") == Some("strict_live")
                    && !call.iter().any(|argument| argument == "--list")
            })
            .collect::<Vec<_>>();
        assert!(
            !live.is_empty(),
            "selected live target must execute: {calls:?}"
        );
        for call in live {
            assert!(call.iter().any(|argument| argument == "--locked"));
            assert!(call.iter().any(|argument| argument == "--nocapture"));
            assert!(
                !Self::target(call, "--features").is_some_and(|features| features
                    .contains("sqlite")
                    && features.contains("qdrant"))
            );
        }
    }
}

#[test]
fn embedded_verifier_records_scoped_success_and_exact_targets() {
    let fixture = VerifierFixture::new();
    let output = fixture
        .embedded_command("embedded_success")
        .env("RUSTFLAGS", "-C codegen-units=8")
        .env("CARGO_TARGET_DIR", fixture.directory.path().join("target"))
        .env("RUSTC_WRAPPER", "fixture-compiler-wrapper")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let receipt = fixture.embedded_receipt();
    assert_eq!(receipt["execution"], "PASS");
    assert_eq!(receipt["suite"], "embedded_lance");
    assert!(receipt.get("prototype_qualification").is_none());
    assert_eq!(
        receipt["source"]["digest"],
        receipt["source_after"]["digest"]
    );
    assert_eq!(receipt["source"]["digest"].as_str().unwrap().len(), 64);
    assert_eq!(receipt["lockfile_sha256"].as_str().unwrap().len(), 64);
    assert!(
        receipt["compiler"]
            .as_str()
            .unwrap()
            .starts_with("rustc 1.99.0 ")
    );
    assert!(receipt["filesystem"]["status"].is_string());
    assert_eq!(
        receipt["build_environment"]["RUSTFLAGS"],
        "-C codegen-units=8"
    );
    assert_eq!(
        receipt["build_environment"]["RUSTC_WRAPPER"],
        "fixture-compiler-wrapper"
    );
    assert_eq!(
        receipt["build_environment"]["CARGO_TARGET_DIR"],
        fixture.directory.path().join("target").to_str().unwrap()
    );
    let commands = receipt["commands"].as_array().unwrap();
    let expected = [
        "lancedb_native_feasibility",
        "lancedb_durability_feasibility",
        "lancedb_state_store",
    ];
    assert_eq!(commands.len(), expected.len());
    for (command, target) in commands.iter().zip(expected) {
        assert_eq!(command["target"], target);
        assert_eq!(command["exit_code"], 0);
        assert_eq!(command["counts"]["passed"], 2);
        assert!(
            fixture
                .directory
                .path()
                .join("evidence")
                .join(command["log"].as_str().unwrap())
                .is_file()
        );
    }
    let calls = fixture.calls();
    assert_eq!(calls.len(), 3, "{calls:?}");
    for (call, target) in calls.iter().zip(expected) {
        assert_eq!(VerifierFixture::target(call, "--test"), Some(target));
        assert!(call.iter().any(|arg| arg == "--locked"));
        assert!(call.iter().any(|arg| arg == "--no-default-features"));
        assert!(!call.iter().any(|arg| arg == "--all-features"));
    }
}

#[test]
fn embedded_verifier_preserves_failure_and_completed_evidence() {
    let fixture = VerifierFixture::new();
    let output = fixture
        .embedded_command("embedded_failure")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(23), "{output:?}");
    let receipt = fixture.embedded_receipt();
    assert_eq!(receipt["execution"], "FAIL");
    let commands = receipt["commands"].as_array().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0]["exit_code"], 0);
    assert_eq!(commands[1]["exit_code"], 23);
    let log = fixture
        .directory
        .path()
        .join("evidence")
        .join(commands[1]["log"].as_str().unwrap());
    assert!(
        fs::read_to_string(log)
            .unwrap()
            .contains("fixture embedded test failure")
    );
    assert_eq!(fixture.calls().len(), 2);
}

#[test]
fn embedded_verifier_rejects_an_unqualified_compiler_before_cargo() {
    let fixture = VerifierFixture::new();
    let output = fixture
        .embedded_command("embedded_compiler")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let receipt = fixture.embedded_receipt();
    assert_eq!(receipt["execution"], "INCOMPLETE");
    assert!(
        receipt["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error.as_str().unwrap().contains("1.99.0"))
    );
    assert!(fixture.calls().is_empty());
}

#[test]
fn embedded_verifier_does_not_accept_an_empty_suite() {
    let fixture = VerifierFixture::new();
    let output = fixture.embedded_command("embedded_empty").output().unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let receipt = fixture.embedded_receipt();
    assert_eq!(receipt["execution"], "INCOMPLETE");
    assert_eq!(receipt["commands"][0]["counts"]["passed"], 0);
}

#[test]
fn embedded_verifier_reports_changed_source_instead_of_a_qualified_receipt() {
    let fixture = VerifierFixture::new();
    let output = fixture
        .embedded_command("embedded_changed")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let receipt = fixture.embedded_receipt();
    assert_eq!(receipt["execution"], "INCOMPLETE");
    assert_ne!(
        receipt["source"]["digest"],
        receipt["source_after"]["digest"]
    );
}

#[test]
fn embedded_verifier_does_not_overwrite_existing_evidence() {
    let fixture = VerifierFixture::new();
    let evidence = fixture.directory.path().join("evidence");
    fs::create_dir(&evidence).unwrap();
    fs::write(evidence.join("summary.json"), "preserve this receipt").unwrap();
    let output = fixture
        .embedded_command("embedded_success")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert_eq!(
        fs::read_to_string(evidence.join("summary.json")).unwrap(),
        "preserve this receipt"
    );
    assert!(fixture.calls().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn embedded_verifier_records_interruption_after_owned_command_settles() {
    let fixture = VerifierFixture::new();
    let mut child = fixture
        .embedded_command("embedded_interrupted")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let ready = fixture.directory.path().join("ready");
    let deadline = Instant::now() + Duration::from_secs(10);
    let started = tokio::time::timeout_at(deadline, async {
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                return Err(format!(
                    "verifier exited before command admission: {status}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await;
    let signal = Command::new("bash")
        .args(["-c", "kill -TERM -- \"$1\"", "owned-verifier-stop"])
        .arg(child.id().to_string())
        .status()
        .unwrap();
    let settled =
        VerifierFixture::wait_for_process(&mut child, Instant::now() + Duration::from_secs(40))
            .await;
    let command_group = fs::read_to_string(&ready)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok());
    let mut cleanup_errors = Vec::new();
    if let Some(group) = command_group {
        // Cargo is deliberately in a different group. Clean it independently
        // even if a regressed verifier exits before its descendants settle.
        let mut observed_remaining_group = false;
        for signal in ["-TERM", "-KILL"] {
            match VerifierFixture::process_group_exists(group) {
                Ok(false) => break,
                Ok(true) => observed_remaining_group = true,
                Err(error) => cleanup_errors.push(error),
            }
            let result = Command::new("bash")
                .args([
                    "-c",
                    "kill \"$1\" -- \"-$2\" 2>/dev/null || true",
                    "owned-command-stop",
                    signal,
                ])
                .arg(group.to_string())
                .status();
            if let Err(error) = result {
                cleanup_errors.push(format!("owned command termination failed: {error}"));
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                // Reap the direct verifier as soon as its cleanup completes.
                let _ = child.try_wait();
                match VerifierFixture::process_group_exists(group) {
                    Ok(false) => break,
                    Ok(true) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(true) => break,
                    Err(error) => {
                        cleanup_errors.push(error);
                        break;
                    }
                }
            }
        }
        if observed_remaining_group && settled.is_ok() {
            cleanup_errors.push("verifier exited before its owned command group settled".into());
        }
        match VerifierFixture::process_group_exists(group) {
            Ok(false) => {}
            Ok(true) => cleanup_errors.push("owned command group remained after cleanup".into()),
            Err(error) => cleanup_errors.push(error),
        }
    }
    if !matches!(child.try_wait(), Ok(Some(_))) {
        let _ = child.kill();
        if let Err(error) = child.wait() {
            cleanup_errors.push(format!("owned verifier reaping failed: {error}"));
        }
    }
    assert!(cleanup_errors.is_empty(), "{cleanup_errors:?}");
    started
        .expect("bounded command admission")
        .expect("command admitted");
    assert!(signal.success());
    assert_eq!(settled.expect("owned verifier joins").code(), Some(2));
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("settled")).unwrap(),
        "settled"
    );
    let receipt = fixture.embedded_receipt();
    assert_eq!(receipt["execution"], "INCOMPLETE");
    assert!(receipt["commands"][0]["exit_code"].as_i64().is_some());
}

#[test]
fn strict_verifier_runs_selected_deterministic_lanes_before_owned_services() {
    let fixture = VerifierFixture::new();
    let output = fixture.run("none", None);
    assert!(output.status.success(), "{output:?}");
    let calls = fixture.calls();
    let first_service = calls.iter().position(|call| call[0] == "docker").unwrap();
    let deterministic = &calls[..first_service];
    for target in [
        "public_api",
        "database_backend",
        "state_store_backend",
        "lancedb_native_feasibility",
        "lancedb_durability_feasibility",
        "lancedb_state_store",
        "source_contract",
        "strict_env_contract",
    ] {
        let selected = deterministic
            .iter()
            .filter(|call| VerifierFixture::target(call, "--test") == Some(target))
            .collect::<Vec<_>>();
        assert_eq!(
            selected.len(),
            4,
            "default and three selected alternatives: {calls:?}"
        );
        let mut features = selected
            .iter()
            .map(|call| VerifierFixture::target(call, "--features"))
            .collect::<Vec<_>>();
        features.sort();
        assert_eq!(
            features,
            [None, Some("postgres"), Some("qdrant"), Some("surrealdb")]
        );
    }
    fixture.assert_live_once();
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("downs"))
            .unwrap()
            .trim(),
        "1"
    );
}

#[test]
fn strict_verifier_script_contract_failure_never_mutates_services() {
    let fixture = VerifierFixture::new();
    let output = fixture.run("script_contract", None);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        fixture
            .directory
            .path()
            .join("script-contract-ran")
            .exists()
    );
    assert!(fixture.calls().is_empty());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("execution PASS"));
}

#[test]
fn strict_verifier_deterministic_failure_never_mutates_services() {
    let fixture = VerifierFixture::new();
    let output = fixture.run("deterministic", None);
    assert_eq!(output.status.code(), Some(23), "{output:?}");
    let calls = fixture.calls();
    assert!(
        calls
            .iter()
            .any(|call| VerifierFixture::target(call, "--test") == Some("database_backend"))
    );
    assert!(!calls.iter().any(|call| call[0] != "cargo"));
    assert!(
        !calls
            .iter()
            .any(|call| VerifierFixture::target(call, "--test") == Some("strict_live"))
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("fixture deterministic failure"));
}

#[test]
fn strict_verifier_missing_environment_has_no_cleanup_authority() {
    for missing in VerifierFixture::REQUIRED.iter().filter(|name| {
        !matches!(
            **name,
            "COMPOSE_PROJECT_NAME" | "TEST_LANCEDB_EMBEDDED_PATH"
        )
    }) {
        let fixture = VerifierFixture::new();
        let output = fixture.run("none", Some(missing));
        assert!(!output.status.success());
        assert!(
            fixture.calls().is_empty(),
            "{missing} must fail before any command"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(missing));
        assert!(!stderr.contains("private-fixture-value"));
    }
}

#[test]
fn strict_verifier_cleanup_failure_cannot_report_success() {
    let fixture = VerifierFixture::new();
    let output = fixture.run("cleanup", None);
    fixture.assert_live_once();
    assert_eq!(output.status.code(), Some(37), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("fixture cleanup failure"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("execution PASS"));
}

#[test]
fn strict_verifier_cleanup_failure_preserves_primary_live_failure() {
    let fixture = VerifierFixture::new();
    let output = fixture.run("live_cleanup", None);
    fixture.assert_live_once();
    assert_eq!(output.status.code(), Some(29), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("fixture live failure"));
    assert!(stderr.contains("fixture cleanup failure"));
}

#[test]
fn strict_verifier_failed_startup_still_reports_its_cleanup_failure() {
    let fixture = VerifierFixture::new();
    let output = fixture.run("startup_cleanup", None);
    assert_eq!(output.status.code(), Some(31), "{output:?}");
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("downs"))
            .unwrap()
            .trim(),
        "1"
    );
    assert!(
        !fixture
            .calls()
            .iter()
            .any(|call| VerifierFixture::target(call, "--test") == Some("strict_live"))
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("fixture startup failure"));
    assert!(stderr.contains("fixture cleanup failure"));
}

#[test]
fn strict_verifier_successful_cleanup_preserves_live_failure() {
    let fixture = VerifierFixture::new();
    let output = fixture.run("live", None);
    fixture.assert_live_once();
    assert_eq!(output.status.code(), Some(29), "{output:?}");
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("downs"))
            .unwrap()
            .trim(),
        "1"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("fixture live failure"));
}

#[cfg(unix)]
#[tokio::test]
async fn strict_verifier_bounds_stalled_surreal_headers() {
    VerifierFixture::new()
        .assert_readiness_timeout(ReadinessStall::SurrealHeaders)
        .await;
}

#[cfg(unix)]
#[tokio::test]
async fn strict_verifier_bounds_stalled_qdrant_body() {
    VerifierFixture::new()
        .assert_readiness_timeout(ReadinessStall::QdrantBody)
        .await;
}

#[test]
fn strict_verifier_exhausted_readiness_fails_and_cleans_owned_services() {
    let fixture = VerifierFixture::new();
    // Fast command failures prove attempt selection and cleanup only; the
    // stalled-response cases above exercise real curl deadline behavior.
    fixture.executable(
        "curl",
        "#!/usr/bin/env bash\nprintf 'curl\\n' >> \"${STORAGE_CALLS}\"\nexit 22\n",
    );
    fixture.executable("sleep", "#!/usr/bin/env bash\nexit 0\n");
    let output = fixture.run("none", None);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let calls = fixture.calls();
    assert_eq!(calls.iter().filter(|call| call[0] == "curl").count(), 60);
    assert!(
        !calls
            .iter()
            .any(|call| VerifierFixture::target(call, "--test") == Some("strict_live"))
    );
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("downs"))
            .unwrap()
            .trim(),
        "1"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("SurrealDB did not become ready"));
}

#[test]
fn default_strict_verifier_needs_no_alternative_environment_or_services() {
    let fixture = VerifierFixture::new();
    let selected = fixture.command("none", None);
    let mut command = Command::new("bash");
    command.arg(
        fixture
            .directory
            .path()
            .join("storage-sdk/scripts/verify_strict_live.sh"),
    );
    command.env_clear();
    for (name, value) in selected.get_envs() {
        if let Some(value) = value
            && !name.to_string_lossy().starts_with("TEST_")
        {
            command.env(name, value);
        }
    }
    let output = command.output().unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(fixture.calls().iter().all(|call| call[0] == "cargo"));
    fixture.assert_live_once();
}
