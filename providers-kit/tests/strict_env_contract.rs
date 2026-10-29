use std::fs;
use std::path::PathBuf;
#[cfg(unix)]
use std::process::{Child, ExitStatus, Stdio};
use std::process::{Command, Output};
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::time::Instant;

use tempfile::TempDir;

struct VerifierFixture {
    directory: TempDir,
}

impl VerifierFixture {
    const REQUIRED: [&'static str; 22] = [
        "TEST_OPENAI_API_KEY",
        "TEST_OPENAI_BASE_URL",
        "TEST_OPENAI_MODEL_DEFAULT",
        "TEST_OPENAI_MODEL_EMBEDDINGS",
        "TEST_OPENAI_MODEL_INTERACTIVE",
        "TEST_OPENAI_FILE_SEARCH_STORE_ID",
        "TEST_ANTHROPIC_API_KEY",
        "TEST_ANTHROPIC_BASE_URL",
        "TEST_ANTHROPIC_MODEL_DEFAULT",
        "TEST_ANTHROPIC_MODEL_THINKING",
        "TEST_GEMINI_API_KEY",
        "TEST_GEMINI_BASE_URL",
        "TEST_GEMINI_MODEL_DEFAULT",
        "TEST_GEMINI_MODEL_EMBEDDINGS",
        "TEST_GEMINI_MODEL_THINKING_LEVEL",
        "TEST_GEMINI_SERVICE_TIER",
        "TEST_GEMINI_FILE_SEARCH_STORE_ID",
        "TEST_GEMINI_MODEL_FILE_SEARCH",
        "TEST_OLLAMA_BASE_URL",
        "TEST_OLLAMA_MODEL_DEFAULT",
        "TEST_OLLAMA_MODEL_REASONING",
        "TEST_OLLAMA_MODEL_EMBEDDINGS",
    ];

    const LONG_CASES: [&'static str; 7] = [
        "live::batches::openai_inline_batch_completes",
        "live::batches::openai_provider_file_batch_completes",
        "live::batches::openai_batch_preserves_mixed_row_outcomes",
        "live::batches::openai_batch_result_limit_is_enforced",
        "live::batches::anthropic_batch_preserves_mixed_row_outcomes",
        "live::batches::anthropic_batch_result_limit_is_enforced",
        "live::batches::anthropic_inline_batch_completes",
    ];

    const WORKSPACE_CRATES: [&'static str; 5] = [
        "agents-kit",
        "providers-kit",
        "storage-kit",
        "tools-kit",
        "app-kit",
    ];

    fn new() -> Self {
        let fixture = Self {
            directory: tempfile::tempdir().expect("temporary verifier workspace"),
        };
        fs::create_dir_all(fixture.directory.path().join("providers-kit/scripts"))
            .expect("temporary scripts directory");
        fs::create_dir(fixture.directory.path().join("bin")).expect("temporary command directory");
        fs::create_dir(fixture.directory.path().join("scripts"))
            .expect("temporary workspace scripts directory");
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("scripts/release_stage.sh"),
            fixture.directory.path().join("scripts/release_stage.sh"),
        )
        .expect("copy release stage forwarding helper");
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/verify_strict_live.sh"),
            fixture
                .directory
                .path()
                .join("providers-kit/scripts/verify_strict_live.sh"),
        )
        .expect("copy actual verifier for process-level orchestration checks");
        // These commands record orchestration only; they cannot prove provider behavior.
        fixture.write_executable(
            "providers-kit/scripts/check.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\nprintf 'check\\n' >> \"${VERIFIER_CALLS}\"\n",
        );
        fixture.write_executable(
            "bin/curl",
            r#"#!/usr/bin/env bash
set -euo pipefail
printf 'curl\n' >> "${VERIFIER_CALLS}"
case "${@: -1}" in
    https://ollama.invalid/api/version) printf '{"version":"fixture"}\n' ;;
    https://ollama.invalid/api/tags) printf '{"models":[{"name":"private-dummy-value"},{"name":"gemma4:e4b-it-qat"},{"name":"qwen3.5:9b-q4_K_M"}]}\n' ;;
    *) exit 91 ;;
esac
"#,
        );
        fixture.write_executable(
            "bin/cargo",
            r#"#!/usr/bin/env bash
set -euo pipefail
printf 'cargo\n' >> "${VERIFIER_CALLS}"
printf '%s\n' "$@" >> "${VERIFIER_CARGO_ARGS}"
if [[ "${1-}" == clippy && -n "${VERIFIER_WORKSPACE_CLIPPY-}" ]]; then
    printf '%s\n' "$*" >> "${VERIFIER_WORKSPACE_CLIPPY}"
fi
if [[ -n "${VERIFIER_CARGO_OUTPUT-}" ]]; then
    cat "${VERIFIER_CARGO_OUTPUT}"
fi
if [[ -n "${VERIFIER_EVIDENCE_METADATA-}" ]]; then
    python3 - "${VERIFIER_EVIDENCE_METADATA}" <<'PY'
import json, os, pathlib, stat, sys

configured = os.environ.get("PROVIDERS_KIT_EVIDENCE_DIR")
directory = pathlib.Path(configured) if configured else None
metadata = {"configured": configured, "exists": directory is not None and directory.is_dir()}
if metadata["exists"]:
    metadata["directory_mode"] = stat.S_IMODE(directory.stat().st_mode)
    evidence = directory / "fixture-case.json"
    descriptor = os.open(evidence, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump({"fixture": "command orchestration only, not a provider response"}, output)
    metadata["evidence"] = str(evidence)
    metadata["file_mode"] = stat.S_IMODE(evidence.stat().st_mode)
pathlib.Path(sys.argv[1]).write_text(json.dumps(metadata))
PY
fi
exit "${VERIFIER_CARGO_EXIT}"
"#,
        );
        fixture.write_realtime_grades(&Self::realtime_grades());
        fixture
    }

    fn realtime_grades() -> [serde_json::Value; 2] {
        [
            serde_json::json!({
                "version": 1,
                "case": "live::resources::openai_interactive_session",
                "model_id": "private-dummy-value",
                "configured_model": "private-dummy-value",
                "sdk_contract": true,
                "model_exact_copy": {"function_turn": false, "audio_turn": true}
            }),
            serde_json::json!({
                "version": 1,
                "case": "live::resources::openai_interactive_cancellation_and_subsequent_progress",
                "model_id": "private-dummy-value",
                "configured_model": "private-dummy-value",
                "sdk_contract": true,
                "model_exact_copy": {"post_cancellation_function_turn": false}
            }),
        ]
    }

    fn write_realtime_grades(&self, grades: &[serde_json::Value]) {
        let output = grades
            .iter()
            .map(|grade| format!("PROVIDERS_KIT_REALTIME_GRADE={grade}\n"))
            .collect::<String>();
        fs::write(self.directory.path().join("cargo-output"), output).unwrap();
    }

    fn workspace() -> Self {
        let fixture = Self::new();
        fs::create_dir_all(fixture.directory.path().join("scripts")).unwrap();
        fs::create_dir_all(fixture.directory.path().join("scripts/tests")).unwrap();
        for name in [
            "test_release_stage_hooks.py",
            "test_release_evidence.py",
            "test_release_qualification.py",
            "test_import_release_evidence.py",
        ] {
            fs::write(
                fixture.directory.path().join("scripts/tests").join(name),
                r#"import pathlib, unittest
class RunnerFixture(unittest.TestCase):
    def test_forwarding_helper_is_available(self):
        root = pathlib.Path(__file__).resolve().parents[2]
        self.assertTrue((root / 'scripts/release_stage.sh').is_file())
"#,
            )
            .unwrap();
        }
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_owned();
        fs::copy(
            workspace.join("scripts/verify_workspace_strict.sh"),
            fixture
                .directory
                .path()
                .join("scripts/verify_workspace_strict.sh"),
        )
        .unwrap();
        for name in [
            "check_no_path_wiring.sh",
            "check_no_type_aliases.sh",
            "check_docs_site.sh",
        ] {
            fixture.write_executable(&format!("scripts/{name}"),
                "#!/usr/bin/env bash\nset -euo pipefail\nprintf 'workspace-check\\n' >> \"${VERIFIER_CALLS}\"\n");
        }
        for name in Self::WORKSPACE_CRATES {
            fs::create_dir_all(fixture.directory.path().join(name).join("scripts")).unwrap();
            fs::write(
                fixture.directory.path().join(name).join("strict-tests.env"),
                format!(
                    "printf '{name}-env\\n' >> \"${{VERIFIER_ENV_CALLS}}\"\nexport VERIFIER_CRATE_ENV='{name}'\n"
                ),
            )
            .unwrap();
            fixture.write_executable(
                &format!("{name}/scripts/verify_strict_live.sh"),
                &format!(
                    r#"#!/usr/bin/env bash
set -euo pipefail
printf '{name}\n' >> "${{VERIFIER_CALLS}}"
printf '{name}|%s|%s|%s\n' "${{PROVIDERS_KIT_RUN_LONG_BATCHES-unset}}" "$#" "${{VERIFIER_CRATE_ENV-unset}}" >> "${{VERIFIER_GATE_ENV}}"
if [[ "${{VERIFIER_FAILED_CRATE}}" == '{name}' ]]; then
    exit "${{VERIFIER_GATE_EXIT}}"
fi
"#
                ),
            );
        }
        fs::copy(
            workspace.join("storage-kit/scripts/feature_policy.py"),
            fixture
                .directory
                .path()
                .join("storage-kit/scripts/feature_policy.py"),
        )
        .unwrap();
        fixture
    }

    fn run_workspace(
        &self,
        arguments: &[&str],
        inherited: Option<&str>,
        configured: Option<&str>,
        failed_crate: Option<&str>,
        gate_exit: u8,
    ) -> Output {
        let provider_setting = configured
            .map(|value| format!("export PROVIDERS_KIT_RUN_LONG_BATCHES='{value}'\n"))
            .unwrap_or_default();
        fs::write(
            self.directory.path().join("providers-kit/strict-tests.env"),
            format!(
                "printf 'provider-env\\n' >> \"${{VERIFIER_ENV_CALLS}}\"\nexport VERIFIER_CRATE_ENV='providers-kit'\n{provider_setting}"
            ),
        )
        .unwrap();
        let original_path = std::env::var_os("PATH").unwrap();
        let path = std::env::join_paths(
            std::iter::once(self.directory.path().join("bin"))
                .chain(std::env::split_paths(&original_path)),
        )
        .unwrap();
        let mut command = Command::new("bash");
        command
            .arg(
                self.directory
                    .path()
                    .join("scripts/verify_workspace_strict.sh"),
            )
            .args(arguments)
            .env_clear()
            .env("PATH", path)
            .env("VERIFIER_CALLS", self.directory.path().join("calls"))
            .env(
                "VERIFIER_CARGO_ARGS",
                self.directory.path().join("cargo-args"),
            )
            .env("VERIFIER_CARGO_EXIT", "0")
            .env(
                "VERIFIER_WORKSPACE_CLIPPY",
                self.directory.path().join("workspace-clippy"),
            )
            .env(
                "VERIFIER_ENV_CALLS",
                self.directory.path().join("env-calls"),
            )
            .env("VERIFIER_GATE_ENV", self.directory.path().join("gate-env"))
            .env("VERIFIER_FAILED_CRATE", failed_crate.unwrap_or(""))
            .env("VERIFIER_GATE_EXIT", gate_exit.to_string());
        if let Some(value) = inherited {
            command.env("PROVIDERS_KIT_RUN_LONG_BATCHES", value);
        }
        command.output().unwrap()
    }

    fn actual_storage_gate(&self) {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_owned();
        for name in ["check.sh", "feature_policy.py", "verify_strict_live.sh"] {
            fs::copy(
                workspace.join("storage-kit/scripts").join(name),
                self.directory.path().join("storage-kit/scripts").join(name),
            )
            .unwrap();
        }
        fs::create_dir_all(self.directory.path().join("storage-kit/scripts/tests")).unwrap();
        // Validate the copied selector through actual unittest discovery without
        // recursively running the repository's complete script-contract suite.
        fs::write(
            self.directory.path().join("storage-kit/scripts/tests/test_fixture_contract.py"),
            r#"import importlib.util, pathlib, unittest
class FixtureContract(unittest.TestCase):
    def test_copied_selector_default(self):
        root = pathlib.Path(__file__).resolve().parents[3]
        spec = importlib.util.spec_from_file_location('fixture_policy', root / 'storage-kit/scripts/feature_policy.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.StorageSelection(root).result()['backends'], ['lance'])
        (root / 'script-contract-ran').write_text('executed')
"#,
        ).unwrap();
        // Use the tracked disposable example, never the user's credentials.
        fs::copy(
            workspace.join("storage-kit/strict-tests.env.example"),
            self.directory.path().join("storage-kit/strict-tests.env"),
        )
        .unwrap();
        self.write_executable(
            "bin/cargo",
            r#"#!/usr/bin/env bash
set -euo pipefail
printf 'cargo\n' >> "${VERIFIER_CALLS}"
while (( $# > 0 )); do
    if [[ "$1" == --test && "$2" == database_backend ]]; then
        printf 'storage-deterministic\n' >> "${VERIFIER_CALLS}"
        printf 'fixture Storage deterministic failure\n' >&2
        exit 23
    fi
    shift
done
"#,
        );
        self.write_executable(
            "bin/docker",
            r#"#!/usr/bin/env bash
set -euo pipefail
printf 'unexpected-docker\n' >> "${VERIFIER_CALLS}"
exit 91
"#,
        );
    }

    fn write_executable(&self, relative: &str, content: &str) {
        let path = self.directory.path().join(relative);
        fs::write(&path, content).expect("write orchestration command fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .expect("make orchestration command executable");
        }
    }

    fn command(&self, long_batches: Option<&str>, cargo_exit: u8) -> Command {
        let original_path = std::env::var_os("PATH").expect("test process PATH");
        let path = std::env::join_paths(
            std::iter::once(self.directory.path().join("bin"))
                .chain(std::env::split_paths(&original_path)),
        )
        .expect("fixture command PATH");
        let mut command = Command::new("bash");
        command
            .arg(
                self.directory
                    .path()
                    .join("providers-kit/scripts/verify_strict_live.sh"),
            )
            .env_clear()
            .env("PATH", path)
            .envs(Self::REQUIRED.map(|name| (name, "private-dummy-value")))
            .env(
                "TEST_OPENAI_FILE_SEARCH_STORE_ID",
                "vs_private-resource-value",
            )
            .env(
                "TEST_GEMINI_FILE_SEARCH_STORE_ID",
                "fileSearchStores/private-resource-value",
            )
            .env("TEST_OLLAMA_BASE_URL", "https://ollama.invalid/api")
            .env("VERIFIER_CALLS", self.directory.path().join("calls"))
            .env(
                "VERIFIER_CARGO_ARGS",
                self.directory.path().join("cargo-args"),
            )
            .env(
                "VERIFIER_CARGO_OUTPUT",
                self.directory.path().join("cargo-output"),
            )
            .env("VERIFIER_CARGO_EXIT", cargo_exit.to_string());
        if let Some(value) = long_batches {
            command.env("PROVIDERS_KIT_RUN_LONG_BATCHES", value);
        }
        command
    }

    fn run(&self, long_batches: Option<&str>, cargo_exit: u8) -> Output {
        self.command(long_batches, cargo_exit)
            .output()
            .expect("run verifier orchestration fixture")
    }

    #[cfg(unix)]
    async fn wait_for_process(child: &mut Child, deadline: Instant) -> Result<ExitStatus, String> {
        loop {
            if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(
                    "owned verifier process did not exit within its observation bound".into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[cfg(unix)]
    async fn read_request(stream: &mut TcpStream, path: &str) -> Result<(), String> {
        let mut request = Vec::new();
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut bytes = [0; 1024];
            let read = stream
                .read(&mut bytes)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 || request.len() + read > 16_384 {
                return Err("preflight request ended early or exceeded its header bound".into());
            }
            request.extend_from_slice(&bytes[..read]);
        }
        if !request.starts_with(format!("GET {path} HTTP/1.1\r\n").as_bytes()) {
            return Err("preflight requested an unexpected method or endpoint".into());
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn assert_preflight_timeout(&self, stall_tags: bool) {
        use std::os::unix::process::CommandExt;

        // Only orchestration prerequisites are substituted. The curl executable
        // is real, and neither its arguments nor the copied verifier are changed.
        fs::remove_file(self.directory.path().join("bin/curl")).unwrap();
        fs::write(self.directory.path().join(".curlrc"), "").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/api", listener.local_addr().unwrap());
        let stdout_path = self.directory.path().join("preflight.stdout");
        let stderr_path = self.directory.path().join("preflight.stderr");
        let mut command = self.command(Some("0"), 0);
        command
            .env("TEST_OLLAMA_BASE_URL", base)
            .env("CURL_HOME", self.directory.path())
            .stdout(fs::File::create(&stdout_path).unwrap())
            .stderr(fs::File::create(&stderr_path).unwrap())
            .stdin(Stdio::null())
            .process_group(0);
        let mut child = command.spawn().expect("start the actual verifier");
        let mut held = None;
        let mut requests = Vec::new();
        let observation_deadline = Instant::now() + Duration::from_secs(15);
        let observed = tokio::time::timeout_at(observation_deadline, async {
            held = Some(listener.accept().await.map_err(|error| error.to_string())?.0);
            let stream = held.as_mut().unwrap();
            Self::read_request(stream, "/api/version").await?;
            requests.push("/api/version");
            if stall_tags {
                let body = r#"{"version":"fixture"}"#;
                stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                ).as_bytes()).await.map_err(|error| error.to_string())?;
                stream.shutdown().await.map_err(|error| error.to_string())?;
                drop(held.take());
                held = Some(listener.accept().await.map_err(|error| error.to_string())?.0);
                let stream = held.as_mut().unwrap();
                Self::read_request(stream, "/api/tags").await?;
                requests.push("/api/tags");
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"models\":").await.map_err(|error| error.to_string())?;
            }
            // Keep the accepted connection alive. A connect-only deadline cannot
            // satisfy either the stalled-header or the incomplete-body case.
            Self::wait_for_process(&mut child, observation_deadline).await
        }).await;

        // Assertions are restored only after the socket and direct process owner
        // settle. Closing a stalled socket also lets the unbounded baseline curl
        // return EOF instead of leaving it behind when the watchdog expires.
        drop(held.take());
        drop(listener);
        let mut cleanup_errors = Vec::new();
        if let Err(error) =
            Self::wait_for_process(&mut child, Instant::now() + Duration::from_secs(3)).await
        {
            cleanup_errors.push(error);
            // The verifier was launched in its own group. Terminate its shell and
            // any curl descendant together; never kill only the waiting shell.
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
        let stdout = fs::read_to_string(stdout_path).unwrap();
        let stderr = fs::read_to_string(stderr_path).unwrap();
        let cleanup_receipt = child.try_wait();
        eprintln!(
            "preflight stall={} requests={requests:?} observation={observed:?} cleanup_receipt={cleanup_receipt:?} cleanup_errors={cleanup_errors:?}",
            if stall_tags {
                "tags_body"
            } else {
                "version_headers"
            },
        );
        assert!(
            cleanup_errors.is_empty(),
            "{cleanup_errors:?}; observation={observed:?}"
        );
        assert!(
            matches!(cleanup_receipt, Ok(Some(_))),
            "verifier cleanup must have an actual exit receipt"
        );
        // Establish the selected stalled request and fail-fast boundary before
        // restoring a watchdog error from the unchanged-script red baseline.
        assert_eq!(
            requests,
            if stall_tags {
                vec!["/api/version", "/api/tags"]
            } else {
                vec!["/api/version"]
            }
        );
        assert_eq!(
            fs::read_to_string(self.directory.path().join("calls")).unwrap(),
            "check\n"
        );
        assert!(!self.directory.path().join("cargo-args").exists());
        assert!(!stdout.contains("strict-live selection passed"));
        let status = observed
            .expect("Ollama preflight must terminate before the independent safety watchdog")
            .expect("the exact prerequisite request must complete with a bounded failure");
        assert_eq!(status.code(), Some(28), "stdout={stdout}; stderr={stderr}");
    }

    fn cargo_args(&self) -> Vec<String> {
        fs::read_to_string(self.directory.path().join("cargo-args"))
            .expect("verifier must invoke Cargo")
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn assert_prerequisites_and_single_cargo(&self) {
        let calls = fs::read_to_string(self.directory.path().join("calls"))
            .expect("verifier command dispatches");
        assert_eq!(calls.lines().filter(|call| *call == "check").count(), 1);
        assert_eq!(calls.lines().filter(|call| *call == "curl").count(), 2);
        assert_eq!(calls.lines().filter(|call| *call == "cargo").count(), 1);
    }

    fn expected_cargo_args(long_batches: bool) -> Vec<&'static str> {
        let mut args = vec![
            "test",
            "--locked",
            "-p",
            "providers-kit",
            "--test",
            "strict_live",
            "--all-features",
            "--",
            "--nocapture",
            "--test-threads=1",
        ];
        if !long_batches {
            args.push("--exact");
            for name in Self::LONG_CASES {
                args.extend(["--skip", name]);
            }
        }
        args
    }

    fn assert_workspace_gates(&self, inherited: Option<&str>, provider_mode: Option<&str>) {
        let clippy = fs::read_to_string(self.directory.path().join("workspace-clippy"))
            .expect("workspace Clippy records actual command arguments");
        let storage = clippy
            .lines()
            .filter(|line| line.starts_with("clippy --locked -p storage-kit "))
            .collect::<Vec<_>>();
        let expected = [
            "", "redb", "postgres", "surrealdb", "sqlite",
            "redb,postgres,surrealdb,sqlite",
        ].map(|features| {
            let features = if features.is_empty() {
                String::new()
            } else {
                format!(" --features {features}")
            };
            format!("clippy --locked -p storage-kit --all-targets --no-default-features{features} -- -D warnings")
        });
        assert_eq!(storage, expected);
        let observed = fs::read_to_string(self.directory.path().join("gate-env"))
            .expect("every workspace gate records its environment and arguments");
        let expected = Self::WORKSPACE_CRATES.map(|name| {
            let mode = if name == "providers-kit" {
                provider_mode
            } else {
                inherited
            };
            let environment = if name == "storage-kit" { "unset" } else { name };
            format!("{name}|{}|0|{environment}", mode.unwrap_or("unset"))
        });
        assert_eq!(observed.lines().collect::<Vec<_>>(), expected);
        let environments = fs::read_to_string(self.directory.path().join("env-calls"))
            .expect("workspace configuration sourcing remains observable");
        assert_eq!(
            environments.lines().collect::<Vec<_>>(),
            [
                "provider-env",
                "agents-kit-env",
                "provider-env",
                "tools-kit-env",
                "app-kit-env",
            ],
            "default Storage needs no service environment; every other crate keeps its own configuration"
        );
    }
}

#[test]
fn strict_live_verifier_defaults_to_full_and_accepts_explicit_full() {
    for flag in [None, Some("1")] {
        let fixture = VerifierFixture::new();
        let output = fixture.run(flag, 0);
        assert_eq!(output.status.code(), Some(0));
        fixture.assert_prerequisites_and_single_cargo();
        assert_eq!(
            fixture.cargo_args(),
            VerifierFixture::expected_cargo_args(true)
        );
        let stdout = String::from_utf8(output.stdout).expect("verifier stdout");
        assert!(stdout.contains("FULL"), "full coverage must be identified");
        assert!(!stdout.contains("PARTIAL"));
    }
}

#[test]
fn strict_live_verifier_partial_mode_omits_only_seven_named_completion_cases() {
    let fixture = VerifierFixture::new();
    let output = fixture.run(Some("0"), 0);
    assert_eq!(output.status.code(), Some(0));
    fixture.assert_prerequisites_and_single_cargo();
    assert_eq!(
        fixture.cargo_args(),
        VerifierFixture::expected_cargo_args(false)
    );
    let stdout = String::from_utf8(output.stdout).expect("verifier stdout");
    assert!(stdout.contains("PARTIAL"), "omissions must be conspicuous");
    assert!(!stdout.contains("FULL"));
    for name in VerifierFixture::LONG_CASES {
        assert!(
            stdout.contains(name),
            "the omitted case must be named: {name}"
        );
    }
}

#[test]
fn strict_live_verifier_invalid_long_batch_flag_fails_before_commands_and_is_redacted() {
    for flag in [
        "",
        "true",
        "2",
        "private-invalid-toggle",
        "0\nprivate-invalid-toggle",
    ] {
        let fixture = VerifierFixture::new();
        let output = fixture.run(Some(flag), 0);
        assert_eq!(output.status.code(), Some(1), "invalid toggle must fail");
        assert!(
            !fixture.directory.path().join("calls").exists(),
            "invalid toggle must fail before prerequisite or live commands"
        );
        let stderr = String::from_utf8(output.stderr).expect("verifier stderr");
        assert!(stderr.contains("PROVIDERS_KIT_RUN_LONG_BATCHES"));
        assert!(stderr.contains("0") && stderr.contains("1"));
        let stdout = String::from_utf8(output.stdout).expect("verifier stdout");
        for private_value in [
            "private-invalid-toggle",
            "private-dummy-value",
            "private-resource-value",
        ] {
            assert!(!stdout.contains(private_value));
            assert!(!stderr.contains(private_value));
        }
    }
}

#[test]
fn strict_live_verifier_propagates_test_failure_in_full_and_partial_modes() {
    for flag in ["0", "1"] {
        let fixture = VerifierFixture::new();
        let output = fixture.run(Some(flag), 7);
        assert_eq!(output.status.code(), Some(7));
        fixture.assert_prerequisites_and_single_cargo();
    }
}

#[test]
fn strict_live_verifier_realtime_false_copy_grades_are_nonblocking() {
    for mode in ["0", "1"] {
        let fixture = VerifierFixture::new();
        let output = fixture.run(Some(mode), 0);
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        fixture.assert_prerequisites_and_single_cargo();
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(stdout.matches("PROVIDERS_KIT_REALTIME_GRADE=").count(), 2);
        assert!(stdout.contains("\"function_turn\":false"));
        assert!(stdout.contains("\"post_cancellation_function_turn\":false"));
    }
}

#[test]
fn strict_live_verifier_realtime_requires_both_anchored_grade_rows() {
    for mode in ["0", "1"] {
        for output in [
            String::new(),
            format!(
                "PROVIDERS_KIT_REALTIME_GRADE={}\n",
                VerifierFixture::realtime_grades()[0]
            ),
            VerifierFixture::realtime_grades()
                .iter()
                .map(|grade| format!("not-anchored PROVIDERS_KIT_REALTIME_GRADE={grade}\n"))
                .collect(),
        ] {
            let fixture = VerifierFixture::new();
            fs::write(fixture.directory.path().join("cargo-output"), output).unwrap();
            let output = fixture.run(Some(mode), 0);
            assert!(
                !output.status.success(),
                "missing receipts must fail: {output:?}"
            );
            fixture.assert_prerequisites_and_single_cargo();
            assert!(
                !String::from_utf8(output.stdout)
                    .unwrap()
                    .contains("strict-live selection passed")
            );
        }
    }
}

#[test]
fn strict_live_verifier_realtime_rejects_invalid_receipts_without_echoing_them() {
    let base = VerifierFixture::realtime_grades();
    let mut invalid = Vec::new();
    for (field, value) in [
        ("version", serde_json::json!(2)),
        ("version", serde_json::json!(true)),
        ("case", serde_json::json!("private-invalid-grade")),
        ("model_id", serde_json::json!("private-invalid-grade")),
        (
            "configured_model",
            serde_json::json!("private-invalid-grade"),
        ),
        ("sdk_contract", serde_json::json!(false)),
        ("sdk_contract", serde_json::json!(1)),
        (
            "model_exact_copy",
            serde_json::json!({"function_turn": false}),
        ),
        (
            "model_exact_copy",
            serde_json::json!({"function_turn": 0, "audio_turn": true}),
        ),
        (
            "model_exact_copy",
            serde_json::json!({"function_turn": false, "audio_turn": true, "unknown": false}),
        ),
        ("unknown", serde_json::json!("private-invalid-grade")),
    ] {
        let mut grades = base.clone();
        grades[0][field] = value;
        invalid.push(grades.to_vec());
    }
    let mut missing_field = base.clone();
    missing_field[0].as_object_mut().unwrap().remove("model_id");
    invalid.push(missing_field.to_vec());
    invalid.push(vec![base[0].clone(), base[0].clone(), base[1].clone()]);
    for grades in invalid {
        let fixture = VerifierFixture::new();
        fixture.write_realtime_grades(&grades);
        let output = fixture.run(None, 0);
        assert!(
            !output.status.success(),
            "invalid receipts must fail: {grades:?}"
        );
        assert!(
            !String::from_utf8(output.stderr)
                .unwrap()
                .contains("private-invalid-grade")
        );
    }
    for malformed in [
        "{private-invalid-grade}".to_owned(),
        base[0]
            .to_string()
            .replacen("\"version\":1", "\"version\":1,\"version\":1", 1),
        format!("{{\"private-invalid-grade\":\"{}\"}}", "x".repeat(65_536)),
    ] {
        let fixture = VerifierFixture::new();
        fs::write(
            fixture.directory.path().join("cargo-output"),
            format!(
                "PROVIDERS_KIT_REALTIME_GRADE={malformed}\nPROVIDERS_KIT_REALTIME_GRADE={}\n",
                base[1]
            ),
        )
        .unwrap();
        let output = fixture.run(None, 0);
        assert!(!output.status.success(), "malformed receipt must fail");
        assert!(
            !String::from_utf8(output.stderr)
                .unwrap()
                .contains("private-invalid-grade")
        );
    }
}

#[cfg(unix)]
#[test]
fn strict_live_verifier_realtime_log_failure_preserves_cargo_failure() {
    for cargo_exit in [0, 23] {
        let fixture = VerifierFixture::new();
        fixture.write_realtime_grades(&[]);
        fixture.write_executable(
            "bin/tee",
            r#"#!/usr/bin/env bash
set -euo pipefail
# A real tee open/write failure; consume the complete pipeline input normally.
exec /usr/bin/tee "${VERIFIER_LOG_FAILURE}"
"#,
        );
        let output = fixture
            .command(None, cargo_exit)
            .env("VERIFIER_LOG_FAILURE", fixture.directory.path())
            .output()
            .unwrap();
        if cargo_exit == 0 {
            assert!(!output.status.success(), "a failed capture cannot qualify");
        } else {
            assert_eq!(
                output.status.code(),
                Some(i32::from(cargo_exit)),
                "{output:?}"
            );
        }
        fixture.assert_prerequisites_and_single_cargo();
        assert!(
            !String::from_utf8(output.stdout)
                .unwrap()
                .contains("strict-live selection passed")
        );
    }
}

#[cfg(unix)]
#[test]
fn strict_live_verifier_realtime_capture_is_private_and_retained_after_exit() {
    for cargo_exit in [0, 23] {
        let fixture = VerifierFixture::new();
        fixture.write_executable(
            "bin/tee",
            r#"#!/usr/bin/env bash
set -euo pipefail
python3 - "$1" "${VERIFIER_CAPTURE_METADATA}" <<'PY'
import json, pathlib, stat, sys
path = pathlib.Path(sys.argv[1])
pathlib.Path(sys.argv[2]).write_text(json.dumps({
    "path": str(path), "file_mode": stat.S_IMODE(path.stat().st_mode),
    "directory_mode": stat.S_IMODE(path.parent.stat().st_mode),
}))
PY
exec /usr/bin/tee "$@"
"#,
        );
        let metadata = fixture.directory.path().join("capture-metadata");
        let output = fixture
            .command(None, cargo_exit)
            .env("VERIFIER_CAPTURE_METADATA", &metadata)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(i32::from(cargo_exit)),
            "{output:?}"
        );
        let captured: serde_json::Value = serde_json::from_slice(
            &fs::read(metadata).expect("the live process output must be captured in a private log"),
        )
        .unwrap();
        assert_eq!(captured["file_mode"], 0o600);
        assert_eq!(captured["directory_mode"], 0o700);
        let path = PathBuf::from(captured["path"].as_str().unwrap());
        assert!(
            path.is_file(),
            "private output remains available for evidence"
        );
        assert!(
            path.starts_with(fixture.directory.path()),
            "the fixture owns its retained log"
        );
        assert_eq!(
            fs::read(path).unwrap(),
            fs::read(fixture.directory.path().join("cargo-output")).unwrap()
        );
    }
}

#[cfg(unix)]
#[test]
fn strict_live_verifier_defaults_to_fresh_private_evidence_for_every_run() {
    let fixture = VerifierFixture::new();
    let metadata = fixture.directory.path().join("evidence-metadata");
    let mut previous_directory = None;
    for cargo_exit in [0, 23] {
        let output = fixture
            .command(None, cargo_exit)
            .env("VERIFIER_EVIDENCE_METADATA", &metadata)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(i32::from(cargo_exit)));
        let captured: serde_json::Value =
            serde_json::from_slice(&fs::read(&metadata).unwrap()).unwrap();
        assert_eq!(
            captured["exists"], true,
            "the live command must receive an existing evidence directory even when none was configured"
        );
        assert_eq!(captured["directory_mode"], 0o700);
        assert_eq!(captured["file_mode"], 0o600);
        let directory = PathBuf::from(captured["configured"].as_str().unwrap());
        assert!(directory.starts_with(fixture.directory.path().join("target")));
        assert!(directory.is_absolute());
        assert_ne!(previous_directory.as_ref(), Some(&directory));
        let evidence = PathBuf::from(captured["evidence"].as_str().unwrap());
        assert!(
            evidence.is_file(),
            "child evidence survives both success and failure"
        );
        assert_eq!(evidence.parent(), Some(directory.as_path()));
        let contents: serde_json::Value =
            serde_json::from_slice(&fs::read(evidence).unwrap()).unwrap();
        assert_eq!(
            contents["fixture"],
            "command orchestration only, not a provider response"
        );
        if let Some(previous) = previous_directory {
            assert!(previous.join("fixture-case.json").is_file());
        }
        previous_directory = Some(directory);
    }
}

#[cfg(unix)]
#[test]
fn strict_live_verifier_preserves_explicit_private_evidence_directory() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = VerifierFixture::new();
    let directory = fixture.directory.path().join("caller-evidence");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let previous = directory.join("previous.json");
    fs::write(&previous, "previous evidence must survive").unwrap();
    let metadata = fixture.directory.path().join("evidence-metadata");
    let output = fixture
        .command(None, 23)
        .env("PROVIDERS_KIT_EVIDENCE_DIR", &directory)
        .env("VERIFIER_EVIDENCE_METADATA", &metadata)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(23));
    fixture.assert_prerequisites_and_single_cargo();
    let captured: serde_json::Value = serde_json::from_slice(&fs::read(metadata).unwrap()).unwrap();
    assert_eq!(captured["configured"], directory.to_str().unwrap());
    assert_eq!(captured["directory_mode"], 0o700);
    assert_eq!(captured["file_mode"], 0o600);
    assert!(directory.join("fixture-case.json").is_file());
    assert_eq!(
        fs::read_to_string(previous).unwrap(),
        "previous evidence must survive"
    );
}

#[test]
fn strict_live_verifier_rejects_invalid_evidence_directory_before_live_launch() {
    for kind in ["empty", "missing", "file"] {
        let fixture = VerifierFixture::new();
        let invalid = fixture.directory.path().join("private-invalid-evidence");
        if kind == "file" {
            fs::write(&invalid, "preserve the existing file").unwrap();
        }
        let value = if kind == "empty" {
            ""
        } else {
            invalid.to_str().unwrap()
        };
        let output = fixture
            .command(None, 0)
            .env("PROVIDERS_KIT_EVIDENCE_DIR", value)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "invalid {kind} configuration must fail"
        );
        assert!(
            !fixture.directory.path().join("cargo-args").exists(),
            "invalid evidence configuration must fail before the live command starts"
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("PROVIDERS_KIT_EVIDENCE_DIR"));
        assert!(!stderr.contains("private-invalid-evidence"));
        if kind == "file" {
            assert_eq!(
                fs::read_to_string(invalid).unwrap(),
                "preserve the existing file"
            );
        }
    }
}

#[test]
fn strict_live_verifier_evidence_initialization_failure_prevents_live_launch() {
    let fixture = VerifierFixture::new();
    fixture.write_executable("bin/mktemp", "#!/usr/bin/env bash\nexit 29\n");
    let output = fixture.run(None, 0);
    assert_eq!(output.status.code(), Some(29));
    assert!(
        !fixture.directory.path().join("cargo-args").exists(),
        "failed private directory creation must prevent the live command from starting"
    );
    assert!(
        !String::from_utf8(output.stdout)
            .unwrap()
            .contains("strict-live selection passed")
    );
}

#[test]
fn strict_live_script_requires_every_provider_credential_and_model_input() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = fs::read_to_string(manifest.join("scripts/verify_strict_live.sh"))
        .expect("strict-live verifier should be readable");
    let required = VerifierFixture::REQUIRED;
    for name in required {
        assert!(
            script.contains(name),
            "strict-live verifier must require {name}"
        );
    }
    assert!(script.contains("require_env"));
    assert!(script.contains("invalid strict-live resource shape: %s"));
    assert!(!script.contains("invalid strict-live resource shape: %s=%s"));
    assert!(script.contains("--test strict_live"));
    assert!(!script.contains("--ignored"));
    // The process-level cases above allow only the explicit seven-case partial mode.
    for obsolete in ["TEST_OPENAI_BATCH_FILE_ID", "TEST_GEMINI_BATCH_FILE_ID"] {
        assert!(
            !script.contains(obsolete),
            "strict-live must create fresh batch inputs instead of requiring {obsolete}"
        );
    }

    let resources = [
        ("TEST_OPENAI_FILE_SEARCH_STORE_ID", "vs_"),
        ("TEST_GEMINI_FILE_SEARCH_STORE_ID", "fileSearchStores/"),
    ];
    for (name, prefix) in resources {
        let valid = format!("{prefix}private-resource-value");
        for value in [
            None,
            Some(""),
            Some(prefix),
            Some("wrong-prefix-private-resource-value"),
            Some(valid.as_str()),
        ] {
            let mut command = Command::new("bash");
            command
                .arg(manifest.join("scripts/verify_strict_live.sh"))
                .env_clear()
                .env("PATH", std::env::var_os("PATH").expect("test process PATH"))
                .envs(required.map(|name| (name, "private-dummy-value")))
                .env("TEST_OLLAMA_BASE_URL", "invalid-ollama-url");
            for (resource, resource_prefix) in resources {
                command.env(resource, format!("{resource_prefix}private-resource-value"));
            }
            match value {
                Some(value) => command.env(name, value),
                None => command.env_remove(name),
            };
            let output = command.output().expect("run strict-live resource precheck");
            assert_eq!(output.status.code(), Some(1), "precheck for {name}");
            let expected = match value {
                None | Some("") => format!("missing required strict-live variable: {name}\n"),
                Some(value) if value == valid => concat!(
                    "TEST_OLLAMA_BASE_URL must point at the exact Ollama API base ending in /api: ",
                    "invalid-ollama-url\n"
                )
                .to_owned(),
                Some(_) => format!("invalid strict-live resource shape: {name}\n"),
            };
            assert_eq!(
                String::from_utf8(output.stderr).expect("precheck stderr"),
                expected,
                "precheck for {name}"
            );
            let stdout = String::from_utf8(output.stdout).expect("precheck stdout");
            assert!(!stdout.contains("private-resource-value"));
            assert!(!stdout.contains("private-dummy-value"));
        }
    }
}

#[test]
fn deterministic_gate_selects_assertion_checks_without_running_live_tests() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = fs::read_to_string(manifest.join("scripts/check.sh"))
        .expect("deterministic verifier should be readable");
    assert!(
        script
            .split_whitespace()
            .any(|token| token == "strict_live_assertions"),
        "deterministic target selection must include strict-live assertion contracts"
    );
    assert!(
        !script
            .split_whitespace()
            .any(|token| token == "strict_live"),
        "deterministic target selection must not execute strict-live"
    );
    assert!(!script.contains("--test strict_live "));
}

#[test]
fn provider_tests_do_not_hide_execution_with_ignore_attributes() {
    let mut pending = vec![PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests")];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("test directory should be readable") {
            let path = entry.expect("test entry should be readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = fs::read_to_string(&path).expect("test source should be readable");
                for line in source.lines().map(str::trim_start) {
                    assert!(
                        !(line.starts_with("#[ignore")
                            || line.starts_with("#[cfg_attr") && line.contains("ignore")),
                        "{} must use explicit test target selection instead of ignored tests",
                        path.display()
                    );
                }
            }
        }
    }
}

#[test]
fn workspace_completion_rejects_partial_provider_before_any_gate() {
    for (inherited, configured) in [(Some("0"), None), (None, Some("0")), (Some("1"), Some("0"))] {
        let fixture = VerifierFixture::workspace();
        let output = fixture.run_workspace(&[], inherited, configured, None, 0);
        assert!(
            !output.status.success(),
            "workspace completion must reject partial coverage"
        );
        assert!(
            !fixture.directory.path().join("calls").exists(),
            "partial coverage must fail before any gate"
        );
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("PROVIDERS_KIT_RUN_LONG_BATCHES")
        );
    }
}

#[test]
fn workspace_completion_runs_all_crates_with_effective_full_coverage() {
    for (inherited, configured) in [
        (None, None),
        (Some("1"), None),
        (None, Some("1")),
        (Some("0"), Some("1")),
    ] {
        let fixture = VerifierFixture::workspace();
        let output = fixture.run_workspace(&[], inherited, configured, None, 0);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        fixture.assert_workspace_gates(inherited, configured.or(inherited));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("Workspace strict completion passed"));
        assert!(stdout.contains("FULL"));
        assert!(!stdout.contains("QUALIFIED"));
        for name in VerifierFixture::LONG_CASES {
            assert!(!stdout.contains(name), "full mode must not name exclusions");
        }
    }
}

#[test]
fn workspace_completion_preserves_every_crate_failure_in_both_modes() {
    for arguments in [&[][..], &["--skip-long-provider-batches"][..]] {
        for (index, failed_crate) in VerifierFixture::WORKSPACE_CRATES.iter().enumerate() {
            let fixture = VerifierFixture::workspace();
            let output = fixture.run_workspace(arguments, None, None, Some(failed_crate), 7);
            assert_eq!(output.status.code(), Some(7), "{failed_crate}");
            let calls = fs::read_to_string(fixture.directory.path().join("calls")).unwrap();
            let gates = calls
                .lines()
                .filter(|line| VerifierFixture::WORKSPACE_CRATES.contains(line))
                .collect::<Vec<_>>();
            assert_eq!(gates, VerifierFixture::WORKSPACE_CRATES[..=index]);
            let stdout = String::from_utf8(output.stdout).unwrap();
            assert!(!stdout.contains("Workspace strict completion passed"));
        }
    }
}

#[test]
fn workspace_completion_explicit_exclusion_overrides_only_valid_provider_mode() {
    for (inherited, configured) in [
        (None, None),
        (Some("1"), None),
        (Some("0"), None),
        (None, Some("0")),
        (None, Some("1")),
        (Some("0"), Some("1")),
        (Some("1"), Some("0")),
        (Some("private-overridden-toggle"), Some("1")),
    ] {
        let fixture = VerifierFixture::workspace();
        let output = fixture.run_workspace(
            &["--skip-long-provider-batches"],
            inherited,
            configured,
            None,
            0,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        fixture.assert_workspace_gates(inherited, Some("0"));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("Workspace strict completion passed"));
        assert!(stdout.contains("QUALIFIED"));
        assert!(!stdout.contains("FULL"));
        let exclusions = stdout
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("live::"))
            .collect::<Vec<_>>();
        assert_eq!(exclusions, VerifierFixture::LONG_CASES);
        assert!(!stdout.contains("private-overridden-toggle"));
    }
}

#[test]
fn workspace_completion_rejects_invalid_effective_provider_mode_before_work() {
    for arguments in [&[][..], &["--skip-long-provider-batches"][..]] {
        for invalid in [
            "",
            "true",
            "2",
            "private-invalid-toggle",
            "0\nprivate-invalid-toggle",
        ] {
            for (inherited, configured) in [(Some(invalid), None), (Some("1"), Some(invalid))] {
                let fixture = VerifierFixture::workspace();
                let output = fixture.run_workspace(arguments, inherited, configured, None, 0);
                assert!(!output.status.success());
                assert!(!fixture.directory.path().join("calls").exists());
                let stdout = String::from_utf8(output.stdout).unwrap();
                let stderr = String::from_utf8(output.stderr).unwrap();
                assert!(stderr.contains("PROVIDERS_KIT_RUN_LONG_BATCHES"));
                assert!(!stdout.contains("private-invalid-toggle"));
                assert!(!stderr.contains("private-invalid-toggle"));
                assert!(!stdout.contains("Workspace strict completion passed"));
            }
        }
    }
}

#[test]
fn workspace_completion_rejects_unknown_or_duplicate_arguments_before_environment() {
    for arguments in [
        vec!["--private-unknown-option"],
        vec![
            "--skip-long-provider-batches",
            "--skip-long-provider-batches",
        ],
        vec!["--skip-long-provider-batches", "--private-unknown-option"],
        vec!["--skip-long-provider-batches=0"],
        vec!["--", "--skip-long-provider-batches"],
    ] {
        let fixture = VerifierFixture::workspace();
        let output = fixture.run_workspace(&arguments, None, None, None, 0);
        assert!(!output.status.success());
        assert!(!fixture.directory.path().join("calls").exists());
        assert!(!fixture.directory.path().join("env-calls").exists());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("usage:"));
        assert!(!stdout.contains("private-unknown-option"));
        assert!(!stderr.contains("private-unknown-option"));
        assert!(!stdout.contains("Workspace strict completion passed"));
    }
}

#[test]
fn workspace_completion_propagates_storage_deterministic_failure_before_services() {
    for arguments in [&[][..], &["--skip-long-provider-batches"][..]] {
        let fixture = VerifierFixture::workspace();
        fixture.actual_storage_gate();
        let output = fixture.run_workspace(arguments, None, None, None, 0);
        assert_eq!(output.status.code(), Some(23), "{output:?}");
        assert!(
            fixture
                .directory
                .path()
                .join("script-contract-ran")
                .exists()
        );
        let calls = fs::read_to_string(fixture.directory.path().join("calls")).unwrap();
        assert_eq!(
            calls
                .lines()
                .filter(|line| *line == "storage-deterministic")
                .count(),
            1
        );
        assert!(!calls.contains("unexpected-docker"));
        assert!(
            !calls
                .lines()
                .any(|line| matches!(line, "tools-kit" | "app-kit"))
        );
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("fixture Storage deterministic failure")
        );
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains("Workspace strict completion passed")
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn strict_live_verifier_bounds_stalled_ollama_version_headers() {
    VerifierFixture::new().assert_preflight_timeout(false).await;
}

#[cfg(unix)]
#[tokio::test]
async fn strict_live_verifier_bounds_stalled_ollama_tags_body() {
    VerifierFixture::new().assert_preflight_timeout(true).await;
}

#[test]
fn current_model_roles_fail_before_commands_when_missing() {
    let name = "TEST_GEMINI_MODEL_FILE_SEARCH";
    let fixture = VerifierFixture::new();
    let output = fixture.command(None, 0).env_remove(name).output().unwrap();
    assert_eq!(output.status.code(), Some(1), "{name}");
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        format!("missing required strict-live variable: {name}\n")
    );
    assert!(!fixture.directory.path().join("calls").exists());
}
