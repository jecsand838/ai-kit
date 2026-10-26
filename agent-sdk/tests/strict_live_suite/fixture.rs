use crate::codex_input_evidence::CodexInputObservation;
use crate::native_cleanup::NativeCleanup;

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_sdk::config::{AgentConfig, CodexConfig, OpenCodeConfig};
use agent_sdk::conversation::NewConversation;
use agent_sdk::identity::AgentId;
use agent_sdk::model::ModelSelector;
use agent_sdk::workspace::{
    NewWorkspace, WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
    WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
};
use agent_sdk::{Agent, AgentBuilder, AgentStorage, Conversation, Workspace};
use agent_sdk::{AgentBackend, WorkspaceBackend};
use storage_sdk::StateStore;
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};

#[derive(Clone, Copy, Debug)]
pub(super) enum StrictBackend {
    Codex,
    OpenCode,
}

pub(super) struct StrictFixture {
    // Fields drop in declaration order: settle the store before removing files.
    storage: AgentStorage,
    directory: StrictDirectory,
}

enum StrictDirectory {
    Owned { path: PathBuf, retain: AtomicBool },
    ParentOwned(PathBuf),
}

impl StrictDirectory {
    fn path(&self) -> &Path {
        match self {
            Self::Owned { path, .. } | Self::ParentOwned(path) => path,
        }
    }

    fn retain_after_uncertain_cleanup(&self) {
        if let Self::Owned { retain, .. } = self {
            retain.store(true, Ordering::SeqCst);
        }
    }
}

impl Drop for StrictDirectory {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // An early assertion may bypass asynchronous Agent cleanup. Its
            // remaining owners must keep their durable directory available.
            self.retain_after_uncertain_cleanup();
            return;
        }
        if let Self::Owned { path, retain } = self
            && !retain.load(Ordering::SeqCst)
            && let Err(error) = std::fs::remove_dir_all(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            panic!("owned Lance fixture directory cleanup failed: {error}");
        }
    }
}

pub(super) struct StrictOpenCodeProcess {
    address: SocketAddr,
    root: PathBuf,
    database: PathBuf,
    config_content: String,
    client: reqwest::Client,
    child: Option<tokio::process::Child>,
}

/// A real selected App Server over stdio, with a passive framing bridge so the
/// SDK uses its public TCP constructor. No message or response is synthesized.
pub(super) struct StrictCodexProcess {
    address: SocketAddr,
    child: tokio::process::Child,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    input: std::sync::Arc<std::sync::Mutex<CodexInputObservation>>,
    home: PathBuf,
    stderr: PathBuf,
}

impl StrictCodexProcess {
    pub(super) async fn start(directory: &Path) -> Self {
        use futures_util::{SinkExt, StreamExt};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let input = std::sync::Arc::new(std::sync::Mutex::new(CodexInputObservation::default()));
        let observed = std::sync::Arc::clone(&input);
        let home = PathBuf::from(StrictFixture::required("TEST_CODEX_HOME"));
        let stderr_path = directory.join("codex-request.stderr.log");
        let stderr = std::fs::File::create(&stderr_path).unwrap();
        let mut child = tokio::process::Command::new("codex")
            .arg("app-server")
            .args([
                "-c",
                "mcp_servers={}",
                "-c",
                "features.apps=false",
                "-c",
                "features.code_mode=false",
                "-c",
                "features.code_mode_only=false",
                "-c",
                "features.write_stdin_approval=true",
                "-c",
                "features.default_mode_request_user_input=true",
                "-c",
                "approval_policy=\"on-request\"",
                "-c",
                "approvals_reviewer=\"user\"",
            ])
            .env("CODEX_HOME", &home)
            .current_dir(directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true)
            .spawn()
            .expect("selected Codex executable must start");
        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let task = tokio::spawn(async move {
            let connection = tokio::select! {
                _ = &mut stopped => return Ok(()),
                connection = listener.accept() => connection?.0,
            };
            let mut socket = tokio::select! {
                _ = &mut stopped => return Ok(()),
                socket = tokio_tungstenite::accept_async(connection) => socket.map_err(std::io::Error::other)?,
            };
            let mut lines = BufReader::new(stdout).lines();
            loop {
                tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    line = lines.next_line() => {
                        let Some(line) = line? else { return Ok(()); };
                        let frame: serde_json::Value = serde_json::from_str(&line).map_err(std::io::Error::other)?;
                        observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner).observe_native(&frame);
                        let forwarded = socket.send(Message::Text(line.into())).await.map_err(std::io::Error::other);
                        observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finish_native_forwarding(forwarded.is_ok());
                        forwarded?;
                    }
                    frame = socket.next() => {
                        match frame {
                            Some(Ok(Message::Text(text))) => {
                                let value: serde_json::Value = serde_json::from_str(&text).map_err(std::io::Error::other)?;
                                observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner).observe_sdk(&value);
                                let forwarded: std::io::Result<()> = async {
                                    stdin.write_all(text.as_bytes()).await?;
                                    stdin.write_all(b"\n").await?;
                                    stdin.flush().await
                                }.await;
                                observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finish_sdk_forwarding(forwarded.is_ok());
                                forwarded?;
                            }
                            Some(Ok(Message::Close(_))) | None => return Ok(()),
                            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {},
                            Some(Ok(_)) => return Err(std::io::Error::other("unexpected SDK framing")),
                            Some(Err(error)) => return Err(std::io::Error::other(error)),
                        }
                    }
                }
            }
        });
        Self {
            address,
            child,
            stop: Some(stop),
            task: Some(task),
            input,
            home,
            stderr: stderr_path,
        }
    }

    pub(super) fn config(&self) -> AgentConfig {
        AgentConfig::new(
            CodexConfig::tcp(
                self.address,
                Duration::from_secs(10),
                Duration::from_secs(90),
            )
            .unwrap()
            .into(),
            Duration::from_secs(60),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap()
    }

    pub(super) fn input_evidence(&self) -> std::sync::Arc<std::sync::Mutex<CodexInputObservation>> {
        std::sync::Arc::clone(&self.input)
    }

    pub(super) fn prepare_input_evidence(&self, cwd: &Path, model: &str, prompt: &str) {
        self.input
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare_expected(cwd, model, prompt);
    }

    pub(super) fn assert_nonblocking_response(&self) {
        self.input
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .assert_nonblocking_response();
    }

    pub(super) async fn capture_input_rollout(&self, deadline: tokio::time::Instant) {
        let deadline = deadline.min(tokio::time::Instant::now() + Duration::from_secs(10));
        let stderr = CodexInputObservation::read_stderr(&self.stderr, deadline).await;
        let path = self
            .input
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rollout_path();
        let result = match path {
            Some(path) => CodexInputObservation::read_rollout(&self.home, &path, deadline).await,
            None => Err("rollout_path_missing"),
        };
        let mut evidence = self
            .input
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match stderr {
            Ok(bytes) => evidence.observe_stderr(&bytes),
            Err(reason) => evidence.mark_incomplete(reason),
        }
        match result {
            Ok(bytes) => {
                if let Err(reason) = evidence.observe_rollout(&bytes) {
                    evidence.mark_incomplete(reason);
                }
            }
            Err(reason) => evidence.mark_incomplete(reason),
        }
        if tokio::time::Instant::now() >= deadline {
            evidence.mark_incomplete("rollout_snapshot_deadline");
        }
    }

    pub(super) async fn shutdown(mut self) {
        let mut cleanup = NativeCleanup::new(Duration::from_secs(30));
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = self.task.take() {
            let joined = tokio::time::timeout(Duration::from_secs(30), &mut task).await;
            if joined.is_err() {
                task.abort();
                let _ = task.await;
            }
            cleanup
                .settle(async {
                    joined
                        .map_err(std::io::Error::other)?
                        .map_err(std::io::Error::other)?
                })
                .await;
        }
        cleanup.reap(&mut self.child).await;
        cleanup.finish(Ok(Ok(())));
    }
}

impl StrictFixture {
    const CRASH_STATE_ENV: &'static str = "AGENT_SDK_CRASH_STATE_DIRECTORY";
    const CRASH_CHILD_ENV: &'static str = "AGENT_SDK_CUSTOM_REDUCED_LOSS_CHILD";

    pub(super) async fn new() -> Self {
        super::StrictLiveEnvironment::require_all();
        let directory = match std::env::var_os(Self::CRASH_STATE_ENV) {
            Some(path) => {
                assert!(
                    Self::crash_handoff().is_some(),
                    "shared fixture path requires a supervised child"
                );
                let path = PathBuf::from(path);
                assert!(
                    path.is_dir(),
                    "supervised child must reopen its parent's existing Lance directory"
                );
                StrictDirectory::ParentOwned(path)
            }
            None => StrictDirectory::Owned {
                path: std::env::temp_dir().join(Self::unique("agent-sdk-lance")),
                retain: AtomicBool::new(false),
            },
        };
        let state = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            directory.path(),
        )))
        .await
        .expect("strict-live Lance StateStore must connect");
        Self {
            storage: AgentStorage::new(state),
            directory,
        }
    }

    pub(super) fn directory(&self) -> &Path {
        self.directory.path()
    }

    pub(super) fn storage(&self) -> AgentStorage {
        self.storage.clone()
    }

    pub(super) fn required(name: &str) -> String {
        let value = std::env::var(name)
            .unwrap_or_else(|_| panic!("missing required strict-live environment: {name}"));
        assert_eq!(
            value.trim(),
            value,
            "{name} contains surrounding whitespace"
        );
        assert!(!value.is_empty(), "{name} must not be empty");
        value
    }

    pub(super) async fn model(agent: &Agent, backend: StrictBackend, role: &str) -> ModelSelector {
        let configured = Self::required(role);
        let models = agent
            .list_models()
            .await
            .expect("strict-live model inventory must load");
        let mut matches = models.iter().filter(|model| match backend {
            StrictBackend::Codex => model.selector().as_str() == configured,
            StrictBackend::OpenCode => model.display_name() == Some(configured.as_str()),
        });
        let selected = matches
            .next()
            .unwrap_or_else(|| {
                panic!("configured model for {role} must be advertised: {configured}")
            })
            .selector()
            .clone();
        assert!(
            matches.next().is_none(),
            "configured model for {role} must identify exactly one inventory entry: {configured}"
        );
        selected
    }

    pub(super) fn config(
        backend: StrictBackend,
        label: &str,
        lifecycle_timeout: Duration,
    ) -> AgentConfig {
        let backend = match backend {
            StrictBackend::Codex => CodexConfig::tcp(
                Self::codex_address(),
                Duration::from_secs(10),
                Duration::from_secs(90),
            )
            .expect("strict-live Codex config must be valid")
            .into(),
            StrictBackend::OpenCode => return Self::opencode_config(label, lifecycle_timeout).0,
        };
        AgentConfig::new(
            backend,
            lifecycle_timeout,
            NonZeroUsize::new(2).expect("two prompts are nonzero"),
        )
        .expect("strict-live AgentConfig must be valid")
        .with_display_name(label)
        .expect("strict-live display name must be valid")
    }

    // Retain the address supplied by this test owner, without inspecting an
    // opaque SDK config or changing SDK-owned process lifetime.
    pub(super) fn opencode_config(
        label: &str,
        lifecycle_timeout: Duration,
    ) -> (AgentConfig, SocketAddr) {
        let address = Self::available_loopback_address();
        let backend = OpenCodeConfig::spawn(
            PathBuf::from("opencode"),
            Vec::new(),
            address,
            Duration::from_secs(30),
            Duration::from_secs(600),
            Duration::from_secs(30),
        )
        .expect("strict-live OpenCode config must be valid");
        let config = AgentConfig::new(
            backend.into(),
            lifecycle_timeout,
            NonZeroUsize::new(2).expect("two prompts are nonzero"),
        )
        .expect("strict-live AgentConfig must be valid")
        .with_display_name(label)
        .expect("strict-live display name must be valid");
        (config, address)
    }

    pub(super) async fn create(&self, backend: StrictBackend, label: &str) -> (AgentConfig, Agent) {
        let config = Self::config(backend, label, Duration::from_secs(60));
        let agent = AgentBuilder::create(config.clone(), self.storage())
            .build()
            .await
            .expect("strict-live agent must build");
        (config, agent)
    }

    pub(super) async fn hierarchy(
        &self,
        config: AgentConfig,
        workspace_label: &str,
        conversation_label: &str,
    ) -> (Agent, Workspace, Conversation) {
        let deadline = tokio::time::Instant::now() + config.lifecycle_timeout() * 3;
        self.hierarchy_before(config, workspace_label, conversation_label, deadline)
            .await
    }

    pub(super) async fn hierarchy_before(
        &self,
        config: AgentConfig,
        workspace_label: &str,
        conversation_label: &str,
        deadline: tokio::time::Instant,
    ) -> (Agent, Workspace, Conversation) {
        let lifecycle_timeout = config.lifecycle_timeout();
        let agent = tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + lifecycle_timeout),
            AgentBuilder::create(config, self.storage()).build(),
        )
        .await
        .expect("strict-live hierarchy Agent setup must stay bounded")
        .expect("strict-live hierarchy agent must build");
        let mut cleanup = NativeCleanup::new(lifecycle_timeout);
        let observed = NativeCleanup::observe(
            deadline.min(tokio::time::Instant::now() + lifecycle_timeout),
            async {
                agent
                    .new_workspace(Self::workspace(workspace_label))
                    .await
                    .expect("strict-live hierarchy workspace must build")
            },
        )
        .await;
        let workspace = match observed {
            Ok(Ok(workspace)) => workspace,
            failed => {
                cleanup.settle(agent.shutdown()).await;
                cleanup.finish(failed);
                unreachable!("failed workspace setup cannot return success")
            }
        };
        let observed = NativeCleanup::observe(
            deadline.min(tokio::time::Instant::now() + lifecycle_timeout),
            async {
                workspace
                    .new_conversation(Self::conversation(conversation_label))
                    .await
                    .expect("strict-live hierarchy conversation must build")
            },
        )
        .await;
        let conversation = match observed {
            Ok(Ok(conversation)) => conversation,
            failed => {
                cleanup.settle(workspace.close()).await;
                cleanup.settle(agent.shutdown()).await;
                cleanup.finish(failed);
                unreachable!("failed conversation setup cannot return success")
            }
        };
        (agent, workspace, conversation)
    }

    pub(super) async fn reopen(&self, config: AgentConfig, id: AgentId) -> Agent {
        AgentBuilder::reopen(config, self.storage(), id)
            .build()
            .await
            .expect("strict-live agent must reopen")
    }

    pub(super) fn workspace(label: &str) -> NewWorkspace {
        let cwd = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        NewWorkspace::new(
            label,
            Self::workspace_context(cwd),
            WorkspaceDefaults::empty(),
        )
        .expect("strict-live workspace input must be valid")
    }

    pub(super) fn workspace_context(cwd: PathBuf) -> WorkspaceContext {
        WorkspaceContext::new(
            cwd.clone(),
            vec![
                WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadWrite)
                    .expect("strict-live workspace root must be valid"),
            ],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .expect("strict-live workspace context must be valid")
    }

    pub(super) fn conversation(label: &str) -> NewConversation {
        NewConversation::new(Some(label.to_owned()), None, Vec::new())
            .expect("strict-live conversation input must be valid")
    }

    pub(super) fn unique(prefix: &str) -> String {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("strict-live clock must follow the Unix epoch")
            .as_nanos();
        format!("{prefix}-{}-{nonce}", std::process::id())
    }

    pub(super) fn crash_handoff() -> Option<PathBuf> {
        std::env::var_os(Self::CRASH_CHILD_ENV).map(PathBuf::from)
    }

    pub(super) async fn crash_child(
        &self,
        test: &str,
        initial: &[u8],
        timeout: Duration,
    ) -> Vec<u8> {
        let handoff =
            std::env::temp_dir().join(format!("{}.handoff", Self::unique("agent-sdk-crash")));
        std::fs::write(&handoff, initial).expect("strict-live child handoff must initialize");
        let mut command = tokio::process::Command::new(
            std::env::current_exe().expect("strict-live test executable must be available"),
        );
        command
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(Self::CRASH_CHILD_ENV, &handoff)
            .env(Self::CRASH_STATE_ENV, self.directory())
            .kill_on_drop(true);
        let mut child = None;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + timeout, async {
            let child = child.insert(command.spawn().expect("strict-live crash child must start"));
            let status = child
                .wait()
                .await
                .expect("strict-live crash child wait must succeed");
            assert!(status.success(), "strict-live crash child failed: {status}");
            std::fs::read(&handoff).expect("strict-live child handoff must exist")
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(30));
        if let Some(child) = child.as_mut() {
            cleanup.reap(child).await;
            if !matches!(child.try_wait(), Ok(Some(_))) {
                // The shared fixture also outlives this child on error paths.
                // Never remove its dataset while process ownership is uncertain.
                self.directory.retain_after_uncertain_cleanup();
                cleanup
                    .settle(async { Err::<(), _>("crash child reaping remains unconfirmed") })
                    .await;
            }
        }
        cleanup
            .settle(async {
                match std::fs::remove_file(handoff) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    result => result,
                }
            })
            .await;
        cleanup.finish(observed)
    }

    fn codex_address() -> SocketAddr {
        let url = reqwest::Url::parse(&Self::required("TEST_CODEX_WS_URL"))
            .expect("strict-live Codex URL must parse");
        format!(
            "{}:{}",
            url.host_str()
                .expect("strict-live Codex URL must have a host"),
            url.port()
                .expect("strict-live Codex URL must have an explicit port")
        )
        .parse()
        .expect("strict-live Codex host and port must form a socket address")
    }

    pub(super) fn available_loopback_address() -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("strict-live must reserve a loopback port");
        let address = listener
            .local_addr()
            .expect("strict-live loopback listener must expose its address");
        drop(listener);
        address
    }
}

impl StrictOpenCodeProcess {
    const PROCESS_TIMEOUT: Duration = Duration::from_secs(30);
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

    pub(super) fn configured(label: &str, config_content: String) -> Self {
        let root = std::env::temp_dir().join(format!(
            "agent-sdk-opencode-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        for directory in [
            root.join("opencode/database"),
            root.join("opencode/xdg/config"),
            root.join("opencode/xdg/data"),
            root.join("opencode/xdg/cache"),
            root.join("opencode/xdg/state"),
            root.join("opencode/tmp"),
        ] {
            std::fs::create_dir_all(directory)
                .expect("strict-live OpenCode isolated directory must be created");
        }
        let root =
            std::fs::canonicalize(root).expect("strict-live OpenCode root must canonicalize");
        Self {
            address: StrictFixture::available_loopback_address(),
            database: root.join("opencode/database/opencode.db"),
            config_content,
            client: reqwest::Client::builder()
                .timeout(Self::REQUEST_TIMEOUT)
                .build()
                .expect("strict-live OpenCode client must build"),
            root,
            child: None,
        }
    }

    pub(super) async fn start(label: &str) -> Self {
        let mut config_content = StrictFixture::required("OPENCODE_CONFIG_CONTENT");
        if label == "repaired-abort"
            && let Some(directory) = std::env::var_os("AGENT_SDK_OPENCODE_BOUNDARY_DIAGNOSTICS")
        {
            assert!(
                std::env::var("OPENCODE_PURE")
                    .ok()
                    .is_none_or(|value| matches!(value.as_str(), "false" | "0")),
                "boundary diagnostics require external native plugins to remain enabled"
            );
            let directory = PathBuf::from(directory);
            assert!(
                directory.is_absolute() && directory.is_dir(),
                "boundary diagnostics require an existing absolute owned output directory"
            );
            let directory = std::fs::canonicalize(directory)
                .expect("boundary diagnostic output directory must canonicalize");
            let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let artifact_root = crate_root
                .parent()
                .expect("the crate must belong to its workspace")
                .join("target")
                .canonicalize()
                .expect("the workspace artifact directory must exist");
            assert!(
                directory.starts_with(artifact_root),
                "boundary diagnostics must remain in the workspace artifact directory"
            );
            let plugin = crate_root
                .join("tests/strict_live_suite/diagnostics/opencode-boundary.mjs")
                .canonicalize()
                .expect("the static native diagnostic plugin must exist");
            let plugin = reqwest::Url::from_file_path(plugin)
                .expect("the native diagnostic plugin path must form a file URL");
            let mut config: serde_json::Value = serde_json::from_str(&config_content)
                .expect("native diagnostic configuration must be valid JSON");
            let config = config
                .as_object_mut()
                .expect("native diagnostic configuration must be an object");
            let plugins = config
                .entry("plugin")
                .or_insert_with(|| serde_json::json!([]))
                .as_array_mut()
                .expect("existing native plugins must remain an array");
            let options = serde_json::json!({
                "outputDirectory": directory,
                "expectedModelName": StrictFixture::required("TEST_OLLAMA_MODEL_OPENCODE_SHELL")
            });
            plugins.push(serde_json::json!([plugin.as_str(), options]));
            config_content = serde_json::to_string(config)
                .expect("the child-owned diagnostic configuration must encode");
            eprintln!(
                "OpenCode boundary diagnostics enabled; complete sanitized receipts required; missing receipts prove no wire behavior"
            );
        }
        let mut process = Self::configured(label, config_content);
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Self::PROCESS_TIMEOUT,
            process.launch(),
        )
        .await;
        match observed {
            Ok(Ok(())) => process,
            failed => {
                let mut cleanup = NativeCleanup::new(Self::PROCESS_TIMEOUT * 2);
                cleanup
                    .settle::<std::io::Error>(async {
                        process.shutdown().await;
                        Ok(())
                    })
                    .await;
                cleanup.finish(failed);
                unreachable!("failed process startup cannot return an owner")
            }
        }
    }

    pub(super) fn release_config() -> String {
        let mut config: serde_json::Value =
            serde_json::from_str(&StrictFixture::required("OPENCODE_CONFIG_CONTENT"))
                .expect("isolated OpenCode configuration must decode");
        for (role, field) in [
            ("TEST_OLLAMA_MODEL_OPENCODE_DEFAULT", "reasoning_text"),
            ("TEST_OLLAMA_MODEL_OPENCODE_SHELL", "custom_reasoning"),
        ] {
            let model = StrictFixture::required(role);
            let model = config["provider"]["ollama"]["models"]
                .get_mut(&model)
                .expect("configured role must already exist in the isolated overlay");
            model["interleaved"] = serde_json::json!(field);
        }
        config["permission"] = serde_json::json!({"*":"deny"});
        config["agent"]["build"]["permission"] = serde_json::json!({"*":"deny"});
        config.to_string()
    }

    pub(super) async fn verify_release_inventory(&self, agent: &Agent) {
        let native = self
            .client
            .get(format!("{}/provider", self.endpoint()))
            .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert!(
            native["connected"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("ollama"))
        );
        let provider = native["all"]
            .as_array()
            .unwrap()
            .iter()
            .find(|provider| provider["id"] == "ollama")
            .expect("native configured provider must exist");
        let models = agent
            .list_models()
            .await
            .expect("open interleaved response fields must decode");
        for (role, field) in [
            ("TEST_OLLAMA_MODEL_OPENCODE_DEFAULT", "reasoning_text"),
            ("TEST_OLLAMA_MODEL_OPENCODE_SHELL", "custom_reasoning"),
        ] {
            let configured = StrictFixture::required(role);
            assert_eq!(
                provider["models"][&configured]["capabilities"]["interleaved"],
                serde_json::json!({"field":field})
            );
            let selector = StrictFixture::model(agent, StrictBackend::OpenCode, role).await;
            let model = models
                .iter()
                .find(|model| model.selector() == &selector)
                .unwrap();
            assert!(model.reasoning_efforts().is_empty());
            assert!(model.personalities().is_empty());
        }
    }

    pub(super) async fn verify_release_policy(&self, title: &str) {
        let config = self
            .client
            .get(format!("{}/config", self.endpoint()))
            .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(config["permission"]["*"], "deny");
        assert_eq!(config["agent"]["build"]["permission"]["*"], "deny");
        let sessions = self
            .client
            .get(format!("{}/session", self.endpoint()))
            .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let sessions = sessions.as_array().unwrap();
        assert_eq!(
            sessions.len(),
            1,
            "one portable creation must own exactly one native session"
        );
        let session = &sessions[0];
        assert_eq!(session["title"], title);
        assert_eq!(session["directory"], env!("CARGO_MANIFEST_DIR"));
        assert!(session.get("share").is_none());
        assert!(
            session["metadata"]["agent-sdk.creation-correlation"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert_eq!(
            session["permission"].as_array().unwrap().last(),
            Some(&serde_json::json!({"permission":"*", "pattern":"*", "action":"allow"}))
        );
        let exact = self
            .client
            .get(format!(
                "{}/session/{}",
                self.endpoint(),
                session["id"].as_str().unwrap()
            ))
            .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(
            &exact, session,
            "exact native readback retains policy and creation correlation"
        );
    }

    pub(super) async fn verify_rejected_permissions(&self) {
        for (index, rejected) in [
            serde_json::json!({"permissions": {}}),
            serde_json::json!({"permissions": {"bash":"allow"}}),
            serde_json::json!({"agents": {"build": {"permissions": {}}}}),
            serde_json::json!({"agent": {"build": {"permissions": {"bash":"allow"}}}}),
            serde_json::json!({"mode": {"build": {"permissions": []}}}),
        ]
        .into_iter()
        .enumerate()
        {
            let directory = self.root.join(format!("rejected-permissions-{index}"));
            std::fs::create_dir(&directory).unwrap();
            let config = directory.join("opencode.json");
            let bytes = serde_json::to_vec(&rejected).unwrap();
            std::fs::write(&config, &bytes).unwrap();
            let response = self
                .client
                .get(format!("{}/config", self.endpoint()))
                .query(&[("directory", directory.to_str().unwrap())])
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            let error = response.json::<serde_json::Value>().await.unwrap();
            assert_eq!(error["name"], "ConfigInvalidError");
            assert!(
                error["data"]["issues"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(
                        |issue| issue["message"].as_str().is_some_and(|message| message
                            .contains("V2 permissions are not supported by OpenCode V1"))
                    )
            );
            assert_eq!(
                std::fs::read(config).unwrap(),
                bytes,
                "native rejection must not rewrite policy"
            );
            let response = self
                .client
                .get(format!("{}/experimental/session", self.endpoint()))
                .query(&[
                    ("directory", env!("CARGO_MANIFEST_DIR")),
                    ("archived", "true"),
                    ("limit", "100"),
                ])
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
            assert!(response.headers().get("x-next-cursor").is_none());
            assert_eq!(
                response.json::<serde_json::Value>().await.unwrap(),
                serde_json::json!([]),
                "rejected configuration must not create a native session"
            );
        }
    }

    pub(super) fn config(&self, label: &str, lifecycle_timeout: Duration) -> AgentConfig {
        Self::external_config(self.endpoint(), label, lifecycle_timeout)
    }

    pub(super) fn external_config(
        endpoint: String,
        label: &str,
        lifecycle_timeout: Duration,
    ) -> AgentConfig {
        let opencode = OpenCodeConfig::external(
            endpoint,
            Duration::from_secs(10),
            Duration::from_secs(600),
            Duration::from_secs(30),
        )
        .expect("strict-live external OpenCode config must be valid");
        AgentConfig::new(
            opencode.into(),
            lifecycle_timeout,
            NonZeroUsize::new(2).expect("two prompts are nonzero"),
        )
        .expect("strict-live external AgentConfig must be valid")
        .with_display_name(label)
        .expect("strict-live external display name must be valid")
    }

    pub(super) fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(super) async fn abort_evidence(&self, session_id: &str) -> serde_json::Value {
        use std::hash::{Hash, Hasher};

        let mut evidence =
            serde_json::json!({"http_acknowledgement": "not_independently_observed"});
        let log = std::fs::read_to_string(
            self.root
                .join("opencode/xdg/data/opencode/log/opencode.log"),
        );
        evidence["native_cancel_dispatches"] = match log {
            Ok(log) => serde_json::json!(log.lines().filter(|line|
                line.contains(&format!("message=cancel session.id={session_id}"))).count()),
            Err(_) => serde_json::json!("unavailable"),
        };
        for route in ["session/status", "question", "permission"] {
            let response = self
                .client
                .get(format!("{}/{route}", self.endpoint()))
                .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
                .send()
                .await;
            let body = match response {
                Ok(response) if response.status().is_success() => {
                    response.json::<serde_json::Value>().await.ok()
                }
                _ => None,
            };
            evidence[route] = if route == "session/status" {
                match body.and_then(|body| body.as_object().cloned()) {
                    Some(statuses) => match statuses
                        .get(session_id)
                        .and_then(|value| value["type"].as_str())
                    {
                        Some("busy") => serde_json::json!("active"),
                        Some("retry") => serde_json::json!("retry"),
                        Some("idle") | None => serde_json::json!("idle"),
                        Some(_) => serde_json::json!("unknown_status"),
                    },
                    None => serde_json::json!("unavailable"),
                }
            } else {
                match body.and_then(|body| body.as_array().cloned()) {
                    Some(requests) => serde_json::json!({
                        "count": requests.len(),
                        "owned": requests.iter().filter(|request| request["sessionID"].as_str() == Some(session_id))
                            .take(8).map(|request| {
                                let mut hash = std::collections::hash_map::DefaultHasher::new();
                                request["id"].as_str().hash(&mut hash);
                                request["sessionID"].as_str().hash(&mut hash);
                                serde_json::json!({"identity_fingerprint": format!("{:016x}", hash.finish())})
                            }).collect::<Vec<_>>(),
                    }),
                    None => serde_json::json!("unavailable"),
                }
            };
        }
        evidence
    }

    pub(super) async fn native_events(&self) -> reqwest::Response {
        self.client
            .get(format!("{}/event", self.endpoint()))
            .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
            .timeout(Self::PROCESS_TIMEOUT)
            .send()
            .await
            .expect("strict-live OpenCode event stream must respond")
            .error_for_status()
            .expect("strict-live OpenCode event stream must open")
    }

    pub(super) async fn abort_only_session(&self) -> String {
        let inventory = self
            .client
            .get(format!("{}/experimental/session", self.endpoint()))
            .query(&[("archived", "true"), ("limit", "2")])
            .send()
            .await
            .expect("strict-live OpenCode session inventory must respond")
            .error_for_status()
            .expect("strict-live OpenCode session inventory must succeed")
            .json::<serde_json::Value>()
            .await
            .expect("strict-live OpenCode session inventory must decode");
        let sessions = inventory
            .as_array()
            .expect("strict-live OpenCode session inventory must be an array");
        assert_eq!(
            sessions.len(),
            1,
            "strict-live process must own exactly one native session"
        );
        let session_id = sessions[0]
            .get("id")
            .and_then(serde_json::Value::as_str)
            .expect("strict-live OpenCode session must expose its id")
            .to_owned();
        let directory = sessions[0]
            .get("directory")
            .and_then(serde_json::Value::as_str)
            .expect("strict-live OpenCode session must expose its directory")
            .to_owned();
        let accepted = self
            .client
            .post(format!("{}/session/{session_id}/abort", self.endpoint()))
            .query(&[("directory", &directory)])
            .send()
            .await
            .expect("strict-live OpenCode native abort must respond")
            .error_for_status()
            .expect("strict-live OpenCode native abort must succeed")
            .json::<bool>()
            .await
            .expect("strict-live OpenCode native abort must decode");
        assert!(
            accepted,
            "strict-live OpenCode native abort must be accepted"
        );
        session_id
    }

    pub(super) fn assert_single_native_abort(&self, session_id: &str) {
        let log = std::fs::read_to_string(
            self.root
                .join("opencode/xdg/data/opencode/log/opencode.log"),
        )
        .expect("strict-live OpenCode request log must be readable");
        let expected = format!("message=cancel session.id={session_id}");
        assert_eq!(
            log.lines().filter(|line| line.contains(&expected)).count(),
            1,
            "exactly one native abort request must reach stock OpenCode"
        );
    }

    pub(super) async fn stop(&mut self) {
        let Some(child) = self.child.as_mut() else {
            return;
        };
        let mut cleanup = NativeCleanup::new(Self::PROCESS_TIMEOUT);
        cleanup.reap(child).await;
        if cleanup.diagnostics().is_empty() {
            self.child.take();
        }
        cleanup.finish(Ok(Ok(())));
    }

    pub(super) async fn restart(&mut self) {
        assert!(
            self.child.is_none(),
            "OpenCode process must stop before restart"
        );
        self.launch().await;
    }

    pub(super) async fn shutdown(mut self) {
        let mut cleanup = NativeCleanup::new(Self::PROCESS_TIMEOUT);
        if let Some(child) = self.child.as_mut() {
            cleanup.reap(child).await;
        }
        // Never remove an isolated runtime directory while its process may still use it.
        if cleanup.diagnostics().is_empty() {
            self.child.take();
            cleanup
                .settle(async { std::fs::remove_dir_all(self.root) })
                .await;
        }
        cleanup.finish(Ok(Ok(())));
    }

    pub(super) async fn launch(&mut self) {
        let mut command = tokio::process::Command::new("opencode");
        command
            .arg("serve")
            .arg("--hostname")
            .arg(self.address.ip().to_string())
            .arg("--port")
            .arg(self.address.port().to_string())
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("XDG_CONFIG_HOME", self.root.join("opencode/xdg/config"))
            .env("XDG_DATA_HOME", self.root.join("opencode/xdg/data"))
            .env("XDG_CACHE_HOME", self.root.join("opencode/xdg/cache"))
            .env("XDG_STATE_HOME", self.root.join("opencode/xdg/state"))
            .env("TMPDIR", self.root.join("opencode/tmp"))
            .env("OPENCODE_CONFIG_CONTENT", &self.config_content)
            .env("OPENCODE_AUTO_SHARE", "false")
            .env("OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS", "false")
            .env("OPENCODE_DB", &self.database)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = command
            .spawn()
            .expect("strict-live OpenCode process must start");
        self.child = Some(child);
        self.wait_until_ready().await;
    }

    async fn wait_until_ready(&mut self) {
        let deadline = tokio::time::Instant::now() + Self::PROCESS_TIMEOUT;
        let url = format!("http://{}/global/health", self.address);
        loop {
            if self
                .child
                .as_mut()
                .expect("strict-live OpenCode process must be owned")
                .try_wait()
                .expect("strict-live OpenCode status must be observable")
                .is_some()
            {
                panic!("strict-live OpenCode process exited before readiness");
            }
            if let Ok(response) = self.client.get(&url).send().await
                && response.status().is_success()
                && response
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|value| value.get("healthy").and_then(serde_json::Value::as_bool))
                    == Some(true)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "strict-live OpenCode readiness deadline elapsed"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "strict-live OpenCode readiness deadline elapsed"
        );
        let capabilities = self
            .client
            .get(format!("{}/experimental/capabilities", self.endpoint()))
            .query(&[("directory", env!("CARGO_MANIFEST_DIR"))])
            .timeout(remaining)
            .send()
            .await
            .expect("strict-live OpenCode capabilities must respond")
            .error_for_status()
            .expect("strict-live OpenCode capabilities must succeed")
            .json::<serde_json::Value>()
            .await
            .expect("strict-live OpenCode capabilities must decode");
        assert_eq!(
            capabilities
                .get("backgroundSubagents")
                .and_then(serde_json::Value::as_bool),
            Some(false),
            "strict-live OpenCode must disable background subagents"
        );
    }
}
