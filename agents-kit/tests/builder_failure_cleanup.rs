use providers_kit::error::{
    ErrorKind as ProviderErrorKind, ErrorPhase as ProviderErrorPhase, NativeErrorDetails,
    Operation as ProviderOperation,
};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use agents_kit::config::{
    AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval, OpenCodeConfig,
};
use agents_kit::model::ModelSelector;
use agents_kit::tool::ToolSelection;
use agents_kit::{AgentBuilder, AgentStorage};
use providers_kit::config::{OllamaConfig, ProviderKind, TransportConfig};
use providers_kit::{
    KitError as ProviderError, ModelId, Provider, ProviderBackend, ProviderConfig,
};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::{Database, StateStore};
use tools_kit::declaration::{ToolInfo, ToolName};
use tools_kit::execution::{ToolHandlerInput, ToolHandlerOutput};
use tools_kit::schema::JsonSchema;
use tools_kit::{ToolError, ToolHandlerBackend, ToolsetBackend};

#[derive(Clone)]
struct CleanupProbe {
    state: Arc<Mutex<CleanupState>>,
    release: Arc<tokio::sync::Notify>,
}

struct CleanupState {
    events: Vec<&'static str>,
    completed: Vec<&'static str>,
    dropped: Vec<&'static str>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    blocked: Option<&'static str>,
    fail_cleanup: bool,
}

struct CleanupToolset {
    name: &'static str,
    label: &'static str,
    probe: CleanupProbe,
    terminal: OnceLock<tokio::sync::watch::Receiver<bool>>,
}

struct RejectingProvider {
    probe: CleanupProbe,
    failure: ProviderFailure,
}

enum ProviderFailure {
    Unsupported,
    Transport,
    Classified(ProviderErrorKind),
}

struct UnusedHandler;

impl CleanupProbe {
    fn new(blocked: Option<&'static str>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CleanupState {
                events: Vec::new(),
                completed: Vec::new(),
                dropped: Vec::new(),
                tasks: Vec::new(),
                blocked,
                fail_cleanup: false,
            })),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn fail_cleanup(&self) -> bool {
        self.state.lock().expect("probe state").fail_cleanup
    }

    async fn record(&self, label: &'static str) {
        let blocked = {
            let mut state = self
                .state
                .lock()
                .expect("cleanup probe state must not be poisoned");
            state.events.push(label);
            state.blocked == Some(label)
        };
        if blocked {
            self.release.notified().await;
        }
    }

    fn events(&self) -> Vec<&'static str> {
        self.state
            .lock()
            .expect("cleanup probe state must not be poisoned")
            .events
            .clone()
    }

    fn completed(&self) -> Vec<&'static str> {
        self.state
            .lock()
            .expect("cleanup state is not poisoned")
            .completed
            .clone()
    }

    fn dropped(&self) -> Vec<&'static str> {
        self.state
            .lock()
            .expect("cleanup state is not poisoned")
            .dropped
            .clone()
    }

    fn start(&self, label: &'static str) -> tokio::sync::watch::Receiver<bool> {
        let (sender, receiver) = tokio::sync::watch::channel(false);
        let owner = self.clone();
        let task = tokio::spawn(async move {
            owner.record(label).await;
            owner
                .state
                .lock()
                .expect("cleanup state is not poisoned")
                .completed
                .push(label);
            sender.send_replace(true);
        });
        self.state
            .lock()
            .expect("cleanup state is not poisoned")
            .tasks
            .push(task);
        receiver
    }

    async fn join(&self) {
        let tasks = std::mem::take(
            &mut self
                .state
                .lock()
                .expect("cleanup state is not poisoned")
                .tasks,
        );
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("accepted cleanup must settle")
                .expect("cleanup task must join");
        }
    }

    async fn wait_for_events(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.events().len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup must make bounded progress");
    }
}

impl CleanupToolset {
    fn new(name: &'static str, label: &'static str, probe: CleanupProbe) -> Self {
        Self {
            name,
            label,
            probe,
            terminal: OnceLock::new(),
        }
    }

    fn info(&self) -> ToolInfo {
        let name = ToolName::new(self.name).expect("fixture tool name must be valid");
        let mut info = ToolInfo::new(
            name,
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "additionalProperties": false
            })),
        );
        info.description = Some("Exercises deterministic build cleanup".to_owned());
        info
    }
}

impl ToolsetBackend for CleanupToolset {
    type Handler = UnusedHandler;

    fn list_tools(&self) -> impl Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_ {
        std::future::ready(if self.terminal.get().is_some() {
            Err(ToolError::Closed {
                operation: "list_tools".to_owned(),
            })
        } else {
            Ok(vec![self.info()])
        })
    }

    fn tool_handler(
        &self,
        name: ToolName,
    ) -> impl Future<Output = Result<Self::Handler, ToolError>> + Send + '_ {
        std::future::ready(Err(if self.terminal.get().is_some() {
            ToolError::Closed {
                operation: "tool_handler".to_owned(),
            }
        } else {
            ToolError::UnknownTool { name }
        }))
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        let mut terminal = self
            .terminal
            .get_or_init(|| self.probe.start(self.label))
            .clone();
        while !*terminal.borrow_and_update() {
            terminal
                .changed()
                .await
                .expect("tracked cleanup owns terminal publication");
        }
        if self.probe.fail_cleanup() {
            Err(ToolError::Transport {
                tool: None,
                message: "private cleanup sentinel".to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

impl Drop for CleanupToolset {
    fn drop(&mut self) {
        self.probe
            .state
            .lock()
            .expect("cleanup state is not poisoned")
            .dropped
            .push(self.label);
    }
}

impl ToolHandlerBackend for UnusedHandler {
    fn step(
        &mut self,
        _input: ToolHandlerInput,
    ) -> impl Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_ {
        std::future::ready(Err(ToolError::Unsupported {
            operation: "unused cleanup fixture handler".to_owned(),
        }))
    }
}

impl ProviderBackend for RejectingProvider {
    type Model = providers_kit::Model;

    fn model(&self, _id: ModelId) -> Result<Self::Model, ProviderError> {
        Err(match self.failure {
            ProviderFailure::Classified(kind) => ProviderError::new(
                kind,
                ProviderOperation::SelectModel,
                ProviderErrorPhase::RequestSend,
            )
            .with_native_details(NativeErrorDetails {
                message: Some("sensitive mapping sentinel".to_owned()),
                ..NativeErrorDetails::default()
            })
            .with_http_status(401)
            .with_cause(ProviderError::new(
                ProviderErrorKind::Transport,
                ProviderOperation::SelectModel,
                ProviderErrorPhase::RequestSend,
            ))
            .with_supplementary(ProviderError::new(
                ProviderErrorKind::Timeout,
                ProviderOperation::Shutdown,
                ProviderErrorPhase::Cleanup,
            )),
            ProviderFailure::Unsupported => ProviderError::new(
                ProviderErrorKind::Unsupported,
                ProviderOperation::SelectModel,
                ProviderErrorPhase::InputValidation,
            ),
            ProviderFailure::Transport => ProviderError::new(
                ProviderErrorKind::Transport,
                ProviderOperation::SelectModel,
                ProviderErrorPhase::RequestSend,
            )
            .with_native_details(NativeErrorDetails {
                message: Some("sensitive transport detail".to_owned()),
                ..NativeErrorDetails::default()
            }),
        })
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        self.probe.record("provider").await;
        if self.probe.fail_cleanup() {
            Err(ProviderError::new(
                ProviderErrorKind::Transport,
                ProviderOperation::Shutdown,
                ProviderErrorPhase::Lifecycle,
            ))
        } else {
            Ok(())
        }
    }
}

struct BuilderFailureFixture;

impl BuilderFailureFixture {
    async fn builder(
        probe: CleanupProbe,
    ) -> AgentBuilder<StateStore, Database, Provider, CleanupToolset> {
        let provider = Provider::from_config(ProviderConfig::new(
            ProviderKind::Ollama(OllamaConfig::local()),
            TransportConfig::default(),
        ))
        .expect("fixture provider must build without network access");
        Self::builder_with(provider, probe, ["duplicate_tool", "duplicate_tool"]).await
    }

    async fn builder_with<P: ProviderBackend>(
        provider: P,
        probe: CleanupProbe,
        names: [&'static str; 2],
    ) -> AgentBuilder<StateStore, Database, P, CleanupToolset> {
        let custom = CustomAgentConfig::new(
            ModelSelector::new("qwen3").expect("fixture model selector must be valid"),
            ToolSelection::None,
            CustomLoopLimits::new(1, 0, 1).expect("fixture limits must be valid"),
            CustomToolApproval::Allow,
        )
        .expect("fixture Custom config must be valid");
        let config = AgentConfig::new(
            custom.into(),
            Duration::from_secs(3),
            NonZeroUsize::new(1).expect("one prompt is nonzero"),
        )
        .expect("fixture Agent config must be valid");
        let state = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("fixture state store must build");
        AgentBuilder::create_for_backends::<P, CleanupToolset>(config, AgentStorage::new(state))
            .with_provider(provider)
            .with_toolset(CleanupToolset::new(names[0], "first", probe.clone()))
            .with_toolset(CleanupToolset::new(names[1], "second", probe))
    }
}

#[tokio::test]
async fn failed_build_settles_owned_toolsets_once_in_reverse_order() {
    let probe = CleanupProbe::new(None);
    let error = BuilderFailureFixture::builder(probe.clone())
        .await
        .build()
        .await
        .map(|_| ())
        .expect_err("duplicate tool names must fail the build");

    assert!(matches!(
        (error).kind(),
        agents_kit::error::ErrorKind::InvalidConfig
    ));
    assert_eq!(probe.events(), vec!["second", "first"]);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(probe.events(), vec!["second", "first"]);
    probe.join().await;
}

#[tokio::test]
async fn rejected_default_model_settles_composition_once_in_reverse_order() {
    let probe = CleanupProbe::new(None);
    let provider = RejectingProvider {
        probe: probe.clone(),
        failure: ProviderFailure::Unsupported,
    };
    let error =
        BuilderFailureFixture::builder_with(provider, probe.clone(), ["first_tool", "second_tool"])
            .await
            .build()
            .await
            .map(|_| ())
            .expect_err("unsupported default model must fail the build");

    assert!(matches!(
        (error).kind(),
        agents_kit::error::ErrorKind::Unsupported
    ));
    assert_eq!(
        error.context().phase,
        agents_kit::error::ErrorPhase::ModelSelection
    );
    assert_eq!(
        error.cause().unwrap().kind(),
        agents_kit::error::ErrorKind::Unsupported
    );
    assert_eq!(probe.events(), vec!["second", "first", "provider"]);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(probe.events(), vec!["second", "first", "provider"]);
    probe.join().await;
}

#[tokio::test]
async fn model_selection_transport_failure_remains_redacted_integration() {
    let probe = CleanupProbe::new(None);
    let provider = RejectingProvider {
        probe: probe.clone(),
        failure: ProviderFailure::Transport,
    };
    let error =
        BuilderFailureFixture::builder_with(provider, probe.clone(), ["first_tool", "second_tool"])
            .await
            .build()
            .await
            .map(|_| ())
            .expect_err("transport failure must fail the build");

    assert!(matches!(
        (error).kind(),
        agents_kit::error::ErrorKind::Integration
    ));
    assert_eq!(error.message(), "model selection request failed");
    assert_eq!(probe.events(), vec!["second", "first", "provider"]);
    probe.join().await;
}

#[tokio::test]
async fn cancelling_the_waiter_does_not_detach_accepted_cleanup() {
    let probe = CleanupProbe::new(Some("second"));
    let builder = BuilderFailureFixture::builder(probe.clone()).await;
    let task = tokio::spawn(async move { builder.build().await });

    probe.wait_for_events(1).await;
    assert_eq!(probe.events(), vec!["second"]);
    task.abort();
    let join_error = match tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("cancelled build waiter must join")
    {
        Ok(_) => panic!("build waiter must be cancelled"),
        Err(error) => error,
    };
    assert!(join_error.is_cancelled());

    probe.release.notify_waiters();
    probe.wait_for_events(2).await;
    assert_eq!(probe.events(), vec!["second", "first"]);
    probe.join().await;
}

#[tokio::test]
async fn unsupported_native_tool_installation_cleans_direct_values_before_connecting() {
    let probe = CleanupProbe::new(None);
    let native = OpenCodeConfig::external(
        "http://127.0.0.1:1",
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .expect("native configuration must be valid");
    let config = AgentConfig::new(
        native.into(),
        Duration::from_secs(3),
        NonZeroUsize::new(1).expect("one is nonzero"),
    )
    .expect("Agent configuration must be valid");
    let state = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("fixture store must connect");
    let error = AgentBuilder::create_for_backends::<RejectingProvider, CleanupToolset>(
        config,
        AgentStorage::new(state),
    )
    .with_toolset(CleanupToolset::new("first_tool", "first", probe.clone()))
    .with_toolset(CleanupToolset::new("second_tool", "second", probe.clone()))
    .build()
    .await
    .map(|_| ())
    .expect_err("unsupported native tool installation must fail before network I/O");
    assert!(
        matches!((error).kind(), agents_kit::error::ErrorKind::Unsupported),
        "{error:?}"
    );
    assert_eq!(probe.events(), vec!["second", "first"]);
    probe.join().await;
}

#[tokio::test]
async fn direct_tool_cleanup_outlives_deadline_and_admits_each_remaining_source() {
    let probe = CleanupProbe::new(Some("second"));
    let builder = BuilderFailureFixture::builder(probe.clone()).await;
    let error = tokio::time::timeout(Duration::from_secs(5), builder.build())
        .await
        .expect("failed build must honor its lifecycle deadline")
        .map(|_| ())
        .expect_err("duplicate inventory must preserve its primary error");
    assert!(matches!(
        (error).kind(),
        agents_kit::error::ErrorKind::InvalidConfig
    ));
    probe.wait_for_events(2).await;
    assert_eq!(
        probe.events(),
        vec!["second", "first"],
        "shared deadline exhaustion cannot leave a later Toolset unadmitted"
    );
    assert!(
        !probe.completed().contains(&"second"),
        "gated cleanup must still be owned"
    );
    assert_eq!(
        probe.dropped(),
        vec!["second", "first"],
        "accepted cleanup must survive loss of all public Toolset values"
    );

    probe.release.notify_waiters();
    probe.join().await;
    let mut completed = probe.completed();
    completed.sort();
    assert_eq!(
        completed,
        vec!["first", "second"],
        "each accepted cleanup completes exactly once"
    );
    assert_eq!(probe.events(), vec!["second", "first"]);
}

#[tokio::test]
async fn direct_tool_shutdown_replays_its_owned_terminal_result() {
    let probe = CleanupProbe::new(None);
    let tools = CleanupToolset::new("one_tool", "one", probe.clone());
    tools.shutdown().await.expect("first shutdown must finish");
    tools.shutdown().await.expect("later shutdown must replay");
    assert!(matches!(
        tools.list_tools().await,
        Err(ToolError::Closed { .. })
    ));
    assert!(matches!(
        tools
            .tool_handler(ToolName::new("one_tool").expect("name is valid"))
            .await,
        Err(ToolError::Closed { .. })
    ));
    probe.join().await;
    assert_eq!(probe.events(), vec!["one"]);
    assert_eq!(probe.completed(), vec!["one"]);
}

#[tokio::test]
async fn rejected_model_preserves_all_composition_cleanup_failures() {
    let probe = CleanupProbe::new(None);
    probe.state.lock().expect("probe state").fail_cleanup = true;
    let provider = RejectingProvider {
        probe: probe.clone(),
        failure: ProviderFailure::Unsupported,
    };
    let error =
        BuilderFailureFixture::builder_with(provider, probe.clone(), ["first_tool", "second_tool"])
            .await
            .build()
            .await
            .map(|_| ())
            .expect_err("rejected model");
    probe.join().await;
    assert_eq!(probe.events(), vec!["second", "first", "provider"]);
    assert_eq!(error.kind(), agents_kit::error::ErrorKind::Unsupported);
    let cleanup = error
        .supplementary()
        .first()
        .expect("composition cleanup failure retained");
    assert_eq!(cleanup.kind(), agents_kit::error::ErrorKind::Integration);
    assert_eq!(
        cleanup.context().phase,
        agents_kit::error::ErrorPhase::Cleanup
    );
    assert_eq!(cleanup.supplementary().len(), 2);
    assert!(!format!("{error:?} {error}").contains("private cleanup sentinel"));
}

#[tokio::test]
async fn provider_classification_survives_agent_build_boundary() {
    use agents_kit::error::{ErrorDetail, ErrorKind, ErrorPhase, Operation};
    for (kind, expected, detail) in [
        (
            ProviderErrorKind::Authentication,
            ErrorKind::Integration,
            ErrorDetail::AuthenticationRejected,
        ),
        (
            ProviderErrorKind::PermissionDenied,
            ErrorKind::Integration,
            ErrorDetail::PermissionDenied,
        ),
        (
            ProviderErrorKind::QuotaExceeded,
            ErrorKind::Integration,
            ErrorDetail::QuotaExceeded,
        ),
        (
            ProviderErrorKind::Billing,
            ErrorKind::Integration,
            ErrorDetail::BillingRequired,
        ),
        (
            ProviderErrorKind::ServiceUnavailable,
            ErrorKind::Integration,
            ErrorDetail::ServiceUnavailable,
        ),
        (
            ProviderErrorKind::Cancelled,
            ErrorKind::Integration,
            ErrorDetail::RemoteCancelled,
        ),
        (
            ProviderErrorKind::Conflict,
            ErrorKind::Integration,
            ErrorDetail::RemoteConflict,
        ),
        (
            ProviderErrorKind::DeadlineExceeded,
            ErrorKind::Integration,
            ErrorDetail::RemoteDeadlineExceeded,
        ),
    ] {
        let probe = CleanupProbe::new(None);
        let provider = RejectingProvider {
            probe: probe.clone(),
            failure: ProviderFailure::Classified(kind),
        };
        let error = BuilderFailureFixture::builder_with(
            provider,
            probe.clone(),
            ["first_tool", "second_tool"],
        )
        .await
        .build()
        .await
        .map(|_| ())
        .expect_err("rejected model");
        probe.join().await;
        assert_eq!(error.kind(), expected);
        assert_eq!(error.detail(), Some(&detail));
        assert_eq!(error.context().operation, Operation::Build);
        assert_eq!(error.context().phase, ErrorPhase::ModelSelection);
        assert_eq!(error.http_status(), Some(401));
        let dependency = error.cause().expect("normalized provider cause");
        assert_eq!(dependency.context().phase, ErrorPhase::RequestSend);
        assert_eq!(
            dependency.cause().unwrap().detail(),
            Some(&ErrorDetail::TransportFailure)
        );
        assert_eq!(dependency.supplementary().len(), 1);
        assert!(!format!("{error:?}").contains("sensitive mapping sentinel"));
    }
}
