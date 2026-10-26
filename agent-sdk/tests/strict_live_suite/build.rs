use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use super::fixture::StrictFixture;
use agent_sdk::AgentBackend;
use agent_sdk::config::{
    AgentConfig, BackendConfig, CodexConfig, CustomAgentConfig, CustomLoopLimits,
    CustomToolApproval, OpenCodeConfig,
};
use agent_sdk::identity::AgentId;
use agent_sdk::model::ModelSelector;
use agent_sdk::tool::ToolSelection;
use agent_sdk::{Agent, AgentBuilder};
use provider_sdk::config::{OllamaConfig, ProviderKind, RetryPolicy, TransportConfig};
use provider_sdk::{Provider, ProviderConfig};

const BUILD_TIMEOUT: Duration = Duration::from_secs(660);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(660);

struct StrictBuildInputs {
    fixture: StrictFixture,
}

#[derive(Clone, Copy)]
enum StrictBackend {
    Codex,
    OpenCode,
    Custom,
}

enum StrictBuildMode {
    Create,
    Reopen(AgentId),
}

impl StrictBuildInputs {
    fn required(name: &str) -> String {
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

    async fn new() -> Self {
        Self {
            fixture: StrictFixture::new().await,
        }
    }

    fn custom_provider() -> Provider {
        let base_url = reqwest::Url::parse(&Self::required("TEST_CUSTOM_OLLAMA_URL"))
            .expect("TEST_CUSTOM_OLLAMA_URL must be a URL");
        let ollama = OllamaConfig::local()
            .with_base_url(base_url)
            .expect("TEST_CUSTOM_OLLAMA_URL must point at the exact /api path");
        let transport = TransportConfig::new(
            Duration::from_secs(120),
            Duration::from_secs(10),
            Duration::from_secs(30),
            RetryPolicy::default(),
        )
        .expect("strict-live transport policy must be valid");
        Provider::from_config(ProviderConfig::new(ProviderKind::Ollama(ollama), transport))
            .expect("strict-live Ollama Provider must build")
    }

    async fn build(&self, backend: StrictBackend, mode: StrictBuildMode) -> Agent {
        let config = backend.config();
        let mut builder = match mode {
            StrictBuildMode::Create => AgentBuilder::create(config, self.fixture.storage()),
            StrictBuildMode::Reopen(id) => AgentBuilder::reopen(config, self.fixture.storage(), id),
        };
        if matches!(backend, StrictBackend::Custom) {
            builder = builder.with_provider(Self::custom_provider());
        }
        tokio::time::timeout(BUILD_TIMEOUT, builder.build())
            .await
            .unwrap_or_else(|_| panic!("{} Agent build exceeded its bound", backend.label()))
            .unwrap_or_else(|error| panic!("{} Agent build failed: {error:?}", backend.label()))
    }

    async fn shutdown(backend: StrictBackend, agent: Agent) {
        tokio::time::timeout(SHUTDOWN_TIMEOUT, agent.shutdown())
            .await
            .unwrap_or_else(|_| panic!("{} Agent shutdown exceeded its bound", backend.label()))
            .unwrap_or_else(|error| panic!("{} Agent shutdown failed: {error:?}", backend.label()));
    }

    async fn assert_durable_identity_and_cleanup(&self, backend: StrictBackend) {
        let created = self.build(backend, StrictBuildMode::Create).await;
        let id = created.id();
        let live_info = created.info().await;
        let durable_info = self.fixture.storage().agent_info(id.clone()).await;
        let inventory = self.fixture.storage().list_agents().await;
        Self::shutdown(backend, created).await;

        let live_info = live_info
            .unwrap_or_else(|error| panic!("{} live info failed: {error:?}", backend.label()));
        let durable_info = durable_info
            .unwrap_or_else(|error| panic!("{} durable info failed: {error:?}", backend.label()));
        let inventory = inventory
            .unwrap_or_else(|error| panic!("{} inventory failed: {error:?}", backend.label()));
        assert_eq!(live_info.id(), &id);
        assert_eq!(durable_info, live_info);
        assert!(
            inventory.iter().any(|candidate| candidate.id() == &id),
            "{} durable identity was absent from AgentStorage",
            backend.label()
        );

        let reopened = self
            .build(backend, StrictBuildMode::Reopen(id.clone()))
            .await;
        let reopened_id = reopened.id();
        let reopened_info = reopened.info().await;
        Self::shutdown(backend, reopened).await;

        assert_eq!(reopened_id, id);
        assert_eq!(
            reopened_info.unwrap_or_else(|error| {
                panic!("{} reopened info failed: {error:?}", backend.label())
            }),
            durable_info
        );
    }
}

impl StrictBackend {
    const ALL: [Self; 3] = [Self::Codex, Self::OpenCode, Self::Custom];

    fn label(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::OpenCode => "OpenCode",
            Self::Custom => "Custom",
        }
    }

    fn config(self) -> AgentConfig {
        let backend = match self {
            Self::Codex => BackendConfig::Codex(
                CodexConfig::tcp(
                    Self::codex_address(),
                    Duration::from_secs(10),
                    Duration::from_secs(120),
                )
                .expect("strict-live Codex config must be valid"),
            ),
            Self::OpenCode => BackendConfig::OpenCode(
                OpenCodeConfig::external(
                    StrictBuildInputs::required("TEST_OPENCODE_URL"),
                    Duration::from_secs(10),
                    Duration::from_secs(600),
                    Duration::from_secs(30),
                )
                .expect("strict-live OpenCode config must be valid"),
            ),
            Self::Custom => BackendConfig::Custom(
                CustomAgentConfig::new(
                    ModelSelector::new(StrictBuildInputs::required(
                        "TEST_OLLAMA_MODEL_CUSTOM_DEFAULT",
                    ))
                    .expect("strict-live Custom model selector must be valid"),
                    ToolSelection::None,
                    CustomLoopLimits::new(2, 0, 1)
                        .expect("strict-live Custom loop limits must be valid"),
                    CustomToolApproval::Deny,
                )
                .expect("strict-live Custom config must be valid"),
            ),
        };
        AgentConfig::new(
            backend,
            match self {
                Self::OpenCode => Duration::from_secs(600),
                Self::Codex | Self::Custom => Duration::from_secs(120),
            },
            NonZeroUsize::new(1).expect("one is nonzero"),
        )
        .expect("strict-live AgentConfig must be valid")
        .with_display_name(format!(
            "strict-live-{}-durable",
            self.label().to_lowercase()
        ))
        .expect("strict-live display name must be valid")
    }

    fn codex_address() -> SocketAddr {
        let url = reqwest::Url::parse(&StrictBuildInputs::required("TEST_CODEX_WS_URL"))
            .expect("TEST_CODEX_WS_URL must be a URL");
        assert_eq!(
            url.scheme(),
            "ws",
            "TEST_CODEX_WS_URL must use the ws scheme"
        );
        let host = url
            .host_str()
            .expect("TEST_CODEX_WS_URL must include a host");
        let port = url
            .port()
            .expect("TEST_CODEX_WS_URL must include an explicit port");
        format!("{host}:{port}")
            .parse()
            .expect("TEST_CODEX_WS_URL host and port must form a SocketAddr")
    }
}

#[tokio::test]
async fn built_in_agents_create_reopen_and_cleanup_with_durable_identity() {
    tokio::time::timeout(Duration::from_secs(3_600), async {
        let inputs = StrictBuildInputs::new().await;
        for backend in StrictBackend::ALL {
            inputs.assert_durable_identity_and_cleanup(backend).await;
        }
    })
    .await
    .expect("strict-live built-in construction suite exceeded its bound");
}
