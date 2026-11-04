use std::sync::Arc;
use std::time::Duration;

use crate::core::{ManagedCommands, ManagedConversation, SessionEventPort};
use crate::provider::ProviderRuntime;
use crate::session::{SessionBackend, SessionConfig};
use crate::{KitError, ModelId};

use super::models::OllamaModel;

pub(crate) struct OllamaSession {
    conversation: ManagedConversation<OllamaModel>,
}

impl OllamaSession {
    pub(crate) fn new(
        model: OllamaModel,
        config: SessionConfig,
        timeout: Duration,
    ) -> Result<Self, KitError> {
        Ok(Self {
            conversation: ManagedConversation::new(model, config, timeout)?,
        })
    }
    pub(crate) fn bind_provider(&mut self, provider: Arc<ProviderRuntime>) -> Result<(), KitError> {
        self.conversation.bind_provider(provider)
    }
}

impl SessionBackend for OllamaSession {
    type Commands<'a> = ManagedCommands<'a, OllamaModel>;
    type Events<'a> = SessionEventPort<'a>;
    fn id(&self) -> &str {
        self.conversation.control().id()
    }
    fn model_id(&self) -> &ModelId {
        self.conversation.model_id()
    }
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        self.conversation
            .split(|_, result| crate::core::tool_result_text(result.content()).map(drop))
    }
    async fn close(self) -> Result<(), KitError> {
        self.conversation.close().await
    }
}
