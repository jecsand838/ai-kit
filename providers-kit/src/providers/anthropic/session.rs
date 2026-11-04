use std::sync::Arc;
use std::time::Duration;

use crate::core::{ManagedCommands, ManagedConversation, SessionEventPort};
use crate::provider::ProviderRuntime;
use crate::session::{SessionBackend, SessionConfig};
use crate::{KitError, ModelId};

use super::models::AnthropicModel;

pub(crate) struct AnthropicSession {
    conversation: ManagedConversation<AnthropicModel>,
}

impl AnthropicSession {
    pub(crate) fn new(
        model: AnthropicModel,
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

impl SessionBackend for AnthropicSession {
    type Commands<'a> = ManagedCommands<'a, AnthropicModel>;
    type Events<'a> = SessionEventPort<'a>;
    fn id(&self) -> &str {
        self.conversation.control().id()
    }
    fn model_id(&self) -> &ModelId {
        self.conversation.model_id()
    }
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        self.conversation.split(|_, result| {
            super::messages::tool_result_content_value(result.content()).map(drop)
        })
    }
    async fn close(self) -> Result<(), KitError> {
        self.conversation.close().await
    }
}
