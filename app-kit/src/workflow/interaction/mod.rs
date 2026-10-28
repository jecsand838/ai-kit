use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use agents_kit::conversation::NewConversation;
use agents_kit::identity::{ConversationId, NotificationId, WorkspaceId};
use agents_kit::notification::{
    ConversationNotification, ConversationNotificationReceiver, NotificationResolution,
};
use agents_kit::prompt::{PromptRequest, PromptResponse, PromptStream, StructuredOutputRequest};
use agents_kit::{AgentBackend, ConversationBackend, KitError, WorkspaceBackend};
use serde_json::{Map, Value};

use crate::AppError;
use crate::workflow::execution::AgentRequest;

pub(crate) struct Attempt<A: AgentBackend> {
    sequence: u64,
    conversation: Mutex<AttemptConversation<A>>,
    notification: Mutex<VecDeque<Arc<ConversationNotification>>>,
}

enum AttemptConversation<A: AgentBackend> {
    Lookup,
    Creating,
    Ready(Arc<<<A as AgentBackend>::Workspace as WorkspaceBackend>::Conversation>),
}

pub(crate) enum AttemptObservation {
    Notification(Arc<ConversationNotification>),
    Stream(PromptStream),
    Partial(Map<String, Value>),
    Response(PromptResponse),
    Failed(KitError),
}

impl<A: AgentBackend> Attempt<A> {
    pub(crate) fn new(sequence: u64) -> Self {
        Self {
            sequence,
            conversation: Mutex::new(AttemptConversation::Lookup),
            notification: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn recovered(
        sequence: u64,
        conversation: Arc<<<A as AgentBackend>::Workspace as WorkspaceBackend>::Conversation>,
    ) -> Self {
        Self {
            sequence,
            conversation: Mutex::new(AttemptConversation::Ready(conversation)),
            notification: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn prompt(request: &AgentRequest) -> Result<PromptRequest, AppError> {
        let mut prompt = PromptRequest::new(request.prompt()).map_err(AppError::from)?;
        if let Some(schema) = request.structured_schema() {
            prompt = prompt.with_structured_output(
                StructuredOutputRequest::new(schema.clone()).map_err(AppError::from)?,
            );
        }
        Ok(prompt)
    }

    pub(crate) async fn prepare(
        &self,
        agent: Arc<A>,
        workspace_id: &WorkspaceId,
        request: &AgentRequest,
    ) -> Result<(), AppError> {
        let workspace = agent
            .get_workspace(workspace_id.clone())
            .await
            .map_err(AppError::from)?;
        let conversation = match request.attempt_identity() {
            Some(identity) => {
                if &identity.workspace_id != workspace_id {
                    return Err(AppError::Conflict(
                        "durable agent attempt belongs to another workspace".to_owned(),
                    ));
                }
                Arc::new(
                    workspace
                        .get_conversation(identity.conversation_id.clone())
                        .await
                        .map_err(AppError::from)?,
                )
            }
            None => {
                let request =
                    NewConversation::new(None, None, Vec::new()).map_err(AppError::from)?;
                *self
                    .conversation
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    AttemptConversation::Creating;
                Arc::new(
                    workspace
                        .new_conversation(request)
                        .await
                        .map_err(AppError::from)?,
                )
            }
        };
        *self
            .conversation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            AttemptConversation::Ready(conversation);
        Ok(())
    }

    pub(crate) fn may_have_created_conversation(&self) -> bool {
        matches!(
            &*self
                .conversation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            AttemptConversation::Creating | AttemptConversation::Ready(_)
        )
    }

    pub(crate) fn conversation(
        &self,
    ) -> Result<Arc<<<A as AgentBackend>::Workspace as WorkspaceBackend>::Conversation>, AppError>
    {
        match &*self
            .conversation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            AttemptConversation::Ready(conversation) => Ok(Arc::clone(conversation)),
            AttemptConversation::Lookup | AttemptConversation::Creating => Err(
                AppError::Integration("agent attempt preparation is incomplete".to_owned()),
            ),
        }
    }

    pub(crate) fn conversation_id(&self) -> Result<ConversationId, AppError> {
        Ok(self.conversation()?.id())
    }

    pub(crate) fn notification_stream(&self) -> Result<ConversationNotificationReceiver, AppError> {
        self.conversation()?
            .notification_stream()
            .map_err(AppError::from)
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn install_notification(
        &self,
        notification: Arc<ConversationNotification>,
    ) -> Result<(), AppError> {
        let mut current = self
            .notification
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = current
            .iter()
            .find(|current| current.id() == notification.id())
        {
            return if existing.kind() == notification.kind()
                && existing.inserted_at() == notification.inserted_at()
            {
                Ok(())
            } else {
                Err(AppError::Conflict(
                    "notification identity changed its payload".to_owned(),
                ))
            };
        }
        if current.len() >= 256 {
            return Err(AppError::InvalidInput(
                "live notification ownership limit exceeded".to_owned(),
            ));
        }
        current.push_back(notification);
        Ok(())
    }

    pub(crate) fn notification(
        &self,
        id: &NotificationId,
    ) -> Result<Arc<ConversationNotification>, AppError> {
        self.notification
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .find(|notification| notification.id() == id)
            .cloned()
            .ok_or_else(|| AppError::Busy("exact notification is not yet available".to_owned()))
    }

    pub(crate) fn next_notification(&self) -> Option<Arc<ConversationNotification>> {
        self.notification
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .front()
            .cloned()
    }

    pub(crate) fn remove_notification(&self, id: &NotificationId) {
        let removed = {
            let mut notifications = self
                .notification
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            notifications
                .iter()
                .position(|notification| notification.id() == id)
                .and_then(|position| notifications.remove(position))
        };
        drop(removed);
    }

    pub(crate) async fn resolve(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), AppError> {
        self.conversation()?
            .resolve_notification(id, resolution)
            .await
            .map_err(AppError::from)
    }

    pub(crate) async fn abort(&self) -> Result<(), AppError> {
        self.conversation()?.abort().await.map_err(AppError::from)
    }
}

impl From<KitError> for AppError {
    fn from(error: KitError) -> Self {
        match error.kind() {
            agents_kit::error::ErrorKind::InvalidConfig
            | agents_kit::error::ErrorKind::InvalidInput => {
                Self::InvalidInput("agent operation rejected app input".to_owned())
            }
            agents_kit::error::ErrorKind::NotFound => {
                Self::NotFound("agent resource was not found".to_owned())
            }
            agents_kit::error::ErrorKind::Conflict => {
                Self::Conflict("agent operation conflicted".to_owned())
            }
            agents_kit::error::ErrorKind::Busy => Self::Busy("agent resource is busy".to_owned()),
            agents_kit::error::ErrorKind::Closed | agents_kit::error::ErrorKind::Cancelled => {
                Self::Closed("agent resource is closed".to_owned())
            }
            agents_kit::error::ErrorKind::Timeout => {
                Self::Timeout("agent operation timed out".to_owned())
            }
            agents_kit::error::ErrorKind::Unsupported
            | agents_kit::error::ErrorKind::NotYetImplemented
            | agents_kit::error::ErrorKind::Protocol
            | agents_kit::error::ErrorKind::InvalidState
            | agents_kit::error::ErrorKind::Integration => {
                Self::Integration("agent operation failed".to_owned())
            }
        }
    }
}
