use crate::backends::custom::state_store::CustomTerminalDrain;
use crate::core::notification_store::NotificationReceiver;
use crate::{ConversationNotification, ConversationNotificationKind, KitError};
use storage_kit::{DatabaseBackend, StateStoreBackend};
use tools_kit::declaration::ToolName;

const MAX_PRESENTATION_BYTES: usize = 64 * 1_024;
const MAX_SCHEMA_BYTES: usize = 1_024 * 1_024;
const MAX_INTERACTION_SCHEMA_DEPTH: usize = 64;
const MAX_INTERACTION_SCHEMA_NODES: usize = 16_384;

pub(crate) struct CustomNotificationReceiver<S: StateStoreBackend, D: DatabaseBackend> {
    receiver: NotificationReceiver<S>,
    cleanup: CustomTerminalDrain<S, D>,
    state: CustomNotificationReceiverState,
}

enum CustomNotificationReceiverState {
    Receiving,
    Complete,
    Failed,
}

#[derive(Clone, Copy)]
pub(crate) struct CustomNotifications;

pub(crate) enum CustomInteraction {
    ToolApproval {
        visible_name: String,
        presented_arguments: Option<String>,
    },
    Text {
        visible_name: String,
        prompt: String,
        multiline: bool,
    },
    Form {
        visible_name: String,
        prompt: String,
        schema: serde_json::Value,
    },
}

impl CustomNotifications {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) async fn interaction(
        &self,
        interaction: CustomInteraction,
    ) -> Result<ConversationNotificationKind, KitError> {
        interaction.validate_presentation().await?;
        interaction.notification(self)
    }

    fn validate_text(
        &self,
        label: &'static str,
        value: &str,
        require_unpadded: bool,
    ) -> Result<(), KitError> {
        if value.is_empty()
            || value.len() > MAX_PRESENTATION_BYTES
            || (require_unpadded && value.trim() != value)
            || value
                .chars()
                .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
        {
            return Err(KitError::integration(format!(
                "{label} cannot be presented safely"
            )));
        }
        Ok(())
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend> CustomNotificationReceiver<S, D> {
    pub(crate) fn new(
        receiver: NotificationReceiver<S>,
        cleanup: CustomTerminalDrain<S, D>,
    ) -> Self {
        Self {
            receiver,
            cleanup,
            state: CustomNotificationReceiverState::Receiving,
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<Result<ConversationNotification, KitError>> {
        if !matches!(self.state, CustomNotificationReceiverState::Receiving) {
            return None;
        }
        match self.receiver.recv().await {
            Some(Err(error))
                if !matches!(
                    (error).kind(),
                    crate::error::ErrorKind::Timeout | crate::error::ErrorKind::Integration
                ) =>
            {
                self.state = CustomNotificationReceiverState::Failed;
                Some(Err(error))
            }
            Some(result) => Some(result),
            None if !self.receiver.terminal_reclamation_required() => {
                self.state = CustomNotificationReceiverState::Complete;
                None
            }
            None => match self.cleanup.complete().await {
                Ok(()) => {
                    self.state = CustomNotificationReceiverState::Complete;
                    None
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        crate::error::ErrorKind::Timeout | crate::error::ErrorKind::Integration
                    ) =>
                {
                    Some(Err(error))
                }
                Err(error) => {
                    self.state = CustomNotificationReceiverState::Failed;
                    Some(Err(error))
                }
            },
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend> Drop for CustomNotificationReceiver<S, D> {
    fn drop(&mut self) {
        let terminal = self.receiver.terminal_reclamation_required();
        if terminal && matches!(self.state, CustomNotificationReceiverState::Receiving) {
            self.cleanup.schedule();
        }
        self.receiver.release();
    }
}

impl CustomInteraction {
    pub(crate) fn tool_approval(visible_name: String, presented_arguments: Option<String>) -> Self {
        Self::ToolApproval {
            visible_name,
            presented_arguments,
        }
    }

    pub(crate) fn tool_interaction(
        request: &tools_kit::interaction::ToolInteractionRequest,
    ) -> Self {
        match &request.kind {
            tools_kit::interaction::ToolInteractionKind::Text { multiline } => Self::Text {
                visible_name: request.tool_name.as_str().to_owned(),
                prompt: request.prompt.clone(),
                multiline: *multiline,
            },
            tools_kit::interaction::ToolInteractionKind::Form { schema, .. } => Self::Form {
                visible_name: request.tool_name.as_str().to_owned(),
                prompt: request.prompt.clone(),
                schema: schema.clone(),
            },
        }
    }

    fn notification(
        self,
        notifications: &CustomNotifications,
    ) -> Result<ConversationNotificationKind, KitError> {
        let (title, body, response_schema, dismissible) = match self {
            Self::ToolApproval {
                visible_name,
                presented_arguments,
            } => {
                ToolName::new(visible_name.clone()).map_err(|_| {
                    KitError::integration("tool approval name cannot be presented safely")
                })?;
                let mut body = format!("Tool: {visible_name}");
                if let Some(arguments) = presented_arguments {
                    let candidate = format!("{body}\nArguments: {arguments}");
                    if candidate.len() > MAX_PRESENTATION_BYTES {
                        return Err(KitError::integration(
                            "tool approval presentation exceeds the supported limit",
                        ));
                    }
                    body = candidate;
                }
                ("Tool approval".to_owned(), body, None, false)
            }
            Self::Text {
                visible_name,
                prompt,
                multiline,
            } => {
                ToolName::new(visible_name.clone()).map_err(|_| {
                    KitError::integration("tool interaction name cannot be presented safely")
                })?;
                notifications.validate_text("tool interaction prompt", &prompt, false)?;
                let body = format!(
                    "{prompt}\nMultiline input: {}",
                    if multiline { "allowed" } else { "not allowed" }
                );
                (
                    format!("Tool input: {visible_name}"),
                    body,
                    Some(serde_json::json!({
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": "string"
                    })),
                    true,
                )
            }
            Self::Form {
                visible_name,
                prompt,
                schema,
            } => {
                ToolName::new(visible_name.clone()).map_err(|_| {
                    KitError::integration("tool interaction name cannot be presented safely")
                })?;
                notifications.validate_text("tool interaction prompt", &prompt, false)?;
                (
                    format!("Tool input: {visible_name}"),
                    prompt,
                    Some(schema),
                    true,
                )
            }
        };
        notifications.validate_text("interaction title", &title, true)?;
        notifications.validate_text("interaction body", &body, false)?;
        Ok(ConversationNotificationKind::Interaction {
            title,
            body,
            response_schema,
            dismissible,
        })
    }

    async fn validate_presentation(&self) -> Result<(), KitError> {
        let Self::Form { schema, .. } = self else {
            return Ok(());
        };
        let schema = schema.clone();
        tokio::task::spawn_blocking(move || Self::validate_schema(&schema))
            .await
            .map_err(|_| {
                KitError::integration("tool interaction validation task failed")
                    .with_detail(crate::error::ErrorDetail::TaskTerminated)
            })?
    }

    pub(crate) async fn validate_submission(
        request: tools_kit::interaction::ToolInteractionRequest,
        content: serde_json::Value,
    ) -> Result<(), KitError> {
        match request.kind {
            tools_kit::interaction::ToolInteractionKind::Text { multiline }
                if content.is_string() =>
            {
                let text = content.as_str().ok_or_else(|| {
                    KitError::invalid_input("submitted tool interaction content is invalid")
                })?;
                if !multiline
                    && text
                        .chars()
                        .any(|character| matches!(character, '\n' | '\r'))
                {
                    return Err(KitError::invalid_input(
                        "submitted tool interaction content must be single-line",
                    ));
                }
                Ok(())
            }
            tools_kit::interaction::ToolInteractionKind::Form { schema, .. }
                if content.is_object() =>
            {
                tokio::task::spawn_blocking(move || {
                    Self::validate_schema(&schema)?;
                    let validator =
                        jsonschema::draft202012::options()
                            .build(&schema)
                            .map_err(|_| {
                                KitError::integration("tool interaction schema is unusable")
                            })?;
                    validator.validate(&content).map_err(|_| {
                        KitError::invalid_input("submitted tool interaction content is invalid")
                    })
                })
                .await
                .map_err(|_| {
                    KitError::integration("tool interaction validation task failed")
                        .with_detail(crate::error::ErrorDetail::TaskTerminated)
                })?
            }
            tools_kit::interaction::ToolInteractionKind::Text { .. }
            | tools_kit::interaction::ToolInteractionKind::Form { .. } => Err(
                KitError::invalid_input("submitted content does not match the tool interaction"),
            ),
        }
    }

    fn validate_schema(schema: &serde_json::Value) -> Result<(), KitError> {
        if !schema.is_object() || !Self::accepts_object_content(schema) {
            return Err(KitError::integration(
                "tool interaction schema is not a self-contained object schema",
            ));
        }
        Self::validate_schema_shape(schema)?;
        let encoded = serde_json::to_vec(schema).map_err(|_| {
            KitError::integration("tool interaction schema cannot be encoded safely")
        })?;
        if encoded.len() > MAX_SCHEMA_BYTES
            || jsonschema::draft202012::meta::validate(schema).is_err()
            || jsonschema::draft202012::options().build(schema).is_err()
        {
            return Err(KitError::integration(
                "tool interaction schema is not a self-contained object schema",
            ));
        }
        Ok(())
    }

    fn accepts_object_content(schema: &serde_json::Value) -> bool {
        match schema.get("type") {
            Some(serde_json::Value::String(kind)) => kind == "object",
            Some(serde_json::Value::Array(kinds)) => kinds
                .iter()
                .any(|kind| kind.as_str().is_some_and(|kind| kind == "object")),
            Some(
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Object(_),
            )
            | None => false,
        }
    }

    fn validate_schema_shape(schema: &serde_json::Value) -> Result<(), KitError> {
        let mut stack = vec![(schema, 0_usize)];
        let mut visited = 0_usize;
        while let Some((value, depth)) = stack.pop() {
            visited = visited.checked_add(1).ok_or_else(|| {
                KitError::integration("tool interaction schema exceeds the supported limit")
            })?;
            if visited > MAX_INTERACTION_SCHEMA_NODES || depth > MAX_INTERACTION_SCHEMA_DEPTH {
                return Err(KitError::integration(
                    "tool interaction schema exceeds the supported limit",
                ));
            }
            let next_depth = depth.checked_add(1).ok_or_else(|| {
                KitError::integration("tool interaction schema exceeds the supported limit")
            })?;
            match value {
                serde_json::Value::Array(values) => {
                    for value in values {
                        if visited.saturating_add(stack.len()) >= MAX_INTERACTION_SCHEMA_NODES {
                            return Err(KitError::integration(
                                "tool interaction schema exceeds the supported limit",
                            ));
                        }
                        stack.push((value, next_depth));
                    }
                }
                serde_json::Value::Object(values) => {
                    for (key, value) in values {
                        if matches!(key.as_str(), "$ref" | "$dynamicRef")
                            && value
                                .as_str()
                                .is_none_or(|reference| !reference.starts_with('#'))
                        {
                            return Err(KitError::integration(
                                "tool interaction schema is not a self-contained object schema",
                            ));
                        }
                        if visited.saturating_add(stack.len()) >= MAX_INTERACTION_SCHEMA_NODES {
                            return Err(KitError::integration(
                                "tool interaction schema exceeds the supported limit",
                            ));
                        }
                        stack.push((value, next_depth));
                    }
                }
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => {}
            }
        }
        Ok(())
    }
}
