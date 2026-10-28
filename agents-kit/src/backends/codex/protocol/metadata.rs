use crate::KitError;
use crate::backends::codex::protocol::notifications::{
    CodexKnownNotificationKind, CodexScope, CodexServerNotification,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(tag = "method", content = "params", rename_all_fields = "camelCase")]
pub(super) enum CodexMetadataNotification {
    #[serde(rename = "thread/readState/changed")]
    ThreadReadStateChanged {
        thread_id: String,
        #[serde(deserialize_with = "CodexThreadReadState::deserialize_object")]
        read_state: CodexThreadReadState,
    },
    #[serde(rename = "thread/reverted")]
    ThreadReverted { thread_id: String },
    #[serde(rename = "thread/queue/changed")]
    ThreadQueueChanged { thread_id: String },
    #[serde(rename = "thread/attachment/updated")]
    ThreadAttachmentUpdated {
        thread_id: String,
        attachment_type: String,
        identity_key: String,
        attachment_id: String,
        operation: CodexAttachmentOperation,
    },
    #[serde(rename = "thread/prediction/updated")]
    ThreadPredictionUpdated {
        thread_id: String,
        source_turn_id: String,
        result: CodexPredictionResult,
    },
    #[serde(rename = "project/changed")]
    ProjectChanged {
        project_id: String,
        change_type: CodexProjectChange,
    },
    #[serde(rename = "thread/project/updated")]
    ThreadProjectUpdated {
        thread_id: String,
        project_id: Option<String>,
    },
    #[serde(rename = "thread/environment/connected")]
    EnvironmentConnected {
        thread_id: String,
        environment_id: String,
    },
    #[serde(rename = "thread/environment/disconnected")]
    EnvironmentDisconnected {
        thread_id: String,
        environment_id: String,
    },
    #[serde(rename = "autoApprovalReview/strictReviewRequired")]
    StrictReviewRequired {
        thread_id: String,
        turn_id: String,
        started_at_ms: i64,
    },
    #[serde(rename = "rawResponse/completed")]
    RawResponseCompleted {
        thread_id: String,
        turn_id: String,
        response_id: String,
        usage: Option<CodexResponseUsage>,
        usage_metadata: Option<CodexResponseUsageMetadata>,
    },
    #[serde(rename = "mcpServer/event/stream/notification")]
    McpEvent {
        subscription_id: String,
        notification: CodexMcpEvent,
    },
    #[serde(rename = "account/gatewayOAuth/changed")]
    GatewayOAuthChanged {
        auth_url: Option<String>,
        provider_id: String,
        status: CodexGatewayOAuthStatus,
        error: Option<String>,
    },
    #[serde(rename = "modelProvider/authRecoveryStarted")]
    AuthRecoveryStarted {
        thread_id: String,
        turn_id: String,
        provider: String,
        message: String,
    },
    #[serde(rename = "modelProvider/authRecoveryCompleted")]
    AuthRecoveryCompleted {
        thread_id: String,
        turn_id: String,
        provider: String,
        message: String,
    },
    #[serde(rename = "thread/realtime/item/started")]
    RealtimeItemStarted {
        thread_id: String,
        item: CodexRealtimeItem,
    },
    #[serde(rename = "thread/realtime/item/transcript/delta")]
    RealtimeTranscriptDelta {
        thread_id: String,
        item_id: String,
        delta: String,
    },
    #[serde(rename = "thread/realtime/item/completed")]
    RealtimeItemCompleted {
        thread_id: String,
        item: CodexRealtimeItem,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CodexAttachmentOperation {
    Created,
    Deleted,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum CodexPredictionResult {
    Completed { text: Option<String> },
    Failed,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CodexProjectChange {
    Created,
    Updated,
    Deleted,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CodexResponseUsage {
    total_tokens: i64,
    input_tokens: i64,
    cached_input_tokens: i64,
    #[serde(default)]
    cache_write_input_tokens: i64,
    output_tokens: i64,
    reasoning_output_tokens: i64,
}

#[derive(Deserialize)]
pub(super) struct CodexResponseUsageMetadata {
    amount: Option<String>,
    metadata: Option<Value>,
}

#[derive(Deserialize)]
pub(super) struct CodexMcpEvent {
    method: String,
    params: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CodexGatewayOAuthStatus {
    NotReady,
    Started,
    Succeeded,
    Failed,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CodexRealtimeItem {
    id: String,
    realtime_session_id: String,
    #[serde(flatten)]
    content: CodexRealtimeContent,
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum CodexRealtimeContent {
    RealtimeSessionStarted,
    TranscriptSegment {
        role: CodexTranscriptRole,
        text: String,
    },
    BemItemPromoted {
        turn_id: String,
        item_id: String,
        presentation: CodexRealtimePresentation,
    },
    RealtimeSessionClosed {
        outcome: CodexRealtimeOutcome,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexTranscriptRole {
    User,
    Assistant,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexRealtimeOutcome {
    Ended,
    Failed,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum CodexRealtimePresentation {
    WholeItem,
    InlineMarkdown,
    InlineVisualization { index: u32 },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CodexThreadReadState {
    first_unread: Option<CodexThreadUnreadPosition>,
    revision: String,
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum CodexThreadUnreadPosition {
    ThreadStart,
    Turn { turn_id: String },
}

impl CodexThreadReadState {
    fn deserialize_object<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom(
                "thread read state must be an object",
            ));
        }
        serde_json::from_value(value).map_err(serde::de::Error::custom)
    }
}

impl CodexMetadataNotification {
    pub(super) fn decode(method: &str, params: Value) -> Result<CodexServerNotification, KitError> {
        if !params.is_object() {
            return Err(KitError::protocol(
                "runtime metadata notification is malformed",
            ));
        }
        let notification: Self =
            serde_json::from_value(serde_json::json!({"method":method,"params":params}))
                .map_err(|_| KitError::protocol("runtime metadata notification is malformed"))?;
        let (scope, kind) = match notification {
            Self::ThreadReadStateChanged {
                thread_id,
                read_state,
            } => {
                // Revisions are opaque native metadata, not Kit ordering tokens.
                drop(read_state.revision);
                match read_state.first_unread {
                    None | Some(CodexThreadUnreadPosition::ThreadStart) => {}
                    Some(CodexThreadUnreadPosition::Turn { turn_id }) => {
                        Self::identity(&turn_id)?;
                    }
                }
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::ThreadReadStateChanged,
                )
            }
            Self::ThreadReverted { thread_id } => (
                Self::thread(thread_id)?,
                CodexKnownNotificationKind::ThreadReverted,
            ),
            Self::ThreadQueueChanged { thread_id } => (
                Self::thread(thread_id)?,
                CodexKnownNotificationKind::ThreadQueueChanged,
            ),
            Self::ThreadAttachmentUpdated {
                thread_id,
                attachment_type,
                identity_key,
                attachment_id,
                operation,
            } => {
                for id in [&attachment_type, &identity_key, &attachment_id] {
                    Self::identity(id)?;
                }
                match operation {
                    CodexAttachmentOperation::Created | CodexAttachmentOperation::Deleted => {}
                }
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::ThreadAttachmentUpdated,
                )
            }
            Self::ThreadPredictionUpdated {
                thread_id,
                source_turn_id,
                result,
            } => {
                match result {
                    CodexPredictionResult::Completed { text } => drop(text),
                    CodexPredictionResult::Failed => {}
                }
                (
                    Self::turn(thread_id, source_turn_id)?,
                    CodexKnownNotificationKind::ThreadPredictionUpdated,
                )
            }
            Self::ProjectChanged {
                project_id,
                change_type,
            } => {
                Self::identity(&project_id)?;
                match change_type {
                    CodexProjectChange::Created
                    | CodexProjectChange::Updated
                    | CodexProjectChange::Deleted => {}
                }
                (
                    CodexScope::Connection,
                    CodexKnownNotificationKind::ProjectChanged,
                )
            }
            Self::ThreadProjectUpdated {
                thread_id,
                project_id,
            } => {
                if let Some(id) = project_id {
                    Self::identity(&id)?;
                }
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::ThreadProjectUpdated,
                )
            }
            Self::EnvironmentConnected {
                thread_id,
                environment_id,
            } => {
                Self::identity(&environment_id)?;
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::EnvironmentConnected,
                )
            }
            Self::EnvironmentDisconnected {
                thread_id,
                environment_id,
            } => {
                Self::identity(&environment_id)?;
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::EnvironmentDisconnected,
                )
            }
            Self::StrictReviewRequired {
                thread_id,
                turn_id,
                started_at_ms,
            } => {
                let _ = started_at_ms;
                (
                    Self::turn(thread_id, turn_id)?,
                    CodexKnownNotificationKind::StrictReviewRequired,
                )
            }
            Self::RawResponseCompleted {
                thread_id,
                turn_id,
                response_id,
                usage,
                usage_metadata,
            } => {
                Self::identity(&response_id)?;
                if let Some(usage) = usage {
                    usage.validate()?;
                }
                if let Some(CodexResponseUsageMetadata { amount, metadata }) = usage_metadata {
                    drop((amount, metadata));
                }
                (
                    Self::turn(thread_id, turn_id)?,
                    CodexKnownNotificationKind::RawResponseCompleted,
                )
            }
            Self::McpEvent {
                subscription_id,
                notification,
            } => {
                Self::identity(&subscription_id)?;
                Self::identity(&notification.method)?;
                drop(notification.params);
                (CodexScope::Connection, CodexKnownNotificationKind::McpEvent)
            }
            Self::GatewayOAuthChanged {
                auth_url,
                provider_id,
                status,
                error,
            } => {
                Self::identity(&provider_id)?;
                drop((auth_url, error));
                match status {
                    CodexGatewayOAuthStatus::NotReady
                    | CodexGatewayOAuthStatus::Started
                    | CodexGatewayOAuthStatus::Succeeded
                    | CodexGatewayOAuthStatus::Failed => {}
                }
                (
                    CodexScope::Connection,
                    CodexKnownNotificationKind::GatewayOAuthChanged,
                )
            }
            Self::AuthRecoveryStarted {
                thread_id,
                turn_id,
                provider,
                message,
            } => {
                Self::identity(&provider)?;
                drop(message);
                (
                    Self::turn(thread_id, turn_id)?,
                    CodexKnownNotificationKind::AuthRecoveryStarted,
                )
            }
            Self::AuthRecoveryCompleted {
                thread_id,
                turn_id,
                provider,
                message,
            } => {
                Self::identity(&provider)?;
                drop(message);
                (
                    Self::turn(thread_id, turn_id)?,
                    CodexKnownNotificationKind::AuthRecoveryCompleted,
                )
            }
            Self::RealtimeItemStarted { thread_id, item } => {
                item.validate()?;
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::RealtimeItemStarted,
                )
            }
            Self::RealtimeTranscriptDelta {
                thread_id,
                item_id,
                delta,
            } => {
                Self::identity(&item_id)?;
                drop(delta);
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::RealtimeTranscriptDelta,
                )
            }
            Self::RealtimeItemCompleted { thread_id, item } => {
                item.validate()?;
                (
                    Self::thread(thread_id)?,
                    CodexKnownNotificationKind::RealtimeItemCompleted,
                )
            }
        };
        CodexServerNotification::known(scope, kind)
    }

    fn identity(id: &str) -> Result<(), KitError> {
        if id.is_empty() || id.trim() != id {
            Err(KitError::protocol("runtime metadata identity is malformed"))
        } else {
            Ok(())
        }
    }

    fn thread(thread_id: String) -> Result<CodexScope, KitError> {
        Self::identity(&thread_id)?;
        Ok(CodexScope::Thread { thread_id })
    }

    fn turn(thread_id: String, turn_id: String) -> Result<CodexScope, KitError> {
        Self::identity(&thread_id)?;
        Self::identity(&turn_id)?;
        Ok(CodexScope::Turn { thread_id, turn_id })
    }
}

impl CodexResponseUsage {
    fn validate(self) -> Result<(), KitError> {
        if [
            self.total_tokens,
            self.input_tokens,
            self.cached_input_tokens,
            self.cache_write_input_tokens,
            self.output_tokens,
            self.reasoning_output_tokens,
        ]
        .into_iter()
        .any(|count| count < 0)
        {
            Err(KitError::protocol("response usage is malformed"))
        } else {
            Ok(())
        }
    }
}

impl CodexRealtimeItem {
    fn validate(self) -> Result<(), KitError> {
        CodexMetadataNotification::identity(&self.id)?;
        CodexMetadataNotification::identity(&self.realtime_session_id)?;
        match self.content {
            CodexRealtimeContent::RealtimeSessionStarted => {}
            CodexRealtimeContent::TranscriptSegment { role, text } => {
                drop(text);
                match role {
                    CodexTranscriptRole::User | CodexTranscriptRole::Assistant => {}
                }
            }
            CodexRealtimeContent::BemItemPromoted {
                turn_id,
                item_id,
                presentation,
            } => {
                CodexMetadataNotification::identity(&turn_id)?;
                CodexMetadataNotification::identity(&item_id)?;
                match presentation {
                    CodexRealtimePresentation::WholeItem
                    | CodexRealtimePresentation::InlineMarkdown => {}
                    CodexRealtimePresentation::InlineVisualization { index } => {
                        let _ = index;
                    }
                }
            }
            CodexRealtimeContent::RealtimeSessionClosed { outcome } => match outcome {
                CodexRealtimeOutcome::Ended | CodexRealtimeOutcome::Failed => {}
            },
        }
        Ok(())
    }
}
