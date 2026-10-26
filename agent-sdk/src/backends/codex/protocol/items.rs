use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::SdkError;

// Selected non-assistant items share the same decoder for live events and history.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum CodexAuxiliaryItem {
    FunctionCallOutput {
        id: String,
        name: String,
        namespace: Option<String>,
        output: CodexFunctionOutput,
    },
    #[serde(rename_all = "camelCase")]
    DynamicToolCall {
        id: String,
        tool: String,
        namespace: Option<String>,
        arguments: Value,
        status: CodexDynamicStatus,
        content_items: Option<Vec<CodexDynamicContent>>,
        success: Option<bool>,
        duration_ms: Option<i64>,
    },
    #[serde(rename_all = "camelCase")]
    CollabAgentToolCall {
        id: String,
        tool: CodexCollabTool,
        status: CodexCollabStatus,
        sender_thread_id: String,
        receiver_thread_ids: Vec<String>,
        prompt: Option<String>,
        model: Option<String>,
        // The native ReasoningEffort accepts custom strings.
        reasoning_effort: Option<String>,
        agents_states: BTreeMap<String, CodexCollabAgentState>,
    },
    #[serde(rename_all = "camelCase")]
    SubAgentActivity {
        id: String,
        kind: CodexSubAgentActivity,
        agent_thread_id: String,
        agent_path: String,
    },
}

impl CodexAuxiliaryItem {
    pub(super) fn decode(item: &serde_json::Map<String, Value>) -> Result<Self, SdkError> {
        serde_json::from_value(Value::Object(item.clone()))
            .map_err(|_| SdkError::protocol("native tool item is malformed"))
    }

    pub(super) fn validate_support(&self) -> Result<(), SdkError> {
        let id = match self {
            Self::FunctionCallOutput { id, .. }
            | Self::DynamicToolCall { id, .. }
            | Self::CollabAgentToolCall { id, .. }
            | Self::SubAgentActivity { id, .. } => id,
        };
        if id.is_empty() || id.trim() != id {
            return Err(SdkError::protocol("native tool item identity is malformed"));
        }
        match self {
            Self::FunctionCallOutput {
                id: _,
                name,
                namespace,
                output,
            } => {
                let _ = (name, namespace);
                match output {
                    CodexFunctionOutput::Text(text) => {
                        let _ = text;
                    }
                    CodexFunctionOutput::ContentItems(items) => {
                        let _ = items;
                    }
                }
                Ok(())
            }
            Self::DynamicToolCall {
                id: _,
                tool,
                namespace,
                arguments,
                status,
                content_items,
                success,
                duration_ms,
            } => {
                let _ = (tool, namespace, arguments, status, success, duration_ms);
                if content_items.as_ref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| matches!(item, CodexDynamicContent::Audio { .. }))
                }) {
                    return Err(SdkError::unsupported(
                        "native audio tool output has no portable event",
                    ));
                }
                Ok(())
            }
            Self::CollabAgentToolCall {
                id: _,
                tool,
                status,
                sender_thread_id,
                receiver_thread_ids,
                prompt,
                model,
                reasoning_effort,
                agents_states,
            } => {
                if reasoning_effort.as_deref() == Some("") {
                    return Err(SdkError::protocol("native reasoning effort is malformed"));
                }
                let _ = (
                    tool,
                    status,
                    sender_thread_id,
                    receiver_thread_ids,
                    prompt,
                    model,
                    agents_states,
                );
                Err(SdkError::unsupported(
                    "native collaboration has no portable foreground lifecycle",
                ))
            }
            Self::SubAgentActivity {
                id: _,
                kind,
                agent_thread_id,
                agent_path,
            } => {
                let _ = (kind, agent_thread_id, agent_path);
                Err(SdkError::unsupported(
                    "native collaboration has no portable foreground lifecycle",
                ))
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum CodexFunctionOutput {
    Text(String),
    ContentItems(Vec<CodexFunctionContent>),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum CodexFunctionContent {
    InputText {
        #[serde(rename = "text")]
        _text: String,
    },
    InputImage {
        #[serde(flatten)]
        _image: CodexImageReference,
        #[serde(rename = "detail")]
        _detail: Option<CodexImageDetail>,
    },
    InputAudio {
        #[serde(rename = "audio_url")]
        _audio_url: String,
    },
    EncryptedContent {
        #[serde(rename = "encrypted_content")]
        _encrypted_content: String,
    },
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum CodexImageReference {
    Inline {
        #[serde(rename = "image_url")]
        _image_url: String,
    },
    File {
        #[serde(rename = "file_id")]
        _file_id: String,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum CodexImageDetail {
    Auto,
    Low,
    High,
    Original,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum CodexDynamicContent {
    #[serde(rename = "inputText")]
    Text {
        #[serde(rename = "text")]
        _text: String,
    },
    #[serde(rename = "inputImage")]
    Image {
        #[serde(rename = "imageUrl")]
        _image_url: String,
    },
    #[serde(rename = "inputAudio")]
    Audio {
        #[serde(rename = "audioUrl")]
        _audio_url: String,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CodexDynamicStatus {
    InProgress,
    Completed,
    Failed,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CodexCollabTool {
    SpawnAgent,
    SendInput,
    ResumeAgent,
    Wait,
    CloseAgent,
    SendMessage,
    FollowupTask,
    InterruptAgent,
    ListAgents,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CodexCollabStatus {
    InProgress,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CodexSubAgentActivity {
    Started,
    Interacted,
    Interrupted,
    Completed,
}

#[derive(Deserialize)]
pub(super) struct CodexCollabAgentState {
    #[serde(rename = "status")]
    _status: CodexCollabAgentStatus,
    #[serde(rename = "message")]
    _message: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexCollabAgentStatus {
    PendingInit,
    Running,
    Interrupted,
    Completed,
    Errored,
    Shutdown,
    NotFound,
}
