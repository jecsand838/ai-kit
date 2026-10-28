use crate::backends::codex::protocol::metadata::CodexMetadataNotification;
use crate::error::{ErrorDetail, ErrorPhase, Operation};
use serde::Deserialize;
use serde_json::Value;

use crate::KitError;

pub(crate) enum CodexServerNotification {
    Item(CodexItemNotification),
    AgentMessageDelta(CodexAgentMessageDelta),
    TurnTerminal(CodexTurnTerminal),
    RequestResolved(CodexRequestResolved),
    ThreadDeleted(CodexThreadDeleted),
    ThreadStarted(CodexThreadStarted),
    Warning(CodexWarning),
    GuardianWarning(CodexWarning),
    ConnectionProcess(CodexConnectionProcessNotification),
    Known(CodexKnownNotification),
    UnknownFuture(CodexUnknownNotification),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexAgentMessageDelta {
    thread_id: String,
    turn_id: String,
    item_id: String,
    delta: String,
}

impl CodexAgentMessageDelta {
    fn decode(params: Value) -> Result<Self, KitError> {
        let delta: Self = serde_json::from_value(params)
            .map_err(|_| KitError::protocol("assistant output delta is malformed"))?;
        if delta.thread_id.is_empty() || delta.turn_id.is_empty() || delta.item_id.is_empty() {
            return Err(KitError::protocol(
                "assistant output delta has an empty owner",
            ));
        }
        Ok(delta)
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }
    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }
    pub(crate) fn text(&self) -> &str {
        &self.delta
    }
}

pub(crate) struct CodexItemNotification {
    thread_id: String,
    turn_id: String,
    lifecycle: CodexItemLifecycle,
    item: CodexThreadItem,
}

pub(crate) enum CodexThreadItem {
    AgentMessage(CodexAgentMessage),
    CommandExecution(CodexCommandExecution),
    Other(CodexOtherThreadItemKind),
}

#[derive(Deserialize)]
pub(crate) struct CodexAgentMessage {
    text: String,
    phase: Option<CodexMessagePhase>,
    delivery: Option<CodexMessageDelivery>,
    questions: Option<Vec<CodexAsyncQuestion>>,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum CodexMessagePhase {
    Commentary,
    PartialAnswer,
    FinalAnswer,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexMessageDelivery {
    Async,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CodexAsyncQuestion {
    #[serde(rename = "title")]
    _title: String,
    #[serde(rename = "options")]
    _options: Option<Vec<String>>,
}

pub(crate) struct CodexCommandExecution {
    status: CodexCommandExecutionStatus,
    exit_code: Option<i32>,
}

pub(crate) struct CodexTurnTerminal {
    thread_id: String,
    turn_id: String,
    status: CodexTurnStatus,
    error: Option<CodexTurnError>,
}

pub(crate) struct CodexRequestResolved {
    thread_id: String,
    request_id: Value,
}

pub(crate) struct CodexThreadDeleted {
    thread_id: String,
}

pub(crate) struct CodexThreadStarted {
    thread_id: String,
    thread_source: Option<String>,
}

pub(crate) struct CodexWarning {
    scope: CodexScope,
    message: String,
}

pub(crate) enum CodexConnectionProcessNotification {
    OutputDelta {
        _process_handle: String,
        _delta_base64: String,
        _cap_reached: bool,
    },
    Exited {
        _process_handle: String,
        _exit_code: i32,
        _stdout: String,
        _stdout_cap_reached: bool,
        _stderr: String,
        _stderr_cap_reached: bool,
    },
}

pub(crate) struct CodexKnownNotification {
    scope: CodexScope,
    kind: CodexKnownNotificationKind,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexErrorNotification {
    #[serde(rename = "error")]
    _error: CodexTurnError,
    #[serde(rename = "willRetry")]
    _will_retry: bool,
    #[serde(rename = "threadId")]
    _thread_id: String,
    #[serde(rename = "turnId")]
    _turn_id: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexTurnError {
    #[serde(rename = "message")]
    message: String,
    #[serde(
        default,
        rename = "codexErrorInfo",
        deserialize_with = "CodexErrorInfo::deserialize_optional"
    )]
    codex_error_info: Option<CodexErrorInfo>,
    #[serde(default, rename = "additionalDetails")]
    _additional_details: Option<String>,
    #[serde(default, rename = "misalignment")]
    _misalignment: Option<CodexMisalignment>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexMisalignment {
    #[serde(rename = "errorType")]
    _error_type: Option<String>,
    #[serde(rename = "detailedExplanation")]
    _detailed_explanation: Option<String>,
    #[serde(rename = "steer")]
    _steer: Option<CodexMisalignmentSteer>,
    #[serde(rename = "reviewTarget")]
    _review_target: Option<String>,
}

#[derive(Clone, Deserialize)]
struct CodexMisalignmentSteer {
    #[serde(rename = "message")]
    _message: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexErrorInfo {
    ContextWindowExceeded,
    SessionBudgetExceeded,
    UsageLimitExceeded,
    RateLimitExceeded,
    FlexUnavailable,
    MisalignmentPolicyViolation,
    TooManyDenials,
    ServerOverloaded,
    CyberPolicy,
    HttpConnectionFailed {
        #[serde(rename = "httpStatusCode")]
        http_status_code: Option<u16>,
    },
    ResponseStreamConnectionFailed {
        #[serde(rename = "httpStatusCode")]
        http_status_code: Option<u16>,
    },
    InternalServerError,
    Unauthorized,
    BadRequest,
    ThreadRollbackFailed,
    SandboxError,
    ResponseStreamDisconnected {
        #[serde(rename = "httpStatusCode")]
        http_status_code: Option<u16>,
    },
    ResponseTooManyFailedAttempts {
        #[serde(rename = "httpStatusCode")]
        http_status_code: Option<u16>,
    },
    ActiveTurnNotSteerable {
        #[serde(rename = "turnKind")]
        _turn_kind: CodexNonSteerableTurnKind,
    },
    Other,
}

impl CodexErrorInfo {
    fn deserialize_optional<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Self>, D::Error> {
        let Some(value) = Option::<Value>::deserialize(deserializer)? else {
            return Ok(None);
        };
        let recognized = |name: &str| {
            matches!(
                name,
                "contextWindowExceeded"
                    | "sessionBudgetExceeded"
                    | "usageLimitExceeded"
                    | "rateLimitExceeded"
                    | "flexUnavailable"
                    | "serverOverloaded"
                    | "cyberPolicy"
                    | "misalignmentPolicyViolation"
                    | "tooManyDenials"
                    | "httpConnectionFailed"
                    | "responseStreamConnectionFailed"
                    | "internalServerError"
                    | "unauthorized"
                    | "badRequest"
                    | "threadRollbackFailed"
                    | "sandboxError"
                    | "responseStreamDisconnected"
                    | "responseTooManyFailedAttempts"
                    | "activeTurnNotSteerable"
                    | "other"
            )
        };
        let known = match &value {
            Value::String(name) => recognized(name),
            Value::Object(fields) => fields.keys().any(|name| recognized(name)),
            _ => return Err(serde::de::Error::custom("invalid error classification")),
        };
        if known {
            serde_json::from_value(value)
                .map(Some)
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Some(Self::Other))
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexNonSteerableTurnKind {
    Review,
    Compact,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexTurnDiffUpdatedNotification {
    #[serde(rename = "threadId")]
    _thread_id: String,
    #[serde(rename = "turnId")]
    _turn_id: String,
    #[serde(rename = "diff")]
    _diff: String,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum CodexCommandAction {
    Read {
        #[serde(rename = "command")]
        _command: String,
        #[serde(rename = "name")]
        _name: String,
        #[serde(rename = "path")]
        _path: String,
    },
    ListFiles {
        #[serde(rename = "command")]
        _command: String,
        #[serde(rename = "path")]
        _path: Option<String>,
    },
    Search {
        #[serde(rename = "command")]
        _command: String,
        #[serde(rename = "query")]
        _query: Option<String>,
        #[serde(rename = "path")]
        _path: Option<String>,
    },
    Unknown {
        #[serde(rename = "command")]
        _command: String,
    },
}

pub(crate) struct CodexUnknownNotification {
    method: String,
    scope: CodexScope,
    _payload: Value,
}

#[derive(Clone, Copy)]
pub(crate) enum CodexTurnStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
}

#[derive(Clone, Copy)]
pub(crate) enum CodexItemLifecycle {
    Started,
    Completed,
}

#[derive(Clone, Copy)]
pub(crate) enum CodexCommandExecutionStatus {
    InProgress,
    Completed,
    Failed,
    Declined,
}

pub(crate) enum CodexOtherThreadItemKind {
    UserMessage,
    HookPrompt,
    Plan,
    Reasoning,
    FileChange,
    McpToolCall,
    ToolOutput,
    WebSearch,
    ImageView,
    ImageGeneration,
    Sleep,
    EnteredReviewMode,
    ExitedReviewMode,
    ContextCompaction,
}

#[derive(Clone, Copy)]
pub(crate) enum CodexKnownNotificationKind {
    ThreadReadStateChanged,
    ThreadReverted,
    ThreadQueueChanged,
    ThreadAttachmentUpdated,
    ThreadPredictionUpdated,
    ProjectChanged,
    ThreadProjectUpdated,
    EnvironmentConnected,
    EnvironmentDisconnected,
    StrictReviewRequired,
    RawResponseCompleted,
    McpEvent,
    GatewayOAuthChanged,
    AuthRecoveryStarted,
    AuthRecoveryCompleted,
    RealtimeItemStarted,
    RealtimeTranscriptDelta,
    RealtimeItemCompleted,
    Error,
    TurnStarted,
    HookStarted,
    HookCompleted,
    TurnDiffUpdated,
    TurnPlanUpdated,
    ItemGuardianApprovalReviewStarted,
    ItemGuardianApprovalReviewCompleted,
    RawResponseItemCompleted,
    PlanDelta,
    CommandExecOutputDelta,
    CommandExecutionOutputDelta,
    TerminalInteraction,
    FileChangeOutputDelta,
    FileChangePatchUpdated,
    McpToolCallProgress,
    ReasoningSummaryTextDelta,
    ReasoningSummaryPartAdded,
    ReasoningTextDelta,
    TurnModerationMetadata,
    ModelRerouted,
    ModelVerification,
    ModelSafetyBufferingUpdated,
    ThreadGoalUpdated,
    ThreadTokenUsageUpdated,
    ThreadCompacted,
    ThreadStatusChanged,
    ThreadArchived,
    ThreadUnarchived,
    ThreadClosed,
    ThreadNameUpdated,
    ThreadGoalCleared,
    ThreadSettingsUpdated,
    ThreadRealtimeStarted,
    ThreadRealtimeItemAdded,
    ThreadRealtimeTranscriptDelta,
    ThreadRealtimeTranscriptDone,
    ThreadRealtimeOutputAudioDelta,
    ThreadRealtimeSdp,
    ThreadRealtimeError,
    ThreadRealtimeClosed,
    SkillsChanged,
    McpServerOauthLoginCompleted,
    McpServerStartupStatusUpdated,
    AccountUpdated,
    AccountRateLimitsUpdated,
    AppListUpdated,
    RemoteControlStatusChanged,
    ExternalAgentConfigImportProgress,
    ExternalAgentConfigImportCompleted,
    FsChanged,
    DeprecationNotice,
    ConfigWarning,
    FuzzyFileSearchSessionUpdated,
    FuzzyFileSearchSessionCompleted,
    WindowsWorldWritableWarning,
    WindowsSandboxSetupCompleted,
    AccountLoginCompleted,
}

pub(crate) enum CodexScope {
    Connection,
    Thread { thread_id: String },
    Turn { thread_id: String, turn_id: String },
}

impl CodexServerNotification {
    pub(crate) fn decode(method: String, params: Value) -> Result<Self, KitError> {
        match method.as_str() {
            "thread/readState/changed"
            | "thread/reverted"
            | "thread/queue/changed"
            | "thread/attachment/updated"
            | "thread/prediction/updated"
            | "project/changed"
            | "thread/project/updated"
            | "thread/environment/connected"
            | "thread/environment/disconnected"
            | "autoApprovalReview/strictReviewRequired"
            | "rawResponse/completed"
            | "mcpServer/event/stream/notification"
            | "account/gatewayOAuth/changed"
            | "modelProvider/authRecoveryStarted"
            | "modelProvider/authRecoveryCompleted"
            | "thread/realtime/item/started"
            | "thread/realtime/item/completed"
            | "thread/realtime/item/transcript/delta" => {
                CodexMetadataNotification::decode(&method, params)
            }
            "item/started" => Self::item(params, CodexItemLifecycle::Started),
            "item/completed" => Self::item(params, CodexItemLifecycle::Completed),
            "turn/completed" => Self::turn_completed(params).map(Self::TurnTerminal),
            "serverRequest/resolved" => Self::request_resolved(params).map(Self::RequestResolved),
            "thread/deleted" => Ok(Self::ThreadDeleted(CodexThreadDeleted {
                thread_id: CodexScope::thread_id(&params)?.to_owned(),
            })),
            "warning" => Self::warning(params).map(Self::Warning),
            "guardianWarning" => Self::guardian_warning(params).map(Self::GuardianWarning),
            "process/outputDelta" => Self::process_output(params).map(Self::ConnectionProcess),
            "process/exited" => Self::process_exited(params).map(Self::ConnectionProcess),
            "turn/started" => Self::known(
                CodexScope::started_turn(&params)?,
                CodexKnownNotificationKind::TurnStarted,
            ),
            "hook/started" => Self::known(
                CodexScope::optional_turn(&params)?,
                CodexKnownNotificationKind::HookStarted,
            ),
            "hook/completed" => Self::known(
                CodexScope::optional_turn(&params)?,
                CodexKnownNotificationKind::HookCompleted,
            ),
            "error" => {
                serde_json::from_value::<CodexErrorNotification>(params.clone())
                    .map_err(|_| KitError::protocol("error notification is malformed"))?;
                Self::known_turn(&params, CodexKnownNotificationKind::Error)
            }
            "turn/diff/updated" => {
                serde_json::from_value::<CodexTurnDiffUpdatedNotification>(params.clone())
                    .map_err(|_| {
                        KitError::protocol("prompt execution diff notification is malformed")
                    })?;
                Self::known_turn(&params, CodexKnownNotificationKind::TurnDiffUpdated)
            }
            "turn/plan/updated" => {
                Self::known_turn(&params, CodexKnownNotificationKind::TurnPlanUpdated)
            }
            "item/autoApprovalReview/started" => Self::known_turn(
                &params,
                CodexKnownNotificationKind::ItemGuardianApprovalReviewStarted,
            ),
            "item/autoApprovalReview/completed" => Self::known_turn(
                &params,
                CodexKnownNotificationKind::ItemGuardianApprovalReviewCompleted,
            ),
            "rawResponseItem/completed" => Self::known_turn(
                &params,
                CodexKnownNotificationKind::RawResponseItemCompleted,
            ),
            "item/agentMessage/delta" => {
                CodexAgentMessageDelta::decode(params).map(Self::AgentMessageDelta)
            }
            "item/plan/delta" => Self::known_turn(&params, CodexKnownNotificationKind::PlanDelta),
            "command/exec/outputDelta" => {
                Self::known_connection(CodexKnownNotificationKind::CommandExecOutputDelta)
            }
            "item/commandExecution/outputDelta" => Self::known_turn(
                &params,
                CodexKnownNotificationKind::CommandExecutionOutputDelta,
            ),
            "item/commandExecution/terminalInteraction" => {
                Self::known_turn(&params, CodexKnownNotificationKind::TerminalInteraction)
            }
            "item/fileChange/outputDelta" => {
                Self::known_turn(&params, CodexKnownNotificationKind::FileChangeOutputDelta)
            }
            "item/fileChange/patchUpdated" => {
                Self::known_turn(&params, CodexKnownNotificationKind::FileChangePatchUpdated)
            }
            "item/mcpToolCall/progress" => {
                Self::known_turn(&params, CodexKnownNotificationKind::McpToolCallProgress)
            }
            "item/reasoning/summaryTextDelta" => Self::known_turn(
                &params,
                CodexKnownNotificationKind::ReasoningSummaryTextDelta,
            ),
            "item/reasoning/summaryPartAdded" => Self::known_turn(
                &params,
                CodexKnownNotificationKind::ReasoningSummaryPartAdded,
            ),
            "item/reasoning/textDelta" => {
                Self::known_turn(&params, CodexKnownNotificationKind::ReasoningTextDelta)
            }
            "turn/moderationMetadata" => {
                Self::known_turn(&params, CodexKnownNotificationKind::TurnModerationMetadata)
            }
            "model/rerouted" => {
                Self::known_turn(&params, CodexKnownNotificationKind::ModelRerouted)
            }
            "model/verification" => {
                Self::known_turn(&params, CodexKnownNotificationKind::ModelVerification)
            }
            "model/safetyBuffering/updated" => Self::known_turn(
                &params,
                CodexKnownNotificationKind::ModelSafetyBufferingUpdated,
            ),
            "thread/started" => Self::thread_started(params).map(Self::ThreadStarted),
            "thread/goal/updated" => Self::known(
                CodexScope::optional_turn(&params)?,
                CodexKnownNotificationKind::ThreadGoalUpdated,
            ),
            "thread/tokenUsage/updated" => {
                Self::known_turn(&params, CodexKnownNotificationKind::ThreadTokenUsageUpdated)
            }
            "thread/compacted" => {
                Self::known_turn(&params, CodexKnownNotificationKind::ThreadCompacted)
            }
            "thread/status/changed" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadStatusChanged)
            }
            "thread/archived" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadArchived)
            }
            "thread/unarchived" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadUnarchived)
            }
            "thread/closed" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadClosed)
            }
            "thread/name/updated" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadNameUpdated)
            }
            "thread/goal/cleared" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadGoalCleared)
            }
            "thread/settings/updated" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadSettingsUpdated)
            }
            "thread/realtime/started" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadRealtimeStarted)
            }
            "thread/realtime/itemAdded" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadRealtimeItemAdded)
            }
            "thread/realtime/transcript/delta" => Self::known_thread(
                &params,
                CodexKnownNotificationKind::ThreadRealtimeTranscriptDelta,
            ),
            "thread/realtime/transcript/done" => Self::known_thread(
                &params,
                CodexKnownNotificationKind::ThreadRealtimeTranscriptDone,
            ),
            "thread/realtime/outputAudio/delta" => Self::known_thread(
                &params,
                CodexKnownNotificationKind::ThreadRealtimeOutputAudioDelta,
            ),
            "thread/realtime/sdp" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadRealtimeSdp)
            }
            "thread/realtime/error" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadRealtimeError)
            }
            "thread/realtime/closed" => {
                Self::known_thread(&params, CodexKnownNotificationKind::ThreadRealtimeClosed)
            }
            "skills/changed" => Self::known_connection(CodexKnownNotificationKind::SkillsChanged),
            "mcpServer/oauthLogin/completed" => {
                Self::known_connection(CodexKnownNotificationKind::McpServerOauthLoginCompleted)
            }
            "mcpServer/startupStatus/updated" => {
                Self::known_connection(CodexKnownNotificationKind::McpServerStartupStatusUpdated)
            }
            "account/updated" => Self::known_connection(CodexKnownNotificationKind::AccountUpdated),
            "account/rateLimits/updated" => {
                Self::known_connection(CodexKnownNotificationKind::AccountRateLimitsUpdated)
            }
            "app/list/updated" => {
                Self::known_connection(CodexKnownNotificationKind::AppListUpdated)
            }
            "remoteControl/status/changed" => {
                Self::known_connection(CodexKnownNotificationKind::RemoteControlStatusChanged)
            }
            "externalAgentConfig/import/progress" => Self::known_connection(
                CodexKnownNotificationKind::ExternalAgentConfigImportProgress,
            ),
            "externalAgentConfig/import/completed" => Self::known_connection(
                CodexKnownNotificationKind::ExternalAgentConfigImportCompleted,
            ),
            "fs/changed" => Self::known_connection(CodexKnownNotificationKind::FsChanged),
            "deprecationNotice" => {
                Self::known_connection(CodexKnownNotificationKind::DeprecationNotice)
            }
            "configWarning" => Self::known_connection(CodexKnownNotificationKind::ConfigWarning),
            "fuzzyFileSearch/sessionUpdated" => {
                Self::known_connection(CodexKnownNotificationKind::FuzzyFileSearchSessionUpdated)
            }
            "fuzzyFileSearch/sessionCompleted" => {
                Self::known_connection(CodexKnownNotificationKind::FuzzyFileSearchSessionCompleted)
            }
            "windows/worldWritableWarning" => {
                Self::known_connection(CodexKnownNotificationKind::WindowsWorldWritableWarning)
            }
            "windowsSandbox/setupCompleted" => {
                Self::known_connection(CodexKnownNotificationKind::WindowsSandboxSetupCompleted)
            }
            "account/login/completed" => {
                Self::known_connection(CodexKnownNotificationKind::AccountLoginCompleted)
            }
            _ => Ok(Self::UnknownFuture(CodexUnknownNotification {
                method,
                scope: CodexScope::inferred(&params)?,
                _payload: params,
            })),
        }
    }

    fn item(params: Value, lifecycle: CodexItemLifecycle) -> Result<Self, KitError> {
        let timestamp_field = match lifecycle {
            CodexItemLifecycle::Started => "startedAtMs",
            CodexItemLifecycle::Completed => "completedAtMs",
        };
        params
            .get(timestamp_field)
            .and_then(Value::as_i64)
            .ok_or_else(|| KitError::protocol("runtime event lifecycle time is malformed"))?;
        let CodexScope::Turn { thread_id, turn_id } = CodexScope::turn(&params)? else {
            return Err(KitError::protocol("runtime event scope is malformed"));
        };
        let item = params
            .get("item")
            .and_then(Value::as_object)
            .ok_or_else(|| KitError::protocol("runtime event is malformed"))?;
        let item_type = item
            .get("type")
            .and_then(Value::as_str)
            .filter(|item_type| !item_type.is_empty())
            .ok_or_else(|| KitError::protocol("runtime event type is malformed"))?;
        item.get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && id.trim() == *id)
            .ok_or_else(|| KitError::protocol("runtime event id is malformed"))?;
        let item = match item_type {
            "agentMessage" => CodexThreadItem::AgentMessage(CodexAgentMessage::decode(item)?),
            "commandExecution" => {
                let command = CodexCommandExecution::decode(item)?;
                let lifecycle_matches_status = matches!(
                    (lifecycle, command.status()),
                    (
                        CodexItemLifecycle::Started,
                        CodexCommandExecutionStatus::InProgress
                    ) | (
                        CodexItemLifecycle::Completed,
                        CodexCommandExecutionStatus::Completed
                            | CodexCommandExecutionStatus::Failed
                            | CodexCommandExecutionStatus::Declined
                    )
                );
                if !lifecycle_matches_status {
                    return Err(KitError::protocol(
                        "command runtime event lifecycle contradicts its status",
                    ));
                }
                CodexThreadItem::CommandExecution(command)
            }
            "userMessage" => CodexThreadItem::Other(CodexOtherThreadItemKind::UserMessage),
            "hookPrompt" => CodexThreadItem::Other(CodexOtherThreadItemKind::HookPrompt),
            "plan" => CodexThreadItem::Other(CodexOtherThreadItemKind::Plan),
            "reasoning" => CodexThreadItem::Other(CodexOtherThreadItemKind::Reasoning),
            "fileChange" => CodexThreadItem::Other(CodexOtherThreadItemKind::FileChange),
            "mcpToolCall" => CodexThreadItem::Other(CodexOtherThreadItemKind::McpToolCall),
            "functionCallOutput"
            | "dynamicToolCall"
            | "collabAgentToolCall"
            | "subAgentActivity" => {
                super::items::CodexAuxiliaryItem::decode(item)?.validate_support()?;
                CodexThreadItem::Other(CodexOtherThreadItemKind::ToolOutput)
            }
            "webSearch" => CodexThreadItem::Other(CodexOtherThreadItemKind::WebSearch),
            "imageView" => CodexThreadItem::Other(CodexOtherThreadItemKind::ImageView),
            "imageGeneration" => CodexThreadItem::Other(CodexOtherThreadItemKind::ImageGeneration),
            "sleep" => CodexThreadItem::Other(CodexOtherThreadItemKind::Sleep),
            "enteredReviewMode" => {
                CodexThreadItem::Other(CodexOtherThreadItemKind::EnteredReviewMode)
            }
            "exitedReviewMode" => {
                CodexThreadItem::Other(CodexOtherThreadItemKind::ExitedReviewMode)
            }
            "contextCompaction" => {
                CodexThreadItem::Other(CodexOtherThreadItemKind::ContextCompaction)
            }
            _ => return Err(KitError::protocol("runtime event type is not selected")),
        };
        Ok(Self::Item(CodexItemNotification {
            thread_id,
            turn_id,
            lifecycle,
            item,
        }))
    }

    fn turn_completed(params: Value) -> Result<CodexTurnTerminal, KitError> {
        let thread_id = CodexScope::thread_id(&params)?.to_owned();
        let turn = Self::turn(&params)?;
        let turn_id = turn
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .ok_or_else(|| KitError::protocol("terminal prompt execution id is malformed"))?
            .to_owned();
        let status = match turn.get("status").and_then(Value::as_str) {
            Some("completed") => CodexTurnStatus::Completed,
            Some("interrupted") => CodexTurnStatus::Interrupted,
            Some("failed") => CodexTurnStatus::Failed,
            Some("inProgress") => CodexTurnStatus::InProgress,
            _ => {
                return Err(KitError::protocol(
                    "terminal prompt execution status is malformed",
                ));
            }
        };
        let error = match turn.get("error") {
            None | Some(Value::Null) => None,
            Some(error) => Some(CodexTurnError::decode(error.clone())?),
        };
        Ok(CodexTurnTerminal {
            thread_id,
            turn_id,
            status,
            error,
        })
    }

    fn turn(params: &Value) -> Result<&serde_json::Map<String, Value>, KitError> {
        let turn = params
            .get("turn")
            .and_then(Value::as_object)
            .ok_or_else(|| KitError::protocol("prompt execution is malformed"))?;
        turn.get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .ok_or_else(|| KitError::protocol("prompt execution id is malformed"))?;
        turn.get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| KitError::protocol("prompt execution items are malformed"))?;
        if let Some(view) = turn.get("itemsView")
            && !matches!(view.as_str(), Some("notLoaded" | "summary" | "full"))
        {
            return Err(KitError::protocol(
                "prompt execution runtime event view is malformed",
            ));
        }
        if !matches!(
            turn.get("status").and_then(Value::as_str),
            Some("completed" | "interrupted" | "failed" | "inProgress")
        ) {
            return Err(KitError::protocol("prompt execution status is malformed"));
        }
        for field in ["startedAt", "completedAt", "durationMs"] {
            if let Some(value) = turn.get(field)
                && !value.is_null()
                && value.as_i64().is_none()
            {
                return Err(KitError::protocol("prompt execution timing is malformed"));
            }
        }
        if let Some(error) = turn.get("error")
            && !error.is_null()
            && error
                .get("message")
                .and_then(Value::as_str)
                .filter(|message| !message.is_empty())
                .is_none()
        {
            return Err(KitError::protocol("prompt execution error is malformed"));
        }
        Ok(turn)
    }

    fn request_resolved(params: Value) -> Result<CodexRequestResolved, KitError> {
        let request_id = params
            .get("requestId")
            .filter(|value| value.as_str().is_some() || value.as_i64().is_some())
            .cloned()
            .ok_or_else(|| KitError::protocol("resolved request id is malformed"))?;
        Ok(CodexRequestResolved {
            thread_id: CodexScope::thread_id(&params)?.to_owned(),
            request_id,
        })
    }

    fn warning(params: Value) -> Result<CodexWarning, KitError> {
        let message = Self::required_string(&params, "message")?;
        let scope = match params.get("threadId") {
            None | Some(Value::Null) => CodexScope::Connection,
            Some(Value::String(thread_id))
                if !thread_id.is_empty() && thread_id.trim() == thread_id =>
            {
                CodexScope::Thread {
                    thread_id: thread_id.clone(),
                }
            }
            Some(_) => return Err(KitError::protocol("runtime warning scope is malformed")),
        };
        Ok(CodexWarning { scope, message })
    }

    fn guardian_warning(params: Value) -> Result<CodexWarning, KitError> {
        Ok(CodexWarning {
            scope: CodexScope::thread(&params)?,
            message: Self::required_string(&params, "message")?,
        })
    }

    fn process_output(params: Value) -> Result<CodexConnectionProcessNotification, KitError> {
        let process_handle = Self::string(&params, "processHandle")?;
        match params.get("stream").and_then(Value::as_str) {
            Some("stdout" | "stderr") => {}
            _ => return Err(KitError::protocol("runtime event stream is malformed")),
        }
        let delta_base64 = Self::string(&params, "deltaBase64")?;
        let cap_reached = params
            .get("capReached")
            .and_then(Value::as_bool)
            .ok_or_else(|| KitError::protocol("runtime event bound state is malformed"))?;
        Ok(CodexConnectionProcessNotification::OutputDelta {
            _process_handle: process_handle,
            _delta_base64: delta_base64,
            _cap_reached: cap_reached,
        })
    }

    fn process_exited(params: Value) -> Result<CodexConnectionProcessNotification, KitError> {
        let process_handle = Self::string(&params, "processHandle")?;
        let exit_code = params
            .get("exitCode")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| KitError::protocol("runtime event exit status is malformed"))?;
        let stdout = Self::string(&params, "stdout")?;
        let stdout_cap_reached = params
            .get("stdoutCapReached")
            .and_then(Value::as_bool)
            .ok_or_else(|| KitError::protocol("runtime event output bound is malformed"))?;
        let stderr = Self::string(&params, "stderr")?;
        let stderr_cap_reached = params
            .get("stderrCapReached")
            .and_then(Value::as_bool)
            .ok_or_else(|| KitError::protocol("runtime event error bound is malformed"))?;
        Ok(CodexConnectionProcessNotification::Exited {
            _process_handle: process_handle,
            _exit_code: exit_code,
            _stdout: stdout,
            _stdout_cap_reached: stdout_cap_reached,
            _stderr: stderr,
            _stderr_cap_reached: stderr_cap_reached,
        })
    }

    pub(super) fn known(
        scope: CodexScope,
        kind: CodexKnownNotificationKind,
    ) -> Result<Self, KitError> {
        Ok(Self::Known(CodexKnownNotification { scope, kind }))
    }

    fn thread_started(params: Value) -> Result<CodexThreadStarted, KitError> {
        let thread = params
            .get("thread")
            .and_then(Value::as_object)
            .ok_or_else(|| KitError::protocol("started conversation payload is malformed"))?;
        let thread_id = thread
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .ok_or_else(|| KitError::protocol("started conversation scope is malformed"))?
            .to_owned();
        let thread_source = match thread.get("threadSource") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if !value.is_empty() && value.trim() == value => {
                Some(value.clone())
            }
            Some(_) => {
                return Err(KitError::protocol(
                    "conversation creation state is malformed",
                ));
            }
        };
        Ok(CodexThreadStarted {
            thread_id,
            thread_source,
        })
    }

    fn known_turn(params: &Value, kind: CodexKnownNotificationKind) -> Result<Self, KitError> {
        Self::known(CodexScope::turn(params)?, kind)
    }

    fn known_thread(params: &Value, kind: CodexKnownNotificationKind) -> Result<Self, KitError> {
        Self::known(CodexScope::thread(params)?, kind)
    }

    fn known_connection(kind: CodexKnownNotificationKind) -> Result<Self, KitError> {
        Self::known(CodexScope::Connection, kind)
    }

    fn required_string(params: &Value, field: &str) -> Result<String, KitError> {
        params
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .map(str::to_owned)
            .ok_or_else(|| KitError::protocol("runtime event field is malformed"))
    }

    fn string(params: &Value, field: &str) -> Result<String, KitError> {
        params
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| KitError::protocol("runtime event field is malformed"))
    }
}

impl CodexItemNotification {
    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub(crate) fn lifecycle(&self) -> CodexItemLifecycle {
        self.lifecycle
    }

    pub(crate) fn into_item(self) -> CodexThreadItem {
        self.item
    }
}

impl CodexAgentMessage {
    fn decode(item: &serde_json::Map<String, Value>) -> Result<Self, KitError> {
        let message: Self = serde_json::from_value(Value::Object(item.clone()))
            .map_err(|_| KitError::protocol("agent message is malformed"))?;
        message.validate_support()?;
        Ok(message)
    }

    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    pub(crate) fn validate_support(&self) -> Result<(), KitError> {
        if self.delivery.is_some() || self.questions.is_some() {
            return Err(KitError::unsupported(
                "asynchronous questions have no portable response continuation",
            ));
        }
        Ok(())
    }

    pub(crate) fn is_eligible_output(&self) -> bool {
        matches!(self.phase, None | Some(CodexMessagePhase::FinalAnswer))
    }
}

impl CodexCommandExecution {
    fn decode(item: &serde_json::Map<String, Value>) -> Result<Self, KitError> {
        Self::string(item, "command")?;
        Self::string(item, "cwd")?;
        if !matches!(
            item.get("processId"),
            None | Some(Value::Null) | Some(Value::String(_))
        ) {
            return Err(KitError::protocol(
                "command runtime event process id is malformed",
            ));
        }
        if !matches!(
            item.get("source").and_then(Value::as_str),
            None | Some("agent" | "userShell" | "unifiedExecStartup" | "unifiedExecInteraction")
        ) {
            return Err(KitError::protocol(
                "command runtime event source is malformed",
            ));
        }
        let status = match item.get("status").and_then(Value::as_str) {
            Some("inProgress") => CodexCommandExecutionStatus::InProgress,
            Some("completed") => CodexCommandExecutionStatus::Completed,
            Some("failed") => CodexCommandExecutionStatus::Failed,
            Some("declined") => CodexCommandExecutionStatus::Declined,
            _ => {
                return Err(KitError::protocol(
                    "command runtime event status is malformed",
                ));
            }
        };
        serde_json::from_value::<Vec<CodexCommandAction>>(
            item.get("commandActions")
                .cloned()
                .ok_or_else(|| KitError::protocol("command runtime event actions are malformed"))?,
        )
        .map_err(|_| KitError::protocol("command runtime event actions are malformed"))?;
        if !matches!(
            item.get("aggregatedOutput"),
            None | Some(Value::Null) | Some(Value::String(_))
        ) {
            return Err(KitError::protocol(
                "command runtime event output is malformed",
            ));
        }
        let exit_code = match item.get("exitCode") {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())
                    .ok_or_else(|| {
                        KitError::protocol("command runtime event exit code is malformed")
                    })?,
            ),
        };
        if let Some(value) = item.get("durationMs")
            && !value.is_null()
            && value.as_i64().filter(|duration| *duration >= 0).is_none()
        {
            return Err(KitError::protocol(
                "command runtime event duration is malformed",
            ));
        }
        Ok(Self { status, exit_code })
    }

    fn string(item: &serde_json::Map<String, Value>, field: &str) -> Result<String, KitError> {
        item.get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| KitError::protocol("runtime event field is malformed"))
    }

    pub(crate) fn status(&self) -> CodexCommandExecutionStatus {
        self.status
    }

    pub(crate) fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }
}

impl CodexTurnTerminal {
    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub(crate) fn status(&self) -> CodexTurnStatus {
        self.status
    }

    pub(crate) fn error(&self) -> Option<&CodexTurnError> {
        self.error.as_ref()
    }
}

impl CodexTurnError {
    fn decode(value: Value) -> Result<Self, KitError> {
        let error: Self = serde_json::from_value(value).map_err(|_| {
            KitError::protocol("terminal error is malformed")
                .with_context(Operation::Prompt, ErrorPhase::ResponseDecode)
                .with_detail(ErrorDetail::MalformedData)
        })?;
        error.validate()?;
        Ok(error)
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.message.is_empty() {
            return Err(KitError::protocol("terminal error is malformed")
                .with_context(Operation::Prompt, ErrorPhase::ResponseDecode)
                .with_detail(ErrorDetail::MissingData));
        }
        Ok(())
    }

    pub(crate) fn to_error(&self) -> KitError {
        let (detail, status) = match &self.codex_error_info {
            Some(CodexErrorInfo::ContextWindowExceeded) => (ErrorDetail::ContextLimit, None),
            Some(CodexErrorInfo::SessionBudgetExceeded) => (ErrorDetail::SessionLimit, None),
            Some(CodexErrorInfo::UsageLimitExceeded) => (ErrorDetail::ResourceExhausted, None),
            Some(CodexErrorInfo::RateLimitExceeded) => (ErrorDetail::RateLimited, None),
            Some(CodexErrorInfo::FlexUnavailable | CodexErrorInfo::ServerOverloaded) => {
                (ErrorDetail::ServiceUnavailable, None)
            }
            Some(
                CodexErrorInfo::CyberPolicy
                | CodexErrorInfo::MisalignmentPolicyViolation
                | CodexErrorInfo::TooManyDenials,
            ) => (ErrorDetail::PolicyRejected, None),
            Some(
                CodexErrorInfo::HttpConnectionFailed { http_status_code }
                | CodexErrorInfo::ResponseStreamConnectionFailed { http_status_code }
                | CodexErrorInfo::ResponseStreamDisconnected { http_status_code },
            ) => (ErrorDetail::TransportFailure, *http_status_code),
            Some(CodexErrorInfo::InternalServerError) => (ErrorDetail::RemoteInternal, None),
            Some(CodexErrorInfo::Unauthorized) => (ErrorDetail::AuthenticationRejected, None),
            Some(CodexErrorInfo::BadRequest) => (ErrorDetail::InvalidRequest, None),
            Some(CodexErrorInfo::ActiveTurnNotSteerable { .. }) => {
                (ErrorDetail::PreconditionFailed, None)
            }
            Some(CodexErrorInfo::ResponseTooManyFailedAttempts { http_status_code }) => {
                (ErrorDetail::RemoteRejected, *http_status_code)
            }
            Some(
                CodexErrorInfo::ThreadRollbackFailed
                | CodexErrorInfo::SandboxError
                | CodexErrorInfo::Other,
            )
            | None => (ErrorDetail::RemoteRejected, None),
        };
        let error = KitError::integration("foreground prompt failed")
            .with_context(Operation::Prompt, ErrorPhase::ModelGeneration)
            .with_detail(detail);
        match status {
            Some(status) => error.with_http_status(status),
            None => error,
        }
    }
}

impl CodexRequestResolved {
    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn request_id(&self) -> &Value {
        &self.request_id
    }
}

impl CodexThreadDeleted {
    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }
}

impl CodexThreadStarted {
    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn thread_source(&self) -> Option<&str> {
        self.thread_source.as_deref()
    }
}

impl CodexWarning {
    pub(crate) fn thread_id(&self) -> Option<&str> {
        self.scope.thread_id_value()
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl CodexKnownNotification {
    pub(crate) fn into_scope(self) -> CodexScope {
        self.scope
    }

    pub(crate) fn kind(&self) -> CodexKnownNotificationKind {
        self.kind
    }

    pub(crate) fn thread_id(&self) -> Option<&str> {
        self.scope.thread_id_value()
    }
}

impl CodexUnknownNotification {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn thread_id(&self) -> Option<&str> {
        self.scope.thread_id_value()
    }
}

impl CodexScope {
    fn started_turn(params: &Value) -> Result<Self, KitError> {
        let thread_id = Self::thread_id(params)?.to_owned();
        let turn_id = CodexServerNotification::turn(params)?
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .ok_or_else(|| KitError::protocol("started prompt execution scope is malformed"))?;
        Ok(Self::Turn {
            thread_id,
            turn_id: turn_id.to_owned(),
        })
    }

    fn optional_turn(params: &Value) -> Result<Self, KitError> {
        match params.get("turnId") {
            None | Some(Value::Null) => Self::thread(params),
            Some(_) => Self::turn(params),
        }
    }

    fn inferred(params: &Value) -> Result<Self, KitError> {
        match (params.get("threadId"), params.get("turnId")) {
            (None, None) => Ok(Self::Connection),
            (Some(_), None) => Self::thread(params),
            (Some(_), Some(_)) => Self::turn(params),
            (None, Some(_)) => Err(KitError::protocol(
                "notification has a prompt execution without a conversation",
            )),
        }
    }

    fn thread(params: &Value) -> Result<Self, KitError> {
        Ok(Self::Thread {
            thread_id: Self::thread_id(params)?.to_owned(),
        })
    }

    fn turn(params: &Value) -> Result<Self, KitError> {
        Ok(Self::Turn {
            thread_id: Self::thread_id(params)?.to_owned(),
            turn_id: params
                .get("turnId")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty() && value.trim() == *value)
                .ok_or_else(|| KitError::protocol("prompt execution scope is malformed"))?
                .to_owned(),
        })
    }

    fn thread_id(params: &Value) -> Result<&str, KitError> {
        params
            .get("threadId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .ok_or_else(|| KitError::protocol("conversation scope is malformed"))
    }

    pub(crate) fn thread_id_value(&self) -> Option<&str> {
        match self {
            Self::Connection => None,
            Self::Thread { thread_id } | Self::Turn { thread_id, .. } => Some(thread_id),
        }
    }
}
