use crate::SdkError;
use crate::error::{ErrorDetail, ErrorKind, ErrorPhase};

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct OpenCodeGlobalEnvelope {
    #[serde(default)]
    directory: OpenCodeOptional<String>,
    #[serde(default)]
    project: OpenCodeOptional<String>,
    #[serde(default)]
    workspace: OpenCodeOptional<String>,
    payload: OpenCodeGlobalPayload,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct OpenCodeGlobalPayload {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    properties: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
    #[serde(rename = "syncEvent", default)]
    sync_event: OpenCodeOptional<OpenCodeLiveSyncEvent>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct OpenCodeLiveSyncEvent {
    id: String,
    #[serde(rename = "aggregateID")]
    aggregate_id: String,
    seq: u64,
    #[serde(rename = "type")]
    event_type: String,
    data: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct OpenCodeManifestEmpty {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) enum OpenCodeOptional<T> {
    #[default]
    Missing,
    Present(T),
}

impl<T: serde::Serialize> serde::Serialize for OpenCodeOptional<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Missing => serializer.serialize_none(),
            Self::Present(value) => value.serialize(serializer),
        }
    }
}

impl<'de, T> serde::Deserialize<'de> for OpenCodeOptional<T>
where
    T: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Present)
    }
}

impl<T> OpenCodeOptional<T> {
    fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    fn as_ref(&self) -> Option<&T> {
        match self {
            Self::Missing => None,
            Self::Present(value) => Some(value),
        }
    }

    fn is_none_or(&self, predicate: impl FnOnce(&T) -> bool) -> bool {
        self.as_ref().is_none_or(predicate)
    }

    pub(super) fn into_option(self) -> Option<T> {
        match self {
            Self::Missing => None,
            Self::Present(value) => Some(value),
        }
    }
}

impl OpenCodeOptional<String> {
    fn as_deref(&self) -> Option<&str> {
        self.as_ref().map(String::as_str)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct OpenCodePermissionRequest {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    permission: String,
    patterns: Vec<String>,
    #[serde(rename = "metadata")]
    _metadata: serde_json::Map<String, serde_json::Value>,
    #[serde(rename = "always")]
    _always: Vec<String>,
    #[serde(
        rename = "tool",
        default,
        skip_serializing_if = "OpenCodeOptional::is_missing"
    )]
    _tool: OpenCodeOptional<OpenCodeRequestTool>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodePermissionV2Request {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "action")]
    _action: String,
    #[serde(rename = "resources")]
    _resources: Vec<String>,
    #[serde(rename = "save", default)]
    _save: OpenCodeOptional<Vec<String>>,
    #[serde(rename = "metadata", default)]
    _metadata: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
    #[serde(rename = "source", default)]
    _source: OpenCodeOptional<OpenCodePermissionSource>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodePermissionSource {
    #[serde(rename = "type")]
    _kind: OpenCodePermissionSourceKind,
    #[serde(rename = "messageID")]
    _message_id: String,
    #[serde(rename = "callID")]
    _call_id: String,
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
enum OpenCodePermissionSourceKind {
    #[serde(rename = "tool")]
    Tool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct OpenCodeQuestionRequest {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    questions: Vec<OpenCodeQuestion>,
    #[serde(
        rename = "tool",
        default,
        skip_serializing_if = "OpenCodeOptional::is_missing"
    )]
    tool: OpenCodeOptional<OpenCodeRequestTool>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct OpenCodeRequestTool {
    #[serde(rename = "messageID")]
    message_id: String,
    #[serde(rename = "callID")]
    call_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct OpenCodeQuestion {
    question: String,
    header: String,
    #[serde(rename = "options")]
    _options: Vec<OpenCodeQuestionOption>,
    #[serde(rename = "multiple", default)]
    _multiple: bool,
    #[serde(rename = "custom", default = "OpenCodeQuestion::custom_default")]
    _custom: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct OpenCodeQuestionOption {
    #[serde(rename = "label")]
    _label: String,
    #[serde(rename = "description")]
    _description: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeSessionEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(skip)]
    native_share: bool,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct OpenCodeSessionRecordEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    info: OpenCodeSessionInfo,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSessionInfo {
    id: String,
    #[serde(rename = "slug")]
    _slug: String,
    #[serde(rename = "projectID")]
    _project_id: String,
    #[serde(rename = "workspaceID", default)]
    workspace_id: OpenCodeOptional<String>,
    #[serde(rename = "directory")]
    _directory: String,
    #[serde(rename = "path", default)]
    _path: OpenCodeOptional<String>,
    #[serde(rename = "parentID", default)]
    parent_id: OpenCodeOptional<String>,
    #[serde(default)]
    summary: OpenCodeOptional<OpenCodeSessionSummary>,
    #[serde(default)]
    cost: OpenCodeOptional<f64>,
    #[serde(default)]
    tokens: OpenCodeOptional<OpenCodeTokenUsage>,
    #[serde(rename = "share", default)]
    share: OpenCodeOptional<OpenCodeSessionShare>,
    #[serde(rename = "title")]
    _title: String,
    #[serde(rename = "agent", default)]
    _agent: OpenCodeOptional<String>,
    #[serde(rename = "model", default)]
    _model: OpenCodeOptional<OpenCodeModelReference>,
    #[serde(rename = "version")]
    _version: String,
    #[serde(rename = "metadata", default)]
    _metadata: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
    time: OpenCodeSessionTime,
    #[serde(rename = "permission", default)]
    _permission: OpenCodeOptional<Vec<OpenCodePermissionRule>>,
    #[serde(default)]
    revert: OpenCodeOptional<OpenCodeSessionRevert>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSessionSummary {
    additions: f64,
    deletions: f64,
    files: f64,
    #[serde(default)]
    diffs: OpenCodeOptional<Vec<OpenCodeFileDiff>>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSessionShare {
    #[serde(rename = "url")]
    _url: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSessionTime {
    #[serde(rename = "created")]
    _created: u64,
    #[serde(rename = "updated")]
    _updated: u64,
    #[serde(rename = "compacting", default)]
    _compacting: OpenCodeOptional<u64>,
    #[serde(default)]
    archived: OpenCodeOptional<f64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSessionRevert {
    #[serde(rename = "messageID")]
    message_id: String,
    #[serde(rename = "partID", default)]
    part_id: OpenCodeOptional<String>,
    #[serde(rename = "snapshot", default)]
    _snapshot: OpenCodeOptional<String>,
    #[serde(rename = "diff", default)]
    _diff: OpenCodeOptional<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeModelReference {
    #[serde(rename = "id")]
    _id: String,
    #[serde(rename = "providerID")]
    _provider_id: String,
    #[serde(rename = "variant", default)]
    _variant: OpenCodeOptional<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeTokenUsage {
    #[serde(default)]
    total: OpenCodeOptional<f64>,
    input: f64,
    output: f64,
    reasoning: f64,
    cache: OpenCodeTokenCache,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeTokenCache {
    read: f64,
    write: f64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeMessageUpdatedEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    info: OpenCodeMessageInfo,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenCodePromptMessageEvidence {
    session_id: String,
    message_id: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "role")]
enum OpenCodeMessageInfo {
    #[serde(rename = "user")]
    User {
        id: String,
        #[serde(rename = "sessionID")]
        session_id: String,
        time: OpenCodeUserMessageTime,
        #[serde(rename = "format", default)]
        _format: OpenCodeOptional<OpenCodeOutputFormat>,
        #[serde(default)]
        summary: OpenCodeOptional<OpenCodeMessageSummary>,
        #[serde(rename = "agent")]
        _agent: String,
        #[serde(rename = "model")]
        _model: OpenCodeProviderModelReference,
        #[serde(rename = "system", default)]
        _system: OpenCodeOptional<String>,
        #[serde(rename = "tools", default)]
        _tools: OpenCodeOptional<std::collections::BTreeMap<String, bool>>,
        #[serde(rename = "parentID", default)]
        parent_id: OpenCodeOptional<String>,
    },
    #[serde(rename = "assistant")]
    Assistant {
        id: String,
        #[serde(rename = "sessionID")]
        session_id: String,
        #[serde(rename = "time")]
        time: OpenCodeAssistantMessageTime,
        #[serde(rename = "error", default)]
        error: OpenCodeOptional<OpenCodeAssistantError>,
        #[serde(rename = "parentID")]
        parent_id: String,
        #[serde(rename = "modelID")]
        _model_id: String,
        #[serde(rename = "providerID")]
        _provider_id: String,
        #[serde(rename = "mode")]
        _mode: String,
        #[serde(rename = "agent")]
        _agent: String,
        #[serde(rename = "path")]
        _path: OpenCodeAssistantPath,
        #[serde(rename = "summary", default)]
        _summary: OpenCodeOptional<bool>,
        cost: f64,
        tokens: OpenCodeTokenUsage,
        #[serde(rename = "structured", default)]
        _structured: OpenCodeOptional<serde_json::Value>,
        #[serde(rename = "variant", default)]
        _variant: OpenCodeOptional<String>,
        #[serde(rename = "finish", default)]
        _finish: OpenCodeOptional<String>,
    },
}

#[derive(Debug, serde::Deserialize)]
#[serde(transparent)]
pub(super) struct OpenCodeForegroundMessageInfo(OpenCodeMessageInfo);

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeUserMessageTime {
    created: f64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeAssistantMessageTime {
    #[serde(rename = "created")]
    _created: u64,
    #[serde(rename = "completed", default)]
    _completed: OpenCodeOptional<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "type")]
enum OpenCodeOutputFormat {
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "json_schema")]
    JsonSchema {
        #[serde(rename = "schema")]
        _schema: serde_json::Map<String, serde_json::Value>,
        #[serde(
            rename = "retryCount",
            default = "OpenCodeOutputFormat::default_retry_count"
        )]
        _retry_count: u64,
    },
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeMessageSummary {
    #[serde(rename = "title", default)]
    _title: OpenCodeOptional<String>,
    #[serde(rename = "body", default)]
    _body: OpenCodeOptional<String>,
    diffs: Vec<OpenCodeFileDiff>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeAssistantPath {
    #[serde(rename = "cwd")]
    _cwd: String,
    #[serde(rename = "root")]
    _root: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeMessageRemovedEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "messageID")]
    message_id: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodePartUpdatedEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    part: OpenCodeMessagePart,
    time: f64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenCodePromptPartEvidence {
    session_id: String,
    message_id: String,
    part_id: String,
}

#[derive(Clone, Debug)]
pub(crate) enum OpenCodePartEffect {
    Process(OpenCodeProcessEffect),
    Files(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OpenCodeProcessEffect {
    Running {
        session_id: String,
        message_id: String,
        part_id: String,
    },
    Exited {
        session_id: String,
        message_id: String,
        part_id: String,
        exit_code: Option<i32>,
    },
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodePartRemovedEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "messageID")]
    message_id: String,
    #[serde(rename = "partID")]
    part_id: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodePartDeltaEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "messageID")]
    message_id: String,
    #[serde(rename = "partID")]
    part_id: String,
    #[serde(rename = "field")]
    field: String,
    #[serde(rename = "delta")]
    delta: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodePartBase {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "messageID")]
    message_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(transparent)]
pub(super) struct OpenCodeMessageParts(Vec<OpenCodeMessagePart>);

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "type")]
enum OpenCodeMessagePart {
    #[serde(rename = "snapshot")]
    Snapshot {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "snapshot")]
        _snapshot: String,
    },
    #[serde(rename = "patch")]
    Patch {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "hash")]
        _hash: String,
        #[serde(rename = "files")]
        files: Vec<String>,
    },
    #[serde(rename = "text")]
    Text {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "text")]
        _text: String,
        #[serde(rename = "synthetic", default)]
        _synthetic: OpenCodeOptional<bool>,
        #[serde(rename = "ignored", default)]
        _ignored: OpenCodeOptional<bool>,
        #[serde(rename = "time", default)]
        _time: OpenCodeOptional<OpenCodePartTime>,
        #[serde(rename = "metadata", default)]
        _metadata: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
    },
    #[serde(rename = "reasoning")]
    Reasoning {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "text")]
        _text: String,
        #[serde(rename = "metadata", default)]
        _metadata: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
        #[serde(rename = "time")]
        _time: OpenCodePartTime,
    },
    #[serde(rename = "file")]
    File {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "mime")]
        _mime: String,
        #[serde(rename = "filename", default)]
        _filename: OpenCodeOptional<String>,
        #[serde(rename = "url")]
        _url: String,
        #[serde(default)]
        source: OpenCodeOptional<OpenCodeFilePartSource>,
    },
    #[serde(rename = "agent")]
    Agent {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "name")]
        _name: String,
        #[serde(rename = "source", default)]
        _source: OpenCodeOptional<OpenCodeAgentPartSource>,
    },
    #[serde(rename = "compaction")]
    Compaction {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "auto")]
        _automatic: bool,
        #[serde(rename = "overflow", default)]
        _overflow: OpenCodeOptional<bool>,
        #[serde(default)]
        tail_start_id: OpenCodeOptional<String>,
    },
    #[serde(rename = "subtask")]
    Subtask {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "prompt")]
        _prompt: String,
        #[serde(rename = "description")]
        _description: String,
        #[serde(rename = "agent")]
        _agent: String,
        #[serde(rename = "model", default)]
        _model: OpenCodeOptional<OpenCodeProviderModelReference>,
        #[serde(rename = "command", default)]
        _command: OpenCodeOptional<String>,
    },
    #[serde(rename = "retry")]
    Retry {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "attempt")]
        _attempt: u64,
        #[serde(rename = "error")]
        error: OpenCodeApiError,
        #[serde(rename = "time")]
        _time: OpenCodeRetryTime,
    },
    #[serde(rename = "step-start")]
    StepStart {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "snapshot", default)]
        _snapshot: OpenCodeOptional<String>,
    },
    #[serde(rename = "step-finish")]
    StepFinish {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "reason")]
        _reason: String,
        #[serde(rename = "snapshot", default)]
        _snapshot: OpenCodeOptional<String>,
        cost: f64,
        tokens: OpenCodeTokenUsage,
    },
    #[serde(rename = "tool")]
    Tool {
        #[serde(flatten)]
        base: OpenCodePartBase,
        #[serde(rename = "callID")]
        _call_id: String,
        #[serde(rename = "tool")]
        tool: String,
        state: OpenCodeToolState,
        #[serde(rename = "metadata", default)]
        _metadata: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
    },
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodePartTime {
    #[serde(rename = "start")]
    _start: u64,
    #[serde(rename = "end", default)]
    _end: OpenCodeOptional<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "type")]
enum OpenCodeFilePartSource {
    #[serde(rename = "file")]
    File {
        #[serde(rename = "text")]
        _text: OpenCodeSourceText,
        #[serde(rename = "path")]
        _path: String,
    },
    #[serde(rename = "symbol")]
    Symbol {
        #[serde(rename = "text")]
        _text: OpenCodeSourceText,
        #[serde(rename = "path")]
        _path: String,
        #[serde(rename = "range")]
        _range: OpenCodeSourceRange,
        #[serde(rename = "name")]
        _name: String,
        #[serde(rename = "kind")]
        _kind: u64,
    },
    #[serde(rename = "resource")]
    Resource {
        #[serde(rename = "text")]
        _text: OpenCodeSourceText,
        #[serde(rename = "clientName")]
        _client_name: String,
        #[serde(rename = "uri")]
        _uri: String,
    },
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSourceText {
    #[serde(rename = "value")]
    _value: String,
    start: f64,
    end: f64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSourceRange {
    #[serde(rename = "start")]
    _start: OpenCodeSourcePosition,
    #[serde(rename = "end")]
    _end: OpenCodeSourcePosition,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeSourcePosition {
    #[serde(rename = "line")]
    _line: u64,
    #[serde(rename = "character")]
    _character: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeAgentPartSource {
    #[serde(rename = "value")]
    _value: String,
    #[serde(rename = "start")]
    _start: u64,
    #[serde(rename = "end")]
    _end: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeProviderModelReference {
    #[serde(rename = "providerID")]
    _provider_id: String,
    #[serde(rename = "modelID")]
    _model_id: String,
    #[serde(rename = "variant", default)]
    _variant: OpenCodeOptional<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeRetryTime {
    #[serde(rename = "created")]
    _created: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "status")]
enum OpenCodeToolState {
    #[serde(rename = "pending")]
    Pending {
        #[serde(rename = "input")]
        _input: serde_json::Map<String, serde_json::Value>,
        #[serde(rename = "raw")]
        _raw: String,
    },
    #[serde(rename = "running")]
    Running {
        #[serde(rename = "input")]
        _input: serde_json::Map<String, serde_json::Value>,
        #[serde(rename = "title", default)]
        _title: OpenCodeOptional<String>,
        #[serde(rename = "metadata", default)]
        _metadata: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
        #[serde(rename = "time")]
        _time: OpenCodeToolStartTime,
    },
    #[serde(rename = "completed")]
    Completed {
        #[serde(rename = "input")]
        _input: serde_json::Map<String, serde_json::Value>,
        #[serde(rename = "output")]
        _output: String,
        #[serde(rename = "title")]
        _title: String,
        #[serde(rename = "metadata")]
        metadata: serde_json::Map<String, serde_json::Value>,
        #[serde(rename = "time")]
        _time: OpenCodeToolCompletedTime,
        #[serde(default)]
        attachments: OpenCodeOptional<Vec<OpenCodeFilePart>>,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(rename = "input")]
        _input: serde_json::Map<String, serde_json::Value>,
        #[serde(rename = "error")]
        _error: String,
        #[serde(rename = "metadata", default)]
        _metadata: OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
        #[serde(rename = "time")]
        _time: OpenCodeToolErrorTime,
    },
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeToolStartTime {
    #[serde(rename = "start")]
    _start: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeToolCompletedTime {
    #[serde(rename = "start")]
    _start: u64,
    #[serde(rename = "end")]
    _end: u64,
    #[serde(rename = "compacted", default)]
    _compacted: OpenCodeOptional<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeToolErrorTime {
    #[serde(rename = "start")]
    _start: u64,
    #[serde(rename = "end")]
    _end: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeFilePart {
    #[serde(flatten)]
    base: OpenCodePartBase,
    #[serde(rename = "type")]
    _kind: OpenCodeFilePartKind,
    #[serde(rename = "mime")]
    _mime: String,
    #[serde(rename = "filename", default)]
    _filename: OpenCodeOptional<String>,
    #[serde(rename = "url")]
    _url: String,
    #[serde(default)]
    source: OpenCodeOptional<OpenCodeFilePartSource>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
enum OpenCodeFilePartKind {
    #[serde(rename = "file")]
    File,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "name", content = "data")]
enum OpenCodeApiError {
    #[serde(rename = "APIError")]
    Api(OpenCodeApiErrorData),
}

#[derive(Clone, Debug)]
pub(super) struct OpenCodeAssistantError {
    error: SdkError,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "name", content = "data")]
enum OpenCodeSelectedAssistantError {
    #[serde(rename = "ProviderAuthError")]
    Auth(OpenCodeProviderAuthErrorData),
    #[serde(rename = "UnknownError")]
    Unknown(OpenCodeUnknownErrorData),
    #[serde(rename = "MessageOutputLengthError")]
    OutputLength(OpenCodeEmptyErrorData),
    #[serde(rename = "MessageAbortedError")]
    Aborted(OpenCodeMessageErrorData),
    #[serde(rename = "StructuredOutputError")]
    StructuredOutput(OpenCodeStructuredOutputErrorData),
    #[serde(rename = "ContextOverflowError")]
    ContextOverflow(OpenCodeContextOverflowErrorData),
    #[serde(rename = "ContentFilterError")]
    ContentFilter(OpenCodeMessageErrorData),
    #[serde(rename = "APIError")]
    Api(OpenCodeApiErrorData),
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeProviderAuthErrorData {
    #[serde(rename = "providerID")]
    _provider_id: String,
    #[serde(rename = "message")]
    _message: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeUnknownErrorData {
    #[serde(rename = "message")]
    _message: String,
    #[serde(rename = "ref", default)]
    _reference: OpenCodeOptional<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeEmptyErrorData {}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeMessageErrorData {
    #[serde(rename = "message")]
    _message: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeStructuredOutputErrorData {
    #[serde(rename = "message")]
    _message: String,
    #[serde(rename = "retries")]
    _retries: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeContextOverflowErrorData {
    #[serde(rename = "message")]
    _message: String,
    #[serde(rename = "responseBody", default)]
    _response_body: OpenCodeOptional<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeApiErrorData {
    #[serde(rename = "message")]
    _message: String,
    #[serde(rename = "statusCode", default)]
    _status_code: OpenCodeOptional<u64>,
    #[serde(rename = "isRetryable")]
    _is_retryable: bool,
    #[serde(rename = "responseHeaders", default)]
    _response_headers: OpenCodeOptional<std::collections::BTreeMap<String, String>>,
    #[serde(rename = "responseBody", default)]
    _response_body: OpenCodeOptional<String>,
    #[serde(rename = "metadata", default)]
    _metadata: OpenCodeOptional<std::collections::BTreeMap<String, String>>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeSessionDiffEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    diff: Vec<OpenCodeFileDiff>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeFileDiff {
    #[serde(rename = "file", default)]
    file: OpenCodeOptional<String>,
    #[serde(rename = "patch", default)]
    _patch: OpenCodeOptional<String>,
    additions: f64,
    deletions: f64,
    #[serde(rename = "status", default)]
    _status: OpenCodeOptional<OpenCodeFileDiffStatus>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum OpenCodeFileDiffStatus {
    Added,
    Deleted,
    Modified,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct OpenCodeTodoUpdatedEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "todos")]
    _todos: Vec<OpenCodeTodo>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct OpenCodeTodo {
    #[serde(rename = "content")]
    _content: String,
    #[serde(rename = "status")]
    _status: String,
    #[serde(rename = "priority")]
    _priority: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeSessionErrorEvent {
    #[serde(rename = "sessionID", default)]
    session_id: OpenCodeOptional<String>,
    #[serde(rename = "error", default)]
    _error: OpenCodeOptional<OpenCodeAssistantError>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeInstanceDisposed {
    directory: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeSessionStatusEvent {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "status")]
    status: OpenCodeSessionStatus,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(super) enum OpenCodeSessionStatus {
    Idle,
    Busy,
    Retry {
        attempt: u64,
        message: String,
        #[serde(rename = "action", default)]
        _action: OpenCodeOptional<OpenCodeRetryAction>,
        next: u64,
    },
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct OpenCodeRetryAction {
    #[serde(rename = "reason")]
    _reason: String,
    #[serde(rename = "provider")]
    _provider: String,
    #[serde(rename = "title")]
    _title: String,
    #[serde(rename = "message")]
    _message: String,
    #[serde(rename = "label")]
    _label: String,
    #[serde(rename = "link", default)]
    _link: OpenCodeOptional<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodePermissionReplied {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "requestID")]
    request_id: String,
    reply: OpenCodePermissionReply,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OpenCodePermissionReply {
    Once,
    Always,
    Reject,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeQuestionReplied {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "requestID")]
    request_id: String,
    answers: Vec<Vec<String>>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeQuestionRejected {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "requestID")]
    request_id: String,
}

impl OpenCodeGlobalEnvelope {
    pub(super) fn into_parts(
        self,
    ) -> (
        Option<String>,
        Option<String>,
        Option<String>,
        OpenCodeGlobalPayload,
    ) {
        (
            self.directory.into_option(),
            self.project.into_option(),
            self.workspace.into_option(),
            self.payload,
        )
    }
}

impl OpenCodeGlobalPayload {
    pub(super) fn into_parts(
        self,
    ) -> (
        String,
        String,
        OpenCodeOptional<serde_json::Map<String, serde_json::Value>>,
        Option<OpenCodeLiveSyncEvent>,
    ) {
        (
            self.id,
            self.event_type,
            self.properties,
            self.sync_event.into_option(),
        )
    }
}

impl OpenCodeLiveSyncEvent {
    pub(super) fn into_parts(
        self,
    ) -> (
        String,
        String,
        u64,
        String,
        serde_json::Map<String, serde_json::Value>,
    ) {
        (
            self.id,
            self.aggregate_id,
            self.seq,
            self.event_type,
            self.data,
        )
    }
}

impl OpenCodePermissionRequest {
    pub(crate) fn validate_v1(self) -> Result<Self, crate::SdkError> {
        if !self.id.starts_with("per") || !self.session_id.starts_with("ses") {
            return Err(crate::SdkError::protocol(
                "v1 permission identity is malformed",
            ));
        }
        Ok(self)
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn permission(&self) -> &str {
        &self.permission
    }

    pub(crate) fn patterns(&self) -> &[String] {
        &self.patterns
    }

    pub(crate) fn tool_origin(&self) -> Option<&OpenCodeRequestTool> {
        self._tool.as_ref()
    }
}

impl OpenCodePermissionV2Request {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }
}

impl OpenCodeQuestionRequest {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn questions(&self) -> &[OpenCodeQuestion] {
        &self.questions
    }

    pub(crate) fn tool_origin(&self) -> Option<&OpenCodeRequestTool> {
        self.tool.as_ref()
    }

    pub(crate) fn validate_v1(self) -> Result<Self, crate::SdkError> {
        if !self.id.starts_with("que")
            || !self.session_id.starts_with("ses")
            || self
                .tool
                .as_ref()
                .is_some_and(|tool| !tool.message_id.starts_with("msg"))
        {
            return Err(crate::SdkError::protocol(
                "v1 question identity is malformed",
            ));
        }
        Ok(self)
    }
}

impl OpenCodeRequestTool {
    pub(crate) fn message_id(&self) -> &str {
        &self.message_id
    }

    pub(crate) fn call_id(&self) -> &str {
        &self.call_id
    }
}

impl OpenCodeQuestion {
    pub(crate) fn question(&self) -> &str {
        &self.question
    }

    pub(crate) fn header(&self) -> &str {
        &self.header
    }
}

impl OpenCodeSessionRecordEvent {
    pub(super) fn into_session(self) -> Result<OpenCodeSessionEvent, crate::SdkError> {
        if self.info.id != self.session_id || !self.info.is_valid() {
            return Err(crate::SdkError::protocol(
                "selected session record is malformed",
            ));
        }
        OpenCodeSessionEvent::from_record_id(self.session_id, self.info.share.as_ref().is_some())
    }
}

impl OpenCodeSessionInfo {
    fn is_valid(&self) -> bool {
        self.id.starts_with("ses")
            && self
                .workspace_id
                .as_deref()
                .is_none_or(|id| id.starts_with("wrk"))
            && self
                .parent_id
                .as_deref()
                .is_none_or(|id| id.starts_with("ses"))
            && self
                .summary
                .as_ref()
                .is_none_or(OpenCodeSessionSummary::is_valid)
            && self.cost.is_none_or(|value| value.is_finite())
            && self
                .tokens
                .as_ref()
                .is_none_or(OpenCodeTokenUsage::is_valid)
            && self.time.archived.is_none_or(|value| value.is_finite())
            && self
                .revert
                .as_ref()
                .is_none_or(OpenCodeSessionRevert::is_valid)
    }
}

impl OpenCodeSessionSummary {
    fn is_valid(&self) -> bool {
        self.additions.is_finite()
            && self.deletions.is_finite()
            && self.files.is_finite()
            && self
                .diffs
                .as_ref()
                .is_none_or(|diffs| diffs.iter().all(OpenCodeFileDiff::is_valid))
    }
}

impl OpenCodeSessionRevert {
    fn is_valid(&self) -> bool {
        self.message_id.starts_with("msg")
            && self
                .part_id
                .as_deref()
                .is_none_or(|id| id.starts_with("prt"))
    }
}

impl OpenCodeTokenUsage {
    fn is_valid(&self) -> bool {
        self.total.is_none_or(|value| value.is_finite())
            && self.input.is_finite()
            && self.output.is_finite()
            && self.reasoning.is_finite()
            && self.cache.read.is_finite()
            && self.cache.write.is_finite()
    }
}

impl OpenCodeMessageUpdatedEvent {
    pub(crate) fn validate(self) -> Result<Self, crate::SdkError> {
        if !self.info.is_valid_for(&self.session_id) {
            return Err(crate::SdkError::protocol(
                "selected message record is malformed",
            ));
        }
        Ok(self)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn assistant_observation(&self) -> Option<(&str, &str, bool)> {
        match &self.info {
            OpenCodeMessageInfo::Assistant {
                id,
                parent_id,
                time,
                ..
            } => Some((id, parent_id, time._completed.as_ref().is_some())),
            OpenCodeMessageInfo::User { .. } => None,
        }
    }

    pub(crate) fn prompt_message_evidence(&self) -> Option<OpenCodePromptMessageEvidence> {
        match &self.info {
            OpenCodeMessageInfo::User {
                id,
                session_id,
                parent_id,
                ..
            } if parent_id.as_ref().is_none() => Some(OpenCodePromptMessageEvidence {
                session_id: session_id.clone(),
                message_id: id.clone(),
            }),
            OpenCodeMessageInfo::User { .. } | OpenCodeMessageInfo::Assistant { .. } => None,
        }
    }
}

impl OpenCodePromptMessageEvidence {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn message_id(&self) -> &str {
        &self.message_id
    }
}

impl OpenCodeMessageInfo {
    fn is_valid_for(&self, expected_session_id: &str) -> bool {
        match self {
            Self::User {
                id,
                session_id,
                time,
                summary,
                parent_id,
                ..
            } => {
                id.starts_with("msg")
                    && id.trim() == id
                    && session_id == expected_session_id
                    && session_id.starts_with("ses")
                    && session_id.trim() == session_id
                    && time.created.is_finite()
                    && time.created >= 0.0
                    && parent_id.as_ref().is_none()
                    && summary
                        .as_ref()
                        .is_none_or(OpenCodeMessageSummary::is_valid)
            }
            Self::Assistant {
                id,
                session_id,
                parent_id,
                cost,
                tokens,
                ..
            } => {
                id.starts_with("msg")
                    && id.trim() == id
                    && session_id == expected_session_id
                    && session_id.starts_with("ses")
                    && session_id.trim() == session_id
                    && parent_id.starts_with("msg")
                    && parent_id.trim() == parent_id
                    && cost.is_finite()
                    && tokens.is_valid()
            }
        }
    }
}

impl OpenCodeForegroundMessageInfo {
    fn has_native_id(id: &str) -> bool {
        let Some(body) = id.strip_prefix("msg_") else {
            return false;
        };
        let bytes = body.as_bytes();
        bytes.len() == 26
            && bytes[..12]
                .iter()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            && bytes[12..].iter().all(u8::is_ascii_alphanumeric)
    }

    pub(super) fn message_id(&self) -> &str {
        match &self.0 {
            OpenCodeMessageInfo::User { id, .. } | OpenCodeMessageInfo::Assistant { id, .. } => id,
        }
    }

    pub(super) fn assistant_parent_id(
        &self,
        expected_session_id: &str,
    ) -> Result<&str, crate::SdkError> {
        match &self.0 {
            OpenCodeMessageInfo::Assistant {
                id,
                session_id,
                parent_id,
                time,
                ..
            } => {
                if !Self::has_native_id(id)
                    || !Self::has_native_id(parent_id)
                    || !self.0.is_valid_for(session_id)
                {
                    return Err(crate::SdkError::protocol(
                        "foreground response contains malformed Assistant metadata",
                    ));
                }
                if session_id != expected_session_id
                    || parent_id == id
                    || time._completed.as_ref().is_none()
                {
                    return Err(crate::SdkError::invalid_state(
                        "foreground response contradicts its prompt",
                    ));
                }
                Ok(parent_id)
            }
            OpenCodeMessageInfo::User { id, session_id, .. } => {
                if !Self::has_native_id(id) || !self.0.is_valid_for(session_id) {
                    return Err(crate::SdkError::protocol(
                        "foreground response contains malformed user metadata",
                    ));
                }
                Err(crate::SdkError::invalid_state(
                    "foreground response is not an Assistant",
                ))
            }
        }
    }

    pub(super) fn verify_prompt_parent(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
    ) -> Result<(), crate::SdkError> {
        match &self.0 {
            OpenCodeMessageInfo::User {
                id,
                session_id,
                parent_id,
                ..
            } => {
                if !Self::has_native_id(id) || !self.0.is_valid_for(session_id) {
                    return Err(crate::SdkError::protocol(
                        "foreground parent contains malformed user metadata",
                    ));
                }
                if session_id != expected_session_id
                    || id != expected_message_id
                    || parent_id.as_ref().is_some()
                {
                    return Err(crate::SdkError::invalid_state(
                        "foreground parent contradicts its prompt",
                    ));
                }
                Ok(())
            }
            OpenCodeMessageInfo::Assistant { id, session_id, .. } => {
                if !Self::has_native_id(id) || !self.0.is_valid_for(session_id) {
                    return Err(crate::SdkError::protocol(
                        "foreground parent contains malformed Assistant metadata",
                    ));
                }
                Err(crate::SdkError::invalid_state(
                    "foreground parent contradicts its prompt",
                ))
            }
        }
    }

    pub(super) fn cancelled_parent_id(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
    ) -> Result<&str, SdkError> {
        let parent_id = self.assistant_parent_id(expected_session_id)?;
        if self.message_id() != expected_message_id
            || !matches!(
                &self.0,
                OpenCodeMessageInfo::Assistant { error, .. }
                    if error.as_ref().is_some_and(|error| error.error.kind() == ErrorKind::Cancelled)
            )
        {
            return Err(SdkError::invalid_state(
                "interaction origin is not its cancelled Assistant",
            ));
        }
        Ok(parent_id)
    }

    pub(super) fn into_assistant(
        self,
        expected_session_id: &str,
    ) -> Result<(String, Option<OpenCodeAssistantError>), crate::SdkError> {
        self.assistant_parent_id(expected_session_id)?;
        match self.0 {
            OpenCodeMessageInfo::Assistant { id, error, .. } => Ok((id, error.into_option())),
            OpenCodeMessageInfo::User { .. } => Err(crate::SdkError::invalid_state(
                "foreground response is not an Assistant",
            )),
        }
    }
}

impl OpenCodeOutputFormat {
    fn default_retry_count() -> u64 {
        2
    }
}

impl OpenCodeMessageSummary {
    fn is_valid(&self) -> bool {
        self.diffs.iter().all(OpenCodeFileDiff::is_valid)
    }
}

impl OpenCodeMessageRemovedEvent {
    pub(super) fn validate(self) -> Result<Self, crate::SdkError> {
        if !self.message_id.starts_with("msg")
            || !self.session_id.starts_with("ses")
            || self.session_id.trim() != self.session_id
        {
            return Err(crate::SdkError::protocol(
                "selected message removal is malformed",
            ));
        }
        Ok(self)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }
    pub(crate) fn message_id(&self) -> &str {
        &self.message_id
    }
}

impl OpenCodePartUpdatedEvent {
    pub(crate) fn validate(self) -> Result<Self, crate::SdkError> {
        if !self.time.is_finite() || !self.part.is_valid_for(&self.session_id) {
            return Err(crate::SdkError::protocol(
                "selected message part is malformed",
            ));
        }
        Ok(self)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn prompt_part_evidence(&self) -> Option<OpenCodePromptPartEvidence> {
        match &self.part {
            OpenCodeMessagePart::Text {
                base, _synthetic, ..
            } if !_synthetic.as_ref().copied().unwrap_or(false) => {
                Some(OpenCodePromptPartEvidence {
                    session_id: base.session_id.clone(),
                    message_id: base.message_id.clone(),
                    part_id: base.id.clone(),
                })
            }
            _ => None,
        }
    }

    pub(crate) fn observation_part(&self) -> (&str, &str, bool) {
        let base = self.part.base();
        let observable = matches!(
            &self.part,
            OpenCodeMessagePart::Text { _synthetic, _ignored, .. }
                if !_synthetic.as_ref().copied().unwrap_or(false)
                    && !_ignored.as_ref().copied().unwrap_or(false)
        );
        (&base.id, &base.message_id, observable)
    }

    pub(crate) fn effect(&self) -> Result<Option<OpenCodePartEffect>, crate::SdkError> {
        match &self.part {
            OpenCodeMessagePart::Patch { files, .. } => {
                Ok(Some(OpenCodePartEffect::Files(files.clone())))
            }
            OpenCodeMessagePart::Tool {
                base, tool, state, ..
            } if tool == "bash" => {
                let effect = match state {
                    OpenCodeToolState::Pending { .. } => return Ok(None),
                    OpenCodeToolState::Running { .. } => OpenCodeProcessEffect::running(base),
                    OpenCodeToolState::Completed { metadata, .. } => {
                        let exit_code = match metadata.get("exit") {
                            Some(serde_json::Value::Null) => None,
                            Some(value) => {
                                let exit = value.as_i64().ok_or_else(|| {
                                    crate::SdkError::protocol("shell exit status is malformed")
                                })?;
                                Some(i32::try_from(exit).map_err(|_| {
                                    crate::SdkError::invalid_state(
                                        "shell exit status exceeds the portable range",
                                    )
                                })?)
                            }
                            None => {
                                return Err(crate::SdkError::protocol(
                                    "completed shell part lacks an exit status",
                                ));
                            }
                        };
                        OpenCodeProcessEffect::exited(base, exit_code)
                    }
                    OpenCodeToolState::Error { .. } => OpenCodeProcessEffect::exited(base, None),
                };
                Ok(Some(OpenCodePartEffect::Process(effect)))
            }
            OpenCodeMessagePart::Snapshot { .. }
            | OpenCodeMessagePart::Text { .. }
            | OpenCodeMessagePart::Reasoning { .. }
            | OpenCodeMessagePart::File { .. }
            | OpenCodeMessagePart::Agent { .. }
            | OpenCodeMessagePart::Compaction { .. }
            | OpenCodeMessagePart::Subtask { .. }
            | OpenCodeMessagePart::Retry { .. }
            | OpenCodeMessagePart::StepStart { .. }
            | OpenCodeMessagePart::StepFinish { .. }
            | OpenCodeMessagePart::Tool { .. } => Ok(None),
        }
    }
}

impl OpenCodeProcessEffect {
    fn running(base: &OpenCodePartBase) -> Self {
        Self::Running {
            session_id: base.session_id.clone(),
            message_id: base.message_id.clone(),
            part_id: base.id.clone(),
        }
    }

    fn exited(base: &OpenCodePartBase, exit_code: Option<i32>) -> Self {
        Self::Exited {
            session_id: base.session_id.clone(),
            message_id: base.message_id.clone(),
            part_id: base.id.clone(),
            exit_code,
        }
    }

    pub(crate) fn session_id(&self) -> &str {
        match self {
            Self::Running { session_id, .. } | Self::Exited { session_id, .. } => session_id,
        }
    }

    pub(crate) fn message_id(&self) -> &str {
        match self {
            Self::Running { message_id, .. } | Self::Exited { message_id, .. } => message_id,
        }
    }

    pub(crate) fn part_id(&self) -> &str {
        match self {
            Self::Running { part_id, .. } | Self::Exited { part_id, .. } => part_id,
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }
}

impl OpenCodePromptPartEvidence {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn message_id(&self) -> &str {
        &self.message_id
    }

    pub(crate) fn part_id(&self) -> &str {
        &self.part_id
    }
}

impl OpenCodePartRemovedEvent {
    pub(super) fn validate(self) -> Result<Self, crate::SdkError> {
        if !self.message_id.starts_with("msg")
            || !self.part_id.starts_with("prt")
            || !self.session_id.starts_with("ses")
            || self.session_id.trim() != self.session_id
        {
            return Err(crate::SdkError::protocol(
                "selected message part removal is malformed",
            ));
        }
        Ok(self)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }
    pub(crate) fn message_id(&self) -> &str {
        &self.message_id
    }
    pub(crate) fn part_id(&self) -> &str {
        &self.part_id
    }
}

impl OpenCodePartDeltaEvent {
    pub(super) fn validate(self) -> Result<Self, crate::SdkError> {
        if !self.session_id.starts_with("ses")
            || self.session_id.trim() != self.session_id
            || !self.message_id.starts_with("msg")
            || !self.part_id.starts_with("prt")
        {
            return Err(crate::SdkError::protocol(
                "selected message part delta is malformed",
            ));
        }
        Ok(self)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }
    pub(crate) fn message_id(&self) -> &str {
        &self.message_id
    }
    pub(crate) fn part_id(&self) -> &str {
        &self.part_id
    }
    pub(crate) fn text(&self) -> Option<&str> {
        (self.field == "text").then_some(self.delta.as_str())
    }
}

impl OpenCodeMessagePart {
    fn base(&self) -> &OpenCodePartBase {
        match self {
            Self::Snapshot { base, .. }
            | Self::Patch { base, .. }
            | Self::Text { base, .. }
            | Self::Reasoning { base, .. }
            | Self::File { base, .. }
            | Self::Agent { base, .. }
            | Self::Compaction { base, .. }
            | Self::Subtask { base, .. }
            | Self::Retry { base, .. }
            | Self::StepStart { base, .. }
            | Self::StepFinish { base, .. }
            | Self::Tool { base, .. } => base,
        }
    }

    fn is_valid_for(&self, expected_session_id: &str) -> bool {
        let base = self.base();
        if base.session_id != expected_session_id
            || !base.session_id.starts_with("ses")
            || base.session_id.trim() != base.session_id
            || !base.id.starts_with("prt")
            || base.id.trim() != base.id
            || !base.message_id.starts_with("msg")
            || base.message_id.trim() != base.message_id
        {
            return false;
        }
        match self {
            Self::File { source, .. } => {
                source.as_ref().is_none_or(OpenCodeFilePartSource::is_valid)
            }
            Self::Compaction { tail_start_id, .. } => tail_start_id
                .as_deref()
                .is_none_or(|id| id.starts_with("msg")),
            Self::Retry { error, .. } => error.is_selected(),
            Self::StepFinish { cost, tokens, .. } => cost.is_finite() && tokens.is_valid(),
            Self::Tool { state, .. } => state.is_valid_for(expected_session_id),
            Self::Snapshot { .. }
            | Self::Patch { .. }
            | Self::Text { .. }
            | Self::Reasoning { .. }
            | Self::Agent { .. }
            | Self::Subtask { .. }
            | Self::StepStart { .. } => true,
        }
    }

    fn verify_message_owner(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
    ) -> Result<(), crate::SdkError> {
        let base = self.base();
        if !self.is_valid_for(&base.session_id) {
            return Err(crate::SdkError::protocol(
                "foreground response contains a malformed message part",
            ));
        }
        if base.session_id != expected_session_id || base.message_id != expected_message_id {
            return Err(crate::SdkError::invalid_state(
                "foreground response part contradicts its message",
            ));
        }
        Ok(())
    }

    fn is_private_prompt_part(&self, expected_part_id: &str) -> Result<bool, crate::SdkError> {
        if self.base().id != expected_part_id {
            return Ok(false);
        }
        match self {
            Self::Text { _synthetic, .. } if !_synthetic.as_ref().copied().unwrap_or(false) => {
                Ok(true)
            }
            _ => Err(crate::SdkError::invalid_state(
                "foreground parent part contradicts its prompt",
            )),
        }
    }

    fn into_output_text(self) -> Option<(String, String)> {
        match self {
            Self::Text {
                base,
                _text: text,
                _synthetic,
                ..
            } if !_synthetic.as_ref().copied().unwrap_or(false) => Some((base.id, text)),
            _ => None,
        }
    }
}

impl OpenCodeMessageParts {
    pub(super) fn verify_interrupted_call(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
        expected_call_id: &str,
    ) -> Result<(), SdkError> {
        let mut matching_calls = 0;
        for part in &self.0 {
            part.verify_message_owner(expected_session_id, expected_message_id)?;
            if let OpenCodeMessagePart::Tool {
                _call_id, state, ..
            } = part
                && _call_id == expected_call_id
            {
                matching_calls += 1;
                if !matches!(
                    state,
                    OpenCodeToolState::Error { _metadata, .. }
                        if _metadata.as_ref().is_some_and(|metadata|
                            metadata.get("interrupted") == Some(&serde_json::Value::Bool(true)))
                ) {
                    return Err(SdkError::invalid_state(
                        "interaction origin tool was not interrupted",
                    ));
                }
            }
        }
        if matching_calls != 1 {
            return Err(SdkError::invalid_state(
                "interaction origin does not contain one matching tool call",
            ));
        }
        Ok(())
    }

    pub(super) fn verify_prompt_parent(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
        expected_part_id: &str,
    ) -> Result<(), crate::SdkError> {
        let mut matching_parts = 0;
        for part in &self.0 {
            part.verify_message_owner(expected_session_id, expected_message_id)?;
            if part.is_private_prompt_part(expected_part_id)? {
                matching_parts += 1;
            }
        }
        if matching_parts != 1 {
            return Err(crate::SdkError::invalid_state(
                "foreground parent does not contain its private request part",
            ));
        }
        Ok(())
    }

    pub(super) fn into_output_texts(
        self,
        expected_session_id: &str,
        expected_message_id: &str,
    ) -> Result<Vec<(String, String)>, crate::SdkError> {
        let mut text_parts = Vec::new();
        for part in self.0 {
            part.verify_message_owner(expected_session_id, expected_message_id)?;
            if let Some(text) = part.into_output_text() {
                text_parts.push(text);
            }
        }
        Ok(text_parts)
    }
}

impl OpenCodeFilePartSource {
    fn is_valid(&self) -> bool {
        match self {
            Self::File { _text: text, .. }
            | Self::Symbol { _text: text, .. }
            | Self::Resource { _text: text, .. } => text.start.is_finite() && text.end.is_finite(),
        }
    }
}

impl OpenCodeToolState {
    fn is_valid_for(&self, expected_session_id: &str) -> bool {
        match self {
            Self::Completed { attachments, .. } => attachments.as_ref().is_none_or(|attachments| {
                attachments
                    .iter()
                    .all(|attachment| attachment.is_valid_for(expected_session_id))
            }),
            Self::Pending { .. } | Self::Running { .. } | Self::Error { .. } => true,
        }
    }
}

impl OpenCodeFilePart {
    fn is_valid_for(&self, expected_session_id: &str) -> bool {
        self.base.session_id == expected_session_id
            && self.base.id.starts_with("prt")
            && self.base.message_id.starts_with("msg")
            && self
                .source
                .as_ref()
                .is_none_or(OpenCodeFilePartSource::is_valid)
    }
}

impl OpenCodeApiError {
    fn is_selected(&self) -> bool {
        match self {
            Self::Api(data) => {
                let _ = data;
                true
            }
        }
    }
}

impl OpenCodeSelectedAssistantError {
    fn into_sdk_error(self) -> SdkError {
        let error = match self {
            Self::Auth(data) => {
                let _ = data;
                SdkError::integration("model authentication was rejected")
                    .with_detail(ErrorDetail::AuthenticationRejected)
            }
            Self::Unknown(data) => {
                let _ = data;
                SdkError::integration("model execution failed")
                    .with_detail(ErrorDetail::RemoteRejected)
            }
            Self::OutputLength(data) => {
                let _ = data;
                SdkError::integration("model output limit was reached")
                    .with_detail(ErrorDetail::OutputLimit)
            }
            Self::Aborted(data) => {
                let _ = data;
                SdkError::cancelled("foreground prompt was cancelled")
            }
            Self::ContentFilter(data) => {
                let _ = data;
                SdkError::integration("model output was filtered")
                    .with_detail(ErrorDetail::ContentFiltered)
            }
            Self::StructuredOutput(data) => {
                let _ = data;
                SdkError::protocol("model structured output is invalid")
                    .with_detail(ErrorDetail::StructuredOutputMismatch)
            }
            Self::ContextOverflow(data) => {
                let _ = data;
                SdkError::integration("model context limit was reached")
                    .with_detail(ErrorDetail::ContextLimit)
            }
            Self::Api(data) => data.into_sdk_error(),
        };
        error.at_phase(ErrorPhase::CompletionValidation)
    }
}

impl OpenCodeApiErrorData {
    fn into_sdk_error(self) -> SdkError {
        let Some(status) = self
            ._status_code
            .into_option()
            .and_then(|status| u16::try_from(status).ok())
        else {
            return SdkError::integration("model service rejected the request")
                .with_detail(ErrorDetail::RemoteRejected);
        };
        let classified =
            crate::backends::opencode::protocol::http::OpenCodeHttp::status_error_code(status);
        SdkError::integration(classified.message())
            .with_http_status(status)
            .with_detail(
                classified
                    .detail()
                    .copied()
                    .unwrap_or(ErrorDetail::RemoteRejected),
            )
    }
}

impl OpenCodeAssistantError {
    pub(super) fn into_sdk_error(self) -> SdkError {
        self.error
    }
}

impl<'de> serde::Deserialize<'de> for OpenCodeAssistantError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let selected =
            <OpenCodeSelectedAssistantError as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self {
            error: selected.into_sdk_error(),
        })
    }
}

impl OpenCodeSessionDiffEvent {
    pub(crate) fn validate(self) -> Result<Self, crate::SdkError> {
        if !self.diff.iter().all(OpenCodeFileDiff::is_valid) {
            return Err(crate::SdkError::protocol(
                "selected session diff is malformed",
            ));
        }
        OpenCodeSessionEvent::from_id(self.session_id.clone())?;
        Ok(self)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn files(&self) -> Vec<String> {
        self.diff
            .iter()
            .filter_map(|diff| diff.file.as_ref().cloned())
            .collect()
    }
}

impl OpenCodeFileDiff {
    fn is_valid(&self) -> bool {
        self.additions.is_finite() && self.deletions.is_finite()
    }
}

impl OpenCodeTodoUpdatedEvent {
    pub(super) fn into_session(self) -> Result<OpenCodeSessionEvent, crate::SdkError> {
        OpenCodeSessionEvent::from_id(self.session_id)
    }
}

impl OpenCodeSessionErrorEvent {
    pub(crate) fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }
}

impl OpenCodeSessionEvent {
    fn from_id(session_id: String) -> Result<Self, crate::SdkError> {
        Self::from_record_id(session_id, false)
    }

    fn from_record_id(session_id: String, native_share: bool) -> Result<Self, crate::SdkError> {
        if !session_id.starts_with("ses") || session_id.trim() != session_id {
            return Err(crate::SdkError::protocol(
                "known session identity is malformed",
            ));
        }
        Ok(Self {
            session_id,
            native_share,
        })
    }

    pub(crate) fn decode(
        event_type: &str,
        properties: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Self, crate::SdkError> {
        let Some(session_id) = properties
            .get("sessionID")
            .and_then(serde_json::Value::as_str)
        else {
            return Err(crate::SdkError::protocol(
                "known session event has no session identity",
            ));
        };
        let valid = match event_type {
            "session.compacted" | "session.idle" => true,
            event_type if event_type.starts_with("session.next.") => {
                Self::session_next(event_type, properties)
            }
            "command.executed" => {
                ["name", "arguments"]
                    .into_iter()
                    .all(|key| Self::string(properties, key))
                    && Self::identifier(properties, "messageID", "msg_")
            }
            "tui.session.select" => true,
            _ => false,
        };
        if !valid {
            return Err(crate::SdkError::protocol(
                "known session event data is malformed",
            ));
        }
        Self::from_id(session_id.to_owned())
    }

    fn session_next(
        event_type: &str,
        properties: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        if !Self::finite(properties.get("timestamp")) {
            return false;
        }
        let message = || Self::identifier(properties, "messageID", "msg_");
        let assistant = || Self::identifier(properties, "assistantMessageID", "msg_");
        let call = || Self::string(properties, "callID");
        match event_type {
            "session.next.agent.switched" => message() && Self::string(properties, "agent"),
            "session.next.model.switched" => message() && Self::model(properties.get("model")),
            "session.next.moved" => {
                properties
                    .get("location")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|location| {
                        Self::string(location, "directory")
                            && Self::optional_identifier(location, "workspaceID", "wrk")
                    })
                    && Self::optional_string(properties, "subdirectory")
            }
            "session.next.prompted" | "session.next.prompt.admitted" => {
                message()
                    && Self::prompt(properties.get("prompt"))
                    && matches!(
                        properties
                            .get("delivery")
                            .and_then(serde_json::Value::as_str),
                        Some("steer" | "queue")
                    )
            }
            "session.next.context.updated" | "session.next.synthetic" => {
                message() && Self::string(properties, "text")
            }
            "session.next.shell.started" => {
                message() && call() && Self::string(properties, "command")
            }
            "session.next.shell.ended" => call() && Self::string(properties, "output"),
            "session.next.step.started" => {
                assistant()
                    && Self::string(properties, "agent")
                    && Self::model(properties.get("model"))
                    && Self::optional_string(properties, "snapshot")
            }
            "session.next.step.ended" => {
                assistant()
                    && Self::string(properties, "finish")
                    && Self::finite(properties.get("cost"))
                    && Self::tokens(properties.get("tokens"))
                    && Self::optional_string(properties, "snapshot")
                    && properties.get("files").is_none_or(Self::string_array_value)
            }
            "session.next.step.failed" => {
                assistant() && Self::unknown_error(properties.get("error"))
            }
            "session.next.text.started" => assistant() && Self::string(properties, "textID"),
            "session.next.text.delta" => {
                assistant()
                    && Self::string(properties, "textID")
                    && Self::string(properties, "delta")
            }
            "session.next.text.ended" => {
                assistant()
                    && Self::string(properties, "textID")
                    && Self::string(properties, "text")
            }
            "session.next.reasoning.started" => {
                assistant()
                    && Self::string(properties, "reasoningID")
                    && Self::optional_metadata(properties.get("providerMetadata"))
            }
            "session.next.reasoning.delta" => {
                assistant()
                    && Self::string(properties, "reasoningID")
                    && Self::string(properties, "delta")
            }
            "session.next.reasoning.ended" => {
                assistant()
                    && Self::string(properties, "reasoningID")
                    && Self::string(properties, "text")
                    && Self::optional_metadata(properties.get("providerMetadata"))
            }
            "session.next.tool.input.started" => {
                assistant() && call() && Self::string(properties, "name")
            }
            "session.next.tool.input.delta" => {
                assistant() && call() && Self::string(properties, "delta")
            }
            "session.next.tool.input.ended" => {
                assistant() && call() && Self::string(properties, "text")
            }
            "session.next.tool.called" => {
                assistant()
                    && call()
                    && Self::string(properties, "tool")
                    && properties
                        .get("input")
                        .is_some_and(serde_json::Value::is_object)
                    && Self::provider(properties.get("provider"))
            }
            "session.next.tool.progress" => {
                assistant()
                    && call()
                    && properties
                        .get("structured")
                        .is_some_and(serde_json::Value::is_object)
                    && Self::tool_content(properties.get("content"))
            }
            "session.next.tool.success" => {
                assistant()
                    && call()
                    && properties
                        .get("structured")
                        .is_some_and(serde_json::Value::is_object)
                    && Self::tool_content(properties.get("content"))
                    && properties
                        .get("outputPaths")
                        .is_none_or(Self::string_array_value)
                    && Self::provider(properties.get("provider"))
            }
            "session.next.tool.failed" => {
                assistant()
                    && call()
                    && Self::unknown_error(properties.get("error"))
                    && Self::provider(properties.get("provider"))
            }
            "session.next.retried" => {
                Self::finite(properties.get("attempt"))
                    && properties
                        .get("error")
                        .and_then(serde_json::Value::as_object)
                        .is_some_and(|error| {
                            Self::string(error, "message")
                                && error
                                    .get("isRetryable")
                                    .is_some_and(serde_json::Value::is_boolean)
                                && error
                                    .get("statusCode")
                                    .is_none_or(|value| Self::finite(Some(value)))
                                && Self::optional_string_record(error.get("responseHeaders"))
                                && Self::optional_string(error, "responseBody")
                                && Self::optional_string_record(error.get("metadata"))
                        })
            }
            "session.next.compaction.started" => {
                message()
                    && matches!(
                        properties.get("reason").and_then(serde_json::Value::as_str),
                        Some("auto" | "manual")
                    )
            }
            "session.next.compaction.delta" => message() && Self::string(properties, "text"),
            "session.next.compaction.ended" => {
                message()
                    && matches!(
                        properties.get("reason").and_then(serde_json::Value::as_str),
                        Some("auto" | "manual")
                    )
                    && Self::string(properties, "text")
                    && Self::string(properties, "recent")
            }
            "session.next.revert.staged" => Self::revert(properties.get("revert")),
            "session.next.revert.cleared" => true,
            "session.next.revert.committed" => message(),
            _ => false,
        }
    }

    fn string(properties: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
        properties
            .get(key)
            .is_some_and(serde_json::Value::is_string)
    }

    fn optional_string(properties: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
        properties.get(key).is_none_or(serde_json::Value::is_string)
    }

    fn optional_identifier(
        properties: &serde_json::Map<String, serde_json::Value>,
        key: &str,
        prefix: &str,
    ) -> bool {
        properties
            .get(key)
            .is_none_or(|value| value.as_str().is_some_and(|id| id.starts_with(prefix)))
    }

    fn optional_string_record(value: Option<&serde_json::Value>) -> bool {
        value.is_none_or(|value| {
            value
                .as_object()
                .is_some_and(|record| record.values().all(serde_json::Value::is_string))
        })
    }

    fn identifier(
        properties: &serde_json::Map<String, serde_json::Value>,
        key: &str,
        prefix: &str,
    ) -> bool {
        properties
            .get(key)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.starts_with(prefix))
    }

    fn finite(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_f64)
            .is_some_and(f64::is_finite)
    }

    fn string_array_value(value: &serde_json::Value) -> bool {
        value
            .as_array()
            .is_some_and(|values| values.iter().all(serde_json::Value::is_string))
    }

    fn model(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_object)
            .is_some_and(|model| {
                Self::string(model, "id")
                    && Self::string(model, "providerID")
                    && Self::optional_string(model, "variant")
            })
    }

    fn prompt(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_object)
            .is_some_and(|prompt| {
                Self::string(prompt, "text")
                    && prompt.get("files").is_none_or(|files| {
                        files.as_array().is_some_and(|files| {
                            files.iter().all(|file| {
                                file.as_object().is_some_and(|file| {
                                    Self::string(file, "uri")
                                        && Self::string(file, "mime")
                                        && Self::optional_string(file, "name")
                                        && Self::optional_string(file, "description")
                                        && file.get("source").is_none_or(Self::source)
                                })
                            })
                        })
                    })
                    && prompt.get("agents").is_none_or(|agents| {
                        agents.as_array().is_some_and(|agents| {
                            agents.iter().all(|agent| {
                                agent.as_object().is_some_and(|agent| {
                                    Self::string(agent, "name")
                                        && agent.get("source").is_none_or(Self::source)
                                })
                            })
                        })
                    })
            })
    }

    fn source(value: &serde_json::Value) -> bool {
        value.as_object().is_some_and(|source| {
            Self::finite(source.get("start"))
                && Self::finite(source.get("end"))
                && Self::string(source, "text")
        })
    }

    fn tokens(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_object)
            .is_some_and(|tokens| {
                ["input", "output", "reasoning"]
                    .into_iter()
                    .all(|key| Self::finite(tokens.get(key)))
                    && tokens
                        .get("cache")
                        .and_then(serde_json::Value::as_object)
                        .is_some_and(|cache| {
                            ["read", "write"]
                                .into_iter()
                                .all(|key| Self::finite(cache.get(key)))
                        })
            })
    }

    fn provider(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_object)
            .is_some_and(|provider| {
                provider
                    .get("executed")
                    .is_some_and(serde_json::Value::is_boolean)
                    && Self::optional_metadata(provider.get("metadata"))
            })
    }

    fn optional_metadata(value: Option<&serde_json::Value>) -> bool {
        value.is_none_or(|metadata| {
            metadata
                .as_object()
                .is_some_and(|metadata| metadata.values().all(serde_json::Value::is_object))
        })
    }

    fn unknown_error(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_object)
            .is_some_and(|error| {
                error.get("type").and_then(serde_json::Value::as_str) == Some("unknown")
                    && Self::string(error, "message")
            })
    }

    fn tool_content(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| {
                items.iter().all(|item| {
                    item.as_object().is_some_and(|item| {
                        match item.get("type").and_then(serde_json::Value::as_str) {
                            Some("text") => Self::string(item, "text"),
                            Some("file") => {
                                Self::string(item, "uri")
                                    && Self::string(item, "mime")
                                    && Self::optional_string(item, "name")
                            }
                            _ => false,
                        }
                    })
                })
            })
    }

    fn revert(value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(serde_json::Value::as_object)
            .is_some_and(|revert| {
                Self::identifier(revert, "messageID", "msg_")
                    && Self::optional_string(revert, "partID")
                    && Self::optional_string(revert, "snapshot")
                    && Self::optional_string(revert, "diff")
                    && revert.get("files").is_none_or(|files| {
                        files.as_array().is_some_and(|files| {
                            files.iter().all(|file| {
                                file.as_object().is_some_and(|file| {
                                    Self::string(file, "path")
                                        && matches!(
                                            file.get("status").and_then(serde_json::Value::as_str),
                                            Some("added" | "modified" | "deleted")
                                        )
                                        && file
                                            .get("additions")
                                            .and_then(serde_json::Value::as_u64)
                                            .is_some()
                                        && file
                                            .get("deletions")
                                            .and_then(serde_json::Value::as_u64)
                                            .is_some()
                                        && Self::string(file, "patch")
                                })
                            })
                        })
                    })
            })
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn has_native_share(&self) -> bool {
        self.native_share
    }
}

impl OpenCodeInstanceDisposed {
    pub(crate) fn directory(&self) -> &str {
        &self.directory
    }
}

impl OpenCodeSessionStatusEvent {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn is_active(&self) -> bool {
        matches!(
            &self.status,
            OpenCodeSessionStatus::Busy | OpenCodeSessionStatus::Retry { .. }
        )
    }
}

impl OpenCodePermissionReplied {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(crate) fn reply(&self) -> OpenCodePermissionReply {
        self.reply
    }
}

impl OpenCodeQuestionReplied {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(crate) fn answer_count(&self) -> usize {
        self.answers.len()
    }
}

impl OpenCodeQuestionRejected {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum OpenCodeSharePolicy {
    Manual,
    Auto,
    Disabled,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(super) struct OpenCodePermissionRule {
    permission: String,
    pattern: String,
    action: OpenCodePermissionAction,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum OpenCodePermissionAction {
    Allow,
    Deny,
    Ask,
}

impl OpenCodePermissionRule {
    pub(super) fn full_access() -> Self {
        Self {
            permission: "*".to_owned(),
            pattern: "*".to_owned(),
            action: OpenCodePermissionAction::Allow,
        }
    }
}

impl OpenCodeQuestion {
    fn custom_default() -> bool {
        true
    }
}
