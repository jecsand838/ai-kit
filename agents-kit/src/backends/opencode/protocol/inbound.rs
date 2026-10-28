use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::KitError;
use crate::backends::opencode::protocol::schemas::{
    OpenCodeGlobalEnvelope, OpenCodeInstanceDisposed, OpenCodeManifestEmpty,
    OpenCodeMessageRemovedEvent, OpenCodeMessageUpdatedEvent, OpenCodePartDeltaEvent,
    OpenCodePartRemovedEvent, OpenCodePartUpdatedEvent, OpenCodePermissionReplied,
    OpenCodePermissionRequest, OpenCodePermissionV2Request, OpenCodeQuestionRejected,
    OpenCodeQuestionReplied, OpenCodeQuestionRequest, OpenCodeSessionDiffEvent,
    OpenCodeSessionErrorEvent, OpenCodeSessionEvent, OpenCodeSessionRecordEvent,
    OpenCodeSessionStatusEvent, OpenCodeTodoUpdatedEvent,
};
use crate::backends::opencode::protocol::sse::OpenCodeSseFrame;
use crate::backends::opencode::protocol::sync::OpenCodeHistoryRow;

#[derive(Clone, Debug)]
pub(crate) enum OpenCodeInboundEvent {
    Normal(OpenCodeNormalEvent),
    Sync(OpenCodeSequencedEvent),
    History(OpenCodeSequencedEvent),
}

#[derive(Clone, Debug)]
pub(crate) struct OpenCodeRoutingContext {
    directory: Option<String>,
    _project: Option<String>,
    _workspace: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct OpenCodeNormalEvent {
    routing: OpenCodeRoutingContext,
    id: String,
    event_type: String,
    data: Map<String, Value>,
    event: OpenCodeKnownEvent,
    encoded_len: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct OpenCodeSequencedEvent {
    routing: Option<OpenCodeRoutingContext>,
    id: String,
    aggregate_id: String,
    sequence: u64,
    event_type: String,
    data: Map<String, Value>,
    event: OpenCodeKnownEvent,
    encoded_len: usize,
}

#[derive(Clone, Debug)]
pub(crate) enum OpenCodeKnownEvent {
    Connected,
    Heartbeat,
    Disposed,
    InstanceDisposed(OpenCodeInstanceDisposed),
    SessionCreated(OpenCodeSessionEvent),
    SessionUpdated(OpenCodeSessionEvent),
    SessionDeleted(OpenCodeSessionEvent),
    SessionCompacted(OpenCodeSessionEvent),
    SessionIdle(OpenCodeSessionEvent),
    SessionStatus(OpenCodeSessionStatusEvent),
    MessageUpdated(OpenCodeMessageUpdatedEvent),
    MessageRemoved(OpenCodeMessageRemovedEvent),
    MessagePartUpdated(OpenCodePartUpdatedEvent),
    MessagePartRemoved(OpenCodePartRemovedEvent),
    MessagePartDelta(OpenCodePartDeltaEvent),
    SessionDiff(OpenCodeSessionDiffEvent),
    SessionError(OpenCodeSessionErrorEvent),
    PermissionAsked(OpenCodePermissionRequest),
    PermissionReplied(OpenCodePermissionReplied),
    PermissionV2Asked(OpenCodePermissionV2Request),
    PermissionV2Replied(OpenCodePermissionReplied),
    QuestionAsked(OpenCodeQuestionRequest),
    QuestionReplied(OpenCodeQuestionReplied),
    QuestionRejected(OpenCodeQuestionRejected),
    QuestionV2Asked(OpenCodeQuestionRequest),
    QuestionV2Replied(OpenCodeQuestionReplied),
    QuestionV2Rejected(OpenCodeQuestionRejected),
    SessionNext(OpenCodeSessionNextEvent),
    PrivateSession(OpenCodePrivateSessionEvent),
    TodoUpdated(OpenCodeSessionEvent),
    PrivateGlobal(OpenCodePrivateGlobalEvent),
    UnknownFuture,
}

#[derive(Clone, Debug)]
pub(crate) struct OpenCodeSessionNextEvent {
    kind: OpenCodeSessionNextKind,
    session: OpenCodeSessionEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct OpenCodePrivateSessionEvent {
    kind: OpenCodePrivateSessionKind,
    session: OpenCodeSessionEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenCodePrivateSessionKind {
    CommandExecuted,
    TuiSessionSelected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenCodeSessionNextKind {
    AgentSwitched,
    ModelSwitched,
    Moved,
    Prompted,
    PromptAdmitted,
    ContextUpdated,
    Synthetic,
    ShellStarted,
    ShellEnded,
    StepStarted,
    StepEnded,
    StepFailed,
    TextStarted,
    TextDelta,
    TextEnded,
    ReasoningStarted,
    ReasoningDelta,
    ReasoningEnded,
    ToolInputStarted,
    ToolInputDelta,
    ToolInputEnded,
    ToolCalled,
    ToolProgress,
    ToolSuccess,
    ToolFailed,
    Retried,
    CompactionStarted,
    CompactionDelta,
    CompactionEnded,
    RevertStaged,
    RevertCleared,
    RevertCommitted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenCodePrivateGlobalEvent {
    ModelsRefreshed,
    IntegrationUpdated,
    IntegrationConnectionUpdated,
    CatalogUpdated,
    FileEdited,
    ReferenceUpdated,
    PluginAdded,
    ProjectDirectoriesUpdated,
    FileWatcherUpdated,
    PtyCreated,
    PtyUpdated,
    PtyExited,
    PtyDeleted,
    InstallationUpdated,
    InstallationUpdateAvailable,
    LspUpdated,
    McpToolsChanged,
    McpBrowserOpenFailed,
    ProjectUpdated,
    TuiPromptAppend,
    TuiCommandExecute,
    TuiToastShow,
    VcsBranchUpdated,
    WorkspaceReady,
    WorkspaceFailed,
    WorkspaceStatus,
    WorktreeReady,
    WorktreeFailed,
}

impl OpenCodeInboundEvent {
    pub(crate) fn from_global_frame(frame: OpenCodeSseFrame) -> Result<Self, KitError> {
        let (sse_event, data, _) = frame.into_parts();
        if sse_event.as_deref().is_some_and(|name| name != "message") {
            return Err(KitError::protocol(
                "global stream emitted an unsupported SSE event field",
            ));
        }
        let encoded_len = data.len();
        let envelope: OpenCodeGlobalEnvelope = serde_json::from_str(&data)
            .map_err(|_| KitError::protocol("global event envelope is malformed"))?;
        let (directory, project, workspace, payload) = envelope.into_parts();
        let routing = OpenCodeRoutingContext {
            directory,
            _project: project,
            _workspace: workspace,
        };
        let (outer_id, event_type, properties, sync_event) = payload.into_parts();
        Self::validate_identity(&outer_id, "global event")?;
        if !outer_id.starts_with("evt_") {
            return Err(KitError::protocol("global event identity is malformed"));
        }
        if event_type == "sync" {
            if properties.into_option().is_some() {
                return Err(KitError::protocol(
                    "sync envelope unexpectedly contains normal event data",
                ));
            }
            let sync = sync_event
                .ok_or_else(|| KitError::protocol("sync envelope has no sequenced event"))?;
            let (id, aggregate_id, sequence, event_type, data) = sync.into_parts();
            if outer_id != id {
                return Err(KitError::protocol("sync envelope identities disagree"));
            }
            return Self::sequenced(
                Some(routing),
                id,
                aggregate_id,
                sequence,
                event_type,
                data,
                encoded_len,
            )
            .map(Self::Sync);
        }
        if sync_event.is_some() {
            return Err(KitError::protocol(
                "normal event unexpectedly contains sync data",
            ));
        }
        let properties = properties
            .into_option()
            .ok_or_else(|| KitError::protocol("normal event has no properties"))?;
        let data = properties.clone();
        let event = OpenCodeKnownEvent::decode_normal(&event_type, properties)?;
        if let OpenCodeKnownEvent::InstanceDisposed(disposed) = &event
            && routing.directory.as_deref() != Some(disposed.directory())
        {
            return Err(KitError::protocol(
                "instance-disposed routing context is contradictory",
            ));
        }
        Ok(Self::Normal(OpenCodeNormalEvent {
            routing,
            id: outer_id,
            event_type,
            data,
            event,
            encoded_len,
        }))
    }

    pub(crate) fn from_history_row(row: OpenCodeHistoryRow) -> Result<Self, KitError> {
        let encoded_len = row.encoded_len();
        let row = row.into_validated()?;
        Self::sequenced(
            None,
            row.id,
            row.aggregate_id,
            row.seq,
            row.event_type,
            row.data,
            encoded_len,
        )
        .map(Self::History)
    }

    fn sequenced(
        routing: Option<OpenCodeRoutingContext>,
        id: String,
        aggregate_id: String,
        sequence: u64,
        event_type: String,
        data: Map<String, Value>,
        encoded_len: usize,
    ) -> Result<OpenCodeSequencedEvent, KitError> {
        Self::validate_identity(&id, "event")?;
        if !id.starts_with("evt_") {
            return Err(KitError::protocol("event identity is malformed"));
        }
        Self::validate_identity(&aggregate_id, "aggregate")?;
        let canonical_data = data.clone();
        let event = OpenCodeKnownEvent::decode_sequenced(&event_type, data)?;
        if !matches!(event, OpenCodeKnownEvent::UnknownFuture) {
            let session_id = event.session_id().ok_or_else(|| {
                KitError::protocol("selected durable event has no session identity")
            })?;
            if aggregate_id != session_id {
                return Err(KitError::protocol(
                    "durable aggregate does not match its selected session",
                ));
            }
        }
        Ok(OpenCodeSequencedEvent {
            routing,
            id,
            aggregate_id,
            sequence,
            event_type,
            data: canonical_data,
            event,
            encoded_len,
        })
    }

    fn validate_identity(value: &str, owner: &str) -> Result<(), KitError> {
        if value.is_empty() || value.trim() != value {
            return Err(KitError::protocol(format!("{owner} identity is malformed")));
        }
        Ok(())
    }
}

impl OpenCodeInboundEvent {
    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Self::Normal(event) => event.encoded_len,
            Self::Sync(event) | Self::History(event) => event.encoded_len,
        }
    }
}

impl OpenCodeNormalEvent {
    pub(crate) fn routing(&self) -> &OpenCodeRoutingContext {
        &self.routing
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn event(&self) -> &OpenCodeKnownEvent {
        &self.event
    }

    pub(crate) fn event_type(&self) -> &str {
        &self.event_type
    }

    pub(crate) fn data(&self) -> &Map<String, Value> {
        &self.data
    }
}

impl OpenCodeSequencedEvent {
    pub(crate) fn routing(&self) -> Option<&OpenCodeRoutingContext> {
        self.routing.as_ref()
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn event(&self) -> &OpenCodeKnownEvent {
        &self.event
    }

    pub(crate) fn event_type(&self) -> &str {
        &self.event_type
    }

    pub(crate) fn data(&self) -> &Map<String, Value> {
        &self.data
    }
}

impl OpenCodeRoutingContext {
    pub(crate) fn directory(&self) -> Option<&str> {
        self.directory.as_deref()
    }
}

impl OpenCodeKnownEvent {
    pub(crate) fn session_id(&self) -> Option<&str> {
        match self {
            Self::Connected
            | Self::Heartbeat
            | Self::Disposed
            | Self::InstanceDisposed(_)
            | Self::PrivateGlobal(_)
            | Self::UnknownFuture => None,
            Self::SessionCreated(event)
            | Self::SessionUpdated(event)
            | Self::SessionDeleted(event) => Some(event.session_id()),
            Self::SessionCompacted(event) | Self::SessionIdle(event) => Some(event.session_id()),
            Self::SessionStatus(event) => Some(event.session_id()),
            Self::MessageUpdated(event) => Some(event.session_id()),
            Self::MessageRemoved(event) => Some(event.session_id()),
            Self::MessagePartUpdated(event) => Some(event.session_id()),
            Self::MessagePartRemoved(event) => Some(event.session_id()),
            Self::MessagePartDelta(event) => Some(event.session_id()),
            Self::SessionDiff(event) => Some(event.session_id()),
            Self::SessionError(event) => event.session_id(),
            Self::PermissionAsked(event) => Some(event.session_id()),
            Self::PermissionReplied(event) | Self::PermissionV2Replied(event) => {
                Some(event.session_id())
            }
            Self::PermissionV2Asked(event) => Some(event.session_id()),
            Self::QuestionAsked(event) => Some(event.session_id()),
            Self::QuestionReplied(event) => Some(event.session_id()),
            Self::QuestionRejected(event) => Some(event.session_id()),
            Self::QuestionV2Asked(event) => Some(event.session_id()),
            Self::QuestionV2Replied(event) => Some(event.session_id()),
            Self::QuestionV2Rejected(event) => Some(event.session_id()),
            Self::SessionNext(event) => Some(event.session_id()),
            Self::PrivateSession(event) => Some(event.session_id()),
            Self::TodoUpdated(event) => Some(event.session_id()),
        }
    }

    fn decode_normal(event_type: &str, properties: Map<String, Value>) -> Result<Self, KitError> {
        OpenCodeInboundEvent::validate_identity(event_type, "event type")?;
        let event = Self::decode_selected(event_type, properties)
            .transpose()?
            .unwrap_or(Self::UnknownFuture);
        Self::validate_session_identity(&event)?;
        Ok(event)
    }

    fn decode_sequenced(
        event_type: &str,
        properties: Map<String, Value>,
    ) -> Result<Self, KitError> {
        let (canonical, version) = Self::versioned_type(event_type)?;
        let Some(event) = Self::decode_selected(canonical, properties) else {
            return Ok(Self::UnknownFuture);
        };
        let event = event?;
        if event.durable_version() != Some(version) {
            return Err(KitError::protocol("durable event version is not selected"));
        }
        Self::validate_session_identity(&event)?;
        Ok(event)
    }

    fn decode_selected(
        event_type: &str,
        properties: Map<String, Value>,
    ) -> Option<Result<Self, KitError>> {
        let event = match event_type {
            "server.connected" => {
                Self::decode_as(properties).map(|_: OpenCodeManifestEmpty| Self::Connected)
            }
            "server.heartbeat" => {
                Self::decode_as(properties).map(|_: OpenCodeManifestEmpty| Self::Heartbeat)
            }
            "global.disposed" => {
                Self::decode_as(properties).map(|_: OpenCodeManifestEmpty| Self::Disposed)
            }
            "server.instance.disposed" => Self::decode_as(properties).map(Self::InstanceDisposed),
            "session.created" => Self::decode_as::<OpenCodeSessionRecordEvent>(properties)
                .and_then(OpenCodeSessionRecordEvent::into_session)
                .map(Self::SessionCreated),
            "session.updated" => Self::decode_as::<OpenCodeSessionRecordEvent>(properties)
                .and_then(OpenCodeSessionRecordEvent::into_session)
                .map(Self::SessionUpdated),
            "session.deleted" => Self::decode_as::<OpenCodeSessionRecordEvent>(properties)
                .and_then(OpenCodeSessionRecordEvent::into_session)
                .map(Self::SessionDeleted),
            "session.compacted" => {
                OpenCodeSessionEvent::decode(event_type, &properties).map(Self::SessionCompacted)
            }
            "session.idle" => {
                OpenCodeSessionEvent::decode(event_type, &properties).map(Self::SessionIdle)
            }
            "session.status" => Self::decode_as(properties).map(Self::SessionStatus),
            "message.updated" => Self::decode_as::<OpenCodeMessageUpdatedEvent>(properties)
                .and_then(OpenCodeMessageUpdatedEvent::validate)
                .map(Self::MessageUpdated),
            "message.removed" => Self::decode_as::<OpenCodeMessageRemovedEvent>(properties)
                .and_then(OpenCodeMessageRemovedEvent::validate)
                .map(Self::MessageRemoved),
            "message.part.updated" => Self::decode_as::<OpenCodePartUpdatedEvent>(properties)
                .and_then(OpenCodePartUpdatedEvent::validate)
                .map(Self::MessagePartUpdated),
            "message.part.removed" => Self::decode_as::<OpenCodePartRemovedEvent>(properties)
                .and_then(OpenCodePartRemovedEvent::validate)
                .map(Self::MessagePartRemoved),
            "message.part.delta" => Self::decode_as::<OpenCodePartDeltaEvent>(properties)
                .and_then(OpenCodePartDeltaEvent::validate)
                .map(Self::MessagePartDelta),
            "session.diff" => Self::decode_as::<OpenCodeSessionDiffEvent>(properties)
                .and_then(OpenCodeSessionDiffEvent::validate)
                .map(Self::SessionDiff),
            "session.error" => Self::decode_as(properties).map(Self::SessionError),
            "permission.asked" => Self::decode_as(properties).map(Self::PermissionAsked),
            "permission.replied" => Self::decode_as(properties).map(Self::PermissionReplied),
            "permission.v2.asked" => Self::decode_as(properties).map(Self::PermissionV2Asked),
            "permission.v2.replied" => Self::decode_as(properties).map(Self::PermissionV2Replied),
            "question.asked" => Self::decode_as::<OpenCodeQuestionRequest>(properties)
                .and_then(OpenCodeQuestionRequest::validate_v1)
                .map(Self::QuestionAsked),
            "question.replied" => Self::decode_as(properties).map(Self::QuestionReplied),
            "question.rejected" => Self::decode_as(properties).map(Self::QuestionRejected),
            "question.v2.asked" => Self::decode_as(properties).map(Self::QuestionV2Asked),
            "question.v2.replied" => Self::decode_as(properties).map(Self::QuestionV2Replied),
            "question.v2.rejected" => Self::decode_as(properties).map(Self::QuestionV2Rejected),
            "session.next.agent.switched" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::AgentSwitched,
            ),
            "session.next.model.switched" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ModelSwitched,
            ),
            "session.next.moved" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::Moved)
            }
            "session.next.prompted" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::Prompted)
            }
            "session.next.prompt.admitted" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::PromptAdmitted,
            ),
            "session.next.context.updated" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ContextUpdated,
            ),
            "session.next.synthetic" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::Synthetic)
            }
            "session.next.shell.started" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ShellStarted,
            ),
            "session.next.shell.ended" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::ShellEnded)
            }
            "session.next.step.started" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::StepStarted)
            }
            "session.next.step.ended" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::StepEnded)
            }
            "session.next.step.failed" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::StepFailed)
            }
            "session.next.text.started" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::TextStarted)
            }
            "session.next.text.delta" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::TextDelta)
            }
            "session.next.text.ended" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::TextEnded)
            }
            "session.next.reasoning.started" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ReasoningStarted,
            ),
            "session.next.reasoning.delta" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ReasoningDelta,
            ),
            "session.next.reasoning.ended" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ReasoningEnded,
            ),
            "session.next.tool.input.started" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ToolInputStarted,
            ),
            "session.next.tool.input.delta" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ToolInputDelta,
            ),
            "session.next.tool.input.ended" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ToolInputEnded,
            ),
            "session.next.tool.called" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::ToolCalled)
            }
            "session.next.tool.progress" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::ToolProgress,
            ),
            "session.next.tool.success" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::ToolSuccess)
            }
            "session.next.tool.failed" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::ToolFailed)
            }
            "session.next.retried" => {
                Self::session_next(event_type, properties, OpenCodeSessionNextKind::Retried)
            }
            "session.next.compaction.started" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::CompactionStarted,
            ),
            "session.next.compaction.delta" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::CompactionDelta,
            ),
            "session.next.compaction.ended" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::CompactionEnded,
            ),
            "session.next.revert.staged" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::RevertStaged,
            ),
            "session.next.revert.cleared" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::RevertCleared,
            ),
            "session.next.revert.committed" => Self::session_next(
                event_type,
                properties,
                OpenCodeSessionNextKind::RevertCommitted,
            ),
            "todo.updated" => Self::decode_as::<OpenCodeTodoUpdatedEvent>(properties)
                .and_then(OpenCodeTodoUpdatedEvent::into_session)
                .map(Self::TodoUpdated),
            "command.executed" => Self::private_session(
                event_type,
                properties,
                OpenCodePrivateSessionKind::CommandExecuted,
            ),
            "tui.session.select" => Self::private_session(
                event_type,
                properties,
                OpenCodePrivateSessionKind::TuiSessionSelected,
            ),
            _ => {
                return OpenCodePrivateGlobalEvent::decode_selected(event_type, &properties)
                    .map(|event| event.map(Self::PrivateGlobal));
            }
        };
        Some(event)
    }

    fn validate_session_identity(event: &Self) -> Result<(), KitError> {
        if let Some(session_id) = event.session_id() {
            if !session_id.starts_with("ses") {
                return Err(KitError::protocol("session identity is malformed"));
            }
            OpenCodeInboundEvent::validate_identity(session_id, "session")?;
        }
        let request = match event {
            Self::PermissionAsked(request) => Some((request.id(), "per")),
            Self::PermissionV2Asked(request) => Some((request.id(), "per")),
            Self::PermissionReplied(event) | Self::PermissionV2Replied(event) => {
                Some((event.request_id(), "per"))
            }
            Self::QuestionAsked(request) | Self::QuestionV2Asked(request) => {
                Some((request.id(), "que"))
            }
            Self::QuestionReplied(event) | Self::QuestionV2Replied(event) => {
                Some((event.request_id(), "que"))
            }
            Self::QuestionRejected(event) | Self::QuestionV2Rejected(event) => {
                Some((event.request_id(), "que"))
            }
            _ => None,
        };
        if request.is_some_and(|(id, prefix)| !id.starts_with(prefix)) {
            return Err(KitError::protocol("interaction identity is malformed"));
        }
        Ok(())
    }

    fn decode_as<T>(properties: Map<String, Value>) -> Result<T, KitError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(Value::Object(properties))
            .map_err(|_| KitError::protocol("known event data is malformed"))
    }

    fn session_next(
        event_type: &str,
        properties: Map<String, Value>,
        kind: OpenCodeSessionNextKind,
    ) -> Result<Self, KitError> {
        let session = OpenCodeSessionEvent::decode(event_type, &properties)?;
        Ok(Self::SessionNext(OpenCodeSessionNextEvent {
            kind,
            session,
        }))
    }

    fn private_session(
        event_type: &str,
        properties: Map<String, Value>,
        kind: OpenCodePrivateSessionKind,
    ) -> Result<Self, KitError> {
        let session = OpenCodeSessionEvent::decode(event_type, &properties)?;
        Ok(Self::PrivateSession(OpenCodePrivateSessionEvent {
            kind,
            session,
        }))
    }

    fn versioned_type(event_type: &str) -> Result<(&str, u64), KitError> {
        let Some((base, version_text)) = event_type.rsplit_once('.') else {
            return Err(KitError::protocol("durable event type has no version"));
        };
        if version_text.is_empty() || !version_text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(KitError::protocol(
                "durable event type has a malformed version",
            ));
        }
        let version = version_text
            .parse::<u64>()
            .map_err(|_| KitError::protocol("durable event type has a malformed version"))?;
        if version.to_string() != version_text {
            return Err(KitError::protocol(
                "durable event type has a noncanonical version",
            ));
        }
        if base.is_empty() || base.trim() != base {
            return Err(KitError::protocol("durable event type is malformed"));
        }
        Ok((base, version))
    }

    fn durable_version(&self) -> Option<u64> {
        match self {
            Self::SessionCreated(_)
            | Self::SessionUpdated(_)
            | Self::SessionDeleted(_)
            | Self::MessageUpdated(_)
            | Self::MessageRemoved(_)
            | Self::MessagePartUpdated(_)
            | Self::MessagePartRemoved(_) => Some(1),
            Self::SessionNext(event) => event.durable_version(),
            Self::Connected
            | Self::Heartbeat
            | Self::Disposed
            | Self::InstanceDisposed(_)
            | Self::SessionCompacted(_)
            | Self::SessionIdle(_)
            | Self::SessionStatus(_)
            | Self::MessagePartDelta(_)
            | Self::SessionDiff(_)
            | Self::SessionError(_)
            | Self::PermissionAsked(_)
            | Self::PermissionReplied(_)
            | Self::PermissionV2Asked(_)
            | Self::PermissionV2Replied(_)
            | Self::QuestionAsked(_)
            | Self::QuestionReplied(_)
            | Self::QuestionRejected(_)
            | Self::QuestionV2Asked(_)
            | Self::QuestionV2Replied(_)
            | Self::QuestionV2Rejected(_)
            | Self::PrivateSession(_)
            | Self::TodoUpdated(_)
            | Self::PrivateGlobal(_)
            | Self::UnknownFuture => None,
        }
    }
}

impl OpenCodeSessionNextEvent {
    pub(crate) fn session_id(&self) -> &str {
        self.session.session_id()
    }

    pub(crate) fn is_private_state(&self) -> bool {
        match self.kind {
            OpenCodeSessionNextKind::AgentSwitched
            | OpenCodeSessionNextKind::ModelSwitched
            | OpenCodeSessionNextKind::Moved
            | OpenCodeSessionNextKind::Prompted
            | OpenCodeSessionNextKind::PromptAdmitted
            | OpenCodeSessionNextKind::ContextUpdated
            | OpenCodeSessionNextKind::Synthetic
            | OpenCodeSessionNextKind::ShellStarted
            | OpenCodeSessionNextKind::ShellEnded
            | OpenCodeSessionNextKind::StepStarted
            | OpenCodeSessionNextKind::StepEnded
            | OpenCodeSessionNextKind::StepFailed
            | OpenCodeSessionNextKind::TextStarted
            | OpenCodeSessionNextKind::TextDelta
            | OpenCodeSessionNextKind::TextEnded
            | OpenCodeSessionNextKind::ReasoningStarted
            | OpenCodeSessionNextKind::ReasoningDelta
            | OpenCodeSessionNextKind::ReasoningEnded
            | OpenCodeSessionNextKind::ToolInputStarted
            | OpenCodeSessionNextKind::ToolInputDelta
            | OpenCodeSessionNextKind::ToolInputEnded
            | OpenCodeSessionNextKind::ToolCalled
            | OpenCodeSessionNextKind::ToolProgress
            | OpenCodeSessionNextKind::ToolSuccess
            | OpenCodeSessionNextKind::ToolFailed
            | OpenCodeSessionNextKind::Retried
            | OpenCodeSessionNextKind::CompactionStarted
            | OpenCodeSessionNextKind::CompactionDelta
            | OpenCodeSessionNextKind::CompactionEnded
            | OpenCodeSessionNextKind::RevertStaged
            | OpenCodeSessionNextKind::RevertCleared
            | OpenCodeSessionNextKind::RevertCommitted => true,
        }
    }

    fn durable_version(&self) -> Option<u64> {
        match self.kind {
            OpenCodeSessionNextKind::StepEnded | OpenCodeSessionNextKind::StepFailed => Some(2),
            OpenCodeSessionNextKind::TextDelta
            | OpenCodeSessionNextKind::ReasoningDelta
            | OpenCodeSessionNextKind::ToolInputDelta
            | OpenCodeSessionNextKind::CompactionDelta => None,
            OpenCodeSessionNextKind::AgentSwitched
            | OpenCodeSessionNextKind::ModelSwitched
            | OpenCodeSessionNextKind::Moved
            | OpenCodeSessionNextKind::Prompted
            | OpenCodeSessionNextKind::PromptAdmitted
            | OpenCodeSessionNextKind::ContextUpdated
            | OpenCodeSessionNextKind::Synthetic
            | OpenCodeSessionNextKind::ShellStarted
            | OpenCodeSessionNextKind::ShellEnded
            | OpenCodeSessionNextKind::StepStarted
            | OpenCodeSessionNextKind::TextStarted
            | OpenCodeSessionNextKind::TextEnded
            | OpenCodeSessionNextKind::ReasoningStarted
            | OpenCodeSessionNextKind::ReasoningEnded
            | OpenCodeSessionNextKind::ToolInputStarted
            | OpenCodeSessionNextKind::ToolInputEnded
            | OpenCodeSessionNextKind::ToolCalled
            | OpenCodeSessionNextKind::ToolProgress
            | OpenCodeSessionNextKind::ToolSuccess
            | OpenCodeSessionNextKind::ToolFailed
            | OpenCodeSessionNextKind::Retried
            | OpenCodeSessionNextKind::CompactionStarted
            | OpenCodeSessionNextKind::CompactionEnded
            | OpenCodeSessionNextKind::RevertStaged
            | OpenCodeSessionNextKind::RevertCleared
            | OpenCodeSessionNextKind::RevertCommitted => Some(1),
        }
    }
}

impl OpenCodePrivateSessionEvent {
    pub(crate) fn session_id(&self) -> &str {
        self.session.session_id()
    }

    pub(crate) fn is_private_state(&self) -> bool {
        match self.kind {
            OpenCodePrivateSessionKind::CommandExecuted
            | OpenCodePrivateSessionKind::TuiSessionSelected => true,
        }
    }
}

impl OpenCodePrivateGlobalEvent {
    fn decode_selected(
        event_type: &str,
        properties: &Map<String, Value>,
    ) -> Option<Result<Self, KitError>> {
        let event = match event_type {
            "models-dev.refreshed" => Self::ModelsRefreshed,
            "integration.updated" => Self::IntegrationUpdated,
            "integration.connection.updated" => Self::IntegrationConnectionUpdated,
            "catalog.updated" => Self::CatalogUpdated,
            "file.edited" => Self::FileEdited,
            "reference.updated" => Self::ReferenceUpdated,
            "plugin.added" => Self::PluginAdded,
            "project.directories.updated" => Self::ProjectDirectoriesUpdated,
            "file.watcher.updated" => Self::FileWatcherUpdated,
            "pty.created" => Self::PtyCreated,
            "pty.updated" => Self::PtyUpdated,
            "pty.exited" => Self::PtyExited,
            "pty.deleted" => Self::PtyDeleted,
            "installation.updated" => Self::InstallationUpdated,
            "installation.update-available" => Self::InstallationUpdateAvailable,
            "lsp.updated" => Self::LspUpdated,
            "mcp.tools.changed" => Self::McpToolsChanged,
            "mcp.browser.open.failed" => Self::McpBrowserOpenFailed,
            "project.updated" => Self::ProjectUpdated,
            "tui.prompt.append" => Self::TuiPromptAppend,
            "tui.command.execute" => Self::TuiCommandExecute,
            "tui.toast.show" => Self::TuiToastShow,
            "vcs.branch.updated" => Self::VcsBranchUpdated,
            "workspace.ready" => Self::WorkspaceReady,
            "workspace.failed" => Self::WorkspaceFailed,
            "workspace.status" => Self::WorkspaceStatus,
            "worktree.ready" => Self::WorktreeReady,
            "worktree.failed" => Self::WorktreeFailed,
            _ => return None,
        };
        let valid = match event {
            Self::ModelsRefreshed
            | Self::IntegrationUpdated
            | Self::CatalogUpdated
            | Self::ReferenceUpdated
            | Self::LspUpdated => true,
            Self::IntegrationConnectionUpdated => Self::string(properties, "integrationID"),
            Self::FileEdited => Self::string(properties, "file"),
            Self::PluginAdded => Self::string(properties, "id"),
            Self::ProjectDirectoriesUpdated => Self::string(properties, "projectID"),
            Self::FileWatcherUpdated => {
                Self::string(properties, "file")
                    && matches!(
                        properties.get("event").and_then(Value::as_str),
                        Some("add" | "change" | "unlink")
                    )
            }
            Self::PtyCreated | Self::PtyUpdated => properties
                .get("info")
                .and_then(Value::as_object)
                .is_some_and(Self::pty_info),
            Self::PtyExited => {
                properties
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id.starts_with("pty"))
                    && properties.get("exitCode").and_then(Value::as_u64).is_some()
            }
            Self::PtyDeleted => properties
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.starts_with("pty")),
            Self::InstallationUpdated | Self::InstallationUpdateAvailable => {
                Self::string(properties, "version")
            }
            Self::McpToolsChanged => Self::string(properties, "server"),
            Self::McpBrowserOpenFailed => {
                Self::string(properties, "mcpName") && Self::string(properties, "url")
            }
            Self::ProjectUpdated => Self::project(properties),
            Self::TuiPromptAppend => Self::string(properties, "text"),
            Self::TuiCommandExecute => Self::string(properties, "command"),
            Self::TuiToastShow => {
                Self::optional_string(properties, "title")
                    && Self::string(properties, "message")
                    && matches!(
                        properties.get("variant").and_then(Value::as_str),
                        Some("info" | "success" | "warning" | "error")
                    )
                    && properties
                        .get("duration")
                        .is_none_or(|duration| duration.as_u64().is_some_and(|value| value > 0))
            }
            Self::VcsBranchUpdated => Self::optional_string(properties, "branch"),
            Self::WorkspaceReady => Self::string(properties, "name"),
            Self::WorkspaceFailed | Self::WorktreeFailed => Self::string(properties, "message"),
            Self::WorkspaceStatus => {
                properties
                    .get("workspaceID")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id.starts_with("wrk"))
                    && matches!(
                        properties.get("status").and_then(Value::as_str),
                        Some("connected" | "connecting" | "disconnected" | "error")
                    )
            }
            Self::WorktreeReady => {
                Self::string(properties, "name") && Self::optional_string(properties, "branch")
            }
        };
        if !valid {
            return Some(Err(KitError::protocol(
                "private known event data is malformed",
            )));
        }
        Some(Ok(event))
    }

    fn string(properties: &Map<String, Value>, key: &str) -> bool {
        properties.get(key).is_some_and(Value::is_string)
    }

    fn optional_string(properties: &Map<String, Value>, key: &str) -> bool {
        properties.get(key).is_none_or(Value::is_string)
    }

    fn string_array(value: Option<&Value>) -> bool {
        value
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().all(Value::is_string))
    }

    fn pty_info(info: &Map<String, Value>) -> bool {
        info.get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.starts_with("pty"))
            && ["title", "command", "cwd"]
                .into_iter()
                .all(|key| Self::string(info, key))
            && Self::string_array(info.get("args"))
            && matches!(
                info.get("status").and_then(Value::as_str),
                Some("running" | "exited")
            )
            && info.get("pid").and_then(Value::as_u64).is_some()
            && info
                .get("exitCode")
                .is_none_or(|value| value.as_u64().is_some())
    }

    fn project(properties: &Map<String, Value>) -> bool {
        ["id", "worktree"]
            .into_iter()
            .all(|key| Self::string(properties, key))
            && properties
                .get("vcs")
                .is_none_or(|value| value.as_str() == Some("git"))
            && Self::optional_string(properties, "name")
            && properties.get("icon").is_none_or(|icon| {
                icon.as_object().is_some_and(|icon| {
                    ["url", "override", "color"]
                        .into_iter()
                        .all(|key| Self::optional_string(icon, key))
                })
            })
            && properties.get("commands").is_none_or(|commands| {
                commands
                    .as_object()
                    .is_some_and(|commands| Self::optional_string(commands, "start"))
            })
            && properties
                .get("time")
                .and_then(Value::as_object)
                .is_some_and(|time| {
                    ["created", "updated"]
                        .into_iter()
                        .all(|key| time.get(key).and_then(Value::as_u64).is_some())
                        && time
                            .get("initialized")
                            .is_none_or(|value| value.as_u64().is_some())
                })
            && Self::string_array(properties.get("sandboxes"))
    }
}
