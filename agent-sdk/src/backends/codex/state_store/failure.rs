use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::SdkError;
use crate::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase, Operation};

/// A private, message-free snapshot of an already bounded portable failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CodexStoredFailure {
    kind: ErrorKind,
    operation: Operation,
    phase: ErrorPhase,
    detail: Option<ErrorDetail>,
    http_status: Option<u16>,
    cause: Option<Box<Self>>,
    supplementary: Vec<Self>,
    truncated: bool,
}

impl CodexStoredFailure {
    pub(super) fn capture(error: &SdkError) -> Self {
        Self {
            kind: error.kind(),
            operation: error.context().operation,
            phase: error.context().phase,
            detail: error.detail().copied(),
            http_status: error.http_status(),
            cause: error.cause().map(|cause| Box::new(Self::capture(cause))),
            supplementary: error.supplementary().iter().map(Self::capture).collect(),
            truncated: error.diagnostics_truncated(),
        }
    }

    pub(super) fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub(super) fn restore(&self) -> SdkError {
        let mut error = SdkError::new(self.kind, "recorded prompt operation failed")
            .with_context(self.operation, self.phase);
        if let Some(detail) = self.detail {
            error = error.with_detail(detail);
        }
        if let Some(status) = self.http_status {
            error = error.with_http_status(status);
        }
        if let Some(cause) = &self.cause {
            error = error.with_cause(cause.restore());
        }
        for supplementary in &self.supplementary {
            error = error.with_supplementary(supplementary.restore());
        }
        if self.truncated {
            error = error.with_diagnostics_truncated();
        }
        error
    }

    fn encode(&self) -> Value {
        serde_json::json!({
            "kind": Self::kind_tag(self.kind),
            "context": {
                "operation": Self::operation_tag(self.operation),
                "phase": Self::phase_tag(self.phase),
            },
            "detail": self.detail.map(Self::encode_detail),
            "http_status": self.http_status,
            "cause": self.cause.as_ref().map(|cause| cause.encode()),
            "supplementary": self.supplementary.iter().map(Self::encode).collect::<Vec<_>>(),
            "truncated": self.truncated,
        })
    }

    fn decode(value: &Value, depth: usize, nodes: &mut usize) -> Result<Self, &'static str> {
        if depth > 8 || *nodes == 32 {
            return Err("stored failure exceeds its diagnostic bound");
        }
        *nodes += 1;
        let object = Self::object(
            value,
            &[
                "kind",
                "context",
                "detail",
                "http_status",
                "cause",
                "supplementary",
                "truncated",
            ],
        )?;
        let kind = Self::decode_kind(
            object["kind"]
                .as_str()
                .ok_or("invalid stored failure kind")?,
        )?;
        let context = Self::object(&object["context"], &["operation", "phase"])?;
        let operation = Self::decode_operation(
            context["operation"]
                .as_str()
                .ok_or("invalid stored failure operation")?,
        )?;
        let phase = Self::decode_phase(
            context["phase"]
                .as_str()
                .ok_or("invalid stored failure phase")?,
        )?;
        let detail = match &object["detail"] {
            Value::Null => None,
            value => Some(Self::decode_detail(value)?),
        };
        let http_status = match &object["http_status"] {
            Value::Null => None,
            value => Some(
                value
                    .as_u64()
                    .and_then(|status| u16::try_from(status).ok())
                    .ok_or("invalid stored failure HTTP status")?,
            ),
        };
        let cause = match &object["cause"] {
            Value::Null => None,
            value => Some(Box::new(Self::decode(value, depth + 1, nodes)?)),
        };
        let supplementary = object["supplementary"]
            .as_array()
            .ok_or("invalid supplementary failures")?
            .iter()
            .map(|value| Self::decode(value, depth + 1, nodes))
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = object["truncated"]
            .as_bool()
            .ok_or("invalid diagnostic truncation fact")?;
        Ok(Self {
            kind,
            operation,
            phase,
            detail,
            http_status,
            cause,
            supplementary,
            truncated,
        })
    }

    fn object<'a>(
        value: &'a Value,
        fields: &[&str],
    ) -> Result<&'a Map<String, Value>, &'static str> {
        value
            .as_object()
            .filter(|object| {
                object.len() == fields.len()
                    && fields.iter().all(|field| object.contains_key(*field))
            })
            .ok_or("stored failure shape is invalid")
    }

    fn kind_tag(kind: ErrorKind) -> &'static str {
        match kind {
            ErrorKind::InvalidConfig => "invalid_config",
            ErrorKind::InvalidInput => "invalid_input",
            ErrorKind::NotFound => "not_found",
            ErrorKind::Conflict => "conflict",
            ErrorKind::Closed => "closed",
            ErrorKind::Busy => "busy",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::NotYetImplemented => "not_yet_implemented",
            ErrorKind::Protocol => "protocol",
            ErrorKind::InvalidState => "invalid_state",
            ErrorKind::Integration => "integration",
        }
    }

    fn decode_kind(tag: &str) -> Result<ErrorKind, &'static str> {
        match tag {
            "invalid_config" => Ok(ErrorKind::InvalidConfig),
            "invalid_input" => Ok(ErrorKind::InvalidInput),
            "not_found" => Ok(ErrorKind::NotFound),
            "conflict" => Ok(ErrorKind::Conflict),
            "closed" => Ok(ErrorKind::Closed),
            "busy" => Ok(ErrorKind::Busy),
            "cancelled" => Ok(ErrorKind::Cancelled),
            "timeout" => Ok(ErrorKind::Timeout),
            "unsupported" => Ok(ErrorKind::Unsupported),
            "not_yet_implemented" => Ok(ErrorKind::NotYetImplemented),
            "protocol" => Ok(ErrorKind::Protocol),
            "invalid_state" => Ok(ErrorKind::InvalidState),
            "integration" => Ok(ErrorKind::Integration),
            _ => Err("unknown stored failure kind"),
        }
    }

    fn operation_tag(operation: Operation) -> &'static str {
        match operation {
            Operation::Unspecified => "unspecified",
            Operation::Configure => "configure",
            Operation::ValidateInput => "validate_input",
            Operation::Build => "build",
            Operation::Reopen => "reopen",
            Operation::ListAgents => "list_agents",
            Operation::AgentInfo => "agent_info",
            Operation::NewWorkspace => "new_workspace",
            Operation::ListWorkspaces => "list_workspaces",
            Operation::WorkspaceInfo => "workspace_info",
            Operation::GetWorkspace => "get_workspace",
            Operation::CheckWorkspaceUpdate => "check_workspace_update",
            Operation::UpdateWorkspace => "update_workspace",
            Operation::DeleteWorkspace => "delete_workspace",
            Operation::ListModels => "list_models",
            Operation::ListTools => "list_tools",
            Operation::IngestKnowledge => "ingest_knowledge",
            Operation::CloseWorkspace => "close_workspace",
            Operation::NewConversation => "new_conversation",
            Operation::ListConversations => "list_conversations",
            Operation::ConversationInfo => "conversation_info",
            Operation::GetConversation => "get_conversation",
            Operation::Prompt => "prompt",
            Operation::Abort => "abort",
            Operation::ReadFile => "read_file",
            Operation::NotificationStream => "notification_stream",
            Operation::ResolveNotification => "resolve_notification",
            Operation::CloseConversation => "close_conversation",
            Operation::Shutdown => "shutdown",
        }
    }

    fn decode_operation(tag: &str) -> Result<Operation, &'static str> {
        match tag {
            "unspecified" => Ok(Operation::Unspecified),
            "configure" => Ok(Operation::Configure),
            "validate_input" => Ok(Operation::ValidateInput),
            "build" => Ok(Operation::Build),
            "reopen" => Ok(Operation::Reopen),
            "list_agents" => Ok(Operation::ListAgents),
            "agent_info" => Ok(Operation::AgentInfo),
            "new_workspace" => Ok(Operation::NewWorkspace),
            "list_workspaces" => Ok(Operation::ListWorkspaces),
            "workspace_info" => Ok(Operation::WorkspaceInfo),
            "get_workspace" => Ok(Operation::GetWorkspace),
            "check_workspace_update" => Ok(Operation::CheckWorkspaceUpdate),
            "update_workspace" => Ok(Operation::UpdateWorkspace),
            "delete_workspace" => Ok(Operation::DeleteWorkspace),
            "list_models" => Ok(Operation::ListModels),
            "list_tools" => Ok(Operation::ListTools),
            "ingest_knowledge" => Ok(Operation::IngestKnowledge),
            "close_workspace" => Ok(Operation::CloseWorkspace),
            "new_conversation" => Ok(Operation::NewConversation),
            "list_conversations" => Ok(Operation::ListConversations),
            "conversation_info" => Ok(Operation::ConversationInfo),
            "get_conversation" => Ok(Operation::GetConversation),
            "prompt" => Ok(Operation::Prompt),
            "abort" => Ok(Operation::Abort),
            "read_file" => Ok(Operation::ReadFile),
            "notification_stream" => Ok(Operation::NotificationStream),
            "resolve_notification" => Ok(Operation::ResolveNotification),
            "close_conversation" => Ok(Operation::CloseConversation),
            "shutdown" => Ok(Operation::Shutdown),
            _ => Err("unknown stored failure operation"),
        }
    }

    fn phase_tag(phase: ErrorPhase) -> &'static str {
        match phase {
            ErrorPhase::Unspecified => "unspecified",
            ErrorPhase::Configuration => "configuration",
            ErrorPhase::InputValidation => "input_validation",
            ErrorPhase::Admission => "admission",
            ErrorPhase::ClientSetup => "client_setup",
            ErrorPhase::ProcessStart => "process_start",
            ErrorPhase::ServerVerification => "server_verification",
            ErrorPhase::InventoryVerification => "inventory_verification",
            ErrorPhase::EventReady => "event_ready",
            ErrorPhase::EventSynchronization => "event_synchronization",
            ErrorPhase::ModelSelection => "model_selection",
            ErrorPhase::ModelGeneration => "model_generation",
            ErrorPhase::ToolExecution => "tool_execution",
            ErrorPhase::CompletionValidation => "completion_validation",
            ErrorPhase::KnowledgeRead => "knowledge_read",
            ErrorPhase::KnowledgeWrite => "knowledge_write",
            ErrorPhase::RequestBuild => "request_build",
            ErrorPhase::RequestSend => "request_send",
            ErrorPhase::ResponseRead => "response_read",
            ErrorPhase::ResponseDecode => "response_decode",
            ErrorPhase::StateRead => "state_read",
            ErrorPhase::StateWrite => "state_write",
            ErrorPhase::StateCommit => "state_commit",
            ErrorPhase::Reconciliation => "reconciliation",
            ErrorPhase::InteractionResolution => "interaction_resolution",
            ErrorPhase::Cancellation => "cancellation",
            ErrorPhase::Publication => "publication",
            ErrorPhase::Settlement => "settlement",
            ErrorPhase::Cleanup => "cleanup",
            ErrorPhase::Lifecycle => "lifecycle",
        }
    }

    fn decode_phase(tag: &str) -> Result<ErrorPhase, &'static str> {
        match tag {
            "unspecified" => Ok(ErrorPhase::Unspecified),
            "configuration" => Ok(ErrorPhase::Configuration),
            "input_validation" => Ok(ErrorPhase::InputValidation),
            "admission" => Ok(ErrorPhase::Admission),
            "client_setup" => Ok(ErrorPhase::ClientSetup),
            "process_start" => Ok(ErrorPhase::ProcessStart),
            "server_verification" => Ok(ErrorPhase::ServerVerification),
            "inventory_verification" => Ok(ErrorPhase::InventoryVerification),
            "event_ready" => Ok(ErrorPhase::EventReady),
            "event_synchronization" => Ok(ErrorPhase::EventSynchronization),
            "model_selection" => Ok(ErrorPhase::ModelSelection),
            "model_generation" => Ok(ErrorPhase::ModelGeneration),
            "tool_execution" => Ok(ErrorPhase::ToolExecution),
            "completion_validation" => Ok(ErrorPhase::CompletionValidation),
            "knowledge_read" => Ok(ErrorPhase::KnowledgeRead),
            "knowledge_write" => Ok(ErrorPhase::KnowledgeWrite),
            "request_build" => Ok(ErrorPhase::RequestBuild),
            "request_send" => Ok(ErrorPhase::RequestSend),
            "response_read" => Ok(ErrorPhase::ResponseRead),
            "response_decode" => Ok(ErrorPhase::ResponseDecode),
            "state_read" => Ok(ErrorPhase::StateRead),
            "state_write" => Ok(ErrorPhase::StateWrite),
            "state_commit" => Ok(ErrorPhase::StateCommit),
            "reconciliation" => Ok(ErrorPhase::Reconciliation),
            "interaction_resolution" => Ok(ErrorPhase::InteractionResolution),
            "cancellation" => Ok(ErrorPhase::Cancellation),
            "publication" => Ok(ErrorPhase::Publication),
            "settlement" => Ok(ErrorPhase::Settlement),
            "cleanup" => Ok(ErrorPhase::Cleanup),
            "lifecycle" => Ok(ErrorPhase::Lifecycle),
            _ => Err("unknown stored failure phase"),
        }
    }

    fn encode_detail(detail: ErrorDetail) -> Value {
        let tag = match detail {
            ErrorDetail::AuthenticationRejected => "authentication_rejected",
            ErrorDetail::PermissionDenied => "permission_denied",
            ErrorDetail::InvalidRequest => "invalid_request",
            ErrorDetail::ResourceMissing => "resource_missing",
            ErrorDetail::RemoteConflict => "remote_conflict",
            ErrorDetail::RequestTooLarge => "request_too_large",
            ErrorDetail::PreconditionFailed => "precondition_failed",
            ErrorDetail::ResourceExhausted => "resource_exhausted",
            ErrorDetail::RateLimited => "rate_limited",
            ErrorDetail::QuotaExceeded => "quota_exceeded",
            ErrorDetail::BillingRequired => "billing_required",
            ErrorDetail::ServiceUnavailable => "service_unavailable",
            ErrorDetail::RemoteInternal => "remote_internal",
            ErrorDetail::RemoteCancelled => "remote_cancelled",
            ErrorDetail::RemoteDeadlineExceeded => "remote_deadline_exceeded",
            ErrorDetail::RemoteRejected => "remote_rejected",
            ErrorDetail::TransportFailure => "transport_failure",
            ErrorDetail::DependencyClosed => "dependency_closed",
            ErrorDetail::DependencyCancelled => "dependency_cancelled",
            ErrorDetail::TaskTerminated => "task_terminated",
            ErrorDetail::OutputLimit => "output_limit",
            ErrorDetail::ContextLimit => "context_limit",
            ErrorDetail::SessionLimit => "session_limit",
            ErrorDetail::ContentFiltered => "content_filtered",
            ErrorDetail::PolicyRejected => "policy_rejected",
            ErrorDetail::ContinuationRequired => "continuation_required",
            ErrorDetail::MalformedData => "malformed_data",
            ErrorDetail::MissingData => "missing_data",
            ErrorDetail::InvalidData => "invalid_data",
            ErrorDetail::UnknownDiscriminator => "unknown_discriminator",
            ErrorDetail::CorrelationMismatch => "correlation_mismatch",
            ErrorDetail::InvalidSequence => "invalid_sequence",
            ErrorDetail::IncompleteStream => "incomplete_stream",
            ErrorDetail::StructuredOutputMismatch => "structured_output_mismatch",
            ErrorDetail::InvalidToolOutput => "invalid_tool_output",
            ErrorDetail::StaleRevision => "stale_revision",
            ErrorDetail::AttachmentMismatch => "attachment_mismatch",
            ErrorDetail::TransactionConflict => "transaction_conflict",
            ErrorDetail::AcceptanceUnknown => "acceptance_unknown",
            ErrorDetail::OwnershipConflict => "ownership_conflict",
            ErrorDetail::ResponseLimit { limit } => {
                return serde_json::json!({"kind":"response_limit", "limit":limit});
            }
            ErrorDetail::DeadlineElapsed { kind, timeout } => {
                return serde_json::json!({
                    "kind":"deadline_elapsed",
                    "deadline": match kind {
                        DeadlineKind::Lifecycle => "lifecycle",
                        DeadlineKind::Request => "request",
                        DeadlineKind::Operation => "operation",
                        DeadlineKind::Observation => "observation",
                    },
                    "timeout": timeout.map(|timeout| serde_json::json!({"seconds":timeout.as_secs(),"nanoseconds":timeout.subsec_nanos()})),
                });
            }
        };
        serde_json::json!({"kind":tag})
    }

    fn decode_detail(value: &Value) -> Result<ErrorDetail, &'static str> {
        let tag = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or("invalid stored detail kind")?;
        if tag == "response_limit" {
            let object = Self::object(value, &["kind", "limit"])?;
            return Ok(ErrorDetail::ResponseLimit {
                limit: object["limit"]
                    .as_u64()
                    .and_then(|limit| usize::try_from(limit).ok())
                    .ok_or("invalid response limit")?,
            });
        }
        if tag == "deadline_elapsed" {
            let object = Self::object(value, &["kind", "deadline", "timeout"])?;
            let kind = match object["deadline"].as_str() {
                Some("lifecycle") => DeadlineKind::Lifecycle,
                Some("request") => DeadlineKind::Request,
                Some("operation") => DeadlineKind::Operation,
                Some("observation") => DeadlineKind::Observation,
                _ => return Err("invalid deadline kind"),
            };
            let timeout = match &object["timeout"] {
                Value::Null => None,
                value => {
                    let duration = Self::object(value, &["seconds", "nanoseconds"])?;
                    let seconds = duration["seconds"]
                        .as_u64()
                        .ok_or("invalid deadline seconds")?;
                    let nanos = duration["nanoseconds"]
                        .as_u64()
                        .filter(|nanos| *nanos < 1_000_000_000)
                        .ok_or("invalid deadline nanoseconds")?;
                    Some(Duration::new(seconds, nanos as u32))
                }
            };
            return Ok(ErrorDetail::DeadlineElapsed { kind, timeout });
        }
        Self::object(value, &["kind"])?;
        match tag {
            "authentication_rejected" => Ok(ErrorDetail::AuthenticationRejected),
            "permission_denied" => Ok(ErrorDetail::PermissionDenied),
            "invalid_request" => Ok(ErrorDetail::InvalidRequest),
            "resource_missing" => Ok(ErrorDetail::ResourceMissing),
            "remote_conflict" => Ok(ErrorDetail::RemoteConflict),
            "request_too_large" => Ok(ErrorDetail::RequestTooLarge),
            "precondition_failed" => Ok(ErrorDetail::PreconditionFailed),
            "resource_exhausted" => Ok(ErrorDetail::ResourceExhausted),
            "rate_limited" => Ok(ErrorDetail::RateLimited),
            "quota_exceeded" => Ok(ErrorDetail::QuotaExceeded),
            "billing_required" => Ok(ErrorDetail::BillingRequired),
            "service_unavailable" => Ok(ErrorDetail::ServiceUnavailable),
            "remote_internal" => Ok(ErrorDetail::RemoteInternal),
            "remote_cancelled" => Ok(ErrorDetail::RemoteCancelled),
            "remote_deadline_exceeded" => Ok(ErrorDetail::RemoteDeadlineExceeded),
            "remote_rejected" => Ok(ErrorDetail::RemoteRejected),
            "transport_failure" => Ok(ErrorDetail::TransportFailure),
            "dependency_closed" => Ok(ErrorDetail::DependencyClosed),
            "dependency_cancelled" => Ok(ErrorDetail::DependencyCancelled),
            "task_terminated" => Ok(ErrorDetail::TaskTerminated),
            "output_limit" => Ok(ErrorDetail::OutputLimit),
            "context_limit" => Ok(ErrorDetail::ContextLimit),
            "session_limit" => Ok(ErrorDetail::SessionLimit),
            "content_filtered" => Ok(ErrorDetail::ContentFiltered),
            "policy_rejected" => Ok(ErrorDetail::PolicyRejected),
            "continuation_required" => Ok(ErrorDetail::ContinuationRequired),
            "malformed_data" => Ok(ErrorDetail::MalformedData),
            "missing_data" => Ok(ErrorDetail::MissingData),
            "invalid_data" => Ok(ErrorDetail::InvalidData),
            "unknown_discriminator" => Ok(ErrorDetail::UnknownDiscriminator),
            "correlation_mismatch" => Ok(ErrorDetail::CorrelationMismatch),
            "invalid_sequence" => Ok(ErrorDetail::InvalidSequence),
            "incomplete_stream" => Ok(ErrorDetail::IncompleteStream),
            "structured_output_mismatch" => Ok(ErrorDetail::StructuredOutputMismatch),
            "invalid_tool_output" => Ok(ErrorDetail::InvalidToolOutput),
            "stale_revision" => Ok(ErrorDetail::StaleRevision),
            "attachment_mismatch" => Ok(ErrorDetail::AttachmentMismatch),
            "transaction_conflict" => Ok(ErrorDetail::TransactionConflict),
            "acceptance_unknown" => Ok(ErrorDetail::AcceptanceUnknown),
            "ownership_conflict" => Ok(ErrorDetail::OwnershipConflict),
            _ => Err("unknown stored failure detail"),
        }
    }
}

impl Serialize for CodexStoredFailure {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.encode().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CodexStoredFailure {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let mut nodes = 0;
        Self::decode(&value, 1, &mut nodes).map_err(serde::de::Error::custom)
    }
}
