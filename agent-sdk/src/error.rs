//! Portable, safely formatted failures for Agent operations.

use std::fmt;
use std::time::Duration;

/// Broad portable failure category. Categories do not establish retry safety.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Invalid passive configuration.
    InvalidConfig,
    /// Invalid caller request.
    InvalidInput,
    /// A portable resource was proven absent.
    NotFound,
    /// Caller input conflicts with durable facts.
    Conflict,
    /// The owning lifecycle is closed.
    Closed,
    /// Existing work prevents admission.
    Busy,
    /// Accepted work durably reached cancellation.
    Cancelled,
    /// A local deadline elapsed.
    Timeout,
    /// The selected implementation cannot preserve requested semantics.
    Unsupported,
    /// The selected adapter does not yet implement the capability.
    NotYetImplemented,
    /// External data contradicts its protocol.
    Protocol,
    /// SDK-owned facts contradict their invariants.
    InvalidState,
    /// A dependency operation failed.
    Integration,
}

/// Caller-visible operation. Accepted streamed and unary work both use `Prompt`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    /// No operation was supplied by the constructor.
    Unspecified,
    /// Construct passive configuration.
    Configure,
    /// Validate passive request data.
    ValidateInput,
    /// Construct a new Agent.
    Build,
    /// Reopen an existing Agent.
    Reopen,
    /// List stored Agents.
    ListAgents,
    /// Read Agent information.
    AgentInfo,
    /// Create a workspace.
    NewWorkspace,
    /// List workspaces.
    ListWorkspaces,
    /// Read workspace information.
    WorkspaceInfo,
    /// Open a workspace handle.
    GetWorkspace,
    /// Check workspace update readiness.
    CheckWorkspaceUpdate,
    /// Update a workspace.
    UpdateWorkspace,
    /// Delete a workspace.
    DeleteWorkspace,
    /// List advertised models.
    ListModels,
    /// List executable tools.
    ListTools,
    /// Ingest managed knowledge.
    IngestKnowledge,
    /// Close a workspace handle.
    CloseWorkspace,
    /// Create a conversation.
    NewConversation,
    /// List conversations.
    ListConversations,
    /// Read conversation information.
    ConversationInfo,
    /// Open a conversation handle.
    GetConversation,
    /// Execute or observe an accepted prompt.
    Prompt,
    /// Abort accepted prompt work.
    Abort,
    /// Read a workspace-relative file.
    ReadFile,
    /// Receive durable notifications.
    NotificationStream,
    /// Resolve a pending interaction.
    ResolveNotification,
    /// Close a conversation.
    CloseConversation,
    /// Shut down an Agent.
    Shutdown,
}

/// Semantic step at which the failure was detected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorPhase {
    /// The detecting step was not specified.
    Unspecified,
    /// Passive configuration validation.
    Configuration,
    /// Request validation.
    InputValidation,
    /// Operation admission.
    Admission,
    /// Client construction.
    ClientSetup,
    /// Process startup.
    ProcessStart,
    /// Server health and protocol verification.
    ServerVerification,
    /// Model or tool inventory verification.
    InventoryVerification,
    /// Waiting for the event owner to become ready.
    EventReady,
    /// Event synchronization.
    EventSynchronization,
    /// Selecting the configured model.
    ModelSelection,
    /// Model generation.
    ModelGeneration,
    /// Executable tool work.
    ToolExecution,
    /// Validating a completed model turn.
    CompletionValidation,
    /// Reading managed knowledge.
    KnowledgeRead,
    /// Writing managed knowledge.
    KnowledgeWrite,
    /// Building a native request.
    RequestBuild,
    /// Sending a native request.
    RequestSend,
    /// Reading a response.
    ResponseRead,
    /// Decoding a response.
    ResponseDecode,
    /// Reading control-plane state.
    StateRead,
    /// Writing control-plane state.
    StateWrite,
    /// Committing control-plane state.
    StateCommit,
    /// Reconciling existing work.
    Reconciliation,
    /// Resolving an interaction.
    InteractionResolution,
    /// Cancelling accepted work.
    Cancellation,
    /// Publishing an outcome to observers.
    Publication,
    /// Settling an owned operation.
    Settlement,
    /// Releasing owned resources.
    Cleanup,
    /// Lifecycle admission or closure.
    Lifecycle,
}

/// Safe operation and detection context.
///
/// ```
/// use agent_sdk::error::{ErrorContext, ErrorPhase, Operation};
/// let context = ErrorContext { operation: Operation::Prompt, phase: ErrorPhase::ModelGeneration };
/// assert_eq!(context.operation, Operation::Prompt);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorContext {
    /// Caller-visible operation.
    pub operation: Operation,
    /// Detecting semantic step.
    pub phase: ErrorPhase,
}

/// The owner of a local deadline, without implying that side effects stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadlineKind {
    /// Construction, shutdown or another lifecycle bound.
    Lifecycle,
    /// One dependency request.
    Request,
    /// One logical operation.
    Operation,
    /// Waiting for an independently owned operation.
    Observation,
}

/// Specific sourced evidence, independent of the broad recovery category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorDetail {
    /// Authentication was rejected.
    AuthenticationRejected,
    /// Permission was denied.
    PermissionDenied,
    /// A remote request was rejected as invalid.
    InvalidRequest,
    /// A dependency resource was absent.
    ResourceMissing,
    /// A remote service reported conflict.
    RemoteConflict,
    /// A request exceeded an accepted size.
    RequestTooLarge,
    /// A remote precondition failed.
    PreconditionFailed,
    /// Resources were exhausted without a more precise classification.
    ResourceExhausted,
    /// An authoritative rate limit was reported.
    RateLimited,
    /// An authoritative quota limit was reported.
    QuotaExceeded,
    /// Billing requirements prevented execution.
    BillingRequired,
    /// A service was unavailable.
    ServiceUnavailable,
    /// A remote internal failure was reported.
    RemoteInternal,
    /// A remote service reported cancellation, not Agent cancellation.
    RemoteCancelled,
    /// A remote service reported its deadline elapsed.
    RemoteDeadlineExceeded,
    /// A remote rejection had no narrower supported classification.
    RemoteRejected,
    /// Dependency transport failed.
    TransportFailure,
    /// A dependency was closed, not necessarily the Agent.
    DependencyClosed,
    /// A dependency cancelled its work, not necessarily the Agent prompt.
    DependencyCancelled,
    /// An owned task terminated unexpectedly.
    TaskTerminated,
    /// Generation reached its output limit.
    OutputLimit,
    /// Generation exceeded a context limit.
    ContextLimit,
    /// A session budget was exhausted.
    SessionLimit,
    /// Generated content was filtered.
    ContentFiltered,
    /// A policy rejected execution.
    PolicyRejected,
    /// Provider-managed continuation is required.
    ContinuationRequired,
    /// Received data could not be decoded.
    MalformedData,
    /// Required response data was absent.
    MissingData,
    /// Received data had an invalid value or shape.
    InvalidData,
    /// An unknown protocol discriminator was received.
    UnknownDiscriminator,
    /// Response correlation did not match its owner.
    CorrelationMismatch,
    /// Protocol steps or fields contradicted each other.
    InvalidSequence,
    /// A stream ended without its required terminal.
    IncompleteStream,
    /// Output did not satisfy the requested schema.
    StructuredOutputMismatch,
    /// A completed tool result violated its output contract.
    InvalidToolOutput,
    /// A supplied revision was stale.
    StaleRevision,
    /// Reopened attachments disagreed with durable configuration.
    AttachmentMismatch,
    /// The StateStore reported an exact transaction conflict.
    TransactionConflict,
    /// Non-idempotent acceptance could not be established.
    AcceptanceUnknown,
    /// Exclusive runtime ownership could not be established.
    OwnershipConflict,
    /// A response exceeded a known configured limit.
    ResponseLimit {
        /// Maximum accepted size or count.
        limit: usize,
    },
    /// A local deadline actually elapsed.
    DeadlineElapsed {
        /// The owner of the deadline.
        kind: DeadlineKind,
        /// Configured duration when known, never inferred from elapsed time.
        timeout: Option<Duration>,
    },
}

/// An opaque, safely formatted failure with bounded normalized evidence.
///
/// Messages supplied by callers must already be redacted. Never include native
/// identities, routes, credentials, storage keys, payloads or foreign prose.
///
/// ```
/// use agent_sdk::{SdkError, error::{ErrorKind, Operation, ErrorPhase, ErrorDetail}};
/// let error = SdkError::integration("model authentication was rejected")
///     .with_context(Operation::Prompt, ErrorPhase::ModelGeneration)
///     .with_detail(ErrorDetail::AuthenticationRejected)
///     .with_http_status(401);
/// assert_eq!(error.kind(), ErrorKind::Integration);
/// assert_eq!(error.http_status(), Some(401));
/// ```
///
/// Runtime failures cannot be reconstructed from untrusted serialized messages.
/// ```compile_fail
/// let error = agent_sdk::SdkError::integration("safe failure");
/// let _ = serde_json::to_string(&error);
/// ```
#[derive(Clone)]
pub struct SdkError {
    data: Box<ErrorData>,
}

#[derive(Clone)]
struct ErrorData {
    kind: ErrorKind,
    context: ErrorContext,
    message: String,
    detail: Option<ErrorDetail>,
    http_status: Option<u16>,
    cause: Option<SdkError>,
    supplementary: Vec<SdkError>,
    truncated: bool,
}

impl SdkError {
    /// Constructs a classified failure with already-redacted caller context.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::new(ErrorKind::Integration, "safe dependency failure");
    /// assert_eq!(error.kind(), ErrorKind::Integration);
    /// ```
    pub fn new(kind: ErrorKind, safe_message: impl Into<String>) -> Self {
        let mut error = Self {
            data: Box::new(ErrorData {
                kind,
                context: ErrorContext {
                    operation: Operation::Unspecified,
                    phase: ErrorPhase::Unspecified,
                },
                message: safe_message.into(),
                detail: None,
                http_status: None,
                cause: None,
                supplementary: Vec::new(),
                truncated: false,
            }),
        };
        error.bound();
        error
    }

    /// Creates an invalid-configuration failure with safe context.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::invalid_config("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::InvalidConfig);
    /// ```
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidConfig, message)
            .with_context(Operation::Configure, ErrorPhase::Configuration)
    }
    /// Creates an invalid-input failure with safe context.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::invalid_input("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::InvalidInput);
    /// ```
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidInput, message)
            .with_context(Operation::ValidateInput, ErrorPhase::InputValidation)
    }
    /// Creates a proven missing-portable-resource failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::not_found("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::NotFound);
    /// ```
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }
    /// Creates a durable-fact conflict.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::conflict("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Conflict);
    /// ```
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }
    /// Creates an owning-lifecycle closure failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::closed("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Closed);
    /// ```
    pub fn closed(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Closed, message).at_phase(ErrorPhase::Lifecycle)
    }
    /// Creates a busy-admission failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::busy("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Busy);
    /// ```
    pub fn busy(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Busy, message).at_phase(ErrorPhase::Admission)
    }
    /// Creates a durable Agent cancellation failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::cancelled("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Cancelled);
    /// ```
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, message).at_phase(ErrorPhase::Cancellation)
    }
    /// Creates a local deadline failure; attach the known owner and duration.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::timeout("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Timeout);
    /// ```
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Timeout, message).with_detail(ErrorDetail::DeadlineElapsed {
            kind: DeadlineKind::Operation,
            timeout: None,
        })
    }
    /// Creates an unsupported-semantics failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::unsupported("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Unsupported);
    /// ```
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }
    /// Creates an incomplete-adapter failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::not_yet_implemented("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::NotYetImplemented);
    /// ```
    pub fn not_yet_implemented(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotYetImplemented, message)
    }
    /// Creates a malformed or contradictory protocol failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::protocol("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Protocol);
    /// ```
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Protocol, message)
    }
    /// Creates an invalid-owned-state failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::invalid_state("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::InvalidState);
    /// ```
    pub fn invalid_state(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidState, message)
    }
    /// Creates a dependency execution failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::integration("safe failure context");
    /// assert_eq!(error.kind(), ErrorKind::Integration);
    /// ```
    pub fn integration(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Integration, message)
    }
    /// Attaches the caller operation and detecting phase without changing evidence.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{Operation, ErrorPhase};
    /// let error = SdkError::protocol("missing reply").with_context(Operation::Prompt, ErrorPhase::ResponseDecode);
    /// assert_eq!(error.context().phase, ErrorPhase::ResponseDecode);
    /// ```
    pub fn with_context(mut self, operation: Operation, phase: ErrorPhase) -> Self {
        self.data.context = ErrorContext { operation, phase };
        self
    }
    /// Attaches sourced portable evidence without changing the broad category.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorDetail};
    /// let error = SdkError::protocol("missing reply").with_detail(ErrorDetail::MissingData);
    /// assert_eq!(error.detail(), Some(&ErrorDetail::MissingData));
    /// ```
    pub fn with_detail(mut self, detail: ErrorDetail) -> Self {
        self.data.detail = Some(detail);
        self
    }
    /// Attaches an observed HTTP status, never an inferred status.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// let error = SdkError::integration("rejected request").with_http_status(403);
    /// assert_eq!(error.http_status(), Some(403));
    /// ```
    pub fn with_http_status(mut self, status: u16) -> Self {
        self.data.http_status = Some(status);
        self
    }
    /// Attaches a normalized causal failure, separately from cleanup failures.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// let error = SdkError::integration("generation failed").with_cause(SdkError::timeout("dependency deadline elapsed"));
    /// assert_eq!(error.cause().unwrap().kind(), ErrorKind::Timeout);
    /// ```
    pub fn with_cause(mut self, cause: Self) -> Self {
        self.data.cause = Some(cause);
        self.bound();
        self
    }
    /// Appends independent cleanup or earlier-attempt evidence in observation order.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// let error = SdkError::protocol("invalid reply").with_supplementary(SdkError::integration("cleanup failed"));
    /// assert_eq!(error.supplementary().len(), 1);
    /// ```
    pub fn with_supplementary(mut self, error: Self) -> Self {
        self.data.supplementary.push(error);
        self.bound();
        self
    }
    /// Returns the broad portable category.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorKind};
    /// assert_eq!(SdkError::closed("agent shut down").kind(), ErrorKind::Closed);
    /// ```
    pub fn kind(&self) -> ErrorKind {
        self.data.kind
    }
    /// Returns the caller operation and detecting phase.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{Operation, ErrorPhase};
    /// let error = SdkError::protocol("invalid reply").with_context(Operation::Prompt, ErrorPhase::ResponseDecode);
    /// assert_eq!(error.context().operation, Operation::Prompt);
    /// ```
    pub fn context(&self) -> &ErrorContext {
        &self.data.context
    }
    /// Returns narrower evidence when a detector established it.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// use agent_sdk::error::{ErrorDetail};
    /// let error = SdkError::integration("output truncated").with_detail(ErrorDetail::OutputLimit);
    /// assert_eq!(error.detail(), Some(&ErrorDetail::OutputLimit));
    /// ```
    pub fn detail(&self) -> Option<&ErrorDetail> {
        self.data.detail.as_ref()
    }
    /// Returns the actual observed HTTP status.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// assert_eq!(SdkError::integration("no HTTP response").http_status(), None);
    /// ```
    pub fn http_status(&self) -> Option<u16> {
        self.data.http_status
    }
    /// Returns the normalized causal failure.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// assert!(SdkError::invalid_input("empty prompt").cause().is_none());
    /// ```
    pub fn cause(&self) -> Option<&Self> {
        self.data.cause.as_ref()
    }
    /// Returns independent supplementary failures in observation order.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// assert!(SdkError::invalid_input("empty prompt").supplementary().is_empty());
    /// ```
    pub fn supplementary(&self) -> &[Self] {
        &self.data.supplementary
    }
    /// Reports whether any retained diagnostic tree evidence was truncated.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// let error = SdkError::integration("x".repeat(5000));
    /// assert!(error.diagnostics_truncated());
    /// assert_eq!(error.message().len(), 4096);
    /// ```
    pub fn diagnostics_truncated(&self) -> bool {
        self.data.truncated
    }
    /// Returns bounded, already-redacted caller context.
    ///
    /// ```
    /// use agent_sdk::SdkError;
    /// let error = SdkError::invalid_input("prompt must be nonempty");
    /// assert_eq!(error.message(), "prompt must be nonempty");
    /// ```
    pub fn message(&self) -> &str {
        &self.data.message
    }

    pub(crate) fn at_operation(mut self, operation: Operation) -> Self {
        self.data.context.operation = operation;
        self.inherit_operation(operation);
        self
    }
    fn inherit_operation(&mut self, operation: Operation) {
        if self.data.context.operation == Operation::Unspecified {
            self.data.context.operation = operation;
        }
        if let Some(cause) = &mut self.data.cause {
            cause.inherit_operation(operation);
        }
        for error in &mut self.data.supplementary {
            error.inherit_operation(operation);
        }
    }
    pub(crate) fn at_phase(mut self, phase: ErrorPhase) -> Self {
        self.data.context.phase = phase;
        self
    }
    pub(crate) fn in_phase(self, phase: ErrorPhase) -> Self {
        if self.context().phase == ErrorPhase::Unspecified {
            self.at_phase(phase)
        } else {
            self
        }
    }
    pub(crate) fn with_diagnostics_truncated(mut self) -> Self {
        self.data.truncated = true;
        self
    }
    pub(crate) fn with_cleanup_result(self, cleanup: Result<(), Self>) -> Self {
        match cleanup {
            Ok(()) => self,
            Err(error) => self.with_supplementary(error.in_phase(ErrorPhase::Cleanup)),
        }
    }

    pub(crate) fn record_cleanup(primary: &mut Option<Self>, error: Self) {
        let error = error.in_phase(ErrorPhase::Cleanup);
        *primary = Some(match primary.take() {
            Some(primary) => primary.with_supplementary(error),
            None => error,
        });
    }

    pub(crate) fn from_state_store(
        source: storage_sdk::StateStoreError,
        message: &'static str,
    ) -> Self {
        use storage_sdk::StateStoreError;
        match source {
            StateStoreError::TransactionConflict { .. } => {
                Self::conflict(message).with_detail(ErrorDetail::TransactionConflict)
            }
            StateStoreError::TimedOut { timeout_ms, .. } => {
                Self::timeout(message).with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Operation,
                    timeout: Some(Duration::from_millis(timeout_ms)),
                })
            }
            StateStoreError::Unavailable { .. } => {
                Self::integration(message).with_detail(ErrorDetail::ServiceUnavailable)
            }
            StateStoreError::InvalidConfig(_) | StateStoreError::InvalidRequest(_) => {
                Self::integration(message).with_detail(ErrorDetail::InvalidData)
            }
            StateStoreError::Serialization(_) => {
                Self::integration(message).with_detail(ErrorDetail::MalformedData)
            }
            StateStoreError::BackendOperation { .. } => Self::integration(message),
        }
    }
    pub(crate) fn from_provider(source: &provider_sdk::SdkError) -> Self {
        use provider_sdk::error::{
            ErrorKind as ProviderKind, ErrorPhase as ProviderPhase, ResponseViolation,
        };
        let (kind, detail) = match source.kind() {
            ProviderKind::Configuration => {
                (ErrorKind::InvalidConfig, Some(ErrorDetail::InvalidData))
            }
            ProviderKind::InvalidInput => (ErrorKind::InvalidInput, Some(ErrorDetail::InvalidData)),
            ProviderKind::Unsupported => (ErrorKind::Unsupported, None),
            ProviderKind::Transport => {
                (ErrorKind::Integration, Some(ErrorDetail::TransportFailure))
            }
            ProviderKind::Timeout => (
                ErrorKind::Timeout,
                Some(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Request,
                    timeout: source.timeout(),
                }),
            ),
            ProviderKind::Closed => (ErrorKind::Integration, Some(ErrorDetail::DependencyClosed)),
            ProviderKind::InvalidState => (ErrorKind::InvalidState, Some(ErrorDetail::InvalidData)),
            ProviderKind::InvalidResponse(violation) => (
                ErrorKind::Protocol,
                Some(match violation {
                    ResponseViolation::MalformedJson | ResponseViolation::InvalidUtf8 => {
                        ErrorDetail::MalformedData
                    }
                    ResponseViolation::MissingField { .. } => ErrorDetail::MissingData,
                    ResponseViolation::InvalidField { .. } => ErrorDetail::InvalidData,
                    ResponseViolation::UnknownVariant { .. } => ErrorDetail::UnknownDiscriminator,
                    ResponseViolation::IdentityMismatch { .. } => ErrorDetail::CorrelationMismatch,
                    ResponseViolation::ProtocolViolation { .. } => ErrorDetail::InvalidSequence,
                    ResponseViolation::UnexpectedEnd => ErrorDetail::IncompleteStream,
                    ResponseViolation::LimitExceeded { limit } => {
                        ErrorDetail::ResponseLimit { limit }
                    }
                }),
            ),
            ProviderKind::InvalidRequest => {
                (ErrorKind::Integration, Some(ErrorDetail::InvalidRequest))
            }
            ProviderKind::Authentication => (
                ErrorKind::Integration,
                Some(ErrorDetail::AuthenticationRejected),
            ),
            ProviderKind::PermissionDenied => {
                (ErrorKind::Integration, Some(ErrorDetail::PermissionDenied))
            }
            ProviderKind::NotFound => (ErrorKind::Integration, Some(ErrorDetail::ResourceMissing)),
            ProviderKind::Conflict => (ErrorKind::Integration, Some(ErrorDetail::RemoteConflict)),
            ProviderKind::RequestTooLarge => {
                (ErrorKind::Integration, Some(ErrorDetail::RequestTooLarge))
            }
            ProviderKind::FailedPrecondition => (
                ErrorKind::Integration,
                Some(ErrorDetail::PreconditionFailed),
            ),
            ProviderKind::ResourceExhausted => {
                (ErrorKind::Integration, Some(ErrorDetail::ResourceExhausted))
            }
            ProviderKind::RateLimited => (ErrorKind::Integration, Some(ErrorDetail::RateLimited)),
            ProviderKind::QuotaExceeded => {
                (ErrorKind::Integration, Some(ErrorDetail::QuotaExceeded))
            }
            ProviderKind::Billing => (ErrorKind::Integration, Some(ErrorDetail::BillingRequired)),
            ProviderKind::ServiceUnavailable => (
                ErrorKind::Integration,
                Some(ErrorDetail::ServiceUnavailable),
            ),
            ProviderKind::Internal => (ErrorKind::Integration, Some(ErrorDetail::RemoteInternal)),
            ProviderKind::Cancelled => (ErrorKind::Integration, Some(ErrorDetail::RemoteCancelled)),
            ProviderKind::DeadlineExceeded => (
                ErrorKind::Integration,
                Some(ErrorDetail::RemoteDeadlineExceeded),
            ),
            ProviderKind::ProviderRejected => {
                (ErrorKind::Integration, Some(ErrorDetail::RemoteRejected))
            }
        };
        let phase = match source.context().phase {
            ProviderPhase::Unspecified => ErrorPhase::Unspecified,
            ProviderPhase::Configuration => ErrorPhase::Configuration,
            ProviderPhase::InputValidation => ErrorPhase::InputValidation,
            ProviderPhase::RequestBuild => ErrorPhase::RequestBuild,
            ProviderPhase::RequestSend | ProviderPhase::SessionSend => ErrorPhase::RequestSend,
            ProviderPhase::ResponseHeaders
            | ProviderPhase::ReadBody
            | ProviderPhase::Stream
            | ProviderPhase::SessionReceive => ErrorPhase::ResponseRead,
            ProviderPhase::Decode => ErrorPhase::ResponseDecode,
            ProviderPhase::Normalize => ErrorPhase::CompletionValidation,
            ProviderPhase::SessionHandshake => ErrorPhase::ClientSetup,
            ProviderPhase::Lifecycle | ProviderPhase::SessionClose => ErrorPhase::Lifecycle,
            ProviderPhase::Cleanup => ErrorPhase::Cleanup,
        };
        let mut error = Self::new(kind, "provider operation failed").at_phase(phase);
        if let Some(detail) = detail {
            error = error.with_detail(detail);
        }
        if let Some(status) = source.http_status() {
            error = error.with_http_status(status);
        }
        if let Some(cause) = source.cause() {
            error = error.with_cause(Self::from_provider(cause));
        }
        for supplementary in source.supplementary() {
            error = error.with_supplementary(Self::from_provider(supplementary));
        }
        if source.diagnostics_truncated() {
            error = error.with_diagnostics_truncated();
        }
        error
    }
    fn bound(&mut self) {
        self.bound_tree(1, &mut 32, &mut (64 * 1024));
    }
    fn bound_tree(&mut self, depth: usize, nodes: &mut usize, bytes: &mut usize) {
        *nodes -= 1;
        let limit = (*bytes).min(4096);
        if self.data.message.len() > limit {
            let mut boundary = limit;
            while !self.data.message.is_char_boundary(boundary) {
                boundary -= 1;
            }
            self.data.message.truncate(boundary);
            self.data.truncated = true;
        }
        *bytes -= self.data.message.len();
        if depth == 8 || *nodes == 0 {
            self.data.truncated |= self.data.cause.is_some() || !self.data.supplementary.is_empty();
            self.data.cause = None;
            self.data.supplementary.clear();
            return;
        }
        if let Some(cause) = &mut self.data.cause {
            cause.bound_tree(depth + 1, nodes, bytes);
            self.data.truncated |= cause.diagnostics_truncated();
        }
        let mut retained = 0;
        for error in &mut self.data.supplementary {
            if *nodes == 0 {
                break;
            }
            error.bound_tree(depth + 1, nodes, bytes);
            self.data.truncated |= error.diagnostics_truncated();
            retained += 1;
        }
        self.data.truncated |= retained < self.data.supplementary.len();
        self.data.supplementary.truncate(retained);
    }
}

impl fmt::Display for SdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} during {:?}/{:?}",
            self.kind(),
            self.context().operation,
            self.context().phase
        )?;
        if let Some(detail) = self.detail() {
            write!(f, ": {detail:?}")?;
        }
        if !self.message().is_empty() {
            write!(f, ": {}", self.message())?;
        }
        if let Some(status) = self.http_status() {
            write!(f, " (HTTP {status})")?;
        }
        if !self.supplementary().is_empty() {
            write!(
                f,
                " ({} supplementary failures)",
                self.supplementary().len()
            )?;
        }
        if self.diagnostics_truncated() {
            f.write_str(" (diagnostics truncated)")?;
        }
        Ok(())
    }
}

impl fmt::Debug for SdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SdkError")
            .field("kind", &self.kind())
            .field("context", self.context())
            .field("detail", &self.detail())
            .field("message", &self.message())
            .field("http_status", &self.http_status())
            .field("cause", &self.cause())
            .field("supplementary", &self.supplementary())
            .field("diagnostics_truncated", &self.diagnostics_truncated())
            .finish()
    }
}

impl std::error::Error for SdkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

impl From<tool_sdk::ToolError> for SdkError {
    fn from(source: tool_sdk::ToolError) -> Self {
        use tool_sdk::ToolError;
        let error = match source {
            ToolError::Timeout { timeout_secs, .. } => Self::timeout(
                "tool operation deadline elapsed",
            )
            .with_detail(ErrorDetail::DeadlineElapsed {
                kind: DeadlineKind::Request,
                timeout: Some(Duration::from_secs(timeout_secs)),
            }),
            ToolError::Closed { .. } => Self::integration("tool runtime is closed")
                .with_detail(ErrorDetail::DependencyClosed),
            ToolError::Cancelled { .. } => Self::integration("tool operation was cancelled")
                .with_detail(ErrorDetail::DependencyCancelled),
            ToolError::InvalidOutput { .. } => Self::integration("tool output is invalid")
                .with_detail(ErrorDetail::InvalidToolOutput),
            ToolError::Protocol { .. } => Self::protocol("tool protocol data is invalid")
                .with_detail(ErrorDetail::MalformedData),
            ToolError::Transport { .. } => Self::integration("tool transport failed")
                .with_detail(ErrorDetail::TransportFailure),
            ToolError::UnknownTool { .. } => Self::integration("selected tool is absent")
                .with_detail(ErrorDetail::ResourceMissing),
            ToolError::InvalidConfiguration { .. } | ToolError::DuplicateName { .. } => {
                Self::integration("tool configuration is invalid")
                    .with_detail(ErrorDetail::InvalidData)
            }
            ToolError::InvalidArguments { .. }
            | ToolError::InvalidInteraction { .. }
            | ToolError::UndeclaredInteraction { .. } => {
                Self::integration("tool input is invalid").with_detail(ErrorDetail::InvalidData)
            }
            ToolError::InteractionDeclined { .. } => {
                Self::integration("tool interaction was declined")
                    .with_detail(ErrorDetail::PermissionDenied)
            }
            ToolError::Unsupported { .. } => Self::unsupported("tool operation is unsupported"),
            ToolError::BuildCleanup { primary, cleanup } => Self::from(*primary)
                .with_supplementary(Self::from(*cleanup).at_phase(ErrorPhase::Cleanup)),
        };
        error.in_phase(ErrorPhase::ToolExecution)
    }
}

impl From<reqwest::Error> for SdkError {
    fn from(source: reqwest::Error) -> Self {
        if source.is_timeout() {
            Self::timeout("HTTP request deadline elapsed")
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Request,
                    timeout: None,
                })
                .at_phase(ErrorPhase::RequestSend)
        } else {
            let error = Self::integration("HTTP operation failed")
                .with_detail(ErrorDetail::TransportFailure);
            match source.status() {
                Some(status) => error.with_http_status(status.as_u16()),
                None => error,
            }
        }
    }
}

impl From<std::io::Error> for SdkError {
    fn from(source: std::io::Error) -> Self {
        match source.kind() {
            std::io::ErrorKind::TimedOut => Self::timeout("IO operation deadline elapsed"),
            std::io::ErrorKind::PermissionDenied => Self::integration("IO permission was denied")
                .with_detail(ErrorDetail::PermissionDenied),
            std::io::ErrorKind::NotFound => Self::integration("IO resource was absent")
                .with_detail(ErrorDetail::ResourceMissing),
            _ => {
                Self::integration("IO operation failed").with_detail(ErrorDetail::TransportFailure)
            }
        }
    }
}
