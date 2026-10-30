//! Structured failures and explicitly accessed, potentially sensitive native diagnostics.
//!
//! [`KitError`] formats only classification, schema labels, and numeric context. Native
//! messages, identifiers and bodies are available through [`KitError::native_details`]
//! for deliberate inspection; they must not be copied into ordinary logs.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

const DETAIL_STRING_LIMIT: usize = 65_536;
const DETAIL_TOTAL_LIMIT: usize = 1_048_576;
const DETAIL_RECORD_LIMIT: usize = 256;
const ERROR_DEPTH_LIMIT: usize = 8;
const ERROR_NODE_LIMIT: usize = 32;

/// Evidence used to choose a portable classification; it does not imply retry safety.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClassificationBasis {
    /// A caller implementation explicitly selected the classification.
    Explicit,
    /// The status used for classification, independently of any status in the body.
    HttpStatus {
        /// Actual HTTP status used for this classification.
        status: u16,
    },
    /// A recognized native service code.
    NativeCode,
    /// A recognized inner native error type, not the envelope or event type.
    NativeErrorType,
    /// A recognized canonical status name.
    CanonicalStatus,
    /// A recognized numeric RPC status.
    RpcCode,
    /// A recognized reason and domain in the first retained reason record.
    NativeReason,
    /// A native hosted outcome or an actual nonzero execution exit code.
    NativeOutcome,
    /// A remote failure without a more specific supported classification.
    Unclassified,
}

/// A Kit-owned input invariant identified without disclosing caller values.
///
/// Labels are trusted static schema names and rules, never native diagnostic text.
/// Caller implementations can attach one with [`KitError::with_validation_violation`].
///
/// ```
/// use providers_kit::error::ValidationViolation;
/// let violation = ValidationViolation::OutOfRange {
///     field: "generation.temperature", rule: "finite and non-negative",
/// };
/// assert!(format!("{violation:?}").contains("generation.temperature"));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationViolation {
    /// A required field was not supplied.
    MissingField {
        /// Trusted field name.
        field: &'static str,
    },
    /// A required value or collection was empty.
    Empty {
        /// Trusted field name.
        field: &'static str,
    },
    /// A value did not satisfy its required syntax.
    InvalidFormat {
        /// Trusted field name.
        field: &'static str,
        /// Trusted syntax rule, without the rejected value.
        rule: &'static str,
    },
    /// A value violated a non-range invariant.
    InvalidValue {
        /// Trusted field name.
        field: &'static str,
        /// Trusted value constraint.
        rule: &'static str,
    },
    /// A value was outside its permitted range.
    OutOfRange {
        /// Trusted field name.
        field: &'static str,
        /// Trusted range description.
        rule: &'static str,
    },
    /// A value was repeated where uniqueness is required.
    Duplicate {
        /// Trusted field name.
        field: &'static str,
    },
    /// A value or collection exceeded a known limit.
    LimitExceeded {
        /// Trusted field name.
        field: &'static str,
        /// Maximum permitted count or size.
        limit: usize,
    },
    /// Two fields cannot be used together.
    Incompatible {
        /// Trusted field name.
        field: &'static str,
        /// Trusted name of the incompatible field.
        other: &'static str,
    },
    /// The selected implementation cannot support the requested input feature.
    Unsupported {
        /// Trusted field name.
        field: &'static str,
        /// Trusted capability constraint.
        rule: &'static str,
    },
}

/// JSON decoding category, retained independently of a formatted parser message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum JsonErrorCategory {
    /// Input/output failed during decoding.
    Io,
    /// Invalid JSON syntax.
    Syntax,
    /// Valid JSON did not match the expected schema.
    Data,
    /// Input ended before a JSON value was complete.
    Eof,
}

/// Observed JSON value shape, including the absence of a required field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum JsonValueKind {
    /// The field is absent.
    Absent,
    /// JSON null.
    Null,
    /// A JSON boolean.
    Boolean,
    /// A JSON number.
    Number,
    /// A JSON string.
    String,
    /// A JSON array.
    Array,
    /// A JSON object.
    Object,
}

impl JsonValueKind {
    pub(crate) fn of(value: Option<&serde_json::Value>) -> Self {
        match value {
            None => Self::Absent,
            Some(serde_json::Value::Null) => Self::Null,
            Some(serde_json::Value::Bool(_)) => Self::Boolean,
            Some(serde_json::Value::Number(_)) => Self::Number,
            Some(serde_json::Value::String(_)) => Self::String,
            Some(serde_json::Value::Array(_)) => Self::Array,
            Some(serde_json::Value::Object(_)) => Self::Object,
        }
    }
}

/// Exact response observations, accessed deliberately because values may be sensitive.
///
/// The corresponding trusted field name remains in [`ResponseViolation`].
/// Formatting this value never prints observed strings.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseDetails {
    /// Parser location and category; line and column use the parser's numbering.
    Json {
        /// Parser error category.
        category: JsonErrorCategory,
        /// One-based line, when available from the parser.
        line: usize,
        /// Parser column, which may be zero for an immediate input failure.
        column: usize,
    },
    /// A field was absent or had the wrong JSON shape.
    FieldType {
        /// Required shape.
        expected: JsonValueKind,
        /// Observed shape.
        actual: JsonValueKind,
    },
    /// Unrecognized native discriminator text.
    Discriminator {
        /// Exact observed discriminator, subject to documented bounds.
        actual: String,
    },
    /// Two identifiers did not agree.
    Identity {
        /// Expected identifier.
        expected: String,
        /// Received identifier.
        actual: String,
    },
    /// An index was outside the available collection.
    Index {
        /// Received index.
        index: usize,
        /// Number of available entries.
        available: usize,
    },
    /// Observed bytes when a response limit was exceeded.
    Size {
        /// Observed byte count; this may be a lower bound after early termination.
        observed: usize,
    },
}

impl ResponseDetails {
    pub(crate) fn json(error: &serde_json::Error) -> Self {
        Self::Json {
            category: match error.classify() {
                serde_json::error::Category::Io => JsonErrorCategory::Io,
                serde_json::error::Category::Syntax => JsonErrorCategory::Syntax,
                serde_json::error::Category::Data => JsonErrorCategory::Data,
                serde_json::error::Category::Eof => JsonErrorCategory::Eof,
            },
            line: error.line(),
            column: error.column(),
        }
    }

    fn bound(&mut self, budget: &mut DetailBudget) {
        match self {
            Self::Discriminator { actual } => budget.string(actual),
            Self::Identity { expected, actual } => {
                budget.string(expected);
                budget.string(actual);
            }
            Self::Json { .. } | Self::FieldType { .. } | Self::Index { .. } | Self::Size { .. } => {
            }
        }
    }
}

impl fmt::Debug for ResponseDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json {
                category,
                line,
                column,
            } => formatter
                .debug_struct("Json")
                .field("category", category)
                .field("line", line)
                .field("column", column)
                .finish(),
            Self::FieldType { expected, actual } => formatter
                .debug_struct("FieldType")
                .field("expected", expected)
                .field("actual", actual)
                .finish(),
            Self::Index { index, available } => formatter
                .debug_struct("Index")
                .field("index", index)
                .field("available", available)
                .finish(),
            Self::Size { observed } => formatter
                .debug_struct("Size")
                .field("observed", observed)
                .finish(),
            Self::Discriminator { .. } => {
                formatter.write_str("Discriminator { actual: <redacted> }")
            }
            Self::Identity { .. } => {
                formatter.write_str("Identity { expected: <redacted>, actual: <redacted> }")
            }
        }
    }
}

/// Portable failure classification. This classification does not promise retry safety.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Invalid provider configuration.
    Configuration,
    /// Invalid input detected before provider execution.
    InvalidInput,
    /// The implementation cannot perform the requested operation.
    Unsupported,
    /// The connection or transport failed without a more authoritative rejection.
    Transport,
    /// A local operation deadline elapsed.
    Timeout,
    /// The service reported that its deadline elapsed.
    DeadlineExceeded,
    /// Lifecycle admission is closed.
    Closed,
    /// The operation is incompatible with the current local state.
    InvalidState,
    /// The response violates a known schema or protocol invariant.
    InvalidResponse(ResponseViolation),
    /// The service rejected the request as invalid.
    InvalidRequest,
    /// Authentication was rejected.
    Authentication,
    /// The authenticated caller lacks permission.
    PermissionDenied,
    /// A requested model or resource was not found.
    NotFound,
    /// The request conflicts with current service state.
    Conflict,
    /// The request exceeds the service's size limit.
    RequestTooLarge,
    /// A service precondition is not satisfied.
    FailedPrecondition,
    /// Capacity or quota was exhausted; the service did not distinguish which.
    ResourceExhausted,
    /// The service explicitly identified a rate limit.
    RateLimited,
    /// The service explicitly identified exhausted quota.
    QuotaExceeded,
    /// Billing prevents execution.
    Billing,
    /// The service is unavailable or overloaded.
    ServiceUnavailable,
    /// The service reported an internal error.
    Internal,
    /// The service reported cancellation.
    Cancelled,
    /// A remote rejection without a recognized portable category.
    ProviderRejected,
}

impl ErrorKind {
    pub(crate) fn from_http_status(status: u16) -> Self {
        match status {
            400 | 422 => Self::InvalidRequest,
            401 => Self::Authentication,
            402 => Self::Billing,
            403 => Self::PermissionDenied,
            404 => Self::NotFound,
            408 | 504 => Self::DeadlineExceeded,
            409 => Self::Conflict,
            412 => Self::FailedPrecondition,
            413 => Self::RequestTooLarge,
            429 => Self::ResourceExhausted,
            500 => Self::Internal,
            502 | 503 | 529 => Self::ServiceUnavailable,
            _ => Self::ProviderRejected,
        }
    }
    pub(crate) fn from_canonical_status(status: Option<&str>) -> ErrorKind {
        match status {
            Some("INVALID_ARGUMENT" | "OUT_OF_RANGE") => Self::InvalidRequest,
            Some("UNAUTHENTICATED") => Self::Authentication,
            Some("PERMISSION_DENIED") => Self::PermissionDenied,
            Some("NOT_FOUND") => Self::NotFound,
            Some("ALREADY_EXISTS" | "ABORTED") => Self::Conflict,
            Some("RESOURCE_EXHAUSTED") => Self::ResourceExhausted,
            Some("FAILED_PRECONDITION") => Self::FailedPrecondition,
            Some("DEADLINE_EXCEEDED") => Self::DeadlineExceeded,
            Some("CANCELLED") => Self::Cancelled,
            Some("UNAVAILABLE") => Self::ServiceUnavailable,
            Some("INTERNAL" | "DATA_LOSS" | "UNKNOWN") => Self::Internal,
            Some("UNIMPLEMENTED") => Self::Unsupported,
            _ => Self::ProviderRejected,
        }
    }

    pub(crate) fn from_rpc_code(code: Option<i64>) -> ErrorKind {
        match code {
            Some(1) => Self::Cancelled,
            Some(2 | 13 | 15) => Self::Internal,
            Some(3 | 11) => Self::InvalidRequest,
            Some(4) => Self::DeadlineExceeded,
            Some(5) => Self::NotFound,
            Some(6 | 10) => Self::Conflict,
            Some(7) => Self::PermissionDenied,
            Some(8) => Self::ResourceExhausted,
            Some(9) => Self::FailedPrecondition,
            Some(12) => Self::Unsupported,
            Some(14) => Self::ServiceUnavailable,
            Some(16) => Self::Authentication,
            _ => Self::ProviderRejected,
        }
    }
}

/// A response invariant, assigned at its detection site without interpreting prose.
///
/// Field and rule labels are trusted, static schema descriptions. Implementors must
/// never place credentials, payloads, URLs or native messages in these labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseViolation {
    /// JSON could not be decoded into the expected response schema.
    MalformedJson,
    /// Text bytes were not valid UTF-8.
    InvalidUtf8,
    /// A required response field was absent.
    MissingField {
        /// Static schema field name, never the field's value.
        field: &'static str,
    },
    /// A present response field had an invalid value.
    InvalidField {
        /// Static schema field name, never the field's value.
        field: &'static str,
    },
    /// A response discriminator was not recognized.
    UnknownVariant {
        /// Static discriminator field name.
        field: &'static str,
    },
    /// Response identifiers did not agree.
    IdentityMismatch {
        /// Static identity field name.
        field: &'static str,
    },
    /// The response violated an ordering or semantic invariant.
    ProtocolViolation {
        /// Trusted, static description of the invariant.
        rule: &'static str,
    },
    /// A response stream ended before its terminal event.
    UnexpectedEnd,
    /// Response data exceeded a byte limit.
    LimitExceeded {
        /// Maximum permitted bytes.
        limit: usize,
    },
}

impl ResponseViolation {
    pub(crate) fn from_json_error(error: &serde_json::Error, field: &'static str) -> Self {
        match error.classify() {
            serde_json::error::Category::Data => Self::InvalidField { field },
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                Self::MalformedJson
            }
            serde_json::error::Category::Io => Self::ProtocolViolation {
                rule: "JSON input read failure",
            },
        }
    }
}

/// Caller-visible operation associated with a failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    /// No enclosing behavioral call is available (for example, a standalone DTO check).
    Unspecified,
    /// Provider construction.
    Configure,
    /// Standalone input validation.
    ValidateInput,
    /// Exact model selection.
    SelectModel,
    /// Model discovery.
    ListModels,
    /// Model metadata lookup.
    ModelInfo,
    /// File upload.
    UploadFile,
    /// File listing.
    ListFiles,
    /// File metadata lookup.
    FileInfo,
    /// File deletion.
    DeleteFile,
    /// Bounded file download.
    DownloadFile,
    /// Batch status lookup.
    BatchStatus,
    /// Batch cancellation.
    CancelBatch,
    /// Bounded batch result retrieval and normalization.
    BatchResults,
    /// Batch listing.
    ListBatches,
    /// Runtime health check.
    Health,
    /// Shared provider shutdown.
    Shutdown,
    /// Unary generation.
    Generate,
    /// Streaming generation, including stream consumption.
    GenerateStream,
    /// Embedding generation.
    Embed,
    /// Batch submission.
    SubmitBatch,
    /// Stateful session opening.
    OpenSession,
    /// Sending a session command.
    SessionSend,
    /// Receiving a session event.
    SessionReceive,
    /// Session closure.
    SessionClose,
}

/// Stage at which the failure was observed; it does not assert that execution is safe to retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorPhase {
    /// A caller implementation supplied no finer phase.
    Unspecified,
    /// Provider configuration validation.
    Configuration,
    /// Local request validation.
    InputValidation,
    /// Request encoding or construction.
    RequestBuild,
    /// Sending a request or opening its transport.
    RequestSend,
    /// Reading or validating the response status and headers.
    ResponseHeaders,
    /// Reading response bytes.
    ReadBody,
    /// Decoding a response or frame.
    Decode,
    /// Converting decoded native data to portable values.
    Normalize,
    /// A service error or protocol failure during a generation stream.
    Stream,
    /// Session connection negotiation.
    SessionHandshake,
    /// Sending a session command.
    SessionSend,
    /// Receiving a session event.
    SessionReceive,
    /// Closing a session connection.
    SessionClose,
    /// Lifecycle admission or shutdown.
    Lifecycle,
    /// Releasing owned resources after an operation.
    Cleanup,
}

/// Safe operation context, including the location of a failed batch result row.
///
/// ```
/// use providers_kit::error::{ErrorContext, ErrorPhase, Operation};
/// let context = ErrorContext {
///     operation: Operation::BatchResults, phase: ErrorPhase::Normalize, row: Some(0),
/// };
/// assert_eq!(context.row, Some(0));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorContext {
    /// The enclosing public operation.
    pub operation: Operation,
    /// The stage at which this error was detected.
    pub phase: ErrorPhase,
    /// Zero-based logical batch result row, excluding blank JSONL lines.
    pub row: Option<usize>,
}

/// One structured native fact with its meaning preserved across providers.
///
/// Unknown native fields remain in the bounded body instead of an untyped public map.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeDetail {
    /// A reason is meaningful within its reported domain.
    Reason {
        /// Native reason domain.
        domain: String,
        /// Native reason value.
        reason: String,
    },
    /// A resource explicitly identified by the service, not a caller correlation ID.
    Resource {
        /// Native resource type, if supplied.
        resource_type: Option<String>,
        /// Exact native resource name.
        name: String,
        /// Native resource owner, if supplied.
        owner: Option<String>,
        /// Native description, if supplied.
        description: Option<String>,
    },
    /// Native request field validation failure.
    FieldViolation {
        /// Native field path.
        field: String,
        /// Native reason, when supplied.
        reason: Option<String>,
        /// Native diagnostic prose, when supplied.
        description: Option<String>,
    },
    /// Native retry guidance; it does not authorize an automatic retry.
    RetryDelay {
        /// Whole nonnegative seconds.
        seconds: u64,
        /// Reported fractional nanoseconds.
        nanos: u32,
    },
    /// Native quota violation.
    QuotaViolation {
        /// Native quota subject, when supplied.
        subject: Option<String>,
        /// Native diagnostic prose, when supplied.
        description: Option<String>,
    },
    /// Native precondition failure.
    PreconditionViolation {
        /// Native precondition kind, when supplied.
        kind: Option<String>,
        /// Native subject, when supplied.
        subject: Option<String>,
        /// Native diagnostic prose, when supplied.
        description: Option<String>,
    },
}

impl NativeDetail {
    fn strings(&self) -> [Option<&str>; 4] {
        match self {
            Self::Reason { domain, reason } => [Some(domain), Some(reason), None, None],
            Self::Resource {
                resource_type,
                name,
                owner,
                description,
            } => [
                resource_type.as_deref(),
                Some(name),
                owner.as_deref(),
                description.as_deref(),
            ],
            Self::FieldViolation {
                field,
                reason,
                description,
            } => [Some(field), reason.as_deref(), description.as_deref(), None],
            Self::QuotaViolation {
                subject,
                description,
            } => [subject.as_deref(), description.as_deref(), None, None],
            Self::PreconditionViolation {
                kind,
                subject,
                description,
            } => [
                kind.as_deref(),
                subject.as_deref(),
                description.as_deref(),
                None,
            ],
            Self::RetryDelay { .. } => [None, None, None, None],
        }
    }

    fn bound(&mut self, budget: &mut DetailBudget) {
        match self {
            Self::Reason { domain, reason } => {
                budget.string(domain);
                budget.string(reason);
            }
            Self::Resource {
                resource_type,
                name,
                owner,
                description,
            } => {
                budget.string(name);
                for value in [resource_type, owner, description].into_iter().flatten() {
                    budget.string(value);
                }
            }
            Self::FieldViolation {
                field,
                reason,
                description,
            } => {
                budget.string(field);
                for value in [reason, description].into_iter().flatten() {
                    budget.string(value);
                }
            }
            Self::QuotaViolation {
                subject,
                description,
            } => {
                for value in [subject, description].into_iter().flatten() {
                    budget.string(value);
                }
            }
            Self::PreconditionViolation {
                kind,
                subject,
                description,
            } => {
                for value in [kind, subject, description].into_iter().flatten() {
                    budget.string(value);
                }
            }
            Self::RetryDelay { .. } => {}
        }
    }
}

impl fmt::Debug for NativeDetail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Reason { .. } => "Reason",
            Self::Resource { .. } => "Resource",
            Self::FieldViolation { .. } => "FieldViolation",
            Self::RetryDelay { .. } => "RetryDelay",
            Self::QuotaViolation { .. } => "QuotaViolation",
            Self::PreconditionViolation { .. } => "PreconditionViolation",
        };
        formatter.write_str(name)?;
        formatter.write_str(" { details: <redacted> }")
    }
}

struct DetailBudget {
    remaining: usize,
    remaining_nodes: usize,
    truncated: bool,
}

impl DetailBudget {
    fn new() -> Self {
        Self {
            remaining: DETAIL_TOTAL_LIMIT,
            remaining_nodes: ERROR_NODE_LIMIT,
            truncated: false,
        }
    }

    fn string(&mut self, value: &mut String) {
        let limit = DETAIL_STRING_LIMIT.min(self.remaining);
        if value.len() > limit {
            let mut end = limit;
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            value.truncate(end);
            self.truncated = true;
        }
        self.remaining -= value.len();
    }
}

/// Potentially sensitive native details, available only through explicit inspection.
///
/// ```
/// use providers_kit::error::NativeErrorDetails;
/// let details = NativeErrorDetails { message: Some("private payload".into()), ..Default::default() };
/// assert!(!format!("{details:?}").contains("private payload"));
/// ```
///
/// All string fields are bounded when attached to [`KitError`]. `Debug` deliberately
/// hides them, including native codes and request IDs, because a service or proxy can
/// return arbitrary text in any of them. Raw bodies retain provider-specific fields
/// not covered by the portable metadata vocabulary.
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeErrorDetails {
    /// Native or Kit diagnostic prose, potentially containing caller input.
    pub message: Option<String>,
    /// Bounded native response or frame body, potentially containing secrets.
    pub body: Option<String>,
    /// Native error code; its meaning depends on the wire protocol.
    pub code: Option<String>,
    /// Native error type, distinct from its code.
    pub error_type: Option<String>,
    /// Native event discriminator, distinct from the inner error type.
    pub event_type: Option<String>,
    /// Native hosted execution outcome.
    pub outcome: Option<String>,
    /// Native parameter associated with the failure.
    pub parameter: Option<String>,
    /// Native request correlation ID.
    pub request_id: Option<String>,
    /// ID of the server event reporting the failure.
    pub event_id: Option<String>,
    /// ID of the client event that caused the failure.
    pub related_event_id: Option<String>,
    /// Native tool-call correlation, never a missing-resource identifier.
    pub tool_call_id: Option<String>,
    /// Native stream event sequence number, when reported.
    pub sequence_number: Option<u64>,
    /// Native status name (for example, a Google RPC canonical status).
    pub native_status: Option<String>,
    /// HTTP status reported by a native body or embedded result, not an invented outer status.
    pub reported_http_status: Option<u16>,
    /// Native RPC canonical numeric code, distinct from HTTP status.
    pub rpc_code: Option<i64>,
    /// Native execution exit code, distinct from HTTP and RPC codes.
    pub execution_exit_code: Option<i64>,
    /// Native Retry-After header; no automatic retry is implied.
    pub retry_after: Option<String>,
    /// Request URL, potentially containing credentials or private resource names.
    pub url: Option<String>,
    /// Repeated structured native evidence; at most 256 records are retained.
    pub details: Vec<NativeDetail>,
    /// At least one detail was truncated or the body was known to be incomplete.
    pub truncated: bool,
    /// Number of structured native records omitted during bounded capture.
    pub omitted_details: usize,
}

impl NativeErrorDetails {
    fn bound(&mut self) {
        let mut budget = DetailBudget::new();
        self.bound_with_budget(&mut budget);
    }

    fn bound_with_budget(&mut self, budget: &mut DetailBudget) {
        // Reserve the named classification evidence before optional diagnostics.
        for value in [
            &mut self.code,
            &mut self.error_type,
            &mut self.native_status,
            &mut self.outcome,
        ]
        .into_iter()
        .flatten()
        {
            budget.string(value);
        }
        // The first reason is the one used by a NativeReason classifier. Retain
        // that record even when it occurs after the general record limit.
        let reason_index = self
            .details
            .iter()
            .position(|detail| matches!(detail, NativeDetail::Reason { .. }));
        if let Some(index) = reason_index {
            self.details[index].bound(budget);
            if index >= DETAIL_RECORD_LIMIT {
                let reason = self.details.remove(index);
                self.details.insert(0, reason);
            }
        }
        let omitted = self.details.len().saturating_sub(DETAIL_RECORD_LIMIT);
        self.omitted_details = self.omitted_details.saturating_add(omitted);
        self.details.truncate(DETAIL_RECORD_LIMIT);
        let retained_reason = self
            .details
            .iter()
            .position(|detail| matches!(detail, NativeDetail::Reason { .. }));
        for (index, detail) in self.details.iter_mut().enumerate() {
            if Some(index) != retained_reason {
                detail.bound(budget);
            }
        }
        for value in [
            &mut self.event_type,
            &mut self.parameter,
            &mut self.request_id,
            &mut self.event_id,
            &mut self.related_event_id,
            &mut self.tool_call_id,
            &mut self.retry_after,
            &mut self.url,
            &mut self.message,
            &mut self.body,
        ]
        .into_iter()
        .flatten()
        {
            budget.string(value);
        }
        self.truncated |= budget.truncated || self.omitted_details != 0;
    }

    fn validate(&self) -> Result<(), KitError> {
        if self.details.len() > DETAIL_RECORD_LIMIT {
            return Err(KitError::invalid_input(
                ValidationViolation::LimitExceeded {
                    field: "native_details.details",
                    limit: DETAIL_RECORD_LIMIT,
                },
                "native details exceed 256 records",
            ));
        }
        if self.omitted_details != 0 && !self.truncated {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidValue {
                    field: "native_details.truncated",
                    rule: "required when detail records were omitted",
                },
                "omitted native details require a truncation marker",
            ));
        }
        let named = [
            self.message.as_deref(),
            self.body.as_deref(),
            self.code.as_deref(),
            self.error_type.as_deref(),
            self.event_type.as_deref(),
            self.outcome.as_deref(),
            self.parameter.as_deref(),
            self.request_id.as_deref(),
            self.event_id.as_deref(),
            self.related_event_id.as_deref(),
            self.tool_call_id.as_deref(),
            self.native_status.as_deref(),
            self.retry_after.as_deref(),
            self.url.as_deref(),
        ];
        let strings = named.into_iter().flatten().chain(
            self.details
                .iter()
                .flat_map(|detail| detail.strings().into_iter().flatten()),
        );
        let mut total = 0_usize;
        for value in strings {
            if value.len() > DETAIL_STRING_LIMIT {
                return Err(KitError::invalid_input(
                    ValidationViolation::LimitExceeded {
                        field: "native_details.string",
                        limit: DETAIL_STRING_LIMIT,
                    },
                    "native detail exceeds 64 KiB",
                ));
            }
            total = total.saturating_add(value.len());
        }
        if total > DETAIL_TOTAL_LIMIT {
            return Err(KitError::invalid_input(
                ValidationViolation::LimitExceeded {
                    field: "native_details",
                    limit: DETAIL_TOTAL_LIMIT,
                },
                "native details exceed 1 MiB in total",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for NativeErrorDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeErrorDetails")
            .field("details", &"<redacted>")
            .field("truncated", &self.truncated)
            .field("omitted_details", &self.omitted_details)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RemoteCategory {
    Unsupported,
    InvalidRequest,
    Authentication,
    PermissionDenied,
    NotFound,
    Conflict,
    RequestTooLarge,
    FailedPrecondition,
    ResourceExhausted,
    RateLimited,
    QuotaExceeded,
    Billing,
    ServiceUnavailable,
    Internal,
    DeadlineExceeded,
    Cancelled,
    ProviderRejected,
}

impl RemoteCategory {
    fn from_kind(kind: ErrorKind) -> Result<Self, KitError> {
        match kind {
            ErrorKind::Unsupported => Ok(Self::Unsupported),
            ErrorKind::InvalidRequest => Ok(Self::InvalidRequest),
            ErrorKind::Authentication => Ok(Self::Authentication),
            ErrorKind::PermissionDenied => Ok(Self::PermissionDenied),
            ErrorKind::NotFound => Ok(Self::NotFound),
            ErrorKind::Conflict => Ok(Self::Conflict),
            ErrorKind::RequestTooLarge => Ok(Self::RequestTooLarge),
            ErrorKind::FailedPrecondition => Ok(Self::FailedPrecondition),
            ErrorKind::ResourceExhausted => Ok(Self::ResourceExhausted),
            ErrorKind::RateLimited => Ok(Self::RateLimited),
            ErrorKind::QuotaExceeded => Ok(Self::QuotaExceeded),
            ErrorKind::Billing => Ok(Self::Billing),
            ErrorKind::ServiceUnavailable => Ok(Self::ServiceUnavailable),
            ErrorKind::Internal => Ok(Self::Internal),
            ErrorKind::DeadlineExceeded => Ok(Self::DeadlineExceeded),
            ErrorKind::Cancelled => Ok(Self::Cancelled),
            ErrorKind::ProviderRejected => Ok(Self::ProviderRejected),
            ErrorKind::Configuration
            | ErrorKind::InvalidInput
            | ErrorKind::Transport
            | ErrorKind::Timeout
            | ErrorKind::Closed
            | ErrorKind::InvalidState
            | ErrorKind::InvalidResponse(_) => Err(KitError::invalid_input(
                ValidationViolation::InvalidValue {
                    field: "remote_failure.kind",
                    rule: "must classify a remote outcome",
                },
                "remote failure cannot use a local or response-schema classification",
            )),
        }
    }

    fn kind(self) -> ErrorKind {
        match self {
            Self::Unsupported => ErrorKind::Unsupported,
            Self::InvalidRequest => ErrorKind::InvalidRequest,
            Self::Authentication => ErrorKind::Authentication,
            Self::PermissionDenied => ErrorKind::PermissionDenied,
            Self::NotFound => ErrorKind::NotFound,
            Self::Conflict => ErrorKind::Conflict,
            Self::RequestTooLarge => ErrorKind::RequestTooLarge,
            Self::FailedPrecondition => ErrorKind::FailedPrecondition,
            Self::ResourceExhausted => ErrorKind::ResourceExhausted,
            Self::RateLimited => ErrorKind::RateLimited,
            Self::QuotaExceeded => ErrorKind::QuotaExceeded,
            Self::Billing => ErrorKind::Billing,
            Self::ServiceUnavailable => ErrorKind::ServiceUnavailable,
            Self::Internal => ErrorKind::Internal,
            Self::DeadlineExceeded => ErrorKind::DeadlineExceeded,
            Self::Cancelled => ErrorKind::Cancelled,
            Self::ProviderRejected => ErrorKind::ProviderRejected,
        }
    }
}

/// An opaque remote failure, including failures inside successful batch or hosted results.
///
/// Serialization deliberately exports all retained native evidence, including the
/// bounded body. It is not a redacted logging format. Deserialization rejects invalid
/// categories, inconsistent evidence and values exceeding the documented bounds.
///
/// ```
/// use providers_kit::error::{ErrorKind, NativeErrorDetails, RemoteFailure};
/// let failure = RemoteFailure::new(ErrorKind::NotFound, NativeErrorDetails::default())?;
/// assert_eq!(failure.kind(), ErrorKind::NotFound);
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone, Eq, PartialEq)]
pub struct RemoteFailure {
    category: RemoteCategory,
    basis: ClassificationBasis,
    details: Box<NativeErrorDetails>,
}

impl RemoteFailure {
    /// Constructs an explicitly classified caller failure without claiming native inference.
    ///
    /// Details must already fit 64 KiB per string, 1 MiB total string bytes, and 256
    /// repeated records. Oversized caller evidence is rejected rather than discarded.
    ///
    /// # Errors
    ///
    /// Returns an input error for local-only categories, invalid numeric/detail fields, or evidence exceeding the documented capture bounds. Explicit classification does not authenticate origin or establish retry safety.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{RemoteFailure, NativeErrorDetails, ErrorKind, ClassificationBasis};
    /// let failure = RemoteFailure::new(
    ///     ErrorKind::NotFound,
    ///     NativeErrorDetails {
    ///         message: Some("private detail".into()),
    ///         ..Default::default()
    ///     },
    /// )?;
    /// assert_eq!(failure.kind(), ErrorKind::NotFound);
    /// assert_eq!(failure.classification_basis(), ClassificationBasis::Explicit);
    /// assert_eq!(failure.native_details().message.as_deref(), Some("private detail"));
    /// assert!(!format!("{failure:?}").contains("private detail"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn new(kind: ErrorKind, details: NativeErrorDetails) -> Result<Self, KitError> {
        let category = RemoteCategory::from_kind(kind)?;
        details.validate()?;
        Ok(Self {
            category,
            basis: ClassificationBasis::Explicit,
            details: Box::new(details),
        })
    }

    /// Returns the same portable classification vocabulary used by [`KitError`].
    ///
    /// See [`crate::error::RemoteFailure::new`] for a complete example.
    ///
    pub fn kind(&self) -> ErrorKind {
        self.category.kind()
    }

    /// Returns the evidence used for classification, independently of the retained body.
    ///
    /// See [`crate::error::RemoteFailure::new`] for a complete example.
    ///
    pub fn classification_basis(&self) -> ClassificationBasis {
        self.basis
    }

    /// Explicitly exposes potentially sensitive native evidence.
    ///
    /// See [`crate::error::RemoteFailure::new`] for a complete example.
    ///
    pub fn native_details(&self) -> &NativeErrorDetails {
        &self.details
    }

    pub(crate) fn from_native(
        kind: ErrorKind,
        basis: ClassificationBasis,
        mut details: NativeErrorDetails,
    ) -> Result<Self, KitError> {
        let category = RemoteCategory::from_kind(kind)?;
        details.bound();
        let failure = Self {
            category,
            basis,
            details: Box::new(details),
        };
        failure.validate()?;
        Ok(failure)
    }

    fn validate(&self) -> Result<(), KitError> {
        self.details.validate()?;
        let present = match self.basis {
            ClassificationBasis::Explicit => true,
            ClassificationBasis::HttpStatus { status } => {
                (100..=599).contains(&status)
                    && !(200..300).contains(&status)
                    && self.kind() == ErrorKind::from_http_status(status)
            }
            ClassificationBasis::NativeCode => self
                .details
                .code
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()),
            ClassificationBasis::NativeErrorType => self
                .details
                .error_type
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()),
            ClassificationBasis::CanonicalStatus => {
                self.details.native_status.as_deref().is_some_and(|value| {
                    let kind = ErrorKind::from_canonical_status(Some(value));
                    kind != ErrorKind::ProviderRejected && self.kind() == kind
                })
            }
            ClassificationBasis::RpcCode => self.details.rpc_code.is_some_and(|code| {
                let kind = ErrorKind::from_rpc_code(Some(code));
                kind != ErrorKind::ProviderRejected && self.kind() == kind
            }),
            ClassificationBasis::NativeReason => self
                .details
                .details
                .iter()
                .find_map(|detail| match detail {
                    NativeDetail::Reason { domain, reason } => {
                        Some(!domain.trim().is_empty() && !reason.trim().is_empty())
                    }
                    _ => None,
                })
                .unwrap_or(false),
            ClassificationBasis::NativeOutcome => match self.details.outcome.as_deref() {
                Some("OUTCOME_FAILED" | "URL_RETRIEVAL_STATUS_ERROR") => {
                    self.kind() == ErrorKind::ProviderRejected
                }
                Some("OUTCOME_DEADLINE_EXCEEDED") => self.kind() == ErrorKind::DeadlineExceeded,
                Some("URL_RETRIEVAL_STATUS_PAYWALL" | "URL_RETRIEVAL_STATUS_UNSAFE") => {
                    self.kind() == ErrorKind::PermissionDenied
                }
                None => {
                    self.details
                        .execution_exit_code
                        .is_some_and(|code| code != 0)
                        && self.kind() == ErrorKind::ProviderRejected
                }
                Some(_) => false,
            },
            ClassificationBasis::Unclassified => self.category == RemoteCategory::ProviderRejected,
        };
        if !present {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidValue {
                    field: "remote_failure.basis",
                    rule: "requires corresponding classification evidence",
                },
                "remote classification basis does not have corresponding evidence",
            ));
        }
        Ok(())
    }
}

impl Serialize for RemoteFailure {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            kind: RemoteCategory,
            basis: ClassificationBasis,
            details: &'a NativeErrorDetails,
        }
        Wire {
            kind: self.category,
            basis: self.basis,
            details: &self.details,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RemoteFailure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            kind: RemoteCategory,
            basis: ClassificationBasis,
            details: NativeErrorDetails,
        }
        let wire = Wire::deserialize(deserializer)?;
        let failure = Self {
            category: wire.kind,
            basis: wire.basis,
            details: Box::new(wire.details),
        };
        failure.validate().map_err(serde::de::Error::custom)?;
        Ok(failure)
    }
}

impl fmt::Display for RemoteFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "remote {:?} ({:?})", self.kind(), self.basis)
    }
}

impl fmt::Debug for RemoteFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteFailure")
            .field("kind", &self.kind())
            .field("basis", &self.basis)
            .field("details", &self.details)
            .finish()
    }
}

impl std::error::Error for RemoteFailure {}

#[derive(Clone)]
enum ErrorFailure {
    Classified {
        kind: ErrorKind,
        details: Box<NativeErrorDetails>,
    },
    Remote(RemoteFailure),
}

impl ErrorFailure {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Classified { kind, .. } => *kind,
            Self::Remote(failure) => failure.kind(),
        }
    }

    fn details(&self) -> &NativeErrorDetails {
        match self {
            Self::Classified { details, .. } => details,
            Self::Remote(failure) => &failure.details,
        }
    }

    fn details_mut(&mut self) -> &mut NativeErrorDetails {
        match self {
            Self::Classified { details, .. } => details,
            Self::Remote(failure) => &mut failure.details,
        }
    }
}

#[derive(Clone)]
struct ErrorData {
    failure: ErrorFailure,
    validation: Option<ValidationViolation>,
    truncated: bool,
    status: Option<u16>,
    timeout: Option<Duration>,
    response: Option<ResponseDetails>,
    request_target: Option<String>,
    correlation_id: Option<String>,
    cause: Option<Box<KitError>>,
    supplementary: Vec<KitError>,
}

impl ErrorData {
    fn bound(&mut self) {
        self.bound_tree(&mut DetailBudget::new(), 1);
    }

    fn bound_tree(&mut self, budget: &mut DetailBudget, depth: usize) {
        budget.remaining_nodes -= 1;
        let previous_truncation = budget.truncated;
        budget.truncated = false;
        // Retain this node's observations and local correlation before its native body.
        if let Some(response) = self.response.as_mut() {
            response.bound(budget);
        }
        for value in [&mut self.request_target, &mut self.correlation_id]
            .into_iter()
            .flatten()
        {
            budget.string(value);
        }
        self.failure.details_mut().bound_with_budget(budget);
        self.truncated |= self.failure.details().truncated;
        if depth == ERROR_DEPTH_LIMIT {
            self.truncated |= self.cause.is_some() || !self.supplementary.is_empty();
            self.cause = None;
            self.supplementary.clear();
        } else {
            if self.cause.as_ref().is_some_and(|cause| {
                budget.remaining_nodes == 0 || !cause.data.retains_remote_evidence(budget)
            }) {
                self.cause = None;
                self.truncated = true;
            } else if let Some(cause) = self.cause.as_mut() {
                cause.data.bound_tree(budget, depth + 1);
            }
            let mut omitted = false;
            self.supplementary.retain_mut(|child| {
                if budget.remaining_nodes == 0 || !child.data.retains_remote_evidence(budget) {
                    omitted = true;
                    false
                } else {
                    child.data.bound_tree(budget, depth + 1);
                    true
                }
            });
            self.truncated |= omitted;
        }
        self.truncated |= budget.truncated;
        budget.truncated |= previous_truncation || self.truncated;
    }

    fn retains_remote_evidence(&self, available: &DetailBudget) -> bool {
        let ErrorFailure::Remote(failure) = &self.failure else {
            return true;
        };
        let mut budget = DetailBudget {
            remaining: available.remaining,
            remaining_nodes: available.remaining_nodes,
            truncated: false,
        };
        if let Some(mut response) = self.response.clone() {
            response.bound(&mut budget);
        }
        for value in [&self.request_target, &self.correlation_id]
            .into_iter()
            .flatten()
        {
            budget.string(&mut value.clone());
        }
        let mut retained = failure.clone();
        retained.details.bound_with_budget(&mut budget);
        // A child is optional diagnostic context. Omit its subtree instead of
        // exposing an invalid serializable RemoteFailure or changing its basis.
        retained.validate().is_ok()
    }
}

/// Structured error returned by the portable provider API.
///
/// ```
/// use providers_kit::KitError;
/// use providers_kit::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
/// let error = KitError::new(
///     ErrorKind::InvalidResponse(ResponseViolation::MissingField { field: "modelVersion" }),
///     Operation::BatchResults,
///     ErrorPhase::Normalize,
/// ).with_row(0);
/// assert_eq!(error.context().row, Some(0));
/// assert!(error.to_string().contains("modelVersion"));
/// ```
///
/// Runtime errors are deliberately not serializable. Accepted remote outcomes use
/// [`RemoteFailure`]; runtime errors may contain trusted static schema labels and
/// causal Kit failures that must not be reconstructed from untrusted JSON.
///
/// ```compile_fail
/// use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
/// let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::RequestSend);
/// let _ = serde_json::to_string(&error);
/// ```
#[derive(Clone)]
pub struct KitError {
    context: ErrorContext,
    data: Box<ErrorData>,
}

impl KitError {
    /// Constructs a classified failure without native details.
    ///
    /// ```
    /// use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// let error = KitError::new(ErrorKind::Unsupported, Operation::Embed, ErrorPhase::InputValidation);
    /// assert_eq!(error.kind(), ErrorKind::Unsupported);
    /// ```
    pub fn new(kind: ErrorKind, operation: Operation, phase: ErrorPhase) -> Self {
        Self {
            context: ErrorContext {
                operation,
                phase,
                row: None,
            },
            data: Box::new(ErrorData {
                failure: ErrorFailure::Classified {
                    kind,
                    details: Box::default(),
                },
                validation: None,
                truncated: false,
                status: None,
                timeout: None,
                response: None,
                request_target: None,
                correlation_id: None,
                cause: None,
                supplementary: Vec::new(),
            }),
        }
    }

    /// Attaches a remote failure to the operation that observed it.
    ///
    /// This does not invent an outer HTTP status from an embedded result or stream.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{RemoteFailure, NativeErrorDetails, ErrorKind, ClassificationBasis};
    /// let failure = RemoteFailure::new(
    ///     ErrorKind::NotFound,
    ///     NativeErrorDetails {
    ///         message: Some("private detail".into()),
    ///         ..Default::default()
    ///     },
    /// )?;
    /// use providers_kit::error::{KitError, Operation, ErrorPhase};
    /// let error = KitError::from_remote(failure, Operation::FileInfo, ErrorPhase::ReadBody);
    /// assert_eq!(error.kind(), ErrorKind::NotFound);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn from_remote(failure: RemoteFailure, operation: Operation, phase: ErrorPhase) -> Self {
        Self::new(failure.kind(), operation, phase).with_remote_failure(failure)
    }

    pub(crate) fn with_remote_failure(mut self, failure: RemoteFailure) -> Self {
        self.data.failure = ErrorFailure::Remote(failure);
        self.data.bound();
        self
    }

    /// Returns the basis for classification; caller construction remains explicit.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// let _basis = error.classification_basis();
    /// ```
    ///
    pub fn classification_basis(&self) -> ClassificationBasis {
        match &self.data.failure {
            ErrorFailure::Classified { .. } => ClassificationBasis::Explicit,
            ErrorFailure::Remote(failure) => failure.classification_basis(),
        }
    }

    /// Returns an attached remote failure, if one was represented explicitly.
    ///
    /// Absence does not establish that the implementation made no service request.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// assert!(error.remote_failure().is_none());
    /// ```
    ///
    pub fn remote_failure(&self) -> Option<&RemoteFailure> {
        match &self.data.failure {
            ErrorFailure::Classified { .. } => None,
            ErrorFailure::Remote(failure) => Some(failure),
        }
    }

    /// Explicitly exposes potentially sensitive observations for a response violation.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// assert!(error.response_details().is_none());
    /// ```
    ///
    pub fn response_details(&self) -> Option<&ResponseDetails> {
        self.data.response.as_ref()
    }

    /// Returns the safe, detector-assigned local input invariant, when applicable.
    ///
    /// ```
    /// use providers_kit::{generation::GenerationConfig, error::ValidationViolation};
    /// let error = GenerationConfig::default().with_temperature(-1.0).unwrap_err();
    /// assert!(matches!(error.validation_violation(), Some(ValidationViolation::OutOfRange {
    ///     field: "generation.temperature", ..
    /// })));
    /// ```
    pub fn validation_violation(&self) -> Option<&ValidationViolation> {
        self.data.validation.as_ref()
    }

    /// Attaches trusted field/rule labels without exposing rejected caller values.
    ///
    /// ```
    /// use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation, ValidationViolation}};
    /// let error = KitError::new(ErrorKind::InvalidInput, Operation::ValidateInput, ErrorPhase::InputValidation)
    ///     .with_validation_violation(ValidationViolation::Empty { field: "messages" });
    /// assert_eq!(error.validation_violation(), Some(&ValidationViolation::Empty { field: "messages" }));
    /// ```
    pub fn with_validation_violation(mut self, violation: ValidationViolation) -> Self {
        self.data.validation = Some(violation);
        self
    }

    /// Reports whether this tree lost diagnostics to string, record, depth, or node bounds.
    ///
    /// A tree retains at most eight levels, 32 nodes, and 1 MiB of diagnostic strings.
    /// The primary error is retained before its cause subtree and supplementary errors.
    ///
    /// ```
    /// use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation}};
    /// let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody)
    ///     .with_native_details(NativeErrorDetails {
    ///         message: Some("x".repeat(65_537)), ..Default::default()
    ///     });
    /// assert!(error.diagnostics_truncated());
    /// ```
    pub fn diagnostics_truncated(&self) -> bool {
        self.data.truncated
    }

    /// Returns the cause of this failure, separately from cleanup or diagnostic failures.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// assert!(error.cause().is_none());
    /// ```
    ///
    pub fn cause(&self) -> Option<&Self> {
        self.data.cause.as_deref()
    }

    /// Explicitly exposes the local requested target; this is not native attribution.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// assert!(error.request_target().is_none());
    /// ```
    ///
    pub fn request_target(&self) -> Option<&str> {
        self.data.request_target.as_deref()
    }

    /// Explicitly exposes caller correlation, such as a batch custom ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// assert!(error.correlation_id().is_none());
    /// ```
    ///
    pub fn correlation_id(&self) -> Option<&str> {
        self.data.correlation_id.as_deref()
    }

    /// Retains response observations without placing native values in trusted labels.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// let details = providers_kit::error::ResponseDetails::Json {
    ///     category: providers_kit::error::JsonErrorCategory::Syntax,
    ///     line: 1,
    ///     column: 2,
    /// };
    /// let error = error.with_response_details(details);
    /// assert!(error.response_details().is_some());
    /// ```
    ///
    pub fn with_response_details(mut self, details: ResponseDetails) -> Self {
        self.data.response = Some(details);
        self.data.bound();
        self
    }

    /// Retains an actual causal failure. Independent cleanup belongs in supplementary errors.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// let cause = KitError::new(ErrorKind::Timeout, Operation::Generate, ErrorPhase::ReadBody);
    /// let error = error.with_cause(cause);
    /// assert_eq!(error.cause().unwrap().kind(), ErrorKind::Timeout);
    /// ```
    ///
    pub fn with_cause(mut self, cause: Self) -> Self {
        self.data.cause = Some(Box::new(cause));
        self.data.bound();
        self
    }

    /// Retains a bounded local target without asserting that the provider identified it.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// let error = error.with_request_target("private-resource");
    /// assert_eq!(error.request_target(), Some("private-resource"));
    /// assert!(!format!("{error}").contains("private-resource"));
    /// ```
    ///
    pub fn with_request_target(mut self, target: impl Into<String>) -> Self {
        self.data.request_target = Some(target.into());
        self.data.bound();
        self
    }

    /// Retains bounded caller correlation without treating it as a missing-resource identity.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::error::{KitError, ErrorKind, ErrorPhase, Operation};
    /// let error = KitError::new(ErrorKind::Closed, Operation::Generate, ErrorPhase::Lifecycle);
    /// let error = error.with_correlation_id("row-1");
    /// assert_eq!(error.correlation_id(), Some("row-1"));
    /// ```
    ///
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.data.correlation_id = Some(id.into());
        self.data.bound();
        self
    }

    /// Returns the portable classification without interpreting native prose.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// assert_eq!(error.kind(), ErrorKind::Transport);
    /// ```
    pub fn kind(&self) -> ErrorKind {
        self.data.failure.kind()
    }
    /// Returns safe operation, phase and batch row context.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// assert_eq!(error.context().operation, Operation::Generate);
    /// ```
    pub fn context(&self) -> &ErrorContext {
        &self.context
    }
    /// Returns the actual unsuccessful HTTP status, if one was received.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// assert_eq!(error.http_status(), None);
    /// ```
    pub fn http_status(&self) -> Option<u16> {
        self.data.status
    }
    /// Returns the local deadline that expired, if available.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// assert_eq!(error.timeout(), None);
    /// ```
    pub fn timeout(&self) -> Option<Duration> {
        self.data.timeout
    }
    /// Explicitly exposes potentially sensitive native details; do not log them by default.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// assert!(error.native_details().message.is_none());
    /// ```
    pub fn native_details(&self) -> &NativeErrorDetails {
        self.data.failure.details()
    }
    /// Returns failures collecting diagnostics or releasing resources after the primary failure.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// assert!(error.supplementary().is_empty());
    /// ```
    pub fn supplementary(&self) -> &[KitError] {
        &self.data.supplementary
    }

    /// Attaches an observed HTTP status, never a synthetic status inferred from an event code.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// let error = error.with_http_status(503);
    /// assert_eq!(error.http_status(), Some(503));
    /// ```
    pub fn with_http_status(mut self, status: u16) -> Self {
        self.data.status = Some(status);
        self
    }
    /// Attaches a local deadline; service timeout responses retain their native classification.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// let error = error.with_timeout(std::time::Duration::from_secs(10));
    /// assert_eq!(error.timeout(), Some(std::time::Duration::from_secs(10)));
    /// ```
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.data.timeout = Some(timeout);
        self
    }
    /// Attaches the zero-based logical batch row, excluding blank JSONL lines.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// let error = error.with_row(2);
    /// assert_eq!(error.context().row, Some(2));
    /// ```
    pub fn with_row(mut self, row: usize) -> Self {
        self.context.row = Some(row);
        self
    }
    /// Attaches explicit native details, bounding each string to 64 KiB on UTF-8 boundaries.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// let details = providers_kit::error::NativeErrorDetails {
    ///     request_id: Some("sensitive-correlation-id".into()),
    ///     ..Default::default()
    /// };
    /// let error = error.with_native_details(details);
    /// assert!(!error.to_string().contains("sensitive-correlation-id"));
    /// ```
    pub fn with_native_details(mut self, mut details: NativeErrorDetails) -> Self {
        details.bound();
        // Replacing classification evidence is explicit caller construction; it
        // must not preserve a stale adapter-derived basis over unrelated details.
        match &mut self.data.failure {
            ErrorFailure::Classified {
                details: current, ..
            } => **current = details,
            ErrorFailure::Remote(failure) => {
                *failure.details = details;
                failure.basis = ClassificationBasis::Explicit;
            }
        }
        self.data.bound();
        self
    }
    /// Preserves a secondary failure without replacing this failure's classification or context.
    ///
    /// ```
    ///
    /// # use providers_kit::{KitError, error::{ErrorKind, ErrorPhase, Operation}};
    /// # let error = KitError::new(ErrorKind::Transport, Operation::Generate, ErrorPhase::ReadBody);
    /// let secondary = KitError::new(ErrorKind::Timeout, Operation::Generate, ErrorPhase::ReadBody);
    /// let error = error.with_supplementary(secondary);
    /// assert_eq!(error.kind(), ErrorKind::Transport);
    /// assert_eq!(error.supplementary()[0].kind(), ErrorKind::Timeout);
    /// ```
    pub fn with_supplementary(mut self, error: Self) -> Self {
        self.data.supplementary.push(error);
        self.data.bound();
        self
    }

    pub(crate) fn at_operation(mut self, operation: Operation) -> Self {
        self.context.operation = operation;
        if let Some(cause) = self.data.cause.take() {
            self.data.cause = Some(Box::new(
                if cause.context.operation == Operation::Unspecified {
                    cause.at_operation(operation)
                } else {
                    *cause
                },
            ));
        }
        self.data.supplementary = self
            .data
            .supplementary
            .into_iter()
            .map(|error| {
                if error.context.operation == Operation::Unspecified {
                    error.at_operation(operation)
                } else {
                    error
                }
            })
            .collect();
        self
    }
    pub(crate) fn at_phase(mut self, phase: ErrorPhase) -> Self {
        self.context.phase = phase;
        self
    }
    pub(crate) fn with_message(mut self, message: impl Into<String>) -> Self {
        self.data.failure.details_mut().message = Some(message.into());
        self.data.bound();
        self
    }
    pub(crate) fn with_body(mut self, body: Option<String>) -> Self {
        self.data.failure.details_mut().body = body;
        self.data.bound();
        self
    }
    pub(crate) fn with_diagnostic(mut self, details: Option<NativeErrorDetails>) -> Self {
        if let Some(details) = details {
            self.data.failure.details_mut().body = details.body;
            self.data.failure.details_mut().truncated |= details.truncated;
        }
        self.data.bound();
        self
    }
    // Adapter-only enrichment preserves the already-validated native basis.
    pub(crate) fn with_native_capture(mut self, details: NativeErrorDetails) -> Self {
        *self.data.failure.details_mut() = details;
        self.data.bound();
        self
    }
    pub(crate) fn config(violation: ValidationViolation, message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::Configuration,
            Operation::Configure,
            ErrorPhase::Configuration,
        )
        .with_validation_violation(violation)
        .with_message(message)
    }
    pub(crate) fn invalid_input(
        violation: ValidationViolation,
        message: impl Into<String>,
    ) -> Self {
        Self::new(
            ErrorKind::InvalidInput,
            Operation::ValidateInput,
            ErrorPhase::InputValidation,
        )
        .with_validation_violation(violation)
        .with_message(message)
    }
    pub(crate) fn invalid_response(reason: ResponseViolation) -> Self {
        Self::new(
            ErrorKind::InvalidResponse(reason),
            Operation::Unspecified,
            ErrorPhase::Normalize,
        )
    }
    pub(crate) fn unsupported(violation: ValidationViolation, message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::Unsupported,
            Operation::Unspecified,
            ErrorPhase::InputValidation,
        )
        .with_validation_violation(violation)
        .with_message(message)
    }
    pub(crate) fn invalid_state(message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::InvalidState,
            Operation::Unspecified,
            ErrorPhase::Lifecycle,
        )
        .with_message(message)
    }
    pub(crate) fn closed(message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::Closed,
            Operation::Unspecified,
            ErrorPhase::Lifecycle,
        )
        .with_message(message)
    }
}

impl fmt::Display for KitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:?} during {:?}/{:?}",
            self.kind(),
            self.context.operation,
            self.context.phase
        )?;
        if let Some(violation) = self.validation_violation() {
            write!(formatter, " ({violation:?})")?;
        }
        if self.diagnostics_truncated() {
            formatter.write_str(" (diagnostics truncated)")?;
        }
        if let Some(status) = self.http_status() {
            write!(formatter, " (HTTP {status})")?;
        }
        if let Some(row) = self.context.row {
            write!(formatter, " (row {row})")?;
        }
        if let Some(timeout) = self.timeout() {
            write!(formatter, " (deadline {timeout:?})")?;
        }
        if !self.supplementary().is_empty() {
            write!(
                formatter,
                " ({} supplementary failures)",
                self.supplementary().len()
            )?;
        }
        Ok(())
    }
}

impl fmt::Debug for KitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KitError")
            .field("kind", &self.kind())
            .field("basis", &self.classification_basis())
            .field("context", &self.context)
            .field("validation", &self.validation_violation())
            .field("diagnostics_truncated", &self.diagnostics_truncated())
            .field("http_status", &self.http_status())
            .field("timeout", &self.timeout())
            .field("native", &self.native_details())
            .field("response", &self.response_details())
            .field("cause", &self.cause())
            .field("supplementary", &self.supplementary())
            .finish()
    }
}

impl std::error::Error for KitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(cause) = self.cause() {
            return Some(cause);
        }
        self.remote_failure()
            .map(|failure| failure as &(dyn std::error::Error + 'static))
    }
}
