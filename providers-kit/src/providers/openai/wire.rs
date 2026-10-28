use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::KitError;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeErrorDetails, Operation, RemoteFailure,
    ResponseDetails, ResponseViolation,
};
use crate::transport::CheckedRequestSpec;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiErrorEnvelope {
    pub(crate) error: OpenAiErrorObject,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiErrorObject {
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) code: Option<String>,
    #[serde(default)]
    pub(crate) param: Option<String>,
    #[serde(default, rename = "type")]
    pub(crate) kind: Option<String>,
    #[serde(default)]
    pub(crate) event_id: Option<String>,
}

impl OpenAiErrorEnvelope {
    pub(crate) fn parse(body: &str) -> Result<Option<RemoteFailure>, KitError> {
        let value: Value =
            serde_json::from_str(body).map_err(|error| Self::decode_error(&error, body))?;
        if value.get("error").is_none() {
            return Ok(None);
        }
        let envelope: Self =
            serde_json::from_str(body).map_err(|error| Self::decode_error(&error, body))?;
        envelope
            .error
            .remote_failure(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
            .map(Some)
    }

    fn decode_error(error: &serde_json::Error, body: &str) -> KitError {
        KitError::invalid_response(ResponseViolation::from_json_error(error, "error"))
            .at_phase(ErrorPhase::Decode)
            .with_response_details(ResponseDetails::json(error))
            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
    }
}

impl OpenAiErrorObject {
    fn classify(details: &NativeErrorDetails) -> (ErrorKind, ClassificationBasis) {
        let code = match details.code.as_deref() {
            Some("invalid_api_key") => ErrorKind::Authentication,
            Some("model_not_found" | "previous_response_not_found") => ErrorKind::NotFound,
            Some(
                "credit_balance_exhausted"
                | "organization_spend_limit_exceeded"
                | "project_spend_limit_exceeded",
            ) => ErrorKind::Billing,
            Some("organization_usage_limit_exceeded" | "insufficient_quota") => {
                ErrorKind::QuotaExceeded
            }
            Some("rate_limit_exceeded" | "slow_down") => ErrorKind::RateLimited,
            Some("server_is_overloaded") => ErrorKind::ServiceUnavailable,
            Some("server_error") => ErrorKind::Internal,
            _ => ErrorKind::ProviderRejected,
        };
        if code != ErrorKind::ProviderRejected {
            return (code, ClassificationBasis::NativeCode);
        }
        let kind = match details.error_type.as_deref() {
            Some("invalid_request_error") => ErrorKind::InvalidRequest,
            Some("authentication_error") => ErrorKind::Authentication,
            Some("permission_error") => ErrorKind::PermissionDenied,
            Some("not_found_error") => ErrorKind::NotFound,
            Some("conflict_error") => ErrorKind::Conflict,
            Some("rate_limit_error") => ErrorKind::RateLimited,
            Some("insufficient_quota") => ErrorKind::QuotaExceeded,
            Some("server_error" | "api_error") => ErrorKind::Internal,
            Some("service_unavailable_error") => ErrorKind::ServiceUnavailable,
            _ => ErrorKind::ProviderRejected,
        };
        (
            kind,
            if kind == ErrorKind::ProviderRejected {
                ClassificationBasis::Unclassified
            } else {
                ClassificationBasis::NativeErrorType
            },
        )
    }

    pub(crate) fn remote_failure(
        &self,
        body: Option<NativeErrorDetails>,
    ) -> Result<RemoteFailure, KitError> {
        let mut details = body.unwrap_or_default();
        details.message = Some(self.message.clone());
        details.code = self.code.clone();
        details.error_type = self.kind.clone();
        details.parameter = self.param.clone();
        details.related_event_id = self.event_id.clone();
        Self::remote_details(details)
    }

    // Batch-level errors use optional native fields, unlike unary error envelopes.
    pub(crate) fn remote_details(details: NativeErrorDetails) -> Result<RemoteFailure, KitError> {
        let reported_status = details.reported_http_status;
        let (kind, basis) = Self::classify(&details);
        let failure = RemoteFailure::from_native(kind, basis, details)?;
        match reported_status {
            Some(status) => CheckedRequestSpec::classify_http_failure(failure, status),
            None => Ok(failure),
        }
    }

    pub(crate) fn into_sdk_error(
        self,
        phase: ErrorPhase,
        body: Option<NativeErrorDetails>,
    ) -> KitError {
        match self.remote_failure(body) {
            Ok(failure) => KitError::from_remote(failure, Operation::Unspecified, phase),
            Err(error) => error.at_phase(phase),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub(crate) struct OpenAiList<T> {
    pub(crate) object: Option<String>,
    pub(crate) data: Vec<T>,
    pub(crate) first_id: Option<String>,
    pub(crate) last_id: Option<String>,
    pub(crate) has_more: Option<bool>,
}

impl<T> OpenAiList<T> {
    pub(crate) fn required_next_cursor(&self, operation: &str) -> Result<Option<String>, KitError> {
        match self.has_more {
            Some(true) => self.last_id.clone().map(Some).ok_or_else(|| {
                KitError::invalid_response(ResponseViolation::MissingField { field: "last_id" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "{operation} response set `has_more` without returning `last_id`"
                    ))
                    .with_diagnostic(None)
            }),
            Some(false) => Ok(None),
            None => Err(KitError::invalid_response(ResponseViolation::MissingField {
                field: "has_more",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message(format!(
                "{operation} response did not include required `has_more`"
            ))
            .with_diagnostic(None)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct OpenAiTokenDetails {
    pub(crate) cached_tokens: Option<u64>,
    pub(crate) reasoning_tokens: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct OpenAiUsage {
    pub(crate) input_tokens: Option<u64>,
    pub(crate) output_tokens: Option<u64>,
    pub(crate) total_tokens: Option<u64>,
    pub(crate) input_tokens_details: Option<OpenAiTokenDetails>,
    pub(crate) output_tokens_details: Option<OpenAiTokenDetails>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiResponseObject {
    pub(crate) model: String,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) output: Vec<OpenAiOutputItem>,
    #[serde(default)]
    pub(crate) usage: Option<OpenAiUsage>,
    #[serde(default)]
    pub(crate) output_text: Option<String>,
    #[serde(default)]
    pub(crate) incomplete_details: Option<Value>,
    #[serde(default)]
    pub(crate) error: Option<OpenAiErrorObject>,
    #[serde(default)]
    pub(crate) metadata: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiOutputItem {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) role: Option<String>,
    pub(crate) content: Option<Vec<OpenAiContentItem>>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) call_id: Option<String>,
    #[serde(default)]
    pub(crate) arguments: Option<String>,
    #[serde(default)]
    pub(crate) code: Option<String>,
    #[serde(default)]
    pub(crate) input: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) result: Option<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiContentItem {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) text: Option<String>,
    #[serde(default)]
    pub(crate) refusal: Option<String>,
    #[serde(default)]
    pub(crate) annotations: Vec<OpenAiAnnotation>,
    #[serde(default)]
    pub(crate) image_url: Option<String>,
    #[serde(default)]
    pub(crate) file_id: Option<String>,
    #[serde(default)]
    pub(crate) file_url: Option<String>,
    #[serde(default)]
    pub(crate) data: Option<String>,
    #[serde(default)]
    pub(crate) media_type: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiAnnotation {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) url: Option<String>,
    #[serde(default)]
    pub(crate) text: Option<String>,
    #[serde(default)]
    pub(crate) start_index: Option<usize>,
    #[serde(default)]
    pub(crate) end_index: Option<usize>,
    #[serde(default)]
    pub(crate) file_id: Option<String>,
    #[serde(default)]
    pub(crate) filename: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiEmbeddingObject {
    #[serde(default)]
    pub(crate) object: Option<String>,
    pub(crate) index: usize,
    pub(crate) embedding: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiEmbeddingsResponse {
    #[serde(default)]
    pub(crate) object: Option<String>,
    pub(crate) model: String,
    pub(crate) data: Vec<OpenAiEmbeddingObject>,
    #[serde(default)]
    pub(crate) usage: Option<OpenAiUsage>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiFileObject {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) object: Option<String>,
    #[serde(default)]
    pub(crate) bytes: Option<u64>,
    #[serde(default)]
    pub(crate) filename: Option<String>,
    #[serde(default)]
    pub(crate) purpose: Option<String>,
    #[serde(default)]
    pub(crate) created_at: Option<i64>,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) expires_at: Option<i64>,
    #[serde(default)]
    pub(crate) mime_type: Option<String>,
    #[serde(default)]
    pub(crate) metadata: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiDeleteObject {
    pub(crate) id: String,
    pub(crate) deleted: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiBatchObject {
    pub(crate) id: String,
    pub(crate) status: String,
    #[serde(default)]
    pub(crate) created_at: Option<i64>,
    #[serde(default)]
    pub(crate) completed_at: Option<i64>,
    #[serde(default)]
    pub(crate) output_file_id: Option<String>,
    #[serde(default)]
    pub(crate) error_file_id: Option<String>,
    #[serde(default)]
    pub(crate) input_file_id: Option<String>,
    #[serde(default)]
    pub(crate) endpoint: Option<String>,
    #[serde(default)]
    pub(crate) errors: Option<Value>,
    #[serde(default)]
    pub(crate) metadata: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiModelObject {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) created: Option<i64>,
    #[serde(default)]
    pub(crate) owned_by: Option<String>,
    #[serde(default)]
    pub(crate) metadata: Option<Map<String, Value>>,
}
