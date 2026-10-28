use crate::KitError;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeDetail, NativeErrorDetails, Operation,
    RemoteFailure, ResponseDetails, ResponseViolation,
};
use crate::generation::FinishReason;
use crate::transport::CheckedRequestSpec;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct GeminiErrorEnvelope {
    pub(crate) error: GeminiErrorObject,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct GeminiErrorObject {
    pub(crate) code: i64,
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) details: Vec<Value>,
}

impl GeminiErrorEnvelope {
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

impl GeminiErrorObject {
    pub(crate) fn remote_failure(
        &self,
        body: Option<NativeErrorDetails>,
    ) -> Result<RemoteFailure, KitError> {
        let mut details = body.unwrap_or_default();
        details.native_status = self.status.clone();
        details.reported_http_status = u16::try_from(self.code).ok();
        details.message = Some(self.message.clone());
        Self::capture_details(&self.details, &mut details);
        Self::classify_remote(details)
    }

    fn classify_remote(mut details: NativeErrorDetails) -> Result<RemoteFailure, KitError> {
        if details.rpc_code == Some(0)
            || details.native_status.as_deref() == Some("OK")
            || details
                .reported_http_status
                .is_some_and(|status| (200..300).contains(&status))
        {
            let actual = if details.native_status.as_deref() == Some("OK") {
                "OK".to_owned()
            } else if details.rpc_code == Some(0) {
                "0".to_owned()
            } else {
                details
                    .reported_http_status
                    .map(|status| status.to_string())
                    .unwrap_or_default()
            };
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "native error envelope reports success",
                })
                .at_phase(ErrorPhase::Decode)
                .with_response_details(ResponseDetails::Discriminator { actual })
                .with_native_capture(details),
            );
        }
        // ErrorInfo reasons have meaning only within their reported domain.
        let reason = details
            .details
            .iter()
            .enumerate()
            .find_map(|(index, detail)| {
                let NativeDetail::Reason { domain, reason } = detail else {
                    return None;
                };
                if domain != "googleapis.com" {
                    return None;
                }
                let kind = match reason.as_str() {
                    "API_KEY_INVALID" | "ACCESS_TOKEN_EXPIRED" | "CREDENTIALS_MISSING" => {
                        ErrorKind::Authentication
                    }
                    "API_KEY_SERVICE_BLOCKED"
                    | "API_KEY_HTTP_REFERRER_BLOCKED"
                    | "API_KEY_IP_ADDRESS_BLOCKED"
                    | "API_KEY_ANDROID_APP_BLOCKED"
                    | "API_KEY_IOS_APP_BLOCKED"
                    | "ACCESS_TOKEN_SCOPE_INSUFFICIENT" => ErrorKind::PermissionDenied,
                    "RATE_LIMIT_EXCEEDED" => ErrorKind::RateLimited,
                    "RESOURCE_QUOTA_EXCEEDED" => ErrorKind::QuotaExceeded,
                    "BILLING_DISABLED" => ErrorKind::Billing,
                    _ => return None,
                };
                Some((index, kind))
            });
        if let Some((index, kind)) = reason {
            // Keep the discriminator used to classify ahead of optional detail truncation.
            let selected = details.details.remove(index);
            details.details.insert(0, selected);
            return RemoteFailure::from_native(kind, ClassificationBasis::NativeReason, details);
        }
        let status = ErrorKind::from_canonical_status(details.native_status.as_deref());
        if status != ErrorKind::ProviderRejected {
            return RemoteFailure::from_native(
                status,
                ClassificationBasis::CanonicalStatus,
                details,
            );
        }
        let rpc = ErrorKind::from_rpc_code(details.rpc_code);
        if rpc != ErrorKind::ProviderRejected {
            return RemoteFailure::from_native(rpc, ClassificationBasis::RpcCode, details);
        }
        let reported_status = details.reported_http_status;
        let failure = RemoteFailure::from_native(
            ErrorKind::ProviderRejected,
            ClassificationBasis::Unclassified,
            details,
        )?;
        match reported_status {
            Some(status) if (400..=599).contains(&status) => {
                CheckedRequestSpec::classify_http_failure(failure, status)
            }
            _ => Ok(failure),
        }
    }

    fn capture_details(values: &[Value], details: &mut NativeErrorDetails) {
        for value in values {
            match value.get("@type").and_then(Value::as_str) {
                Some("type.googleapis.com/google.rpc.ErrorInfo") => {
                    if let (Some(domain), Some(reason)) = (
                        value.get("domain").and_then(Value::as_str),
                        value.get("reason").and_then(Value::as_str),
                    ) {
                        details.details.push(NativeDetail::Reason {
                            domain: domain.into(),
                            reason: reason.into(),
                        });
                    }
                }
                Some("type.googleapis.com/google.rpc.ResourceInfo") => {
                    if let Some(name) = value.get("resourceName").and_then(Value::as_str) {
                        details.details.push(NativeDetail::Resource {
                            name: name.into(),
                            resource_type: value
                                .get("resourceType")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            owner: value
                                .get("owner")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            description: value
                                .get("description")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        });
                    }
                }
                Some("type.googleapis.com/google.rpc.BadRequest") => {
                    if let Some(violations) = value.get("fieldViolations").and_then(Value::as_array)
                    {
                        for violation in violations {
                            if let Some(field) = violation.get("field").and_then(Value::as_str) {
                                details.details.push(NativeDetail::FieldViolation {
                                    field: field.into(),
                                    reason: violation
                                        .get("reason")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                    description: violation
                                        .get("description")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                });
                            }
                        }
                    }
                }
                Some("type.googleapis.com/google.rpc.RetryInfo") => {
                    if let Some(delay) = value.get("retryDelay").and_then(Value::as_str)
                        && let Some((seconds, nanos)) = Self::retry_delay(delay)
                    {
                        details
                            .details
                            .push(NativeDetail::RetryDelay { seconds, nanos });
                    }
                }
                Some("type.googleapis.com/google.rpc.QuotaFailure") => {
                    if let Some(violations) = value.get("violations").and_then(Value::as_array) {
                        for violation in violations {
                            details.details.push(NativeDetail::QuotaViolation {
                                subject: violation
                                    .get("subject")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                description: violation
                                    .get("description")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                            });
                        }
                    }
                }
                Some("type.googleapis.com/google.rpc.PreconditionFailure") => {
                    if let Some(violations) = value.get("violations").and_then(Value::as_array) {
                        for violation in violations {
                            details.details.push(NativeDetail::PreconditionViolation {
                                kind: violation
                                    .get("type")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                subject: violation
                                    .get("subject")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                description: violation
                                    .get("description")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                            });
                        }
                    }
                }
                _ => {} // Unknown or malformed optional detail remains in the bounded original body.
            }
        }
    }

    fn retry_delay(value: &str) -> Option<(u64, u32)> {
        let value = value.strip_suffix('s')?;
        let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
        if seconds.is_empty()
            || !seconds.bytes().all(|byte| byte.is_ascii_digit())
            || fraction.len() > 9
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        let seconds = seconds.parse().ok()?;
        let nanos = if fraction.is_empty() {
            0
        } else {
            fraction.parse::<u32>().ok()? * 10_u32.pow(9 - fraction.len() as u32)
        };
        Some((seconds, nanos))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiInlineData {
    pub(crate) mime_type: Option<String>,
    pub(crate) data: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiFileData {
    pub(crate) mime_type: Option<String>,
    pub(crate) file_uri: Option<String>,
    pub(crate) display_name: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct GeminiFunctionCall {
    pub(crate) id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) args: Option<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiFunctionResponse {
    pub(crate) id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) response: Option<Value>,
    pub(crate) will_continue: Option<bool>,
    pub(crate) parts: Vec<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiToolCall {
    pub(crate) tool_type: Option<String>,
    pub(crate) args: Option<Value>,
    pub(crate) id: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiToolResponse {
    pub(crate) tool_type: Option<String>,
    pub(crate) response: Option<Value>,
    pub(crate) id: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiPart {
    pub(crate) text: Option<String>,
    pub(crate) inline_data: Option<GeminiInlineData>,
    pub(crate) file_data: Option<GeminiFileData>,
    pub(crate) function_call: Option<GeminiFunctionCall>,
    pub(crate) function_response: Option<GeminiFunctionResponse>,
    pub(crate) tool_call: Option<GeminiToolCall>,
    pub(crate) tool_response: Option<GeminiToolResponse>,
    pub(crate) executable_code: Option<Value>,
    pub(crate) code_execution_result: Option<Value>,
    pub(crate) thought_signature: Option<String>,
    pub(crate) thought: Option<bool>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct GeminiContent {
    pub(crate) role: Option<String>,
    #[serde(default, deserialize_with = "GeminiContent::deserialize_parts")]
    pub(crate) parts: Vec<GeminiPart>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

impl GeminiContent {
    fn deserialize_parts<'de, D>(deserializer: D) -> Result<Vec<GeminiPart>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<Vec<GeminiPart>>::deserialize(deserializer).map(Option::unwrap_or_default)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiCitationSource {
    pub(crate) start_index: Option<usize>,
    pub(crate) end_index: Option<usize>,
    pub(crate) uri: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) license: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiGroundingMetadata {
    pub(crate) grounding_chunks: Vec<GeminiGroundingChunk>,
    pub(crate) grounding_supports: Vec<GeminiGroundingSupport>,
    pub(crate) web_search_queries: Vec<String>,
    pub(crate) retrieval_queries: Vec<String>,
    pub(crate) citation_metadata: Option<GeminiCitationMetadata>,
    pub(crate) search_entry_point: Option<Value>,
    pub(crate) google_maps_widget_context_token: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiGroundingChunk {
    pub(crate) web: Option<GeminiGroundingChunkWeb>,
    pub(crate) maps: Option<GeminiGroundingChunkMaps>,
    #[serde(
        default,
        deserialize_with = "optional_present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) retrieved_context: Option<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiGroundingChunkWeb {
    pub(crate) uri: Option<String>,
    pub(crate) title: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiGroundingChunkMaps {
    pub(crate) uri: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) place_id: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiGroundingSupport {
    pub(crate) grounding_chunk_indices: Vec<usize>,
    pub(crate) confidence_scores: Vec<f64>,
    pub(crate) rendered_parts: Vec<usize>,
    pub(crate) segment: Option<GeminiGroundingSegment>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

fn optional_present_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiGroundingSegment {
    pub(crate) part_index: Option<usize>,
    pub(crate) start_index: Option<usize>,
    pub(crate) end_index: Option<usize>,
    pub(crate) text: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiCitationMetadata {
    pub(crate) citation_sources: Vec<GeminiCitationSource>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiCandidate {
    pub(crate) index: Option<usize>,
    pub(crate) content: Option<GeminiContent>,
    pub(crate) finish_reason: Option<String>,
    pub(crate) finish_message: Option<String>,
    pub(crate) token_count: Option<u64>,
    pub(crate) grounding_metadata: Option<GeminiGroundingMetadata>,
    pub(crate) url_context_metadata: Option<GeminiUrlContextMetadata>,
    pub(crate) safety_ratings: Vec<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

impl GeminiCandidate {
    pub(crate) fn finish_reason(&self) -> Result<Option<FinishReason>, KitError> {
        let Some(reason) = self.finish_reason.as_deref() else {
            return Ok(None);
        };
        let reason = match reason {
            "STOP" => FinishReason::Stop,
            "MAX_TOKENS" => FinishReason::Length,
            "SAFETY"
            | "RECITATION"
            | "LANGUAGE"
            | "BLOCKLIST"
            | "PROHIBITED_CONTENT"
            | "SPII"
            | "IMAGE_SAFETY"
            | "IMAGE_PROHIBITED_CONTENT"
            | "IMAGE_RECITATION"
            | "ESCALATION" => FinishReason::ContentFilter,
            "MALFORMED_FUNCTION_CALL"
            | "UNEXPECTED_TOOL_CALL"
            | "TOO_MANY_TOOL_CALLS"
            | "MISSING_THOUGHT_SIGNATURE"
            | "MALFORMED_RESPONSE"
            | "OTHER"
            | "IMAGE_OTHER"
            | "NO_IMAGE"
            | "PUP_LIMITED_DISABLED" => FinishReason::Error,
            "CONTINUATION" => {
                return Err(KitError::new(ErrorKind::Unsupported, Operation::Unspecified, ErrorPhase::Normalize)
                    .with_message("Gemini long-decode continuation is not represented by this bounded generation operation"));
            }
            other => {
                return Err(
                    KitError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "finishReason",
                    })
                    .with_message(format!(
                        "Gemini response used unknown finish reason `{other}`"
                    )),
                );
            }
        };
        Ok(Some(reason))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiUrlContextMetadata {
    pub(crate) url_metadata: Vec<GeminiUrlMetadata>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiUrlMetadata {
    pub(crate) retrieved_url: Option<String>,
    pub(crate) url_retrieval_status: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiUsageMetadata {
    pub(crate) prompt_token_count: Option<u64>,
    pub(crate) cached_content_token_count: Option<u64>,
    pub(crate) candidates_token_count: Option<u64>,
    pub(crate) thoughts_token_count: Option<u64>,
    pub(crate) total_token_count: Option<u64>,
    pub(crate) traffic_type: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiPromptFeedback {
    pub(crate) block_reason: Option<String>,
    pub(crate) safety_ratings: Vec<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiGenerateContentResponse {
    pub(crate) candidates: Vec<GeminiCandidate>,
    pub(crate) usage_metadata: Option<GeminiUsageMetadata>,
    pub(crate) model_version: Option<String>,
    pub(crate) response_id: Option<String>,
    pub(crate) service_tier: Option<String>,
    pub(crate) prompt_feedback: Option<GeminiPromptFeedback>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct GeminiEmbedding {
    pub(crate) values: Vec<f32>,
    pub(crate) statistics: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct GeminiEmbedContentResponse {
    pub(crate) embedding: Option<GeminiEmbedding>,
    pub(crate) usage_metadata: Option<GeminiUsageMetadata>,
    pub(crate) truncated: Option<bool>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeminiBatchEmbedContentsResponse {
    pub(crate) embeddings: Vec<GeminiEmbedding>,
    pub(crate) usage_metadata: Option<GeminiUsageMetadata>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeminiFileObject {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) mime_type: Option<String>,
    #[serde(default)]
    pub(crate) size_bytes: Option<String>,
    #[serde(default)]
    pub(crate) create_time: Option<String>,
    #[serde(default)]
    pub(crate) update_time: Option<String>,
    #[serde(default)]
    pub(crate) expiration_time: Option<String>,
    #[serde(default)]
    pub(crate) uri: Option<String>,
    #[serde(default)]
    pub(crate) download_uri: Option<String>,
    #[serde(default, deserialize_with = "optional_gemini_file_state")]
    pub(crate) state: Option<String>,
    #[serde(default)]
    pub(crate) source: Option<String>,
    #[serde(default, rename = "sha256_hash")]
    pub(crate) sha256_hash: Option<String>,
    #[serde(default)]
    pub(crate) error: Option<GeminiErrorObject>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

fn optional_gemini_file_state<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(state) => Ok(Some(state)),
        Value::Null => Err(D::Error::custom(
            "Gemini File.state must be omitted or a string enum value",
        )),
        other => Err(D::Error::custom(format!(
            "Gemini File.state must be a string enum value, got {other}"
        ))),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct GeminiFileUploadEnvelope {
    pub(crate) file: GeminiFileObject,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeminiFilesList {
    #[serde(default)]
    pub(crate) files: Vec<GeminiFileObject>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeminiModelInfo {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) base_model_id: Option<String>,
    #[serde(default)]
    pub(crate) version: Option<String>,
    #[serde(default)]
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) input_token_limit: Option<u64>,
    #[serde(default)]
    pub(crate) output_token_limit: Option<u64>,
    #[serde(default)]
    pub(crate) supported_generation_methods: Vec<String>,
    #[serde(default)]
    pub(crate) thinking: Option<Value>,
    #[serde(default)]
    pub(crate) temperature: Option<f32>,
    #[serde(default)]
    pub(crate) top_p: Option<f32>,
    #[serde(default)]
    pub(crate) top_k: Option<u32>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeminiModelsList {
    pub(crate) models: Vec<GeminiModelInfo>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct GeminiRpcStatus {
    pub(crate) code: Option<i64>,
    pub(crate) message: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) details: Vec<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

impl GeminiRpcStatus {
    pub(crate) fn remote_failure(
        &self,
        body: Option<NativeErrorDetails>,
    ) -> Result<RemoteFailure, KitError> {
        let mut details = body.unwrap_or_default();
        details.rpc_code = self.code;
        details.message = self.message.clone();
        details.native_status = self.status.clone();
        GeminiErrorObject::capture_details(&self.details, &mut details);
        GeminiErrorObject::classify_remote(details)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct GeminiOperation {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) done: Option<bool>,
    #[serde(default)]
    pub(crate) error: Option<GeminiRpcStatus>,
    #[serde(default)]
    pub(crate) response: Option<Value>,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeminiOperationsList {
    pub(crate) operations: Vec<GeminiOperation>,
    pub(crate) next_page_token: Option<String>,
}
