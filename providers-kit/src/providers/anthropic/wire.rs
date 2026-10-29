use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

use super::api::AnthropicApiContext;
use crate::KitError;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeErrorDetails, RemoteFailure, ResponseDetails,
    ResponseViolation,
};
use crate::transport::CheckedRequestSpec;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicErrorEnvelope {
    #[serde(default, rename = "type")]
    pub(crate) envelope_type: Option<String>,
    pub(crate) error: AnthropicErrorObject,
    #[serde(default)]
    pub(crate) request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicErrorObject {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) message: String,
}

impl AnthropicErrorEnvelope {
    pub(crate) fn parse(body: &str) -> Result<Option<RemoteFailure>, KitError> {
        let value: Value =
            serde_json::from_str(body).map_err(|error| Self::decode_error(&error, body))?;
        if value.get("error").is_none() {
            return Ok(None);
        }
        let envelope: Self =
            serde_json::from_str(body).map_err(|error| Self::decode_error(&error, body))?;
        envelope
            .remote_failure(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
            .map(Some)
    }

    fn decode_error(error: &serde_json::Error, body: &str) -> KitError {
        KitError::invalid_response(ResponseViolation::from_json_error(error, "error"))
            .at_phase(ErrorPhase::Decode)
            .with_response_details(ResponseDetails::json(error))
            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
    }

    pub(crate) fn remote_failure(
        &self,
        body: Option<NativeErrorDetails>,
    ) -> Result<RemoteFailure, KitError> {
        let mut details = body.unwrap_or_default();
        details.error_type = Some(self.error.kind.clone());
        details.event_type = self.envelope_type.clone().or(details.event_type);
        details.message = Some(self.error.message.clone());
        details.request_id = self.request_id.clone().or(details.request_id);
        let kind = AnthropicApiContext::classify_error(&self.error.kind);
        let basis = if kind == ErrorKind::ProviderRejected {
            ClassificationBasis::Unclassified
        } else {
            ClassificationBasis::NativeErrorType
        };
        RemoteFailure::from_native(kind, basis, details)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct AnthropicUsage {
    pub(crate) input_tokens: Option<u64>,
    pub(crate) output_tokens: Option<u64>,
    pub(crate) cache_creation_input_tokens: Option<u64>,
    pub(crate) cache_read_input_tokens: Option<u64>,
    pub(crate) service_tier: Option<String>,
    pub(crate) output_tokens_details: Option<AnthropicOutputTokenDetails>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

impl AnthropicUsage {
    pub(crate) fn update_totals(&mut self, newer: Self) {
        self.input_tokens = newer.input_tokens.or(self.input_tokens);
        self.output_tokens = newer.output_tokens.or(self.output_tokens);
        self.cache_creation_input_tokens = newer
            .cache_creation_input_tokens
            .or(self.cache_creation_input_tokens);
        self.cache_read_input_tokens = newer
            .cache_read_input_tokens
            .or(self.cache_read_input_tokens);
        if let Some(newer) = newer.output_tokens_details {
            let current = self.output_tokens_details.get_or_insert_default();
            current.thinking_tokens = newer.thinking_tokens.or(current.thinking_tokens);
        }
        self.extra.extend(newer.extra);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct AnthropicOutputTokenDetails {
    pub(crate) thinking_tokens: Option<u64>,
}

pub(crate) enum AnthropicCitation {
    WebSearch {
        url: String,
        title: Option<String>,
        cited_text: String,
    },
    Character {
        document_index: Number,
        start_char_index: Number,
        end_char_index: Number,
        file_id: Option<String>,
        document_title: Option<String>,
        cited_text: String,
    },
    Page {
        document_index: Number,
        start_page_number: Number,
        end_page_number: Number,
        file_id: Option<String>,
        document_title: Option<String>,
        cited_text: String,
    },
    ContentBlock {
        document_index: Number,
        start_block_index: Number,
        end_block_index: Number,
        file_id: Option<String>,
        document_title: Option<String>,
        cited_text: String,
    },
    SearchResult {
        search_result_index: Number,
        start_block_index: Number,
        end_block_index: Number,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicContentBlock {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) text: Option<String>,
    #[serde(default)]
    pub(crate) citations: Option<Vec<Value>>,
    pub(crate) id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) input: Option<Value>,
    pub(crate) source: Option<Value>,
    pub(crate) thinking: Option<String>,
    pub(crate) signature: Option<String>,
    pub(crate) data: Option<String>,
    pub(crate) media_type: Option<String>,
    pub(crate) tool_use_id: Option<String>,
    pub(crate) is_error: Option<bool>,
    pub(crate) content: Option<Value>,
    pub(crate) title: Option<String>,
    pub(crate) context: Option<String>,
    pub(crate) caller: Option<Value>,
    pub(crate) file_id: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicContainerRef {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) expires_at: Option<String>,
    #[serde(default)]
    pub(crate) skills: Vec<AnthropicSkillRef>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicSkillRef {
    pub(crate) skill_id: String,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicMessageResponse {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) role: String,
    pub(crate) content: Vec<AnthropicContentBlock>,
    pub(crate) model: String,
    #[serde(default)]
    pub(crate) stop_reason: Option<String>,
    #[serde(default)]
    pub(crate) stop_sequence: Option<String>,
    #[serde(default)]
    pub(crate) usage: Option<AnthropicUsage>,
    #[serde(default)]
    pub(crate) container: Option<AnthropicContainerRef>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicFileObject {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) filename: Option<String>,
    #[serde(default)]
    pub(crate) mime_type: Option<String>,
    #[serde(default)]
    pub(crate) size_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) created_at: Option<String>,
    #[serde(default)]
    pub(crate) downloadable: Option<bool>,
    #[serde(default)]
    pub(crate) metadata: Option<Map<String, Value>>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicDeletedFile {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) kind: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub(crate) struct AnthropicList<T> {
    pub(crate) data: Vec<T>,
    pub(crate) first_id: Option<String>,
    pub(crate) last_id: Option<String>,
    pub(crate) has_more: Option<bool>,
    pub(crate) next_page: Option<String>,
}

impl<T> AnthropicList<T> {
    pub(crate) fn validated_page(&self, operation: &str) -> Result<Option<String>, KitError> {
        match self.next_page.as_deref() {
            Some(cursor) if cursor.trim().is_empty() => {
                Err(KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "next_page",
                })
                .with_message(format!("{operation} response returned a blank `next_page`"))
                .with_diagnostic(None))
            }
            Some(cursor) => Ok(Some(cursor.to_string())),
            None => Ok(None),
        }
    }

    pub(crate) fn validated_after_id(&self, operation: &str) -> Result<Option<String>, KitError> {
        match (self.has_more, self.last_id.as_deref()) {
            (Some(true), Some(cursor)) if !cursor.trim().is_empty() => Ok(Some(cursor.to_string())),
            (Some(true), _) => Err(KitError::invalid_response(ResponseViolation::MissingField {
                field: "last_id",
            })
            .with_message(format!(
                "{operation} response set `has_more` without returning `last_id`"
            ))
            .with_diagnostic(None)),
            (Some(false), _) => Ok(None),
            (None, _) => Err(KitError::invalid_response(ResponseViolation::MissingField {
                field: "has_more",
            })
            .with_message(format!("{operation} response did not include `has_more`"))
            .with_diagnostic(None)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicModelInfo {
    pub(crate) id: String,
    #[serde(default, rename = "type")]
    pub(crate) kind: Option<String>,
    #[serde(default)]
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) created_at: Option<String>,
    #[serde(default)]
    pub(crate) max_input_tokens: Option<u64>,
    #[serde(default)]
    pub(crate) max_tokens: Option<u64>,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct AnthropicMessageBatchRequestCounts {
    pub(crate) canceled: u64,
    pub(crate) errored: u64,
    pub(crate) expired: u64,
    pub(crate) processing: u64,
    pub(crate) succeeded: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicMessageBatch {
    pub(crate) id: String,
    #[serde(default, rename = "type")]
    pub(crate) kind: Option<String>,
    #[serde(default)]
    pub(crate) archived_at: Option<String>,
    #[serde(default)]
    pub(crate) cancel_initiated_at: Option<String>,
    #[serde(default)]
    pub(crate) created_at: Option<String>,
    #[serde(default)]
    pub(crate) ended_at: Option<String>,
    #[serde(default)]
    pub(crate) expires_at: Option<String>,
    #[serde(default)]
    pub(crate) processing_status: Option<String>,
    #[serde(default)]
    pub(crate) request_counts: Option<AnthropicMessageBatchRequestCounts>,
    #[serde(default)]
    pub(crate) results_url: Option<String>,
    #[serde(default)]
    pub(crate) usage: Option<AnthropicUsage>,
    #[serde(default)]
    pub(crate) error: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnthropicBatchResultLine {
    #[serde(default)]
    pub(crate) custom_id: Option<String>,
    pub(crate) result: Value,
}
