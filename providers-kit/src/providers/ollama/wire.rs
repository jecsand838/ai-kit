use crate::KitError;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, RemoteFailure, ResponseDetails, ResponseViolation,
};
use crate::transport::CheckedRequestSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct OllamaErrorEnvelope {
    pub(crate) error: Option<String>,
}

impl OllamaErrorEnvelope {
    pub(crate) fn parse(body: &str) -> Result<Option<RemoteFailure>, KitError> {
        let value: Value =
            serde_json::from_str(body).map_err(|error| Self::decode_error(&error, body))?;
        if value.get("error").is_none() {
            return Ok(None);
        }
        let envelope: Self =
            serde_json::from_str(body).map_err(|error| Self::decode_error(&error, body))?;
        let Some(message) = envelope.error else {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "error",
            })
            .at_phase(ErrorPhase::Decode)
            .with_response_details(crate::error::ResponseDetails::FieldType {
                expected: crate::error::JsonValueKind::String,
                actual: crate::error::JsonValueKind::of(value.get("error")),
            })
            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes()))));
        };
        let mut details = CheckedRequestSpec::diagnostic_body(body.as_bytes());
        details.message = Some(message);
        RemoteFailure::from_native(
            ErrorKind::ProviderRejected,
            ClassificationBasis::Unclassified,
            details,
        )
        .map(Some)
    }

    fn decode_error(error: &serde_json::Error, body: &str) -> KitError {
        KitError::invalid_response(ResponseViolation::from_json_error(error, "error"))
            .at_phase(ErrorPhase::Decode)
            .with_response_details(ResponseDetails::json(error))
            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct OllamaFunctionCall {
    pub(crate) name: Option<String>,
    pub(crate) arguments: Option<Value>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct OllamaToolCall {
    pub(crate) id: Option<String>,
    pub(crate) function: Option<OllamaFunctionCall>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct OllamaMessage {
    pub(crate) role: Option<String>,
    pub(crate) content: String,
    #[serde(default)]
    pub(crate) thinking: Option<String>,
    #[serde(default)]
    pub(crate) tool_calls: Vec<OllamaToolCall>,
    #[serde(default)]
    pub(crate) images: Vec<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct OllamaChatResponse {
    pub(crate) model: String,
    #[serde(default)]
    pub(crate) created_at: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<OllamaMessage>,
    #[serde(default)]
    pub(crate) done: bool,
    #[serde(default)]
    pub(crate) done_reason: Option<String>,
    #[serde(default)]
    pub(crate) total_duration: Option<u64>,
    #[serde(default)]
    pub(crate) load_duration: Option<u64>,
    #[serde(default)]
    pub(crate) prompt_eval_count: Option<u64>,
    #[serde(default)]
    pub(crate) prompt_eval_cached_count: Option<u64>,
    #[serde(default)]
    pub(crate) prompt_eval_duration: Option<u64>,
    #[serde(default)]
    pub(crate) eval_count: Option<u64>,
    #[serde(default)]
    pub(crate) eval_duration: Option<u64>,
    #[serde(default)]
    pub(crate) context: Option<Vec<i64>>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct OllamaEmbedResponse {
    pub(crate) model: String,
    pub(crate) embeddings: Vec<Vec<f32>>,
    #[serde(default)]
    pub(crate) total_duration: Option<u64>,
    #[serde(default)]
    pub(crate) load_duration: Option<u64>,
    #[serde(default)]
    pub(crate) prompt_eval_count: Option<u64>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct OllamaModelDetails {
    pub(crate) parent_model: Option<String>,
    pub(crate) format: Option<String>,
    pub(crate) family: Option<String>,
    pub(crate) families: Vec<String>,
    pub(crate) parameter_size: Option<String>,
    pub(crate) quantization_level: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct OllamaModelInfo {
    pub(crate) name: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) size: Option<u64>,
    pub(crate) digest: Option<String>,
    pub(crate) modified_at: Option<String>,
    pub(crate) expires_at: Option<String>,
    pub(crate) size_vram: Option<u64>,
    pub(crate) details: Option<OllamaModelDetails>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OllamaTagsResponse {
    pub(crate) models: Vec<OllamaModelInfo>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct OllamaShowResponse {
    pub(crate) parameters: Option<String>,
    pub(crate) license: Option<String>,
    pub(crate) modified_at: Option<String>,
    pub(crate) details: Option<OllamaModelDetails>,
    pub(crate) template: Option<String>,
    pub(crate) model_info: Option<Map<String, Value>>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OllamaVersionResponse {
    pub(crate) version: String,
}
