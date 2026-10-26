use serde_json::{Value, json};

use crate::SdkError;
use crate::embedding::{EmbedRequest, EmbedResponse};
use crate::error::{ErrorPhase, ResponseDetails, ResponseViolation};
use crate::generation::Usage;
use crate::transport::CheckedRequestSpec;
use crate::transport::HttpResponse;

use super::api::OLLAMA_REQUESTS;
use super::client::OllamaClient;
use super::models::OllamaModel;
use super::wire::OllamaEmbedResponse;

impl OllamaClient {
    pub(super) async fn create_raw_embed(
        &self,
        body: Value,
    ) -> Result<HttpResponse<Value>, SdkError> {
        OLLAMA_REQUESTS
            .json::<Value>(
                "embed",
                self.api()
                    .request(reqwest::Method::POST, "embed")?
                    .json(&body),
                self.api().transport().execution(),
            )
            .await
    }
}

impl OllamaModel {
    pub(crate) fn build_embed_body(model: &str, request: &EmbedRequest) -> Result<Value, SdkError> {
        let input = if request.inputs().len() == 1 {
            Value::String(request.inputs()[0].clone())
        } else {
            Value::Array(
                request
                    .inputs()
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            )
        };

        let mut root = serde_json::Map::from_iter([
            ("model".to_string(), json!(model)),
            ("input".to_string(), input),
        ]);
        if let Some(dimensions) = request.dimensions() {
            root.insert("dimensions".to_owned(), json!(dimensions.get()));
        }
        Ok(Value::Object(root))
    }

    pub(crate) fn normalize_embed_response(
        raw: &Value,
        request: &EmbedRequest,
    ) -> Result<EmbedResponse, SdkError> {
        let decoded =
            serde_json::from_value::<OllamaEmbedResponse>(raw.clone()).map_err(|err| {
                SdkError::invalid_response(ResponseViolation::InvalidField { field: "response" })
                    .with_response_details(ResponseDetails::json(&err))
                    .at_phase(ErrorPhase::Decode)
                    .with_message(format!("failed to decode Ollama embed response: {err}"))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
            })?;
        let input_count = request.inputs().len();
        if decoded.embeddings.len() != input_count {
            return Err(
                SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "embedding_count",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "Ollama embed response returned {} vectors for {input_count} inputs",
                    decoded.embeddings.len()
                ))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
            );
        }
        if let Some((index, _)) = decoded
            .embeddings
            .iter()
            .enumerate()
            .find(|(_, embedding)| embedding.is_empty())
        {
            return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "embeddings",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message(format!(
                "Ollama embed response included an empty vector at index {index}"
            ))
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)));
        }
        if request.dimensions().is_some_and(|dimensions| {
            decoded
                .embeddings
                .iter()
                .any(|vector| vector.len() != dimensions.get() as usize)
        }) {
            return Err(
                SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "embedding_dimensions",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message("Ollama embedding dimensions did not match the request")
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
            );
        }
        Ok(EmbedResponse {
            vectors: decoded.embeddings,
            usage: Usage::reported_tokens(decoded.prompt_eval_count, None),
        })
    }
}
