use serde_json::{Map, Value, json};

use crate::KitError;
use crate::error::ResponseViolation;
use crate::error::ValidationViolation;
use crate::transport::HttpResponse;

use crate::embedding::{EmbedRequest, EmbedResponse};
use crate::generation::Usage;
use crate::transport::CheckedRequestSpec;

use super::api::{GEMINI_REQUESTS, GeminiApiContext};
use super::wire::{
    GeminiBatchEmbedContentsResponse, GeminiEmbedContentResponse, GeminiUsageMetadata,
};

pub(crate) enum GeminiEmbedRequest {
    Single {
        path: String,
        body: Value,
    },
    Batch {
        path: String,
        body: Value,
        input_count: usize,
    },
}

pub(crate) enum GeminiEmbedResponse {
    Single {
        response: HttpResponse<GeminiEmbedContentResponse>,
    },
    Batch {
        response: HttpResponse<GeminiBatchEmbedContentsResponse>,
        input_count: usize,
    },
}

pub(crate) fn build_embed_request(
    model: &str,
    request: &EmbedRequest,
) -> Result<GeminiEmbedRequest, KitError> {
    let input_count = request.inputs().len();

    if input_count == 1 {
        let Some(input) = request.inputs().first() else {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "embedding.inputs",
                },
                "Gemini embeddings require at least one input",
            ));
        };
        let body = build_single_embed_body(request, input)?;
        let path = format!("v1beta/{}:embedContent", model);
        Ok(GeminiEmbedRequest::Single { path, body })
    } else {
        let body = build_batch_embed_body(request, model)?;
        let path = format!("v1beta/{}:batchEmbedContents", model);
        Ok(GeminiEmbedRequest::Batch {
            path,
            body,
            input_count,
        })
    }
}

pub(crate) async fn send_embed_request(
    api: &GeminiApiContext,
    request: GeminiEmbedRequest,
) -> Result<GeminiEmbedResponse, KitError> {
    match request {
        GeminiEmbedRequest::Single { path, body } => {
            let response = GEMINI_REQUESTS
                .json::<GeminiEmbedContentResponse>(
                    &path,
                    api.request(reqwest::Method::POST, &path)?.json(&body),
                    api.transport().execution(),
                )
                .await?;
            Ok(GeminiEmbedResponse::Single { response })
        }
        GeminiEmbedRequest::Batch {
            path,
            body,
            input_count,
        } => {
            let response = GEMINI_REQUESTS
                .json::<GeminiBatchEmbedContentsResponse>(
                    &path,
                    api.request(reqwest::Method::POST, &path)?.json(&body),
                    api.transport().execution(),
                )
                .await?;
            Ok(GeminiEmbedResponse::Batch {
                response,
                input_count,
            })
        }
    }
}

pub(crate) fn normalize_embed_response(
    response: GeminiEmbedResponse,
) -> Result<EmbedResponse, KitError> {
    match response {
        GeminiEmbedResponse::Single { response } => response.normalize(|response| {
            let usage = usage_from_embedding_metadata(response.usage_metadata.as_ref());
            let body = CheckedRequestSpec::diagnostic_json(&response);
            let vector = response.embedding.ok_or_else(|| {
                KitError::invalid_response(ResponseViolation::MissingField { field: "embedding" })
                    .with_message(
                        "Gemini embedContent response did not include embedding".to_string(),
                    )
                    .with_diagnostic(body.clone())
            })?;
            if vector.values.is_empty() {
                return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "embedding.values",
                })
                .with_message(
                    "Gemini embedContent response included an empty embedding vector".to_string(),
                )
                .with_diagnostic(body));
            }
            Ok(EmbedResponse {
                vectors: vec![vector.values],
                usage,
            })
        }),
        GeminiEmbedResponse::Batch {
            response,
            input_count,
        } => response.normalize(|response| {
            let usage = usage_from_embedding_metadata(response.usage_metadata.as_ref());
            let body = CheckedRequestSpec::diagnostic_json(&response);
            if response.embeddings.len() != input_count {
                return Err(KitError::invalid_response(ResponseViolation::ProtocolViolation { rule: "embedding vector count must match input count" }).with_message(format!(
                        "Gemini batchEmbedContents response returned {} vectors for {input_count} inputs",
                        response.embeddings.len()
                    )).with_diagnostic(body));
            }
            if let Some((index, _)) = response
                .embeddings
                .iter()
                .enumerate()
                .find(|(_, embedding)| embedding.values.is_empty())
            {
                return Err(KitError::invalid_response(ResponseViolation::InvalidField { field: "embeddings.values" }).with_message(format!(
                        "Gemini batchEmbedContents response included an empty embedding vector at index {index}"
                    )).with_diagnostic(body));
            }
            let vectors = response
                .embeddings
                .into_iter()
                .map(|embedding| embedding.values)
                .collect();
            Ok(EmbedResponse { vectors, usage })
        }),
    }
}

fn build_single_embed_body(request: &EmbedRequest, input: &str) -> Result<Value, KitError> {
    let mut body = Map::new();
    body.insert("content".to_string(), embedding_input_to_content(input));
    apply_common_embed_fields(&mut body, request);
    Ok(Value::Object(body))
}

fn apply_common_embed_fields(target: &mut Map<String, Value>, request: &EmbedRequest) {
    if let Some(output_dimensionality) = request.dimensions() {
        target.insert(
            "outputDimensionality".to_string(),
            json!(output_dimensionality.get()),
        );
    }
}

fn build_batch_embed_body(request: &EmbedRequest, model: &str) -> Result<Value, KitError> {
    let requests = request
        .inputs()
        .iter()
        .map(|input| {
            let mut single = Map::new();
            single.insert("model".to_string(), Value::String(model.to_string()));
            single.insert("content".to_string(), embedding_input_to_content(input));
            apply_common_embed_fields(&mut single, request);
            Value::Object(single)
        })
        .collect::<Vec<_>>();
    Ok(json!({"requests": requests}))
}

fn embedding_input_to_content(input: &str) -> Value {
    json!({"parts": [{"text": input}]})
}

fn usage_from_embedding_metadata(metadata: Option<&GeminiUsageMetadata>) -> Usage {
    metadata
        .map(|metadata| {
            Usage::reported_tokens(metadata.prompt_token_count, metadata.candidates_token_count)
                .with_reported_total(metadata.total_token_count)
                .with_cached_input(metadata.cached_content_token_count)
                .with_reasoning(metadata.thoughts_token_count)
        })
        .unwrap_or_default()
}
