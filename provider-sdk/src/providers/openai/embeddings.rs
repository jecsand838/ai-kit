use reqwest::Method;
use serde_json::{Map, Value};

use crate::SdkError;
use crate::embedding::EmbedResponse;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::transport::CheckedRequestSpec;
use crate::transport::HttpResponse;

use super::api::{OPENAI_REQUESTS, OpenAiApiContext, insert_non_null};
use super::responses::usage_from_openai;
use super::wire::OpenAiEmbeddingsResponse;

pub(crate) fn build_embeddings_body(
    model: &str,
    request: &crate::embedding::EmbedRequest,
) -> Result<(Value, usize), SdkError> {
    let input_count = request.inputs().len();
    let input = request
        .inputs()
        .iter()
        .cloned()
        .map(Value::String)
        .collect();

    let mut body = Map::new();
    body.insert("model".to_string(), Value::String(model.to_string()));
    body.insert("input".to_string(), Value::Array(input));
    insert_non_null(
        &mut body,
        "dimensions",
        request
            .dimensions()
            .map(|value| Value::Number(serde_json::Number::from(value.get()))),
    );
    body.insert(
        "encoding_format".to_string(),
        Value::String("float".to_string()),
    );
    Ok((Value::Object(body), input_count))
}

pub(crate) async fn create_raw_embeddings(
    api: &OpenAiApiContext,
    body: Value,
) -> Result<HttpResponse<OpenAiEmbeddingsResponse>, SdkError> {
    OPENAI_REQUESTS
        .json::<OpenAiEmbeddingsResponse>(
            "embeddings",
            api.request(Method::POST, "embeddings")?.json(&body),
            api.transport().execution(),
        )
        .await
}

pub(crate) fn normalize_embeddings_response(
    response: OpenAiEmbeddingsResponse,
    input_count: usize,
) -> Result<EmbedResponse, SdkError> {
    let usage = usage_from_openai(response.usage.as_ref());
    let body = CheckedRequestSpec::diagnostic_json(&response);
    if response.data.len() != input_count {
        return Err(
            SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                rule: "embedding_count",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message(format!(
                "OpenAI embeddings response returned {} vectors for {input_count} inputs",
                response.data.len()
            ))
            .with_diagnostic(body),
        );
    }
    let mut vectors = std::iter::repeat_with(|| None)
        .take(input_count)
        .collect::<Vec<Option<Vec<f32>>>>();
    for item in response.data {
        let Some(slot) = vectors.get_mut(item.index) else {
            return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "data.index",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message(format!(
                "OpenAI embeddings response included out-of-range index {}",
                item.index
            ))
            .with_diagnostic(body));
        };
        if slot.is_some() || item.embedding.is_empty() {
            return Err(
                SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "unique_nonempty_embeddings",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "OpenAI embeddings response included duplicate or empty index {}",
                    item.index
                ))
                .with_diagnostic(body),
            );
        }
        *slot = Some(item.embedding);
    }
    let vectors = vectors
        .into_iter()
        .enumerate()
        .map(|(index, vector)| {
            vector.ok_or_else(|| {
                SdkError::invalid_response(ResponseViolation::MissingField {
                    field: "data.embedding",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!("OpenAI embeddings response omitted index {index}"))
                .with_diagnostic(body.clone())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(EmbedResponse { vectors, usage })
}
