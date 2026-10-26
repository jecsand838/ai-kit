// Discovery is descriptive and never dynamically unlocks an operation. Source:
// https://ai.google.dev/gemini-api/docs/models
use crate::SdkError;
use crate::error::ResponseViolation;

use crate::models::ModelId;
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::transport::CheckedRequestSpec;
use reqwest::Method;

use crate::providers::gemini::api::{GEMINI_REQUESTS, GeminiApiContext};
use crate::providers::gemini::wire::{GeminiModelInfo, GeminiModelsList};

pub(crate) async fn list_models(
    api: &GeminiApiContext,
    request: ModelListRequest,
) -> Result<ModelPage, SdkError> {
    let (limit, cursor) = request.into_parts();
    let url = api.absolute_url_with_page(
        "v1beta/models",
        limit.map(std::num::NonZeroUsize::get),
        cursor.as_deref(),
    )?;
    let response = GEMINI_REQUESTS
        .json::<GeminiModelsList>(
            url.as_str(),
            api.request_absolute(Method::GET, url.clone(), true),
            api.transport().retryable_execution(),
        )
        .await?;
    response.normalize(|response| {
        let next_cursor = match response.next_page_token.as_deref() {
            Some(cursor) if cursor.trim().is_empty() => {
                return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                    field: "nextPageToken",
                })
                .with_message("Gemini model list returned an empty next page token".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&response)));
            }
            Some(cursor) => Some(cursor.to_string()),
            None => None,
        };
        Ok(ModelPage {
            models: response
                .models
                .into_iter()
                .map(model_summary_from_gemini)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        })
    })
}

pub(crate) async fn model_info(
    api: &GeminiApiContext,
    id: ModelId,
) -> Result<ModelSummary, SdkError> {
    let model_id = api.exact_model_id("Gemini model lookups", id.as_str())?;
    let path = format!("v1beta/{model_id}");
    let response = GEMINI_REQUESTS
        .json::<GeminiModelInfo>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().retryable_execution(),
        )
        .await?;
    response.normalize(|response| {
        let body = CheckedRequestSpec::diagnostic_json(&response);
        if response.name != model_id {
            return Err(
                SdkError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "model.name",
                })
                .with_message("Gemini model lookup returned a different model id".to_string())
                .with_diagnostic(body),
            );
        }
        model_summary_from_gemini(response)
    })
}

pub(crate) fn model_summary_from_gemini(model: GeminiModelInfo) -> Result<ModelSummary, SdkError> {
    let body = CheckedRequestSpec::diagnostic_json(&model);
    let id = ModelId::new(model.name).map_err(|_| {
        SdkError::invalid_response(ResponseViolation::InvalidField {
            field: "model.name",
        })
        .with_message("Gemini model response contained an invalid model id".to_string())
        .with_diagnostic(body)
    })?;
    Ok(ModelSummary {
        id,
        display_name: model.display_name,
        description: model.description,
        context_window_tokens: model.input_token_limit,
    })
}
