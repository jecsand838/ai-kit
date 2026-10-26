// Discovery is descriptive. A returned model id does not imply portable
// operation support. Source: https://developers.openai.com/api/docs/models
use crate::SdkError;
use crate::error::ValidationViolation;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::models::ModelId;
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::transport::CheckedRequestSpec;

use reqwest::Method;

use crate::providers::openai::api::{OPENAI_REQUESTS, OpenAiApiContext};
use crate::providers::openai::wire::{OpenAiList, OpenAiModelObject};

pub(crate) async fn list_models(
    api: &OpenAiApiContext,
    request: ModelListRequest,
) -> Result<ModelPage, SdkError> {
    let (limit, cursor) = request.into_parts();
    if cursor.is_some() {
        return Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "models.cursor",
                rule: "pagination cursor unavailable",
            },
            "OpenAI model-list cursors",
        ));
    }
    let models = OPENAI_REQUESTS
        .json::<OpenAiList<OpenAiModelObject>>(
            "models",
            api.request(Method::GET, "models")?,
            api.transport().retryable_execution(),
        )
        .await?;
    models.normalize(|models| {
        let limit = limit.map_or(usize::MAX, std::num::NonZeroUsize::get);
        Ok(ModelPage {
            models: models
                .data
                .into_iter()
                .take(limit)
                .map(normalize_model)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: None,
        })
    })
}

pub(crate) async fn model_info(
    api: &OpenAiApiContext,
    id: ModelId,
) -> Result<ModelSummary, SdkError> {
    validate_model_path_id(&id)?;
    let path = format!("models/{id}");
    let model = OPENAI_REQUESTS
        .json::<OpenAiModelObject>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().retryable_execution(),
        )
        .await?;
    model.normalize(|model| {
        let body = CheckedRequestSpec::diagnostic_json(&model);
        let summary = normalize_model(model)?;
        if summary.id != id {
            return Err(
                SdkError::invalid_response(ResponseViolation::IdentityMismatch { field: "id" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message("OpenAI model lookup returned a different model id".to_string())
                    .with_diagnostic(body),
            );
        }
        Ok(summary)
    })
}

fn validate_model_path_id(id: &ModelId) -> Result<(), SdkError> {
    if !id
        .as_str()
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(SdkError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "model_id",
                rule: "exact path-safe identifier",
            },
            "OpenAI model metadata requires an exact path-safe model id",
        ));
    }
    Ok(())
}

fn normalize_model(model: OpenAiModelObject) -> Result<ModelSummary, SdkError> {
    let body = CheckedRequestSpec::diagnostic_json(&model);
    let id = ModelId::new(model.id).map_err(|_| {
        SdkError::invalid_response(ResponseViolation::InvalidField { field: "id" })
            .at_phase(ErrorPhase::Normalize)
            .with_message("OpenAI model response contained an invalid model id".to_string())
            .with_diagnostic(body)
    })?;
    Ok(ModelSummary {
        id,
        display_name: None,
        description: None,
        context_window_tokens: None,
    })
}
