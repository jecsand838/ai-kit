// Discovery is descriptive and never implies operation support. Source:
// https://platform.claude.com/docs/en/about-claude/models/overview
use crate::KitError;
use crate::error::ResponseViolation;
use crate::error::ValidationViolation;

use crate::models::ModelId;
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::transport::CheckedRequestSpec;

use reqwest::Method;

use crate::providers::anthropic::api::{ANTHROPIC_REQUESTS, AnthropicApiContext};
use crate::providers::anthropic::wire::{AnthropicList, AnthropicModelInfo};

pub(crate) async fn list_models(
    api: &AnthropicApiContext,
    request: ModelListRequest,
) -> Result<ModelPage, KitError> {
    let (limit, cursor) = request.into_parts();
    let mut path = "v1/models".to_string();
    let query = {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        if let Some(limit) = limit {
            query.append_pair("limit", &limit.to_string());
        }
        if let Some(cursor) = cursor {
            query.append_pair("after_id", &cursor);
        }
        query.finish()
    };
    if !query.is_empty() {
        path.push('?');
        path.push_str(&query);
    }
    let models = ANTHROPIC_REQUESTS
        .json::<AnthropicList<AnthropicModelInfo>>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().retryable_execution(),
        )
        .await?;
    models.normalize(|models| {
        let next_cursor = models.validated_after_id("Anthropic model list")?;
        Ok(ModelPage {
            models: models
                .data
                .into_iter()
                .map(normalize_model)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        })
    })
}

pub(crate) async fn model_info(
    api: &AnthropicApiContext,
    id: ModelId,
) -> Result<ModelSummary, KitError> {
    validate_model_path_id(&id)?;
    let path = format!("v1/models/{id}");
    let model = ANTHROPIC_REQUESTS
        .json::<AnthropicModelInfo>(
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
                KitError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "model.id",
                })
                .with_message("Anthropic model lookup returned a different model id".to_string())
                .with_diagnostic(body),
            );
        }
        Ok(summary)
    })
}

fn validate_model_path_id(id: &ModelId) -> Result<(), KitError> {
    if !id
        .as_str()
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(KitError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "model_id",
                rule: "exact path-safe identifier",
            },
            "Anthropic model metadata requires an exact path-safe model id",
        ));
    }
    Ok(())
}

fn normalize_model(model: AnthropicModelInfo) -> Result<ModelSummary, KitError> {
    let body = CheckedRequestSpec::diagnostic_json(&model);
    let id = ModelId::new(model.id).map_err(|_| {
        KitError::invalid_response(ResponseViolation::InvalidField { field: "model.id" })
            .with_message("Anthropic model response contained an invalid model id".to_string())
            .with_diagnostic(body)
    })?;
    Ok(ModelSummary {
        id,
        display_name: model.display_name,
        description: model.description,
        context_window_tokens: model.max_input_tokens,
    })
}
