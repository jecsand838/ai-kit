// Installed-model discovery is descriptive and never implies operation support.
// Source: https://docs.ollama.com/api/tags
use crate::SdkError;
use crate::error::ValidationViolation;
use crate::error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation, ResponseViolation};
use crate::models::ModelId;
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::transport::CheckedRequestSpec;
use crate::transport::HttpResponse;

use reqwest::Method;

use crate::providers::ollama::api::{OLLAMA_REQUESTS, OllamaApiContext};
use crate::providers::ollama::wire::{OllamaModelInfo, OllamaShowResponse, OllamaTagsResponse};

struct OllamaShowModelRequest {
    model: String,
    verbose: bool,
}

impl OllamaShowModelRequest {
    async fn execute(self, api: &OllamaApiContext) -> Result<OllamaShowResponse, SdkError> {
        let body = serde_json::json!({
            "model": self.model,
            "verbose": self.verbose,
        });
        OLLAMA_REQUESTS
            .json(
                "show",
                api.request(Method::POST, "show")?.json(&body),
                api.transport().execution(),
            )
            .await
            .map(HttpResponse::into_value)
    }
}

pub(crate) async fn list_models(
    api: &OllamaApiContext,
    request: ModelListRequest,
) -> Result<ModelPage, SdkError> {
    let (limit, cursor) = request.into_parts();
    if cursor.is_some() {
        return Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "models.cursor",
                rule: "pagination cursor unavailable",
            },
            "Ollama model-list cursors",
        ));
    }
    let raw = fetch_tags(api).await?;
    raw.normalize(|raw| {
        let limit = limit.map_or(usize::MAX, std::num::NonZeroUsize::get);
        Ok(ModelPage {
            models: raw
                .models
                .iter()
                .take(limit)
                .map(model_summary_from_tag)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: None,
        })
    })
}

pub(crate) async fn model_info(
    api: &OllamaApiContext,
    model_id: ModelId,
) -> Result<ModelSummary, SdkError> {
    let received = fetch_tags(api).await?;
    let mut summary = received.normalize(|tags| {
        let tag = find_exact_model_tag(&tags, model_id.as_str())?;
        model_summary_from_tag(tag)
    })?;
    let show = OllamaShowModelRequest {
        model: summary.id.as_str().to_string(),
        verbose: false,
    }
    .execute(api)
    .await?;
    summary.context_window_tokens = context_window(&show);
    Ok(summary)
}

async fn fetch_tags(api: &OllamaApiContext) -> Result<HttpResponse<OllamaTagsResponse>, SdkError> {
    OLLAMA_REQUESTS
        .json::<OllamaTagsResponse>(
            "tags",
            api.request(Method::GET, "tags")?,
            api.transport().execution(),
        )
        .await
}

fn find_exact_model_tag<'a>(
    tags: &'a OllamaTagsResponse,
    model_id: &str,
) -> Result<&'a OllamaModelInfo, SdkError> {
    tags.models
        .iter()
        .find(|model| model.model.as_deref() == Some(model_id))
        .ok_or_else(|| {
            SdkError::new(
                ErrorKind::NotFound,
                Operation::ModelInfo,
                ErrorPhase::Normalize,
            )
            .with_request_target(model_id)
            .with_native_details(NativeErrorDetails {
                message: Some("requested model was absent from the installed catalog".to_owned()),
                ..NativeErrorDetails::default()
            })
        })
}

fn model_summary_from_tag(model: &OllamaModelInfo) -> Result<ModelSummary, SdkError> {
    let body = CheckedRequestSpec::diagnostic_json(model);
    let id = match model.model.clone() {
        Some(id) => id,
        None => {
            return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                field: "model",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message("Ollama tags response did not include model identity".to_string())
            .with_diagnostic(body));
        }
    };
    let id = ModelId::new(id).map_err(|_| {
        SdkError::invalid_response(ResponseViolation::InvalidField { field: "model" })
            .at_phase(ErrorPhase::Normalize)
            .with_message("Ollama tags response contained an invalid model id".to_string())
            .with_diagnostic(body)
    })?;
    Ok(ModelSummary {
        id,
        display_name: model.name.clone(),
        description: None,
        context_window_tokens: None,
    })
}

fn context_window(show: &OllamaShowResponse) -> Option<u64> {
    let model_info = show.model_info.as_ref()?;
    model_info.iter().find_map(|(key, value)| {
        if key == "context_length" || key.ends_with(".context_length") {
            value.as_u64()
        } else {
            None
        }
    })
}
