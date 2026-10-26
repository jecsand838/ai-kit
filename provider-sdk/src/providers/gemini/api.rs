use std::sync::Arc;

use reqwest::Method;
use url::Url;

use super::wire::GeminiErrorEnvelope;
use crate::SdkError;
use crate::error::ValidationViolation;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::transport::HttpResponse;

use crate::config::GeminiConfig;
use crate::config::TransportConfig;
use crate::transport::{AuthMode, AuthScheme, CheckedRequestSpec, ProviderHttpContext};

#[derive(Clone)]
pub(crate) struct GeminiApiContext {
    config: GeminiConfig,
    transport: ProviderHttpContext,
}

pub(super) const GEMINI_REQUESTS: CheckedRequestSpec =
    CheckedRequestSpec::new(GeminiErrorEnvelope::parse);

impl GeminiApiContext {
    pub(crate) fn shared(
        config: GeminiConfig,
        transport: TransportConfig,
    ) -> Result<Arc<Self>, SdkError> {
        let transport = ProviderHttpContext::new(
            config.base_url().clone(),
            AuthScheme::Header {
                name: "x-goog-api-key".to_string(),
                value: config.api_key().clone(),
                prefix: None,
            },
            transport,
        )?;

        Ok(Arc::new(Self { config, transport }))
    }

    pub(crate) fn config(&self) -> &GeminiConfig {
        &self.config
    }

    pub(crate) fn transport(&self) -> &ProviderHttpContext {
        &self.transport
    }

    pub(crate) fn exact_model_id(
        &self,
        target: &'static str,
        model: &str,
    ) -> Result<String, SdkError> {
        if model.strip_prefix("models/").is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        }) {
            Ok(model.to_string())
        } else {
            Err(SdkError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "model_id",
                    rule: "exact models/ resource identifier",
                },
                format!(
                    "{target} must use exact Gemini model ids like `models/{{model}}`; got `{model}`"
                ),
            ))
        }
    }

    pub(crate) fn request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder, SdkError> {
        self.transport.request(method, path)
    }

    pub(crate) fn request_absolute(
        &self,
        method: Method,
        url: Url,
        authenticated: bool,
    ) -> reqwest::RequestBuilder {
        let auth = if authenticated {
            AuthMode::Apply
        } else {
            AuthMode::Skip
        };
        self.transport.request_absolute(method, url, auth)
    }

    pub(crate) fn absolute_url(&self, path: &str) -> Result<Url, SdkError> {
        self.transport.url(path)
    }

    pub(crate) fn absolute_url_with_page(
        &self,
        path: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<Url, SdkError> {
        self.transport.url_with_query(path, |url| {
            let mut pairs = url.query_pairs_mut();
            if let Some(limit) = limit {
                pairs.append_pair("pageSize", &limit.to_string());
            }
            if let Some(cursor) = cursor {
                pairs.append_pair("pageToken", cursor);
            }
        })
    }

    pub(crate) async fn start_resumable_upload(
        &self,
        file_name: &str,
        media_type: &str,
        content_length: usize,
    ) -> Result<Url, SdkError> {
        let url = self.absolute_url("upload/v1beta/files")?;
        let body = serde_json::json!({
            "file": {
                "display_name": file_name
            }
        });
        let response = self
            .request_absolute(Method::POST, url.clone(), true)
            .header("X-Goog-Upload-Protocol", "resumable")
            .header("X-Goog-Upload-Command", "start")
            .header(
                "X-Goog-Upload-Header-Content-Length",
                content_length.to_string(),
            )
            .header("X-Goog-Upload-Header-Content-Type", media_type)
            .header("Content-Type", "application/json")
            .json(&body);
        let response = GEMINI_REQUESTS
            .response(url.as_str(), response, self.transport.execution())
            .await?;
        HttpResponse::from_response(response).normalize(|response| {
            let upload_url = response
                .headers()
                .get("x-goog-upload-url")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    SdkError::invalid_response(ResponseViolation::MissingField {
                        field: "x-goog-upload-url",
                    })
                    .with_message(
                        "Gemini resumable upload response did not include x-goog-upload-url"
                            .to_string(),
                    )
                    .with_diagnostic(None)
                    .at_phase(ErrorPhase::ResponseHeaders)
                })?;
            let upload_url = Url::parse(upload_url).map_err(|_| {
                SdkError::invalid_response(ResponseViolation::InvalidField {
                    field: "x-goog-upload-url",
                })
                .with_message("Gemini upload URL was invalid".to_string())
                .with_diagnostic(None)
                .at_phase(ErrorPhase::ResponseHeaders)
            })?;
            if !matches!(upload_url.scheme(), "http" | "https")
                || upload_url.host_str().is_none()
                || !upload_url.username().is_empty()
                || upload_url.password().is_some()
            {
                return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                    field: "x-goog-upload-url",
                })
                .with_message(
                    "Gemini upload URL must be HTTP(S) with a host and no userinfo".to_string(),
                )
                .with_diagnostic(None)
                .at_phase(ErrorPhase::ResponseHeaders));
            }
            Ok(upload_url)
        })
    }

    pub(crate) async fn upload_bytes<T>(
        &self,
        upload_url: Url,
        data: &[u8],
    ) -> Result<HttpResponse<T>, SdkError>
    where
        T: serde::de::DeserializeOwned,
    {
        let upload_path = upload_url.to_string();
        GEMINI_REQUESTS
            .json(
                &upload_path,
                self.request_absolute(Method::POST, upload_url, false)
                    .header("X-Goog-Upload-Offset", "0")
                    .header("X-Goog-Upload-Command", "upload, finalize")
                    .header("Content-Length", data.len().to_string())
                    .body(data.to_vec()),
                self.transport.execution(),
            )
            .await
    }
}
