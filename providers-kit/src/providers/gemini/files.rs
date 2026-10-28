// Gemini file lifecycle stays provider-owned through private GeminiClient file
// operations and upload helpers. Resumable upload/download/list/delete are
// direct ProviderBackend behavior. Chat-capable Gemini
// model receivers own whether they can consume uploaded file references. Do not
// expose Gemini file clients, upload URLs, or native file bypasses publicly.
// Source:
// https://ai.google.dev/gemini-api/docs/files
use std::num::NonZeroUsize;
use url::Url;

use crate::KitError;
use crate::error::ResponseViolation;
use crate::error::ValidationViolation;

use crate::files::{FileListRequest, FileObject, FilePage, FileState, FileUploadRequest};
use crate::generation::FileReference;
use crate::transport::CheckedRequestSpec;

use reqwest::Method;

use super::api::{GEMINI_REQUESTS, GeminiApiContext};
use super::wire::{GeminiFileObject, GeminiFileUploadEnvelope, GeminiFilesList};

pub(crate) async fn upload_file(
    api: &GeminiApiContext,
    request: FileUploadRequest,
) -> Result<FileObject, KitError> {
    let (file_name, media_type, data) = request.into_parts();
    let upload_url = api
        .start_resumable_upload(&file_name, &media_type, data.len())
        .await?;
    let envelope = api
        .upload_bytes::<GeminiFileUploadEnvelope>(upload_url, &data)
        .await?;
    envelope.normalize(|envelope| file_object_from_gemini(envelope.file))
}

pub(crate) async fn list_files(
    api: &GeminiApiContext,
    request: FileListRequest,
) -> Result<FilePage, KitError> {
    let (limit, cursor) = request.into_parts();
    let url = api.absolute_url_with_page(
        "v1beta/files",
        limit.map(std::num::NonZeroUsize::get),
        cursor.as_deref(),
    )?;
    let response = GEMINI_REQUESTS
        .json::<GeminiFilesList>(
            url.as_str(),
            api.request_absolute(Method::GET, url.clone(), true),
            api.transport().execution(),
        )
        .await?;
    response.normalize(|response| {
        let next_cursor = match response.next_page_token.as_deref() {
            Some(cursor) if cursor.trim().is_empty() => {
                return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "nextPageToken",
                })
                .with_message("Gemini file list returned an empty next page token".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&response)));
            }
            Some(cursor) => Some(cursor.to_string()),
            None => None,
        };
        Ok(FilePage {
            files: response
                .files
                .into_iter()
                .map(file_object_from_gemini)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        })
    })
}

pub(crate) async fn file_info(
    api: &GeminiApiContext,
    file_id: String,
) -> Result<FileObject, KitError> {
    validate_file_id(&file_id)?;
    let path = format!("v1beta/{file_id}");
    let response = GEMINI_REQUESTS
        .json::<GeminiFileObject>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await?;
    response.normalize(|response| {
        if response.name != file_id {
            return Err(
                KitError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "file.name",
                })
                .with_message("Gemini file lookup returned a different lifecycle id".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&response)),
            );
        }
        file_object_from_gemini(response)
    })
}

pub(crate) async fn delete_file(api: &GeminiApiContext, file_id: String) -> Result<(), KitError> {
    validate_file_id(&file_id)?;
    let path = format!("v1beta/{file_id}");
    let response = GEMINI_REQUESTS
        .json::<serde_json::Map<String, serde_json::Value>>(
            &path,
            api.request(Method::DELETE, &path)?,
            api.transport().execution(),
        )
        .await?;
    response.normalize(|response| {
        if !response.is_empty() {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "file deletion response must be an empty object",
                })
                .with_message("Gemini file deletion must return an empty JSON object".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&response)),
            );
        }
        Ok(())
    })
}

pub(crate) async fn download_file(
    _api: &GeminiApiContext,
    file_id: String,
    _max_bytes: NonZeroUsize,
) -> Result<Vec<u8>, KitError> {
    validate_file_id(&file_id)?;
    Err(KitError::unsupported(
        ValidationViolation::Unsupported {
            field: "files.download",
            rule: "uploaded media download unavailable",
        },
        "Gemini uploaded Files API media cannot be downloaded",
    ))
}

pub(super) fn validate_file_id(file_id: &str) -> Result<(), KitError> {
    let valid = file_id.strip_prefix("files/").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    });
    if !valid {
        return Err(KitError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "file.id",
                rule: "exact files/ resource identifier",
            },
            "Gemini file id must use the exact `files/...` resource shape",
        ));
    }
    Ok(())
}

pub(crate) fn file_object_from_gemini(file: GeminiFileObject) -> Result<FileObject, KitError> {
    if validate_file_id(&file.name).is_err() {
        return Err(KitError::invalid_response(ResponseViolation::InvalidField {
            field: "file.name",
        })
        .with_message("Gemini file response included an invalid lifecycle id".to_string())
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)));
    }
    let size_bytes = file
        .size_bytes
        .as_deref()
        .map(|value| {
            value.parse::<u64>().map_err(|err| {
                KitError::invalid_response(ResponseViolation::InvalidField { field: "sizeBytes" })
                    .with_message(format!(
                        "Gemini file size_bytes was not a valid integer: {err}"
                    ))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(value))
            })
        })
        .transpose()?;

    let input_reference = file
        .uri
        .as_ref()
        .map(|uri| {
            Url::parse(uri)
                .map(|url| FileReference::Url {
                    url,
                    media_type: file.mime_type.clone(),
                })
                .map_err(|err| {
                    KitError::invalid_response(ResponseViolation::InvalidField { field: "uri" })
                        .with_message(format!("Gemini file input URI was invalid: {err}"))
                        .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(uri.as_bytes())))
                })
        })
        .transpose()?;
    if input_reference
        .as_ref()
        .is_some_and(|reference| reference.validate().is_err())
    {
        return Err(
            KitError::invalid_response(ResponseViolation::InvalidField { field: "uri" })
                .with_message(
                    "Gemini file response included an unusable generation reference".to_string(),
                )
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)),
        );
    }
    let state = file_state_from_gemini(file.state.as_deref(), file.error)?;
    Ok(FileObject {
        id: file.name,
        file_name: file.display_name,
        size_bytes,
        media_type: file.mime_type,
        created_at: file.create_time,
        state,
        input_reference,
    })
}

fn file_state_from_gemini(
    state: Option<&str>,
    error: Option<super::wire::GeminiErrorObject>,
) -> Result<Option<FileState>, KitError> {
    match state {
        Some("PROCESSING") => Ok(Some(FileState::Processing)),
        Some("ACTIVE") => Ok(Some(FileState::Active)),
        Some("FAILED") => Ok(Some(FileState::Failed {
            code: error.as_ref().map(|error| error.code.to_string()),
            message: error.map(|error| error.message),
        })),
        Some(other) => Err(
            KitError::invalid_response(ResponseViolation::UnknownVariant { field: "state" })
                .with_message(format!("Gemini returned unknown file state `{other}`"))
                .with_diagnostic(None),
        ),
        None if error.is_some() => Ok(Some(FileState::Failed {
            code: error.as_ref().map(|error| error.code.to_string()),
            message: error.map(|error| error.message),
        })),
        None => Ok(None),
    }
}
