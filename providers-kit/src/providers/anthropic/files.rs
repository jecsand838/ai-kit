// Anthropic file lifecycle stays provider-owned through private AnthropicClient
// file operations. Whether a file can be reused in a Messages request is a
// concrete Claude model validation decision. Do not expose Anthropic file
// clients or native bypasses publicly. Source:
// https://docs.anthropic.com/en/docs/build-with-claude/files
use reqwest::Method;
use reqwest::multipart::Form;
use std::num::NonZeroUsize;

use crate::KitError;
use crate::error::ValidationViolation;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::transport::HttpResponse;

use crate::core::multipart_file_part;
use crate::files::{FileListRequest, FileObject, FilePage, FileUploadRequest};
use crate::generation::FileReference;
use crate::transport::CheckedRequestSpec;

use super::api::{ANTHROPIC_REQUESTS, AnthropicApiContext};
use super::wire::{AnthropicDeletedFile, AnthropicFileObject, AnthropicList};

pub(crate) async fn upload_file(
    api: &AnthropicApiContext,
    request: FileUploadRequest,
) -> Result<FileObject, KitError> {
    let (file_name, media_type, data) = request.into_parts();
    upload_file_bytes(api, file_name, media_type, data)
        .await?
        .normalize(normalize_file)
}

pub(crate) async fn list_files(
    api: &AnthropicApiContext,
    request: FileListRequest,
) -> Result<FilePage, KitError> {
    let (limit, cursor) = request.into_parts();
    let mut path = "v1/files".to_string();
    let query = {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        if let Some(limit) = limit {
            query.append_pair("limit", &limit.to_string());
        }
        if let Some(cursor) = cursor {
            query.append_pair("page", &cursor);
        }
        query.finish()
    };
    if !query.is_empty() {
        path.push('?');
        path.push_str(&query);
    }
    let listed = ANTHROPIC_REQUESTS
        .json::<AnthropicList<AnthropicFileObject>>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await?;
    listed.normalize(|listed| {
        let next_cursor = listed.validated_page("Anthropic file list")?;
        Ok(FilePage {
            files: listed
                .data
                .into_iter()
                .map(normalize_file)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        })
    })
}

pub(crate) async fn file_info(
    api: &AnthropicApiContext,
    file_id: String,
) -> Result<FileObject, KitError> {
    validate_file_id(&file_id)?;
    let path = format!("v1/files/{file_id}");
    let file = ANTHROPIC_REQUESTS
        .json::<AnthropicFileObject>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await?;
    file.normalize(|file| {
        if file.id != file_id {
            return Err(
                KitError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "file.id",
                })
                .with_message("Anthropic file lookup returned a different file id".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)),
            );
        }
        normalize_file(file)
    })
}

pub(crate) async fn delete_file(
    api: &AnthropicApiContext,
    file_id: String,
) -> Result<(), KitError> {
    validate_file_id(&file_id)?;
    let path = format!("v1/files/{file_id}");
    let body = ANTHROPIC_REQUESTS
        .text(
            &path,
            api.request(Method::DELETE, &path)?,
            api.transport().execution(),
        )
        .await?;
    body.normalize(|body| {
        let deleted: AnthropicDeletedFile = serde_json::from_str(&body).map_err(|err| {
            KitError::invalid_response(ResponseViolation::from_json_error(&err, "response"))
                .with_response_details(crate::error::ResponseDetails::json(&err))
                .with_message(format!(
                    "failed to decode Anthropic file delete response from {path}: {err}"
                ))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
                .at_phase(ErrorPhase::Decode)
        })?;
        if deleted.kind != "file_deleted" || deleted.id != file_id {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "file deletion must confirm requested identity",
                })
                .with_message(
                    "Anthropic file deletion did not confirm the requested file".to_string(),
                )
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes()))),
            );
        }
        Ok(())
    })
}

pub(crate) async fn download_file(
    api: &AnthropicApiContext,
    file_id: String,
    max_bytes: NonZeroUsize,
) -> Result<Vec<u8>, KitError> {
    validate_file_id(&file_id)?;
    let metadata_path = format!("v1/files/{file_id}");
    let received = ANTHROPIC_REQUESTS
        .json::<AnthropicFileObject>(
            &metadata_path,
            api.request(Method::GET, &metadata_path)?,
            api.transport().execution(),
        )
        .await?;
    received.normalize(|file| {
        if file.id != file_id {
            return Err(
                KitError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "file.id",
                })
                .with_message(
                    "Anthropic file download lookup returned a different file id".to_string(),
                )
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)),
            );
        }
        ensure_file_downloadable(&file)
    })?;
    let path = format!("{metadata_path}/content");
    ANTHROPIC_REQUESTS
        .bytes_with_limit(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
            max_bytes.get(),
        )
        .await
        .map(HttpResponse::into_value)
}

fn validate_file_id(file_id: &str) -> Result<(), KitError> {
    let valid = file_id.strip_prefix("file_").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    });
    if !valid {
        return Err(KitError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "file.id",
                rule: "exact file_ resource identifier",
            },
            "Anthropic file id must use the exact `file_...` resource shape",
        ));
    }
    Ok(())
}

pub(crate) async fn upload_file_bytes(
    api: &AnthropicApiContext,
    file_name: String,
    media_type: String,
    data: Vec<u8>,
) -> Result<HttpResponse<AnthropicFileObject>, KitError> {
    let file_part = multipart_file_part(file_name, media_type, data)?;
    let form = Form::new().part("file", file_part);
    ANTHROPIC_REQUESTS
        .json::<AnthropicFileObject>(
            "v1/files",
            api.request(Method::POST, "v1/files")?.multipart(form),
            api.transport().execution(),
        )
        .await
}

pub(crate) fn normalize_file(file: AnthropicFileObject) -> Result<FileObject, KitError> {
    if validate_file_id(&file.id).is_err() {
        return Err(KitError::invalid_response(ResponseViolation::InvalidField {
            field: "file.id",
        })
        .with_message("Anthropic file response included an invalid resource id".to_string())
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)));
    }
    let input_reference = matches!(
        file.mime_type.as_deref(),
        Some(
            "image/jpeg"
                | "image/png"
                | "image/gif"
                | "image/webp"
                | "application/pdf"
                | "text/plain"
        )
    )
    .then(|| FileReference::Provider {
        id: file.id.clone(),
        media_type: file.mime_type.clone(),
    });
    Ok(FileObject {
        id: file.id,
        file_name: file.filename,
        size_bytes: file.size_bytes,
        media_type: file.mime_type,
        created_at: file.created_at,
        state: None,
        input_reference,
    })
}

fn ensure_file_downloadable(file: &AnthropicFileObject) -> Result<(), KitError> {
    if file.downloadable != Some(true) {
        return Err(KitError::unsupported(
            ValidationViolation::Unsupported {
                field: "files.download",
                rule: "resource must allow downloading",
            },
            "Anthropic file is not downloadable",
        ));
    }
    Ok(())
}
